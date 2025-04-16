/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.execution

import org.apache.arrow.c.ArrowSchema
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.iceberg.spark.source.IcebergWriteUtil
import org.apache.iceberg.{DataFile, DataFiles, Metrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.{BatchWrite, PhysicalWriteInfo, Write, WriterCommitMessage}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkException, TaskContext}

case class IcebergAppendDataExec( override val query: SparkPlan,
                                  override val refreshCache: () => Unit,
                                  override val write: Write) extends V2ExistingTableWriteExec with ValidatablePlan {

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergAppendDataExec =
    copy(query = newChild)

  def writingTaskBatch:WritingColumnarBatchSparkTask[_] = DataWritingColumnarBatchSparkTask

  override def doValidateInternal(): ValidationResult = {
    if (IcebergWriteUtil.supportedDataType(write)) {
      return ValidationResult.failed("Contains unsupported data type")
    }
    ValidationResult.succeeded
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  def getJniWrapper(localSchema: StructType, timezoneId: String): (Long, IcebergWriteJniWrapper) = {
    val schema = SparkArrowUtil.toArrowSchema(localSchema, timezoneId)
    val arrowAlloc = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(arrowAlloc)
    ArrowAbiUtil.exportSchema(arrowAlloc, schema, cSchema)
    val runtime = Runtimes.contextInstance(
      BackendsApiManager.getBackendName, "IcebergWrite#write")
    val jniWrapper = IcebergWriteJniWrapper.create(runtime)
    val writer = jniWrapper.init(cSchema.memoryAddress())
    cSchema.close()
    (writer, jniWrapper)
  }

  def writeBatch(writer: Long, jniWrapper: IcebergWriteJniWrapper, batch: ColumnarBatch): String = {
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    jniWrapper.write(writer, batchHandle)
  }

  protected def writeColumnarBatchWithV2(batchWrite: BatchWrite, writerHandle: Long,
                                         jniWrapper: IcebergWriteJniWrapper): ColumnarBatch = {
    val rdd: RDD[ColumnarBatch] = {
      val tempRdd = query.executeColumnar()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else {
        tempRdd
      }
    }
    // introduce a local var to avoid serializing the whole class
    val task = writingTaskBatch
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(s"Start processing data source write support: $batchWrite. " +
      s"The input RDD has ${messages.length} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = customMetrics

    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[ColumnarBatch]) =>
          task.run(writerHandle, jniWrapper, context, iter, writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingColumnarBatchSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      logInfo(s"Data source write support $batchWrite is committing.")
      batchWrite.commit(messages)
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(StreamWriterCommitProgressUtil.getStreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException(
              errorClass = "_LEGACY_ERROR_TEMP_2070",
              messageParameters = Map.empty,
              cause = cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        throw cause
    }

    null
  }

  protected def runColumnarBatch(): ColumnarBatch = {
    val batchWrite = write.toBatch
    val(writerHandle, jniWrapper) = getJniWrapper(schema, SQLConf.get.sessionLocalTimeZone)
    val writtenRows = writeColumnarBatchWithV2(write.toBatch, writerHandle, jniWrapper)
    refreshCache()
    writtenRows
  }

  private lazy val result: Seq[ColumnarBatch] = Seq()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    sparkContext.parallelize(result, 1)
//    val localSchema = schema
//    val timezoneId = SQLConf.get.sessionLocalTimeZone
//    val batchWrite = write.toBatch
//    val (writer, jniWrapper) = getJniWrapper(localSchema, timezoneId)
//    val rdd = child.executeColumnar()
//    val messages = ListBuffer[WriterCommitMessage]()
//
//    val newRdd = rdd.mapPartitions { batches => batches.map { batch =>
//      val jsonMsg = writeBatch(writer, jniWrapper, batch)
//      val dataFiles = parseDataFile(jsonMsg)
//      val commit = IcebergWriteUtil.commitDataFiles(dataFiles.toArray)
//      batchWrite.onDataWriterCommit(commit)
//      messages.append(commit)
//      batch
//    }}
//    batchWrite.commit(messages.toArray)
//    newRdd
  }

  private def parseDataFile(json: String): Seq[DataFile] = {
    // TODO: add partition
    val builder = DataFiles.builder(null).withPath("")
      .withFormat(IcebergWriteUtil.getFileFormat(write))
      .withFileSizeInBytes(-1).withMetrics(new Metrics())
    Seq(builder.build())
  }

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

}

object IcebergAppendDataExec {
  def apply(original: AppendDataExec): IcebergAppendDataExec = {
    IcebergAppendDataExec(
      original.query, original.refreshCache, original.write
    )
  }

  private def toStructType(schema: Seq[Attribute]): StructType = {
    StructType(schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }
}



case class PhysicalWriteInfoImpl(numPartitions: Int) extends PhysicalWriteInfo

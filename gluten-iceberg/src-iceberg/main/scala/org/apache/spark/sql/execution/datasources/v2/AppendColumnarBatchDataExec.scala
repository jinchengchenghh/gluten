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
package org.apache.spark.sql.execution.datasources.v2

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.c.ArrowSchema
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.connector.write.DataFileJson
import org.apache.gluten.execution.IcebergWriteJniWrapper
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.iceberg.spark.source.IcebergWriteUtil
import org.apache.iceberg.{DataFile, DataFiles, FileFormat, Metrics}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

case class DataWritingColumnarBatchSparkTaskResult(
                                                    numRows: Long,
                                                    writerCommitMessage: WriterCommitMessage)

trait WritingColumnarBatchSparkTask[W <: DataWriter[ColumnarBatch]] extends Logging with Serializable {

  protected def write(writer: W, row: ColumnarBatch): Unit

  def getJniWrapper(localSchema: StructType, format: Int,
                    directory: String): (Long, IcebergWriteJniWrapper) = {
    val schema = SparkArrowUtil.toArrowSchema(localSchema, SQLConf.get.sessionLocalTimeZone)
    val arrowAlloc = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(arrowAlloc)
    ArrowAbiUtil.exportSchema(arrowAlloc, schema, cSchema)
    val runtime = Runtimes.contextInstance(
      BackendsApiManager.getBackendName, "IcebergWrite#write")
    val jniWrapper = new IcebergWriteJniWrapper(runtime)
    // TODO: support the codec
    val writer = jniWrapper.init(cSchema.memoryAddress(), format, directory, "none")
    cSchema.close()
    (writer, jniWrapper)
  }

  def run(schema: StructType, format: Int,
          directory: String,
           context: TaskContext,
           iter: Iterator[ColumnarBatch],
           customMetrics: Map[String, SQLMetric]): DataWritingColumnarBatchSparkTaskResult = {
    val(writerHandle, jniWrapper) = getJniWrapper(schema, format, directory)
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = ColumnarBatchDataWriter(writerHandle, jniWrapper, format).asInstanceOf[W]

    var count = 0
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
        val batch = iter.next()
        // Count is here.
        count += batch.numRows()
        write(dataWriter, batch)
      }

      CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
      logInfo(s"Writer for partition ${context.partitionId()} is committing.")

      val msg = dataWriter.commit()

      logInfo(s"Committed partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")

      DataWritingColumnarBatchSparkTaskResult(count, msg)

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Aborting commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
      dataWriter.abort()
      logError(s"Aborted commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
    }, finallyBlock = {
      dataWriter.close()
    })
  }
}

object DataWritingColumnarBatchSparkTask extends WritingColumnarBatchSparkTask[DataWriter[ColumnarBatch]] {

  override protected def write(writer: DataWriter[ColumnarBatch], batch: ColumnarBatch): Unit = {
      writer.write(batch)
  }
}

object StreamWriterCommitProgressUtil {
  def getStreamWriterCommitProgress(numOutputRows: Long): StreamWriterCommitProgress = {
    StreamWriterCommitProgress(numOutputRows)
  }
}

case class ColumnarBatchDataWriter(writer: Long, jniWrapper: IcebergWriteJniWrapper, format: Int) extends DataWriter[ColumnarBatch] with Logging {

  override def write(batch: ColumnarBatch): Unit = {
    val batchHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
    jniWrapper.write(writer, batchHandle)
  }

  override def commit: WriterCommitMessage = {
    val dataFiles = jniWrapper.commit().map(d => parseDataFile(d))
    IcebergWriteUtil.commitDataFiles(dataFiles)
  }

  override def abort(): Unit = {
    logInfo("Abort the ColumnarBatchDataWriter")
  }

  override def close(): Unit = {
    logDebug("Close the ColumnarBatchDataWriter")
  }

  private def parseDataFile(json: String): DataFile = {
    val mapper = new ObjectMapper()
    mapper.readValue(json, classOf[DataFileJson])
    // TODO: add partition
    val builder = DataFiles.builder(null).withPath("")
      .withFormat(getFileFormat)
      .withFileSizeInBytes(-1).withMetrics(new Metrics())
    builder.build()
  }
  
  private def getFileFormat: FileFormat = {
    format match {
      case 0 => FileFormat.ORC
      case 1 => FileFormat.PARQUET
      case _ => throw new UnsupportedOperationException()
    }
  }
}

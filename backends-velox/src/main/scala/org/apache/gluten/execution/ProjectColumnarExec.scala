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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.extension.{GlutenPlan, ValidationResult}
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.iterator.Iterators
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, MutableProjection, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.{OrderPreservingNodeShim, PartitioningPreservingNodeShim, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.vectorized.{MutableColumnarRow, WritableColumnVector}
import org.apache.spark.sql.utils.SparkSchemaUtil
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

case class ProjectColumnarExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode
  with PartitioningPreservingNodeShim
  with OrderPreservingNodeShim
  with GlutenPlan {

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def supportsColumnar: Boolean = true

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions {
      batches =>
        val res: Iterator[Iterator[ColumnarBatch]] = new Iterator[Iterator[ColumnarBatch]] {
          override def hasNext: Boolean = batches.hasNext

          override def next(): Iterator[ColumnarBatch] = {
            val batch = batches.next()
            if (batch.numRows == 0) {
              Iterator.empty
            } else {
              val proj = MutableProjection.create(projectList, child.output)
              val numRows = batch.numRows()
              val arrowBatch =
                ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance(), batch)

              val schema =
                SparkShimLoader.getSparkShims.structFromAttributes(child.output.map(_.toAttribute))
              val vectors: Array[WritableColumnVector] = ArrowWritableColumnVector
                .allocateColumns(numRows, schema)
                .map {
                  vector =>
                    vector.setValueCount(numRows)
                    vector.asInstanceOf[WritableColumnVector]
                }
              val targetRow = new MutableColumnarRow(vectors)
              for (i <- 0 until numRows) {
                targetRow.rowId = i
                proj.target(targetRow).apply(arrowBatch.getRow(i))
              }
              val targetBatch =
                new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), numRows)
              val veloxBatch = ColumnarBatches
                .ensureOffloaded(ArrowBufferAllocators.contextInstance(), targetBatch)
              Iterators
                .wrap(Iterator.single(veloxBatch))
                .recycleIterator({
                  arrowBatch.close()
                  targetBatch.close()
                })
                .create()

            }
          }
        }
        Iterators
          .wrap(res.flatten)
          .protectInvocationFlow() // Spark may call `hasNext()` again after a false output which
          // is not allowed by Gluten iterators. E.g. GroupedIterator#fetchNextGroupIterator
          .recyclePayload(_.close())
          .create()
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!GlutenConfig.getConf.enableProjectColumnarExec) {
      return ValidationResult.failed("Config disable this feature")
    }
    if (!(SparkSchemaUtil.checkSchema(schema) && SparkSchemaUtil.checkSchema(child.schema))) {
      return ValidationResult.failed("Input type or output type cannot convert to arrow")
    }
    ValidationResult.succeeded
  }
}

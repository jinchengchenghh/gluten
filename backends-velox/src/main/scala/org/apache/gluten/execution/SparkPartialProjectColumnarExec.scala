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
import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper}
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.exec.Runtimes
import org.apache.gluten.expression.{ExpressionConverter, ExpressionMappings}
import org.apache.gluten.extension.{GlutenPlan, ValidationResult}
import org.apache.gluten.memory.nmm.NativeMemoryManagers
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.Iterators

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, Nondeterministic, SortOrder, UnsafeProjection}
import org.apache.spark.sql.execution.{ExplainUtils, OrderPreservingNodeShim, PartitioningPreservingNodeShim, ProjectExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ListBuffer

case class SparkPartialProjectColumnarExec(original: ProjectExec, child: SparkPlan)
  extends GlutenPlan
  with PartitioningPreservingNodeShim
  with OrderPreservingNodeShim {

  private val supported: ListBuffer[NamedExpression] = ListBuffer()
  private val unSupported: ListBuffer[NamedExpression] = ListBuffer()
  private val unSupportedIndexInOriginal: ListBuffer[Int] = ListBuffer()
  private val unSupportedAttribute = ListBuffer[AttributeReference]()
  // May add the extra attribute from unsupported to ProjectTransformer
  private val extraSupported: ListBuffer[NamedExpression] = ListBuffer()
  private val expressionsMap = ExpressionMappings.expressionsMap
  private val debug = GlutenConfig.getConf.debug
  original.projectList.zipWithIndex.foreach(
    p => {
      if (isSupported(p._1)) {
        supported.append(p._1)
      } else {
        unSupported.append(p._1)
        unSupportedIndexInOriginal.append(p._2)
      }
    })
  unSupported.foreach(expr => getUnsupportedReference(expr, unSupportedAttribute))
  private val unSupportedIndexInSupported =
    getUnsupportedIndexInSupported(unSupportedAttribute, supported, extraSupported)
  val transformer = ProjectExecTransformer(supported ++ extraSupported, original.child)

  log.warn(this.verboseStringWithOperatorId())

  final override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  final override lazy val supportsColumnar: Boolean = true

  override protected def outputExpressions: Seq[NamedExpression] = original.projectList

  override def output: Seq[Attribute] = original.output

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  // TODO: wait to check in native, such as empty2null which has substrait name in Gluten but not
  // register in native, not urgent, user function always does not exist in expression map
  private def isSupported(expr: Expression): Boolean = {
    val (substraitName, _) = ExpressionConverter.getSubstraitName(expr, expressionsMap)
    substraitName != null && expr.children.forall(isSupported)
  }

  private def validateExpression(expr: Expression): Boolean = {
    !expr.isInstanceOf[Nondeterministic] && expr.children.forall(validateExpression)
  }

  private def getUnsupportedReference(
      expr: Expression,
      unSupportedChildOutput: ListBuffer[AttributeReference]): Unit = expr match {
    case attr: AttributeReference =>
      if (!unSupportedChildOutput.contains(attr)) {
        unSupportedChildOutput.append(attr)
      }
    case e: Expression => e.children.foreach(getUnsupportedReference(_, unSupportedChildOutput))
    case _ => throw new GlutenNotSupportException(s"Not support $expr, which should be supported")
  }

  private def getUnsupportedIndexInSupported(
      unSupportedChildOutput: ListBuffer[AttributeReference],
      supported: ListBuffer[NamedExpression],
      extraSupported: ListBuffer[NamedExpression]): Seq[Int] = {
    val supportedIndex = ListBuffer[Int]()
    unSupportedChildOutput.foreach(
      u => {
        val index = supported.indexWhere(s => s.exprId.equals(u.exprId))
        // Add new expr to ProjectExecTransformer
        if (index < 0) {
          supportedIndex.append(supported.size)
          extraSupported.append(u)
        } else {
          supportedIndex.append(index)
        }
      })
    supportedIndex
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!GlutenConfig.getConf.enableColumnarPartialProject) {
      return ValidationResult.notOk("Config disable this feature")
    }
    if (!original.projectList.forall(validateExpression(_))) {
      return ValidationResult.notOk("Contains expression not supported")
    }
    val validationResult = transformer.doValidate()
    if (!validationResult.isValid) {
      ValidationResult.notOk(s"Project fallback reason ${validationResult.reason.getOrElse("")}")
    } else if (original.output.isEmpty) {
      ValidationResult.notOk(
        s"Project fallback because output is ${validationResult.reason.getOrElse("")}")
    } else if (supported.isEmpty) {
      ValidationResult.notOk(s"All expressions in project list are not supported")
    } else {
      ValidationResult.ok
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val batchSize = conf.columnBatchSize
    child.executeColumnar().mapPartitions {
      batches =>
        val res: Iterator[Iterator[ColumnarBatch]] = new Iterator[Iterator[ColumnarBatch]] {
          override def hasNext: Boolean = batches.hasNext

          override def next(): Iterator[ColumnarBatch] = {
            val batch = batches.next()
            if (batch.numRows == 0 || output.isEmpty) {
              Iterator.empty
            } else {
              getUnsupportedBatchIterator(batch, batchSize).map {
                b =>
//                  print("batch 1" + ColumnarBatches.toString(b, 0, 20) + "\n")
//                  print("batch 2" + ColumnarBatches.toString(batch, 0, 20) + "\n")
                  val handle =
                    ColumnarBatches.composeWithReorder(
                      b,
                      unSupportedIndexInOriginal.toArray,
                      batch,
                      extraSupported.size)
                  b.close()
                  ColumnarBatches.create(Runtimes.contextInstance(), handle)
              }
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

  /**
   * add c2r and r2c for unsupported expression child data c2r get Iterator[InternalRow], then call
   * Spark project, then r2c
   *
   * @param batch
   *   the batch from Velox ProjectTransformer, only contains the child data
   * @param batchSize
   *   number of column batch rows
   * @return
   *   batch with unSupported expression and its relevant attribute reference in front of batch.
   *   batch columns is unSupportedAttribute + unSupported
   */
  private def getUnsupportedBatchIterator(
      batch: ColumnarBatch,
      batchSize: Int): Iterator[ColumnarBatch] = {
    val unSupportedAllColumn = unSupportedAttribute ++ unSupported
    val unSupportedAllColumnAttributes = unSupportedAllColumn.map(_.toAttribute)
    val proj = UnsafeProjection.create(unSupportedAllColumn, unSupportedAllColumnAttributes)
    val childData = selectColumnarBatch(batch, unSupportedIndexInSupported)
    val rows = VeloxColumnarToRowExec
      .toRowIterator(Iterator.single[ColumnarBatch](childData), unSupportedAttribute)
      .map(proj)
    val schema = SparkShimLoader.getSparkShims.structFromAttributes(unSupportedAllColumnAttributes)
    // TODO: Optimize to convert to columnar batch ignore some columns
    RowToVeloxColumnarExec.toColumnarBatchIterator(
      rows,
      schema,
      batchSize,
      ColumnarBatches.getRuntime(batch))
  }

  private def selectColumnarBatch(cb: ColumnarBatch, indexes: Seq[Int]): ColumnarBatch = {
    val handle = ColumnarBatchJniWrapper
      .create()
      .select(
        NativeMemoryManagers.contextInstance(this.getClass.getSimpleName).getNativeInstanceHandle,
        ColumnarBatches.getNativeHandle(cb),
        indexes.toArray)
    ColumnarBatches.create(ColumnarBatches.getRuntime(cb), handle)
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", original.projectList)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("SparkProjetc", unSupported)}
       |""".stripMargin
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): SparkPartialProjectColumnarExec =
    copy(child = newChild)

}

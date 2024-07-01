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
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper}
import org.apache.gluten.exec.Runtimes
import org.apache.gluten.extension.{GlutenPlan, ValidationResult}
import org.apache.gluten.extension.columnar.validator.Validator.Passed
import org.apache.gluten.extension.columnar.validator.Validators.FallbackComplexExpressions
import org.apache.gluten.memory.nmm.NativeMemoryManagers
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.Iterators

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression, Nondeterministic, ScalaUDF, UnsafeProjection}
import org.apache.spark.sql.execution.{ExplainUtils, ProjectExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, TimestampType, YearMonthIntervalType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ListBuffer

/**
 * Change the Project to ProjectExecTransformer + SparkPartialProjectColumnarExec e.g. sum(myudf(a)
 * + b + hash(c)), child is (a, b,c ) SparkPartialProjectColumnarExec (a, b, c, myudf(a)),
 * ProjectExecTransformer(myudf(a) + b + hash(c))
 *
 * @param original
 *   extract the ScalaUDF from original project list as Alias in UnsafeProjection and
 *   AttributeReference in SparkPartialProjectColumnarExec output
 * @param child
 *   child plan
 */
case class SparkPartialProjectColumnarExec(original: ProjectExec, child: SparkPlan)(
    replacedAliasUdf: ListBuffer[Alias])
  extends UnaryExecNode
  with GlutenPlan {

  private val debug = GlutenConfig.getConf.debug

  private val projectAttributes: ListBuffer[Attribute] = ListBuffer()
  private val projectIndexInChild: ListBuffer[Int] = ListBuffer()
  private var UDFAttrNotExists = false
  private var hasComplexDataType = false
  hasComplexDataType = replacedAliasUdf.forall(a => validateDataType(a.dataType))
  if (!hasComplexDataType) {
    getProjectIndexInChildOutput(replacedAliasUdf)
  }
  log.warn(this.verboseStringWithOperatorId())

  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genSparkPartialProjectMetric(sparkContext)

  override def output: Seq[Attribute] = child.output ++ replacedAliasUdf.map(_.toAttribute)

  final override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  final override protected def otherCopyArgs: Seq[AnyRef] = {
    replacedAliasUdf :: Nil
  }

  final override lazy val supportsColumnar: Boolean = true

  private def validateExpression(expr: Expression): Boolean = {
    !expr.isInstanceOf[Nondeterministic] && expr.children.forall(validateExpression)
  }

  private def validateDataType(dataType: DataType): Boolean = {
    dataType match {
      case _: BooleanType => true
      case _: ByteType => true
      case _: ShortType => true
      case _: IntegerType => true
      case _: LongType => true
      case _: FloatType => true
      case _: DoubleType => true
      case _: StringType => true
      case _: TimestampType => true
      case _: DateType => true
      case _: BinaryType => true
      case _: DecimalType => true
      case YearMonthIntervalType.DEFAULT => true
      case _: NullType => true
      case _ => false
    }
  }

  private def getProjectIndexInChildOutput(exprs: Seq[Expression]): Unit = {
    exprs.foreach {
      case a: AttributeReference =>
        val index = child.output.indexWhere(s => s.exprId.equals(a.exprId))
        // Some child operator as HashAggregateTransformer will not have udf child column
        if (index < 0) {
          UDFAttrNotExists = true
          log.debug(s"Expression $a should exist in child output ${child.output}")
          return
        } else if (!validateDataType(a.dataType)) {
          hasComplexDataType = true
          log.debug(s"Expression $a contains unsupported data type ${a.dataType}")
        } else if (!projectIndexInChild.contains(index)) {
          projectAttributes.append(a.toAttribute)
          projectIndexInChild.append(index)
        }
      case a: Alias if a.name.startsWith(ProjectColumnarExec.projectPrefix) =>
        getProjectIndexInChildOutput(a.child.children)
      case p => getProjectIndexInChildOutput(p.children)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!GlutenConfig.getConf.enableColumnarPartialProject) {
      return ValidationResult.notOk("Config disable this feature")
    }
    if (UDFAttrNotExists) {
      return ValidationResult.notOk("Attribute in the UDF does not exists in its child")
    }
    if (hasComplexDataType) {
      return ValidationResult.notOk("Attribute in the UDF contains unsupported type")
    }
    if (!original.projectList.forall(validateExpression(_))) {
      return ValidationResult.notOk("Contains expression not supported")
    }
    if (original.output.isEmpty) {
      ValidationResult.notOk("Project fallback because output is empty")
    } else if (replacedAliasUdf.isEmpty) {
      ValidationResult.notOk("No Scala UDF")
    } else if (isComplexExpression()) {
      ValidationResult.notOk("Fallback by complex expression")
    } else {
      ValidationResult.ok
    }
  }

  private def isComplexExpression(): Boolean = {
    new FallbackComplexExpressions(GlutenConfig.getConf.fallbackExpressionsThreshold)
      .validate(original) match {
      case Passed => false
      case _ => true
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val batchSize = conf.columnBatchSize
    val totalTime = longMetric("time")
    child.executeColumnar().mapPartitions {
      batches =>
        val res: Iterator[Iterator[ColumnarBatch]] = new Iterator[Iterator[ColumnarBatch]] {
          override def hasNext: Boolean = batches.hasNext

          override def next(): Iterator[ColumnarBatch] = {
            val batch = batches.next()
            if (batch.numRows == 0 || output.isEmpty) {
              Iterator.empty
            } else {
              val start = System.currentTimeMillis()
              val batchIterator = getProjectedBatch(batch, batchSize).map {
                b =>
//                  print("batch 1" + ColumnarBatches.toString(batch, 0, 20) + "\n")
//                  print("batch 2" + ColumnarBatches.toString(b, 0, 20) + "\n")
                  val compositeBatch = if (b.numCols() != 0) {
                    val handle = ColumnarBatches.compose(batch, b)
                    b.close()
                    val composite = ColumnarBatches.create(Runtimes.contextInstance(), handle)
                    composite
                  } else {
                    b.close()
                    ColumnarBatches.retain(batch)
                    batch
                  }
                  if (debug && compositeBatch.numCols() != output.length) {
                    throw new IllegalStateException(
                      s"Composite batch column number is ${compositeBatch.numCols()}, " +
                        s"output size is ${output.length}, " +
                        s"original batch column number is ${batch.numCols()}")
                  }
                  compositeBatch
              }
              totalTime += System.currentTimeMillis() - start
              batchIterator
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
   */
  private def getProjectedBatch(
      childBatch: ColumnarBatch,
      batchSize: Int): Iterator[ColumnarBatch] = {
    // select part of child output and child data
    val proj = UnsafeProjection.create(replacedAliasUdf, projectAttributes)
    val childData = selectColumnarBatch(childBatch, projectIndexInChild)
    val rows = VeloxColumnarToRowExec
      .toRowIterator(Iterator.single[ColumnarBatch](childData), projectAttributes)
      .map(proj)
    val schema =
      SparkShimLoader.getSparkShims.structFromAttributes(replacedAliasUdf.map(_.toAttribute))
    // TODO: Optimize to convert to columnar batch ignore some columns
    RowToVeloxColumnarExec.toColumnarBatchIterator(
      rows,
      schema,
      batchSize,
      ColumnarBatches.getRuntime(childBatch))
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
       |${ExplainUtils.generateFieldString("Output", output)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("ScalaUDF", replacedAliasUdf)}
       |${ExplainUtils.generateFieldString("ProjectOutput", projectAttributes)}
       |${ExplainUtils.generateFieldString("ProjectInputIndex", projectIndexInChild)}
       |""".stripMargin
  }

  override def simpleString(maxFields: Int): String =
    super.simpleString(maxFields) + " PartialProject " + replacedAliasUdf

  override protected def withNewChildInternal(
      newChild: SparkPlan): SparkPartialProjectColumnarExec = {
    copy(child = newChild)(replacedAliasUdf)
  }
}

object ProjectColumnarExec {

  val projectPrefix = "_SparkPartialProject"

  private val replacedAliasUdf: ListBuffer[Alias] = ListBuffer()

  private def replaceExpressionUDF(expr: Expression): Expression = {
    if (expr == null) return null
    expr match {
      case u: ScalaUDF =>
        val replaceIndex = replacedAliasUdf.indexWhere(r => r.child.equals(u))
        if (replaceIndex == -1) {
          val replace = Alias(u, s"$projectPrefix${replacedAliasUdf.size}")()
          replacedAliasUdf.append(replace)
          replace.toAttribute
        } else {
          replacedAliasUdf(replaceIndex).toAttribute
        }
      case au @ Alias(_: ScalaUDF, _) =>
        val replaceIndex = replacedAliasUdf.indexWhere(r => r.exprId == au.exprId)
        if (replaceIndex == -1) {
          replacedAliasUdf.append(au)
          au.toAttribute
        } else {
          replacedAliasUdf(replaceIndex).toAttribute
        }
      // Alias(HiveSimpleUDF) not exists, only be Alias(ToPrettyString(HiveSimpleUDF)),
      // so don't process this condition
      case h if h.getClass.getSimpleName.equals("HiveSimpleUDF") =>
        val replaceIndex = replacedAliasUdf.indexWhere(r => r.child.equals(h))
        if (replaceIndex == -1) {
          val replace = Alias(h, s"$projectPrefix${replacedAliasUdf.size}")()
          replacedAliasUdf.append(replace)
          replace.toAttribute
        } else {
          replacedAliasUdf(replaceIndex).toAttribute
        }
      case p => p.withNewChildren(p.children.map(c => replaceExpressionUDF(c)))
    }
  }

  def create(original: ProjectExec): ProjectExecTransformer = {
    val newProjectList = original.projectList.map {
      p => replaceExpressionUDF(p).asInstanceOf[NamedExpression]
    }
    val partialProject = SparkPartialProjectColumnarExec(original, original.child)(replacedAliasUdf)
    ProjectExecTransformer(newProjectList, partialProject)
  }
}

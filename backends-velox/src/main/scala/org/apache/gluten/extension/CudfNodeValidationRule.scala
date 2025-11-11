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
package org.apache.gluten.extension

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.cudf.VeloxCudfPlanValidatorJniWrapper
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.extension.CudfNodeValidationRule.{createGPUColumnarExchange, setTagForWholeStageTransformer}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, GPUColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}

// Add the node name prefix 'Cudf' to GlutenPlan when can offload to cudf
case class CudfNodeValidationRule(glutenConf: GlutenConfig) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableColumnarCudf) {
      return plan
    }
    logInfo("apply the plan for CudfNodeValidationRule")
    val transformedPlan = plan.transformUp {
      case shuffle @ ColumnarShuffleExchangeExec(
            _,
            v @ VeloxResizeBatchesExec(w: WholeStageTransformer, _, _),
            _,
            _,
            _) =>
        setTagForWholeStageTransformer(w)
        log.info("Transforms to GpuResizeBufferColumnarBatchExec with VeloxResizeBatchesExec")
        GpuResizeBufferColumnarBatchExec(
          GPUColumnarShuffleExchangeExec(
            shuffle.outputPartitioning,
            shuffle.child,
            shuffle.shuffleOrigin,
            shuffle.projectOutputAttributes,
            shuffle.advisoryPartitionSize),
          10000
        )
      case shuffle @ ColumnarShuffleExchangeExec(_, w: WholeStageTransformer, _, _, _) =>
        setTagForWholeStageTransformer(w)
        logInfo(s"Transforms to GpuResizeBufferColumnarBatchExec")
        GpuResizeBufferColumnarBatchExec(
          GPUColumnarShuffleExchangeExec(
            shuffle.outputPartitioning,
            w,
            shuffle.shuffleOrigin,
            shuffle.projectOutputAttributes,
            shuffle.advisoryPartitionSize),
          10000
        )
      case transformer: WholeStageTransformer =>
        setTagForWholeStageTransformer(transformer)
        logInfo(s"Whole stage transformer transforms to ${transformer.toString()}")
        transformer
    }
    logInfo(s"After transformUp, the plan is ${transformedPlan.toString()}")
    val finalPlan = transformedPlan.transformUp {
      case a @ AQEShuffleReadExec(
            s @ ShuffleQueryStageExec(
              _,
              shuffle @ ColumnarShuffleExchangeExec(_, w: WholeStageTransformer, _, _, _),
              _),
            _) =>
        logInfo(
          "Transform to GPUColumnarShuffleExchangeExec from AQEShuffleReadExec" +
            "and ShuffleQueryStageExec")
        GpuResizeBufferColumnarBatchExec(
          a.copy(child = s.copy(plan = createGPUColumnarExchange(shuffle))),
          10000)
      // Since it's transformed in a bottom to up order, so we may first encounter
      // ShuffeQueryStageExec, which is transformed to VeloxResizeBatchesExec(ShuffeQueryStageExec),
      // then we see AQEShuffleReadExec
      case a @ AQEShuffleReadExec(
            GpuResizeBufferColumnarBatchExec(
              s @ ShuffleQueryStageExec(
                _,
                shuffle @ ColumnarShuffleExchangeExec(_, w: WholeStageTransformer, _, _, _),
                _),
              _),
            _) =>
        logInfo(
          "Transform to GpuResizeBufferColumnarBatchExec from AQEShuffleReadExec" +
            "and GpuResizeBufferColumnarBatchExec")
        GpuResizeBufferColumnarBatchExec(
          a.copy(child = s.copy(plan = createGPUColumnarExchange(shuffle))),
          10000)
      case s @ ShuffleQueryStageExec(
            _,
            shuffle @ ColumnarShuffleExchangeExec(_, w: WholeStageTransformer, _, _, _),
            _) =>
        logInfo("Transform to GpuResizeBufferColumnarBatchExec from ShuffleQueryStageExec")
        GpuResizeBufferColumnarBatchExec(s.copy(plan = createGPUColumnarExchange(shuffle)), 10000)
      case shuffle @ ColumnarShuffleExchangeExec(_, w: WholeStageTransformer, _, _, _) =>
        logInfo("Transform to GPUColumnarShuffleExchangeExec from ColumnarShuffleExchangeExec")
        createGPUColumnarExchange(shuffle)
      case s =>
        logInfo(s"Transform to GpuResizeBufferColumnarBatchExec from other node $s")
        s
      // The BroadcastExchange is not supported now
    }
    logInfo(s"After transformUp, the final plan is ${finalPlan.toString()}")
    finalPlan
  }
}

object CudfNodeValidationRule {
  def setTagForWholeStageTransformer(transformer: WholeStageTransformer): Unit = {
    if (!VeloxConfig.get.cudfEnableTableScan) {
      // Spark3.2 does not have exists
      val hasLeaf = transformer.find {
        case _: LeafTransformSupport => true
        case _ => false
      }.isDefined
      if (!hasLeaf && VeloxConfig.get.cudfEnableValidation) {
        if (
          VeloxCudfPlanValidatorJniWrapper.validate(
            transformer.substraitPlan.toProtobuf.toByteArray)
        ) {
          transformer.foreach {
            case _: LeafTransformSupport =>
            case t: TransformSupport =>
              t.setTagValue(CudfTag.CudfTag, true)
            case _ =>
          }
          transformer.setTagValue(CudfTag.CudfTag, true)
        }
      } else {
        transformer.setTagValue(CudfTag.CudfTag, !hasLeaf)
      }
    } else {
      transformer.setTagValue(CudfTag.CudfTag, true)
    }
  }

  def createGPUColumnarExchange(shuffle: ColumnarShuffleExchangeExec): SparkPlan = {
    val exec = GPUColumnarShuffleExchangeExec(
      shuffle.outputPartitioning,
      shuffle.child,
      shuffle.shuffleOrigin,
      shuffle.projectOutputAttributes,
      shuffle.advisoryPartitionSize)
    val res = exec.doValidate()
    if (!res.ok()) {
      throw new GlutenNotSupportException(res.reason())
    }
    exec
  }
}

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
import org.apache.gluten.execution.{CudfTag, LeafTransformSupport, TransformSupport, VeloxResizeBatchesExec, WholeStageTransformer}
import org.apache.gluten.extension.CudfNodeValidationRule.{createGPUColumnarExchange, setTagForWholeStageTransformer}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarInputAdapter, ColumnarShuffleExchangeExec, GPUColumnarShuffleExchangeExec, InputIteratorTransformer, SparkPlan}

// Add the node name prefix 'Cudf' to GlutenPlan when can offload to cudf
case class CudfNodeValidationRule(glutenConf: GlutenConfig) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableColumnarCudf) {
      return plan
    }
    logInfo("apply the plan for CudfNodeValidationRule")
    val transformedPlan = plan.transformUp {
      case transformer: WholeStageTransformer =>
        setTagForWholeStageTransformer(transformer)
        logInfo(s"Whole stage transformer transforms to ${plan.toString()}")
        transformer
    }
    logInfo(s"After transformUp, the plan is ${transformedPlan.toString()}")
    transformedPlan.transformUp {
      case shuffle @ ColumnarShuffleExchangeExec(
            _,
            VeloxResizeBatchesExec(w: WholeStageTransformer, _, _),
            _,
            _,
            _) if w.isCudf =>
        logInfo("Transform to GPUColumnarShuffleExchangeExec and remove VeloxResizeBatchesExec")
        createGPUColumnarExchange(shuffle, w)
      case shuffle @ ColumnarShuffleExchangeExec(_, w: WholeStageTransformer, _, _, _)
          if w.isCudf =>
        logInfo("Transform to GPUColumnarShuffleExchangeExec")
        createGPUColumnarExchange(shuffle, w)
      case i @ InputIteratorTransformer(ColumnarInputAdapter(shuffle: ColumnarShuffleExchangeExec))
          if i.isCudf =>
        logInfo("Transform to GPUColumnarShuffleExchangeExec after InputIteratorTransformer")
        createGPUColumnarExchange(shuffle, shuffle.child)
      // The BroadcastExchange is not supported now
    }
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

  def createGPUColumnarExchange(
      shuffle: ColumnarShuffleExchangeExec,
      child: SparkPlan): SparkPlan = {
    val exec = GPUColumnarShuffleExchangeExec(
      shuffle.outputPartitioning,
      child,
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

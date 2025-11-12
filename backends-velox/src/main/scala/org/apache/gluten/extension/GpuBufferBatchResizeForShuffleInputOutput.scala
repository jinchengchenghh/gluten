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

import org.apache.gluten.config.{GlutenConfig, HashShuffleWriterType, VeloxConfig}
import org.apache.gluten.execution.{GpuResizeBufferColumnarBatchExec, VeloxResizeBatchesExec}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

/**
 * Try to append [[VeloxResizeBatchesExec]] for shuffle input and output to make the batch sizes in
 * good shape.
 */
case class GpuBufferBatchResizeForShuffleInputOutput(glutenConfig: GlutenConfig)
  extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConfig.enableColumnarCudf) {
      return plan
    }
    val range = VeloxConfig.get.veloxResizeBatchesShuffleInputOutputRange
    val batchSize = 10000
    logInfo("try to apply GpuBufferBatchResizeForShuffleInputOutput")
    plan.transformUp {
      case shuffle: ColumnarShuffleExchangeExec
          if shuffle.shuffleWriterType == HashShuffleWriterType &&
            VeloxConfig.get.veloxResizeBatchesShuffleInput =>
        val appendBatches =
          VeloxResizeBatchesExec(shuffle.child, range.min, range.max)
        shuffle.withNewChildren(Seq(appendBatches))
      case a @ AQEShuffleReadExec(ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExec, _), _) =>
        GpuResizeBufferColumnarBatchExec(a, batchSize)
      case a @ AQEShuffleReadExec(
            ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: ColumnarShuffleExchangeExec), _),
            _) =>
        logInfo("Apply to AQEShuffleReadExec with ReusedExchangeExec")
        GpuResizeBufferColumnarBatchExec(a, batchSize)
      // Since it's transformed in a bottom to up order, so we may first encounter
      // ShuffeQueryStageExec, which is transformed to VeloxResizeBatchesExec(ShuffeQueryStageExec),
      // then we see AQEShuffleReadExec
      case a @ AQEShuffleReadExec(
            GpuResizeBufferColumnarBatchExec(
              s @ ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExec, _),
              _),
            _) =>
        GpuResizeBufferColumnarBatchExec(a.copy(child = s), 10000)
      case a @ AQEShuffleReadExec(
            GpuResizeBufferColumnarBatchExec(
              s @ ShuffleQueryStageExec(
                _,
                ReusedExchangeExec(_, _: ColumnarShuffleExchangeExec),
                _),
              _),
            _) =>
        GpuResizeBufferColumnarBatchExec(a.copy(child = s), 10000)
      case s @ ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExec, _) =>
        GpuResizeBufferColumnarBatchExec(s, 10000)
      case s @ ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: ColumnarShuffleExchangeExec), _) =>
        GpuResizeBufferColumnarBatchExec(s, 10000)
      case a: AQEShuffleReadExec =>
        logInfo(s"got another AQEShuffleReadExec ${a}")
        a
    }
  }
}

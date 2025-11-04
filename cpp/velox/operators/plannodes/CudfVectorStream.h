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

#pragma once

#include "CudfVectorStream.h"
#include "velox/experimental/cudf/exec/NvtxHelper.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

namespace gluten {

class CudfVectorStream : public RowVectorStream {
 public:
  CudfVectorStream(
      facebook::velox::exec::DriverCtx* driverCtx,
      facebook::velox::memory::MemoryPool* pool,
      ResultIterator* iterator,
      const facebook::velox::RowTypePtr& outputType)
      : RowVectorStream(driverCtx, pool, iterator, outputType) {}

  // Convert arrow batch to row vector and use new output columns
  facebook::velox::RowVectorPtr next() override {
    auto cb = nextInternal();
    if (cb == nullptr) {
      return nullptr;
    }
    auto vb = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
    VELOX_CHECK_NOT_NULL(vb);
    auto vp = vb->getRowVector();
    VELOX_DCHECK(vp != nullptr);
    auto cudfVector = std::dynamic_pointer_cast<facebook::velox::cudf_velox::CudfVector>(vp);
    VELOX_CHECK_NOT_NULL(cudfVector);
    return std::make_shared<facebook::velox::cudf_velox::CudfVector>(
        vp->pool(), outputType_, vp->size(), cudfVector->release(), vp->stream());
  }
};

class CudfValueStreamNode final : public facebook::velox::core::PlanNode {
 public:
  CudfValueStreamNode(
      const facebook::velox::core::PlanNodeId& id,
      const facebook::velox::RowTypePtr& outputType,
      std::shared_ptr<ResultIterator> iterator)
      : facebook::velox::core::PlanNode(id), outputType_(outputType), iterator_(std::move(iterator)) {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return kEmptySources_;
  };

  ResultIterator* iterator() const {
    return iterator_.get();
  }

  std::string_view name() const override {
    return "CudfValueStream";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("CudfValueStream plan node is not serializable");
  }

 private:
  void addDetails(std::stringstream& stream) const override{};

  const facebook::velox::RowTypePtr outputType_;
  std::shared_ptr<ResultIterator> iterator_;
  const std::vector<facebook::velox::core::PlanNodePtr> kEmptySources_;
};

class CudfValueStream : public ValueStream
// Extends NvtxHelper to identify it as GPU node, so not add CudfFormVelox operator.
#ifdef GLUTEN_ENABLE_GPU
    ,
                    public facebook::velox::cudf_velox::NvtxHelper
#endif
{
 public:
  CudfValueStream(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      std::shared_ptr<const ValueStreamNode> valueStreamNode)
      : ValueStream(operatorId, driverCtx, valueStreamNode)
#ifdef GLUTEN_ENABLE_GPU
        ,
        facebook::velox::cudf_velox::NvtxHelper(
            nvtx3::rgb{160, 82, 45}, // Sienna
            operatorId,
            fmt::format("[{}]", valueStreamNode->id()))
#endif
  {
    ResultIterator* itr = valueStreamNode->iterator();
    rvStream_ = std::make_unique<CudfVectorStream>(driverCtx, pool(), itr, outputType_);
  }
};

class CudfVectorStreamOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override {
    if (auto valueStreamNode = std::dynamic_pointer_cast<const CudfValueStreamNode>(node)) {
      return std::make_unique<CudfValueStream>(id, ctx, valueStreamNode);
    }
    return nullptr;
  }
};
} // namespace gluten

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

#include "shuffle/GpuShuffleReader.h"

#include <arrow/array/array_binary.h>
#include <arrow/io/buffered.h>

#include "memory/VeloxColumnarBatch.h"
#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "utils/Common.h"
#include "utils/Macros.h"
#include "utils/Timer.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"
#include "velox/vector/FlatVector.h"

#include <algorithm>

#include "cudf/GpuLock.h"

#include <cuda_runtime.h>
#include <cudf/column/column.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_buffer.hpp>

using namespace facebook::velox;

namespace gluten {
namespace {

arrow::Result<BlockType> readBlockType(arrow::io::InputStream* inputStream) {
  BlockType type;
  ARROW_ASSIGN_OR_RAISE(auto bytes, inputStream->Read(sizeof(BlockType), &type));
  if (bytes == 0) {
    // Reach EOS.
    return BlockType::kEndOfStream;
  }
  return type;
}

struct DispatchColumn {
  rmm::cuda_stream_view stream;
  rmm::device_async_resource_ref mr;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  int32_t bufferIdx;
  // numRows.
  const int32_t numRows;

  std::unique_ptr<rmm::device_buffer> getMaskBuffer(const std::shared_ptr<arrow::Buffer>& buffer) {
    if (buffer == nullptr || buffer->size() == 0) {
      return std::make_unique<rmm::device_buffer>(0, stream, mr);
    }

    auto const allocationSize = cudf::bitmask_allocation_size_bytes(static_cast<cudf::size_type>(buffer->size()));
    auto mask = std::make_unique<rmm::device_buffer>(allocationSize, stream, mr);
    CUDF_CUDA_TRY(
        cudaMemcpyAsync(mask->data(), buffer->data(), allocationSize, cudaMemcpyHostToDevice, stream.value()));
    return mask;
  }

  // For timestamp, it is cudf cudf::type_id::TIMESTAMP_NANOSECONDS, Velox uses int128_t while cudf uses int64_t to represent it.
  template <TypeKind Kind, typename T = typename TypeTraits<Kind>::NativeType>
  std::unique_ptr<cudf::column> readFlatColumn(cudf::type_id typeId) {
    // === Step 1: get CPU buffers ===
    auto nulls = buffers[bufferIdx++];
    auto values = buffers[bufferIdx++];

    // === Step 2: allocate GPU device buffers and copy ===
    rmm::device_buffer dataBuf(values->size(), stream);
    CUDF_CUDA_TRY(
        cudaMemcpyAsync(dataBuf.data(), values->data(), values->size(), cudaMemcpyHostToDevice, stream.value()));

    auto nullBuf = getMaskBuffer(nulls);

    // === Step 3: create cudf::column ===
    cudf::data_type cudfType{typeId};
    size_t nullCount = nulls == nullptr || nulls->size() == 0
        ? 0
        : cudf::null_count(reinterpret_cast<const cudf::bitmask_type*>(nulls->data()), 0, numRows, stream);
    return std::make_unique<cudf::column>(cudfType, numRows, std::move(dataBuf), std::move(*nullBuf), nullCount);
  }

  /// We can optimize it in shuffle writer side, returns the offset buffer instead of length buffer.
  /// Then we don't need to recover the offsetBuf by rawLengths, also change the merge strategic, update the merge buffer offset from last offset.
  std::unique_ptr<cudf::column> getOffsetsColumn(const StringLengthType* const rawLengths) {
    VELOX_CHECK_GT(numRows, 0);

    // --- 1. Build offsets on host ---
    std::vector<int32_t> offsets(numRows + 1);
    offsets[0] = 0;
    for (size_t i = 0; i < numRows; ++i) {
      offsets[i + 1] = offsets[i] + rawLengths[i];
    }

    // --- 2. Copy offsets to GPU ---
    rmm::device_buffer offsetBuf((numRows + 1) * sizeof(int32_t), stream, mr);
    CUDF_CUDA_TRY(cudaMemcpyAsync(
        offsetBuf.data(), offsets.data(), (numRows + 1) * sizeof(int32_t), cudaMemcpyHostToDevice, stream.value()));

    // --- 3. Empty null mask (no nulls in offset column) ---
    rmm::device_buffer nullBuf(0, stream, mr);

    // --- 4. Create cudf::column ---
    return std::make_unique<cudf::column>(
        cudf::data_type{cudf::type_id::INT32},
        static_cast<cudf::size_type>(numRows + 1),
        std::move(offsetBuf),
        std::move(nullBuf),
        0); // null_count = 0
  }

  std::unique_ptr<cudf::column> readFlatColumnStringView(cudf::type_id /*typeId*/) {
    auto nulls = buffers[bufferIdx++];
    auto lengths = buffers[bufferIdx++];
    auto valueBuffer = buffers[bufferIdx++];

    if (numRows == 0) {
      return make_empty_column(cudf::type_id::STRING);
    }

    auto mask = getMaskBuffer(nulls);

    // === Step 3: create cudf::column ===
    size_t nullCount = nulls == nullptr || nulls->size() == 0
        ? 0
        : cudf::null_count(reinterpret_cast<const cudf::bitmask_type*>(nulls->data()), 0, numRows, stream);

    const auto* rawLength = lengths->data_as<StringLengthType>();
    auto offsetColumn = getOffsetsColumn(rawLength);

    rmm::device_buffer chars(valueBuffer->size(), stream, mr);
    CUDF_CUDA_TRY(cudaMemcpyAsync(
        chars.data(), valueBuffer->data_as<uint8_t>(), chars.size(), cudaMemcpyDefault, stream.value()));
    return cudf::make_strings_column(
        numRows, std::move(offsetColumn), std::move(chars), nullCount, std::move(*mask.release()));
  }
};

template <>
std::unique_ptr<cudf::column> DispatchColumn::readFlatColumn<TypeKind::VARCHAR>(cudf::type_id typeId) {
  return readFlatColumnStringView(typeId);
}

template <>
std::unique_ptr<cudf::column> DispatchColumn::readFlatColumn<TypeKind::VARBINARY>(cudf::type_id typeId) {
  return readFlatColumnStringView(typeId);
}

std::shared_ptr<VeloxColumnarBatch> makeCudfTable(
    RowTypePtr type,
    int32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    memory::MemoryPool* pool,
    int64_t& deserializeTime) {
  ScopedTimer timer(&deserializeTime);
  std::vector<std::unique_ptr<cudf::column>> cudfColumns;
  cudfColumns.reserve(type->size());

  int32_t bufferIdx = 0;
  auto stream = cudf_velox::cudfGlobalStreamPool().get_stream();
  DispatchColumn dispatch{stream, cudf::get_current_device_resource_ref(), std::move(buffers), bufferIdx, numRows};
  for (auto colType : type->children()) {
    auto res = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
        dispatch.readFlatColumn, colType->kind(), cudf_velox::veloxToCudfTypeId(colType));
    cudfColumns.emplace_back(std::move(res));
  }
  auto cudfTable = std::make_unique<cudf::table>(std::move(cudfColumns));
  stream.synchronize();
  return std::make_shared<VeloxColumnarBatch>(
      std::make_shared<cudf_velox::CudfVector>(pool, type, numRows, std::move(cudfTable), stream), type->children().size());
}

} // namespace

GpuHashShuffleReaderDeserializer::GpuHashShuffleReaderDeserializer(
    const std::shared_ptr<StreamReader>& streamReader,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const facebook::velox::RowTypePtr& rowType,
    int64_t readerBufferSize,
    VeloxMemoryManager* memoryManager,
    int64_t& deserializeTime,
    int64_t& decompressTime)
    : streamReader_(streamReader),
      schema_(schema),
      codec_(codec),
      rowType_(rowType),
      readerBufferSize_(readerBufferSize),
      memoryManager_(memoryManager),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime) {
        std::cout << "Create shuffle reader GpuHashShuffleReaderDeserializer "<<std::endl;
      }

bool GpuHashShuffleReaderDeserializer::resolveNextBlockType() {
  GLUTEN_ASSIGN_OR_THROW(auto blockType, readBlockType(in_.get()));
  switch (blockType) {
    case BlockType::kEndOfStream:
      return false;
    case BlockType::kPlainPayload:
      return true;
    default:
      throw GlutenException(fmt::format("Unsupported block type: {}", static_cast<int32_t>(blockType)));
  }
  return true;
}

void GpuHashShuffleReaderDeserializer::loadNextStream() {
  if (reachedEos_) {
    return;
  }

  auto in = streamReader_->readNextStream(memoryManager_->defaultArrowMemoryPool());
  if (in == nullptr) {
    reachedEos_ = true;
    return;
  }

  GLUTEN_ASSIGN_OR_THROW(
      in_,
      arrow::io::BufferedInputStream::Create(
          readerBufferSize_, memoryManager_->defaultArrowMemoryPool(), std::move(in)));
}

std::shared_ptr<ColumnarBatch> GpuHashShuffleReaderDeserializer::next() {
  if (in_ == nullptr) {
    loadNextStream();

    if (reachedEos_) {
      return nullptr;
    }
  }

  // Resolve next block type
  while (!resolveNextBlockType()) {
    loadNextStream();

    if (reachedEos_) {
      return nullptr;
    }
  }

  uint32_t numRows = 0;

  GLUTEN_ASSIGN_OR_THROW(
      auto arrowBuffers,
      BlockPayload::deserialize(
          in_.get(),
          codec_,
          memoryManager_->defaultArrowMemoryPool(),
          numRows,
          deserializeTime_,
          decompressTime_));

  lockGpu();

  std::cout << "[DEBUG] makeCudfTable"<<std::endl;

  return makeCudfTable(
      rowType_,
      static_cast<int32_t>(numRows),
      std::move(arrowBuffers),
      memoryManager_->getLeafMemoryPool().get(),
      deserializeTime_);
}



} // namespace gluten
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
#include "GpuBufferColumnarBatch.h"
#include "compute/VeloxRuntime.h"
#include "utils/Timer.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/util/bitmap.h>

namespace gluten {

using namespace facebook;
using namespace facebook::velox;

std::shared_ptr<ArrowSchema> GpuBufferColumnarBatch::exportArrowSchema() {
  throw GlutenException("GpuBufferColumnarBatch does not support exportArrowSchema");
}

std::shared_ptr<ArrowArray> GpuBufferColumnarBatch::exportArrowArray() {
  throw GlutenException("GpuBufferColumnarBatch does not support exportArrowArray");
}

std::vector<char> GpuBufferColumnarBatch::toUnsafeRow(int32_t rowId) const {
  throw GlutenException("GpuBufferColumnarBatch does not support toUnsafeRow");
}

int64_t GpuBufferColumnarBatch::numBytes() {
  int64_t numBytes = 0;
  for (const auto& buffer : buffers_) {
    numBytes += buffer->size();
  }
  return numBytes;
}

// Optimize to release the previous buffer after merge it.
// Optimize to not allocate the returned batch buffers in the beginning.
std::shared_ptr<GpuBufferColumnarBatch> GpuBufferColumnarBatch::compose(
    arrow::MemoryPool* pool,
    const std::vector<std::shared_ptr<GpuBufferColumnarBatch>>& batches,
    int32_t numRows) {
  GLUTEN_CHECK(!batches.empty(), "No batches to compose");
  // Compute the returned GpuBufferColumnarBatch buffers.

  auto& type = batches[0]->getRowType();
  const auto bufferSize = batches[0]->buffers().size();
  std::vector<size_t> bufferSizes;
  bufferSizes.resize(bufferSize);
  // offsetBuffer length is more than original length.
  std::vector<bool> isOffsetBuffer;
  isOffsetBuffer.reserve(bufferSize);

  for (const auto& colType : type->children()) {
    if (colType->isFixedWidth()) {
      isOffsetBuffer.push_back(false);
      isOffsetBuffer.push_back(false);
    } else {
      isOffsetBuffer.push_back(false);
      isOffsetBuffer.push_back(true);
      isOffsetBuffer.push_back(false);
    }
  }
  VELOX_CHECK_EQ(isOffsetBuffer.size(), bufferSize);
  // This buffer may be more than the actual reauired buffer for null buffer.
  for (const auto& batch : batches) {
    for (auto i = 0; i < bufferSize; ++i) {
      // The null buffer may be null or length = 0.
      // Maybe optimize later, detect if the null buffer is all true. And set the return null buffer to 0.
      if (batch->bufferAt(i) == nullptr || batch->bufferAt(i)->size() == 0) {
        bufferSizes[i] += arrow::bit_util::BytesForBits(batch->numRows());
      }
      bufferSizes[i] += batch->buffers()[i]->size();
    }
  }

  // Add the first offset 0.
  for (auto i = 0; i < bufferSize; ++i) {
    if (bufferSizes[i] != 0 && isOffsetBuffer[i]) {
      bufferSizes[i] += sizeof(int32_t);
    }
  }

  std::vector<std::shared_ptr<arrow::Buffer>> returnBuffers;
  returnBuffers.reserve(bufferSize);
  for (auto size : bufferSizes) {
    std::shared_ptr<arrow::Buffer> buffer;
    // May optimize to reuse the first batch buffer.
    GLUTEN_ASSIGN_OR_THROW(buffer, arrow::AllocateResizableBuffer(size, pool));
    returnBuffers.emplace_back(buffer);
  }

  int32_t bufferIdx = 0;
  for (const auto& colType : type->children()) {
    size_t rowNumber = 0;
    // Also records the value buffer offset.
    size_t stringOffset = 0;
    for (auto i = 0; i < batches.size(); ++i) {
      const auto& batch = batches[i];
      if (batch->numRows() == 0) {
        continue;
      }
      // Combine the null buffer
      // The last byte may still have space to write when nullBitsRemainder != 0.
      auto* dst = returnBuffers[bufferIdx]->mutable_data();
      std::cout <<"[DEBUG] compose null buffer, rowNumber: "<< rowNumber <<", batch->numRows(): "<< batch->numRows() << std::endl;
      if (batch->bufferAt(bufferIdx) == nullptr) {
        std::cout << "[DEBUG] compose null buffer, set all bits to true" << std::endl;
        arrow::bit_util::SetBitsTo(dst, rowNumber, batch->numRows(), true);
      } else {
        std::cout << "[DEBUG] compose non null buffer, copy bitmap" << std::endl;
        arrow::internal::CopyBitmap(batch->bufferAt(bufferIdx)->data(), 0, batch->numRows(), dst, rowNumber);
      }
      std::cout <<"Set the null buffer succeeds"<< std::endl;

      if (colType->isFixedWidth()) {
        // The buffer is values.
        const auto bufferSize = batch->bufferAt(bufferIdx + 1)->size();
        std::cout << "[DEBUG] compose fixed width buffer, memcpy" << std::endl;
        VELOX_CHECK_LE(stringOffset + bufferSize, returnBuffers[bufferIdx + 1]->size());
        memcpy(
            returnBuffers[bufferIdx + 1]->mutable_data() + stringOffset,
            batch->bufferAt(bufferIdx + 1)->data(),
            bufferSize);
        stringOffset += bufferSize;
        std::cout << "[DEBUG] composed fixed width buffer, stringOffset: "<< stringOffset << std::endl;
      } else {
        std::cout << "[DEBUG] compose string buffer, memcpy" << std::endl;
        // String, lengths, values
        memcpy(
            returnBuffers[bufferIdx + 2]->mutable_data() + stringOffset,
            batch->bufferAt(bufferIdx + 2)->data(),
            batch->bufferAt(bufferIdx + 2)->size());
        std::cout << "[DEBUG] composed string buffer, memcpy lengths" << std::endl;
        const auto* lengths = reinterpret_cast<const int32_t*>(batch->bufferAt(bufferIdx + 1)->data());
        auto* offsetBuffer = reinterpret_cast<int32_t*>(returnBuffers[bufferIdx + 1]->mutable_data());
        for (auto j = 0; j < batch->numRows(); ++j) {
          offsetBuffer[rowNumber + j] = stringOffset;
          stringOffset += lengths[j];
        }
        offsetBuffer[rowNumber + batch->numRows()] = stringOffset;
      }
      rowNumber += batch->numRows();
      std::cout << "[DEBUG] rowNumber is " << rowNumber << std::endl;
    }
    if (colType->isFixedWidth()) {
      bufferIdx += 2;
    } else {
      bufferIdx += 3;
    }
    std::cout <<"Composed one column"<< std::endl;
  }

  std::cout <<"[DEBUG] generate the GpuBufferColumnarBatch"<< std::endl;

  return std::make_shared<GpuBufferColumnarBatch>(batches[0]->getRowType(), std::move(returnBuffers), numRows);
}

} // namespace gluten

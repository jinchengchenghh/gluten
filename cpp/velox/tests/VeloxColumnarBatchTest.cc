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

#include "memory/VeloxColumnarBatch.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace gluten {
class VeloxColumnarBatchTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(VeloxColumnarBatchTest, flattenTruncatedVector) {
  vector_size_t inputSize = 1'00;
  vector_size_t childSize = 1'000;
  auto mapVector = makeMapVector<int32_t, int64_t>(
      childSize, [](auto row) { return 1; }, [](auto row) { return row; }, [](auto row) { return row; });
  auto mapKeys = mapVector->mapKeys();
  auto mapValues = mapVector->mapValues();

  // First, make a row vector with the mapKeys and mapValues as children.
  // Make the row vector size less than the children size.
  auto input = std::make_shared<RowVector>(
      pool(),
      ROW({INTEGER(), BIGINT(), MAP(INTEGER(), BIGINT())}),
      nullptr,
      inputSize,
      std::vector<VectorPtr>{mapKeys, mapValues});

  auto batch = std::make_shared<VeloxColumnarBatch>(input);
  ASSERT_NO_THROW(batch->getFlattenedRowVector());

  // Allocate a dummy indices and wrap the original mapVector with it as a dictionary, to force it get decoded in
  // flattenVector.
  auto indices = allocateIndices(childSize, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < childSize; i++) {
    rawIndices[i] = i;
  }
  auto encodedMapVector = BaseVector::wrapInDictionary(nullptr, indices, inputSize, mapVector);
  auto inputOfMap = makeRowVector({encodedMapVector});
  auto batchOfMap = std::make_shared<VeloxColumnarBatch>(inputOfMap);
  ASSERT_NO_THROW(batchOfMap->getFlattenedRowVector());
}

TEST_F(VeloxColumnarBatchTest, fromCompositeReorderColumnarBatch) {
  std::vector<VectorPtr> children1 = {
      makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt, 4}),
      makeNullableFlatVector<int8_t>({1, -1, std::nullopt, std::nullopt, -2}),
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt})};

  std::vector<VectorPtr> children2 = {
      makeNullableFlatVector<int8_t>({10, 20, 30, std::nullopt, 40}),
      makeNullableFlatVector<int8_t>({12, -1, std::nullopt, std::nullopt, -2}),
      makeNullableFlatVector<int32_t>({13, 2, 3, 4, std::nullopt})};

  auto batch1 = std::make_shared<VeloxColumnarBatch>(makeRowVector(children1));
  auto batch2 = std::make_shared<VeloxColumnarBatch>(makeRowVector(children2));
  std::vector<int32_t> cb1Indices = {1, 4};
  auto reorderBatch = CompositeReorderColumnarBatch::create(batch1, cb1Indices, batch2);
  auto vector = VeloxColumnarBatch::from(pool(), reorderBatch)->getRowVector();
  std::cout << "vector content " << vector->toString(0, 10) << std::endl;
  std::vector<VectorPtr> childrenExpected = {
      makeNullableFlatVector<int8_t>({10, 20, 30, std::nullopt, 40}),
      makeNullableFlatVector<int8_t>({1, -1, std::nullopt, std::nullopt, -2}),
      makeNullableFlatVector<int8_t>({12, -1, std::nullopt, std::nullopt, -2}),
      makeNullableFlatVector<int32_t>({13, 2, 3, 4, std::nullopt}),
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt})};
  auto expectedVector = makeRowVector(childrenExpected);
  test::assertEqualVectors(vector, expectedVector);
}
} // namespace gluten

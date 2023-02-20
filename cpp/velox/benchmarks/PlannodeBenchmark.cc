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

#include <benchmark/benchmark.h>
#include <gflags/gflags.h>
#include <velox/exec/PlanNodeStats.h>
#include <velox/exec/Task.h>
#include <velox/exec/tests/utils/PlanBuilder.h>

#include <chrono>

#include "BenchmarkUtils.h"
#include "compute/VeloxBackend.h"
#include "utils/exception.h"

#include <filesystem>
#include <thread>

// This benchmark use parquet file as input, and velox PlanNode to query
// You can run the sql and get the velox PlanNode in console, recover to PlanNode code

auto BM_Generic = [](::benchmark::State& state, GetInputFunc* getInputIterator) {
  if (FLAGS_cpu != -1) {
    setCpu(FLAGS_cpu);
  } else {
    setCpu(state.thread_index());
  }

  auto planNodeIdGenerator = std::make_shared<facebook::velox::core::PlanNodeIdGenerator>();
  facebook::velox::core::PlanNodeId customerScanNodeId;
  facebook::velox::core::PlanNodeId ordersScanNodeId;

  std::vector<std::string> customerColumns = {"c_custkey", "c_nationkey"};
  std::vector<std::string> ordersColumns = {"o_orderkey", "o_custkey"};

  static constexpr const char* kCustomer = "customer";
  static constexpr const char* kOrders = "orders";
  auto customerSelectedRowType =
      facebook::velox::ROW({{"c_custkey", facebook::velox::BIGINT()}, {"c_nationkey", facebook::velox::BIGINT()}});
  auto ordersSelectedRowType =
      facebook::velox::ROW({{"o_orderkey", facebook::velox::BIGINT()}, {"o_custkey", facebook::velox::BIGINT()}});
  auto orders = facebook::velox::exec::test::PlanBuilder(planNodeIdGenerator)
                    .tableScan(kOrders, ordersSelectedRowType, {}, {})
                    .capturePlanNodeId(ordersScanNodeId)
                    .planNode();

  auto customer = facebook::velox::exec::test::PlanBuilder(planNodeIdGenerator)
                      .tableScan(kCustomer, customerSelectedRowType, {})
                      .capturePlanNodeId(customerScanNodeId)
                      .planNode();

  auto customerJoinOrder = facebook::velox::exec::test::PlanBuilder(planNodeIdGenerator)
                               .tableScan(kCustomer, customerSelectedRowType, {})
                               .capturePlanNodeId(customerScanNodeId)
                               .hashJoin({"c_custkey"}, {"o_custkey"}, orders, "", {"o_orderkey", "c_nationkey"})
                               .project({"o_orderkey as orderkey", "c_nationkey"})
                               .planNode();

  std::unordered_map<facebook::velox::core::PlanNodeId, std::vector<std::string>> dataFiles;
  {
    std::vector<std::string> cFiles;
    std::vector<std::string> oFiles;
    std::string customerPath = "/mnt/DP_disk2/tpch/test_result/joinbigsome/customer";
    for (const auto& entry : std::filesystem::directory_iterator(customerPath)) {
      std::cout << entry.path().string() << std::endl;
      cFiles.emplace_back(entry.path().string());
    }
    std::string oPath = "/mnt/DP_disk2/tpch/test_result/joinbigsome/orders";
    for (const auto& entry : std::filesystem::directory_iterator(oPath)) {
      std::cout << entry.path().string() << std::endl;
      oFiles.emplace_back(entry.path().string());
    }
    // cFiles.emplace_back("/mnt/DP_disk2/tpch/test_result/joinbig/customer/part-00000-f84b19fc-c75a-4a12-834e-08a55332bd23-c000.snappy.parquet");
    // oFiles.emplace_back("/mnt/DP_disk2/tpch/test_result/joinbig/orders/part-00000-e9b344ec-0523-4ce4-84b4-daedb8ade05e-c000.snappy.parquet");
    dataFiles[ordersScanNodeId] = oFiles;
    dataFiles[customerScanNodeId] = cFiles;
  }
  auto startTime = std::chrono::steady_clock::now();
  for (auto _ : state) {
    executeQuery(customerJoinOrder, dataFiles);
  }

  auto endTime = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();

  state.counters["elapsed_time"] =
      benchmark::Counter(duration, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
};

int main(int argc, char** argv) {
  InitVeloxBackend();
  ::benchmark::Initialize(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

#define GENERIC_BENCHMARK(NAME, FUNC)                                                                          \
  do {                                                                                                         \
    auto* bm = ::benchmark::RegisterBenchmark(NAME, BM_Generic, FUNC)->MeasureProcessCPUTime()->UseRealTime(); \
    if (FLAGS_threads > 0) {                                                                                   \
      bm->Threads(FLAGS_threads);                                                                              \
    } else {                                                                                                   \
      bm->ThreadRange(1, std::thread::hardware_concurrency());                                                 \
    }                                                                                                          \
    if (FLAGS_iterations > 0) {                                                                                \
      bm->Iterations(FLAGS_iterations);                                                                        \
    }                                                                                                          \
  } while (0)

  GENERIC_BENCHMARK("InputFromBatchVector", getInputFromBatchVector);
  // GENERIC_BENCHMARK("InputFromBatchStream", getInputFromBatchStream);

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}

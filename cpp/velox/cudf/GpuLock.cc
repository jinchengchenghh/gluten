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

#include "GpuLock.h"
#include <mutex>
#include <condition_variable>
#include <optional>
#include <stdexcept>
#include <glog/logging.h>
#include <unordered_map>

#include <iostream>

namespace gluten {

namespace {
struct GpuLockState {
  std::mutex gGpuMutex;
  std::condition_variable gGpuCv;
  int maxConcurrent;       // allow 2 concurrent tasks
  int currentHolders = 0;            // how many threads hold the lock

  // track per-thread recursion counts
  std::unordered_map<std::thread::id, int> recursionCount;
};

GpuLockState& getGpuLockState() {
  static GpuLockState gGpuLockState;
  return gGpuLockState;
}
}

void setMaxConcurrent(int maxConcurrent) {
    std::cout <<"set the max concurrent to " << maxConcurrent << std::endl;
    getGpuLockState().maxConcurrent = maxConcurrent;
}

void lockGpu() {
    auto& state = getGpuLockState();
    std::thread::id tid = std::this_thread::get_id();

    std::unique_lock<std::mutex> lock(state.gGpuMutex);

    // Reentrant lock for same thread
    if (state.recursionCount.count(tid) > 0) {
        state.recursionCount[tid]++;
        return;
    }

    // Wait until a slot becomes available
    state.gGpuCv.wait(lock, [&] {
        return state.currentHolders < state.maxConcurrent;
    });

    // Acquire
    state.currentHolders++;
    state.recursionCount[tid] = 1;
}


void unlockGpu() {
    auto& state = getGpuLockState();
    std::thread::id tid = std::this_thread::get_id();

    std::unique_lock<std::mutex> lock(state.gGpuMutex);

    // Not locked by this thread
    if (state.recursionCount.count(tid) == 0) {
        LOG(INFO) <<"unlockGpu() called by non-owner thread!"<< std::endl;
        return;
    }

    // Handle recursion release
    state.recursionCount[tid]--;
    if (state.recursionCount[tid] > 0) {
        return; // still owns recursively
    }

    // Fully release
    state.recursionCount.erase(tid);
    state.currentHolders--;

    lock.unlock();
    state.gGpuCv.notify_one();
}


} // namespace gluten

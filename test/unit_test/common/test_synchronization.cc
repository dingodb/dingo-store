// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <pthread.h>
#include <sched.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "common/synchronization.h"
#include "fmt/core.h"

class BthreadSemaphoreTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(BthreadSemaphoreTest, Semaphore01) {
  GTEST_SKIP() << "skip...";

  const int producer_count = 6;
  const int consumer_count = 10;

  dingodb::BthreadSemaphore sem;

  auto producer_func = [&sem]() {
    for (int64_t i = 0; i < 50000000; ++i) {
      sem.Release(1);
      if ((i + 1) % 100000 == 0) {
        bthread_usleep(1000L);
      }
    }

    std::cout << "produce finish." << std::endl;
  };

  std::atomic<int64_t> count = 0;
  auto consumer_func = [&sem, &count]() {
    for (;;) {
      sem.Acquire();
      count.fetch_add(1);
    }
  };

  std::vector<dingodb::Bthread> producer_bthreads;
  producer_bthreads.reserve(producer_count);
  for (int i = 0; i < producer_count; ++i) {
    producer_bthreads.push_back(dingodb::Bthread(producer_func));
  }

  std::vector<dingodb::Bthread> consumer_bthreads;
  consumer_bthreads.reserve(consumer_count);
  for (int i = 0; i < consumer_count; ++i) {
    consumer_bthreads.push_back(dingodb::Bthread(consumer_func));
  }

  for (;;) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::cout << "count: " << count.load() << " value: " << sem.GetValue() << std::endl;
  }

  for (auto& thread : producer_bthreads) {
    thread.Join();
  }

  for (auto& thread : consumer_bthreads) {
    thread.Join();
  }
}
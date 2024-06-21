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

#include "common/runnable.h"
#include "fmt/core.h"

class TempTestTask : public dingodb::TaskRunnable {
 public:
  TempTestTask() = default;
  ~TempTestTask() override = default;

  std::string Type() override { return "TempTestTask"; }

  void Run() override {
    int64_t mulple = 1;
    for (int i = 1; i < 100000; ++i) {
      mulple *= i;
    }
  }
};

class SimpleWorkerSetTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(SimpleWorkerSetTest, Performance01) {
  GTEST_SKIP() << "Performence test, skip...";

  bool use_pthead = false;
  bool use_prior = false;
  auto worker_set = dingodb::SimpleWorkerSet::New("unit_test", 128, 0, use_pthead, use_prior);
  ASSERT_TRUE(worker_set->Init());

  std::this_thread::sleep_for(std::chrono::seconds(1));

  int64_t times = 20 * 1000 * 1000;
  for (int64_t i = 0; i < times; ++i) {
    auto task = std::make_shared<TempTestTask>();
    worker_set->Execute(task);
  }

  std::cout << "finish..." << std::endl;
  worker_set->Destroy();
  std::cout << "exit..." << std::endl;
}
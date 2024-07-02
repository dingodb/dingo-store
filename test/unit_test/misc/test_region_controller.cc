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

#include <iostream>
#include <memory>

#include "store/region_controller.h"

class RegionControlExecutorTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

class TestTask : public dingodb::TaskRunnable {
 public:
  TestTask() = default;
  ~TestTask() override = default;

  std::string Type() override { return ""; }
  void Run() override {
    bthread_usleep(1 * 1000 * 1000);
    LOG(INFO) << "TestTask run...";
  }
};

TEST_F(RegionControlExecutorTest, Stop) {
  auto region_executor = std::make_shared<dingodb::RegionControlExecutor>(1000);

  region_executor->Init();

  LOG(INFO) << "here 0001";

  region_executor->Execute(std::make_shared<TestTask>());

  LOG(INFO) << "here 0002";

  region_executor->Execute(std::make_shared<TestTask>());
  LOG(INFO) << "here 0003";

  region_executor->Stop();

  LOG(INFO) << "here 0004";
}
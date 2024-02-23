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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>

#include "gtest/gtest.h"
#include "sdk/utils/thread_pool_actuator.h"

namespace dingodb {
namespace sdk {

static const int kThreadNum = 8;

class ThreadPoolActuatorTest : public testing::Test {
 public:
  void SetUp() override { actuator = std::make_unique<ThreadPoolActuator>(); }

  void TearDown() override { actuator.reset(); }

  std::unique_ptr<ThreadPoolActuator> actuator;
};

TEST_F(ThreadPoolActuatorTest, Start) {
  bool res = actuator->Start(kThreadNum);
  EXPECT_TRUE(res);
}

TEST_F(ThreadPoolActuatorTest, Stop) {
  bool res = actuator->Start(kThreadNum);
  EXPECT_TRUE(res);
  res = actuator->Stop();
  EXPECT_TRUE(res);
  res = actuator->Stop();
  EXPECT_FALSE(res);
}

TEST_F(ThreadPoolActuatorTest, ThreadNum) {
  bool res = actuator->Start(kThreadNum);
  int num = actuator->ThreadNum();
  EXPECT_EQ(num, kThreadNum);
}

TEST_F(ThreadPoolActuatorTest, Execute) {
  bool res = actuator->Start(kThreadNum);
  int num = actuator->ThreadNum();
  EXPECT_EQ(num, kThreadNum);

  std::mutex mutex;
  std::condition_variable cond;
  std::atomic<int> count(2);

  actuator->Execute([&]() {
    EXPECT_EQ(count.fetch_sub(1), 2);
    cond.notify_all();
  });

  actuator->Execute([&]() {
    EXPECT_EQ(count.fetch_sub(1), 1);
    cond.notify_all();
  });

  {
    std::unique_lock<std::mutex> lg(mutex);
    while (count.load() != 0) {
      cond.wait_for(lg, std::chrono::milliseconds(1));
    }
  }

  EXPECT_EQ(count.load(), 0);
}

TEST_F(ThreadPoolActuatorTest, Schedule) {
  bool res = actuator->Start(kThreadNum);

  std::mutex mutex;
  std::condition_variable cond;
  std::atomic<int> count(2);

  actuator->Schedule(
      [&]() {
        EXPECT_EQ(count.fetch_sub(1), 2);
        cond.notify_all();
      },
      5);

  actuator->Schedule(
      [&]() {
        EXPECT_EQ(count.fetch_sub(1), 1);
        cond.notify_all();
      },
      10);

  int max_wait = 100;
  {
    std::unique_lock<std::mutex> lg(mutex);
    while (count.load() != 0) {
      std::cout << "wait 1 ms" << std::endl;
      cond.wait_for(lg, std::chrono::milliseconds(1));
    }
  }

  EXPECT_EQ(count.load(), 0);
}

}  // namespace sdk
}  // namespace dingodb
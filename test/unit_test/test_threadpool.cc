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

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "common/threadpool.h"

class ThreadPoolTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(ThreadPoolTest, Normal) {
  GTEST_SKIP() << "skip test...";
  dingodb::ThreadPool thread_pool("unit_test", 1);

  std::vector<dingodb::ThreadPool::TaskPtr> tasks;
  tasks.push_back(
      thread_pool.ExecuteTask([](void *) { std::this_thread::sleep_for(std::chrono::seconds(3)); }, nullptr));

  std::this_thread::sleep_for(std::chrono::seconds(1));

  tasks.push_back(thread_pool.ExecuteTask([](void *) { LOG(INFO) << "thread priority 1"; }, nullptr, 1));

  tasks.push_back(thread_pool.ExecuteTask([](void *) { LOG(INFO) << "thread priority 2 1"; }, nullptr, 2));
  tasks.push_back(thread_pool.ExecuteTask([](void *) { LOG(INFO) << "thread priority 2 3"; }, nullptr, 2));
  tasks.push_back(thread_pool.ExecuteTask([](void *) { LOG(INFO) << "thread priority 2 2"; }, nullptr, 2));

  tasks.push_back(thread_pool.ExecuteTask([](void *) { LOG(INFO) << "thread priority 3"; }, nullptr, 3));

  for (auto &task : tasks) {
    task->Join();
  }

  EXPECT_EQ(true, true);
}

static int GetThreadPolicy(pthread_attr_t &attr) {
  int policy;
  int rs = pthread_attr_getschedpolicy(&attr, &policy);
  assert(rs == 0);
  switch (policy) {
    case SCHED_FIFO:
      LOG(INFO) << "policy = SCHED_FIFO";
      break;
    case SCHED_RR:
      LOG(INFO) << "policy = SCHED_RR";
      break;
    case SCHED_OTHER:
      LOG(INFO) << "policy = SCHED_OTHER";
      break;
    default:
      LOG(INFO) << "policy = UNKNOWN";
      break;
  }
  return policy;
}

static void ShowThreadPriority(pthread_attr_t &, int policy) {
  int priority = sched_get_priority_max(policy);
  assert(priority != -1);
  LOG(INFO) << "max_priority = " << priority;
  priority = sched_get_priority_min(policy);
  assert(priority != -1);
  LOG(INFO) << "min_priority = " << priority;
}

static int GetThreadPriority(pthread_attr_t &attr) {
  struct sched_param param;
  int rs = pthread_attr_getschedparam(&attr, &param);
  assert(rs == 0);
  LOG(INFO) << "priority = " << param.__sched_priority;
  return param.__sched_priority;
}

static void SetThreadPolicy(pthread_attr_t &attr, int policy) {
  int rs = pthread_attr_setschedpolicy(&attr, policy);
  assert(rs == 0);
  GetThreadPolicy(attr);
}

TEST_F(ThreadPoolTest, Priority) {
  GTEST_SKIP() << "skip test...";

  pthread_attr_t attr;
  struct sched_param sched;

  int rs;
  rs = pthread_attr_init(&attr);

  assert(rs == 0);
  int policy = GetThreadPolicy(attr);
  LOG(INFO) << "Show current configuration of priority";
  ShowThreadPriority(attr, policy);
  LOG(INFO) << "Show SCHED_FIFO of priority";
  ShowThreadPriority(attr, SCHED_FIFO);
  LOG(INFO) << "Show SCHED_RR of priority";
  ShowThreadPriority(attr, SCHED_RR);
  LOG(INFO) << "Show priority of current thread";
  int priority = GetThreadPriority(attr);
  LOG(INFO) << "Set thread policy";
  LOG(INFO) << "Set SCHED_FIFO policy";
  SetThreadPolicy(attr, SCHED_FIFO);
  LOG(INFO) << "Set SCHED_RR policy";
  SetThreadPolicy(attr, SCHED_RR);
  LOG(INFO) << "Restore current policy";
  SetThreadPolicy(attr, policy);

  rs = pthread_attr_destroy(&attr);
}
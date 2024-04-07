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

#include "common/threadpool.h"
#include "fmt/core.h"
#include "gtest/gtest.h"

class ThreadPoolTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(ThreadPoolTest, ExecuteTask) {
  dingodb::ThreadPool thread_pool("unit_test", 1);

  int count = 0;

  auto task = thread_pool.ExecuteTask([&count](void *) { ++count; }, nullptr);
  ASSERT_NE(nullptr, task);

  task->Join();

  ASSERT_EQ(1, count);
}

TEST_F(ThreadPoolTest, ExecuteMultiTask) {
  dingodb::ThreadPool thread_pool("unit_test", 3);

  int task_count = 10;
  std::atomic<int> count = 0;

  std::vector<dingodb::ThreadPool::TaskPtr> tasks;
  for (int i = 0; i < task_count; ++i) {
    auto task = thread_pool.ExecuteTask([&count](void *) { count.fetch_add(1); }, nullptr);
    ASSERT_NE(nullptr, task);
    tasks.push_back(task);
  }

  for (auto &task : tasks) {
    task->Join();
  }

  ASSERT_EQ(10, count.load());
}

TEST_F(ThreadPoolTest, ExecuteTaskPriority) {
  dingodb::ThreadPool thread_pool("unit_test", 1);

  std::atomic<int> count = 1;
  std::vector<int> run_orders(3, 0);
  std::vector<dingodb::ThreadPool::TaskPtr> tasks;
  tasks.push_back(
      thread_pool.ExecuteTask([](void *) { std::this_thread::sleep_for(std::chrono::milliseconds(2)); }, nullptr));

  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  tasks.push_back(thread_pool.ExecuteTask(
      [&run_orders, pos = 0, &count](void *) { run_orders[pos] = count.fetch_add(1); }, nullptr, 1));

  tasks.push_back(thread_pool.ExecuteTask(
      [&run_orders, pos = 1, &count](void *) { run_orders[pos] = count.fetch_add(1); }, nullptr, 2));

  tasks.push_back(thread_pool.ExecuteTask(
      [&run_orders, pos = 2, &count](void *) { run_orders[pos] = count.fetch_add(1); }, nullptr, 3));

  for (auto &task : tasks) {
    task->Join();
  }

  ASSERT_EQ(3, run_orders[0]);
  ASSERT_EQ(2, run_orders[1]);
  ASSERT_EQ(1, run_orders[2]);
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
  int max_priority = sched_get_priority_max(policy);
  int min_priority = sched_get_priority_min(policy);
  LOG(INFO) << fmt::format("policy {} priority=[{}, {}]", policy, min_priority, max_priority);
}

static int GetThreadPriority(pthread_attr_t &attr) {
  struct sched_param param;
  int ret = pthread_attr_getschedparam(&attr, &param);
  LOG(INFO) << "priority = " << param.__sched_priority;
  return param.__sched_priority;
}

static void SetThreadPolicy(pthread_attr_t &attr, int policy) {
  int ret = pthread_attr_setschedpolicy(&attr, policy);
  ASSERT_EQ(0, ret);
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
  ShowThreadPriority(attr, policy);
  ShowThreadPriority(attr, SCHED_FIFO);
  ShowThreadPriority(attr, SCHED_RR);

  int priority = GetThreadPriority(attr);
  SetThreadPolicy(attr, SCHED_FIFO);
  SetThreadPolicy(attr, SCHED_RR);
  SetThreadPolicy(attr, policy);

  rs = pthread_attr_destroy(&attr);
}

TEST_F(ThreadPoolTest, ConditionVariable) {
  GTEST_SKIP() << "Performence test, skip...";

  dingodb::ThreadPool thread_pool("unit_test", 8);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  int64_t times = 20 * 1000 * 1000;
  std::vector<dingodb::ThreadPool::TaskPtr> tasks;
  tasks.reserve(times);
  for (int64_t i = 1; i <= times; ++i) {
    tasks.push_back(thread_pool.ExecuteTask(
        [](void *) {
          // std::cout << "thread: " << std::this_thread::get_id() << std::endl;

          // std::this_thread::sleep_for(std::chrono::milliseconds(2));
          int64_t mulple = 1;
          for (int i = 1; i < 100000; ++i) {
            mulple *= i;
          }
        },
        nullptr));

    if (i % 100 == 0) {
      // thread_pool.Notify();
    }
  }

  for (auto &task : tasks) {
    task->Join();
  }
}
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
#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <vector>

#include "common/helper.h"
#include "common/logging.h"
#include "common/runnable.h"
#include "common/synchronization.h"
#include "gflags/gflags.h"

DEFINE_int64(worker_set_worker_num, 10, "The number of workers in the WorkerSet test");
DEFINE_int64(worker_set_max_pending_num, 100000, "The max pending number in the WorkerSet test");
DEFINE_int32(worker_set_bthread_num, 20, "The number of bthreads in the WorkerSet test");
DEFINE_int32(worker_set_task_num_per_bthread, 10000, "The number of tasks per bthread in the WorkerSet test");

class DingoWorkerSetTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

class TestWorkerSetTask : public dingodb::TaskRunnable {
 public:
  using Handler = std::function<void(void)>;
  TestWorkerSetTask(std::atomic<uint64_t>* total_time_ns, std::atomic<uint64_t>* total_count)
      : total_time_ns_(total_time_ns), total_count_(total_count) {
    start_time_ns_ = dingodb::Helper::TimestampNs();
  }
  ~TestWorkerSetTask() override = default;

  std::string Type() override { return "TEST_TASK"; }

  void Run() override {
    end_time_ns_ = dingodb::Helper::TimestampNs();
    total_time_ns_->fetch_add(end_time_ns_ - start_time_ns_);
    total_count_->fetch_sub(1, std::memory_order_relaxed);
  }

 private:
  int64_t start_time_ns_{0};
  int64_t end_time_ns_{0};

  std::atomic<uint64_t>* total_time_ns_{nullptr};
  std::atomic<uint64_t>* total_count_{nullptr};
};

TEST(DingoWorkerSetTest, init) {
  bool ret = false;

  dingodb::WorkerSetPtr test_worker_set = dingodb::WorkerSet::New("TestWorkerSet", 10, 100);
  ret = test_worker_set->Init();
  EXPECT_TRUE(ret);

  dingodb::PriorWorkerSetPtr test_prior_worker_set = dingodb::PriorWorkerSet::New("TestPriorWorkerSet", 10, 100, false);
  ret = test_worker_set->Init();
  EXPECT_TRUE(ret);
}

TEST(DingoWorkerSetTest, perf) {
  bool ret = false;

  dingodb::WorkerSetPtr test_worker_set =
      dingodb::WorkerSet::New("TestWorkerSet", FLAGS_worker_set_worker_num, FLAGS_worker_set_max_pending_num);
  ret = test_worker_set->Init();
  EXPECT_TRUE(ret);

  int32_t thread_num = FLAGS_worker_set_bthread_num;
  int32_t count_per_thread = FLAGS_worker_set_task_num_per_bthread;

  std::atomic<uint64_t> total_count = thread_num * count_per_thread;
  std::atomic<uint64_t> total_time_ns{0};

  auto gen_task_func = [&]() {
    for (int32_t i = 0; i < count_per_thread; i++) {
      auto task = std::make_shared<TestWorkerSetTask>(&total_time_ns, &total_count);

      bool exec_result = false;
      while (!exec_result) {
        exec_result = test_worker_set->ExecuteRR(task);
        if (!exec_result) {
          LOG(ERROR) << "ExecuteRR failed";
        }
      }
    }
  };

  std::vector<dingodb::Bthread> threads;
  threads.reserve(thread_num);
  for (int32_t i = 0; i < thread_num; i++) {
    threads.emplace_back(dingodb::Bthread());
  }

  DINGO_LOG(ERROR) << "Bthread is push_back";

  for (auto& thread : threads) {
    thread.Run(gen_task_func);
  }

  DINGO_LOG(ERROR) << "Bthread is running";

  while (total_count.load(std::memory_order_relaxed) > 0) {
    // DINGO_LOG(ERROR) << "total_count: " << total_count.load(std::memory_order_relaxed);
    bthread_usleep(1000);
  }

  DINGO_LOG(ERROR) << "total_time_ns of WorkerSet: " << total_time_ns.load(std::memory_order_relaxed);
}

TEST(DingoWorkerSetTest, perf_prior) {
  bool ret = false;

  dingodb::PriorWorkerSetPtr test_worker_set = dingodb::PriorWorkerSet::New(
      "TestPriorWorkerSet", FLAGS_worker_set_worker_num, FLAGS_worker_set_max_pending_num, false);
  ret = test_worker_set->Init();
  EXPECT_TRUE(ret);

  int32_t thread_num = FLAGS_worker_set_bthread_num;
  int32_t count_per_thread = FLAGS_worker_set_task_num_per_bthread;

  std::atomic<uint64_t> total_count = thread_num * count_per_thread;
  std::atomic<uint64_t> total_time_ns{0};

  auto gen_task_func = [&]() {
    for (int32_t i = 0; i < count_per_thread; i++) {
      auto task = std::make_shared<TestWorkerSetTask>(&total_time_ns, &total_count);

      bool exec_result = false;
      while (!exec_result) {
        exec_result = test_worker_set->ExecuteRR(task);
        if (!exec_result) {
          LOG(ERROR) << "ExecuteRR failed";
        }
      }
    }
  };

  std::vector<dingodb::Bthread> threads;
  threads.reserve(thread_num);
  for (int32_t i = 0; i < thread_num; i++) {
    threads.emplace_back(dingodb::Bthread());
  }

  DINGO_LOG(ERROR) << "Bthread is push_back";

  for (auto& thread : threads) {
    thread.Run(gen_task_func);
  }

  DINGO_LOG(ERROR) << "Bthread is running";

  while (total_count.load(std::memory_order_relaxed) > 0) {
    // DINGO_LOG(ERROR) << "total_count: " << total_count.load(std::memory_order_relaxed);
    bthread_usleep(1000);
  }

  DINGO_LOG(ERROR) << "total_time_ns of WorkerSet: " << total_time_ns.load(std::memory_order_relaxed);
}

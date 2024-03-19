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

#ifndef DINGODB_COMMON_THREADPOOL_H_
#define DINGODB_COMMON_THREADPOOL_H_

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "common/synchronization.h"

namespace dingodb {

class ThreadPool {
 public:
  ThreadPool(const std::string &thread_name, uint32_t thread_num);
  ThreadPool(const std::string &thread_name, uint32_t thread_num, std::function<void(void)> init_thread);
  ~ThreadPool();

  using Funcer = std::function<void(void *)>;
  // Thread task
  struct Task {
    // The value bigger, the priority higher
    int32_t priority{0};
    Funcer func;
    void *arg{nullptr};
    BthreadCondPtr cond{nullptr};

    bool operator()(const std::shared_ptr<Task> &lhs, const std::shared_ptr<Task> &rhs) {
      return lhs->priority < rhs->priority;
    }

    void Join();
    void WakeUp();
  };
  using TaskPtr = std::shared_ptr<Task>;

  TaskPtr ExecuteTask(Funcer func, void *arg, int priority = 0);

  uint64_t TotalTaskCount();
  void IncTotalTaskCount();

  int64_t PendingTaskCount();
  void IncPendingTaskCount();
  void DecPendingTaskCount();

 private:
  std::string thread_name_;
  bool stop_;

  std::vector<std::thread> workers_;

  std::mutex task_mutex_;
  std::condition_variable task_condition_;
  std::priority_queue<TaskPtr, std::vector<TaskPtr>, Task> tasks_;

  // metrics
  bvar::Adder<uint64_t> total_task_count_metrics_;
  bvar::Adder<int64_t> pending_task_count_metrics_;
};

using ThreadPoolPtr = std::shared_ptr<ThreadPool>;

}  // namespace dingodb

#endif  // DINGODB_COMMON_THREADPOOL_H_

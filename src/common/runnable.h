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

#ifndef DINGODB_COMMON_RUNNABLE_H_
#define DINGODB_COMMON_RUNNABLE_H_

#include <memory>
#include <string>

#include "bthread/execution_queue.h"

namespace dingodb {

class TaskRunnable {
 public:
  TaskRunnable();
  virtual ~TaskRunnable();

  virtual std::string Type() = 0;

  virtual void Run() = 0;
};

using TaskRunnablePtr = std::shared_ptr<TaskRunnable>;

int ExecuteRoutine(void*, bthread::TaskIterator<TaskRunnablePtr>& iter);

// Run task worker
class Worker {
 public:
  Worker() = default;
  ~Worker() = default;

  static std::shared_ptr<Worker> New() { return std::make_shared<Worker>(); }

  bool Init();
  void Destroy();

  bool Execute(TaskRunnablePtr task);

 private:
  // Execution queue is available.
  std::atomic<bool> is_available_;
  bthread::ExecutionQueueId<TaskRunnablePtr> queue_id_;  // NOLINT
};

using WorkerPtr = std::shared_ptr<Worker>;

}  // namespace dingodb

#endif  // DINGODB_COMMON_RUNNABLE_H_
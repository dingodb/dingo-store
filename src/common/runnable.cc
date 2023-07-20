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

#include "common/runnable.h"

#include "common/logging.h"

namespace dingodb {

int ExecuteRoutine(void*, bthread::TaskIterator<TaskRunnable*>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  {
    std::unique_ptr<TaskRunnable> self_guard(*iter);
    for (; iter; ++iter) {
      (*iter)->Run();
    }
  }

  return 0;
}

bool Worker::Init() {
  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;

  if (bthread::execution_queue_start(&queue_id_, &options, ExecuteRoutine, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Start worker execution queue failed";
    return false;
  }

  is_available_.store(true, std::memory_order_relaxed);

  return true;
}

void Worker::Destroy() {
  is_available_.store(false, std::memory_order_relaxed);

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "Worker execution queue stop failed";
    return;
  }

  if (bthread::execution_queue_join(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "Worker execution queue join failed";
  }
}

bool Worker::Execute(TaskRunnable* task) {
  if (!is_available_.load(std::memory_order_relaxed)) {
    DINGO_LOG(ERROR) << "Worker execute queue is not available.";
    return false;
  }

  if (bthread::execution_queue_execute(queue_id_, task) != 0) {
    DINGO_LOG(ERROR) << "Worker execution queue execute failed";
    return false;
  }

  return true;
}

}  // namespace dingodb
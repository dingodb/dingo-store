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

#include <cstdint>
#include <memory>

#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"

namespace dingodb {

TaskRunnable::TaskRunnable() { DINGO_LOG(INFO) << "new exec task..."; }
TaskRunnable::~TaskRunnable() { DINGO_LOG(INFO) << "delete exec task..."; }

int ExecuteRoutine(void*, bthread::TaskIterator<TaskRunnablePtr>& iter) {  // NOLINT
  for (; iter; ++iter) {
    if (iter.is_queue_stopped()) {
      DINGO_LOG(INFO) << fmt::format("[execqueue][type({})] task is stopped.", (*iter)->Type());
      continue;
    }
    if (*iter == nullptr) {
      DINGO_LOG(WARNING) << fmt::format("[execqueue][type({})] task is nullptr.", (*iter)->Type());
      continue;
    }

    int64_t start_time = Helper::TimestampMs();
    (*iter)->Run();
    DINGO_LOG(INFO) << fmt::format("[execqueue][type({})] run task elapsed time {}(ms).", (*iter)->Type(),
                                   Helper::TimestampMs() - start_time);
  }

  return 0;
}

bool Worker::Init() {
  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;

  if (bthread::execution_queue_start(&queue_id_, &options, ExecuteRoutine, nullptr) != 0) {
    DINGO_LOG(ERROR) << "[execqueue] start worker execution queue failed";
    return false;
  }

  is_available_.store(true, std::memory_order_relaxed);

  return true;
}

void Worker::Destroy() {
  is_available_.store(false, std::memory_order_relaxed);

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "[execqueue] worker execution queue stop failed";
    return;
  }

  if (bthread::execution_queue_join(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "[execqueue] worker execution queue join failed";
  }
}

bool Worker::Execute(TaskRunnablePtr task) {
  if (task == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[execqueue][type({})] task is nullptr.", task->Type());
    return false;
  }

  if (!is_available_.load(std::memory_order_relaxed)) {
    DINGO_LOG(ERROR) << fmt::format("[execqueue][type({})] worker execute queue is not available.", task->Type());
    return false;
  }

  if (bthread::execution_queue_execute(queue_id_, task) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[execqueue][type({})] worker execution queue execute failed", task->Type());
    return false;
  }

  return true;
}

}  // namespace dingodb
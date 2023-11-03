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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "butil/compiler_specific.h"
#include "client/coordinator_client_function.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"

namespace dingodb {

TaskRunnable::TaskRunnable() { DINGO_LOG(DEBUG) << "new exec task..."; }
TaskRunnable::~TaskRunnable() { DINGO_LOG(DEBUG) << "delete exec task..."; }

int ExecuteRoutine(void* meta, bthread::TaskIterator<TaskRunnablePtr>& iter) {  // NOLINT
  Worker* worker = static_cast<Worker*>(meta);

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
    DINGO_LOG(DEBUG) << fmt::format("[execqueue][type({})] run task elapsed time {}(ms).", (*iter)->Type(),
                                    Helper::TimestampMs() - start_time);

    if (worker != nullptr) {
      worker->DecPendingTaskCount();
      worker->Nodify(Worker::EventType::kFinishTask);
    }
  }

  return 0;
}

bool Worker::Init() {
  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;

  if (bthread::execution_queue_start(&queue_id_, &options, ExecuteRoutine, this) != 0) {
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

  IncPendingTaskCount();
  IncTotalTaskCount();

  Nodify(EventType::kAddTask);

  return true;
}

uint64_t Worker::TotalTaskCount() { return total_task_count_.load(std::memory_order_relaxed); }
void Worker::IncTotalTaskCount() { total_task_count_.fetch_add(1, std::memory_order_relaxed); }

uint64_t Worker::PendingTaskCount() { return pending_task_count_.load(std::memory_order_relaxed); }
void Worker::IncPendingTaskCount() { pending_task_count_.fetch_add(1, std::memory_order_relaxed); }
void Worker::DecPendingTaskCount() { pending_task_count_.fetch_sub(1, std::memory_order_relaxed); }

void Worker::Nodify(EventType type) {
  if (notify_func_ != nullptr) {
    notify_func_(type);
  }
}

WorkerSet::WorkerSet(std::string name, uint32_t worker_num, uint32_t max_pending_task_count)
    : name_(name),
      worker_num_(worker_num),
      max_pending_task_count_(max_pending_task_count),
      active_worker_id_(0),
      total_task_count_(fmt::format("dingo_{}_total_task_count", name)),
      pending_task_count_(fmt::format("dingo_{}_pending_task_count", name)) {}

WorkerSet::~WorkerSet() = default;

bool WorkerSet::Init() {
  for (int i = 0; i < worker_num_; ++i) {
    auto worker = Worker::New([this](Worker::EventType type) { WatchWorker(type); });
    if (!worker->Init()) {
      return false;
    }
    workers_.push_back(worker);
  }

  return true;
}

void WorkerSet::Destroy() {
  for (const auto& worker : workers_) {
    worker->Destroy();
  }
}

bool WorkerSet::ExecuteRR(TaskRunnablePtr task) {
  if (BAIDU_UNLIKELY(max_pending_task_count_ > 0 &&
                     pending_task_counter_.load(std::memory_order_relaxed) > max_pending_task_count_)) {
    return false;
  }

  auto ret = workers_[active_worker_id_.fetch_add(1) % worker_num_]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

bool WorkerSet::ExecuteHashByRegionId(int64_t region_id, TaskRunnablePtr task) {
  if (BAIDU_UNLIKELY(max_pending_task_count_ > 0 &&
                     pending_task_counter_.load(std::memory_order_relaxed) > max_pending_task_count_)) {
    return false;
  }

  auto ret = workers_[region_id % worker_num_]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

void WorkerSet::WatchWorker(Worker::EventType type) {
  if (type == Worker::EventType::kFinishTask) {
    DecPendingTaskCount();
  }
}

uint64_t WorkerSet::TotalTaskCount() { return total_task_count_.get_value(); }

void WorkerSet::IncTotalTaskCount() { total_task_count_ << 1; }

uint64_t WorkerSet::PendingTaskCount() { return pending_task_count_.get_value(); }

void WorkerSet::IncPendingTaskCount() {
  pending_task_count_ << 1;
  pending_task_counter_.fetch_add(1, std::memory_order_relaxed);
}

void WorkerSet::DecPendingTaskCount() {
  pending_task_count_ << -1;
  pending_task_counter_.fetch_sub(1, std::memory_order_relaxed);
}

}  // namespace dingodb
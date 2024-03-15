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
#include <cstdint>
#include <string>

#include "butil/compiler_specific.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"

namespace dingodb {

TaskRunnable::TaskRunnable() : id_(GenId()) { create_time_us_ = Helper::TimestampUs(); }
TaskRunnable::~TaskRunnable() = default;

uint64_t TaskRunnable::Id() const { return id_; }

uint64_t TaskRunnable::GenId() {
  static std::atomic<uint64_t> gen_id = 1;
  return gen_id.fetch_add(1, std::memory_order_relaxed);
}

int ExecuteRoutine(void* meta, bthread::TaskIterator<TaskRunnablePtr>& iter) {  // NOLINT
  Worker* worker = static_cast<Worker*>(meta);

  for (; iter; ++iter) {
    if (BAIDU_UNLIKELY(*iter == nullptr)) {
      DINGO_LOG(WARNING) << fmt::format("[execqueue][type()] task is nullptr.");
      continue;
    }

    if (BAIDU_LIKELY(!iter.is_queue_stopped())) {
      int64_t start_time = Helper::TimestampMs();
      (*iter)->Run();
      DINGO_LOG(DEBUG) << fmt::format("[execqueue][type({})] run task elapsed time {}(ms).", (*iter)->Type(),
                                      Helper::TimestampMs() - start_time);
    } else {
      DINGO_LOG(INFO) << fmt::format("[execqueue][type({})] task is stopped.", (*iter)->Type());
    }

    if (BAIDU_LIKELY(worker != nullptr)) {
      worker->PopPendingTaskTrace((*iter)->Id());
      worker->DecPendingTaskCount();
      worker->Notify(WorkerEventType::kFinishTask);
    }
  }

  return 0;
}

Worker::Worker(NotifyFuncer notify_func) : is_available_(false), notify_func_(notify_func) {
  bthread_mutex_init(&trace_mutex_, nullptr);
}

Worker::~Worker() { bthread_mutex_destroy(&trace_mutex_); }

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

  AppendPendingTaskTrace(task->Id(), task->Trace());

  if (bthread::execution_queue_execute(queue_id_, task) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[execqueue][type({})] worker execution queue execute failed", task->Type());
    PopPendingTaskTrace(task->Id());
    return false;
  }

  IncPendingTaskCount();
  IncTotalTaskCount();

  Notify(WorkerEventType::kAddTask);

  return true;
}

uint64_t Worker::TotalTaskCount() { return total_task_count_.load(std::memory_order_relaxed); }
void Worker::IncTotalTaskCount() { total_task_count_.fetch_add(1, std::memory_order_relaxed); }

int32_t Worker::PendingTaskCount() { return pending_task_count_.load(std::memory_order_relaxed); }
void Worker::IncPendingTaskCount() { pending_task_count_.fetch_add(1, std::memory_order_relaxed); }
void Worker::DecPendingTaskCount() { pending_task_count_.fetch_sub(1, std::memory_order_relaxed); }

void Worker::Notify(WorkerEventType type) {
  if (notify_func_ != nullptr) {
    notify_func_(type);
  }
}

void Worker::AppendPendingTaskTrace(uint64_t task_id, const std::string& trace) {
  if (!trace.empty()) {
    BAIDU_SCOPED_LOCK(trace_mutex_);
    pending_task_traces_.insert_or_assign(task_id, trace);
  }
}

void Worker::PopPendingTaskTrace(uint64_t task_id) {
  BAIDU_SCOPED_LOCK(trace_mutex_);

  auto it = pending_task_traces_.find(task_id);
  if (it != pending_task_traces_.end()) {
    pending_task_traces_.erase(it);
  }
}

std::vector<std::string> Worker::GetPendingTaskTrace() {
  BAIDU_SCOPED_LOCK(trace_mutex_);

  std::vector<std::string> traces;
  traces.reserve(pending_task_traces_.size());
  for (auto& [_, trace] : pending_task_traces_) {
    traces.push_back(trace);
  }
  return traces;
}

WorkerSet::WorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count)
    : name_(name),
      worker_num_(worker_num),
      max_pending_task_count_(max_pending_task_count),
      active_worker_id_(0),
      total_task_count_metrics_(fmt::format("dingo_worker_set_{}_total_task_count", name)),
      pending_task_count_metrics_(fmt::format("dingo_worker_set_{}_pending_task_count", name)) {}

WorkerSet::~WorkerSet() = default;

bool WorkerSet::Init() {
  for (int i = 0; i < worker_num_; ++i) {
    auto worker = Worker::New([this](WorkerEventType type) { WatchWorker(type); });
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
                     pending_task_count_.load(std::memory_order_relaxed) > max_pending_task_count_)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}",
                                      pending_task_count_.load(std::memory_order_relaxed), max_pending_task_count_);
    return false;
  }

  auto ret = workers_[active_worker_id_.fetch_add(1) % worker_num_]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

bool WorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) {
  if (BAIDU_UNLIKELY(max_pending_task_count_ > 0 &&
                     pending_task_count_.load(std::memory_order_relaxed) > max_pending_task_count_)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}",
                                      pending_task_count_.load(std::memory_order_relaxed), max_pending_task_count_);
    return false;
  }

  auto ret = workers_[LeastPendingTaskWorker()]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

bool WorkerSet::ExecuteHashByRegionId(int64_t region_id, TaskRunnablePtr task) {
  if (BAIDU_UNLIKELY(max_pending_task_count_ > 0 &&
                     pending_task_count_.load(std::memory_order_relaxed) > max_pending_task_count_)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}",
                                      pending_task_count_.load(std::memory_order_relaxed), max_pending_task_count_);
    return false;
  }

  auto ret = workers_[region_id % worker_num_]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

void WorkerSet::WatchWorker(WorkerEventType type) {
  if (type == WorkerEventType::kFinishTask) {
    DecPendingTaskCount();
  }
}

uint32_t WorkerSet::LeastPendingTaskWorker() {
  uint32_t index = 0;
  int32_t min_pending_count = INT32_MAX;
  uint32_t worker_num = workers_.size();
  for (uint32_t i = 0; i < worker_num; ++i) {
    auto& worker = workers_[i];
    int32_t pending_count = worker->PendingTaskCount();
    if (pending_count < min_pending_count) {
      min_pending_count = pending_count;
      index = i;
    }
  }

  return index;
}

uint64_t WorkerSet::TotalTaskCount() { return total_task_count_metrics_.get_value(); }

void WorkerSet::IncTotalTaskCount() { total_task_count_metrics_ << 1; }

uint64_t WorkerSet::PendingTaskCount() { return pending_task_count_.load(std::memory_order_relaxed); }

void WorkerSet::IncPendingTaskCount() {
  pending_task_count_metrics_ << 1;
  pending_task_count_.fetch_add(1, std::memory_order_relaxed);
}

void WorkerSet::DecPendingTaskCount() {
  pending_task_count_metrics_ << -1;
  pending_task_count_.fetch_sub(1, std::memory_order_relaxed);
}

std::vector<std::vector<std::string>> WorkerSet::GetPendingTaskTrace() {
  std::vector<std::vector<std::string>> traces;

  traces.reserve(workers_.size());
  for (auto& worker : workers_) {
    traces.push_back(worker->GetPendingTaskTrace());
  }

  return traces;
}

PriorWorkerSet::PriorWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread)
    : name_(name),
      worker_num_(worker_num),
      use_pthread_(use_pthread),
      max_pending_task_count_(max_pending_task_count),
      total_task_count_metrics_(fmt::format("dingo_prior_worker_set_{}_total_task_count", name)),
      pending_task_count_metrics_(fmt::format("dingo_prior_worker_set_{}_pending_task_count", name)),
      queue_wait_metrics_(fmt::format("dingo_prior_worker_set_{}_queue_wait_latency", name)),
      queue_run_metrics_(fmt::format("dingo_prior_worker_set_{}_queue_run_latency", name)) {
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
}

PriorWorkerSet::~PriorWorkerSet() {
  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool PriorWorkerSet::Init() {
  uint32_t i = 0;

  auto worker_function = [this, &i]() {
    if (use_pthread_) {
      pthread_setname_np(pthread_self(), (name_ + ":" + std::to_string(i)).c_str());
    }

    while (true) {
      bthread_mutex_lock(&mutex_);
      while (pending_task_count_.load(std::memory_order_relaxed) == 0) {
        bthread_cond_wait(&cond_, &mutex_);
      }

      // get task from task queue
      TaskRunnablePtr task = nullptr;
      int64_t now_time_us = 0;
      if (!tasks_.empty()) {
        task = tasks_.top();
        tasks_.pop();

        now_time_us = Helper::TimestampUs();
        queue_wait_metrics_ << now_time_us - task->CreateTimeUs();
      }

      bthread_mutex_unlock(&mutex_);

      if (BAIDU_UNLIKELY(task != nullptr)) {
        task->Run();
        queue_run_metrics_ << Helper::TimestampUs() - now_time_us;
        DecPendingTaskCount();
        Notify(WorkerEventType::kFinishTask);
      }
    }
  };

  if (use_pthread_) {
    for (i = 0; i < worker_num_; ++i) {
      pthread_workers_.push_back(std::thread(worker_function));
    }
  } else {
    for (i = 0; i < worker_num_; ++i) {
      bthread_workers_.push_back(Bthread(worker_function));
    }
  }

  return true;
}

void PriorWorkerSet::Destroy() {
  if (use_pthread_) {
    for (auto& std_thread : pthread_workers_) {
      std_thread.join();
    }
  } else {
    for (auto& bthread : bthread_workers_) {
      bthread.Join();
    }
  }
}

bool PriorWorkerSet::Execute(TaskRunnablePtr task) {
  auto pend_task_count = pending_task_count_.load(std::memory_order_relaxed);

  if (BAIDU_UNLIKELY(max_pending_task_count_ > 0 && pend_task_count > max_pending_task_count_)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}",
                                      pending_task_count_.load(std::memory_order_relaxed), max_pending_task_count_);
    return false;
  }

  IncPendingTaskCount();
  IncTotalTaskCount();

  bthread_mutex_lock(&mutex_);
  tasks_.push(task);
  bthread_cond_signal(&cond_);
  bthread_mutex_unlock(&mutex_);

  return true;
}

bool PriorWorkerSet::ExecuteRR(TaskRunnablePtr task) { return Execute(task); }

bool PriorWorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) { return Execute(task); }

bool PriorWorkerSet::ExecuteHashByRegionId(int64_t /*region_id*/, TaskRunnablePtr task) { return Execute(task); }

void PriorWorkerSet::WatchWorker(WorkerEventType type) {
  if (type == WorkerEventType::kFinishTask) {
    DecPendingTaskCount();
  }
}

uint64_t PriorWorkerSet::TotalTaskCount() { return total_task_count_metrics_.get_value(); }

void PriorWorkerSet::IncTotalTaskCount() { total_task_count_metrics_ << 1; }

uint64_t PriorWorkerSet::PendingTaskCount() { return pending_task_count_.load(std::memory_order_relaxed); }

void PriorWorkerSet::IncPendingTaskCount() {
  pending_task_count_metrics_ << 1;
  pending_task_count_.fetch_add(1, std::memory_order_relaxed);
}

void PriorWorkerSet::DecPendingTaskCount() {
  pending_task_count_metrics_ << -1;
  pending_task_count_.fetch_sub(1, std::memory_order_relaxed);
}

std::vector<std::vector<std::string>> PriorWorkerSet::GetPendingTaskTrace() {  // NOLINT
  std::vector<std::vector<std::string>> traces;

  return traces;
}

void PriorWorkerSet::Notify(WorkerEventType type) {
  if (notify_func_ != nullptr) {
    notify_func_(type);
  }
}

}  // namespace dingodb
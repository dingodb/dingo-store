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

#include "bthread/bthread.h"
#include "butil/compiler_specific.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "gflags/gflags.h"

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
  if (BAIDU_UNLIKELY(task == nullptr)) {
    DINGO_LOG(ERROR) << fmt::format("[execqueue][type({})] task is nullptr.", task->Type());
    return false;
  }

  if (BAIDU_UNLIKELY(!is_available_.load(std::memory_order_relaxed))) {
    DINGO_LOG(ERROR) << fmt::format("[execqueue][type({})] worker execute queue is not available.", task->Type());
    return false;
  }

  AppendPendingTaskTrace(task->Id(), task->Trace());

  if (BAIDU_UNLIKELY(bthread::execution_queue_execute(queue_id_, task) != 0)) {
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

WorkerSet::WorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread,
                     bool is_inplace_run)
    : name_(name),
      worker_num_(worker_num),
      max_pending_task_count_(max_pending_task_count),
      use_pthread_(use_pthread),
      is_inplace_run(is_inplace_run),
      total_task_count_metrics_(fmt::format("dingo_worker_set_{}_total_task_count", name)),
      pending_task_count_metrics_(fmt::format("dingo_worker_set_{}_pending_task_count", name)),
      queue_wait_metrics_(fmt::format("dingo_worker_set_{}_queue_wait_latency", name)),
      queue_run_metrics_(fmt::format("dingo_worker_set_{}_queue_run_latency", name)){};

bool ExecqWorkerSet::Init() {
  for (int i = 0; i < WorkerNum(); ++i) {
    auto worker = Worker::New([this](WorkerEventType type) { WatchWorker(type); });
    if (!worker->Init()) {
      return false;
    }

    workers_.push_back(worker);
  }

  return true;
}

void ExecqWorkerSet::Destroy() {
  for (const auto& worker : workers_) {
    worker->Destroy();
  }
}

bool ExecqWorkerSet::ExecuteRR(TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  uint64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  auto ret = workers_[active_worker_id_.fetch_add(1) % WorkerNum()]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

bool ExecqWorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  uint64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  auto ret = workers_[LeastPendingTaskWorker()]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

bool ExecqWorkerSet::ExecuteHashByRegionId(int64_t region_id, TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  uint64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  auto ret = workers_[region_id % WorkerNum()]->Execute(task);
  if (ret) {
    IncPendingTaskCount();
    IncTotalTaskCount();
  }

  return ret;
}

uint32_t ExecqWorkerSet::LeastPendingTaskWorker() {
  uint32_t min_pending_index = 0;
  int32_t min_pending_count = INT32_MAX;
  uint32_t worker_num = workers_.size();

  for (uint32_t i = 0; i < worker_num; ++i) {
    auto& worker = workers_[i];
    int32_t pending_count = worker->PendingTaskCount();
    if (pending_count < min_pending_count) {
      min_pending_count = pending_count;
      min_pending_index = i;
    }
  }

  return min_pending_index;
}

std::vector<std::vector<std::string>> ExecqWorkerSet::GetPendingTaskTrace() {
  std::vector<std::vector<std::string>> traces;

  traces.reserve(workers_.size());
  for (auto& worker : workers_) {
    traces.push_back(worker->GetPendingTaskTrace());
  }

  return traces;
}

SimpleWorkerSet::SimpleWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count,
                                 bool use_pthread, bool is_inplace_run)
    : WorkerSet(name, worker_num, max_pending_task_count, use_pthread, is_inplace_run) {
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
}

SimpleWorkerSet::~SimpleWorkerSet() {
  Destroy();

  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool SimpleWorkerSet::Init() {
  auto worker_function = [this]() {
    if (IsUsePthread()) {
      pthread_setname_np(pthread_self(), GenWorkerName().c_str());
    }

    while (true) {
      bthread_mutex_lock(&mutex_);
      while (!is_stop && tasks_.empty()) {
        bthread_cond_wait(&cond_, &mutex_);
      }

      if (is_stop && tasks_.empty()) {
        bthread_mutex_unlock(&mutex_);
        break;
      }

      // get task from task queue
      TaskRunnablePtr task = nullptr;
      if (BAIDU_LIKELY(!tasks_.empty())) {
        task = tasks_.front();
        tasks_.pop();
      }

      bthread_mutex_unlock(&mutex_);

      if (BAIDU_LIKELY(task != nullptr)) {
        int64_t now_time_us = Helper::TimestampUs();
        QueueWaitMetrics(now_time_us - task->CreateTimeUs());

        task->Run();

        QueueRunMetrics(Helper::TimestampUs() - now_time_us);
        DecPendingTaskCount();
        Notify(WorkerEventType::kFinishTask);
      }
    }

    stoped_count.fetch_add(1);
  };

  if (IsUsePthread()) {
    for (int i = 0; i < WorkerNum(); ++i) {
      pthread_workers_.push_back(std::thread(worker_function));
    }
  } else {
    for (int i = 0; i < WorkerNum(); ++i) {
      bthread_workers_.push_back(Bthread(worker_function));
    }
  }

  return true;
}

void SimpleWorkerSet::Destroy() {
  // guarantee idempotent
  if (IsDestroied()) {
    return;
  }

  // stop worker thread/bthread
  bthread_mutex_lock(&mutex_);
  is_stop = true;
  bthread_mutex_unlock(&mutex_);

  while (stoped_count.load() < WorkerNum()) {
    bthread_cond_signal(&cond_);
    bthread_usleep(100000);
  }

  // join thread/bthread
  if (IsUsePthread()) {
    for (auto& std_thread : pthread_workers_) {
      std_thread.join();
    }
  } else {
    for (auto& bthread : bthread_workers_) {
      bthread.Join();
    }
  }
}

bool SimpleWorkerSet::Execute(TaskRunnablePtr task) {
  int64_t max_pending_task_count = MaxPendingTaskCount();
  uint64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  IncPendingTaskCount();
  IncTotalTaskCount();

  // if the pending task count is less than the worker number, execute the task directly
  // else push the task to the task queue
  // the total count of pending task will be decreased in the worker function
  // and the total concurrency is limited by the worker number
  if (is_inplace_run && pending_task_count < WorkerNum()) {
    int64_t now_time_us = Helper::TimestampUs();

    task->Run();

    QueueRunMetrics(Helper::TimestampUs() - now_time_us);

    DecPendingTaskCount();
    Notify(WorkerEventType::kFinishTask);

  } else {
    bthread_mutex_lock(&mutex_);
    tasks_.push(task);
    bthread_mutex_unlock(&mutex_);
    bthread_cond_signal(&cond_);
  }

  return true;
}

bool SimpleWorkerSet::ExecuteRR(TaskRunnablePtr task) { return Execute(task); }

bool SimpleWorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) { return Execute(task); }

bool SimpleWorkerSet::ExecuteHashByRegionId(int64_t /*region_id*/, TaskRunnablePtr task) { return Execute(task); }

PriorWorkerSet::PriorWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread,
                               bool is_inplace_run)
    : WorkerSet(name, worker_num, max_pending_task_count, use_pthread, is_inplace_run) {
  bthread_mutex_init(&mutex_, nullptr);
  bthread_cond_init(&cond_, nullptr);
}

PriorWorkerSet::~PriorWorkerSet() {
  Destroy();

  bthread_cond_destroy(&cond_);
  bthread_mutex_destroy(&mutex_);
}

bool PriorWorkerSet::Init() {
  auto worker_function = [this]() {
    if (IsUsePthread()) {
      pthread_setname_np(pthread_self(), GenWorkerName().c_str());
    }

    while (true) {
      bthread_mutex_lock(&mutex_);
      while (!is_stop && PendingTaskCount() == 0) {
        bthread_cond_wait(&cond_, &mutex_);
      }
      if (is_stop && PendingTaskCount() == 0) {
        bthread_mutex_unlock(&mutex_);
        break;
      }

      // get task from task queue
      TaskRunnablePtr task = nullptr;
      if (BAIDU_LIKELY(!tasks_.empty())) {
        task = tasks_.top();
        tasks_.pop();
      }

      bthread_mutex_unlock(&mutex_);

      if (BAIDU_LIKELY(task != nullptr)) {
        int64_t now_time_us = Helper::TimestampUs();
        QueueWaitMetrics(now_time_us - task->CreateTimeUs());

        task->Run();

        QueueRunMetrics(Helper::TimestampUs() - now_time_us);
        DecPendingTaskCount();
        Notify(WorkerEventType::kFinishTask);
      }
    }

    stoped_count.fetch_add(1);
  };

  if (IsUsePthread()) {
    for (int i = 0; i < WorkerNum(); ++i) {
      pthread_workers_.push_back(std::thread(worker_function));
    }
  } else {
    for (int i = 0; i < WorkerNum(); ++i) {
      bthread_workers_.push_back(Bthread(worker_function));
    }
  }

  return true;
}

void PriorWorkerSet::Destroy() {
  // guarantee idempotent
  if (IsDestroied()) {
    return;
  }

  // stop worker thread/bthread
  bthread_mutex_lock(&mutex_);
  is_stop = true;
  bthread_mutex_unlock(&mutex_);

  while (stoped_count.load() < WorkerNum()) {
    bthread_cond_signal(&cond_);
    bthread_usleep(100000);
  }

  // join thread/bthread
  if (IsUsePthread()) {
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
  int64_t max_pending_task_count = MaxPendingTaskCount();
  uint64_t pending_task_count = PendingTaskCount();

  if (BAIDU_UNLIKELY(max_pending_task_count > 0 && pending_task_count > max_pending_task_count)) {
    DINGO_LOG(WARNING) << fmt::format("[execqueue] exceed max pending task limit, {}/{}", pending_task_count,
                                      max_pending_task_count);
    return false;
  }

  IncPendingTaskCount();
  IncTotalTaskCount();

  // if the pending task count is less than the worker number, execute the task directly
  // else push the task to the task queue
  // the total count of pending task will be decreased in the worker function
  // and the total concurrency is limited by the worker number
  if (is_inplace_run && pending_task_count < WorkerNum()) {
    int64_t now_time_us = Helper::TimestampUs();

    task->Run();

    QueueRunMetrics(Helper::TimestampUs() - now_time_us);

    DecPendingTaskCount();
    Notify(WorkerEventType::kFinishTask);

  } else {
    bthread_mutex_lock(&mutex_);
    tasks_.push(task);
    bthread_mutex_unlock(&mutex_);
    bthread_cond_signal(&cond_);
  }

  return true;
}

bool PriorWorkerSet::ExecuteRR(TaskRunnablePtr task) { return Execute(task); }

bool PriorWorkerSet::ExecuteLeastQueue(TaskRunnablePtr task) { return Execute(task); }

bool PriorWorkerSet::ExecuteHashByRegionId(int64_t /*region_id*/, TaskRunnablePtr task) { return Execute(task); }

}  // namespace dingodb
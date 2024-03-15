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

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "bthread/execution_queue.h"
#include "bthread/types.h"
#include "bvar/latency_recorder.h"
#include "common/synchronization.h"

namespace dingodb {

class TaskRunnable {
 public:
  TaskRunnable();
  virtual ~TaskRunnable();

  uint64_t Id() const;
  static uint64_t GenId();

  virtual std::string Type() = 0;

  virtual void Run() = 0;

  virtual std::string Trace() { return ""; }

  int32_t Priority() const { return priority_; }
  void SetPriority(int32_t priority) { priority_ = priority; }

  // Operator overloading to compare tasks.
  bool operator<(const TaskRunnable& other) const {
    // Note: Higher priority tasks should come first.
    return priority_ < other.Priority();
  }

  int64_t CreateTimeUs() const { return create_time_us_; }

 private:
  uint64_t id_{0};
  int32_t priority_{0};
  int64_t create_time_us_{0};
};

using TaskRunnablePtr = std::shared_ptr<TaskRunnable>;

// Custom Comparator for priority_queue
struct CompareTaskRunnable {
  bool operator()(const TaskRunnablePtr& lhs, TaskRunnablePtr& rhs) const { return lhs.get() < rhs.get(); }
};

int ExecuteRoutine(void*, bthread::TaskIterator<TaskRunnablePtr>& iter);

enum class WorkerEventType {
  kAddTask = 0,
  kFinishTask = 1,
};
using NotifyFuncer = std::function<void(WorkerEventType)>;

// Run task worker
class Worker {
 public:
  Worker(NotifyFuncer notify_func);
  ~Worker();

  static std::shared_ptr<Worker> New() { return std::make_shared<Worker>(nullptr); }
  static std::shared_ptr<Worker> New(NotifyFuncer notify_func) { return std::make_shared<Worker>(notify_func); }

  bool Init();
  void Destroy();

  bool Execute(TaskRunnablePtr task);

  uint64_t TotalTaskCount();
  void IncTotalTaskCount();

  int32_t PendingTaskCount();
  void IncPendingTaskCount();
  void DecPendingTaskCount();

  void Notify(WorkerEventType type);

  void AppendPendingTaskTrace(uint64_t task_id, const std::string& trace);
  void PopPendingTaskTrace(uint64_t task_id);
  std::vector<std::string> GetPendingTaskTrace();

 private:
  // Execution queue is available.
  std::atomic<bool> is_available_;
  bthread::ExecutionQueueId<TaskRunnablePtr> queue_id_;

  // Metrics
  std::atomic<uint64_t> total_task_count_{0};
  std::atomic<int32_t> pending_task_count_{0};

  // Notify
  NotifyFuncer notify_func_;

  // trace
  bool is_use_trace_;
  bthread_mutex_t trace_mutex_;
  std::map<uint64_t, std::string> pending_task_traces_;
};

using WorkerPtr = std::shared_ptr<Worker>;

class WorkerSet {
 public:
  WorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count);
  ~WorkerSet();

  static std::shared_ptr<WorkerSet> New(std::string name, uint32_t worker_num, uint32_t max_pending_task_count) {
    return std::make_shared<WorkerSet>(name, worker_num, max_pending_task_count);
  }

  bool Init();
  void Destroy();

  bool ExecuteRR(TaskRunnablePtr task);
  bool ExecuteLeastQueue(TaskRunnablePtr task);
  bool ExecuteHashByRegionId(int64_t region_id, TaskRunnablePtr task);

  void WatchWorker(WorkerEventType type);

  uint64_t TotalTaskCount();
  void IncTotalTaskCount();

  uint64_t PendingTaskCount();
  void IncPendingTaskCount();
  void DecPendingTaskCount();

  std::vector<std::vector<std::string>> GetPendingTaskTrace();

 private:
  uint32_t LeastPendingTaskWorker();

  const std::string name_;
  int64_t max_pending_task_count_;
  uint32_t worker_num_;
  std::vector<WorkerPtr> workers_;
  std::atomic<uint64_t> active_worker_id_;

  std::atomic<int64_t> pending_task_count_{0};

  // Metrics
  bvar::Adder<uint64_t> total_task_count_metrics_;
  bvar::Adder<int64_t> pending_task_count_metrics_;
};

using WorkerSetPtr = std::shared_ptr<WorkerSet>;

class PriorWorkerSet {
 public:
  PriorWorkerSet(std::string name, uint32_t worker_num, int64_t max_pending_task_count, bool use_pthread);
  ~PriorWorkerSet();

  static std::shared_ptr<PriorWorkerSet> New(std::string name, uint32_t worker_num, uint32_t max_pending_task_count,
                                             bool use_pthead) {
    return std::make_shared<PriorWorkerSet>(name, worker_num, max_pending_task_count, use_pthead);
  }

  bool Init();
  void Destroy();

  bool Execute(TaskRunnablePtr task);
  bool ExecuteRR(TaskRunnablePtr task);
  bool ExecuteLeastQueue(TaskRunnablePtr task);
  bool ExecuteHashByRegionId(int64_t region_id, TaskRunnablePtr task);

  void WatchWorker(WorkerEventType type);

  uint64_t TotalTaskCount();
  void IncTotalTaskCount();

  uint64_t PendingTaskCount();
  void IncPendingTaskCount();
  void DecPendingTaskCount();

  std::vector<std::vector<std::string>> GetPendingTaskTrace();

  void Notify(WorkerEventType type);

 private:
  const std::string name_;

  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
  std::priority_queue<TaskRunnablePtr, std::vector<TaskRunnablePtr>, CompareTaskRunnable> tasks_;

  bool use_pthread_;
  std::vector<Bthread> bthread_workers_;
  std::vector<std::thread> pthread_workers_;

  int64_t max_pending_task_count_;
  uint32_t worker_num_;

  std::atomic<int64_t> pending_task_count_{0};

  // Notify
  NotifyFuncer notify_func_;

  // Metrics
  bvar::Adder<uint64_t> total_task_count_metrics_;
  bvar::Adder<int64_t> pending_task_count_metrics_;
  bvar::LatencyRecorder queue_wait_metrics_;
  bvar::LatencyRecorder queue_run_metrics_;
};

using PriorWorkerSetPtr = std::shared_ptr<PriorWorkerSet>;

}  // namespace dingodb

#endif  // DINGODB_COMMON_RUNNABLE_H_
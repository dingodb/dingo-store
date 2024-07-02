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

#ifndef DINGODB_MVCC_TS_PROVIDER_H_
#define DINGODB_MVCC_TS_PROVIDER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/runnable.h"
#include "common/synchronization.h"

namespace dingodb {

class CoordinatorInteraction;
using CoordinatorInteractionPtr = std::shared_ptr<CoordinatorInteraction>;

namespace mvcc {

class BatchTsList;
using BatchTsListPtr = std::shared_ptr<BatchTsList>;

class TsProvider;
using TsProviderPtr = std::shared_ptr<TsProvider>;

// one batch ts, take from tso service
class BatchTs {
 public:
  BatchTs(int64_t physical, int64_t logical, uint32_t count);
  ~BatchTs() = default;

  BatchTs(const BatchTs& batch_ts) {
    physical_ = batch_ts.physical_;
    start_ts_.store(batch_ts.start_ts_.load());
    end_ts_ = batch_ts.end_ts_;
  }

  BatchTs& operator=(const BatchTs& batch_ts) {
    physical_ = batch_ts.physical_;
    start_ts_.store(batch_ts.start_ts_.load());
    end_ts_ = batch_ts.end_ts_;

    return *this;
  }

  static BatchTs* New(int64_t physical, int64_t logical, uint32_t count) {
    return new BatchTs(physical, logical, count);
  }

  static BatchTs* New() { return New(0, 0, 0); }

  int64_t GetTs() {
    int64_t ts = start_ts_.fetch_add(1, std::memory_order_relaxed);
    return ts < end_ts_ ? ts : 0;
  }

  int64_t Remain() {
    int64_t remain = end_ts_ - start_ts_.load(std::memory_order_relaxed);
    return remain >= 0 ? remain : 0;
  }

  int64_t Physical() const { return physical_; }

  void SetDeadTime(int64_t dead_time) { dead_time_ = dead_time; }
  int64_t DeadTime() const { return dead_time_; };

  void Flush() { start_ts_.store(end_ts_); }

  std::atomic<BatchTs*> next{nullptr};

 private:
  int64_t physical_{0};
  std::atomic<int64_t> start_ts_{0};
  int64_t end_ts_{0};

  // dead time ms
  int64_t dead_time_{0};
};

// manage BatchTs cache
// use lock-free list
class BatchTsList {
 public:
  BatchTsList();
  ~BatchTsList();

  static BatchTsListPtr New() { return std::make_shared<BatchTsList>(); }

  void Push(BatchTs* batch_ts);
  int64_t GetTs(int64_t after_ts = 0);

  void Flush();

  void SetMinValidTs(int64_t min_valid_ts) {
    if (min_valid_ts > min_valid_ts_.load(std::memory_order_acquire)) {
      min_valid_ts_.store(min_valid_ts, std::memory_order_release);
    }
  }

  uint32_t ActualCount();
  uint32_t ActualDeadCount();

  int64_t ActiveCount() const { return active_count_.load(std::memory_order_relaxed); }
  int64_t DeadCount() const { return dead_count_.load(std::memory_order_relaxed); }

  void CleanDead();

  std::string DebugInfo();

 private:
  void PushDead(BatchTs* batch_ts);

  bool IsStale(BatchTs* batch_ts);
  bool IsValid(int64_t ts) { return ts > 0 && ts > min_valid_ts_.load(std::memory_order_acquire); }

  // available BatchTs
  std::atomic<BatchTs*> head_{nullptr};
  std::atomic<BatchTs*> tail_{nullptr};

  // temporary save dead BatchTs, for delay delete.
  std::atomic<BatchTs*> dead_head_{nullptr};
  std::atomic<BatchTs*> dead_tail_{nullptr};

  std::atomic<int64_t> last_physical_{0};

  std::atomic<int64_t> min_valid_ts_{0};

  // statistics
  std::atomic<int64_t> active_count_{1};
  std::atomic<int64_t> dead_count_{1};

  std::atomic<int64_t> push_count_{1};
  std::atomic<int64_t> pop_count_{0};

  std::atomic<int64_t> dead_push_count_{1};
  std::atomic<int64_t> dead_pop_count_{0};
};

// manage local tso, provide ts to customer
class TsProvider : public std::enable_shared_from_this<TsProvider> {
 public:
  TsProvider(CoordinatorInteractionPtr interaction) : interaction_(interaction) {
    worker_ = Worker::New();
    batch_ts_list_ = BatchTsList::New();
  }
  ~TsProvider() = default;

  TsProviderPtr GetSelfPtr() { return shared_from_this(); }

  static TsProviderPtr New(CoordinatorInteractionPtr interaction) { return std::make_shared<TsProvider>(interaction); }

  bool Init();

  int64_t GetTs(int64_t after_ts = 0);

  uint64_t RenewEpoch() const { return renew_epoch_.load(std::memory_order_relaxed); };

  void SetMinValidTs(int64_t min_valid_ts) {
    if (batch_ts_list_ != nullptr) {
      batch_ts_list_->SetMinValidTs(min_valid_ts);
    }
  }

  void TriggerRenewBatchTs();

  std::string DebugInfo();

 private:
  friend class TakeBatchTsTask;

  BatchTs* SendTsoRequest();

  void RenewBatchTs();
  void LaunchRenewBatchTs(bool is_sync);

  // manage BatchTs cache
  BatchTsListPtr batch_ts_list_;

  // for run take BatchTs task
  WorkerPtr worker_;

  // access coodinator tso service
  CoordinatorInteractionPtr interaction_;

  // statistics
  std::atomic<uint64_t> get_ts_count_{0};
  std::atomic<uint64_t> get_ts_fail_count_{0};
  std::atomic<uint64_t> renew_epoch_{0};
};

// take BatchTs task, run at worker
class TakeBatchTsTask : public TaskRunnable {
 public:
  TakeBatchTsTask(bool is_sync, uint64_t renew_num, TsProviderPtr ts_provider)
      : renew_num_(renew_num), ts_provider_(ts_provider) {
    if (is_sync) {
      cond_ = std::make_shared<BthreadCond>();
    }
  }
  ~TakeBatchTsTask() override = default;

  std::string Type() override { return "BatchTsTask"; }

  void Run() override {
    if (renew_num_ == ts_provider_->RenewEpoch()) {
      ts_provider_->RenewBatchTs();
    }

    Notify();
  }

  void Wait() {
    if (cond_ != nullptr) {
      cond_->IncreaseWait();
    }
  }

  void Notify() {
    if (cond_ != nullptr) {
      cond_->DecreaseSignal();
    }
  }

 private:
  uint64_t renew_num_{0};
  BthreadCondPtr cond_{nullptr};
  TsProviderPtr ts_provider_{nullptr};
};

}  // namespace mvcc

}  // namespace dingodb

#endif  // DINGODB_MVCC_TS_PROVIDER_H_
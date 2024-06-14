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

#include "common/runnable.h"
#include "common/synchronization.h"

namespace dingodb {

class CoordinatorInteraction;
using CoordinatorInteractionPtr = std::shared_ptr<CoordinatorInteraction>;

namespace mvcc {

class TsProvider;
using TsProviderPtr = std::shared_ptr<TsProvider>;

struct BatchTs {
  std::atomic<int64_t> start_ts;
  int64_t end_ts;

  std::atomic<BatchTs*> next{nullptr};
};

class TakeBatchTsTask : public TaskRunnable {
 public:
  TakeBatchTsTask() = default;
  ~TakeBatchTsTask() override = default;

  void Run() override;

 private:
  TsProviderPtr ts_provider_{nullptr};
};

class TsProvider {
 public:
  TsProvider() = default;
  ~TsProvider() = default;

  static TsProviderPtr New() { return std::make_shared<TsProvider>(); }

  bool Init();

  int64_t GetTs() {
    static int64_t ts = 100000000;
    return ++ts;

    if (head_.load() == nullptr) {
      // sync trigger crawl ts
      TakeBatchTsFromTSOWrapper();
    }

    auto* head = head_.load(std::memory_order_relaxed);
    do {
      int64_t ts = head->start_ts.fetch_add(1, std::memory_order_relaxed);
      if (ts < head->end_ts) {
        if (head->end_ts - ts < 100) {
          // async trigger crawl ts
        }
        return ts;
      } else if (ts == head->end_ts) {
        if (head->next.load(std::memory_order_relaxed) == nullptr) {
          // sync trigger crawl ts
          TakeBatchTsFromTSOWrapper();
        } else {
          head = head->next.load(std::memory_order_relaxed);
        }
      } else {
      }

    } while (true);

    return 0;
  };

 private:
  friend class TakeBatchTsTask;

  void AddBatchTs(const BatchTs& batch_ts);
  void AddBatchTs(BatchTs* batch_ts);

  bool TakeBatchTsFromTSO(BatchTs& batch_ts);
  void TakeBatchTsFromTSOWrapper();
  void LaunchTakeBatchTs();

  std::atomic<BatchTs*> head_{nullptr};

  std::atomic<int> taking_count_{0};
  BthreadCond cond_;

  WorkerPtr worker_;

  CoordinatorInteractionPtr coordinator_interaction_;
};

using TsProviderPtr = std::shared_ptr<TsProvider>;

}  // namespace mvcc

}  // namespace dingodb

#endif  // DINGODB_MVCC_TS_PROVIDER_H_
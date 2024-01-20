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

#ifndef DINGODB_SPLIT_CHECKER_H_
#define DINGODB_SPLIT_CHECKER_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/runnable.h"
#include "engine/raw_engine.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_metrics_manager.h"
#include "proto/common.pb.h"

namespace dingodb {

class MergedIterator {
 public:
  explicit MergedIterator(RawEnginePtr raw_engine, const std::vector<std::string>& cf_names,
                          const std::string& end_key);
  ~MergedIterator() = default;

  struct Entry {
    int iter_pos;
    std::string key;
    uint32_t value_size;

    bool operator()(const Entry& lhs, const Entry& rhs) { return lhs.key > rhs.key; }
  };

  void Seek(const std::string& target);
  bool Valid();
  void Next();

  std::string_view Key();
  uint32_t KeyValueSize();

 private:
  void Next(IteratorPtr iter, int iter_pos);

  RawEnginePtr raw_engine_;
  std::vector<IteratorPtr> iters_;
  std::priority_queue<Entry, std::vector<Entry>, Entry> min_heap_;
};

class SplitChecker {
 public:
  enum class Policy {
    kHalf = 0,
    kSize = 1,
    kKeys = 2,
  };

  SplitChecker(Policy policy) : policy_(policy) {}
  virtual ~SplitChecker() = default;

  // Get policy of split region.
  Policy GetPolicy() { return policy_; };
  std::string GetPolicyName() {
    if (policy_ == Policy::kHalf) {
      return "HALF";
    } else if (policy_ == Policy::kSize) {
      return "SIZE";
    } else if (policy_ == Policy::kKeys) {
      return "KEYS";
    }
    return "";
  };

  // Calculate region split key.
  virtual std::string SplitKey(store::RegionPtr region, const pb::common::Range& physical_range,
                               const std::vector<std::string>& cf_names, uint32_t& count) = 0;

 private:
  Policy policy_;
};

// Split region based half.
class HalfSplitChecker : public SplitChecker {
 public:
  HalfSplitChecker(std::shared_ptr<RawEngine> raw_engine, int64_t split_threshold_size, uint32_t split_chunk_size)
      : SplitChecker(SplitChecker::Policy::kHalf),
        raw_engine_(raw_engine),
        split_threshold_size_(split_threshold_size),
        split_chunk_size_(split_chunk_size) {}
  ~HalfSplitChecker() override = default;

  // base physics key, contain key of multi version.
  std::string SplitKey(store::RegionPtr region, const pb::common::Range& physical_range,
                       const std::vector<std::string>& cf_names, uint32_t& count) override;

 private:
  // Split region when exceed the split_threshold_size.
  int64_t split_threshold_size_;
  // Sampling chunk size.
  uint32_t split_chunk_size_;
  std::shared_ptr<RawEngine> raw_engine_;
};

// Split region based size.
class SizeSplitChecker : public SplitChecker {
 public:
  SizeSplitChecker(std::shared_ptr<RawEngine> raw_engine, int64_t split_size, float split_ratio)
      : SplitChecker(SplitChecker::Policy::kSize),
        raw_engine_(raw_engine),
        split_size_(split_size),
        split_ratio_(split_ratio) {}
  ~SizeSplitChecker() override = default;

  // base physics key, contain key of multi version.
  std::string SplitKey(store::RegionPtr region, const pb::common::Range& physical_range,
                       const std::vector<std::string>& cf_names, uint32_t& count) override;

 private:
  // Split when region exceed the split_size.
  int64_t split_size_;
  // Split key position.
  float split_ratio_;
  std::shared_ptr<RawEngine> raw_engine_;
};

// Split region based keys.
class KeysSplitChecker : public SplitChecker {
 public:
  KeysSplitChecker(std::shared_ptr<RawEngine> raw_engine, uint32_t split_keys_number, float split_keys_ratio)
      : SplitChecker(SplitChecker::Policy::kKeys),
        raw_engine_(raw_engine),
        split_keys_number_(split_keys_number),
        split_keys_ratio_(split_keys_ratio) {}
  ~KeysSplitChecker() override = default;

  // base logic key, ignore key of multi version.
  std::string SplitKey(store::RegionPtr region, const pb::common::Range& physical_range,
                       const std::vector<std::string>& cf_names, uint32_t& count) override;

 private:
  // Split when region key number exceed split_key_number.
  uint32_t split_keys_number_;
  // Split key position.
  float split_keys_ratio_;
  std::shared_ptr<RawEngine> raw_engine_;
};

// Multiple worker run split check task.
class SplitCheckWorkers {
 public:
  SplitCheckWorkers() : offset_(0) { bthread_mutex_init(&mutex_, nullptr); }
  ~SplitCheckWorkers() { bthread_mutex_destroy(&mutex_); }

  bool Init(uint32_t num);
  void Destroy();

  bool Execute(TaskRunnablePtr task);

  bool IsExistRegionChecking(int64_t region_id);
  void AddRegionChecking(int64_t region_id);
  void DeleteRegionChecking(int64_t region_id);

 private:
  // Protect checking_regions_.
  bthread_mutex_t mutex_;
  // Region of doing check.
  std::set<int64_t> checking_regions_;

  // Indicate workers offset for round-robin.
  uint32_t offset_;  // NOLINT
  std::vector<WorkerPtr> workers_;
};

// Check region whether need to split.
class SplitCheckTask : public TaskRunnable {
 public:
  SplitCheckTask(std::shared_ptr<SplitCheckWorkers> split_check_workers, store::RegionPtr region,
                 store::RegionMetricsPtr region_metrics, std::shared_ptr<SplitChecker> split_checker)
      : split_check_workers_(split_check_workers),
        region_(region),
        region_metrics_(region_metrics),
        split_checker_(split_checker) {}
  ~SplitCheckTask() override = default;

  std::string Type() override { return "SPLIT_CHECK"; }

  void Run() override {
    SplitCheck();
    if (region_ != nullptr && split_check_workers_ != nullptr) {
      split_check_workers_->DeleteRegionChecking(region_->Id());
    }
  }

 private:
  void SplitCheck();

  std::shared_ptr<SplitCheckWorkers> split_check_workers_;
  store::RegionPtr region_;
  store::RegionMetricsPtr region_metrics_;
  std::shared_ptr<SplitChecker> split_checker_;
};

// Pre split check, if region approximate size exceed threshold size, then check region actual size.
class PreSplitCheckTask : public TaskRunnable {
 public:
  PreSplitCheckTask(std::shared_ptr<SplitCheckWorkers> split_check_workers)
      : split_check_workers_(split_check_workers) {}
  ~PreSplitCheckTask() override = default;

  std::string Type() override { return "PRE_SPLIT_CHECK"; }

  void Run() override { PreSplitCheck(); }

 private:
  void PreSplitCheck();
  std::shared_ptr<SplitCheckWorkers> split_check_workers_;
};

// Pre roughly check all region whether need split.
class PreSplitChecker {
 public:
  PreSplitChecker() {
    worker_ = Worker::New();
    split_check_workers_ = std::make_shared<SplitCheckWorkers>();
  }
  ~PreSplitChecker() = default;

  bool Init(int num);
  void Destroy();

  // Trigger pre split check for split region.
  static void TriggerPreSplitCheck(void*);

  std::shared_ptr<SplitCheckWorkers> GetSplitCheckWorkers() { return split_check_workers_; }

 private:
  bool Execute(TaskRunnablePtr task);

  // For pre split check.
  WorkerPtr worker_;
  // For split check.
  std::shared_ptr<SplitCheckWorkers> split_check_workers_;
};

}  // namespace dingodb

#endif
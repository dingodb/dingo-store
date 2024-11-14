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

#ifndef DINGODB_MERGE_CHECKER_H_
#define DINGODB_MERGE_CHECKER_H_

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

// Multiple worker run merge check task.
class MergeCheckWorkers {
 public:
  MergeCheckWorkers() : offset_(0) { bthread_mutex_init(&mutex_, nullptr); }
  ~MergeCheckWorkers() { bthread_mutex_destroy(&mutex_); }

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

// Check region whether need to merge.
class MergeCheckTask : public TaskRunnable {
 public:
  MergeCheckTask(std::shared_ptr<MergeCheckWorkers> merge_check_workers, store::RegionPtr merge_from_region,
                 store::RegionPtr merge_to_region)
      : merge_check_workers_(merge_check_workers),
        merge_from_region_(merge_from_region),
        merge_to_region_(merge_to_region) {}
  ~MergeCheckTask() override = default;

  std::string Type() override { return "MERGE_CHECK"; }

  void Run() override {
    MergeCheck();
    merge_check_workers_->DeleteRegionChecking(merge_from_region_->Id());
    merge_check_workers_->DeleteRegionChecking(merge_to_region_->Id());
  }

 private:
  void MergeCheck();

  std::shared_ptr<MergeCheckWorkers> merge_check_workers_;
  store::RegionPtr merge_from_region_;
  store::RegionPtr merge_to_region_;
};

// merge check, if region approximate size exceed threshold size, then check region actual size and keys count.
class PreMergeCheckTask : public TaskRunnable {
 public:
  PreMergeCheckTask(std::shared_ptr<MergeCheckWorkers> merge_check_workers)
      : merge_check_workers_(merge_check_workers) {}
  ~PreMergeCheckTask() override = default;

  std::string Type() override { return "PRE_MERGE_CHECK"; }

  void Run() override { PreMergeCheck(); }

 private:
  void PreMergeCheck();
  bool NeedMerge(store::RegionPtr from_region, store::RegionMetricsPtr from_region_metrics, store::RegionPtr to_region,
                 store::RegionMetricsPtr to_region_metrics, std::string& reason);
  butil::Status UpdateActualSizeAndCount(store::RegionPtr region, store::RegionMetricsPtr region_metrics,
                                         int64_t& region_size, int64_t& reigon_keys_count);

  std::shared_ptr<MergeCheckWorkers> merge_check_workers_;
};

// Pre roughly check all region whether need merge.
class PreMergeChecker {
 public:
  PreMergeChecker() {
    worker_ = Worker::New();
    merge_check_workers_ = std::make_shared<MergeCheckWorkers>();
  }
  ~PreMergeChecker() = default;

  bool Init(int num);
  void Destroy();

  // Trigger pre merge check for merge region.
  static void TriggerPreMergeCheck(void*);

  std::shared_ptr<MergeCheckWorkers> GetMergeCheckWorkers() { return merge_check_workers_; }

 private:
  bool Execute(TaskRunnablePtr task);
  // For pre merge check.
  WorkerPtr worker_;
  // For merge check.
  std::shared_ptr<MergeCheckWorkers> merge_check_workers_;
};

}  // namespace dingodb

#endif
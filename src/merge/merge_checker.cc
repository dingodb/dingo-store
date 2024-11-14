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

#include "merge/merge_checker.h"

#include <sys/stat.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

#include "common/constant.h"
#include "common/helper.h"
#include "common/runnable.h"
#include "config/config_helper.h"
#include "engine/iterator.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
#include "server/service_helper.h"
#include "vector/vector_index_manager.h"

namespace dingodb {

DECLARE_bool(region_enable_auto_merge);

static std::atomic<bool> g_sequential_scan(true);

bool MergeCheckWorkers::Init(uint32_t num) {
  for (int i = 0; i < num; ++i) {
    auto worker = Worker::New();
    if (!worker->Init()) {
      return false;
    }
    workers_.push_back(worker);
  }

  return true;
}

void MergeCheckWorkers::Destroy() {
  for (auto& worker : workers_) {
    worker->Destroy();
  }
}

bool MergeCheckWorkers::Execute(TaskRunnablePtr task) {
  if (!workers_[offset_]->Execute(task)) {
    return false;
  }
  offset_ = (offset_ + 1) % workers_.size();

  return true;
}

bool MergeCheckWorkers::IsExistRegionChecking(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return checking_regions_.find(region_id) != checking_regions_.end();
}

void MergeCheckWorkers::AddRegionChecking(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (checking_regions_.find(region_id) == checking_regions_.end()) {
    checking_regions_.insert(region_id);
  }
}

void MergeCheckWorkers::DeleteRegionChecking(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  checking_regions_.erase(region_id);
}

static bool ValidRegion(const store::RegionPtr& region, std::string& reason) {
  bool need_scan_check = true;
  do {
    if (region->State() != pb::common::NORMAL) {
      need_scan_check = false;
      reason = "region state is not normal";
      break;
    }
    if (region->DisableChange()) {
      need_scan_check = false;
      reason = "region is disable merge";
      break;
    }
    if (region->TemporaryDisableChange()) {
      need_scan_check = false;
      reason = "region is temporary disable change";
      break;
    }
    int runing_num = VectorIndexManager::GetVectorIndexTaskRunningNum();
    if (runing_num > Constant::kVectorIndexTaskRunningNumExpectValue) {
      need_scan_check = false;
      reason = fmt::format("runing vector index task num({}) too many, exceed expect num({})", runing_num,
                           Constant::kVectorIndexTaskRunningNumExpectValue);
      break;
    }
  } while (false);
  return need_scan_check;
}

static bool CheckLeaderAndFollowerStatus(int64_t region_id) {
  auto region = Server::GetInstance().GetRegion(region_id);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[merge.check][region({})] Not found region.", region_id);
    return false;
  }
  if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
    return true;
  } else if (region->GetStoreEngineType() == pb::common::STORE_ENG_MEMORY) {
    DINGO_LOG(ERROR) << fmt::format("[merge.check][region({})] Not support memory engine.", region_id);
    return false;
  }

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[merge.check][region({})] get engine failed.", region_id);
    return false;
  }

  auto node = raft_store_engine->GetNode(region_id);
  if (node == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[merge.check][region({})] get raft node failed.", region_id);
    return false;
  }

  if (!node->IsLeader()) {
    return false;
  }

  auto status = Helper::ValidateRaftStatusForSplitMerge(node->GetStatus());
  if (!status.ok()) {
    DINGO_LOG(INFO) << fmt::format("[merge.check][region({})] {}", region_id, status.error_str());
    return false;
  }

  return true;
}

void MergeCheckTask::MergeCheck() {
  if (merge_from_region_ == nullptr || merge_to_region_ == nullptr) {
    return;
  }

  int64_t start_time = Helper::TimestampMs();
  auto epoch = merge_from_region_->Epoch();
  auto plain_range = merge_from_region_->Range(false);
  auto encode_range = mvcc::Codec::EncodeRange(plain_range);

  std::vector<std::string> raw_cf_names;
  std::vector<std::string> txn_cf_names;
  Helper::GetColumnFamilyNames(encode_range.start_key(), raw_cf_names, txn_cf_names);

  std::vector<std::string> cf_names = txn_cf_names.empty() ? raw_cf_names : txn_cf_names;

  DINGO_LOG(INFO) << fmt::format("[merge.check][region({})] Will check merge for raw_range{} cf_names({})",
                                 merge_from_region_->Id(), Helper::RangeToString(plain_range),
                                 Helper::VectorToString(cf_names));

  bool need_merge = true;
  std::string reason;
  do {
    need_merge = ValidRegion(merge_from_region_, reason);
    if (!need_merge) {
      break;
    }
    if (CheckLeaderAndFollowerStatus(merge_from_region_->Id())) {
      need_merge = false;
      reason = "not leader or follower abnormal";
      break;
    }
    // validate region store engine
    if (merge_from_region_->Definition().store_engine() != merge_to_region_->Definition().store_engine()) {
      need_merge = false;
      reason = "store_engine is different";
      break;
    }
    // validate region raw engine
    if (merge_from_region_->Definition().raw_engine() != merge_to_region_->Definition().raw_engine()) {
      need_merge = false;
      reason = "raw_engine is different";
      break;
    }
    // validate region part id
    if (merge_from_region_->Definition().part_id() != merge_to_region_->Definition().part_id()) {
      need_merge = false;
      reason = "region partition is different";
      break;
    }
    // validate region peers
    if (Helper::IsDifferencePeers(merge_from_region_->Definition(), merge_to_region_->Definition())) {
      need_merge = false;
      reason = "region peers is differencce";
      break;
    }
  } while (false);

  DINGO_LOG(INFO) << fmt::format(
      "[merge.check][region({})] merge check result({}) reason({}) "
      " elapsed time({}ms)",
      merge_from_region_->Id(), need_merge, reason, Helper::TimestampMs() - start_time);
  if (!need_merge) {
    return;
  }

  // Invoke coordinator SplitRegion api.
  auto coordinator_interaction = Server::GetInstance().GetCoordinatorInteraction();
  pb::coordinator::MergeRegionRequest request;
  pb::coordinator::MergeRegionResponse response;
  request.mutable_merge_request()->set_target_region_id(merge_to_region_->Id());
  request.mutable_merge_request()->set_source_region_id(merge_from_region_->Id());

  auto status = coordinator_interaction->SendRequest("MergeRegion", request, response);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[merge.check][region({})] send MergeRegion failed, error: {}",
                                      merge_from_region_->Id(), Helper::PrintStatus(status));
    return;
  }
}

bool PreMergeCheckTask::NeedMerge(store::RegionPtr from_region, store::RegionMetricsPtr from_region_metrics,
                                  store::RegionPtr to_region, store::RegionMetricsPtr to_region_metrics,
                                  std::string& reason) {
  int64_t merge_check_size = ConfigHelper::GetMergeCheckSize();
  int64_t merge_check_keys_count = ConfigHelper::GetMergeCheckKeysCount();
  int64_t start_time = Helper::TimestampMs();
  int64_t from_region_size = 0;
  int64_t from_region_keys_count = 0;
  int64_t to_region_size = 0;
  int64_t to_region_keys_count = 0;
  if (from_region_metrics->LastUpdateMetricsTimestamp() + Constant::kRegionMetricsUpdateSecondDefaultValue * 1000 <
      Helper::TimestampMs()) {
    auto status = UpdateActualSizeAndCount(from_region, from_region_metrics, from_region_size, from_region_keys_count);
    if (!status.ok()) {
      reason =
          fmt::format("update from_region({}) actual size and count err({})", from_region->Id(), status.error_cstr());
      DINGO_LOG(ERROR) << fmt::format(
          "[merge.check][region({})] update from_region actual size and count failed, err:{})", from_region->Id(),
          status.error_cstr());
      return false;
    }
  } else {
    from_region_size = from_region_metrics->RegionSize();
    from_region_keys_count = from_region_metrics->KeyCount();
  }
  if (from_region_size > merge_check_size || from_region_keys_count > merge_check_keys_count) {
    // no need to merge
    reason = "region size or keys count are too big to merge";
    return false;
  }

  if (to_region_metrics->LastUpdateMetricsTimestamp() + Constant::kRegionMetricsUpdateSecondDefaultValue * 1000 <
      Helper::TimestampMs()) {
    auto status = UpdateActualSizeAndCount(to_region, to_region_metrics, to_region_size, to_region_keys_count);
    if (!status.ok()) {
      reason = fmt::format("update to_region({}) actual size and count err({})", to_region->Id(), status.error_cstr());
      DINGO_LOG(ERROR) << fmt::format(
          "[merge.check][region({})] update to_region actual size and count failed, err:{})", to_region->Id(),
          status.error_cstr());
      return false;
    }
  } else {
    to_region_size = to_region_metrics->RegionSize();
    to_region_keys_count = to_region_metrics->KeyCount();
  }

  int64_t split_threshold_size = ConfigHelper::GetRegionMaxSize();
  float merge_size_ratio = ConfigHelper::GetMergeSizeRatio();
  if (from_region_size + to_region_size > split_threshold_size * merge_size_ratio) {
    reason = "split size after merge";
    DINGO_LOG(INFO) << fmt::format(
        "[merge.check][region({})] from_merge_size({}) plus to_merge_size({}) greater than split_threshold_size({}) * "
        "merge_size_ratio({})",
        from_region->Id(), from_region_size, to_region_size, merge_check_size, split_threshold_size, merge_size_ratio);
    return false;
  }

  uint32_t split_key_number = ConfigHelper::GetSplitKeysNumber();
  float merge_keys_ratio = ConfigHelper::GetMergeKeysRatio();
  if (from_region_keys_count + to_region_keys_count > split_key_number * merge_keys_ratio) {
    reason = "split keys after merge";
    DINGO_LOG(INFO) << fmt::format(
        "[merge.check][region({})] from_region_keys_count({}) plus to_region_keys_count({}) greater than "
        "split_key_number({}) * merge_keys_ratio({})",
        from_region->Id(), from_region_keys_count, to_region_keys_count, merge_check_size, split_key_number,
        merge_keys_ratio);
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("[merge.check][region({})] can auto merge ", from_region->Id());
  return true;
}

// base physics key, contain key of multi version.
butil::Status PreMergeCheckTask::UpdateActualSizeAndCount(store::RegionPtr region,
                                                          store::RegionMetricsPtr region_metrics, int64_t& region_size,
                                                          int64_t& reigon_keys_count) {
  int64_t start_time = Helper::TimestampMs();
  auto plain_range = region->Range(false);
  auto encode_range = mvcc::Codec::EncodeRange(plain_range);

  std::vector<std::string> raw_cf_names;
  std::vector<std::string> txn_cf_names;
  Helper::GetColumnFamilyNames(encode_range.start_key(), raw_cf_names, txn_cf_names);

  std::vector<std::string> cf_names = txn_cf_names.empty() ? raw_cf_names : txn_cf_names;

  DINGO_LOG(INFO) << fmt::format("[merge.check][region({})] Will check auto merge for raw_range{} cf_names({})",
                                 region->Id(), Helper::RangeToString(plain_range), Helper::VectorToString(cf_names));

  auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
  if (raw_engine == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[merge.check][region({})] get raw_engine failed, skip this region.", region->Id());
    return butil::Status(pb::error::EREGION_UNAVAILABLE, "get raw_engine failed");
  }
  MergedIterator iter(raw_engine, cf_names, encode_range.end_key());
  iter.Seek(encode_range.start_key());

  int64_t size = 0;
  int64_t count = 0;
  for (; iter.Valid(); iter.Next()) {
    size += iter.KeyValueSize();
    count++;
  }
  region_size = size;
  reigon_keys_count = count;
  // Update region key count and size metrics.
  if (region_metrics != nullptr) {
    if (count > 0) {
      region_metrics->SetKeyCount(count);
      region_metrics->SetNeedUpdateKeyCount(false);
    }
    if (size > 0) {
      region_metrics->SetRegionSize(size);
    }
  }

  DINGO_LOG(INFO) << fmt::format("[merge.check][region({})] update actual size({}), count({}) elapsed time({}ms)",
                                 region->Id(), region_size, count, Helper::TimestampMs() - start_time);
  return butil::Status::OK();
}

void PreMergeCheckTask::PreMergeCheck() {
  // if system capacity is very low, suspend all merge check to avoid merge region.
  auto ret = ServiceHelper::ValidateClusterReadOnly();
  if (!ret.ok()) {
    DINGO_LOG(INFO) << fmt::format("[merge.check] cluster is read-only, suspend all merge check, error: {} {}",
                                   pb::error::Errno_Name(ret.error_code()), ret.error_str());
    return;
  }
  auto metrics = Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics();
  auto regions = GET_STORE_REGION_META->GetAllAliveRegion();
  if (regions.size() < 2) {
    return;
  }
  bool sequence = g_sequential_scan.load(std::memory_order_relaxed);
  if (sequence) {
    std::sort(regions.begin(), regions.end(), [](const store::RegionPtr& a, const store::RegionPtr& b) {
      return a->Definition().range().start_key() < b->Definition().range().start_key();
    });
  } else {
    std::sort(regions.begin(), regions.end(), [](const store::RegionPtr& a, const store::RegionPtr& b) {
      return a->Definition().range().start_key() > b->Definition().range().start_key();
    });
  }

  int64_t interval = ConfigHelper::GetSplitMergeInterval();
  // Region of doing check.
  for (int i = 0; i < regions.size() - 2; i++) {
    auto merge_from = regions[i];
    auto merge_to = regions[i + 1];
    int64_t merge_check_size = ConfigHelper::GetMergeCheckSize();
    int64_t merge_check_keys_count = ConfigHelper::GetMergeCheckKeysCount();

    auto region_metric = metrics->GetMetrics(merge_from->Id());
    auto to_region_metric = metrics->GetMetrics(merge_to->Id());
    bool need_scan_check = true;
    std::string reason;
    do {
      if (region_metric == nullptr || to_region_metric == nullptr) {
        need_scan_check = false;
        reason = "region metric is nullptr";
        break;
      }
      if (merge_check_workers_ == nullptr) {
        need_scan_check = false;
        reason = "merge check worker is nullptr";
        break;
      }
      need_scan_check = ValidRegion(merge_from, reason);
      if (!need_scan_check) {
        break;
      }
      if (merge_check_workers_->IsExistRegionChecking(merge_from->Id())) {
        need_scan_check = false;
        reason = "region already exist merge check";
        break;
      }
      if (region_metric->RegionSize() > merge_check_size) {
        need_scan_check = false;
        reason = "region approximate size too big";
        break;
      }
      if (region_metric->KeyCount() > merge_check_keys_count) {
        need_scan_check = false;
        reason = "region approximate keys count too big";
        break;
      }
      if (CheckLeaderAndFollowerStatus(merge_from->Id())) {
        need_scan_check = false;
        reason = "not leader or follower abnormal";
        break;
      }
      if (merge_from->LastSplitTimestamp() != 0 &&
          merge_from->LastSplitTimestamp() + interval * 1000 > Helper::TimestampMs()) {
        need_scan_check = false;
        reason = "region recently has split";
        break;
      }
      if (merge_to->LastSplitTimestamp() != 0 &&
          merge_to->LastSplitTimestamp() + interval * 1000 > Helper::TimestampMs()) {
        need_scan_check = false;
        reason = "region recently has split";
        break;
      }
      // validate region part id
      if (merge_from->Definition().part_id() != merge_to->Definition().part_id()) {
        need_scan_check = false;
        reason = "region partition is different";
        break;
      }
      if (sequence && merge_from->Definition().range().end_key() != merge_to->Definition().range().start_key()) {
        need_scan_check = false;
        reason = "region range has not continuous";
        break;
      }

      if (!sequence && merge_from->Definition().range().start_key() != merge_to->Definition().range().end_key()) {
        need_scan_check = false;
        reason = "region range has not continuous";
        break;
      }
      // validate region store engine
      if (merge_from->Definition().store_engine() != merge_to->Definition().store_engine()) {
        need_scan_check = false;
        reason = "store_engine is different";
        break;
      }
      // validate region raw engine
      if (merge_from->Definition().raw_engine() != merge_to->Definition().raw_engine()) {
        need_scan_check = false;
        reason = "raw_engine is different";
        break;
      }
      // validate region peers
      if (Helper::IsDifferencePeers(merge_from->Definition(), merge_to->Definition())) {
        need_scan_check = false;
        reason = "region peers is differencce";
        break;
      }
      if (!NeedMerge(merge_from, region_metric, merge_to, to_region_metric, reason)) {
        need_scan_check = false;
        break;
      }

    } while (false);
    DINGO_LOG(INFO) << fmt::format(
        "[merge.check][region({})] premerge check result({}) reason({}) split_after_merge_interval({})",
        merge_from->Id(), need_scan_check, reason, interval);
    if (!need_scan_check) {
      continue;
    }
    ++i;
    merge_check_workers_->AddRegionChecking(merge_from->Id());
    merge_check_workers_->AddRegionChecking(merge_to->Id());
    auto task = std::make_shared<MergeCheckTask>(merge_check_workers_, merge_from, merge_to);
    if (!merge_check_workers_->Execute(task)) {
      merge_check_workers_->DeleteRegionChecking(merge_from->Id());
      merge_check_workers_->DeleteRegionChecking(merge_to->Id());
    };
  }
}

bool PreMergeChecker::Init(int num) {
  if (!worker_->Init()) {
    return false;
  }

  if (!merge_check_workers_->Init(num)) {
    return false;
  }

  return true;
}

void PreMergeChecker::Destroy() {
  worker_->Destroy();
  merge_check_workers_->Destroy();
}

bool PreMergeChecker::Execute(TaskRunnablePtr task) { return worker_->Execute(task); }

void PreMergeChecker::TriggerPreMergeCheck(void*) {
  if (!FLAGS_region_enable_auto_merge) {
    DINGO_LOG(INFO) << "disable auto merge";
    return;
  }
  // Free at ExecuteRoutine()
  auto task = std::make_shared<PreMergeCheckTask>(Server::GetInstance().GetPreMergeChecker()->GetMergeCheckWorkers());
  Server::GetInstance().GetPreMergeChecker()->Execute(task);
  g_sequential_scan.store(!g_sequential_scan.load(std::memory_order_relaxed));
}

}  // namespace dingodb
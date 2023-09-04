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

#include "split/split_checker.h"

#include <sys/stat.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constant.h"
#include "common/failpoint.h"
#include "common/helper.h"
#include "config/config_helper.h"
#include "engine/iterator.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_index_manager.h"

namespace dingodb {

std::string HalfSplitChecker::SplitKey(store::RegionPtr region, uint32_t& count) {
  auto iter = raw_engine_->NewMultipleRangeIterator(raw_engine_, Constant::kStoreDataCF, region->PhysicsRange());
  iter->Init();

  uint64_t size = 0;
  uint64_t chunk_size = 0;
  std::vector<std::string> keys;
  bool is_split = false;
  for (; iter->IsValid(); iter->Next()) {
    uint64_t key_value_size = iter->KeyValueSize();
    size += key_value_size;
    chunk_size += key_value_size;
    if (chunk_size >= split_chunk_size_) {
      chunk_size = 0;
      keys.push_back(iter->FirstRangeKey());
    }
    if (size >= split_threshold_size_) {
      is_split = true;
    }

    ++count;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[split.check][region({})] policy(HALF) split_threshold_size({}) split_chunk_size({}) actual_size({}) count({})",
      region->Id(), split_threshold_size_, split_chunk_size_, size, count);

  int mid = keys.size() / 2;
  return !is_split || keys.empty() ? "" : keys[mid];
}

std::string SizeSplitChecker::SplitKey(store::RegionPtr region, uint32_t& count) {
  auto iter = raw_engine_->NewMultipleRangeIterator(raw_engine_, Constant::kStoreDataCF, region->PhysicsRange());
  iter->Init();

  uint64_t size = 0;
  std::string split_key;
  bool is_split = false;
  uint32_t split_pos = split_size_ * split_ratio_;
  for (; iter->IsValid(); iter->Next()) {
    size += iter->KeyValueSize();
    if (split_key.empty() && size >= split_pos) {
      split_key = iter->FirstRangeKey();
    } else if (size >= split_size_) {
      is_split = true;
    }

    ++count;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[split.check][region({})] policy(SIZE) split_size({}) split_ratio({}) actual_size({}) count({})", region->Id(),
      split_size_, split_ratio_, size, count);

  return is_split ? split_key : "";
}

std::string KeysSplitChecker::SplitKey(store::RegionPtr region, uint32_t& count) {
  auto iter = raw_engine_->NewMultipleRangeIterator(raw_engine_, Constant::kStoreDataCF, region->PhysicsRange());
  iter->Init();

  uint64_t size = 0;
  uint64_t split_key_count = 0;
  std::string split_key;
  bool is_split = false;
  uint32_t split_key_number = split_keys_number_ * split_keys_ratio_;
  for (; iter->IsValid(); iter->Next()) {
    ++split_key_count;
    size += iter->KeyValueSize();

    if (split_key.empty() && split_key_count >= split_key_number) {
      split_key = iter->FirstRangeKey();
    } else if (split_key_count == split_keys_number_) {
      is_split = true;
    }

    ++count;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[split.check][region({})] policy(KEYS) split_key_number({}) split_key_ratio({}) actual_size({}) count({})",
      region->Id(), split_keys_number_, split_keys_ratio_, size, count);

  return is_split ? split_key : "";
}

static bool CheckLeaderAndFollowerStatus(uint64_t region_id) {
  auto raft_store_engine = Server::GetInstance()->GetRaftStoreEngine();
  if (raft_store_engine == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[split.check][region({})] get engine failed.", region_id);
    return false;
  }

  auto node = raft_store_engine->GetNode(region_id);
  if (node == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[split.check][region({})] get raft node failed.", region_id);
    return false;
  }

  if (!node->IsLeader()) {
    return false;
  }

  auto status = Helper::ValidateRaftStatusForSplit(node->GetStatus());
  if (!status.ok()) {
    DINGO_LOG(INFO) << fmt::format("[split.check][region({})] {}", region_id, status.error_str());
    return false;
  }

  return true;
}

void SplitCheckTask::SplitCheck() {
  if (region_ == nullptr) {
    return;
  }
  if (split_checker_ == nullptr) {
    return;
  }

  uint64_t start_time = Helper::TimestampMs();
  auto epoch = region_->Epoch();

  // Get split key.
  uint32_t key_count = 0;
  std::string split_key = split_checker_->SplitKey(region_, key_count);

  // Update region key count metrics.
  if (region_metrics_ != nullptr && key_count > 0) {
    region_metrics_->SetKeyCount(key_count);
    region_metrics_->SetNeedUpdateKeyCount(false);
  }

  bool need_split = true;
  std::string reason;
  do {
    if (split_key.empty()) {
      reason = "split key is empty";
      need_split = false;
      break;
    }
    if (region_->Type() == pb::common::INDEX_REGION) {
      split_key = VectorCodec::RemoveVectorPrefix(split_key);
    }
    if (region_->Epoch().version() != epoch.version()) {
      reason = "region version change";
      need_split = false;
      break;
    }
    if (!region_->CheckKeyInRange(split_key)) {
      reason = fmt::format("invalid split key, not in region range {}", region_->RangeToString());
      need_split = false;
      break;
    }
    if (region_->DisableSplit()) {
      reason = "region disable split";
      need_split = false;
      break;
    }
    if (region_->TemporaryDisableSplit()) {
      reason = "region temporary disable split";
      need_split = false;
      break;
    }
    if (region_->State() != pb::common::NORMAL) {
      reason = fmt::format("region state is {}, not normal", pb::common::StoreRegionState_Name(region_->State()));
      need_split = false;
      break;
    }
    if (!CheckLeaderAndFollowerStatus(region_->Id())) {
      reason = "not leader or follower abnormal";
      need_split = false;
      break;
    }
  } while (false);

  DINGO_LOG(INFO) << fmt::format(
      "[split.check][region({})] split check result({}) reason({}) split_policy({}) split_key({}) elapsed time({}ms)",
      region_->Id(), need_split, reason, split_checker_->GetPolicyName(), Helper::StringToHex(split_key),
      Helper::TimestampMs() - start_time);
  if (!need_split) {
    return;
  }

  // Invoke coordinator SplitRegion api.
  auto coordinator_interaction = Server::GetInstance()->GetCoordinatorInteraction();
  pb::coordinator::SplitRegionRequest request;
  request.mutable_split_request()->set_split_from_region_id(region_->Id());
  request.mutable_split_request()->set_split_watershed_key(split_key);
  request.mutable_split_request()->set_store_create_region(ConfigHelper::GetSplitStrategy() ==
                                                           pb::raft::POST_CREATE_REGION);
  pb::coordinator::SplitRegionResponse response;
  auto status = coordinator_interaction->SendRequest("SplitRegion", request, response);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[split.check][region({})] send SplitRegion failed, error: {} {}", region_->Id(),
                                      pb::error::Errno_Name(status.error_code()), status.error_str());
    return;
  }
}

bool SplitCheckWorkers::Init(uint32_t num) {
  for (int i = 0; i < num; ++i) {
    auto worker = std::make_shared<Worker>();
    if (!worker->Init()) {
      return false;
    }
    workers_.push_back(worker);
  }

  return true;
}

void SplitCheckWorkers::Destroy() {
  for (auto& worker : workers_) {
    worker->Destroy();
  }
}

bool SplitCheckWorkers::Execute(TaskRunnable* task) {
  if (!workers_[offset_]->Execute(task)) {
    return false;
  }
  offset_ = (offset_ + 1) % workers_.size();

  return true;
}

bool SplitCheckWorkers::IsExistRegionChecking(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return checking_regions_.find(region_id) != checking_regions_.end();
}

void SplitCheckWorkers::AddRegionChecking(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (checking_regions_.find(region_id) == checking_regions_.end()) {
    checking_regions_.insert(region_id);
  }
}

void SplitCheckWorkers::DeleteRegionChecking(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  checking_regions_.erase(region_id);
}

static std::shared_ptr<SplitChecker> BuildSplitChecker(std::shared_ptr<RawEngine> raw_engine) {
  std::string policy = ConfigHelper::GetSplitPolicy();
  if (policy == "HALF") {
    uint32_t split_threshold_size = ConfigHelper::GetRegionMaxSize();
    uint32_t split_chunk_size = ConfigHelper::GetSplitChunkSize();
    return std::make_shared<HalfSplitChecker>(raw_engine, split_threshold_size, split_chunk_size);

  } else if (policy == "SIZE") {
    uint32_t split_threshold_size = ConfigHelper::GetRegionMaxSize();
    float split_ratio = ConfigHelper::GetSplitSizeRatio();
    return std::make_shared<SizeSplitChecker>(raw_engine, split_threshold_size, split_ratio);

  } else if (policy == "KEYS") {
    uint32_t split_key_number = ConfigHelper::GetSplitKeysNumber();
    float split_keys_ratio = ConfigHelper::GetSplitKeysRatio();
    return std::make_shared<KeysSplitChecker>(raw_engine, split_key_number, split_keys_ratio);
  }

  DINGO_LOG(ERROR) << fmt::format("[split.check] build split checker failed, policy {}", policy);

  return nullptr;
}

void PreSplitCheckTask::PreSplitCheck() {
  auto raw_engine = Server::GetInstance()->GetRawEngine();
  auto metrics = Server::GetInstance()->GetStoreMetricsManager()->GetStoreRegionMetrics();
  auto regions = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetAllAliveRegion();
  uint32_t split_check_approximate_size = ConfigHelper::GetSplitCheckApproximateSize();
  for (auto& region : regions) {
    auto region_metric = metrics->GetMetrics(region->Id());
    bool need_scan_check = true;
    std::string reason;
    do {
      if (region_metric == nullptr) {
        need_scan_check = false;
        reason = "region metric is nullptr";
        break;
      }
      if (split_check_workers_ == nullptr) {
        need_scan_check = false;
        reason = "split check worker is nullptr";
        break;
      }
      if (region->State() != pb::common::NORMAL) {
        need_scan_check = false;
        reason = "region state is not normal";
        break;
      }
      if (region->DisableSplit()) {
        need_scan_check = false;
        reason = "region is disable split";
        break;
      }
      if (region->TemporaryDisableSplit()) {
        need_scan_check = false;
        reason = "region is temporary disable split";
        break;
      }
      if (split_check_workers_->IsExistRegionChecking(region->Id())) {
        need_scan_check = false;
        reason = "region already exist split check";
        break;
      }
      if (!CheckLeaderAndFollowerStatus(region->Id())) {
        need_scan_check = false;
        reason = "not leader or follower abnormal";
        break;
      }
      if (region_metric->InnerRegionMetrics().region_size() < split_check_approximate_size) {
        need_scan_check = false;
        reason = "region approximate size too small";
        break;
      }
    } while (false);

    DINGO_LOG(INFO) << fmt::format(
        "[split.check][region({})] pre split check result({}) reason({}) approximate size({}/{})", region->Id(),
        need_scan_check, reason, region_metric == nullptr ? 0 : region_metric->InnerRegionMetrics().region_size(),
        split_check_approximate_size);
    if (!need_scan_check) {
      continue;
    }

    auto split_checker = BuildSplitChecker(raw_engine);
    if (split_checker == nullptr) {
      continue;
    }

    if (split_check_workers_->Execute(new SplitCheckTask(split_check_workers_, region, region_metric, split_checker))) {
      split_check_workers_->AddRegionChecking(region->Id());
    }
  }
}

bool PreSplitChecker::Init(int num) {
  if (!worker_->Init()) {
    return false;
  }

  if (!split_check_workers_->Init(num)) {
    return false;
  }

  return true;
}

void PreSplitChecker::Destroy() {
  worker_->Destroy();
  split_check_workers_->Destroy();
}

bool PreSplitChecker::Execute(TaskRunnable* task) { return worker_->Execute(task); }

void PreSplitChecker::TriggerPreSplitCheck(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new PreSplitCheckTask(Server::GetInstance()->GetPreSplitChecker()->GetSplitCheckWorkers());
  if (!Server::GetInstance()->GetPreSplitChecker()->Execute(task)) {
    delete task;
  }
}

}  // namespace dingodb
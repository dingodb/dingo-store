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

#include "config/config_manager.h"
#include "engine/iterator.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "server/server.h"
#include "vector/vector_index_manager.h"

namespace dingodb {

std::string HalfSplitChecker::SplitKey(store::RegionPtr region, uint32_t& count) {
  IteratorOptions options;
  options.upper_bound = region->RawRange().end_key();
  auto iter = raw_engine_->NewIterator(Constant::kStoreDataCF, options);

  uint64_t chunk_size = 0;
  uint64_t size = 0;
  std::vector<std::string> keys;
  bool is_split = false;
  for (iter->Seek(region->RawRange().start_key()); iter->Valid(); iter->Next()) {
    ++count;
    chunk_size += iter->Key().size() + iter->Value().size();
    size += iter->Key().size() + iter->Value().size();

    if (chunk_size >= split_chunk_size_) {
      chunk_size = 0;
      keys.push_back(std::string(iter->Key()));
    }
    if (size >= split_threshold_size_) {
      is_split = true;
    }
  }

  int mid = keys.size() / 2;
  return !is_split || keys.empty() ? "" : keys[mid];
}

std::string SizeSplitChecker::SplitKey(store::RegionPtr region, uint32_t& count) {
  uint32_t split_pos = split_size_ * split_ratio_;
  IteratorOptions options;
  options.upper_bound = region->RawRange().end_key();
  auto iter = raw_engine_->NewIterator(Constant::kStoreDataCF, options);

  uint64_t size = 0;
  std::string split_key;
  bool is_split = false;
  for (iter->Seek(region->RawRange().start_key()); iter->Valid(); iter->Next()) {
    ++count;
    size += iter->Key().size() + iter->Value().size();

    if (split_key.empty() && size >= split_pos) {
      split_key = iter->Key();
    } else if (size >= split_size_) {
      is_split = true;
    }
  }

  return is_split ? split_key : "";
}

std::string KeysSplitChecker::SplitKey(store::RegionPtr region, uint32_t& count) {
  uint32_t split_key_number = split_key_number_ * split_key_ratio_;

  IteratorOptions options;
  options.upper_bound = region->RawRange().end_key();
  auto iter = raw_engine_->NewIterator(Constant::kStoreDataCF, options);

  uint64_t split_key_count = 0;
  std::string split_key;
  bool is_split = false;
  for (iter->Seek(region->RawRange().start_key()); iter->Valid(); iter->Next()) {
    ++count;
    ++split_key_count;

    if (split_key.empty() && split_key_count >= split_key_number) {
      split_key = iter->Key();
    } else if (split_key_count == split_key_number_) {
      is_split = true;
    }
  }

  return is_split ? split_key : "";
}

void SplitCheckTask::SplitCheck() {
  if (region_ == nullptr) {
    return;
  }
  if (split_checker_ == nullptr) {
    return;
  }

  // Get split key.
  uint32_t key_count = 0;
  std::string split_key = split_checker_->SplitKey(region_, key_count);

  // Update region key count metrics.
  if (region_metrics_ != nullptr && key_count > 0) {
    region_metrics_->SetKeyCount(key_count);
    region_metrics_->SetNeedUpdateKeyCount(false);
  }

  if (split_key.empty()) {
    return;
  }
  if (region_->DisableSplit()) {
    return;
  }
  if (region_->State() != pb::common::NORMAL) {
    DINGO_LOG(WARNING) << "Region state it not NORMAL, not launch split.";
    return;
  }

  DINGO_LOG(INFO) << fmt::format("Need split region {} split_key {}", region_->Id(), split_key);

  // Invoke coordinator SplitRegion api.
  auto coordinator_interaction = Server::GetInstance()->GetCoordinatorInteraction();
  pb::coordinator::SplitRegionRequest request;
  request.mutable_split_request()->set_split_from_region_id(region_->Id());
  request.mutable_split_request()->set_split_watershed_key(split_key);
  pb::coordinator::SplitRegionResponse response;
  auto status = coordinator_interaction->SendRequest("SplitRegion", request, response);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("Send SplitRegion failed, region {} error: {} {}", region_->Id(),
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

static std::shared_ptr<SplitChecker> BuildSplitChecker(std::shared_ptr<dingodb::Config> config /*NOLINT*/,
                                                       std::shared_ptr<RawEngine> raw_engine /*NOLINT*/) {
  std::string policy = config->GetString("region.split_policy");
  if (policy == "HALF") {
    uint32_t split_threshold_size = config->GetInt("region.split_threshold_size");
    uint32_t split_chunk_size = config->GetInt("region.split_chunk_size");
    return std::make_shared<HalfSplitChecker>(raw_engine, split_threshold_size, split_chunk_size);

  } else if (policy == "SIZE") {
    uint32_t split_threshold_size = config->GetInt("region.split_threshold_size");
    float split_ratio = static_cast<float>(config->GetDouble("region.split_ratio"));
    return std::make_shared<SizeSplitChecker>(raw_engine, split_threshold_size, split_ratio);

  } else if (policy == "KEYS") {
    uint32_t split_key_number = config->GetInt("region.split_key_number");
    float split_key_ratio = static_cast<float>(config->GetDouble("region.split_key_ratio"));
    return std::make_shared<KeysSplitChecker>(raw_engine, split_key_number, split_key_ratio);
  }

  DINGO_LOG(ERROR) << fmt::format("Build split checker failed, policy {}", policy);

  return nullptr;
}

bool IsLeader(std::shared_ptr<dingodb::Engine> engine, uint64_t region_id) {  // NOLINT
  if (engine->GetID() == pb::common::ENG_RAFT_STORE) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(engine);
    auto node = raft_kv_engine->GetNode(region_id);
    if (node == nullptr) {
      return false;
    }

    if (!node->IsLeader()) {
      return false;
    }
  }

  return true;
}

void PreSplitCheckTask::PreSplitCheck() {
  auto config = Server::GetInstance()->GetConfig();
  auto engine = Server::GetInstance()->GetEngine();
  auto raw_engine = Server::GetInstance()->GetRawEngine();
  auto metrics = Server::GetInstance()->GetStoreMetricsManager()->GetStoreRegionMetrics();
  auto regions = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetAllAliveRegion();
  uint32_t split_check_approximate_size = config->GetInt("region.split_check_approximate_size");
  for (auto& region : regions) {
    auto region_metric = metrics->GetMetrics(region->Id());
    if (region_metric == nullptr) {
      continue;
    }
    if (split_check_workers_ == nullptr) {
      continue;
    }
    if (region->State() != pb::common::NORMAL) {
      continue;
    }
    if (region->DisableSplit()) {
      continue;
    }
    if (split_check_workers_->IsExistRegionChecking(region->Id())) {
      continue;
    }
    if (!IsLeader(engine, region->Id())) {
      continue;
    }

    DINGO_LOG(INFO) << fmt::format("pre split check region {} approximate size {} threshold size {}", region->Id(),
                                   region_metric->InnerRegionMetrics().region_size(), split_check_approximate_size);
    if (region_metric->InnerRegionMetrics().region_size() < split_check_approximate_size) {
      continue;
    }

    auto split_checker = BuildSplitChecker(config, raw_engine);
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
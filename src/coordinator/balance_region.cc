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

#include "coordinator/balance_region.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config_helper.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

DEFINE_double(balance_region_default_score, 100, "balance region default score");

DEFINE_double(balance_region_limit_score_diff, 15, "balance region limit score diff");

namespace dingodb {

namespace balanceregion {

void Tracker::Print() {
  std::string store_type_name = pb::common::StoreType_Name(store_type);
  DINGO_LOG(INFO) << fmt::format("[balance.region.{}] ==========================================================",
                                 store_type_name);
  for (auto& record : records) {
    for (auto& filter_record : record->filter_records) {
      DINGO_LOG(INFO) << fmt::format("[balance.region.{}]  {}", store_type_name, filter_record);
    }

    DINGO_LOG(INFO) << fmt::format("[balance.region.{}] region({} {}->{}) score({})", store_type_name,
                                   record->region_id, record->source_store_id, record->target_store_id,
                                   record->region_score);
  }

  for (auto& filter_record : filter_records) {
    DINGO_LOG(INFO) << fmt::format("[balance.region.{}] {}", store_type_name, filter_record);
  }

  DINGO_LOG(INFO) << fmt::format("[balance.region.{}] region score {} ", store_type_name, region_score);

  DINGO_LOG(INFO) << fmt::format("[balance.region.{}] ", store_type_name);

  if (task) {
    DINGO_LOG(INFO) << fmt::format("[balance.region.{}] change task region({}) {}->{}", store_type_name,
                                   task->region_id, task->source_store_id, task->target_store_id);
  }

  DINGO_LOG(INFO) << fmt::format("[balance.region.{}] =========================end=================================",
                                 pb::common::StoreType_Name(store_type));
}

bool StoreStateFilter::Check(const dingodb::pb::common::Store& store) {
  if (store.state() != pb::common::StoreState::STORE_NORMAL) {
    if (tracker_) {
      tracker_->filter_records.push_back(fmt::format("[filter.store({})] state({}) is not normal", store.id(),
                                                     pb::common::StoreState_Name(store.state())));
    }
    return false;
  }
  return true;
}

bool ResourceFilter::Check(const dingodb::pb::common::Store& store, int64_t region_id) {
  // store metric
  std::vector<pb::common::StoreMetrics> store_metrics;
  coordinator_controller_->GetStoreMetrics(store.id(), store_metrics);
  if (store_metrics.empty()) {
    if (tracker_) {
      tracker_->GetLastRecord()->filter_records.push_back(
          fmt::format("[filter.follower({})] not found store metric", store.id()));
    }
    return false;
  }
  auto& store_metric = store_metrics[0];

  // region metric
  std::vector<pb::common::RegionMetrics> region_metrics;
  coordinator_controller_->GetRegionMetrics(region_id, region_metrics);
  if (region_metrics.empty()) {
    if (tracker_) {
      tracker_->GetLastRecord()->filter_records.push_back(
          fmt::format("[filter.follower({}).region({})] not found region metric", store.id(), region_id));
    }
    return false;
  }
  auto& region_metric = region_metrics[0];

  bool result = true;
  std::string record;

  // check store free capacity and  region need size
  int64_t free_capacity = store_metric.store_own_metrics().system_free_capacity();
  int64_t region_size = region_metric.region_size();
  if (free_capacity <= 0) {
    record = fmt::format("[filter.resource({}).region({})] missing store free capacity", store.id(), region_id);
    result = false;
  } else if (region_size <= 0) {
    record = fmt::format("[filter.resource({}).region({})] missing region size", store.id(), region_id);
    result = false;
  } else if (free_capacity <= region_size) {
    record = fmt::format("[filter.resource({}).region({})] store free capacity is not enough({}/{})", store.id(),
                         region_id, free_capacity, region_size);
    result = false;
  }

  if (tracker_ && !record.empty()) {
    tracker_->GetLastRecord()->filter_records.push_back(record);
  }

  return result;
}

bool RegionStateFilter::Check(const dingodb::pb::common::Store& store, int64_t region_id) {
  // store metric
  std::vector<pb::common::StoreMetrics> store_metrics;
  coordinator_controller_->GetStoreRegionMetrics(store.id(), store_metrics);
  if (store_metrics.empty()) {
    if (tracker_) {
      tracker_->GetLastRecord()->filter_records.push_back(
          fmt::format("[filter.follower({}).region({})] not found store metric", store.id(), region_id));
    }
    return false;
  }
  auto& store_metric = store_metrics[0];

  auto region_metrics_map = store_metric.region_metrics_map();
  std::vector<pb::common::RegionMetrics> region_metrics;
  if (region_metrics_map.find(region_id) != region_metrics_map.end()) {
    auto& region_metrics = region_metrics_map[region_id];
  } else {
    if (tracker_) {
      tracker_->GetLastRecord()->filter_records.push_back(
          fmt::format("[filter.follower({}).region({})] not found region metric", store.id(), region_id));
    }
    return false;
  }
  auto& region_metric = region_metrics[0];

  bool result = true;
  std::string record;
  if (region_metric.store_region_state() != pb::common::StoreRegionState::NORMAL) {
    record = fmt::format("[filter.region({})] store region state({}) is not normal", region_id,
                         pb::common::StoreRegionState_Name(region_metric.store_region_state()));
    result = false;
  } else if (region_metric.region_status().raft_status() != pb::common::REGION_RAFT_HEALTHY) {
    record = fmt::format("[filter.region({})] region status({}) is not healthy", region_id,
                         pb::common::RegionRaftStatus_Name(region_metric.region_status().raft_status()));
    result = false;
  } else if (region_metric.region_status().replica_status() != pb::common::REPLICA_NORMAL) {
    record = fmt::format("[filter.region({})] replica status({}) is not normal", region_id,
                         pb::common::ReplicaStatus_Name(region_metric.region_status().replica_status()));
    result = false;
  } else if (region_metric.region_status().heartbeat_status() != pb::common::REGION_ONLINE) {
    record = fmt::format("[filter.region({})] heartbeat status({}) is not normal", region_id,
                         pb::common::RegionHeartbeatState_Name(region_metric.region_status().heartbeat_status()));
    result = false;
  } else if (!region_metric.braft_status().unstable_followers().empty()) {
    record = fmt::format("[filter.region({})] exist unstable followers", region_id);
    result = false;
  } else if (region_metric.braft_status().pending_queue_size() > 10) {
    record = fmt::format("[filter.region({})] raft pending_queue_size({}) too large", region_id,
                         region_metric.braft_status().pending_queue_size());
    result = false;
  } else if (region_metric.braft_status().raft_state() != pb::common::STATE_FOLLOWER) {
    record = fmt::format("[filter.region({})] raft_state is ({})", region_id,
                         pb::common::RaftNodeState_Name(region_metric.braft_status().raft_state()));
    result = false;
  }

  if (tracker_ && !record.empty()) {
    tracker_->GetLastRecord()->filter_records.push_back(record);
  }

  return result;
}

bool RegionPlacementSafeguard::Check(const dingodb::pb::common::Store& store, int64_t region_id) {
  // store metric
  std::vector<pb::common::StoreMetrics> store_metrics;
  coordinator_controller_->GetStoreMetrics(store.id(), store_metrics);
  if (store_metrics.empty()) {
    if (tracker_) {
      tracker_->GetLastRecord()->filter_records.push_back(
          fmt::format("[filter.follower({})] not found store metric", store.id()));
    }
    return false;
  }
  auto& store_metric = store_metrics[0];

  auto region_metrics_map = store_metric.region_metrics_map();
  std::vector<pb::common::RegionMetrics> region_metrics;
  if (region_metrics_map.find(region_id) != region_metrics_map.end()) {
    if (tracker_) {
      tracker_->GetLastRecord()->filter_records.push_back(
          fmt::format("[filter.follower({}).region({})] has replica region", store.id(), region_id));
    }
    return false;
  }

  return true;
}

bool StoreEntry::Less::operator()(const StoreEntryPtr& lhs, const StoreEntryPtr& rhs) {
  float l_score = lhs->Score();
  float r_score = rhs->Score();
  if (std::abs(l_score - r_score) < 0.000001f) {
    return lhs->Id() > rhs->Id();
  }

  return l_score < r_score;
}

bool StoreEntry::Greater::operator()(const StoreEntryPtr& lhs, const StoreEntryPtr& rhs) {
  float l_score = lhs->Score();
  float r_score = rhs->Score();
  if (std::abs(l_score - r_score) < 0.000001f) {
    return lhs->Id() > rhs->Id();
  }

  return l_score > r_score;
}

int64_t StoreEntry::Id() { return store_.id(); }

pb::common::Store& StoreEntry::Store() { return store_; }

std::vector<int64_t> StoreEntry::LeaderRegionIds() { return leader_region_ids_; }

std::vector<int64_t> StoreEntry::FollowerRegionIds() { return follower_region_ids_; }

bool StoreEntry::IsLeader(int64_t region_id) {
  for (auto leader_region_id : leader_region_ids_) {
    if (region_id == leader_region_id) {
      return true;
    }
  }

  return false;
}

bool StoreEntry::IsFollower(int64_t region_id) {
  for (auto follower_region_id : follower_region_ids_) {
    if (region_id == follower_region_id) {
      return true;
    }
  }

  return false;
}

float StoreEntry::Score() {
  float capacity_score = 0;
  auto free_size = store_metric_.store_own_metrics().system_free_capacity();
  auto capacity = store_metric_.store_own_metrics().system_total_capacity();
  capacity_score = static_cast<float>(capacity - free_size) / capacity * FLAGS_balance_region_default_score;

  if (capacity_score < 0 || capacity_score > 100) {
    DINGO_LOG(WARNING) << fmt::format(
        "[balance.region] region capacity score : {} {} , total capacity : {} , free capacity : {}", capacity_score,
        capacity_score > 100 ? "higher than 100" : "lower than 0", capacity, free_size);
  }
  capacity_score_ = (capacity_score < 0) ? 0 : capacity_score;
  capacity_score_ = (capacity_score > 100) ? 100 : capacity_score;

  float region_count_score = 0;
  int64_t region_approximate_size = ConfigHelper::GetSplitCheckApproximateSize();
  auto region_count = store_metric_.region_metrics_map_size();
  float max_region_count = static_cast<float>(capacity) / region_approximate_size;
  region_count_score = static_cast<float>(region_count) / max_region_count * FLAGS_balance_region_default_score;
  if (region_count_score > 100 || region_count_score < 0) {
    DINGO_LOG(WARNING) << fmt::format(
        "[balance.region] region count score : {} {} , region_approximate_size : {} , region_count : {} , "
        "max region count : {}",
        region_count_score, region_count_score > 100 ? "higher than 100" : "lower than 0", region_approximate_size,
        region_count, max_region_count);
  }
  region_count_score_ = (region_count_score < 0) ? 0 : region_count_score;
  region_count_score_ = (region_count_score > 100) ? 100 : region_count_score;

  float region_count_ratio = ConfigHelper::GetBalanceRegionCountRatio();
  total_score_ = region_count_ratio * region_count_score + (1 - region_count_ratio) * capacity_score;
  return total_score_;
}

bool CandidateStores::HasStore() { return index_ < stores_.size(); }

StoreEntryPtr CandidateStores::Store(int64_t store_id) {
  for (auto& store : stores_) {
    if (store->Id() == store_id) {
      return store;
    }
  }

  return nullptr;
}

StoreEntryPtr CandidateStores::GetStore() { return HasStore() ? stores_[index_] : nullptr; }

uint32_t CandidateStores::StoreSize() { return stores_.size(); }

void CandidateStores::Next() { ++index_; }

void CandidateStores::Sort() {
  if (asc_) {
    std::sort(stores_.begin(), stores_.end(), StoreEntry::Less());
  } else {
    std::sort(stores_.begin(), stores_.end(), StoreEntry::Greater());
  }
}

std::string CandidateStores::ToString() {
  std::string str;
  for (const auto& store : stores_) {
    str += fmt::format(" store_id : {} (total_score : {}, region_count_score : {}, capacity_score : {}) ", store->Id(),
                       store->Score(), store->GetRegionCountScore(), store->GetCapacityScore());
  }
  return str;
}

static pb::common::RegionType GetRegionTypeByStoreType(pb::common::StoreType store_type) {
  if (store_type == pb::common::NODE_TYPE_STORE) {
    return pb::common::RegionType::STORE_REGION;
  } else if (store_type == pb::common::NODE_TYPE_INDEX) {
    return pb::common::RegionType::INDEX_REGION;
  } else if (store_type == pb::common::NODE_TYPE_DOCUMENT) {
    return pb::common::RegionType::DOCUMENT_REGION;
  }

  return pb::common::RegionType::STORE_REGION;
}

// parse time period, format start_hour1,end_hour1;start_hour2,end_hour2
// e.g. str=1,3;4,5 parsed [1,3] [4,5]
std::vector<std::pair<int, int>> BalanceRegionScheduler::ParseTimePeriod(const std::string& time_period) {
  std::vector<std::pair<int, int>> result;

  std::vector<std::string> parts;
  Helper::SplitString(time_period, ';', parts);
  for (auto& part : parts) {
    std::vector<std::string> sub_parts;
    Helper::SplitString(part, ',', sub_parts);
    if (sub_parts.empty()) {
      continue;
    }

    bool is_valid = true;
    int start_hour = 0;
    if (!sub_parts[0].empty()) {
      char* endptr = nullptr;
      start_hour = std::strtol(sub_parts[0].c_str(), &endptr, 10);
      if (endptr != nullptr && (endptr - sub_parts[0].c_str()) != sub_parts[0].size()) {
        is_valid = false;
      }
    }

    int end_hour = 23;
    if (sub_parts.size() > 1 && !sub_parts[1].empty()) {
      char* endptr = nullptr;
      end_hour = std::strtol(sub_parts[1].c_str(), &endptr, 10);
      if (endptr != nullptr && (endptr - sub_parts[1].c_str()) != sub_parts[1].size()) {
        is_valid = false;
      }
    }
    if (is_valid) {
      result.push_back(std::make_pair(start_hour, end_hour));
    }
  }

  return result;
}

bool BalanceRegionScheduler::ShouldRun() {
  std::string time_period_value = ConfigHelper::GetBalanceRegionInspectionTimePeriod();
  if (time_period_value.empty()) {
    return false;
  }
  auto time_periods = ParseTimePeriod(time_period_value);
  if (time_periods.empty()) {
    return false;
  }

  int current_hour = Helper::NowHour();
  DINGO_LOG(INFO) << fmt::format("[balance.region] time_period({}), current hour({})", time_period_value, current_hour);

  for (auto time_period : time_periods) {
    if (current_hour >= time_period.first && current_hour <= time_period.second) {
      return true;
    }
  }

  return false;
}

butil::Status BalanceRegionScheduler::LaunchBalanceRegion(std::shared_ptr<CoordinatorControl> coordinator_controller,
                                                          std::shared_ptr<Engine> raft_engine,
                                                          pb::common::StoreType store_type, bool dryrun, bool force,
                                                          TrackerPtr tracker) {
  // can not balance document region
  if (store_type == pb::common::NODE_TYPE_DOCUMENT) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "can not balance document region");
  }

  // check run timing
  if (!force && !ShouldRun()) {
    return butil::Status(pb::error::EINTERNAL, "current time not should run.");
  }

  // not allow parallel running
  static std::atomic<bool> is_running = false;
  if (is_running.load()) {
    return butil::Status(pb::error::EINTERNAL, "already exist balance region running.");
  }
  is_running.store(true);
  DEFER(is_running.store(false));

  DINGO_LOG(INFO) << fmt::format("[balance.region] launch balance region store_type({}) dryrun({})",
                                 pb::common::StoreType_Name(store_type), dryrun);
  if (tracker) tracker->store_type = store_type;

  // ready filters
  std::vector<FilterPtr> store_filters;
  store_filters.push_back(std::make_shared<StoreStateFilter>(tracker));

  std::vector<FilterPtr> region_filters;
  region_filters.push_back(std::make_shared<RegionStateFilter>(coordinator_controller, tracker));

  std::vector<FilterPtr> resource_filters;
  resource_filters.push_back(std::make_shared<ResourceFilter>(coordinator_controller, tracker));

  std::vector<FilterPtr> placement_safe_filters;
  placement_safe_filters.push_back(std::make_shared<RegionPlacementSafeguard>(coordinator_controller, tracker));

  auto balance_region_scheduler =
      std::make_shared<BalanceRegionScheduler>(coordinator_controller, raft_engine, store_filters, region_filters,
                                               resource_filters, placement_safe_filters, tracker);

  // get all region and store
  pb::common::RegionMap region_map;
  auto region_type = GetRegionTypeByStoreType(store_type);
  coordinator_controller->GetRegionMapFull(region_map, region_type);
  if (region_map.regions().empty()) {
    return butil::Status(pb::error::EINTERNAL, "region map is empty");
  }
  pb::common::StoreMap store_map;
  coordinator_controller->GetStoreMap(store_map, store_type);
  if (store_map.stores().empty()) {
    return butil::Status(pb::error::EINTERNAL, "store map is empty");
  }

  auto change_region_task = balance_region_scheduler->Schedule(region_map, store_map);
  if (change_region_task == nullptr) {
    return butil::Status(pb::error::OK, "change region task is empty, maybe region is balance");
  }

  if (!dryrun) {
    balance_region_scheduler->CommitChangRegionTaskList(change_region_task);
  }

  if (tracker) {
    tracker->task = change_region_task;
  }

  return butil::Status::OK();
}

ChangeRegionTaskPtr BalanceRegionScheduler::Schedule(const pb::common::RegionMap& region_map,
                                                     const pb::common::StoreMap& store_map) {
  CHECK(!region_map.regions().empty()) << "region map is empty.";
  CHECK(!store_map.stores().empty()) << "store map is empty.";
  CHECK(raft_engine_ != nullptr) << "raft_engine is nullptr.";
  CHECK(coordinator_controller_ != nullptr) << "coordinator_controller is nullptr.";

  auto store_region_id_map = GenerateStoreRegionMap(region_map);
  if (store_region_id_map.empty()) {
    DINGO_LOG(WARNING) << "[balance.region] store region map is emtpy.";
    return nullptr;
  }

  // generate all store entry
  auto store_entries = GenerateStoreEntries(store_region_id_map, store_map);
  if (store_entries.empty()) {
    DINGO_LOG(WARNING) << "[balance.region] store entries is emtpy.";
    return {};
  }

  // create source and target candidate stores
  auto source_candidate_stores = CandidateStores::New(store_entries, false);
  auto target_candidate_stores = CandidateStores::New(store_entries, true);

  if (tracker_) {
    tracker_->region_score = source_candidate_stores->ToString();
  }

  ChangeRegionTaskPtr change_region_task;

  while (source_candidate_stores->HasStore() && target_candidate_stores->HasStore()) {
    if (source_candidate_stores->GetStore()->Score() <= target_candidate_stores->GetStore()->Score()) {
      if (tracker_) {
        tracker_->filter_records.push_back(
            fmt::format("[filter.store] source stroe : ({})  score ({})  lower than target store ({}) score ({}) ",
                        source_candidate_stores->GetStore()->Id(), source_candidate_stores->GetStore()->Score(),
                        target_candidate_stores->GetStore()->Id(), target_candidate_stores->GetStore()->Score()));
      }
      break;
    }
    if (source_candidate_stores->GetStore()->Score() - target_candidate_stores->GetStore()->Score() <=
        FLAGS_balance_region_limit_score_diff) {
      if (tracker_) {
        tracker_->filter_records.push_back(fmt::format(
            "[filter.store] source stroe : ({})  score ({})  subtract target store ({}) score ({}) less than ({}) ",
            source_candidate_stores->GetStore()->Id(), source_candidate_stores->GetStore()->Score(),
            target_candidate_stores->GetStore()->Id(), target_candidate_stores->GetStore()->Score(),
            FLAGS_balance_region_limit_score_diff));
      }
      break;
    }

    {
      auto record = tracker_ != nullptr ? tracker_->AddRecord() : nullptr;
      if (record) {
        record->region_score = source_candidate_stores->ToString();
      }
      change_region_task = GenerateChangeRegionTask(source_candidate_stores, target_candidate_stores);
      if (change_region_task != nullptr) {
        if (record) {
          record->region_id = change_region_task->region_id;
          record->source_store_id = change_region_task->source_store_id;
          record->target_store_id = change_region_task->target_store_id;
        }
        break;
      } else {
        source_candidate_stores->Next();
      }
    }

    {
      auto record = tracker_ != nullptr ? tracker_->AddRecord() : nullptr;
      if (record) {
        record->region_score = source_candidate_stores->ToString();
      }
      change_region_task = GenerateChangeRegionTask(source_candidate_stores, target_candidate_stores);
      if (change_region_task != nullptr) {
        if (record) {
          record->region_id = change_region_task->region_id;
          record->source_store_id = change_region_task->source_store_id;
          record->target_store_id = change_region_task->target_store_id;
        }
        break;
      } else {
        target_candidate_stores->Next();
      }
    }
  }

  return change_region_task;
}

// commit change region task to raft
void BalanceRegionScheduler::CommitChangRegionTaskList(const ChangeRegionTaskPtr& task) {
  dingodb::pb::coordinator_internal::MetaIncrement meta_increment;
  auto status1 =
      coordinator_controller_->ChangePairPeerRegionWithTaskList(task->region_id, task->new_store_ids, meta_increment);
  DINGO_LOG_IF(ERROR, !status1.ok()) << fmt::format("generate ChangePairPeerRegionWithTaskList, error: {}",
                                                    status1.error_str());

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);

  DINGO_LOG(INFO) << "[balance.region] meta_increment: " << meta_increment.ShortDebugString();

  auto status2 = raft_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  DINGO_LOG_IF(ERROR, !status2.ok()) << fmt::format("commit raft failed, error: {}", status2.error_str());
}

// 1. 获取source节点的所有为follower的region
// 2. 过滤不健康region
// 3. 剩下的region按照从大到小排序
// 4. 过滤副本隔离和存储资源
// 5. 生成任务
ChangeRegionTaskPtr BalanceRegionScheduler::GenerateChangeRegionTask(CandidateStoresPtr source_stores,
                                                                     CandidateStoresPtr target_stores) {
  auto source_store_entry = source_stores->GetStore();
  auto target_store_entry = target_stores->GetStore();
  if (source_store_entry->Score() <= target_store_entry->Score()) {
    return nullptr;
  }
  auto region_ids = FilterRegion(source_store_entry->FollowerRegionIds());
  if (region_ids.empty()) {
    return nullptr;
  }

  std::vector<pb::common::RegionMetrics> region_metrics;
  for (auto id : region_ids) {
    std::vector<pb::common::RegionMetrics> region_metric;
    coordinator_controller_->GetRegionMetrics(id, region_metric);
    if (!region_metric.empty()) {
      region_metrics.push_back(region_metric[0]);
    } else {
      DINGO_LOG(WARNING) << fmt::format("[balance.region] not found store region metric : {}", id);
    }
  }

  std::sort(region_metrics.begin(), region_metrics.end(),
            [](const pb::common::RegionMetrics& a, const pb::common::RegionMetrics& b) {
              return a.region_size() > b.region_size();
            });

  for (const auto& region_metric : region_metrics) {
    if (!FilterResource(target_store_entry->Store(), region_metric.id()) &&
        !FilterPlacementSafeguard(target_store_entry->Store(), region_metric.id())) {
      return GenerateChangeRegionTask(region_metric, source_store_entry->Id(), target_store_entry);
    }
  }
  return nullptr;
}

ChangeRegionTaskPtr BalanceRegionScheduler::GenerateChangeRegionTask(pb::common::RegionMetrics region_metrics,
                                                                     int64_t source_store_id,
                                                                     StoreEntryPtr target_store_entry) {
  auto task = std::make_shared<ChangeRegionTask>();

  task->region_id = region_metrics.id();
  task->source_store_id = source_store_id;
  task->target_store_id = target_store_entry->Id();
  task->new_store_ids.push_back(task->target_store_id);
  for (const auto& peer : region_metrics.region_definition().peers()) {
    if (peer.store_id() != task->source_store_id) {
      task->new_store_ids.push_back(peer.store_id());
    }
  }
  return task;
}

pb::common::Store BalanceRegionScheduler::GetStore(const pb::common::StoreMap& store_map, int64_t store_id) {
  for (const auto& store : store_map.stores()) {
    if (store.id() == store_id) {
      return store;
    }
  }

  return {};
}

std::vector<StoreEntryPtr> BalanceRegionScheduler::GenerateStoreEntries(const StoreRegionMap& store_region_id_map,
                                                                        const pb::common::StoreMap& store_map) {
  std::vector<StoreEntryPtr> store_entries;
  for (const auto& store : store_map.stores()) {
    if (store.id() == 0) {
      continue;
    }
    if (FilterStore(store)) {
      continue;
    }

    std::vector<pb::common::StoreMetrics> store_metrics;
    coordinator_controller_->GetStoreMetrics(store.id(), store_metrics);

    if (store_metrics.empty()) {
      DINGO_LOG(WARNING) << fmt::format("[balance.region] not found store store metric : {}", store.id());
      continue;
    }

    if (store_region_id_map.find(store.id()) != store_region_id_map.end()) {
      const auto& leader_region_ids = store_region_id_map.at(store.id()).first;
      const auto& follower_region_ids = store_region_id_map.at(store.id()).second;

      store_entries.push_back(StoreEntry::New(store, store_metrics[0], leader_region_ids, follower_region_ids));
    } else {
      store_entries.push_back(StoreEntry::New(store, store_metrics[0], std::vector<long>(), std::vector<long>()));
    }
  }
  return store_entries;
}

BalanceRegionScheduler::StoreRegionMap BalanceRegionScheduler::GenerateStoreRegionMap(
    const pb::common::RegionMap& region_map) {
  StoreRegionMap store_region_id_map;
  for (const auto& region : region_map.regions()) {
    if (region.leader_store_id() == 0) {
      if (tracker_) {
        tracker_->filter_records.push_back(fmt::format("[filter.region({})] unknown leader", region.id()));
      }
      continue;
    }
    for (const auto& peer : region.definition().peers()) {
      if (store_region_id_map.find(peer.store_id()) == store_region_id_map.end()) {
        store_region_id_map.insert(
            std::make_pair(peer.store_id(), std::make_pair(std::vector<int64_t>{}, std::vector<int64_t>{})));
      }

      auto it = store_region_id_map.find(peer.store_id());
      auto& leader_region_ids = it->second.first;
      auto& follower_region_ids = it->second.second;

      if (peer.store_id() == region.leader_store_id()) {
        leader_region_ids.push_back(region.id());
      } else {
        follower_region_ids.push_back(region.id());
      }
    }
  }

  return store_region_id_map;
}

bool BalanceRegionScheduler::FilterStore(const dingodb::pb::common::Store& store) {
  for (auto& filter : store_filters_) {
    if (!filter->Check(store)) {
      return true;
    }
  }

  return false;
}

bool BalanceRegionScheduler::FilterRegion(int64_t region_id) {
  for (auto& filter : region_filters_) {
    if (!filter->Check(region_id)) {
      return true;
    }
  }

  return false;
}

std::vector<int64_t> BalanceRegionScheduler::FilterRegion(std::vector<int64_t> region_ids) {
  std::vector<int64_t> reserve_region_ids;
  for (auto region_id : region_ids) {
    if (!FilterRegion(region_id)) {
      reserve_region_ids.push_back(region_id);
    }
  }

  return reserve_region_ids;
}

bool BalanceRegionScheduler::FilterResource(const dingodb::pb::common::Store& store, int64_t region_id) {
  for (auto& filter : resource_filters_) {
    if (!filter->Check(store, region_id)) {
      return true;
    }
  }

  return false;
}

bool BalanceRegionScheduler::FilterPlacementSafeguard(const dingodb::pb::common::Store& store, int64_t region_id) {
  for (auto& filter : placement_safe_filters_) {
    if (!filter->Check(store, region_id)) {
      return true;
    }
  }

  return false;
}

}  // namespace balanceregion

}  // namespace dingodb
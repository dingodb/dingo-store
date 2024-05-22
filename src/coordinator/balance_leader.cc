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

#include "coordinator/balance_leader.h"

#include <algorithm>
#include <atomic>
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
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"

DEFINE_uint32(balacne_leader_task_batch_size, 4, "balance leader task batch size");

DEFINE_uint32(balacne_leader_random_select_region_num, 10, "balance leader random select region num");

namespace dingodb {

namespace balance {

bool StoreStateFilter::Check(dingodb::pb::common::Store& store) {
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
  coordinator_controller_->GetStoreRegionMetrics(store.id(), store_metrics);
  if (store_metrics.empty()) {
    if (tracker_) {
      tracker_->GetLastRecord()->filter_records.push_back(
          fmt::format("[filter.follower({}).region({})] not found store metric", store.id(), region_id));
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

  // check store free memory and vector index region need memory
  if (region_metric.region_definition().index_parameter().index_type() == pb::common::INDEX_TYPE_VECTOR) {
    int64_t free_memory = store_metric.store_own_metrics().system_free_memory();
    int64_t memory_bytes = region_metric.vector_index_metrics().memory_bytes();
    if (free_memory <= 0) {
      record = fmt::format("[filter.resource({}).region({})] missing store free memory", store.id(), region_id);
      result = false;
    } else if (memory_bytes <= 0) {
      record = fmt::format("[filter.resource({}).region({})] missing vector index memory size", store.id(), region_id);
      result = false;
    } else if (free_memory <= memory_bytes) {
      record = fmt::format("[filter.resource({}).region({})] store free memory is not enough({}/{})", store.id(),
                           region_id, free_memory, memory_bytes);
      result = false;
    }
  }

  if (tracker_ && !record.empty()) {
    tracker_->GetLastRecord()->filter_records.push_back(record);
  }

  return result;
}

bool RegionHealthFilter::Check(int64_t region_id) {
  std::vector<pb::common::RegionMetrics> region_metrics;
  coordinator_controller_->GetRegionMetrics(region_id, region_metrics);
  if (region_metrics.empty()) {
    if (tracker_) {
      tracker_->GetLastRecord()->filter_records.push_back(
          fmt::format("[filter.region({})] not found region metric", region_id));
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
  }

  if (tracker_ && !record.empty()) {
    tracker_->GetLastRecord()->filter_records.push_back(record);
  }

  return result;
}

bool TaskFilter::Check(int64_t region_id) {
  butil::FlatMap<int64_t, pb::coordinator::TaskList> task_lists;
  coordinator_controller_->GetTaskListAll(task_lists);

  bool result = true;
  std::string record;
  for (auto& [_, task_list] : task_lists) {
    for (const auto& task : task_list.tasks()) {
      for (const auto& store_operation : task.store_operations()) {
        for (const auto& region_cmd : store_operation.region_cmds()) {
          if (region_cmd.region_id() == region_id) {
            record = fmt::format("[filter.task] region({}) has not finish task({})", region_id,
                                 pb::coordinator::RegionCmdType_Name(region_cmd.region_cmd_type()));
            result = false;
          }
        }
      }
    }
  }

  if (tracker_ && !record.empty()) {
    tracker_->filter_records.push_back(record);
  }
  return true;
}

void Tracker::Print() {
  std::string store_type_name = pb::common::StoreType_Name(store_type);
  DINGO_LOG(INFO) << fmt::format("[balance.leader.{}] ==========================================================",
                                 store_type_name);
  for (auto& record : records) {
    for (auto& filter_record : record->filter_records) {
      DINGO_LOG(INFO) << fmt::format("[balance.leader.{}] round({}) {}", store_type_name, record->round, filter_record);
    }

    DINGO_LOG(INFO) << fmt::format("[balance.leader.{}] round({}) region({} {}->{}) score({})", store_type_name,
                                   record->round, record->region_id, record->source_store_id, record->target_store_id,
                                   record->leader_score);
  }

  for (auto& filter_record : filter_records) {
    DINGO_LOG(INFO) << fmt::format("[balance.leader.{}] {}", store_type_name, filter_record);
  }

  DINGO_LOG(INFO) << fmt::format("[balance.leader.{}] leader score {} -> {}", store_type_name, leader_score,
                                 expect_leader_score);

  DINGO_LOG(INFO) << fmt::format("[balance.leader.{}] transfer task count {}", store_type_name, tasks.size());
  for (auto& task : tasks) {
    DINGO_LOG(INFO) << fmt::format("[balance.leader.{}] transfer task region({}) {}->{}", store_type_name,
                                   task->region_id, task->source_store_id, task->target_store_id);
  }

  DINGO_LOG(INFO) << fmt::format("[balance.leader.{}] =========================end=================================",
                                 pb::common::StoreType_Name(store_type));
}

bool StoreEntry::Less::operator()(const StoreEntryPtr& lhs, const StoreEntryPtr& rhs) {
  float l_score = lhs->LeaderScore();
  float r_score = rhs->LeaderScore();
  if (std::abs(l_score - r_score) < 0.000001f) {
    return lhs->Id() > rhs->Id();
  }

  return l_score < r_score;
}

bool StoreEntry::Greater::operator()(const StoreEntryPtr& lhs, const StoreEntryPtr& rhs) {
  float l_score = lhs->LeaderScore();
  float r_score = rhs->LeaderScore();
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

int32_t StoreEntry::DeltaLeaderNum() const { return delta_leader_num_; }

void StoreEntry::IncDeltaLeaderNum() { ++delta_leader_num_; }

void StoreEntry::DecDeltaLeaderNum() { --delta_leader_num_; }

float StoreEntry::LeaderScore() { return LeaderScore(0); }

float StoreEntry::LeaderScore(int32_t delta) {
  int32_t leader_num = static_cast<int32_t>(leader_region_ids_.size());
  int32_t leader_num_weight = store_.leader_num_weight() > 0 ? store_.leader_num_weight() : 1;
  return static_cast<float>(leader_num + delta_leader_num_ + delta) / leader_num_weight;
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
    str += fmt::format("{}({}),", store->Id(), store->LeaderScore());
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

static pb::common::IndexType GetIndexTypeByRegionType(pb::common::StoreType store_type) {
  if (store_type == pb::common::NODE_TYPE_STORE) {
    return pb::common::INDEX_TYPE_NONE;
  } else if (store_type == pb::common::NODE_TYPE_INDEX) {
    return pb::common::INDEX_TYPE_VECTOR;
  } else if (store_type == pb::common::NODE_TYPE_DOCUMENT) {
    return pb::common::INDEX_TYPE_DOCUMENT;
  }

  return pb::common::INDEX_TYPE_NONE;
}

// parse time period, format start_hour1,end_hour1;start_hour2,end_hour2
// e.g. str=1,3;4,5 parsed [1,3] [4,5]
std::vector<std::pair<int, int>> BalanceLeaderScheduler::ParseTimePeriod(const std::string& time_period) {
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

bool BalanceLeaderScheduler::ShouldRun() {
  std::string time_period_value = ConfigHelper::GetBalanceLeaderInspectionTimePeriod();
  if (time_period_value.empty()) {
    return false;
  }
  auto time_periods = ParseTimePeriod(time_period_value);
  if (time_periods.empty()) {
    return false;
  }

  int current_hour = Helper::NowHour();
  DINGO_LOG(INFO) << fmt::format("[balance.leader] time_period({}), current hour({})", time_period_value, current_hour);

  for (auto time_period : time_periods) {
    if (current_hour >= time_period.first && current_hour <= time_period.second) {
      return true;
    }
  }

  return false;
}

butil::Status BalanceLeaderScheduler::LaunchBalanceLeader(std::shared_ptr<CoordinatorControl> coordinator_controller,
                                                          std::shared_ptr<Engine> raft_engine,
                                                          pb::common::StoreType store_type, bool dryrun,
                                                          TrackerPtr tracker) {
  // check run timing
  if (!ShouldRun()) {
    return butil::Status(pb::error::EINTERNAL, "current time not should run.");
  }

  // not allow parallel running
  static std::atomic<bool> is_running = false;
  if (is_running.load()) {
    return butil::Status(pb::error::EINTERNAL, "already exist balance leader running.");
  }
  is_running.store(true);
  DEFER(is_running.store(false));

  DINGO_LOG(INFO) << fmt::format("[balance.leader] launch balance leader store_type({}) dryrun({})",
                                 pb::common::StoreType_Name(store_type), dryrun);
  if (tracker) tracker->store_type = store_type;

  // ready filters
  std::vector<FilterPtr> store_filters;
  store_filters.push_back(std::make_shared<StoreStateFilter>(tracker));

  std::vector<FilterPtr> region_filters;
  region_filters.push_back(std::make_shared<RegionHealthFilter>(coordinator_controller, tracker));

  std::vector<FilterPtr> task_filters;
  task_filters.push_back(std::make_shared<TaskFilter>(coordinator_controller, tracker));

  std::vector<FilterPtr> resource_filters;
  resource_filters.push_back(std::make_shared<ResourceFilter>(coordinator_controller, tracker));

  auto balance_leader_scheduler = std::make_shared<BalanceLeaderScheduler>(
      coordinator_controller, raft_engine, store_filters, region_filters, task_filters, resource_filters, tracker);

  // get all region and store
  pb::common::RegionMap region_map;
  auto region_type = GetRegionTypeByStoreType(store_type);
  auto index_type = GetIndexTypeByRegionType(store_type);
  coordinator_controller->GetRegionMapFull(region_map, region_type, index_type);
  if (region_map.regions().empty()) {
    return butil::Status(pb::error::EINTERNAL, "region map is empty");
  }
  pb::common::StoreMap store_map;
  coordinator_controller->GetStoreMap(store_map, store_type);
  if (store_map.stores().empty()) {
    return butil::Status(pb::error::EINTERNAL, "store map is empty");
  }

  auto transfer_leader_tasks = balance_leader_scheduler->Schedule(region_map, store_map);
  if (transfer_leader_tasks.empty()) {
    return butil::Status(0, "transfer leader task is empty, maybe leader is balance");
  }

  if (!dryrun) {
    balance_leader_scheduler->CommitTransferLeaderTaskList(transfer_leader_tasks);
  }

  if (tracker) {
    tracker->tasks = transfer_leader_tasks;
  }

  return butil::Status::OK();
}

std::vector<TransferLeaderTaskPtr> BalanceLeaderScheduler::Schedule(const pb::common::RegionMap& region_map,
                                                                    const pb::common::StoreMap& store_map) {
  CHECK(!region_map.regions().empty()) << "region map is empty.";
  CHECK(!store_map.stores().empty()) << "store map is empty.";
  CHECK(raft_engine_ != nullptr) << "raft_engine is nullptr.";
  CHECK(coordinator_controller_ != nullptr) << "coordinator_controller is nullptr.";

  auto store_region_id_map = GenerateStoreRegionMap(region_map);
  if (store_region_id_map.empty()) {
    DINGO_LOG(WARNING) << "[balance.leader] store region map is emtpy.";
    return {};
  }

  // generate all store entry
  auto store_entries = GenerateStoreEntries(store_region_id_map, store_map);
  if (store_entries.empty()) {
    DINGO_LOG(WARNING) << "[balance.leader] store entries is emtpy.";
    return {};
  }

  // create source and target candidate stores
  auto source_candidate_stores = CandidateStores::New(store_entries, false);
  auto target_candidate_stores = CandidateStores::New(store_entries, true);

  if (tracker_) {
    tracker_->leader_score = source_candidate_stores->ToString();
  }

  int32_t round = 0;
  std::set<int64_t> used_regions;
  std::vector<TransferLeaderTaskPtr> transfer_leader_tasks;
  while (source_candidate_stores->HasStore() || target_candidate_stores->HasStore()) {
    if (source_candidate_stores->HasStore()) {
      auto record = tracker_ != nullptr ? tracker_->AddRecord() : nullptr;
      if (record) {
        record->round = ++round;
        record->leader_score = source_candidate_stores->ToString();
      }

      auto transfer_leader_task = GenerateTransferOutLeaderTask(source_candidate_stores, used_regions);
      if (transfer_leader_task != nullptr) {
        if (record) {
          record->region_id = transfer_leader_task->region_id;
          record->source_store_id = transfer_leader_task->source_store_id;
          record->target_store_id = transfer_leader_task->target_store_id;
        }

        used_regions.insert(transfer_leader_task->region_id);
        transfer_leader_tasks.push_back(transfer_leader_task);
        // adjust leader score
        ReadjustLeaderScore(source_candidate_stores, target_candidate_stores, transfer_leader_task);

        if (transfer_leader_tasks.size() >= FLAGS_balacne_leader_task_batch_size) {
          break;
        }

      } else {
        source_candidate_stores->Next();
      }
    }

    if (target_candidate_stores->HasStore()) {
      auto record = tracker_ != nullptr ? tracker_->AddRecord() : nullptr;
      if (record) {
        record->round = ++round;
        record->leader_score = source_candidate_stores->ToString();
      }

      auto transfer_leader_task = GenerateTransferInLeaderTask(target_candidate_stores, used_regions);
      if (transfer_leader_task != nullptr) {
        if (record) {
          record->region_id = transfer_leader_task->region_id;
          record->source_store_id = transfer_leader_task->source_store_id;
          record->target_store_id = transfer_leader_task->target_store_id;
        }

        used_regions.insert(transfer_leader_task->region_id);
        transfer_leader_tasks.push_back(transfer_leader_task);
        // adjust leader score
        ReadjustLeaderScore(source_candidate_stores, target_candidate_stores, transfer_leader_task);

        if (transfer_leader_tasks.size() >= FLAGS_balacne_leader_task_batch_size) {
          break;
        }
      } else {
        target_candidate_stores->Next();
      }
    }
  }

  // if region runing task then eliminate transfer leader task
  transfer_leader_tasks = FilterTask(transfer_leader_tasks);

  if (tracker_) {
    tracker_->expect_leader_score = source_candidate_stores->ToString();
  }

  return transfer_leader_tasks;
}

// commit transfer leader task to raft
void BalanceLeaderScheduler::CommitTransferLeaderTaskList(const std::vector<TransferLeaderTaskPtr>& tasks) {
  dingodb::pb::coordinator_internal::MetaIncrement meta_increment;
  auto* task_list = coordinator_controller_->CreateTaskList(meta_increment, "BalanceLeader");
  for (const auto& task : tasks) {
    auto* mut_task = task_list->add_tasks();

    auto* mut_store_operation = mut_task->add_store_operations();
    mut_store_operation->set_id(task->source_store_id);

    auto* mut_region_cmd = mut_store_operation->add_region_cmds();
    mut_region_cmd->set_id(
        coordinator_controller_->GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
    mut_region_cmd->set_job_id(task_list->id());
    mut_region_cmd->set_region_id(task->region_id);
    mut_region_cmd->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_TRANSFER_LEADER);

    auto* mut_request = mut_region_cmd->mutable_transfer_leader_request();
    auto* mut_peer = mut_request->mutable_peer();
    mut_peer->set_store_id(task->target_store_id);
    mut_peer->set_role(pb::common::PeerRole::VOTER);
    *mut_peer->mutable_raft_location() = task->target_raft_location;
    *mut_peer->mutable_server_location() = task->target_server_location;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);

  DINGO_LOG(INFO) << "[balance.leader] meta_increment: " << meta_increment.ShortDebugString();

  auto status = raft_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  DINGO_LOG_IF(ERROR, !status.ok()) << fmt::format("commit raft failed, error: {}", status.error_str());
}

pb::common::Store BalanceLeaderScheduler::GetStore(const pb::common::StoreMap& store_map, int64_t store_id) {
  for (const auto& store : store_map.stores()) {
    if (store.id() == store_id) {
      return store;
    }
  }

  return {};
}

std::vector<StoreEntryPtr> BalanceLeaderScheduler::GenerateStoreEntries(const StoreRegionMap& store_region_id_map,
                                                                        const pb::common::StoreMap& store_map) {
  std::vector<StoreEntryPtr> store_entries;
  for (const auto& [store_id, pair] : store_region_id_map) {
    const auto& leader_region_ids = pair.first;
    const auto& follower_region_ids = pair.second;

    // todo filter
    auto store = GetStore(store_map, store_id);
    if (store.id() == 0) {
      continue;
    }

    if (FilterStore(store)) {
      continue;
    }

    store_entries.push_back(StoreEntry::New(store, leader_region_ids, follower_region_ids));
  }

  return store_entries;
}

BalanceLeaderScheduler::StoreRegionMap BalanceLeaderScheduler::GenerateStoreRegionMap(
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

void BalanceLeaderScheduler::ReadjustLeaderScore(CandidateStoresPtr source_candidate_stores,
                                                 CandidateStoresPtr target_candidate_stores,
                                                 TransferLeaderTaskPtr task) {
  CHECK(source_candidate_stores != nullptr) << "source_candidate_stores is nullptr.";
  CHECK(target_candidate_stores != nullptr) << "target_candidate_stores is nullptr.";
  CHECK(task != nullptr) << "transfer leader task is nullptr.";

  auto source_store = source_candidate_stores->Store(task->source_store_id);
  if (source_store != nullptr) {
    source_store->DecDeltaLeaderNum();
  }

  auto target_store = source_candidate_stores->Store(task->target_store_id);
  if (target_store != nullptr) {
    target_store->IncDeltaLeaderNum();
  }

  source_candidate_stores->Sort();
  target_candidate_stores->Sort();
}

std::vector<int64_t> BalanceLeaderScheduler::FilterUsedRegion(std::vector<int64_t> region_ids,
                                                              const std::set<int64_t>& used_regions) {
  std::vector<int64_t> reserve_region_ids;
  for (auto region_id : region_ids) {
    if (used_regions.find(region_id) == used_regions.end()) {
      reserve_region_ids.push_back(region_id);
    }
  }

  return reserve_region_ids;
}

pb::coordinator_internal::RegionInternal BalanceLeaderScheduler::PickOneRegion(std::vector<int64_t> region_ids) {
  if (region_ids.empty()) {
    return {};
  }
  int64_t picked_region_id = region_ids[Helper::GenerateRealRandomInteger(0, region_ids.size() - 1)];

  return coordinator_controller_->GetRegion(picked_region_id);
}

std::vector<StoreEntryPtr> BalanceLeaderScheduler::GetFollowerStores(CandidateStoresPtr candidate_stores,
                                                                     pb::coordinator_internal::RegionInternal& region,
                                                                     int64_t leader_store_id) {
  std::vector<StoreEntryPtr> store_entries;

  for (const auto& peer : region.definition().peers()) {
    if (peer.role() == pb::common::PeerRole::VOTER && peer.store_id() != leader_store_id) {
      auto store_entry = candidate_stores->Store(peer.store_id());
      if (store_entry != nullptr) {
        store_entries.push_back(store_entry);
      }
    }
  }

  std::sort(store_entries.begin(), store_entries.end(), StoreEntry::Less());
  return store_entries;
}

StoreEntryPtr BalanceLeaderScheduler::GetLeaderStore(CandidateStoresPtr candidate_stores,
                                                     pb::coordinator_internal::RegionInternal& region) {
  for (const auto& peer : region.definition().peers()) {
    if (peer.role() == pb::common::PeerRole::VOTER) {
      auto store_entry = candidate_stores->Store(peer.store_id());
      if (store_entry != nullptr && store_entry->IsLeader(region.id())) {
        return store_entry;
      }
    }
  }

  return nullptr;
}

std::vector<StoreEntryPtr> FilterScoreLessThanLeader(StoreEntryPtr leader_store_entry,
                                                     std::vector<StoreEntryPtr>& follower_store_entries) {
  std::vector<StoreEntryPtr> reserve_store_entries;
  for (auto& follower_store_entry : follower_store_entries) {
    if (follower_store_entry->LeaderScore(1) <= leader_store_entry->LeaderScore(-1)) {
      reserve_store_entries.push_back(follower_store_entry);
    }
  }

  return reserve_store_entries;
}

TransferLeaderTaskPtr BalanceLeaderScheduler::GenerateTransferLeaderTask(int64_t region_id, int64_t leader_store_id,
                                                                         StoreEntryPtr follower_store_entry) {
  auto task = std::make_shared<TransferLeaderTask>();

  task->region_id = region_id;
  task->source_store_id = leader_store_id;
  task->target_store_id = follower_store_entry->Id();

  auto& store = follower_store_entry->Store();
  task->target_raft_location = store.raft_location();
  task->target_server_location = store.server_location();

  return task;
}

// 1. 选择一个region，需要过滤
// 2. 得到该region的follower节点
// 3. 过滤掉部分follower节点
// 4. 剩下的follower节点按leader_score由小到大排序
// 5. 生成任务
TransferLeaderTaskPtr BalanceLeaderScheduler::GenerateTransferOutLeaderTask(CandidateStoresPtr candidate_stores,
                                                                            const std::set<int64_t>& used_regions) {
  auto source_store_entry = candidate_stores->GetStore();
  auto region = PickOneRegion(FilterRegion(FilterUsedRegion(source_store_entry->LeaderRegionIds(), used_regions)));
  if (region.id() == 0) {
    return nullptr;
  }

  auto follower_store_entries = GetFollowerStores(candidate_stores, region, source_store_entry->Id());
  if (follower_store_entries.empty()) {
    return nullptr;
  }

  // filter store has enough resource
  follower_store_entries = FilterResource(follower_store_entries, region.id());
  if (follower_store_entries.empty()) {
    return nullptr;
  }

  // filter score less than leader
  follower_store_entries = FilterScoreLessThanLeader(source_store_entry, follower_store_entries);
  if (follower_store_entries.empty()) {
    return nullptr;
  }

  for (auto& store_entry : follower_store_entries) {
    auto task = GenerateTransferLeaderTask(region.id(), source_store_entry->Id(), store_entry);
    if (task) {
      return task;
    }
  }

  return nullptr;
}

TransferLeaderTaskPtr BalanceLeaderScheduler::GenerateTransferInLeaderTask(CandidateStoresPtr candidate_stores,
                                                                           const std::set<int64_t>& used_regions) {
  auto target_store_entry = candidate_stores->GetStore();
  auto region = PickOneRegion(FilterRegion(FilterUsedRegion(target_store_entry->FollowerRegionIds(), used_regions)));
  if (region.id() == 0) {
    return nullptr;
  }

  // check store has enough resource
  if (!FilterResource(target_store_entry->Store(), region.id())) {
    return nullptr;
  }

  auto leader_store_entry = GetLeaderStore(candidate_stores, region);
  if (leader_store_entry == nullptr || leader_store_entry->LeaderScore(-1) < target_store_entry->LeaderScore(1)) {
    return nullptr;
  }

  auto task = GenerateTransferLeaderTask(region.id(), leader_store_entry->Id(), target_store_entry);
  if (task) {
    return task;
  }

  return nullptr;
}

bool BalanceLeaderScheduler::FilterStore(dingodb::pb::common::Store& store) {
  for (auto& filter : store_filters_) {
    if (!filter->Check(store)) {
      return true;
    }
  }

  return false;
}

bool BalanceLeaderScheduler::FilterRegion(int64_t region_id) {
  for (auto& filter : region_filters_) {
    if (!filter->Check(region_id)) {
      return true;
    }
  }

  return false;
}

std::vector<int64_t> BalanceLeaderScheduler::FilterRegion(std::vector<int64_t> region_ids) {
  std::vector<int64_t> reserve_region_ids;
  for (auto region_id : region_ids) {
    if (!FilterRegion(region_id)) {
      reserve_region_ids.push_back(region_id);
    }
  }

  return reserve_region_ids;
}

bool BalanceLeaderScheduler::FilterTask(int64_t region_id) {
  for (auto& filter : region_filters_) {
    if (!filter->Check(region_id)) {
      return true;
    }
  }

  return false;
}

std::vector<TransferLeaderTaskPtr> BalanceLeaderScheduler::FilterTask(
    std::vector<TransferLeaderTaskPtr>& transfer_leader_tasks) {
  std::vector<TransferLeaderTaskPtr> reserve_tasks;
  for (auto& task : transfer_leader_tasks) {
    if (!FilterTask(task->region_id)) {
      reserve_tasks.push_back(task);
    }
  }

  return reserve_tasks;
}

bool BalanceLeaderScheduler::FilterResource(const dingodb::pb::common::Store& store, int64_t region_id) {
  for (auto& filter : resource_filters_) {
    if (!filter->Check(store, region_id)) {
      return true;
    }
  }

  return false;
}

std::vector<StoreEntryPtr> BalanceLeaderScheduler::FilterResource(const std::vector<StoreEntryPtr>& store_entries,
                                                                  int64_t region_id) {
  std::vector<StoreEntryPtr> reserve_store_entries;
  for (const auto& store_entry : store_entries) {
    if (!FilterResource(store_entry->Store(), region_id)) {
      reserve_store_entries.push_back(store_entry);
    }
  }

  return reserve_store_entries;
}

}  // namespace balance

}  // namespace dingodb
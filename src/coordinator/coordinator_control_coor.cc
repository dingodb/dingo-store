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

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "braft/configuration.h"
#include "brpc/channel.h"
#include "butil/scoped_lock.h"
#include "butil/strings/string_split.h"
#include "butil/synchronization/lock.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "engine/snapshot.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"

namespace dingodb {

// TODO: add epoch logic
void CoordinatorControl::GetCoordinatorMap(uint64_t cluster_id, uint64_t& epoch, pb::common::Location& leader_location,
                                           std::vector<pb::common::Location>& locations) {
  if (cluster_id < 0) {
    return;
  }
  epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_COORINATOR);
  leader_location.mutable_host()->assign("127.0.0.1");
  leader_location.set_port(19190);

  if (raft_node_ == nullptr) {
    DINGO_LOG(ERROR) << "GetCoordinatorMap raft_node_ is nullptr";
    return;
  }

  std::vector<braft::PeerId> peers;
  raft_node_->ListPeers(&peers);

  // get all server_location from raft_node
  for (const auto& peer : peers) {
    pb::common::Location raft_location;
    pb::common::Location server_location;

    int ret = Helper::PeerIdToLocation(peer, raft_location);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "GetCoordinatorMap cannot transform raft peerid, peerid=" << peer;
      continue;
    }

    GetServerLocation(raft_location, server_location);

    locations.push_back(server_location);
  }

  // get leader server_location from raft_node
  auto leader_peer_id = raft_node_->GetLeaderId();
  pb::common::Location leader_raft_location;

  int ret = Helper::PeerIdToLocation(leader_peer_id, leader_raft_location);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "GetCoordinatorMap cannot transform raft leader peerid, peerid=" << leader_peer_id;
    return;
  }

  GetServerLocation(leader_raft_location, leader_location);
}

void CoordinatorControl::GetStoreMap(pb::common::StoreMap& store_map) {
  uint64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);
  store_map.set_epoch(store_map_epoch);

  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    for (auto& elemnt : store_map_) {
      auto* tmp_region = store_map.add_stores();
      tmp_region->CopyFrom(elemnt.second);
    }
  }
}

void CoordinatorControl::GetStoreMetrics(std::vector<pb::common::StoreMetrics>& store_metrics) {
  BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
  for (auto& elemnt : store_metrics_map_) {
    store_metrics.push_back(elemnt.second);
  }
}

void CoordinatorControl::GetPushStoreMap(butil::FlatMap<uint64_t, pb::common::Store>& store_to_push) {
  BAIDU_SCOPED_LOCK(store_need_push_mutex_);
  store_to_push.swap(store_need_push_);
}

int CoordinatorControl::ValidateStore(uint64_t store_id, const std::string& keyring) {
  if (keyring == std::string("TO_BE_CONTINUED")) {
    DINGO_LOG(INFO) << "ValidateStore store_id=" << store_id << " debug pass with TO_BE_CONTINUED";
    return 0;
  }

  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    auto* store_in_map = store_map_.seek(store_id);
    if (store_in_map != nullptr) {
      if (store_in_map->keyring() == keyring) {
        DINGO_LOG(INFO) << "ValidateStore store_id=" << store_id << " succcess";
        return 0;
      }

      DINGO_LOG(INFO) << "ValidateStore store_id=" << store_id << "keyring wrong fail input_keyring=" << keyring
                      << " correct_keyring=" << store_in_map->keyring();
      return -1;
    }
  }

  DINGO_LOG(INFO) << "ValidateStore store_id=" << store_id << " not exist fail";

  return -1;
}

int CoordinatorControl::CreateStore(uint64_t cluster_id, uint64_t& store_id, std::string& keyring,
                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return -1;
  }

  store_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_STORE, meta_increment);
  keyring = Helper::GenerateRandomString(16);

  pb::common::Store store;
  store.set_id(store_id);
  store.mutable_keyring()->assign(keyring);
  store.set_state(::dingodb::pb::common::StoreState::STORE_NEW);

  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_STORE, meta_increment);
  auto* store_increment = meta_increment.add_stores();
  store_increment->set_id(store_id);
  store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

  auto* store_increment_store = store_increment->mutable_store();
  store_increment_store->CopyFrom(store);

  // on_apply
  // store_map_epoch++;                                  // raft_kv_put
  // store_map_.insert(std::make_pair(store_id, store));  // raft_kv_put
  return 0;
}

int CoordinatorControl::DeleteStore(uint64_t cluster_id, uint64_t store_id, std::string keyring,
                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0 || store_id <= 0 || keyring.length() <= 0) {
    return -1;
  }

  pb::common::Store store_to_delete;
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    auto* temp_store = store_map_.seek(store_id);
    if (temp_store == nullptr) {
      DINGO_LOG(INFO) << "DeleteStore store_id not exists, id=" << store_id;
      return -1;
    }

    store_to_delete = *temp_store;
    if (keyring == store_to_delete.keyring()) {
      DINGO_LOG(INFO) << "DeleteStore store_id id=" << store_id << " keyring not equal, input keyring=" << keyring
                      << " but store's keyring=" << store_to_delete.keyring();
      return -1;
    }
  }

  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_STORE, meta_increment);
  auto* store_increment = meta_increment.add_stores();
  store_increment->set_id(store_id);
  store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

  auto* store_increment_store = store_increment->mutable_store();
  store_increment_store->CopyFrom(store_to_delete);

  // on_apply
  // store_map_epoch++;                                  // raft_kv_put
  // store_map_.insert(std::make_pair(store_id, store));  // raft_kv_put
  return 0;
}
// UpdateStoreMap
uint64_t CoordinatorControl::UpdateStoreMap(const pb::common::Store& store,
                                            pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);

  bool need_update_epoch = false;
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    auto* temp_store = store_map_.seek(store.id());
    if (temp_store != nullptr) {
      if (temp_store->state() != store.state()) {
        DINGO_LOG(INFO) << "STORE STATUS CHANGE store_id = " << store.id() << " old status = " << temp_store->state()
                        << " new status = " << store.state();

        // update meta_increment
        need_update_epoch = true;
        auto* store_increment = meta_increment.add_stores();
        store_increment->set_id(store.id());
        store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        auto* store_increment_store = store_increment->mutable_store();
        store_increment_store->CopyFrom(store);

        // on_apply
        // store_map_epoch++;              // raft_kv_put
        // store_map_[store.id()] = store;  // raft_kv_put
      }
    } else {
      DINGO_LOG(INFO) << "NEED ADD NEW STORE store_id = " << store.id();

      // update meta_increment
      need_update_epoch = true;
      auto* store_increment = meta_increment.add_stores();
      store_increment->set_id(store.id());
      store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

      auto* store_increment_store = store_increment->mutable_store();
      store_increment_store->CopyFrom(store);

      // on_apply
      // store_map_epoch++;                                    // raft_kv_put
      // store_map_.insert(std::make_pair(store.id(), store));  // raft_kv_put
    }
  }

  if (need_update_epoch) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_STORE, meta_increment);
  }

  DINGO_LOG(INFO) << "UpdateStoreMap store_id=" << store.id();

  return store_map_epoch;
}

void CoordinatorControl::GetRegionMap(pb::common::RegionMap& region_map) {
  region_map.set_epoch(GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION));
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    for (auto& elemnt : region_map_) {
      auto* tmp_region = region_map.add_regions();
      tmp_region->CopyFrom(elemnt.second);
    }
  }
}

int CoordinatorControl::CreateRegion(const std::string& region_name, const std::string& resource_tag,
                                     int32_t replica_num, pb::common::Range region_range, uint64_t schema_id,
                                     uint64_t table_id, uint64_t& new_region_id,
                                     pb::coordinator_internal::MetaIncrement& meta_increment) {
  std::vector<pb::common::Store> stores_for_regions;
  std::vector<pb::common::Store> selected_stores_for_regions;

  // when resource_tag exists, select store with resource_tag
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    for (const auto& element : store_map_) {
      const auto& store = element.second;
      if (store.state() != pb::common::StoreState::STORE_NORMAL) {
        continue;
      }

      if (resource_tag.length() == 0) {
        stores_for_regions.push_back(store);
      } else if (store.resource_tag() == resource_tag) {
        stores_for_regions.push_back(store);
      }
    }
  }

  // if not enough stores is selected, return -1
  if (stores_for_regions.size() < replica_num) {
    DINGO_LOG(INFO) << "Not enough stores for create region";
    return -1;
  }

  // select replica_num stores
  // POC version select the first replica_num stores
  selected_stores_for_regions.reserve(replica_num);
  for (int i = 0; i < replica_num; i++) {
    selected_stores_for_regions.push_back(stores_for_regions[i]);
  }

  // generate new region
  uint64_t const create_region_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION, meta_increment);
  if (region_map_.seek(create_region_id) != nullptr) {
    DINGO_LOG(ERROR) << "create_region_id =" << create_region_id << " is illegal, cannot create region!!";
    return -1;
  }

  // create new region in memory
  pb::common::Region new_region;
  new_region.set_id(create_region_id);
  new_region.set_epoch(1);
  new_region.set_name(region_name + std::string("_") + std::to_string(create_region_id));
  new_region.set_state(::dingodb::pb::common::RegionState::REGION_NEW);
  auto* range = new_region.mutable_range();
  range->CopyFrom(region_range);
  // add store_id and its peer location to region
  for (int i = 0; i < replica_num; i++) {
    auto store = selected_stores_for_regions[i];
    auto* peer = new_region.add_peers();
    peer->set_store_id(store.id());
    peer->set_role(::dingodb::pb::common::PeerRole::VOTER);
    peer->mutable_server_location()->CopyFrom(store.server_location());
    peer->mutable_raft_location()->CopyFrom(store.raft_location());
  }

  new_region.set_schema_id(schema_id);
  new_region.set_table_id(table_id);

  // update meta_increment
  auto* region_increment = meta_increment.add_regions();
  region_increment->set_id(create_region_id);
  region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  region_increment->set_table_id(table_id);

  auto* region_increment_region = region_increment->mutable_region();
  region_increment_region->CopyFrom(new_region);

  // on_apply
  // region_map_epoch++;                                               // raft_kv_put
  // region_map_.insert(std::make_pair(create_region_id, new_region));  // raft_kv_put

  new_region_id = create_region_id;

  return 0;
}

int CoordinatorControl::DropRegion(uint64_t region_id, pb::coordinator_internal::MetaIncrement& meta_increment) {
  // set region state to DELETE
  bool need_update_epoch = false;
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    auto* region_to_delete = region_map_.seek(region_id);
    if (region_to_delete != nullptr) {
      region_to_delete->set_state(::dingodb::pb::common::RegionState::REGION_DELETE);

      // update meta_increment
      need_update_epoch = true;
      auto* region_increment = meta_increment.add_regions();
      region_increment->set_id(region_id);
      region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

      auto* region_increment_region = region_increment->mutable_region();
      region_increment_region->CopyFrom(*region_to_delete);

      // on_apply
      // region_map_[region_id].set_state(::dingodb::pb::common::RegionState::REGION_DELETE);
      DINGO_LOG(INFO) << "drop region success, id = " << region_id;
    } else {
      // delete regions on the fly (usually in CreateTable)
      for (int i = 0; i < meta_increment.regions_size(); i++) {
        auto* region_in_increment = meta_increment.mutable_regions(i);
        if (region_in_increment->id() == region_id) {
          region_in_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
        }
      }

      DINGO_LOG(ERROR) << "ERROR drop region id not exists, id = " << region_id;
      return -1;
    }
  }

  if (need_update_epoch) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);
  }

  return 0;
}

// UpdateRegionMap
uint64_t CoordinatorControl::UpdateRegionMap(std::vector<pb::common::Region>& regions,
                                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t region_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION);

  bool need_to_get_next_epoch = false;
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    for (const auto& region : regions) {
      auto* temp_region = region_map_.seek(region.id());
      if (temp_region != nullptr) {
        DINGO_LOG(INFO) << " update region to region_map in heartbeat, region_id=" << region.id();

        // if state not change, just update leader_store_id
        if (temp_region->state() == region.state()) {
          // update the region's leader_store_id, no need to apply raft
          if (temp_region->leader_store_id() != region.leader_store_id()) {
            temp_region->set_leader_store_id(region.leader_store_id());
          }
          continue;
        } else {
          // state not equal, need to update region data and apply raft
          DINGO_LOG(INFO) << "REGION STATUS CHANGE region_id = " << region.id()
                          << " old status = " << region_map_[region.id()].state() << " new status = " << region.state();
          // maybe need to build a state machine here
          // if a region is set to DELETE, it will never be updated to other normal state
          const auto& region_delete_state_name =
              dingodb::pb::common::RegionState_Name(pb::common::RegionState::REGION_DELETE);
          const auto& region_state_in_map = dingodb::pb::common::RegionState_Name(temp_region->state());
          const auto& region_state_in_req = dingodb::pb::common::RegionState_Name(region.state());

          // if store want to update a region state from DELETE_* to other NON DELETE_* state, it is illegal
          if (region_state_in_map.rfind(region_delete_state_name, 0) == 0 &&
              region_state_in_req.rfind(region_delete_state_name, 0) != 0) {
            DINGO_LOG(INFO) << "illegal intend to update region state from REGION_DELETE to " << region_state_in_req
                            << " region_id=" << region.id();
            continue;
          }
        }

        // update meta_increment
        auto* region_increment = meta_increment.add_regions();
        region_increment->set_id(region.id());
        region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        auto* region_increment_region = region_increment->mutable_region();
        region_increment_region->CopyFrom(region);

        need_to_get_next_epoch = true;

        // on_apply
        // region_map_[region.id()].CopyFrom(region);  // raft_kv_put
        // region_map_epoch++;                        // raft_kv_put
      } else if (region.id() == 0) {
        DINGO_LOG(INFO) << " found illegal null region in heartbeat, region_id=0"
                        << " name=" << region.name() << " leader_store_id=" << region.leader_store_id()
                        << " state=" << region.state();
      } else {
        DINGO_LOG(INFO) << " found illegal region in heartbeat, region_id=" << region.id() << " name=" << region.name()
                        << " leader_store_id=" << region.leader_store_id() << " state=" << region.state();

        auto* region_increment = meta_increment.add_regions();
        region_increment->set_id(region.id());
        region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

        auto* region_increment_region = region_increment->mutable_region();
        region_increment_region->CopyFrom(region);
        region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_ILLEGAL);

        need_to_get_next_epoch = true;

        // region_map_.insert(std::make_pair(region.id(), region));  // raft_kv_put
      }
    }
  }

  if (need_to_get_next_epoch) {
    region_map_epoch = GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);
  }

  DINGO_LOG(INFO) << "UpdateRegionMapMulti epoch=" << region_map_epoch;

  return region_map_epoch;
}

void CoordinatorControl::GetExecutorMap(pb::common::ExecutorMap& executor_map) {
  uint64_t executor_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR);
  executor_map.set_epoch(executor_map_epoch);
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    for (auto& elemnt : executor_map_) {
      auto* tmp_region = executor_map.add_executors();
      tmp_region->CopyFrom(elemnt.second);
    }
  }
}

void CoordinatorControl::GetPushExecutorMap(butil::FlatMap<uint64_t, pb::common::Executor>& executor_to_push) {
  BAIDU_SCOPED_LOCK(executor_need_push_mutex_);
  executor_to_push.swap(executor_need_push_);
}

int CoordinatorControl::ValidateExecutor(uint64_t executor_id, const std::string& keyring) {
  if (keyring == std::string("TO_BE_CONTINUED")) {
    DINGO_LOG(INFO) << "ValidateExecutor executor_id=" << executor_id << " debug pass with TO_BE_CONTINUED";
    return 0;
  }

  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    auto* executor_in_map = executor_map_.seek(executor_id);
    if (executor_in_map != nullptr) {
      if (executor_in_map->keyring() == keyring) {
        DINGO_LOG(INFO) << "ValidateExecutor executor_id=" << executor_id << " succcess";
        return 0;
      }

      DINGO_LOG(INFO) << "ValidateExecutor executor_id=" << executor_id
                      << "keyring wrong fail input_keyring=" << keyring
                      << " correct_keyring=" << executor_in_map->keyring();
      return -1;
    }
  }

  DINGO_LOG(INFO) << "ValidateExecutor executor_id=" << executor_id << " not exist fail";

  return -1;
}

int CoordinatorControl::CreateExecutor(uint64_t cluster_id, uint64_t& executor_id, std::string& keyring,
                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return -1;
  }

  executor_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR, meta_increment);
  keyring = Helper::GenerateRandomString(16);

  pb::common::Executor executor;
  executor.set_id(executor_id);
  executor.mutable_keyring()->assign(keyring);
  executor.set_state(::dingodb::pb::common::ExecutorState::EXECUTOR_NEW);

  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executors();
  executor_increment->set_id(executor_id);
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

  auto* executor_increment_executor = executor_increment->mutable_executor();
  executor_increment_executor->CopyFrom(executor);

  // on_apply
  // executor_map_epoch++;                                  // raft_kv_put
  // executor_map_.insert(std::make_pair(executor_id, executor));  // raft_kv_put
  return 0;
}

int CoordinatorControl::DeleteExecutor(uint64_t cluster_id, uint64_t executor_id, std::string keyring,
                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0 || executor_id <= 0 || keyring.length() <= 0) {
    return -1;
  }

  pb::common::Executor executor_to_delete;
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    auto* temp_executor = executor_map_.seek(executor_id);
    if (temp_executor == nullptr) {
      DINGO_LOG(INFO) << "DeleteExecutor executor_id not exists, id=" << executor_id;
      return -1;
    }

    executor_to_delete = *temp_executor;
    if (keyring == executor_to_delete.keyring()) {
      DINGO_LOG(INFO) << "DeleteExecutor executor_id id=" << executor_id
                      << " keyring not equal, input keyring=" << keyring
                      << " but executor's keyring=" << executor_to_delete.keyring();
      return -1;
    }
  }

  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executors();
  executor_increment->set_id(executor_id);
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

  auto* executor_increment_executor = executor_increment->mutable_executor();
  executor_increment_executor->CopyFrom(executor_to_delete);

  // on_apply
  // executor_map_epoch++;                                  // raft_kv_put
  // executor_map_.insert(std::make_pair(executor_id, executor));  // raft_kv_put
  return 0;
}

uint64_t CoordinatorControl::UpdateExecutorMap(const pb::common::Executor& executor,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t executor_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR);

  bool need_update_epoch = false;
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    auto* temp_executor = executor_map_.seek(executor.id());
    // if (executor_map_.find(executor.id()) != executor_map_.end()) {
    if (temp_executor != nullptr) {
      if (temp_executor->state() != executor.state()) {
        DINGO_LOG(INFO) << "executor STATUS CHANGE executor_id = " << executor.id()
                        << " old status = " << temp_executor->state() << " new status = " << executor.state();

        // update meta_increment
        need_update_epoch = true;
        auto* executor_increment = meta_increment.add_executors();
        executor_increment->set_id(executor.id());
        executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        auto* executor_increment_executor = executor_increment->mutable_executor();
        executor_increment_executor->CopyFrom(executor);

        // on_apply
        // executor_map_epoch++;              // raft_kv_put
        // executor_map_[executor.id()] = executor;  // raft_kv_put
      }
    } else {
      DINGO_LOG(INFO) << "NEED ADD NEW executor executor_id = " << executor.id();

      // update meta_increment
      need_update_epoch = true;
      auto* executor_increment = meta_increment.add_executors();
      executor_increment->set_id(executor.id());
      executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

      auto* executor_increment_executor = executor_increment->mutable_executor();
      executor_increment_executor->CopyFrom(executor);

      // on_apply
      // executor_map_epoch++;                                    // raft_kv_put
      // executor_map_.insert(std::make_pair(executor.id(), executor));  // raft_kv_put
    }
  }

  if (need_update_epoch) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  }

  DINGO_LOG(INFO) << "UpdateExecutorMap executor_id=" << executor.id();

  return executor_map_epoch;
}

uint64_t CoordinatorControl::UpdateStoreMetrics(const pb::common::StoreMetrics& store_metrics,
                                                pb::coordinator_internal::MetaIncrement& meta_increment) {
  //   uint64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);
  if (store_metrics.id() <= 0) {
    DINGO_LOG(ERROR) << "ERROR: UpdateStoreMetrics store_metrics.id() <= 0, store_metrics.id() = "
                     << store_metrics.id();
    return -1;
  }

  bool need_update_epoch = false;
  {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    if (store_metrics_map_.seek(store_metrics.id()) != nullptr) {
      DINGO_LOG(DEBUG) << "STORE METIRCS UPDATE store_metrics.id = " << store_metrics.id();

      // update meta_increment
      need_update_epoch = true;
      auto* store_metrics_increment = meta_increment.add_store_metrics();
      store_metrics_increment->set_id(store_metrics.id());
      store_metrics_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

      auto* store_metrics_increment_store = store_metrics_increment->mutable_store_metrics();
      store_metrics_increment_store->CopyFrom(store_metrics);

      // on_apply
      // store_metrics_map_epoch++;              // raft_kv_put
      // store_metrics_map_[store_metrics.id()] = store;  // raft_kv_put
    } else {
      DINGO_LOG(INFO) << "NEED ADD NEW STORE store_metrics.id = " << store_metrics.id();

      // update meta_increment
      need_update_epoch = true;
      auto* store_metrics_increment = meta_increment.add_store_metrics();
      store_metrics_increment->set_id(store_metrics.id());
      store_metrics_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

      auto* store_metrics_increment_store = store_metrics_increment->mutable_store_metrics();
      store_metrics_increment_store->CopyFrom(store_metrics);

      // on_apply
      // store_metrics_map_epoch++;                                    // raft_kv_put
      // store_metrics_map_.insert(std::make_pair(store_metrics.id(), store));  // raft_kv_put
    }
  }

  //   if (need_update_epoch) {
  //     GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_STORE, meta_increment);
  //   }

  DINGO_LOG(INFO) << "UpdateStoreMetricsMap store_metrics.id=" << store_metrics.id();

  return 0;
}

void CoordinatorControl::GetMemoryInfo(pb::coordinator::CoordinatorMemoryInfo& memory_info) {
  // compute size
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_temp_mutex_);

    // set count & size
    memory_info.set_id_epoch_map_temp_count(id_epoch_map_temp_.size());
    for (auto& it : id_epoch_map_temp_) {
      memory_info.set_id_epoch_map_temp_size(memory_info.id_epoch_map_temp_size() + sizeof(it.first) +
                                             it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.id_epoch_map_temp_size());
  }
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);

    // set term & index
    auto* temp_index = id_epoch_map_.seek(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX);
    auto* temp_term = id_epoch_map_.seek(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM);

    // if (id_epoch_map_.find(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM) != id_epoch_map_.end()) {
    if (temp_term != nullptr) {
      // memory_info.set_applied_term(id_epoch_map_.at(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM).value());
      memory_info.set_applied_term(temp_term->value());
    }
    // if (id_epoch_map_.find(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX) != id_epoch_map_.end()) {
    if (temp_index != nullptr) {
      // memory_info.set_applied_index(id_epoch_map_.at(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX).value());
      memory_info.set_applied_index(temp_index->value());
    }

    // set count & size
    memory_info.set_id_epoch_map_count(id_epoch_map_.size());
    for (auto& it : id_epoch_map_) {
      memory_info.set_id_epoch_map_size(memory_info.id_epoch_map_size() + sizeof(it.first) + it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.id_epoch_map_size());

    // dump id & epoch to kv
    for (auto& it : id_epoch_map_) {
      const google::protobuf::EnumDescriptor* enum_descriptor =
          dingodb::pb::coordinator_internal::IdEpochType_descriptor();
      const google::protobuf::EnumValueDescriptor* enum_value_descriptor = enum_descriptor->FindValueByNumber(it.first);
      std::string name = enum_value_descriptor->name();

      auto* id_epoch = memory_info.add_id_epoch_values();
      id_epoch->set_key(name);
      id_epoch->set_value(std::to_string(it.second.value()));
    }
  }
  {
    BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    memory_info.set_coordinator_map_count(coordinator_map_.size());
    for (auto& it : coordinator_map_) {
      memory_info.set_coordinator_map_size(memory_info.coordinator_map_size() + sizeof(it.first) +
                                           it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.coordinator_map_size());
  }
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    memory_info.set_store_map_count(store_map_.size());
    for (auto& it : store_map_) {
      memory_info.set_store_map_size(memory_info.store_map_size() + sizeof(it.first) + it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.store_map_size());
  }
  {
    BAIDU_SCOPED_LOCK(store_need_push_mutex_);
    memory_info.set_store_need_push_count(store_need_push_.size());
    for (auto& it : store_need_push_) {
      memory_info.set_store_need_push_size(memory_info.store_need_push_size() + sizeof(it.first) +
                                           it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.store_need_push_size());
  }
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    memory_info.set_executor_map_count(executor_map_.size());
    for (auto& it : executor_map_) {
      memory_info.set_executor_map_size(memory_info.executor_map_size() + sizeof(it.first) + it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.executor_map_size());
  }
  {
    BAIDU_SCOPED_LOCK(executor_need_push_mutex_);
    memory_info.set_executor_need_push_count(executor_need_push_.size());
    for (auto& it : executor_need_push_) {
      memory_info.set_executor_need_push_size(memory_info.executor_need_push_size() + sizeof(it.first) +
                                              it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.executor_need_push_size());
  }
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    memory_info.set_schema_map_count(schema_map_.size());
    for (auto& it : schema_map_) {
      memory_info.set_schema_map_size(memory_info.schema_map_size() + sizeof(it.first) + it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.schema_map_size());
  }
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    memory_info.set_region_map_count(region_map_.size());
    for (auto& it : region_map_) {
      memory_info.set_region_map_size(memory_info.region_map_size() + sizeof(it.first) + it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.region_map_size());
  }
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    memory_info.set_table_map_count(table_map_.size());
    for (auto& it : table_map_) {
      memory_info.set_table_map_size(memory_info.table_map_size() + sizeof(it.first) + it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.table_map_size());
  }
  {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    memory_info.set_store_metrics_map_count(store_metrics_map_.size());
    for (auto& it : store_metrics_map_) {
      memory_info.set_store_metrics_map_size(memory_info.store_metrics_map_size() + sizeof(it.first) +
                                             it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.store_metrics_map_size());
  }
  {
    BAIDU_SCOPED_LOCK(table_metrics_map_mutex_);
    memory_info.set_table_metrics_map_count(table_metrics_map_.size());
    for (auto& it : table_metrics_map_) {
      memory_info.set_table_metrics_map_size(memory_info.table_metrics_map_size() + sizeof(it.first) +
                                             it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.table_metrics_map_size());
  }
}

}  // namespace dingodb

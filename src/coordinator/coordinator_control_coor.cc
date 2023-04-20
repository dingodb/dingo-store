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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "braft/configuration.h"
#include "brpc/channel.h"
#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/strings/string_split.h"
#include "butil/synchronization/lock.h"
#include "butil/time.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "engine/snapshot.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
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
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    butil::FlatMap<uint64_t, pb::common::Store> store_map_copy;
    store_map_copy.init(100);
    store_map_.GetFlatMapCopy(store_map_copy);

    for (auto& element : store_map_copy) {
      auto* tmp_region = store_map.add_stores();
      tmp_region->CopyFrom(element.second);
    }
  }
}

void CoordinatorControl::GetStoreMetrics(std::vector<pb::common::StoreMetrics>& store_metrics) {
  BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
  for (auto& elemnt : store_metrics_map_) {
    store_metrics.push_back(elemnt.second);
  }
}

pb::error::Errno CoordinatorControl::GetOrphanRegion(uint64_t store_id,
                                                     std::map<uint64_t, pb::common::RegionMetrics>& orphan_regions) {
  BAIDU_SCOPED_LOCK(this->store_metrics_map_mutex_);

  for (const auto& it : this->store_metrics_map_) {
    if (it.first != store_id && store_id != 0) {
      continue;
    }

    const auto& store_metrics = it.second;
    for (const auto& region : store_metrics.region_metrics_map()) {
      if (region.second.store_region_state() == pb::common::StoreRegionState::ORPHAN) {
        orphan_regions.insert(std::make_pair(it.first, region.second));
      }
    }
  }

  return pb::error::Errno::OK;
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
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    pb::common::Store store_in_map;
    int ret = store_map_.Get(store_id, store_in_map);

    // auto* store_in_map = store_map_.seek(store_id);
    if (ret > 0) {
      if (store_in_map.keyring() == keyring) {
        DINGO_LOG(INFO) << "ValidateStore store_id=" << store_id << " succcess";
        return 0;
      }

      DINGO_LOG(INFO) << "ValidateStore store_id=" << store_id << "keyring wrong fail input_keyring=" << keyring
                      << " correct_keyring=" << store_in_map.keyring();
      return -1;
    }
  }

  DINGO_LOG(INFO) << "ValidateStore store_id=" << store_id << " not exist fail";

  return -1;
}

pb::error::Errno CoordinatorControl::CreateStore(uint64_t cluster_id, uint64_t& store_id, std::string& keyring,
                                                 pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  store_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_STORE, meta_increment);
  keyring = Helper::GenerateRandomString(16);

  pb::common::Store store;
  store.set_id(store_id);
  store.set_keyring(keyring);
  store.set_state(::dingodb::pb::common::StoreState::STORE_NEW);
  store.set_in_state(::dingodb::pb::common::StoreInState::STORE_IN);
  store.set_create_timestamp(butil::gettimeofday_ms());

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
  return pb::error::Errno::OK;
}

pb::error::Errno CoordinatorControl::DeleteStore(uint64_t cluster_id, uint64_t store_id, std::string keyring,
                                                 pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0 || store_id <= 0 || keyring.length() <= 0) {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  pb::common::Store store_to_delete;
  {
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    int ret = store_map_.Get(store_id, store_to_delete);
    if (ret < 0) {
      DINGO_LOG(INFO) << "DeleteStore store_id not exists, id=" << store_id;
      return pb::error::Errno::ESTORE_NOT_FOUND;
    }

    if (keyring != store_to_delete.keyring()) {
      DINGO_LOG(INFO) << "DeleteStore store_id id=" << store_id << " keyring not equal, input keyring=" << keyring
                      << " but store's keyring=" << store_to_delete.keyring();
      return pb::error::Errno::EKEYRING_ILLEGAL;
    }

    if (store_to_delete.state() != pb::common::StoreState::STORE_OFFLINE &&
        store_to_delete.state() != pb::common::StoreState::STORE_NEW) {
      DINGO_LOG(INFO) << "DeleteStore store_id id=" << store_id << " already deleted";
      return pb::error::Errno::ESTORE_IN_USE;
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
  return pb::error::Errno::OK;
}

// UpdateStoreMap
uint64_t CoordinatorControl::UpdateStoreMap(const pb::common::Store& store,
                                            pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);

  bool need_update_epoch = false;
  {
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    pb::common::Store store_to_update;
    int ret = store_map_.Get(store.id(), store_to_update);
    if (ret > 0) {
      if (store_to_update.state() == pb::common::StoreState::STORE_NEW) {
        // this is a new store's first heartbeat
        // so we need to update the store's state to STORE_NORMAL
        // and update the store's server_location
        // and update the store's raft_location
        // and update the store's last_seen_timestamp
        DINGO_LOG(INFO) << "STORE STATUS CHANGE store_id = " << store.id()
                        << " old status = " << store_to_update.state() << " new status = " << store.state();

        // update meta_increment
        need_update_epoch = true;
        auto* store_increment = meta_increment.add_stores();
        store_increment->set_id(store.id());
        store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        auto* store_increment_store = store_increment->mutable_store();
        store_increment_store->CopyFrom(store_to_update);  // only update server_location & raft_location & state

        // only update server_location & raft_location & state & last_seen_timestamp
        store_increment_store->mutable_server_location()->CopyFrom(store.server_location());
        store_increment_store->mutable_raft_location()->CopyFrom(store.raft_location());
        store_increment_store->set_state(pb::common::StoreState::STORE_NORMAL);
        store_increment_store->set_last_seen_timestamp(butil::gettimeofday_ms());
      } else {
        // this is normall heartbeat,
        // so only need to update state & last_seen_timestamp, no need to update epoch
        auto* store_increment = meta_increment.add_stores();
        store_increment->set_id(store.id());
        store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        auto* store_increment_store = store_increment->mutable_store();
        store_increment_store->CopyFrom(store_to_update);  // only update server_location & raft_location & state

        // only update state & last_seen_timestamp
        store_increment_store->set_state(pb::common::StoreState::STORE_NORMAL);
        store_increment_store->set_last_seen_timestamp(butil::gettimeofday_ms());
      }
    } else {
      // this is a special new store's first heartbeat
      // only store using keyring=TO_BE_CONTINUED can get into this branch
      // so we just add this store into store_map_
      DINGO_LOG(INFO) << "NEED ADD NEW STORE store_id = " << store.id();

      // update meta_increment
      need_update_epoch = true;
      auto* store_increment = meta_increment.add_stores();
      store_increment->set_id(store.id());
      store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

      auto* store_increment_store = store_increment->mutable_store();
      store_increment_store->CopyFrom(store);
      store_increment_store->set_state(pb::common::StoreState::STORE_NORMAL);
      store_increment_store->set_last_seen_timestamp(butil::gettimeofday_ms());

      // setup create_timestamp
      store_increment_store->set_create_timestamp(butil::gettimeofday_ms());
    }
  }

  if (need_update_epoch) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_STORE, meta_increment);
  }

  DINGO_LOG(INFO) << "UpdateStoreMap store_id=" << store.id();

  return store_map_epoch;
}

bool CoordinatorControl::TrySetRegionToDown(uint64_t region_id) {
  pb::common::Region region_to_update;
  int ret = region_map_.Get(region_id, region_to_update);
  if (ret > 0) {
    if (region_to_update.state() != pb::common::RegionState::REGION_NEW &&
        region_to_update.last_update_timestamp() + (60 * 1000) < butil::gettimeofday_ms()) {
      // update region's state to REGION_DOWN
      region_to_update.set_state(pb::common::RegionState::REGION_DOWN);
      region_map_.Put(region_id, region_to_update);
      return true;
    }
  }
  return false;
}

bool CoordinatorControl::TrySetStoreToOffline(uint64_t store_id) {
  pb::common::Store store_to_update;
  int ret = store_map_.Get(store_id, store_to_update);
  if (ret > 0) {
    if (store_to_update.state() == pb::common::StoreState::STORE_NORMAL &&
        store_to_update.last_seen_timestamp() + (60 * 1000) < butil::gettimeofday_ms()) {
      // update store's state to STORE_OFFLINE
      store_to_update.set_state(pb::common::StoreState::STORE_OFFLINE);
      store_map_.Put(store_id, store_to_update);
      return true;
    }
  }
  return false;
}

bool CoordinatorControl::TrySetExecutorToOffline(std::string executor_id) {
  pb::common::Executor executor_to_update;
  int ret = executor_map_.Get(executor_id, executor_to_update);
  if (ret > 0) {
    if (executor_to_update.state() == pb::common::ExecutorState::EXECUTOR_NORMAL &&
        executor_to_update.last_seen_timestamp() + (60 * 1000) < butil::gettimeofday_ms()) {
      // update executor's state to EXECUTOR_OFFLINE
      executor_to_update.set_state(pb::common::ExecutorState::EXECUTOR_OFFLINE);
      executor_map_.Put(executor_id, executor_to_update);
      return true;
    }
  }
  return false;
}

void CoordinatorControl::GetRegionMap(pb::common::RegionMap& region_map) {
  region_map.set_epoch(GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION));
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    butil::FlatMap<uint64_t, pb::common::Region> region_map_copy;
    region_map_copy.init(30000);
    region_map_.GetFlatMapCopy(region_map_copy);
    for (auto& elemnt : region_map_copy) {
      auto* tmp_region = region_map.add_regions();
      tmp_region->CopyFrom(elemnt.second);
    }
  }
}

pb::error::Errno CoordinatorControl::QueryRegion(uint64_t region_id, pb::common::Region& region) {
  if (region_id <= 0) {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  int ret = region_map_.Get(region_id, region);
  if (ret < 0) {
    DINGO_LOG(INFO) << "QueryRegion region_id not exists, id=" << region_id;
    return pb::error::Errno::EREGION_NOT_FOUND;
  }

  return pb::error::Errno::OK;
}

pb::error::Errno CoordinatorControl::CreateRegionForSplit(const std::string& region_name,
                                                          const std::string& resource_tag, int32_t replica_num,
                                                          pb::common::Range region_range, uint64_t schema_id,
                                                          uint64_t table_id, uint64_t split_from_region_id,
                                                          uint64_t& new_region_id,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  std::vector<uint64_t> store_ids;

  pb::common::Region split_from_region;
  int ret = region_map_.Get(split_from_region_id, split_from_region);
  if (ret < 0) {
    DINGO_LOG(INFO) << "CreateRegionForSplit region_id not exists, id=" << split_from_region_id;
    return pb::error::Errno::EREGION_NOT_FOUND;
  }

  for (const auto& peer : split_from_region.definition().peers()) {
    store_ids.push_back(peer.store_id());
  }

  // create region with split_from_region_id & store_ids
  return CreateRegion(region_name, resource_tag, replica_num, region_range, schema_id, table_id, store_ids,
                      split_from_region_id, new_region_id, meta_increment);
}

pb::error::Errno CoordinatorControl::CreateRegion(const std::string& region_name, const std::string& resource_tag,
                                                  int32_t replica_num, pb::common::Range region_range,
                                                  uint64_t schema_id, uint64_t table_id, uint64_t& new_region_id,
                                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  std::vector<uint64_t> store_ids;
  return CreateRegion(region_name, resource_tag, replica_num, region_range, schema_id, table_id, store_ids, 0,
                      new_region_id, meta_increment);
}

pb::error::Errno CoordinatorControl::CreateRegion(const std::string& region_name, const std::string& resource_tag,
                                                  int32_t replica_num, pb::common::Range region_range,
                                                  uint64_t schema_id, uint64_t table_id,
                                                  std::vector<uint64_t>& store_ids, uint64_t split_from_region_id,
                                                  uint64_t& new_region_id,
                                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  std::vector<pb::common::Store> stores_for_regions;
  std::vector<pb::common::Store> selected_stores_for_regions;

  // if store_ids is not null, select store with store_ids
  // or when resource_tag exists, select store with resource_tag
  butil::FlatMap<uint64_t, pb::common::Store> store_map_copy;
  store_map_copy.init(100);
  store_map_.GetFlatMapCopy(store_map_copy);

  // select store for region
  if (store_ids.empty()) {
    for (const auto& element : store_map_copy) {
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
  } else {
    for (const auto& element : store_map_copy) {
      const auto& store = element.second;
      if (store.state() != pb::common::StoreState::STORE_NORMAL) {
        continue;
      }

      for (const auto& store_id : store_ids) {
        if (store.id() == store_id) {
          stores_for_regions.push_back(store);
          break;
        }
      }
    }
  }

  // if not enough stores is selected, return -1
  if (stores_for_regions.size() < replica_num) {
    DINGO_LOG(INFO) << "Not enough stores for create region";
    return pb::error::Errno::EREGION_UNAVAILABLE;
  }

  // select replica_num stores
  // POC version select the first replica_num stores
  selected_stores_for_regions.reserve(replica_num);
  for (int i = 0; i < replica_num; i++) {
    selected_stores_for_regions.push_back(stores_for_regions[i]);
  }

  // generate new region
  uint64_t const create_region_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION, meta_increment);
  if (region_map_.Exists(create_region_id)) {
    DINGO_LOG(ERROR) << "create_region_id =" << create_region_id << " is illegal, cannot create region!!";
    return pb::error::Errno::EREGION_UNAVAILABLE;
  }

  // create new region in memory begin
  pb::common::Region new_region;
  new_region.set_id(create_region_id);
  new_region.set_epoch(1);
  // new_region.set_name(region_name + std::string("_") + std::to_string(create_region_id));
  new_region.set_state(::dingodb::pb::common::RegionState::REGION_NEW);
  new_region.set_create_timestamp(butil::gettimeofday_ms());
  // auto* range = new_region.mutable_range();
  // range->CopyFrom(region_range);
  // add store_id and its peer location to region
  // for (int i = 0; i < replica_num; i++) {
  //   auto store = selected_stores_for_regions[i];
  //   auto* peer = new_region.add_peers();
  //   peer->set_store_id(store.id());
  //   peer->set_role(::dingodb::pb::common::PeerRole::VOTER);
  //   peer->mutable_server_location()->CopyFrom(store.server_location());
  //   peer->mutable_raft_location()->CopyFrom(store.raft_location());
  // }

  // new_region.set_schema_id(schema_id);
  // new_region.set_table_id(table_id);

  // create region definition begin
  auto* region_definition = new_region.mutable_metrics()->mutable_region_definition();
  region_definition->set_id(create_region_id);
  region_definition->set_name(region_name + std::string("_") + std::to_string(create_region_id));
  region_definition->set_epoch(1);
  region_definition->set_schema_id(schema_id);
  region_definition->set_table_id(table_id);
  auto* range_in_definition = region_definition->mutable_range();
  range_in_definition->CopyFrom(region_range);

  // add store_id and its peer location to region
  for (int i = 0; i < replica_num; i++) {
    auto store = selected_stores_for_regions[i];
    auto* peer = region_definition->add_peers();
    peer->set_store_id(store.id());
    peer->set_role(::dingodb::pb::common::PeerRole::VOTER);
    peer->mutable_server_location()->CopyFrom(store.server_location());
    peer->mutable_raft_location()->CopyFrom(store.raft_location());
  }

  new_region.mutable_definition()->CopyFrom(*region_definition);
  // create region definition end
  // create new region in memory end

  // create store operations
  std::vector<pb::coordinator::StoreOperation> store_operations;
  for (int i = 0; i < replica_num; i++) {
    auto store = selected_stores_for_regions[i];
    pb::coordinator::StoreOperation store_operation;

    store_operation.set_id(store.id());
    auto* region_cmd = store_operation.add_region_cmds();
    region_cmd->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
    region_cmd->set_create_timestamp(butil::gettimeofday_ms());
    region_cmd->set_region_id(create_region_id);
    region_cmd->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_CREATE);
    auto* create_request = region_cmd->mutable_create_request();
    create_request->mutable_region_definition()->CopyFrom(*region_definition);
    create_request->set_split_from_region_id(
        split_from_region_id);  // setup split_from_region_id for creating sub region for split

    store_operations.push_back(store_operation);
  }

  // update meta_increment
  auto* region_increment = meta_increment.add_regions();
  region_increment->set_id(create_region_id);
  region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  region_increment->set_table_id(table_id);

  auto* region_increment_region = region_increment->mutable_region();
  region_increment_region->CopyFrom(new_region);

  // add store operations to meta_increment
  for (const auto& store_operation : store_operations) {
    auto* store_operation_increment = meta_increment.add_store_operations();
    store_operation_increment->set_id(store_operation.id());
    store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
    store_operation_increment->mutable_store_operation()->CopyFrom(store_operation);

    DINGO_LOG(INFO) << "store_operation_increment = " << store_operation_increment->DebugString();
  }

  // on_apply
  // region_map_epoch++;                                               // raft_kv_put
  // region_map_.insert(std::make_pair(create_region_id, new_region));  // raft_kv_put

  new_region_id = create_region_id;

  return pb::error::Errno::OK;
}

pb::error::Errno CoordinatorControl::DropRegion(uint64_t region_id,
                                                pb::coordinator_internal::MetaIncrement& meta_increment) {
  // set region state to DELETE
  bool need_update_epoch = false;
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    pb::common::Region region_to_delete;
    int ret = region_map_.Get(region_id, region_to_delete);
    if (ret > 0) {
      if (region_to_delete.state() != ::dingodb::pb::common::RegionState::REGION_DELETED &&
          region_to_delete.state() != ::dingodb::pb::common::RegionState::REGION_DELETE &&
          region_to_delete.state() != ::dingodb::pb::common::RegionState::REGION_DELETING) {
        region_to_delete.set_state(::dingodb::pb::common::RegionState::REGION_DELETE);

        // update meta_increment
        // update region to DELETE, not delete region really, not
        need_update_epoch = true;
        auto* region_increment = meta_increment.add_regions();
        region_increment->set_id(region_id);
        region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        auto* region_increment_region = region_increment->mutable_region();
        region_increment_region->CopyFrom(region_to_delete);
        region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_DELETE);

        // add store operations to meta_increment
        // for (int i = 0; i < region_to_delete.peers_size(); i++) {
        //   auto* peer = region_to_delete.mutable_peers(i);
        for (const auto& peer : region_to_delete.definition().peers()) {
          pb::coordinator::StoreOperation store_operation;
          store_operation.set_id(peer.store_id());
          auto* region_cmd = store_operation.add_region_cmds();
          region_cmd->set_region_id(region_id);
          region_cmd->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_DELETE);
          auto* delete_request = region_cmd->mutable_delete_request();
          delete_request->set_region_id(region_id);

          auto* store_operation_increment = meta_increment.add_store_operations();
          store_operation_increment->set_id(store_operation.id());
          store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
          store_operation_increment->mutable_store_operation()->CopyFrom(store_operation);
        }

        // on_apply
        // region_map_[region_id].set_state(::dingodb::pb::common::RegionState::REGION_DELETE);
        DINGO_LOG(INFO) << "drop region success, id = " << region_id;
      }
    } else {
      // delete regions on the fly (usually in CreateTable)
      for (int i = 0; i < meta_increment.regions_size(); i++) {
        auto* region_in_increment = meta_increment.mutable_regions(i);
        if (region_in_increment->id() == region_id) {
          region_in_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
        }
      }

      DINGO_LOG(ERROR) << "ERROR drop region id not exists, id = " << region_id;
      return pb::error::Errno::EREGION_NOT_FOUND;
    }
  }

  if (need_update_epoch) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);
  }

  return pb::error::Errno::OK;
}

// DropRegionPermanently
// delete region from disk
pb::error::Errno CoordinatorControl::DropRegionPermanently(uint64_t region_id,
                                                           pb::coordinator_internal::MetaIncrement& meta_increment) {
  // set region state to DELETE
  bool need_update_epoch = false;
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    pb::common::Region region_to_delete;
    int ret = region_map_.Get(region_id, region_to_delete);
    if (ret < 0) {
      DINGO_LOG(INFO) << "DropRegionPermanently region not exists, id = " << region_id;
      return pb::error::Errno::EREGION_NOT_FOUND;
    }

    // if region is dropped, do real delete
    if (region_to_delete.state() != ::dingodb::pb::common::RegionState::REGION_DELETE ||
        region_to_delete.state() != ::dingodb::pb::common::RegionState::REGION_DELETE ||
        region_to_delete.state() != ::dingodb::pb::common::RegionState::REGION_DELETING) {
      region_to_delete.set_state(::dingodb::pb::common::RegionState::REGION_DELETE);

      // update meta_increment
      // update region to DELETE, not delete region really, not
      need_update_epoch = true;
      auto* region_increment = meta_increment.add_regions();
      region_increment->set_id(region_id);
      region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

      auto* region_increment_region = region_increment->mutable_region();
      region_increment_region->CopyFrom(region_to_delete);
      region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_DELETE);

      // on_apply
      // region_map_[region_id].set_state(::dingodb::pb::common::RegionState::REGION_DELETE);
      DINGO_LOG(INFO) << "DropRegionPermanently drop region success, id = " << region_id;
    }
  }

  if (need_update_epoch) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);
  }

  return pb::error::Errno::OK;
}

pb::error::Errno CoordinatorControl::SplitRegion(uint64_t split_from_region_id, uint64_t split_to_region_id,
                                                 std::string split_watershed_key,
                                                 pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate split_from_region_id
  pb::common::Region split_from_region;
  int ret = region_map_.Get(split_from_region_id, split_from_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "SplitRegion from region not exists, id = " << split_from_region_id;
    return pb::error::Errno::EREGION_NOT_FOUND;
  }

  // validate split_to_region_id
  pb::common::Region split_to_region;
  ret = region_map_.Get(split_to_region_id, split_to_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "SplitRegion to region not exists, id = " << split_from_region_id;
    return pb::error::Errno::EREGION_NOT_FOUND;
  }

  // validate split_watershed_key
  if (split_watershed_key.empty()) {
    DINGO_LOG(ERROR) << "SplitRegion split_watershed_key is empty";
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // validate split_from_region_id and split_to_region_id
  if (split_from_region_id == split_to_region_id) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region_id == split_to_region_id";
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // validate split_from_region and split_to_region has same peers
  if (split_from_region.definition().peers_size() != split_to_region.definition().peers_size()) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region and split_to_region has different peers size";
    return pb::error::Errno::ESPLIT_PEER_NOT_MATCH;
  }

  std::vector<uint64_t> split_from_region_peers;
  std::vector<uint64_t> split_to_region_peers;
  split_from_region_peers.reserve(split_from_region.definition().peers_size());
  for (int i = 0; i < split_from_region.definition().peers_size(); i++) {
    split_from_region_peers.push_back(split_to_region.definition().peers(i).store_id());
  }
  split_to_region_peers.reserve(split_to_region.definition().peers_size());
  for (int i = 0; i < split_to_region.definition().peers_size(); i++) {
    split_to_region_peers.push_back(split_to_region.definition().peers(i).store_id());
  }
  std::sort(split_from_region_peers.begin(), split_from_region_peers.end());
  std::sort(split_to_region_peers.begin(), split_to_region_peers.end());

  bool equal =
      std::equal(split_from_region_peers.begin(), split_from_region_peers.end(), split_to_region_peers.begin());
  if (!equal) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region and split_to_region has different peers";
    return pb::error::Errno::ESPLIT_PEER_NOT_MATCH;
  }

  // validate split_from_region and split_to_region has NORMAL status
  if (split_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      split_to_region.state() != ::dingodb::pb::common::RegionState::REGION_STANDBY) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region or split_to_region is not ready for split, "
                        "split_from_region_id = "
                     << split_from_region_id << " from_state=" << split_from_region.state()
                     << ", split_to_region_id = " << split_to_region_id << " to_state=" << split_to_region.state();
    return pb::error::Errno::ESPLIT_STATUS_ILLEGAL;
  }

  // generate store operation for stores
  pb::coordinator::RegionCmd region_cmd;
  region_cmd.set_region_id(split_from_region_id);
  region_cmd.set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_SPLIT);
  region_cmd.mutable_split_request()->set_split_watershed_key(split_watershed_key);
  region_cmd.mutable_split_request()->set_split_from_region_id(split_from_region_id);
  region_cmd.mutable_split_request()->set_split_to_region_id(split_to_region_id);
  region_cmd.set_create_timestamp(butil::gettimeofday_ms());

  // for (auto it : split_from_region_peers) {
  //   auto* store_operation_increment = meta_increment.add_store_operations();
  //   store_operation_increment->set_id(it);  // this store id
  //   store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  //   auto* store_operation = store_operation_increment->mutable_store_operation();
  //   store_operation->set_id(it);
  //   auto* region_cmd_to_add = store_operation->add_region_cmds();
  //   region_cmd_to_add->CopyFrom(region_cmd);
  //   region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  // }

  // only send split region_cmd to split_from_region_id's leader store id
  if (split_from_region.leader_store_id() == 0) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region_id's leader_store_id is 0, split_from_region_id="
                     << split_from_region_id << ", split_to_region_id=" << split_to_region_id;
    return pb::error::Errno::ESPLIT_STATUS_ILLEGAL;
  }

  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(split_from_region.leader_store_id());  // this store id
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  auto* store_operation = store_operation_increment->mutable_store_operation();
  store_operation->set_id(split_from_region.leader_store_id());
  auto* region_cmd_to_add = store_operation->add_region_cmds();
  region_cmd_to_add->CopyFrom(region_cmd);
  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));

  return pb::error::Errno::OK;
}

pb::error::Errno CoordinatorControl::MergeRegion(uint64_t merge_from_region_id, uint64_t merge_to_region_id,
                                                 pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate merge_from_region_id
  pb::common::Region merge_from_region;
  int ret = region_map_.Get(merge_from_region_id, merge_from_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "MergeRegion from region not exists, id = " << merge_from_region_id;
    return pb::error::Errno::EREGION_NOT_FOUND;
  }

  // validate merge_to_region_id
  pb::common::Region merge_to_region;
  ret = region_map_.Get(merge_to_region_id, merge_to_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "MergeRegion to region not exists, id = " << merge_from_region_id;
    return pb::error::Errno::EREGION_NOT_FOUND;
  }

  // validate merge_from_region_id and merge_to_region_id
  if (merge_from_region_id == merge_to_region_id) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region_id == merge_to_region_id";
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // validate merge_from_region and merge_to_region has same peers
  if (merge_from_region.definition().peers_size() != merge_to_region.definition().peers_size()) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region and merge_to_region has different peers size";
    return pb::error::Errno::EMERGE_PEER_NOT_MATCH;
  }

  std::vector<uint64_t> merge_from_region_peers;
  std::vector<uint64_t> merge_to_region_peers;
  merge_from_region_peers.reserve(merge_from_region.definition().peers_size());
  for (int i = 0; i < merge_from_region.definition().peers_size(); i++) {
    merge_from_region_peers.push_back(merge_to_region.definition().peers(i).store_id());
  }
  merge_to_region_peers.reserve(merge_to_region.definition().peers_size());
  for (int i = 0; i < merge_to_region.definition().peers_size(); i++) {
    merge_to_region_peers.push_back(merge_to_region.definition().peers(i).store_id());
  }
  std::sort(merge_from_region_peers.begin(), merge_from_region_peers.end());
  std::sort(merge_to_region_peers.begin(), merge_to_region_peers.end());
  if (merge_from_region_peers != merge_to_region_peers) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region and merge_to_region has different peers";
    return pb::error::Errno::EMERGE_PEER_NOT_MATCH;
  }

  // validate merge_from_region and merge_to_region status
  if (merge_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      merge_to_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region or merge_to_region is not NORMAL";
    return pb::error::Errno::EMERGE_STATUS_ILLEGAL;
  }

  // validate merge_from_region and merge_to_region has same start_key and end_key
  if (merge_from_region.definition().range().start_key() != merge_to_region.definition().range().end_key()) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region and merge_to_region has different start_key or end_key";
    return pb::error::Errno::EMERGE_RANGE_NOT_MATCH;
  }

  // generate store operation for stores
  pb::coordinator::RegionCmd region_cmd;
  region_cmd.set_region_id(merge_from_region_id);
  region_cmd.set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_MERGE);
  region_cmd.mutable_merge_request()->set_merge_from_region_id(merge_from_region_id);
  region_cmd.mutable_merge_request()->set_merge_to_region_id(merge_to_region_id);

  // for (auto it : merge_from_region_peers) {
  //   auto* store_operation_increment = meta_increment.add_store_operations();
  //   store_operation_increment->set_id(it);  // this store id
  //   store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  //   auto* store_operation = store_operation_increment->mutable_store_operation();
  //   store_operation->set_id(it);
  //   auto* region_cmd_to_add = store_operation->add_region_cmds();
  //   region_cmd_to_add->CopyFrom(region_cmd);
  //   region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  // }

  // only send merge region_cmd to merge_from_region_id's leader store id
  if (merge_from_region.leader_store_id() == 0) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region_id's leader_store_id is 0, merge_from_region_id="
                     << merge_from_region_id << ", merge_to_region_id=" << merge_to_region_id;
    return pb::error::Errno::EMERGE_STATUS_ILLEGAL;
  }

  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(merge_from_region.leader_store_id());
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  auto* store_operation = store_operation_increment->mutable_store_operation();
  store_operation->set_id(merge_from_region.leader_store_id());
  auto* region_cmd_to_add = store_operation->add_region_cmds();
  region_cmd_to_add->CopyFrom(region_cmd);
  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));

  return pb::error::Errno::OK;
}

// ChangePeerRegion
pb::error::Errno CoordinatorControl::ChangePeerRegion(uint64_t region_id, std::vector<uint64_t>& new_store_ids,
                                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate region_id
  pb::common::Region region;
  int ret = region_map_.Get(region_id, region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ChangePeerRegion region not exists, id = " << region_id;
    return pb::error::Errno::EREGION_NOT_FOUND;
  }

  // validate new_store_ids
  if (new_store_ids.size() != (region.definition().peers_size() + 1) ||
      new_store_ids.size() != (region.definition().peers_size() - 1) || new_store_ids.empty()) {
    DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids size not match, region_id = " << region_id;
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // validate new_store_ids only has one new store or only less one store
  std::vector<uint64_t> old_store_ids;
  old_store_ids.reserve(region.definition().peers_size());
  for (int i = 0; i < region.definition().peers_size(); i++) {
    old_store_ids.push_back(region.definition().peers(i).store_id());
  }

  std::vector<uint64_t> new_store_ids_diff_more;
  std::vector<uint64_t> new_store_ids_diff_less;

  std::sort(new_store_ids.begin(), new_store_ids.end());
  std::sort(old_store_ids.begin(), old_store_ids.end());

  std::set_difference(new_store_ids.begin(), new_store_ids.end(), old_store_ids.begin(), old_store_ids.end(),
                      std::inserter(new_store_ids_diff_more, new_store_ids_diff_more.begin()));
  std::set_difference(old_store_ids.begin(), old_store_ids.end(), new_store_ids.begin(), new_store_ids.end(),
                      std::inserter(new_store_ids_diff_less, new_store_ids_diff_less.begin()));

  if (new_store_ids_diff_more.size() != 1 || new_store_ids_diff_less.size() != 1) {
    DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids can only has one new store, region_id = " << region_id;
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  if (new_store_ids_diff_less.size() == 1 && new_store_ids_diff_more.size() == 1) {
    DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids only has one new store, region_id = " << region_id;
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // this is the new definition of region
  pb::common::RegionDefinition new_region_definition;
  new_region_definition.CopyFrom(region.definition());
  new_region_definition.clear_peers();

  // generate store operation for stores
  if (new_store_ids_diff_less.size() == 1) {
    // shrink region
    auto* store_operation_increment = meta_increment.add_store_operations();
    store_operation_increment->set_id(new_store_ids_diff_less.at(0));  // this store id
    store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
    auto* store_operation = store_operation_increment->mutable_store_operation();
    store_operation->set_id(new_store_ids_diff_less.at(0));
    auto* region_cmd_to_add = store_operation->add_region_cmds();
    region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
    region_cmd_to_add->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_CHANGE_PEER);
    region_cmd_to_add->set_region_id(region_id);
    region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());

    auto* region_definition = region_cmd_to_add->mutable_change_peer_request()->mutable_region_definition();
    region_definition->CopyFrom(region.definition());
    region_definition->clear_peers();  // this region on store will be deleted in the future

    // calculate new peers
    for (int i = 0; i < region.definition().peers_size(); i++) {
      if (region.definition().peers(i).store_id() != new_store_ids_diff_less.at(0)) {
        auto* peer = new_region_definition.add_peers();
        peer->CopyFrom(region.definition().peers(i));
      }
    }
  } else if (new_store_ids_diff_more.size() == 1) {
    // expand region
    // calculate new peers
    // validate new_store_ids_diff_more is legal
    pb::common::Store store_to_add_peer;
    int ret = store_map_.Get(new_store_ids_diff_more.at(0), store_to_add_peer);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids_diff_more not exists, region_id = " << region_id;
      return pb::error::Errno::ESTORE_NOT_FOUND;
    }

    // generate new peer from store
    auto* peer = new_region_definition.add_peers();
    peer->set_store_id(store_to_add_peer.id());
    peer->set_role(::dingodb::pb::common::PeerRole::VOTER);
    peer->mutable_server_location()->CopyFrom(store_to_add_peer.server_location());
    peer->mutable_raft_location()->CopyFrom(store_to_add_peer.raft_location());

    // add old peer to new_region_definition
    for (int i = 0; i < region.definition().peers_size(); i++) {
      auto* peer = new_region_definition.add_peers();
      peer->CopyFrom(region.definition().peers(i));
    }

    auto* store_operation_increment = meta_increment.add_store_operations();
    store_operation_increment->set_id(new_store_ids_diff_more.at(0));  // this store id
    store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
    auto* store_operation = store_operation_increment->mutable_store_operation();
    store_operation->set_id(new_store_ids_diff_more.at(0));
    auto* region_cmd_to_add = store_operation->add_region_cmds();
    region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
    region_cmd_to_add->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_CREATE);
    region_cmd_to_add->set_region_id(region_id);
    region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());

    auto* region_definition = region_cmd_to_add->mutable_create_request()->mutable_region_definition();
    region_definition->CopyFrom(store_to_add_peer);  // new create region on store
  } else {
    DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids not match, region_id = " << region_id;
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // generate store operation for old_store_id
  for (auto it : old_store_ids) {
    auto* store_operation_increment = meta_increment.add_store_operations();
    store_operation_increment->set_id(it);  // this store id
    store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
    auto* store_operation = store_operation_increment->mutable_store_operation();
    store_operation->set_id(it);
    auto* region_cmd_to_add = store_operation->add_region_cmds();
    region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
    region_cmd_to_add->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_CHANGE_PEER);
    region_cmd_to_add->set_region_id(region_id);
    region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());

    auto* region_definition = region_cmd_to_add->mutable_change_peer_request()->mutable_region_definition();
    region_definition->CopyFrom(new_region_definition);
  }

  return pb::error::Errno::OK;
}

// CleanStoreOperation
pb::error::Errno CoordinatorControl::CleanStoreOperation(uint64_t store_id,
                                                         pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::coordinator::StoreOperation store_operation;
  int ret = store_operation_map_.Get(store_id, store_operation);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "CleanStoreOperation store operation not exists, store_id = " << store_id;
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // clean store operation
  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(store_id);
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  store_operation_increment->mutable_store_operation()->CopyFrom(store_operation);

  return pb::error::Errno::OK;
}

// AddStoreOperation
pb::error::Errno CoordinatorControl::AddStoreOperation(const pb::coordinator::StoreOperation& store_operation,
                                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate store id
  uint64_t store_id = store_operation.id();
  int ret = store_map_.Exists(store_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "AddStoreOperation store not exists, store_id = " << store_id;
    return pb::error::Errno::ESTORE_NOT_FOUND;
  }

  // validate store operation region_cmd
  if (store_operation.region_cmds_size() == 0) {
    DINGO_LOG(ERROR) << "AddStoreOperation store operation region cmd empty, store_id = " << store_id;
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // check if region is has ongoing region_cmd
  pb::coordinator::StoreOperation store_operation_ongoing;
  ret = store_operation_map_.Get(store_id, store_operation_ongoing);
  if (ret > 0) {
    for (const auto& it : store_operation.region_cmds()) {
      for (const auto& it2 : store_operation_ongoing.region_cmds()) {
        if (it.region_id() == it2.region_id()) {
          DINGO_LOG(ERROR) << "AddStoreOperation store operation region cmd ongoing conflict, unable to add new "
                              "region_cmd, store_id = "
                           << store_id << ", region_id = " << it.region_id();
          return pb::error::Errno::EREGION_CMD_ONGOING_CONFLICT;
        }
      }
    }
  }

  // add store operation
  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(store_id);
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  store_operation_increment->mutable_store_operation()->CopyFrom(store_operation);

  auto* store_operation_to_update_id = store_operation_increment->mutable_store_operation();
  for (int i = 0; i < store_operation_to_update_id->region_cmds_size(); i++) {
    store_operation_to_update_id->mutable_region_cmds(i)->set_id(
        GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  }
  return pb::error::Errno::OK;
}

pb::error::Errno CoordinatorControl::RemoveStoreOperation(uint64_t store_id, uint64_t region_cmd_id,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::coordinator::StoreOperation store_operation;
  int ret = store_operation_map_.Get(store_id, store_operation);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "RemoveStoreOperation store operation not exists, store_id = " << store_id;
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // check if region_cmd_id exists
  pb::coordinator_internal::MetaIncrementStoreOperation store_operation_increment;
  store_operation_increment.set_id(store_id);
  store_operation_increment.set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  auto* store_operation_to_delete = store_operation_increment.mutable_store_operation();
  store_operation_to_delete->set_id(store_id);

  for (const auto& it : store_operation.region_cmds()) {
    if (it.id() == region_cmd_id) {
      // remove region_cmd
      auto* region_cmd_to_delete = store_operation_to_delete->add_region_cmds();
      region_cmd_to_delete->CopyFrom(it);
    }
  }

  if (store_operation_to_delete->region_cmds_size() == 0) {
    DINGO_LOG(ERROR) << "RemoveStoreOperation region_cmd_id not exists, store_id = " << store_id
                     << ", region_cmd_id = " << region_cmd_id;
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  auto* store_operation_increment_to_delete = meta_increment.add_store_operations();
  store_operation_increment_to_delete->CopyFrom(store_operation_increment);

  return pb::error::Errno::OK;
}

// UpdateRegionMap
uint64_t CoordinatorControl::UpdateRegionMap(std::vector<pb::common::Region>& regions,
                                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t region_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION);

  bool need_to_get_next_epoch = false;
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    for (const auto& region : regions) {
      pb::common::Region region_to_update;
      int ret = region_map_.Get(region.id(), region_to_update);
      if (ret > 0) {
        DINGO_LOG(INFO) << " update region to region_map in heartbeat, region_id=" << region.id();

        // if state not change, just update leader_store_id
        if (region_to_update.state() == region.state()) {
          // update the region's leader_store_id, no need to apply raft
          if (region_to_update.leader_store_id() != region.leader_store_id()) {
            region_to_update.set_leader_store_id(region.leader_store_id());
            region_map_.Put(region.id(), region_to_update);
          }
          continue;
        } else {
          // state not equal, need to update region data and apply raft
          DINGO_LOG(INFO) << "REGION STATUS CHANGE region_id = " << region.id()
                          << " old status = " << region_to_update.state() << " new status = " << region.state();
          // maybe need to build a state machine here
          // if a region is set to DELETE, it will never be updated to other normal state
          const auto& region_delete_state_name =
              dingodb::pb::common::RegionState_Name(pb::common::RegionState::REGION_DELETE);
          const auto& region_state_in_map = dingodb::pb::common::RegionState_Name(region_to_update.state());
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
                        << " name=" << region.definition().name() << " leader_store_id=" << region.leader_store_id()
                        << " state=" << region.state();
      } else {
        DINGO_LOG(INFO) << " found illegal region in heartbeat, region_id=" << region.id()
                        << " name=" << region.definition().name() << " leader_store_id=" << region.leader_store_id()
                        << " state=" << region.state();

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
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    butil::FlatMap<std::string, pb::common::Executor> executor_map_copy;
    executor_map_copy.init(100);
    executor_map_.GetFlatMapCopy(executor_map_copy);
    for (auto& element : executor_map_copy) {
      auto* tmp_region = executor_map.add_executors();
      tmp_region->CopyFrom(element.second);
    }
  }
}

pb::error::Errno CoordinatorControl::GetExecutorUserMap(uint64_t cluster_id,
                                                        pb::common::ExecutorUserMap& executor_user_map) {
  if (cluster_id < 0) {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  // uint64_t executor_user_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR_USER);
  // executor_user_map.set_epoch(executor_user_map_epoch);
  {
    // BAIDU_SCOPED_LOCK(executor_user_map_mutex_);
    butil::FlatMap<std::string, pb::coordinator_internal::ExecutorUserInternal> executor_user_map_copy;
    executor_user_map_copy.init(100);
    executor_user_map_.GetFlatMapCopy(executor_user_map_copy);
    for (auto& element : executor_user_map_copy) {
      auto* tmp_region = executor_user_map.add_executor_users();
      tmp_region->set_user(element.second.id());
      tmp_region->set_keyring(element.second.keyring());
    }
  }
  return pb::error::OK;
}

void CoordinatorControl::GetPushExecutorMap(butil::FlatMap<std::string, pb::common::Executor>& executor_to_push) {
  BAIDU_SCOPED_LOCK(executor_need_push_mutex_);
  executor_to_push.swap(executor_need_push_);
}

bool CoordinatorControl::ValidateExecutorUser(const pb::common::ExecutorUser& executor_user) {
  if (executor_user.keyring() == std::string("TO_BE_CONTINUED")) {
    DINGO_LOG(INFO) << "ValidateExecutorUser debug pass with TO_BE_CONTINUED";
    return true;
  }

  pb::coordinator_internal::ExecutorUserInternal executor_user_in_map;
  int ret = executor_user_map_.Get(executor_user.user(), executor_user_in_map);
  if (ret > 0) {
    if (executor_user_in_map.id() == executor_user.user() &&
        executor_user_in_map.keyring() == executor_user.keyring()) {
      DINGO_LOG(INFO) << "ValidateExecutorUser succcess";
      return true;
    }

    DINGO_LOG(INFO) << "ValidateExecutorUser user or keyring wrong fail, input_user=" << executor_user.user()
                    << "  input_keyring=" << executor_user.keyring();
    return -1;
  } else {
    DINGO_LOG(INFO) << "ValidateExecutorUser user " << executor_user.user() << " not exist fail";
  }

  return -1;
}

pb::error::Errno CoordinatorControl::CreateExecutor(uint64_t cluster_id, pb::common::Executor& executor,
                                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  pb::common::Executor executor_to_create;
  executor_to_create.CopyFrom(executor);

  if (!ValidateExecutorUser(executor_to_create.executor_user())) {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  if (executor_to_create.id().empty() &&
      ((!executor_to_create.server_location().host().empty()) && executor_to_create.server_location().port() != 0)) {
    executor_to_create.set_id(executor_to_create.server_location().host() + ":" +
                              std::to_string(executor_to_create.server_location().port()));
  } else {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  executor_to_create.set_state(::dingodb::pb::common::ExecutorState::EXECUTOR_NEW);
  executor_to_create.set_create_timestamp(butil::gettimeofday_ms());

  executor.CopyFrom(executor_to_create);

  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executors();
  executor_increment->set_id(executor_to_create.id());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

  auto* executor_increment_executor = executor_increment->mutable_executor();
  executor_increment_executor->CopyFrom(executor_to_create);

  return pb::error::OK;
}

pb::error::Errno CoordinatorControl::DeleteExecutor(uint64_t cluster_id, const pb::common::Executor& executor,
                                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0 || executor.id().length() <= 0 || executor.executor_user().user().length() <= 0) {
    return pb::error::EILLEGAL_PARAMTETERS;
  }

  pb::common::Executor executor_to_delete;
  {
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    int ret = executor_map_.Get(executor.id(), executor_to_delete);
    if (ret < 0) {
      DINGO_LOG(INFO) << "DeleteExecutor executor_id not exists, id=" << executor.id();
      return pb::error::EEXECUTOR_NOT_FOUND;
    }

    // validate executor_user
    if (!ValidateExecutorUser(executor.executor_user())) {
      DINGO_LOG(INFO) << "DeleteExecutor executor_id id=" << executor.id() << " validate user fail";
      return pb::error::EILLEGAL_PARAMTETERS;
    }
  }

  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executors();
  executor_increment->set_id(executor.id());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

  auto* executor_increment_executor = executor_increment->mutable_executor();
  executor_increment_executor->CopyFrom(executor_to_delete);

  return pb::error::Errno::OK;
}

pb::error::Errno CoordinatorControl::CreateExecutorUser(uint64_t cluster_id, pb::common::ExecutorUser& executor_user,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  if (executor_user_map_.Exists(executor_user.user())) {
    DINGO_LOG(INFO) << "CreateExecutorUser user already exists, user=" << executor_user.user();
    return pb::error::Errno::EUSER_ALREADY_EXIST;
  }

  // executor_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR, meta_increment);
  if (executor_user.keyring().length() <= 0) {
    executor_user.set_keyring(Helper::GenerateRandomString(16));
  }

  // update meta_increment
  // GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executor_users();
  executor_increment->set_id(executor_user.user());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  executor_increment->mutable_executor_user()->set_id(executor_user.user());
  executor_increment->mutable_executor_user()->set_keyring(executor_user.keyring());

  return pb::error::Errno::OK;
}

pb::error::Errno CoordinatorControl::UpdateExecutorUser(uint64_t cluster_id,
                                                        const pb::common::ExecutorUser& executor_user,
                                                        const pb::common::ExecutorUser& executor_user_update,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  pb::coordinator_internal::ExecutorUserInternal executor_user_internal;
  int ret = executor_user_map_.Get(executor_user.user(), executor_user_internal);
  if (ret < 0) {
    DINGO_LOG(INFO) << "UpdateExecutorUser user not exists, user=" << executor_user.user();
    return pb::error::Errno::EUSER_NOT_EXIST;
  }

  if (executor_user.keyring().length() > 0 && executor_user.keyring() != executor_user_internal.keyring()) {
    DINGO_LOG(INFO) << "UpdateExecutorUser user keyring not equal, user=" << executor_user.user()
                    << " input keyring=" << executor_user.keyring()
                    << " but executor_user's keyring=" << executor_user_internal.keyring();
    return pb::error::Errno::EKEYRING_ILLEGAL;
  }

  // executor_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR, meta_increment);

  // update meta_increment
  // GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executor_users();
  executor_increment->set_id(executor_user.user());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  executor_increment->mutable_executor_user()->set_id(executor_user.user());
  executor_increment->mutable_executor_user()->set_keyring(executor_user_update.keyring());

  return pb::error::Errno::OK;
}

pb::error::Errno CoordinatorControl::DeleteExecutorUser(uint64_t cluster_id, pb::common::ExecutorUser& executor_user,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return pb::error::Errno::EILLEGAL_PARAMTETERS;
  }

  pb::coordinator_internal::ExecutorUserInternal executor_user_in_map;
  int ret = executor_user_map_.Get(executor_user.user(), executor_user_in_map);
  if (ret < 0) {
    DINGO_LOG(INFO) << "DeleteExecutorUser user not exists, user=" << executor_user.user();
    return pb::error::Errno::EUSER_NOT_EXIST;
  }

  if (executor_user.keyring().length() > 0 && executor_user.keyring() != executor_user_in_map.keyring()) {
    DINGO_LOG(INFO) << "DeleteExecutorUser keyring not equal, input keyring=" << executor_user.keyring()
                    << " but executor_user's keyring=" << executor_user_in_map.keyring();
    return pb::error::Errno::EKEYRING_ILLEGAL;
  }

  // executor_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR, meta_increment);

  // update meta_increment
  // GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executor_users();
  executor_increment->set_id(executor_user.user());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  executor_increment->mutable_executor_user()->set_id(executor_user.user());
  executor_increment->mutable_executor_user()->set_keyring(executor_user.keyring());

  return pb::error::Errno::OK;
}

// UpdateExecutorMap
uint64_t CoordinatorControl::UpdateExecutorMap(const pb::common::Executor& executor,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t executor_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR);

  bool need_update_epoch = false;
  {
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    pb::common::Executor executor_to_update;
    int ret = executor_map_.Get(executor.id(), executor_to_update);
    if (ret > 0) {
      if (executor_to_update.state() == pb::common::ExecutorState::EXECUTOR_NEW) {
        // this is a new executor's first heartbeat
        // so we need to update the executor's state to executor_NORMAL
        // and update the executor's server_location
        // and update the executor's raft_location
        // and update the executor's last_seen_timestamp
        DINGO_LOG(INFO) << "executor STATUS CHANGE executor_id = " << executor.id()
                        << " old status = " << executor_to_update.state() << " new status = " << executor.state();

        // update meta_increment
        need_update_epoch = true;
        auto* executor_increment = meta_increment.add_executors();
        executor_increment->set_id(executor.id());
        executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        auto* executor_increment_executor = executor_increment->mutable_executor();
        executor_increment_executor->CopyFrom(
            executor_to_update);  // only update server_location & raft_location & state

        // only update server_location & raft_location & state & last_seen_timestamp
        executor_increment_executor->mutable_server_location()->CopyFrom(executor.server_location());
        executor_increment_executor->set_state(pb::common::ExecutorState::EXECUTOR_NORMAL);
        executor_increment_executor->set_last_seen_timestamp(butil::gettimeofday_ms());
      } else {
        // this is normall heartbeat,
        // so only need to update state & last_seen_timestamp, no need to update epoch
        auto* executor_increment = meta_increment.add_executors();
        executor_increment->set_id(executor.id());
        executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        auto* executor_increment_executor = executor_increment->mutable_executor();
        executor_increment_executor->CopyFrom(
            executor_to_update);  // only update server_location & raft_location & state

        // only update state & last_seen_timestamp
        executor_increment_executor->set_state(pb::common::ExecutorState::EXECUTOR_NORMAL);
        executor_increment_executor->set_last_seen_timestamp(butil::gettimeofday_ms());
      }
    } else {
      // this is a special new executor's first heartbeat
      // only executor using keyring=TO_BE_CONTINUED can get into this branch
      // so we just add this executor into executor_map_
      DINGO_LOG(INFO) << "NEED ADD NEW executor executor_id = " << executor.id();

      // update meta_increment
      need_update_epoch = true;
      auto* executor_increment = meta_increment.add_executors();
      executor_increment->set_id(executor.id());
      executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

      auto* executor_increment_executor = executor_increment->mutable_executor();
      executor_increment_executor->CopyFrom(executor);
      executor_increment_executor->set_state(pb::common::ExecutorState::EXECUTOR_NORMAL);
      executor_increment_executor->set_last_seen_timestamp(butil::gettimeofday_ms());

      // setup create_timestamp
      executor_increment_executor->set_create_timestamp(butil::gettimeofday_ms());
    }
  }

  if (need_update_epoch) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  }

  DINGO_LOG(INFO) << "UpdateExecutorMap executor_id=" << executor.id();

  return executor_map_epoch;
}

// Update RegionMap and StoreOperation
void CoordinatorControl::UpdateRegionMapAndStoreOperation(const pb::common::StoreMetrics& store_metrics,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  // update region_map
  for (const auto& it : store_metrics.region_metrics_map()) {
    const auto& region_metrics = it.second;
    if (!region_metrics.has_braft_status()) {
      DINGO_LOG(ERROR) << "region_metrics has no braft_status,store_id=" << store_metrics.id()
                       << " region_id=" << region_metrics.id();
      continue;
    }

    // use leader store's region_metrics to update region_map
    if (region_metrics.braft_status().raft_state() != pb::common::RaftNodeState::STATE_LEADER) {
      continue;
    }

    // update region_map
    pb::common::Region region_to_update;
    int ret = region_map_.Get(region_metrics.id(), region_to_update);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERROR: UpdateRegionMapAndStoreOperation region not exists, region_id = "
                       << region_metrics.id();
      continue;
    }

    // when region leader change or region state change, we need to update region_map_
    // or when region last_update_timestamp is too old, we need to update region_map_
    bool need_update_region = false;
    bool need_update_region_definition = false;
    if (region_to_update.leader_store_id() != store_metrics.id()) {
      DINGO_LOG(INFO) << "region leader change region_id = " << region_metrics.id()
                      << " old leader_store_id = " << region_to_update.leader_store_id()
                      << " new leader_store_id = " << store_metrics.id();
      need_update_region = true;
    }

    if (region_to_update.metrics().store_region_state() != region_metrics.store_region_state()) {
      DINGO_LOG(INFO) << "region state change region_id = " << region_metrics.id()
                      << " old state = " << region_to_update.metrics().store_region_state()
                      << " new state = " << region_metrics.store_region_state();
      need_update_region = true;
    }

    if (region_to_update.last_update_timestamp() + 60 * 1000 < butil::gettimeofday_ms()) {
      DINGO_LOG(INFO) << "region last_update_timestamp too old region_id = " << region_metrics.id()
                      << " last_update_timestamp = " << region_to_update.last_update_timestamp()
                      << " now = " << butil::gettimeofday_ms();
      need_update_region = true;
    }

    if (region_to_update.definition().range().start_key() != region_metrics.region_definition().range().start_key() ||
        region_to_update.definition().range().end_key() != region_metrics.region_definition().range().end_key()) {
      DINGO_LOG(INFO) << "region range change region_id = " << region_metrics.id() << " old range = ["
                      << region_to_update.definition().range().start_key() << ", "
                      << region_to_update.definition().range().end_key() << ")"
                      << " new range = [" << region_metrics.region_definition().range().start_key() << ", "
                      << region_metrics.region_definition().range().end_key() << ")";
      need_update_region = true;
      need_update_region_definition = true;
    }

    if (!need_update_region) {
      DINGO_LOG(DEBUG) << "region no need to update region_id = " << region_metrics.id()
                       << " last_update_timestamp = " << region_to_update.last_update_timestamp()
                       << " now = " << butil::gettimeofday_ms();
      continue;
    }

    // update meta_increment
    auto* region_increment = meta_increment.add_regions();
    region_increment->set_id(region_metrics.id());
    region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

    auto* region_increment_region = region_increment->mutable_region();
    region_increment_region->CopyFrom(region_to_update);

    // update leader_store_id
    region_increment_region->set_leader_store_id(store_metrics.id());

    // update last_update_timestamp
    region_increment_region->set_last_update_timestamp(butil::gettimeofday_ms());

    // update region metrics
    region_increment_region->mutable_metrics()->CopyFrom(region_metrics);

    if (region_metrics.store_region_state() == pb::common::StoreRegionState::NORMAL) {
      region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_NORMAL);
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::SPLITTING) {
      region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_SPLITTING);
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::MERGING) {
      region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_MERGING);
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::DELETING) {
      region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_DELETING);
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::DELETED) {
      region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_DELETED);
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::STANDBY) {
      region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_STANDBY);
    } else {
      DINGO_LOG(ERROR) << "ERROR: UpdateRegionMapAndStoreOperation region state error, store_id=" << store_metrics.id()
                       << " region_id = " << region_metrics.id() << " state = " << region_metrics.store_region_state();
      continue;
    }

    // for split and merge, we need to update region definition
    if (need_update_region_definition) {
      region_increment_region->mutable_definition()->CopyFrom(region_metrics.region_definition());
    }

    // update RegionRaftStatus
    if (region_metrics.has_braft_status()) {
      DINGO_LOG(ERROR) << "region braft_status not found in heartbeat region_id = " << region_metrics.id();
      continue;
    }

    auto region_raft_status = ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY;
    for (const auto& follower : region_metrics.braft_status().stable_followers()) {
      if (follower.second.next_index() < region_metrics.braft_status().last_index() + 10) {
        region_raft_status = region_raft_status < ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_LAGGY
                                 ? region_raft_status
                                 : ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_LAGGY;
      }

      if (follower.second.installing_snapshot()) {
        region_raft_status = region_raft_status < ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_RECOVERING
                                 ? region_raft_status
                                 : ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_RECOVERING;
      }
    }

    // if peer cannot connected, ustable_followers_size will not be 0
    // so we need set replica status to DEGRAED
    if (region_metrics.braft_status().unstable_followers_size() > 0) {
      region_raft_status = ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_CONSECUTIVE_ERROR;
      region_increment_region->set_replica_status(::dingodb::pb::common::ReplicaStatus::REPLICA_DEGRAED);
    } else {
      region_increment_region->set_replica_status(::dingodb::pb::common::ReplicaStatus::REPLICA_NORMAL);
    }

    region_increment_region->set_raft_status(region_raft_status);
  }

  // update store operation
  pb::coordinator::StoreOperation store_operation;
  GetStoreOperation(store_metrics.id(), store_operation);

  if (store_operation.region_cmds_size() == 0) {
    return;
  }

  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(store_metrics.id());
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  auto* store_operation_increment_store_operation = store_operation_increment->mutable_store_operation();

  // add to be deleted region_cmd to increment
  for (auto& it : *store_operation.mutable_region_cmds()) {
    if (it.region_cmd_type() == pb::coordinator::RegionCmdType::CMD_CREATE) {
      // check CMD_CREATE region_id exists
      if (store_metrics.region_metrics_map().contains(it.region_id())) {
        DINGO_LOG(INFO) << "CMD_CREATE store_id=" << store_metrics.id() << " region_id = " << it.region_id()
                        << " exists, remove store_operation";
        auto* region_cmd_for_delete = store_operation_increment_store_operation->add_region_cmds();
        region_cmd_for_delete->CopyFrom(it);
      }
    } else if (it.region_cmd_type() == pb::coordinator::RegionCmdType::CMD_DELETE) {
      // check CMD_DELETE region_id exists
      if (!store_metrics.region_metrics_map().contains(it.region_id())) {
        DINGO_LOG(INFO) << "CMD_DELETE store_id=" << store_metrics.id() << "region_id = " << it.region_id()
                        << " not exists";
        auto* region_cmd_for_delete = store_operation_increment_store_operation->add_region_cmds();
        region_cmd_for_delete->CopyFrom(it);
      } else if (store_metrics.region_metrics_map().at(it.region_id()).store_region_state() ==
                     pb::common::StoreRegionState::DELETED ||
                 store_metrics.region_metrics_map().at(it.region_id()).store_region_state() ==
                     pb::common::StoreRegionState::DELETING) {
        DINGO_LOG(INFO) << "CMD_DELETE store_id=" << store_metrics.id() << "region_id = " << it.region_id()
                        << " exists, but state is DELETED or DELETING";
        auto* region_cmd_for_delete = store_operation_increment_store_operation->add_region_cmds();
        region_cmd_for_delete->CopyFrom(it);
      } else {
        DINGO_LOG(INFO) << "CMD_DELETE store_id=" << store_metrics.id() << "region_id = " << it.region_id()
                        << " exists, but state is not DELETED or DELETING";
      }
    } else if (it.region_cmd_type() == pb::coordinator::RegionCmdType::CMD_SPLIT) {
      // check CMD_SPLIT region_id exists
      if (!store_metrics.region_metrics_map().contains(it.region_id())) {
        DINGO_LOG(INFO) << "CMD_SPLIT store_id=" << store_metrics.id() << "region_id = " << it.region_id()
                        << " not exists";
        auto* region_cmd_for_delete = store_operation_increment_store_operation->add_region_cmds();
        region_cmd_for_delete->CopyFrom(it);
      } else if (store_metrics.region_metrics_map().at(it.region_id()).store_region_state() ==
                 pb::common::StoreRegionState::SPLITTING) {
        DINGO_LOG(INFO) << "CMD_SPLIT store_id=" << store_metrics.id() << "region_id = " << it.region_id()
                        << " exists, and state is SPLITTING, it's same as expected";
        auto* region_cmd_for_delete = store_operation_increment_store_operation->add_region_cmds();
        region_cmd_for_delete->CopyFrom(it);
      } else {
        DINGO_LOG(INFO) << "CMD_SPLIT store_id=" << store_metrics.id() << "region_id = " << it.region_id()
                        << " exists, but state is not SPLITTING";
      }
    } else if (it.region_cmd_type() == pb::coordinator::RegionCmdType::CMD_MERGE) {
      // check CMD_MERGE region_id exists
      if (!store_metrics.region_metrics_map().contains(it.region_id())) {
        DINGO_LOG(INFO) << "CMD_MERGE store_id=" << store_metrics.id() << "region_id = " << it.region_id()
                        << " not exists";
        auto* region_cmd_for_delete = store_operation_increment_store_operation->add_region_cmds();
        region_cmd_for_delete->CopyFrom(it);
      } else if (store_metrics.region_metrics_map().at(it.region_id()).store_region_state() ==
                 pb::common::StoreRegionState::MERGING) {
        DINGO_LOG(INFO) << "CMD_MERGE store_id=" << store_metrics.id() << "region_id = " << it.region_id()
                        << " exists, but state is MERGING";
        auto* region_cmd_for_delete = store_operation_increment_store_operation->add_region_cmds();
        region_cmd_for_delete->CopyFrom(it);
      } else {
        DINGO_LOG(INFO) << "CMD_MERGE store_id=" << store_metrics.id() << "region_id = " << it.region_id()
                        << " exists, but state is not MERGING";
      }
    }
  }
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

  // use region_metrics_map to update region_map and store_operation
  if (store_metrics.region_metrics_map_size() > 0) {
    UpdateRegionMapAndStoreOperation(store_metrics, meta_increment);
  }

  DINGO_LOG(INFO) << "UpdateStoreMetricsMap store_metrics.id=" << store_metrics.id();

  return 0;
}

void CoordinatorControl::GetMemoryInfo(pb::coordinator::CoordinatorMemoryInfo& memory_info) {
  // compute size
  memory_info.set_id_epoch_safe_map_temp_count(id_epoch_map_safe_temp_.Size());
  memory_info.set_id_epoch_safe_map_temp_size(id_epoch_map_safe_temp_.MemorySize());

  {
    // BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);

    // set term & index
    pb::coordinator_internal::IdEpochInternal temp_term;
    pb::coordinator_internal::IdEpochInternal temp_index;
    int ret_term = id_epoch_map_.Get(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM, temp_term);
    int ret_index = id_epoch_map_.Get(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX, temp_index);

    if (ret_term >= 0) {
      memory_info.set_applied_term(temp_term.value());
    }
    if (ret_index >= 0) {
      memory_info.set_applied_index(temp_index.value());
    }

    // set count & size
    memory_info.set_id_epoch_map_count(id_epoch_map_.Size());
    memory_info.set_total_size(memory_info.total_size() + id_epoch_map_.MemorySize());

    // dump id & epoch to kv
    butil::FlatMap<uint64_t, pb::coordinator_internal::IdEpochInternal> id_epoch_map_temp;
    id_epoch_map_temp.init(100);
    int ret = id_epoch_map_.GetFlatMapCopy(id_epoch_map_temp);
    for (auto& it : id_epoch_map_temp) {
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
    // BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    memory_info.set_coordinator_map_count(coordinator_map_.Size());
    memory_info.set_coordinator_map_size(coordinator_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.coordinator_map_size());
  }
  {
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    memory_info.set_store_map_count(store_map_.Size());
    memory_info.set_store_map_size(store_map_.MemorySize());
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
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    memory_info.set_executor_map_count(executor_map_.Size());
    memory_info.set_executor_map_size(executor_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.executor_map_size());
  }
  {
    BAIDU_SCOPED_LOCK(executor_need_push_mutex_);
    memory_info.set_executor_need_push_count(executor_need_push_.size());
    for (auto& it : executor_need_push_) {
      memory_info.set_executor_need_push_size(memory_info.executor_need_push_size() + it.first.size() +
                                              it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.executor_need_push_size());
  }
  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    memory_info.set_schema_map_count(schema_map_.Size());
    memory_info.set_schema_map_size(schema_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.schema_map_size());
  }
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    memory_info.set_region_map_count(region_map_.Size());
    memory_info.set_region_map_size(region_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.region_map_size());
  }
  {
    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    memory_info.set_table_map_count(table_map_.Size());
    memory_info.set_table_map_size(table_map_.MemorySize());
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
    // BAIDU_SCOPED_LOCK(table_metrics_map_mutex_);
    memory_info.set_table_metrics_map_count(table_metrics_map_.Size());
    memory_info.set_table_metrics_map_size(table_metrics_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.table_metrics_map_size());
  }
}

void CoordinatorControl::GetStoreOperation(uint64_t store_id, pb::coordinator::StoreOperation& store_operation) {
  store_operation_map_.Get(store_id, store_operation);
}

void CoordinatorControl::GetStoreOperations(
    butil::FlatMap<uint64_t, pb::coordinator::StoreOperation>& store_operations) {
  store_operation_map_.GetFlatMapCopy(store_operations);

  // auto store_opertion_kvs = store_operation_meta_->TransformToKvWithAll();
  // for (auto& it : store_opertion_kvs) {
  //   DINGO_LOG(DEBUG) << "store_operation_meta_ key=" << it.key() << " value=" << it.value();
  // }
}

}  // namespace dingodb

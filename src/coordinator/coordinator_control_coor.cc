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

#include "braft/closure_helper.h"
#include "braft/configuration.h"
#include "braft/raft.h"
#include "brpc/channel.h"
#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "butil/strings/string_split.h"
#include "butil/synchronization/lock.h"
#include "butil/time.h"
#include "bvar/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "engine/snapshot.h"
#include "gflags/gflags.h"
#include "google/protobuf/unknown_field_set.h"
#include "metrics/coordinator_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"

namespace dingodb {

DECLARE_int32(executor_heartbeat_timeout);
DECLARE_int32(store_heartbeat_timeout);
DECLARE_int32(region_heartbeat_timeout);
DECLARE_int32(region_delete_after_deleted_time);

DEFINE_int32(
    region_update_timeout, 20,
    "region update timeout in seconds, will not update region info if no state change and (now - last_update_time) > "
    "region_update_timeout");

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

void CoordinatorControl::GetStoreMetrics(uint64_t store_id, std::vector<pb::common::StoreMetrics>& store_metrics) {
  BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
  if (store_id == 0) {
    for (auto& elemnt : store_metrics_map_) {
      store_metrics.push_back(elemnt.second);
    }
  } else {
    auto* it = store_metrics_map_.seek(store_id);
    if (it != nullptr) {
      store_metrics.push_back(*it);
    }
  }
}

void CoordinatorControl::DeleteStoreMetrics(uint64_t store_id) {
  BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
  if (store_id == 0) {
    for (const auto& it : store_metrics_map_) {
      coordinator_bvar_metrics_store_.DeleteStoreBvar(it.first);
    }
    store_metrics_map_.clear();
  } else {
    store_metrics_map_.erase(store_id);
    coordinator_bvar_metrics_store_.DeleteStoreBvar(store_id);
  }
}

butil::Status CoordinatorControl::GetOrphanRegion(uint64_t store_id,
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

  return butil::Status::OK();
}

void CoordinatorControl::GetPushStoreMap(butil::FlatMap<uint64_t, pb::common::Store>& store_to_push) {
  BAIDU_SCOPED_LOCK(store_need_push_mutex_);
  store_to_push.swap(store_need_push_);
}

int CoordinatorControl::ValidateStore(uint64_t store_id, const std::string& keyring) {
  if (keyring == std::string("TO_BE_CONTINUED")) {
    DINGO_LOG(DEBUG) << "ValidateStore store_id=" << store_id << " debug pass with TO_BE_CONTINUED";
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

butil::Status CoordinatorControl::CreateStore(uint64_t cluster_id, uint64_t& store_id, std::string& keyring,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id <= 0");
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
  return butil::Status::OK();
}

butil::Status CoordinatorControl::DeleteStore(uint64_t cluster_id, uint64_t store_id, std::string keyring,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0 || store_id <= 0 || keyring.length() <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "cluster_id <= 0 || store_id <= 0 || keyring.length() <= 0");
  }

  pb::common::Store store_to_delete;
  {
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    int ret = store_map_.Get(store_id, store_to_delete);
    if (ret < 0) {
      DINGO_LOG(INFO) << "DeleteStore store_id not exists, id=" << store_id;
      return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "store_id not exists");
    }

    if (keyring != store_to_delete.keyring()) {
      DINGO_LOG(INFO) << "DeleteStore store_id id=" << store_id << " keyring not equal, input keyring=" << keyring
                      << " but store's keyring=" << store_to_delete.keyring();
      return butil::Status(pb::error::Errno::EKEYRING_ILLEGAL, "keyring not equal");
    }

    if (store_to_delete.state() != pb::common::StoreState::STORE_OFFLINE &&
        store_to_delete.state() != pb::common::StoreState::STORE_NEW) {
      DINGO_LOG(INFO) << "DeleteStore store_id id=" << store_id << " already deleted";
      return butil::Status(pb::error::Errno::ESTORE_IN_USE, "store already deleted");
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
  return butil::Status::OK();
}

butil::Status CoordinatorControl::UpdateStore(uint64_t cluster_id, uint64_t store_id, std::string keyring,
                                              pb::common::StoreInState in_state,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0 || store_id <= 0 || keyring.length() <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "cluster_id <= 0 || store_id <= 0 || keyring.length() <= 0");
  }

  pb::common::Store store_to_update;
  {
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    int ret = store_map_.Get(store_id, store_to_update);
    if (ret < 0) {
      DINGO_LOG(INFO) << "UpdateStore store_id not exists, id=" << store_id;
      return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "store_id not exists");
    }

    if (keyring != store_to_update.keyring() && keyring != std::string("TO_BE_CONTINUED")) {
      DINGO_LOG(INFO) << "UpdateStore store_id id=" << store_id << " keyring not equal, input keyring=" << keyring
                      << " but store's keyring=" << store_to_update.keyring();
      return butil::Status(pb::error::Errno::EKEYRING_ILLEGAL, "keyring not equal");
    }

    if (store_to_update.in_state() == in_state) {
      DINGO_LOG(INFO) << "UpdateStore store_id id=" << store_id << " already input state, no need to update";
      return butil::Status::OK();
    }

    store_to_update.set_in_state(in_state);
  }

  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_STORE, meta_increment);
  auto* store_increment = meta_increment.add_stores();
  store_increment->set_id(store_id);
  store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

  auto* store_increment_store = store_increment->mutable_store();
  store_increment_store->CopyFrom(store_to_update);

  // on_apply
  // store_map_epoch++;                                  // raft_kv_put
  // store_map_.insert(std::make_pair(store_id, store));  // raft_kv_put
  return butil::Status::OK();
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
        region_to_update.last_update_timestamp() + (FLAGS_region_update_timeout * 1000) < butil::gettimeofday_ms()) {
      // update region's heartbeat state to REGION_DOWN
      region_to_update.set_heartbeat_state(pb::common::RegionHeartbeatState::REGION_DOWN);
      region_map_.PutIfExists(region_id, region_to_update);
      return true;
    }
  }
  return false;
}

bool CoordinatorControl::TrySetRegionToOnline(uint64_t region_id) {
  pb::common::Region region_to_update;
  int ret = region_map_.Get(region_id, region_to_update);
  if (ret > 0) {
    if (region_to_update.heartbeat_state() != pb::common::RegionHeartbeatState::REGION_ONLINE &&
        region_to_update.last_update_timestamp() + (FLAGS_region_update_timeout * 1000) >= butil::gettimeofday_ms()) {
      // update region's heartbeat state to REGION_ONLINE
      region_to_update.set_heartbeat_state(pb::common::RegionHeartbeatState::REGION_ONLINE);
      region_map_.PutIfExists(region_id, region_to_update);
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
        store_to_update.last_seen_timestamp() + (FLAGS_store_heartbeat_timeout * 1000) < butil::gettimeofday_ms()) {
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
        executor_to_update.last_seen_timestamp() + (FLAGS_executor_heartbeat_timeout * 1000) <
            butil::gettimeofday_ms()) {
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
      // tmp_region->CopyFrom(elemnt.second);
      tmp_region->set_id(elemnt.second.id());
      tmp_region->mutable_definition()->set_name(elemnt.second.definition().name());
      tmp_region->set_state(elemnt.second.state());
      tmp_region->set_raft_status(elemnt.second.raft_status());
      tmp_region->set_replica_status(elemnt.second.replica_status());
      tmp_region->set_heartbeat_state(elemnt.second.heartbeat_state());
      tmp_region->set_leader_store_id(elemnt.second.leader_store_id());
      tmp_region->set_create_timestamp(elemnt.second.create_timestamp());
      tmp_region->set_last_update_timestamp(elemnt.second.last_update_timestamp());
    }
  }
}

void CoordinatorControl::GetRegionMapFull(pb::common::RegionMap& region_map) {
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

void CoordinatorControl::GetRegionCount(uint64_t& region_count) { region_count = region_map_.Size(); }

void CoordinatorControl::GetRegionIdsInMap(std::vector<uint64_t>& region_ids) { region_map_.GetAllKeys(region_ids); }

void CoordinatorControl::CleanRegionBvars() {}

void CoordinatorControl::DeleteRegionBvar(uint64_t region_id) {
  coordinator_bvar_metrics_region_.DeleteRegionBvar(region_id);
}

butil::Status CoordinatorControl::QueryRegion(uint64_t region_id, pb::common::Region& region) {
  if (region_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "region_id must be positive");
  }

  int ret = region_map_.Get(region_id, region);
  if (ret < 0) {
    DINGO_LOG(INFO) << "QueryRegion region_id not exists, id=" << region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region_id not exists");
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CreateRegionForSplitInternal(
    uint64_t split_from_region_id, uint64_t& new_region_id, pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (split_from_region_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "split_from_region_id must be positive");
  }

  std::vector<uint64_t> store_ids;

  pb::common::Region split_from_region;
  int ret = region_map_.Get(split_from_region_id, split_from_region);
  if (ret < 0) {
    DINGO_LOG(INFO) << "CreateRegionForSplit region_id not exists, id=" << split_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region_id not exists");
  }

  for (const auto& peer : split_from_region.definition().peers()) {
    store_ids.push_back(peer.store_id());
  }

  split_from_region.mutable_definition()->set_name(split_from_region.definition().name() + "_split");

  auto new_range = split_from_region.definition().range();
  new_range.set_start_key(split_from_region.definition().range().end_key());
  new_range.set_end_key(split_from_region.definition().range().start_key());

  // create region with split_from_region_id & store_ids
  return CreateRegion(split_from_region.definition().name(), "", store_ids.size(), new_range,
                      split_from_region.definition().schema_id(), split_from_region.definition().table_id(), store_ids,
                      split_from_region_id, new_region_id, meta_increment);
}

butil::Status CoordinatorControl::CreateRegionForSplit(const std::string& region_name, const std::string& resource_tag,
                                                       pb::common::Range region_range, uint64_t schema_id,
                                                       uint64_t table_id, uint64_t split_from_region_id,
                                                       uint64_t& new_region_id,
                                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (split_from_region_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "split_from_region_id must be positive");
  }

  std::vector<uint64_t> store_ids;

  pb::common::Region split_from_region;
  int ret = region_map_.Get(split_from_region_id, split_from_region);
  if (ret < 0) {
    DINGO_LOG(INFO) << "CreateRegionForSplit region_id not exists, id=" << split_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region_id not exists");
  }

  for (const auto& peer : split_from_region.definition().peers()) {
    store_ids.push_back(peer.store_id());
  }

  // create region with split_from_region_id & store_ids
  return CreateRegion(region_name, resource_tag, store_ids.size(), region_range, schema_id, table_id, store_ids,
                      split_from_region_id, new_region_id, meta_increment);
}

butil::Status CoordinatorControl::CreateRegion(const std::string& region_name, const std::string& resource_tag,
                                               int32_t replica_num, pb::common::Range region_range, uint64_t schema_id,
                                               uint64_t table_id, uint64_t& new_region_id,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  std::vector<uint64_t> store_ids;
  return CreateRegion(region_name, resource_tag, replica_num, region_range, schema_id, table_id, store_ids, 0,
                      new_region_id, meta_increment);
}

butil::Status CoordinatorControl::SelectStore(int32_t replica_num, const std::string& resource_tag,
                                              std::vector<uint64_t>& store_ids,
                                              std::vector<pb::common::Store>& selected_stores_for_regions) {
  DINGO_LOG(INFO) << "SelectStore replica_num=" << replica_num << ", resource_tag=" << resource_tag
                  << ", store_ids.size=" << store_ids.size();

  std::vector<pb::common::Store> stores_for_regions;

  // if store_ids is not null, select store with store_ids
  // or when resource_tag exists, select store with resource_tag
  butil::FlatMap<uint64_t, pb::common::Store> store_map_copy;
  store_map_copy.init(100);
  store_map_.GetFlatMapCopy(store_map_copy);

  // select store for region
  if (store_ids.empty()) {
    for (const auto& element : store_map_copy) {
      const auto& store = element.second;
      if (store.state() != pb::common::StoreState::STORE_NORMAL ||
          store.in_state() != pb::common::StoreInState::STORE_IN) {
        DINGO_LOG(INFO) << "Store state not normal or in, store_id=" << store.id()
                        << ", state=" << pb::common::StoreState_Name(store.state())
                        << ", in_state=" << pb::common::StoreInState_Name(store.in_state());
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
    std::string selected_store_ids;
    for (const auto& store : stores_for_regions) {
      selected_store_ids += std::to_string(store.id()) + ",";
    }
    DINGO_LOG(INFO) << "Not enough stores STORE_NORMAL for create region, replica_num=" << replica_num
                    << ", resource_tag=" << resource_tag << ", store_ids.size=" << store_ids.size()
                    << ", selected_store_ids=" << selected_store_ids;
    return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "Not enough stores for create region");
  }

  struct StoreMore {
    pb::common::Store store;
    uint64_t weight;
    uint64_t region_num;
    uint64_t free_capacity;
    uint64_t total_capacity;
  };

  // check and sort store by capacity, regions_num
  std::vector<StoreMore> store_more_vec;
  {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    for (const auto& it : stores_for_regions) {
      StoreMore store_more;
      store_more.store = it;

      auto* ptr = store_metrics_map_.seek(it.id());
      if (ptr != nullptr) {
        store_more.region_num = ptr->region_metrics_map_size() > 0 ? ptr->region_metrics_map_size() : 0;
        store_more.free_capacity = ptr->free_capacity() > 0 ? ptr->free_capacity() : 0;
        store_more.total_capacity = ptr->total_capacity() > 0 ? ptr->total_capacity() : 0;
      } else {
        store_more.region_num = 0;
        store_more.free_capacity = 0;
        store_more.total_capacity = 0;
      }

      if (store_more.total_capacity == 0) {
        store_more.weight = 0;
      } else {
        store_more.weight =
            store_more.free_capacity * 100 / store_more.total_capacity + (100 / (store_more.region_num + 1));
      }

      store_more.weight = Helper::GenerateRandomInteger(0, 20);

      store_more_vec.push_back(store_more);
      DINGO_LOG(INFO) << "store_more_vec.push_back store_id=" << store_more.store.id()
                      << ", region_num=" << store_more.region_num << ", free_capacity=" << store_more.free_capacity
                      << ", total_capacity=" << store_more.total_capacity << ", weight=" << store_more.weight;
    }
  }

  // if not enough stores is selected, return -1
  if (store_more_vec.size() < replica_num) {
    DINGO_LOG(INFO) << "Not enough stores with metrics for create region";
    return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "Not enough stores for create region");
  }

  // sort store by weight
  std::sort(store_more_vec.begin(), store_more_vec.end(),
            [](const StoreMore& a, const StoreMore& b) { return a.weight > b.weight; });

  DINGO_LOG(INFO) << "store_more_vec.size=" << store_more_vec.size() << ", replica_num=" << replica_num;

  // select replica_num stores
  std::string store_ids_str;
  selected_stores_for_regions.reserve(replica_num);
  for (int i = 0; i < replica_num; i++) {
    selected_stores_for_regions.push_back(store_more_vec[i].store);
    store_ids_str += std::to_string(store_more_vec[i].store.id()) + ",";
  }

  DINGO_LOG(INFO) << "selected_stores_for_regions.size=" << selected_stores_for_regions.size()
                  << ", store_ids_str=" << store_ids_str;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CreateRegion(const std::string& region_name, const std::string& resource_tag,
                                               int32_t replica_num, pb::common::Range region_range, uint64_t schema_id,
                                               uint64_t table_id, std::vector<uint64_t>& store_ids,
                                               uint64_t split_from_region_id, uint64_t& new_region_id,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  std::vector<pb::common::Store> selected_stores_for_regions;

  // select store for region
  auto ret = SelectStore(replica_num, resource_tag, store_ids, selected_stores_for_regions);
  if (!ret.ok()) {
    return ret;
  }

  // generate new region
  if (new_region_id <= 0) {
    new_region_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION, meta_increment);
  }

  uint64_t const create_region_id = new_region_id;

  if (region_map_.Exists(create_region_id)) {
    DINGO_LOG(ERROR) << "create_region_id =" << create_region_id << " is illegal, cannot create region!!";
    return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "create_region_id is illegal");
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
    region_cmd->set_is_notify(true);  // for create region, we need immediately heartbeat
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

  // need to update table's range distribution
  if (split_from_region_id > 0 && table_id > 0) {
    pb::coordinator_internal::TableInternal table_internal;
    int ret = table_map_.Get(table_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(INFO) << "CreateRegionForSplit table_id not exists, id=" << table_id;
      return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not exists");
    }

    // update table's range distribution
    auto* update_table_internal = meta_increment.add_tables();
    update_table_internal->set_id(table_id);
    update_table_internal->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
    auto* update_table_internal_table = update_table_internal->mutable_table();
    update_table_internal_table->set_id(table_id);
    for (const auto& it : table_internal.partitions()) {
      update_table_internal_table->add_partitions()->CopyFrom(it);
    }

    auto* new_partition = update_table_internal_table->add_partitions();
    // new_partition->mutable_range()->set_start_key(region_range.start_key());
    // new_partition->mutable_range()->set_end_key(region_range.end_key());
    new_partition->set_region_id(create_region_id);
  }

  // on_apply
  // region_map_epoch++;                                               // raft_kv_put
  // region_map_.insert(std::make_pair(create_region_id, new_region));  // raft_kv_put

  // new_region_id = create_region_id;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DropRegion(uint64_t region_id,
                                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  return DropRegion(region_id, false, meta_increment);
}

butil::Status CoordinatorControl::DropRegion(uint64_t region_id, bool need_update_table_range,
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

        // use TaskList to drop & purge region
        for (const auto& peer : region_to_delete.definition().peers()) {
          auto* increment_task_list = CreateTaskList(meta_increment);

          // this is delete_region task
          AddDeleteTask(increment_task_list, peer.store_id(), region_id, meta_increment);

          // this is purge_region task
          AddPurgeTask(increment_task_list, peer.store_id(), region_id, meta_increment);
        }

        // need to update table's range distribution if table_id > 0
        if (need_update_table_range && region_to_delete.definition().table_id() > 0) {
          pb::coordinator_internal::TableInternal table_internal;
          int ret = table_map_.Get(region_to_delete.definition().table_id(), table_internal);
          if (ret < 0) {
            DINGO_LOG(WARNING) << "DropRegion table_id not exists, region_id=" << region_id
                               << " region_id=" << region_to_delete.definition().table_id();
            // return pb::error::Errno::ETABLE_NOT_FOUND;
          } else {
            // update table's range distribution
            auto* update_table_internal = meta_increment.add_tables();
            update_table_internal->set_id(region_to_delete.definition().table_id());
            update_table_internal->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
            auto* update_table_internal_table = update_table_internal->mutable_table();
            update_table_internal_table->set_id(region_to_delete.definition().table_id());
            for (const auto& it : table_internal.partitions()) {
              if (it.region_id() != region_id) {
                update_table_internal_table->add_partitions()->CopyFrom(it);
              }
            }
          }
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
      return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "drop region id not exists");
    }
  }

  if (need_update_epoch) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);
  }

  return butil::Status::OK();
}

// DropRegionPermanently
// delete region from disk
butil::Status CoordinatorControl::DropRegionPermanently(uint64_t region_id,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  // set region state to DELETE
  bool need_update_epoch = false;
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    pb::common::Region region_to_delete;
    int ret = region_map_.Get(region_id, region_to_delete);
    if (ret < 0) {
      DINGO_LOG(INFO) << "DropRegionPermanently region not exists, id = " << region_id;
      return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "DropRegionPermanently region not exists");
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

  return butil::Status::OK();
}

butil::Status CoordinatorControl::SplitRegion(uint64_t split_from_region_id, uint64_t split_to_region_id,
                                              std::string split_watershed_key,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate split_from_region_id
  pb::common::Region split_from_region;
  int ret = region_map_.Get(split_from_region_id, split_from_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "SplitRegion from region not exists, id = " << split_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "SplitRegion from region not exists");
  }

  // validate split_to_region_id
  pb::common::Region split_to_region;
  ret = region_map_.Get(split_to_region_id, split_to_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "SplitRegion to region not exists, id = " << split_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "SplitRegion to region not exists");
  }

  // validate split_from_region and split_to_region has NORMAL status
  if (split_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      split_from_region.raft_status() != ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      split_from_region.heartbeat_state() != ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region is not ready for split, "
                        "split_from_region_id = "
                     << split_from_region_id << " from_state=" << split_from_region.state();
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region is not ready for split");
  }

  // validate split_watershed_key
  if (split_watershed_key.empty()) {
    DINGO_LOG(ERROR) << "SplitRegion split_watershed_key is empty";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "SplitRegion split_watershed_key is empty");
  }

  // validate split_from_region_id and split_to_region_id
  if (split_from_region_id == split_to_region_id) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region_id == split_to_region_id";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "SplitRegion split_from_region_id == split_to_region_id");
  }

  // validate split_from_region and split_to_region has same peers
  if (split_from_region.definition().peers_size() != split_to_region.definition().peers_size()) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region and split_to_region has different peers size";
    return butil::Status(pb::error::Errno::ESPLIT_PEER_NOT_MATCH,
                         "SplitRegion split_from_region and split_to_region has different peers size");
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
    return butil::Status(pb::error::Errno::ESPLIT_PEER_NOT_MATCH,
                         "SplitRegion split_from_region and split_to_region has different peers");
  }

  // validate split_from_region and split_to_region has NORMAL status
  if (split_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      split_to_region.state() != ::dingodb::pb::common::RegionState::REGION_STANDBY) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region or split_to_region is not ready for split, "
                        "split_from_region_id = "
                     << split_from_region_id << " from_state=" << split_from_region.state()
                     << ", split_to_region_id = " << split_to_region_id << " to_state=" << split_to_region.state();
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region or split_to_region is not ready for split");
  }

  // generate store operation for stores
  pb::coordinator::RegionCmd region_cmd;
  region_cmd.set_region_id(split_from_region_id);
  region_cmd.set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_SPLIT);
  region_cmd.mutable_split_request()->set_split_watershed_key(split_watershed_key);
  region_cmd.mutable_split_request()->set_split_from_region_id(split_from_region_id);
  region_cmd.mutable_split_request()->set_split_to_region_id(split_to_region_id);
  region_cmd.set_create_timestamp(butil::gettimeofday_ms());

  // only send split region_cmd to split_from_region_id's leader store id
  if (split_from_region.leader_store_id() == 0) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region_id's leader_store_id is 0, split_from_region_id="
                     << split_from_region_id << ", split_to_region_id=" << split_to_region_id;
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region_id's leader_store_id is 0");
  }

  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(split_from_region.leader_store_id());  // this store id
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  auto* store_operation = store_operation_increment->mutable_store_operation();
  store_operation->set_id(split_from_region.leader_store_id());
  auto* region_cmd_to_add = store_operation->add_region_cmds();
  region_cmd_to_add->CopyFrom(region_cmd);
  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));

  return butil::Status::OK();
}

butil::Status CoordinatorControl::SplitRegionWithTaskList(uint64_t split_from_region_id, uint64_t split_to_region_id,
                                                          std::string split_watershed_key,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  auto validate_ret = ValidateTaskListConflict(split_from_region_id, split_to_region_id);
  if (!validate_ret.ok()) {
    DINGO_LOG(ERROR) << "SplitRegionWithTaskList validate task list conflict failed, split_from_region_id="
                     << split_from_region_id << ", split_to_region_id=" << split_to_region_id;
    return validate_ret;
  }

  if (split_to_region_id > 0) {
    return SplitRegion(split_from_region_id, split_to_region_id, split_watershed_key, meta_increment);
  }

  // validate split_from_region_id
  pb::common::Region split_from_region;
  int ret = region_map_.Get(split_from_region_id, split_from_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "SplitRegion from region not exists, id = " << split_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "SplitRegion from region not exists");
  }

  // validate split_watershed_key
  if (split_watershed_key.empty()) {
    DINGO_LOG(ERROR) << "SplitRegion split_watershed_key is empty";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "SplitRegion split_watershed_key is empty");
  }

  if (split_from_region.definition().range().start_key().compare(split_watershed_key) >= 0 ||
      split_from_region.definition().range().end_key().compare(split_watershed_key) <= 0) {
    DINGO_LOG(ERROR) << "SplitRegion split_watershed_key is illegal, split_watershed_key = "
                     << Helper::StringToHex(split_watershed_key) << ", split_from_region_id = " << split_from_region_id
                     << " start_key=" << Helper::StringToHex(split_from_region.definition().range().start_key())
                     << ", end_key=" << Helper::StringToHex(split_from_region.definition().range().end_key());
    return butil::Status(pb::error::Errno::EKEY_INVALID, "SplitRegion split_watershed_key is illegal");
  }

  // validate split_from_region and split_to_region has NORMAL status
  if (split_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      split_from_region.raft_status() != ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      split_from_region.heartbeat_state() != ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region is not ready for split, "
                        "split_from_region_id = "
                     << split_from_region_id << " from_state=" << split_from_region.state();
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL, "SplitRegion split_from_region is not ready");
  }

  // only send split region_cmd to split_from_region_id's leader store id
  if (split_from_region.leader_store_id() == 0) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region_id's leader_store_id is 0, split_from_region_id="
                     << split_from_region_id << ", split_to_region_id=" << split_to_region_id;
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region_id's leader_store_id is 0");
  }

  // create task list
  auto* new_task_list = CreateTaskList(meta_increment);

  // call create_region to get store_operations
  pb::coordinator_internal::MetaIncrement meta_increment_tmp;
  uint64_t new_region_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION, meta_increment);
  CreateRegionForSplitInternal(split_from_region_id, new_region_id, meta_increment_tmp);

  // build create_region task
  auto* create_region_task = new_task_list->add_tasks();
  for (const auto& it : meta_increment_tmp.store_operations()) {
    const auto& store_operation = it.store_operation();
    create_region_task->add_store_operations()->CopyFrom(store_operation);
  }

  // update region_map for new_region_id
  for (const auto& it : meta_increment_tmp.regions()) {
    meta_increment.add_regions()->CopyFrom(it);
  }

  // update table_map for new_region_id
  // CreateRegion will update table_map if region is create for split
  for (const auto& it : meta_increment_tmp.tables()) {
    meta_increment.add_tables()->CopyFrom(it);
  }

  // build split_region task
  AddSplitTask(new_task_list, split_from_region.leader_store_id(), split_from_region_id, new_region_id,
               split_watershed_key, meta_increment);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::MergeRegionWithTaskList(uint64_t merge_from_region_id, uint64_t merge_to_region_id,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  auto validate_ret = ValidateTaskListConflict(merge_from_region_id, merge_to_region_id);
  if (!validate_ret.ok()) {
    DINGO_LOG(ERROR) << "mergeRegionWithTaskList validate task list conflict failed, merge_from_region_id="
                     << merge_from_region_id << ", merge_to_region_id=" << merge_to_region_id;
    return validate_ret;
  }

  // validate merge_from_region_id
  pb::common::Region merge_from_region;
  int ret = region_map_.Get(merge_from_region_id, merge_from_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "MergeRegion from region not exists, id = " << merge_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "MergeRegion from region not exists");
  }

  // validate merge_to_region_id
  pb::common::Region merge_to_region;
  ret = region_map_.Get(merge_to_region_id, merge_to_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "MergeRegion to region not exists, id = " << merge_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "MergeRegion to region not exists");
  }

  // validate merge_from_region and merge_to_region has NORMAL status
  if (merge_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      merge_from_region.raft_status() != ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      merge_from_region.heartbeat_state() != ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region is not ready for merge, "
                        "merge_from_region_id = "
                     << merge_from_region_id << " from_state=" << merge_from_region.state();
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL,
                         "MergeRegion merge_from_region is not ready for merge");
  }

  // validate merge_to_region and merge_to_region has NORMAL status
  if (merge_to_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      merge_to_region.raft_status() != ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      merge_to_region.heartbeat_state() != ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "MergeRegion merge_to_region is not ready for merge, "
                        "merge_to_region_id = "
                     << merge_to_region_id << " from_state=" << merge_to_region.state();
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL, "MergeRegion merge_to_region is not ready for merge");
  }

  // validate merge_from_region_id and merge_to_region_id
  if (merge_from_region_id == merge_to_region_id) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region_id == merge_to_region_id";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "MergeRegion merge_from_region_id == merge_to_region_id");
  }

  // validate merge_from_region and merge_to_region has same peers
  if (merge_from_region.definition().peers_size() != merge_to_region.definition().peers_size()) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region and merge_to_region has different peers size";
    return butil::Status(pb::error::Errno::EMERGE_PEER_NOT_MATCH,
                         "MergeRegion merge_from_region and merge_to_region has different peers size");
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
    return butil::Status(pb::error::Errno::EMERGE_PEER_NOT_MATCH,
                         "MergeRegion merge_from_region and merge_to_region has different peers");
  }

  // validate merge_from_region and merge_to_region status
  if (merge_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      merge_to_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region or merge_to_region is not NORMAL";
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL,
                         "MergeRegion merge_from_region or merge_to_region is not NORMAL");
  }

  // validate merge_from_region and merge_to_region has same start_key and end_key
  if (merge_from_region.definition().range().start_key() != merge_to_region.definition().range().end_key()) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region and merge_to_region has different start_key or end_key";
    return butil::Status(pb::error::Errno::EMERGE_RANGE_NOT_MATCH,
                         "MergeRegion merge_from_region and merge_to_region has different start_key or end_key");
  }

  // only send merge region_cmd to merge_from_region_id's leader store id
  if (merge_from_region.leader_store_id() == 0) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region_id's leader_store_id is 0, merge_from_region_id="
                     << merge_from_region_id << ", merge_to_region_id=" << merge_to_region_id;
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL,
                         "MergeRegion merge_from_region_id's leader_store_id is 0");
  }

  // build task list
  auto* new_task_list = CreateTaskList(meta_increment);

  // build merege task
  AddMergeTask(new_task_list, merge_from_region.leader_store_id(), merge_from_region_id, merge_to_region_id,
               meta_increment);

  // build drop region task
  auto* drop_region_task = new_task_list->add_tasks();
  auto* task_pre_check_drop = drop_region_task->mutable_pre_check();
  task_pre_check_drop->set_type(pb::coordinator::TaskPreCheckType::REGION_CHECK);
  auto* region_check = task_pre_check_drop->mutable_region_check();
  region_check->set_region_id(merge_from_region_id);
  region_check->set_state(::dingodb::pb::common::RegionState::REGION_NORMAL);
  region_check->mutable_range()->set_start_key(merge_from_region.definition().range().end_key());
  region_check->mutable_range()->set_end_key(merge_from_region.definition().range().end_key());

  // call drop_region to get store_operations
  pb::coordinator_internal::MetaIncrement meta_increment_tmp;
  DropRegion(merge_from_region_id, true, meta_increment_tmp);
  for (const auto& it : meta_increment_tmp.store_operations()) {
    const auto& store_operation = it.store_operation();
    drop_region_task->add_store_operations()->CopyFrom(store_operation);
  }

  // update region_map for drop region
  for (const auto& it : meta_increment_tmp.regions()) {
    meta_increment.add_regions()->CopyFrom(it);
  }

  return butil::Status::OK();
}

// ChangePeerRegionWithTaskList
butil::Status CoordinatorControl::ChangePeerRegionWithTaskList(
    uint64_t region_id, std::vector<uint64_t>& new_store_ids, pb::coordinator_internal::MetaIncrement& meta_increment) {
  auto validate_ret = ValidateTaskListConflict(region_id, region_id);
  if (!validate_ret.ok()) {
    DINGO_LOG(ERROR) << "ChangePeerRegionWithTaskList validate task list conflict failed, change_peer_region_id="
                     << region_id;
    return validate_ret;
  }

  // validate region_id
  pb::common::Region region;
  int ret = region_map_.Get(region_id, region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ChangePeerRegion region not exists, id = " << region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "ChangePeerRegion region not exists");
  }

  // validate region has NORMAL status
  if (region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      region.raft_status() != ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      region.heartbeat_state() != ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "ChangePeerRegion region is not ready for change_peer, region_id = " << region_id;
    return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                         "ChangePeerRegion region is not ready for change_peer");
  }

  // validate new_store_ids
  if (new_store_ids.size() != (region.definition().peers_size() + 1) &&
      new_store_ids.size() != (region.definition().peers_size() - 1) && (!new_store_ids.empty())) {
    DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids size not match, region_id = " << region_id
                     << " old_size = " << region.definition().peers_size() << " new_size = " << new_store_ids.size();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "ChangePeerRegion new_store_ids size not match");
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

  if (new_store_ids_diff_more.size() + new_store_ids_diff_less.size() != 1) {
    DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids can only has one diff store, region_id = " << region_id
                     << " new_store_ids_diff_more.size() = " << new_store_ids_diff_more.size()
                     << " new_store_ids_diff_less.size() = " << new_store_ids_diff_less.size();
    for (auto it : new_store_ids_diff_more) DINGO_LOG(ERROR) << "new_store_ids_diff_more = " << it;
    for (auto it : new_store_ids_diff_less) DINGO_LOG(ERROR) << "new_store_ids_diff_less = " << it;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "ChangePeerRegion new_store_ids can only has one diff store");
  }

  // this is the new definition of region
  pb::common::RegionDefinition new_region_definition;
  new_region_definition.CopyFrom(region.definition());
  new_region_definition.clear_peers();

  // generate store operation for stores
  if (new_store_ids_diff_less.size() == 1) {
    if (region.leader_store_id() == new_store_ids_diff_less.at(0) && new_store_ids.size() < 3) {
      DINGO_LOG(ERROR) << "ChangePeerRegion region.leader_store_id() == new_store_ids_diff_less.at(0), region_id = "
                       << region_id << " new_store_ids.size() = " << new_store_ids.size();
      return butil::Status(
          pb::error::Errno::ECHANGE_PEER_UNABLE_TO_REMOVE_LEADER,
          "ChangePeerRegion region.leader_store_id() == new_store_ids_diff_less.at(0) and new_store_ids.size() < 3");
    }

    if (region.leader_store_id() == 0) {
      DINGO_LOG(ERROR) << "ChangePeerRegion region.leader_store_id() == 0, region_id = " << region_id;
      return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                           "ChangePeerRegion region.leader_store_id() == 0");
    }

    // calculate new peers
    for (int i = 0; i < region.definition().peers_size(); i++) {
      if (region.definition().peers(i).store_id() != new_store_ids_diff_less.at(0)) {
        auto* peer = new_region_definition.add_peers();
        peer->CopyFrom(region.definition().peers(i));
      }
    }

    // build new task_list
    auto* increment_task_list = CreateTaskList(meta_increment);

    // this is change_peer task
    AddChangePeerTask(increment_task_list, region.leader_store_id(), region_id, new_region_definition, meta_increment);

    // this is delete_region task
    AddDeleteTaskWithCheck(increment_task_list, new_store_ids_diff_less.at(0), region_id, new_region_definition.peers(),
                           meta_increment);

    // this is purge_region task
    AddPurgeTask(increment_task_list, new_store_ids_diff_less.at(0), region_id, meta_increment);

  } else if (new_store_ids_diff_more.size() == 1) {
    // expand region
    // calculate new peers
    // validate new_store_ids_diff_more is legal
    pb::common::Store store_to_add_peer;
    int ret = store_map_.Get(new_store_ids_diff_more.at(0), store_to_add_peer);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids_diff_more not exists, region_id = " << region_id;
      return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "ChangePeerRegion new_store_ids_diff_more not exists");
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

    if (region.leader_store_id() == 0) {
      DINGO_LOG(ERROR) << "ChangePeerRegion region.leader_store_id() == 0, region_id = " << region_id;
      return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                           "ChangePeerRegion region.leader_store_id() == 0");
    }

    // build new task_list
    auto* increment_task_list = CreateTaskList(meta_increment);

    // this create region task
    AddCreateTask(increment_task_list, new_store_ids_diff_more.at(0), region_id, new_region_definition, meta_increment);

    // this change peer check task, no store_operation, only for check
    auto* change_peer_check_task = increment_task_list->add_tasks();
    auto* region_check = change_peer_check_task->mutable_pre_check();
    region_check->set_type(::dingodb::pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK);
    region_check->mutable_store_region_check()->set_store_id(new_store_ids_diff_more.at(0));
    region_check->mutable_store_region_check()->set_region_id(region_id);

    // this is change peer task
    AddChangePeerTask(increment_task_list, region.leader_store_id(), region_id, new_region_definition, meta_increment);

  } else {
    DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids not match, region_id = " << region_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "ChangePeerRegion new_store_ids not match");
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::TransferLeaderRegionWithTaskList(
    uint64_t region_id, uint64_t new_leader_store_id, pb::coordinator_internal::MetaIncrement& meta_increment) {
  // check region_id exists
  pb::common::Region region;
  int ret = region_map_.Get(region_id, region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion region_id not exists, region_id = " << region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "TransferLeaderRegion region_id not exists");
  }

  if (region.state() != pb::common::RegionState::REGION_NORMAL) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion region.state() != REGION_NORMAL, region_id = " << region_id;
    return butil::Status(pb::error::Errno::EREGION_STATE, "TransferLeaderRegion region.state() != REGION_NORMAL");
  }

  if (region.heartbeat_state() != pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion region.heartbeat_state() != REGION_ONLINE, region_id = " << region_id;
    return butil::Status(pb::error::Errno::EREGION_STATE,
                         "TransferLeaderRegion region.heartbeat_state() != REGION_ONLINE");
  }

  if (region.leader_store_id() == new_leader_store_id) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion new_leader_store_id == old_leader_store_id, region_id = " << region_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "TransferLeaderRegion new_leader_store_id == old_leader_store_id");
  }

  if (region.leader_store_id() == 0) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion region.leader_store_id() == 0, region_id = " << region_id;
    return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                         "TransferLeaderRegion region.leader_store_id() == 0");
  }

  // check new_leader_store_id exists
  pb::common::Store store_to_transfer_leader;
  ret = store_map_.Get(new_leader_store_id, store_to_transfer_leader);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion new_leader_store_id not exists, region_id = " << region_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "TransferLeaderRegion new_leader_store_id not exists");
  }

  if (store_to_transfer_leader.state() != pb::common::StoreState::STORE_NORMAL) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion new_leader_store_id not running, region_id = " << region_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "TransferLeaderRegion new_leader_store_id not running");
  }

  // check new_leader_store_id in region
  pb::common::Peer new_leader_peer;
  for (int i = 0; i < region.definition().peers_size(); i++) {
    if (region.definition().peers(i).store_id() == new_leader_store_id) {
      new_leader_peer = region.definition().peers(i);
      break;
    }
  }

  if (new_leader_peer.store_id() == 0) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion new_leader_store_id not in region, region_id = " << region_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "TransferLeaderRegion new_leader_store_id not in region");
  }

  // build new task_list
  auto* increment_task_list = CreateTaskList(meta_increment);

  // this transfer leader task
  AddTransferLeaderTask(increment_task_list, region.leader_store_id(), region_id, new_leader_peer, meta_increment);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::ValidateTaskListConflict(uint64_t region_id, uint64_t second_region_id) {
  // check task_list conflict
  butil::FlatMap<uint64_t, pb::coordinator::TaskList> task_list_map_temp;
  task_list_map_temp.init(1000);
  int ret = task_list_map_.GetFlatMapCopy(task_list_map_temp);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ValidateTaskListConflict task_list_map_.GetFlatMapCopy failed, region_id = " << region_id;
    return butil::Status(pb::error::Errno::EINTERNAL, "ValidateTaskListConflict task_list_map_.GetFlatMapCopy failed");
  }

  for (const auto& task_list : task_list_map_temp) {
    for (const auto& task : task_list.second.tasks()) {
      for (const auto& store_operation : task.store_operations()) {
        for (const auto& region_cmd : store_operation.region_cmds()) {
          if (region_cmd.region_id() == region_id || region_cmd.region_id() == second_region_id) {
            DINGO_LOG(ERROR) << "ValidateTaskListConflict task_list conflict, region_id = " << region_id;
            return butil::Status(
                pb::error::Errno::ETASK_LIST_CONFLICT,
                "ValidateTaskListConflict task_list conflict, region_id = " + std::to_string(region_id));
          }
        }
      }
    }
  }

  // check store operation conflict
  butil::FlatMap<uint64_t, pb::coordinator::StoreOperation> store_operation_map_temp;
  store_operation_map_temp.init(1000);
  ret = store_operation_map_.GetFlatMapCopy(store_operation_map_temp);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ValidateTaskListConflict store_operation_map_.GetFlatMapCopy failed, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::EINTERNAL,
                         "ValidateTaskListConflict store_operation_map_.GetFlatMapCopy failed, region_id = " +
                             std::to_string(region_id));
  }

  for (const auto& store_operation : store_operation_map_temp) {
    for (const auto& region_cmd : store_operation.second.region_cmds()) {
      if (region_cmd.region_id() == region_id || region_cmd.region_id() == second_region_id) {
        DINGO_LOG(ERROR) << "ValidateTaskListConflict store_operation conflict, region_id = " << region_id;
        return butil::Status(
            pb::error::Errno::ESTORE_OPERATION_CONFLICT,
            "ValidateTaskListConflict store_operation conflict, region_id = " + std::to_string(region_id));
      }
    }
  }

  return butil::Status::OK();
}

// CleanStoreOperation
butil::Status CoordinatorControl::CleanStoreOperation(uint64_t store_id,
                                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::coordinator::StoreOperation store_operation;
  int ret = store_operation_map_.Get(store_id, store_operation);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "CleanStoreOperation store operation not exists, store_id = " << store_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "CleanStoreOperation store operation not exists");
  }

  // clean store operation
  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(store_id);
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  store_operation_increment->mutable_store_operation()->CopyFrom(store_operation);

  return butil::Status::OK();
}

// AddStoreOperation
butil::Status CoordinatorControl::AddStoreOperation(const pb::coordinator::StoreOperation& store_operation,
                                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate store id
  uint64_t store_id = store_operation.id();
  int ret = store_map_.Exists(store_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "AddStoreOperation store not exists, store_id = " << store_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "AddStoreOperation store not exists");
  }

  // validate store operation region_cmd
  if (store_operation.region_cmds_size() == 0) {
    DINGO_LOG(ERROR) << "AddStoreOperation store operation region cmd empty, store_id = " << store_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "AddStoreOperation store operation region cmd empty");
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
          return butil::Status(pb::error::Errno::EREGION_CMD_ONGOING_CONFLICT,
                               "AddStoreOperation store operation region cmd ongoing conflict");
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
  return butil::Status::OK();
}

butil::Status CoordinatorControl::RemoveStoreOperation(uint64_t store_id, uint64_t region_cmd_id,
                                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::coordinator::StoreOperation store_operation;
  int ret = store_operation_map_.Get(store_id, store_operation);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "RemoveStoreOperation store operation not exists, store_id = " << store_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "RemoveStoreOperation store operation not exists");
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
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "RemoveStoreOperation region_cmd_id not exists");
  }

  auto* store_operation_increment_to_delete = meta_increment.add_store_operations();
  store_operation_increment_to_delete->CopyFrom(store_operation_increment);

  return butil::Status::OK();
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

butil::Status CoordinatorControl::GetExecutorUserMap(uint64_t cluster_id,
                                                     pb::common::ExecutorUserMap& executor_user_map) {
  if (cluster_id < 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id < 0");
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
  return butil::Status::OK();
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

butil::Status CoordinatorControl::CreateExecutor(uint64_t cluster_id, pb::common::Executor& executor,
                                                 pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id <= 0");
  }

  pb::common::Executor executor_to_create;
  executor_to_create.CopyFrom(executor);

  if (!ValidateExecutorUser(executor_to_create.executor_user())) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "ValidateExecutorUser fail");
  }

  if (executor_to_create.id().empty() &&
      ((!executor_to_create.server_location().host().empty()) && executor_to_create.server_location().port() != 0)) {
    executor_to_create.set_id(executor_to_create.server_location().host() + ":" +
                              std::to_string(executor_to_create.server_location().port()));
  } else {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "executor_to_create.id() is empty");
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

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DeleteExecutor(uint64_t cluster_id, const pb::common::Executor& executor,
                                                 pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0 || executor.id().length() <= 0 || executor.executor_user().user().length() <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id <= 0 || executor.id().length() <= 0");
  }

  pb::common::Executor executor_to_delete;
  {
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    int ret = executor_map_.Get(executor.id(), executor_to_delete);
    if (ret < 0) {
      DINGO_LOG(INFO) << "DeleteExecutor executor_id not exists, id=" << executor.id();
      return butil::Status(pb::error::Errno::EEXECUTOR_NOT_FOUND, "executor_id not exists");
    }

    // validate executor_user
    if (!ValidateExecutorUser(executor.executor_user())) {
      DINGO_LOG(INFO) << "DeleteExecutor executor_id id=" << executor.id() << " validate user fail";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "ValidateExecutorUser fail");
    }
  }

  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executors();
  executor_increment->set_id(executor.id());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

  auto* executor_increment_executor = executor_increment->mutable_executor();
  executor_increment_executor->CopyFrom(executor_to_delete);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CreateExecutorUser(uint64_t cluster_id, pb::common::ExecutorUser& executor_user,
                                                     pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id <= 0");
  }

  if (executor_user_map_.Exists(executor_user.user())) {
    DINGO_LOG(INFO) << "CreateExecutorUser user already exists, user=" << executor_user.user();
    return butil::Status(pb::error::Errno::EUSER_ALREADY_EXIST, "user already exists");
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

  return butil::Status::OK();
}

butil::Status CoordinatorControl::UpdateExecutorUser(uint64_t cluster_id, const pb::common::ExecutorUser& executor_user,
                                                     const pb::common::ExecutorUser& executor_user_update,
                                                     pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id <= 0");
  }

  pb::coordinator_internal::ExecutorUserInternal executor_user_internal;
  int ret = executor_user_map_.Get(executor_user.user(), executor_user_internal);
  if (ret < 0) {
    DINGO_LOG(INFO) << "UpdateExecutorUser user not exists, user=" << executor_user.user();
    return butil::Status(pb::error::Errno::EUSER_NOT_EXIST, "user not exists");
  }

  if (executor_user.keyring().length() > 0 && executor_user.keyring() != executor_user_internal.keyring()) {
    DINGO_LOG(INFO) << "UpdateExecutorUser user keyring not equal, user=" << executor_user.user()
                    << " input keyring=" << executor_user.keyring()
                    << " but executor_user's keyring=" << executor_user_internal.keyring();
    return butil::Status(pb::error::Errno::EKEYRING_ILLEGAL, "user keyring not equal");
  }

  // executor_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR, meta_increment);

  // update meta_increment
  // GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executor_users();
  executor_increment->set_id(executor_user.user());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  executor_increment->mutable_executor_user()->set_id(executor_user.user());
  executor_increment->mutable_executor_user()->set_keyring(executor_user_update.keyring());

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DeleteExecutorUser(uint64_t cluster_id, pb::common::ExecutorUser& executor_user,
                                                     pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id <= 0");
  }

  pb::coordinator_internal::ExecutorUserInternal executor_user_in_map;
  int ret = executor_user_map_.Get(executor_user.user(), executor_user_in_map);
  if (ret < 0) {
    DINGO_LOG(INFO) << "DeleteExecutorUser user not exists, user=" << executor_user.user();
    return butil::Status(pb::error::Errno::EUSER_NOT_EXIST, "user not exists");
  }

  if (executor_user.keyring().length() > 0 && executor_user.keyring() != executor_user_in_map.keyring()) {
    DINGO_LOG(INFO) << "DeleteExecutorUser keyring not equal, input keyring=" << executor_user.keyring()
                    << " but executor_user's keyring=" << executor_user_in_map.keyring();
    return butil::Status(pb::error::Errno::EKEYRING_ILLEGAL, "keyring not equal");
  }

  // executor_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR, meta_increment);

  // update meta_increment
  // GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executor_users();
  executor_increment->set_id(executor_user.user());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  executor_increment->mutable_executor_user()->set_id(executor_user.user());
  executor_increment->mutable_executor_user()->set_keyring(executor_user.keyring());

  return butil::Status::OK();
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

    // when region leader change or region state change, we need to update region_map_
    // or when region last_update_timestamp is too old, we need to update region_map_
    bool need_update_region = false;
    bool need_update_region_definition = false;
    bool region_metrics_is_not_leader = false;

    // use leader store's region_metrics to update region_map
    // if region's store_region_stat is deleting or deleted, there will be no leaders in this region
    // so we need to update region state using any node of this region
    if (region_metrics.braft_status().raft_state() != pb::common::RaftNodeState::STATE_LEADER) {
      if (region_metrics.store_region_state() != pb::common::StoreRegionState::DELETING &&
          region_metrics.store_region_state() != pb::common::StoreRegionState::DELETED) {
        continue;
      }
      region_metrics_is_not_leader = true;
    }

    // update region_map
    pb::common::Region region_to_update;
    int ret = region_map_.Get(region_metrics.id(), region_to_update);
    if (ret < 0) {
      DINGO_LOG(WARNING)
          << "region deleted and coordinator has purged it, something wrong in store, need to purge this "
             "region on store again, region_id = "
          << region_metrics.id();
      continue;
    }

    if (region_metrics_is_not_leader) {
      if (region_to_update.state() != pb::common::RegionState::REGION_DELETE &&
          region_to_update.state() != pb::common::RegionState::REGION_DELETING &&
          region_to_update.state() != pb::common::RegionState::REGION_DELETED) {
        DINGO_LOG(INFO) << "region is not deleted, follower can't update region_map, store_id=" << store_metrics.id()
                        << " region_id = " << region_metrics.id();
        continue;
      }
    }

    if (region_metrics.has_braft_status() && region_to_update.leader_store_id() != store_metrics.id()) {
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

    // if region is updated to REGION_DELETED, there is no need to update region by timestamp
    // once region is deleted, all stores will update this region's store_region_state, after the last store reported
    // region's state to DELETED, there is no need to update the region's state in region_map, after timeout, we will
    // trigger purge_request to store to purge this region's meta
    if (region_to_update.last_update_timestamp() + FLAGS_region_update_timeout * 1000 < butil::gettimeofday_ms() &&
        region_to_update.state() != pb::common::RegionState::REGION_DELETED) {
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

    if (region_to_update.definition().peers_size() != region_metrics.region_definition().peers_size()) {
      DINGO_LOG(INFO) << "region peers size change region_id = " << region_metrics.id()
                      << " old peers size = " << region_to_update.definition().peers_size()
                      << " new peers size = " << region_metrics.region_definition().peers_size();
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

    if (region_to_update.state() == pb::common::RegionState::REGION_DELETE ||
        region_to_update.state() == pb::common::RegionState::REGION_DELETING ||
        region_to_update.state() == pb::common::RegionState::REGION_DELETED) {
      if (region_metrics.store_region_state() == pb::common::StoreRegionState::DELETED) {
        region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_DELETED);
      } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::DELETING) {
        region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_DELETING);
      }
    } else {
      if (region_metrics.store_region_state() == pb::common::StoreRegionState::NORMAL) {
        region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_NORMAL);
      } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::SPLITTING) {
        region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_SPLITTING);
      } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::MERGING) {
        region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_MERGING);
      } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::STANDBY) {
        region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_STANDBY);
      } else {
        DINGO_LOG(ERROR) << "ERROR: UpdateRegionMapAndStoreOperation region state error, store_id="
                         << store_metrics.id() << " region_id = " << region_metrics.id()
                         << " state = " << region_metrics.store_region_state();
        continue;
      }
    }

    // for split and merge, we need to update region definition
    if (need_update_region_definition) {
      region_increment_region->mutable_definition()->CopyFrom(region_metrics.region_definition());
    }

    auto region_raft_status = ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY;
    auto region_replica_status = ::dingodb::pb::common::ReplicaStatus::REPLICA_NORMAL;

    uint32_t consecutive_error_follower_count = 0;
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

      if (follower.second.consecutive_error_times() > 10) {
        region_raft_status = region_raft_status < ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_CONSECUTIVE_ERROR
                                 ? region_raft_status
                                 : ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_CONSECUTIVE_ERROR;

        consecutive_error_follower_count++;
      }
    }

    if (consecutive_error_follower_count == 0) {
      region_replica_status = ::dingodb::pb::common::ReplicaStatus::REPLICA_NORMAL;
    } else if (consecutive_error_follower_count < region_metrics.braft_status().stable_followers_size()) {
      region_replica_status = ::dingodb::pb::common::ReplicaStatus::REPLICA_DEGRAED;
    } else {
      region_replica_status = ::dingodb::pb::common::ReplicaStatus::REPLICA_UNAVAILABLE;
    }
    region_increment_region->set_replica_status(region_replica_status);

    // if peer cannot connected, ustable_followers_size will not be 0
    // so we need set replica status to DEGRAED
    // if (region_metrics.braft_status().unstable_followers_size() > 0) {
    //   region_raft_status = ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_CONSECUTIVE_ERROR;
    //   region_increment_region->set_replica_status(::dingodb::pb::common::ReplicaStatus::REPLICA_DEGRAED);
    // } else {
    //   region_increment_region->set_replica_status(::dingodb::pb::common::ReplicaStatus::REPLICA_NORMAL);
    // }

    region_increment_region->set_raft_status(region_raft_status);

    // mbvar region
    if (region_to_update.state() != pb::common::RegionState::REGION_DELETED) {
      coordinator_bvar_metrics_region_.UpdateRegionBvar(region_metrics.id(), region_metrics.row_count(),
                                                        region_metrics.region_size());
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

  {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    store_metrics_map_.insert(store_metrics.id(), store_metrics);
    // if (store_metrics_map_.seek(store_metrics.id()) != nullptr) {
    //   DINGO_LOG(DEBUG) << "STORE METIRCS UPDATE store_metrics.id = " << store_metrics.id();

    //   // update meta_increment
    //   auto* store_metrics_increment = meta_increment.add_store_metrics();
    //   store_metrics_increment->set_id(store_metrics.id());
    //   store_metrics_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

    //   auto* store_metrics_increment_store = store_metrics_increment->mutable_store_metrics();
    //   store_metrics_increment_store->CopyFrom(store_metrics);

    //   // set is_partial_region_metrics
    //   if (store_metrics.is_partial_region_metrics()) {
    //     store_metrics_increment->set_is_partial_region_metrics(store_metrics.is_partial_region_metrics());
    //   }
    // } else {
    //   DINGO_LOG(INFO) << "NEED ADD NEW STORE store_metrics.id = " << store_metrics.id();

    //   // update meta_increment
    //   auto* store_metrics_increment = meta_increment.add_store_metrics();
    //   store_metrics_increment->set_id(store_metrics.id());
    //   store_metrics_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

    //   auto* store_metrics_increment_store = store_metrics_increment->mutable_store_metrics();
    // }
  }

  // mbvar store
  coordinator_bvar_metrics_store_.UpdateStoreBvar(store_metrics.id(), store_metrics.total_capacity(),
                                                  store_metrics.free_capacity());

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
  {
    memory_info.set_store_operation_map_count(store_operation_map_.Size());
    memory_info.set_store_operation_map_size(store_operation_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.store_operation_map_size());
  }
  {
    memory_info.set_executor_user_map_count(executor_user_map_.Size());
    memory_info.set_executor_user_map_size(executor_user_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.executor_user_map_size());
  }
  {
    memory_info.set_task_list_map_count(task_list_map_.Size());
    memory_info.set_task_list_map_size(task_list_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.task_list_map_size());
  }
}

int CoordinatorControl::GetStoreOperation(uint64_t store_id, pb::coordinator::StoreOperation& store_operation) {
  return store_operation_map_.Get(store_id, store_operation);
}

int CoordinatorControl::GetStoreOperations(
    butil::FlatMap<uint64_t, pb::coordinator::StoreOperation>& store_operations) {
  store_operation_map_.GetFlatMapCopy(store_operations);

  return store_operations.size();
}

void CoordinatorControl::GetTaskList(butil::FlatMap<uint64_t, pb::coordinator::TaskList>& task_lists) {
  task_list_map_.GetFlatMapCopy(task_lists);
}

pb::coordinator::TaskList* CoordinatorControl::CreateTaskList(pb::coordinator_internal::MetaIncrement& meta_increment) {
  auto* task_list_increment = meta_increment.add_task_lists();
  task_list_increment->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TASK_LIST, meta_increment));
  task_list_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  auto* increment_task_list = task_list_increment->mutable_task_list();
  increment_task_list->set_id(task_list_increment->id());

  return increment_task_list;
}

void CoordinatorControl::AddCreateTask(pb::coordinator::TaskList* task_list, uint64_t store_id, uint64_t region_id,
                                       const pb::common::RegionDefinition& region_definition,
                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this create region task
  auto* new_task = task_list->add_tasks();
  auto* store_operation_add = new_task->add_store_operations();
  store_operation_add->set_id(store_id);
  auto* region_cmd_to_add = store_operation_add->add_region_cmds();
  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_add->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_CREATE);
  region_cmd_to_add->set_region_id(region_id);
  region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());

  region_cmd_to_add->mutable_create_request()->mutable_region_definition()->CopyFrom(region_definition);
}

void CoordinatorControl::AddDeleteTask(pb::coordinator::TaskList* task_list, uint64_t store_id, uint64_t region_id,
                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this is delete_region task
  auto* delete_region_task = task_list->add_tasks();

  auto* store_operation_delete = delete_region_task->add_store_operations();
  store_operation_delete->set_id(store_id);
  auto* region_cmd_delete = store_operation_delete->add_region_cmds();
  region_cmd_delete->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_delete->set_region_id(region_id);
  region_cmd_delete->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_DELETE);
  region_cmd_delete->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_delete->mutable_delete_request()->set_region_id(region_id);
}

void CoordinatorControl::AddDeleteTaskWithCheck(
    pb::coordinator::TaskList* task_list, uint64_t store_id, uint64_t region_id,
    const ::google::protobuf::RepeatedPtrField< ::dingodb::pb::common::Peer>& peers,
    pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this is delete_region task
  // precheck if region in RegionMap is REGION_NORMAL and REGION_RAFT_HEALTHY
  auto* delete_region_task = task_list->add_tasks();
  auto* region_check = delete_region_task->mutable_pre_check();
  region_check->set_type(::dingodb::pb::coordinator::TaskPreCheckType::REGION_CHECK);
  region_check->mutable_region_check()->set_region_id(region_id);
  region_check->mutable_region_check()->mutable_peers()->CopyFrom(peers);
  region_check->mutable_region_check()->set_state(::dingodb::pb::common::RegionState::REGION_NORMAL);
  region_check->mutable_region_check()->set_raft_status(::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY);

  auto* store_operation_delete = delete_region_task->add_store_operations();
  store_operation_delete->set_id(store_id);
  auto* region_cmd_to_delete = store_operation_delete->add_region_cmds();
  region_cmd_to_delete->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_delete->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_DELETE);
  region_cmd_to_delete->set_region_id(region_id);
  region_cmd_to_delete->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_delete->mutable_delete_request()->set_region_id(region_id);
}

void CoordinatorControl::AddPurgeTask(pb::coordinator::TaskList* task_list, uint64_t store_id, uint64_t region_id,
                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this is purge_region task
  auto* purge_region_task = task_list->add_tasks();

  // precheck if region on store is DELETED
  auto* purge_region_check = purge_region_task->mutable_pre_check();
  purge_region_check->set_type(::dingodb::pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK);
  purge_region_check->mutable_store_region_check()->set_store_id(store_id);
  purge_region_check->mutable_store_region_check()->set_region_id(region_id);
  purge_region_check->mutable_store_region_check()->set_store_region_state(
      ::dingodb::pb::common::StoreRegionState::DELETED);

  auto* store_operation_purge = purge_region_task->add_store_operations();
  store_operation_purge->set_id(store_id);
  auto* region_cmd_to_purge = store_operation_purge->add_region_cmds();
  region_cmd_to_purge->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_purge->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_PURGE);
  region_cmd_to_purge->set_region_id(region_id);
  region_cmd_to_purge->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_purge->mutable_purge_request()->set_region_id(region_id);
}

void CoordinatorControl::AddChangePeerTask(pb::coordinator::TaskList* task_list, uint64_t store_id, uint64_t region_id,
                                           const pb::common::RegionDefinition& region_definition,
                                           pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this is change_peer task
  auto* new_task = task_list->add_tasks();
  auto* store_operation_change = new_task->add_store_operations();
  store_operation_change->set_id(store_id);
  auto* region_cmd_to_change = store_operation_change->add_region_cmds();
  region_cmd_to_change->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_change->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_CHANGE_PEER);
  region_cmd_to_change->set_region_id(region_id);
  region_cmd_to_change->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_change->set_is_notify(true);
  region_cmd_to_change->mutable_change_peer_request()->mutable_region_definition()->CopyFrom(region_definition);
}

void CoordinatorControl::AddTransferLeaderTask(pb::coordinator::TaskList* task_list, uint64_t store_id,
                                               uint64_t region_id, const pb::common::Peer& new_leader_peer,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this is transfer_leader task
  auto* new_task = task_list->add_tasks();
  auto* store_operation_transfer = new_task->add_store_operations();
  store_operation_transfer->set_id(store_id);
  auto* region_cmd_to_transfer = store_operation_transfer->add_region_cmds();
  region_cmd_to_transfer->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_transfer->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_TRANSFER_LEADER);
  region_cmd_to_transfer->set_region_id(region_id);
  region_cmd_to_transfer->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_transfer->set_is_notify(true);  // notify store to do immediately heartbeat

  region_cmd_to_transfer->mutable_transfer_leader_request()->mutable_peer()->CopyFrom(new_leader_peer);
}

void CoordinatorControl::AddMergeTask(pb::coordinator::TaskList* task_list, uint64_t store_id, uint64_t region_id,
                                      uint64_t merge_to_region_id,
                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  // build merege task
  auto* merge_task = task_list->add_tasks();
  auto* store_operation_merge = merge_task->add_store_operations();
  store_operation_merge->set_id(store_id);
  auto* region_cmd_to_add = store_operation_merge->add_region_cmds();

  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_add->set_region_id(region_id);
  region_cmd_to_add->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_MERGE);
  region_cmd_to_add->mutable_merge_request()->set_merge_from_region_id(store_id);
  region_cmd_to_add->mutable_merge_request()->set_merge_to_region_id(merge_to_region_id);
  region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_add->set_is_notify(true);  // notify store to do immediately heartbeat
}

void CoordinatorControl::AddSplitTask(pb::coordinator::TaskList* task_list, uint64_t store_id, uint64_t region_id,
                                      uint64_t split_to_region_id, const std::string& water_shed_key,
                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  // build split_region task
  auto* split_region_task = task_list->add_tasks();
  auto* region_check = split_region_task->mutable_pre_check();
  region_check->set_type(::dingodb::pb::coordinator::TaskPreCheckType::REGION_CHECK);
  region_check->mutable_region_check()->set_region_id(split_to_region_id);
  region_check->mutable_region_check()->set_state(::dingodb::pb::common::RegionState::REGION_STANDBY);

  // generate store operation for stores
  auto* store_operation_split = split_region_task->add_store_operations();
  store_operation_split->set_id(store_id);
  auto* region_cmd_to_add = store_operation_split->add_region_cmds();

  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_add->set_region_id(region_id);
  region_cmd_to_add->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_SPLIT);
  region_cmd_to_add->mutable_split_request()->set_split_watershed_key(water_shed_key);
  region_cmd_to_add->mutable_split_request()->set_split_from_region_id(region_id);
  region_cmd_to_add->mutable_split_request()->set_split_to_region_id(split_to_region_id);
  region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_add->set_is_notify(true);  // notify store to do immediately heartbeat
}

bool CoordinatorControl::DoTaskPreCheck(const pb::coordinator::TaskPreCheck& task_pre_check) {
  if (task_pre_check.type() == pb::coordinator::TaskPreCheckType::REGION_CHECK) {
    pb::common::Region region;
    int ret = region_map_.Get(task_pre_check.region_check().region_id(), region);
    if (ret < 0) {
      DINGO_LOG(INFO) << "region_map_.Get(" << task_pre_check.region_check().region_id() << ") failed";
      return false;
    }

    bool check_passed = true;
    const auto& region_check = task_pre_check.region_check();

    if (region_check.state() != 0 && region_check.state() != region.state()) {
      check_passed = false;
    }

    if (region_check.raft_status() != 0 && region_check.raft_status() != region.raft_status()) {
      check_passed = false;
    }

    if (region_check.replica_status() != 0 && region_check.replica_status() != region.replica_status()) {
      check_passed = false;
    }

    if (region_check.has_range()) {
      if (region_check.range().start_key() != region.definition().range().start_key() ||
          region_check.range().end_key() != region.definition().range().end_key()) {
        check_passed = false;
      }
    }

    if (region_check.peers_size() > 0) {
      std::vector<uint64_t> peers_to_check;
      std::vector<uint64_t> peers_of_region;

      for (const auto& it : region_check.peers()) {
        peers_to_check.push_back(it.store_id());
      }
      for (const auto& it : region.definition().peers()) {
        peers_of_region.push_back(it.store_id());
      }

      std::sort(peers_to_check.begin(), peers_to_check.end());
      std::sort(peers_of_region.begin(), peers_of_region.end());

      if (!std::equal(peers_to_check.begin(), peers_to_check.end(), peers_of_region.begin(), peers_of_region.end())) {
        check_passed = false;
      }
    }

    DINGO_LOG(INFO) << "task pre check passed, check_type=REGION_CHECK";

    return check_passed;
  } else if (task_pre_check.type() == pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK) {
    pb::common::RegionMetrics region;
    {
      BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
      auto* ret = store_metrics_map_.seek(task_pre_check.store_region_check().store_id());
      if (ret == nullptr) {
        DINGO_LOG(INFO) << "store_metrics_map_.seek(" << task_pre_check.store_region_check().store_id() << ") failed";
        return false;
      }

      const auto& region_metrics_map = ret->region_metrics_map();
      if (region_metrics_map.find(task_pre_check.store_region_check().region_id()) == region_metrics_map.end()) {
        DINGO_LOG(INFO) << "region_metrics_map.find(" << task_pre_check.store_region_check().region_id() << ") failed";
        return false;
      }

      region = region_metrics_map.at(task_pre_check.store_region_check().region_id());
    }

    bool check_passed = true;
    const auto& region_check = task_pre_check.store_region_check();

    if (region_check.store_region_state() != 0 && region_check.store_region_state() != region.store_region_state()) {
      check_passed = false;
    }

    if (region_check.raft_node_status() != 0 && region_check.raft_node_status() != region.braft_status().raft_state()) {
      check_passed = false;
    }

    if (region_check.has_range()) {
      if (region_check.range().start_key() != region.region_definition().range().start_key() ||
          region_check.range().end_key() != region.region_definition().range().end_key()) {
        check_passed = false;
      }
    }

    if (region_check.peers_size() > 0) {
      std::vector<uint64_t> peers_to_check;
      std::vector<uint64_t> peers_of_region;

      for (const auto& it : region_check.peers()) {
        peers_to_check.push_back(it.store_id());
      }
      for (const auto& it : region.region_definition().peers()) {
        peers_of_region.push_back(it.store_id());
      }

      std::sort(peers_to_check.begin(), peers_to_check.end());
      std::sort(peers_of_region.begin(), peers_of_region.end());

      if (!std::equal(peers_to_check.begin(), peers_to_check.end(), peers_of_region.begin(), peers_of_region.end())) {
        check_passed = false;
      }
    }

    DINGO_LOG(INFO) << "task pre check passed, check_type=REGION_CHECK";
    return check_passed;
  } else {
    DINGO_LOG(INFO) << "task pre check passed, check_type=NONE";
    return true;
  }
}

butil::Status CoordinatorControl::ProcessSingleTaskList(const pb::coordinator::TaskList& task_list,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (task_list.ByteSizeLong() == 0) {
    DINGO_LOG(ERROR) << "task_to_process.ByteSizeLong() == 0";
    return butil::Status(pb::error::EINTERNAL, "task_to_process.ByteSizeLong() == 0");
  }

  // check step
  if (task_list.next_step() == task_list.tasks_size()) {
    DINGO_LOG(INFO) << "task_list.next_step() == task_list.tasks_size() - 1, will delete this task_list";
    auto* task_list_increment = meta_increment.add_task_lists();
    task_list_increment->set_id(task_list.id());
    task_list_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
    task_list_increment->mutable_task_list()->CopyFrom(task_list);

    return butil::Status::OK();
  }

  // process task
  const auto& task = task_list.tasks(task_list.next_step());

  DINGO_LOG(INFO) << "process task=" << task.DebugString();

  // do pre check
  bool can_advance_task = DoTaskPreCheck(task.pre_check());

  // advance task
  if (!can_advance_task) {
    DINGO_LOG(INFO) << "can not advance task, skip this task_list, task_list=" << task_list.ShortDebugString();
    return butil::Status::OK();
  }

  // do task, send all store_operations
  for (const auto& it : task.store_operations()) {
    auto* store_operation_increment = meta_increment.add_store_operations();
    store_operation_increment->set_id(it.id());
    store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
    store_operation_increment->mutable_store_operation()->CopyFrom(it);
  }

  // advance step, update task_list
  auto* task_list_increment = meta_increment.add_task_lists();
  task_list_increment->set_id(task_list.id());
  task_list_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  task_list_increment->mutable_task_list()->CopyFrom(task_list);
  task_list_increment->mutable_task_list()->set_next_step(task_list.next_step() + 1);

  DINGO_LOG(INFO) << "task_list id=" << task_list.id() << " step=" << task_list.next_step()
                  << " advance+1 total_task_count=" << task_list.tasks_size();

  return butil::Status::OK();
}

// is processing task list
void CoordinatorControl::ReleaseProcessTaskListStatus(const butil::Status&) { is_processing_task_list_.store(false); }

butil::Status CoordinatorControl::ProcessTaskList() {
  if (is_processing_task_list_.load()) {
    DINGO_LOG(INFO) << "is_processing_task_list is true, skip process task list";
    return butil::Status::OK();
  }
  DINGO_LOG(DEBUG) << "start process task lists";

  AtomicGuard atomic_guard(is_processing_task_list_);

  butil::FlatMap<uint64_t, pb::coordinator::TaskList> task_list_map;
  task_list_map.init(100);
  auto ret = task_list_map_.GetFlatMapCopy(task_list_map);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "task_list_map_.GetFlatMapCopy failed";
    return butil::Status(pb::error::EINTERNAL, "task_list_map_.GetFlatMapCopy failed");
  }

  if (task_list_map.empty()) {
    DINGO_LOG(DEBUG) << "task_list_map is empty";
    return butil::Status::OK();
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  for (const auto& it : task_list_map) {
    const auto& task_list = it.second;

    ProcessSingleTaskList(task_list, meta_increment);
  }

  if (meta_increment.ByteSizeLong() == 0) {
    return butil::Status::OK();
  }

  // this callback will destruct by itself, so we don't need to delete it
  auto* done = braft::NewCallback(this, &CoordinatorControl::ReleaseProcessTaskListStatus);

  // prepare for raft process
  atomic_guard.Release();
  SubmitMetaIncrement(done, meta_increment);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CleanTaskList(uint64_t task_list_id,
                                                pb::coordinator_internal::MetaIncrement& meta_increment) {
  butil::FlatMap<uint64_t, pb::coordinator::TaskList> task_list_map;
  task_list_map.init(100);
  auto ret = task_list_map_.GetFlatMapCopy(task_list_map);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "task_list_map_.GetFlatMapCopy failed";
    return butil::Status(pb::error::EINTERNAL, "task_list_map_.GetFlatMapCopy failed");
  }

  if (task_list_map.empty()) {
    DINGO_LOG(INFO) << "task_list_map is empty";
    return butil::Status::OK();
  }

  for (const auto& it : task_list_map) {
    const auto& task_list = it.second;

    if (task_list_id == 0 || task_list.id() == task_list_id) {
      auto* task_list_increment = meta_increment.add_task_lists();
      task_list_increment->set_id(task_list.id());
      task_list_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
      task_list_increment->mutable_task_list()->CopyFrom(task_list);
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb

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
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include "braft/configuration.h"
#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "metrics/coordinator_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "server/server.h"
#include "vector/vector_index_hnsw.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

DECLARE_int32(executor_heartbeat_timeout);
DECLARE_int32(store_heartbeat_timeout);
DECLARE_int32(region_heartbeat_timeout);
DECLARE_int32(region_delete_after_deleted_time);

DECLARE_bool(ip2hostname);

DEFINE_int32(table_delete_after_deleted_time, 86400, "delete table after deleted time in seconds");
DEFINE_int32(index_delete_after_deleted_time, 86400, "delete index after deleted time in seconds");
DEFINE_int64(store_metrics_keep_time_s, 3600, "store metrics keep time in seconds");

DEFINE_int32(
    region_update_timeout, 25,
    "region update timeout in seconds, will not update region info if no state change and (now - last_update_time) > "
    "region_update_timeout");

DEFINE_int32(
    region_down_after_secondes, 60,
    "region down after secondes, will not update region info if no state change and (now - last_update_time) > "
    "region_down_after_secondes");

DECLARE_int64(max_hnsw_memory_size_of_region);

DECLARE_int32(max_hnsw_nlinks_of_region);

DEFINE_int32(max_send_region_cmd_per_store, 100, "max send region cmd per store");

DEFINE_int64(max_region_count, 40000, "max region of dingo");

// TODO: add epoch logic
void CoordinatorControl::GetCoordinatorMap(int64_t cluster_id, int64_t& epoch, pb::common::Location& leader_location,
                                           std::vector<pb::common::Location>& locations,
                                           pb::common::CoordinatorMap& coordinator_map) {
  if (cluster_id < 0) {
    return;
  }
  epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_COORINATOR);

  if (raft_node_ == nullptr) {
    DINGO_LOG(ERROR) << "GetCoordinatorMap raft_node_ is nullptr";
    return;
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

  // get all peers
  std::vector<braft::PeerId> peers;
  raft_node_->ListPeers(&peers);
  auto braft_status = raft_node_->GetStatus();
  const auto& peer_status_map = braft_status->stable_followers();

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

    auto* new_coordinator = coordinator_map.add_coordinators();
    *new_coordinator->mutable_location() = server_location;

    if (peer == leader_peer_id) {
      new_coordinator->set_state(pb::common::CoordinatorState::COORDINATOR_NORMAL);
    } else if (peer_status_map.find(peer.to_string()) != peer_status_map.end()) {
      const auto& peer_status = peer_status_map.at(peer.to_string());
      if (peer_status.consecutive_error_times() > 0) {
        new_coordinator->set_state(pb::common::CoordinatorState::COORDINATOR_OFFLINE);
      } else {
        new_coordinator->set_state(pb::common::CoordinatorState::COORDINATOR_NORMAL);
      }
    } else {
      DINGO_LOG(ERROR) << "GetCoordinatorMap cannot find peer_status, peerid=" << peer;
      new_coordinator->set_state(pb::common::CoordinatorState::COORDINATOR_ERROR);
    }
  }
}

std::mt19937 CoordinatorControl::GetUrbg() {
  std::random_device rd;
  std::mt19937 g(rd());

  return g;
}

void CoordinatorControl::GetStoreMap(pb::common::StoreMap& store_map) {
  int64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);
  store_map.set_epoch(store_map_epoch);

  {
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    butil::FlatMap<int64_t, pb::common::Store> store_map_copy;
    store_map_copy.init(100);
    store_map_.GetRawMapCopy(store_map_copy);

    for (auto& element : store_map_copy) {
      auto* tmp_region = store_map.add_stores();
      *tmp_region = element.second;
    }
  }
}

void CoordinatorControl::GetStoreRegionMetrics(int64_t store_id, std::vector<pb::common::StoreMetrics>& store_metrics) {
  std::vector<int64_t> store_ids_to_get_full;
  std::set<int64_t> store_ids_to_get_own;
  std::vector<int64_t> all_store_ids;
  if (store_id == 0) {
    store_map_.GetAllKeys(all_store_ids);
  } else {
    all_store_ids.push_back(store_id);
  }

  {
    BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
    for (const auto& id : all_store_ids) {
      if (store_region_metrics_map_.find(id) != store_region_metrics_map_.end()) {
        const auto& store_metric = store_region_metrics_map_.at(id);
        if (store_metric.region_metrics_map_size() > 0) {
          store_metrics.push_back(store_metric);
        }

        if ((!store_metric.has_store_own_metrics()) || store_metric.store_own_metrics().id() == 0) {
          store_ids_to_get_own.insert(id);
        }

      } else {
        store_ids_to_get_full.push_back(id);
      }
    }
  }

  // for store that has no own metrics, so we get store_own_metric from store_metrics_map
  if (!store_ids_to_get_full.empty()) {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    for (const auto& id : store_ids_to_get_full) {
      if (store_metrics_map_.find(id) != store_metrics_map_.end()) {
        pb::common::StoreMetrics store_metric;
        store_metric.set_id(id);
        (*store_metric.mutable_store_own_metrics()) = store_metrics_map_.at(id).store_own_metrics;
        store_metrics.push_back(store_metric);

        DINGO_LOG(INFO) << "GetStoreRegionMetrics... store_ids_to_get_full OK store_id=" << id
                        << " store_own_metrics=" << store_metric.store_own_metrics().ShortDebugString();
      }
    }
  }

  // for store that only has partial heartbeat, so we get store_own_metric from store_map_
  if (!store_ids_to_get_own.empty()) {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    for (auto& metrics_to_update : store_metrics) {
      if (store_ids_to_get_own.count(metrics_to_update.id()) > 0) {
        if (store_metrics_map_.find(metrics_to_update.id()) != store_metrics_map_.end()) {
          (*metrics_to_update.mutable_store_own_metrics()) =
              store_metrics_map_.at(metrics_to_update.id()).store_own_metrics;

          DINGO_LOG(INFO) << "GetStoreRegionMetrics... store_ids_to_get_own OK store_id=" << metrics_to_update.id()
                          << " store_own_metrics=" << metrics_to_update.store_own_metrics().ShortDebugString();
        } else {
          DINGO_LOG(WARNING) << "GetStoreRegionMetrics... store_ids_to_get_own store_id=" << metrics_to_update.id()
                             << " not exist in store_metrics_map_";
        }
      }
    }
  }
}

void CoordinatorControl::GetStoreRegionMetrics(int64_t store_id, int64_t region_id,
                                               std::vector<pb::common::StoreMetrics>& store_metrics) {
  if (region_id == 0) {
    GetStoreRegionMetrics(store_id, store_metrics);
    return;
  } else {
    BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
    if (store_id == 0) {
      for (auto& [id, store_metric] : store_region_metrics_map_) {
        pb::common::StoreMetrics tmp_store_metrics;
        tmp_store_metrics.set_id(id);

        const auto& region_metrics_map = store_metric.region_metrics_map();
        if (region_metrics_map.find(region_id) != region_metrics_map.end()) {
          tmp_store_metrics.mutable_region_metrics_map()->insert({region_id, region_metrics_map.at(region_id)});
        }

        store_metrics.push_back(tmp_store_metrics);
      }
    } else {
      if (store_region_metrics_map_.find(store_id) != store_region_metrics_map_.end()) {
        pb::common::StoreMetrics tmp_store_metrics;
        tmp_store_metrics.set_id(store_id);

        const auto& region_metrics_map = store_region_metrics_map_.at(store_id).region_metrics_map();
        if (region_metrics_map.find(region_id) != region_metrics_map.end()) {
          tmp_store_metrics.mutable_region_metrics_map()->insert({region_id, region_metrics_map.at(region_id)});
        }

        store_metrics.push_back(tmp_store_metrics);
      } else {
        DINGO_LOG(ERROR) << "GetStoreMetrics store_id=" << store_id << " not exist";
      }
    }
  }
}

void CoordinatorControl::DeleteStoreRegionMetrics(int64_t store_id) {
  {
    BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
    if (store_id == 0) {
      for (const auto& it : store_region_metrics_map_) {
        coordinator_bvar_metrics_store_.DeleteStoreBvar(it.first);
      }
      store_region_metrics_map_.clear();
    } else {
      store_region_metrics_map_.erase(store_id);
      coordinator_bvar_metrics_store_.DeleteStoreBvar(store_id);
    }
  }

  {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    if (store_id == 0) {
      store_metrics_map_.clear();
    } else {
      store_metrics_map_.erase(store_id);
    }
  }
}

void CoordinatorControl::GetRegionMetrics(int64_t region_id,
                                          std::vector<pb::common::RegionMetrics>& region_metrics_array) {
  if (region_id == 0) {
    butil::FlatMap<int64_t, pb::common::RegionMetrics> region_metrics_map_copy;
    auto ret = region_metrics_map_.GetRawMapCopy(region_metrics_map_copy);
    if (ret < 0) {
      DINGO_LOG(INFO) << "GetRegionMetrics region_metrics_map_.GetRawMapCopy failed";
      return;
    }

    for (const auto& region_metrics : region_metrics_map_copy) {
      region_metrics_array.push_back(region_metrics.second);
    }
  } else {
    pb::common::RegionMetrics region_metrics;
    auto ret = region_metrics_map_.Get(region_id, region_metrics);
    if (ret < 0) {
      DINGO_LOG(INFO) << "GetRegionMetrics region_metrics_map_.Get failed, region_id=" << region_id;
      return;
    }

    region_metrics_array.push_back(region_metrics);
  }
}

void CoordinatorControl::DeleteRegionMetrics(int64_t region_id) {
  if (region_id == 0) {
    return;
  }

  region_metrics_map_.Erase(region_id);
}

butil::Status CoordinatorControl::GetOrphanRegion(int64_t store_id,
                                                  std::map<int64_t, pb::common::RegionMetrics>& orphan_regions) {
  BAIDU_SCOPED_LOCK(this->store_region_metrics_map_mutex_);

  for (const auto& it : store_region_metrics_map_) {
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

int CoordinatorControl::ValidateStore(int64_t store_id, const std::string& keyring) {
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

butil::Status CoordinatorControl::CreateStore(int64_t cluster_id, int64_t& store_id, std::string& keyring,
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
  *store_increment_store = store;

  // on_apply
  // store_map_epoch++;                                  // raft_kv_put
  // store_map_.insert(std::make_pair(store_id, store));  // raft_kv_put
  return butil::Status::OK();
}

butil::Status CoordinatorControl::DeleteStore(int64_t cluster_id, int64_t store_id, std::string keyring,
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
  *store_increment_store = store_to_delete;

  // on_apply
  // store_map_epoch++;                                  // raft_kv_put
  // store_map_.insert(std::make_pair(store_id, store));  // raft_kv_put
  return butil::Status::OK();
}

butil::Status CoordinatorControl::UpdateStore(int64_t cluster_id, int64_t store_id, std::string keyring,
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
  *store_increment_store = store_to_update;

  // on_apply
  // store_map_epoch++;                                  // raft_kv_put
  // store_map_.insert(std::make_pair(store_id, store));  // raft_kv_put
  return butil::Status::OK();
}

// UpdateStoreMap
int64_t CoordinatorControl::UpdateStoreMap(const pb::common::Store& store,
                                           pb::coordinator_internal::MetaIncrement& meta_increment) {
  int64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);

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
        *store_increment_store = store_to_update;  // only update server_location & raft_location & state

        // only update server_location & raft_location & state & last_seen_timestamp
        *(store_increment_store->mutable_server_location()) = store.server_location();
        *(store_increment_store->mutable_raft_location()) = store.raft_location();
        store_increment_store->set_state(pb::common::StoreState::STORE_NORMAL);
        store_increment_store->set_last_seen_timestamp(butil::gettimeofday_ms());
      } else if (store_to_update.server_location().host() != store.server_location().host() ||
                 store_to_update.server_location().port() != store.server_location().port() ||
                 store_to_update.raft_location().host() != store.raft_location().host() ||
                 store_to_update.raft_location().port() != store.raft_location().port() ||
                 store_to_update.resource_tag() != store.resource_tag() ||
                 store_to_update.keyring() != store.keyring()) {
        // this is normal heartbeat, with state change or location change
        // so only need to update state & last_seen_timestamp, no need to update epoch
        auto* store_increment = meta_increment.add_stores();
        store_increment->set_id(store.id());
        store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        auto* store_increment_store = store_increment->mutable_store();
        *store_increment_store = store_to_update;  // only update server_location & raft_location & state

        // only update state & last_seen_timestamp
        store_increment_store->set_state(pb::common::StoreState::STORE_NORMAL);
        store_increment_store->set_last_seen_timestamp(butil::gettimeofday_ms());
      } else {
        // this is normal heartbeat, with no state change and no location change
        store_to_update.set_last_seen_timestamp(butil::gettimeofday_ms());
        store_to_update.set_state(pb::common::StoreState::STORE_NORMAL);
        auto ret = store_map_.Put(store_to_update.id(), store_to_update);
        if (ret < 0) {
          DINGO_LOG(ERROR) << "UpdateStoreMap store_map_.Put failed, store_id=" << store_to_update.id();
        }
        DINGO_LOG(INFO) << "NORMAL HEARTBEAT store_id = " << store_to_update.id()
                        << " status = " << pb::common::StoreState_Name(store_to_update.state())
                        << " time = " << store_to_update.last_seen_timestamp();
        return store_map_epoch;
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
      *store_increment_store = store;
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

bool CoordinatorControl::TrySetRegionToDown(int64_t region_id) {
  pb::common::RegionMetrics region_to_update;
  int ret = region_metrics_map_.Get(region_id, region_to_update);
  if (ret > 0) {
    if (region_to_update.region_status().last_update_timestamp() + (FLAGS_region_down_after_secondes * 1000) <
        butil::gettimeofday_ms()) {
      // update region's heartbeat state to REGION_DOWN
      region_to_update.mutable_region_status()->set_heartbeat_status(pb::common::RegionHeartbeatState::REGION_DOWN);
      region_metrics_map_.PutIfExists(region_id, region_to_update);
      return true;
    }
  }
  return false;
}

bool CoordinatorControl::TrySetRegionToOnline(int64_t region_id) {
  pb::common::RegionMetrics region_to_update;
  int ret = region_metrics_map_.Get(region_id, region_to_update);
  if (ret > 0) {
    if (region_to_update.region_status().heartbeat_status() != pb::common::RegionHeartbeatState::REGION_ONLINE &&
        region_to_update.region_status().last_update_timestamp() + (FLAGS_region_down_after_secondes * 1000) >=
            butil::gettimeofday_ms()) {
      // update region's heartbeat state to REGION_ONLINE
      region_to_update.mutable_region_status()->set_heartbeat_status(pb::common::RegionHeartbeatState::REGION_ONLINE);
      region_metrics_map_.PutIfExists(region_id, region_to_update);
      return true;
    }
  }
  return false;
}

bool CoordinatorControl::TrySetStoreToOffline(int64_t store_id) {
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

void CoordinatorControl::GenRegionFull(const pb::coordinator_internal::RegionInternal& region_internal,
                                       pb::common::Region& region) {
  region.set_id(region_internal.id());
  *(region.mutable_definition()) = region_internal.definition();
  region.set_state(region_internal.state());
  region.set_create_timestamp(region_internal.create_timestamp());
  region.set_region_type(region_internal.region_type());

  pb::common::RegionMetrics region_metrics;
  auto ret = region_metrics_map_.Get(region_internal.id(), region_metrics);
  if (ret < 0) {
    DINGO_LOG(INFO) << "GenRegionFull... Get region_metrics failed, region_id=" << region_internal.id();
    return;
  }

  DINGO_LOG(DEBUG) << "GenRegionFull... Get region_metrics succ, region_id=" << region_internal.id()
                   << " region_metrics=" << region_metrics.DebugString();

  region.set_leader_store_id(region_metrics.leader_store_id());
  *(region.mutable_status()) = region_metrics.region_status();
  *(region.mutable_metrics()) = region_metrics;
}

void CoordinatorControl::GenRegionSlim(const pb::coordinator_internal::RegionInternal& region_internal,
                                       pb::common::Region& region) {
  region.set_id(region_internal.id());
  region.mutable_definition()->set_name(region_internal.definition().name());
  region.mutable_definition()->mutable_epoch()->set_conf_version(region_internal.definition().epoch().conf_version());
  region.mutable_definition()->mutable_epoch()->set_version(region_internal.definition().epoch().version());
  region.mutable_definition()->mutable_range()->set_start_key(region_internal.definition().range().start_key());
  region.mutable_definition()->mutable_range()->set_start_key(region_internal.definition().range().start_key());
  region.mutable_definition()->mutable_range()->set_end_key(region_internal.definition().range().end_key());
  *region.mutable_definition()->mutable_index_parameter() = region_internal.definition().index_parameter();
  region.set_state(region_internal.state());
  region.set_create_timestamp(region_internal.create_timestamp());
  region.set_region_type(region_internal.region_type());

  pb::common::RegionMetrics region_metrics;
  auto ret = region_metrics_map_.Get(region_internal.id(), region_metrics);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "GenRegionSlim... Get region_metrics failed, region_id=" << region_internal.id();
    return;
  }

  region.set_leader_store_id(region_metrics.leader_store_id());
  *(region.mutable_status()) = region_metrics.region_status();

  if (region_internal.definition().index_parameter().has_vector_index_parameter()) {
    *(region.mutable_metrics()->mutable_vector_index_status()) = region_metrics.vector_index_status();
  }
}

void CoordinatorControl::UpdateClusterReadOnly() {
  BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
  bool cluster_is_read_only = false;
  for (const auto& store_metrics : store_region_metrics_map_) {
    if (store_metrics.second.store_own_metrics().is_ready_only()) {
      pb::common::Store store;
      auto ret = store_map_.Get(store_metrics.first, store);
      if (ret < 0) {
        DINGO_LOG(WARNING) << "UpdateClusterReadOnly... Get store failed, store_id=" << store_metrics.first;
        continue;
      }

      if (store.state() != pb::common::StoreState::STORE_NORMAL) {
        DINGO_LOG(WARNING) << "UpdateClusterReadOnly... store_id=" << store_metrics.first
                           << " is_read_only=" << store_metrics.second.store_own_metrics().is_ready_only()
                           << " but store.state()=" << store.state();
        continue;
      }

      DINGO_LOG(WARNING) << "UpdateClusterReadOnly... store_id=" << store_metrics.first
                         << " is_read_only=" << store_metrics.second.store_own_metrics().is_ready_only();
      cluster_is_read_only = true;
      break;
    }
  }

  if (Server::GetInstance().IsReadOnly() != cluster_is_read_only) {
    DINGO_LOG(WARNING) << "UpdateClusterReadOnly... cluster_is_read_only=" << cluster_is_read_only;
    Server::GetInstance().SetReadOnly(cluster_is_read_only);
  }
}

void CoordinatorControl::UpdateRegionState() {
  // update region_state by last_update_timestamp
  pb::coordinator_internal::MetaIncrement meta_increment;

  butil::FlatMap<int64_t, pb::coordinator_internal::RegionInternal> region_map_temp;
  auto ret = region_map_.GetRawMapCopy(region_map_temp);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "UpdateRegionState... GetRawMapCopy failed";
    return;
  }

  for (const auto& it : region_map_temp) {
    pb::common::RegionMetrics region_metrics;
    auto ret = region_metrics_map_.Get(it.first, region_metrics);
    if (ret < 0) {
      if (it.second.state() != pb::common::RegionState::REGION_NEW) {
        DINGO_LOG(WARNING) << "UpdateRegionState... Get region_metrics failed, region_id=" << it.first;
      }
      continue;
    }

    DINGO_LOG(DEBUG) << "CoordinatorUpdateState... region " << it.first << " state "
                     << pb::common::RegionState_Name(it.second.state()) << " last_update_timestamp "
                     << region_metrics.region_status().last_update_timestamp() << " now " << butil::gettimeofday_ms();

    if (region_metrics.region_status().last_update_timestamp() + (FLAGS_region_heartbeat_timeout * 1000) >=
        butil::gettimeofday_ms()) {
      if (region_metrics.region_status().heartbeat_status() != pb::common::RegionHeartbeatState::REGION_ONLINE) {
        DINGO_LOG(INFO) << "CoordinatorUpdateState... update region " << it.first << " state to online";
        TrySetRegionToOnline(it.first);
      }
      continue;
    }

    if (it.second.state() != pb::common::RegionState::REGION_NEW &&
        it.second.state() != pb::common::RegionState::REGION_DELETE &&
        it.second.state() != pb::common::RegionState::REGION_DELETING &&
        it.second.state() != pb::common::RegionState::REGION_DELETED) {
      DINGO_LOG(INFO) << "CoordinatorUpdateState... update region " << it.first << " state to offline";
      TrySetRegionToDown(it.first);
    } else if (it.second.state() == pb::common::RegionState::REGION_DELETED &&
               region_metrics.region_status().last_update_timestamp() +
                       (FLAGS_region_delete_after_deleted_time * 1000) <
                   butil::gettimeofday_ms()) {
      DINGO_LOG(INFO) << "CoordinatorUpdateState... start to purge region " << it.first;

      // construct store_operation to purge region
      // for (const auto& it_peer : it.definition().peers()) {
      //   auto* purge_region_operation_increment = meta_increment.add_store_operations();
      //   purge_region_operation_increment->set_id(it_peer.store_id());  // this is store_id
      //   purge_region_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

      //   auto* purge_region_operation = purge_region_operation_increment->mutable_store_operation();
      //   purge_region_operation->set_id(it_peer.store_id());  // this is store_id
      //   auto* purge_region_cmd = purge_region_operation->add_region_cmds();
      //   purge_region_cmd->set_id(
      //       coordinator_control->GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD,
      //       meta_increment));
      //   purge_region_cmd->set_region_id(it.id());  // this is region_id
      //   DINGO_LOG(INFO) << " purge set_region_id " << it.id();
      //   purge_region_cmd->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_PURGE);
      //   purge_region_cmd->set_create_timestamp(butil::gettimeofday_ms());
      //   purge_region_cmd->mutable_purge_request()->set_region_id(it.id());  // this region_id
      //   DINGO_LOG(INFO) << " purge_region_cmd set_region_id " << it.id();

      //   DINGO_LOG(INFO) << "CoordinatorUpdateState... purge region " << it.id() << " from store " <<
      //   it_peer.store_id()
      //                   << " region_cmd_type " << purge_region_cmd->region_cmd_type();
      // }

      // delete regions
      // auto* region_delete_increment = meta_increment.add_regions();
      // region_delete_increment->set_id(it.id());
      // region_delete_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
      // *(region_delete_increment->mutable_region()) = it;

      // mbvar
      // DeleteRegionBvar(it.first);

      // DINGO_LOG(INFO) << "CoordinatorUpdateState... purge region delete region_map " << it.first << " from store "
      //                 << it.first << " region_cmd_type " << region_delete_increment->region().DebugString()
      //                 << " request=" << meta_increment.DebugString();
    }
  }
}

int64_t CoordinatorControl::GetRegionLeaderId(int64_t region_id) {
  pb::common::RegionMetrics region_metrics;
  auto ret = region_metrics_map_.Get(region_id, region_metrics);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetRegionLeaderId failed, region_id: " << region_id << " not exists in region_metrics_map_";
    return 0;
  }

  return region_metrics.leader_store_id();
}

pb::common::RegionStatus CoordinatorControl::GetRegionStatus(int64_t region_id) {
  pb::common::RegionStatus region_status;
  pb::common::RegionMetrics region_metrics;

  auto ret = region_metrics_map_.Get(region_id, region_metrics);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "GetRegionStatus failed, region_id: " << region_id << " not exists in region_metrics_map_";
    return region_status;
  }

  return region_metrics.region_status();
}

pb::common::RegionMetrics CoordinatorControl::GetRegionMetrics(int64_t region_id) {
  pb::common::RegionMetrics region_metrics;

  auto ret = region_metrics_map_.Get(region_id, region_metrics);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "GetRegionMetrics failed, region_id: " << region_id << " not exists in region_metrics_map_";
    return region_metrics;
  }

  return region_metrics;
}

void CoordinatorControl::GetRegionLeaderAndStatus(int64_t region_id, pb::common::RegionStatus& region_status,
                                                  int64_t& leader_store_id) {
  pb::common::RegionMetrics region_metrics;

  auto ret = region_metrics_map_.Get(region_id, region_metrics);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetRegionLeaderId failed, region_id: " << region_id << " not exists in region_metrics_map_";
    return;
  }

  leader_store_id = region_metrics.leader_store_id();
  region_status = region_metrics.region_status();
}

void CoordinatorControl::GetRegionMap(pb::common::RegionMap& region_map) {
  region_map.set_epoch(GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION));
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    butil::FlatMap<int64_t, pb::coordinator_internal::RegionInternal> region_internal_map_copy;
    region_internal_map_copy.init(30000);
    region_map_.GetRawMapCopy(region_internal_map_copy);
    butil::FlatMap<int64_t, pb::common::RegionMetrics> region_metrics_map_copy;
    region_metrics_map_copy.init(30000);
    region_metrics_map_.GetRawMapCopy(region_metrics_map_copy);

    for (auto& element : region_internal_map_copy) {
      auto* tmp_region = region_map.add_regions();
      GenRegionSlim(element.second, *tmp_region);
    }
  }
}

void CoordinatorControl::GetRegionMapFull(pb::common::RegionMap& region_map) {
  region_map.set_epoch(GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION));
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    butil::FlatMap<int64_t, pb::coordinator_internal::RegionInternal> region_internal_map_copy;
    region_internal_map_copy.init(30000);
    region_map_.GetRawMapCopy(region_internal_map_copy);

    for (auto& element : region_internal_map_copy) {
      auto* tmp_region = region_map.add_regions();
      GenRegionFull(element.second, *tmp_region);
    }
  }
}

void CoordinatorControl::GetDeletedRegionMap(pb::common::RegionMap& region_map) {
  std::vector<pb::coordinator_internal::RegionInternal> region_internal_map_copy;
  auto ret = deleted_region_meta_->GetAllElements(region_internal_map_copy);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "GetDeletedRegionMap failed, ret: " << ret;
    return;
  }

  for (auto& element : region_internal_map_copy) {
    auto* tmp_region = region_map.add_regions();
    GenRegionFull(element, *tmp_region);
  }
}

butil::Status CoordinatorControl::AddDeletedRegionMap(int64_t region_id, bool force) {
  auto ret = region_map_.Exists(region_id);
  if (ret) {
    DINGO_LOG(WARNING) << "cannnot add deleted_region region_id: " << region_id << " already exists in region_map_";
    if (!force) {
      return butil::Status(pb::error::Errno::EINTERNAL, "region_id already exists in region_map_");
    }
  }

  pb::coordinator_internal::MetaIncrement meta_increment;
  // add the deleted region to deleted_region_meta_
  auto* deleted_region_increment = meta_increment.add_deleted_regions();
  deleted_region_increment->set_id(region_id);
  deleted_region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

  auto* deleted_region_increment_region = deleted_region_increment->mutable_region();
  deleted_region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_DELETE);
  deleted_region_increment_region->set_id(region_id);
  deleted_region_increment_region->mutable_definition()->set_name("MANUAL_ADD");
  deleted_region_increment_region->set_deleted_timestamp(butil::gettimeofday_ms());

  SubmitMetaIncrementSync(meta_increment);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CleanDeletedRegionMap(int64_t region_id) {
  uint32_t i = 0;
  pb::coordinator_internal::MetaIncrement meta_increment;

  if (region_id == 0) {
    std::vector<int64_t> deleted_region_ids;
    auto ret = deleted_region_meta_->GetAllIds(deleted_region_ids);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "CleanDeletedRegionMap failed, region_id: " << region_id << " GetAllIds failed, ret: " << ret;
      return ret;
    }

    for (const auto& id : deleted_region_ids) {
      auto* deleted_region_increment = meta_increment.add_deleted_regions();
      deleted_region_increment->set_id(id);
      deleted_region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

      auto* deleted_region_increment_region = deleted_region_increment->mutable_region();
      deleted_region_increment_region->set_id(id);

      if (i++ > 1000) {
        auto ret1 = SubmitMetaIncrementSync(meta_increment);
        if (!ret1.ok()) {
          DINGO_LOG(ERROR) << "CleanDeletedRegionMap failed, region_id: " << region_id
                           << " SubmitMetaIncrementSync failed, ret: " << ret1;
          return ret1;
        }
        i = 0;
        meta_increment.Clear();
      }
    }

  } else {
    auto ret = deleted_region_meta_->Exists(region_id);
    if (!ret) {
      DINGO_LOG(WARNING) << "CleanDeletedRegionMap failed, region_id: " << region_id
                         << " not exists in deleted_region_meta_";
      return butil::Status(pb::error::Errno::EINTERNAL, "region_id not exists in deleted_region_meta_");
    }

    auto* deleted_region_increment = meta_increment.add_deleted_regions();
    deleted_region_increment->set_id(region_id);
    deleted_region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

    auto* deleted_region_increment_region = deleted_region_increment->mutable_region();
    deleted_region_increment_region->set_id(region_id);
  }

  if (meta_increment.ByteSizeLong() > 0) {
    auto ret1 = SubmitMetaIncrementSync(meta_increment);
    if (!ret1.ok()) {
      DINGO_LOG(ERROR) << "CleanDeletedRegionMap failed, region_id: " << region_id
                       << " SubmitMetaIncrementSync failed, ret: " << ret1;
      return ret1;
    }
  }

  return butil::Status::OK();
}

void CoordinatorControl::GetRegionCount(int64_t& region_count) { region_count = region_map_.Size(); }

void CoordinatorControl::GetRegionIdsInMap(std::vector<int64_t>& region_ids) { region_map_.GetAllKeys(region_ids); }

void CoordinatorControl::RecycleOutdatedStoreMetrics() {
  std::vector<int64_t> store_ids_to_erase_metrics;
  {
    // check store_metrics_map_, if update_time is too old (more than 1 hour), delete it
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    for (const auto& [id, slim] : store_metrics_map_) {
      if (slim.update_time + (FLAGS_store_metrics_keep_time_s * 1000) < butil::gettimeofday_ms()) {
        DINGO_LOG(INFO) << "RecycleOutdatedStoreMetrics delete outdated store_metrics, store_id: " << id
                        << " update_time: " << slim.update_time
                        << " store_metrics_keep_time(s): " << FLAGS_store_metrics_keep_time_s;
        store_ids_to_erase_metrics.push_back(id);
      }
    }

    for (const auto& id : store_ids_to_erase_metrics) {
      store_metrics_map_.erase(id);
    }
  }

  {
    BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
    for (const auto& id : store_ids_to_erase_metrics) {
      store_region_metrics_map_.erase(id);
    }
  }
}

void CoordinatorControl::RecycleDeletedTableAndIndex() {
  if (IsLeader()) {
    DINGO_LOG(INFO) << "Start to RecycleOrphanRegionOnStore, timestamp=" << butil::gettimeofday_ms();
  } else {
    DINGO_LOG(INFO) << "RecycleDeletedTableAndIndex skip because not leader";
    return;
  }

  // butil::FlatMap<int64_t, pb::coordinator_internal::TableInternal> delete_tables;
  // butil::FlatMap<int64_t, pb::coordinator_internal::TableInternal> delete_indexes;

  // delete_tables.init(3000);
  // delete_indexes.init(3000);

  std::vector<pb::coordinator_internal::TableInternal> delete_tables;
  std::vector<pb::coordinator_internal::TableInternal> delete_indexes;
  deleted_table_meta_->GetAllElements(delete_tables);
  deleted_index_meta_->GetAllElements(delete_indexes);

  pb::coordinator_internal::MetaIncrement meta_increment;

  for (const auto& table : delete_tables) {
    if (table.definition().delete_timestamp() + (FLAGS_table_delete_after_deleted_time * 1000) <
        butil::gettimeofday_ms()) {
      DINGO_LOG(INFO) << "RecycleDeletedTableAndIndex delete obsolete deleted_table table_id:" << table.id()
                      << " deleted_timestamp: " << table.definition().delete_timestamp()
                      << " table_delete_after_deleted_time: " << FLAGS_table_delete_after_deleted_time;

      auto* deleted_table_increment = meta_increment.add_deleted_tables();
      deleted_table_increment->set_id(table.id());
      deleted_table_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
      auto* deleted_table = deleted_table_increment->mutable_table();
      deleted_table->set_id(table.id());
    }
  }

  for (const auto& index : delete_indexes) {
    if (index.definition().delete_timestamp() + (FLAGS_index_delete_after_deleted_time * 1000) <
        butil::gettimeofday_ms()) {
      DINGO_LOG(INFO) << "RecycleDeletedTableAndIndex delete obsolete deleted_index index_id:" << index.id()
                      << " deleted_timestamp: " << index.definition().delete_timestamp()
                      << " index_delete_after_deleted_time: " << FLAGS_index_delete_after_deleted_time;

      auto* deleted_index_increment = meta_increment.add_deleted_indexes();
      deleted_index_increment->set_id(index.id());
      deleted_index_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
      auto* deleted_index = deleted_index_increment->mutable_table();
      deleted_index->set_id(index.id());
    }
  }

  if (meta_increment.ByteSizeLong() > 0) {
    SubmitMetaIncrementSync(meta_increment);
  }
}

void CoordinatorControl::RecycleOrphanRegionOnStore() {
  if (IsLeader()) {
    DINGO_LOG(INFO) << "Start to RecycleOrphanRegionOnStore, timestamp=" << butil::gettimeofday_ms();
  } else {
    DINGO_LOG(INFO) << "RecycleOrphanRegionOnStore skip because not leader";
    return;
  }

  std::map<int64_t, pb::coordinator_internal::RegionInternal> delete_regions;
  auto ret = deleted_region_meta_->GetAllIdElements(delete_regions);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "RecycleOrphanRegionOnStore failed, ret: " << ret;
    return;
  }

  if (delete_regions.empty()) {
    DINGO_LOG(DEBUG) << "No region to recycle";
    return;
  }

  std::map<int64_t, std::vector<int64_t>> region_id_on_store;

  // load all region_id to region_id_on_store
  {
    BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
    for (auto& store_metric : store_region_metrics_map_) {
      const auto& region_metrics_map = store_metric.second.region_metrics_map();
      for (const auto& region_metric : region_metrics_map) {
        region_id_on_store[store_metric.first].push_back(region_metric.first);
      }
    }
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // delete orphan region on store
  for (const auto& ids : region_id_on_store) {
    DINGO_LOG(INFO) << "Start to RecycleOrphanRegionOnStore for store_id: " << ids.first
                    << " region_id size: " << ids.second.size();

    pb::coordinator_internal::StoreOperationInternal store_operation;
    bool is_store_operation_get = false;

    for (const auto& region_id : ids.second) {
      // if region_id is in delete_region_map, need to delete region on this store
      if (delete_regions.find(region_id) != delete_regions.end()) {
        if (!is_store_operation_get) {
          store_operation_map_.Get(ids.first, store_operation);
          is_store_operation_get = true;
        }

        bool region_need_recycle = true;
        for (auto region_cmd_id : store_operation.region_cmd_ids()) {
          pb::coordinator_internal::RegionCmdInternal region_cmd;
          auto ret = region_cmd_map_.Get(region_cmd_id, region_cmd);
          if (ret < 0) {
            continue;
          }

          if (region_cmd.region_cmd().region_id() == region_id &&
              region_cmd.region_cmd().region_cmd_type() == pb::coordinator::RegionCmdType::CMD_DELETE) {
            region_need_recycle = false;
            break;
          }
        }

        if (!region_need_recycle) {
          DINGO_LOG(INFO) << "RecycleOrphanRegionOnStore delete skip because is_deleting store_id:" << ids.first
                          << " region_id: " << region_id;
          continue;
        }

        DINGO_LOG(INFO) << "RecycleOrphanRegionOnStore delete store_id:" << ids.first << " region_id: " << region_id;

        auto* increment_task_list = CreateTaskList(meta_increment);

        // this is delete_region task
        AddDeleteTask(increment_task_list, ids.first, region_id, nullptr, meta_increment);

        // this is purge_region task
        // AddPurgeTask(increment_task_list, ids.first, region_id, meta_increment);
      }
    }
  }

  // delete too old delete_region
  for (const auto& region : delete_regions) {
    DINGO_LOG(DEBUG) << "RecycleOrphanRegionOnStore meet obsolete deleted_region region_id:" << region.first
                     << " deleted_timestamp: " << region.second.deleted_timestamp()
                     << " region_delete_after_deleted_time: " << FLAGS_region_delete_after_deleted_time;

    if (region.second.deleted_timestamp() + (FLAGS_region_delete_after_deleted_time * 1000) <
        butil::gettimeofday_ms()) {
      DINGO_LOG(INFO) << "RecycleOrphanRegionOnStore delete obsolete deleted_region region_id:" << region.first
                      << " deleted_timestamp: " << region.second.deleted_timestamp()
                      << " region_delete_after_deleted_time: " << FLAGS_region_delete_after_deleted_time;

      auto* deleted_region_increment = meta_increment.add_deleted_regions();
      deleted_region_increment->set_id(region.second.id());
      deleted_region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
      auto* deleted_region = deleted_region_increment->mutable_region();
      deleted_region->set_id(region.second.id());
    }

    // delete region_metrics
    region_metrics_map_.Erase(region.first);
  }

  if (meta_increment.ByteSizeLong() > 0) {
    SubmitMetaIncrementSync(meta_increment);
  }
}

void CoordinatorControl::RecycleOrphanRegionOnCoordinator() {
  if (IsLeader()) {
    DINGO_LOG(INFO) << "Start to RecycleOrphanRegionOnStore, timestamp=" << butil::gettimeofday_ms();
  } else {
    DINGO_LOG(INFO) << "RecycleOrphanRegionOnCoordinator skip because not leader";
    return;
  }

  butil::FlatMap<int64_t, pb::coordinator_internal::RegionInternal> regions;
  regions.init(3000);
  region_map_.GetRawMapCopy(regions);

  if (regions.empty()) {
    DINGO_LOG(DEBUG) << "No region to recycle";
    return;
  }

  std::vector<int64_t> delete_region_ids;
  for (const auto& it : regions) {
    const auto& region = it.second;
    if (region.definition().table_id() > 0) {
      auto exists = deleted_table_meta_->Exists(region.definition().table_id());
      if (exists) {
        DINGO_LOG(INFO) << "RecycleOrphanRegionOnCoordinator region_id: " << region.id()
                        << " table_id: " << region.definition().table_id() << " is deleted";
        delete_region_ids.push_back(region.id());
      }
    } else if (region.definition().index_id() > 0) {
      auto exists = deleted_index_meta_->Exists(region.definition().index_id());
      if (exists) {
        DINGO_LOG(INFO) << "RecycleOrphanRegionOnCoordinator region_id: " << region.id()
                        << " index_id: " << region.definition().index_id() << " is deleted";
        delete_region_ids.push_back(region.id());
      }
    } else {
      DINGO_LOG(INFO) << "RecycleOrphanRegionOnCoordinator region_id: " << region.id()
                      << " table_id: " << region.definition().table_id()
                      << " index_id: " << region.definition().index_id() << " is not table or index";
    }
  }

  pb::coordinator_internal::MetaIncrement meta_increment;
  for (auto& region_id : delete_region_ids) {
    DINGO_LOG(WARNING) << "RecycleOrphanRegionOnCoordinator delete region_id: " << region_id;
    DropRegion(region_id, meta_increment);
  }

  if (meta_increment.ByteSizeLong() > 0) {
    SubmitMetaIncrementSync(meta_increment);

    // clear up region_metrics
    std::vector<int64_t> region_ids_in_metrics;
    region_metrics_map_.GetAllKeys(region_ids_in_metrics);
    for (auto region_id : region_ids_in_metrics) {
      if (!region_map_.Exists(region_id)) {
        region_metrics_map_.Erase(region_id);
      }
    }
  }
}

void CoordinatorControl::DeleteRegionBvar(int64_t region_id) {
  coordinator_bvar_metrics_region_.DeleteRegionBvar(region_id);
}

// create region ids
butil::Status CoordinatorControl::CreateRegionId(uint32_t count, std::vector<int64_t>& region_ids,
                                                 pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (count <= 0) {
    DINGO_LOG(ERROR) << "CreateRegionId count must be positive, count=" << count;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "count must be positive");
  }

  DINGO_LOG(INFO) << "CreateRegionId count=" << count;

  for (uint32_t i = 0; i < count; i++) {
    int64_t region_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION, meta_increment);
    region_ids.push_back(region_id);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::QueryRegion(int64_t region_id, pb::common::Region& region) {
  if (region_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "region_id must be positive");
  }

  pb::coordinator_internal::RegionInternal region_internal;
  int ret = region_map_.Get(region_id, region_internal);
  if (ret < 0) {
    DINGO_LOG(INFO) << "QueryRegion region_id not exists, id=" << region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region_id not exists");
  }

  GenRegionFull(region_internal, region);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CreateRegionForSplitInternal(
    int64_t split_from_region_id, int64_t& new_region_id, bool is_shadow_create,
    std::vector<pb::coordinator::StoreOperation>& store_operations,
    pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (split_from_region_id <= 0) {
    DINGO_LOG(ERROR) << "CreateRegionForSplit region_id not exists, id=" << split_from_region_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "split_from_region_id must be positive");
  }

  std::vector<int64_t> store_ids;

  pb::coordinator_internal::RegionInternal split_from_region;
  int ret = region_map_.Get(split_from_region_id, split_from_region);
  if (ret < 0) {
    DINGO_LOG(INFO) << "CreateRegionForSplit region_id not exists, id=" << split_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region_id not exists");
  }

  for (const auto& peer : split_from_region.definition().peers()) {
    store_ids.push_back(peer.store_id());
  }

  std::string new_region_name = split_from_region.definition().name();
  auto pos = split_from_region.definition().name().rfind('[');
  if (pos != std::string::npos) {
    new_region_name = split_from_region.definition().name().substr(0, pos);
  }
  split_from_region.mutable_definition()->set_name(new_region_name);

  std::string max_start_key = Helper::GenMaxStartKey();

  auto new_range = split_from_region.definition().range();
  new_range.set_start_key(max_start_key);
  new_range.set_end_key(split_from_region.definition().range().start_key());

  DINGO_LOG(INFO) << "CreateRegionForSplitInternal split_from_region_id=" << split_from_region_id
                  << ", new_region_id=" << new_region_id
                  << ", new_range start_key=" << Helper::StringToHex(new_range.start_key())
                  << ", end_key=" << Helper::StringToHex(new_range.end_key())
                  << ", is_shadow_create=" << is_shadow_create;

  // create region with split_from_region_id & store_ids
  if (is_shadow_create) {
    return CreateShadowRegion(split_from_region.definition().name(), split_from_region.region_type(),
                              split_from_region.definition().raw_engine(), "", store_ids.size(), new_range,
                              split_from_region.definition().schema_id(), split_from_region.definition().table_id(),
                              split_from_region.definition().index_id(), split_from_region.definition().part_id(),
                              split_from_region.definition().tenant_id(),
                              split_from_region.definition().index_parameter(), store_ids, split_from_region_id,
                              new_region_id, meta_increment);
  } else {
    return CreateRegionFinal(split_from_region.definition().name(), split_from_region.region_type(),
                             split_from_region.definition().raw_engine(), "", store_ids.size(), new_range,
                             split_from_region.definition().schema_id(), split_from_region.definition().table_id(),
                             split_from_region.definition().index_id(), split_from_region.definition().part_id(),
                             split_from_region.definition().tenant_id(),
                             split_from_region.definition().index_parameter(), store_ids, split_from_region_id,
                             new_region_id, store_operations, meta_increment);
  }
}

butil::Status CoordinatorControl::CreateRegionForSplit(const std::string& region_name,
                                                       pb::common::RegionType region_type,
                                                       const std::string& resource_tag, pb::common::Range region_range,
                                                       int64_t split_from_region_id, int64_t& new_region_id,
                                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (split_from_region_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "split_from_region_id must be positive");
  }

  std::vector<int64_t> store_ids;

  pb::coordinator_internal::RegionInternal split_from_region;
  int ret = region_map_.Get(split_from_region_id, split_from_region);
  if (ret < 0) {
    DINGO_LOG(INFO) << "CreateRegionForSplit region_id not exists, id=" << split_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "region_id not exists");
  }

  for (const auto& peer : split_from_region.definition().peers()) {
    store_ids.push_back(peer.store_id());
  }

  // create region with split_from_region_id & store_ids
  std::vector<pb::coordinator::StoreOperation> store_operations;
  return CreateRegionFinal(region_name, region_type, split_from_region.definition().raw_engine(), resource_tag,
                           store_ids.size(), region_range, split_from_region.definition().schema_id(),
                           split_from_region.definition().table_id(), split_from_region.definition().index_id(),
                           split_from_region.definition().part_id(), split_from_region.definition().tenant_id(),
                           split_from_region.definition().index_parameter(), store_ids, split_from_region_id,
                           new_region_id, store_operations, meta_increment);
}

butil::Status CoordinatorControl::SelectStore(pb::common::StoreType store_type, int32_t replica_num,
                                              const std::string& resource_tag,
                                              const pb::common::IndexParameter& index_parameter,
                                              std::vector<int64_t>& store_ids,
                                              std::vector<pb::common::Store>& selected_stores_for_regions) {
  DINGO_LOG(INFO) << "SelectStore replica_num=" << replica_num << ", resource_tag=" << resource_tag
                  << ", store_ids.size=" << store_ids.size();

  std::vector<pb::common::Store> stores_for_regions;

  // if store_ids is not null, select store with store_ids
  // or when resource_tag exists, select store with resource_tag
  butil::FlatMap<int64_t, pb::common::Store> store_map_copy;
  store_map_copy.init(100);
  store_map_.GetRawMapCopy(store_map_copy);

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

      if (store.store_type() != store_type) {
        DINGO_LOG(INFO) << "Store type not match, store_id=" << store.id()
                        << ", store_type=" << pb::common::StoreType_Name(store.store_type())
                        << ", expect_store_type=" << pb::common::StoreType_Name(store_type);
        continue;
      }

      if (resource_tag.length() == 0) {
        stores_for_regions.push_back(store);
      } else if (store.resource_tag() == resource_tag) {
        stores_for_regions.push_back(store);
      }
    }
  } else {
    if (store_ids.size() != replica_num) {
      DINGO_LOG(INFO) << "Store ids size not match, store_ids.size=" << store_ids.size()
                      << ", replica_num=" << replica_num;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "store_ids size not match replica_num");
    }

    for (const auto id : store_ids) {
      auto* ptr = store_map_copy.seek(id);
      if (ptr == nullptr) {
        selected_stores_for_regions.clear();
        DINGO_LOG(WARNING) << "Store id not found in CreateRegion with store_ids provided, store_id=" << id;
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "store_id not found in store_map");
      }

      const auto& store = *ptr;
      if (store.state() != pb::common::StoreState::STORE_NORMAL ||
          store.in_state() != pb::common::StoreInState::STORE_IN) {
        selected_stores_for_regions.clear();
        DINGO_LOG(INFO) << "Store state not normal or in, store_id=" << store.id()
                        << ", state=" << pb::common::StoreState_Name(store.state())
                        << ", in_state=" << pb::common::StoreInState_Name(store.in_state());
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "store state not normal or in");
      }

      if (store.store_type() != store_type) {
        selected_stores_for_regions.clear();
        DINGO_LOG(INFO) << "Store type not match, store_id=" << store.id()
                        << ", store_type=" << pb::common::StoreType_Name(store.store_type())
                        << ", expect_store_type=" << pb::common::StoreType_Name(store_type);
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "store type not match");
      }

      selected_stores_for_regions.push_back(store);
    }

    if (selected_stores_for_regions.size() != replica_num) {
      selected_stores_for_regions.clear();
      DINGO_LOG(INFO) << "Store ids size not match, store_ids.size=" << store_ids.size()
                      << ", replica_num=" << replica_num;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "store_ids size not match replica_num");
    }

    return butil::Status::OK();
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

  // check store metrics limit
  // now for all region, if disk/memory is lower then 5%, return -1
  // now for hnsw, if memory is less than hnsw memory limit, return -1
  butil::Status status = butil::Status::OK();
  std::vector<pb::common::Store> tmp_stores_for_regions;
  // if (store_type == pb::common::StoreType::NODE_TYPE_INDEX) {
  for (const auto& store : stores_for_regions) {
    std::vector<pb::common::StoreMetrics> tmp_store_metrics;
    GetStoreRegionMetrics(store.id(), tmp_store_metrics);
    if (tmp_store_metrics.empty()) {
      DINGO_LOG(WARNING) << "Store metrics not found, store_id=" << store.id() << ", just make use of it";
      tmp_stores_for_regions.push_back(store);
      continue;
    }

    const auto& store_metrics = tmp_store_metrics[0];
    const auto& store_own_metrics = store_metrics.store_own_metrics();
    // if (store_metrics.region_metrics_map_size() == 0) {
    //   DINGO_LOG(INFO) << "Store metrics region_metrics_map_size is 0, store_id=" << store.id()
    //                   << ", just make use of it";
    //   tmp_stores_for_regions.push_back(store);
    //   continue;
    // }

    if (store_own_metrics.system_total_memory() == 0) {
      DINGO_LOG(WARNING) << "Store metrics system_total_memory is 0, store_id=" << store.id()
                         << ", just make use of it";
      tmp_stores_for_regions.push_back(store);
      continue;
    }

    if (store_own_metrics.system_available_memory() < store_own_metrics.system_total_memory() * 0.05) {
      DINGO_LOG(ERROR) << "Store metrics system_available_memory < system_total_memory * 0.05, store_id=" << store.id()
                       << ", system_free_memory=" << store_own_metrics.system_free_memory()
                       << ", system_available_memory=" << store_own_metrics.system_available_memory()
                       << ", system_total_memory=" << store_own_metrics.system_total_memory();
      status = butil::Status(pb::error::Errno::EREGION_UNAVAILABLE,
                             "Not enough stores for create region, one store has low memory");
      continue;
    }

    if (store_own_metrics.system_total_capacity() == 0) {
      DINGO_LOG(WARNING) << "Store metrics system_total_capacity is 0, store_id=" << store.id()
                         << ", just make use of it";
      tmp_stores_for_regions.push_back(store);
      continue;
    }

    if (store_own_metrics.system_free_capacity() < store_own_metrics.system_total_capacity() * 0.05) {
      DINGO_LOG(ERROR) << "Store metrics system_free_capacity < system_total_capacity * 0.05, store_id=" << store.id()
                       << ", system_free_capacity=" << store_own_metrics.system_free_capacity()
                       << ", system_total_capacity=" << store_own_metrics.system_total_capacity();
      status = butil::Status(pb::error::Errno::EREGION_UNAVAILABLE,
                             "Not enough stores for create region, one store has low capacity");
      continue;
    }

    if (index_parameter.vector_index_parameter().vector_index_type() != pb::common::VECTOR_INDEX_TYPE_HNSW) {
      DINGO_LOG(INFO) << "Store metrics vector_index_type is not hnsw, store_id=" << store.id()
                      << ", vector_index_type=" << index_parameter.vector_index_parameter().vector_index_type();
      tmp_stores_for_regions.push_back(store);
      continue;
    }

    const auto& hnsw_parameter = index_parameter.vector_index_parameter().hnsw_parameter();
    int64_t new_hnsw_index_plan_memory = hnsw_parameter.dimension() * hnsw_parameter.max_elements() * 4;
    DINGO_LOG(INFO) << "Store metrics new_hnsw_index_plan_memory=" << new_hnsw_index_plan_memory
                    << ", store_id=" << store.id() << ", region_count=" << store_metrics.region_metrics_map_size();
    if (new_hnsw_index_plan_memory > store_own_metrics.system_available_memory() * 0.95) {
      DINGO_LOG(INFO) << "Store metrics hnsw_memory_plan_used > system_available_memory * 0.95, store_id=" << store.id()
                      << ", new_hnsw_memory_plan_used=" << new_hnsw_index_plan_memory
                      << ", system_available_memory=" << store_own_metrics.system_available_memory();
      status = butil::Status(pb::error::Errno::EREGION_UNAVAILABLE,
                             "Not enough stores for create region, one store has low memory for hnsw");
      continue;
    }

    // we have implement the elastic max_elements for hnsw index, so we don't need to check the max_elements memory
    // for hnsw calc memory
    int64_t vector_index_used_memory = 0;
    int64_t hnsw_memory_plan_used = 0;
    for (const auto& region_metrics : store_metrics.region_metrics_map()) {
      const auto& vector_index_parameter =
          region_metrics.second.region_definition().index_parameter().vector_index_parameter();
      if (vector_index_parameter.vector_index_type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE) {
        vector_index_used_memory += region_metrics.second.vector_index_metrics().memory_bytes();
      }
      if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
        const auto& hnsw_parameter = vector_index_parameter.hnsw_parameter();
        hnsw_memory_plan_used += hnsw_parameter.dimension() * hnsw_parameter.max_elements();
      }
    }
    DINGO_LOG(INFO) << "Store metrics vector_index_used_memory=" << vector_index_used_memory
                    << ", hnsw_memory_plan=" << hnsw_memory_plan_used << ", store_id=" << store.id()
                    << ", region_count=" << store_metrics.region_metrics_map_size();

    tmp_stores_for_regions.push_back(store);

    // DEPRECATED
    // if ((hnsw_memory_plan_used + new_hnsw_index_plan_memory) * 0.30 < store_own_metrics.system_total_memory()) {
    //   DINGO_LOG(INFO) << "Store metrics hnsw_memory_plan_used * 0.30 < system_total_memory, store_id=" << store.id()
    //                   << ", hnsw_memory_plan_used=" << hnsw_memory_plan_used
    //                   << ", new_hnsw_memory_plan_used=" << new_hnsw_index_plan_memory
    //                   << ", system_total_memory=" << store_own_metrics.system_total_memory();
    //   tmp_stores_for_regions.push_back(store);
    //   continue;
    // } else {
    //   DINGO_LOG(ERROR) << "Store metrics hnsw_memory_plan_used * 0.30 >= system_total_memory, store_id=" <<
    //   store.id()
    //                    << ", hnsw_memory_plan_used=" << hnsw_memory_plan_used
    //                    << ", new_hnsw_memory_plan_used=" << new_hnsw_index_plan_memory
    //                    << ", system_total_memory=" << store_own_metrics.system_total_memory();
    //   status = butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "Not enough stores for create region");
    //   continue;
    // }
  }

  // if not enough stores is selected, return -1
  if (tmp_stores_for_regions.size() < replica_num) {
    std::string selected_store_ids;
    for (const auto& store : tmp_stores_for_regions) {
      selected_store_ids += std::to_string(store.id()) + ",";
    }
    DINGO_LOG(INFO) << "Not enough stores STORE_NORMAL for create region after vector index memory check, replica_num="
                    << replica_num << ", resource_tag=" << resource_tag << ", store_ids.size=" << store_ids.size()
                    << ", selected_store_ids=" << selected_store_ids;
    return status;
  }
  stores_for_regions.swap(tmp_stores_for_regions);

  struct StoreMore {
    pb::common::Store store;
    int64_t weight;
    int64_t region_num;
    int64_t system_total_capacity;    // total capacity of this store
    int64_t system_free_capacity;     // free capacity of this store
    int64_t system_cpu_usage;         // cpu usage of this store process
    int64_t system_total_memory;      // total memory of the host this store process running on
    int64_t system_free_memory;       // total free memory of the host this store process running on
    int64_t system_available_memory;  // total available memory of the host this store process running on
    int64_t system_io_util;           // io utilization of the host this store process running on
    int64_t process_used_cpu;         // cpu usage of this store process
    int64_t process_used_memory;      // total used memory of this store process
    int64_t process_used_capacity;    // free capacity of this store
  };

  // check and sort store by capacity, regions_num
  std::vector<StoreMore> store_more_vec;
  for (const auto& it : stores_for_regions) {
    StoreMore store_more;
    store_more.store = it;

    bool has_metrics = false;
    pb::common::StoreOwnMetrics store_own_metrics;

    {
      BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
      auto it2 = store_metrics_map_.find(it.id());
      if (it2 != store_metrics_map_.end()) {
        store_own_metrics = it2->second.store_own_metrics;
        store_more.region_num = it2->second.region_num > 0 ? it2->second.region_num : 0;
      }
    }

    if (store_own_metrics.ByteSizeLong() > 0) {
      store_more.system_free_capacity =
          store_own_metrics.system_free_capacity() > 0 ? store_own_metrics.system_free_capacity() : 0;
      store_more.system_total_capacity =
          store_own_metrics.system_total_capacity() > 0 ? store_own_metrics.system_total_capacity() : 0;
      store_more.system_total_memory =
          store_own_metrics.system_total_memory() > 0 ? store_own_metrics.system_free_memory() : 0;
      store_more.system_free_memory =
          store_own_metrics.system_free_memory() > 0 ? store_own_metrics.system_free_memory() : 0;
      store_more.system_available_memory =
          store_own_metrics.system_available_memory() > 0 ? store_own_metrics.system_available_memory() : 0;
      has_metrics = true;
    }

    if (has_metrics) {
      if (store_type == pb::common::StoreType::NODE_TYPE_STORE) {
        store_more.weight = store_more.system_free_capacity * 100 / store_more.system_total_capacity +
                            (100 / (store_more.region_num + 1));
      } else if (store_type == pb::common::StoreType::NODE_TYPE_INDEX) {
        store_more.weight = store_more.system_available_memory * 100 / store_more.system_total_memory +
                            (100 / (store_more.region_num + 1));
      }

      if (store_more.system_total_capacity == 0) {
        store_more.weight = 0;
      }

      if (store_more.system_available_memory == 0) {
        store_more.weight = 0;
      }

      store_more.weight = store_more.weight * Helper::GenerateRealRandomInteger(1, 20);
    } else {
      store_more.weight = 0;
    }

    store_more_vec.push_back(store_more);
    DINGO_LOG(INFO) << "store_more_vec.push_back store_id=" << store_more.store.id()
                    << ", region_num=" << store_more.region_num << ", free_capacity=" << store_more.system_free_capacity
                    << ", total_capacity=" << store_more.system_total_capacity
                    << ", free_memory=" << store_more.system_free_memory
                    << ", available_memory=" << store_more.system_available_memory
                    << ", total_memory=" << store_more.system_total_memory << ", weight=" << store_more.weight;
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

butil::Status CoordinatorControl::ValidateMaxRegionCount() {
  auto region_count = region_map_.Size();
  if (region_count > FLAGS_max_region_count) {
    DINGO_LOG(ERROR) << "ValidateMaxRegionCount region_count=" << region_count
                     << ", max_region_count=" << FLAGS_max_region_count;
    return butil::Status(pb::error::Errno::EREGION_COUNT_EXCEED_LIMIT, "region count reach max limit");
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CheckRegionPrefix(const std::string& start_key, const std::string& end_key) {
  if (start_key.size() < 8 || end_key.size() < 8) {
    DINGO_LOG(ERROR) << "region range illegal, start_key: " << Helper::StringToHex(start_key)
                     << ", end_key: " << Helper::StringToHex(end_key);
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "region range illegal");
  }

  auto max_start_key = Helper::GenMaxStartKey();
  if (start_key[0] != end_key[0] && start_key != max_start_key) {
    DINGO_LOG(ERROR) << "region range illegal, start_key: " << Helper::StringToHex(start_key)
                     << ", end_key: " << Helper::StringToHex(end_key);
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "region range illegal");
  }

  if (start_key[0] == max_start_key[0] && start_key != max_start_key) {
    DINGO_LOG(ERROR) << "region range illegal, start_key: " << Helper::StringToHex(start_key)
                     << ", end_key: " << Helper::StringToHex(end_key);
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "region range illegal");
  }

  // check if range is overlaping exist region
  std::vector<pb::coordinator_internal::RegionInternal> regions;
  auto ret1 = ScanRegions(start_key, end_key, 0, regions);
  if (!ret1.ok()) {
    DINGO_LOG(ERROR) << "ScanRegions failed, start_key: " << Helper::StringToHex(start_key)
                     << ", end_key: " << Helper::StringToHex(end_key);
    return ret1;
  }

  if (!regions.empty() && start_key != max_start_key) {
    DINGO_LOG(ERROR) << "region range is overlaping exist region, start_key: " << Helper::StringToHex(start_key)
                     << ", end_key: " << Helper::StringToHex(end_key);
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "region range is overlaping exist region");
  }

  auto region_prefix = start_key[0];
  if (region_prefix == 0) {
    DINGO_LOG(ERROR) << "region has no prefix, this is a legacy region, and is not allowed now, start_key: "
                     << Helper::StringToHex(start_key) << ", end_key: " << Helper::StringToHex(end_key);
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "region has no prefix, this is a legacy region");
  } else if (region_prefix == Constant::kExecutorRaw) {
    DINGO_LOG(INFO) << "region is kExecutorRaw";
    return butil::Status::OK();
  } else if (region_prefix == Constant::kExecutorTxn) {
    DINGO_LOG(INFO) << "region is kExecutorTxn";
    return butil::Status::OK();
  } else if (region_prefix == Constant::kClientRaw) {
    DINGO_LOG(INFO) << "region is kClientRaw";
    return butil::Status::OK();
  } else if (region_prefix == Constant::kClientTxn) {
    DINGO_LOG(INFO) << "region is kClientTxn";
    return butil::Status::OK();
  } else if (region_prefix == max_start_key[0]) {
    DINGO_LOG(INFO) << "region is MAX_START_KEY";
    return butil::Status::OK();
  } else {
    DINGO_LOG(ERROR) << "region prefix is not legal, start_key: " << Helper::StringToHex(start_key)
                     << ", end_key: " << Helper::StringToHex(end_key);
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "region prefix is not legal");
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CreateShadowRegion(
    const std::string& region_name, pb::common::RegionType region_type, pb::common::RawEngine raw_engine,
    const std::string& resource_tag, int32_t replica_num, pb::common::Range region_range, int64_t schema_id,
    int64_t table_id, int64_t index_id, int64_t part_id, int64_t tenant_id,
    const pb::common::IndexParameter& index_parameter, std::vector<int64_t>& store_ids, int64_t split_from_region_id,
    int64_t& new_region_id, pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "CreateShadowRegion replica_num=" << replica_num << ", region_name=" << region_name
                  << ", region_type=" << pb::common::RegionType_Name(region_type) << ", resource_tag=" << resource_tag
                  << ", store_ids.size=" << store_ids.size() << ", region_range=" << region_range.ShortDebugString()
                  << ", schema_id=" << schema_id << ", table_id=" << table_id << ", index_id=" << index_id
                  << ", part_id=" << part_id << ", tenant_id=" << tenant_id
                  << ", index_parameter=" << index_parameter.ShortDebugString()
                  << ", split_from_region_id=" << split_from_region_id;

  auto ret = CheckRegionPrefix(region_range.start_key(), region_range.end_key());
  if (!ret.ok()) {
    return ret;
  }

  if (Server::GetInstance().IsReadOnly() || GetForceReadOnly()) {
    DINGO_LOG(WARNING) << "CreateShadowRegion cluster is read only, cannot create region, is_read_only = "
                       << Server::GetInstance().IsReadOnly() << ", force_read_only = " << GetForceReadOnly();
    return butil::Status(pb::error::Errno::ESYSTEM_CLUSTER_READ_ONLY, "cluster is read only, cannot create region");
  }

  auto ret1 = ValidateMaxRegionCount();
  if (!ret1.ok()) {
    return ret1;
  }

  std::vector<pb::common::Store> selected_stores_for_regions;
  pb::common::IndexParameter new_index_parameter = index_parameter;

  // setup store_type
  pb::common::StoreType store_type = pb::common::StoreType::NODE_TYPE_STORE;
  if (region_type == pb::common::RegionType::INDEX_REGION && new_index_parameter.has_vector_index_parameter()) {
    store_type = pb::common::StoreType::NODE_TYPE_INDEX;

    // validate vector index region range
    // range's start_key and end_key must be less than 16 bytes
    if (region_range.start_key().size() != Constant::kVectorKeyMinLenWithPrefix &&
        region_range.start_key().size() != Constant::kVectorKeyMaxLenWithPrefix) {
      DINGO_LOG(ERROR) << "CreateRegion vector index region range start_key size is not 8 or 16, start_key="
                       << Helper::StringToHex(region_range.start_key());
      return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE,
                           "vector index region range start_key size is not 8 or 16 bytes");
    }

    if (region_range.end_key().size() != Constant::kVectorKeyMinLenWithPrefix &&
        region_range.end_key().size() != Constant::kVectorKeyMaxLenWithPrefix) {
      DINGO_LOG(ERROR) << "CreateRegion vector index region range end_key size is not 8 or 16, end_key="
                       << Helper::StringToHex(region_range.end_key());
      return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE,
                           "vector index region range end_key size is not 8 or 16 bytes");
    }

    if (part_id <= 0) {
      DINGO_LOG(ERROR) << "CreateRegion vector index region part_id is not legal, part_id=" << part_id;
      return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "vector index region part_id is not legal");
    }

    // if vector index is hnsw, need to limit max_elements of each region to less than 512MB / dimenstion / 4
    if (new_index_parameter.vector_index_parameter().vector_index_type() ==
        pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      auto* hnsw_parameter = new_index_parameter.mutable_vector_index_parameter()->mutable_hnsw_parameter();
      auto ret1 = VectorIndexHnsw::CheckAndSetHnswParameter(*hnsw_parameter);
      if (!ret1.ok()) {
        DINGO_LOG(ERROR) << "CreateRegion vector index region hnsw parameter is not legal, ret=" << ret1;
        return ret1;
      }
    }
  }

  // select store for region
  butil::FlatMap<int64_t, pb::common::Store> store_map_copy;
  store_map_copy.init(100);
  store_map_.GetRawMapCopy(store_map_copy);
  selected_stores_for_regions.reserve(store_ids.size());
  for (auto id : store_ids) {
    if (store_map_copy.seek(id) != nullptr) {
      selected_stores_for_regions.push_back(store_map_copy[id]);
    }
  }
  if (selected_stores_for_regions.size() != store_ids.size()) {
    DINGO_LOG(ERROR) << "CreateShadowRegion store_ids not exists, store_ids.size=" << store_ids.size()
                     << ", selected_stores_for_regions.size=" << selected_stores_for_regions.size();
    return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "Not enough stores for create shadow region");
  }
  if (selected_stores_for_regions.size() < replica_num) {
    DINGO_LOG(ERROR) << "CreateShadowRegion store_ids not enough, store_ids.size=" << store_ids.size()
                     << ", selected_stores_for_regions.size=" << selected_stores_for_regions.size()
                     << ", replicat_num=" << replica_num;
    return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "Not enough stores for create shadow region");
  }

  // generate new region
  if (new_region_id <= 0) {
    new_region_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION, meta_increment);
  }

  int64_t const create_region_id = new_region_id;

  if (region_map_.Exists(create_region_id)) {
    DINGO_LOG(ERROR) << "create_region_id =" << create_region_id << " is illegal, cannot create region!!";
    return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "create_region_id is illegal");
  }

  // create new region in memory begin
  pb::coordinator_internal::RegionInternal new_region;
  new_region.set_id(create_region_id);
  new_region.set_epoch(1);
  new_region.set_state(::dingodb::pb::common::RegionState::REGION_NEW);
  new_region.set_create_timestamp(butil::gettimeofday_ms());
  new_region.set_region_type(region_type);

  // create region definition begin
  auto* region_definition = new_region.mutable_definition();
  region_definition->set_id(create_region_id);
  region_definition->set_name(region_name + std::string("[") + std::to_string(create_region_id) + std::string("]"));
  region_definition->mutable_epoch()->set_conf_version(1);
  region_definition->mutable_epoch()->set_version(1);
  region_definition->set_schema_id(schema_id);
  region_definition->set_table_id(table_id);
  region_definition->set_index_id(index_id);
  region_definition->set_part_id(part_id);
  region_definition->set_part_id(tenant_id);
  region_definition->set_raw_engine(raw_engine);
  if (new_index_parameter.index_type() != pb::common::IndexType::INDEX_TYPE_NONE) {
    *(region_definition->mutable_index_parameter()) = new_index_parameter;
  }

  // set region range in region definition, this is provided by sdk
  auto* range_in_definition = region_definition->mutable_range();
  *range_in_definition = region_range;

  // add store_id and its peer location to region
  for (int i = 0; i < replica_num; i++) {
    auto store = selected_stores_for_regions[i];
    auto* peer = region_definition->add_peers();
    peer->set_store_id(store.id());
    peer->set_role(::dingodb::pb::common::PeerRole::VOTER);
    *(peer->mutable_server_location()) = store.server_location();
    *(peer->mutable_raft_location()) = store.raft_location();
  }

  *(new_region.mutable_definition()) = (*region_definition);
  // create region definition end
  // create new region in memory end

  // update meta_increment
  auto* region_increment = meta_increment.add_regions();
  region_increment->set_id(create_region_id);
  region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  region_increment->set_table_id(table_id);

  auto* region_increment_region = region_increment->mutable_region();
  *region_increment_region = new_region;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CreateRegionFinal(
    const std::string& region_name, pb::common::RegionType region_type, pb::common::RawEngine raw_engine,
    const std::string& resource_tag, int32_t replica_num, pb::common::Range region_range, int64_t schema_id,
    int64_t table_id, int64_t index_id, int64_t part_id, int64_t tenant_id,
    const pb::common::IndexParameter& index_parameter, std::vector<int64_t>& store_ids, int64_t split_from_region_id,
    int64_t& new_region_id, std::vector<pb::coordinator::StoreOperation>& store_operations,
    pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "CreateRegion replica_num=" << replica_num << ", region_name=" << region_name
                  << ", region_type=" << pb::common::RegionType_Name(region_type) << ", resource_tag=" << resource_tag
                  << ", store_ids.size=" << store_ids.size() << ", region_range=" << region_range.ShortDebugString()
                  << ", schema_id=" << schema_id << ", table_id=" << table_id << ", index_id=" << index_id
                  << ", part_id=" << part_id << ", tenant_id=" << tenant_id
                  << ", index_parameter=" << index_parameter.ShortDebugString()
                  << ", split_from_region_id=" << split_from_region_id;

  auto ret = CheckRegionPrefix(region_range.start_key(), region_range.end_key());
  if (!ret.ok()) {
    return ret;
  }

  if (Server::GetInstance().IsReadOnly() || GetForceReadOnly()) {
    DINGO_LOG(WARNING) << "CreateRegionFinal cluster is read only, cannot create region, is_read_only = "
                       << Server::GetInstance().IsReadOnly() << ", force_read_only = " << GetForceReadOnly();
    return butil::Status(pb::error::Errno::ESYSTEM_CLUSTER_READ_ONLY, "cluster is read only, cannot create region");
  }

  auto ret1 = ValidateMaxRegionCount();
  if (!ret1.ok()) {
    return ret1;
  }

  std::vector<pb::common::Store> selected_stores_for_regions;
  pb::common::IndexParameter new_index_parameter = index_parameter;

  // setup store_type
  pb::common::StoreType store_type = pb::common::StoreType::NODE_TYPE_STORE;
  if (region_type == pb::common::RegionType::INDEX_REGION && new_index_parameter.has_vector_index_parameter()) {
    store_type = pb::common::StoreType::NODE_TYPE_INDEX;

    // validate vector index region range
    // range's start_key and end_key must be less than 16 bytes
    if (region_range.start_key().size() != Constant::kVectorKeyMinLenWithPrefix &&
        region_range.start_key().size() != Constant::kVectorKeyMaxLenWithPrefix) {
      DINGO_LOG(ERROR) << "CreateRegion vector index region range start_key size is not 8 or 16, start_key="
                       << Helper::StringToHex(region_range.start_key());
      return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE,
                           "vector index region range start_key size is not 8 or 16 bytes");
    }

    if (region_range.end_key().size() != Constant::kVectorKeyMinLenWithPrefix &&
        region_range.end_key().size() != Constant::kVectorKeyMaxLenWithPrefix) {
      DINGO_LOG(ERROR) << "CreateRegion vector index region range end_key size is not 8 or 16, end_key="
                       << Helper::StringToHex(region_range.end_key());
      return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE,
                           "vector index region range end_key size is not 8 or 16 bytes");
    }

    if (part_id <= 0) {
      DINGO_LOG(ERROR) << "CreateRegion vector index region part_id is not legal, part_id=" << part_id;
      return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "vector index region part_id is not legal");
    }

    // if vector index is hnsw, need to limit max_elements of each region to less than 512MB / dimenstion / 4
    if (new_index_parameter.vector_index_parameter().vector_index_type() ==
        pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      auto* hnsw_parameter = new_index_parameter.mutable_vector_index_parameter()->mutable_hnsw_parameter();
      auto ret1 = VectorIndexHnsw::CheckAndSetHnswParameter(*hnsw_parameter);
      if (!ret1.ok()) {
        DINGO_LOG(ERROR) << "CreateRegion vector index region hnsw parameter is not legal, ret=" << ret1;
        return ret1;
      }
    }
  }

  // select store for region
  ret = SelectStore(store_type, replica_num, resource_tag, new_index_parameter, store_ids, selected_stores_for_regions);
  if (!ret.ok()) {
    return ret;
  }

  // generate new region
  if (new_region_id <= 0) {
    new_region_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION, meta_increment);
  }

  int64_t const create_region_id = new_region_id;

  if (region_map_.Exists(create_region_id)) {
    DINGO_LOG(ERROR) << "create_region_id =" << create_region_id << " is illegal, cannot create region!!";
    return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "create_region_id is illegal");
  }

  // create new region in memory begin
  pb::coordinator_internal::RegionInternal new_region;
  new_region.set_id(create_region_id);
  new_region.set_epoch(1);
  new_region.set_state(::dingodb::pb::common::RegionState::REGION_NEW);
  new_region.set_create_timestamp(butil::gettimeofday_ms());
  new_region.set_region_type(region_type);

  // create region definition begin
  auto* region_definition = new_region.mutable_definition();
  region_definition->set_id(create_region_id);
  region_definition->set_name(region_name + std::string("[") + std::to_string(create_region_id) + std::string("]"));
  region_definition->mutable_epoch()->set_conf_version(1);
  region_definition->mutable_epoch()->set_version(1);
  region_definition->set_schema_id(schema_id);
  region_definition->set_table_id(table_id);
  region_definition->set_index_id(index_id);
  region_definition->set_part_id(part_id);
  region_definition->set_tenant_id(tenant_id);
  region_definition->set_raw_engine(raw_engine);
  *(region_definition->mutable_index_parameter()) = new_index_parameter;

  // set region range in region definition, this is provided by sdk
  auto* range_in_definition = region_definition->mutable_range();
  *range_in_definition = region_range;

  // add store_id and its peer location to region
  for (int i = 0; i < replica_num; i++) {
    auto store = selected_stores_for_regions[i];
    auto* peer = region_definition->add_peers();
    peer->set_store_id(store.id());
    peer->set_role(::dingodb::pb::common::PeerRole::VOTER);
    *(peer->mutable_server_location()) = store.server_location();
    *(peer->mutable_raft_location()) = store.raft_location();
  }

  *(new_region.mutable_definition()) = (*region_definition);
  // create region definition end
  // create new region in memory end

  // create store operations
  // std::vector<pb::coordinator::StoreOperation> store_operations;
  for (int i = 0; i < replica_num; i++) {
    auto store = selected_stores_for_regions[i];
    pb::coordinator::StoreOperation store_operation;

    store_operation.set_id(store.id());
    auto* region_cmd = store_operation.add_region_cmds();
    region_cmd->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
    region_cmd->set_region_id(create_region_id);
    region_cmd->set_create_timestamp(butil::gettimeofday_ms());
    region_cmd->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_CREATE);
    region_cmd->set_is_notify(true);  // for create region, we need immediately heartbeat
    auto* create_request = region_cmd->mutable_create_request();
    *(create_request->mutable_region_definition()) = (*region_definition);
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
  *region_increment_region = new_region;

  // add store operations to meta_increment
  for (const auto& store_operation : store_operations) {
    for (const auto& region_cmd : store_operation.region_cmds()) {
      auto ret = AddRegionCmd(store_operation.id(), 0, region_cmd, meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "CreateRegion AddRegionCmd failed, store_id=" << store_operation.id()
                         << ", region_cmd=" << region_cmd.ShortDebugString();
        return ret;
      }
      DINGO_LOG(INFO) << "AddRegionCmd store_id=" << store_operation.id()
                      << ", region_cmd=" << region_cmd.ShortDebugString();
    }

    DINGO_LOG(INFO) << "store_operation_increment = " << store_operation.ShortDebugString();
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetCreateRegionStoreIds(pb::common::RegionType region_type,
                                                          pb::common::RawEngine raw_engine,
                                                          const std::string& resource_tag, int32_t replica_num,
                                                          const pb::common::IndexParameter& index_parameter,
                                                          std::vector<int64_t>& store_ids) {
  DINGO_LOG(INFO) << "GetCreateRegionStoreIds replica_num=" << replica_num
                  << ", raw_engine=" << pb::common::RawEngine_Name(raw_engine)
                  << ", region_type=" << pb::common::RegionType_Name(region_type) << ", resource_tag=" << resource_tag
                  << ", index_parameter=" << index_parameter.ShortDebugString();

  if (Server::GetInstance().IsReadOnly() || GetForceReadOnly()) {
    DINGO_LOG(WARNING) << "CreateRegionFinal cluster is read only, cannot create region, is_read_only = "
                       << Server::GetInstance().IsReadOnly() << ", force_read_only = " << GetForceReadOnly();
    return butil::Status(pb::error::Errno::ESYSTEM_CLUSTER_READ_ONLY, "cluster is read only, cannot create region");
  }

  std::vector<pb::common::Store> selected_stores_for_regions;

  // setup store_type
  pb::common::StoreType store_type = pb::common::StoreType::NODE_TYPE_STORE;
  if (region_type == pb::common::RegionType::INDEX_REGION && index_parameter.has_vector_index_parameter()) {
    store_type = pb::common::StoreType::NODE_TYPE_INDEX;
  }

  // select store for region
  auto ret =
      SelectStore(store_type, replica_num, resource_tag, index_parameter, store_ids, selected_stores_for_regions);
  if (!ret.ok()) {
    return ret;
  }

  if (selected_stores_for_regions.size() != replica_num) {
    DINGO_LOG(ERROR) << "GetCreateRegionStoreIds replica_num=" << replica_num
                     << ", selected_stores_for_regions.size=" << selected_stores_for_regions.size();
    return butil::Status(pb::error::Errno::EREGION_UNAVAILABLE, "Not enough stores for create region");
  }

  store_ids.clear();
  for (const auto& store : selected_stores_for_regions) {
    store_ids.push_back(store.id());
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DropRegion(int64_t region_id,
                                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  std::vector<pb::coordinator::StoreOperation> store_operations;
  return this->DropRegionFinal(region_id, store_operations, meta_increment);
}

butil::Status CoordinatorControl::DropRegionFinal(int64_t region_id,
                                                  std::vector<pb::coordinator::StoreOperation>& store_operations,
                                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  // set region state to DELETE
  bool need_update_epoch = false;
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    pb::coordinator_internal::RegionInternal region_to_delete;
    int ret = region_map_.Get(region_id, region_to_delete);
    if (ret > 0) {
      if (region_to_delete.state() != ::dingodb::pb::common::RegionState::REGION_DELETED &&
          region_to_delete.state() != ::dingodb::pb::common::RegionState::REGION_DELETE &&
          region_to_delete.state() != ::dingodb::pb::common::RegionState::REGION_DELETING) {
        region_to_delete.set_state(::dingodb::pb::common::RegionState::REGION_DELETE);

        // update meta_increment
        // 1.delete region from region_map_
        need_update_epoch = true;
        auto* region_increment = meta_increment.add_regions();
        region_increment->set_id(region_id);
        region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

        auto* region_increment_region = region_increment->mutable_region();
        region_increment_region->set_id(region_id);

        // 2.add the deleted region to deleted_region_meta_
        auto* deleted_region_increment = meta_increment.add_deleted_regions();
        deleted_region_increment->set_id(region_id);
        deleted_region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

        auto* deleted_region_increment_region = deleted_region_increment->mutable_region();
        // *deleted_region_increment_region = region_to_delete;
        deleted_region_increment_region->set_state(::dingodb::pb::common::RegionState::REGION_DELETE);
        deleted_region_increment_region->set_id(region_id);
        deleted_region_increment_region->mutable_definition()->set_name(region_to_delete.definition().name());
        deleted_region_increment_region->set_deleted_timestamp(butil::gettimeofday_ms());

        // use TaskList to drop & purge region
        for (const auto& peer : region_to_delete.definition().peers()) {
          auto* increment_task_list = CreateTaskList(meta_increment);

          // this is delete_region task
          pb::coordinator::StoreOperation store_operation;
          AddDeleteTask(increment_task_list, peer.store_id(), region_id, &store_operation, meta_increment);

          // this is purge_region task
          // AddPurgeTask(increment_task_list, peer.store_id(), region_id,
          // meta_increment);

          // generate store operation for caller
          store_operations.push_back(store_operation);
        }

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
butil::Status CoordinatorControl::DropRegionPermanently(int64_t region_id,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  // set region state to DELETE
  bool need_update_epoch = false;
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    pb::coordinator_internal::RegionInternal region_to_delete;
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
      *(region_increment_region) = region_to_delete;
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

butil::Status CoordinatorControl::SplitRegion(int64_t split_from_region_id, int64_t split_to_region_id,
                                              std::string split_watershed_key,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate split_from_region_id
  pb::coordinator_internal::RegionInternal split_from_region;
  int ret = region_map_.Get(split_from_region_id, split_from_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "SplitRegion from region not exists, id = " << split_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "SplitRegion from region not exists");
  }

  // validate split_to_region_id
  pb::coordinator_internal::RegionInternal split_to_region;
  ret = region_map_.Get(split_to_region_id, split_to_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "SplitRegion to region not exists, id = " << split_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "SplitRegion to region not exists");
  }

  auto region_status = GetRegionStatus(split_from_region_id);

  // validate split_from_region and split_to_region has NORMAL status
  if (split_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      region_status.raft_status() != ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      region_status.heartbeat_status() != ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region is not ready for split, "
                        "split_from_region_id = "
                     << split_from_region_id << " from_state=" << split_from_region.state();
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region is not ready for split");
  }

  // check if all peers are healthy
  auto peer_status = CheckRegionAllPeerOnline(split_from_region_id);
  if (!peer_status.ok()) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region is not ready for split, "
                        "split_from_region_id = "
                     << split_from_region_id << " from_state=" << split_from_region.state()
                     << ",  error: " << peer_status.error_str();
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region is not ready for split, error: %s", peer_status.error_cstr());
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
    DINGO_LOG(ERROR) << "SplitRegion split_from_region and "
                        "split_to_region has different peers size";
    return butil::Status(pb::error::Errno::ESPLIT_PEER_NOT_MATCH,
                         "SplitRegion split_from_region and "
                         "split_to_region has different peers size");
  }

  std::vector<int64_t> split_from_region_peers;
  std::vector<int64_t> split_to_region_peers;
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
    DINGO_LOG(ERROR) << "SplitRegion split_from_region and "
                        "split_to_region has different peers";
    return butil::Status(pb::error::Errno::ESPLIT_PEER_NOT_MATCH,
                         "SplitRegion split_from_region and "
                         "split_to_region has different peers");
  }

  // validate split_from_region and split_to_region has NORMAL status
  if (split_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      split_to_region.state() != ::dingodb::pb::common::RegionState::REGION_STANDBY) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region or "
                        "split_to_region is not ready for split, "
                        "split_from_region_id = "
                     << split_from_region_id << " from_state=" << split_from_region.state()
                     << ", split_to_region_id = " << split_to_region_id << " to_state=" << split_to_region.state();
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region or "
                         "split_to_region is not ready for split");
  }

  // generate store operation for stores
  pb::coordinator::RegionCmd region_cmd;
  region_cmd.set_region_id(split_from_region_id);
  region_cmd.set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_SPLIT);
  region_cmd.mutable_split_request()->set_split_watershed_key(split_watershed_key);
  region_cmd.mutable_split_request()->set_split_from_region_id(split_from_region_id);
  region_cmd.mutable_split_request()->set_split_to_region_id(split_to_region_id);
  region_cmd.set_create_timestamp(butil::gettimeofday_ms());

  // only send split region_cmd to split_from_region_id's leader store id
  auto leader_store_id = GetRegionLeaderId(split_from_region_id);
  if (leader_store_id == 0) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region_id's "
                        "leader_store_id is 0, split_from_region_id="
                     << split_from_region_id << ", split_to_region_id=" << split_to_region_id;
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region_id's leader_store_id is 0");
  }

  return AddRegionCmd(leader_store_id, 0, region_cmd, meta_increment);
}

butil::Status CoordinatorControl::SplitRegionWithTaskList(int64_t split_from_region_id, int64_t split_to_region_id,
                                                          std::string split_watershed_key, bool store_create_region,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "SplitRegionWithTaskList split_from_region_id=" << split_from_region_id
                  << ", split_to_region_id=" << split_to_region_id
                  << ", split_watershed_key=" << Helper::StringToHex(split_watershed_key)
                  << ", store_create_region=" << store_create_region;

  auto validate_ret = ValidateTaskListConflict(split_from_region_id, split_to_region_id);
  if (!validate_ret.ok()) {
    DINGO_LOG(ERROR) << "SplitRegionWithTaskList validate task list "
                        "conflict failed, split_from_region_id="
                     << split_from_region_id << ", split_to_region_id=" << split_to_region_id;
    return validate_ret;
  }

  if (split_to_region_id > 0) {
    return SplitRegion(split_from_region_id, split_to_region_id, split_watershed_key, meta_increment);
  }

  // validate split_from_region_id
  pb::coordinator_internal::RegionInternal split_from_region;
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
    DINGO_LOG(ERROR) << "SplitRegion split_watershed_key is illegal, "
                        "split_watershed_key = "
                     << Helper::StringToHex(split_watershed_key) << ", split_from_region_id = " << split_from_region_id
                     << " start_key=" << Helper::StringToHex(split_from_region.definition().range().start_key())
                     << ", end_key=" << Helper::StringToHex(split_from_region.definition().range().end_key());
    return butil::Status(pb::error::Errno::EKEY_INVALID, "SplitRegion split_watershed_key is illegal");
  }

  auto region_metrics = GetRegionMetrics(split_from_region_id);

  // validate split_from_region and split_to_region has NORMAL status
  if (split_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      region_metrics.region_status().raft_status() != ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      region_metrics.region_status().heartbeat_status() != ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region is not ready for split, "
                        "split_from_region_id = "
                     << split_from_region_id << " from_state=" << split_from_region.state();
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL, "SplitRegion split_from_region is not ready");
  }

  // check if all peers are healthy
  auto peer_status = CheckRegionAllPeerOnline(split_from_region_id);
  if (!peer_status.ok()) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region is not ready for split, "
                        "split_from_region_id = "
                     << split_from_region_id << " from_state=" << split_from_region.state()
                     << ",  error: " << peer_status.error_str();
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region is not ready for split, error: %s", peer_status.error_cstr());
  }

  // check if vector_index_region has latest vector index epoch equal to region epoch.
  if (split_from_region.region_type() == pb::common::RegionType::INDEX_REGION &&
      split_from_region.definition().index_parameter().has_vector_index_parameter()) {
    auto vector_index_version = region_metrics.vector_index_status().last_build_epoch_version();
    auto region_version = split_from_region.definition().epoch().version();

    if (region_version != vector_index_version) {
      DINGO_LOG(ERROR) << "SplitRegion split_from_region vector index epoch is not equal to region epoch, "
                          "split_from_region_id = "
                       << split_from_region_id << " from_state=" << split_from_region.state()
                       << ", vector_index_version=" << vector_index_version << ", region_version=" << region_version;
      return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                           "SplitRegion split_from_region vector index epoch is not equal to region epoch");
    }
  }

  // only send split region_cmd to split_from_region_id's leader store id
  auto leader_store_id = GetRegionLeaderId(split_from_region_id);
  if (leader_store_id == 0) {
    DINGO_LOG(ERROR) << "SplitRegion split_from_region_id's "
                        "leader_store_id is 0, split_from_region_id="
                     << split_from_region_id << ", split_to_region_id=" << split_to_region_id;
    return butil::Status(pb::error::Errno::ESPLIT_STATUS_ILLEGAL,
                         "SplitRegion split_from_region_id's leader_store_id is 0");
  }

  // call create_region to get store_operations
  pb::coordinator_internal::MetaIncrement meta_increment_tmp;
  int64_t new_region_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION, meta_increment);
  std::vector<pb::coordinator::StoreOperation> store_operations;
  auto status_ret = CreateRegionForSplitInternal(split_from_region_id, new_region_id, store_create_region,
                                                 store_operations, meta_increment_tmp);
  if (!status_ret.ok()) {
    DINGO_LOG(ERROR) << "SplitRegionWithTaskList create region for split "
                        "failed, split_from_region_id="
                     << split_from_region_id << ", split_to_region_id=" << split_to_region_id
                     << ", errcode=" << status_ret.error_code() << ", errmsg=" << status_ret.error_str();
    return status_ret;
  }

  // create task list
  auto* new_task_list = CreateTaskList(meta_increment);

  // check if need to send load vector index to store
  if (split_from_region.region_type() == pb::common::RegionType::INDEX_REGION &&
      split_from_region.definition().index_parameter().has_vector_index_parameter()) {
    const auto& vector_index_status = region_metrics.vector_index_status();

    if (vector_index_status.apply_log_id() > vector_index_status.snapshot_log_id()) {
      DINGO_LOG(INFO) << "SplitRegionWithTaskList vector index region has "
                         "to do snapshot_vector_index, split_from_region_id="
                      << split_from_region_id << ", split_to_region_id=" << split_to_region_id
                      << ", apply_log_id=" << vector_index_status.apply_log_id()
                      << ", snapshot_log_id=" << vector_index_status.snapshot_log_id();

      // save snapshot log first
      // force node to save vector index to speed up hold_vector_index
      for (const auto& peer : split_from_region.definition().peers()) {
        AddSnapshotVectorIndexTask(new_task_list, peer.store_id(), split_from_region_id,
                                   vector_index_status.apply_log_id(), meta_increment);
      }

      // check if snapshot_vector_index is finished
      AddCheckVectorIndexSnapshotLogIdTask(new_task_list, split_from_region_id, vector_index_status.apply_log_id());
    }

    // send load vector index to store
    for (const auto& peer : split_from_region.definition().peers()) {
      AddLoadVectorIndexTask(new_task_list, peer.store_id(), split_from_region_id, meta_increment);
    }

    // check vector index is ready
    for (const auto& peer : split_from_region.definition().peers()) {
      AddCheckStoreVectorIndexTask(new_task_list, peer.store_id(), split_from_region_id,
                                   split_from_region.definition().epoch().version());
    }
  }

  // build create_region task
  auto* create_region_task = new_task_list->add_tasks();
  for (const auto& it : store_operations) {
    auto* new_store_operation = create_region_task->add_store_operations();
    *new_store_operation = it;
  }

  // update region_map for new_region_id
  for (const auto& it : meta_increment_tmp.regions()) {
    auto* new_region = meta_increment.add_regions();
    *new_region = it;
  }

  // fix: now update table/index is done in raft apply, no need to add to
  // meta_increment update table_map for new_region_id CreateRegion will
  // update table_map if region is create for split for (const auto& it :
  // meta_increment_tmp.tables()) {
  //   *(meta_increment.add_tables())=it;
  // }

  // build split_region pre_check for each store region
  for (const auto& it : store_operations) {
    AddCheckStoreRegionTask(new_task_list, it.id(), new_region_id);
  }

  // build split_region task
  AddSplitTask(new_task_list, leader_store_id, split_from_region_id, new_region_id, split_watershed_key,
               store_create_region, meta_increment);

  // check if split_to_region'state change to NORMAL, this state change means split is fininshed.
  AddCheckSplitResultTask(new_task_list, new_region_id);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::MergeRegionWithTaskList(int64_t merge_from_region_id, int64_t merge_to_region_id,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "MergeRegionWithTaskList merge_from_region_id=" << merge_from_region_id
                  << ", merge_to_region_id=" << merge_to_region_id;

  auto validate_ret = ValidateTaskListConflict(merge_from_region_id, merge_to_region_id);
  if (!validate_ret.ok()) {
    DINGO_LOG(ERROR) << "mergeRegionWithTaskList validate task list "
                        "conflict failed, merge_from_region_id="
                     << merge_from_region_id << ", merge_to_region_id=" << merge_to_region_id;
    return validate_ret;
  }

  // validate merge_from_region_id
  pb::coordinator_internal::RegionInternal merge_from_region;
  int ret = region_map_.Get(merge_from_region_id, merge_from_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "MergeRegion from region not exists, id = " << merge_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "MergeRegion from region not exists");
  }

  // validate merge_to_region_id
  pb::coordinator_internal::RegionInternal merge_to_region;
  ret = region_map_.Get(merge_to_region_id, merge_to_region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "MergeRegion to region not exists, id = " << merge_from_region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "MergeRegion to region not exists");
  }

  auto merge_from_region_metrics = GetRegionMetrics(merge_from_region_id);
  auto merge_to_region_metrics = GetRegionMetrics(merge_to_region_id);

  // validate merge_from_region and merge_to_region has NORMAL status
  if (merge_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      merge_from_region_metrics.region_status().raft_status() !=
          ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      merge_from_region_metrics.region_status().heartbeat_status() !=
          ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region is not ready for merge, "
                        "merge_from_region_id = "
                     << merge_from_region_id << " from_state=" << merge_from_region.state();
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL,
                         "MergeRegion merge_from_region is not ready for merge");
  }

  // validate merge_to_region and merge_to_region has NORMAL status
  if (merge_to_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      merge_to_region_metrics.region_status().raft_status() !=
          ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      merge_to_region_metrics.region_status().heartbeat_status() !=
          ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "MergeRegion merge_to_region is not ready for merge, "
                        "merge_to_region_id = "
                     << merge_to_region_id << " from_state=" << merge_to_region.state();
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL, "MergeRegion merge_to_region is not ready for merge");
  }

  // check if all peers are healthy
  auto peer_status = CheckRegionAllPeerOnline(merge_from_region_id);
  if (!peer_status.ok()) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region is not ready for merge, "
                        "merge_from_region_id = "
                     << merge_from_region_id << " from_state=" << merge_from_region.state()
                     << ",  error: " << peer_status.error_str();
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL,
                         "MergeRegion merge_from_region is not ready for merge, error: %s", peer_status.error_cstr());
  }

  peer_status = CheckRegionAllPeerOnline(merge_to_region_id);
  if (!peer_status.ok()) {
    DINGO_LOG(ERROR) << "MergeRegion merge_to_region is not ready for merge, "
                        "merge_to_region_id = "
                     << merge_to_region_id << " from_state=" << merge_to_region.state()
                     << ",  error: " << peer_status.error_str();
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL,
                         "MergeRegion merge_to_region is not ready for merge, error: %s", peer_status.error_cstr());
  }

  // validate merge_from_region_id and merge_to_region_id
  if (merge_from_region_id == merge_to_region_id) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region_id == merge_to_region_id";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "MergeRegion merge_from_region_id == merge_to_region_id");
  }

  // validate region peers
  if (Helper::IsDifferencePeers(merge_from_region.definition(), merge_to_region.definition())) {
    return butil::Status(pb::error::EMERGE_PEER_NOT_MATCH, "Peers is differencce.");
  }

  // validate merge_from_region and merge_to_region status
  if (merge_from_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      merge_to_region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region or "
                        "merge_to_region is not NORMAL";
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL,
                         "MergeRegion merge_from_region or merge_to_region is not NORMAL");
  }

  // validate merge_from_region adn merge_to_region has same part_id
  if (merge_from_region.definition().part_id() != merge_to_region.definition().part_id()) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region and "
                        "merge_to_region has different part_id";
    return butil::Status(pb::error::Errno::EMERGE_PART_NOT_MATCH,
                         "MergeRegion merge_from_region and "
                         "merge_to_region has different part_id");
  }

  // validate merge_from_region and merge_to_region has continuous range
  pb::common::Range new_range;
  bool range_match = false;
  if (merge_from_region.definition().range().start_key() == merge_to_region.definition().range().end_key()) {
    range_match = true;
    new_range.set_start_key(merge_to_region.definition().range().start_key());
    new_range.set_end_key(merge_from_region.definition().range().end_key());
  }
  if (merge_to_region.definition().range().start_key() == merge_from_region.definition().range().end_key()) {
    range_match = true;
    new_range.set_start_key(merge_from_region.definition().range().start_key());
    new_range.set_end_key(merge_to_region.definition().range().end_key());
  }

  if (!range_match) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region and merge_to_region has not continuous range.";
    return butil::Status(pb::error::Errno::EMERGE_RANGE_NOT_MATCH,
                         "MergeRegion merge_from_region and merge_to_region has not continuous range");
  }

  bool merge_from_is_vector_index = false;
  bool merge_to_is_vector_index = false;

  if (merge_from_region.region_type() == pb::common::RegionType::INDEX_REGION &&
      merge_from_region.definition().index_parameter().has_vector_index_parameter()) {
    DINGO_LOG(INFO) << "MergeRegion merge_from_region is vector index region, "
                       "merge_from_region_id="
                    << merge_from_region_id << ", merge_to_region_id=" << merge_to_region_id;
    merge_from_is_vector_index = true;
  }

  if (merge_to_region.region_type() == pb::common::RegionType::INDEX_REGION &&
      merge_to_region.definition().index_parameter().has_vector_index_parameter()) {
    DINGO_LOG(INFO) << "MergeRegion merge_to_region is vector index region, "
                       "merge_from_region_id="
                    << merge_from_region_id << ", merge_to_region_id=" << merge_to_region_id;
    merge_to_is_vector_index = true;
  }

  DINGO_LOG(INFO) << "MergeRegion merge_from_region_id=" << merge_from_region_id
                  << ", merge_to_region_id=" << merge_to_region_id
                  << ", merge_from_is_vector_index=" << merge_from_is_vector_index
                  << ", merge_to_is_vector_index=" << merge_to_is_vector_index;

  if (merge_from_is_vector_index != merge_to_is_vector_index) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region and merge_to_region has different region type.";
    return butil::Status(pb::error::Errno::EMERGE_REGION_TYPE_NOT_MATCH,
                         "MergeRegion merge_from_region and merge_to_region has different region type");
  }

  if (merge_from_is_vector_index) {
    // check if vector index parameter is match
    auto merge_from_vector_index_parameter = merge_from_region.definition().index_parameter().vector_index_parameter();
    auto merge_to_vector_index_parameter = merge_to_region.definition().index_parameter().vector_index_parameter();

    if (merge_from_vector_index_parameter.vector_index_type() != merge_to_vector_index_parameter.vector_index_type()) {
      DINGO_LOG(ERROR) << "MergeRegion merge_from_region and merge_to_region has different vector index type.";
      return butil::Status(pb::error::Errno::EMERGE_VECTOR_INDEX_TYPE_NOT_MATCH,
                           "MergeRegion merge_from_region and merge_to_region has different vector index type");
    }

    auto is_compatiablity = VectorIndexUtils::CheckVectorIndexParameterCompatibility(merge_from_vector_index_parameter,
                                                                                     merge_to_vector_index_parameter);
    if (!is_compatiablity.ok()) {
      DINGO_LOG(ERROR)
          << "MergeRegion merge_from_region and merge_to_region has different vector index parameter. errmsg: "
          << is_compatiablity.error_str();
      return is_compatiablity;
    }

    // check if vector_index_region has latest vector index epoch equal to region epoch.
    if (merge_from_region.region_type() == pb::common::RegionType::INDEX_REGION &&
        merge_from_region.definition().index_parameter().has_vector_index_parameter()) {
      auto vector_index_version = merge_from_region_metrics.vector_index_status().last_build_epoch_version();
      auto region_version = merge_from_region.definition().epoch().version();

      if (region_version != vector_index_version) {
        DINGO_LOG(ERROR) << "MergeRegion merge_from_region vector index epoch is not equal to region epoch, "
                            "merge_from_region_id = "
                         << merge_from_region_id << " from_state=" << merge_from_region.state()
                         << ", vector_index_version=" << vector_index_version << ", region_version=" << region_version;
        return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL,
                             "MergeRegion merge_from_region vector index epoch is not equal to region epoch");
      }
    }
  }

  // only send merge region_cmd to merge_from_region_id's leader store id
  auto leader_store_id = GetRegionLeaderId(merge_from_region_id);
  if (leader_store_id == 0) {
    DINGO_LOG(ERROR) << "MergeRegion merge_from_region_id's "
                        "leader_store_id is 0, merge_from_region_id="
                     << merge_from_region_id << ", merge_to_region_id=" << merge_to_region_id;
    return butil::Status(pb::error::Errno::EMERGE_STATUS_ILLEGAL,
                         "MergeRegion merge_from_region_id's leader_store_id is 0");
  }

  // build task list
  auto* new_task_list = CreateTaskList(meta_increment);

  // check if need to send load vector index to store
  if (merge_from_is_vector_index) {
    DINGO_LOG(INFO) << "MergeRegion merge_from_region is vector index region, "
                       "merge_from_region_id="
                    << merge_from_region_id << ", merge_to_region_id=" << merge_to_region_id;

    // send load vector index to store
    for (const auto& peer : merge_from_region.definition().peers()) {
      AddLoadVectorIndexTask(new_task_list, peer.store_id(), merge_from_region_id, meta_increment);
    }

    // check vector index is ready
    for (const auto& peer : merge_from_region.definition().peers()) {
      AddCheckStoreVectorIndexTask(new_task_list, peer.store_id(), merge_from_region_id,
                                   merge_from_region.definition().epoch().version());
    }
  }

  // build merge task
  AddMergeTask(new_task_list, leader_store_id, merge_from_region_id, merge_to_region_id, meta_increment);

  // check if all merge_from_region is updated to TOMBSTONE
  for (const auto& peer : merge_from_region.definition().peers()) {
    AddCheckTombstoneRegionTask(new_task_list, peer.store_id(), merge_from_region_id);
  }

  // build drop region task
  auto* drop_region_task = new_task_list->add_tasks();
  auto* task_pre_check_drop = drop_region_task->mutable_pre_check();
  task_pre_check_drop->set_type(pb::coordinator::TaskPreCheckType::REGION_CHECK);
  auto* region_check = task_pre_check_drop->mutable_region_check();
  region_check->set_region_id(merge_from_region_id);
  region_check->set_state(::dingodb::pb::common::RegionState::REGION_TOMBSTONE);
  auto* coordinator_operation = drop_region_task->add_coordinator_operations();
  coordinator_operation->set_coordinator_op_type(pb::coordinator::COORDINATOR_OP_TYPE_DROP_REGION);
  coordinator_operation->mutable_drop_region_operation()->set_region_id(merge_from_region_id);

  // check if merge_to_region'state change to NORMAL, this state change means split is fininshed.
  AddCheckMergeResultTask(new_task_list, merge_to_region_id, new_range);

  return butil::Status::OK();
}

// ChangePeerRegionWithTaskList
butil::Status CoordinatorControl::ChangePeerRegionWithTaskList(
    int64_t region_id, std::vector<int64_t>& new_store_ids, pb::coordinator_internal::MetaIncrement& meta_increment) {
  auto validate_ret = ValidateTaskListConflict(region_id, region_id);
  if (!validate_ret.ok()) {
    DINGO_LOG(ERROR) << "ChangePeerRegionWithTaskList validate task list "
                        "conflict failed, change_peer_region_id="
                     << region_id;
    return validate_ret;
  }

  // validate region_id
  pb::coordinator_internal::RegionInternal region;
  int ret = region_map_.Get(region_id, region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ChangePeerRegion region not exists, id = " << region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "ChangePeerRegion region not exists");
  }

  // validate region has NORMAL status
  auto region_status = GetRegionStatus(region_id);
  if (region.state() != ::dingodb::pb::common::RegionState::REGION_NORMAL ||
      region_status.raft_status() != ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY ||
      region_status.heartbeat_status() != ::dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                     << ", region is not ready for "
                        "change_peer, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                         "ChangePeerRegion region is not ready for change_peer");
  }

  // check if region leader is healthy
  auto leader_store_status = CheckRegionLeaderOnline(region_id);
  if (!leader_store_status.ok()) {
    DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                     << ", region leader is not ready for "
                        "change_peer, region_id = "
                     << region_id << ", error: " << leader_store_status.error_str();
    return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                         "ChangePeerRegion region leader is not ready for change_peer, error: %s",
                         leader_store_status.error_cstr());
  }

  // validate new_store_ids
  if (new_store_ids.size() != (region.definition().peers_size() + 1) &&
      new_store_ids.size() != (region.definition().peers_size() - 1) && (!new_store_ids.empty())) {
    DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                     << ", new_store_ids size not match, region_id = " << region_id
                     << " old_size = " << region.definition().peers_size() << " new_size = " << new_store_ids.size();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "ChangePeerRegion new_store_ids size not match");
  }

  // validate new_store_ids only has one new store or only less one store
  std::vector<int64_t> old_store_ids;
  old_store_ids.reserve(region.definition().peers_size());
  for (int i = 0; i < region.definition().peers_size(); i++) {
    old_store_ids.push_back(region.definition().peers(i).store_id());
  }

  if (old_store_ids.empty()) {
    DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                     << ", old_store_ids is empty, region_id = " << region_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "ChangePeerRegion old_store_ids is empty");
  }

  std::vector<int64_t> new_store_ids_diff_more;
  std::vector<int64_t> new_store_ids_diff_less;

  std::sort(new_store_ids.begin(), new_store_ids.end());
  std::sort(old_store_ids.begin(), old_store_ids.end());

  std::set_difference(new_store_ids.begin(), new_store_ids.end(), old_store_ids.begin(), old_store_ids.end(),
                      std::inserter(new_store_ids_diff_more, new_store_ids_diff_more.begin()));
  std::set_difference(old_store_ids.begin(), old_store_ids.end(), new_store_ids.begin(), new_store_ids.end(),
                      std::inserter(new_store_ids_diff_less, new_store_ids_diff_less.begin()));

  if (new_store_ids_diff_more.size() + new_store_ids_diff_less.size() != 1) {
    DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                     << ", new_store_ids can only has one "
                        "diff store, region_id = "
                     << region_id << " new_store_ids_diff_more.size() = " << new_store_ids_diff_more.size()
                     << " new_store_ids_diff_less.size() = " << new_store_ids_diff_less.size();
    for (auto it : new_store_ids_diff_more) DINGO_LOG(ERROR) << "new_store_ids_diff_more = " << it;
    for (auto it : new_store_ids_diff_less) DINGO_LOG(ERROR) << "new_store_ids_diff_less = " << it;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "ChangePeerRegion new_store_ids can only has one diff store");
  }

  if (new_store_ids_diff_more.size() == 1) {
    auto store_status = CheckStoreNormal(new_store_ids_diff_more.at(0));
    if (!store_status.ok()) {
      DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                       << ", new_store_ids_diff_more store is not normal, region_id = " << region_id
                       << " store_id = " << new_store_ids_diff_more.at(0);
      return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                           "ChangePeerRegion new_store_ids_diff_more store is not normal, %s",
                           store_status.error_cstr());
    }
  }

  // for region with epoch > 1, check if all peer has eligible snapshot (snapshot's epoch version is equal to region
  if (region.definition().epoch().version() > 1) {
    for (const auto& store_id : old_store_ids) {
      BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
      if (store_region_metrics_map_.find(store_id) == store_region_metrics_map_.end()) {
        DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                         << ", store_metrics_map find failed, store_id = " << store_id;
        return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "ChangePeerRegion store_metrics_map find failed");
      }

      auto it = store_region_metrics_map_[store_id].region_metrics_map().find(region_id);
      if (it == store_region_metrics_map_[store_id].region_metrics_map().end()) {
        DINGO_LOG(ERROR) << "ChangePeerRegion region_metrics_map seek failed, region_id = " << region_id;
        return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "ChangePeerRegion region_metrics_map seek failed");
      }

      const auto& region_metrics = it->second;
      DINGO_LOG(INFO) << "ChangePeerRegion, region_id=" << region_id
                      << ", region_metrics.epoch_version() = " << region_metrics.region_definition().epoch().version()
                      << ", region.epoch_version() = " << region.definition().epoch().version()
                      << " snapshot.epoch_version() = " << region_metrics.snapshot_epoch_version();

      if (region_metrics.snapshot_epoch_version() < region.definition().epoch().version()) {
        DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                         << ", region_metrics.snapshot_epoch_version() < "
                            "region.definition().epoch().version(), region_id = "
                         << region_id << " snapshot_epoch_version = " << region_metrics.snapshot_epoch_version()
                         << " region.epoch_version() = " << region.definition().epoch().version();
        return butil::Status(pb::error::Errno::EREGION_SNAPSHOT_EPOCH_NOT_MATCH,
                             "ChangePeerRegion region_metrics.snapshot_epoch_version() < "
                             "region.definition().epoch().version()");
      }
    }
  }

  // this is the new definition of region
  pb::common::RegionDefinition new_region_definition;
  new_region_definition = region.definition();
  new_region_definition.clear_peers();

  // generate store operation for stores
  auto leader_store_id = GetRegionLeaderId(region_id);
  if (new_store_ids_diff_less.size() == 1) {
    if (leader_store_id == new_store_ids_diff_less.at(0) && new_store_ids.size() < 3) {
      DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                       << ", region.leader_store_id() == "
                          "new_store_ids_diff_less.at(0), region_id = "
                       << region_id << " new_store_ids.size() = " << new_store_ids.size();
      return butil::Status(pb::error::Errno::ECHANGE_PEER_UNABLE_TO_REMOVE_LEADER,
                           "ChangePeerRegion region.leader_store_id() == "
                           "new_store_ids_diff_less.at(0) and new_store_ids.size() < 3");
    }

    if (leader_store_id == 0) {
      DINGO_LOG(ERROR) << "ChangePeerRegion, region_id=" << region_id
                       << ", region.leader_store_id() == "
                          "0, region_id = "
                       << region_id;
      return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                           "ChangePeerRegion region.leader_store_id() == 0");
    }

    // calculate new peers
    for (int i = 0; i < region.definition().peers_size(); i++) {
      if (region.definition().peers(i).store_id() != new_store_ids_diff_less.at(0)) {
        auto* peer = new_region_definition.add_peers();
        *peer = region.definition().peers(i);
      }
    }

    // build new task_list
    auto* increment_task_list = CreateTaskList(meta_increment);

    // this is change_peer task
    AddChangePeerTask(increment_task_list, leader_store_id, region_id, new_region_definition, meta_increment);

    // this is delete_region task
    AddDeleteTaskWithCheck(increment_task_list, new_store_ids_diff_less.at(0), region_id, new_region_definition.peers(),
                           meta_increment);

    AddCheckChangePeerResultTask(increment_task_list, region_id, new_region_definition);

    // this is purge_region task
    // AddPurgeTask(increment_task_list, new_store_ids_diff_less.at(0),
    // region_id, meta_increment);

  } else if (new_store_ids_diff_more.size() == 1) {
    // expand region
    // calculate new peers
    // validate new_store_ids_diff_more is legal
    pb::common::Store store_to_add_peer;
    int ret = store_map_.Get(new_store_ids_diff_more.at(0), store_to_add_peer);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids_diff_more not "
                          "exists, region_id = "
                       << region_id;
      return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "ChangePeerRegion new_store_ids_diff_more not exists");
    }

    pb::common::Store old_store_to_check;
    ret = store_map_.Get(old_store_ids.at(0), old_store_to_check);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ChangePeerRegion old_store_ids not exists, region_id = " << region_id;
      return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "ChangePeerRegion old_store_ids not exists");
    }

    // check if new store and old store has same type
    if (old_store_to_check.store_type() != store_to_add_peer.store_type()) {
      DINGO_LOG(ERROR)
          << "ChangePeerRegion old_store_ids and new_store_ids_diff_more has different store_type, region_id = "
          << region_id << ", old_store_to_check.store_type() = " << old_store_to_check.store_type()
          << ", new_store_to_add_peer.store_type() = " << store_to_add_peer.store_type();
      return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND,
                           "ChangePeerRegion old_store_ids and new_store_ids_diff_more has different store_type");
    }

    // generate new peer from store
    auto* peer = new_region_definition.add_peers();
    peer->set_store_id(store_to_add_peer.id());
    peer->set_role(::dingodb::pb::common::PeerRole::VOTER);
    *(peer->mutable_server_location()) = store_to_add_peer.server_location();
    *(peer->mutable_raft_location()) = store_to_add_peer.raft_location();

    // add old peer to new_region_definition
    for (int i = 0; i < region.definition().peers_size(); i++) {
      auto* peer = new_region_definition.add_peers();
      *peer = region.definition().peers(i);
    }

    auto leader_store_id = GetRegionLeaderId(region_id);
    if (leader_store_id == 0) {
      DINGO_LOG(ERROR) << "ChangePeerRegion region.leader_store_id() == "
                          "0, region_id = "
                       << region_id;
      return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                           "ChangePeerRegion region.leader_store_id() == 0");
    }

    // build new task_list
    auto* increment_task_list = CreateTaskList(meta_increment);

    // this create region task
    AddCreateTask(increment_task_list, new_store_ids_diff_more.at(0), region_id, new_region_definition, meta_increment);

    // this change peer check task, no store_operation, only for check
    // auto* change_peer_check_task = increment_task_list->add_tasks();
    // auto* region_check = change_peer_check_task->mutable_pre_check();
    // region_check->set_type(pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK);
    // region_check->mutable_store_region_check()->set_store_id(new_store_ids_diff_more.at(0));
    // region_check->mutable_store_region_check()->set_region_id(region_id);
    AddCheckStoreRegionTask(increment_task_list, new_store_ids_diff_more.at(0), region_id);

    // this is change peer task
    AddChangePeerTask(increment_task_list, leader_store_id, region_id, new_region_definition, meta_increment);

    AddCheckChangePeerResultTask(increment_task_list, region_id, new_region_definition);

  } else {
    DINGO_LOG(ERROR) << "ChangePeerRegion new_store_ids not match, region_id = " << region_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "ChangePeerRegion new_store_ids not match");
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::TransferLeaderRegionWithTaskList(
    int64_t region_id, int64_t new_leader_store_id, pb::coordinator_internal::MetaIncrement& meta_increment) {
  // check region_id exists
  pb::coordinator_internal::RegionInternal region;
  int ret = region_map_.Get(region_id, region);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion region_id not exists, region_id = " << region_id;
    return butil::Status(pb::error::Errno::EREGION_NOT_FOUND, "TransferLeaderRegion region_id not exists");
  }

  if (region.state() != pb::common::RegionState::REGION_NORMAL) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion region.state() != "
                        "REGION_NORMAL, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::EREGION_STATE, "TransferLeaderRegion region.state() != REGION_NORMAL");
  }

  auto region_status = GetRegionStatus(region_id);
  if (region_status.heartbeat_status() != pb::common::RegionHeartbeatState::REGION_ONLINE) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion region.heartbeat_state() "
                        "!= REGION_ONLINE, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::EREGION_STATE,
                         "TransferLeaderRegion region.heartbeat_state() != REGION_ONLINE");
  }

  auto leader_store_id = GetRegionLeaderId(region_id);
  if (leader_store_id == new_leader_store_id) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion new_leader_store_id == "
                        "old_leader_store_id, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "TransferLeaderRegion new_leader_store_id == "
                         "old_leader_store_id");
  }

  if (leader_store_id == 0) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion region.leader_store_id() "
                        "== 0, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::ECHANGE_PEER_STATUS_ILLEGAL,
                         "TransferLeaderRegion region.leader_store_id() == 0");
  }

  // check new_leader_store_id exists
  pb::common::Store store_to_transfer_leader;
  ret = store_map_.Get(new_leader_store_id, store_to_transfer_leader);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion new_leader_store_id not "
                        "exists, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "TransferLeaderRegion new_leader_store_id not exists");
  }

  if (store_to_transfer_leader.state() != pb::common::StoreState::STORE_NORMAL) {
    DINGO_LOG(ERROR) << "TransferLeaderRegion new_leader_store_id not "
                        "running, region_id = "
                     << region_id;
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
    DINGO_LOG(ERROR) << "TransferLeaderRegion new_leader_store_id not in "
                        "region, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "TransferLeaderRegion new_leader_store_id not in region");
  }

  // build new task_list
  auto* increment_task_list = CreateTaskList(meta_increment);

  // this transfer leader task
  AddTransferLeaderTask(increment_task_list, leader_store_id, region_id, new_leader_peer, meta_increment);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::ValidateTaskListConflict(int64_t region_id, int64_t second_region_id) {
  // check task_list conflict
  butil::FlatMap<int64_t, pb::coordinator::TaskList> task_list_map_temp;
  task_list_map_temp.init(1000);
  int ret = task_list_map_.GetRawMapCopy(task_list_map_temp);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ValidateTaskListConflict task_list_map_.GetRawMapCopy "
                        "failed, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::EINTERNAL, "ValidateTaskListConflict task_list_map_.GetRawMapCopy failed");
  }

  for (const auto& task_list : task_list_map_temp) {
    for (const auto& task : task_list.second.tasks()) {
      for (const auto& store_operation : task.store_operations()) {
        for (const auto& region_cmd : store_operation.region_cmds()) {
          bool is_conflict = false;
          if (region_cmd.region_id() == region_id || region_cmd.region_id() == second_region_id) {
            is_conflict = true;
          } else if (region_cmd.region_cmd_type() == pb::coordinator::CMD_MERGE) {
            if (region_cmd.merge_request().source_region_id() == region_id ||
                region_cmd.merge_request().source_region_id() == second_region_id ||
                region_cmd.merge_request().target_region_id() == region_id ||
                region_cmd.merge_request().target_region_id() == second_region_id) {
              is_conflict = true;
            }
          } else if (region_cmd.region_cmd_type() == pb::coordinator::CMD_SPLIT) {
            if (region_cmd.split_request().split_from_region_id() == region_id ||
                region_cmd.split_request().split_from_region_id() == second_region_id ||
                region_cmd.split_request().split_to_region_id() == region_id ||
                region_cmd.split_request().split_to_region_id() == second_region_id) {
              is_conflict = true;
            }
          }

          if (is_conflict) {
            std::string s = fmt::format("ValidateTaskListConflict task_list conflict, region_id = {}", region_id);
            DINGO_LOG(ERROR) << s;
            return butil::Status(pb::error::Errno::ETASK_LIST_CONFLICT, s);
          }
        }
      }
    }
  }

  // check store operation conflict
  butil::FlatMap<int64_t, pb::coordinator_internal::StoreOperationInternal> store_operation_map_temp;
  store_operation_map_temp.init(1000);
  ret = store_operation_map_.GetRawMapCopy(store_operation_map_temp);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ValidateTaskListConflict store_operation_map_.GetRawMapCopy "
                        "failed, region_id = "
                     << region_id;
    return butil::Status(pb::error::Errno::EINTERNAL,
                         "ValidateTaskListConflict store_operation_map_.GetRawMapCopy "
                         "failed, region_id = " +
                             std::to_string(region_id));
  }

  for (const auto& store_operation : store_operation_map_temp) {
    for (auto region_cmd_id : store_operation.second.region_cmd_ids()) {
      pb::coordinator_internal::RegionCmdInternal region_cmd_internal;
      auto ret = region_cmd_map_.Get(region_cmd_id, region_cmd_internal);
      if (ret < 0) {
        continue;
      }
      const auto& region_cmd = region_cmd_internal.region_cmd();

      bool is_conflict = false;
      if (region_cmd.region_id() == region_id || region_cmd.region_id() == second_region_id) {
        is_conflict = true;
      } else if (region_cmd.region_cmd_type() == pb::coordinator::CMD_MERGE) {
        if (region_cmd.merge_request().source_region_id() == region_id ||
            region_cmd.merge_request().source_region_id() == second_region_id ||
            region_cmd.merge_request().target_region_id() == region_id ||
            region_cmd.merge_request().target_region_id() == second_region_id) {
          is_conflict = true;
        }
      } else if (region_cmd.region_cmd_type() == pb::coordinator::CMD_SPLIT) {
        if (region_cmd.split_request().split_from_region_id() == region_id ||
            region_cmd.split_request().split_from_region_id() == second_region_id ||
            region_cmd.split_request().split_to_region_id() == region_id ||
            region_cmd.split_request().split_to_region_id() == second_region_id) {
          is_conflict = true;
        }
      }

      if (is_conflict) {
        std::string s = fmt::format("ValidateTaskListConflict store_operation conflict, region_id = {}", region_id);
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::ESTORE_OPERATION_CONFLICT, s);
      }
    }
  }

  return butil::Status::OK();
}

// CleanStoreOperation
butil::Status CoordinatorControl::CleanStoreOperation(int64_t store_id,
                                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::coordinator_internal::StoreOperationInternal store_operation;
  int ret = store_operation_map_.Get(store_id, store_operation);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "CleanStoreOperation store operation not exists, store_id = " << store_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "CleanStoreOperation store operation not exists");
  }

  // clean store operation
  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(store_id);
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  *(store_operation_increment->mutable_store_operation()) = store_operation;

  // clean region_cmd
  for (auto region_cmd_id : store_operation.region_cmd_ids()) {
    auto* region_cmd_increment = meta_increment.add_region_cmds();
    region_cmd_increment->set_id(region_cmd_id);
    region_cmd_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
    region_cmd_increment->mutable_region_cmd()->set_id(region_cmd_id);
  }

  return butil::Status::OK();
}

// MoveRegionCmd
// move region_cmd from one store to another store
butil::Status CoordinatorControl::MoveRegionCmd(int64_t old_store_id, int64_t new_store_id, int64_t region_cmd_id,
                                                pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate old_store_id
  int ret = store_map_.Exists(old_store_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "MoveRegionCmd store not exists, old_store_id = " << old_store_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "MoveRegionCmd old_store_id not exists");
  }

  // validate new_store_id
  ret = store_map_.Exists(new_store_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "MoveRegionCmd store not exists, new_store_id = " << new_store_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "MoveRegionCmd new_store_id not exists");
  }

  // validate region_cmd_id
  if (region_cmd_id <= 0) {
    DINGO_LOG(ERROR) << "MoveRegionCmd region_cmd_id <= 0, region_cmd_id = " << region_cmd_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "MoveRegionCmd region_cmd_id <= 0");
  }

  // validate region_cmd_id exists in region_cmd_map_
  ret = region_cmd_map_.Exists(region_cmd_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "MoveRegionCmd region_cmd_id not exists, region_cmd_id = " << region_cmd_id;
    return butil::Status(pb::error::Errno::EREGION_CMD_NOT_FOUND, "MoveRegionCmd region_cmd_id not exists");
  }

  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(old_store_id);  // this store id
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::MODIFY);
  auto* store_operation = store_operation_increment->mutable_store_operation();
  store_operation->set_id(old_store_id);
  auto* move_region_cmd = store_operation_increment->mutable_move_region_cmd();
  move_region_cmd->set_region_cmd_id(region_cmd_id);
  move_region_cmd->set_from_store_id(old_store_id);
  move_region_cmd->set_to_store_id(new_store_id);

  return butil::Status::OK();
}

// AddRegionCmd
butil::Status CoordinatorControl::AddRegionCmd(int64_t store_id, int64_t job_id,
                                               const pb::coordinator::RegionCmd& region_cmd,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate store id
  int ret = store_map_.Exists(store_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "AddRegionCmd store not exists, store_id = " << store_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "AddRegionCmd store not exists");
  }

  int64_t region_cmd_id = region_cmd.id();
  if (region_cmd_id <= 0) {
    region_cmd_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment);
    if (region_cmd_id == 0) {
      DINGO_LOG(ERROR) << "AddRegionCmd GetNextId failed, store_id = " << store_id;
      return butil::Status(pb::error::Errno::EINTERNAL, "AddRegionCmd GetNextId failed");
    }
  }

  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(store_id);  // this store id
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  auto* store_operation = store_operation_increment->mutable_store_operation();
  store_operation->set_id(store_id);
  store_operation->add_region_cmd_ids(region_cmd_id);

  auto* region_cmd_increment = meta_increment.add_region_cmds();
  region_cmd_increment->set_id(region_cmd_id);
  region_cmd_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  region_cmd_increment->mutable_region_cmd()->set_id(region_cmd_id);
  auto* cmd_tmp = region_cmd_increment->mutable_region_cmd()->mutable_region_cmd();
  *cmd_tmp = region_cmd;
  cmd_tmp->set_id(region_cmd_id);
  cmd_tmp->set_job_id(job_id);
  cmd_tmp->set_store_id(store_id);

  return butil::Status::OK();
}

// UpdateRegionCmd
butil::Status CoordinatorControl::UpdateRegionCmd(int64_t store_id, const pb::coordinator::RegionCmd& region_cmd,
                                                  const pb::error::Error& error,
                                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate store id
  int ret = store_map_.Exists(store_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "UpdateRegionCmd store not exists, store_id = " << store_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "UpdateRegionCmd store not exists");
  }

  if (region_cmd.id() == 0) {
    DINGO_LOG(ERROR) << "UpdateRegionCmd GetNextId failed, store_id = " << store_id;
    return butil::Status(pb::error::Errno::EINTERNAL, "UpdateRegionCmd GetNextId failed");
  }

  auto* region_cmd_increment = meta_increment.add_region_cmds();
  region_cmd_increment->set_id(region_cmd.id());
  region_cmd_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  region_cmd_increment->mutable_region_cmd()->set_id(region_cmd.id());
  auto* cmd_tmp = region_cmd_increment->mutable_region_cmd()->mutable_region_cmd();
  *cmd_tmp = region_cmd;
  *(region_cmd_increment->mutable_region_cmd()->mutable_error()) = error;

  return butil::Status::OK();
}

// RemoveRegionCmd
butil::Status CoordinatorControl::RemoveRegionCmd(int64_t store_id, int64_t region_cmd_id,
                                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate store id
  int ret = store_map_.Exists(store_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "RemoveRegionCmd store not exists, store_id = " << store_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "RemoveRegionCmd store not exists");
  }

  // validate region_cmd_id
  ret = region_cmd_map_.Exists(region_cmd_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "RemoveRegionCmd region_cmd_id not exists, region_cmd_id = " << region_cmd_id;
    return butil::Status(pb::error::Errno::EREGION_CMD_NOT_FOUND, "RemoveRegionCmd region_cmd_id not exists");
  }

  auto* store_operation_increment = meta_increment.add_store_operations();
  store_operation_increment->set_id(store_id);  // this store id
  store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  auto* store_operation = store_operation_increment->mutable_store_operation();
  store_operation->set_id(store_id);
  store_operation->add_region_cmd_ids(region_cmd_id);

  auto* region_cmd_increment = meta_increment.add_region_cmds();
  region_cmd_increment->set_id(region_cmd_id);
  region_cmd_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  region_cmd_increment->mutable_region_cmd()->set_id(region_cmd_id);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetRegionCmd(int64_t store_id, int64_t start_region_cmd_id, int64_t end_region_cmd_id,
                                               std::vector<pb::coordinator::RegionCmd>& region_cmds,
                                               std::vector<pb::error::Error>& region_cmd_errors) {
  // validate store id
  int ret = store_map_.Exists(store_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "GetRegionCmd store not exists, store_id = " << store_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "GetRegionCmd store not exists");
  }

  // validate region_cmd_id
  if (end_region_cmd_id < start_region_cmd_id) {
    DINGO_LOG(ERROR) << "GetRegionCmd end_region_cmd_id < "
                        "start_region_cmd_id, end_region_cmd_id = "
                     << end_region_cmd_id << " start_region_cmd_id = " << start_region_cmd_id;
    return butil::Status(pb::error::Errno::EREGION_CMD_NOT_FOUND,
                         "GetRegionCmd end_region_cmd_id < start_region_cmd_id");
  }

  std::vector<int64_t> region_cmd_ids;
  region_cmd_map_.GetAllKeys(region_cmd_ids);

  if (region_cmd_ids.empty()) {
    DINGO_LOG(INFO) << "GetRegionCmd region_cmd_ids is empty, store_id: " << store_id;
    return butil::Status::OK();
  }

  for (auto region_cmd_id : region_cmd_ids) {
    if (region_cmd_id >= start_region_cmd_id && region_cmd_id < end_region_cmd_id) {
      pb::coordinator_internal::RegionCmdInternal region_cmd_internal;
      int ret = region_cmd_map_.Get(region_cmd_id, region_cmd_internal);
      if (ret < 0) {
        DINGO_LOG(ERROR) << "GetRegionCmd region_cmd_id not exists, region_cmd_id = " << region_cmd_id;
        return butil::Status(pb::error::Errno::EREGION_CMD_NOT_FOUND, "GetRegionCmd region_cmd_id not exists");
      }

      region_cmds.push_back(region_cmd_internal.region_cmd());
      region_cmd_errors.push_back(region_cmd_internal.error());
    }
  }

  return butil::Status::OK();
}

// AddStoreOperation
butil::Status CoordinatorControl::AddStoreOperation(const pb::coordinator::StoreOperation& store_operation,
                                                    bool check_conflict,
                                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate store id
  int64_t store_id = store_operation.id();
  int ret = store_map_.Exists(store_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "AddStoreOperation store not exists, store_id = " << store_id;
    return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "AddStoreOperation store not exists");
  }

  // validate store operation region_cmd
  if (store_operation.region_cmds_size() == 0) {
    DINGO_LOG(ERROR) << "AddStoreOperation store operation region cmd "
                        "empty, store_id = "
                     << store_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "AddStoreOperation store operation region cmd empty");
  }

  // check if region is has ongoing region_cmd
  if (check_conflict) {
    pb::coordinator_internal::StoreOperationInternal store_operation_ongoing;
    ret = store_operation_map_.Get(store_id, store_operation_ongoing);
    if (ret > 0) {
      for (const auto& it : store_operation.region_cmds()) {
        for (const auto& region_cmd_id : store_operation_ongoing.region_cmd_ids()) {
          pb::coordinator_internal::RegionCmdInternal region_cmd;
          ret = region_cmd_map_.Get(region_cmd_id, region_cmd);
          if (ret < 0) {
            continue;
          }

          if (it.region_id() == region_cmd.region_cmd().region_id()) {
            DINGO_LOG(ERROR) << "AddStoreOperation store operation region cmd ongoing "
                                "conflict, unable to add new "
                                "region_cmd, store_id = "
                             << store_id << ", region_id = " << it.region_id();
            return butil::Status(pb::error::Errno::EREGION_CMD_ONGOING_CONFLICT,
                                 "AddStoreOperation store operation region cmd ongoing "
                                 "conflict");
          }
        }
      }
    }
  }

  // add store operation
  for (const auto& region_cmd : store_operation.region_cmds()) {
    auto ret = AddRegionCmd(store_id, 0, region_cmd, meta_increment);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "AddStoreOperation AddRegionCmd failed, store_id = " << store_id;
      return ret;
    }
  }
  return butil::Status::OK();
}

void CoordinatorControl::GetExecutorMap(pb::common::ExecutorMap& executor_map) {
  int64_t executor_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR);
  executor_map.set_epoch(executor_map_epoch);
  {
    std::map<std::string, pb::common::Executor> executor_map_copy;
    executor_map_.GetRawMapCopy(executor_map_copy);
    for (auto& element : executor_map_copy) {
      auto* tmp_region = executor_map.add_executors();
      *tmp_region = element.second;
    }
  }
}

butil::Status CoordinatorControl::GetExecutorUserMap(int64_t cluster_id,
                                                     pb::common::ExecutorUserMap& executor_user_map) {
  if (cluster_id < 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id < 0");
  }

  {
    std::map<std::string, pb::coordinator_internal::ExecutorUserInternal> executor_user_map_copy;
    executor_user_map_.GetRawMapCopy(executor_user_map_copy);
    for (auto& element : executor_user_map_copy) {
      auto* tmp_region = executor_user_map.add_executor_users();
      tmp_region->set_user(element.second.id());
      tmp_region->set_keyring(element.second.keyring());
    }
  }
  return butil::Status::OK();
}

bool CoordinatorControl::ValidateExecutorUser(const pb::common::ExecutorUser& executor_user) {
  if (executor_user.keyring() == std::string("TO_BE_CONTINUED")) {
    DINGO_LOG(DEBUG) << "ValidateExecutorUser debug pass with TO_BE_CONTINUED";
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

butil::Status CoordinatorControl::CreateExecutor(int64_t cluster_id, pb::common::Executor& executor,
                                                 pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id <= 0");
  }

  pb::common::Executor executor_to_create;
  executor_to_create = executor;

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

  executor = executor_to_create;

  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  auto* executor_increment = meta_increment.add_executors();
  executor_increment->set_id(executor_to_create.id());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

  auto* executor_increment_executor = executor_increment->mutable_executor();
  *executor_increment_executor = executor_to_create;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DeleteExecutor(int64_t cluster_id, const pb::common::Executor& executor,
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
  *executor_increment_executor = executor_to_delete;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CreateExecutorUser(int64_t cluster_id, pb::common::ExecutorUser& executor_user,
                                                     pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id <= 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cluster_id <= 0");
  }

  if (executor_user_map_.Exists(executor_user.user())) {
    DINGO_LOG(INFO) << "CreateExecutorUser user already exists, user=" << executor_user.user();
    return butil::Status(pb::error::Errno::EUSER_ALREADY_EXIST, "user already exists");
  }

  // executor_id =
  // GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR,
  // meta_increment);
  if (executor_user.keyring().length() <= 0) {
    executor_user.set_keyring(Helper::GenerateRandomString(16));
  }

  // update meta_increment
  // GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR,
  // meta_increment);
  auto* executor_increment = meta_increment.add_executor_users();
  executor_increment->set_id(executor_user.user());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  executor_increment->mutable_executor_user()->set_id(executor_user.user());
  executor_increment->mutable_executor_user()->set_keyring(executor_user.keyring());

  return butil::Status::OK();
}

butil::Status CoordinatorControl::UpdateExecutorUser(int64_t cluster_id, const pb::common::ExecutorUser& executor_user,
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

  // executor_id =
  // GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR,
  // meta_increment);

  // update meta_increment
  // GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR,
  // meta_increment);
  auto* executor_increment = meta_increment.add_executor_users();
  executor_increment->set_id(executor_user.user());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  executor_increment->mutable_executor_user()->set_id(executor_user.user());
  executor_increment->mutable_executor_user()->set_keyring(executor_user_update.keyring());

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DeleteExecutorUser(int64_t cluster_id, pb::common::ExecutorUser& executor_user,
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

  // executor_id =
  // GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR,
  // meta_increment);

  // update meta_increment
  // GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR,
  // meta_increment);
  auto* executor_increment = meta_increment.add_executor_users();
  executor_increment->set_id(executor_user.user());
  executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  executor_increment->mutable_executor_user()->set_id(executor_user.user());
  executor_increment->mutable_executor_user()->set_keyring(executor_user.keyring());

  return butil::Status::OK();
}

// UpdateExecutorMap
int64_t CoordinatorControl::UpdateExecutorMap(const pb::common::Executor& executor,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  int64_t executor_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR);

  bool need_update_epoch = false;
  {
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
        *executor_increment_executor = executor_to_update;  // only update server_location &
                                                            // raft_location & state

        // only update server_location & raft_location & state &
        // last_seen_timestamp
        *(executor_increment_executor->mutable_server_location()) = executor.server_location();
        executor_increment_executor->set_state(pb::common::ExecutorState::EXECUTOR_NORMAL);
        executor_increment_executor->set_last_seen_timestamp(butil::gettimeofday_ms());
        executor_increment_executor->set_create_timestamp(butil::gettimeofday_ms());
      } else {
        // this is normall heartbeat,
        // so only need to update state & last_seen_timestamp, no need to
        // update epoch
        // and no need to write into raft
        // auto* executor_increment = meta_increment.add_executors();
        // executor_increment->set_id(executor.id());
        // executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

        // auto* executor_increment_executor = executor_increment->mutable_executor();
        // *executor_increment_executor = executor_to_update;  // only update server_location &
        //                                                     // raft_location & state

        // // only update state & last_seen_timestamp
        // executor_increment_executor->set_state(pb::common::ExecutorState::EXECUTOR_NORMAL);
        // executor_increment_executor->set_last_seen_timestamp(butil::gettimeofday_ms());

        *executor_to_update.mutable_server_location() = executor.server_location();
        *executor_to_update.mutable_executor_user() = executor.executor_user();
        *executor_to_update.mutable_resource_tag() = executor.resource_tag();
        executor_to_update.set_state(pb::common::ExecutorState::EXECUTOR_NORMAL);
        executor_to_update.set_last_seen_timestamp(butil::gettimeofday_ms());
        executor_map_.Put(executor.id(), executor_to_update);
      }
    } else {
      // this is a special new executor's first heartbeat
      // only executor using keyring=TO_BE_CONTINUED can get into this
      // branch so we just add this executor into executor_map_
      DINGO_LOG(INFO) << "NEED ADD NEW executor executor_id = " << executor.id();

      // update meta_increment
      need_update_epoch = true;
      auto* executor_increment = meta_increment.add_executors();
      executor_increment->set_id(executor.id());
      executor_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

      auto* executor_increment_executor = executor_increment->mutable_executor();
      *executor_increment_executor = executor;
      executor_increment_executor->set_state(pb::common::ExecutorState::EXECUTOR_NORMAL);
      executor_increment_executor->set_last_seen_timestamp(butil::gettimeofday_ms());

      // setup create_timestamp
      executor_increment_executor->set_create_timestamp(butil::gettimeofday_ms());
    }
  }

  if (need_update_epoch) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR, meta_increment);
  }

  DINGO_LOG(DEBUG) << "UpdateExecutorMap executor_id=" << executor.id();

  return executor_map_epoch;
}

pb::common::RegionState CoordinatorControl::GenRegionState(
    const pb::common::RegionMetrics& region_metrics, const pb::coordinator_internal::RegionInternal& region_internal) {
  pb::common::RegionState old_state = region_internal.state();
  pb::common::RegionState new_state = old_state;

  DEFER(if (new_state != old_state) {
    LOG(INFO) << fmt::format("region_state_change, region_id=({}), store_regin_state is {}, region_state from {} to {}",
                             region_metrics.id(),
                             pb::common::StoreRegionState_Name(region_metrics.store_region_state()),
                             pb::common::RegionState_Name(old_state), pb::common::RegionState_Name(new_state));
  });

  if (region_internal.state() == pb::common::RegionState::REGION_DELETE ||
      region_internal.state() == pb::common::RegionState::REGION_DELETING ||
      region_internal.state() == pb::common::RegionState::REGION_DELETED) {
    if (region_metrics.store_region_state() == pb::common::StoreRegionState::DELETED) {
      new_state = pb::common::RegionState::REGION_DELETED;
      return new_state;
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::DELETING) {
      new_state = pb::common::RegionState::REGION_DELETING;
      return new_state;
    }
  } else {
    if (region_metrics.store_region_state() == pb::common::StoreRegionState::NORMAL) {
      new_state = pb::common::RegionState::REGION_NORMAL;
      return new_state;
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::SPLITTING) {
      new_state = pb::common::RegionState::REGION_SPLITTING;
      return new_state;
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::MERGING) {
      new_state = pb::common::RegionState::REGION_MERGING;
      return new_state;
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::STANDBY) {
      new_state = pb::common::RegionState::REGION_STANDBY;
      return new_state;
    } else if (region_metrics.store_region_state() == pb::common::StoreRegionState::TOMBSTONE) {
      new_state = pb::common::RegionState::REGION_TOMBSTONE;
      return new_state;
    } else {
      new_state = pb::common::RegionState::REGION_NONE;
      return new_state;
    }
  }

  new_state = pb::common::RegionState::REGION_NONE;
  return new_state;
}

pb::common::RegionStatus CoordinatorControl::GenRegionStatus(const pb::common::RegionMetrics& region_metrics) {
  pb::common::RegionStatus region_status;

  region_status.set_last_update_timestamp(butil::gettimeofday_ms());

  auto region_heartbeat_status = pb::common::RegionHeartbeatState::REGION_ONLINE;
  auto region_raft_status = pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY;
  auto region_replica_status = pb::common::ReplicaStatus::REPLICA_NORMAL;

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

  // if peer cannot connected, ustable_followers_size will not be 0
  // so we need set replica status to DEGRAED
  // if (region_metrics.braft_status().unstable_followers_size() > 0) {
  //   region_raft_status =
  //   ::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_CONSECUTIVE_ERROR;
  //   region_increment_region->set_replica_status(::dingodb::pb::common::ReplicaStatus::REPLICA_DEGRAED);
  // } else {
  //   region_increment_region->set_replica_status(::dingodb::pb::common::ReplicaStatus::REPLICA_NORMAL);
  // }

  region_status.set_heartbeat_status(region_heartbeat_status);
  region_status.set_replica_status(region_replica_status);
  region_status.set_raft_status(region_raft_status);

  return region_status;
}

// Update RegionMap and StoreOperation
void CoordinatorControl::UpdateRegionMapAndStoreOperation(const pb::common::StoreMetrics& store_metrics,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  // for split/merge partial heartbeat, only 2 region is legal.
  if (store_metrics.is_partial_region_metrics() && store_metrics.is_update_epoch_version()) {
    if (store_metrics.region_metrics_map_size() != 2) {
      DINGO_LOG(ERROR) << "UpdateRegionMapAndStoreOperation partial heartbeat, split/merge, but "
                          "region_metrics_map_size != 2, size="
                       << store_metrics.region_metrics_map_size();
      for (const auto& it : store_metrics.region_metrics_map()) {
        DINGO_LOG(ERROR) << "UpdateRegionMapAndStoreOperation partial heartbeat, split/merge, but "
                            "region_metrics_map_size != 2, region_metrics="
                         << it.second.ShortDebugString();
      }

      return;
    }

    DINGO_LOG(INFO) << "UpdateRegionMapAndStoreOperation partial heartbeat, split/merge, region_metrics_map_size = "
                    << store_metrics.region_metrics_map_size()
                    << ", region_metrics=" << store_metrics.ShortDebugString();
  } else if (store_metrics.is_partial_region_metrics()) {
    DINGO_LOG(INFO) << "UpdateRegionMapAndStoreOperation partial heartbeat, not split/merge, region_metrics_map_size = "
                    << store_metrics.region_metrics_map_size()
                    << ", region_metrics=" << store_metrics.ShortDebugString();
  } else {
    DINGO_LOG(INFO) << "UpdateRegionMapAndStoreOperation full heartbeat, region_metrics_map_size = "
                    << store_metrics.region_metrics_map_size();
  }

  // update region_map
  for (const auto& it : store_metrics.region_metrics_map()) {
    const auto& region_metrics = it.second;

    pb::coordinator_internal::RegionInternal region_to_update;
    auto ret = region_map_.Get(region_metrics.id(), region_to_update);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "region_to_update is not found in region_map_, region_id = " << region_metrics.id();
      continue;
    }

    // when region leader change or region state change, we need to update
    // region_map_ or when region last_update_timestamp is too old, we
    // need to update region_map_
    bool need_update_region_state = false;
    bool need_update_region_definition = false;
    bool need_update_region_metrics = false;
    bool region_metrics_is_not_leader = false;
    bool leader_has_old_epoch = false;

    pb::common::RegionMetrics region_metrics_to_update;
    auto ret1 = region_metrics_map_.Get(region_metrics.id(), region_metrics_to_update);
    if (ret1 < 0) {
      region_metrics_to_update = region_metrics;
      *(region_metrics_to_update.mutable_region_status()) = GenRegionStatus(region_metrics);
      region_metrics_map_.Put(region_metrics.id(), region_metrics_to_update);

      DINGO_LOG(INFO) << "region_metrics_to_update is first time put into region_metrics_map_, region_id = "
                      << region_metrics.id() << ", from store_id: " << store_metrics.id();
    }

    // this will update to RegionMetrics
    pb::common::RegionStatus region_status_to_update = region_metrics_to_update.region_status();
    region_status_to_update.set_last_update_timestamp(butil::gettimeofday_ms());

    // this will update to RegionInternal
    auto region_state_to_update = region_to_update.state();

    // use leader store's region_metrics to update region_map
    // if region's store_region_stat is deleting or deleted, there will be
    // no leaders in this region so we need to update region state using
    // any node of this region
    if (region_metrics.braft_status().raft_state() != pb::common::RaftNodeState::STATE_LEADER) {
      region_metrics_is_not_leader = true;
    }

    if (region_metrics.leader_store_id() > 0 && region_metrics.leader_store_id() != store_metrics.id()) {
      region_metrics_is_not_leader = true;
    }

    // DINGO_LOG(INFO) << "region_id: " << region_metrics.id()
    //                 << ", region_metrics_is_not_leader: " << region_metrics_is_not_leader
    //                 << ", leader_store_id: " << region_metrics.leader_store_id();

    // if region_epoch is old than region_map_, skip update definition
    if (region_to_update.definition().epoch().conf_version() <
            region_metrics.region_definition().epoch().conf_version() ||
        region_to_update.definition().epoch().version() < region_metrics.region_definition().epoch().version()) {
      DINGO_LOG(INFO) << "region_metrics has new epoch, region_id = " << region_metrics.id()
                      << " old conf_version = " << region_to_update.definition().epoch().conf_version()
                      << " new conf_version = " << region_metrics.region_definition().epoch().conf_version()
                      << " old version = " << region_to_update.definition().epoch().version()
                      << " new version = " << region_metrics.region_definition().epoch().version();
      need_update_region_definition = true;
      need_update_region_metrics = true;
    };

    if (region_metrics_is_not_leader) {
      if ((!need_update_region_definition) && region_to_update.state() != pb::common::RegionState::REGION_DELETE &&
          region_to_update.state() != pb::common::RegionState::REGION_DELETING &&
          region_to_update.state() != pb::common::RegionState::REGION_DELETED) {
        DINGO_LOG(DEBUG) << "region is not deleted and need_update_region_definition is false, follower can't update "
                            "region_map, store_id="
                         << store_metrics.id() << " region_id = " << region_metrics.id();
        continue;
      } else {
        DINGO_LOG(INFO) << "follower will update RegionMap, store_id=" << store_metrics.id()
                        << " region_id = " << region_metrics.id();
      }
    } else {
      if (region_to_update.definition().epoch().conf_version() >
              region_metrics.region_definition().epoch().conf_version() ||
          region_to_update.definition().epoch().version() > region_metrics.region_definition().epoch().version()) {
        DINGO_LOG(WARNING) << "leader region in RegionMap epoch is old, region_id = " << region_metrics.id()
                           << " old conf_version = " << region_metrics.region_definition().epoch().conf_version()
                           << " new conf_version = " << region_to_update.definition().epoch().conf_version()
                           << " old version = " << region_metrics.region_definition().epoch().version()
                           << " new version = " << region_to_update.definition().epoch().version();
        leader_has_old_epoch = true;
      }
    }

    if (!region_metrics_is_not_leader) {
      if (region_metrics_to_update.leader_store_id() != store_metrics.id()) {
        DINGO_LOG(INFO) << "region leader change region_id = " << region_metrics.id()
                        << " old leader_store_id = " << region_metrics_to_update.leader_store_id()
                        << " new leader_store_id = " << store_metrics.id();
        need_update_region_metrics = true;
      }
    }

    if (region_metrics_to_update.store_region_state() != region_metrics.store_region_state()) {
      DINGO_LOG(INFO) << "region state change region_id = " << region_metrics.id()
                      << " old state = " << region_metrics_to_update.store_region_state()
                      << " new state = " << region_metrics.store_region_state();
      need_update_region_metrics = true;
    }

    region_state_to_update = GenRegionState(region_metrics, region_to_update);

    if (region_state_to_update != pb::common::RegionState::REGION_NONE &&
        region_state_to_update != region_to_update.state()) {
      DINGO_LOG(INFO) << "UpdateRegionMapAndStoreOperation region_map_ update state region_id = " << region_metrics.id()
                      << ", new_state: " << pb::common::RegionState_Name(region_state_to_update)
                      << ", old_state: " << pb::common::RegionState_Name(region_to_update.state());

      need_update_region_state = true;
      need_update_region_metrics = true;
    }

    // if region is updated to REGION_DELETED, there is no need to update
    // region by timestamp once region is deleted, all stores will update
    // this region's store_region_state, after the last store reported
    // region's state to DELETED, there is no need to update the region's
    // state in region_map, after timeout, we will trigger purge_request
    // to store to purge this region's meta
    if (region_metrics_to_update.mutable_region_status()->last_update_timestamp() + FLAGS_region_update_timeout * 1000 <
            butil::gettimeofday_ms() &&
        region_to_update.state() != pb::common::RegionState::REGION_DELETED) {
      DINGO_LOG(DEBUG) << "region last_update_timestamp too old region_id = " << region_metrics.id()
                       << " last_update_timestamp = "
                       << region_metrics_to_update.region_status().last_update_timestamp()
                       << " now = " << butil::gettimeofday_ms();
      need_update_region_metrics = true;
    }

    if (region_to_update.definition().range().start_key() != region_metrics.region_definition().range().start_key() ||
        region_to_update.definition().range().end_key() != region_metrics.region_definition().range().end_key()) {
      DINGO_LOG(INFO) << "region range change region_id = " << region_metrics.id() << " old range = ["
                      << Helper::StringToHex(region_to_update.definition().range().start_key()) << ", "
                      << Helper::StringToHex(region_to_update.definition().range().end_key()) << ")"
                      << " new range = [" << Helper::StringToHex(region_metrics.region_definition().range().start_key())
                      << ", " << Helper::StringToHex(region_metrics.region_definition().range().end_key()) << ")";
      if (!leader_has_old_epoch) {
        need_update_region_metrics = true;
      }

      if (!need_update_region_definition) {
        DINGO_LOG(WARNING) << "region range change, but need_update_region_definition is false, region_id = "
                           << region_metrics.id();
      }
    }

    if (region_to_update.definition().peers_size() != region_metrics.region_definition().peers_size()) {
      DINGO_LOG(INFO) << "region peers size change region_id = " << region_metrics.id()
                      << " old peers size = " << region_to_update.definition().peers_size()
                      << " new peers size = " << region_metrics.region_definition().peers_size();
      if (!leader_has_old_epoch) {
        need_update_region_definition = true;
        need_update_region_metrics = true;
      }
    }

    if (store_metrics.is_partial_region_metrics() && !store_metrics.is_update_epoch_version()) {
      DINGO_LOG(INFO) << "region partial heartbeat with no is_update_epoch_version, it's not split/merge, can not "
                         "update region definition, region_id = "
                      << region_metrics.id();
      need_update_region_definition = false;
    }

    if (!(need_update_region_state || need_update_region_definition || need_update_region_metrics)) {
      DINGO_LOG(DEBUG) << "region no need to update region_id = " << region_metrics.id() << " last_update_timestamp = "
                       << region_metrics_to_update.region_status().last_update_timestamp()
                       << " now = " << butil::gettimeofday_ms();
      continue;
    }

    if (need_update_region_definition || need_update_region_state) {
      // update meta_increment
      auto* region_increment = meta_increment.add_regions();
      region_increment->set_id(region_metrics.id());
      region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

      auto* region_increment_region = region_increment->mutable_region();
      *region_increment_region = region_to_update;

      if (need_update_region_definition) {
        *(region_increment_region->mutable_definition()) = region_metrics.region_definition();

        DINGO_LOG(INFO) << "UpdateRegionMapAndStoreOperation region_map_ update definition region_id = "
                        << region_metrics.id()
                        << ", definition: " << region_metrics.region_definition().ShortDebugString();
      }

      if (need_update_region_state) {
        region_increment_region->set_state(region_state_to_update);
      }
    }

    if (need_update_region_metrics) {
      region_metrics_to_update = region_metrics;

      if (!region_metrics_is_not_leader) {
        region_metrics_to_update.set_leader_store_id(store_metrics.id());
        region_status_to_update = GenRegionStatus(region_metrics);
      }

      *(region_metrics_to_update.mutable_region_status()) = region_status_to_update;

      region_metrics_map_.Put(region_metrics.id(), region_metrics_to_update);

      DINGO_LOG(DEBUG) << "UpdateRegionMapAndStoreOperation region_metrics_map_ update region_id = "
                       << region_metrics.id() << " last_update_timestamp = "
                       << region_metrics_to_update.region_status().last_update_timestamp()
                       << " now = " << butil::gettimeofday_ms();
    }

    // mbvar region
    if (region_to_update.state() != pb::common::RegionState::REGION_DELETED) {
      coordinator_bvar_metrics_region_.UpdateRegionBvar(region_metrics.id(), region_metrics.row_count(),
                                                        region_metrics.region_size());
    }
  }
}

int64_t CoordinatorControl::UpdateStoreMetrics(const pb::common::StoreMetrics& store_metrics,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  //   int64_t store_map_epoch =
  //   GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);
  if (store_metrics.id() <= 0) {
    DINGO_LOG(ERROR) << "ERROR: UpdateStoreMetrics store_metrics.id() <= "
                        "0, store_metrics.id() = "
                     << store_metrics.id();
    return -1;
  }

  if (!store_metrics.is_partial_region_metrics()) {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    StoreMetricsSlim store_metrics_slim;
    store_metrics_slim.store_id = store_metrics.id();
    store_metrics_slim.store_own_metrics = store_metrics.store_own_metrics();
    store_metrics_slim.region_num = store_metrics.region_metrics_map_size();
    store_metrics_slim.update_time = butil::gettimeofday_ms();

    store_metrics_map_.insert_or_assign(store_metrics.id(), std::move(store_metrics_slim));

    DINGO_LOG(INFO) << "UpdateStoreMetrics store_metrics.id=" << store_metrics.id()
                    << ", metrics: " << store_metrics.store_own_metrics().ShortDebugString();
  }

  if (store_metrics.region_metrics_map_size() <= 0) {
    DINGO_LOG(INFO) << "UpdateStoreMetrics store_metrics.region_metrics_map_size() <= 0, store_id="
                    << store_metrics.id() << ", do not update StoreMetrics";
    return 0;
  }

  {
    BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
    if (store_metrics.is_partial_region_metrics()) {
      if (store_region_metrics_map_.find(store_metrics.id()) == store_region_metrics_map_.end()) {
        store_region_metrics_map_.insert_or_assign(store_metrics.id(), store_metrics);
      } else {
        for (const auto& region_metrics : store_metrics.region_metrics_map()) {
          store_region_metrics_map_[store_metrics.id()].mutable_region_metrics_map()->insert(
              {region_metrics.first, region_metrics.second});
        }
      }
    } else {
      store_region_metrics_map_.insert_or_assign(store_metrics.id(), store_metrics);
    }
  }

  // mbvar store
  coordinator_bvar_metrics_store_.UpdateStoreBvar(store_metrics.id(),
                                                  store_metrics.store_own_metrics().system_total_capacity(),
                                                  store_metrics.store_own_metrics().system_free_capacity());

  // use region_metrics_map to update region_map and store_operation
  UpdateRegionMapAndStoreOperation(store_metrics, meta_increment);

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
    butil::FlatMap<int64_t, pb::coordinator_internal::IdEpochInternal> id_epoch_map_temp;
    id_epoch_map_temp.init(100);
    int ret = id_epoch_map_.GetRawMapCopy(id_epoch_map_temp);
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
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    memory_info.set_executor_map_count(executor_map_.Size());
    memory_info.set_executor_map_size(executor_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.executor_map_size());
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
    memory_info.set_region_metrics_map_count(region_metrics_map_.Size());
    memory_info.set_region_metrics_map_size(region_metrics_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.region_metrics_map_size());
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
                                             it.second.store_own_metrics.ByteSizeLong() + sizeof(int64_t) * 2);
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.store_metrics_map_size());
  }
  {
    BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
    memory_info.set_store_region_metrics_map_count(store_region_metrics_map_.size());
    for (auto& it : store_region_metrics_map_) {
      memory_info.set_store_region_metrics_map_size(memory_info.store_region_metrics_map_size() + sizeof(it.first) +
                                                    it.second.ByteSizeLong());
    }
    memory_info.set_total_size(memory_info.total_size() + memory_info.store_region_metrics_map_size());
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
    memory_info.set_region_cmd_map_count(region_cmd_map_.Size());
    memory_info.set_region_cmd_map_size(region_cmd_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.region_cmd_map_size());
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
  {
    memory_info.set_index_map_count(index_map_.Size());
    memory_info.set_index_map_size(index_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.index_map_size());
  }
  {
    memory_info.set_index_metrics_map_count(index_metrics_map_.Size());
    memory_info.set_index_metrics_map_size(index_metrics_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.index_metrics_map_size());
  }
  {
    int64_t deleted_table_count = deleted_table_meta_->Count();
    memory_info.set_deleted_table_map_count(deleted_table_count);
  }
  {
    int64_t deleted_index_count = deleted_index_meta_->Count();
    memory_info.set_deleted_index_map_count(deleted_index_count);
  }
  {
    int64_t deleted_region_count = deleted_region_meta_->Count();
    memory_info.set_deleted_region_map_count(deleted_region_count);
  }
  {
    memory_info.set_table_index_map_count(table_index_map_.Size());
    memory_info.set_table_index_map_size(table_index_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.table_index_map_size());
  }
  { memory_info.set_common_disk_map_count(common_disk_meta_->Count()); }
  {
    memory_info.set_common_mem_map_count(common_mem_meta_->Count());
    memory_info.set_common_mem_map_size(common_mem_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.common_mem_map_size());
  }
  {
    memory_info.set_tenant_map_count(tenant_map_.Size());
    memory_info.set_tenant_map_size(tenant_map_.MemorySize());
    memory_info.set_total_size(memory_info.total_size() + memory_info.tenant_map_size());
  }
  {
    int64_t meta_watch_count = meta_watch_node_map_.Size();
    memory_info.set_meta_watch_count(meta_watch_count);
  }
  {
    int64_t meta_event_count = meta_event_map_.Size();
    memory_info.set_meta_event_count(meta_event_count);
  }
}

int CoordinatorControl::GetStoreOperation(int64_t store_id, pb::coordinator::StoreOperation& store_operation) {
  DINGO_LOG(INFO) << "GetStoreOperation store_id = " << store_id;

  store_operation.set_id(store_id);
  pb::coordinator_internal::StoreOperationInternal store_operation_internal;
  int ret = store_operation_map_.Get(store_id, store_operation_internal);
  if (ret < 0) {
    return ret;
  }

  for (auto region_cmd_id : store_operation_internal.region_cmd_ids()) {
    pb::coordinator_internal::RegionCmdInternal region_cmd;
    ret = region_cmd_map_.Get(region_cmd_id, region_cmd);
    if (ret < 0) {
      continue;
    }
    auto* region_cmd_add = store_operation.add_region_cmds();
    region_cmd_add->set_region_cmd_type(region_cmd.region_cmd().region_cmd_type());
    region_cmd_add->set_region_id(region_cmd.region_cmd().region_id());
    region_cmd_add->set_create_timestamp(region_cmd.region_cmd().create_timestamp());
    region_cmd_add->set_id(region_cmd.region_cmd().id());

    if (region_cmd.error().errcode() != pb::error::Errno::OK) {
      region_cmd_add->set_status(pb::coordinator::RegionCmdStatus::STATUS_FAIL);
    }
  }

  return 0;
}

int CoordinatorControl::GetStoreOperationOfNotCreateForSend(int64_t store_id,
                                                            pb::coordinator::StoreOperation& store_operation) {
  pb::coordinator_internal::StoreOperationInternal store_operation_internal;
  int ret = store_operation_map_.Get(store_id, store_operation_internal);
  if (ret < 0) {
    return ret;
  }

  store_operation.set_id(store_operation_internal.id());

  uint32_t region_cmd_count = 0;
  for (auto region_cmd_id : store_operation_internal.region_cmd_ids()) {
    pb::coordinator_internal::RegionCmdInternal region_cmd;
    ret = region_cmd_map_.Get(region_cmd_id, region_cmd);
    if (ret < 0) {
      continue;
    }

    if (region_cmd.region_cmd().region_cmd_type() == pb::coordinator::RegionCmdType::CMD_DELETE) {
      DINGO_LOG(DEBUG) << "first round skip CMD_DELETE region_cmd_id = " << region_cmd.region_cmd().id()
                       << " region_id = " << region_cmd.region_cmd().region_id() << " store_id = " << store_id
                       << " region_cmd_type = "
                       << pb::coordinator::RegionCmdType_Name(region_cmd.region_cmd().region_cmd_type());
      continue;
    }

    if (region_cmd.region_cmd().region_cmd_type() == pb::coordinator::RegionCmdType::CMD_CREATE) {
      continue;
    }

    *(store_operation.add_region_cmds()) = region_cmd.region_cmd();

    region_cmd_count++;

    if (region_cmd_count > FLAGS_max_send_region_cmd_per_store) {
      DINGO_LOG(WARNING) << "GetStoreOperationForSend first_round region_cmd_count > "
                            "FLAGS_max_send_region_cmd_per_store, store_id = "
                         << store_id << " send_region_cmd_count = " << region_cmd_count
                         << ", real_region_cmd_count = " << store_operation_internal.region_cmd_ids_size();
      return 0;
    }
  }

  for (auto region_cmd_id : store_operation_internal.region_cmd_ids()) {
    pb::coordinator_internal::RegionCmdInternal region_cmd;
    ret = region_cmd_map_.Get(region_cmd_id, region_cmd);
    if (ret < 0) {
      continue;
    }

    if (region_cmd.region_cmd().region_cmd_type() != pb::coordinator::RegionCmdType::CMD_DELETE) {
      continue;
    }

    if (region_cmd.region_cmd().region_cmd_type() == pb::coordinator::RegionCmdType::CMD_CREATE) {
      continue;
    }

    *(store_operation.add_region_cmds()) = region_cmd.region_cmd();

    region_cmd_count++;

    if (region_cmd_count > FLAGS_max_send_region_cmd_per_store) {
      DINGO_LOG(WARNING) << "GetStoreOperationForSend second round region_cmd_count > "
                            "FLAGS_max_send_region_cmd_per_store, store_id = "
                         << store_id << " send_region_cmd_count = " << region_cmd_count
                         << ", real_region_cmd_count = " << store_operation_internal.region_cmd_ids_size();
      return 0;
    }
  }

  return 0;
}

int CoordinatorControl::GetStoreOperationOfCreateForSend(int64_t store_id,
                                                         pb::coordinator::StoreOperation& store_operation) {
  pb::coordinator_internal::StoreOperationInternal store_operation_internal;
  int ret = store_operation_map_.Get(store_id, store_operation_internal);
  if (ret < 0) {
    return ret;
  }

  store_operation.set_id(store_operation_internal.id());

  uint32_t region_cmd_count = 0;
  for (auto region_cmd_id : store_operation_internal.region_cmd_ids()) {
    pb::coordinator_internal::RegionCmdInternal region_cmd;
    ret = region_cmd_map_.Get(region_cmd_id, region_cmd);
    if (ret < 0) {
      continue;
    }

    if (region_cmd.region_cmd().region_cmd_type() != pb::coordinator::RegionCmdType::CMD_CREATE) {
      continue;
    }

    *(store_operation.add_region_cmds()) = region_cmd.region_cmd();

    region_cmd_count++;

    if (region_cmd_count > FLAGS_max_send_region_cmd_per_store) {
      DINGO_LOG(WARNING) << "GetStoreOperationForSend first_round region_cmd_count > "
                            "FLAGS_max_send_region_cmd_per_store, store_id = "
                         << store_id << " send_region_cmd_count = " << region_cmd_count
                         << ", real_region_cmd_count = " << store_operation_internal.region_cmd_ids_size();
      return 0;
    }
  }

  return 0;
}

void CoordinatorControl::GetTaskListAll(butil::FlatMap<int64_t, pb::coordinator::TaskList>& task_lists) {
  task_list_map_.GetRawMapCopy(task_lists);
}

void CoordinatorControl::GetTaskList(int64_t task_list_id, pb::coordinator::TaskList& task_list) {
  task_list_map_.Get(task_list_id, task_list);
}

pb::coordinator::TaskList* CoordinatorControl::CreateTaskList(pb::coordinator_internal::MetaIncrement& meta_increment) {
  auto* task_list_increment = meta_increment.add_task_lists();
  task_list_increment->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TASK_LIST, meta_increment));
  task_list_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  auto* increment_task_list = task_list_increment->mutable_task_list();
  increment_task_list->set_id(task_list_increment->id());

  return increment_task_list;
}

void CoordinatorControl::AddCreateTask(pb::coordinator::TaskList* task_list, int64_t store_id, int64_t region_id,
                                       const pb::common::RegionDefinition& region_definition,
                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this create region task
  auto* new_task = task_list->add_tasks();
  auto* store_operation_add = new_task->add_store_operations();
  store_operation_add->set_id(store_id);
  auto* region_cmd_to_add = store_operation_add->add_region_cmds();
  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_add->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_CREATE);
  region_cmd_to_add->set_region_id(region_id);
  region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());

  *(region_cmd_to_add->mutable_create_request()->mutable_region_definition()) = region_definition;
}

void CoordinatorControl::AddDeleteTask(pb::coordinator::TaskList* task_list, int64_t store_id, int64_t region_id,
                                       pb::coordinator::StoreOperation* store_operation,
                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this is delete_region task
  auto* delete_region_task = task_list->add_tasks();

  auto* store_operation_delete = delete_region_task->add_store_operations();
  GenDeleteRegionStoreOperation(*store_operation_delete, store_id, region_id, meta_increment);

  if (store_operation != nullptr) {
    *store_operation = *store_operation_delete;
    return;
  }
}

void CoordinatorControl::GenDeleteRegionStoreOperation(pb::coordinator::StoreOperation& store_operation,
                                                       int64_t store_id, int64_t region_id,
                                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  auto* store_operation_delete = &store_operation;
  store_operation_delete->set_id(store_id);
  auto* region_cmd_delete = store_operation_delete->add_region_cmds();
  region_cmd_delete->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_delete->set_region_id(region_id);
  region_cmd_delete->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_DELETE);
  region_cmd_delete->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_delete->mutable_delete_request()->set_region_id(region_id);
}

void CoordinatorControl::AddDeleteTaskWithCheck(
    pb::coordinator::TaskList* task_list, int64_t store_id, int64_t region_id,
    const ::google::protobuf::RepeatedPtrField<::dingodb::pb::common::Peer>& peers,
    pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this is delete_region task
  // precheck if region in RegionMap is REGION_NORMAL and
  // REGION_RAFT_HEALTHY
  auto* delete_region_task = task_list->add_tasks();
  auto* region_check = delete_region_task->mutable_pre_check();
  region_check->set_type(pb::coordinator::TaskPreCheckType::REGION_CHECK);
  region_check->mutable_region_check()->set_region_id(region_id);
  *(region_check->mutable_region_check()->mutable_peers()) = peers;
  region_check->mutable_region_check()->set_state(::dingodb::pb::common::RegionState::REGION_NORMAL);
  region_check->mutable_region_check()->set_raft_status(::dingodb::pb::common::RegionRaftStatus::REGION_RAFT_HEALTHY);

  auto* store_operation_delete = delete_region_task->add_store_operations();
  GenDeleteRegionStoreOperation(*store_operation_delete, store_id, region_id, meta_increment);
}

// void CoordinatorControl::AddPurgeTask(pb::coordinator::TaskList*
// task_list, int64_t store_id, int64_t region_id,
//                                       pb::coordinator_internal::MetaIncrement&
//                                       meta_increment) {
//   // this is purge_region task
//   auto* purge_region_task = task_list->add_tasks();

//   // precheck if region on store is DELETED
//   auto* purge_region_check = purge_region_task->mutable_pre_check();
//   purge_region_check->set_type(pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK);
//   purge_region_check->mutable_store_region_check()->set_store_id(store_id);
//   purge_region_check->mutable_store_region_check()->set_region_id(region_id);
//   purge_region_check->mutable_store_region_check()->set_store_region_state(
//       ::dingodb::pb::common::StoreRegionState::DELETED);

//   auto* store_operation_purge =
//   purge_region_task->add_store_operations();
//   store_operation_purge->set_id(store_id);
//   auto* region_cmd_to_purge = store_operation_purge->add_region_cmds();
//   region_cmd_to_purge->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD,
//   meta_increment)); region_cmd_to_purge->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_PURGE);
//   region_cmd_to_purge->set_region_id(region_id);
//   region_cmd_to_purge->set_create_timestamp(butil::gettimeofday_ms());
//   region_cmd_to_purge->mutable_purge_request()->set_region_id(region_id);
// }

void CoordinatorControl::AddCheckTombstoneRegionTask(pb::coordinator::TaskList* task_list, int64_t store_id,
                                                     int64_t region_id) {
  // this is drop region task
  auto* check_tombstone_region_task = task_list->add_tasks();

  // precheck if region on store is TOMBSTONE
  auto* purge_region_check = check_tombstone_region_task->mutable_pre_check();
  purge_region_check->set_type(pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK);
  purge_region_check->mutable_store_region_check()->set_store_id(store_id);
  purge_region_check->mutable_store_region_check()->set_region_id(region_id);
  purge_region_check->mutable_store_region_check()->set_store_region_state(pb::common::StoreRegionState::TOMBSTONE);
}

void CoordinatorControl::AddChangePeerTask(pb::coordinator::TaskList* task_list, int64_t store_id, int64_t region_id,
                                           const pb::common::RegionDefinition& region_definition,
                                           pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this is change_peer task
  auto* new_task = task_list->add_tasks();
  auto* store_operation_change = new_task->add_store_operations();
  store_operation_change->set_id(store_id);
  auto* region_cmd_to_change = store_operation_change->add_region_cmds();
  region_cmd_to_change->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_change->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_CHANGE_PEER);
  region_cmd_to_change->set_region_id(region_id);
  region_cmd_to_change->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_change->set_is_notify(true);
  *(region_cmd_to_change->mutable_change_peer_request()->mutable_region_definition()) = region_definition;
}

void CoordinatorControl::AddTransferLeaderTask(pb::coordinator::TaskList* task_list, int64_t store_id,
                                               int64_t region_id, const pb::common::Peer& new_leader_peer,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  // this is transfer_leader task
  auto* new_task = task_list->add_tasks();
  auto* store_operation_transfer = new_task->add_store_operations();
  store_operation_transfer->set_id(store_id);
  auto* region_cmd_to_transfer = store_operation_transfer->add_region_cmds();

  region_cmd_to_transfer->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_transfer->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_TRANSFER_LEADER);
  region_cmd_to_transfer->set_region_id(region_id);
  region_cmd_to_transfer->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_transfer->set_is_notify(true);  // notify store to do immediately heartbeat

  *(region_cmd_to_transfer->mutable_transfer_leader_request()->mutable_peer()) = new_leader_peer;
}

void CoordinatorControl::AddMergeTask(pb::coordinator::TaskList* task_list, int64_t store_id,
                                      int64_t merge_from_region_id, int64_t merge_to_region_id,
                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  // build merege task
  auto* merge_task = task_list->add_tasks();
  auto* store_operation_merge = merge_task->add_store_operations();
  store_operation_merge->set_id(store_id);
  auto* region_cmd_to_add = store_operation_merge->add_region_cmds();

  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_add->set_region_id(merge_from_region_id);
  region_cmd_to_add->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_MERGE);
  region_cmd_to_add->mutable_merge_request()->set_source_region_id(merge_from_region_id);
  region_cmd_to_add->mutable_merge_request()->set_target_region_id(merge_to_region_id);
  region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_add->set_is_notify(true);  // notify store to do immediately heartbeat
}

void CoordinatorControl::AddSnapshotVectorIndexTask(pb::coordinator::TaskList* task_list, int64_t store_id,
                                                    int64_t region_id, int64_t snapshot_log_id,
                                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  // build split_region task
  auto* region_save_vector_task = task_list->add_tasks();

  // generate store operation for stores
  auto* store_operation_save_vector = region_save_vector_task->add_store_operations();
  store_operation_save_vector->set_id(store_id);
  auto* region_cmd_to_add = store_operation_save_vector->add_region_cmds();

  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_add->set_region_id(region_id);
  region_cmd_to_add->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_SNAPSHOT_VECTOR_INDEX);
  region_cmd_to_add->mutable_snapshot_vector_index_request()->set_vector_index_id(region_id);
  region_cmd_to_add->mutable_snapshot_vector_index_request()->set_raft_log_index(snapshot_log_id);
  region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_add->set_is_notify(true);  // notify store to do immediately heartbeat
}

void CoordinatorControl::AddCheckVectorIndexSnapshotLogIdTask(pb::coordinator::TaskList* task_list, int64_t region_id,
                                                              int64_t vector_snapshot_log_id) {
  // build check_vector_index task
  auto* check_vector_task = task_list->add_tasks();
  auto* region_check = check_vector_task->mutable_pre_check();
  region_check->set_type(pb::coordinator::TaskPreCheckType::REGION_CHECK);
  region_check->mutable_region_check()->set_region_id(region_id);
  region_check->mutable_region_check()->set_vector_snapshot_log_id(vector_snapshot_log_id);
}

void CoordinatorControl::AddSplitTask(pb::coordinator::TaskList* task_list, int64_t store_id, int64_t region_id,
                                      int64_t split_to_region_id, const std::string& water_shed_key,
                                      bool store_create_region,
                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  // build split_region task
  auto* split_region_task = task_list->add_tasks();
  if (!store_create_region) {
    auto* region_check = split_region_task->mutable_pre_check();
    region_check->set_type(pb::coordinator::TaskPreCheckType::REGION_CHECK);
    region_check->mutable_region_check()->set_region_id(split_to_region_id);
    region_check->mutable_region_check()->set_state(::dingodb::pb::common::RegionState::REGION_STANDBY);
  }

  // generate store operation for stores
  auto* store_operation_split = split_region_task->add_store_operations();
  store_operation_split->set_id(store_id);
  auto* region_cmd_to_add = store_operation_split->add_region_cmds();

  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_add->set_region_id(region_id);
  region_cmd_to_add->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_SPLIT);
  region_cmd_to_add->mutable_split_request()->set_split_watershed_key(water_shed_key);
  region_cmd_to_add->mutable_split_request()->set_split_from_region_id(region_id);
  region_cmd_to_add->mutable_split_request()->set_split_to_region_id(split_to_region_id);
  region_cmd_to_add->mutable_split_request()->set_store_create_region(store_create_region);
  region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_add->set_is_notify(true);  // notify store to do immediately heartbeat
}

void CoordinatorControl::AddCheckSplitResultTask(pb::coordinator::TaskList* task_list, int64_t split_to_region_id) {
  // build split_region task
  auto* split_result_check_task = task_list->add_tasks();
  auto* split_result_check = split_result_check_task->mutable_pre_check();
  split_result_check->set_type(pb::coordinator::TaskPreCheckType::REGION_CHECK);
  split_result_check->mutable_region_check()->set_region_id(split_to_region_id);
  split_result_check->mutable_region_check()->set_state(pb::common::RegionState::REGION_NORMAL);
}

void CoordinatorControl::AddCheckMergeResultTask(pb::coordinator::TaskList* task_list, int64_t merge_to_region_id,
                                                 const pb::common::Range& range) {
  // build merge_region task
  auto* split_result_check_task = task_list->add_tasks();
  auto* split_result_check = split_result_check_task->mutable_pre_check();
  split_result_check->set_type(pb::coordinator::TaskPreCheckType::REGION_CHECK);
  split_result_check->mutable_region_check()->set_region_id(merge_to_region_id);
  split_result_check->mutable_region_check()->set_state(pb::common::RegionState::REGION_NORMAL);
  *split_result_check->mutable_region_check()->mutable_range() = range;
}

void CoordinatorControl::AddCheckStoreVectorIndexTask(pb::coordinator::TaskList* task_list, int64_t store_id,
                                                      int64_t region_id, int64_t vector_index_version) {
  // build check_vector_index task
  auto* check_vector_task = task_list->add_tasks();
  auto* region_check = check_vector_task->mutable_pre_check();
  region_check->set_type(pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK);
  region_check->mutable_store_region_check()->set_store_id(store_id);
  region_check->mutable_store_region_check()->set_region_id(region_id);
  region_check->mutable_store_region_check()->set_check_vector_index_is_hold(true);
  region_check->mutable_store_region_check()->set_is_hold_vector_index(true);
  region_check->mutable_store_region_check()->set_check_vector_index_is_ready(true);
  region_check->mutable_store_region_check()->set_is_ready(true);

  if (vector_index_version > 0) {
    region_check->mutable_store_region_check()->set_vector_index_version(vector_index_version);
  }
}

void CoordinatorControl::AddLoadVectorIndexTask(pb::coordinator::TaskList* task_list, int64_t store_id,
                                                int64_t region_id,
                                                pb::coordinator_internal::MetaIncrement& meta_increment) {
  // build check_vector_index task
  auto* load_vector_task = task_list->add_tasks();
  auto* region_check = load_vector_task->mutable_pre_check();
  region_check->set_type(pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK);
  region_check->mutable_store_region_check()->set_store_id(store_id);
  region_check->mutable_store_region_check()->set_region_id(region_id);

  // generate store operation for stores
  auto* store_operation = load_vector_task->add_store_operations();
  store_operation->set_id(store_id);
  auto* region_cmd_to_add = store_operation->add_region_cmds();

  region_cmd_to_add->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
  region_cmd_to_add->set_region_id(region_id);
  region_cmd_to_add->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_HOLD_VECTOR_INDEX);
  region_cmd_to_add->mutable_hold_vector_index_request()->set_region_id(region_id);
  region_cmd_to_add->mutable_hold_vector_index_request()->set_is_hold(true);
  region_cmd_to_add->set_create_timestamp(butil::gettimeofday_ms());
  region_cmd_to_add->set_is_notify(true);  // notify store to do immediately heartbeat
}

void CoordinatorControl::AddCheckStoreRegionTask(pb::coordinator::TaskList* task_list, int64_t store_id,
                                                 int64_t region_id) {
  // build check_vector_index task
  auto* check_region_task = task_list->add_tasks();
  auto* region_check = check_region_task->mutable_pre_check();
  region_check->set_type(pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK);
  region_check->mutable_store_region_check()->set_store_id(store_id);
  region_check->mutable_store_region_check()->set_region_id(region_id);
}

void CoordinatorControl::AddCheckChangePeerResultTask(pb::coordinator::TaskList* task_list, int64_t region_id,
                                                      const pb::common::RegionDefinition& region_definition) {
  auto* pre_check = task_list->add_tasks()->mutable_pre_check();
  pre_check->set_type(pb::coordinator::TaskPreCheckType::REGION_CHECK);
  pre_check->mutable_region_check()->set_region_id(region_id);
  *(pre_check->mutable_region_check()->mutable_peers()) = region_definition.peers();
}

bool CoordinatorControl::DoTaskPreCheck(const pb::coordinator::TaskPreCheck& task_pre_check) {
  if (task_pre_check.type() == pb::coordinator::TaskPreCheckType::REGION_CHECK) {
    pb::coordinator_internal::RegionInternal region;
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

    auto region_status = GetRegionStatus(region.id());
    if (region_check.raft_status() != 0 && region_check.raft_status() != region_status.raft_status()) {
      check_passed = false;
    }

    if (region_check.replica_status() != 0 && region_check.replica_status() != region_status.replica_status()) {
      check_passed = false;
    }

    if (region_check.has_range()) {
      if (region_check.range().start_key() != region.definition().range().start_key() ||
          region_check.range().end_key() != region.definition().range().end_key()) {
        check_passed = false;
      }
    }

    if (region_check.peers_size() > 0) {
      std::vector<int64_t> peers_to_check;
      std::vector<int64_t> peers_of_region;

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

    if (region_check.vector_snapshot_log_id() > 0) {
      auto region_metrics = GetRegionMetrics(task_pre_check.region_check().region_id());
      if (region_check.vector_snapshot_log_id() > region_metrics.vector_index_status().snapshot_log_id()) {
        DINGO_LOG(INFO) << "check vector_index failed, region_check.vector_snapshot_log_id()="
                        << region_check.vector_snapshot_log_id()
                        << " region_metrics.vector_index_status().snapshot_log_id()="
                        << region_metrics.vector_index_status().snapshot_log_id()
                        << ", region_id=" << task_pre_check.region_check().region_id()
                        << ", region_metrics=" << region_metrics.ShortDebugString();
        check_passed = false;
      }
    }

    DINGO_LOG(INFO) << "task pre check_passed: " << check_passed
                    << ", check_type=REGION_CHECK, region_id: " << task_pre_check.region_check().region_id();

    return check_passed;
  } else if (task_pre_check.type() == pb::coordinator::TaskPreCheckType::STORE_REGION_CHECK) {
    pb::common::RegionMetrics store_region_metrics;
    {
      BAIDU_SCOPED_LOCK(store_region_metrics_map_mutex_);
      auto it = store_region_metrics_map_.find(task_pre_check.store_region_check().store_id());
      if (it == store_region_metrics_map_.end()) {
        DINGO_LOG(INFO) << "store_region_metrics_map_.find(" << task_pre_check.store_region_check().store_id()
                        << ") failed";
        return false;
      }

      const auto& region_metrics_map = it->second.region_metrics_map();
      if (region_metrics_map.find(task_pre_check.store_region_check().region_id()) == region_metrics_map.end()) {
        DINGO_LOG(INFO) << "region_metrics_map.find(" << task_pre_check.store_region_check().region_id() << ") failed";
        return false;
      }

      store_region_metrics = region_metrics_map.at(task_pre_check.store_region_check().region_id());
    }

    bool check_passed = true;
    const auto& store_region_check = task_pre_check.store_region_check();

    if (store_region_check.store_region_state() != 0 &&
        store_region_check.store_region_state() != store_region_metrics.store_region_state()) {
      check_passed = false;
    }

    if (store_region_check.raft_node_status() != 0 &&
        store_region_check.raft_node_status() != store_region_metrics.braft_status().raft_state()) {
      check_passed = false;
    }

    if (store_region_check.has_range()) {
      if (store_region_check.range().start_key() != store_region_metrics.region_definition().range().start_key() ||
          store_region_check.range().end_key() != store_region_metrics.region_definition().range().end_key()) {
        check_passed = false;
      }
    }

    if (store_region_check.peers_size() > 0) {
      std::vector<int64_t> peers_to_check;
      std::vector<int64_t> peers_of_region;

      for (const auto& it : store_region_check.peers()) {
        peers_to_check.push_back(it.store_id());
      }
      for (const auto& it : store_region_metrics.region_definition().peers()) {
        peers_of_region.push_back(it.store_id());
      }

      std::sort(peers_to_check.begin(), peers_to_check.end());
      std::sort(peers_of_region.begin(), peers_of_region.end());

      if (!std::equal(peers_to_check.begin(), peers_to_check.end(), peers_of_region.begin(), peers_of_region.end())) {
        check_passed = false;
      }
    }

    // check vector_index
    if (store_region_check.check_vector_index_is_hold()) {
      if (!store_region_metrics.has_vector_index_status()) {
        DINGO_LOG(INFO) << "check vector_index faild, region.has_vector_index_status() is false, can't do check, wait "
                           "for heartbeat. store_id="
                        << store_region_check.store_id() << ", region_id=" << store_region_check.region_id();
        check_passed = false;
      } else if (store_region_check.is_hold_vector_index() !=
                 store_region_metrics.vector_index_status().is_hold_vector_index()) {
        DINGO_LOG(INFO) << "check vector_index failed, region_check.is_hold_vector_index()="
                        << store_region_check.is_hold_vector_index()
                        << " region.vector_index_status().is_hold_vector_index()="
                        << store_region_metrics.vector_index_status().is_hold_vector_index()
                        << ", store_id=" << store_region_check.store_id()
                        << ", region_id=" << store_region_check.region_id()
                        << ", region=" << store_region_metrics.ShortDebugString();
        check_passed = false;
      }
    }

    if (store_region_check.check_vector_index_is_ready()) {
      if (!store_region_metrics.has_vector_index_status()) {
        DINGO_LOG(INFO) << "check vector_index faild, region.has_vector_index_status() is false, can't do check, wait "
                           "for heartbeat. store_id="
                        << store_region_check.store_id() << ", region_id=" << store_region_check.region_id();
        check_passed = false;
      } else if (store_region_check.is_ready() != store_region_metrics.vector_index_status().is_ready()) {
        DINGO_LOG(INFO) << "check vector_index failed, region_check.is_ready()=" << store_region_check.is_ready()
                        << " region.vector_index_status().is_ready()="
                        << store_region_metrics.vector_index_status().is_ready()
                        << ", store_id=" << store_region_check.store_id()
                        << ", region_id=" << store_region_check.region_id()
                        << ", region=" << store_region_metrics.ShortDebugString();
        check_passed = false;
      }
    }

    if (store_region_check.vector_index_version() > 0) {
      if (!store_region_metrics.has_vector_index_status()) {
        DINGO_LOG(INFO) << "check vector_index faild, region.has_vector_index_status() is false, can't do check, wait "
                           "for heartbeat. store_id="
                        << store_region_check.store_id() << ", region_id=" << store_region_check.region_id();
        check_passed = false;
      } else if (store_region_check.vector_index_version() !=
                 store_region_metrics.vector_index_status().last_build_epoch_version()) {
        DINGO_LOG(INFO) << "check vector_index failed, region_check.vector_index_version()="
                        << store_region_check.vector_index_version()
                        << " region.vector_index_status().vector_index_version()="
                        << store_region_metrics.vector_index_status().last_build_epoch_version()
                        << ", store_id=" << store_region_check.store_id()
                        << ", region_id=" << store_region_check.region_id()
                        << ", region=" << store_region_metrics.ShortDebugString();
        check_passed = false;
      }
    }

    DINGO_LOG(INFO) << "task pre check_passed: " << check_passed
                    << ", check_type=STORE_REGION_CHECK, store_id: " << task_pre_check.store_region_check().store_id()
                    << ", region_id: " << task_pre_check.store_region_check().region_id();

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
    DINGO_LOG(INFO) << "task_list.next_step() == task_list.tasks_size() "
                       "- 1, will delete this task_list";
    auto* task_list_increment = meta_increment.add_task_lists();
    task_list_increment->set_id(task_list.id());
    task_list_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
    *(task_list_increment->mutable_task_list()) = task_list;

    return butil::Status::OK();
  }

  // process task
  const auto& task = task_list.tasks(task_list.next_step());

  DINGO_LOG(INFO) << "process task=" << task.ShortDebugString();

  // do pre check
  bool can_advance_task = DoTaskPreCheck(task.pre_check());

  // advance task
  if (!can_advance_task) {
    DINGO_LOG(INFO) << "can not advance task, skip this task_list, task_list=" << task_list.ShortDebugString();
    return butil::Status::OK();
  }

  // do task, send all store_operations
  for (const auto& it : task.store_operations()) {
    for (const auto& region_cmd : it.region_cmds()) {
      DINGO_LOG(INFO) << "====task_list_id: " << task_list.id();
      auto ret = AddRegionCmd(it.id(), task_list.id(), region_cmd, meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "AddRegionCmd failed, store_id=" << it.id()
                         << " region_cmd=" << region_cmd.ShortDebugString();
        return ret;
      }
    }
  }

  // do task, submit all coordinator_operations
  for (const auto& it : task.coordinator_operations()) {
    auto ret = AddCoordinatorOperation(it, meta_increment);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "AddCoordinatorOperation failed, coordinator_operation=" << it.ShortDebugString();
      return ret;
    }
  }

  // advance step, update task_list
  auto* task_list_increment = meta_increment.add_task_lists();
  task_list_increment->set_id(task_list.id());
  task_list_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  *(task_list_increment->mutable_task_list()) = task_list;
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

  butil::FlatMap<int64_t, pb::coordinator::TaskList> task_list_map;
  task_list_map.init(100);
  auto ret = task_list_map_.GetRawMapCopy(task_list_map);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "task_list_map_.GetRawMapCopy failed";
    return butil::Status(pb::error::EINTERNAL, "task_list_map_.GetRawMapCopy failed");
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

  // // this callback will destruct by itself, so we don't need to delete it
  // auto* done = braft::NewCallback(this, &CoordinatorControl::ReleaseProcessTaskListStatus);

  // // prepare for raft process
  // atomic_guard.Release();
  // SubmitMetaIncrementAsync(done, meta_increment);

  SubmitMetaIncrementSync(meta_increment);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CleanTaskList(int64_t task_list_id,
                                                pb::coordinator_internal::MetaIncrement& meta_increment) {
  butil::FlatMap<int64_t, pb::coordinator::TaskList> task_list_map;
  task_list_map.init(100);
  auto ret = task_list_map_.GetRawMapCopy(task_list_map);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "task_list_map_.GetRawMapCopy failed";
    return butil::Status(pb::error::EINTERNAL, "task_list_map_.GetRawMapCopy failed");
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
      *(task_list_increment->mutable_task_list()) = task_list;
    }
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetRangeRegionMap(std::vector<std::string>& start_keys,
                                                    std::vector<pb::coordinator_internal::RegionInternal>& regions) {
  range_region_map_.GetAllKeyValues(start_keys, regions);
  return butil::Status::OK();
}

butil::Status CoordinatorControl::ScanRegions(const std::string& start_key, const std::string& end_key, int64_t limit,
                                              std::vector<pb::coordinator_internal::RegionInternal>& regions) {
  DINGO_LOG(DEBUG) << "ScanRegions start_key=" << Helper::StringToHex(start_key)
                   << " end_key=" << Helper::StringToHex(end_key) << " limit=" << limit;

  const std::string& lower_bound = start_key;
  std::string upper_bound;

  if (end_key == std::string(1, '\0')) {
    upper_bound = std::string(9, '\xff');
  } else if (end_key.empty()) {
    upper_bound = start_key + std::string(1, '\0');
  } else {
    upper_bound = end_key;
  }

  // DINGO_LOG(INFO) << "ScanRegions lower_bound=" << Helper::StringToHex(lower_bound)
  //                 << " upper_bound=" << Helper::StringToHex(upper_bound);

  std::vector<pb::coordinator_internal::RegionInternal> region_internals;
  auto ret = range_region_map_.FindIntervalValues(
      region_internals, lower_bound, upper_bound, nullptr,
      [lower_bound, upper_bound](const pb::coordinator_internal::RegionInternal& region) {
        return region.id() > 0 && region.definition().range().end_key() > lower_bound;
      });
  if (ret < 0) {
    DINGO_LOG(ERROR) << "range_region_map_.FindIntervalValues failed";
    return butil::Status(pb::error::EINTERNAL, "range_region_map_.FindIntervalValues failed");
  }

  DINGO_LOG(DEBUG) << "ScanRegions lower_bound=" << Helper::StringToHex(lower_bound)
                   << " upper_bound=" << Helper::StringToHex(upper_bound)
                   << " region_internals.size()=" << region_internals.size();

  for (const auto& region_internal : region_internals) {
    if (end_key.empty()) {
      if (region_internal.definition().range().start_key() <= start_key &&
          region_internal.definition().range().end_key() > start_key) {
        regions.push_back(region_internal);
        break;
      } else {
        continue;
      }
    }

    regions.push_back(region_internal);

    if (limit > 0 && regions.size() >= static_cast<size_t>(limit)) {
      break;
    }
  }

  DINGO_LOG(INFO) << "ScanRegions start_key=" << Helper::StringToHex(start_key)
                  << " end_key=" << Helper::StringToHex(end_key) << " upper_bound=" << Helper::StringToHex(upper_bound)
                  << " limit=" << limit << " region_internals.size()=" << region_internals.size()
                  << " regions.size()=" << regions.size();

  return butil::Status::OK();
}

butil::Status CoordinatorControl::UpdateGCSafePoint(int64_t safe_point,
                                                    pb::coordinator::UpdateGCSafePointRequest::GcFlagType gc_flag,
                                                    int64_t& new_safe_point, bool& gc_stop,
                                                    std::map<int64_t, int64_t>& tenant_safe_points,
                                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "UpdateGCSafePoint safe_point=" << safe_point
                  << ", gc_flag=" << pb::coordinator::UpdateGCSafePointRequest::GcFlagType_Name(gc_flag)
                  << ", tenant_safe_points.count: " << tenant_safe_points.size();

  // update gc_stop
  pb::coordinator_internal::CommonInternal gc_stop_element;
  auto ret1 = common_disk_meta_->Get(Constant::kGcStopKey, gc_stop_element);
  if (!ret1.ok()) {
    DINGO_LOG(ERROR) << "common_disk_meta_->Get(Constant::kGcStopKey) failed, errcode=" << ret1.error_code()
                     << " errmsg=" << ret1.error_str();
    return ret1;
  }

  DINGO_LOG(INFO) << "UpdateGCSafePoint now_gc_stop=" << gc_stop_element.value() << ", gc_flag=" << gc_flag;

  if (gc_flag == pb::coordinator::UpdateGCSafePointRequest::GC_NONE) {
    gc_stop = (gc_stop_element.value() == Constant::kGcStopValueTrue);
  } else if (gc_flag == pb::coordinator::UpdateGCSafePointRequest::GC_STOP) {
    gc_stop = true;
    if (gc_stop_element.value() != Constant::kGcStopValueTrue) {
      DINGO_LOG(INFO) << "UpdateGCSafePoint now_gc_stop=" << gc_stop_element.value() << ", update to true";
      gc_stop_element.set_id(Constant::kGcStopKey);
      gc_stop_element.set_value(Constant::kGcStopValueTrue);

      auto* increment = meta_increment.add_common_disk_s();
      increment->set_id(Constant::kGcStopKey);
      increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
      *(increment->mutable_common()) = gc_stop_element;
    }
  } else if (gc_flag == pb::coordinator::UpdateGCSafePointRequest::GC_START) {
    gc_stop = false;
    if (gc_stop_element.value() == Constant::kGcStopValueTrue) {
      DINGO_LOG(INFO) << "UpdateGCSafePoint now_gc_stop=" << gc_stop_element.value() << ", update to false";
      gc_stop_element.set_id(Constant::kGcStopKey);
      gc_stop_element.set_value(Constant::kGcStopValueFalse);

      auto* increment = meta_increment.add_common_disk_s();
      increment->set_id(Constant::kGcStopKey);
      increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
      *(increment->mutable_common()) = gc_stop_element;
    }
  } else {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "gc_flag not support");
  }

  // update safe_point_ts
  int64_t now_safe_point = GetPresentId(pb::coordinator_internal::IdEpochType::ID_GC_SAFE_POINT);

  if (now_safe_point >= safe_point) {
    DINGO_LOG(WARNING) << "UpdateGCSafePoint now_safe_point=" << now_safe_point << " >= safe_point=" << safe_point
                       << ", skip update default";
    new_safe_point = now_safe_point;
  } else {
    new_safe_point = safe_point;
    UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_GC_SAFE_POINT, new_safe_point, meta_increment);
  }

  // check and update tenant safe_points
  std::map<int64_t, int64_t> new_tenant_safe_points;

  for (const auto [tenant_id, safe_point_in_map] : tenant_safe_points) {
    if (tenant_id == 0) {
      continue;
    }

    pb::coordinator_internal::TenantInternal tenant;
    auto ret = tenant_map_.Get(tenant_id, tenant);
    if (ret < 0) {
      std::string s = "tenant_map_.Get failed, tenant_id=" + std::to_string(tenant_id);
      return butil::Status(pb::error::EINTERNAL, s);
    }

    if (tenant.safe_point_ts() < safe_point_in_map) {
      new_tenant_safe_points[tenant_id] = tenant.safe_point_ts();
      auto* tenant_increment = meta_increment.add_tenants();
      tenant_increment->set_id(tenant_id);
      tenant_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
      auto* increment_tenant = tenant_increment->mutable_tenant();
      *increment_tenant = tenant;
      increment_tenant->set_safe_point_ts(tenant_safe_points[tenant_id]);
      increment_tenant->set_update_timestamp(butil::gettimeofday_ms());
    }
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetGCSafePoint(int64_t& safe_point, bool& gc_stop,
                                                 const std::vector<int64_t>& tenant_ids, bool get_all_tenant,
                                                 std::map<int64_t, int64_t>& tenant_safe_points) {
  safe_point = GetPresentId(pb::coordinator_internal::IdEpochType::ID_GC_SAFE_POINT);
  pb::coordinator_internal::CommonInternal common;
  common_disk_meta_->Get(Constant::kGcStopKey, common);
  gc_stop = (common.value() == Constant::kGcStopValueTrue);

  if (get_all_tenant) {
    // get all tenant_ids
    std::vector<pb::meta::Tenant> tenants;
    auto ret1 = GetAllTenants(tenants);
    if (!ret1.ok()) {
      DINGO_LOG(ERROR) << "GetAllTenants failed, errcode=" << ret1.error_code() << " errmsg=" << ret1.error_str();
      return ret1;
    }

    for (const auto& tenant : tenants) {
      if (tenant.id() != 0) {
        tenant_safe_points[tenant.id()] = tenant.safe_point_ts();
      }
    }

    return butil::Status::OK();
  } else {
    // get safe_points for tenant_ids
    for (const auto tenant_id : tenant_ids) {
      if (tenant_id == 0) {
        tenant_safe_points[tenant_id] = safe_point;
        continue;
      }

      pb::coordinator_internal::TenantInternal tenant;
      auto ret = tenant_map_.Get(tenant_id, tenant);
      if (ret < 0) {
        std::string s = "tenant_map_.Get failed, tenant_id=" + std::to_string(tenant_id);
        return butil::Status(pb::error::EINTERNAL, s);
      }

      tenant_safe_points[tenant_id] = tenant.safe_point_ts();
    }
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::AddCoordinatorOperation(
    const pb::coordinator::CoordinatorOperation& coordinator_operation,
    pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (coordinator_operation.coordinator_op_type() == pb::coordinator::COORDINATOR_OP_TYPE_DROP_REGION) {
    const auto& drop_region = coordinator_operation.drop_region_operation();
    if (drop_region.region_id() > 0) {
      pb::coordinator_internal::RegionInternal region;
      auto ret = region_map_.Get(drop_region.region_id(), region);
      if (ret < 0) {
        DINGO_LOG(ERROR) << "region_map_.Get(" << drop_region.region_id() << ") failed";
        return butil::Status(pb::error::EINTERNAL, "region_map_.Get failed");
      }

      // call drop_region to get store_operations
      std::vector<pb::coordinator::StoreOperation> store_operations;
      auto ret1 = DropRegionFinal(drop_region.region_id(), store_operations, meta_increment);
      if (!ret1.ok()) {
        DINGO_LOG(ERROR) << "DropRegionFinal failed, region_id=" << drop_region.region_id();
        return ret1;
      }

      DINGO_LOG(INFO) << "DropRegionFinal success, region_id=" << drop_region.region_id()
                      << " store_operations.size()=" << store_operations.size();

      return butil::Status::OK();
    } else {
      DINGO_LOG(ERROR) << "drop_region.region_id() <= 0";
      return butil::Status(pb::error::EINTERNAL, "drop_region.region_id() <= 0");
    }
  } else {
    DINGO_LOG(ERROR) << "AddCoordinatorOperation failed, coordinator_operation.coordinator_op_type()="
                     << coordinator_operation.coordinator_op_type() << " not support";
    return butil::Status(pb::error::EINTERNAL, "coordinator_operation.coordinator_op_type() not support");
  }
  return butil::Status::OK();
}

/**
 * The `CheckStoreOperationResult` function checks the result of a store operation.
 * It takes as input a command type and an error code.
 * If the error code is `OK` or `EREGION_REPEAT_COMMAND`, the function returns true, indicating that the operation was
 * successful. If the error code is `ERAFT_NOTLEADER`, the function returns false, indicating that the operation
 * failed because the current node is not the leader. For other command types, the function checks the error code and
 * logs an error message if the operation failed. In some cases, even if the operation failed, the function returns
 * true to indicate that the failure is expected or recoverable. For example, if a `CMD_CREATE` operation fails with
 * an `EREGION_EXIST` error code, the function returns true because the region already exists.
 *
 * @param cmd_type The type of the command that was executed.
 * @param errcode The error code returned by the command execution.
 * @return A boolean indicating whether the operation was successful or if the failure is expected or recoverable.
 */
bool CoordinatorControl::CheckStoreOperationResult(pb::coordinator::RegionCmdType cmd_type, pb::error::Errno errcode) {
  using pb::coordinator::RegionCmdType;
  using pb::error::Errno;

  if (errcode == Errno::OK || errcode == Errno::EREGION_REPEAT_COMMAND) {
    return true;
  }

  if (errcode == Errno::ERAFT_NOTLEADER) {
    return false;
  }

  switch (cmd_type) {
    case RegionCmdType::CMD_CREATE:
      DINGO_LOG(ERROR) << "CheckStoreOperationResult... create region failed, errcode=" << errcode;
      if (errcode == Errno::EREGION_EXIST) {
        return true;
      }
      break;
    case RegionCmdType::CMD_DELETE:
      DINGO_LOG(ERROR) << "CheckStoreOperationResult... delete region failed, errcode=" << errcode;
      if (errcode == Errno::EREGION_DELETING || errcode == Errno::EREGION_NOT_FOUND) {
        return true;
      }
      break;
    case RegionCmdType::CMD_SPLIT:
      DINGO_LOG(ERROR) << "CheckStoreOperationResult... split region failed, errcode=" << errcode;
      if (errcode == Errno::EREGION_SPLITING) {
        return true;
      }
      break;
    case RegionCmdType::CMD_MERGE:
      DINGO_LOG(ERROR) << "CheckStoreOperationResult... merge region failed, errcode=" << errcode;
      if (errcode == Errno::EREGION_MERGEING) {
        return true;
      }
      break;
    case RegionCmdType::CMD_CHANGE_PEER:
      DINGO_LOG(ERROR) << "CheckStoreOperationResult... change peer region failed, errcode=" << errcode;
      if (errcode == Errno::EREGION_PEER_CHANGEING) {
        return true;
      }
      break;
    case RegionCmdType::CMD_PURGE:
      DINGO_LOG(ERROR) << "CheckStoreOperationResult... purge region failed, errcode=" << errcode;
      if (errcode == Errno::EREGION_NOT_FOUND) {
        return true;
      }
      break;
    default:
      DINGO_LOG(ERROR) << "CheckStoreOperationResult... unknown region cmd type " << cmd_type
                       << ", errcode=" << errcode;
      break;
  }

  return false;
}

/**
 * The `SendStoreOperation` function is responsible for sending a store operation to a specific store in the system.
 * It first checks the state of the store. If the store is in a normal state and its last seen timestamp is within the
 * heartbeat timeout, it proceeds with sending the operation. If the store is not in a normal state or its last seen
 * timestamp is beyond the heartbeat timeout, it logs a warning message and returns without sending the operation. If
 * the store is offline, it attempts to set the store to offline and returns. It then checks if the store operation
 * has any region commands. If not, it logs a debug message and returns without sending the operation. Note: The
 * actual sending of the store operation is not implemented in this function.
 *
 * @param store The store to which the operation is to be sent.
 * @param store_operation The operation to be sent to the store.
 * @param meta_increment A reference to a MetaIncrement object.
 */
void CoordinatorControl::SendStoreOperation(const pb::common::Store& store,
                                            const pb::coordinator::StoreOperation& store_operation,
                                            pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (store.state() == pb::common::StoreState::STORE_NORMAL) {
    if (store.last_seen_timestamp() + (FLAGS_store_heartbeat_timeout * 1000) < butil::gettimeofday_ms()) {
      DINGO_LOG(INFO) << "... update store " << store.id() << " state to offline";
      TrySetStoreToOffline(store.id());
      return;
    }
  } else {
    DINGO_LOG(WARNING) << "... store " << store.id() << " state is not STORE_NORMAL, will not send store_operation";
    return;
  }

  // send store_operation
  if (store_operation.region_cmds_size() <= 0) {
    DINGO_LOG(DEBUG) << "... store_operation.region_cmds_size() <= 0, store_id=" << store.id()
                     << " region_cmds_size=" << store_operation.region_cmds_size();
    return;
  }

  DINGO_LOG(INFO) << "... send store_operation to store " << store.id();

  // send store_operation to store
  // prepare request and response
  pb::push::PushStoreOperationRequest request;
  pb::push::PushStoreOperationResponse response;

  *(request.mutable_store_operation()) = store_operation;

  // send rpcs
  if (!store.has_server_location()) {
    DINGO_LOG(ERROR) << "... store " << store.id() << " has no server_location";
    return;
  }
  if (store.server_location().port() <= 0 || store.server_location().port() > 65535) {
    DINGO_LOG(ERROR) << "... store " << store.id() << " has invalid server_location.port "
                     << store.server_location().port();
    return;
  }

  auto status = RpcSendPushStoreOperation(store.server_location(), request, response);
  if (status.error_code() == pb::error::Errno::ESEND_STORE_OPERATION_FAIL) {
    DINGO_LOG(WARNING) << "... send store_operation to store " << store.id()
                       << " failed ESEND_STORE_OPERATION_FAIL, will try this store future";
    return;
  }

  // check response
  if (status.ok()) {
    DINGO_LOG(INFO) << "... send store_operation to store " << store.id()
                    << " all success, will delete these region_cmds, count: " << store_operation.region_cmds_size();
    // delete region_cmd
    for (const auto& region_cmd : store_operation.region_cmds()) {
      auto ret = RemoveRegionCmd(store_operation.id(), region_cmd.id(), meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "... region_cmd is success, but remove region_cmd failed, store_id=" << store.id()
                         << " region_cmd_id=" << region_cmd.id();
      } else {
        DINGO_LOG(INFO) << "... region_cmd is success, and remove region_cmd success, store_id=" << store.id()
                        << " region_cmd_id=" << region_cmd.id();
      }
    }

    // if (meta_increment.ByteSizeLong() > 0) {
    //   SubmitMetaIncrementSync(meta_increment);
    // }

    return;
  }

  if (response.region_cmd_results_size() <= 0) {
    DINGO_LOG(WARNING) << "... send store_operation to store " << store.id()
                       << " failed, but no region_cmd result, will try this store future, region_cmd_count: "
                       << store_operation.region_cmds_size();
    return;
  }

  DINGO_LOG(WARNING) << "... send store_operation to store " << store.id()
                     << " failed, will check each region_cmd result, region_cmd_count: "
                     << store_operation.region_cmds_size()
                     << ", region_cmd_result_count: " << response.region_cmd_results_size();

  for (const auto& it_cmd : response.region_cmd_results()) {
    auto errcode = it_cmd.error().errcode();
    auto cmd_type = it_cmd.region_cmd_type();

    // if a region_cmd response as NOT_LEADER, we need to add this region_cmd to store_operation of new store_id
    // again
    if (errcode == pb::error::Errno::ERAFT_NOTLEADER) {
      DINGO_LOG(INFO) << "... send store_operation to store_id=" << store.id()
                      << " region_cmd_id=" << it_cmd.region_cmd_id() << " result=[" << it_cmd.error().errcode() << "]["
                      << pb::error::Errno_descriptor()->FindValueByNumber(it_cmd.error().errcode())->name()
                      << " failed, will add this region_cmd to new store_operation, store_id: "
                      << it_cmd.error().store_id()
                      << ", leader_locaton: " << it_cmd.error().leader_location().ShortDebugString();

      // add region_cmd to new store_operation
      for (const auto& region_cmd : store_operation.region_cmds()) {
        if (region_cmd.id() == it_cmd.region_cmd_id()) {
          DINGO_LOG(INFO) << "... region_cmd_id=" << region_cmd.id()
                          << " is meet ERAFT_NOTLEADER, will add to new store_id: " << it_cmd.error().store_id()
                          << ", region_cmd: " << region_cmd.ShortDebugString();

          if (it_cmd.error().store_id() == 0) {
            DINGO_LOG(ERROR) << "... region_cmd_id=" << region_cmd.id()
                             << " is meet ERAFT_NOTLEADER, but new store_id is 0, will not add to new store_id";
            break;
          } else if (it_cmd.error().store_id() == store.id()) {
            DINGO_LOG(ERROR) << "... region_cmd_id=" << region_cmd.id()
                             << " is meet ERAFT_NOTLEADER, but new store_id is same as old store_id, will not add "
                                "to new store_id";
            break;
          }

          auto ret = MoveRegionCmd(store.id(), it_cmd.error().store_id(), it_cmd.region_cmd_id(), meta_increment);
          if (!ret.ok()) {
            DINGO_LOG(ERROR) << "... MoveRegionCmd failed, store_id=" << store.id()
                             << " region_cmd_id=" << it_cmd.region_cmd_id()
                             << " new_store_id=" << it_cmd.error().store_id();
          } else {
            DINGO_LOG(INFO) << "... MoveRegionCmd success, store_id=" << store.id()
                            << " region_cmd_id=" << it_cmd.region_cmd_id()
                            << " new_store_id=" << it_cmd.error().store_id();
          }

          // auto ret = AddRegionCmd(it_cmd.error().store_id(), region_cmd.job_id(), region_cmd, meta_increment);
          // if (!ret.ok()) {
          //   DINGO_LOG(ERROR) << "... add region_cmd failed for NOTLEADER re-routing, store_id=" << store.id()
          //                    << " region_cmd_id=" << region_cmd.id();
          // } else {
          //   // delete store_operation
          //   auto ret = RemoveRegionCmd(store.id(), it_cmd.region_cmd_id(), meta_increment);
          //   if (!ret.ok()) {
          //     DINGO_LOG(ERROR) << "... remove store_operation failed for NOTLEADER re-routing, store_id=" <<
          //     store.id()
          //                      << " region_cmd_id=" << it_cmd.region_cmd_id();
          //   } else {
          //     DINGO_LOG(INFO) << "... remove store_operation success for NOTLEADER re-routing, store_id=" <<
          //     store.id()
          //                     << " region_cmd_id=" << it_cmd.region_cmd_id();
          //   }
          // }
          break;
        }
      }

      continue;
    }

    auto need_delete = CheckStoreOperationResult(cmd_type, errcode);
    if (!need_delete) {
      DINGO_LOG(INFO) << "... send store_operation to store_id=" << store.id()
                      << " region_cmd_id=" << it_cmd.region_cmd_id() << "] errcode=["
                      << pb::error::Errno_Name(it_cmd.error().errcode()) << "] failed, will try this region_cmd future";
      // update region_cmd error
      for (const auto& region_cmd : store_operation.region_cmds()) {
        if (region_cmd.id() == it_cmd.region_cmd_id()) {
          auto ret = UpdateRegionCmd(store.id(), region_cmd, it_cmd.error(), meta_increment);
          if (!ret.ok()) {
            DINGO_LOG(ERROR) << "... update region_cmd failed, store_id=" << store.id()
                             << " region_cmd_id=" << region_cmd.id();
          }
          break;
        }
      }
    } else {
      DINGO_LOG(INFO) << "... send store_operation to store_id=" << store.id()
                      << " region_cmd_id=" << it_cmd.region_cmd_id() << " result=[" << it_cmd.error().errcode() << "]["
                      << pb::error::Errno_Name(it_cmd.error().errcode()) << " success, will delete this region_cmd";

      // delete store_operation
      auto ret = RemoveRegionCmd(store.id(), it_cmd.region_cmd_id(), meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "... remove store_operation failed, store_id=" << store.id()
                         << " region_cmd_id=" << it_cmd.region_cmd_id();
      }
    }
  }

  // if (meta_increment.ByteSizeLong() > 0) {
  //   SubmitMetaIncrementSync(meta_increment);
  // }
}

/**
 * The `TryToSendStoreOperations` function is responsible for preparing and sending store operations to stores in a
 * distributed system. It first updates the store state based on the last seen timestamp and prepares to send store
 * operations. The function does not update the store state to online, this is done in the `on_apply` of
 * `store_heartbeat`. It then retrieves the current state of the stores and shuffles them to ensure load distribution.
 * The function then prepares to send two types of store operations to the shuffled stores:
 * 1. `create_region` store operations.
 * 2. Other store operations.
 * Note: The actual sending of the operations is not implemented in this function.
 */
void CoordinatorControl::TryToSendStoreOperations() {
  // update store_state by last_seen_timestamp and send store operation to store
  // here we only update store_state to offline if last_seen_timestamp is too old
  // we will not update store_state to online here
  // in on_apply of store_heartbeat, we will update store_state to online
  pb::common::StoreMap store_map_temp;
  GetStoreMap(store_map_temp);
  std::vector<pb::common::Store> shuff_store_map;
  for (const auto& store : store_map_temp.stores()) {
    if (store.has_server_location() && store.has_raft_location()) {
      shuff_store_map.push_back(store);
    } else {
      DINGO_LOG(ERROR) << "... store " << store.id()
                       << " has no server_location or raft_location, store: " << store.ShortDebugString();
    }
  }
  std::shuffle(shuff_store_map.begin(), shuff_store_map.end(), CoordinatorControl::GetUrbg());

  // 1.send store_operation of create_region
  {
    pb::coordinator_internal::MetaIncrement meta_increment;

    std::map<int64_t, pb::common::Store> tmp_store_map;
    for (const auto& store : shuff_store_map) {
      tmp_store_map[store.id()] = store;
    }

    std::map<int64_t, std::vector<pb::coordinator::RegionCmd>> tmp_create_operation;

    for (const auto& store : shuff_store_map) {
      pb::coordinator::StoreOperation create_store_operation;
      int ret = GetStoreOperationOfCreateForSend(store.id(), create_store_operation);
      if (ret < 0) {
        DINGO_LOG(DEBUG) << "... no store_operation for store " << store.id();
        continue;
      }

      if (create_store_operation.region_cmds_size() <= 0) {
        DINGO_LOG(DEBUG) << "... store_operation.region_cmds_size() <= 0, store_id=" << store.id()
                         << " region_cmds_size=" << create_store_operation.region_cmds_size();
        continue;
      }

      std::vector<pb::coordinator::RegionCmd> region_cmds =
          Helper::PbRepeatedToVector(create_store_operation.region_cmds());

      std::shuffle(region_cmds.begin(), region_cmds.end(), CoordinatorControl::GetUrbg());

      tmp_create_operation[store.id()] = region_cmds;
    }

    for (int32_t i = 0; i < 1000; i++) {
      if (tmp_create_operation.empty()) {
        break;
      }

      DINGO_LOG(INFO) << "... send store_operation of create_region, loop: " << i++;

      for (auto& it : tmp_create_operation) {
        auto store_id = it.first;
        auto& region_cmds = it.second;

        if (region_cmds.empty()) {
          tmp_create_operation.erase(store_id);
          break;
        }

        auto region_cmd = region_cmds.back();
        region_cmds.pop_back();

        auto store_it = tmp_store_map.find(store_id);
        if (store_it == tmp_store_map.end()) {
          DINGO_LOG(ERROR) << "... store " << store_id << " not found";
          continue;
        }
        auto store = store_it->second;

        // send store_operation to store
        // prepare request and response
        pb::push::PushStoreOperationRequest request;
        pb::push::PushStoreOperationResponse response;

        pb::coordinator::StoreOperation store_operation;
        store_operation.set_id(store.id());
        *(store_operation.add_region_cmds()) = region_cmd;
        *(request.mutable_store_operation()) = store_operation;

        // send rpcs
        SendStoreOperation(store, store_operation, meta_increment);
      }
    }

    if (meta_increment.ByteSizeLong() > 0) {
      SubmitMetaIncrementSync(meta_increment);
    }
  }

  // 2.send other store_operation
  {
    pb::coordinator_internal::MetaIncrement meta_increment;

    for (const auto& store : shuff_store_map) {
      if (store.state() == pb::common::StoreState::STORE_NORMAL) {
        if (store.last_seen_timestamp() + (FLAGS_store_heartbeat_timeout * 1000) < butil::gettimeofday_ms()) {
          DINGO_LOG(INFO) << "... update store " << store.id() << " state to offline";
          TrySetStoreToOffline(store.id());
          continue;
        }
      } else {
        continue;
      }

      // send store_operation
      pb::coordinator::StoreOperation store_operation;
      int ret = GetStoreOperationOfNotCreateForSend(store.id(), store_operation);
      if (ret < 0) {
        DINGO_LOG(DEBUG) << "... no store_operation for store " << store.id();
        continue;
      }

      if (store_operation.region_cmds_size() <= 0) {
        DINGO_LOG(DEBUG) << "... store_operation.region_cmds_size() <= 0, store_id=" << store.id()
                         << " region_cmds_size=" << store_operation.region_cmds_size();
        continue;
      }

      DINGO_LOG(INFO) << "... send store_operation to store " << store.id();

      // send store_operation to store
      // prepare request and response
      pb::push::PushStoreOperationRequest request;
      pb::push::PushStoreOperationResponse response;

      *(request.mutable_store_operation()) = store_operation;

      // send rpcs
      SendStoreOperation(store, store_operation, meta_increment);
    }

    if (meta_increment.ByteSizeLong() > 0) {
      SubmitMetaIncrementSync(meta_increment);
    }
  }
}

/**
 * The `RpcSendPushStoreOperation` function is responsible for sending a `PushStoreOperation` request to a remote
 * store server. It first builds the remote server location string from the provided location. It then initializes a
 * BRPC channel to the remote server and sets a timeout for the RPC. The function sends a `PushStoreOperation` request
 * to the remote server and waits for a response. If the RPC fails, it logs an error message and retries the operation
 * up to a maximum number of times. If the RPC succeeds, it logs a success message and returns a success status. If
 * the RPC response indicates an error, it logs detailed error information including the error code and message, the
 * store ID, the number of region commands in the request, and the number of region command results in the response.
 *
 * @param location The location of the remote store server.
 * @param request The `PushStoreOperationRequest` to be sent to the remote store server.
 * @param response The `PushStoreOperationResponse` received from the remote store server.
 * @return A `butil::Status` indicating the result of the operation. If the operation was successful, the status is
 * OK. If the operation failed, the status indicates the error.
 */
butil::Status CoordinatorControl::RpcSendPushStoreOperation(const pb::common::Location& location,
                                                            pb::push::PushStoreOperationRequest& request,
                                                            pb::push::PushStoreOperationResponse& response) {
  // build send location string
  auto store_server_location_string = location.host() + ":" + std::to_string(location.port());

  int retry_times = 0;
  int max_retry_times = 3;

  do {
    braft::PeerId remote_node(store_server_location_string);

    // rpc
    brpc::Channel channel;
    if (channel.Init(remote_node.addr, nullptr) != 0) {
      DINGO_LOG(ERROR) << "... channel init failed";
      return butil::Status(pb::error::Errno::ESTORE_NOT_FOUND, "cannot connect store");
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(30000L);

    pb::push::PushService_Stub(&channel).PushStoreOperation(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
      DINGO_LOG(ERROR) << "... rpc failed, will retry, error code: " << cntl.ErrorCode()
                       << ", error message: " << cntl.ErrorText();
      continue;
    }

    auto errcode = response.error().errcode();
    if (errcode == pb::error::Errno::OK) {
      DINGO_LOG(INFO) << "... rpc success, will not retry, store_id: " << request.store_operation().id()
                      << ", region_cmd_count: " << request.store_operation().region_cmds_size();
      return butil::Status::OK();
    } else {
      DINGO_LOG(ERROR) << "... rpc failed, error code: " << response.error().errcode()
                       << ", error message: " << response.error().errmsg()
                       << ", store_id: " << request.store_operation().id()
                       << ", region_cmd_count: " << request.store_operation().region_cmds_size()
                       << ", region_cmd_result_count: " << response.region_cmd_results_size();
      for (const auto& it : response.region_cmd_results()) {
        DINGO_LOG(ERROR) << "... rpc failed, region_cmd_id: " << it.region_cmd_id()
                         << ", region_cmd_type: " << it.region_cmd_type() << ", error code: " << it.error().errcode()
                         << ", error message: " << it.error().errmsg();
      }
      return butil::Status(response.error().errcode(), response.error().errmsg());
    }
  } while (++retry_times < max_retry_times);

  return butil::Status(pb::error::Errno::ESEND_STORE_OPERATION_FAIL,
                       "connect with store server fail, no leader found or connect timeout, retry count: %d",
                       retry_times);
}

butil::Status CoordinatorControl::UpdateRegionCmdStatus(int64_t task_list_id, int64_t region_cmd_id,
                                                        pb::coordinator::RegionCmdStatus status, pb::error::Error error,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "UpdateRegionCmd task_list_id=" << task_list_id << " region_cmd_id=" << region_cmd_id
                  << " status=" << pb::coordinator::RegionCmdStatus_Name(status)
                  << " error=" << error.ShortDebugString();

  pb::coordinator::TaskList task_list;
  auto ret = task_list_map_.Get(task_list_id, task_list);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "task_list_map_.Get(" << task_list_id << ") failed";
    return butil::Status::OK();
  }

  if (task_list.id() <= 0) {
    DINGO_LOG(WARNING) << "task_list_map_.Get(" << task_list_id << ") failed, task_list.id() <= 0";
    return butil::Status::OK();
  }

  auto* task_list_increment = meta_increment.add_task_lists();
  task_list_increment->set_id(task_list.id());
  task_list_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  *(task_list_increment->mutable_task_list()) = task_list;
  task_list_increment->set_is_partial_update(true);
  auto* region_cmd_status = task_list_increment->add_region_cmds_status();
  region_cmd_status->set_region_cmd_id(region_cmd_id);
  region_cmd_status->set_status(status);
  *(region_cmd_status->mutable_error()) = error;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CheckRegionAllPeerOnline(int64_t region_id) {
  pb::common::RegionMetrics region_metrics;
  auto ret = region_metrics_map_.Get(region_id, region_metrics);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "region_metrics_map_.Get(" << region_id << ") failed";
    return butil::Status(pb::error::EINTERNAL, "region_metrics_map_.Get failed");
  }

  if (region_metrics.id() <= 0) {
    DINGO_LOG(ERROR) << "region_metrics_map_.Get(" << region_id << ") failed, region_metrics.id() <= 0";
    return butil::Status(pb::error::EINTERNAL, "region_metrics_map_.Get failed");
  }

  if (region_metrics.region_definition().peers_size() <= 0) {
    DINGO_LOG(ERROR) << "region_metrics_map_.Get(" << region_id
                     << ") failed, region_metrics.region_definition.peers_size() <= 0";
    return butil::Status(pb::error::EINTERNAL, "region_metrics_map_.Get failed");
  }

  for (const auto& peer : region_metrics.region_definition().peers()) {
    pb::common::Store store;
    auto ret1 = store_map_.Get(peer.store_id(), store);
    if (ret1 < 0) {
      DINGO_LOG(ERROR) << "store_map_.Get(" << peer.store_id() << ") failed";
      return butil::Status(pb::error::EINTERNAL, "store_map_.Get failed");
    }

    if (store.state() != pb::common::StoreState::STORE_NORMAL) {
      DINGO_LOG(INFO) << "store " << store.id() << " state is not STORE_NORMAL, region_id=" << region_id;
      return butil::Status(pb::error::EBRAFT_EINVAL, "store state is not STORE_NORMAL");
    }
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CheckRegionLeaderOnline(int64_t region_id) {
  pb::common::RegionMetrics region_metrics;
  auto ret = region_metrics_map_.Get(region_id, region_metrics);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "region_metrics_map_.Get(" << region_id << ") failed";
    return butil::Status(pb::error::EINTERNAL, "region_metrics_map_.Get failed");
  }

  if (region_metrics.id() <= 0) {
    DINGO_LOG(ERROR) << "region_metrics_map_.Get(" << region_id << ") failed, region_metrics.id() <= 0";
    return butil::Status(pb::error::EINTERNAL, "region_metrics_map_.Get failed");
  }

  if (region_metrics.leader_store_id() <= 0) {
    DINGO_LOG(ERROR) << "region_metrics_map_.Get(" << region_id << ") failed, region_metrics.leader_store_id() <= 0";
    return butil::Status(pb::error::EINTERNAL, "region_metrics_map_.Get failed");
  }

  pb::common::Store store;
  auto ret2 = store_map_.Get(region_metrics.leader_store_id(), store);
  if (ret2 < 0) {
    DINGO_LOG(ERROR) << "store_map_.Get(" << region_metrics.leader_store_id() << ") failed";
    return butil::Status(pb::error::EINTERNAL, "store_map_.Get failed");
  }

  if (store.state() != pb::common::StoreState::STORE_NORMAL) {
    DINGO_LOG(INFO) << "store " << store.id() << " state is not STORE_NORMAL, region_id=" << region_id;
    return butil::Status(pb::error::EBRAFT_EINVAL, "store state is not STORE_NORMAL");
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CheckStoreNormal(int64_t store_id) {
  pb::common::Store store;
  auto ret = store_map_.Get(store_id, store);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "store_map_.Get(" << store_id << ") failed";
    return butil::Status(pb::error::EINTERNAL, "store_map_.Get failed");
  }

  if (store.state() != pb::common::StoreState::STORE_NORMAL) {
    DINGO_LOG(INFO) << "store " << store.id() << " state is not STORE_NORMAL";
    return butil::Status(pb::error::EBRAFT_EINVAL, "store state is not STORE_NORMAL");
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::UpdateForceReadOnly(bool is_force_read_only,
                                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "UpdateForceReadOnly is_force_read_only = " << is_force_read_only;

  // update gc_stop
  pb::coordinator_internal::CommonInternal element;
  auto ret1 = common_mem_meta_->Get(Constant::kForceReadOnlyKey, element);
  if (!ret1.ok()) {
    DINGO_LOG(ERROR) << "common_mem_meta_->Get(Constant::kForceReadOnlyKey) failed, errcode=" << ret1.error_code()
                     << " errmsg=" << ret1.error_str();
    return ret1;
  }

  DINGO_LOG(INFO) << "UpdateForceReadOnly old_force_read_only=" << element.value();

  bool old_is_force_read_only = (element.value() == Constant::kForceReadOnlyValueTrue);
  if (old_is_force_read_only == is_force_read_only) {
    DINGO_LOG(INFO) << "UpdateForceReadOnly old_is_force_read_only=" << old_is_force_read_only
                    << " == is_force_read_only=" << is_force_read_only << ", skip update";
    return butil::Status::OK();
  }

  element.set_id(Constant::kForceReadOnlyKey);
  if (is_force_read_only) {
    element.set_value(Constant::kForceReadOnlyValueTrue);
  } else {
    element.set_value(Constant::kForceReadOnlyValueFalse);
  }

  auto* increment = meta_increment.add_common_mem_s();
  increment->set_id(Constant::kForceReadOnlyKey);
  increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  *(increment->mutable_common()) = element;

  DINGO_LOG(INFO) << "UpdateForceReadOnly new_force_read_only=" << element.value();

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetForceReadOnly(bool& is_force_read_only) {
  // update gc_stop
  pb::coordinator_internal::CommonInternal element;
  auto ret1 = common_mem_meta_->Get(Constant::kForceReadOnlyKey, element);
  if (!ret1.ok()) {
    DINGO_LOG(ERROR) << "common_mem_meta_->Get(Constant::kForceReadOnlyKey) failed, errcode=" << ret1.error_code()
                     << " errmsg=" << ret1.error_str();
    return ret1;
  }

  DINGO_LOG(DEBUG) << "GetForceReadOnly old_force_read_only=" << element.value();

  is_force_read_only = (element.value() == Constant::kForceReadOnlyValueTrue);

  return butil::Status::OK();
}

bool CoordinatorControl::GetForceReadOnly() {
  bool is_force_read_only = false;
  GetForceReadOnly(is_force_read_only);
  return is_force_read_only;
}

}  // namespace dingodb

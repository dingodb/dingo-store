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

#include "store/heartbeat.h"

#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "butil/status.h"
#include "butil/time.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/push.pb.h"
#include "server/server.h"
#include "server/service_helper.h"
#include "store/region_controller.h"

namespace dingodb {

DEFINE_int32(executor_heartbeat_timeout, 30, "executor heartbeat timeout in seconds");
DEFINE_int32(store_heartbeat_timeout, 30, "store heartbeat timeout in seconds");
DEFINE_int32(region_heartbeat_timeout, 30, "region heartbeat timeout in seconds");
DEFINE_int32(region_delete_after_deleted_time, 604800, "delete region after deleted time in seconds");

void HeartbeatTask::SendStoreHeartbeat(std::shared_ptr<CoordinatorInteraction> coordinator_interaction,
                                       std::vector<uint64_t> region_ids, bool is_update_epoch_version) {
  auto start_time = Helper::TimestampMs();
  auto engine = Server::GetInstance()->GetEngine();
  auto raft_store_engine = Server::GetInstance()->GetRaftStoreEngine();

  pb::coordinator::StoreHeartbeatRequest request;

  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();

  request.set_self_storemap_epoch(store_meta_manager->GetStoreServerMeta()->GetEpoch());
  // request.set_self_regionmap_epoch(store_meta_manager->GetStoreRegionMeta()->GetEpoch());

  // store
  *(request.mutable_store()) = (*store_meta_manager->GetStoreServerMeta()->GetStore(Server::GetInstance()->Id()));

  // store_metrics
  auto metrics_manager = Server::GetInstance()->GetStoreMetricsManager();
  auto* mut_store_metrics = request.mutable_store_metrics();
  *mut_store_metrics = (*metrics_manager->GetStoreMetrics()->Metrics());
  // setup id for store_metrics here, coordinator need this id to update store_metrics
  mut_store_metrics->set_id(Server::GetInstance()->Id());
  mut_store_metrics->set_is_update_epoch_version(is_update_epoch_version);

  auto* mut_region_metrics_map = mut_store_metrics->mutable_region_metrics_map();
  auto region_metrics = metrics_manager->GetStoreRegionMetrics();
  std::vector<store::RegionPtr> region_metas;
  if (region_ids.empty()) {
    region_metas = store_meta_manager->GetStoreRegionMeta()->GetAllRegion();
  } else {
    mut_store_metrics->set_is_partial_region_metrics(true);
    for (auto region_id : region_ids) {
      auto region_meta = store_meta_manager->GetStoreRegionMeta()->GetRegion(region_id);
      if (region_meta != nullptr) {
        region_metas.push_back(region_meta);
      }
    }
  }
  for (const auto& region_meta : region_metas) {
    pb::common::RegionMetrics tmp_region_metrics;
    auto metrics = region_metrics->GetMetrics(region_meta->Id());
    if (metrics != nullptr) {
      tmp_region_metrics = metrics->InnerRegionMetrics();
    }

    tmp_region_metrics.set_id(region_meta->Id());
    tmp_region_metrics.set_leader_store_id(region_meta->LeaderId());
    tmp_region_metrics.set_store_region_state(region_meta->State());
    *(tmp_region_metrics.mutable_region_definition()) = region_meta->InnerRegion().definition();
    tmp_region_metrics.set_snapshot_epoch_version(region_meta->InnerRegion().snapshot_epoch_version());

    if ((region_meta->State() == pb::common::StoreRegionState::NORMAL ||
         region_meta->State() == pb::common::StoreRegionState::STANDBY ||
         region_meta->State() == pb::common::StoreRegionState::SPLITTING ||
         region_meta->State() == pb::common::StoreRegionState::MERGING) &&
        raft_store_engine != nullptr) {
      auto raft_node = raft_store_engine->GetNode(region_meta->Id());
      if (raft_node != nullptr) {
        *(tmp_region_metrics.mutable_braft_status()) = (*raft_node->GetStatus());
      }
    }

    auto vector_index_wrapper = region_meta->VectorIndexWrapper();
    if (vector_index_wrapper != nullptr) {
      auto* vector_index_status = tmp_region_metrics.mutable_vector_index_status();
      vector_index_status->set_is_ready(vector_index_wrapper->IsReady());
      vector_index_status->set_is_stop(vector_index_wrapper->IsStop());
      vector_index_status->set_is_build_error(vector_index_wrapper->IsBuildError());
      vector_index_status->set_is_rebuild_error(vector_index_wrapper->IsRebuildError());
      vector_index_status->set_apply_log_id(vector_index_wrapper->ApplyLogId());
      vector_index_status->set_snapshot_log_id(vector_index_wrapper->SnapshotLogId());
      vector_index_status->set_is_switching(vector_index_wrapper->IsSwitchingVectorIndex());
      // vector_index_status->set_is_hold_vector_index(vector_index_wrapper->GetOwnVectorIndex() != nullptr);
      vector_index_status->set_is_hold_vector_index(vector_index_wrapper->IsHoldVectorIndex());
    }

    mut_region_metrics_map->insert({region_meta->Id(), tmp_region_metrics});
  }

  DINGO_LOG(INFO) << fmt::format("[heartbeat.store] request region count({}) size({}) elapsed time({} ms)",
                                 mut_region_metrics_map->size(), request.ByteSizeLong(),
                                 Helper::TimestampMs() - start_time);
  start_time = Helper::TimestampMs();
  pb::coordinator::StoreHeartbeatResponse response;
  auto status = coordinator_interaction->SendRequest("StoreHeartbeat", request, response);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[heartbeat.store] store heartbeat failed, error: {} {}",
                                      pb::error::Errno_Name(status.error_code()), status.error_str());
    return;
  }

  DINGO_LOG(INFO) << fmt::format("[heartbeat.store] response size({}) elapsed time({} ms)", response.ByteSizeLong(),
                                 Helper::TimestampMs() - start_time);

  HeartbeatTask::HandleStoreHeartbeatResponse(store_meta_manager, response);
}

static std::vector<std::shared_ptr<pb::common::Store>> GetNewStore(
    std::map<uint64_t, std::shared_ptr<pb::common::Store>> local_stores,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Store>& remote_stores) {
  std::vector<std::shared_ptr<pb::common::Store>> new_stores;
  for (const auto& remote_store : remote_stores) {
    if (local_stores.find(remote_store.id()) == local_stores.end()) {
      new_stores.push_back(std::make_shared<pb::common::Store>(remote_store));
    }
  }

  return new_stores;
}

static std::vector<std::shared_ptr<pb::common::Store>> GetChangedStore(
    std::map<uint64_t, std::shared_ptr<pb::common::Store>> local_stores,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Store>& remote_stores) {
  std::vector<std::shared_ptr<pb::common::Store>> changed_stores;
  for (const auto& remote_store : remote_stores) {
    if (remote_store.id() == 0) {
      continue;
    }
    auto it = local_stores.find(remote_store.id());
    if (it != local_stores.end()) {
      if (it->second->raft_location().host() != remote_store.raft_location().host() ||
          it->second->raft_location().port() != remote_store.raft_location().port()) {
        changed_stores.push_back(std::make_shared<pb::common::Store>(remote_store));
      }
    }
  }

  return changed_stores;
}

static std::vector<std::shared_ptr<pb::common::Store>> GetDeletedStore(
    std::map<uint64_t, std::shared_ptr<pb::common::Store>> local_stores,
    const google::protobuf::RepeatedPtrField<pb::common::Store>& remote_stores) {
  std::set<uint64_t> remote_store_ids;
  for (const auto& store : remote_stores) {
    remote_store_ids.insert(store.id());
  }

  std::vector<std::shared_ptr<pb::common::Store>> stores_to_delete;
  for (const auto& store : local_stores) {
    if (remote_store_ids.find(store.first) == remote_store_ids.end()) {
      stores_to_delete.push_back(store.second);
    }
  }

  return stores_to_delete;
}

void HeartbeatTask::HandleStoreHeartbeatResponse(std::shared_ptr<dingodb::StoreMetaManager> store_meta_manager,
                                                 const pb::coordinator::StoreHeartbeatResponse& response) {
  // Handle store meta data.
  auto store_server_meta = store_meta_manager->GetStoreServerMeta();
  auto local_stores = store_server_meta->GetAllStore();
  auto remote_stores = response.storemap().stores();

  auto new_stores = GetNewStore(local_stores, remote_stores);
  DINGO_LOG(INFO) << fmt::format("[heartbeat.store] new store size: {} / {}", new_stores.size(), local_stores.size());
  for (const auto& store : new_stores) {
    store_server_meta->AddStore(store);
  }

  auto changed_stores = GetChangedStore(local_stores, remote_stores);
  DINGO_LOG(INFO) << fmt::format("[heartbeat.store] changed store size: {} / {}", changed_stores.size(),
                                 local_stores.size());
  for (const auto& store : changed_stores) {
    store_server_meta->UpdateStore(store);
  }

  auto deleted_stores = GetDeletedStore(local_stores, remote_stores);
  DINGO_LOG(INFO) << fmt::format("[heartbeat.store] deleted store size: {} / {}", deleted_stores.size(),
                                 local_stores.size());
  for (const auto& store : deleted_stores) {
    store_server_meta->DeleteStore(store->id());
  }

  // set up read-only
  auto is_read_only = Server::GetInstance()->IsReadOnly();
  if (is_read_only != response.cluster_state().cluster_is_read_only()) {
    Server::GetInstance()->SetReadOnly(response.cluster_state().cluster_is_read_only());
    DINGO_LOG(WARNING) << fmt::format("[heartbeat.store] cluster set read-only to {}", is_read_only);
  }
}

static std::atomic<bool> g_store_recycle_orphan_running(false);
void CoordinatorRecycleOrphanTask::CoordinatorRecycleOrphan(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "CoordinatorRecycleOrphan... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CoordinatorRecycleOrphan... this is leader";

  if (g_store_recycle_orphan_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "CoordinatorRecycleOrphan... g_store_recycle_orphan_running is true, return";
    return;
  }

  AtomicGuard guard(g_store_recycle_orphan_running);

  coordinator_control->RecycleOrphanRegionOnStore();

  coordinator_control->RecycleOrphanRegionOnCoordinator();

  coordinator_control->RecycleDeletedTableAndIndex();

  coordinator_control->RemoveOneTimeWatch();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_update_state_running(false);
void CoordinatorUpdateStateTask::CoordinatorUpdateState(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is leader";

  if (g_coordinator_update_state_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "CoordinatorUpdateState... g_coordinator_update_state_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_update_state_running);

  // update executor_state by last_seen_timestamp
  pb::common::ExecutorMap executor_map_temp;
  coordinator_control->GetExecutorMap(executor_map_temp);
  for (const auto& it : executor_map_temp.executors()) {
    if (it.state() == pb::common::ExecutorState::EXECUTOR_NORMAL) {
      if (it.last_seen_timestamp() + (FLAGS_executor_heartbeat_timeout * 1000) < butil::gettimeofday_ms()) {
        DINGO_LOG(INFO) << "CoordinatorUpdateState... update executor " << it.id() << " state to offline";
        coordinator_control->TrySetExecutorToOffline(it.id());
        continue;
      }
    } else {
      continue;
    }
  }

  coordinator_control->UpdateRegionState();

  // now update store state in SendCoordinatorPushToStore

  // update store_state by last_seen_timestamp and send store operation to store
  // here we only update store_state to offline if last_seen_timestamp is too old
  // we will not update store_state to online here
  // in on_apply of store_heartbeat, we will update store_state to online
  // pb::common::StoreMap store_map_temp;
  // coordinator_control->GetStoreMap(store_map_temp);
  // for (const auto& it : store_map_temp.stores()) {
  //   if (it.state() == pb::common::StoreState::STORE_NORMAL) {
  //     if (it.last_seen_timestamp() + (60 * 1000) < butil::gettimeofday_ms()) {
  //       DINGO_LOG(INFO) << "SendCoordinatorPushToStore... update store " << it.id() << " state to offline";
  //       coordinator_control->TrySetStoreToOffline(it.id());
  //       continue;
  //     }
  //   } else {
  //     continue;
  //   }
  // }

  // update cluster is_read_only
  coordinator_control->UpdateClusterReadOnly();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_task_list_process_running(false);
void CoordinatorTaskListProcessTask::CoordinatorTaskListProcess(
    std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is leader";

  if (g_coordinator_task_list_process_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "CoordinatorTaskListProcess... g_coordinator_task_list_process_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_task_list_process_running);

  coordinator_control->ProcessTaskList();
}

bool CheckStoreOperationResult(pb::coordinator::RegionCmdType cmd_type, pb::error::Errno errcode) {
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

// this is for coordinator
static std::atomic<bool> g_coordinator_push_to_store_running(false);
void CoordinatorPushTask::SendCoordinatorPushToStore(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "... this is leader";

  if (g_coordinator_push_to_store_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "... g_coordinator_push_to_store_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_push_to_store_running);

  // update store_state by last_seen_timestamp and send store operation to store
  // here we only update store_state to offline if last_seen_timestamp is too old
  // we will not update store_state to online here
  // in on_apply of store_heartbeat, we will update store_state to online
  pb::common::StoreMap store_map_temp;
  coordinator_control->GetStoreMap(store_map_temp);
  for (const auto& it : store_map_temp.stores()) {
    if (it.state() == pb::common::StoreState::STORE_NORMAL) {
      if (it.last_seen_timestamp() + (FLAGS_store_heartbeat_timeout * 1000) < butil::gettimeofday_ms()) {
        DINGO_LOG(INFO) << "... update store " << it.id() << " state to offline";
        coordinator_control->TrySetStoreToOffline(it.id());
        continue;
      }
    } else {
      continue;
    }

    // send store_operation
    pb::coordinator::StoreOperation store_operation;
    int ret = coordinator_control->GetStoreOperationForSend(it.id(), store_operation);
    if (ret < 0) {
      DINGO_LOG(DEBUG) << "... no store_operation for store " << it.id();
      continue;
    }

    if (store_operation.region_cmds_size() <= 0) {
      DINGO_LOG(DEBUG) << "... store_operation.region_cmds_size() <= 0, store_id=" << it.id()
                       << " region_cmds_size=" << store_operation.region_cmds_size();
      continue;
    }

    DINGO_LOG(INFO) << "... send store_operation to store " << it.id();

    // send store_operation to store
    // prepare request and response
    pb::push::PushStoreOperationRequest request;
    pb::push::PushStoreOperationResponse response;

    *(request.mutable_store_operation()) = store_operation;

    // send rpcs
    if (!it.has_server_location()) {
      DINGO_LOG(ERROR) << "... store " << it.id() << " has no server_location";
      continue;
    }
    if (it.server_location().port() <= 0 || it.server_location().port() > 65535) {
      DINGO_LOG(ERROR) << "... store " << it.id() << " has invalid server_location.port "
                       << it.server_location().port();
      continue;
    }

    pb::coordinator_internal::MetaIncrement meta_increment;

    auto status = Heartbeat::RpcSendPushStoreOperation(it.server_location(), request, response);
    if (status.error_code() == pb::error::Errno::ESEND_STORE_OPERATION_FAIL) {
      DINGO_LOG(WARNING) << "... send store_operation to store " << it.id()
                         << " failed ESEND_STORE_OPERATION_FAIL, will try this store future";
      continue;
    }

    // check response
    if (status.ok()) {
      DINGO_LOG(INFO) << "... send store_operation to store " << it.id()
                      << " all success, will delete these region_cmds, count: " << store_operation.region_cmds_size();
      // delete region_cmd
      for (const auto& region_cmd : store_operation.region_cmds()) {
        auto ret = coordinator_control->RemoveRegionCmd(store_operation.id(), region_cmd.id(), meta_increment);
        if (!ret.ok()) {
          DINGO_LOG(ERROR) << "... remove region_cmd failed, store_id=" << it.id()
                           << " region_cmd_id=" << region_cmd.id();
        }
      }

      if (meta_increment.ByteSizeLong() > 0) {
        coordinator_control->SubmitMetaIncrementSync(meta_increment);
      }

      continue;
    }

    if (response.region_cmd_results_size() <= 0) {
      DINGO_LOG(WARNING) << "... send store_operation to store " << it.id()
                         << " failed, but no region_cmd result, will try this store future, region_cmd_count: "
                         << store_operation.region_cmds_size();
      continue;
    }

    DINGO_LOG(WARNING) << "... send store_operation to store " << it.id()
                       << " failed, will check each region_cmd result, region_cmd_count: "
                       << store_operation.region_cmds_size()
                       << ", region_cmd_result_count: " << response.region_cmd_results_size();

    for (const auto& it_cmd : response.region_cmd_results()) {
      auto errcode = it_cmd.error().errcode();
      auto cmd_type = it_cmd.region_cmd_type();

      // if a region_cmd response as NOT_LEADER, we need to add this region_cmd to store_operation of new store_id again
      if (errcode == pb::error::Errno::ERAFT_NOTLEADER) {
        DINGO_LOG(INFO) << "... send store_operation to store_id=" << it.id()
                        << " region_cmd_id=" << it_cmd.region_cmd_id() << " result=[" << it_cmd.error().errcode()
                        << "][" << pb::error::Errno_descriptor()->FindValueByNumber(it_cmd.error().errcode())->name()
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
            } else if (it_cmd.error().store_id() == it.id()) {
              DINGO_LOG(ERROR)
                  << "... region_cmd_id=" << region_cmd.id()
                  << " is meet ERAFT_NOTLEADER, but new store_id is same as old store_id, will not add to new store_id";
              break;
            }

            auto ret = coordinator_control->AddRegionCmd(it_cmd.error().store_id(), region_cmd, meta_increment);
            if (!ret.ok()) {
              DINGO_LOG(ERROR) << "... add region_cmd failed for NOTLEADER re-routing, store_id=" << it.id()
                               << " region_cmd_id=" << region_cmd.id();
            } else {
              // delete store_operation
              auto ret = coordinator_control->RemoveRegionCmd(it.id(), it_cmd.region_cmd_id(), meta_increment);
              if (!ret.ok()) {
                DINGO_LOG(ERROR) << "... remove store_operation failed for NOTLEADER re-routing, store_id=" << it.id()
                                 << " region_cmd_id=" << it_cmd.region_cmd_id();
              } else {
                DINGO_LOG(INFO) << "... remove store_operation success for NOTLEADER re-routing, store_id=" << it.id()
                                << " region_cmd_id=" << it_cmd.region_cmd_id();
              }
            }
            break;
          }
        }

        continue;
      }

      auto need_delete = CheckStoreOperationResult(cmd_type, errcode);
      if (!need_delete) {
        DINGO_LOG(INFO) << "... send store_operation to store_id=" << it.id()
                        << " region_cmd_id=" << it_cmd.region_cmd_id() << "] errcode=["
                        << pb::error::Errno_Name(it_cmd.error().errcode())
                        << "] failed, will try this region_cmd future";
        // update region_cmd error
        for (const auto& region_cmd : store_operation.region_cmds()) {
          if (region_cmd.id() == it_cmd.region_cmd_id()) {
            auto ret = coordinator_control->UpdateRegionCmd(it.id(), region_cmd, it_cmd.error(), meta_increment);
            if (!ret.ok()) {
              DINGO_LOG(ERROR) << "... update region_cmd failed, store_id=" << it.id()
                               << " region_cmd_id=" << region_cmd.id();
            }
            break;
          }
        }
      } else {
        DINGO_LOG(INFO) << "... send store_operation to store_id=" << it.id()
                        << " region_cmd_id=" << it_cmd.region_cmd_id() << " result=[" << it_cmd.error().errcode()
                        << "][" << pb::error::Errno_Name(it_cmd.error().errcode())
                        << " success, will delete this region_cmd";

        // delete store_operation
        auto ret = coordinator_control->RemoveRegionCmd(it.id(), it_cmd.region_cmd_id(), meta_increment);
        if (!ret.ok()) {
          DINGO_LOG(ERROR) << "... remove store_operation failed, store_id=" << it.id()
                           << " region_cmd_id=" << it_cmd.region_cmd_id();
        }
      }
    }

    if (meta_increment.ByteSizeLong() > 0) {
      coordinator_control->SubmitMetaIncrementSync(meta_increment);
    }
  }

  butil::FlatMap<uint64_t, pb::common::Store> store_to_push;
  store_to_push.init(1000, 80);  // notice: FlagMap must init before use
  coordinator_control->GetPushStoreMap(store_to_push);

  if (store_to_push.empty()) {
    // DINGO_LOG(INFO) << "SendCoordinatorPushToStore... No store to push";
    return;
  }

  // generate new heartbeat response
  pb::coordinator::StoreHeartbeatResponse heartbeat_response;
  {
    // auto* new_regionmap = heartbeat_response.mutable_regionmap();
    // coordinator_control->GetRegionMap(*new_regionmap);

    auto* new_storemap = heartbeat_response.mutable_storemap();
    coordinator_control->GetStoreMap(*new_storemap);

    heartbeat_response.set_storemap_epoch(new_storemap->epoch());
    // heartbeat_response.set_regionmap_epoch(new_regionmap->epoch());

    DINGO_LOG(INFO) << "will send to store with response:" << heartbeat_response.ShortDebugString();
  }

  // prepare request and response
  pb::push::PushHeartbeatRequest request;
  pb::push::PushHeartbeatResponse response;

  auto* heart_response_to_send = request.mutable_heartbeat_response();
  *heart_response_to_send = heartbeat_response;

  // send heartbeat to all stores need to push
  for (const auto& store_pair : store_to_push) {
    const pb::common::Store& store_need_send = store_pair.second;
    const pb::common::Location& store_server_location = store_need_send.server_location();

    if (store_server_location.host().length() <= 0 || store_server_location.port() <= 0) {
      DINGO_LOG(ERROR) << "illegal store_server_location=" << store_server_location.host() << ":"
                       << store_server_location.port();
      return;
    }

    // build send location string
    auto store_server_location_string =
        store_server_location.host() + ":" + std::to_string(store_server_location.port());

    // send rpc
    braft::PeerId remote_node(store_server_location_string);

    // rpc
    brpc::Channel channel;
    if (channel.Init(remote_node.addr, nullptr) != 0) {
      DINGO_LOG(ERROR) << "Fail to init channel to " << remote_node;
      return;
    }

    // start rpc
    pb::push::PushService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(500L);

    stub.PushHeartbeat(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
      DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
      return;
    }

    DINGO_LOG(DEBUG) << "SendCoordinatorPushToStore to " << store_server_location_string
                     << " response latency=" << cntl.latency_us() << " msg=" << response.DebugString();
  }
}

// this is for coordinator
static std::atomic<bool> g_coordinator_calc_metrics_running(false);
void CalculateTableMetricsTask::CalculateTableMetrics(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    // DINGO_LOG(INFO) << "SendCoordinatorPushToStore... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CalculateTableMetrics... this is leader";

  if (g_coordinator_calc_metrics_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "CalculateTableMetrics... g_coordinator_calc_metrics_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_calc_metrics_running);

  coordinator_control->CalculateTableMetrics();
  coordinator_control->CalculateIndexMetrics();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_lease_running(false);
void LeaseTask::ExecLeaseTask(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    // DINGO_LOG(INFO) << "ExecLeaseTask... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "ExecLeaseTask... this is leader";

  if (g_coordinator_lease_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "ExecLeaseTask... g_coordinator_lease_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_lease_running);

  coordinator_control->LeaseTask();
}

// this is for coordinator
static std::atomic<bool> g_coordinator_compaction_running(false);
void CompactionTask::ExecCompactionTask(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    // DINGO_LOG(INFO) << "ExecCompactionTask... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "ExecCompactionTask... this is leader";

  if (g_coordinator_compaction_running.load(std::memory_order_relaxed)) {
    DINGO_LOG(INFO) << "ExecCompactionTask... g_coordinator_compaction_running is true, return";
    return;
  }

  AtomicGuard guard(g_coordinator_compaction_running);

  coordinator_control->CompactionTask();
}

// this is for index
void VectorIndexScrubTask::ScrubVectorIndex() {
  auto status = VectorIndexManager::ScrubVectorIndex();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("Scrub vector index failed, error: {}", status.error_str());
  }
}

bool Heartbeat::Init() {
  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;

  if (bthread::execution_queue_start(&queue_id_, &options, ExecuteRoutine, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Start heartbeat execution queue failed";
    return false;
  }

  is_available_.store(true, std::memory_order_relaxed);

  return true;
}

void Heartbeat::Destroy() {
  is_available_.store(false, std::memory_order_relaxed);

  if (bthread::execution_queue_stop(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "heartbeat execution queue stop failed";
    return;
  }

  if (bthread::execution_queue_join(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "heartbeat execution queue join failed";
  }
}

bool Heartbeat::Execute(TaskRunnable* task) {
  if (!is_available_.load(std::memory_order_relaxed)) {
    DINGO_LOG(ERROR) << "Heartbeat execute queue is not available.";
    return false;
  }

  if (bthread::execution_queue_execute(queue_id_, task) != 0) {
    DINGO_LOG(ERROR) << "heartbeat execution queue execute failed";
    return false;
  }

  return true;
}

void Heartbeat::TriggerStoreHeartbeat(std::vector<uint64_t> region_ids, bool is_update_epoch_version) {
  // Free at ExecuteRoutine()
  TaskRunnable* task =
      new HeartbeatTask(Server::GetInstance()->GetCoordinatorInteraction(), region_ids, is_update_epoch_version);
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

void Heartbeat::TriggerCoordinatorPushToStore(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new CoordinatorPushTask(Server::GetInstance()->GetCoordinatorControl());
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

void Heartbeat::TriggerCoordinatorUpdateState(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new CoordinatorUpdateStateTask(Server::GetInstance()->GetCoordinatorControl());
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

void Heartbeat::TriggerCoordinatorTaskListProcess(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new CoordinatorTaskListProcessTask(Server::GetInstance()->GetCoordinatorControl());
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

void Heartbeat::TriggerCoordinatorRecycleOrphan(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new CoordinatorRecycleOrphanTask(Server::GetInstance()->GetCoordinatorControl());
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

void Heartbeat::TriggerCalculateTableMetrics(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new CalculateTableMetricsTask(Server::GetInstance()->GetCoordinatorControl());
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

void Heartbeat::TriggerLeaseTask(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new LeaseTask(Server::GetInstance()->GetCoordinatorControl());
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

void Heartbeat::TriggerCompactionTask(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new CompactionTask(Server::GetInstance()->GetCoordinatorControl());
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

void Heartbeat::TriggerScrubVectorIndex(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new VectorIndexScrubTask();
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

butil::Status Heartbeat::RpcSendPushStoreOperation(const pb::common::Location& location,
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
      return butil::Status(response.error().errcode(), response.error().errmsg());
    }
  } while (++retry_times < max_retry_times);

  return butil::Status(pb::error::Errno::ESEND_STORE_OPERATION_FAIL,
                       "connect with store server fail, no leader found or connect timeout, retry count: %d",
                       retry_times);
}

}  // namespace dingodb

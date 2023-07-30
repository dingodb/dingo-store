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
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/push.pb.h"
#include "server/server.h"

namespace dingodb {

DEFINE_int32(executor_heartbeat_timeout, 30, "executor heartbeat timeout in seconds");
DEFINE_int32(store_heartbeat_timeout, 30, "store heartbeat timeout in seconds");
DEFINE_int32(region_heartbeat_timeout, 30, "region heartbeat timeout in seconds");
DEFINE_int32(region_delete_after_deleted_time, 604800, "delete region after deleted time in seconds");

void HeartbeatTask::SendStoreHeartbeat(std::shared_ptr<CoordinatorInteraction> coordinator_interaction,
                                       uint64_t region_id) {
  auto start_time = Helper::TimestampMs();
  auto engine = Server::GetInstance()->GetEngine();
  auto raft_kv_engine =
      (engine->GetID() == pb::common::ENG_RAFT_STORE) ? std::dynamic_pointer_cast<RaftStoreEngine>(engine) : nullptr;

  pb::coordinator::StoreHeartbeatRequest request;
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();

  request.set_self_storemap_epoch(store_meta_manager->GetStoreServerMeta()->GetEpoch());
  // request.set_self_regionmap_epoch(store_meta_manager->GetStoreRegionMeta()->GetEpoch());

  // store
  request.mutable_store()->CopyFrom(*store_meta_manager->GetStoreServerMeta()->GetStore(Server::GetInstance()->Id()));

  // store_metrics
  auto metrics_manager = Server::GetInstance()->GetStoreMetricsManager();
  auto* mut_store_metrics = request.mutable_store_metrics();
  mut_store_metrics->CopyFrom(*metrics_manager->GetStoreMetrics()->Metrics());
  // setup id for store_metrics here, coordinator need this id to update store_metrics
  mut_store_metrics->set_id(Server::GetInstance()->Id());

  auto* mut_region_metrics_map = mut_store_metrics->mutable_region_metrics_map();
  auto region_metrics = metrics_manager->GetStoreRegionMetrics();
  std::vector<store::RegionPtr> region_metas;
  if (region_id == 0) {
    region_metas = store_meta_manager->GetStoreRegionMeta()->GetAllRegion();
  } else {
    mut_store_metrics->set_is_partial_region_metrics(true);
    auto region_meta = store_meta_manager->GetStoreRegionMeta()->GetRegion(region_id);
    if (region_meta != nullptr) {
      region_metas.push_back(region_meta);
    }
  }
  for (const auto& region_meta : region_metas) {
    pb::common::RegionMetrics tmp_region_metrics;
    auto metrics = region_metrics->GetMetrics(region_meta->Id());
    if (metrics != nullptr) {
      tmp_region_metrics.CopyFrom(metrics->InnerRegionMetrics());
    }

    tmp_region_metrics.set_id(region_meta->Id());
    tmp_region_metrics.set_leader_store_id(region_meta->LeaderId());
    tmp_region_metrics.set_store_region_state(region_meta->State());
    tmp_region_metrics.mutable_region_definition()->CopyFrom(region_meta->InnerRegion().definition());

    if ((region_meta->State() == pb::common::StoreRegionState::NORMAL ||
         region_meta->State() == pb::common::StoreRegionState::STANDBY ||
         region_meta->State() == pb::common::StoreRegionState::SPLITTING ||
         region_meta->State() == pb::common::StoreRegionState::MERGING) &&
        raft_kv_engine != nullptr) {
      auto raft_node = raft_kv_engine->GetNode(region_meta->Id());
      if (raft_node != nullptr) {
        tmp_region_metrics.mutable_braft_status()->CopyFrom(*raft_node->GetStatus());
      }
    }

    mut_region_metrics_map->insert({region_meta->Id(), tmp_region_metrics});
  }

  DINGO_LOG(DEBUG) << "StoreHeartbeat request: " << request.ShortDebugString();
  DINGO_LOG(INFO) << "StoreHeartbeatRequest size: " << request.ByteSizeLong()
                  << " used time: " << Helper::TimestampMs() - start_time;
  start_time = Helper::TimestampMs();
  pb::coordinator::StoreHeartbeatResponse response;
  auto status = coordinator_interaction->SendRequest("StoreHeartbeat", request, response);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("Store heartbeat failed, error: {} {}",
                                      pb::error::Errno_Name(status.error_code()), status.error_str());
    return;
  }

  DINGO_LOG(INFO) << "StoreHeartbeatResponse size: " << response.ByteSizeLong()
                  << " used time: " << Helper::TimestampMs() - start_time;

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
  DINGO_LOG(INFO) << "new store size: " << new_stores.size() << " / " << local_stores.size();
  for (const auto& store : new_stores) {
    store_server_meta->AddStore(store);
  }

  auto changed_stores = GetChangedStore(local_stores, remote_stores);
  DINGO_LOG(INFO) << "changed store size: " << changed_stores.size() << " / " << local_stores.size();
  for (const auto& store : changed_stores) {
    store_server_meta->UpdateStore(store);
  }

  auto deleted_stores = GetDeletedStore(local_stores, remote_stores);
  DINGO_LOG(INFO) << "deleted store size: " << deleted_stores.size() << " / " << local_stores.size();
  for (const auto& store : deleted_stores) {
    store_server_meta->DeleteStore(store->id());
  }
}

void CoordinatorRecycleOrphanTask::CoordinatorRecycleOrphan(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "CoordinatorRecycleOrphan... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CoordinatorRecycleOrphan... this is leader";

  coordinator_control->RecycleOrphanRegionOnStore();
}

// this is for coordinator
void CoordinatorUpdateStateTask::CoordinatorUpdateState(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is leader";

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

  // update region_state by last_update_timestamp
  pb::coordinator_internal::MetaIncrement meta_increment;
  pb::common::RegionMap region_map_temp;
  coordinator_control->GetRegionMap(region_map_temp);
  for (const auto& it : region_map_temp.regions()) {
    DINGO_LOG(INFO) << "CoordinatorUpdateState... region " << it.id() << " state " << it.state()
                    << " last_update_timestamp " << it.last_update_timestamp() << " now " << butil::gettimeofday_ms();
    if (it.last_update_timestamp() + (FLAGS_region_heartbeat_timeout * 1000) >= butil::gettimeofday_ms()) {
      if (it.heartbeat_state() != pb::common::RegionHeartbeatState::REGION_ONLINE) {
        DINGO_LOG(INFO) << "CoordinatorUpdateState... update region " << it.id() << " state to online";
        coordinator_control->TrySetRegionToOnline(it.id());
      }
      continue;
    }

    if (it.state() != pb::common::RegionState::REGION_NEW && it.state() != pb::common::RegionState::REGION_DELETE &&
        it.state() != pb::common::RegionState::REGION_DELETING &&
        it.state() != pb::common::RegionState::REGION_DELETED) {
      DINGO_LOG(INFO) << "CoordinatorUpdateState... update region " << it.id() << " state to offline";
      coordinator_control->TrySetRegionToDown(it.id());
    } else if (it.state() == pb::common::RegionState::REGION_DELETED &&
               it.last_update_timestamp() + (FLAGS_region_delete_after_deleted_time * 1000) <
                   butil::gettimeofday_ms()) {
      DINGO_LOG(INFO) << "CoordinatorUpdateState... start to purge region " << it.id();

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
      //   purge_region_cmd->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_PURGE);
      //   purge_region_cmd->set_create_timestamp(butil::gettimeofday_ms());
      //   purge_region_cmd->mutable_purge_request()->set_region_id(it.id());  // this region_id
      //   DINGO_LOG(INFO) << " purge_region_cmd set_region_id " << it.id();

      //   DINGO_LOG(INFO) << "CoordinatorUpdateState... purge region " << it.id() << " from store " <<
      //   it_peer.store_id()
      //                   << " region_cmd_type " << purge_region_cmd->region_cmd_type();
      // }

      // delete regions
      auto* region_delete_increment = meta_increment.add_regions();
      region_delete_increment->set_id(it.id());
      region_delete_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
      region_delete_increment->mutable_region()->CopyFrom(it);

      // mbvar
      coordinator_control->DeleteRegionBvar(it.id());

      DINGO_LOG(INFO) << "CoordinatorUpdateState... purge region delete region_map " << it.id() << " from store "
                      << it.id() << " region_cmd_type " << region_delete_increment->region().DebugString()
                      << " request=" << meta_increment.DebugString();
    }
  }

  if (meta_increment.ByteSizeLong() > 0) {
    // commit to raft
    coordinator_control->SubmitMetaIncrement(meta_increment);
  }

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
}

// this is for coordinator
void CoordinatorTaskListProcessTask::CoordinatorTaskListProcess(
    std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CoordinatorUpdateState... this is leader";

  coordinator_control->ProcessTaskList();
}

bool CheckStoreOperationResult(pb::coordinator::RegionCmdType cmd_type, pb::error::Errno errcode) {
  using pb::coordinator::RegionCmdType;
  using pb::error::Errno;

  if (errcode == Errno::OK || errcode == Errno::EREGION_REPEAT_COMMAND) {
    return true;
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
void CoordinatorPushTask::SendCoordinatorPushToStore(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    DINGO_LOG(DEBUG) << "... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "... this is leader";

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
    int ret = coordinator_control->GetStoreOperation(it.id(), store_operation);
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

    request.mutable_store_operation()->CopyFrom(store_operation);

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

    auto status = Heartbeat::RpcSendPushStoreOperation(it.server_location(), request, response);

    pb::coordinator_internal::MetaIncrement meta_increment;

    // check response
    if (status.ok()) {
      DINGO_LOG(INFO) << "... send store_operation to store " << it.id()
                      << " all success, will delete these region_cmds";
      // delete store_operation
      auto* store_operation_increment = meta_increment.add_store_operations();
      store_operation_increment->set_id(it.id());
      store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

      auto* store_operation_increment_delete = store_operation_increment->mutable_store_operation();
      store_operation_increment_delete->CopyFrom(store_operation);

      coordinator_control->SubmitMetaIncrement(meta_increment);

      continue;
    }

    if (response.region_cmd_results_size() <= 0) {
      DINGO_LOG(WARNING) << "... send store_operation to store " << it.id()
                         << " failed, but no region_cmd result, will try this store future";
      continue;
    }

    DINGO_LOG(WARNING) << "... send store_operation to store " << it.id()
                       << " failed, will check each region_cmd result";
    for (const auto& it_cmd : response.region_cmd_results()) {
      auto errcode = it_cmd.error().errcode();
      auto cmd_type = it_cmd.region_cmd_type();

      auto need_delete = CheckStoreOperationResult(cmd_type, errcode);
      if (!need_delete) {
        DINGO_LOG(INFO) << "... send store_operation to store_id=" << it.id()
                        << " region_cmd_id=" << it_cmd.region_cmd_id() << " result=[" << it_cmd.error().errcode()
                        << "][" << pb::error::Errno_descriptor()->FindValueByNumber(it_cmd.error().errcode())->name()
                        << " failed, will try this region_cmd future";
        continue;
      }

      DINGO_LOG(INFO) << "... send store_operation to store_id=" << it.id()
                      << " region_cmd_id=" << it_cmd.region_cmd_id() << " result=[" << it_cmd.error().errcode() << "]["
                      << pb::error::Errno_descriptor()->FindValueByNumber(it_cmd.error().errcode())->name()
                      << " success, will delete this region_cmd";

      // delete store_operation
      auto* store_operation_increment = meta_increment.add_store_operations();
      store_operation_increment->set_id(it.id());
      store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

      auto* store_operation_increment_delete = store_operation_increment->mutable_store_operation();
      store_operation_increment_delete->set_id(it.id());
      auto* region_cmd = store_operation_increment_delete->add_region_cmds();
      region_cmd->set_id(it_cmd.region_cmd_id());
    }

    if (meta_increment.ByteSizeLong() > 0) {
      coordinator_control->SubmitMetaIncrement(meta_increment);
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
  heart_response_to_send->CopyFrom(heartbeat_response);

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

    // add StoreOperation
    // pb::coordinator::StoreOperation store_operation;
    // coordinator_control->GetStoreOperation(store_need_send.id(), store_operation);
    // heartbeat_response.mutable_store_operation()->CopyFrom(store_operation);
    // if (store_operation.region_cmds_size() > 0) {
    //   DINGO_LOG(INFO) << "SendCoordinatorPushToStore will send to store with store_operation:"
    //                   << store_operation.ShortDebugString();
    // }

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
void CalculateTableMetricsTask::CalculateTableMetrics(std::shared_ptr<CoordinatorControl> coordinator_control) {
  if (!coordinator_control->IsLeader()) {
    // DINGO_LOG(INFO) << "SendCoordinatorPushToStore... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CalculateTableMetrics... this is leader";

  coordinator_control->CalculateTableMetrics();
  coordinator_control->CalculateIndexMetrics();
}

// this is for index
void VectorIndexScrubTask::ScrubVectorIndex() {
  auto vector_index_manager = Server::GetInstance()->GetVectorIndexManager();
  if (vector_index_manager == nullptr) {
    DINGO_LOG(ERROR) << "vector_index_manager is nullptr";
    return;
  }

  auto status = vector_index_manager->ScrubVectorIndex();
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

void Heartbeat::TriggerStoreHeartbeat(uint64_t region_id) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new HeartbeatTask(Server::GetInstance()->GetCoordinatorInteraction(), region_id);
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

void Heartbeat::TriggerScrubVectorIndex(void*) {
  // Free at ExecuteRoutine()
  TaskRunnable* task = new VectorIndexScrubTask();
  if (!Server::GetInstance()->GetHeartbeat()->Execute(task)) {
    delete task;
  }
}

butil::Status Heartbeat::RpcSendPushStoreOperation(const pb::common::Location& location,
                                                   const pb::push::PushStoreOperationRequest& request,
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
    cntl.set_timeout_ms(5000L);

    pb::push::PushService_Stub(&channel).PushStoreOperation(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
      DINGO_LOG(ERROR) << "... rpc failed, will retry, error code: " << cntl.ErrorCode()
                       << ", error message: " << cntl.ErrorText();
      continue;
    }

    auto errcode = response.error().errcode();
    if (errcode == pb::error::Errno::OK) {
      DINGO_LOG(INFO) << "... rpc success, will not retry";
      return butil::Status::OK();
    } else if (errcode == pb::error::Errno::ERAFT_NOTLEADER) {
      if (response.error().has_leader_location()) {
        DINGO_LOG(WARNING) << "... rpc failed, ERAFT_NOTLEADER, will retry, error code: " << errcode
                           << ", error message: " << response.error().errmsg()
                           << ", leader location: " << response.error().leader_location().host() << ":"
                           << response.error().leader_location().port();
        store_server_location_string =
            response.error().leader_location().host() + ":" + std::to_string(response.error().leader_location().port());
        continue;
      } else {
        DINGO_LOG(WARNING) << "... rpc failed, ERAFT_NOTLEADER, will retry, error code: " << errcode
                           << ", error message: " << response.error().errmsg();
        continue;
      }
    } else {
      DINGO_LOG(ERROR) << "... rpc failed, error code: " << response.error().errcode()
                       << ", error message: " << response.error().errmsg();
      return butil::Status(response.error().errcode(), response.error().errmsg());
    }
  } while (++retry_times < max_retry_times);

  return butil::Status(pb::error::Errno::EINTERNAL,
                       "connect with store server fail, no leader found or connect timeout, retry count: %d",
                       retry_times);
}

}  // namespace dingodb

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

#include "store/region_controller.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/service_access.h"
#include "config/config_helper.h"
#include "config/config_manager.h"
#include "event/store_state_machine_event.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "metrics/store_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
#include "store/heartbeat.h"
#include "vector/codec.h"
#include "vector/vector_index_hnsw.h"
#include "vector/vector_index_snapshot_manager.h"

namespace dingodb {

butil::Status CreateRegionTask::PreValidateCreateRegion(const pb::coordinator::RegionCmd& command) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  return ValidateCreateRegion(store_meta_manager, command.region_id());
}

butil::Status CreateRegionTask::ValidateCreateRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                                     int64_t region_id) {
  auto region = store_meta_manager->GetStoreRegionMeta()->GetRegion(region_id);
  if (region != nullptr && region->State() != pb::common::StoreRegionState::NEW) {
    return butil::Status(pb::error::EREGION_EXIST, fmt::format("Region {} already exist", region_id));
  }

  return butil::Status();
}

butil::Status CreateRegionTask::CreateRegion(const pb::common::RegionDefinition& definition, int64_t parent_region_id) {
  auto region = store::Region::New(definition);
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] create region, region: {}", region->Id(),
                                 region->InnerRegion().ShortDebugString());

  // Valiate region
  auto status = ValidateCreateRegion(store_meta_manager, region->Id());
  if (!status.ok()) {
    return status;
  }

  // Add region to store region meta manager
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] create region, save region meta", region->Id());
  auto store_region_meta = store_meta_manager->GetStoreRegionMeta();
  region->SetState(pb::common::StoreRegionState::NEW);
  store_region_meta->AddRegion(region);

  // Add region metrics
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] create region add region metrics", region->Id());
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());

  // Add raft node
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] create region, add raft node", region->Id());
  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Not found raft store engine");
  }

  RaftControlAble::AddNodeParameter parameter;
  parameter.role = Server::GetInstance().GetRole();
  parameter.is_restart = false;
  parameter.raft_endpoint = Server::GetInstance().RaftEndpoint();

  auto config = ConfigManager::GetInstance().GetConfig();
  parameter.raft_path = config->GetString("raft.path");
  parameter.election_timeout_ms = 200;
  parameter.snapshot_interval_s = config->GetInt("raft.snapshot_interval_s");
  parameter.log_max_segment_size = config->GetInt64("raft.segmentlog_max_segment_size");
  parameter.log_path = config->GetString("raft.log_path");

  auto raft_meta = StoreRaftMeta::NewRaftMeta(region->Id());
  parameter.raft_meta = raft_meta;
  parameter.region_metrics = region_metrics;
  auto listener_factory = std::make_shared<StoreSmEventListenerFactory>();
  parameter.listeners = listener_factory->Build();

  status = raft_store_engine->AddNode(region, parameter);
  if (!status.ok()) {
    return status;
  }

  Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);
  Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta()->AddRaftMeta(raft_meta);

  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] create region, update region state NORMAL",
                                 region->Id());
  if (parent_region_id == 0) {
    store_region_meta->UpdateState(region, pb::common::StoreRegionState::NORMAL);
  } else {
    store_region_meta->UpdateState(region, pb::common::StoreRegionState::STANDBY);
  }

  return butil::Status();
}

void CreateRegionTask::Run() {
  auto region_definition = region_cmd_->create_request().region_definition();

  auto status = CreateRegion(region_definition, region_cmd_->create_request().split_from_region_id());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] create region failed, error: {}",
                                    region_definition.id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
}

butil::Status DeleteRegionTask::PreValidateDeleteRegion(const pb::coordinator::RegionCmd& command) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  return ValidateDeleteRegion(store_meta_manager,
                              store_meta_manager->GetStoreRegionMeta()->GetRegion(command.region_id()));
}

butil::Status DeleteRegionTask::ValidateDeleteRegion(std::shared_ptr<StoreMetaManager> /*store_meta_manager*/,
                                                     store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Region is not exist, can't delete.");
  }
  if (region->State() == pb::common::StoreRegionState::DELETING ||
      region->State() == pb::common::StoreRegionState::DELETED) {
    return butil::Status(pb::error::EREGION_DELETING, "Region is deleting or deleted.");
  }

  if (region->State() == pb::common::StoreRegionState::SPLITTING ||
      region->State() == pb::common::StoreRegionState::MERGING) {
    return butil::Status(pb::error::EREGION_STATE, "Region state not allow delete.");
  }

  return butil::Status();
}

butil::Status DeleteRegionTask::DeleteRegion(std::shared_ptr<Context> ctx, int64_t region_id) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  auto store_region_meta = store_meta_manager->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(region_id);

  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] delete region.", region_id);
  // Valiate region
  auto status = ValidateDeleteRegion(store_meta_manager, region);
  if (!status.ok()) {
    return status;
  }

  // Update state
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] delete region update region state DELETING", region_id);
  store_region_meta->UpdateState(region, pb::common::StoreRegionState::DELETING);

  // Raft kv engine
  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    // Delete raft
    DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] delete region, delete raft node", region_id);
    raft_store_engine->DestroyNode(ctx, region_id);
    Server::GetInstance().GetLogStorageManager()->DeleteStorage(region_id);
  }

  // Update state
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] delete region, update region state DELETED", region_id);
  store_region_meta->UpdateState(region, pb::common::StoreRegionState::DELETED);

  // Delete metrics
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] delete region, delete region metrics", region_id);
  Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics()->DeleteMetrics(region_id);
  StoreBvarMetrics::GetInstance().DeleteMetrics(std::to_string(region_id));

  // Delete raft meta
  store_meta_manager->GetStoreRaftMeta()->DeleteRaftMeta(region_id);

  // Delete data
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] delete region, delete data", region_id);
  auto writer = Server::GetInstance().GetRawEngine()->Writer();
  status = writer->KvDeleteRange(Helper::GetColumnFamilyNames(), region->Range());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] delete region data failled.", region_id);
  }

  // Index region
  if (Server::GetInstance().GetRole() == pb::common::ClusterRole::INDEX) {
    auto vector_index_wrapper = region->VectorIndexWrapper();
    if (vector_index_wrapper != nullptr) {
      vector_index_wrapper->Destroy();
    }
  }

  // Delete region executor
  auto region_controller = Server::GetInstance().GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(region_id);
  command->set_create_timestamp(Helper::TimestampMs());
  command->set_region_cmd_type(pb::coordinator::CMD_DESTROY_EXECUTOR);
  command->mutable_destroy_executor_request()->set_region_id(region_id);

  status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(), command);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[control.region][region({})] dispatch region executor command failed, error: {} {}", region_id,
        status.error_code(), status.error_str());
  }

  // Purge region for coordinator recycle_orphan_region mechanism
  // TODO: need to implement a better mechanism of tombstone for region's meta info
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] purge region.", region_id);
  store_region_meta->DeleteRegion(region_id);

  return butil::Status();
}

void DeleteRegionTask::Run() {
  auto status = DeleteRegion(ctx_, region_cmd_->delete_request().region_id());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] delete region failed, error: {}",
                                    region_cmd_->delete_request().region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
}

butil::Status SplitRegionTask::PreValidateSplitRegion(const pb::coordinator::RegionCmd& command) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  return ValidateSplitRegion(store_meta_manager->GetStoreRegionMeta(), command.split_request());
}

butil::Status SplitRegionTask::ValidateSplitRegion(std::shared_ptr<StoreRegionMeta> store_region_meta,
                                                   const pb::coordinator::SplitRequest& split_request) {
  auto parent_region_id = split_request.split_from_region_id();
  auto child_region_id = split_request.split_to_region_id();

  auto parent_region = store_region_meta->GetRegion(parent_region_id);
  if (parent_region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Parent region not exist.");
  }

  if (parent_region->DisableChange()) {
    return butil::Status(pb::error::EREGION_DISABLE_CHANGE, "Disable region change.");
  }

  if (parent_region->TemporaryDisableChange()) {
    return butil::Status(pb::error::EREGION_DISABLE_CHANGE, "Temporary disable region change.");
  }

  if (ConfigHelper::GetSplitStrategy() == pb::raft::PRE_CREATE_REGION) {
    auto child_region = store_region_meta->GetRegion(child_region_id);
    if (child_region == nullptr) {
      return butil::Status(pb::error::EREGION_NOT_FOUND, "Child region not exist.");
    }
    if (child_region->State() != pb::common::STANDBY) {
      return butil::Status(pb::error::EREGION_STATE, "Child region state is not STANDBY.");
    }
  }

  const auto& split_key = split_request.split_watershed_key();
  auto range = parent_region->Range();
  if (range.start_key().compare(split_key) >= 0 || range.end_key().compare(split_key) <= 0) {
    return butil::Status(
        pb::error::EKEY_INVALID,
        fmt::format("Split key is invalid, range: [{}-{}) split_key: {}", Helper::StringToHex(range.start_key()),
                    Helper::StringToHex(range.end_key()), Helper::StringToHex(split_key)));
  }

  if (parent_region->State() == pb::common::SPLITTING) {
    return butil::Status(pb::error::EREGION_SPLITING, "Parent region state is splitting.");
  }

  if (parent_region->State() != pb::common::NORMAL) {
    return butil::Status(pb::error::EREGION_STATE, "Parent region state is NORMAL, not allow split.");
  }

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Not found raft store engine");
  }

  auto node = raft_store_engine->GetNode(parent_region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "No found raft node.");
  }

  if (!node->IsLeader()) {
    return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
  }

  auto status = Helper::ValidateRaftStatusForSplit(node->GetStatus());
  if (!status.ok()) {
    return status;
  }

  if (parent_region->Type() == pb::common::INDEX_REGION) {
    if (!VectorCodec::IsValidKey(split_key)) {
      return butil::Status(pb::error::EKEY_INVALID,
                           fmt::format("Split key is invalid, length {} is wrong", split_key.size()));
    }
    // Check follower whether hold vector index.
    auto self_peer = node->GetPeerId();
    std::vector<braft::PeerId> peers;
    node->ListPeers(&peers);
    for (const auto& peer : peers) {
      if (peer != self_peer) {
        pb::node::CheckVectorIndexRequest request;
        request.set_vector_index_id(parent_region_id);
        pb::node::CheckVectorIndexResponse response;
        auto status = ServiceAccess::CheckVectorIndex(request, peer.addr, response);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format(
              "[control.region][region({})] check peer {} hold vector index failed, error: {}", parent_region_id,
              Helper::EndPointToStr(peer.addr), status.error_str());
        }

        if (!response.is_exist()) {
          return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "Not found vector index %lu at peer %s",
                               parent_region_id, Helper::EndPointToStr(peer.addr).c_str());
        }
      }
    }
  }

  return butil::Status();
}

butil::Status SplitRegionTask::SplitRegion() {
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();

  auto status = ValidateSplitRegion(store_region_meta, region_cmd_->split_request());
  if (!status.ok()) {
    return status;
  }

  auto parent_region = store_region_meta->GetRegion(region_cmd_->split_request().split_from_region_id());
  if (parent_region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Parent region not exist.");
  }

  // Commit raft log
  ctx_->SetRegionId(region_cmd_->split_request().split_from_region_id());
  return Server::GetInstance().GetEngine()->AsyncWrite(
      ctx_, WriteDataBuilder::BuildWrite(region_cmd_->split_request(), parent_region->Epoch()),
      [](std::shared_ptr<Context>, butil::Status status) {
        if (!status.ok()) {
          LOG(ERROR) << fmt::format("[control.region][region()] write split failed, error: {}", status.error_str());
        }
      });
}

void SplitRegionTask::Run() {
  auto status = SplitRegion();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] Split failed, error: {}",
                                    region_cmd_->split_request().split_from_region_id(),
                                    region_cmd_->split_request().split_to_region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
}

butil::Status ChangeRegionTask::PreValidateChangeRegion(const pb::coordinator::RegionCmd& command) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  return ValidateChangeRegion(store_meta_manager, command.change_peer_request().region_definition());
}

// Check region leader
static butil::Status CheckLeader(int64_t region_id) {
  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Not found raft store engine");
  }

  auto node = raft_store_engine->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "No found raft node.");
  }

  if (!node->IsLeader()) {
    return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
  }

  return butil::Status();
}

butil::Status ChangeRegionTask::ValidateChangeRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                                     const pb::common::RegionDefinition& region_definition) {
  auto region = store_meta_manager->GetStoreRegionMeta()->GetRegion(region_definition.id());
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Region not exist, can't change.");
  }

  if (region->State() != pb::common::StoreRegionState::NORMAL) {
    return butil::Status(pb::error::EREGION_STATE, "Region state not allow change.");
  }

  if (region->DisableChange()) {
    return butil::Status(pb::error::EREGION_DISABLE_CHANGE, "Disable region change.");
  }

  if (region->TemporaryDisableChange()) {
    return butil::Status(pb::error::EREGION_DISABLE_CHANGE, "Temporary disable region change.");
  }

  return CheckLeader(region_definition.id());
}

butil::Status ChangeRegionTask::ChangeRegion(std::shared_ptr<Context> ctx,
                                             const pb::common::RegionDefinition& region_definition) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] change region, region definition: {}",
                                 region_definition.id(), region_definition.ShortDebugString());

  // Valiate region
  auto status = ValidateChangeRegion(store_meta_manager, region_definition);
  if (!status.ok()) {
    return status;
  }

  auto filter_peers_by_role = [region_definition](pb::common::PeerRole role) -> std::vector<pb::common::Peer> {
    std::vector<pb::common::Peer> peers;
    for (const auto& peer : region_definition.peers()) {
      if (peer.role() == role) {
        peers.push_back(peer);
      }
    }
    return peers;
  };

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    return raft_store_engine->ChangeNode(ctx, region_definition.id(), filter_peers_by_role(pb::common::VOTER));
  }

  return butil::Status();
}

void ChangeRegionTask::Run() {
  auto status = ChangeRegion(ctx_, region_cmd_->change_peer_request().region_definition());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] Change region failed, error: {}",
                                    region_cmd_->change_peer_request().region_definition().id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
}

butil::Status TransferLeaderTask::PreValidateTransferLeader(const pb::coordinator::RegionCmd& command) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  return ValidateTransferLeader(store_meta_manager, command.region_id(), command.transfer_leader_request().peer());
}

butil::Status TransferLeaderTask::ValidateTransferLeader(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                                         int64_t region_id, const pb::common::Peer& peer) {
  auto region = store_meta_manager->GetStoreRegionMeta()->GetRegion(region_id);
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Region not exist, can't transfer leader.");
  }

  if (region->State() != pb::common::StoreRegionState::NORMAL) {
    return butil::Status(pb::error::EREGION_STATE, "Region state not allow transfer leader.");
  }

  if (peer.store_id() == Server::GetInstance().Id()) {
    return butil::Status(pb::error::ERAFT_TRANSFER_LEADER, "The peer is already leader, not need transfer.");
  }

  if (peer.raft_location().host().empty() || peer.raft_location().host() == "0.0.0.0") {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Raft location is invalid.");
  }

  return butil::Status();
}

butil::Status TransferLeaderTask::TransferLeader(std::shared_ptr<Context>, int64_t region_id,
                                                 const pb::common::Peer& peer) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] transfer leader, peer: {}", region_id,
                                  peer.ShortDebugString());

  auto status = ValidateTransferLeader(store_meta_manager, region_id, peer);
  if (!status.ok()) {
    return status;
  }

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    return raft_store_engine->TransferLeader(region_id, peer);
  }

  return butil::Status();
}

void TransferLeaderTask::Run() {
  auto status = TransferLeader(ctx_, region_cmd_->region_id(), region_cmd_->transfer_leader_request().peer());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] transfer leader failed, error: {}",
                                    region_cmd_->change_peer_request().region_definition().id(), status.error_cstr());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
}

butil::Status SnapshotRegionTask::Snapshot(std::shared_ptr<Context> ctx, int64_t region_id) {
  auto engine = Server::GetInstance().GetEngine();
  return engine->DoSnapshot(ctx, region_id);
}

void SnapshotRegionTask::Run() {
  auto status = Snapshot(ctx_, region_cmd_->region_id());
  if (!status.ok()) {
    DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] snapshot region failed, error: {}",
                                    region_cmd_->region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status PurgeRegionTask::PreValidatePurgeRegion(const pb::coordinator::RegionCmd& command) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  return ValidatePurgeRegion(store_meta_manager->GetStoreRegionMeta()->GetRegion(command.region_id()));
}

butil::Status PurgeRegionTask::ValidatePurgeRegion(store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Region is not exist, can't purge.");
  }
  if (region->State() != pb::common::StoreRegionState::DELETED) {
    return butil::Status(pb::error::EREGION_DELETED, "Region is not deleted, can't purge.");
  }

  return butil::Status();
}

butil::Status PurgeRegionTask::PurgeRegion(std::shared_ptr<Context>, int64_t region_id) {
  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] purge region.", region_id);
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  auto store_region_meta = store_meta_manager->GetStoreRegionMeta();
  store_region_meta->DeleteRegion(region_id);

  return butil::Status();
}

void PurgeRegionTask::Run() {
  auto status = PurgeRegion(ctx_, region_cmd_->purge_request().region_id());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] purge region failed, error: {}",
                                    region_cmd_->purge_request().region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
}

butil::Status StopRegionTask::PreValidateStopRegion(const pb::coordinator::RegionCmd& command) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  return ValidateStopRegion(store_meta_manager->GetStoreRegionMeta()->GetRegion(command.region_id()));
}

butil::Status StopRegionTask::ValidateStopRegion(store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Region is not exist, can't delete peer.");
  }
  if (region->State() != pb::common::StoreRegionState::ORPHAN) {
    return butil::Status(pb::error::EREGION_STATE, "Region is not orphan.");
  }

  return butil::Status();
}

butil::Status StopRegionTask::StopRegion(std::shared_ptr<Context> ctx, int64_t region_id) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  auto store_region_meta = store_meta_manager->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(region_id);

  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] stop region.", region_id);
  // Valiate region
  auto status = ValidateStopRegion(region);
  if (!status.ok()) {
    return status;
  }

  // Shutdown raft node
  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    // Delete raft
    DINGO_LOG(INFO) << fmt::format("[control.region][region({})] delete peer delete raft node.", region_id);
    raft_store_engine->StopNode(ctx, region_id);
  }

  return butil::Status();
}

void StopRegionTask::Run() {
  auto status = StopRegion(ctx_, region_cmd_->stop_request().region_id());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[control.region][region({})] Delete peer region failed, error: {}",
                                      region_cmd_->stop_request().region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status DestroyRegionExecutorTask::DestroyRegionExecutor(std::shared_ptr<Context>, int64_t region_id) {
  auto regoin_controller = Server::GetInstance().GetRegionController();
  if (regoin_controller == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] region controller is nullptr.", region_id);
    return butil::Status(pb::error::EINTERNAL, "Region controller is nullptr");
  }

  regoin_controller->UnRegisterExecutor(region_id);

  return butil::Status();
}

void DestroyRegionExecutorTask::Run() {
  auto status = DestroyRegionExecutor(ctx_, region_cmd_->destroy_executor_request().region_id());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] destroy executor region failed, error: {}",
                                    region_cmd_->destroy_executor_request().region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status SnapshotVectorIndexTask::SaveSnapshot(std::shared_ptr<Context> /*ctx*/, int64_t vector_index_id) {
  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] save snapshot.", vector_index_id);
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  auto store_region_meta = store_meta_manager->GetStoreRegionMeta();

  auto region = store_region_meta->GetRegion(vector_index_id);
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", vector_index_id));
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper == nullptr || vector_index_wrapper->GetOwnVectorIndex() == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, fmt::format("Not found vector index {}", vector_index_id));
  }

  auto status = VectorIndexManager::SaveVectorIndex(vector_index_wrapper);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void SnapshotVectorIndexTask::Run() {
  auto status = SaveSnapshot(ctx_, region_cmd_->snapshot_vector_index_request().vector_index_id());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] save vector index snapshot failed, error: {}",
                                    region_cmd_->snapshot_vector_index_request().vector_index_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status UpdateDefinitionTask::PreValidateUpdateDefinition(const pb::coordinator::RegionCmd& command) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  return ValidateUpdateDefinition(store_meta_manager->GetStoreRegionMeta()->GetRegion(command.region_id()));
}

butil::Status UpdateDefinitionTask::ValidateUpdateDefinition(store::RegionPtr region) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Region is not exist, can't delete peer.");
  }

  if (region->State() != pb::common::StoreRegionState::NORMAL) {
    return butil::Status(pb::error::EREGION_STATE, "Region state not allow change.");
  }

  return butil::Status();
}

void UpdateDefinitionTask::Run() {
  auto status = UpdateDefinition(ctx_, region_cmd_->region_id(),
                                 region_cmd_->update_definition_request().new_region_definition());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[control.region][region({})] update Region Defition failed, error: {}",
                                      region_cmd_->region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status UpdateDefinitionTask::UpdateDefinition(std::shared_ptr<Context> /*ctx*/, int64_t region_id,
                                                     const pb::common::RegionDefinition& new_definition) {
  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] update definition.", region_id);
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  auto store_region_meta = store_meta_manager->GetStoreRegionMeta();

  auto region = store_region_meta->GetRegion(region_id);
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", region_id));
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, fmt::format("Not found vector index {}", region_id));
  }
  auto vector_index = vector_index_wrapper->GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, fmt::format("Not found vector index {}", region_id));
  }

  // check if is changing hnsw max_elements
  if (new_definition.index_parameter().vector_index_parameter().has_hnsw_parameter()) {
    auto hnsw_index = std::dynamic_pointer_cast<VectorIndexHnsw>(vector_index);
    if (hnsw_index == nullptr) {
      return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, fmt::format("Not found hnsw index {}", region_id));
    }

    auto new_max_elements = new_definition.index_parameter().vector_index_parameter().hnsw_parameter().max_elements();
    int64_t old_max_elements = 0;
    auto ret = hnsw_index->GetMaxElements(old_max_elements);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] get hnsw index max elements failed.", region_id);
      return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND,
                           fmt::format("Get hnsw index max elements failed {}", region_id));
    }

    if (new_max_elements <= old_max_elements) {
      DINGO_LOG(INFO) << fmt::format(
          "[control.region][region({})] update definition new max elements {} <= old max elements {}, skip", region_id,
          new_max_elements, old_max_elements);
      return butil::Status::OK();
    } else {
      ret = hnsw_index->ResizeMaxElements(
          new_definition.index_parameter().vector_index_parameter().hnsw_parameter().max_elements());
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] resize hnsw index max elements failed.",
                                        region_id);
        return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND,
                             fmt::format("Resize hnsw index max elements failed {}", region_id));
      }

      // update region definition in store meta
      region->SetIndexParameter(new_definition.index_parameter());
      store_region_meta->UpdateRegion(region);

      DINGO_LOG(INFO) << fmt::format(
          "[control.region][region({})] update definition new max elements {} > old max elements {}, resize success",
          region_id, new_max_elements, old_max_elements);

      return butil::Status::OK();
    }
  } else {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                         fmt::format("Not found hnsw index parameter in region_cmd {}", region_id));
  }

  return butil::Status::OK();
}

butil::Status SwitchSplitTask::PreValidateSwitchSplit(const pb::coordinator::RegionCmd& command) {
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();

  auto region = store_region_meta->GetRegion(command.switch_split_request().region_id());
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND,
                         fmt::format("Not found region {}", command.switch_split_request().region_id()));
  }

  return butil::Status::OK();
}

void SwitchSplitTask::Run() {
  auto status = SwitchSplit(ctx_, region_cmd_->switch_split_request().region_id(),
                            region_cmd_->switch_split_request().disable_split());
  if (!status.ok()) {
    DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] switch split executor region failed, error: {}",
                                    region_cmd_->switch_split_request().region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status SwitchSplitTask::SwitchSplit(std::shared_ptr<Context>, int64_t region_id, bool disable_split) {
  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] switch split.", region_id);
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();

  auto region = store_region_meta->GetRegion(region_id);
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", region_id));
  }

  store_region_meta->UpdateDisableChange(region, disable_split);

  return butil::Status();
}

butil::Status HoldVectorIndexTask::PreValidateHoldVectorIndex(const pb::coordinator::RegionCmd& command) {
  return ValidateHoldVectorIndex(command.hold_vector_index_request().region_id());
}

butil::Status HoldVectorIndexTask::ValidateHoldVectorIndex(int64_t region_id) {
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(region_id);
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", region_id));
  }

  // Validate is follower
  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    auto node = raft_store_engine->GetNode(region_id);
    if (node == nullptr) {
      return butil::Status(pb::error::ERAFT_NOT_FOUND, "No found raft node %lu.", region_id);
    }
  }

  return butil::Status();
}

butil::Status HoldVectorIndexTask::HoldVectorIndex(std::shared_ptr<Context> /*ctx*/, int64_t region_id, bool is_hold) {
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(region_id);
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", region_id));
  }

  auto status = ValidateHoldVectorIndex(region_id);
  if (!status.ok()) {
    return status;
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, fmt::format("Not found vector index {}", region_id));
  }

  if (is_hold) {
    vector_index_wrapper->SetIsTempHoldVectorIndex(true);
    // Load vector index.
    if (!vector_index_wrapper->IsOwnReady()) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.hold][index_id({})] launch load or build vector index.", region_id);
      VectorIndexManager::LaunchLoadOrBuildVectorIndex(vector_index_wrapper);
    }
  } else {
    // Delete vector index.
    if (vector_index_wrapper->IsOwnReady()) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.hold][region({})] delete vector index.", region_id);
      vector_index_wrapper->ClearVectorIndex();
    }
  }

  return butil::Status();
}

void HoldVectorIndexTask::Run() {
  auto status = HoldVectorIndex(ctx_, region_cmd_->hold_vector_index_request().region_id(),
                                region_cmd_->hold_vector_index_request().is_hold());
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[control.region][region({})] hold vector index  failed, {}",
                                      region_cmd_->switch_split_request().region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

bool ControlExecutor::Init() { return worker_->Init(); }

bool ControlExecutor::Execute(TaskRunnablePtr task) { return worker_->Execute(task); }

void ControlExecutor::Stop() { worker_->Destroy(); }

bool RegionCommandManager::Init() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    DINGO_LOG(ERROR) << "[control.region] scan store raft meta failed.";
    return false;
  }

  if (!kvs.empty()) {
    TransformFromKv(kvs);
  }
  return true;
}

bool RegionCommandManager::IsExist(int64_t command_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return region_commands_.find(command_id) != region_commands_.end();
}

void RegionCommandManager::AddCommand(std::shared_ptr<pb::coordinator::RegionCmd> region_cmd) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (region_commands_.find(region_cmd->id()) != region_commands_.end()) {
      DINGO_LOG(WARNING) << fmt::format("[control.region][commond({})] region control command already exist!",
                                        region_cmd->id());
      return;
    }

    region_commands_.insert(std::make_pair(region_cmd->id(), region_cmd));
  }

  meta_writer_->Put(TransformToKv(region_cmd));
}

void RegionCommandManager::UpdateCommandStatus(std::shared_ptr<pb::coordinator::RegionCmd> region_cmd,
                                               pb::coordinator::RegionCmdStatus status) {
  region_cmd->set_status(status);
  meta_writer_->Put(TransformToKv(region_cmd));
}

void RegionCommandManager::UpdateCommandStatus(int64_t command_id, pb::coordinator::RegionCmdStatus status) {
  auto region_cmd = GetCommand(command_id);
  if (region_cmd != nullptr) {
    UpdateCommandStatus(region_cmd, status);
  }
}

std::shared_ptr<pb::coordinator::RegionCmd> RegionCommandManager::GetCommand(int64_t command_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = region_commands_.find(command_id);
  if (it == region_commands_.end()) {
    return nullptr;
  }

  return it->second;
}

std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> RegionCommandManager::GetCommands(
    pb::coordinator::RegionCmdStatus status) {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> commands;
  for (auto& [_, command] : region_commands_) {
    if (command->status() == status) {
      commands.push_back(command);
    }
  }

  std::sort(commands.begin(), commands.end(),
            [](const std::shared_ptr<pb::coordinator::RegionCmd>& a,
               const std::shared_ptr<pb::coordinator::RegionCmd>& b) { return a->id() < b->id(); });

  return commands;
}

std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> RegionCommandManager::GetCommands(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> commands;
  for (auto& [_, command] : region_commands_) {
    if (command->region_id() == region_id) {
      commands.push_back(command);
    }
  }

  std::sort(commands.begin(), commands.end(),
            [](const std::shared_ptr<pb::coordinator::RegionCmd>& a,
               const std::shared_ptr<pb::coordinator::RegionCmd>& b) { return a->id() < b->id(); });

  return commands;
}

std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> RegionCommandManager::GetAllCommand() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> commands;
  commands.reserve(region_commands_.size());
  for (auto& [_, command] : region_commands_) {
    commands.push_back(command);
  }

  std::sort(commands.begin(), commands.end(),
            [](const std::shared_ptr<pb::coordinator::RegionCmd>& a,
               const std::shared_ptr<pb::coordinator::RegionCmd>& b) { return a->id() < b->id(); });

  return commands;
}

std::shared_ptr<pb::common::KeyValue> RegionCommandManager::TransformToKv(std::any obj) {
  auto region_cmd = std::any_cast<std::shared_ptr<pb::coordinator::RegionCmd>>(obj);
  std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(region_cmd->id()));
  kv->set_value(region_cmd->SerializeAsString());

  return kv;
}

void RegionCommandManager::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& kv : kvs) {
    int64_t command_id = ParseRegionId(kv.key());
    auto region_cmd = std::make_shared<pb::coordinator::RegionCmd>();
    region_cmd->ParsePartialFromArray(kv.value().data(), kv.value().size());
    region_commands_.insert_or_assign(command_id, region_cmd);
  }
}

bool RegionController::Init() {
  share_executor_ = std::make_shared<ControlExecutor>();
  if (!share_executor_->Init()) {
    DINGO_LOG(ERROR) << "[control.region] share executor init failed.";
    return false;
  }

  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  auto regions = store_meta_manager->GetStoreRegionMeta()->GetAllAliveRegion();
  for (auto& region : regions) {
    if (!RegisterExecutor(region->Id())) {
      DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] register region control executor failed.",
                                      region->Id());
      return false;
    }
  }

  return true;
}

bool RegionController::Recover() {
  auto commands =
      Server::GetInstance().GetRegionCommandManager()->GetCommands(pb::coordinator::RegionCmdStatus::STATUS_NONE);

  for (auto& command : commands) {
    auto ctx = std::make_shared<Context>();

    auto status = InnerDispatchRegionControlCommand(ctx, command);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[control.region] recover region control command failed, error: {}",
                                      status.error_str());
    }
  }

  return true;
}

void RegionController::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);
  for (auto [_, executor] : executors_) {
    executor->Stop();
  }

  share_executor_->Stop();
}

std::vector<int64_t> RegionController::GetAllRegion() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<int64_t> region_ids;
  region_ids.reserve(executors_.size());
  for (auto [region_id, _] : executors_) {
    region_ids.push_back(region_id);
  }
  return region_ids;
}

bool RegionController::RegisterExecutor(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (executors_.find(region_id) == executors_.end()) {
    auto executor = std::make_shared<RegionControlExecutor>(region_id);
    if (!executor->Init()) {
      DINGO_LOG(ERROR) << "[control.region] region controller executor init failed.";
      return false;
    }
    executors_.insert({region_id, executor});
  }

  return true;
}

void RegionController::UnRegisterExecutor(int64_t region_id) {
  std::shared_ptr<RegionControlExecutor> executor;
  {
    BAIDU_SCOPED_LOCK(mutex_);
    auto it = executors_.find(region_id);
    if (it != executors_.end()) {
      executor = it->second;
      executors_.erase(it);
    }
  }
  if (executor != nullptr) {
    executor->Stop();
  }
}

std::shared_ptr<RegionControlExecutor> RegionController::GetRegionControlExecutor(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = executors_.find(region_id);
  if (it == executors_.end()) {
    return nullptr;
  }

  return it->second;
}

butil::Status RegionController::InnerDispatchRegionControlCommand(std::shared_ptr<Context> ctx,
                                                                  std::shared_ptr<pb::coordinator::RegionCmd> command) {
  DINGO_LOG(DEBUG) << fmt::format("[control.region][region({})] dispatch region control command, commad id: {} {}",
                                  command->region_id(), command->id(),
                                  pb::coordinator::RegionCmdType_Name(command->region_cmd_type()));

  // Create region, need to add region control executor
  if (command->region_cmd_type() == pb::coordinator::RegionCmdType::CMD_CREATE) {
    RegisterExecutor(command->region_id());
  }

  auto executor = (command->region_cmd_type() == pb::coordinator::RegionCmdType::CMD_PURGE ||
                   command->region_cmd_type() == pb::coordinator::RegionCmdType::CMD_DESTROY_EXECUTOR)
                      ? share_executor_
                      : GetRegionControlExecutor(command->region_id());
  if (executor == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] not find region control executor.",
                                    command->region_id());
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not find regon control executor");
  }

  auto it = task_builders.find(command->region_cmd_type());
  if (it == task_builders.end()) {
    DINGO_LOG(ERROR) << "[control.region] not exist region control command.";
    return butil::Status(pb::error::EINTERNAL, "Not exist region control command");
  }

  // Free at ExecuteRoutine()
  auto task = it->second(ctx, command);
  if (task == nullptr) {
    DINGO_LOG(ERROR) << "[control.region] not support region control command.";
    return butil::Status(pb::error::EINTERNAL, "Not support region control command");
  }
  if (!executor->Execute(task)) {
    return butil::Status(pb::error::EINTERNAL, "Execute region control command failed");
  }

  return butil::Status();
}

butil::Status RegionController::DispatchRegionControlCommand(std::shared_ptr<Context> ctx,
                                                             std::shared_ptr<pb::coordinator::RegionCmd> command) {
  // Check repeat region command
  auto region_command_manager = Server::GetInstance().GetRegionCommandManager();
  if (region_command_manager->IsExist(command->id())) {
    return butil::Status(pb::error::EREGION_REPEAT_COMMAND, "Repeat region control command");
  }

  // Save region command
  region_command_manager->AddCommand(command);

  return InnerDispatchRegionControlCommand(ctx, command);
}

RegionController::ValidateFunc RegionController::GetValidater(pb::coordinator::RegionCmdType cmd_type) {
  auto it = validaters.find(cmd_type);
  if (it == validaters.end()) {
    DINGO_LOG(ERROR) << "[control.region] unknown command type: " << pb::coordinator::RegionCmdType_Name(cmd_type);
    return nullptr;
  }

  return it->second;
}

RegionController::TaskBuilderMap RegionController::task_builders = {
    {pb::coordinator::CMD_CREATE,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<CreateRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_DELETE,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<DeleteRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_SPLIT,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<SplitRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_MERGE,
     [](std::shared_ptr<Context>, std::shared_ptr<pb::coordinator::RegionCmd>) -> TaskRunnablePtr { return nullptr; }},
    {pb::coordinator::CMD_CHANGE_PEER,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<ChangeRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_TRANSFER_LEADER,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<TransferLeaderTask>(ctx, command);
     }},
    {pb::coordinator::CMD_SNAPSHOT,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<SnapshotRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_PURGE,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<PurgeRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_STOP,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<StopRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_DESTROY_EXECUTOR,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<DestroyRegionExecutorTask>(ctx, command);
     }},
    {pb::coordinator::CMD_SNAPSHOT_VECTOR_INDEX,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<SnapshotVectorIndexTask>(ctx, command);
     }},
    {pb::coordinator::CMD_UPDATE_DEFINITION,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<UpdateDefinitionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_SWITCH_SPLIT,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<SwitchSplitTask>(ctx, command);
     }},
    {pb::coordinator::CMD_HOLD_VECTOR_INDEX,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnablePtr {
       return std::make_shared<HoldVectorIndexTask>(ctx, command);
     }},
};

RegionController::ValidaterMap RegionController::validaters = {
    {pb::coordinator::CMD_CREATE, CreateRegionTask::PreValidateCreateRegion},
    {pb::coordinator::CMD_DELETE, DeleteRegionTask::PreValidateDeleteRegion},
    {pb::coordinator::CMD_SPLIT, SplitRegionTask::PreValidateSplitRegion},
    {pb::coordinator::CMD_CHANGE_PEER, ChangeRegionTask::PreValidateChangeRegion},
    {pb::coordinator::CMD_TRANSFER_LEADER, TransferLeaderTask::PreValidateTransferLeader},
    {pb::coordinator::CMD_PURGE, PurgeRegionTask::PreValidatePurgeRegion},
    {pb::coordinator::CMD_STOP, StopRegionTask::PreValidateStopRegion},
    {pb::coordinator::CMD_UPDATE_DEFINITION, UpdateDefinitionTask::PreValidateUpdateDefinition},
    {pb::coordinator::CMD_SWITCH_SPLIT, SwitchSplitTask::PreValidateSwitchSplit},
    {pb::coordinator::CMD_HOLD_VECTOR_INDEX, HoldVectorIndexTask::PreValidateHoldVectorIndex},
};

}  // namespace dingodb

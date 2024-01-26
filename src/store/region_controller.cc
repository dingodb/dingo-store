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
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/role.h"
#include "common/service_access.h"
#include "config/config_helper.h"
#include "config/config_manager.h"
#include "event/store_state_machine_event.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
#include "store/heartbeat.h"
#include "vector/codec.h"
#include "vector/vector_index_hnsw.h"

DEFINE_int64(merge_committed_log_gap, 16, "merge commited log gap");
DEFINE_int32(init_election_timeout_ms, 1000, "init election timeout");

namespace dingodb {

// Notify coordinator region command execute result.
static void NotifyRegionCmdStatus(RegionCmdPtr region_cmd, butil::Status status) {
  auto coordinatro_interaction = Server::GetInstance().GetCoordinatorInteraction();
  if (coordinatro_interaction == nullptr) {
    DINGO_LOG(ERROR) << fmt::format(
        "[control.region][region({}).job_id({}).cmd_id({})] coordinator interaction is null.", region_cmd->region_id(),
        region_cmd->job_id(), region_cmd->id());
    return;
  }

  pb::coordinator::UpdateRegionCmdStatusRequest request;
  pb::coordinator::UpdateRegionCmdStatusResponse response;

  request.set_task_list_id(region_cmd->job_id());
  request.set_region_cmd_id(region_cmd->id());
  request.set_status(pb::coordinator::RegionCmdStatus::STATUS_FAIL);
  request.mutable_error()->set_errcode(static_cast<pb::error::Errno>(status.error_code()));
  request.mutable_error()->set_errmsg(status.error_str());

  status = coordinatro_interaction->SendRequest("UpdateRegionCmdStatus", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[control.region][region({}).job_id({}).cmd_id({})] send UpdateRegionCmdStatus failed, error: {}.",
        region_cmd->region_id(), region_cmd->job_id(), region_cmd->id(), Helper::PrintStatus(status));
  }
}

butil::Status CreateRegionTask::PreValidateCreateRegion(const pb::coordinator::RegionCmd& command) {
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

  return ValidateCreateRegion(store_meta_manager, command.region_id(), command.create_request().region_definition());
}

butil::Status CreateRegionTask::ValidateCreateRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                                     int64_t region_id,
                                                     const pb::common::RegionDefinition& region_definiton) {
  auto region = store_meta_manager->GetStoreRegionMeta()->GetRegion(region_id);
  if (region != nullptr && region->State() != pb::common::StoreRegionState::NEW) {
    return butil::Status(pb::error::EREGION_EXIST, fmt::format("Region {} already exist", region_id));
  }

  // check if there is a range conflict in the store
  auto all_regions = store_meta_manager->GetStoreRegionMeta()->GetAllRegion();

  for (const auto& r : all_regions) {
    if (r->Id() == region_id) {
      DINGO_LOG(WARNING) << fmt::format("[control.region][region({})] create region, region already exist", region_id);
      return butil::Status(pb::error::EREGION_EXIST, fmt::format("Region {} already exist", region_id));
    }

    if (Helper::IsConflictRange(r->Range(), region_definiton.range())) {
      return butil::Status(
          pb::error::EREGION_RANGE_CONFLICT,
          fmt::format("CreateRegion {} range ({}-{}) conflict with local region {} range ({}-{})", region_id,
                      Helper::StringToHex(region_definiton.range().start_key()),
                      Helper::StringToHex(region_definiton.range().end_key()), r->Id(),
                      Helper::StringToHex(r->Range().start_key()), Helper::StringToHex(r->Range().end_key())));
    }
  }

  return butil::Status();
}

butil::Status CreateRegionTask::CreateRegion(const pb::common::RegionDefinition& definition, int64_t parent_region_id) {
  auto region = store::Region::New(definition);
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] create region, region: {}", region->Id(),
                                 region->InnerRegion().ShortDebugString());

  // Valiate region
  auto status = ValidateCreateRegion(store_meta_manager, region->Id(), definition);
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
  parameter.role = GetRole();
  parameter.is_restart = false;
  parameter.raft_endpoint = Server::GetInstance().RaftEndpoint();

  auto config = ConfigManager::GetInstance().GetRoleConfig();
  parameter.raft_path = config->GetString("raft.path");
  parameter.election_timeout_ms = FLAGS_init_election_timeout_ms;
  parameter.log_max_segment_size = config->GetInt64("raft.segmentlog_max_segment_size");
  parameter.log_path = config->GetString("raft.log_path");

  auto raft_meta = store::RaftMeta::New(region->Id());
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
  } else {
    ADD_REGION_CHANGE_RECORD(*region_cmd_);
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

  auto region_raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
  if (region_raw_engine == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[control.region][region({})] delete region, delete data, raw engine is null",
                                    region_id);
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
  if (!Helper::InvalidRange(region->Range())) {
    std::vector<std::string> raw_cf_names;
    std::vector<std::string> txn_cf_names;

    Helper::GetColumnFamilyNames(region->Range().start_key(), raw_cf_names, txn_cf_names);

    if (!raw_cf_names.empty()) {
      status = region_raw_engine->Writer()->KvDeleteRange(raw_cf_names, region->Range());
      if (!status.ok()) {
        DINGO_LOG(FATAL) << fmt::format("[control.region][region({})] delete region data raw failed, error: {}",
                                        region->Id(), status.error_str());
      }
    }

    if (!txn_cf_names.empty()) {
      pb::common::Range txn_range = Helper::GetMemComparableRange(region->Range());
      status = region_raw_engine->Writer()->KvDeleteRange(txn_cf_names, txn_range);
      if (!status.ok()) {
        DINGO_LOG(FATAL) << fmt::format("[control.region][region({})] delete region data txn failed, error: {}",
                                        region->Id(), status.error_str());
      }
    }
  }

  // Index region
  if (GetRole() == pb::common::ClusterRole::INDEX) {
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
  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] delete region finish, ref_count({})", region_id,
                                 region.use_count());
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

  return ValidateSplitRegion(store_meta_manager->GetStoreRegionMeta(), command.split_request(), command.job_id());
}

butil::Status SplitRegionTask::ValidateSplitRegion(std::shared_ptr<StoreRegionMeta> store_region_meta,
                                                   const pb::coordinator::SplitRequest& split_request, int64_t job_id) {
  auto parent_region_id = split_request.split_from_region_id();
  auto child_region_id = split_request.split_to_region_id();

  if (job_id == 0) {
    return butil::Status(pb::error::EJOB_ID_EMPTY, "Job id is empty.");
  }
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
    int64_t region_epoch_version = parent_region->Epoch().version();
    auto vector_index_wrapper = parent_region->VectorIndexWrapper();
    if (vector_index_wrapper != nullptr && vector_index_wrapper->LastBuildEpochVersion() != region_epoch_version) {
      return butil::Status(pb::error::EVECTOR_INDEX_SNAPSHOT_VERSION_NOT_MATCH,
                           "Not match vector index(%lu) snapshot version(%lu/%lu)", parent_region_id,
                           vector_index_wrapper->LastBuildEpochVersion(), region_epoch_version);
    }
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
        request.set_need_hold_if_absent(true);
        request.set_job_id(job_id);
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
        if (response.last_build_epoch_version() != region_epoch_version) {
          return butil::Status(pb::error::EVECTOR_INDEX_SNAPSHOT_VERSION_NOT_MATCH,
                               "Not match vector index(%lu) snapshot version(%lu/%lu) at peer %s", parent_region_id,
                               response.last_build_epoch_version(), region_epoch_version,
                               Helper::EndPointToStr(peer.addr).c_str());
        }
      }
    }
  }

  return butil::Status();
}

butil::Status SplitRegionTask::SplitRegion() {
  auto store_region_meta = GET_STORE_REGION_META;

  auto status = ValidateSplitRegion(store_region_meta, region_cmd_->split_request(), region_cmd_->job_id());
  if (!status.ok()) {
    return status;
  }

  auto parent_region = store_region_meta->GetRegion(region_cmd_->split_request().split_from_region_id());
  if (parent_region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Parent region not exist.");
  }

  ADD_REGION_CHANGE_RECORD(*region_cmd_);

  // Commit raft log
  ctx_->SetRegionId(region_cmd_->split_request().split_from_region_id());
  ctx_->SetRegionEpoch(parent_region->Epoch());
  return Server::GetInstance().GetEngine()->AsyncWrite(
      ctx_, WriteDataBuilder::BuildWrite(region_cmd_->job_id(), region_cmd_->split_request(), parent_region->Epoch()),
      [](std::shared_ptr<Context>, butil::Status status) {
        if (!status.ok()) {
          LOG(ERROR) << fmt::format("[control.region][region()] write split failed, error: {}", status.error_str());
        }
      });
}

void SplitRegionTask::Run() {
  DINGO_LOG(INFO) << fmt::format("[split.spliting][job_id({}).region({}->{})] Run split region, details: {}",
                                 region_cmd_->job_id(), region_cmd_->split_request().split_from_region_id(),
                                 region_cmd_->split_request().split_to_region_id(), region_cmd_->ShortDebugString());
  auto status = SplitRegion();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][job_id({}).region({}->{})] Split failed, error: {}",
                                    region_cmd_->job_id(), region_cmd_->split_request().split_from_region_id(),
                                    region_cmd_->split_request().split_to_region_id(), Helper::PrintStatus(status));

    NotifyRegionCmdStatus(region_cmd_, status);
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
}

butil::Status MergeRegionTask::PreValidateMergeRegion(const pb::coordinator::RegionCmd& command) {
  auto store_region_meta = GET_STORE_REGION_META;
  const auto& merge_request = command.merge_request();
  if (command.region_id() != merge_request.source_region_id()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param source_region_id must equal RegionCmd.region_id");
  }
  if (command.job_id() == 0) {
    return butil::Status(pb::error::EJOB_ID_EMPTY, "Job id is empty.");
  }

  int64_t min_applied_log_id = 0;
  auto status = ValidateMergeRegion(store_region_meta, merge_request, min_applied_log_id);
  if (!status.ok()) {
    DINGO_LOG(INFO) << fmt::format("[merge.merging][job_id({}).region({}/{})] Merge failed, error: {} {}",
                                   command.job_id(), merge_request.source_region_id(), merge_request.target_region_id(),
                                   status.error_code(), status.error_str());
  }

  return status;
}

// Check split/merge/change_peer raft log
butil::Status CheckChangeRegionLog(int64_t region_id, int64_t min_applied_log_id) {
  auto log_storage = Server::GetInstance().GetLogStorageManager()->GetLogStorage(region_id);
  if (log_storage == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND_LOG_STORAGE, fmt::format("Not found log storage {}", region_id));
  }

  bool has = log_storage->HasSpecificLog(min_applied_log_id, INT64_MAX, [&](const LogEntry& log_entry) -> bool {
    if (log_entry.type == LogEntryType::kEntryTypeData) {
      auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
      butil::IOBufAsZeroCopyInputStream wrapper(log_entry.data);
      CHECK(raft_cmd->ParseFromZeroCopyStream(&wrapper));
      for (const auto& request : raft_cmd->requests()) {
        if (request.cmd_type() == pb::raft::CmdType::SPLIT || request.cmd_type() == pb::raft::CmdType::PREPARE_MERGE ||
            request.cmd_type() == pb::raft::CmdType::COMMIT_MERGE ||
            request.cmd_type() == pb::raft::CmdType::ROLLBACK_MERGE) {
          LOG(INFO) << fmt::format("[merge.merging][region({})] Exist split/merge/change_peer log recently", region_id)
                    << ", min_applied_log_id: " << min_applied_log_id
                    << ", cmd_type: " << pb::raft::CmdType_Name(request.cmd_type());
          return true;
        }
      }
    } else if (log_entry.type == LogEntryType::kEntryTypeConfiguration) {
      LOG(INFO) << fmt::format("[merge.merging][region({})] Exist split/merge/change_peer log recently", region_id)
                << ", min_applied_log_id: " << min_applied_log_id << ", log_entry_type: kEntryTypeConfiguration";
      return true;
    }
    return false;
  });

  if (has) {
    return butil::Status(pb::error::ERAFT_EXIST_CHANGE_LOG,
                         fmt::format("Exist split/merge/change_peer log recently {}", region_id));
  }
  return butil::Status();
}

butil::Status MergeRegionTask::ValidateMergeRegion(std::shared_ptr<StoreRegionMeta> store_region_meta,
                                                   const pb::coordinator::MergeRequest& merge_request,
                                                   int64_t& source_min_applied_log_id) {
  if (merge_request.source_region_id() == 0 || merge_request.target_region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param source_region_id/target_region_id is error");
  }
  auto source_region = store_region_meta->GetRegion(merge_request.source_region_id());
  if (source_region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found source region");
  }

  auto target_region = store_region_meta->GetRegion(merge_request.target_region_id());
  if (target_region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found target region");
  }

  if (source_region->TemporaryDisableChange()) {
    return butil::Status(pb::error::EREGION_DISABLE_CHANGE, "Temporary disable source region change.");
  }

  if (target_region->TemporaryDisableChange()) {
    return butil::Status(pb::error::EREGION_DISABLE_CHANGE, "Temporary disable target region change.");
  }

  // Check region adjoin
  if (source_region->Range().end_key() != target_region->Range().start_key() &&
      source_region->Range().start_key() != target_region->Range().end_key()) {
    return butil::Status(pb::error::EREGION_NOT_NEIGHBOR, "Not neighbor region");
  }

  // Check region peers
  if (Helper::IsDifferencePeers(source_region->Definition(), target_region->Definition())) {
    return butil::Status(pb::error::EMERGE_PEER_NOT_MATCH, "Peers is differencce.");
  }

  // Check source region follower commit log progress.
  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Not found raft store engine");
  }

  auto source_node = raft_store_engine->GetNode(source_region->Id());
  if (source_node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "No found raft node.");
  }
  auto target_node = raft_store_engine->GetNode(target_region->Id());
  if (target_node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "No found raft node.");
  }
  if (!source_node->IsLeader()) {
    return butil::Status(pb::error::ERAFT_NOTLEADER, source_node->GetLeaderId().to_string());
  }
  auto source_raft_status = source_node->GetStatus();
  if (source_raft_status == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Get source region raft status failed");
  }
  auto target_raft_status = target_node->GetStatus();
  if (target_raft_status == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Get target region raft status failed");
  }

  if (!source_raft_status->unstable_followers().empty()) {
    return butil::Status(pb::error::EINTERNAL, "Has unstable followers");
  }

  // Check remote source/target region epoch
  for (const auto& [peer_addr, follower] : source_raft_status->stable_followers()) {
    if (follower.consecutive_error_times() > 0) {
      return butil::Status(pb::error::EINTERNAL, "follower %s abnormal", peer_addr.c_str());
    }

    auto region_metas =
        ServiceAccess::GetRegionInfo({source_region->Id(), target_region->Id()}, Helper::GetEndPoint(peer_addr));
    if (region_metas.size() != 2) {
      return butil::Status(pb::error::EINTERNAL, "Get remote region info failed at node(%s)", peer_addr.c_str());
    }
    for (const auto& region_meta : region_metas) {
      if (region_meta.id() == source_region->Id() &&
          !Helper::IsEqualRegionEpoch(source_region->Epoch(), region_meta.definition().epoch())) {
        return butil::Status(pb::error::EREGION_VERSION,
                             fmt::format("Not match source region epoch ({} / {}) at node({})",
                                         Helper::RegionEpochToString(region_meta.definition().epoch()),
                                         Helper::RegionEpochToString(source_region->Epoch()), peer_addr));
      } else if (region_meta.id() == target_region->Id() &&
                 !Helper::IsEqualRegionEpoch(target_region->Epoch(), region_meta.definition().epoch())) {
        return butil::Status(pb::error::EREGION_VERSION,
                             fmt::format("Not match target region epoch ({} / {}) at node({})",
                                         Helper::RegionEpochToString(region_meta.definition().epoch()),
                                         Helper::RegionEpochToString(target_region->Epoch()), peer_addr));
      }
    }
  }

  // Check raft status
  source_min_applied_log_id = source_raft_status->known_applied_index();
  int64_t source_min_committed_log_id = source_raft_status->committed_index();
  int64_t target_min_applied_log_id = target_raft_status->known_applied_index();
  int64_t target_min_committed_log_id = target_raft_status->committed_index();
  for (const auto& [peer_addr, follower] : source_raft_status->stable_followers()) {
    auto raft_status_entries =
        ServiceAccess::GetRaftStatus({source_region->Id(), target_region->Id()}, Helper::GetEndPoint(peer_addr));
    if (raft_status_entries.size() != 2) {
      return butil::Status(pb::error::EINTERNAL, "Get remote raft node info failed at node(%s)", peer_addr.c_str());
    }
    for (const auto& entry : raft_status_entries) {
      if (entry.raft_status().peer_id().empty()) {
        return butil::Status(pb::error::EINTERNAL, "Get peer raft status failed.");
      }

      if (entry.region_id() == source_region->Id()) {
        source_min_committed_log_id = std::min(source_min_committed_log_id, entry.raft_status().committed_index());
        source_min_applied_log_id = std::min(source_min_applied_log_id, entry.raft_status().known_applied_index());

      } else if (entry.region_id() == target_region->Id()) {
        target_min_committed_log_id = std::min(target_min_committed_log_id, entry.raft_status().committed_index());
        target_min_applied_log_id = std::min(target_min_applied_log_id, entry.raft_status().known_applied_index());
      }
    }
  }

  if (source_raft_status->last_index() - source_min_committed_log_id > FLAGS_merge_committed_log_gap) {
    return butil::Status(
        pb::error::EINTERNAL,
        fmt::format("Source region log gap too large, merge_committed_log_gap({}) "
                    "last_index({}) committed_index({}).",
                    FLAGS_merge_committed_log_gap, source_raft_status->last_index(), source_min_committed_log_id));
  }

  if (target_raft_status->last_index() - target_min_committed_log_id > FLAGS_merge_committed_log_gap) {
    return butil::Status(
        pb::error::EINTERNAL,
        fmt::format("Target region log gap too large, merge_committed_log_gap({}) "
                    "last_index({}) committed_index({}).",
                    FLAGS_merge_committed_log_gap, target_raft_status->last_index(), target_min_committed_log_id));
  }

  // Check whether have split/merge/change_peer raft log since min_applied_log_id.
  auto status = CheckChangeRegionLog(source_region->Id(), source_min_committed_log_id + 1);
  if (!status.ok()) {
    return status;
  }
  status = CheckChangeRegionLog(target_region->Id(), target_min_committed_log_id + 1);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status MergeRegionTask::MergeRegion() {
  const auto& merge_request = region_cmd_->merge_request();
  auto store_region_meta = GET_STORE_REGION_META;
  assert(store_region_meta != nullptr);

  // Todo: forbid truncate raft log.
  if (region_cmd_->job_id() == 0) {
    return butil::Status(pb::error::EJOB_ID_EMPTY, "Job id is empty.");
  }
  auto source_region = store_region_meta->GetRegion(merge_request.source_region_id());
  if (source_region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND,
                         fmt::format("Not found source region {}", merge_request.source_region_id()));
  }

  auto target_region = store_region_meta->GetRegion(merge_request.target_region_id());
  if (target_region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND,
                         fmt::format("Not found target region {}", merge_request.target_region_id()));
  }

  int64_t min_applied_log_id = 0;
  auto status = ValidateMergeRegion(store_region_meta, merge_request, min_applied_log_id);
  if (!status.ok()) {
    return status;
  }

  ADD_REGION_CHANGE_RECORD(*region_cmd_);

  // Commit raft cmd
  auto ctx = std::make_shared<Context>();
  ctx->SetRegionId(source_region->Id());
  ctx->SetRegionEpoch(source_region->Epoch());

  status = Server::GetInstance().GetStorage()->PrepareMerge(ctx, region_cmd_->job_id(), target_region->Definition(),
                                                            min_applied_log_id);
  if (!status.ok()) {
    return status;
  }

  // Disable region change
  store_region_meta->UpdateTemporaryDisableChange(source_region, true);
  store_region_meta->UpdateTemporaryDisableChange(target_region, true);

  return butil::Status();
}

void MergeRegionTask::Run() {
  DINGO_LOG(INFO) << fmt::format("[merge.merging][job_id({}).region({}/{})] Run merge region, details: {}",
                                 region_cmd_->job_id(), region_cmd_->merge_request().source_region_id(),
                                 region_cmd_->merge_request().target_region_id(), region_cmd_->ShortDebugString());

  auto status = MergeRegion();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[merge.merging][job_id({}).region({}/{})] Merge failed, error: {}",
                                    region_cmd_->job_id(), region_cmd_->merge_request().source_region_id(),
                                    region_cmd_->merge_request().target_region_id(), Helper::PrintStatus(status));

    NotifyRegionCmdStatus(region_cmd_, status);
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

  if (command.job_id() == 0) {
    return butil::Status(pb::error::EJOB_ID_EMPTY, "Job id is empty.");
  }

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

butil::Status ChangeRegionTask::ChangeRegion(std::shared_ptr<Context> ctx, RegionCmdPtr command) {
  const auto& region_definition = command->change_peer_request().region_definition();
  auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();
  DINGO_LOG(INFO) << fmt::format("[control.region][region({})] change region, region definition: {}",
                                 region_definition.id(), region_definition.ShortDebugString());

  if (command->job_id() == 0) {
    return butil::Status(pb::error::EJOB_ID_EMPTY, "Job id is empty.");
  }

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
  auto status = ChangeRegion(ctx_, region_cmd_);
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
  return engine->SaveSnapshot(ctx, region_id, true);
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

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
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
          "[control.region][region({})] update definition new max elements {} > old max elements {}, resize "
          "success",
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
  auto store_region_meta = GET_STORE_REGION_META;

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
  auto store_region_meta = GET_STORE_REGION_META;

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
  auto store_region_meta = GET_STORE_REGION_META;
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

butil::Status HoldVectorIndexTask::HoldVectorIndex(std::shared_ptr<Context> /*ctx*/, RegionCmdPtr region_cmd) {
  int64_t region_id = region_cmd->hold_vector_index_request().region_id();
  bool is_hold = region_cmd->hold_vector_index_request().is_hold();

  auto store_region_meta = GET_STORE_REGION_META;
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
    ADD_REGION_CHANGE_RECORD(*region_cmd);
    // Load vector index.
    DINGO_LOG(INFO) << fmt::format("[vector_index.hold][index_id({})] launch load or build vector index.", region_id);
    // use slow load
    VectorIndexManager::LaunchLoadAsyncBuildVectorIndex(vector_index_wrapper, true, false, region_cmd->job_id(),
                                                        "hold vector index");

  } else {
    // Delete vector index.
    if (vector_index_wrapper->IsOwnReady()) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.hold][region({})] delete vector index.", region_id);
      vector_index_wrapper->ClearVectorIndex(fmt::format("{}-nohold", region_cmd->job_id()));
    }
  }

  return butil::Status();
}

void HoldVectorIndexTask::Run() {
  auto status = HoldVectorIndex(ctx_, region_cmd_);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[control.region][region({})] hold vector index  failed, {}",
                                      region_cmd_->switch_split_request().region_id(), status.error_str());
  }

  Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
}

butil::Status SnapshotVectorIndexTask::SaveSnapshotSync(std::shared_ptr<Context> /*ctx*/, int64_t vector_index_id) {
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

  auto status = VectorIndexManager::SaveVectorIndex(vector_index_wrapper, "from client");
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

butil::Status SnapshotVectorIndexTask::PreValidateSnapshotVectorIndex(const pb::coordinator::RegionCmd& command) {
  return ValidateSaveVectorIndex(command.snapshot_vector_index_request().vector_index_id());
}

butil::Status SnapshotVectorIndexTask::ValidateSaveVectorIndex(int64_t region_id) {
  auto store_region_meta = GET_STORE_REGION_META;
  auto region = store_region_meta->GetRegion(region_id);
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", region_id));
  }

  // Validate raft node exists
  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    auto node = raft_store_engine->GetNode(region_id);
    if (node == nullptr) {
      return butil::Status(pb::error::ERAFT_NOT_FOUND, "No found raft node %lu.", region_id);
    }
  }

  return butil::Status();
}

butil::Status SnapshotVectorIndexTask::SaveSnapshotAsync(std::shared_ptr<Context> /*ctx*/, RegionCmdPtr region_cmd) {
  int64_t region_id = region_cmd->snapshot_vector_index_request().vector_index_id();
  int64_t raft_log_index = region_cmd->snapshot_vector_index_request().raft_log_index();

  auto store_region_meta = GET_STORE_REGION_META;
  auto region = store_region_meta->GetRegion(region_id);
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", region_id));
  }

  auto status = ValidateSaveVectorIndex(region_id);
  if (!status.ok()) {
    return status;
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, fmt::format("Not found vector index {}", region_id));
  }

  ADD_REGION_CHANGE_RECORD(*region_cmd);

  if (raft_log_index > 0 && vector_index_wrapper->SnapshotLogId() >= raft_log_index) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.save][index_id({})] skip save vector index.", region_id) << ", "
                    << vector_index_wrapper->SnapshotLogId() << " >= " << raft_log_index;
    return butil::Status();
  }

  // Save vector index.
  DINGO_LOG(INFO) << fmt::format("[vector_index.save][index_id({})] region_save_vector_index.", region_id);
  VectorIndexManager::LaunchSaveVectorIndex(vector_index_wrapper,
                                            fmt::format("{}-region_save_vector_index", region_cmd->job_id()));

  return butil::Status();
}

void SnapshotVectorIndexTask::Run() {
  if (region_cmd_->snapshot_vector_index_request().raft_log_index() == 0) {
    auto status = SaveSnapshotSync(ctx_, region_cmd_->snapshot_vector_index_request().vector_index_id());
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[control.region][region({})] save vector index snapshot failed, error: {}",
                                      region_cmd_->snapshot_vector_index_request().vector_index_id(),
                                      status.error_str());
    }

    Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
        region_cmd_,
        status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
  } else {
    auto status = SaveSnapshotAsync(ctx_, region_cmd_);
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format("[control.region][region({})] save vector index  failed, {}",
                                        region_cmd_->switch_split_request().region_id(), status.error_str());
    }

    Server::GetInstance().GetRegionCommandManager()->UpdateCommandStatus(
        region_cmd_,
        status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
  }

  // Notify coordinator
  if (region_cmd_->is_notify()) {
    Heartbeat::TriggerStoreHeartbeat({region_cmd_->region_id()});
  }
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

void RegionCommandManager::AddCommand(RegionCmdPtr region_cmd) {
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

void RegionCommandManager::UpdateCommandStatus(RegionCmdPtr region_cmd, pb::coordinator::RegionCmdStatus status) {
  region_cmd->set_status(status);
  meta_writer_->Put(TransformToKv(region_cmd));
}

void RegionCommandManager::UpdateCommandStatus(int64_t command_id, pb::coordinator::RegionCmdStatus status) {
  auto region_cmd = GetCommand(command_id);
  if (region_cmd != nullptr) {
    UpdateCommandStatus(region_cmd, status);
  }
}

RegionCmdPtr RegionCommandManager::GetCommand(int64_t command_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = region_commands_.find(command_id);
  if (it == region_commands_.end()) {
    return nullptr;
  }

  return it->second;
}

std::vector<RegionCmdPtr> RegionCommandManager::GetCommands(pb::coordinator::RegionCmdStatus status) {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<RegionCmdPtr> commands;
  for (auto& [_, command] : region_commands_) {
    if (command->status() == status) {
      commands.push_back(command);
    }
  }

  std::sort(commands.begin(), commands.end(),
            [](const RegionCmdPtr& a, const RegionCmdPtr& b) { return a->id() < b->id(); });

  return commands;
}

std::vector<RegionCmdPtr> RegionCommandManager::GetCommands(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<RegionCmdPtr> commands;
  for (auto& [_, command] : region_commands_) {
    if (command->region_id() == region_id) {
      commands.push_back(command);
    }
  }

  std::sort(commands.begin(), commands.end(),
            [](const RegionCmdPtr& a, const RegionCmdPtr& b) { return a->id() < b->id(); });

  return commands;
}

std::vector<RegionCmdPtr> RegionCommandManager::GetAllCommand() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<RegionCmdPtr> commands;
  commands.reserve(region_commands_.size());
  for (auto& [_, command] : region_commands_) {
    commands.push_back(command);
  }

  std::sort(commands.begin(), commands.end(),
            [](const RegionCmdPtr& a, const RegionCmdPtr& b) { return a->id() < b->id(); });

  return commands;
}

std::shared_ptr<pb::common::KeyValue> RegionCommandManager::TransformToKv(std::any obj) {
  auto region_cmd = std::any_cast<RegionCmdPtr>(obj);
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

  auto regions = Server::GetInstance().GetAllAliveRegion();
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

butil::Status RegionController::InnerDispatchRegionControlCommand(std::shared_ptr<Context> ctx, RegionCmdPtr command) {
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

butil::Status RegionController::DispatchRegionControlCommand(std::shared_ptr<Context> ctx, RegionCmdPtr command) {
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
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<CreateRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_DELETE,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<DeleteRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_SPLIT,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<SplitRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_MERGE,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<MergeRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_CHANGE_PEER,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<ChangeRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_TRANSFER_LEADER,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<TransferLeaderTask>(ctx, command);
     }},
    {pb::coordinator::CMD_SNAPSHOT,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<SnapshotRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_PURGE,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<PurgeRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_STOP,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<StopRegionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_DESTROY_EXECUTOR,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<DestroyRegionExecutorTask>(ctx, command);
     }},
    {pb::coordinator::CMD_SNAPSHOT_VECTOR_INDEX,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<SnapshotVectorIndexTask>(ctx, command);
     }},
    {pb::coordinator::CMD_UPDATE_DEFINITION,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<UpdateDefinitionTask>(ctx, command);
     }},
    {pb::coordinator::CMD_SWITCH_SPLIT,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<SwitchSplitTask>(ctx, command);
     }},
    {pb::coordinator::CMD_HOLD_VECTOR_INDEX,
     [](std::shared_ptr<Context> ctx, RegionCmdPtr command) -> TaskRunnablePtr {
       return std::make_shared<HoldVectorIndexTask>(ctx, command);
     }},
};

RegionController::ValidaterMap RegionController::validaters = {
    {pb::coordinator::CMD_CREATE, CreateRegionTask::PreValidateCreateRegion},
    {pb::coordinator::CMD_DELETE, DeleteRegionTask::PreValidateDeleteRegion},
    {pb::coordinator::CMD_SPLIT, SplitRegionTask::PreValidateSplitRegion},
    {pb::coordinator::CMD_MERGE, MergeRegionTask::PreValidateMergeRegion},
    {pb::coordinator::CMD_CHANGE_PEER, ChangeRegionTask::PreValidateChangeRegion},
    {pb::coordinator::CMD_TRANSFER_LEADER, TransferLeaderTask::PreValidateTransferLeader},
    {pb::coordinator::CMD_PURGE, PurgeRegionTask::PreValidatePurgeRegion},
    {pb::coordinator::CMD_STOP, StopRegionTask::PreValidateStopRegion},
    {pb::coordinator::CMD_UPDATE_DEFINITION, UpdateDefinitionTask::PreValidateUpdateDefinition},
    {pb::coordinator::CMD_SWITCH_SPLIT, SwitchSplitTask::PreValidateSwitchSplit},
    {pb::coordinator::CMD_HOLD_VECTOR_INDEX, HoldVectorIndexTask::PreValidateHoldVectorIndex},
    {pb::coordinator::CMD_SNAPSHOT_VECTOR_INDEX, SnapshotVectorIndexTask::PreValidateSnapshotVectorIndex},
};

}  // namespace dingodb

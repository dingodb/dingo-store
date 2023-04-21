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
#include <cstddef>
#include <cstdint>
#include <memory>

#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "event/store_state_machine_event.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"

namespace dingodb {

butil::Status CreateRegionTask::ValidateCreateRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                                     uint64_t region_id) {
  auto region = store_meta_manager->GetStoreRegionMeta()->GetRegion(region_id);
  if (region != nullptr && region->state() != pb::common::StoreRegionState::NEW) {
    return butil::Status(pb::error::EREGION_ALREADY_EXIST, "Region already exist");
  }

  return butil::Status();
}

butil::Status CreateRegionTask::CreateRegion(std::shared_ptr<Context> ctx,
                                             std::shared_ptr<pb::store_internal::Region> region,
                                             uint64_t split_from_region_id) {
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  DINGO_LOG(DEBUG) << butil::StringPrintf("Create region %lu, %s", region->id(), region->ShortDebugString().c_str());

  // Valiate region
  auto status = ValidateCreateRegion(store_meta_manager, region->id());
  if (!status.ok()) {
    return status;
  }

  // Add region to store region meta manager
  DINGO_LOG(DEBUG) << butil::StringPrintf("Create region %lu save region meta", region->id());
  auto store_region_meta = store_meta_manager->GetStoreRegionMeta();
  region->set_state(pb::common::StoreRegionState::NEW);
  store_region_meta->AddRegion(region);

  // Add raft node
  DINGO_LOG(DEBUG) << butil::StringPrintf("Create region %lu add raft node", region->id());
  auto engine = Server::GetInstance()->GetEngine();
  if (engine->GetID() == pb::common::ENG_RAFT_STORE) {
    auto raft_meta = StoreRaftMeta::NewRaftMeta(region->id());
    Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta()->AddRaftMeta(raft_meta);

    auto listener_factory = std::make_shared<StoreSmEventListenerFactory>();

    auto raft_kv_engine = std::dynamic_pointer_cast<RaftKvEngine>(engine);
    status = raft_kv_engine->AddNode(ctx, region, raft_meta, listener_factory->Build());
    if (!status.ok()) {
      return status;
    }
  }

  DINGO_LOG(DEBUG) << butil::StringPrintf("Create region %lu update region state NORMAL", region->id());
  if (split_from_region_id == 0) {
    store_region_meta->UpdateState(region, pb::common::StoreRegionState::NORMAL);
  } else {
    store_region_meta->UpdateState(region, pb::common::StoreRegionState::STANDBY);
  }

  // Add region metrics
  DINGO_LOG(DEBUG) << butil::StringPrintf("Create region %lu add region metrics", region->id());
  Server::GetInstance()->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(
      StoreRegionMetrics::NewMetrics(region->id()));

  return butil::Status();
}

void CreateRegionTask::Run() {
  auto region = std::make_shared<pb::store_internal::Region>();
  region->set_id(region_cmd_->region_id());
  region->mutable_definition()->CopyFrom(region_cmd_->create_request().region_definition());
  region->set_state(pb::common::StoreRegionState::NEW);

  auto status = CreateRegion(ctx_, region, region_cmd_->create_request().split_from_region_id());
  if (!status.ok()) {
    DINGO_LOG(DEBUG) << butil::StringPrintf("Create region %lu failed, %s", region->id(), status.error_cstr());
  }

  Server::GetInstance()->GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status DeleteRegionTask::ValidateDeleteRegion(std::shared_ptr<StoreMetaManager> /*store_meta_manager*/,
                                                     std::shared_ptr<pb::store_internal::Region> region) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Region is not exist, can't delete.");
  }
  if (region->state() == pb::common::StoreRegionState::DELETING ||
      region->state() == pb::common::StoreRegionState::DELETED) {
    return butil::Status(pb::error::EREGION_ALREADY_DELETED, "Region is deleting or deleted.");
  }

  if (region->state() == pb::common::StoreRegionState::SPLITTING ||
      region->state() == pb::common::StoreRegionState::MERGING) {
    return butil::Status(pb::error::EREGION_STATE, "Region state not allow delete.");
  }

  return butil::Status();
}

butil::Status DeleteRegionTask::DeleteRegion(std::shared_ptr<Context> ctx, uint64_t region_id) {
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  auto store_region_meta = store_meta_manager->GetStoreRegionMeta();
  auto region = store_region_meta->GetRegion(region_id);

  DINGO_LOG(DEBUG) << butil::StringPrintf("Delete region %lu", region_id);
  // Valiate region
  auto status = ValidateDeleteRegion(store_meta_manager, region);
  if (!status.ok()) {
    return status;
  }

  // Update state
  DINGO_LOG(DEBUG) << butil::StringPrintf("Delete region %lu update region state DELETING", region_id);
  store_region_meta->UpdateState(region, pb::common::StoreRegionState::DELETING);

  // Shutdown raft node
  auto engine = Server::GetInstance()->GetEngine();

  // Delete data
  DINGO_LOG(DEBUG) << butil::StringPrintf("Delete region %lu delete data", region_id);
  auto writer = engine->GetRawEngine()->NewWriter(Constant::kStoreDataCF);
  writer->KvDeleteRange(region->definition().range());

  // Raft kv engine
  if (engine->GetID() == pb::common::ENG_RAFT_STORE) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftKvEngine>(engine);
    // Delete raft
    DINGO_LOG(DEBUG) << butil::StringPrintf("Delete region %lu delete raft node", region_id);
    raft_kv_engine->DestroyNode(ctx, region_id);
  }

  // Delete meta data
  DINGO_LOG(DEBUG) << butil::StringPrintf("Delete region %lu update region state DELETED", region_id);
  store_region_meta->UpdateState(region, pb::common::StoreRegionState::DELETED);
  DINGO_LOG(DEBUG) << butil::StringPrintf("Delete region %lu delete region metrics", region_id);
  Server::GetInstance()->GetStoreMetricsManager()->GetStoreRegionMetrics()->DeleteMetrics(region_id);

  return butil::Status();
}

void DeleteRegionTask::Run() {
  auto status = DeleteRegion(ctx_, region_cmd_->delete_request().region_id());
  if (!status.ok()) {
    DINGO_LOG(DEBUG) << butil::StringPrintf("Delete region %lu failed, %s", region_cmd_->delete_request().region_id(),
                                            status.error_cstr());
  }

  Server::GetInstance()->GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status SplitRegionTask::ValidateSplitRegion(std::shared_ptr<StoreRegionMeta> store_region_meta,
                                                   const pb::coordinator::SplitRequest& split_request) {
  auto parent_region_id = split_request.split_from_region_id();
  auto child_region_id = split_request.split_to_region_id();

  auto parent_region = store_region_meta->GetRegion(parent_region_id);
  if (parent_region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Parent region not exist.");
  }
  auto child_region = store_region_meta->GetRegion(child_region_id);
  if (child_region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Child region not exist.");
  }

  const auto& split_key = split_request.split_watershed_key();
  auto range = parent_region->definition().range();
  if (range.start_key().compare(split_key) >= 0 || range.end_key().compare(split_key) <= 0) {
    return butil::Status(pb::error::EKEY_SPLIT, "Split key is invalid.");
  }

  if (parent_region->state() == pb::common::StoreRegionState::SPLITTING) {
    return butil::Status(pb::error::EREGION_ALREADY_SPLIT, "Parent region state is splitting.");
  }

  if (parent_region->state() == pb::common::StoreRegionState::NEW ||
      parent_region->state() == pb::common::StoreRegionState::MERGING ||
      parent_region->state() == pb::common::StoreRegionState::DELETING ||
      parent_region->state() == pb::common::StoreRegionState::DELETED) {
    return butil::Status(pb::error::EREGION_STATE, "Parent region state not allow split.");
  }

  auto engine = Server::GetInstance()->GetEngine();
  if (engine->GetID() == pb::common::ENG_RAFT_STORE) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftKvEngine>(engine);
    auto node = raft_kv_engine->GetNode(parent_region_id);
    if (node == nullptr) {
      return butil::Status(pb::error::ERAFT_NOTNODE, "No found raft node.");
    }

    if (!node->IsLeader()) {
      return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
    }
  }

  return butil::Status();
}

butil::Status SplitRegionTask::SplitRegion() {
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();

  auto status = ValidateSplitRegion(store_region_meta, region_cmd_->split_request());
  if (!status.ok()) {
    return status;
  }

  // Commit raft log
  WriteData write_data;
  auto datum = std::make_shared<SplitDatum>();
  datum->from_region_id = region_cmd_->split_request().split_from_region_id();
  datum->to_region_id = region_cmd_->split_request().split_to_region_id();
  datum->split_key = region_cmd_->split_request().split_watershed_key();

  write_data.AddDatums(std::static_pointer_cast<DatumAble>(datum));

  ctx_->SetRegionId(datum->from_region_id);
  return Server::GetInstance()->GetEngine()->AsyncWrite(
      ctx_, write_data, [](std::shared_ptr<Context>, butil::Status status) {
        if (!status.ok()) {
          DINGO_LOG(ERROR) << "Write split failed, " << status.error_str();
        }
      });
}

void SplitRegionTask::Run() {
  auto status = SplitRegion();
  if (!status.ok()) {
    DINGO_LOG(DEBUG) << butil::StringPrintf("Split region %lu failed, %s",
                                            region_cmd_->split_request().split_from_region_id(), status.error_cstr());
  }

  Server::GetInstance()->GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status ChangeRegionTask::ValidateChangeRegion(std::shared_ptr<StoreMetaManager> store_meta_manager,
                                                     const pb::common::RegionDefinition& region_definition) {
  auto region = store_meta_manager->GetStoreRegionMeta()->GetRegion(region_definition.id());
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Region not exist, cant't change.");
  }

  if (region->state() != pb::common::StoreRegionState::NORMAL) {
    return butil::Status(pb::error::EREGION_STATE, "Region state not allow change.");
  }

  auto engine = Server::GetInstance()->GetEngine();
  if (engine->GetID() == pb::common::ENG_RAFT_STORE) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftKvEngine>(engine);
    auto node = raft_kv_engine->GetNode(region_definition.id());
    if (node == nullptr) {
      return butil::Status(pb::error::ERAFT_NOTNODE, "No found raft node.");
    }

    if (!node->IsLeader()) {
      return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
    }
  }

  return butil::Status();
}

butil::Status ChangeRegionTask::ChangeRegion(std::shared_ptr<Context> ctx,
                                             const pb::common::RegionDefinition& region_definition) {
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  DINGO_LOG(DEBUG) << butil::StringPrintf("Change region %lu, %s", region_definition.id(),
                                          region_definition.ShortDebugString().c_str());

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

  auto engine = Server::GetInstance()->GetEngine();
  if (engine->GetID() == pb::common::ENG_RAFT_STORE) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftKvEngine>(engine);
    return raft_kv_engine->ChangeNode(ctx, region_definition.id(), filter_peers_by_role(pb::common::VOTER));
  }

  return butil::Status();
}

void ChangeRegionTask::Run() {
  auto status = ChangeRegion(ctx_, region_cmd_->change_peer_request().region_definition());
  if (!status.ok()) {
    DINGO_LOG(DEBUG) << butil::StringPrintf("Change region %lu failed, %s",
                                            region_cmd_->change_peer_request().region_definition().id(),
                                            status.error_cstr());
  }

  Server::GetInstance()->GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

butil::Status SnapshotRegionTask::Snapshot(std::shared_ptr<Context> ctx, uint64_t region_id) {
  auto engine = Server::GetInstance()->GetEngine();
  return engine->DoSnapshot(ctx, region_id);
}

void SnapshotRegionTask::Run() {
  auto status = Snapshot(ctx_, region_cmd_->region_id());
  if (!status.ok()) {
    DINGO_LOG(DEBUG) << butil::StringPrintf("Snapshot region %lu failed, %s", region_cmd_->region_id(),
                                            status.error_cstr());
  }

  Server::GetInstance()->GetRegionCommandManager()->UpdateCommandStatus(
      region_cmd_,
      status.ok() ? pb::coordinator::RegionCmdStatus::STATUS_DONE : pb::coordinator::RegionCmdStatus::STATUS_FAIL);
}

static int ExecuteRoutine(void*, bthread::TaskIterator<TaskRunnable*>& iter) {
  std::unique_ptr<TaskRunnable> self_guard(*iter);
  if (iter.is_queue_stopped()) {
    return 0;
  }

  for (; iter; ++iter) {
    (*iter)->Run();
  }

  return 0;
}

bool RegionControlExecutor::Init() {
  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;

  if (bthread::execution_queue_start(&queue_id_, &options, ExecuteRoutine, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Start execution queue failed, region: " << region_id_;
    return false;
  }

  return true;
}

bool RegionControlExecutor::Execute(TaskRunnable* task) {
  DINGO_LOG(INFO) << "Execute region control...";
  if (bthread::execution_queue_execute(queue_id_, task) != 0) {
    DINGO_LOG(ERROR) << "region execution queue execute failed, regoin: " << region_id_;
    return false;
  }
  return true;
}

void RegionControlExecutor::Stop() {
  if (bthread::execution_queue_stop(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "region execution queue stop failed, regoin: " << region_id_;
    return;
  }

  if (bthread::execution_queue_join(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "region execution queue join failed, regoin: " << region_id_;
  }
}

bool RegionCommandManager::Init() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    DINGO_LOG(ERROR) << "Scan store raft meta failed!";
    return false;
  }

  if (!kvs.empty()) {
    TransformFromKv(kvs);
  }
  return true;
}

bool RegionCommandManager::IsExist(uint64_t command_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return region_commands_.find(command_id) != region_commands_.end();
}

void RegionCommandManager::AddCommand(std::shared_ptr<pb::coordinator::RegionCmd> region_cmd) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (region_commands_.find(region_cmd->id()) != region_commands_.end()) {
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

void RegionCommandManager::UpdateCommandStatus(uint64_t command_id, pb::coordinator::RegionCmdStatus status) {
  auto region_cmd = GetCommand(command_id);
  if (region_cmd != nullptr) {
    UpdateCommandStatus(region_cmd, status);
  }
}

std::shared_ptr<pb::coordinator::RegionCmd> RegionCommandManager::GetCommand(uint64_t command_id) {
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

std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> RegionCommandManager::GetAllCommand() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> commands;
  for (auto& [_, command] : region_commands_) {
    commands.push_back(command);
  }

  std::sort(commands.begin(), commands.end(),
            [](const std::shared_ptr<pb::coordinator::RegionCmd>& a,
               const std::shared_ptr<pb::coordinator::RegionCmd>& b) { return a->id() < b->id(); });

  return commands;
}

std::shared_ptr<pb::common::KeyValue> RegionCommandManager::TransformToKv(uint64_t command_id) {
  auto region_cmd = GetCommand(command_id);
  if (region_cmd == nullptr) {
    return nullptr;
  }

  return TransformToKv(region_cmd);
}

std::shared_ptr<pb::common::KeyValue> RegionCommandManager::TransformToKv(
    std::shared_ptr<google::protobuf::Message> obj) {
  auto region_cmd = std::dynamic_pointer_cast<pb::coordinator::RegionCmd>(obj);
  std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(region_cmd->id()));
  kv->set_value(region_cmd->SerializeAsString());

  return kv;
}

void RegionCommandManager::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& kv : kvs) {
    uint64_t command_id = ParseRegionId(kv.key());
    auto region_cmd = std::make_shared<pb::coordinator::RegionCmd>();
    region_cmd->ParsePartialFromArray(kv.value().data(), kv.value().size());
    region_commands_.insert_or_assign(command_id, region_cmd);
  }
}

bool RegionController::Init() {
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  auto regions = store_meta_manager->GetStoreRegionMeta()->GetAllAliveRegion();
  for (auto& region : regions) {
    if (!RegisterExecutor(region->id())) {
      DINGO_LOG(ERROR) << "Register region control executor failed, region: " << region->id();
      return false;
    }
  }

  return true;
}

bool RegionController::Recover() {
  auto commands =
      Server::GetInstance()->GetRegionCommandManager()->GetCommands(pb::coordinator::RegionCmdStatus::STATUS_NONE);

  for (auto& command : commands) {
    auto ctx = std::make_shared<Context>();

    auto status = InnerDispatchRegionControlCommand(ctx, command);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Recover region control command failed, error: " << status.error_str();
    }
  }

  return true;
}

void RegionController::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);
  for (auto [_, executor] : executors_) {
    executor->Stop();
  }
}

bool RegionController::RegisterExecutor(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (executors_.find(region_id) == executors_.end()) {
    auto executor = std::make_shared<RegionControlExecutor>(region_id);
    executor->Init();
    executors_.insert({region_id, executor});
  }

  return true;
}

std::shared_ptr<RegionControlExecutor> RegionController::GetRegionControlExecutor(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = executors_.find(region_id);
  if (it == executors_.end()) {
    return nullptr;
  }

  return it->second;
}

butil::Status RegionController::InnerDispatchRegionControlCommand(std::shared_ptr<Context> ctx,
                                                                  std::shared_ptr<pb::coordinator::RegionCmd> command) {
  // Create region, need to add region control executor
  if (command->region_cmd_type() == pb::coordinator::RegionCmdType::CMD_CREATE) {
    RegisterExecutor(command->region_id());
  }

  auto executor = GetRegionControlExecutor(command->region_id());
  if (executor == nullptr) {
    DINGO_LOG(ERROR) << "Not find region control executor, regoin: " << command->region_id();
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not find regon control executor");
  }

  auto it = task_builders.find(command->region_cmd_type());
  if (it == task_builders.end()) {
    DINGO_LOG(ERROR) << "Not exist region control command";
    return butil::Status(pb::error::EINTERNAL, "Not exist region control command");
  }

  // Free at ExecuteRoutine()
  TaskRunnable* task = it->second(ctx, command);
  if (task == nullptr) {
    DINGO_LOG(ERROR) << "Not support region control command";
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
  auto region_command_manager = Server::GetInstance()->GetRegionCommandManager();
  if (region_command_manager->IsExist(command->id())) {
    return butil::Status(pb::error::EREGION_REPEAT_COMMAND, "Repeat region control command");
  }

  // Save region command
  region_command_manager->AddCommand(command);

  return InnerDispatchRegionControlCommand(ctx, command);
}

std::map<pb::coordinator::RegionCmdType, typename RegionController::TaskBuildFunc> RegionController::task_builders = {
    {pb::coordinator::CMD_CREATE,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnable* {
       return new CreateRegionTask(ctx, command);
     }},
    {pb::coordinator::CMD_DELETE,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnable* {
       return new DeleteRegionTask(ctx, command);
     }},
    {pb::coordinator::CMD_SPLIT,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnable* {
       return new SplitRegionTask(ctx, command);
     }},
    {pb::coordinator::CMD_MERGE,
     [](std::shared_ptr<Context>, std::shared_ptr<pb::coordinator::RegionCmd>) -> TaskRunnable* { return nullptr; }},
    {pb::coordinator::CMD_CHANGE_PEER,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnable* {
       return new ChangeRegionTask(ctx, command);
     }},
    {pb::coordinator::CMD_SNAPSHOT,
     [](std::shared_ptr<Context> ctx, std::shared_ptr<pb::coordinator::RegionCmd> command) -> TaskRunnable* {
       return new SnapshotRegionTask(ctx, command);
     }},
};

}  // namespace dingodb
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

#include "coordinator/auto_increment_control.h"

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>

#include "butil/containers/flat_map.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "engine/snapshot.h"
#include "proto/error.pb.h"
#include "server/server.h"

namespace dingodb {

AutoIncrementControl::AutoIncrementControl() {
  // init bthread mutex
  bthread_mutex_init(&auto_increment_map_mutex_, nullptr);

  CHECK_EQ(0, auto_increment_map_.init(256, 70));

  leader_term_.store(-1, butil::memory_order_release);
}

bool AutoIncrementControl::Init() {
  DINGO_LOG(INFO) << "init";
  return true;
}

bool AutoIncrementControl::Recover() {
  DINGO_LOG(INFO) << "recover";
  return true;
}

butil::Status AutoIncrementControl::GetAutoIncrement(int64_t table_id, int64_t& start_id) {
  DINGO_LOG(INFO) << table_id << " | " << start_id;
  butil::Status status;
  {
    BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
    int64_t* start_id_ptr = nullptr;
    start_id_ptr = auto_increment_map_.seek(table_id);

    if (start_id_ptr == nullptr) {
      DINGO_LOG(WARNING) << " cannot find auto increment, table id: " << table_id;
      status = butil::Status(pb::error::Errno::EAUTO_INCREMENT_NOT_FOUND, "auto increment not found");
    } else {
      start_id = *start_id_ptr;
      status = butil::Status::OK();
    }
  }
  return status;
}

butil::Status AutoIncrementControl::GetAutoIncrements(butil::FlatMap<int64_t, int64_t>& auto_increments) {
  BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
  auto_increments = auto_increment_map_;
  return butil::Status::OK();
}

butil::Status AutoIncrementControl::CreateAutoIncrement(int64_t table_id, int64_t start_id,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "create auto increment table id: " << table_id << " start id: " << start_id << "";
  {
    BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
    if (auto_increment_map_.seek(table_id) != nullptr) {
      if (auto_increment_map_[table_id] == start_id) {
        DINGO_LOG(WARNING) << "auto increment table id: " << table_id
                           << " is exist, start id is equal: " << auto_increment_map_[table_id]
                           << " maybe this is a retry request";
        return butil::Status::OK();
      } else {
        DINGO_LOG(WARNING) << "auto increment table id: " << table_id
                           << " is exist, start id: " << auto_increment_map_[table_id];
        return butil::Status(pb::error::Errno::EAUTO_INCREMENT_EXIST, "auto increment exist");
      }
    }
  }

  auto* auto_increment = meta_increment.add_auto_increment();
  auto_increment->set_id(table_id);
  auto* increment = auto_increment->mutable_increment();
  increment->set_start_id(start_id);
  auto_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::CREATE);
  return butil::Status::OK();
}

butil::Status AutoIncrementControl::SyncCreateAutoIncrement(int64_t table_id, int64_t start_id) {
  DINGO_LOG(INFO) << "sync create auto increment table id: " << table_id << " start id: " << start_id << "";
  pb::coordinator_internal::MetaIncrement meta_increment;
  {
    BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
    if (auto_increment_map_.seek(table_id) != nullptr) {
      if (auto_increment_map_[table_id] == start_id) {
        DINGO_LOG(WARNING) << "auto increment table id: " << table_id
                           << " is exist, start id is equal: " << auto_increment_map_[table_id]
                           << " maybe this is a retry request";
        return butil::Status::OK();
      } else {
        DINGO_LOG(WARNING) << "auto increment table id: " << table_id
                           << " is exist, start id: " << auto_increment_map_[table_id];
        return butil::Status(pb::error::Errno::EAUTO_INCREMENT_EXIST, "auto increment exist");
      }
    }
  }

  auto* auto_increment = meta_increment.add_auto_increment();
  auto_increment->set_id(table_id);
  auto* increment = auto_increment->mutable_increment();
  increment->set_start_id(start_id);
  auto_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::CREATE);

  std::shared_ptr<Context> const ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);

  auto status = engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "SubmitMetaIncrement failed, errno=" << status.error_code() << " errmsg=" << status.error_str();
    return status;
  }

  return butil::Status::OK();
}

butil::Status AutoIncrementControl::SyncDeleteAutoIncrement(int64_t table_id) {
  DINGO_LOG(INFO) << "sync delete auto increment table id: " << table_id;
  pb::coordinator_internal::MetaIncrement meta_increment;
  {
    BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
    if (auto_increment_map_.seek(table_id) == nullptr) {
      DINGO_LOG(WARNING) << "table id: " << table_id << " not found, aready deleted?";
      return butil::Status::OK();
    }
  }

  auto* auto_increment = meta_increment.add_auto_increment();
  auto_increment->set_id(table_id);
  auto_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::DELETE);

  std::shared_ptr<Context> const ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);

  auto status = engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "SubmitMetaIncrement failed, errno=" << status.error_code() << " errmsg=" << status.error_str();
    return status;
  }

  return butil::Status::OK();
}

butil::Status AutoIncrementControl::UpdateAutoIncrement(int64_t table_id, int64_t start_id, bool force,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << table_id << " | " << start_id << " | " << force;
  int64_t source_start_id = 0;
  butil::Status ret = GetAutoIncrement(table_id, source_start_id);
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << "cannot find table_id: " << table_id;
    return ret;
  }
  if (start_id <= source_start_id && !force) {
    DINGO_LOG(WARNING) << "start id illegal, table id: " << table_id << "";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "illegal parameters, start id illegal");
  }

  auto* auto_increment = meta_increment.add_auto_increment();
  auto_increment->set_id(table_id);
  auto* increment = auto_increment->mutable_increment();
  increment->set_start_id(start_id);
  increment->set_source_start_id(source_start_id);
  increment->set_update_type(pb::coordinator_internal::AutoIncrementUpdateType::UPDATE_ONLY);
  auto_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  return butil::Status::OK();
}

butil::Status AutoIncrementControl::GenerateAutoIncrement(int64_t table_id, uint32_t count,
                                                          uint32_t auto_increment_increment,
                                                          uint32_t auto_increment_offset,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << table_id << " | " << count << " | " << auto_increment_increment << " | " << auto_increment_offset;
  int64_t source_start_id = 0;
  butil::Status ret = GetAutoIncrement(table_id, source_start_id);
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << "cannot find table_id: " << table_id;
    return ret;
  }

  if (count == 0 || count > kAutoIncrementGenerateCountMax || auto_increment_increment == 0 ||
      auto_increment_increment > kAutoIncrementOffsetMax || auto_increment_offset == 0 ||
      auto_increment_offset > kAutoIncrementOffsetMax) {
    DINGO_LOG(WARNING) << "illegal parameters : " << table_id << " | " << count << " | " << auto_increment_increment
                       << " | " << auto_increment_offset;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "illegal parameters");
  }

  auto* auto_increment = meta_increment.add_auto_increment();
  auto_increment->set_id(table_id);
  auto* increment = auto_increment->mutable_increment();
  increment->set_update_type(pb::coordinator_internal::AutoIncrementUpdateType::READ_MODIFY_WRITE);
  increment->set_generate_count(count);
  increment->set_increment(auto_increment_increment);
  increment->set_offset(auto_increment_offset);

  auto_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  return butil::Status::OK();
}

butil::Status AutoIncrementControl::DeleteAutoIncrement(int64_t table_id,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "table id" << table_id;
  {
    BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
    if (auto_increment_map_.seek(table_id) == nullptr) {
      DINGO_LOG(WARNING) << "table id: " << table_id << " not found, aready deleted?";
      return butil::Status::OK();
    }
  }

  auto* auto_increment = meta_increment.add_auto_increment();
  auto_increment->set_id(table_id);
  auto_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::DELETE);
  return butil::Status::OK();
}

void AutoIncrementControl::GetLeaderLocation(pb::common::Location& leader_server_location) {
  if (raft_node_ == nullptr) {
    DINGO_LOG(ERROR) << "raft_node_ is nullptr";
    return;
  }

  // parse leader raft location from string
  auto leader_string = raft_node_->GetLeaderId().to_string();

  pb::common::Location leader_raft_location;
  int ret = Helper::PeerIdToLocation(raft_node_->GetLeaderId(), leader_raft_location);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "get raft leader failed, ret: " << ret << ".";
    return;
  }

  // GetServerLocation
  GetServerLocation(leader_raft_location, leader_server_location);
}

void AutoIncrementControl::GetServerLocation(pb::common::Location& raft_location,
                                             pb::common::Location& server_location) {
  // find in cache
  auto raft_location_string = raft_location.host() + ":" + std::to_string(raft_location.port());
  auto it = auto_increment_location_cache_.find(raft_location_string);
  if (it != auto_increment_location_cache_.end()) {
    server_location = it->second;
    DINGO_LOG(DEBUG) << "Cache Hit raft_location=" << raft_location.host() << ":" << raft_location.port();
    return;
  }

  Helper::GetServerLocation(raft_location, server_location);

  // transform ip to hostname
  Server::GetInstance().Ip2Hostname(*server_location.mutable_host());

  // add to cache if get server_location
  if (server_location.host().length() > 0 && server_location.port() > 0) {
    DINGO_LOG(INFO) << " Cache Miss, add new cache raft_location=" << raft_location.host() << ":"
                    << raft_location.port();
    auto_increment_location_cache_[raft_location_string] = server_location;
  } else {
    DINGO_LOG(INFO) << " Cache Miss, can't get server_location, raft_location=" << raft_location.host() << ":"
                    << raft_location.port();
  }
}

bool AutoIncrementControl::IsLeader() { return leader_term_.load(butil::memory_order_acquire) > 0; }

void AutoIncrementControl::SetLeaderTerm(int64_t term) { leader_term_.store(term, butil::memory_order_release); }

void AutoIncrementControl::OnLeaderStart(int64_t term) { DINGO_LOG(INFO) << "OnLeaderStart, term=" << term; }

void AutoIncrementControl::OnLeaderStop() { DINGO_LOG(INFO) << "OnLeaderStop"; }

// set raft_node to coordinator_control
void AutoIncrementControl::SetRaftNode(std::shared_ptr<RaftNode> raft_node) { raft_node_ = raft_node; }

// on_apply callback
void AutoIncrementControl::ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement& meta_increment, bool is_leader,
                                              int64_t /*term*/, int64_t /*index*/,
                                              google::protobuf::Message* response) {
  BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
  for (int i = 0; i < meta_increment.auto_increment_size(); i++) {
    const auto& auto_increment = meta_increment.auto_increment(i);
    int64_t table_id = auto_increment.id();
    if (auto_increment.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
      DINGO_LOG(INFO) << "create auto increment, table id: " << table_id
                      << ", start id: " << auto_increment.increment().start_id();
      auto_increment_map_[table_id] = auto_increment.increment().start_id();
    } else if (auto_increment.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
      int64_t* start_id_ptr = auto_increment_map_.seek(table_id);
      if (start_id_ptr == nullptr) {
        DINGO_LOG(WARNING) << "for update, cannot find table id: " << table_id;
        continue;
      }

      int64_t source_start_id = *start_id_ptr;
      if (auto_increment.increment().update_type() ==
          pb::coordinator_internal::AutoIncrementUpdateType::READ_MODIFY_WRITE) {
        int64_t end_id = GetGenerateEndId(source_start_id, auto_increment.increment().generate_count(),
                                          auto_increment.increment().increment(), auto_increment.increment().offset());
        // [source_start_id, end_id) has generated, so next start_id is end_id.
        if (is_leader && response != nullptr) {
          pb::meta::GenerateAutoIncrementResponse* generate_response =
              static_cast<pb::meta::GenerateAutoIncrementResponse*>(response);
          generate_response->set_start_id(source_start_id);
          generate_response->set_end_id(end_id);
          DINGO_LOG(INFO) << "leader grenerate auto increment response: " << generate_response->ShortDebugString();
        }
        auto_increment_map_[table_id] = end_id;
        DINGO_LOG(INFO) << "generate auto increment: [" << source_start_id << ", " << end_id
                        << ") request: " << auto_increment.ShortDebugString();
      } else {
        // check source start id
        if (source_start_id != auto_increment.increment().source_start_id()) {
          DINGO_LOG(WARNING) << "start id compare not equal: " << source_start_id << " | "
                             << auto_increment.increment().source_start_id();
        }
        auto_increment_map_[table_id] = auto_increment.increment().start_id();
        DINGO_LOG(INFO) << "update auto increment, table id: " << table_id
                        << ", old start id: " << auto_increment.increment().source_start_id()
                        << ", start id: " << auto_increment.increment().start_id();
      }
    } else if (auto_increment.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
      DINGO_LOG(INFO) << "delete auto increment " << auto_increment.ShortDebugString();
      auto_increment_map_.erase(table_id);
    }
  }
}

int64_t AutoIncrementControl::GetGenerateEndId(int64_t start_id, uint32_t count, uint32_t increment, uint32_t offset) {
  if (increment == 0 || increment > kAutoIncrementOffsetMax) {
    DINGO_LOG(WARNING) << "invalid auto_increment_increment: " << increment << ", set to default value 1.";
    increment = 1;
  }

  if (offset == 0 || offset > kAutoIncrementOffsetMax || offset > increment) {
    DINGO_LOG(WARNING) << "invalid auto_increment_offset: " << offset << ", set to default value 1.";
    offset = 1;
  }

  if (increment == 1 && offset == 1) {
    return start_id + count;
  }

  int64_t real_start_id = GetRealStartId(start_id, increment, offset);
  return real_start_id + count * increment;
}

int64_t AutoIncrementControl::GetRealStartId(int64_t start_id, uint32_t auto_increment_increment,
                                             uint32_t auto_increment_offset) {
  int64_t remainder = start_id % auto_increment_increment;
  if (remainder < auto_increment_offset) {
    return start_id - remainder + auto_increment_offset;
  } else if (remainder > auto_increment_offset) {
    return start_id - remainder + auto_increment_increment + auto_increment_offset;
  }

  return start_id;
}

int AutoIncrementControl::SaveAutoIncrement(std::string& auto_increment_data) {
  pb::coordinator_internal::AutoIncrementStorage storage;
  {
    BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
    for (const auto& it : auto_increment_map_) {
      auto* element = storage.add_elements();
      element->set_table_id(it.first);
      element->set_start_id(it.second);
    }
  }

  if (!storage.SerializeToString(&auto_increment_data)) {
    DINGO_LOG(ERROR) << "Failed to serialize auto increment storage";
    return -1;
  }

  return 0;
}

int AutoIncrementControl::GetAppliedTermAndIndex(int64_t& term, int64_t& index) {
  term = 0;
  index = 0;
  return 0;
}

std::shared_ptr<Snapshot> AutoIncrementControl::PrepareRaftSnapshot() {
  butil::FlatMap<int64_t, int64_t>* flatmap_for_snapshot = new butil::FlatMap<int64_t, int64_t>();
  flatmap_for_snapshot->init(1000);
  {
    BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
    *flatmap_for_snapshot = auto_increment_map_;
  }

  return std::make_shared<AutoIncrementSnapshot>(flatmap_for_snapshot);
}

bool AutoIncrementControl::LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                                                  pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
  DINGO_LOG(INFO) << "AutoIncrementControl start to LoadMetaToSnapshotFile";

  auto auto_increment_snapshot = std::dynamic_pointer_cast<AutoIncrementSnapshot>(snapshot);
  if (auto_increment_snapshot == nullptr) {
    DINGO_LOG(ERROR) << "Failed to dynamic cast snapshot to auto increment snapshot";
    return false;
  }

  const auto* flatmap_of_snapshot = auto_increment_snapshot->GetSnapshot();

  auto* auto_increment_elements = meta_snapshot_file.mutable_auto_increment_storage();
  for (auto it : (*flatmap_of_snapshot)) {
    auto* element = auto_increment_elements->add_elements();
    element->set_table_id(it.first);
    element->set_start_id(it.second);
  }

  DINGO_LOG(INFO) << "AutoIncrementControl LoadMetaToSnapshotFile success, elements_size="
                  << flatmap_of_snapshot->size();

  return true;
}

bool AutoIncrementControl::LoadMetaFromSnapshotFile(pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
  DINGO_LOG(INFO) << "AutoIncrementControl start to LoadMetaFromSnapshotFile";

  const auto& storage = meta_snapshot_file.auto_increment_storage();

  BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
  auto_increment_map_.clear();
  for (int i = 0; i < storage.elements_size(); i++) {
    const auto& element = storage.elements(i);
    auto_increment_map_[element.table_id()] = element.start_id();
  }

  DINGO_LOG(INFO) << "AutoIncrementControl LoadMetaFromSnapshotFile success, elements_size=" << storage.elements_size();

  return true;
}

int AutoIncrementControl::LoadAutoIncrement(const std::string& auto_increment_file) {
  std::ifstream auto_increment_data_fs(auto_increment_file);
  std::string auto_increment_data((std::istreambuf_iterator<char>(auto_increment_data_fs)),
                                  std::istreambuf_iterator<char>());

  pb::coordinator_internal::AutoIncrementStorage storage;
  if (!storage.ParseFromString(auto_increment_data)) {
    DINGO_LOG(ERROR) << "Failed to deserialize auto increment storage";
    return -1;
  }

  BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
  for (int i = 0; i < storage.elements_size(); i++) {
    const auto& element = storage.elements(i);
    auto_increment_map_[element.table_id()] = auto_increment_map_[element.start_id()];
  }
  return 0;
}

butil::Status AutoIncrementControl::CheckAutoIncrementInTableDefinition(
    const pb::meta::TableDefinition& table_definition, bool& has_auto_increment_column) {
  has_auto_increment_column = false;
  for (int i = 0; i < table_definition.columns_size(); i++) {
    const auto& column = table_definition.columns(i);
    if (column.is_auto_increment()) {
      if (has_auto_increment_column) {
        return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL,
                             "table definition illegal, multi auto_increment column");
      } else {
        if (table_definition.auto_increment() > 0) {
          has_auto_increment_column = true;
        } else {
          return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL,
                               "table definition illegal, auto_increment must be greater than 0");
        }
      }
    }
  }

  return butil::Status::OK();
}

butil::Status AutoIncrementControl::SyncSendCreateAutoIncrementInternal(int64_t table_id, int64_t auto_increment) {
  auto auto_increment_control = Server::GetInstance().GetAutoIncrementControl();
  if (auto_increment_control == nullptr) {
    DINGO_LOG(ERROR) << "CreateTable AutoIncrementControl is null";
    return butil::Status(pb::error::Errno::EAUTO_INCREMENT_WHILE_CREATING_TABLE, "AutoIncrementControl is null");
  }

  if (auto_increment_control->IsLeader()) {
    auto ret = auto_increment_control->SyncCreateAutoIncrement(table_id, auto_increment);
    if (ret.ok()) {
      DINGO_LOG(INFO) << "SyncCreateAutoIncrement success, table id: " << table_id
                      << ", auto_increment: " << auto_increment;
      return butil::Status::OK();
    } else if (ret.error_code() != pb::error::Errno::ERAFT_NOTLEADER) {
      DINGO_LOG(ERROR) << "SyncCreateAutoIncrement failed, table id: " << table_id
                       << ", auto_increment: " << auto_increment << ", ret: " << ret;
      return ret;
    } else {
      DINGO_LOG(WARNING) << "maybe there is a leader change when SyncCreateAutoIncrement, will use SendRequest, ret: "
                         << ret << ", table id: " << table_id << ", auto_increment: " << auto_increment;
    }
  }

  DINGO_LOG(INFO) << "SyncCreateAutoIncrement is not leader, will SendRequest, table id: " << table_id
                  << ", auto_increment: " << auto_increment;

  pb::meta::CreateAutoIncrementRequest request;
  pb::meta::CreateAutoIncrementResponse response;

  auto* table_id_ptr = request.mutable_table_id();
  table_id_ptr->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id_ptr->set_entity_id(table_id);
  request.set_start_id(auto_increment);
  return Server::GetInstance().GetCoordinatorInteractionIncr()->SendRequest("CreateAutoIncrement", request, response);
}

void AutoIncrementControl::AsyncSendUpdateAutoIncrementInternal(int64_t table_id, int64_t auto_increment) {
  if (auto_increment == 0 || auto_increment > kAutoIncrementOffsetMax) {
    DINGO_LOG(ERROR) << "table id: " << table_id << ", auto_increment: " << auto_increment;
    return;
  }

  auto update_function = [table_id, auto_increment]() {
    pb::meta::UpdateAutoIncrementRequest request;
    pb::meta::UpdateAutoIncrementResponse response;

    auto* table_id_ptr = request.mutable_table_id();
    table_id_ptr->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
    table_id_ptr->set_entity_id(table_id);
    request.set_start_id(auto_increment);
    request.set_force(true);

    butil::Status status =
        Server::GetInstance().GetCoordinatorInteractionIncr()->SendRequest("UpdateAutoIncrement", request, response);
    if (status.error_code() != pb::error::Errno::OK) {
      LOG(ERROR) << "error, code: " << status.error_code() << ", message: " << status.error_str();
    }
  };

  Bthread bth(&BTHREAD_ATTR_SMALL);
  bth.Run(update_function);
}

void AutoIncrementControl::AsyncSendDeleteAutoIncrementInternal(int64_t table_id) {
  auto auto_increment_control = Server::GetInstance().GetAutoIncrementControl();
  if (auto_increment_control == nullptr) {
    DINGO_LOG(ERROR) << "CreateTable AutoIncrementControl is null";
  }

  if (auto_increment_control->IsLeader()) {
    auto ret = auto_increment_control->SyncDeleteAutoIncrement(table_id);
    if (ret.ok()) {
      DINGO_LOG(INFO) << "SyncDeleteAutoIncrement success, table id: " << table_id;
      return;
    } else if (ret.error_code() != pb::error::Errno::ERAFT_NOTLEADER) {
      DINGO_LOG(ERROR) << "SyncDeleteAutoIncrement failed, table id: " << table_id << ", ret: " << ret;
    } else {
      DINGO_LOG(WARNING) << "maybe there is a leader change when SyncDeleteAutoIncrement, will use SendRequest, ret: "
                         << ret << ", table id: " << table_id;
    }
  }

  DINGO_LOG(INFO) << "AsyncDeleteAutoIncrement is not leader, will SendRequest, table id: " << table_id;

  auto delete_function = [table_id]() {
    pb::meta::DeleteAutoIncrementRequest request;
    pb::meta::DeleteAutoIncrementResponse response;

    auto* table_id_ptr = request.mutable_table_id();
    table_id_ptr->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
    table_id_ptr->set_entity_id(table_id);

    butil::Status status =
        Server::GetInstance().GetCoordinatorInteractionIncr()->SendRequest("DeleteAutoIncrement", request, response);
    if (status.error_code() != pb::error::Errno::OK) {
      LOG(ERROR) << "error, code: " << status.error_code() << ", message: " << status.error_str();
    }
  };

  Bthread bth(&BTHREAD_ATTR_SMALL);
  bth.Run(delete_function);
}

void AutoIncrementControl::GetMemoryInfo(pb::coordinator::CoordinatorMemoryInfo& memory_info) {
  // compute size
  {
    BAIDU_SCOPED_LOCK(auto_increment_map_mutex_);
    memory_info.set_auto_increment_map_count(auto_increment_map_.size());
    memory_info.set_auto_increment_map_size(auto_increment_map_.size() * sizeof(int64_t) * 2);
    memory_info.set_total_size(memory_info.total_size() + memory_info.auto_increment_map_size());
  }
}

}  // namespace dingodb

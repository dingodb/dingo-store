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

#include "coordinator/coordinator_control.h"

#include <sys/types.h>

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
#include "common/helper.h"
#include "common/logging.h"
#include "engine/snapshot.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"

namespace dingodb {

CoordinatorControl::CoordinatorControl(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer,
                                       std::shared_ptr<RawEngine> raw_engine_of_meta)
    : meta_reader_(meta_reader), meta_writer_(meta_writer), leader_term_(-1), raw_engine_of_meta_(raw_engine_of_meta) {
  bthread_mutex_init(&id_epoch_map_temp_mutex_, nullptr);
  bthread_mutex_init(&id_epoch_map_mutex_, nullptr);
  bthread_mutex_init(&coordinator_map_mutex_, nullptr);
  bthread_mutex_init(&store_map_mutex_, nullptr);
  bthread_mutex_init(&store_need_push_mutex_, nullptr);
  bthread_mutex_init(&executor_map_mutex_, nullptr);
  bthread_mutex_init(&executor_need_push_mutex_, nullptr);
  bthread_mutex_init(&schema_map_mutex_, nullptr);
  bthread_mutex_init(&region_map_mutex_, nullptr);
  bthread_mutex_init(&table_map_mutex_, nullptr);
  root_schema_writed_to_raft_ = false;

  coordinator_meta_ = new MetaMapStorage<pb::coordinator_internal::CoordinatorInternal>(&coordinator_map_);
  store_meta_ = new MetaMapStorage<pb::common::Store>(&store_map_);
  schema_meta_ = new MetaMapStorage<pb::coordinator_internal::SchemaInternal>(&schema_map_);
  region_meta_ = new MetaMapStorage<pb::common::Region>(&region_map_);
  table_meta_ = new MetaMapStorage<pb::coordinator_internal::TableInternal>(&table_map_);
  id_epoch_meta_ = new MetaMapStorage<pb::coordinator_internal::IdEpochInternal>(&id_epoch_map_);
  executor_meta_ = new MetaMapStorage<pb::common::Executor>(&executor_map_);
}

CoordinatorControl::~CoordinatorControl() {
  delete coordinator_meta_;
  delete store_meta_;
  delete schema_meta_;
  delete region_meta_;
  delete table_meta_;
  delete id_epoch_meta_;
  delete executor_meta_;
}

bool CoordinatorControl::IsLeader() { return leader_term_.load(butil::memory_order_acquire) > 0; }
void CoordinatorControl::SetLeaderTerm(int64_t term) { leader_term_.store(term, butil::memory_order_release); }

// OnLeaderStart will init id_epoch_map_temp_ from id_epoch_map_ which is in state machine
void CoordinatorControl::OnLeaderStart(int64_t term) {
  BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
  id_epoch_map_temp_ = id_epoch_map_;
  DINGO_LOG(INFO) << "OnLeaderStart init id_epoch_map_temp_ finished, term=" << term;
}

std::shared_ptr<Snapshot> CoordinatorControl::PrepareRaftSnapshot() {
  DINGO_LOG(INFO) << "PrepareRaftSnapshot";
  return this->raw_engine_of_meta_->GetSnapshot();
}

bool CoordinatorControl::LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                                                pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
  DINGO_LOG(INFO) << "Coordinator start to LoadMetaToSnapshotFile";

  std::vector<pb::common::KeyValue> kvs;

  // 0.id_epoch map
  if (!meta_reader_->Scan(snapshot, id_epoch_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_id_epoch_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }

  DINGO_LOG(INFO) << "Snapshot id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 1.coordinator map
  if (!meta_reader_->Scan(snapshot, coordinator_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_coordinator_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot coordinator_meta, count=" << kvs.size();
  kvs.clear();

  // 2.store map
  if (!meta_reader_->Scan(snapshot, store_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_store_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot store_meta, count=" << kvs.size();
  kvs.clear();

  // 3.executor map
  if (!meta_reader_->Scan(snapshot, executor_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_executor_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot executor_meta, count=" << kvs.size();
  kvs.clear();

  // 4.schema map
  if (!meta_reader_->Scan(snapshot, schema_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_schema_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot schema_meta, count=" << kvs.size();
  kvs.clear();

  // 5.region map
  if (!meta_reader_->Scan(snapshot, region_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_region_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot region_meta, count=" << kvs.size();
  kvs.clear();

  // 6.table map
  if (!meta_reader_->Scan(snapshot, table_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_table_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot table_meta, count=" << kvs.size();
  kvs.clear();

  return true;
}

bool CoordinatorControl::LoadMetaFromSnapshotFile(pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
  DINGO_LOG(INFO) << "Coordinator start to LoadMetaFromSnapshotFile";

  std::vector<pb::common::KeyValue> kvs;

  // 0.id_epoch map
  kvs.reserve(meta_snapshot_file.id_epoch_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.id_epoch_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.id_epoch_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    if (!id_epoch_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 1.coordinator map
  kvs.reserve(meta_snapshot_file.coordinator_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.coordinator_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.coordinator_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    if (!coordinator_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot coordinator_meta, count=" << kvs.size();
  kvs.clear();

  // 2.store map
  kvs.reserve(meta_snapshot_file.store_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.store_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.store_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    if (!store_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot store_meta, count=" << kvs.size();
  kvs.clear();

  // 3.executor map
  kvs.reserve(meta_snapshot_file.executor_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.executor_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.executor_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    if (!executor_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot executor_meta, count=" << kvs.size();
  kvs.clear();

  // 4.schema map
  kvs.reserve(meta_snapshot_file.schema_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.schema_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.schema_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (!schema_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot schema_meta, count=" << kvs.size();
  kvs.clear();

  // 5.region map
  kvs.reserve(meta_snapshot_file.region_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.region_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.region_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    if (!region_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot region_meta, count=" << kvs.size();
  kvs.clear();

  // 6.table map
  kvs.reserve(meta_snapshot_file.table_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.table_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.table_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (!table_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot table_meta, count=" << kvs.size();
  kvs.clear();

  // 7.init id_epoch_map_temp_
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_temp_mutex_);
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    id_epoch_map_temp_ = id_epoch_map_;
  }
  DINGO_LOG(INFO) << "LoadSnapshot id_epoch_map_temp, count=" << id_epoch_map_temp_.size();

  return true;
}

bool CoordinatorControl::Recover() {
  DINGO_LOG(INFO) << "Coordinator start to Recover";

  std::vector<pb::common::KeyValue> kvs;

  // 0.id_epoch map
  if (!meta_reader_->Scan(id_epoch_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);

    if (!id_epoch_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 1.coordinator map
  if (!meta_reader_->Scan(coordinator_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    if (!coordinator_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover coordinator_meta, count=" << kvs.size();
  kvs.clear();

  // 2.store map
  if (!meta_reader_->Scan(store_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    if (!store_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover store_meta, count=" << kvs.size();
  kvs.clear();

  // 3.executor map
  if (!meta_reader_->Scan(executor_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    if (!executor_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover executor_meta, count=" << kvs.size();
  kvs.clear();

  // 4.schema map
  if (!meta_reader_->Scan(schema_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (!schema_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover schema_meta, count=" << kvs.size();
  kvs.clear();

  // 5.region map
  if (!meta_reader_->Scan(region_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    if (!region_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover region_meta, count=" << kvs.size();
  kvs.clear();

  // 6.table map
  if (!meta_reader_->Scan(table_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (!table_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover table_meta, count=" << kvs.size();
  kvs.clear();

  // 7.init id_epoch_map_temp_
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_temp_mutex_);
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    id_epoch_map_temp_ = id_epoch_map_;
  }
  DINGO_LOG(INFO) << "Recover id_epoch_map_temp, count=" << id_epoch_map_temp_.size();

  return true;
}

void CoordinatorControl::GenerateRootSchemas(pb::coordinator_internal::SchemaInternal& root_schema_internal,
                                             pb::coordinator_internal::SchemaInternal& meta_schema_internal,
                                             pb::coordinator_internal::SchemaInternal& dingo_schema_internal) {
  // root schema
  // pb::meta::Schema root_schema;
  root_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  root_schema_internal.set_name("root");

  // meta schema
  // pb::meta::Schema meta_schema;
  meta_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::META_SCHEMA);
  meta_schema_internal.set_name("meta");

  root_schema_internal.add_schema_ids(meta_schema_internal.id());

  // dingo schema
  // pb::meta::Schema dingo_schema;
  dingo_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  dingo_schema_internal.set_name("dingo");

  root_schema_internal.add_schema_ids(dingo_schema_internal.id());

  DINGO_LOG(INFO) << "GenerateRootSchemas 0[" << root_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 1[" << meta_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 2[" << dingo_schema_internal.DebugString();
}

// Init
// init is called after recover
bool CoordinatorControl::Init() {
  // root=0 meta=1 dingo=2, other schema begins from 3
  // init schema_map_ at innocent cluster
  if (schema_map_.empty()) {
    pb::coordinator_internal::SchemaInternal root_schema;
    pb::coordinator_internal::SchemaInternal meta_schema;
    pb::coordinator_internal::SchemaInternal dingo_schema;

    GenerateRootSchemas(root_schema, meta_schema, dingo_schema);

    // add the initial schemas to schema_map_
    schema_map_.insert(std::make_pair(0, root_schema));   // raft_kv_put
    schema_map_.insert(std::make_pair(1, meta_schema));   // raft_kv_put
    schema_map_.insert(std::make_pair(2, dingo_schema));  // raft_kv_put

    // write to rocksdb
    auto const schema_kvs = schema_meta_->TransformToKvWithAll();
    meta_writer_->Put(schema_kvs);

    DINGO_LOG(INFO) << "init schema_map_ finished";
  }

  return true;
}

uint64_t CoordinatorControl::GetPresentId(const pb::coordinator_internal::IdEpochType& key) {
  uint64_t value = 0;
  BAIDU_SCOPED_LOCK(id_epoch_map_temp_mutex_);
  if (id_epoch_map_temp_.find(key) == id_epoch_map_temp_.end()) {
    value = 1000;
    DINGO_LOG(INFO) << "GetPresentId key=" << pb::coordinator_internal::IdEpochType_Name(key)
                    << " not found, generate new id=" << value;
  } else {
    value = id_epoch_map_temp_[key].value();
    DINGO_LOG(INFO) << "GetPresentId key=" << pb::coordinator_internal::IdEpochType_Name(key) << " value=" << value;
  }

  return value;
}

void CoordinatorControl::SetRaftNode(std::shared_ptr<RaftNode> raft_node) { raft_node_ = raft_node; }

void CoordinatorControl::GetServerLocation(pb::common::Location& raft_location, pb::common::Location& server_location) {
  // find in cache
  auto raft_location_string = raft_location.host() + ":" + std::to_string(raft_location.port());
  if (coordinator_location_cache_.find(raft_location_string) != coordinator_location_cache_.end()) {
    server_location.CopyFrom(coordinator_location_cache_[raft_location_string]);
    DINGO_LOG(INFO) << "GetServiceLocation Cache Hit raft_location="
                    << dingodb::Helper::MessageToJsonString(raft_location);
    return;
  }

  Helper::GetServerLocation(raft_location, server_location);

  // add to cache if get server_location
  if (server_location.host().length() > 0 && server_location.port() > 0) {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, add new cache raft_location="
                    << Helper::MessageToJsonString(raft_location);
    coordinator_location_cache_[raft_location_string] = server_location;
  } else {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, can't get server_location, raft_location="
                    << Helper::MessageToJsonString(raft_location);
  }
}

void CoordinatorControl::GetLeaderLocation(pb::common::Location& leader_server_location) {
  if (raft_node_ == nullptr) {
    DINGO_LOG(ERROR) << "GetLeaderLocation raft_node_ is nullptr";
    return;
  }

  // parse leader raft location from string
  auto leader_string = raft_node_->GetLeaderId().to_string();

  pb::common::Location leader_raft_location;
  int ret = Helper::PeerIdToLocation(raft_node_->GetLeaderId(), leader_raft_location);
  if (ret < 0) {
    return;
  }

  // GetServerLocation
  this->GetServerLocation(leader_raft_location, leader_server_location);
}

// GetNextId only update id_epoch_map_temp_ in leader, the persistent id_epoch_map_ will be updated in on_apply
// When on_leader_start, the id_epoch_map_temp_ will init from id_epoch_map_
// only id_epoch_map_ is in state machine, and will persistent to raft and local rocksdb
uint64_t CoordinatorControl::GetNextId(const pb::coordinator_internal::IdEpochType& key,
                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t value = 0;
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_temp_mutex_);
    if (id_epoch_map_temp_.find(key) == id_epoch_map_temp_.end()) {
      value = 1000;
      DINGO_LOG(INFO) << "GetNextId key=" << pb::coordinator_internal::IdEpochType_Name(key)
                      << " not found, generate new id=" << value;
    } else {
      value = id_epoch_map_temp_[key].value();
      DINGO_LOG(INFO) << "GetNextId key=" << pb::coordinator_internal::IdEpochType_Name(key) << " value=" << value;
    }
    value++;

    // update id in memory
    id_epoch_map_temp_[key].set_value(value);
  }

  // generate meta_increment
  auto* idepoch = meta_increment.add_idepochs();
  idepoch->set_id(key);
  idepoch->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

  auto* idepoch_internl = idepoch->mutable_idepoch();
  idepoch_internl->set_id(key);
  idepoch_internl->set_value(value);

  return value;
}

// drop schema
// in: parent_schema_id
// in: schema_id
// return: 0 or -1
int CoordinatorControl::DropSchema(uint64_t parent_schema_id, uint64_t schema_id,
                                   pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_id <= 3) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return -1;
  }

  if (parent_schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: parent_schema_id illegal " << schema_id;
    return -1;
  }

  pb::coordinator_internal::SchemaInternal schema_internal;
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (this->schema_map_.find(schema_id) == schema_map_.end()) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return -1;
    }

    auto& schema_to_delete = schema_map_[schema_id];
    if (schema_to_delete.table_ids_size() > 0 || schema_to_delete.schema_ids_size() > 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema is not null" << schema_id
                       << " table_ids_size=" << schema_to_delete.table_ids_size()
                       << " schema_ids_size=" << schema_to_delete.schema_ids_size();
      return -1;
    }
    // construct schema from schema_internal
    schema_internal = schema_map_.at(schema_id);
  }

  // bump up epoch
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_SCHEMA, meta_increment);

  // delete schema
  auto* schema_to_delete = meta_increment.add_schemas();
  schema_to_delete->set_id(schema_id);
  schema_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  schema_to_delete->set_schema_id(parent_schema_id);

  auto* schema_to_delete_schema = schema_to_delete->mutable_schema_internal();
  schema_to_delete_schema->CopyFrom(schema_internal);

  return 0;
}

// TODO: check name comflicts before create new schema
int CoordinatorControl::CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t& new_schema_id,
                                     pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (schema_map_.find(parent_schema_id) == schema_map_.end()) {
      DINGO_LOG(INFO) << " CreateSchema parent_schema_id is illegal " << parent_schema_id;
      return -1;
    }

    if (schema_name.empty()) {
      DINGO_LOG(INFO) << " CreateSchema schema_name is illegal " << schema_name;
      return -1;
    }
  }

  new_schema_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_SCHEMA, meta_increment);

  // add new schema to parent schema
  // auto* schema_id = schema_map_[parent_schema_id].add_schema_ids();
  // schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  // schema_id->set_parent_entity_id(parent_schema_id);
  // schema_id->set_entity_id(new_schema_id);
  // raft_kv_put

  // add new schema to  schema_map_
  pb::coordinator_internal::SchemaInternal new_schema_internal;
  new_schema_internal.set_id(new_schema_id);
  new_schema_internal.set_name(schema_name);

  // update meta_increment
  auto* schema_increment = meta_increment.add_schemas();
  schema_increment->set_id(new_schema_id);
  schema_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  schema_increment->set_schema_id(parent_schema_id);

  auto* schema_increment_schema = schema_increment->mutable_schema_internal();
  schema_increment_schema->CopyFrom(new_schema_internal);

  // bump up schema map epoch
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_SCHEMA, meta_increment);

  // on_apply
  //  schema_map_.insert(std::make_pair(new_schema_id, new_schema));  // raft_kv_put

  return 0;
}

uint64_t CoordinatorControl::UpdateExecutorMap(const pb::common::Executor& executor,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t executor_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_EXECUTOR);

  bool need_update_epoch = false;
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    if (executor_map_.find(executor.id()) != executor_map_.end()) {
      if (executor_map_[executor.id()].state() != executor.state()) {
        DINGO_LOG(INFO) << "executor STATUS CHANGE executor_id = " << executor.id()
                        << " old status = " << executor_map_[executor.id()].state()
                        << " new status = " << executor.state();

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

// UpdateStoreMap
uint64_t CoordinatorControl::UpdateStoreMap(const pb::common::Store& store,
                                            pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);

  bool need_update_epoch = false;
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    if (store_map_.find(store.id()) != store_map_.end()) {
      if (store_map_[store.id()].state() != store.state()) {
        DINGO_LOG(INFO) << "STORE STATUS CHANGE store_id = " << store.id()
                        << " old status = " << store_map_[store.id()].state() << " new status = " << store.state();

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
    if (store_map_.find(store_id) == store_map_.end()) {
      DINGO_LOG(INFO) << "DeleteStore store_id not exists, id=" << store_id;
      return -1;
    }

    store_to_delete = store_map_[store_id];
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
    if (executor_map_.find(executor_id) == executor_map_.end()) {
      DINGO_LOG(INFO) << "DeleteExecutor executor_id not exists, id=" << executor_id;
      return -1;
    }

    executor_to_delete = executor_map_[executor_id];
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

int CoordinatorControl::CreateTableId(uint64_t schema_id, uint64_t& new_table_id,
                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate schema_id is existed
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (schema_map_.find(schema_id) == schema_map_.end()) {
      DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id;
      return -1;
    }
  }

  // create table id
  new_table_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, meta_increment);
  DINGO_LOG(INFO) << "CreateTableId new_table_id=" << new_table_id;

  return 0;
}

int CoordinatorControl::CreateTable(uint64_t schema_id, const pb::meta::TableDefinition& table_definition,
                                    uint64_t& new_table_id, pb::coordinator_internal::MetaIncrement& meta_increment) {
  // initial schema create
  // if (!root_schema_writed_to_raft_) {
  //   pb::coordinator_internal::SchemaInternal root_schema;
  //   pb::coordinator_internal::SchemaInternal meta_schema;
  //   pb::coordinator_internal::SchemaInternal dingo_schema;

  //   GenerateRootSchemas(root_schema, meta_schema, dingo_schema);
  //   GenerateRootSchemasMetaIncrement(root_schema, meta_schema, dingo_schema, meta_increment);

  //   root_schema_writed_to_raft_ = true;
  // }

  // validate schema_id is existed
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (schema_map_.find(schema_id) == schema_map_.end()) {
      DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id;
      return -1;
    }
  }

  // validate part information
  if (!table_definition.has_table_partition()) {
    DINGO_LOG(ERROR) << "no table_partition provided ";
    return -1;
  }
  auto const& table_partition = table_definition.table_partition();
  if (table_partition.has_hash_partition()) {
    DINGO_LOG(ERROR) << "hash_partiton is not supported";
    return -1;
  } else if (!table_partition.has_range_partition()) {
    DINGO_LOG(ERROR) << "no range_partition provided ";
    return -1;
  }

  auto const& range_partition = table_partition.range_partition();
  if (range_partition.ranges_size() == 0) {
    DINGO_LOG(ERROR) << "no range provided ";
    return -1;
  }

  // create table
  // extract part info, create region for each part

  // if new_table_id is not given, create a new table_id
  if (new_table_id <= 0) {
    new_table_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, meta_increment);
    DINGO_LOG(INFO) << "CreateTable new_table_id=" << new_table_id;
  }

  std::vector<uint64_t> new_region_ids;
  for (int i = 0; i < range_partition.ranges_size(); i++) {
    // int ret = CreateRegion(const std::string &region_name, const std::string
    // &resource_tag, int32_t replica_num, pb::common::Range region_range,
    // uint64_t schema_id, uint64_t table_id, uint64_t &new_region_id)
    std::string const region_name = std::to_string(schema_id) + std::string("_") + table_definition.name() +
                                    std::string("_part_") + std::to_string(i);
    uint64_t new_region_id;
    int const ret = CreateRegion(region_name, "", 3, range_partition.ranges(i), schema_id, new_table_id, new_region_id,
                                 meta_increment);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "CreateRegion failed in CreateTable table_name=" << table_definition.name();
      break;
    }

    new_region_ids.push_back(new_region_id);
  }

  if (new_region_ids.size() < range_partition.ranges_size()) {
    DINGO_LOG(ERROR) << "Not enough regions is created, drop residual regions need=" << range_partition.ranges_size()
                     << " created=" << new_region_ids.size();
    for (auto region_id_to_delete : new_region_ids) {
      int ret = DropRegion(region_id_to_delete, meta_increment);
      if (ret < 0) {
        DINGO_LOG(ERROR) << "DropRegion failed in CreateTable table_name=" << table_definition.name()
                         << " region_id =" << region_id_to_delete;
      }
    }
    return -1;
  }

  // bumper up EPOCH_REGION
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);

  // create table_internal, set id & table_definition
  pb::coordinator_internal::TableInternal table_internal;
  table_internal.set_id(new_table_id);
  auto* definition = table_internal.mutable_definition();
  definition->CopyFrom(table_definition);

  // set part for table_internal
  for (int i = 0; i < new_region_ids.size(); i++) {
    // create part and set region_id & range
    auto* part_internal = table_internal.add_partitions();
    part_internal->set_region_id(new_region_ids[i]);
    auto* part_range = part_internal->mutable_range();
    part_range->CopyFrom(range_partition.ranges(i));
  }

  // add table_internal to table_map_
  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_TABLE, meta_increment);
  auto* table_increment = meta_increment.add_tables();
  table_increment->set_id(new_table_id);
  table_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  table_increment->set_schema_id(schema_id);

  auto* table_increment_table = table_increment->mutable_table();
  table_increment_table->CopyFrom(table_internal);

  // on_apply
  //  table_map_epoch++;                                               // raft_kv_put
  //  table_map_.insert(std::make_pair(new_table_id, table_internal));  // raft_kv_put

  // add table_id to schema
  // auto* table_id = schema_map_[schema_id].add_table_ids();
  // table_id->set_entity_id(new_table_id);
  // table_id->set_parent_entity_id(schema_id);
  // table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  // raft_kv_put

  return 0;
}

int CoordinatorControl::DropRegion(uint64_t region_id, pb::coordinator_internal::MetaIncrement& meta_increment) {
  // set region state to DELETE
  bool need_update_epoch = false;
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    if (region_map_.find(region_id) != region_map_.end()) {
      auto region_to_delete = region_map_[region_id];
      region_to_delete.set_state(::dingodb::pb::common::RegionState::REGION_DELETE);

      // update meta_increment
      need_update_epoch = true;
      auto* region_increment = meta_increment.add_regions();
      region_increment->set_id(region_id);
      region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

      auto* region_increment_region = region_increment->mutable_region();
      region_increment_region->CopyFrom(region_to_delete);

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

// drop table
int CoordinatorControl::DropTable(uint64_t schema_id, uint64_t table_id,
                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return -1;
  }

  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (this->schema_map_.find(schema_id) == schema_map_.end()) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return -1;
    }
  }

  pb::coordinator_internal::TableInternal table_internal;
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (this->table_map_.find(table_id) == table_map_.end()) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return -1;
    }

    // construct Table from table_internal
    table_internal = table_map_.at(table_id);
  }

  bool need_delete_region = false;
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    for (int i = 0; i < table_internal.partitions_size(); i++) {
      // part id
      uint64_t region_id = table_internal.partitions(i).region_id();

      // get region
      if (region_map_.find(region_id) == region_map_.end()) {
        DINGO_LOG(ERROR) << "ERROR cannot find region in regionmap_ while DropTable, table_id =" << table_id
                         << " region_id=" << region_id;
        return -1;
      }

      // update region to DELETE, not delete region really, not
      auto* region_to_delete = meta_increment.add_regions();
      region_to_delete->set_id(region_id);
      region_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);
      auto* region_to_delete_region = region_to_delete->mutable_region();
      region_to_delete_region->CopyFrom(region_map_[region_id]);
      region_to_delete->mutable_region()->set_state(::dingodb::pb::common::RegionState::REGION_DELETE);
      DINGO_LOG(INFO) << "Delete Region in Drop Table, table_id=" << table_id << " region_id=" << region_id;

      need_delete_region = true;
    }
  }

  // bump up region map epoch
  if (need_delete_region) {
    GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);
  }

  // delete table
  auto* table_to_delete = meta_increment.add_tables();
  table_to_delete->set_id(table_id);
  table_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  table_to_delete->set_schema_id(schema_id);

  auto* table_to_delete_table = table_to_delete->mutable_table();
  table_to_delete_table->CopyFrom(table_internal);

  // bump up table map epoch
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_TABLE, meta_increment);

  return 0;
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
  if (region_map_.find(create_region_id) != region_map_.end()) {
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

// UpdateRegionMap
uint64_t CoordinatorControl::UpdateRegionMap(std::vector<pb::common::Region>& regions,
                                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t region_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION);

  bool need_to_get_next_epoch = false;
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    for (const auto& region : regions) {
      if (region_map_.find(region.id()) != region_map_.end()) {
        DINGO_LOG(INFO) << " update region to region_map in heartbeat, region_id=" << region.id();

        // if state not change, just update leader_store_id
        if (region_map_[region.id()].state() == region.state()) {
          // update the region's leader_store_id, no need to apply raft
          if (region_map_[region.id()].leader_store_id() != region.leader_store_id()) {
            region_map_[region.id()].set_leader_store_id(region.leader_store_id());
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
          const auto& region_state_in_map = dingodb::pb::common::RegionState_Name(region_map_[region.id()].state());
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

void CoordinatorControl::GetPushStoreMap(std::map<uint64_t, pb::common::Store>& store_to_push) {
  BAIDU_SCOPED_LOCK(store_need_push_mutex_);
  store_to_push.swap(store_need_push_);
}

void CoordinatorControl::GetPushExecutorMap(std::map<uint64_t, pb::common::Executor>& executor_to_push) {
  BAIDU_SCOPED_LOCK(executor_need_push_mutex_);
  executor_to_push.swap(executor_need_push_);
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

// GetSchemas
// in: schema_id
// out: schemas
void CoordinatorControl::GetSchemas(uint64_t schema_id, std::vector<pb::meta::Schema>& schemas) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  if (!schemas.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: vector schemas is not empty , size=" << schemas.size();
    return;
  }

  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (this->schema_map_.find(schema_id) == schema_map_.end()) {
      return;
    }

    auto& schema_internal = schema_map_[schema_id];
    DINGO_LOG(INFO) << " sub schema count=" << schema_internal.schema_ids_size();

    for (int i = 0; i < schema_internal.schema_ids_size(); i++) {
      uint64_t sub_schema_id = schema_internal.schema_ids(i);

      DINGO_LOG(INFO) << "sub_schema_id=" << sub_schema_id;

      DINGO_LOG(INFO) << schema_internal.DebugString();

      if (schema_map_.find(sub_schema_id) == schema_map_.end()) {
        DINGO_LOG(ERROR) << "ERRROR: sub_schema_id " << sub_schema_id << " not exists";
        DINGO_LOG(INFO) << "ERRROR: sub_schema_id " << sub_schema_id << " not exists";
        continue;
      }
      DINGO_LOG(INFO) << " schema_map_ =" << schema_map_[sub_schema_id].DebugString();

      DINGO_LOG(INFO) << " GetSchemas push_back sub schema id=" << sub_schema_id;

      // construct sub_schema_for_response
      const auto& lean_schema = schema_map_[sub_schema_id];
      pb::meta::Schema sub_schema_for_response;

      auto* sub_schema_common_id = sub_schema_for_response.mutable_id();
      sub_schema_common_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
      sub_schema_common_id->set_entity_id(lean_schema.id());  // this is equal to sub_schema_id
      sub_schema_common_id->set_parent_entity_id(
          schema_id);  // this is sub_schema_id's parent which is the input schema_id

      for (auto x : lean_schema.schema_ids()) {
        auto* temp_id = sub_schema_for_response.add_schema_ids();
        temp_id->set_entity_id(x);
        temp_id->set_parent_entity_id(sub_schema_id);
        temp_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
      }

      for (auto x : lean_schema.table_ids()) {
        auto* temp_id = sub_schema_for_response.add_table_ids();
        temp_id->set_entity_id(x);
        temp_id->set_parent_entity_id(sub_schema_id);
        temp_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
      }

      // push sub_schema_for_response into response
      schemas.push_back(sub_schema_for_response);
    }
  }

  DINGO_LOG(INFO) << "GetSchemas id=" << schema_id << " sub schema count=" << schema_map_.size();
}

// get tables
void CoordinatorControl::GetTables(uint64_t schema_id,
                                   std::vector<pb::meta::TableDefinitionWithId>& table_definition_with_ids) {
  DINGO_LOG(INFO) << "GetTables in control schema_id=" << schema_id;

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  if (!table_definition_with_ids.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: vector table_definition_with_ids is not empty , size="
                     << table_definition_with_ids.size();
    return;
  }

  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (this->schema_map_.find(schema_id) == schema_map_.end()) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return;
    }

    auto& schema_internal = schema_map_[schema_id];
    for (int i = 0; i < schema_internal.table_ids_size(); i++) {
      uint64_t table_id = schema_internal.table_ids(i);
      if (table_map_.find(table_id) == table_map_.end()) {
        DINGO_LOG(ERROR) << "ERRROR: table_id " << table_id << " not exists";
        continue;
      }

      DINGO_LOG(INFO) << "GetTables found table_id=" << table_id;

      // construct return value
      pb::meta::TableDefinitionWithId table_def_with_id;

      // table_def_with_id.mutable_table_id()->CopyFrom(schema_internal.schema().table_ids(i));
      auto* table_id_for_response = table_def_with_id.mutable_table_id();
      table_id_for_response->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
      table_id_for_response->set_entity_id(table_id);
      table_id_for_response->set_parent_entity_id(schema_id);

      table_def_with_id.mutable_table_definition()->CopyFrom(table_map_[table_id].definition());
      table_definition_with_ids.push_back(table_def_with_id);
    }
  }

  DINGO_LOG(INFO) << "GetTables schema_id=" << schema_id << " tables count=" << table_definition_with_ids.size();
}

// get table
void CoordinatorControl::GetTable(uint64_t schema_id, uint64_t table_id, pb::meta::Table& table) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  if (table.id().entity_id() != 0) {
    DINGO_LOG(ERROR) << "ERRROR: table is not empty , table_id=" << table.id().entity_id();
    return;
  }

  pb::coordinator_internal::TableInternal table_internal;
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (this->schema_map_.find(schema_id) == schema_map_.end()) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return;
    }

    if (this->table_map_.find(table_id) == table_map_.end()) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return;
    }

    // construct Table from table_internal
    table_internal = table_map_.at(table_id);
  }

  auto* common_id_table = table.mutable_id();
  common_id_table->set_entity_id(table_id);
  common_id_table->set_parent_entity_id(schema_id);
  common_id_table->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  for (int i = 0; i < table_internal.partitions_size(); i++) {
    auto* part = table.add_parts();

    // part id
    uint64_t region_id = table_internal.partitions(i).region_id();

    auto* common_id_region = part->mutable_id();
    common_id_region->set_entity_id(region_id);
    common_id_region->set_parent_entity_id(table_id);
    common_id_region->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);

    // part range
    auto* part_range = part->mutable_range();
    part_range->CopyFrom(table_internal.partitions(i).range());

    // get region
    if (region_map_.find(region_id) == region_map_.end()) {
      DINGO_LOG(ERROR) << "ERROR cannot find region in regionmap_ while GetTable, table_id =" << table_id
                       << " region_id=" << region_id;
      continue;
    }
    pb::common::Region& part_region = region_map_[region_id];

    // part leader location
    auto* leader_location = part->mutable_leader();

    // part voter & learner locations
    for (int j = 0; j < part_region.peers_size(); j++) {
      const auto& part_peer = part_region.peers(j);
      if (part_peer.store_id() == part_region.leader_store_id()) {
        leader_location->CopyFrom(part_peer.server_location());
      }

      if (part_peer.role() == ::dingodb::pb::common::PeerRole::VOTER) {
        auto* voter_location = part->add_voters();
        voter_location->CopyFrom(part_peer.server_location());
      } else if (part_peer.role() == ::dingodb::pb::common::PeerRole::LEARNER) {
        auto* learner_location = part->add_learners();
        learner_location->CopyFrom(part_peer.server_location());
      }
    }

    // part regionmap_epoch
    uint64_t region_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION);
    part->set_regionmap_epoch(region_map_epoch);

    // part storemap_epoch
    uint64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);
    part->set_storemap_epoch(store_map_epoch);
  }
}

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

// ApplyMetaIncrement is on_apply callback
void CoordinatorControl::ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement& meta_increment,
                                            [[maybe_unused]] bool id_leader, uint64_t term, uint64_t index) {
  // prepare data to write to kv engine
  std::vector<pb::common::KeyValue> meta_write_to_kv;
  std::vector<pb::common::KeyValue> meta_delete_to_kv;

  {
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    // if index < local apply index, just return
    if (id_epoch_map_.find(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX) != id_epoch_map_.end()) {
      uint64_t applied_index = id_epoch_map_[pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX].value();
      if (index <= applied_index) {
        DINGO_LOG(INFO) << "ApplyMetaIncrement index < applied_index, just return, [index=" << index
                        << "][applied_index=" << applied_index << "]";
        return;
      }
    }

    // 0.id & epoch
    // raft_apply_term & raft_apply_index stores in id_epoch_map too
    pb::coordinator_internal::IdEpochInternal raft_apply_term;
    raft_apply_term.set_id(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM);
    raft_apply_term.set_value(term);

    pb::coordinator_internal::IdEpochInternal raft_apply_index;
    raft_apply_index.set_id(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX);
    raft_apply_index.set_value(index);

    meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(raft_apply_term));
    meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(raft_apply_index));

    for (int i = 0; i < meta_increment.idepochs_size(); i++) {
      const auto& idepoch = meta_increment.idepochs(i);
      if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto& create_idepoch = id_epoch_map_[idepoch.id()];
        create_idepoch.CopyFrom(idepoch.idepoch());

        meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));

      } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_idepoch = id_epoch_map_[idepoch.id()];
        update_idepoch.CopyFrom(idepoch.idepoch());

        meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));

      } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        id_epoch_map_.erase(idepoch.id());

        meta_delete_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));
      }
    }
  }

  // 1.coordinator map
  {
    BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    for (int i = 0; i < meta_increment.coordinators_size(); i++) {
      const auto& coordinator = meta_increment.coordinators(i);
      if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        coordinator_map_[coordinator.id()] = coordinator.coordinator();

        // meta_write_kv
        meta_write_to_kv.push_back(coordinator_meta_->TransformToKvValue(coordinator.coordinator()));

      } else if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_coordinator = coordinator_map_[coordinator.id()];
        update_coordinator.CopyFrom(coordinator.coordinator());

        // meta_write_kv
        meta_write_to_kv.push_back(coordinator_meta_->TransformToKvValue(coordinator.coordinator()));

      } else if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        coordinator_map_.erase(coordinator.id());

        // meta_delete_kv
        meta_delete_to_kv.push_back(coordinator_meta_->TransformToKvValue(coordinator.coordinator()));
      }
    }
  }

  // 2.store map
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    for (int i = 0; i < meta_increment.stores_size(); i++) {
      const auto& store = meta_increment.stores(i);
      if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        store_map_[store.id()] = store.store();

        // meta_write_kv
        meta_write_to_kv.push_back(store_meta_->TransformToKvValue(store.store()));

      } else if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_store = store_map_[store.id()];
        update_store.CopyFrom(store.store());

        // meta_write_kv
        meta_write_to_kv.push_back(store_meta_->TransformToKvValue(store.store()));

      } else if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        store_map_.erase(store.id());

        // meta_delete_kv
        meta_delete_to_kv.push_back(store_meta_->TransformToKvValue(store.store()));
      }
    }
  }

  // 3.executor map
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    for (int i = 0; i < meta_increment.executors_size(); i++) {
      const auto& executor = meta_increment.executors(i);
      if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        executor_map_[executor.id()] = executor.executor();

        // meta_write_kv
        meta_write_to_kv.push_back(executor_meta_->TransformToKvValue(executor.executor()));

      } else if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_executor = executor_map_[executor.id()];
        update_executor.CopyFrom(executor.executor());

        // meta_write_kv
        meta_write_to_kv.push_back(executor_meta_->TransformToKvValue(executor.executor()));

      } else if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        executor_map_.erase(executor.id());

        // meta_delete_kv
        meta_delete_to_kv.push_back(executor_meta_->TransformToKvValue(executor.executor()));
      }
    }
  }

  // 4.schema map
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    for (int i = 0; i < meta_increment.schemas_size(); i++) {
      const auto& schema = meta_increment.schemas(i);
      if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // update parent schema for user schemas
        if (schema.id() > pb::meta::ReservedSchemaIds::DINGO_SCHEMA) {
          if (schema_map_.find(schema.schema_id()) != schema_map_.end()) {
            auto& parent_schema = schema_map_[schema.schema_id()];

            // add new created schema's id to its parent schema's schema_ids
            parent_schema.add_schema_ids(schema.id());

            DINGO_LOG(INFO) << "3.schema map CREATE new_sub_schema id=" << schema.id()
                            << " parent_id=" << schema.schema_id();

            // meta_write_kv
            meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(parent_schema));
          }
        }
        schema_map_[schema.id()] = schema.schema_internal();

        // meta_write_kv
        meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));

      } else if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_schema = schema_map_[schema.id()];
        update_schema.CopyFrom(schema.schema_internal());

        // meta_write_kv
        meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));

      } else if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        schema_map_.erase(schema.id());

        // delete from parent schema
        if (schema_map_.find(schema.schema_id()) != schema_map_.end()) {
          auto& parent_schema = schema_map_[schema.schema_id()];

          // according to the protobuf document, we must use CopyFrom for protobuf message data structure here
          pb::coordinator_internal::SchemaInternal new_schema;
          new_schema.CopyFrom(parent_schema);

          new_schema.clear_table_ids();

          // add left schema_id to new_schema
          for (auto x : parent_schema.table_ids()) {
            if (x != schema.id()) {
              new_schema.add_schema_ids(x);
            }
          }
          parent_schema.CopyFrom(new_schema);

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(parent_schema));
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));
      }
    }
  }

  // 5.region map
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    for (int i = 0; i < meta_increment.regions_size(); i++) {
      const auto& region = meta_increment.regions(i);
      if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // add region to region_map
        region_map_[region.id()] = region.region();

        // add_store_for_push
        // only create region will push to store now
        for (int j = 0; j < region.region().peers_size(); j++) {
          uint64_t store_id = region.region().peers(j).store_id();
          DINGO_LOG(INFO) << " add_store_for_push, peers_size=" << region.region().peers_size()
                          << " store_id =" << store_id;

          if (store_need_push_.find(store_id) == store_need_push_.end()) {
            if (store_map_.find(store_id) != store_map_.end()) {
              store_need_push_[store_id] = store_map_[store_id];
              DINGO_LOG(INFO) << " add_store_for_push, store_id=" << store_id
                              << " in create region=" << region.region().id()
                              << " location=" << store_map_[store_id].server_location().host() << ":"
                              << store_map_[store_id].server_location().port();
            } else {
              DINGO_LOG(ERROR) << " add_store_for_push, illegal store_id=" << store_id
                               << " in create region=" << region.region().id();
            }
          }
        }

        // meta_write_kv
        meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update region to region_map
        auto& update_region = region_map_[region.id()];
        update_region.CopyFrom(region.region());

        // meta_write_kv
        meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // remove region from region_map
        region_map_.erase(region.id());

        // meta_delete_kv
        meta_delete_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));
      }
    }
  }

  // 6.table map
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    for (int i = 0; i < meta_increment.tables_size(); i++) {
      const auto& table = meta_increment.tables(i);
      if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // need to update schema, so acquire lock
        BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // add table to table_map
        table_map_[table.id()] = table.table();

        // meta_write_kv
        meta_write_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));

        // add table to parent schema
        if (schema_map_.find(table.schema_id()) != schema_map_.end()) {
          auto& schema = schema_map_[table.schema_id()];

          // add new created table's id to its parent schema's table_ids
          schema.add_table_ids(table.id());

          DINGO_LOG(INFO) << "5.table map CREATE new_sub_table id=" << table.id() << " parent_id=" << table.schema_id();

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema));

        } else {
          DINGO_LOG(ERROR) << " CREATE TABLE apply illegal schema_id=" << table.schema_id()
                           << " table_id=" << table.id() << " table_name=" << table.table().definition().name();
        }

      } else if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update table to table_map
        auto& update_table = table_map_[table.id()];
        update_table.CopyFrom(table.table());

        // meta_write_kv
        meta_write_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));

      } else if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // need to update schema, so acquire lock
        BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // delete table from table_map
        table_map_.erase(table.id());

        // delete from parent schema
        if (schema_map_.find(table.schema_id()) != schema_map_.end()) {
          auto& schema = schema_map_[table.schema_id()];

          // according to the doc, we must use CopyFrom for protobuf message data structure here
          pb::coordinator_internal::SchemaInternal new_schema;
          new_schema.CopyFrom(schema);

          new_schema.clear_table_ids();

          // add left table_id to new_schema
          for (auto x : schema.table_ids()) {
            if (x != table.id()) {
              new_schema.add_table_ids(x);
            }
          }
          schema.CopyFrom(new_schema);

          DINGO_LOG(INFO) << "5.table map DELETE new_sub_table id=" << table.id() << " parent_id=" << table.schema_id();

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema));

        } else {
          DINGO_LOG(ERROR) << " DROP TABLE apply illegal schema_id=" << table.schema_id() << " table_id=" << table.id()
                           << " table_name=" << table.table().definition().name();
        }
        // meta_delete_kv
        meta_delete_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));
      }
    }
  }

  // write update to local engine, begin
  if ((!meta_write_to_kv.empty()) || (!meta_delete_to_kv.empty())) {
    if (!meta_writer_->PutAndDelete(meta_write_to_kv, meta_delete_to_kv)) {
      DINGO_LOG(ERROR) << "ApplyMetaIncrement PutAndDelete failed, exit program";
      exit(-1);
    }
  }
  // write update to local engine, end
}

int CoordinatorControl::ValidateStore(uint64_t store_id, const std::string& keyring) {
  if (keyring == std::string("TO_BE_CONTINUED")) {
    DINGO_LOG(INFO) << "ValidateStore store_id=" << store_id << " debug pass with TO_BE_CONTINUED";
    return 0;
  }

  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    if (store_map_.find(store_id) != store_map_.end()) {
      auto store_in_map = store_map_[store_id];
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

int CoordinatorControl::ValidateExecutor(uint64_t executor_id, const std::string& keyring) {
  if (keyring == std::string("TO_BE_CONTINUED")) {
    DINGO_LOG(INFO) << "ValidateExecutor executor_id=" << executor_id << " debug pass with TO_BE_CONTINUED";
    return 0;
  }

  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    if (executor_map_.find(executor_id) != executor_map_.end()) {
      auto executor_in_map = executor_map_[executor_id];
      if (executor_in_map.keyring() == keyring) {
        DINGO_LOG(INFO) << "ValidateExecutor executor_id=" << executor_id << " succcess";
        return 0;
      }

      DINGO_LOG(INFO) << "ValidateExecutor executor_id=" << executor_id
                      << "keyring wrong fail input_keyring=" << keyring
                      << " correct_keyring=" << executor_in_map.keyring();
      return -1;
    }
  }

  DINGO_LOG(INFO) << "ValidateExecutor executor_id=" << executor_id << " not exist fail";

  return -1;
}

}  // namespace dingodb

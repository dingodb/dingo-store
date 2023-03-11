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

#include "butil/scoped_lock.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

CoordinatorControl::CoordinatorControl(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
    : meta_reader_(meta_reader), meta_writer_(meta_writer) {
  bthread_mutex_init(&control_mutex_, nullptr);
  root_schema_writed_to_raft_ = false;

  coordinator_meta_ = new MetaMapStorage<pb::coordinator_internal::CoordinatorInternal>(&coordinator_map_);
  store_meta_ = new MetaMapStorage<pb::common::Store>(&store_map_);
  schema_meta_ = new MetaMapStorage<pb::coordinator_internal::SchemaInternal>(&schema_map_);
  region_meta_ = new MetaMapStorage<pb::common::Region>(&region_map_);
  table_meta_ = new MetaMapStorage<pb::coordinator_internal::TableInternal>(&table_map_);
  id_epoch_meta_ = new MetaMapStorage<pb::coordinator_internal::IdEpochInternal>(&id_epoch_map_);
}

CoordinatorControl::~CoordinatorControl() {
  delete coordinator_meta_;
  delete store_meta_;
  delete schema_meta_;
  delete region_meta_;
  delete table_meta_;
  delete id_epoch_meta_;
}

bool CoordinatorControl::IsLeader() { return is_leader_.load(); }
void CoordinatorControl::SetLeader() { is_leader_.store(true); }
void CoordinatorControl::SetNotLeader() { is_leader_.store(false); }

bool CoordinatorControl::Recover() {
  BAIDU_SCOPED_LOCK(control_mutex_);

  std::vector<pb::common::KeyValue> kvs;

  LOG(INFO) << "Coordinator start to Recover";

  // coordinator map
  if (!meta_reader_->Scan(coordinator_meta_->Prefix(), kvs)) {
    return false;
  }

  if (!coordinator_meta_->Recover(kvs)) {
    return false;
  }
  LOG(INFO) << "Recover coordinator_meta, count=" << kvs.size();
  kvs.clear();

  // store map
  if (!meta_reader_->Scan(store_meta_->Prefix(), kvs)) {
    return false;
  }

  if (!store_meta_->Recover(kvs)) {
    return false;
  }
  LOG(INFO) << "Recover store_meta, count=" << kvs.size();
  kvs.clear();

  // schema map
  if (!meta_reader_->Scan(schema_meta_->Prefix(), kvs)) {
    return false;
  }

  if (!schema_meta_->Recover(kvs)) {
    return false;
  }
  LOG(INFO) << "Recover schema_meta, count=" << kvs.size();
  kvs.clear();

  // region map
  if (!meta_reader_->Scan(region_meta_->Prefix(), kvs)) {
    return false;
  }

  if (!region_meta_->Recover(kvs)) {
    return false;
  }
  LOG(INFO) << "Recover region_meta, count=" << kvs.size();
  kvs.clear();

  // table map
  if (!meta_reader_->Scan(table_meta_->Prefix(), kvs)) {
    return false;
  }

  if (!table_meta_->Recover(kvs)) {
    return false;
  }
  LOG(INFO) << "Recover table_meta, count=" << kvs.size();
  kvs.clear();

  // id_epoch map
  if (!meta_reader_->Scan(id_epoch_meta_->Prefix(), kvs)) {
    return false;
  }

  if (!id_epoch_meta_->Recover(kvs)) {
    return false;
  }
  LOG(INFO) << "Recover id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  return true;
}

void CoordinatorControl::GenerateRootSchemas(pb::coordinator_internal::SchemaInternal& root_schema_internal,
                                             pb::coordinator_internal::SchemaInternal& meta_schema_internal,
                                             pb::coordinator_internal::SchemaInternal& dingo_schema_internal) {
  // root schema
  // pb::meta::Schema root_schema;
  root_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  auto* root_schema = root_schema_internal.mutable_schema();
  root_schema->set_name("root");
  auto* schema_id = root_schema->mutable_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  // meta schema
  // pb::meta::Schema meta_schema;
  meta_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::META_SCHEMA);
  auto* meta_schema = meta_schema_internal.mutable_schema();
  schema_id = meta_schema->mutable_id();
  meta_schema->set_name("meta");
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::META_SCHEMA);

  auto* sub_schema_id = root_schema->add_schema_ids();
  sub_schema_id->CopyFrom(*schema_id);

  // dingo schema
  // pb::meta::Schema dingo_schema;
  dingo_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  auto* dingo_schema = meta_schema_internal.mutable_schema();
  dingo_schema->set_name("dingo");
  schema_id = dingo_schema->mutable_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  sub_schema_id = root_schema->add_schema_ids();
  sub_schema_id->CopyFrom(*schema_id);
}

void CoordinatorControl::GenerateRootSchemasMetaIncrement(pb::coordinator_internal::SchemaInternal& root_schema,
                                                          pb::coordinator_internal::SchemaInternal& meta_schema,
                                                          pb::coordinator_internal::SchemaInternal& dingo_schema,
                                                          pb::coordinator_internal::MetaIncrement& meta_increment) {
  // update meta_increment
  auto* schema_increment_root = meta_increment.add_schemas();
  auto* schema_increment_meta = meta_increment.add_schemas();
  auto* schema_increment_dingo = meta_increment.add_schemas();

  schema_increment_root->set_id(root_schema.id());
  schema_increment_root->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  schema_increment_root->set_schema_id(root_schema.schema().id().parent_entity_id());
  auto* increment_root = schema_increment_root->mutable_schema_internal();
  increment_root->CopyFrom(root_schema);

  schema_increment_meta->set_id(meta_schema.id());
  schema_increment_meta->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  schema_increment_meta->set_schema_id(meta_schema.schema().id().parent_entity_id());
  auto* increment_meta = schema_increment_meta->mutable_schema_internal();
  increment_meta->CopyFrom(meta_schema);

  schema_increment_dingo->set_id(dingo_schema.id());
  schema_increment_dingo->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  schema_increment_dingo->set_schema_id(dingo_schema.schema().id().parent_entity_id());
  auto* increment_dingo = schema_increment_dingo->mutable_schema_internal();
  increment_dingo->CopyFrom(dingo_schema);
}

// TODO: load data from raft_kv_engine
bool CoordinatorControl::Init() {
  // root=0 meta=1 dingo=2, other schema begins from 3
  // init schema_map_ at innocent cluster
  if (schema_map_.empty()) {
    pb::coordinator_internal::SchemaInternal root_schema;
    pb::coordinator_internal::SchemaInternal meta_schema;
    pb::coordinator_internal::SchemaInternal dingo_schema;

    GenerateRootSchemas(root_schema, meta_schema, dingo_schema);

    // add the initial schemas to schema_map_
    // TODO: data persistence
    schema_map_.insert(std::make_pair(0, root_schema));   // raft_kv_put
    schema_map_.insert(std::make_pair(1, meta_schema));   // raft_kv_put
    schema_map_.insert(std::make_pair(2, dingo_schema));  // raft_kv_put

    // write to rocksdb
    auto const schema_kvs = schema_meta_->TransformToKvWithAll();
    meta_writer_->Put(schema_kvs);

    LOG(INFO) << "init schema_map_ finished";
  }

  return true;
}

uint64_t CoordinatorControl::GetPresentId(const pb::coordinator_internal::IdEpochType& key) {
  uint64_t value = 0;
  if (id_epoch_map_.find(key) == id_epoch_map_.end()) {
    value = 3;
    LOG(INFO) << "GetPresentId key=" << key << " not found, generate new id=" << value;
  } else {
    value = id_epoch_map_[key].value();
    LOG(INFO) << "GetPresentId key=" << key << " value=" << value;
  }

  return value;
}

uint64_t CoordinatorControl::GetNextId(const pb::coordinator_internal::IdEpochType& key,
                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t value = 0;
  if (id_epoch_map_.find(key) == id_epoch_map_.end()) {
    value = 3;
    LOG(INFO) << "GetNextId key=" << key << " not found, generate new id=" << value;
  } else {
    value = id_epoch_map_[key].value();
    LOG(INFO) << "GetNextId key=" << key << " value=" << value;
  }
  value++;

  // update id in memory
  id_epoch_map_[key].set_value(value);

  // generate meta_increment
  auto* idepoch = meta_increment.add_idepochs();
  idepoch->set_id(key);
  idepoch->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

  auto* idepoch_internl = idepoch->mutable_idepoch();
  idepoch_internl->set_id(key);
  idepoch_internl->set_value(value);

  return value;
}

// TODO: check name comflicts before create new schema
int CoordinatorControl::CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t& new_schema_id,
                                     pb::coordinator_internal::MetaIncrement& meta_increment) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  // initial schema create
  if (!root_schema_writed_to_raft_) {
    pb::coordinator_internal::SchemaInternal root_schema;
    pb::coordinator_internal::SchemaInternal meta_schema;
    pb::coordinator_internal::SchemaInternal dingo_schema;

    GenerateRootSchemas(root_schema, meta_schema, dingo_schema);
    GenerateRootSchemasMetaIncrement(root_schema, meta_schema, dingo_schema, meta_increment);

    root_schema_writed_to_raft_ = true;
  }

  // validate
  if (schema_map_.find(parent_schema_id) == schema_map_.end()) {
    LOG(INFO) << " CreateSchema parent_schema_id is illegal " << parent_schema_id;
    return -1;
  }

  if (schema_name.empty()) {
    LOG(INFO) << " CreateSchema schema_name is illegal " << schema_name;
    return -1;
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

  auto* new_schema = new_schema_internal.mutable_schema();
  new_schema->set_name(schema_name);
  auto* the_new_schema_id = new_schema->mutable_id();
  // the_new_schema_id->CopyFrom(*schema_id);
  the_new_schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  the_new_schema_id->set_parent_entity_id(parent_schema_id);
  the_new_schema_id->set_entity_id(new_schema_id);

  // update meta_increment
  auto* schema_increment = meta_increment.add_schemas();
  schema_increment->set_id(new_schema_id);
  schema_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

  auto* schema_increment_schema = schema_increment->mutable_schema_internal();
  schema_increment_schema->CopyFrom(new_schema_internal);

  // on_apply
  //  schema_map_.insert(std::make_pair(new_schema_id, new_schema));  // raft_kv_put

  return 0;
}

// TODO: data persistence
uint64_t CoordinatorControl::UpdateStoreMap(const pb::common::Store& store,
                                            pb::coordinator_internal::MetaIncrement& meta_increment) {
  BAIDU_SCOPED_LOCK(control_mutex_);
  uint64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);

  if (store_map_.find(store.id()) != store_map_.end()) {
    if (store_map_[store.id()].state() != store.state()) {
      LOG(INFO) << "STORE STATUS CHANGE store_id = " << store.id() << " old status = " << store_map_[store.id()].state()
                << " new status = " << store.state();

      // update meta_increment
      store_map_epoch = GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_STORE, meta_increment);
      meta_increment.set_store_map_epoch(store_map_epoch);
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
    LOG(INFO) << "NEED ADD NEW STORE store_id = " << store.id();

    // update meta_increment
    store_map_epoch = GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_STORE, meta_increment);
    meta_increment.set_store_map_epoch(store_map_epoch);
    auto* store_increment = meta_increment.add_stores();
    store_increment->set_id(store.id());
    store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

    auto* store_increment_store = store_increment->mutable_store();
    store_increment_store->CopyFrom(store);

    // on_apply
    // store_map_epoch++;                                    // raft_kv_put
    // store_map_.insert(std::make_pair(store.id(), store));  // raft_kv_put
  }

  LOG(INFO) << "UpdateStoreMap store_id=" << store.id();

  return store_map_epoch;
}

int CoordinatorControl::CreateStore(uint64_t cluster_id, uint64_t& store_id, std::string& password,
                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (cluster_id > 0) {
    BAIDU_SCOPED_LOCK(control_mutex_);

    store_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_STORE, meta_increment);
    password = "TO_BE_CONTINUED";

    pb::common::Store store;
    store.set_id(store_id);

    // update meta_increment
    uint64_t store_map_epoch = GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_STORE, meta_increment);
    meta_increment.set_store_map_epoch(store_map_epoch);
    auto* store_increment = meta_increment.add_stores();
    store_increment->set_id(store_id);
    store_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

    auto* store_increment_store = store_increment->mutable_store();
    store_increment_store->CopyFrom(store);

    // on_apply
    // store_map_epoch++;                                  // raft_kv_put
    // store_map_.insert(std::make_pair(store_id, store));  // raft_kv_put
    return 0;
  } else {
    return -1;
  }
}

int CoordinatorControl::CreateTable(uint64_t schema_id, const pb::meta::TableDefinition& table_definition,
                                    uint64_t& new_table_id, pb::coordinator_internal::MetaIncrement& meta_increment) {
  // initial schema create
  if (!root_schema_writed_to_raft_) {
    pb::coordinator_internal::SchemaInternal root_schema;
    pb::coordinator_internal::SchemaInternal meta_schema;
    pb::coordinator_internal::SchemaInternal dingo_schema;

    GenerateRootSchemas(root_schema, meta_schema, dingo_schema);
    GenerateRootSchemasMetaIncrement(root_schema, meta_schema, dingo_schema, meta_increment);

    root_schema_writed_to_raft_ = true;
  }

  // validate schema_id is existed
  if (schema_map_.find(schema_id) == schema_map_.end()) {
    LOG(ERROR) << "schema_id is illegal " << schema_id;
    return -1;
  }

  // validate part information
  if (!table_definition.has_table_partition()) {
    LOG(ERROR) << "no table_partition provided ";
    return -1;
  }
  auto const& table_partition = table_definition.table_partition();
  if (table_partition.has_hash_partition()) {
    LOG(ERROR) << "hash_partiton is not supported";
    return -1;
  } else if (!table_partition.has_range_partition()) {
    LOG(ERROR) << "no range_partition provided ";
    return -1;
  }

  auto const& range_partition = table_partition.range_partition();
  if (range_partition.ranges_size() == 0) {
    LOG(ERROR) << "no range provided ";
    return -1;
  }

  // create table
  // extract part info, create region for each part
  // TODO: 3 is a temp default value
  {
    BAIDU_SCOPED_LOCK(control_mutex_);
    new_table_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, meta_increment);
    LOG(INFO) << "CreateTable new_table_id=" << new_table_id;
  }

  std::vector<uint64_t> new_region_ids;
  for (int i = 0; i < range_partition.ranges_size(); i++) {
    // int ret = CreateRegion(const std::string &region_name, const std::string
    // &resource_tag, int32_t replica_num, pb::common::Range region_range,
    // uint64_t schema_id, uint64_t table_id, uint64_t &new_region_id)
    std::string const region_name = table_definition.name() + "_part_" + std::to_string(i);
    uint64_t new_region_id;
    int const ret = CreateRegion(region_name, "", 3, range_partition.ranges(i), schema_id, new_table_id, new_region_id,
                                 meta_increment);
    if (ret < 0) {
      LOG(ERROR) << "CreateRegion failed in CreateTable table_name=" << table_definition.name();
      break;
    }

    new_region_ids.push_back(new_region_id);
  }

  if (new_region_ids.size() < range_partition.ranges_size()) {
    LOG(ERROR) << "Not enough regions is created, drop residual regions need=" << range_partition.ranges_size()
               << " created=" << new_region_ids.size();
    for (auto region_id_to_delete : new_region_ids) {
      int ret = DropRegion(region_id_to_delete, meta_increment);
      if (ret < 0) {
        LOG(ERROR) << "DropRegion failed in CreateTable table_name=" << table_definition.name()
                   << " region_id =" << region_id_to_delete;
      }
    }
    return -1;
  }

  {
    BAIDU_SCOPED_LOCK(control_mutex_);

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
    uint64_t table_map_epoch = GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_TABLE, meta_increment);
    meta_increment.set_table_map_epoch(table_map_epoch);
    auto* table_increment = meta_increment.add_tables();
    table_increment->set_id(new_table_id);
    table_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

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
  }

  return 0;
}

int CoordinatorControl::DropRegion(uint64_t region_id, pb::coordinator_internal::MetaIncrement& meta_increment) {
  BAIDU_SCOPED_LOCK(control_mutex_);
  // set region state to DELETE
  if (region_map_.find(region_id) != region_map_.end()) {
    auto region_to_delete = region_map_[region_id];
    region_to_delete.set_state(::dingodb::pb::common::RegionState::REGION_DELETE);

    // update meta_increment
    auto* region_increment = meta_increment.add_regions();
    region_increment->set_id(region_id);
    region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);

    auto* region_increment_region = region_increment->mutable_region();
    region_increment_region->CopyFrom(region_to_delete);

    // on_apply
    // region_map_[region_id].set_state(::dingodb::pb::common::RegionState::REGION_DELETE);
    LOG(INFO) << "drop region success, id = " << region_id;
  } else {
    // delete regions on the fly (usually in CreateTable)
    for (int i = 0; i < meta_increment.regions_size(); i++) {
      auto* region_in_increment = meta_increment.mutable_regions(i);
      if (region_in_increment->id() == region_id) {
        region_in_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
      }
    }

    LOG(ERROR) << "ERROR drop region id not exists, id = " << region_id;
    return -1;
  }

  return 0;
}

int CoordinatorControl::CreateRegion(const std::string& region_name, const std::string& resource_tag,
                                     int32_t replica_num, pb::common::Range region_range, uint64_t schema_id,
                                     uint64_t table_id, uint64_t& new_region_id,
                                     pb::coordinator_internal::MetaIncrement& meta_increment) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  std::vector<pb::common::Store> stores_for_regions;
  std::vector<pb::common::Store> selected_stores_for_regions;

  // when resource_tag exists, select store with resource_tag
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

  // if not enough stores it selected, return -1
  if (stores_for_regions.size() < replica_num) {
    LOG(INFO) << "Not enough stores for create region";
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
    LOG(ERROR) << "create_region_id =" << create_region_id << " is illegal, cannot create region!!";
    return -1;
  }

  // create new region in memory
  pb::common::Region new_region;
  new_region.set_id(create_region_id);
  new_region.set_epoch(1);
  new_region.set_name(region_name);
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

// TODO: data persistence
uint64_t CoordinatorControl::UpdateRegionMap(std::vector<pb::common::Region>& regions,
                                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  uint64_t region_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION);

  bool need_to_get_next_epoch = false;
  for (const auto& region : regions) {
    if (region_map_.find(region.id()) != region_map_.end()) {
      LOG(INFO) << " update region to region_map in heartbeat, region_id=" << region.id();
      if (region_map_[region.id()].state() != region.state()) {
        LOG(INFO) << "REGION STATUS CHANGE region_id = " << region.id()
                  << " old status = " << region_map_[region.id()].state() << " new status = " << region.state();
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
      }
    } else {
      LOG(INFO) << " found illegal region in heartbeat, region_id=" << region.id();

      auto* region_increment = meta_increment.add_regions();
      region_increment->set_id(region.id());
      region_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

      auto* region_increment_region = region_increment->mutable_region();
      region_increment_region->CopyFrom(region);

      need_to_get_next_epoch = true;

      // region_map_.insert(std::make_pair(region.id(), region));  // raft_kv_put
    }
  }

  if (need_to_get_next_epoch) {
    region_map_epoch = GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);
  }

  LOG(INFO) << "UpdateRegionMapMulti epoch=" << region_map_epoch;

  return region_map_epoch;
}

void CoordinatorControl::GetStoreMap(pb::common::StoreMap& store_map) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  uint64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);
  store_map.set_epoch(store_map_epoch);

  for (auto& elemnt : store_map_) {
    auto* tmp_region = store_map.add_stores();
    tmp_region->CopyFrom(elemnt.second);
  }
}

void CoordinatorControl::GetRegionMap(pb::common::RegionMap& region_map) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  region_map.set_epoch(GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION));

  for (auto& elemnt : region_map_) {
    auto* tmp_region = region_map.add_regions();
    tmp_region->CopyFrom(elemnt.second);
  }
}

// GetSchemas
// in: schema_id
// out: schemas
void CoordinatorControl::GetSchemas(uint64_t schema_id, std::vector<pb::meta::Schema>& schemas) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  if (schema_id < 0) {
    LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  if (!schemas.empty()) {
    LOG(ERROR) << "ERRROR: vector schemas is not empty , size=" << schemas.size();
    return;
  }

  if (this->schema_map_.find(schema_id) == schema_map_.end()) {
    return;
  }

  auto& schema_internal = schema_map_[schema_id];
  for (int i = 0; i < schema_internal.schema().schema_ids_size(); i++) {
    int sub_schema_id = schema_internal.schema().schema_ids(i).entity_id();
    if (schema_map_.find(sub_schema_id) == schema_map_.end()) {
      LOG(ERROR) << "ERRROR: sub_schema_id " << sub_schema_id << " not exists";
      continue;
    }

    schemas.push_back(schema_map_[sub_schema_id].schema());
  }

  LOG(INFO) << "GetSchemas id=" << schema_id << " sub schema count=" << schema_map_.size();
}

// get tables
void CoordinatorControl::GetTables(uint64_t schema_id,
                                   std::vector<pb::meta::TableDefinitionWithId>& table_definition_with_ids) {
  LOG(INFO) << "GetTables in control schema_id=" << schema_id;

  if (schema_id < 0) {
    LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  if (!table_definition_with_ids.empty()) {
    LOG(ERROR) << "ERRROR: vector table_definition_with_ids is not empty , size=" << table_definition_with_ids.size();
    return;
  }

  BAIDU_SCOPED_LOCK(control_mutex_);

  if (this->schema_map_.find(schema_id) == schema_map_.end()) {
    LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return;
  }

  auto& schema_internal = schema_map_[schema_id];
  for (int i = 0; i < schema_internal.schema().table_ids_size(); i++) {
    int table_id = schema_internal.schema().table_ids(i).entity_id();
    if (table_map_.find(table_id) == table_map_.end()) {
      LOG(ERROR) << "ERRROR: table_id " << table_id << " not exists";
      continue;
    }

    LOG(INFO) << "GetTables found table_id=" << table_id;

    // construct return value
    pb::meta::TableDefinitionWithId table_def_with_id;
    table_def_with_id.mutable_table_id()->CopyFrom(schema_internal.schema().table_ids(i));
    table_def_with_id.mutable_table_definition()->CopyFrom(table_map_[table_id].definition());
    table_definition_with_ids.push_back(table_def_with_id);
  }

  LOG(INFO) << "GetTables schema_id=" << schema_id << " tables count=" << table_definition_with_ids.size();
}

// get table
void CoordinatorControl::GetTable(uint64_t schema_id, uint64_t table_id, pb::meta::Table& table) {
  if (schema_id < 0) {
    LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  if (table.id().entity_id() != 0) {
    LOG(ERROR) << "ERRROR: table is not empty , table_id=" << table.id().entity_id();
    return;
  }

  BAIDU_SCOPED_LOCK(control_mutex_);

  if (this->schema_map_.find(schema_id) == schema_map_.end()) {
    LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return;
  }

  if (this->table_map_.find(table_id) == table_map_.end()) {
    LOG(ERROR) << "ERRROR: table_id not found" << table_id;
    return;
  }

  // construct Table from table_internal
  auto table_internal = table_map_.at(table_id);
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
      LOG(ERROR) << "ERROR cannot find region in regionmap_ while GetTable, table_id =" << table_id
                 << " region_id=" << region_id;
      continue;
    }
    pb::common::Region& part_region = region_map_[region_id];

    // part leader location
    auto* leader_location = part->mutable_leader();

    // part voter & learner locations
    for (int j = 0; j < part_region.peers_size(); j++) {
      const auto& part_peer = part_region.peers(i);
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

// TODO: fininsh logic
void CoordinatorControl::GetCoordinatorMap(uint64_t cluster_id, uint64_t& epoch,
                                           [[maybe_unused]] pb::common::Location& leader_location,
                                           [[maybe_unused]] std::vector<pb::common::Location>& locations) {
  if (cluster_id < 0) {
    return;
  }
  epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_COORINATOR);
  leader_location.mutable_host()->assign("127.0.0.1");
  leader_location.set_port(19190);
}

// ApplyMetaIncrement is on_apply callback
// leader do need update next_xx_id, so leader call this function with update_ids=false
void CoordinatorControl::ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement& meta_increment, bool update_ids) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  // prepare data to write to kv engine
  std::vector<pb::common::KeyValue> meta_write_to_kv;
  std::vector<pb::common::KeyValue> meta_delete_to_kv;

  // leader do not need to update in-memory cache of id & epoch
  // follower need to update in-memory cache of id & epoch
  // 0.id & epoch
  for (int i = 0; i < meta_increment.idepochs_size(); i++) {
    const auto& idepoch = meta_increment.idepochs(i);
    if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
      if (update_ids) {
        auto& create_idepoch = id_epoch_map_[idepoch.id()];
        create_idepoch.CopyFrom(idepoch.idepoch());
      }

      meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));

    } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
      if (update_ids) {
        auto& update_idepoch = id_epoch_map_[idepoch.id()];
        update_idepoch.CopyFrom(idepoch.idepoch());
      }

      meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));

    } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
      if (update_ids) {
        id_epoch_map_.erase(idepoch.id());
      }

      meta_delete_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));
    }
  }

  // 1.coordinator map
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

  // 2.store map
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

  // 3.schema map
  for (int i = 0; i < meta_increment.schemas_size(); i++) {
    const auto& schema = meta_increment.schemas(i);
    if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
      // update parent schema for user schemas
      if (schema.id() > pb::meta::ReservedSchemaIds::DINGO_SCHEMA) {
        if (schema_map_.find(schema.schema_id()) != schema_map_.end()) {
          auto* new_sub_schema_id = schema_map_[schema.schema_id()].mutable_schema()->add_schema_ids();
          new_sub_schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
          new_sub_schema_id->set_entity_id(schema.id());
          new_sub_schema_id->set_parent_entity_id(schema.schema_id());

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema_map_[schema.schema_id()]));
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

      // meta_delete_kv
      meta_delete_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));
    }
  }

  // 4.region map
  for (int i = 0; i < meta_increment.regions_size(); i++) {
    const auto& region = meta_increment.regions(i);
    if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
      // add region to region_map
      region_map_[region.id()] = region.region();

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

  // 5.table map
  for (int i = 0; i < meta_increment.tables_size(); i++) {
    const auto& table = meta_increment.tables(i);
    if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
      // add table to table_map
      table_map_[table.id()] = table.table();

      // meta_write_kv
      meta_write_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));

      // update parent schema
      pb::meta::DingoCommonId table_common_id;
      table_common_id.set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
      table_common_id.set_entity_id(table.id());
      table_common_id.set_parent_entity_id(table.schema_id());

      if (schema_map_.find(table.schema_id()) != schema_map_.end()) {
        auto* add_table_id = schema_map_[table.schema_id()].mutable_schema()->add_table_ids();
        add_table_id->CopyFrom(table_common_id);

        // meta_write_kv
        meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema_map_[table.schema_id()]));

      } else {
        LOG(ERROR) << " CREATE TABLE apply illegal schema_id=" << table.schema_id() << " table_id=" << table.id()
                   << " table_name=" << table.table().definition().name();
      }

    } else if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
      // update table to table_map
      auto& update_table = table_map_[table.id()];
      update_table.CopyFrom(table.table());

      // meta_write_kv
      meta_write_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));

    } else if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
      // delete table from table_map
      table_map_.erase(table.id());

      // meta_delete_kv
      meta_delete_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));
    }
  }

  // write update to local engine
  if (!meta_writer_->Put(meta_write_to_kv)) {
    LOG(INFO) << "ApplyMetaIncrement meta_write failed, exit program";
    LOG(ERROR) << "ApplyMetaIncrement meta_write failed, exit program";

    exit(-1);
  }
}

}  // namespace dingodb

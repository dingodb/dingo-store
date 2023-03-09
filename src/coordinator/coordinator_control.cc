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

#include <bits/types/FILE.h>
#include <sys/types.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "butil/scoped_lock.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

CoordinatorControl::CoordinatorControl() { bthread_mutex_init(&control_mutex_, nullptr); }

void CoordinatorControl::Init() {
  next_coordinator_id_ = 1;
  next_store_id_ = 1;
  next_region_id_ = 1;
  next_table_id_ = 1;
  next_partition_id_ = 1;

  // root=0 meta=1 dingo=2, other schema begins from 3
  next_schema_id_ = 3;

  // init schema_map_ at innocent cluster
  if (schema_map_.empty()) {
    pb::meta::Schema root_schema;
    root_schema.set_name("root");
    auto* schema_id = root_schema.mutable_id();
    schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
    schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
    schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

    // meta schema
    pb::meta::Schema meta_schema;
    meta_schema.set_name("dingo");
    schema_id = meta_schema.mutable_id();
    schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
    schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
    schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

    auto* sub_schema_id = root_schema.add_schema_ids();
    sub_schema_id->CopyFrom(*schema_id);

    // dingo schema
    pb::meta::Schema dingo_schema;
    dingo_schema.set_name("dingo");
    schema_id = dingo_schema.mutable_id();
    schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
    schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
    schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

    sub_schema_id = root_schema.add_schema_ids();
    sub_schema_id->CopyFrom(*schema_id);

    // add the initial schemas to schema_map_
    // TODO: data persistence
    schema_map_.insert(std::make_pair(0, root_schema));
    schema_map_.insert(std::make_pair(1, meta_schema));
    schema_map_.insert(std::make_pair(2, dingo_schema));

    LOG(INFO) << "init schema_map_ finished";
  }
}

uint64_t CoordinatorControl::CreateCoordinatorId() { return next_coordinator_id_++; }

// TODO: check name comflicts before create new schema
int CoordinatorControl::CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t& new_schema_id) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  // validate
  if (schema_map_.find(parent_schema_id) == schema_map_.end()) {
    LOG(INFO) << " CreateSchema parent_schema_id is illegal " << parent_schema_id;
    return -1;
  }

  if (schema_name.empty()) {
    LOG(INFO) << " CreateSchema schema_name is illegal " << schema_name;
    return -1;
  }

  new_schema_id = CreateSchemaId();

  // add new schema to parent schema
  auto* schema_id = schema_map_[parent_schema_id].add_schema_ids();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(parent_schema_id);
  schema_id->set_entity_id(new_schema_id);

  // add new chema to  schema_map_
  pb::meta::Schema new_schema;
  new_schema.set_name(schema_name);
  auto* the_new_schema_id = new_schema.mutable_id();
  the_new_schema_id->CopyFrom(*schema_id);

  schema_map_.insert(std::make_pair(new_schema_id, new_schema));

  return 0;
}

uint64_t CoordinatorControl::CreateStoreId() { return next_store_id_++; }
uint64_t CoordinatorControl::CreateRegionId() { return next_region_id_++; }
uint64_t CoordinatorControl::CreateSchemaId() { return next_schema_id_++; }
uint64_t CoordinatorControl::CreateTableId() { return next_table_id_++; }
uint64_t CoordinatorControl::CreatePartitionId() { return next_partition_id_++; }

// TODO: data persistence
uint64_t CoordinatorControl::UpdateStoreMap(const pb::common::Store& store) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  bool need_update_epoch = false;
  bool need_add_store = true;
  for (int i = 0; i < store_map_.stores_size(); i++) {
    auto* old_store = store_map_.mutable_stores(i);
    if (old_store->id() == store.id()) {
      // update old store properties
      if (old_store->state() != store.state()) {
        LOG(INFO) << "STORE STATUS CHANGE store_id = " << store.id() << " old status = " << old_store->state()
                  << " new status = " << store.state();
        store_map_.set_epoch(store_map_.epoch() + 1);
        need_update_epoch = true;
      }
      old_store->CopyFrom(store);
      need_add_store = false;
      break;
    }
  }

  if (need_add_store) {
    // create new store
    auto* new_store = store_map_.add_stores();
    new_store->CopyFrom(store);
  }

  LOG(INFO) << "UpdateStoreMap " << store_map_.DebugString();

  return store_map_.epoch();
}

const pb::common::StoreMap& CoordinatorControl::GetStoreMap() {
  BAIDU_SCOPED_LOCK(control_mutex_);

  return this->store_map_;
}

void CoordinatorControl::GetRegionMap(pb::common::RegionMap& region_map) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  region_map.set_epoch(region_map_epoch_);

  for (auto& elemnt : region_map_) {
    auto* tmp_region = region_map.add_regions();
    tmp_region->CopyFrom(elemnt.second);
  }
}

int CoordinatorControl::CreateStore(uint64_t cluster_id, uint64_t& store_id, std::string& password) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  if (cluster_id > 0) {
    store_id = CreateStoreId();
    password = "TO_BE_CONTINUED";

    auto* store = store_map_.add_stores();
    store->set_id(store_id);

    return 0;
  } else {
    return -1;
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

  auto& schema = schema_map_[schema_id];
  for (int i = 0; i < schema.schema_ids_size(); i++) {
    int sub_schema_id = schema.schema_ids(i).entity_id();
    if (schema_map_.find(sub_schema_id) == schema_map_.end()) {
      LOG(ERROR) << "ERRROR: sub_schema_id " << sub_schema_id << " not exists";
      continue;
    }

    schemas.push_back(schema_map_[sub_schema_id]);
  }

  LOG(INFO) << "GetSchemas id=" << schema_id << " sub schema count=" << schema_map_.size();
}

int CoordinatorControl::CreateTable(uint64_t schema_id, const pb::meta::TableDefinition& table_definition,
                                    uint64_t& new_table_id) {
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
    new_table_id = CreateTableId();
    LOG(INFO) << "CreateTable new_table_id=" << new_table_id;
  }

  std::vector<uint64_t> new_region_ids;
  for (int i = 0; i < range_partition.ranges_size(); i++) {
    // int ret = CreateRegion(const std::string &region_name, const std::string
    // &resource_tag, int32_t replica_num, pb::common::Range region_range,
    // uint64_t schema_id, uint64_t table_id, uint64_t &new_region_id)
    std::string region_name = table_definition.name() + "_part_" + std::to_string(i);
    uint64_t new_region_id;
    int ret = CreateRegion(region_name, "", 3, range_partition.ranges(i), schema_id, new_table_id, new_region_id);
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
      int ret = DropRegion(region_id_to_delete);
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
    table_map_[new_table_id] = table_internal;

    // add table_id to schema
    auto* table_id = schema_map_[schema_id].add_table_ids();
    table_id->set_entity_id(new_table_id);
    table_id->set_parent_entity_id(schema_id);
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  }

  return 0;
}

int CoordinatorControl::DropRegion(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(control_mutex_);
  // set region state to DELETE
  if (region_map_.find(region_id) != region_map_.end()) {
    region_map_[region_id].set_state(::dingodb::pb::common::RegionState::REGION_DELETE);
    LOG(INFO) << "drop region success, id = " << region_id;
  } else {
    LOG(ERROR) << "ERROR drop region id not exists, id = " << region_id;
    return -1;
  }

  return 0;
}

int CoordinatorControl::CreateRegion(const std::string& region_name, const std::string& resource_tag,
                                     int32_t replica_num, pb::common::Range region_range, uint64_t schema_id,
                                     uint64_t table_id, uint64_t& new_region_id) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  std::vector<pb::common::Store> stores_for_regions;
  std::vector<pb::common::Store> selected_stores_for_regions;

  // when resource_tag exists, select store with resource_tag
  for (int i = 0; i < store_map_.stores_size(); i++) {
    const auto& store = store_map_.stores(i);
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
  uint64_t create_region_id = CreateRegionId();
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

  region_map_epoch_++;
  new_region_id = create_region_id;

  return 0;
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

  auto& schema = schema_map_[schema_id];
  for (int i = 0; i < schema.table_ids_size(); i++) {
    int table_id = schema.table_ids(i).entity_id();
    if (table_map_.find(table_id) == table_map_.end()) {
      LOG(ERROR) << "ERRROR: table_id " << table_id << " not exists";
      continue;
    }

    LOG(INFO) << "GetTables found table_id=" << table_id;

    // construct return value
    pb::meta::TableDefinitionWithId table_def_with_id;
    table_def_with_id.mutable_table_id()->CopyFrom(schema.table_ids(i));
    table_def_with_id.mutable_table_definition()->CopyFrom(table_map_[table_id].definition());
    table_definition_with_ids.push_back(table_def_with_id);
  }

  LOG(INFO) << "GetTables schema_id=" << schema_id << " tables count=" << table_definition_with_ids.size();
}

// get table
void CoordinatorControl::GetTable(uint64_t schema_id, uint64_t table_id, pb::meta::Table& table) {
  BAIDU_SCOPED_LOCK(control_mutex_);
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
    part->set_regionmap_epoch(region_map_epoch_);

    // part storemap_epoch
    part->set_storemap_epoch(store_map_.epoch());
  }
}

// TODO: data persistence
uint64_t CoordinatorControl::UpdateRegionMap(std::vector<pb::common::Region>& regions) {
  BAIDU_SCOPED_LOCK(control_mutex_);

  bool need_to_update_epoch = false;

  for (const auto& region : regions) {
    if (region_map_.find(region.id()) != region_map_.end()) {
      LOG(INFO) << " update region to region_map in heartbeat, region_id=" << region.id();
      if (region_map_[region.id()].state() != region.state()) {
        LOG(INFO) << "REGION STATUS CHANGE region_id = " << region.id()
                  << " old status = " << region_map_[region.id()].state() << " new status = " << region.state();
        region_map_[region.id()].CopyFrom(region);
        need_to_update_epoch = true;
      }
    } else {
      LOG(ERROR) << " found illegal region in heartbeat, region_id=" << region.id();
      region_map_.insert(std::make_pair(region.id(), region));
      need_to_update_epoch = true;
    }

    if (need_to_update_epoch) {
      region_map_epoch_++;
    }
  }
  LOG(INFO) << "UpdateRegionMapMulti epoch=" << region_map_epoch_;

  return region_map_epoch_;
}

}  // namespace dingodb

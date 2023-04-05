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

#include <cstddef>
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
#include "coordinator/coordinator_control.h"
#include "engine/snapshot.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"

namespace dingodb {

void CoordinatorControl::GenerateRootSchemas(pb::coordinator_internal::SchemaInternal& root_schema_internal,
                                             pb::coordinator_internal::SchemaInternal& meta_schema_internal,
                                             pb::coordinator_internal::SchemaInternal& dingo_schema_internal,
                                             pb::coordinator_internal::SchemaInternal& mysql_schema_internal,
                                             pb::coordinator_internal::SchemaInternal& information_schema_internal) {
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

  // mysql schema
  // pb::mysql::Schema mysql_schema;
  mysql_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::MYSQL_SCHEMA);
  mysql_schema_internal.set_name("mysql");

  root_schema_internal.add_schema_ids(mysql_schema_internal.id());

  // information schema
  // pb::information::Schema information_schema;
  information_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::INFORMATION_SCHEMA);
  information_schema_internal.set_name("information");

  root_schema_internal.add_schema_ids(information_schema_internal.id());

  DINGO_LOG(INFO) << "GenerateRootSchemas 0[" << root_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 1[" << meta_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 2[" << dingo_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 3[" << mysql_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 4[" << information_schema_internal.DebugString();
}

bool CoordinatorControl::ValidateSchema(uint64_t schema_id) {
  BAIDU_SCOPED_LOCK(schema_map_mutex_);
  auto* temp_schema = schema_map_.seek(schema_id);
  if (temp_schema == nullptr) {
    DINGO_LOG(ERROR) << " ValidateSchema schema_id is illegal " << schema_id;
    return false;
  }

  return true;
}

// TODO: check name comflicts before create new schema
int CoordinatorControl::CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t& new_schema_id,
                                     pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate

  if (!ValidateSchema(parent_schema_id)) {
    DINGO_LOG(ERROR) << " CreateSchema parent_schema_id is illegal " << parent_schema_id;
    return -1;
  }

  if (schema_name.empty()) {
    DINGO_LOG(INFO) << " CreateSchema schema_name is illegal " << schema_name;
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

// drop schema
// in: parent_schema_id
// in: schema_id
// return: 0 or -1
int CoordinatorControl::DropSchema(uint64_t parent_schema_id, uint64_t schema_id,
                                   pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_id <= COORDINATOR_ID_OF_MAP_MIN) {
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
    auto* schema_to_delete = schema_map_.seek(schema_id);
    if (schema_to_delete == nullptr) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return -1;
    }

    if (schema_to_delete->table_ids_size() > 0 || schema_to_delete->schema_ids_size() > 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema is not null" << schema_id
                       << " table_ids_size=" << schema_to_delete->table_ids_size()
                       << " schema_ids_size=" << schema_to_delete->schema_ids_size();
      return -1;
    }
    // construct schema from schema_internal
    schema_internal = *schema_to_delete;
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
    auto* schema_internal = schema_map_.seek(schema_id);
    if (schema_internal == nullptr) {
      return;
    }

    DINGO_LOG(INFO) << " sub schema count=" << schema_internal->schema_ids_size();

    for (int i = 0; i < schema_internal->schema_ids_size(); i++) {
      uint64_t sub_schema_id = schema_internal->schema_ids(i);

      DINGO_LOG(INFO) << "sub_schema_id=" << sub_schema_id;

      DINGO_LOG(INFO) << schema_internal->DebugString();

      auto* lean_schema = schema_map_.seek(sub_schema_id);
      if (lean_schema == nullptr) {
        DINGO_LOG(ERROR) << "ERRROR: sub_schema_id " << sub_schema_id << " not exists";
        DINGO_LOG(INFO) << "ERRROR: sub_schema_id " << sub_schema_id << " not exists";
        continue;
      }
      DINGO_LOG(INFO) << " schema_map_ =" << schema_map_[sub_schema_id].DebugString();

      DINGO_LOG(INFO) << " GetSchemas push_back sub schema id=" << sub_schema_id;

      // construct sub_schema_for_response
      pb::meta::Schema sub_schema_for_response;

      auto* sub_schema_common_id = sub_schema_for_response.mutable_id();
      sub_schema_common_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
      sub_schema_common_id->set_entity_id(lean_schema->id());  // this is equal to sub_schema_id
      sub_schema_common_id->set_parent_entity_id(
          schema_id);  // this is sub_schema_id's parent which is the input schema_id

      for (auto x : lean_schema->schema_ids()) {
        auto* temp_id = sub_schema_for_response.add_schema_ids();
        temp_id->set_entity_id(x);
        temp_id->set_parent_entity_id(sub_schema_id);
        temp_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
      }

      for (auto x : lean_schema->table_ids()) {
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

int CoordinatorControl::CreateTableId(uint64_t schema_id, uint64_t& new_table_id,
                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate schema_id is existed
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (schema_map_.seek(schema_id) == nullptr) {
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
    if (schema_map_.seek(schema_id) == nullptr) {
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

// drop table
int CoordinatorControl::DropTable(uint64_t schema_id, uint64_t table_id,
                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return -1;
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return -1;
  }

  pb::coordinator_internal::TableInternal table_internal;
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    auto* temp_table = table_map_.seek(table_id);
    if (temp_table == nullptr) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return -1;
    }

    // construct Table from table_internal
    table_internal = *temp_table;
  }

  bool need_delete_region = false;
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    for (int i = 0; i < table_internal.partitions_size(); i++) {
      // part id
      uint64_t region_id = table_internal.partitions(i).region_id();

      // get region
      if (region_map_.seek(region_id) == nullptr) {
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
    if (this->schema_map_.seek(schema_id) == nullptr) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return;
    }

    auto& schema_internal = schema_map_[schema_id];
    for (int i = 0; i < schema_internal.table_ids_size(); i++) {
      uint64_t table_id = schema_internal.table_ids(i);
      auto* temp_table = table_map_.seek(table_id);
      if (temp_table == nullptr) {
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

      table_def_with_id.mutable_table_definition()->CopyFrom(temp_table->definition());
      table_definition_with_ids.push_back(table_def_with_id);
    }
  }

  DINGO_LOG(INFO) << "GetTables schema_id=" << schema_id << " tables count=" << table_definition_with_ids.size();
}

// get table
void CoordinatorControl::GetTable(uint64_t schema_id, uint64_t table_id,
                                  pb::meta::TableDefinitionWithId& table_definition_with_id) {
  DINGO_LOG(INFO) << "GetTables in control schema_id=" << schema_id;

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  if (table_id <= 0) {
    DINGO_LOG(ERROR) << "ERRROR: table illegal, table_id=" << table_id;
    return;
  }

  // validate schema_id
  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return;
  }

  // validate table_id & get table definition
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    auto* temp_table = table_map_.seek(table_id);
    if (temp_table == nullptr) {
      DINGO_LOG(ERROR) << "ERRROR: table_id " << table_id << " not exists";
      return;
    }

    DINGO_LOG(INFO) << "GetTable found table_id=" << table_id;

    // table_def_with_id.mutable_table_id()->CopyFrom(schema_internal.schema().table_ids(i));
    auto* table_id_for_response = table_definition_with_id.mutable_table_id();
    table_id_for_response->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
    table_id_for_response->set_entity_id(table_id);
    table_id_for_response->set_parent_entity_id(schema_id);

    table_definition_with_id.mutable_table_definition()->CopyFrom(temp_table->definition());
  }

  DINGO_LOG(DEBUG) << "GetTable schema_id=" << schema_id << " table_id=" << table_id
                   << " table_definition_with_id=" << table_definition_with_id.DebugString();
}

// get table range
void CoordinatorControl::GetTableRange(uint64_t schema_id, uint64_t table_id, pb::meta::TableRange& table_range) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  pb::coordinator_internal::TableInternal table_internal;
  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return;
  }
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);

    auto* temp_table = table_map_.seek(table_id);
    if (temp_table == nullptr) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return;
    }

    // construct Table from table_internal
    table_internal = *temp_table;
  }

  for (int i = 0; i < table_internal.partitions_size(); i++) {
    auto* range_distribution = table_range.add_range_distribution();

    // range_distribution id
    uint64_t region_id = table_internal.partitions(i).region_id();

    auto* common_id_region = range_distribution->mutable_id();
    common_id_region->set_entity_id(region_id);
    common_id_region->set_parent_entity_id(table_id);
    common_id_region->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);

    // range_distribution range
    auto* part_range = range_distribution->mutable_range();
    part_range->CopyFrom(table_internal.partitions(i).range());

    // get region
    auto* part_region = region_map_.seek(region_id);

    if (part_region == nullptr) {
      DINGO_LOG(ERROR) << "ERROR cannot find region in regionmap_ while GetTable, table_id =" << table_id
                       << " region_id=" << region_id;
      continue;
    }

    // range_distribution leader location
    auto* leader_location = range_distribution->mutable_leader();

    // range_distribution voter & learner locations
    for (int j = 0; j < part_region->peers_size(); j++) {
      const auto& part_peer = part_region->peers(j);
      if (part_peer.store_id() == part_region->leader_store_id()) {
        leader_location->CopyFrom(part_peer.server_location());
      }

      if (part_peer.role() == ::dingodb::pb::common::PeerRole::VOTER) {
        auto* voter_location = range_distribution->add_voters();
        voter_location->CopyFrom(part_peer.server_location());
      } else if (part_peer.role() == ::dingodb::pb::common::PeerRole::LEARNER) {
        auto* learner_location = range_distribution->add_learners();
        learner_location->CopyFrom(part_peer.server_location());
      }
    }

    // range_distribution regionmap_epoch
    uint64_t region_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION);
    range_distribution->set_regionmap_epoch(region_map_epoch);

    // range_distribution storemap_epoch
    uint64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);
    range_distribution->set_storemap_epoch(store_map_epoch);
  }
}

// get table metrics
void CoordinatorControl::GetTableMetrics(uint64_t schema_id, uint64_t table_id,
                                         pb::meta::TableMetricsWithId& table_metrics) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  if (table_metrics.id().entity_id() != 0) {
    DINGO_LOG(ERROR) << "ERRROR: table is not empty , table_id=" << table_metrics.id().entity_id();
    return;
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return;
  }

  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (table_map_.seek(table_id) == nullptr) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return;
    }
  }

  pb::coordinator_internal::TableMetricsInternal table_metrics_internal;
  {
    BAIDU_SCOPED_LOCK(table_metrics_map_mutex_);
    auto* temp_table_metrics = table_metrics_map_.seek(table_id);
    if (temp_table_metrics == nullptr) {
      DINGO_LOG(INFO) << "table_metrics not found, try to calculate new one" << table_id;

      // calculate table metrics using region metrics
      auto* table_metrics_single = table_metrics_internal.mutable_table_metrics();
      if (CalculateTableMetricsSingle(table_id, *table_metrics_single) < 0) {
        DINGO_LOG(ERROR) << "ERRROR: CalculateTableMetricsSingle failed" << table_id;
      } else {
        table_metrics_internal.set_id(table_id);
        table_metrics_internal.mutable_table_metrics()->CopyFrom(*table_metrics_single);
        // table_metrics_map_[table_id] = table_metrics_internal;
        temp_table_metrics->CopyFrom(table_metrics_internal);

        DINGO_LOG(INFO) << "table_metrics first calculated, table_id=" << table_id
                        << " row_count=" << table_metrics_single->rows_count()
                        << " min_key=" << table_metrics_single->min_key()
                        << " max_key=" << table_metrics_single->max_key()
                        << " part_count=" << table_metrics_single->part_count();
      }
    } else {
      // construct TableMetrics from table_metrics_internal
      DINGO_LOG(DEBUG) << "table_metrics found, return metrics in map" << table_id;
      table_metrics_internal = *temp_table_metrics;
    }
  }

  // construct TableMetricsWithId
  auto* common_id_table = table_metrics.mutable_id();
  common_id_table->set_entity_id(table_id);
  common_id_table->set_parent_entity_id(schema_id);
  common_id_table->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  table_metrics.mutable_table_metrics()->CopyFrom(table_metrics_internal.table_metrics());
}

// CalculateTableMetricsSingle
uint64_t CoordinatorControl::CalculateTableMetricsSingle(uint64_t table_id, pb::meta::TableMetrics& table_metrics) {
  pb::coordinator_internal::TableInternal table_internal;
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    auto* temp_table = table_map_.seek(table_id);
    if (temp_table == nullptr) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return -1;
    }

    // construct Table from table_internal
    table_internal = *temp_table;
  }

  // build result metrics
  uint64_t row_count = 0;
  std::string min_key(10, '\xFF');
  std::string max_key;

  {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);

    for (int i = 0; i < table_internal.partitions_size(); i++) {
      // part id
      uint64_t region_id = table_internal.partitions(i).region_id();

      // get region
      pb::common::Region part_region;
      {
        BAIDU_SCOPED_LOCK(region_map_mutex_);
        auto* temp_region = region_map_.seek(region_id);
        if (temp_region == nullptr) {
          DINGO_LOG(ERROR) << "ERROR cannot find region in regionmap_ while GetTable, table_id =" << table_id
                           << " region_id=" << region_id;
          continue;
        }
        part_region = *temp_region;
      }

      auto* temp_store_metrics = store_metrics_map_.seek(part_region.leader_store_id());
      // if (store_metrics_map_.find(part_region.leader_store_id()) != store_metrics_map_.end()) {
      if (temp_store_metrics != nullptr) {
        // pb::common::StoreMetrics& store_metrics = store_metrics_map_.at(part_region.leader_store_id());
        const auto& region_metrics = temp_store_metrics->region_metrics_map();

        if (region_metrics.find(part_region.id()) != region_metrics.end()) {
          row_count += region_metrics.at(region_id).row_count();

          if (min_key.empty()) {
            min_key = region_metrics.at(region_id).min_key();
          } else {
            if (min_key.compare(region_metrics.at(region_id).min_key()) > 0) {
              min_key = region_metrics.at(region_id).min_key();
            }
          }

          if (max_key.empty()) {
            max_key = region_metrics.at(region_id).max_key();
          } else {
            if (max_key.compare(region_metrics.at(region_id).max_key()) < 0) {
              max_key = region_metrics.at(region_id).max_key();
            }
          }
        }
      }
    }
  }

  table_metrics.set_rows_count(row_count);
  table_metrics.set_min_key(min_key);
  table_metrics.set_max_key(max_key);
  table_metrics.set_part_count(table_internal.partitions_size());

  DINGO_LOG(DEBUG) << "table_metrics calculated in CalculateTableMetricsSingle, table_id=" << table_id
                   << " row_count=" << table_metrics.rows_count() << " min_key=" << table_metrics.min_key()
                   << " max_key=" << table_metrics.max_key() << " part_count=" << table_metrics.part_count();

  return 0;
}

// CalculateTableMetrics
// calculate table metrics using region metrics
// only recalculate when table_metrics_map_ does contain table_id
// if the table_id is not in table_map_, remove it from table_metrics_map_
void CoordinatorControl::CalculateTableMetrics() {
  BAIDU_SCOPED_LOCK(table_metrics_map_mutex_);

  for (auto table_metrics_internal : table_metrics_map_) {
    uint64_t table_id = table_metrics_internal.first;
    pb::meta::TableMetrics table_metrics;
    if (CalculateTableMetricsSingle(table_id, table_metrics) < 0) {
      DINGO_LOG(ERROR) << "ERRROR: CalculateTableMetricsSingle failed, remove metrics from map" << table_id;
      table_metrics_map_.erase(table_id);
    } else {
      table_metrics_internal.second.mutable_table_metrics()->CopyFrom(table_metrics);
    }
  }
}

}  // namespace dingodb

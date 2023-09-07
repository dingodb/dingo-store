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

#include "butil/containers/flat_map.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "coordinator/auto_increment_control.h"
#include "coordinator/coordinator_control.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

void CoordinatorControl::GenerateTableIdAndPartIds(uint64_t schema_id, uint64_t part_count,
                                                   pb::meta::EntityType entity_type,
                                                   pb::coordinator_internal::MetaIncrement& meta_increment,
                                                   pb::meta::TableIdWithPartIds* ids) {
  uint64_t new_table_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, meta_increment);

  auto* table_id = ids->mutable_table_id();
  table_id->set_entity_id(new_table_id);
  table_id->set_parent_entity_id(schema_id);
  table_id->set_entity_type(entity_type);

  for (uint32_t i = 0; i < part_count; i++) {
    uint64_t new_part_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, meta_increment);

    auto* part_id = ids->add_part_ids();
    part_id->set_entity_id(new_part_id);
    part_id->set_parent_entity_id(new_table_id);
    part_id->set_entity_type(pb::meta::EntityType::ENTITY_TYPE_PART);
  }
}

// GenerateRootSchemas
// root schema
// meta schema
// dingo schema
// mysql schema
// information schema
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

  // dingo schema
  // pb::meta::Schema dingo_schema;
  dingo_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  dingo_schema_internal.set_name("dingo");

  // mysql schema
  // pb::mysql::Schema mysql_schema;
  mysql_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::MYSQL_SCHEMA);
  mysql_schema_internal.set_name("mysql");

  // information schema
  // pb::information::Schema information_schema;
  information_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::INFORMATION_SCHEMA);
  information_schema_internal.set_name("information_schema");

  DINGO_LOG(INFO) << "GenerateRootSchemas 0[" << root_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 1[" << meta_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 2[" << dingo_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 3[" << mysql_schema_internal.DebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 4[" << information_schema_internal.DebugString();
}

// ValidateSchema
// check if schema_id is exist
// if exist return true
// else return false
bool CoordinatorControl::ValidateSchema(uint64_t schema_id) {
  // BAIDU_SCOPED_LOCK(schema_map_mutex_);
  bool ret = schema_map_.Exists(schema_id);
  if (!ret) {
    DINGO_LOG(ERROR) << " ValidateSchema schema_id is illegal " << schema_id;
    return false;
  }

  return true;
}

// CreateSchema
// create new schema
// only root schema can have sub schema
// schema_name must be unique
// in: parent_schema_id
// in: schema_name
// out: new_schema_id
// out: meta_increment
// return OK if success
// return other if failed
butil::Status CoordinatorControl::CreateSchema(uint64_t parent_schema_id, std::string schema_name,
                                               uint64_t& new_schema_id,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate if parent_schema_id is root schema
  // only root schema can have sub schema
  if (parent_schema_id != ::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA) {
    DINGO_LOG(ERROR) << " CreateSchema parent_schema_id is not root schema " << parent_schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "parent_schema_id is not root schema");
  }

  if (schema_name.empty()) {
    DINGO_LOG(INFO) << " CreateSchema schema_name is illegal " << schema_name;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_name is empty");
  }

  // check if schema_name exists
  uint64_t value = 0;
  schema_name_map_safe_temp_.Get(schema_name, value);
  if (value != 0) {
    DINGO_LOG(INFO) << " CreateSchema schema_name is exist " << schema_name;
    return butil::Status(pb::error::Errno::ESCHEMA_EXISTS, "schema_name is exist");
  }

  // create new schema id
  new_schema_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_SCHEMA, meta_increment);

  // update schema_name_map_safe_temp_
  if (schema_name_map_safe_temp_.PutIfAbsent(schema_name, new_schema_id) < 0) {
    DINGO_LOG(INFO) << " CreateSchema schema_name" << schema_name
                    << " is exist, when insert new_schema_id=" << new_schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_EXISTS, "schema_name is exist");
  }

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

  return butil::Status::OK();
}

// DropSchema
// drop schema
// in: parent_schema_id
// in: schema_id
// return: 0 or -1
butil::Status CoordinatorControl::DropSchema(uint64_t parent_schema_id, uint64_t schema_id,
                                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_id <= COORDINATOR_ID_OF_MAP_MIN) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  if (parent_schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: parent_schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "parent_schema_id is illegal");
  }

  pb::coordinator_internal::SchemaInternal schema_internal_to_delete;
  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    // auto* schema_to_delete = schema_map_.seek(schema_id);
    int ret = schema_map_.Get(schema_id, schema_internal_to_delete);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
    }

    if (schema_internal_to_delete.table_ids_size() > 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema is not empty" << schema_id
                       << " table_ids_size=" << schema_internal_to_delete.table_ids_size();
      return butil::Status(pb::error::Errno::ESCHEMA_NOT_EMPTY, "schema is not empty");
    }
  }

  // bump up epoch
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_SCHEMA, meta_increment);

  // delete schema
  auto* schema_to_delete = meta_increment.add_schemas();
  schema_to_delete->set_id(schema_id);
  schema_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  schema_to_delete->set_schema_id(parent_schema_id);

  auto* schema_to_delete_schema = schema_to_delete->mutable_schema_internal();
  schema_to_delete_schema->CopyFrom(schema_internal_to_delete);

  // delete schema_name from schema_name_map_safe_temp_
  schema_name_map_safe_temp_.Erase(schema_internal_to_delete.name());

  return butil::Status::OK();
}

// GetSchemas
// get schemas
// in: schema_id
// out: schemas
butil::Status CoordinatorControl::GetSchemas(uint64_t schema_id, std::vector<pb::meta::Schema>& schemas) {
  // only root schema can has sub schemas
  if (schema_id != 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  if (!schemas.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: vector schemas is not empty , size=" << schemas.size();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "vector schemas is not empty");
  }

  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    butil::FlatMap<uint64_t, pb::coordinator_internal::SchemaInternal> schema_map_copy;
    schema_map_copy.init(10000);
    int ret = schema_map_.GetRawMapCopy(schema_map_copy);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema_map_ GetRawMapCopy failed";
      return butil::Status(pb::error::Errno::EINTERNAL, "schema_map_ GetRawMapCopy failed");
    }

    for (const auto& it : schema_map_copy) {
      pb::meta::Schema schema;

      auto* temp_id = schema.mutable_id();
      temp_id->set_entity_id(it.first);
      temp_id->set_parent_entity_id(schema_id);
      temp_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);

      schema.set_name(it.second.name());

      // construct table_ids in schema
      for (auto it_table : it.second.table_ids()) {
        pb::meta::DingoCommonId table_id;
        table_id.set_entity_id(it_table);
        table_id.set_parent_entity_id(temp_id->entity_id());
        table_id.set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

        schema.add_table_ids()->CopyFrom(table_id);
      }

      // construct index_ids in schema
      for (auto it_index : it.second.index_ids()) {
        pb::meta::DingoCommonId index_id;
        index_id.set_entity_id(it_index);
        index_id.set_parent_entity_id(temp_id->entity_id());
        index_id.set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);

        schema.add_index_ids()->CopyFrom(index_id);
      }

      schemas.push_back(schema);
    }
  }

  DINGO_LOG(INFO) << "GetSchemas id=" << schema_id << " sub schema count=" << schema_map_.Size();

  return butil::Status::OK();
}

// GetSchema
// in: schema_id
// out: schema
butil::Status CoordinatorControl::GetSchema(uint64_t schema_id, pb::meta::Schema& schema) {
  // only root schema can has sub schemas
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    pb::coordinator_internal::SchemaInternal temp_schema;
    int ret = schema_map_.Get(schema_id, temp_schema);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found " << schema_id;
      return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
    }

    auto* temp_id = schema.mutable_id();
    temp_id->set_entity_id(temp_schema.id());
    temp_id->set_parent_entity_id(::dingodb::pb::meta::ROOT_SCHEMA);
    temp_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);

    schema.set_name(temp_schema.name());

    for (auto it : temp_schema.table_ids()) {
      pb::meta::DingoCommonId table_id;
      table_id.set_entity_id(it);
      table_id.set_parent_entity_id(schema_id);
      table_id.set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

      schema.add_table_ids()->CopyFrom(table_id);
    }
  }

  DINGO_LOG(INFO) << "GetSchema id=" << schema_id << " sub table count=" << schema.table_ids_size();

  return butil::Status::OK();
}

// GetSchemaByName
// in: schema_name
// out: schema
butil::Status CoordinatorControl::GetSchemaByName(const std::string& schema_name, pb::meta::Schema& schema) {
  if (schema_name.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: schema_name illegal " << schema_name;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_name illegal");
  }

  uint64_t temp_schema_id = 0;
  auto ret = schema_name_map_safe_temp_.Get(schema_name, temp_schema_id);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "WARNING: schema_name not found " << schema_name;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_name not found");
  }

  DINGO_LOG(INFO) << "GetSchemaByName name=" << schema_name << " sub table count=" << schema.table_ids_size();

  return GetSchema(temp_schema_id, schema);
}

// CreateTableId
// in: schema_id
// out: new_table_id, meta_increment
// return: 0 success, -1 failed
butil::Status CoordinatorControl::CreateTableId(uint64_t schema_id, uint64_t& new_table_id,
                                                pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate schema_id is existed
  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    bool ret = schema_map_.Exists(schema_id);
    if (!ret) {
      DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
    }
  }

  // create table id
  new_table_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, meta_increment);
  DINGO_LOG(INFO) << "CreateTableId new_table_id=" << new_table_id;

  return butil::Status::OK();
}

// CreateTable
// in: schema_id, table_definition
// out: new_table_id, meta_increment
// return: 0 success, -1 failed
butil::Status CoordinatorControl::CreateTable(uint64_t schema_id, const pb::meta::TableDefinition& table_definition,
                                              uint64_t& new_table_id,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate schema
  // root schema cannot create table
  if (schema_id < 0 || schema_id == ::dingodb::pb::meta::ROOT_SCHEMA) {
    DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id
                     << ", table_definition:" << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    bool ret = schema_map_.Exists(schema_id);
    if (!ret) {
      DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id
                       << ", table_definition:" << table_definition.DebugString();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
    }
  }

  bool has_auto_increment_column = false;
  butil::Status ret =
      AutoIncrementControl::CheckAutoIncrementInTableDefinition(table_definition, has_auto_increment_column);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "check auto increment in table definition error:" << ret.error_str()
                     << ", table_definition:" << table_definition.ShortDebugString();
    return ret;
  }

  // validate part information
  auto const& table_partition = table_definition.table_partition();
  if (table_partition.partitions_size() == 0) {
    DINGO_LOG(ERROR) << "no partition provided" << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "no partition provided");
  }

  DINGO_LOG(INFO) << "CreateTable table_definition:" << table_definition.DebugString();

  // store new_part_id for next usage
  std::vector<uint64_t> new_part_ids;
  std::vector<pb::common::Range> new_part_ranges;

  if (table_partition.partitions_size() > 0) {
    for (const auto& part : table_partition.partitions()) {
      if (part.id().entity_id() <= 0) {
        DINGO_LOG(ERROR) << "part_id is illegal, part_id=" << part.id().entity_id()
                         << ", table_definition:" << table_definition.DebugString();
        return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is illegal");
      }
      if (part.range().start_key().empty() || part.range().end_key().empty()) {
        DINGO_LOG(ERROR) << "part range is illegal, part_id=" << part.id().entity_id()
                         << ", table_definition:" << table_definition.DebugString();
        return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part range is illegal");
      }
      new_part_ids.push_back(part.id().entity_id());
      new_part_ranges.push_back(part.range());
    }
  } else {
    DINGO_LOG(ERROR) << "no range provided " << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "no partition provided");
  }

  if (new_part_ranges.empty()) {
    DINGO_LOG(ERROR) << "no partition provided, table_definition:" << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "no partition provided");
  }

  if (new_part_ranges.size() != new_part_ids.size()) {
    DINGO_LOG(ERROR) << "new_part_ranges.size() != new_part_ids.size(), table_definition:"
                     << table_definition.DebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "new_part_ranges.size() != new_part_ids.size()");
  }

  // check if part_id is legal
  std::set<uint64_t> part_id_set;
  for (auto id : new_part_ids) {
    auto ret = part_id_set.insert(id);
    if (!ret.second) {
      DINGO_LOG(ERROR) << "part_id is illegal, part_id=" << id
                       << ", table_definition:" << table_definition.DebugString();
      return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is illegal");
    }
  }

  // check if table_name exists
  uint64_t value = 0;
  table_name_map_safe_temp_.Get(std::to_string(schema_id) + table_definition.name(), value);
  if (value != 0) {
    DINGO_LOG(INFO) << " Createtable table_name is exist " << table_definition.name();
    return butil::Status(pb::error::Errno::ETABLE_EXISTS,
                         fmt::format("table_name[{}] is exist in get", table_definition.name().c_str()));
  }

  // if new_table_id is not given, create a new table_id
  if (new_table_id <= 0) {
    DINGO_LOG(ERROR) << "CreateTable new_table_id is illegal:" << new_table_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "new_table_id is illegal");
  } else {
    int ret = table_map_.Exists(new_table_id);
    if (ret) {
      DINGO_LOG(ERROR) << "CreateTable new_table_id is already used:" << new_table_id;
      return butil::Status(pb::error::Errno::ETABLE_EXISTS, "new_table_id is already used by other table");
    }
  }

  // create auto increment
  if (has_auto_increment_column) {
    auto status =
        AutoIncrementControl::SyncSendCreateAutoIncrementInternal(new_table_id, table_definition.auto_increment());
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("send create auto increment internal error, code: {}, message: {} ",
                                      status.error_code(), status.error_str())
                       << ", table_definition:" << table_definition.ShortDebugString();
      return butil::Status(pb::error::Errno::EAUTO_INCREMENT_WHILE_CREATING_TABLE,
                           fmt::format("send create auto increment internal error, code: {}, message: {}",
                                       status.error_code(), status.error_str()));
    }
    DINGO_LOG(INFO) << "CreateTable AutoIncrement send create auto increment internal success";
  }

  // update table_name_map_safe_temp_
  if (table_name_map_safe_temp_.PutIfAbsent(std::to_string(schema_id) + table_definition.name(), new_table_id) < 0) {
    DINGO_LOG(INFO) << " CreateTable table_name" << table_definition.name()
                    << " is exist, when insert new_table_id=" << new_table_id;
    return butil::Status(pb::error::Errno::ETABLE_EXISTS,
                         fmt::format("table_name[{}] is exist in put if absent", table_definition.name().c_str()));
  }

  // create table
  // extract part info, create region for each part

  std::vector<uint64_t> new_region_ids;
  int32_t replica = table_definition.replica();
  if (replica < 1) {
    replica = 3;
  }

  // this is just a null parameter
  pb::common::IndexParameter index_parameter;

  // for partitions
  for (int i = 0; i < new_part_ranges.size(); i++) {
    uint64_t new_region_id = 0;
    uint64_t new_part_id = new_part_ids[i];
    auto new_part_range = new_part_ranges[i];

    std::string const region_name = std::string("T_") + std::to_string(schema_id) + std::string("_") +
                                    table_definition.name() + std::string("_part_") + std::to_string(new_part_id);

    auto ret =
        CreateRegion(region_name, pb::common::RegionType::STORE_REGION, "", replica, new_part_range, new_part_range,
                     schema_id, new_table_id, 0, new_part_id, index_parameter, new_region_id, meta_increment);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "CreateRegion failed in CreateTable table_name=" << table_definition.name()
                       << ", table_definition:" << table_definition.ShortDebugString() << " ret: " << ret.error_str();
      table_name_map_safe_temp_.Erase(std::to_string(schema_id) + table_definition.name());
      return ret;
    }

    DINGO_LOG(INFO) << "CreateTable create region success, region_id=" << new_region_id;

    new_region_ids.push_back(new_region_id);
  }

  if (new_region_ids.size() < new_part_ranges.size()) {
    DINGO_LOG(ERROR) << "Not enough regions is created, drop residual regions need=" << new_part_ranges.size()
                     << " created=" << new_region_ids.size();
    for (auto region_id_to_delete : new_region_ids) {
      auto ret = DropRegion(region_id_to_delete, meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "DropRegion failed in CreateTable table_name=" << table_definition.name()
                         << " region_id =" << region_id_to_delete;
      }
    }

    // remove table_name from map
    table_name_map_safe_temp_.Erase(std::to_string(schema_id) + table_definition.name());
    return butil::Status(pb::error::Errno::ETABLE_REGION_CREATE_FAILED, "Not enough regions is created");
  }

  // bumper up EPOCH_REGION
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);

  // create table_internal, set id & table_definition
  pb::coordinator_internal::TableInternal table_internal;
  table_internal.set_id(new_table_id);
  table_internal.set_schema_id(schema_id);
  table_internal.set_table_id(new_table_id);
  auto* definition = table_internal.mutable_definition();
  definition->CopyFrom(table_definition);

  // set part for table_internal
  for (int i = 0; i < new_region_ids.size(); i++) {
    // create part and set region_id & range
    auto* part_internal = table_internal.add_partitions();
    part_internal->set_region_id(new_region_ids[i]);
    part_internal->set_part_id(new_part_ids[i]);
  }

  // add table_internal to table_map_
  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_TABLE, meta_increment);
  auto* table_increment = meta_increment.add_tables();
  table_increment->set_id(new_table_id);
  table_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  // table_increment->set_schema_id(schema_id);

  auto* table_increment_table = table_increment->mutable_table();
  table_increment_table->CopyFrom(table_internal);

  return butil::Status::OK();
}

// DropTable
// in: schema_id, table_id
// out: meta_increment
// return: errno
butil::Status CoordinatorControl::DropTable(uint64_t schema_id, uint64_t table_id,
                                            pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id not valid");
  }

  pb::coordinator_internal::TableInternal table_internal;
  {
    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    int ret = table_map_.Get(table_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found");
    }
  }

  // call DropRegion
  for (int i = 0; i < table_internal.partitions_size(); i++) {
    // part id
    uint64_t region_id = table_internal.partitions(i).region_id();

    DropRegion(region_id, meta_increment);
  }

  // delete table
  auto* table_to_delete = meta_increment.add_tables();
  table_to_delete->set_id(table_id);
  table_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  // table_to_delete->set_schema_id(schema_id);

  auto* table_to_delete_table = table_to_delete->mutable_table();
  table_to_delete_table->CopyFrom(table_internal);

  // bump up table map epoch
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_TABLE, meta_increment);

  // delete table_name from table_name_safe_map_temp_
  table_name_map_safe_temp_.Erase(std::to_string(schema_id) + table_internal.definition().name());

  bool has_auto_increment_column = false;
  AutoIncrementControl::CheckAutoIncrementInTableDefinition(table_internal.definition(), has_auto_increment_column);
  if (has_auto_increment_column) {
    AutoIncrementControl::AsyncSendDeleteAutoIncrementInternal(table_id);
  }

  return butil::Status::OK();
}

// CreateIndexId
// in: schema_id
// out: new_index_id, meta_increment
// return: 0 success, -1 failed
butil::Status CoordinatorControl::CreateIndexId(uint64_t schema_id, uint64_t& new_index_id,
                                                pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate schema_id is existed
  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    bool ret = schema_map_.Exists(schema_id);
    if (!ret) {
      DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
    }
  }

  // create index id
  // new_index_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_INDEX, meta_increment);
  new_index_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, meta_increment);
  DINGO_LOG(INFO) << "CreateIndexId new_index_id=" << new_index_id;

  return butil::Status::OK();
}

// validate index definition
// in: table_definition
// return: errno
butil::Status CoordinatorControl::ValidateIndexDefinition(const pb::meta::TableDefinition& table_definition) {
  // validate index name
  if (table_definition.name().empty()) {
    DINGO_LOG(ERROR) << "index name is empty";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "index name is empty");
  }

  const auto& index_parameter = table_definition.index_parameter();

  // check index_type is not NONE
  if (index_parameter.index_type() == pb::common::IndexType::INDEX_TYPE_NONE) {
    DINGO_LOG(ERROR) << "index_type is NONE";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "index_type is NONE");
  }

  // if index_type is INDEX_TYPE_VECTOR, check vector_index_parameter
  if (index_parameter.index_type() == pb::common::IndexType::INDEX_TYPE_VECTOR) {
    if (!index_parameter.has_vector_index_parameter()) {
      DINGO_LOG(ERROR) << "index_type is INDEX_TYPE_VECTOR, but vector_index_parameter is not set";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "index_type is INDEX_TYPE_VECTOR, but vector_index_parameter is not set");
    }

    const auto& vector_index_parameter = index_parameter.vector_index_parameter();

    // check vector_index_parameter.index_type is not NONE
    if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE) {
      DINGO_LOG(ERROR) << "vector_index_parameter.index_type is NONE";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "vector_index_parameter.index_type is NONE");
    }

    // if vector_index_type is HNSW, check hnsw_parameter is set
    if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      if (!vector_index_parameter.has_hnsw_parameter()) {
        DINGO_LOG(ERROR) << "vector_index_type is HNSW, but hnsw_parameter is not set";
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "vector_index_type is HNSW, but hnsw_parameter is not set");
      }

      const auto& hnsw_parameter = vector_index_parameter.hnsw_parameter();

      // check hnsw_parameter.dimension
      // The dimension of the vector space. This parameter is required and must be greater than 0.
      if (hnsw_parameter.dimension() <= 0 || hnsw_parameter.dimension() > Constant::kVectorMaxDimension) {
        DINGO_LOG(ERROR) << "hnsw_parameter.dimension is illegal " << hnsw_parameter.dimension();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "hnsw_parameter.dimension is illegal " + std::to_string(hnsw_parameter.dimension()));
      }

      // check hnsw_parameter.metric_type
      // The distance metric used to calculate the similarity between vectors. This parameter is required and must not
      // be METRIC_TYPE_NONE.
      if (hnsw_parameter.metric_type() == pb::common::METRIC_TYPE_NONE) {
        DINGO_LOG(ERROR) << "hnsw_parameter.metric_type is illegal " << hnsw_parameter.metric_type();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "hnsw_parameter.metric_type is illegal " + std::to_string(hnsw_parameter.metric_type()));
      }

      // check hnsw_parameter.ef_construction
      // The size of the dynamic list for the nearest neighbors during the construction of the graph. This parameter
      // affects the quality of the graph and the construction time. A larger value leads to a higher quality graph
      // but slower construction time. This parameter must be greater than 0.
      if (hnsw_parameter.efconstruction() <= 0) {
        DINGO_LOG(ERROR) << "hnsw_parameter.ef_construction is illegal " << hnsw_parameter.efconstruction();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "hnsw_parameter.ef_construction is illegal " + std::to_string(hnsw_parameter.efconstruction()));
      }

      // check hnsw_parameter.max_elements
      // The maximum number of elements that can be indexed. This parameter affects the memory usage of the index.
      // This parameter must be greater than 0.
      if (hnsw_parameter.max_elements() <= 0) {
        DINGO_LOG(ERROR) << "hnsw_parameter.max_elements is illegal " << hnsw_parameter.max_elements();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "hnsw_parameter.max_elements is illegal " + std::to_string(hnsw_parameter.max_elements()));
      }

      // check hnsw_parameter.nlinks
      // The number of links for each element in the graph. This parameter affects the quality of the graph and the
      // search time. A larger value leads to a higher quality graph but slower search time. This parameter must be
      // greater than 1.
      // In HNSW, there is a equation: mult_ = 1 / log(1.0 * M_), where M_ is the nlists
      // During latter processing, HNSW will malloc memory directly proportional to mult_, so when M_==1,  mult_ is
      // infinity, malloc will fail.
      if (hnsw_parameter.nlinks() <= 1) {
        DINGO_LOG(ERROR) << "hnsw_parameter.nlinks is illegal " << hnsw_parameter.nlinks();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "hnsw_parameter.nlinks is illegal " + std::to_string(hnsw_parameter.nlinks()));
      }
    }

    // if vector_index_type is FLAT, check flat_parameter is set
    if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT) {
      if (!vector_index_parameter.has_flat_parameter()) {
        DINGO_LOG(ERROR) << "vector_index_type is FLAT, but flat_parameter is not set";
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "vector_index_type is FLAT, but flat_parameter is not set");
      }

      const auto& flat_parameter = vector_index_parameter.flat_parameter();

      // check flat_parameter.dimension
      if (flat_parameter.dimension() <= 0 || flat_parameter.dimension() > Constant::kVectorMaxDimension) {
        DINGO_LOG(ERROR) << "flat_parameter.dimension is illegal " << flat_parameter.dimension();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "flat_parameter.dimension is illegal " + std::to_string(flat_parameter.dimension()));
      }

      // check flat_parameter.metric_type
      if (flat_parameter.metric_type() == pb::common::METRIC_TYPE_NONE) {
        DINGO_LOG(ERROR) << "flat_parameter.metric_type is illegal " << flat_parameter.metric_type();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "flat_parameter.metric_type is illegal " + std::to_string(flat_parameter.metric_type()));
      }
    }

    // if vector_index_type is IVF_FLAT, check ivf_flat_parameter is set
    if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT) {
      if (!vector_index_parameter.has_ivf_flat_parameter()) {
        DINGO_LOG(ERROR) << "vector_index_type is IVF_FLAT, but ivf_flat_parameter is not set";
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "vector_index_type is IVF_FLAT, but ivf_flat_parameter is not set");
      }

      const auto& ivf_flat_parameter = vector_index_parameter.ivf_flat_parameter();

      // check ivf_flat_parameter.dimension
      // The dimension of the vectors to be indexed. This parameter must be greater than 0.
      if (ivf_flat_parameter.dimension() <= 0 || ivf_flat_parameter.dimension() > Constant::kVectorMaxDimension) {
        DINGO_LOG(ERROR) << "ivf_flat_parameter.dimension is illegal " << ivf_flat_parameter.dimension();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "ivf_flat_parameter.dimension is illegal " + std::to_string(ivf_flat_parameter.dimension()));
      }

      // check ivf_flat_parameter.metric_type
      // The distance metric used to compute the distance between vectors. This parameter affects the accuracy of the
      // search. This parameter must not be METRIC_TYPE_NONE.
      if (ivf_flat_parameter.metric_type() == pb::common::METRIC_TYPE_NONE) {
        DINGO_LOG(ERROR) << "ivf_flat_parameter.metric_type is illegal " << ivf_flat_parameter.metric_type();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "ivf_flat_parameter.metric_type is illegal " + std::to_string(ivf_flat_parameter.metric_type()));
      }

      // check ivf_flat_parameter.ncentroids
      // The number of centroids (clusters) used in the product quantization. This parameter affects the memory usage
      // of the index and the accuracy of the search. This parameter must be greater than 0.
      if (ivf_flat_parameter.ncentroids() <= 0) {
        DINGO_LOG(ERROR) << "ivf_flat_parameter.ncentroids is illegal " << ivf_flat_parameter.ncentroids();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "ivf_flat_parameter.ncentroids is illegal " + std::to_string(ivf_flat_parameter.ncentroids()));
      }
    }

    // if vector_index_type is IVF_PQ, check ivf_pq_parameter is set
    if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ) {
      if (!vector_index_parameter.has_ivf_pq_parameter()) {
        DINGO_LOG(ERROR) << "vector_index_type is IVF_PQ, but ivf_pq_parameter is not set";
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "vector_index_type is IVF_PQ, but ivf_pq_parameter is not set");
      }

      const auto& ivf_pq_parameter = vector_index_parameter.ivf_pq_parameter();

      // check ivf_pq_parameter.dimension
      // The dimension of the vectors to be indexed. This parameter must be greater than 0.
      if (ivf_pq_parameter.dimension() <= 0 || ivf_pq_parameter.dimension() > Constant::kVectorMaxDimension) {
        DINGO_LOG(ERROR) << "ivf_pq_parameter.dimension is illegal " << ivf_pq_parameter.dimension();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "ivf_pq_parameter.dimension is illegal " + std::to_string(ivf_pq_parameter.dimension()));
      }

      // check ivf_pq_parameter.metric_type
      // The distance metric used to compute the distance between vectors. This parameter affects the accuracy of the
      // search. This parameter must not be METRIC_TYPE_NONE.
      if (ivf_pq_parameter.metric_type() == pb::common::METRIC_TYPE_NONE) {
        DINGO_LOG(ERROR) << "ivf_pq_parameter.metric_type is illegal " << ivf_pq_parameter.metric_type();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "ivf_pq_parameter.metric_type is illegal " + std::to_string(ivf_pq_parameter.metric_type()));
      }

      // check ivf_pq_parameter.nlist
      // The number of inverted lists (buckets) used in the index. This parameter affects the memory usage of the
      // index and the accuracy of the search. This parameter must be greater than 0.
      if (ivf_pq_parameter.ncentroids() <= 0) {
        DINGO_LOG(ERROR) << "ivf_pq_parameter.ncentroids is illegal " << ivf_pq_parameter.ncentroids();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "ivf_pq_parameter.ncentroids is illegal " + std::to_string(ivf_pq_parameter.ncentroids()));
      }

      // check ivf_pq_parameter.nsubvector
      // The number of subvectors used in the product quantization. This parameter affects the memory usage of the
      // index and the accuracy of the search. This parameter must be greater than 0.
      if (ivf_pq_parameter.nsubvector() <= 0) {
        DINGO_LOG(ERROR) << "ivf_pq_parameter.nsubvector is illegal " << ivf_pq_parameter.nsubvector();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "ivf_pq_parameter.nsubvector is illegal " + std::to_string(ivf_pq_parameter.nsubvector()));
      }

      // check ivf_pq_parameter.bucket_init_size
      // The number of bits used to represent each subvector in the index. This parameter affects the memory usage of
      // the index and the accuracy of the search. This parameter must be greater than 0.
      if (ivf_pq_parameter.bucket_init_size() <= 0) {
        DINGO_LOG(ERROR) << "ivf_pq_parameter.bucket_init_size is illegal " << ivf_pq_parameter.bucket_init_size();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "ivf_pq_parameter.bucket_init_size is illegal " + std::to_string(ivf_pq_parameter.bucket_init_size()));
      }

      // check ivf_pq_parameter.bucket_max_size
      // The maximum number of vectors that can be added to each inverted list (bucket) in the index. This parameter
      // affects the memory usage of the index and the accuracy of the search. This parameter must be greater than 0.
      if (ivf_pq_parameter.bucket_max_size() <= 0) {
        DINGO_LOG(ERROR) << "ivf_pq_parameter.bucket_max_size is illegal " << ivf_pq_parameter.bucket_max_size();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "ivf_pq_parameter.bucket_max_size is illegal " + std::to_string(ivf_pq_parameter.bucket_max_size()));
      }

      // If all checks pass, return a butil::Status object with no error.
      return butil::Status::OK();
    }

    // if vector_index_type is diskann, check diskann_parameter is set
    if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
      if (!vector_index_parameter.has_diskann_parameter()) {
        DINGO_LOG(ERROR) << "vector_index_type is DISKANN, but diskann_parameter is not set";
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "vector_index_type is DISKANN, but diskann_parameter is not set");
      }

      const auto& diskann_parameter = vector_index_parameter.diskann_parameter();

      // check diskann_parameter.dimension
      // The dimension of the vectors to be indexed. This parameter must be greater than 0.
      if (diskann_parameter.dimension() <= 0 || diskann_parameter.dimension() > Constant::kVectorMaxDimension) {
        DINGO_LOG(ERROR) << "diskann_parameter.dimension is illegal " << diskann_parameter.dimension();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "diskann_parameter.dimension is illegal " + std::to_string(diskann_parameter.dimension()));
      }

      // check diskann_parameter.metric_type
      // The distance metric used to compute the distance between vectors. This parameter affects the accuracy of the
      // search. This parameter must not be METRIC_TYPE_NONE.
      if (diskann_parameter.metric_type() == pb::common::METRIC_TYPE_NONE) {
        DINGO_LOG(ERROR) << "diskann_parameter.metric_type is illegal " << diskann_parameter.metric_type();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "diskann_parameter.metric_type is illegal " + std::to_string(diskann_parameter.metric_type()));
      }

      // check diskann_parameter.num_trees
      // The number of trees to be built in the index. This parameter affects the memory usage of the index and the
      // accuracy of the search. This parameter must be greater than 0.
      if (diskann_parameter.num_trees() <= 0) {
        DINGO_LOG(ERROR) << "diskann_parameter.num_trees is illegal " << diskann_parameter.num_trees();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                             "diskann_parameter.num_trees is illegal " + std::to_string(diskann_parameter.num_trees()));
      }

      // check diskann_parameter.num_neighbors
      // The number of nearest neighbors to be returned by the search. This parameter affects the accuracy of the
      // search. This parameter must be greater than 0.
      if (diskann_parameter.num_neighbors() <= 0) {
        DINGO_LOG(ERROR) << "diskann_parameter.num_neighbors is illegal " << diskann_parameter.num_neighbors();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "diskann_parameter.num_neighbors is illegal " + std::to_string(diskann_parameter.num_neighbors()));
      }

      // check diskann_parameter.num_threads
      // The number of CPU cores to be used in building the index. This parameter affects the speed of the index
      // building. This parameter must be greater than 0.
      if (diskann_parameter.num_threads() <= 0) {
        DINGO_LOG(ERROR) << "diskann_parameter.num_threads is illegal " << diskann_parameter.num_threads();
        return butil::Status(
            pb::error::Errno::EILLEGAL_PARAMTETERS,
            "diskann_parameter.num_threads is illegal " + std::to_string(diskann_parameter.num_threads()));
      }

      // If all checks pass, return a butil::Status object with no error.
      return butil::Status::OK();
    }

    return butil::Status::OK();
  } else if (index_parameter.index_type() == pb::common::IndexType::INDEX_TYPE_SCALAR) {
    // check if scalar_index_parameter is set
    if (!index_parameter.has_scalar_index_parameter()) {
      DINGO_LOG(ERROR) << "index_type is SCALAR, but scalar_index_parameter is not set";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "index_type is SCALAR, but scalar_index_parameter is not set");
    }

    const auto& scalar_index_parameter = index_parameter.scalar_index_parameter();

    // check scalar index type
    if (scalar_index_parameter.scalar_index_type() == pb::common::ScalarIndexType::SCALAR_INDEX_TYPE_NONE) {
      DINGO_LOG(ERROR) << "scalar_index_type is illegal " << scalar_index_parameter.scalar_index_type();
      return butil::Status(
          pb::error::Errno::EILLEGAL_PARAMTETERS,
          "scalar_index_type is illegal " + std::to_string(scalar_index_parameter.scalar_index_type()));
    }

    // check scalar index name
    if (table_definition.name().empty()) {
      DINGO_LOG(ERROR) << "scalar index name is empty.";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "scalar index name is empty.");
    }

    // check colums
    if (table_definition.columns_size() == 0) {
      DINGO_LOG(ERROR) << "scalar index cannot find column";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "scalar index cannot find column.");
    }
  }

  return butil::Status::OK();
}

// CreateIndex
// in: schema_id, table_definition
// out: new_index_id, meta_increment
// return: 0 success, -1 failed
butil::Status CoordinatorControl::CreateIndex(uint64_t schema_id, const pb::meta::TableDefinition& table_definition,
                                              uint64_t table_id, uint64_t& new_index_id,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  // validate schema
  // root schema cannot create index
  if (schema_id < 0 || schema_id == ::dingodb::pb::meta::ROOT_SCHEMA) {
    DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    bool ret = schema_map_.Exists(schema_id);
    if (!ret) {
      DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
    }
  }

  // validate index definition
  auto status = ValidateIndexDefinition(table_definition);
  if (!status.ok()) {
    return status;
  }

  // validate part information
  if (!table_definition.has_table_partition()) {
    DINGO_LOG(ERROR) << "no table_partition provided , table_definition=" << table_definition.DebugString();
    return butil::Status(pb::error::Errno::EINDEX_DEFINITION_ILLEGAL, "no table_partition provided");
  }

  auto const& index_partition = table_definition.table_partition();

  DINGO_LOG(INFO) << "CreateIndex index_definition=" << table_definition.DebugString();

  // store new_part_id for next usage
  std::vector<uint64_t> new_part_ids;
  std::vector<pb::common::Range> new_part_ranges;

  if (index_partition.partitions_size() > 0) {
    for (const auto& part : index_partition.partitions()) {
      if (part.id().entity_id() <= 0) {
        DINGO_LOG(ERROR) << "part_id is illegal, part_id=" << part.id().entity_id()
                         << ", table_definition:" << table_definition.DebugString();
        return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is illegal");
      }
      if (part.range().start_key().empty() || part.range().end_key().empty()) {
        DINGO_LOG(ERROR) << "part range is illegal, part_id=" << part.id().entity_id()
                         << ", table_definition:" << table_definition.DebugString();
        return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part range is illegal");
      }
      new_part_ids.push_back(part.id().entity_id());
      new_part_ranges.push_back(part.range());
    }
  } else {
    DINGO_LOG(ERROR) << "no range provided , table_definition=" << table_definition.DebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "no partition provided");
  }

  if (new_part_ranges.empty()) {
    DINGO_LOG(ERROR) << "no partition provided";
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "no partition provided");
  }

  if (new_part_ranges.size() != new_part_ids.size()) {
    DINGO_LOG(ERROR) << "new_part_ranges.size() != new_part_ids.size()";
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "new_part_ranges.size() != new_part_ids.size()");
  }

  // check if part_id is legal
  std::set<uint64_t> part_id_set;
  for (auto id : new_part_ids) {
    auto ret = part_id_set.insert(id);
    if (!ret.second) {
      DINGO_LOG(ERROR) << "part_id is illegal, part_id=" << id
                       << ", table_definition:" << table_definition.DebugString();
      return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is illegal");
    }
  }

  // check if index_name exists
  uint64_t value = 0;
  index_name_map_safe_temp_.Get(std::to_string(schema_id) + table_definition.name(), value);
  if (value != 0) {
    DINGO_LOG(INFO) << " Createindex index_name is exist " << table_definition.name();
    return butil::Status(pb::error::Errno::EINDEX_EXISTS,
                         fmt::format("index_name[{}] is exist in get", table_definition.name()));
  }

  // if new_index_id is not given, create a new index_id
  if (new_index_id <= 0) {
    DINGO_LOG(ERROR) << "CreateIndex new_index_id is illegal:" << new_index_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "new_index_id is illegal");
  } else {
    int ret = index_map_.Exists(new_index_id);
    if (ret) {
      DINGO_LOG(ERROR) << "CreateIndex new_index_id is already used:" << new_index_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "new_index_id is already used by other index");
    }
  }

  // create auto increment
  if (table_definition.auto_increment() > 0) {
    auto status =
        AutoIncrementControl::SyncSendCreateAutoIncrementInternal(new_index_id, table_definition.auto_increment());
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("send create auto increment internal error, code: {}, message: {} ",
                                      status.error_code(), status.error_str())
                       << ", table_definition=" << table_definition.DebugString();
      return butil::Status(pb::error::Errno::EAUTO_INCREMENT_WHILE_CREATING_TABLE,
                           fmt::format("send create auto increment internal error, code: {}, message: {}",
                                       status.error_code(), status.error_str()));
    }
    DINGO_LOG(INFO) << "CreateIndex AutoIncrement send create auto increment internal success";
  }

  // update index_name_map_safe_temp_
  if (index_name_map_safe_temp_.PutIfAbsent(std::to_string(schema_id) + table_definition.name(), new_index_id) < 0) {
    DINGO_LOG(INFO) << " CreateIndex index_name" << table_definition.name()
                    << " is exist, when insert new_index_id=" << new_index_id;
    return butil::Status(pb::error::Errno::EINDEX_EXISTS,
                         fmt::format("index_name[{}] is exist in put if absent", table_definition.name()));
  }

  // create index
  // extract part info, create region for each part

  std::vector<uint64_t> new_region_ids;
  int32_t replica = table_definition.replica();
  if (replica < 1) {
    replica = 3;
  }

  for (int i = 0; i < new_part_ranges.size(); i++) {
    uint64_t new_region_id = 0;
    uint64_t new_part_id = new_part_ids[i];
    auto new_part_range = new_part_ranges[i];

    std::string const region_name = std::string("I_") + std::to_string(schema_id) + std::string("_") +
                                    table_definition.name() + std::string("_part_") + std::to_string(new_part_id);

    // TODO: after sdk support part_id, this should be removed
    pb::common::Range raw_range;
    raw_range.set_start_key(Helper::EncodeIndexRegionHeader(new_part_id, 0));
    raw_range.set_end_key(Helper::EncodeIndexRegionHeader(new_part_id, UINT64_MAX));

    auto ret = CreateRegion(region_name, pb::common::RegionType::INDEX_REGION, "", replica, new_part_range, raw_range,
                            schema_id, 0, new_index_id, new_part_id, table_definition.index_parameter(), new_region_id,
                            meta_increment);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "CreateRegion failed in CreateIndex index_name=" << table_definition.name();
      break;
    }

    DINGO_LOG(INFO) << "CreateIndex create region success, region_id=" << new_region_id;

    new_region_ids.push_back(new_region_id);
  }

  if (new_region_ids.size() < new_part_ranges.size()) {
    DINGO_LOG(ERROR) << "Not enough regions is created, drop residual regions need=" << new_part_ranges.size()
                     << " created=" << new_region_ids.size();
    for (auto region_id_to_delete : new_region_ids) {
      auto ret = DropRegion(region_id_to_delete, meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "DropRegion failed in CreateIndex index_name=" << table_definition.name()
                         << " region_id =" << region_id_to_delete;
      }
    }

    // remove index_name from map
    index_name_map_safe_temp_.Erase(std::to_string(schema_id) + table_definition.name());
    return butil::Status(pb::error::Errno::EINDEX_REGION_CREATE_FAILED, "Not enough regions is created");
  }

  // bumper up EPOCH_REGION
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);

  // create table_internal, set id & table_definition
  pb::coordinator_internal::TableInternal table_internal;
  table_internal.set_id(new_index_id);
  table_internal.set_schema_id(schema_id);
  table_internal.set_table_id(table_id);
  auto* definition = table_internal.mutable_definition();
  definition->CopyFrom(table_definition);

  // set part for table_internal
  for (int i = 0; i < new_region_ids.size(); i++) {
    // create part and set region_id & range
    auto* part_internal = table_internal.add_partitions();
    part_internal->set_region_id(new_region_ids[i]);
    part_internal->set_part_id(new_part_ids[i]);
  }

  // add table_internal to index_map_
  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_INDEX, meta_increment);
  auto* index_increment = meta_increment.add_indexes();
  index_increment->set_id(new_index_id);
  index_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  // index_increment->set_schema_id(schema_id);

  auto* index_increment_index = index_increment->mutable_table();
  index_increment_index->CopyFrom(table_internal);

  return butil::Status::OK();
}

// update index
// in: index_id
// in: table_definition
// return: errno
// TODO: now only support update hnsw index's max_elements
butil::Status CoordinatorControl::UpdateIndex(uint64_t schema_id, uint64_t index_id,
                                              const pb::meta::TableDefinition& new_table_definition,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  DINGO_LOG(INFO) << "UpdateIndex in control schema_id=" << schema_id;

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (index_id <= 0) {
    DINGO_LOG(ERROR) << "ERRROR: index illegal, index_id=" << index_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "index_id illegal");
  }

  // validate schema_id
  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not valid");
  }

  // validate index_id & get table definition
  pb::coordinator_internal::TableInternal table_internal;
  int ret = index_map_.Get(index_id, table_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: index_id not found" << index_id;
    return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index_id not found");
  }

  DINGO_LOG(INFO) << "GetIndex found index_id=" << index_id;

  DINGO_LOG(DEBUG) << fmt::format("UpdateIndex get_index schema_id={} index_id={} table_internal={}", schema_id,
                                  index_id, table_internal.DebugString());

  // this is for interface compatibility, index created by new interface cannot be updated in this function.
  if (table_internal.table_id() > 0) {
    DINGO_LOG(ERROR) << "ERRROR: cannot update index created by new interface." << index_id;
    return butil::Status(pb::error::Errno::EINDEX_COMPATIBILITY, "cannot update index created by new interface.");
  }

  // validate new_table_definition
  auto status = ValidateIndexDefinition(new_table_definition);
  if (!status.ok()) {
    return status;
  }

  // now only support hnsw max_elements update
  if (new_table_definition.index_parameter().vector_index_parameter().vector_index_type() !=
      table_internal.definition().index_parameter().vector_index_parameter().vector_index_type()) {
    DINGO_LOG(ERROR) << "ERRROR: index type not match, new_table_definition=" << new_table_definition.DebugString()
                     << " table_internal.definition()=" << table_internal.definition().DebugString();
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "index type not match");
  }

  if (new_table_definition.index_parameter().vector_index_parameter().vector_index_type() ==
      pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    if (new_table_definition.index_parameter().vector_index_parameter().hnsw_parameter().max_elements() >
        table_internal.definition().index_parameter().vector_index_parameter().hnsw_parameter().max_elements()) {
      DINGO_LOG(INFO)
          << "UpdateIndex update hnsw max_elements from "
          << table_internal.definition().index_parameter().vector_index_parameter().hnsw_parameter().max_elements()
          << " to " << new_table_definition.index_parameter().vector_index_parameter().hnsw_parameter().max_elements();

      auto* index_increment = meta_increment.add_indexes();
      index_increment->set_id(index_id);
      index_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

      auto* increment_index = index_increment->mutable_table();
      table_internal.mutable_definition()
          ->mutable_index_parameter()
          ->mutable_vector_index_parameter()
          ->mutable_hnsw_parameter()
          ->set_max_elements(
              new_table_definition.index_parameter().vector_index_parameter().hnsw_parameter().max_elements());
      increment_index->CopyFrom(table_internal);

      // create store operations
      std::vector<pb::coordinator::StoreOperation> store_operations;

      // for each partition(region) of the index, build store_operation
      for (int i = 0; i < table_internal.partitions_size(); i++) {
        auto region_id = table_internal.partitions(i).region_id();

        pb::common::Region region;
        auto ret = this->region_map_.Get(region_id, region);
        if (ret < 0) {
          DINGO_LOG(ERROR) << "ERRROR: region_id not found" << region_id;
          return butil::Status(pb::error::Errno::EREGION_NOT_FOUND,
                               "region_id not found, region_id=" + std::to_string(region_id));
        }

        // generate new region_definition
        auto region_definition = region.definition();
        region_definition.mutable_index_parameter()
            ->mutable_vector_index_parameter()
            ->mutable_hnsw_parameter()
            ->set_max_elements(
                new_table_definition.index_parameter().vector_index_parameter().hnsw_parameter().max_elements());

        // for each peer, build store_operation
        for (const auto& peer : region.definition().peers()) {
          pb::coordinator::StoreOperation store_operation;

          store_operation.set_id(peer.store_id());

          auto* region_cmd = store_operation.add_region_cmds();
          region_cmd->set_id(region_id);
          region_cmd->set_create_timestamp(butil::gettimeofday_ms());
          region_cmd->set_region_id(region_id);
          region_cmd->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_UPDATE_DEFINITION);
          region_cmd->set_is_notify(false);

          auto* update_definition_request = region_cmd->mutable_update_definition_request();
          update_definition_request->mutable_new_region_definition()->CopyFrom(region_definition);

          store_operations.push_back(store_operation);
        }
      }

      // add store operations to meta_increment
      for (const auto& store_operation : store_operations) {
        auto* store_operation_increment = meta_increment.add_store_operations();
        store_operation_increment->set_id(store_operation.id());
        store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
        store_operation_increment->mutable_store_operation()->CopyFrom(store_operation);

        DINGO_LOG(INFO) << "store_operation_increment = " << store_operation_increment->DebugString();
      }

      DINGO_LOG(INFO)
          << "UpdateIndex built meta_increment OK, store_operation count=" << store_operations.size()
          << ", new_max_elements="
          << new_table_definition.index_parameter().vector_index_parameter().hnsw_parameter().max_elements()
          << ", old_max_elements="
          << table_internal.definition().index_parameter().vector_index_parameter().hnsw_parameter().max_elements();

      return butil::Status::OK();
    } else {
      DINGO_LOG(INFO)
          << "UpdateIndex skip update hnsw max_elements, new_max_elements="
          << new_table_definition.index_parameter().vector_index_parameter().hnsw_parameter().max_elements()
          << " old_max_elements="
          << table_internal.definition().index_parameter().vector_index_parameter().hnsw_parameter().max_elements();

      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "hnsw max_elements only support increase");
    }
  } else {
    DINGO_LOG(ERROR) << "ERRROR: index type not support, new_table_definition=" << new_table_definition.DebugString()
                     << " table_internal.definition()=" << table_internal.definition().DebugString();
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "index definition update type not support");
  }

  return butil::Status::OK();
}

// DropIndex
// in: schema_id, index_id, check_compatibility
// out: meta_increment
// return: errno
butil::Status CoordinatorControl::DropIndex(uint64_t schema_id, uint64_t index_id, bool check_compatibility,
                                            pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id not valid");
  }

  pb::coordinator_internal::TableInternal table_internal;
  {
    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    int ret = index_map_.Get(index_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: index_id not found" << index_id;
      return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index_id not found");
    }

    // this is for interface compatibility, index created by new interface cannot be dropped in this function.
    if (check_compatibility && table_internal.table_id() > 0) {
      DINGO_LOG(ERROR) << "ERRROR: cannot drop index created by new interface." << index_id;
      return butil::Status(pb::error::Errno::EINDEX_COMPATIBILITY, "cannot drop index created by new interface.");
    }
  }

  // call DropRegion
  for (int i = 0; i < table_internal.partitions_size(); i++) {
    // part id
    uint64_t region_id = table_internal.partitions(i).region_id();

    DropRegion(region_id, meta_increment);
  }

  // delete index
  auto* index_to_delete = meta_increment.add_indexes();
  index_to_delete->set_id(index_id);
  index_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
  // index_to_delete->set_schema_id(schema_id);

  auto* index_to_delete_index = index_to_delete->mutable_table();
  index_to_delete_index->CopyFrom(table_internal);

  // bump up index map epoch
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_INDEX, meta_increment);

  // delete index_name from index_name_safe_map_temp_
  index_name_map_safe_temp_.Erase(std::to_string(schema_id) + table_internal.definition().name());

  if (table_internal.definition().auto_increment() > 0) {
    AutoIncrementControl::AsyncSendDeleteAutoIncrementInternal(index_id);
  }

  return butil::Status::OK();
}

// get tables
butil::Status CoordinatorControl::GetTables(uint64_t schema_id,
                                            std::vector<pb::meta::TableDefinitionWithId>& table_definition_with_ids) {
  DINGO_LOG(INFO) << "GetTables in control schema_id=" << schema_id;

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (!table_definition_with_ids.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: vector table_definition_with_ids is not empty , size="
                     << table_definition_with_ids.size();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "vector table_definition_with_ids is not empty");
  }

  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    pb::coordinator_internal::SchemaInternal schema_internal;
    int ret = schema_map_.Get(schema_id, schema_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
    }

    for (int i = 0; i < schema_internal.table_ids_size(); i++) {
      uint64_t table_id = schema_internal.table_ids(i);

      pb::coordinator_internal::TableInternal table_internal;
      int ret = table_map_.Get(table_id, table_internal);
      if (ret < 0) {
        DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
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

      table_def_with_id.mutable_table_definition()->CopyFrom(table_internal.definition());
      table_definition_with_ids.push_back(table_def_with_id);
    }
  }

  DINGO_LOG(INFO) << "GetTables schema_id=" << schema_id << " tables count=" << table_definition_with_ids.size();

  return butil::Status::OK();
}

// get indexes
butil::Status CoordinatorControl::GetIndexes(uint64_t schema_id,
                                             std::vector<pb::meta::TableDefinitionWithId>& table_definition_with_ids) {
  DINGO_LOG(INFO) << "GetIndexes in control schema_id=" << schema_id;

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (!table_definition_with_ids.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: vector table_definition_with_ids is not empty , size="
                     << table_definition_with_ids.size();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "vector table_definition_with_ids is not empty");
  }

  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    pb::coordinator_internal::SchemaInternal schema_internal;
    int ret = schema_map_.Get(schema_id, schema_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
    }

    for (int i = 0; i < schema_internal.index_ids_size(); i++) {
      uint64_t index_id = schema_internal.index_ids(i);

      pb::coordinator_internal::TableInternal table_internal;
      int ret = index_map_.Get(index_id, table_internal);
      if (ret < 0) {
        DINGO_LOG(ERROR) << "ERRROR: index_id not found" << index_id;
        continue;
      }

      if (table_internal.table_id() > 0) {
        DINGO_LOG(WARNING) << "cannot get index created by new interface." << index_id;
        continue;
      }

      DINGO_LOG(INFO) << "GetIndexes found index_id=" << index_id;

      // construct return value
      pb::meta::TableDefinitionWithId table_def_with_id;

      // table_def_with_id.muindex_index_id()->CopyFrom(schema_internal.schema().index_ids(i));
      auto* index_id_for_response = table_def_with_id.mutable_table_id();
      index_id_for_response->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
      index_id_for_response->set_entity_id(index_id);
      index_id_for_response->set_parent_entity_id(schema_id);

      table_def_with_id.mutable_table_definition()->CopyFrom(table_internal.definition());
      table_definition_with_ids.push_back(table_def_with_id);
    }
  }

  DINGO_LOG(INFO) << "GetIndexes schema_id=" << schema_id << " indexes count=" << table_definition_with_ids.size();

  return butil::Status::OK();
}

// get tables count
butil::Status CoordinatorControl::GetTablesCount(uint64_t schema_id, uint64_t& tables_count) {
  DINGO_LOG(INFO) << "GetTables in control schema_id=" << schema_id;

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    pb::coordinator_internal::SchemaInternal schema_internal;
    int ret = schema_map_.Get(schema_id, schema_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
    }

    tables_count = schema_internal.table_ids_size();
  }

  DINGO_LOG(INFO) << "GetTablesCount schema_id=" << schema_id << " tables count=" << tables_count;

  return butil::Status::OK();
}

// get indexes count
butil::Status CoordinatorControl::GetIndexesCount(uint64_t schema_id, uint64_t& indexes_count) {
  DINGO_LOG(INFO) << "GetIndexes in control schema_id=" << schema_id;

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  pb::coordinator_internal::SchemaInternal schema_internal;
  int ret = schema_map_.Get(schema_id, schema_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
  }

  indexes_count = 0;
  for (int i = 0; i < schema_internal.index_ids_size(); i++) {
    uint64_t index_id = schema_internal.index_ids(i);

    pb::coordinator_internal::TableInternal table_internal;
    if (index_map_.Get(index_id, table_internal) < 0) {
      continue;
    }

    if (table_internal.table_id() > 0) {
      continue;
    }

    ++indexes_count;
  }

  DINGO_LOG(INFO) << "GetIndexesCount schema_id=" << schema_id << " indexes count=" << indexes_count;

  return butil::Status::OK();
}

// get table
butil::Status CoordinatorControl::GetTable(uint64_t schema_id, uint64_t table_id,
                                           pb::meta::TableDefinitionWithId& table_definition_with_id) {
  DINGO_LOG(INFO) << "GetTable in control schema_id=" << schema_id;

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (table_id <= 0) {
    DINGO_LOG(ERROR) << "ERRROR: table illegal, table_id=" << table_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "table_id illegal");
  }

  // validate schema_id
  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not valid");
  }

  // validate table_id & get table definition
  {
    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    pb::coordinator_internal::TableInternal table_internal;
    int ret = table_map_.Get(table_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found");
    }

    DINGO_LOG(INFO) << "GetTable found table_id=" << table_id;

    // table_def_with_id.mutable_table_id()->CopyFrom(schema_internal.schema().table_ids(i));
    auto* table_id_for_response = table_definition_with_id.mutable_table_id();
    table_id_for_response->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
    table_id_for_response->set_entity_id(table_id);
    table_id_for_response->set_parent_entity_id(schema_id);

    table_definition_with_id.mutable_table_definition()->CopyFrom(table_internal.definition());
  }

  DINGO_LOG(DEBUG) << fmt::format("GetTable schema_id={} table_id={} table_definition_with_id={}", schema_id, table_id,
                                  table_definition_with_id.DebugString());

  return butil::Status::OK();
}

// get index
butil::Status CoordinatorControl::GetIndex(uint64_t schema_id, uint64_t index_id, bool check_compatibility,
                                           pb::meta::TableDefinitionWithId& table_definition_with_id) {
  DINGO_LOG(INFO) << "GetIndex in control schema_id=" << schema_id;

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (index_id <= 0) {
    DINGO_LOG(ERROR) << "ERRROR: index illegal, index_id=" << index_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "index_id illegal");
  }

  // validate schema_id
  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not valid");
  }

  // validate index_id & get index definition
  {
    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    pb::coordinator_internal::TableInternal table_internal;
    int ret = index_map_.Get(index_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: index_id not found" << index_id;
      return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index_id not found");
    }

    // if (check_compatibility && table_internal.table_id() > 0) {
    //   return butil::Status(pb::error::Errno::EINDEX_COMPATIBILITY, "cannot get index created by new interface.");
    // }

    DINGO_LOG(INFO) << "GetIndex found index_id=" << index_id;

    auto* index_id_for_response = table_definition_with_id.mutable_table_id();
    index_id_for_response->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
    index_id_for_response->set_entity_id(index_id);
    index_id_for_response->set_parent_entity_id(schema_id);

    table_definition_with_id.mutable_table_definition()->CopyFrom(table_internal.definition());
  }

  DINGO_LOG(DEBUG) << fmt::format("GetIndex schema_id={} index_id={} table_definition_with_id={}", schema_id, index_id,
                                  table_definition_with_id.DebugString());

  return butil::Status::OK();
}

// get table by name
butil::Status CoordinatorControl::GetTableByName(uint64_t schema_id, const std::string& table_name,
                                                 pb::meta::TableDefinitionWithId& table_definition) {
  DINGO_LOG(INFO) << fmt::format("GetTableByName in control schema_id={} table_name={}", schema_id, table_name);

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (table_name.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: table_name illegal " << table_name;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "table_name illegal");
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not valid");
  }

  uint64_t temp_table_id = 0;
  auto ret = table_name_map_safe_temp_.Get(std::to_string(schema_id) + table_name, temp_table_id);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "WARNING: table_name not found " << table_name;
    return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_name not found");
  }

  DINGO_LOG(DEBUG) << fmt::format("GetTableByName schema_id={} table_name={} table_definition={}", schema_id,
                                  table_name, table_definition.DebugString());

  return GetTable(schema_id, temp_table_id, table_definition);
}

// get index by name
butil::Status CoordinatorControl::GetIndexByName(uint64_t schema_id, const std::string& index_name,
                                                 pb::meta::TableDefinitionWithId& table_definition) {
  DINGO_LOG(INFO) << fmt::format("GetIndexByName in control schema_id={} index_name={}", schema_id, index_name);

  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (index_name.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: index_name illegal " << index_name;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "index_name illegal");
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not valid");
  }

  uint64_t temp_index_id = 0;
  auto ret = index_name_map_safe_temp_.Get(std::to_string(schema_id) + index_name, temp_index_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: index_name not found " << index_name;
    return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index_name not found");
  }

  DINGO_LOG(DEBUG) << fmt::format("GetIndexByName schema_id={} index_name={} table_definition={}", schema_id,
                                  index_name, table_definition.DebugString());

  return GetIndex(schema_id, temp_index_id, true, table_definition);
}

// get table range
butil::Status CoordinatorControl::GetTableRange(uint64_t schema_id, uint64_t table_id,
                                                pb::meta::TableRange& table_range) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  pb::coordinator_internal::TableInternal table_internal;
  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
  }
  {
    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    int ret = table_map_.Get(table_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found");
    }
  }

  for (int i = 0; i < table_internal.partitions_size(); i++) {
    auto* range_distribution = table_range.add_range_distribution();

    // range_distribution id
    uint64_t region_id = table_internal.partitions(i).region_id();
    uint64_t part_id = table_internal.partitions(i).part_id();

    auto* common_id_region = range_distribution->mutable_id();
    common_id_region->set_entity_id(region_id);
    common_id_region->set_parent_entity_id(part_id);
    common_id_region->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);

    // get region
    pb::common::Region part_region;
    int ret = region_map_.Get(region_id, part_region);
    if (ret < 0) {
      DINGO_LOG(ERROR) << fmt::format("ERROR cannot find region in regionmap_ while GetTable, table_id={} region_id={}",
                                      table_id, region_id);
      continue;
    }

    // range_distribution range
    auto* part_range = range_distribution->mutable_range();
    // part_range->CopyFrom(table_internal.partitions(i).range());
    part_range->CopyFrom(part_region.definition().range());

    // range_distribution leader location
    auto* leader_location = range_distribution->mutable_leader();

    // range_distribution voter & learner locations
    for (int j = 0; j < part_region.definition().peers_size(); j++) {
      const auto& part_peer = part_region.definition().peers(j);
      if (part_peer.store_id() == part_region.leader_store_id()) {
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

  return butil::Status::OK();
}

// get index range
butil::Status CoordinatorControl::GetIndexRange(uint64_t schema_id, uint64_t index_id,
                                                pb::meta::IndexRange& index_range) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  pb::coordinator_internal::TableInternal table_internal;
  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
  }
  {
    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    int ret = index_map_.Get(index_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: index_id not found" << index_id;
      return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index_id not found");
    }
  }

  for (int i = 0; i < table_internal.partitions_size(); i++) {
    auto* range_distribution = index_range.add_range_distribution();

    // range_distribution id
    uint64_t region_id = table_internal.partitions(i).region_id();
    uint64_t part_id = table_internal.partitions(i).part_id();

    auto* common_id_region = range_distribution->mutable_id();
    common_id_region->set_entity_id(region_id);
    common_id_region->set_parent_entity_id(part_id);
    common_id_region->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);

    // get region
    pb::common::Region part_region;
    int ret = region_map_.Get(region_id, part_region);
    if (ret < 0) {
      DINGO_LOG(ERROR) << fmt::format("ERROR cannot find region in regionmap_ while GetIndex, index_id={} region_id={}",
                                      index_id, region_id);
      continue;
    }

    // range_distribution range
    auto* part_range = range_distribution->mutable_range();
    // part_range->CopyFrom(table_internal.partitions(i).range());
    part_range->CopyFrom(part_region.definition().range());

    // range_distribution leader location
    auto* leader_location = range_distribution->mutable_leader();

    // range_distribution voter & learner locations
    for (int j = 0; j < part_region.definition().peers_size(); j++) {
      const auto& part_peer = part_region.definition().peers(j);
      if (part_peer.store_id() == part_region.leader_store_id()) {
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

  return butil::Status::OK();
}

// get table metrics
butil::Status CoordinatorControl::GetTableMetrics(uint64_t schema_id, uint64_t table_id,
                                                  pb::meta::TableMetricsWithId& table_metrics) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (table_metrics.id().entity_id() != 0) {
    DINGO_LOG(ERROR) << "ERRROR: table is not empty , table_id=" << table_metrics.id().entity_id();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "table is not empty");
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
  }

  {
    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (!table_map_.Exists(table_id)) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found");
    }
  }

  pb::coordinator_internal::TableMetricsInternal table_metrics_internal;
  {
    // BAIDU_SCOPED_LOCK(table_metrics_map_mutex_);
    int ret = table_metrics_map_.Get(table_id, table_metrics_internal);

    // auto* temp_table_metrics = table_metrics_map_.seek(table_id);
    if (ret < 0) {
      DINGO_LOG(INFO) << "table_metrics not found, try to calculate new one" << table_id;

      // calculate table metrics using region metrics
      auto* table_metrics_single = table_metrics_internal.mutable_table_metrics();
      if (CalculateTableMetricsSingle(table_id, *table_metrics_single) < 0) {
        DINGO_LOG(ERROR) << "ERRROR: CalculateTableMetricsSingle failed" << table_id;
        return butil::Status(pb::error::Errno::ETABLE_METRICS_FAILED, "CalculateTableMetricsSingle failed");
      } else {
        table_metrics_internal.set_id(table_id);
        table_metrics_internal.mutable_table_metrics()->CopyFrom(*table_metrics_single);
        // table_metrics_map_[table_id] = table_metrics_internal;
        // temp_table_metrics->CopyFrom(table_metrics_internal);
        table_metrics_map_.Put(table_id, table_metrics_internal);

        DINGO_LOG(INFO) << fmt::format(
            "table_metrics first calculated, table_id={} row_count={} min_key={} max_key={} part_count={}", table_id,
            table_metrics_single->rows_count(), table_metrics_single->min_key(), table_metrics_single->max_key(),
            table_metrics_single->part_count());
      }
    } else {
      // construct TableMetrics from table_metrics_internal
      DINGO_LOG(DEBUG) << "table_metrics found, return metrics in map" << table_id;
      // table_metrics_internal = *temp_table_metrics;
    }
  }

  // construct TableMetricsWithId
  auto* common_id_table = table_metrics.mutable_id();
  common_id_table->set_entity_id(table_id);
  common_id_table->set_parent_entity_id(schema_id);
  common_id_table->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  table_metrics.mutable_table_metrics()->CopyFrom(table_metrics_internal.table_metrics());

  return butil::Status::OK();
}

// get index metrics
butil::Status CoordinatorControl::GetIndexMetrics(uint64_t schema_id, uint64_t index_id,
                                                  pb::meta::IndexMetricsWithId& index_metrics) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  if (index_metrics.id().entity_id() != 0) {
    DINGO_LOG(ERROR) << "ERRROR: index is not empty , index_id=" << index_metrics.id().entity_id();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "index is not empty");
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
  }

  {
    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    if (!index_map_.Exists(index_id)) {
      DINGO_LOG(ERROR) << "ERRROR: index_id not found" << index_id;
      return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index_id not found");
    }
  }

  pb::coordinator_internal::IndexMetricsInternal index_metrics_internal;
  {
    // BAIDU_SCOPED_LOCK(index_metrics_map_mutex_);
    int ret = index_metrics_map_.Get(index_id, index_metrics_internal);

    // auto* temp_index_metrics = index_metrics_map_.seek(index_id);
    if (ret < 0) {
      DINGO_LOG(INFO) << "index_metrics not found, try to calculate new one" << index_id;

      // calculate index metrics using region metrics
      auto* index_metrics_single = index_metrics_internal.mutable_index_metrics();
      if (CalculateIndexMetricsSingle(index_id, *index_metrics_single) < 0) {
        DINGO_LOG(ERROR) << "ERRROR: CalculateIndexMetricsSingle failed" << index_id;
        return butil::Status(pb::error::Errno::EINDEX_METRICS_FAILED, "CalculateIndexMetricsSingle failed");
      } else {
        index_metrics_internal.set_id(index_id);
        index_metrics_internal.mutable_index_metrics()->CopyFrom(*index_metrics_single);
        // index_metrics_map_[index_id] = index_metrics_internal;
        // temp_index_metrics->CopyFrom(index_metrics_internal);
        index_metrics_map_.Put(index_id, index_metrics_internal);

        DINGO_LOG(INFO) << fmt::format(
            "index_metrics first calculated, index_id={} row_count={} min_key={} max_key={} part_count={}", index_id,
            index_metrics_single->rows_count(), index_metrics_single->min_key(), index_metrics_single->max_key(),
            index_metrics_single->part_count());
      }
    } else {
      // construct IndexMetrics from index_metrics_internal
      DINGO_LOG(DEBUG) << "index_metrics found, return metrics in map" << index_id;
      // index_metrics_internal = *temp_index_metrics;
    }
  }

  // construct IndexMetricsWithId
  auto* common_id_index = index_metrics.mutable_id();
  common_id_index->set_entity_id(index_id);
  common_id_index->set_parent_entity_id(schema_id);
  common_id_index->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);

  index_metrics.mutable_index_metrics()->CopyFrom(index_metrics_internal.index_metrics());

  return butil::Status::OK();
}

// CalculateTableMetricsSingle
uint64_t CoordinatorControl::CalculateTableMetricsSingle(uint64_t table_id, pb::meta::TableMetrics& table_metrics) {
  pb::coordinator_internal::TableInternal table_internal;
  {
    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    int ret = table_map_.Get(table_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return -1;
    }
  }

  // build result metrics
  uint64_t row_count = 0;
  uint64_t table_size = 0;
  std::string min_key(10, '\x00');
  std::string max_key(10, '\xFF');

  for (int i = 0; i < table_internal.partitions_size(); i++) {
    // part id
    uint64_t region_id = table_internal.partitions(i).region_id();

    // get region
    pb::common::Region part_region;
    {
      // BAIDU_SCOPED_LOCK(region_map_mutex_);
      int ret = region_map_.Get(region_id, part_region);
      if (ret < 0) {
        DINGO_LOG(ERROR) << fmt::format(
            "ERROR cannot find region in regionmap_ while GetTable, table_id={} region_id={}", table_id, region_id);
        continue;
      }
    }

    if (!part_region.has_metrics()) {
      DINGO_LOG(ERROR) << fmt::format("ERROR region has no metrics, table_id={} region_id={}", table_id, region_id);
      continue;
    }

    const auto& region_metrics = part_region.metrics();
    row_count += region_metrics.row_count();
    table_size += region_metrics.region_size();

    if (min_key.empty()) {
      min_key = region_metrics.min_key();
    } else {
      if (min_key.compare(region_metrics.min_key()) > 0) {
        min_key = region_metrics.min_key();
      }
    }

    if (max_key.empty()) {
      max_key = region_metrics.max_key();
    } else {
      if (max_key.compare(region_metrics.max_key()) < 0) {
        max_key = region_metrics.max_key();
      }
    }
  }

  table_metrics.set_rows_count(row_count);
  table_metrics.set_table_size(table_size);
  table_metrics.set_min_key(min_key);
  table_metrics.set_max_key(max_key);
  table_metrics.set_part_count(table_internal.partitions_size());

  DINGO_LOG(DEBUG) << fmt::format(
      "table_metrics calculated in CalculateTableMetricsSingle, table_id={} row_count={} min_key={} max_key={} "
      "part_count={}",
      table_id, table_metrics.rows_count(), table_metrics.min_key(), table_metrics.max_key(),
      table_metrics.part_count());

  return 0;
}

// CalculateIndexMetricsSingle
uint64_t CoordinatorControl::CalculateIndexMetricsSingle(uint64_t index_id, pb::meta::IndexMetrics& index_metrics) {
  pb::coordinator_internal::TableInternal table_internal;
  {
    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    int ret = index_map_.Get(index_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: index_id not found" << index_id;
      return -1;
    }
  }

  // build result metrics
  uint64_t row_count = 0;
  std::string min_key(10, '\x00');
  std::string max_key(10, '\xFF');

  // about vector
  pb::common::VectorIndexType vector_index_type = pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE;
  int64_t current_count = 0;
  int64_t deleted_count = 0;
  int64_t max_id = -1;
  int64_t min_id = -1;
  int64_t memory_bytes = 0;

  for (int i = 0; i < table_internal.partitions_size(); i++) {
    // part id
    uint64_t region_id = table_internal.partitions(i).region_id();

    // get region
    pb::common::Region part_region;
    {
      // BAIDU_SCOPED_LOCK(region_map_mutex_);
      int ret = region_map_.Get(region_id, part_region);
      if (ret < 0) {
        DINGO_LOG(ERROR) << fmt::format(
            "ERROR cannot find region in regionmap_ while GetIndex, index_id={} region_id={}", index_id, region_id);
        continue;
      }
    }

    if (!part_region.has_metrics()) {
      DINGO_LOG(ERROR) << fmt::format("ERROR region has no metrics, index_id={} region_id={}", index_id, region_id);
      continue;
    }

    const auto& region_metrics = part_region.metrics();
    row_count += region_metrics.row_count();

    if (min_key.empty()) {
      min_key = region_metrics.min_key();
    } else {
      if (min_key.compare(region_metrics.min_key()) > 0) {
        min_key = region_metrics.min_key();
      }
    }

    if (max_key.empty()) {
      max_key = region_metrics.max_key();
    } else {
      if (max_key.compare(region_metrics.max_key()) < 0) {
        max_key = region_metrics.max_key();
      }
    }

    if (region_metrics.has_vector_index_metrics()) {
      const auto& vector_index_metrics = region_metrics.vector_index_metrics();

      pb::common::VectorIndexType internal_vector_index_type = vector_index_metrics.vector_index_type();
      int64_t internal_current_count = vector_index_metrics.current_count();
      int64_t internal_deleted_count = vector_index_metrics.deleted_count();
      int64_t internal_max_id = vector_index_metrics.max_id();
      int64_t internal_min_id = vector_index_metrics.min_id();
      int64_t internal_memory_bytes = vector_index_metrics.memory_bytes();

      vector_index_type = internal_vector_index_type;
      current_count += internal_current_count;
      deleted_count += internal_deleted_count;

      if (max_id < 0) {
        max_id = internal_max_id;
      } else {
        max_id = std::max(max_id, internal_max_id);
      }

      if (min_id < 0) {
        min_id = internal_min_id;
      } else {
        min_id = std::min(min_id, internal_min_id);
      }

      memory_bytes += internal_memory_bytes;
    }
  }

  index_metrics.set_rows_count(row_count);
  index_metrics.set_min_key(min_key);
  index_metrics.set_max_key(max_key);
  index_metrics.set_part_count(table_internal.partitions_size());

  // about vector
  if (vector_index_type != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE) {
    index_metrics.set_vector_index_type(vector_index_type);
    index_metrics.set_current_count(current_count);
    index_metrics.set_deleted_count(deleted_count);
    index_metrics.set_max_id(max_id);
    index_metrics.set_min_id(min_id);
    index_metrics.set_memory_bytes(memory_bytes);
  }

  if (index_metrics.vector_index_type() != pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE) {
    DINGO_LOG(DEBUG) << fmt::format(
        "index_metrics calculated in CalculateIndexMetricsSingle, index_id={} row_count={} min_key={} max_key={} "
        "part_count={} vector_index_type={} current_count={} deleted_count={} max_id={} min_id={} memory_bytes={}",
        index_id, index_metrics.rows_count(), index_metrics.min_key(), index_metrics.max_key(),
        index_metrics.part_count(), static_cast<int>(index_metrics.vector_index_type()), index_metrics.current_count(),
        index_metrics.deleted_count(), index_metrics.max_id(), index_metrics.min_id(), index_metrics.memory_bytes());
  } else {
    DINGO_LOG(DEBUG) << fmt::format(
        "index_metrics calculated in CalculateIndexMetricsSingle, index_id={} row_count={} min_key={} max_key={} "
        "part_count={}",
        index_id, index_metrics.rows_count(), index_metrics.min_key(), index_metrics.max_key(),
        index_metrics.part_count());
  }

  return 0;
}

// CalculateTableMetrics
// calculate table metrics using region metrics
// only recalculate when table_metrics_map_ does contain table_id
// if the table_id is not in table_map_, remove it from table_metrics_map_
void CoordinatorControl::CalculateTableMetrics() {
  // BAIDU_SCOPED_LOCK(table_metrics_map_mutex_);

  butil::FlatMap<uint64_t, pb::coordinator_internal::TableMetricsInternal> temp_table_metrics_map;
  temp_table_metrics_map.init(10000);
  table_metrics_map_.GetRawMapCopy(temp_table_metrics_map);

  for (auto& table_metrics_internal : temp_table_metrics_map) {
    uint64_t table_id = table_metrics_internal.first;
    pb::meta::TableMetrics table_metrics;
    if (CalculateTableMetricsSingle(table_id, table_metrics) < 0) {
      DINGO_LOG(ERROR) << "ERRROR: CalculateTableMetricsSingle failed, remove metrics from map" << table_id;
      table_metrics_map_.Erase(table_id);

      // mbvar table
      coordinator_bvar_metrics_table_.DeleteTableBvar(table_id);

    } else {
      table_metrics_internal.second.mutable_table_metrics()->CopyFrom(table_metrics);

      // update table_metrics_map_ in memory
      table_metrics_map_.PutIfExists(table_id, table_metrics_internal.second);

      // mbvar table
      coordinator_bvar_metrics_table_.UpdateTableBvar(table_id, table_metrics.rows_count(), table_metrics.part_count());
    }
  }
}

// CalculateIndexMetrics
// calculate index metrics using region metrics
// only recalculate when index_metrics_map_ does contain index_id
// if the index_id is not in index_map_, remove it from index_metrics_map_
void CoordinatorControl::CalculateIndexMetrics() {
  // BAIDU_SCOPED_LOCK(index_metrics_map_mutex_);

  butil::FlatMap<uint64_t, pb::coordinator_internal::IndexMetricsInternal> temp_index_metrics_map;
  temp_index_metrics_map.init(10000);
  index_metrics_map_.GetRawMapCopy(temp_index_metrics_map);

  for (auto& index_metrics_internal : temp_index_metrics_map) {
    uint64_t index_id = index_metrics_internal.first;
    pb::meta::IndexMetrics index_metrics;
    if (CalculateIndexMetricsSingle(index_id, index_metrics) < 0) {
      DINGO_LOG(ERROR) << "ERRROR: CalculateIndexMetricsSingle failed, remove metrics from map" << index_id;
      index_metrics_map_.Erase(index_id);

      // mbvar index
      coordinator_bvar_metrics_index_.DeleteIndexBvar(index_id);

    } else {
      index_metrics_internal.second.mutable_index_metrics()->CopyFrom(index_metrics);

      // update index_metrics_map_ in memory
      index_metrics_map_.PutIfExists(index_id, index_metrics_internal.second);

      // mbvar index
      coordinator_bvar_metrics_index_.UpdateIndexBvar(index_id, index_metrics.rows_count(), index_metrics.part_count());
    }
  }
}

butil::Status CoordinatorControl::GenerateTableIds(uint64_t schema_id, const pb::meta::TableWithPartCount& count,
                                                   pb::coordinator_internal::MetaIncrement& meta_increment,
                                                   pb::meta::GenerateTableIdsResponse* response) {
  if (count.index_count() != count.index_part_count_size()) {
    DINGO_LOG(ERROR) << "index count is illegal: " << count.index_count() << " | " << count.index_part_count_size();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "index count is illegal");
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  if (count.has_table()) {
    GenerateTableIdAndPartIds(schema_id, count.table_part_count(), pb::meta::EntityType::ENTITY_TYPE_TABLE,
                              meta_increment, response->add_ids());
  }

  for (uint32_t i = 0; i < count.index_count(); i++) {
    GenerateTableIdAndPartIds(schema_id, count.index_part_count(i), pb::meta::EntityType::ENTITY_TYPE_INDEX,
                              meta_increment, response->add_ids());
  }
  return butil::Status::OK();
}

void CoordinatorControl::CreateTableIndexesMap(pb::coordinator_internal::TableIndexInternal& table_index_internal,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  // update meta_increment
  auto* table_index_increment = meta_increment.add_table_indexes();
  table_index_increment->set_id(table_index_internal.id());
  table_index_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::CREATE);
  table_index_increment->mutable_table_indexes()->CopyFrom(table_index_internal);
}

butil::Status CoordinatorControl::GetTableIndexes(uint64_t schema_id, uint64_t table_id,
                                                  pb::meta::GetTablesResponse* response) {
  pb::meta::TableDefinitionWithId definition_with_id;
  butil::Status ret = GetTable(schema_id, table_id, definition_with_id);
  if (!ret.ok()) {
    return ret;
  }

  response->add_table_definition_with_ids()->CopyFrom(definition_with_id);

  pb::coordinator_internal::TableIndexInternal table_index_internal;
  int result = table_index_map_.Get(table_id, table_index_internal);
  if (result >= 0) {
    // found
    for (const auto& temp_id : table_index_internal.table_ids()) {
      pb::meta::TableDefinitionWithId temp_definition_with_id;
      ret = GetIndex(schema_id, temp_id.entity_id(), false, temp_definition_with_id);
      if (!ret.ok()) {
        return ret;
      }

      response->add_table_definition_with_ids()->CopyFrom(temp_definition_with_id);
    }
  } else {
    // not found
    DINGO_LOG(INFO) << "cannot find indexes, schema_id: " << schema_id << ", table_id: " << table_id;
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DropTableIndexes(uint64_t schema_id, uint64_t table_id,
                                                   pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not valid" << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id not valid");
  }

  // drop indexes of the table
  pb::coordinator_internal::TableIndexInternal table_index_internal;
  butil::Status ret;
  int result = table_index_map_.Get(table_id, table_index_internal);
  if (result >= 0) {
    // find in map
    for (const auto& temp_id : table_index_internal.table_ids()) {
      ret = DropIndex(schema_id, temp_id.entity_id(), false, meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "error while dropping index, schema_id: " << schema_id << ", table_id: " << table_id;
        return ret;
      }
    }

    // delete table indexes relationship map
    auto* table_index_increment = meta_increment.add_table_indexes();
    table_index_increment->set_id(table_id);
    table_index_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::DELETE);
  } else {
    // not find in map
    DINGO_LOG(INFO) << "cannot find indexes, schema_id: " << schema_id << ", table_id: " << table_id;
  }

  // drop table finally
  ret = DropTable(schema_id, table_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "error while dropping index, schema_id: " << schema_id << ", table_id: " << table_id;
    return ret;
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::RemoveTableIndex(uint64_t table_id, uint64_t index_id,
                                                   pb::coordinator_internal::MetaIncrement& meta_increment) {
  butil::Status ret = DropIndex(table_id, index_id, false, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "error while dropping index, table_id: " << table_id << ", index_id: " << index_id;
    return ret;
  }

  pb::coordinator_internal::TableIndexInternal table_index_internal;
  int result = table_index_map_.Get(table_id, table_index_internal);
  if (result < 0) {
    DINGO_LOG(WARNING) << "cannot find table_id in table index map: " << table_id << ", index_id: " << index_id;
    return butil::Status::OK();
  }

  size_t source_size = table_index_internal.table_ids_size();
  bool found_index = false;
  for (size_t i = 0; i < table_index_internal.table_ids_size(); i++) {
    if (table_index_internal.table_ids(i).entity_id() == index_id) {
      found_index = true;
      table_index_internal.mutable_table_ids()->DeleteSubrange(i, 1);
      break;
    }
  }

  if (found_index) {
    DINGO_LOG(INFO) << "remove success, table_id: " << table_id << ", index_id: " << index_id
                    << ", size: " << source_size << " --> " << table_index_internal.table_ids_size();

    auto* table_index_increment = meta_increment.add_table_indexes();
    table_index_increment->set_id(table_id);
    table_index_increment->mutable_table_indexes()->CopyFrom(table_index_internal);
    table_index_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  } else {
    DINGO_LOG(WARNING) << "cannot find index, table_id: " << table_id << ", index_id: " << index_id;
  }

  return butil::Status::OK();
}

// SwitchAutoSplit
// in: schema_id
// in: table_id
// in: auto_split
// out: meta_increment
butil::Status CoordinatorControl::SwitchAutoSplit(uint64_t schema_id, uint64_t table_id, bool auto_split,
                                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id illegal");
  }

  pb::coordinator_internal::TableInternal table_internal;
  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
    return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
  }
  {
    pb::coordinator_internal::TableInternal table_internal_tmp;
    int ret = table_map_.Get(table_id, table_internal_tmp);
    if (ret > 0) {
      table_internal = table_internal_tmp;
    } else {
      ret = index_map_.Get(table_id, table_internal_tmp);
      if (ret > 0) {
        table_internal = table_internal_tmp;
      } else {
        DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
        return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found");
      }
    }
  }

  for (int i = 0; i < table_internal.partitions_size(); i++) {
    uint64_t region_id = table_internal.partitions(i).region_id();

    // get region
    pb::common::Region part_region;
    int ret = region_map_.Get(region_id, part_region);
    if (ret < 0) {
      DINGO_LOG(ERROR) << fmt::format("ERROR cannot find region in regionmap_ while GetTable, table_id={} region_id={}",
                                      table_id, region_id);
      continue;
    }

    // send region_cmd to update auto_split
    auto* store_operation_increment = meta_increment.add_store_operations();
    store_operation_increment->set_id(part_region.leader_store_id());
    store_operation_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
    auto* increment_store_operation = store_operation_increment->mutable_store_operation();
    increment_store_operation->set_id(part_region.leader_store_id());
    auto* region_cmd = increment_store_operation->add_region_cmds();
    region_cmd->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
    region_cmd->set_region_id(region_id);
    region_cmd->set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_SWITCH_SPLIT);
    region_cmd->set_create_timestamp(butil::gettimeofday_ms());
    region_cmd->mutable_switch_split_request()->set_region_id(region_id);
    region_cmd->mutable_switch_split_request()->set_disable_split(!auto_split);
  }

  return butil::Status::OK();
}

}  // namespace dingodb

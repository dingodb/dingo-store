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
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/containers/flat_map.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "coordinator/auto_increment_control.h"
#include "coordinator/coordinator_control.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "server/server.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

DEFINE_int64(max_partition_num_of_table, 1024, "max partition num of table");
DEFINE_int64(max_table_count, 10000, "max table num of dingo");
DEFINE_int64(max_index_count, 10000, "max index num of dingo");
DEFINE_int64(max_tenant_count, 1024, "max tenant num of dingo");
DEFINE_uint32(default_replica_num, 3, "default replica number");

butil::Status CoordinatorControl::GenerateTableIdAndPartIds(int64_t schema_id, int64_t part_count,
                                                            pb::meta::EntityType entity_type,
                                                            pb::coordinator_internal::MetaIncrement& meta_increment,
                                                            pb::meta::TableIdWithPartIds* ids) {
  std::vector<int64_t> new_ids =
      GetNextIds(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, 1 + part_count, meta_increment);

  if (new_ids.empty()) {
    DINGO_LOG(ERROR) << "GenerateTableIdAndPartIds GetNextIds failed";
    return butil::Status(pb::error::Errno::EINTERNAL, "GenerateTableIdAndPartIds GetNextIds failed");
  }

  if (new_ids.size() != 1 + part_count) {
    DINGO_LOG(ERROR) << "GenerateTableIdAndPartIds GetNextIds failed, new_ids.size()=" << new_ids.size()
                     << " part_count=" << part_count;
    return butil::Status(pb::error::Errno::EINTERNAL,
                         "GenerateTableIdAndPartIds GetNextIds failed, new_ids.size() != 1 + part_count");
  }

  auto* table_id = ids->mutable_table_id();
  table_id->set_entity_id(new_ids.at(0));
  table_id->set_parent_entity_id(schema_id);
  table_id->set_entity_type(entity_type);

  for (uint32_t i = 0; i < part_count; i++) {
    auto* part_id = ids->add_part_ids();
    part_id->set_entity_id(new_ids.at(i + 1));
    part_id->set_parent_entity_id(new_ids.at(0));
    part_id->set_entity_type(pb::meta::EntityType::ENTITY_TYPE_PART);
  }

  return butil::Status::OK();
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
  root_schema_internal.set_name(Constant::kRootSchemaName);

  // meta schema
  // pb::meta::Schema meta_schema;
  meta_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::META_SCHEMA);
  meta_schema_internal.set_name(Constant::kMetaSchemaName);

  // dingo schema
  // pb::meta::Schema dingo_schema;
  dingo_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  dingo_schema_internal.set_name(Constant::kDingoSchemaName);

  // mysql schema
  // pb::mysql::Schema mysql_schema;
  mysql_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::MYSQL_SCHEMA);
  mysql_schema_internal.set_name(Constant::kMySQLSchemaName);

  // information schema
  // pb::information::Schema information_schema;
  information_schema_internal.set_id(::dingodb::pb::meta::ReservedSchemaIds::INFORMATION_SCHEMA);
  information_schema_internal.set_name(Constant::kInformationSchemaName);

  DINGO_LOG(INFO) << "GenerateRootSchemas 0[" << root_schema_internal.ShortDebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 1[" << meta_schema_internal.ShortDebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 2[" << dingo_schema_internal.ShortDebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 3[" << mysql_schema_internal.ShortDebugString();
  DINGO_LOG(INFO) << "GenerateRootSchemas 4[" << information_schema_internal.ShortDebugString();
}

// ValidateSchema
// check if schema_id is exist
// if exist return true
// else return false
bool CoordinatorControl::ValidateSchema(int64_t schema_id) {
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
// in: tenant_id
// in: schema_name
// out: new_schema_id
// out: meta_increment
// return OK if success
// return other if failed
butil::Status CoordinatorControl::CreateSchema(int64_t tenant_id, std::string schema_name, int64_t& new_schema_id,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_name.empty()) {
    DINGO_LOG(INFO) << " CreateSchema schema_name is illegal " << schema_name;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_name is empty");
  }

  // check if tenant_id is exists
  if (tenant_id < 0) {
    DINGO_LOG(INFO) << " CreateSchema tenant_id is illegal " << tenant_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id is illegal");
  }

  if (tenant_id > 0) {
    auto ret1 = tenant_map_.Exists(tenant_id);
    if (!ret1) {
      DINGO_LOG(INFO) << " CreateSchema tenant_id is not exist " << tenant_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id is not exist");
    }
  }

  // check if schema_name exists
  auto new_check_name = Helper::GenNewTenantCheckName(tenant_id, schema_name);
  int64_t value = 0;
  schema_name_map_safe_temp_.Get(new_check_name, value);
  if (value != 0) {
    std::string s = " CreateSchema schema_name is exist " + schema_name + ", tenant_id: " + std::to_string(tenant_id);
    DINGO_LOG(INFO) << s;
    return butil::Status(pb::error::Errno::ESCHEMA_EXISTS, s);
  }

  // create new schema id
  new_schema_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_SCHEMA, meta_increment);

  // update schema_name_map_safe_temp_
  if (schema_name_map_safe_temp_.PutIfAbsent(new_check_name, new_schema_id) < 0) {
    std::string s = " CreateSchema schema_name: " + schema_name +
                    " is exist, when insert new_schema_id=" + std::to_string(new_schema_id) +
                    ", tenant_id: " + std::to_string(tenant_id);

    return butil::Status(pb::error::Errno::ESCHEMA_EXISTS, "schema_name is exist");
  }

  // add new schema to  schema_map_
  pb::coordinator_internal::SchemaInternal new_schema_internal;
  new_schema_internal.set_id(new_schema_id);
  new_schema_internal.set_name(schema_name);
  new_schema_internal.set_tenant_id(tenant_id);

  // update meta_increment
  auto* schema_increment = meta_increment.add_schemas();
  schema_increment->set_id(new_schema_id);
  schema_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);

  auto* schema_increment_schema = schema_increment->mutable_schema_internal();
  *schema_increment_schema = new_schema_internal;

  // bump up schema map epoch
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_SCHEMA, meta_increment);

  // on_apply
  //  schema_map_.insert(std::make_pair(new_schema_id, new_schema));  // raft_kv_put

  return butil::Status::OK();
}

// DropSchema
// drop schema
// in: tenant_id
// in: schema_id
// return: 0 or -1
butil::Status CoordinatorControl::DropSchema(int64_t tenant_id, int64_t schema_id,
                                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (schema_id <= COORDINATOR_ID_OF_MAP_MIN) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  // check if tenant_id is exists
  if (tenant_id < 0) {
    DINGO_LOG(INFO) << " DropSchema tenant_id is illegal " << tenant_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id is illegal");
  }

  if (tenant_id > 0) {
    auto ret1 = tenant_map_.Exists(tenant_id);
    if (!ret1) {
      DINGO_LOG(INFO) << " DropSchema tenant_id is not exist " << tenant_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id is not exist");
    }
  }

  pb::coordinator_internal::SchemaInternal schema_internal_to_delete;
  {
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

  auto* schema_to_delete_schema = schema_to_delete->mutable_schema_internal();
  *schema_to_delete_schema = schema_internal_to_delete;

  // delete schema_name from schema_name_map_safe_temp_
  auto new_check_name = Helper::GenNewTenantCheckName(tenant_id, schema_internal_to_delete.name());
  schema_name_map_safe_temp_.Erase(new_check_name);

  return butil::Status::OK();
}

// GetSchemas
// get schemas
// in: tenant_id
// out: schemas
// if tenant_id is -1, means get all schemas
butil::Status CoordinatorControl::GetSchemas(int64_t tenant_id, std::vector<pb::meta::Schema>& schemas) {
  if (!schemas.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: vector schemas is not empty , size=" << schemas.size();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "vector schemas is not empty");
  }

  {
    butil::FlatMap<int64_t, pb::coordinator_internal::SchemaInternal> schema_map_copy;
    schema_map_copy.init(10000);
    int ret = schema_map_.GetRawMapCopy(schema_map_copy);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema_map_ GetRawMapCopy failed";
      return butil::Status(pb::error::Errno::EINTERNAL, "schema_map_ GetRawMapCopy failed");
    }

    for (const auto& [schema_id, schema_internal] : schema_map_copy) {
      // check tenant_id
      if (tenant_id >= 0 && schema_internal.tenant_id() != tenant_id) {
        continue;
      }

      // setup output schema
      pb::meta::Schema schema;

      auto* temp_id = schema.mutable_id();
      temp_id->set_entity_id(schema_id);
      temp_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);

      schema.set_name(schema_internal.name());
      schema.set_revision(schema_internal.revision());
      schema.set_tenant_id(schema_internal.tenant_id());

      // construct table_ids in schema
      for (auto it_table : schema_internal.table_ids()) {
        pb::meta::DingoCommonId table_id;
        table_id.set_entity_id(it_table);
        table_id.set_parent_entity_id(temp_id->entity_id());
        table_id.set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

        *(schema.add_table_ids()) = table_id;
      }

      // construct index_ids in schema
      for (auto it_index : schema_internal.index_ids()) {
        pb::meta::DingoCommonId index_id;
        index_id.set_entity_id(it_index);
        index_id.set_parent_entity_id(temp_id->entity_id());
        index_id.set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);

        *(schema.add_index_ids()) = index_id;
      }

      schemas.push_back(schema);
    }
  }

  DINGO_LOG(INFO) << "GetSchemas tenant_id=" << tenant_id << " sub schema count=" << schema_map_.Size();

  return butil::Status::OK();
}

// GetSchema
// in: schema_id
// out: schema
butil::Status CoordinatorControl::GetSchema(int64_t schema_id, pb::meta::Schema& schema) {
  // only root schema can has sub schemas
  if (schema_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  {
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
    schema.set_revision(temp_schema.revision());
    schema.set_tenant_id(temp_schema.tenant_id());

    for (auto it : temp_schema.table_ids()) {
      pb::meta::DingoCommonId table_id;
      table_id.set_entity_id(it);
      table_id.set_parent_entity_id(schema_id);
      table_id.set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

      *(schema.add_table_ids()) = table_id;
    }

    for (auto it : temp_schema.index_ids()) {
      pb::meta::DingoCommonId table_id;
      table_id.set_entity_id(it);
      table_id.set_parent_entity_id(schema_id);
      table_id.set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);

      *(schema.add_index_ids()) = table_id;
    }
  }

  DINGO_LOG(INFO) << "GetSchema id=" << schema_id << ", sub table count=" << schema.table_ids_size()
                  << ", sub index count=" << schema.index_ids_size();

  return butil::Status::OK();
}

// GetSchemaByName
// in: schema_name
// out: schema
butil::Status CoordinatorControl::GetSchemaByName(int64_t tenant_id, const std::string& schema_name,
                                                  pb::meta::Schema& schema) {
  if (schema_name.empty()) {
    DINGO_LOG(ERROR) << "ERRROR: schema_name illegal " << schema_name;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_name illegal");
  }

  int64_t temp_schema_id = 0;
  auto new_check_name = Helper::GenNewTenantCheckName(tenant_id, schema_name);
  auto ret = schema_name_map_safe_temp_.Get(new_check_name, temp_schema_id);
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
butil::Status CoordinatorControl::CreateTableId(int64_t schema_id, int64_t& new_table_id,
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

butil::Status CoordinatorControl::CreateTableIds(int64_t schema_id, int64_t count, std::vector<int64_t>& new_table_ids,
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

  if (count <= 0) {
    DINGO_LOG(ERROR) << "count is illegal " << count;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "count is illegal");
  }

  // create table id
  new_table_ids = GetNextIds(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, count, meta_increment);
  if (new_table_ids.empty()) {
    DINGO_LOG(ERROR) << "CreateTableIds GetNextIds failed";
    return butil::Status(pb::error::Errno::EINTERNAL, "CreateTableIds GetNextIds failed");
  }

  DINGO_LOG(INFO) << "CreateTableIds new_table_ids req_count=" << count << ", get_count: " << new_table_ids.size();

  return butil::Status::OK();
}

// RollbackCreateTable
butil::Status CoordinatorControl::RollbackCreateTable(int64_t schema_id, const std::string& table_name) {
  // check if table_name exists
  std::string new_table_check_name = Helper::GenNewTableCheckName(schema_id, table_name);
  table_name_map_safe_temp_.Erase(new_table_check_name);

  return butil::Status::OK();
}

// RollbackCreateIndex
butil::Status CoordinatorControl::RollbackCreateIndex(int64_t schema_id, const std::string& table_name) {
  // check if table_name exists
  std::string new_table_check_name = Helper::GenNewTableCheckName(schema_id, table_name);
  index_name_map_safe_temp_.Erase(new_table_check_name);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::ValidateMaxTableCount() {
  auto table_count = table_map_.Size();
  if (table_count > FLAGS_max_table_count) {
    std::string err_str =
        fmt::format("table count exceed limit, table_count={}, max_table_count={}", table_count, FLAGS_max_table_count);
    DINGO_LOG(ERROR) << err_str;
    return butil::Status(pb::error::Errno::ETABLE_COUNT_EXCEED_LIMIT, err_str);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::ValidateMaxIndexCount() {
  auto index_count = index_map_.Size();
  if (index_count > FLAGS_max_index_count) {
    std::string err_str =
        fmt::format("index count exceed limit, index_count={}, max_index_count={}", index_count, FLAGS_max_index_count);
    DINGO_LOG(ERROR) << err_str;
    return butil::Status(pb::error::Errno::EINDEX_COUNT_EXCEED_LIMIT, err_str);
  }

  return butil::Status::OK();
}

// CreateTable
// in: schema_id, table_definition
// out: new_table_id, new_regin_ids meta_increment
// return: 0 success, -1 failed
butil::Status CoordinatorControl::CreateTable(int64_t schema_id, const pb::meta::TableDefinition& table_definition,
                                              int64_t& new_table_id, std::vector<int64_t>& region_ids,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  // check max table limit
  auto ret1 = ValidateMaxTableCount();
  if (!ret1.ok()) {
    DINGO_LOG(ERROR) << "create table exceed max table limit, table_definition:" << table_definition.ShortDebugString();
    return ret1;
  }

  // validate schema
  // root schema cannot create table
  if (schema_id < 0 || schema_id == ::dingodb::pb::meta::ROOT_SCHEMA) {
    DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id
                     << ", table_definition:" << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  int64_t tenant_id = 0;
  {
    pb::coordinator_internal::SchemaInternal schema_internal;
    auto ret = schema_map_.Get(schema_id, schema_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id
                       << ", table_definition:" << table_definition.ShortDebugString();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
    }

    tenant_id = schema_internal.tenant_id();
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

  DINGO_LOG(INFO) << "CreateTable table_definition:" << table_definition.ShortDebugString();

  // check max_partition_num_of_table
  if (table_partition.partitions_size() > FLAGS_max_partition_num_of_table) {
    DINGO_LOG(ERROR) << "partitions_size is too large, partitions_size=" << table_partition.partitions_size()
                     << ", max_partition_num_of_table=" << FLAGS_max_partition_num_of_table
                     << ", table_definition:" << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "partitions_size is too large");
  }

  // store new_part_id for next usage
  std::vector<int64_t> new_part_ids;
  std::vector<pb::common::Range> new_part_ranges;

  pb::common::Range table_internal_range;
  auto ret3 = CalcTableInternalRange(table_partition, table_internal_range);
  if (!ret3.ok()) {
    DINGO_LOG(ERROR) << "CalcTableInternalRange error:" << ret3.error_str()
                     << ", table_definition:" << table_definition.ShortDebugString();
    return ret3;
  }

  if (table_partition.partitions_size() > 0) {
    for (const auto& part : table_partition.partitions()) {
      if (part.id().entity_id() <= 0) {
        DINGO_LOG(ERROR) << "part_id is illegal, part_id=" << part.id().entity_id()
                         << ", table_definition:" << table_definition.ShortDebugString();
        return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is illegal");
      }
      if (part.range().start_key().empty() || part.range().end_key().empty()) {
        DINGO_LOG(ERROR) << "part range is illegal, part_id=" << part.id().entity_id()
                         << ", table_definition:" << table_definition.ShortDebugString();
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
                     << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "new_part_ranges.size() != new_part_ids.size()");
  }

  // check if part_id is legal
  std::set<int64_t> part_id_set;
  for (auto id : new_part_ids) {
    auto ret = part_id_set.insert(id);
    if (!ret.second) {
      DINGO_LOG(ERROR) << "part_id is illegal, part_id=" << id
                       << ", table_definition:" << table_definition.ShortDebugString();
      return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is illegal");
    }
  }

  // check if part_id is continuous
  if (!Helper::IsContinuous(part_id_set)) {
    DINGO_LOG(ERROR) << "part_id is not continuous, table_definition:" << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is not continuous");
  }

  // check if table_name exists
  std::string new_table_check_name = Helper::GenNewTableCheckName(schema_id, table_definition.name());

  int64_t value = 0;
  table_name_map_safe_temp_.Get(new_table_check_name, value);
  if (value != 0) {
    DINGO_LOG(INFO) << " Createtable table_name is exist " << table_definition.name();
    return butil::Status(pb::error::Errno::ETABLE_EXISTS,
                         fmt::format("table_name[{}] is exist in get", table_definition.name().c_str()));
  }

  // update table_name_map_safe_temp_
  if (table_name_map_safe_temp_.PutIfAbsent(new_table_check_name, new_table_id) < 0) {
    DINGO_LOG(INFO) << " CreateTable table_name" << table_definition.name()
                    << " is exist, when insert new_table_id=" << new_table_id;
    return butil::Status(pb::error::Errno::ETABLE_EXISTS,
                         fmt::format("table_name[{}] is exist in put if absent", table_definition.name().c_str()));
  }

  std::atomic<bool> table_create_success(false);
  ON_SCOPE_EXIT([&]() {
    if (table_create_success.load() == false) {
      table_name_map_safe_temp_.Erase(new_table_check_name);
    }
  });

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
    DINGO_LOG(INFO) << "CreateTable AutoIncrement send create auto increment internal success, id: " << new_table_id << ", table_name: " << table_definition.name();
  }

  // create table
  // extract part info, create region for each part

  std::vector<int64_t> new_region_ids;
  int32_t replica = table_definition.replica();
  if (replica < 1) {
    replica = FLAGS_default_replica_num;
  }

  pb::common::RawEngine region_raw_engine_type;
  auto ret2 = TranslateEngineToRawEngine(table_definition.engine(), region_raw_engine_type);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "TranslateEngineToRawEngine error:" << ret2.error_str()
                     << ", table_definition:" << table_definition.ShortDebugString();
    return ret2;
  }

  // this is just a null parameter
  pb::common::IndexParameter index_parameter;

  std::vector<int64_t> store_ids;
  auto ret4 = GetCreateRegionStoreIds(pb::common::RegionType::STORE_REGION, region_raw_engine_type, "", replica,
                                      index_parameter, store_ids);
  if (!ret4.ok()) {
    DINGO_LOG(ERROR) << "GetCreateRegionStoreIds error:" << ret4.error_str()
                     << ", table_definition:" << table_definition.ShortDebugString();
    return ret4;
  }

  // for partitions
  for (int i = 0; i < new_part_ranges.size(); i++) {
    int64_t new_region_id = 0;
    int64_t new_part_id = new_part_ids[i];
    auto new_part_range = new_part_ranges[i];

    std::string const region_name = std::string("T_") + std::to_string(schema_id) + std::string("_") +
                                    table_definition.name() + std::string("_part_") + std::to_string(new_part_id);

    std::vector<pb::coordinator::StoreOperation> store_operations;
    auto ret = CreateRegionFinal(region_name, pb::common::RegionType::STORE_REGION, region_raw_engine_type, "", replica,
                                 new_part_range, schema_id, new_table_id, 0, new_part_id, tenant_id, index_parameter,
                                 store_ids, 0, new_region_id, store_operations, meta_increment);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "CreateRegion failed in CreateTable table_name=" << table_definition.name()
                       << ", table_definition:" << table_definition.ShortDebugString() << " ret: " << ret.error_str();
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

    return butil::Status(pb::error::Errno::ETABLE_REGION_CREATE_FAILED, "Not enough regions is created");
  }

  // bumper up EPOCH_REGION
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);

  // create table_internal, set id & table_definition
  pb::coordinator_internal::TableInternal table_internal;
  table_internal.set_id(new_table_id);
  table_internal.set_schema_id(schema_id);
  table_internal.set_table_id(new_table_id);
  *table_internal.mutable_range() = table_internal_range;
  auto* definition = table_internal.mutable_definition();
  *definition = table_definition;
  definition->set_create_timestamp(butil::gettimeofday_ms());

  // add table_internal to table_map_
  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_TABLE, meta_increment);
  auto* table_increment = meta_increment.add_tables();
  table_increment->set_id(new_table_id);
  table_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  // table_increment->set_schema_id(schema_id);

  auto* table_increment_table = table_increment->mutable_table();
  *table_increment_table = table_internal;

  table_create_success.store(true);

  // return region_ids
  for (const auto& regin_id : new_region_ids) {
    region_ids.push_back(regin_id);
  }

  return butil::Status::OK();
}

// DropTable
// in: schema_id, table_id
// out: meta_increment
// return: errno
butil::Status CoordinatorControl::DropTable(int64_t schema_id, int64_t table_id,
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

    if (table_internal.schema_id() != schema_id) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not belong to schema_id" << table_id << " schema_id=" << schema_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "table_id not belong to schema_id");
    }
  }

  // call DropRegion
  // for (int i = 0; i < table_internal.partitions_size(); i++) {
  //   // part id
  //   int64_t region_id = table_internal.partitions(i).region_id();

  //   DropRegion(region_id, meta_increment);
  // }

  // delete table
  {
    auto* table_to_delete = meta_increment.add_tables();
    table_to_delete->set_id(table_id);
    table_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
    // table_to_delete->set_schema_id(schema_id);

    auto* table_to_delete_table = table_to_delete->mutable_table();
    table_to_delete_table->set_id(table_id);
    table_to_delete_table->set_schema_id(schema_id);
  }

  // add deleted_table
  {
    auto* table_to_delete = meta_increment.add_deleted_tables();
    table_to_delete->set_id(table_id);
    table_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
    // table_to_delete->set_schema_id(schema_id);

    auto* table_to_delete_table = table_to_delete->mutable_table();
    *table_to_delete_table = table_internal;
    table_to_delete_table->mutable_definition()->set_delete_timestamp(butil::gettimeofday_ms());
  }

  // bump up table map epoch
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_TABLE, meta_increment);

  // delete table_name from table_name_safe_map_temp_
  std::string new_table_check_name = Helper::GenNewTableCheckName(schema_id, table_internal.definition().name());
  table_name_map_safe_temp_.Erase(new_table_check_name);

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
butil::Status CoordinatorControl::CreateIndexId(int64_t schema_id, int64_t& new_index_id,
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

butil::Status CoordinatorControl::ValidateScalarIndexParameter(
    const pb::common::ScalarIndexParameter& scalar_index_parameter) {
  // check scalar index type
  if (scalar_index_parameter.scalar_index_type() == pb::common::ScalarIndexType::SCALAR_INDEX_TYPE_NONE) {
    DINGO_LOG(ERROR) << "scalar_index_type is illegal " << scalar_index_parameter.scalar_index_type();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                         "scalar_index_type is illegal " + std::to_string(scalar_index_parameter.scalar_index_type()));
  }

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

    auto ret = VectorIndexUtils::ValidateVectorIndexParameter(vector_index_parameter);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "vector_index_parameter is illegal, error:" << ret.error_str();
      return ret;
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

    auto ret = ValidateScalarIndexParameter(scalar_index_parameter);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "scalar_index_parameter is illegal, error:" << ret.error_str();
      return ret;
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
// out: new_index_id, new_region_ids meta_increment
// return: 0 success, -1 failed
butil::Status CoordinatorControl::CreateIndex(int64_t schema_id, const pb::meta::TableDefinition& table_definition,
                                              int64_t table_id, int64_t& new_index_id, std::vector<int64_t>& region_ids,
                                              pb::coordinator_internal::MetaIncrement& meta_increment) {
  // check max index limit
  auto ret1 = ValidateMaxIndexCount();
  if (!ret1.ok()) {
    DINGO_LOG(ERROR) << "create index exceed max index limit, index_definition:" << table_definition.ShortDebugString();
    return ret1;
  }

  // validate schema
  // root schema cannot create index
  if (schema_id < 0 || schema_id == ::dingodb::pb::meta::ROOT_SCHEMA) {
    DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  int64_t tenant_id = 0;
  {
    pb::coordinator_internal::SchemaInternal schema_internal;
    auto ret = schema_map_.Get(schema_id, schema_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id
                       << ", table_definition:" << table_definition.ShortDebugString();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
    }

    tenant_id = schema_internal.tenant_id();
  }

  // validate index definition
  auto status = ValidateIndexDefinition(table_definition);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // validate part information
  if (!table_definition.has_table_partition()) {
    DINGO_LOG(ERROR) << "no table_partition provided , table_definition=" << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::EINDEX_DEFINITION_ILLEGAL, "no table_partition provided");
  }

  auto const& index_partition = table_definition.table_partition();

  DINGO_LOG(INFO) << "CreateIndex index_definition=" << table_definition.ShortDebugString();

  // check max_partition_num_of_table
  if (index_partition.partitions_size() > FLAGS_max_partition_num_of_table) {
    DINGO_LOG(ERROR) << "partitions_size is too large, partitions_size=" << index_partition.partitions_size()
                     << ", max_partition_num_of_table=" << FLAGS_max_partition_num_of_table
                     << ", table_definition:" << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "partitions_size is too large");
  }

  // store new_part_id for next usage
  std::vector<int64_t> new_part_ids;
  std::vector<pb::common::Range> new_part_ranges;

  pb::common::Range table_internal_range;
  auto ret3 = CalcTableInternalRange(index_partition, table_internal_range);
  if (!ret3.ok()) {
    DINGO_LOG(ERROR) << "CalcTableInternalRange error:" << ret3.error_str()
                     << ", table_definition:" << table_definition.ShortDebugString();
    return ret3;
  }

  if (index_partition.partitions_size() > 0) {
    for (const auto& part : index_partition.partitions()) {
      if (part.id().entity_id() <= 0) {
        DINGO_LOG(ERROR) << "part_id is illegal, part_id=" << part.id().entity_id()
                         << ", table_definition:" << table_definition.ShortDebugString();
        return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is illegal");
      }
      if (part.range().start_key().empty() || part.range().end_key().empty()) {
        DINGO_LOG(ERROR) << "part range is illegal, part_id=" << part.id().entity_id()
                         << ", table_definition:" << table_definition.ShortDebugString();
        return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part range is illegal");
      }
      new_part_ids.push_back(part.id().entity_id());
      new_part_ranges.push_back(part.range());
    }
  } else {
    DINGO_LOG(ERROR) << "no range provided , table_definition=" << table_definition.ShortDebugString();
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
  std::set<int64_t> part_id_set;
  for (auto id : new_part_ids) {
    auto ret = part_id_set.insert(id);
    if (!ret.second) {
      DINGO_LOG(ERROR) << "part_id is illegal, part_id=" << id
                       << ", table_definition:" << table_definition.ShortDebugString();
      return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is illegal");
    }
  }

  // check if part_id is continuous
  if (!Helper::IsContinuous(part_id_set)) {
    DINGO_LOG(ERROR) << "part_id is not continuous, table_definition:" << table_definition.ShortDebugString();
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part_id is not continuous");
  }

  // check if table_name exists
  std::string new_index_check_name = Helper::GenNewTableCheckName(schema_id, table_definition.name());

  // check if index_name exists
  int64_t value = 0;
  index_name_map_safe_temp_.Get(new_index_check_name, value);
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
                       << ", table_definition=" << table_definition.ShortDebugString();
      return butil::Status(pb::error::Errno::EAUTO_INCREMENT_WHILE_CREATING_TABLE,
                           fmt::format("send create auto increment internal error, code: {}, message: {}",
                                       status.error_code(), status.error_str()));
    }
    DINGO_LOG(INFO) << "CreateIndex AutoIncrement send create auto increment internal success";
  }

  // update index_name_map_safe_temp_
  if (index_name_map_safe_temp_.PutIfAbsent(new_index_check_name, new_index_id) < 0) {
    DINGO_LOG(INFO) << " CreateIndex index_name" << table_definition.name()
                    << " is exist, when insert new_index_id=" << new_index_id;
    return butil::Status(pb::error::Errno::EINDEX_EXISTS,
                         fmt::format("index_name[{}] is exist in put if absent", table_definition.name()));
  }

  std::atomic<bool> index_create_success(false);
  ON_SCOPE_EXIT([&]() {
    if (index_create_success.load() == false) {
      index_name_map_safe_temp_.Erase(new_index_check_name);
    }
  });

  // create index
  // extract part info, create region for each part

  std::vector<int64_t> new_region_ids;
  int32_t replica = table_definition.replica();
  if (replica < 1) {
    replica = FLAGS_default_replica_num;
  }

  pb::common::RawEngine region_raw_engine_type;
  auto ret2 = TranslateEngineToRawEngine(table_definition.engine(), region_raw_engine_type);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "TranslateEngineToRawEngine error:" << ret2.error_str()
                     << ", table_definition:" << table_definition.ShortDebugString();
    return ret2;
  }

  std::vector<int64_t> store_ids;
  auto ret4 = GetCreateRegionStoreIds(pb::common::RegionType::INDEX_REGION, region_raw_engine_type, "", replica,
                                      table_definition.index_parameter(), store_ids);
  if (!ret4.ok()) {
    DINGO_LOG(ERROR) << "GetCreateRegionStoreIds error:" << ret4.error_str()
                     << ", table_definition:" << table_definition.ShortDebugString();
    return ret4;
  }

  for (int i = 0; i < new_part_ranges.size(); i++) {
    int64_t new_region_id = 0;
    int64_t new_part_id = new_part_ids[i];
    auto new_part_range = new_part_ranges[i];

    std::string const region_name = std::string("I_") + std::to_string(schema_id) + std::string("_") +
                                    table_definition.name() + std::string("_part_") + std::to_string(new_part_id);

    std::vector<pb::coordinator::StoreOperation> store_operations;
    auto ret = CreateRegionFinal(region_name, pb::common::RegionType::INDEX_REGION, region_raw_engine_type, "", replica,
                                 new_part_range, schema_id, 0, new_index_id, new_part_id, tenant_id,
                                 table_definition.index_parameter(), store_ids, 0, new_region_id, store_operations,
                                 meta_increment);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "CreateRegion failed in CreateIndex index_name=" << table_definition.name();
      return ret;
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

    return butil::Status(pb::error::Errno::EINDEX_REGION_CREATE_FAILED, "Not enough regions is created");
  }

  // bumper up EPOCH_REGION
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_REGION, meta_increment);

  // create table_internal, set id & table_definition
  pb::coordinator_internal::TableInternal table_internal;
  table_internal.set_id(new_index_id);
  table_internal.set_schema_id(schema_id);
  table_internal.set_table_id(table_id);
  *table_internal.mutable_range() = table_internal_range;
  auto* definition = table_internal.mutable_definition();
  *definition = table_definition;
  definition->set_create_timestamp(butil::gettimeofday_ms());

  // add table_internal to index_map_
  // update meta_increment
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_INDEX, meta_increment);
  auto* index_increment = meta_increment.add_indexes();
  index_increment->set_id(new_index_id);
  index_increment->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
  // index_increment->set_schema_id(schema_id);

  auto* index_increment_index = index_increment->mutable_table();
  *index_increment_index = table_internal;

  index_create_success.store(true);

  // return region_ids
  for (const auto& regin_id : new_region_ids) {
    region_ids.push_back(regin_id);
  }

  return butil::Status::OK();
}

// update index
// in: index_id
// in: table_definition
// return: errno
// TODO: now only support update hnsw index's max_elements
butil::Status CoordinatorControl::UpdateIndex(int64_t schema_id, int64_t index_id,
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
                                  index_id, table_internal.ShortDebugString());

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
    DINGO_LOG(ERROR) << "ERRROR: index type not match, new_table_definition=" << new_table_definition.ShortDebugString()
                     << " table_internal.definition()=" << table_internal.definition().ShortDebugString();
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "index type not match");
  }

  std::vector<pb::coordinator_internal::RegionInternal> region_internals;

  // for continuous partitions, merge scan regions.
  // for non-continuous partitions, scan regions one by one parition.
  auto ret2 = GetRegionsByTableInternal(table_internal, region_internals);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "ERRROR: GetRegionsByTableInternal failed, table_id=" << index_id;
    return ret2;
  }

  if (region_internals.empty()) {
    DINGO_LOG(WARNING) << "ERRROR: GetRegionsByTableInternal empty, table_id=" << index_id;
    return butil::Status(pb::error::Errno::EINDEX_DEFINITION_ILLEGAL, "GetRegionsByTableInternal empty");
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
      *increment_index = table_internal;

      // create store operations
      std::vector<pb::coordinator::StoreOperation> store_operations;

      // for each partition(region) of the index, build store_operation

      for (const auto& region : region_internals) {
        if (region.definition().index_id() != index_id) {
          DINGO_LOG(WARNING) << fmt::format("region table_id not match, index_id={} part_id={} region_id={} range={}",
                                            index_id, region.definition().part_id(), region.id(),
                                            region.definition().range().ShortDebugString())
                             << ", region_state: " << pb::common::RegionState_Name(region.state());
          continue;
        }

        auto region_id = region.id();

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
          region_cmd->set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
          region_cmd->set_create_timestamp(butil::gettimeofday_ms());
          region_cmd->set_region_id(region_id);
          region_cmd->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_UPDATE_DEFINITION);
          region_cmd->set_is_notify(false);

          auto* update_definition_request = region_cmd->mutable_update_definition_request();
          *(update_definition_request->mutable_new_region_definition()) = region_definition;

          store_operations.push_back(store_operation);
        }
      }

      // add store operations to meta_increment
      for (const auto& store_operation : store_operations) {
        AddStoreOperation(store_operation, false, meta_increment);

        DINGO_LOG(INFO) << "store_operation_increment = " << meta_increment.ShortDebugString();
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
    DINGO_LOG(ERROR) << "ERRROR: index type not support, new_table_definition="
                     << new_table_definition.ShortDebugString()
                     << " table_internal.definition()=" << table_internal.definition().ShortDebugString();
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "index definition update type not support");
  }

  return butil::Status::OK();
}

// DropIndex
// in: schema_id, index_id, check_compatibility
// out: meta_increment
// return: errno
butil::Status CoordinatorControl::DropIndex(int64_t schema_id, int64_t index_id, bool check_compatibility,
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

    if (table_internal.schema_id() != schema_id) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not match, index_id=" << index_id << " schema_id=" << schema_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id not match");
    }

    // this is for interface compatibility, index created by new interface cannot be dropped in this function.
    if (check_compatibility && table_internal.table_id() > 0) {
      DINGO_LOG(ERROR) << "ERRROR: cannot drop index created by new interface." << index_id;
      return butil::Status(pb::error::Errno::EINDEX_COMPATIBILITY, "cannot drop index created by new interface.");
    }
  }

  // call DropRegion
  // for (int i = 0; i < table_internal.partitions_size(); i++) {
  //   // part id
  //   int64_t region_id = table_internal.partitions(i).region_id();

  //   DropRegion(region_id, meta_increment);
  // }

  // delete index
  {
    auto* index_to_delete = meta_increment.add_indexes();
    index_to_delete->set_id(index_id);
    index_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::DELETE);
    // index_to_delete->set_schema_id(schema_id);

    auto* index_to_delete_index = index_to_delete->mutable_table();
    index_to_delete_index->set_id(index_id);
    index_to_delete_index->set_schema_id(schema_id);
  }

  // addd deleted_index
  {
    auto* index_to_delete = meta_increment.add_deleted_indexes();
    index_to_delete->set_id(index_id);
    index_to_delete->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::CREATE);
    // index_to_delete->set_schema_id(schema_id);

    auto* index_to_delete_index = index_to_delete->mutable_table();
    *index_to_delete_index = table_internal;
    index_to_delete_index->mutable_definition()->set_delete_timestamp(butil::gettimeofday_ms());
  }

  // bump up index map epoch
  GetNextId(pb::coordinator_internal::IdEpochType::EPOCH_INDEX, meta_increment);

  // delete index_name from index_name_safe_map_temp_
  std::string new_index_check_name = Helper::GenNewTableCheckName(schema_id, table_internal.definition().name());
  index_name_map_safe_temp_.Erase(new_index_check_name);

  if (table_internal.definition().auto_increment() > 0) {
    AutoIncrementControl::AsyncSendDeleteAutoIncrementInternal(index_id);
  }

  return butil::Status::OK();
}

// get tables
butil::Status CoordinatorControl::GetTables(int64_t schema_id,
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
    pb::coordinator_internal::SchemaInternal schema_internal;
    int ret = schema_map_.Get(schema_id, schema_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: schema_id not found" << schema_id;
      return butil::Status(pb::error::Errno::ESCHEMA_NOT_FOUND, "schema_id not found");
    }

    for (int i = 0; i < schema_internal.table_ids_size(); i++) {
      int64_t table_id = schema_internal.table_ids(i);

      pb::coordinator_internal::TableInternal table_internal;
      int ret = table_map_.Get(table_id, table_internal);
      if (ret < 0) {
        DINGO_LOG(ERROR) << "ERRROR: table_id not found, id=" << table_id;
        continue;
      }

      DINGO_LOG(INFO) << "GetTables found table_id=" << table_id;

      // construct return value
      pb::meta::TableDefinitionWithId table_def_with_id;

      auto* table_id_for_response = table_def_with_id.mutable_table_id();
      table_id_for_response->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
      table_id_for_response->set_entity_id(table_id);
      table_id_for_response->set_parent_entity_id(schema_id);

      *(table_def_with_id.mutable_table_definition()) = table_internal.definition();
      table_definition_with_ids.push_back(table_def_with_id);
    }
  }

  DINGO_LOG(INFO) << "GetTables schema_id=" << schema_id << " tables count=" << table_definition_with_ids.size();

  return butil::Status::OK();
}

// get indexes
butil::Status CoordinatorControl::GetIndexes(int64_t schema_id,
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
      int64_t index_id = schema_internal.index_ids(i);

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

      auto* index_id_for_response = table_def_with_id.mutable_table_id();
      index_id_for_response->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
      index_id_for_response->set_entity_id(index_id);
      index_id_for_response->set_parent_entity_id(schema_id);

      *(table_def_with_id.mutable_table_definition()) = table_internal.definition();
      table_definition_with_ids.push_back(table_def_with_id);
    }
  }

  DINGO_LOG(INFO) << "GetIndexes schema_id=" << schema_id << " indexes count=" << table_definition_with_ids.size();

  return butil::Status::OK();
}

// get tables count
butil::Status CoordinatorControl::GetTablesCount(int64_t schema_id, int64_t& tables_count) {
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
butil::Status CoordinatorControl::GetIndexesCount(int64_t schema_id, int64_t& indexes_count) {
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
    int64_t index_id = schema_internal.index_ids(i);

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
butil::Status CoordinatorControl::GetTable(int64_t schema_id, int64_t table_id,
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
  pb::coordinator_internal::TableInternal table_internal;
  int ret = table_map_.Get(table_id, table_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
    return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found");
  }

  DINGO_LOG(INFO) << "GetTable found table_id=" << table_id;

  auto* table_id_for_response = table_definition_with_id.mutable_table_id();
  table_id_for_response->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id_for_response->set_entity_id(table_id);
  table_id_for_response->set_parent_entity_id(schema_id);

  *(table_definition_with_id.mutable_table_definition()) = table_internal.definition();

  DINGO_LOG(DEBUG) << fmt::format("GetTable schema_id={} table_id={} table_definition_with_id={}", schema_id, table_id,
                                  table_definition_with_id.ShortDebugString());

  return butil::Status::OK();
}

// get index
butil::Status CoordinatorControl::GetIndex(int64_t schema_id, int64_t index_id, bool /*check_compatibility*/,
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

  *(table_definition_with_id.mutable_table_definition()) = table_internal.definition();

  DINGO_LOG(DEBUG) << fmt::format("GetIndex schema_id={} index_id={} table_definition_with_id={}", schema_id, index_id,
                                  table_definition_with_id.ShortDebugString());

  return butil::Status::OK();
}

// get table by name
butil::Status CoordinatorControl::GetTableByName(int64_t schema_id, const std::string& table_name,
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

  std::string new_table_check_name = Helper::GenNewTableCheckName(schema_id, table_name);

  int64_t temp_table_id = 0;
  auto ret = table_name_map_safe_temp_.Get(new_table_check_name, temp_table_id);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "WARNING: table_name not found " << table_name;
    return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_name not found");
  }

  DINGO_LOG(DEBUG) << fmt::format("GetTableByName schema_id={} table_name={} table_definition={}", schema_id,
                                  table_name, table_definition.ShortDebugString());

  return GetTable(schema_id, temp_table_id, table_definition);
}

// get index by name
butil::Status CoordinatorControl::GetIndexByName(int64_t schema_id, const std::string& index_name,
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

  std::string new_index_check_name = Helper::GenNewTableCheckName(schema_id, index_name);

  int64_t temp_index_id = 0;
  auto ret = index_name_map_safe_temp_.Get(new_index_check_name, temp_index_id);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: index_name not found " << index_name;
    return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index_name not found");
  }

  DINGO_LOG(DEBUG) << fmt::format("GetIndexByName schema_id={} index_name={} table_definition={}", schema_id,
                                  index_name, table_definition.ShortDebugString());

  return GetIndex(schema_id, temp_index_id, true, table_definition);
}

// get table range
butil::Status CoordinatorControl::GetTableRange(int64_t schema_id, int64_t table_id,
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

  if (table_internal.range().start_key().empty() || table_internal.range().end_key().empty()) {
    DINGO_LOG(ERROR) << "ERRROR: table range is empty, table_id=" << table_id;
    return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table range is empty");
  }

  DINGO_LOG(DEBUG) << "table_internal.range: " << Helper::StringToHex(table_internal.range().start_key()) << " - "
                   << Helper::StringToHex(table_internal.range().end_key());

  std::vector<pb::coordinator_internal::RegionInternal> region_internals;
  // for continuous partitions, merge scan regions.
  // for non-continuous partitions, scan regions one by one parition.
  auto ret2 = GetRegionsByTableInternal(table_internal, region_internals);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "ERRROR: GetRegionsByTableInternal failed, table_id=" << table_id;
    return ret2;
  }

  DINGO_LOG(DEBUG) << "GetRegionsByTableInternal table_id=" << table_id << " regions count=" << region_internals.size();

  for (const auto& part_region : region_internals) {
    if (part_region.definition().table_id() != table_id) {
      DINGO_LOG(WARNING)
          << fmt::format("region table_id not match, table_id={}, region_table_id={} part_id={} region_id={} range={}",
                         table_id, part_region.definition().table_id(), part_region.definition().part_id(),
                         part_region.id(), part_region.definition().range().ShortDebugString())
          << ", region_state: " << pb::common::RegionState_Name(part_region.state());
      continue;
    }

    if (part_region.definition().range().start_key() >= part_region.definition().range().end_key()) {
      DINGO_LOG(INFO) << fmt::format("region range illegal, table_id={} region_id={} range={}", table_id,
                                     part_region.id(), part_region.definition().range().ShortDebugString())
                      << ", region_state: " << pb::common::RegionState_Name(part_region.state());
      continue;
    }

    DINGO_LOG(DEBUG) << fmt::format("region range, table_id={} region_id={} range={}", table_id, part_region.id(),
                                    part_region.definition().range().ShortDebugString())
                     << ", region_state: " << pb::common::RegionState_Name(part_region.state());

    auto* range_distribution = table_range.add_range_distribution();
    auto* common_id_region = range_distribution->mutable_id();
    common_id_region->set_entity_id(part_region.id());
    common_id_region->set_parent_entity_id(part_region.definition().part_id());
    common_id_region->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);

    // region epoch
    *(range_distribution->mutable_region_epoch()) = part_region.definition().epoch();

    // region status
    auto* region_status = range_distribution->mutable_status();
    region_status->set_state(part_region.state());

    int64_t leader_id = 0;
    pb::common::RegionStatus inner_region_status;
    GetRegionLeaderAndStatus(part_region.id(), inner_region_status, leader_id);

    region_status->set_raft_status(inner_region_status.raft_status());
    region_status->set_replica_status(inner_region_status.replica_status());
    region_status->set_heartbeat_state(inner_region_status.heartbeat_status());
    region_status->set_last_update_timestamp(inner_region_status.last_update_timestamp());

    region_status->set_region_type(part_region.region_type());
    region_status->set_create_timestamp(part_region.create_timestamp());

    // range_distribution range
    auto* part_range = range_distribution->mutable_range();
    *part_range = part_region.definition().range();

    // range_distribution leader location
    auto* leader_location = range_distribution->mutable_leader();

    // range_distribution voter & learner locations
    for (int j = 0; j < part_region.definition().peers_size(); j++) {
      const auto& part_peer = part_region.definition().peers(j);
      if (part_peer.store_id() == leader_id) {
        *leader_location = part_peer.server_location();
        // transform ip to hostname
        Server::GetInstance().Ip2Hostname(*leader_location->mutable_host());
      }

      if (part_peer.role() == ::dingodb::pb::common::PeerRole::VOTER) {
        auto* voter_location = range_distribution->add_voters();
        *voter_location = part_peer.server_location();
        // transform ip to hostname
        Server::GetInstance().Ip2Hostname(*voter_location->mutable_host());
      } else if (part_peer.role() == ::dingodb::pb::common::PeerRole::LEARNER) {
        auto* learner_location = range_distribution->add_learners();
        *learner_location = part_peer.server_location();
        // transform ip to hostname
        Server::GetInstance().Ip2Hostname(*learner_location->mutable_host());
      }
    }

    // range_distribution regionmap_epoch
    int64_t region_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION);
    range_distribution->set_regionmap_epoch(region_map_epoch);

    // range_distribution storemap_epoch
    int64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);
    range_distribution->set_storemap_epoch(store_map_epoch);
  }

  return butil::Status::OK();
}

// get index range
butil::Status CoordinatorControl::GetIndexRange(int64_t schema_id, int64_t index_id,
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

  if (table_internal.range().start_key().empty() || table_internal.range().end_key().empty()) {
    DINGO_LOG(ERROR) << "ERRROR: index range is empty, index_id=" << index_id;
    return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index range is empty");
  }

  DINGO_LOG(INFO) << "index_internal.range: " << Helper::StringToHex(table_internal.range().start_key()) << " - "
                  << Helper::StringToHex(table_internal.range().end_key());

  std::vector<pb::coordinator_internal::RegionInternal> region_internals;

  // for continuous partitions, merge scan regions.
  // for non-continuous partitions, scan regions one by one parition.
  auto ret2 = GetRegionsByTableInternal(table_internal, region_internals);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "ERRROR: GetRegionsByTableInternal failed, table_id=" << index_id;
    return ret2;
  }

  DINGO_LOG(INFO) << "GetRegionsByTableInternal found region_internals count=" << region_internals.size();

  for (const auto& part_region : region_internals) {
    if (part_region.definition().index_id() != index_id) {
      DINGO_LOG(WARNING)
          << fmt::format("region index_id not match, index_id={} region_index_id={} part_id={} region_id={} range={}",
                         index_id, part_region.definition().index_id(), part_region.definition().part_id(),
                         part_region.id(), part_region.definition().range().ShortDebugString())
          << ", region_state: " << pb::common::RegionState_Name(part_region.state());
      continue;
    }

    if (part_region.definition().range().start_key() >= part_region.definition().range().end_key()) {
      DINGO_LOG(INFO) << fmt::format("region range illegal, index_id={} region_id={} range={}", index_id,
                                     part_region.id(), part_region.definition().range().ShortDebugString())
                      << ", region_state: " << pb::common::RegionState_Name(part_region.state());
      continue;
    }

    auto* range_distribution = index_range.add_range_distribution();
    auto* common_id_region = range_distribution->mutable_id();
    common_id_region->set_entity_id(part_region.id());
    common_id_region->set_parent_entity_id(part_region.definition().part_id());
    common_id_region->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);

    // region epoch
    *(range_distribution->mutable_region_epoch()) = part_region.definition().epoch();

    // region status
    auto* region_status = range_distribution->mutable_status();
    region_status->set_state(part_region.state());

    int64_t leader_id = 0;
    pb::common::RegionStatus inner_region_status;
    GetRegionLeaderAndStatus(part_region.id(), inner_region_status, leader_id);

    region_status->set_raft_status(inner_region_status.raft_status());
    region_status->set_replica_status(inner_region_status.replica_status());
    region_status->set_heartbeat_state(inner_region_status.heartbeat_status());
    region_status->set_last_update_timestamp(inner_region_status.last_update_timestamp());

    region_status->set_region_type(part_region.region_type());
    region_status->set_create_timestamp(part_region.create_timestamp());

    // range_distribution range
    auto* part_range = range_distribution->mutable_range();
    *part_range = part_region.definition().range();

    // range_distribution leader location
    auto* leader_location = range_distribution->mutable_leader();

    // range_distribution voter & learner locations
    for (int j = 0; j < part_region.definition().peers_size(); j++) {
      const auto& part_peer = part_region.definition().peers(j);
      if (part_peer.store_id() == leader_id) {
        *leader_location = part_peer.server_location();
        // transform ip to hostname
        Server::GetInstance().Ip2Hostname(*leader_location->mutable_host());
      }

      if (part_peer.role() == ::dingodb::pb::common::PeerRole::VOTER) {
        auto* voter_location = range_distribution->add_voters();
        *voter_location = part_peer.server_location();
        // transform ip to hostname
        Server::GetInstance().Ip2Hostname(*voter_location->mutable_host());
      } else if (part_peer.role() == ::dingodb::pb::common::PeerRole::LEARNER) {
        auto* learner_location = range_distribution->add_learners();
        *learner_location = part_peer.server_location();
        // transform ip to hostname
        Server::GetInstance().Ip2Hostname(*learner_location->mutable_host());
      }
    }

    // range_distribution regionmap_epoch
    int64_t region_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_REGION);
    range_distribution->set_regionmap_epoch(region_map_epoch);

    // range_distribution storemap_epoch
    int64_t store_map_epoch = GetPresentId(pb::coordinator_internal::IdEpochType::EPOCH_STORE);
    range_distribution->set_storemap_epoch(store_map_epoch);
  }

  return butil::Status::OK();
}

// get table metrics
butil::Status CoordinatorControl::GetTableMetrics(int64_t schema_id, int64_t table_id,
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
        *(table_metrics_internal.mutable_table_metrics()) = (*table_metrics_single);
        // table_metrics_map_[table_id] = table_metrics_internal;
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

  *(table_metrics.mutable_table_metrics()) = table_metrics_internal.table_metrics();

  return butil::Status::OK();
}

// get index metrics
butil::Status CoordinatorControl::GetIndexMetrics(int64_t schema_id, int64_t index_id,
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
        *(index_metrics_internal.mutable_index_metrics()) = (*index_metrics_single);
        // index_metrics_map_[index_id] = index_metrics_internal;
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

  *(index_metrics.mutable_index_metrics()) = index_metrics_internal.index_metrics();

  return butil::Status::OK();
}

// CalculateTableMetricsSingle
int64_t CoordinatorControl::CalculateTableMetricsSingle(int64_t table_id, pb::meta::TableMetrics& table_metrics) {
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
  int64_t row_count = 0;
  int64_t table_size = 0;
  std::string min_key(10, '\x00');
  std::string max_key(10, '\xFF');

  if (table_internal.range().start_key().empty() || table_internal.range().end_key().empty()) {
    DINGO_LOG(ERROR) << "ERRROR: table range is empty, table_id=" << table_id;
    return -1;
  }

  std::vector<pb::coordinator_internal::RegionInternal> region_internals;
  // for continuous partitions, merge scan regions.
  // for non-continuous partitions, scan regions one by one parition.
  auto ret2 = GetRegionsByTableInternal(table_internal, region_internals);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "ERRROR: GetRegionsByTableInternal failed, table_id=" << table_internal.id();
    return -1;
  }

  for (const auto& region_internal : region_internals) {
    if (region_internal.definition().table_id() != table_id) {
      DINGO_LOG(WARNING) << fmt::format("region table_id not match, table_id={} part_id={} region_id={} range={}",
                                        table_id, region_internal.definition().part_id(), region_internal.id(),
                                        region_internal.definition().range().ShortDebugString())
                         << ", region_state: " << pb::common::RegionState_Name(region_internal.state());
      continue;
    }

    // part id
    int64_t region_id = region_internal.id();

    // get region
    pb::common::RegionMetrics region_metrics;
    {
      // BAIDU_SCOPED_LOCK(region_map_mutex_);
      int ret = region_metrics_map_.Get(region_id, region_metrics);
      if (ret < 0) {
        DINGO_LOG(ERROR) << fmt::format(
            "ERROR cannot find region in regionmap_ while GetTable, table_id={} region_id={}", table_id, region_id);
        continue;
      }
    }

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
  table_metrics.set_part_count(table_internal.definition().table_partition().partitions_size());

  DINGO_LOG(DEBUG) << fmt::format(
      "table_metrics calculated in CalculateTableMetricsSingle, table_id={} row_count={} min_key={} max_key={} "
      "part_count={}",
      table_id, table_metrics.rows_count(), table_metrics.min_key(), table_metrics.max_key(),
      table_metrics.part_count());

  return 0;
}

// CalculateIndexMetricsSingle
int64_t CoordinatorControl::CalculateIndexMetricsSingle(int64_t index_id, pb::meta::IndexMetrics& index_metrics) {
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
  int64_t row_count = 0;
  std::string min_key(10, '\x00');
  std::string max_key(10, '\xFF');

  // about vector
  pb::common::VectorIndexType vector_index_type = pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE;
  int64_t current_count = 0;
  int64_t deleted_count = 0;
  int64_t max_id = -1;
  int64_t min_id = -1;
  int64_t memory_bytes = 0;

  if (table_internal.range().start_key().empty() || table_internal.range().end_key().empty()) {
    DINGO_LOG(ERROR) << "ERRROR: index range is empty, index_id=" << index_id;
    return -1;
  }

  std::vector<pb::coordinator_internal::RegionInternal> region_internals;
  // for continuous partitions, merge scan regions.
  // for non-continuous partitions, scan regions one by one parition.
  auto ret2 = GetRegionsByTableInternal(table_internal, region_internals);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "ERRROR: GetRegionsByTableInternal failed, table_id=" << table_internal.id();
    return -1;
  }

  for (const auto& region_internal : region_internals) {
    if (region_internal.definition().index_id() != index_id) {
      DINGO_LOG(WARNING) << fmt::format("region table_id not match, index_id={} part_id={} region_id={} range={}",
                                        index_id, region_internal.definition().part_id(), region_internal.id(),
                                        region_internal.definition().range().ShortDebugString())
                         << ", region_state: " << pb::common::RegionState_Name(region_internal.state());
      continue;
    }

    // part id
    int64_t region_id = region_internal.id();

    // get region
    pb::common::RegionMetrics region_metrics;
    {
      // BAIDU_SCOPED_LOCK(region_map_mutex_);
      int ret = region_metrics_map_.Get(region_id, region_metrics);
      if (ret < 0) {
        DINGO_LOG(ERROR) << fmt::format(
            "ERROR cannot find region in regionmap_ while GetIndex, index_id={} region_id={}", index_id, region_id);
        continue;
      }
    }

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
  index_metrics.set_part_count(table_internal.definition().table_partition().partitions_size());

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

  butil::FlatMap<int64_t, pb::coordinator_internal::TableMetricsInternal> temp_table_metrics_map;
  temp_table_metrics_map.init(10000);
  table_metrics_map_.GetRawMapCopy(temp_table_metrics_map);

  for (auto& table_metrics_internal : temp_table_metrics_map) {
    int64_t table_id = table_metrics_internal.first;
    pb::meta::TableMetrics table_metrics;
    if (CalculateTableMetricsSingle(table_id, table_metrics) < 0) {
      DINGO_LOG(ERROR) << "ERRROR: CalculateTableMetricsSingle failed, remove metrics from map" << table_id;
      table_metrics_map_.Erase(table_id);

      // mbvar table
      coordinator_bvar_metrics_table_.DeleteTableBvar(table_id);

    } else {
      *(table_metrics_internal.second.mutable_table_metrics()) = table_metrics;

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

  butil::FlatMap<int64_t, pb::coordinator_internal::IndexMetricsInternal> temp_index_metrics_map;
  temp_index_metrics_map.init(10000);
  index_metrics_map_.GetRawMapCopy(temp_index_metrics_map);

  for (auto& index_metrics_internal : temp_index_metrics_map) {
    int64_t index_id = index_metrics_internal.first;
    pb::meta::IndexMetrics index_metrics;
    if (CalculateIndexMetricsSingle(index_id, index_metrics) < 0) {
      DINGO_LOG(ERROR) << "ERRROR: CalculateIndexMetricsSingle failed, remove metrics from map" << index_id;
      index_metrics_map_.Erase(index_id);

      // mbvar index
      coordinator_bvar_metrics_index_.DeleteIndexBvar(index_id);

    } else {
      *(index_metrics_internal.second.mutable_index_metrics()) = index_metrics;

      // update index_metrics_map_ in memory
      index_metrics_map_.PutIfExists(index_id, index_metrics_internal.second);

      // mbvar index
      coordinator_bvar_metrics_index_.UpdateIndexBvar(index_id, index_metrics.rows_count(), index_metrics.part_count());
    }
  }
}

butil::Status CoordinatorControl::GenerateTableWithPartIds(int64_t schema_id,
                                                           const pb::meta::TableWithPartCount& table_with_part_count,
                                                           pb::coordinator_internal::MetaIncrement& meta_increment,
                                                           pb::meta::GenerateTableIdsResponse* response) {
  if (table_with_part_count.index_count() != table_with_part_count.index_part_count_size()) {
    DINGO_LOG(ERROR) << "index count is illegal: " << table_with_part_count.index_count() << " | "
                     << table_with_part_count.index_part_count_size();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "index count is illegal");
  }

  if (!ValidateSchema(schema_id)) {
    DINGO_LOG(ERROR) << "schema_id is illegal " << schema_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "schema_id is illegal");
  }

  if (table_with_part_count.has_table()) {
    auto ret = GenerateTableIdAndPartIds(schema_id, table_with_part_count.table_part_count(),
                                         pb::meta::EntityType::ENTITY_TYPE_TABLE, meta_increment, response->add_ids());
    if (!ret.ok()) {
      return ret;
    }
  }

  for (uint32_t i = 0; i < table_with_part_count.index_count(); i++) {
    auto ret = GenerateTableIdAndPartIds(schema_id, table_with_part_count.index_part_count(i),
                                         pb::meta::EntityType::ENTITY_TYPE_INDEX, meta_increment, response->add_ids());
    if (!ret.ok()) {
      return ret;
    }
  }

  return butil::Status::OK();
}

void CoordinatorControl::CreateTableIndexesMap(pb::coordinator_internal::TableIndexInternal& table_index_internal,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  // update meta_increment
  auto* table_index_increment = meta_increment.add_table_indexes();
  table_index_increment->set_id(table_index_internal.id());
  table_index_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::CREATE);
  *(table_index_increment->mutable_table_indexes()) = table_index_internal;
}

butil::Status CoordinatorControl::GetTableIndexes(int64_t schema_id, int64_t table_id,
                                                  pb::meta::GetTablesResponse* response) {
  pb::meta::TableDefinitionWithId main_definition_with_id;
  butil::Status ret = GetTable(schema_id, table_id, main_definition_with_id);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "error while get table_indexes GetTable failed, schema_id: " << schema_id
                     << ", table_id: " << table_id << ", error_code: " << ret.error_code()
                     << ", error_msg: " << ret.error_str();
    return ret;
  }

  pb::coordinator_internal::TableIndexInternal table_index_internal;
  int result = table_index_map_.Get(table_id, table_index_internal);
  if (result >= 0) {
    // found table_index, first add main table definition to response
    *(response->add_table_definition_with_ids()) = main_definition_with_id;

    // get all index's definition and add to response
    for (const auto& index_id : table_index_internal.table_ids()) {
      pb::meta::TableDefinitionWithId index_definition_with_id;
      ret = GetIndex(schema_id, index_id.entity_id(), false, index_definition_with_id);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "error while get table_indexes GetIndex failed, schema_id: " << schema_id
                         << ", index_id: " << index_id.entity_id() << ", error_code: " << ret.error_code()
                         << ", error_msg: " << ret.error_str();
        return ret;
      }

      *(response->add_table_definition_with_ids()) = index_definition_with_id;
    }

    // set revision
    response->set_revision(table_index_internal.revision());

  } else {
    // not found, return error
    std::string s =
        "cannot find indexes, schema_id: " + std::to_string(schema_id) + ", table_id: " + std::to_string(table_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DropTableIndexes(int64_t schema_id, int64_t table_id,
                                                   pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::meta::TableDefinitionWithId definition_with_id;
  butil::Status ret = GetTable(schema_id, table_id, definition_with_id);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "error while dropping index GetTable failed, schema_id: " << schema_id
                     << ", table_id: " << table_id << ", error_code: " << ret.error_code()
                     << ", error_msg: " << ret.error_str();
    return ret;
  }

  // drop indexes of the table
  pb::coordinator_internal::TableIndexInternal table_index_internal;
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
    *table_index_increment->mutable_table_indexes() = table_index_internal;
  } else {
    // not find in map
    std::string s = "cannot find in table_index_map_, schema_id: " + std::to_string(schema_id) +
                    ", table_id: " + std::to_string(table_id);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  // drop table finally
  ret = DropTable(schema_id, table_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "error while dropping index, schema_id: " << schema_id << ", table_id: " << table_id;
    return ret;
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::RemoveTableIndex(int64_t table_id, int64_t index_id,
                                                   pb::coordinator_internal::MetaIncrement& meta_increment) {
  auto ret = DropIndex(table_id, index_id, false, meta_increment);
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
      break;
    }
  }

  if (found_index) {
    DINGO_LOG(INFO) << "remove success, table_id: " << table_id << ", index_id: " << index_id
                    << ", origin_ids_size: " << source_size;

    auto* table_index_increment = meta_increment.add_table_indexes();
    table_index_increment->set_id(table_id);
    table_index_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
    table_index_increment->add_table_ids_to_del()->set_entity_id(index_id);
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
butil::Status CoordinatorControl::SwitchAutoSplit(int64_t schema_id, int64_t table_id, bool auto_split,
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

  if (table_internal.range().start_key().empty() || table_internal.range().end_key().empty()) {
    DINGO_LOG(ERROR) << "ERRROR: table range is empty, table_id=" << table_id;
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "table range is empty");
  }

  std::vector<pb::coordinator_internal::RegionInternal> region_internals;
  // for continuous partitions, merge scan regions.
  // for non-continuous partitions, scan regions one by one parition.
  auto ret2 = GetRegionsByTableInternal(table_internal, region_internals);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "ERRROR: GetRegionsByTableInternal failed, table_id=" << table_internal.id();
    return ret2;
  }

  if (region_internals.empty()) {
    DINGO_LOG(WARNING) << "ERRROR: GetRegionsByTableInternal empty, table_id=" << table_id;
    return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "GetRegionsByTableInternal empty");
  }

  for (const auto& part_region : region_internals) {
    if (part_region.definition().table_id() != table_id && part_region.definition().index_id() != table_id) {
      DINGO_LOG(WARNING) << fmt::format("region table_id not match, table_id={} part_id={} region_id={} range={}",
                                        table_id, part_region.definition().part_id(), part_region.id(),
                                        part_region.definition().range().ShortDebugString())
                         << ", region_state: " << pb::common::RegionState_Name(part_region.state());
      continue;
    }

    auto region_id = part_region.id();

    auto leader_store_id = GetRegionLeaderId(region_id);
    if (leader_store_id <= 0) {
      DINGO_LOG(ERROR) << fmt::format("ERROR cannot find leader in regionmap_ while GetTable, table_id={} region_id={}",
                                      table_id, region_id);
      return butil::Status(pb::error::Errno::EINTERNAL, "some region leader not found");
    }

    // send region_cmd to update auto_split
    pb::coordinator::RegionCmd region_cmd;
    region_cmd.set_id(GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, meta_increment));
    region_cmd.set_region_id(region_id);
    region_cmd.set_region_cmd_type(pb::coordinator::RegionCmdType::CMD_SWITCH_SPLIT);
    region_cmd.set_create_timestamp(butil::gettimeofday_ms());
    region_cmd.mutable_switch_split_request()->set_region_id(region_id);
    region_cmd.mutable_switch_split_request()->set_disable_split(!auto_split);

    auto status = AddRegionCmd(leader_store_id, 0, region_cmd, meta_increment);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "ERRROR: AddRegionCmd failed, table_id=" << table_id << " region_id=" << region_id;
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetDeletedTable(
    int64_t deleted_table_id, std::vector<pb::meta::TableDefinitionWithId>& table_definition_with_ids) {
  if (deleted_table_id == 0) {
    std::vector<pb::coordinator_internal::TableInternal> temp_table_map;
    auto ret = deleted_table_meta_->GetAllElements(temp_table_map);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "get deleted table map failed, error: " << ret;
      return ret;
    }

    for (const auto& deleted_table : temp_table_map) {
      pb::meta::TableDefinitionWithId table_definition_with_id;
      table_definition_with_id.mutable_table_id()->set_entity_id(deleted_table.id());
      *(table_definition_with_id.mutable_table_definition()) = deleted_table.definition();
      table_definition_with_ids.push_back(table_definition_with_id);
    }
  } else {
    pb::coordinator_internal::TableInternal table_internal;
    auto ret = deleted_table_meta_->Get(deleted_table_id, table_internal);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "get deleted table failed, error: " << ret;
      return ret;
    }

    pb::meta::TableDefinitionWithId table_definition_with_id;
    table_definition_with_id.mutable_table_id()->set_entity_id(deleted_table_id);
    *(table_definition_with_id.mutable_table_definition()) = table_internal.definition();
    table_definition_with_ids.push_back(table_definition_with_id);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetDeletedIndex(
    int64_t deleted_index_id, std::vector<pb::meta::TableDefinitionWithId>& table_definition_with_ids) {
  if (deleted_index_id == 0) {
    std::vector<pb::coordinator_internal::TableInternal> temp_table_map;
    auto ret = deleted_index_meta_->GetAllElements(temp_table_map);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "get deleted index map failed, error: " << ret;
      return ret;
    }

    for (const auto& deleted_table : temp_table_map) {
      pb::meta::TableDefinitionWithId table_definition_with_id;
      table_definition_with_id.mutable_table_id()->set_entity_id(deleted_table.id());
      *(table_definition_with_id.mutable_table_definition()) = deleted_table.definition();
      table_definition_with_ids.push_back(table_definition_with_id);
    }
  } else {
    pb::coordinator_internal::TableInternal table_internal;
    auto ret = deleted_index_meta_->Get(deleted_index_id, table_internal);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "get deleted index failed, error: " << ret;
      return ret;
    }

    pb::meta::TableDefinitionWithId table_definition_with_id;
    table_definition_with_id.mutable_table_id()->set_entity_id(deleted_index_id);
    *(table_definition_with_id.mutable_table_definition()) = table_internal.definition();
    table_definition_with_ids.push_back(table_definition_with_id);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CleanDeletedTable(int64_t table_id) {
  pb::coordinator_internal::MetaIncrement meta_increment;

  uint32_t i = 0;
  if (table_id == 0) {
    std::vector<int64_t> temp_table_ids;
    auto ret = deleted_table_meta_->GetAllIds(temp_table_ids);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "get deleted table ids failed, error: " << ret;
      return ret;
    }

    for (const auto& id : temp_table_ids) {
      auto* delete_table = meta_increment.add_deleted_tables();
      delete_table->set_id(id);
      delete_table->set_op_type(pb::coordinator_internal::MetaIncrementOpType::DELETE);
      auto* delete_table_internal = delete_table->mutable_table();
      delete_table_internal->set_id(id);

      if (i++ > 1000) {
        auto ret1 = SubmitMetaIncrementSync(meta_increment);
        if (!ret1.ok()) {
          DINGO_LOG(ERROR) << "submit meta increment failed, table_id: " << table_id << ", error: " << ret1;
          return ret1;
        }
        i = 0;
        meta_increment.Clear();
      }
    }
  } else {
    pb::coordinator_internal::TableInternal table_internal;
    auto ret = deleted_table_meta_->Get(table_id, table_internal);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "get deleted table failed, error: " << ret;
      return ret;
    }

    auto* delete_table = meta_increment.add_deleted_tables();
    delete_table->set_id(table_id);
    delete_table->set_op_type(pb::coordinator_internal::MetaIncrementOpType::DELETE);
    auto* delete_table_internal = delete_table->mutable_table();
    delete_table_internal->set_id(table_id);
  }

  if (meta_increment.ByteSizeLong() > 0) {
    return SubmitMetaIncrementSync(meta_increment);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CleanDeletedIndex(int64_t index_id) {
  pb::coordinator_internal::MetaIncrement meta_increment;
  uint32_t i = 0;

  if (index_id == 0) {
    std::vector<int64_t> temp_table_ids;
    auto ret = deleted_index_meta_->GetAllIds(temp_table_ids);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "get deleted index ids failed, error: " << ret;
      return ret;
    }

    for (const auto& id : temp_table_ids) {
      auto* delete_table = meta_increment.add_deleted_indexes();
      delete_table->set_id(id);
      delete_table->set_op_type(pb::coordinator_internal::MetaIncrementOpType::DELETE);
      auto* delete_table_internal = delete_table->mutable_table();
      delete_table_internal->set_id(id);

      if (i++ > 1000) {
        auto ret1 = SubmitMetaIncrementSync(meta_increment);
        if (!ret1.ok()) {
          DINGO_LOG(ERROR) << "submit meta increment failed, index_id: " << index_id << ", error: " << ret1;
          return ret1;
        }
        i = 0;
        meta_increment.Clear();
      }
    }
  } else {
    pb::coordinator_internal::TableInternal table_internal;
    auto ret = deleted_index_meta_->Get(index_id, table_internal);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "get deleted index failed, error: " << ret;
      return ret;
    }

    auto* delete_table = meta_increment.add_deleted_indexes();
    delete_table->set_id(index_id);
    delete_table->set_op_type(pb::coordinator_internal::MetaIncrementOpType::DELETE);
    auto* delete_table_internal = delete_table->mutable_table();
    delete_table_internal->set_id(index_id);
  }

  if (meta_increment.ByteSizeLong() > 0) {
    return SubmitMetaIncrementSync(meta_increment);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::UpdateTableDefinition(int64_t table_id, bool is_index,
                                                        const pb::meta::TableDefinition& table_definition,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::coordinator_internal::TableInternal table_internal;

  if (!is_index) {
    int ret = table_map_.Get(table_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
      return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found");
    }
  } else {
    int ret = index_map_.Get(table_id, table_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: index_id not found" << table_id;
      return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "index_id not found");
    }
  }

  // check if table_definition is legal
  if (table_internal.definition().name() != table_definition.name()) {
    DINGO_LOG(ERROR) << "ERRROR: table name cannot be changed" << table_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "table name cannot be changed");
  }

  if (table_internal.definition().table_partition().partitions_size() !=
      table_definition.table_partition().partitions_size()) {
    DINGO_LOG(ERROR) << "ERRROR: table partition count cannot be changed" << table_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "table partition count cannot be changed");
  }

  if (table_internal.definition().engine() != table_definition.engine()) {
    DINGO_LOG(ERROR) << "ERRROR: table engine cannot be changed" << table_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "table engine cannot be changed");
  }

  if (table_internal.definition().table_partition().strategy() != table_definition.table_partition().strategy()) {
    DINGO_LOG(ERROR) << "ERRROR: table partition type cannot be changed" << table_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "table partition type cannot be changed");
  }

  std::set<int64_t> old_ids;
  std::set<int64_t> new_ids;
  for (const auto& part_id : table_internal.definition().table_partition().partitions()) {
    old_ids.insert(part_id.id().entity_id());
  }
  for (const auto& part_id : table_definition.table_partition().partitions()) {
    new_ids.insert(part_id.id().entity_id());
  }
  for (const auto& part_id : old_ids) {
    if (new_ids.find(part_id) == new_ids.end()) {
      DINGO_LOG(ERROR) << "ERRROR: table partition id cannot be changed" << table_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "table partition id cannot be changed");
    }
  }

  // update table definition, we do not support change partitions, so we just update other fields
  pb::coordinator_internal::TableInternal table_internal_new = table_internal;
  *(table_internal_new.mutable_definition()) = table_definition;

  if (!is_index) {
    auto* table_increment = meta_increment.add_tables();
    table_increment->set_id(table_id);
    table_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
    *(table_increment->mutable_table()) = table_internal_new;
  } else {
    auto* index_increment = meta_increment.add_indexes();
    index_increment->set_id(table_id);
    index_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
    *(index_increment->mutable_table()) = table_internal_new;
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::AddIndexOnTable(int64_t table_id, int64_t index_id,
                                                  const pb::meta::TableDefinition& table_definition,
                                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::coordinator_internal::TableInternal table_internal;
  int ret = table_map_.Get(table_id, table_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
    return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found");
  }

  // check if index_id is already exist
  pb::coordinator_internal::TableInternal index_internal;
  ret = index_map_.Get(index_id, index_internal);
  if (ret > 0) {
    DINGO_LOG(ERROR) << "ERRROR: index_id already exist" << index_id;
    return butil::Status(pb::error::Errno::EINDEX_EXISTS, "index_id already exist");
  }

  // check if table_id exeist in table_index_map_
  pb::coordinator_internal::TableIndexInternal table_index_internal;
  ret = table_index_map_.Get(table_id, table_index_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: table_id not found in table_index_map_" << table_id;
    return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found in table_index_map_");
  }

  // call CreateIndex to create index
  std::vector<int64_t> region_ids;
  auto status =
      CreateIndex(table_internal.schema_id(), table_definition, table_id, index_id, region_ids, meta_increment);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "ERRROR: CreateIndex failed, table_id=" << table_id << " index_id=" << index_id;
    return status;
  }

  // add index to table_index_map_
  pb::meta::DingoCommonId new_index_id;
  new_index_id.set_entity_id(index_id);
  new_index_id.set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  new_index_id.set_parent_entity_id(table_internal.schema_id());

  auto* table_index_increment = meta_increment.add_table_indexes();
  table_index_increment->set_id(table_id);
  table_index_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  *table_index_increment->add_table_ids_to_add() = new_index_id;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DropIndexOnTable(int64_t table_id, int64_t index_id,
                                                   pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::coordinator_internal::TableInternal table_internal;
  int ret = table_map_.Get(table_id, table_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: table_id not found" << table_id;
    return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found");
  }

  // check if index_id is exist
  pb::coordinator_internal::TableInternal index_internal;
  ret = index_map_.Get(index_id, index_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: index_id not found" << index_id;
    return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index_id not found");
  }

  // check if table_id exeist in table_index_map_
  pb::coordinator_internal::TableIndexInternal table_index_internal;
  ret = table_index_map_.Get(table_id, table_index_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: table_id not found in table_index_map_" << table_id;
    return butil::Status(pb::error::Errno::ETABLE_NOT_FOUND, "table_id not found in table_index_map_");
  }
  bool found = false;
  for (const auto& id : table_index_internal.table_ids()) {
    if (id.entity_id() == index_id) {
      found = true;
      break;
    }
  }
  if (!found) {
    DINGO_LOG(ERROR) << "ERRROR: index_id not found in table_index_map_" << index_id;
    return butil::Status(pb::error::Errno::EINDEX_NOT_FOUND, "index_id not found in table_index_map_");
  }

  // call DropIndex to drop index
  auto status = DropIndex(table_internal.schema_id(), index_id, false, meta_increment);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "ERRROR: DropIndex failed, table_id=" << table_id << " index_id=" << index_id;
    return status;
  }

  // del index from table_index_map_
  pb::meta::DingoCommonId del_index_id;
  for (const auto& id : table_index_internal.table_ids()) {
    if (id.entity_id() != index_id) {
      del_index_id = id;
      break;
    }
  }

  auto* table_index_increment = meta_increment.add_table_indexes();
  table_index_increment->set_id(table_id);
  table_index_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  *table_index_increment->add_table_ids_to_del() = del_index_id;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::CalcTableInternalRange(const pb::meta::PartitionRule& partition_rule,
                                                         pb::common::Range& table_internal_range) {
  if (partition_rule.partitions_size() > 0) {
    table_internal_range = partition_rule.partitions(0).range();
    for (const auto& part : partition_rule.partitions()) {
      if (part.id().entity_id() <= 0) {
        DINGO_LOG(ERROR) << "part_id is illegal, part_id=" << part.id().entity_id();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "part_id is illegal");
      }
      if (part.range().start_key().empty() || part.range().end_key().empty()) {
        DINGO_LOG(ERROR) << "part range is illegal, part_id=" << part.id().entity_id();
        return butil::Status(pb::error::Errno::ETABLE_DEFINITION_ILLEGAL, "part range is illegal");
      }

      if (!part.range().start_key().empty() && !part.range().end_key().empty()) {
        if (table_internal_range.start_key() > part.range().start_key()) {
          table_internal_range.set_start_key(part.range().start_key());
        }
        if (table_internal_range.end_key() < part.range().end_key()) {
          table_internal_range.set_end_key(part.range().end_key());
        }
      } else {
        DINGO_LOG(ERROR) << "part range is illegal, part_id=" << part.id().entity_id();
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "part range is illegal");
      }
    }
  } else {
    DINGO_LOG(ERROR) << "no range provided ";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "no partition provided");
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::TranslateEngineToRawEngine(const pb::common::Engine& engine,
                                                             pb::common::RawEngine& raw_engine) {
  switch (engine) {
    case pb::common::Engine::ENG_ROCKSDB:
    case pb::common::Engine::LSM:
    case pb::common::Engine::TXN_LSM:
      raw_engine = pb::common::RawEngine::RAW_ENG_ROCKSDB;
      break;
    case pb::common::Engine::ENG_BDB:
    case pb::common::Engine::BTREE:
    case pb::common::Engine::TXN_BTREE:
      raw_engine = pb::common::RawEngine::RAW_ENG_BDB;
      break;
    default:
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "engine not support");
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetRegionsByTableInternal(
    const pb::coordinator_internal::TableInternal& table_internal,
    std::vector<pb::coordinator_internal::RegionInternal>& region_internals) {
  // for continuous partitions, merge scan regions.
  // for non-continuous partitions, scan regions one by one parition.
  if (BAIDU_UNLIKELY(table_internal.range().start_key().empty() || table_internal.range().end_key().empty())) {
    for (const auto& partition_range : table_internal.definition().table_partition().partitions()) {
      std::vector<pb::coordinator_internal::RegionInternal> regions_temp;
      auto ret1 = ScanRegions(partition_range.range().start_key(), partition_range.range().end_key(), 0, regions_temp);
      if (!ret1.ok()) {
        DINGO_LOG(ERROR) << "ERRROR: ScanRegions failed, table_id=" << table_internal.id();
        return butil::Status(pb::error::Errno::EINDEX_DEFINITION_ILLEGAL, "ScanRegions failed");
      }

      for (auto& region : regions_temp) {
        region_internals.push_back(std::move(region));
      }
    }
  } else {
    auto ret1 = ScanRegions(table_internal.range().start_key(), table_internal.range().end_key(), 0, region_internals);
    if (!ret1.ok()) {
      DINGO_LOG(ERROR) << "ERRROR: ScanRegions failed, table_id=" << table_internal.id();
      return butil::Status(pb::error::Errno::EINDEX_DEFINITION_ILLEGAL, "ScanRegions failed");
    }
  }

  return butil::Status::OK();
}

// Tenant operations
void CoordinatorControl::GetDefaultTenant(pb::coordinator_internal::TenantInternal& tenant_internal) {
  tenant_internal.set_id(0);
  tenant_internal.set_name("default");
  tenant_internal.set_create_timestamp(butil::gettimeofday_ms());
  tenant_internal.set_update_timestamp(butil::gettimeofday_ms());

  tenant_internal.set_safe_point_ts(GetPresentId(pb::coordinator_internal::IdEpochType::ID_GC_SAFE_POINT));
}

butil::Status CoordinatorControl::CreateTenant(pb::meta::Tenant& tenant,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  // check if max_tenant_count is exceed
  if (tenant_map_.Size() >= FLAGS_max_tenant_count) {
    DINGO_LOG(ERROR) << "ERRROR: max_tenant_count is exceed, max_tenant_count=" << FLAGS_max_tenant_count;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "max_tenant_count is exceed");
  }

  pb::coordinator_internal::TenantInternal tenant_internal;

  if (tenant.id() == 0) {
    // get new tenant_id
    auto new_tenant_id = GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_TENANT, meta_increment);
    if (new_tenant_id <= 0) {
      DINGO_LOG(ERROR) << "ERRROR: GetNextId failed, new_tenant_id=" << new_tenant_id;
      return butil::Status(pb::error::Errno::EINTERNAL, "GetNextId failed");
    }
    tenant.set_id(new_tenant_id);
  }

  int ret = tenant_map_.Get(tenant.id(), tenant_internal);
  if (ret > 0) {
    DINGO_LOG(ERROR) << "ERRROR: tenant_id already exist" << tenant.id();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id already exist");
  }

  auto* tenant_increment = meta_increment.add_tenants();
  tenant_increment->set_id(tenant.id());
  tenant_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::CREATE);
  auto* increment_tenant = tenant_increment->mutable_tenant();
  increment_tenant->set_id(tenant.id());
  increment_tenant->set_name(tenant.name());
  increment_tenant->set_comment(tenant.comment());
  increment_tenant->set_create_timestamp(butil::gettimeofday_ms());
  increment_tenant->set_update_timestamp(butil::gettimeofday_ms());

  return butil::Status::OK();
}

butil::Status CoordinatorControl::DropTenant(int64_t tenant_id,
                                             pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (tenant_id == 0) {
    DINGO_LOG(ERROR) << "ERRROR: cannot drop default tenant";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cannot drop default tenant");
  }

  if (tenant_id < 0) {
    DINGO_LOG(ERROR) << "ERRROR: tenant_id illegal " << tenant_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id illegal");
  }

  pb::coordinator_internal::TenantInternal tenant_internal;

  int ret = tenant_map_.Get(tenant_id, tenant_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: tenant_id not exist" << tenant_id;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id not exist");
  }

  // check if there is schemas of the tenant_id in schema_map_
  std::vector<pb::meta::Schema> schemas;
  auto ret1 = GetSchemas(tenant_id, schemas);
  if (!ret1.ok()) {
    std::string s = "get schemas failed: " + std::to_string(tenant_id) + ", error_str: " + ret1.error_str();
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  if (!schemas.empty()) {
    std::string s = "tenant_id has schemas, cannot drop: " + std::to_string(tenant_id) +
                    ", schemas count: " + std::to_string(schemas.size());
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  auto* tenant_increment = meta_increment.add_tenants();
  tenant_increment->set_id(tenant_id);
  tenant_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::DELETE);
  auto* increment_tenant = tenant_increment->mutable_tenant();
  *increment_tenant = tenant_internal;

  return butil::Status::OK();
}

butil::Status CoordinatorControl::UpdateTenant(pb::meta::Tenant& tenant,
                                               pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (tenant.id() == 0) {
    DINGO_LOG(ERROR) << "ERRROR: cannot update default tenant";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "cannot update default tenant");
  }

  if (tenant.id() < 0) {
    DINGO_LOG(ERROR) << "ERRROR: tenant_id illegal " << tenant.id();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id illegal");
  }

  pb::coordinator_internal::TenantInternal tenant_internal;
  int ret = tenant_map_.Get(tenant.id(), tenant_internal);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ERRROR: tenant_id not exist" << tenant.id();
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id not exist");
  }

  auto* tenant_increment = meta_increment.add_tenants();
  tenant_increment->set_id(tenant.id());
  tenant_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
  auto* increment_tenant = tenant_increment->mutable_tenant();
  increment_tenant->set_id(tenant.id());
  increment_tenant->set_name(tenant.name());
  increment_tenant->set_comment(tenant.comment());
  auto update_time_ms = butil::gettimeofday_ms();
  increment_tenant->set_update_timestamp(update_time_ms);

  tenant.set_update_timestamp(update_time_ms);

  return butil::Status::OK();
}

butil::Status CoordinatorControl::UpdateTenantSafepoint(int64_t tenant_id, int64_t safe_point_ts,
                                                        pb::coordinator_internal::MetaIncrement& meta_increment) {
  pb::coordinator_internal::TenantInternal tenant_internal;

  if (tenant_id != 0) {
    int ret = tenant_map_.Get(tenant_id, tenant_internal);
    if (ret < 0) {
      DINGO_LOG(ERROR) << "ERRROR: tenant_id not exist" << tenant_id;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "tenant_id not exist");
    }

    if (safe_point_ts <= tenant_internal.safe_point_ts()) {
      DINGO_LOG(INFO) << "CAUTION: safe_point_ts is " << safe_point_ts
                      << " smaller than before: " << tenant_internal.safe_point_ts() << ", no need to update";
      return butil::Status::OK();
    }

    auto* tenant_increment = meta_increment.add_tenants();
    tenant_increment->set_id(tenant_id);
    tenant_increment->set_op_type(pb::coordinator_internal::MetaIncrementOpType::UPDATE);
    auto* increment_tenant = tenant_increment->mutable_tenant();
    *increment_tenant = tenant_internal;
    increment_tenant->set_update_timestamp(butil::gettimeofday_ms());
    increment_tenant->set_safe_point_ts(safe_point_ts);
  } else {
    GetDefaultTenant(tenant_internal);
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetTenants(std::vector<int64_t> tenant_ids, std::vector<pb::meta::Tenant>& tenants) {
  if (tenant_ids.empty()) {
    return butil::Status::OK();
  }

  for (const auto tenant_id : tenant_ids) {
    pb::coordinator_internal::TenantInternal tenant_internal;

    if (tenant_id != 0) {
      int ret = tenant_map_.Get(tenant_id, tenant_internal);
      if (ret < 0) {
        std::string s = "tenant_id not exist: " + std::to_string(tenant_id);
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
      }

    } else {
      GetDefaultTenant(tenant_internal);
    }

    pb::meta::Tenant tenant;
    tenant.set_id(tenant_internal.id());
    tenant.set_name(tenant_internal.name());
    tenant.set_comment(tenant_internal.comment());
    tenant.set_create_timestamp(tenant_internal.create_timestamp());
    tenant.set_update_timestamp(tenant_internal.update_timestamp());
    tenant.set_delete_timestamp(tenant_internal.delete_timestamp());
    tenant.set_revision(tenant_internal.revision());

    tenants.push_back(std::move(tenant));
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetAllTenants(std::vector<pb::meta::Tenant>& tenants) {
  butil::FlatMap<int64_t, pb::coordinator_internal::TenantInternal> temp_tenant_map;
  auto ret = tenant_map_.GetRawMapCopy(temp_tenant_map);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "get tenant map failed, error: " << ret;
    return butil::Status(pb::error::Errno::EINTERNAL, "get tenant map failed");
  }

  pb::coordinator_internal::TenantInternal tenant_internal;
  GetDefaultTenant(tenant_internal);

  pb::meta::Tenant tenant;
  tenant.set_id(tenant_internal.id());
  tenant.set_name(tenant_internal.name());
  tenant.set_comment(tenant_internal.comment());
  tenant.set_create_timestamp(tenant_internal.create_timestamp());
  tenant.set_update_timestamp(tenant_internal.update_timestamp());
  tenant.set_delete_timestamp(tenant_internal.delete_timestamp());
  tenant.set_safe_point_ts(tenant_internal.safe_point_ts());
  tenant.set_revision(tenant_internal.revision());

  tenants.push_back(std::move(tenant));

  for (const auto& [tenant_id, tenant_internal] : temp_tenant_map) {
    pb::meta::Tenant tenant;
    tenant.set_id(tenant_internal.id());
    tenant.set_name(tenant_internal.name());
    tenant.set_comment(tenant_internal.comment());
    tenant.set_create_timestamp(tenant_internal.create_timestamp());
    tenant.set_update_timestamp(tenant_internal.update_timestamp());
    tenant.set_delete_timestamp(tenant_internal.delete_timestamp());
    tenant.set_safe_point_ts(tenant_internal.safe_point_ts());
    tenant.set_revision(tenant_internal.revision());

    tenants.push_back(std::move(tenant));
  }

  return butil::Status::OK();
}

butil::Status CoordinatorControl::GetMetaCount(int64_t& schema_count, int64_t& table_count, int64_t& index_count,
                                               int64_t& region_count) {
  schema_count = schema_map_.Size();
  table_count = table_map_.Size();
  index_count = index_map_.Size();
  region_count = region_map_.Size();

  return butil::Status::OK();
}

}  // namespace dingodb

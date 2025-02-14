
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client_v2/meta.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <ostream>
#include <vector>

#include "butil/status.h"
#include "client/coordinator_client_function.h"
#include "client_v2/coordinator.h"
#include "client_v2/helper.h"
#include "client_v2/pretty.h"
#include "client_v2/store.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "coordinator/tso_control.h"
#include "fmt/core.h"
#include "nlohmann/json.hpp"
#include "nlohmann/json_fwd.hpp"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"

namespace client_v2 {

void SetUpMetaSubCommands(CLI::App &app) {
  SetUpCreateTable(app);
  SetUpGetTable(app);
  // SetUpGetTableRange(app); //not support
  SetUpGetTableByName(app);
  SetUpGenTso(app);

  SetUpGetSchemas(app);
  SetUpGetSchema(app);
  SetUpGetSchemaByName(app);
  SetUpGetTablesBySchema(app);
  SetUpCreateSchema(app);
  SetUpDropSchema(app);
  SetUpGetRegionByTable(app);

  // tenant
  SetUpCreateTenant(app);
  SetUpUpdateTenant(app);
  SetUpDropTenant(app);
  SetUpGetTenant(app);

  // backup and restore meta
  SetUpImportMeta(app);
  SetUpExportMeta(app);
}

dingodb::pb::meta::TableDefinitionWithId SendGetIndex(int64_t index_id) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto *mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(index_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTables", request, response);
  if (response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << fmt::format("GetTables failed, error: {} {}",
                                    dingodb::pb::error::Errno_Name(response.error().errcode()),
                                    response.error().errmsg());
    return {};
  }

  return response.table_definition_with_ids()[0];
}

dingodb::pb::meta::TableDefinitionWithId SendGetTable(int64_t table_id) {
  dingodb::pb::meta::GetTableRequest request;
  dingodb::pb::meta::GetTableResponse response;

  auto *mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTable", request, response);

  return response.table_definition_with_id();
}

dingodb::pb::meta::TableRange SendGetTableRange(int64_t table_id) {
  dingodb::pb::meta::GetTableRangeRequest request;
  dingodb::pb::meta::GetTableRangeResponse response;

  auto *mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTableRange", request, response);

  return response.table_range();
}

dingodb::pb::meta::IndexRange SendGetIndexRange(int64_t table_id) {
  dingodb::pb::meta::GetIndexRangeRequest request;
  dingodb::pb::meta::GetIndexRangeResponse response;

  auto *mut_index_id = request.mutable_index_id();
  mut_index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_index_id->set_entity_id(table_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetIndexRange", request, response);

  return response.index_range();
}

int GetCreateTableId(int64_t &table_id) {
  dingodb::pb::meta::CreateTableIdRequest request;
  dingodb::pb::meta::CreateTableIdResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  auto status =
      InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "CreateTableId", request, response);
  DINGO_LOG(INFO) << "SendRequestWithoutContext status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.has_table_id()) {
    table_id = response.table_id().entity_id();
    return 0;
  } else {
    return -1;
  }
}

dingodb::pb::meta::CreateTableRequest BuildCreateTableRequest(const std::string &table_name, int partition_num) {
  dingodb::pb::meta::CreateTableRequest request;

  int64_t new_table_id = 0;
  int ret = GetCreateTableId(new_table_id);
  if (ret != 0) {
    DINGO_LOG(WARNING) << "GetCreateTableId failed";
    return request;
  }

  uint32_t part_count = partition_num;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = 0;
    int ret = GetCreateTableId(new_part_id);
    if (ret != 0) {
      DINGO_LOG(WARNING) << "GetCreateTableId failed";
      return request;
    }
    part_ids.push_back(new_part_id);
  }

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  // setup table_id
  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(schema_id->entity_id());
  table_id->set_entity_id(new_table_id);

  // string name = 1;
  auto *table_definition = request.mutable_table_definition();
  table_definition->set_name(table_name);

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto *column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type("INT");
    column->set_element_type("INT");
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");
  }

  table_definition->set_version(1);
  table_definition->set_ttl(0);
  table_definition->set_engine(::dingodb::pb::common::Engine::ENG_ROCKSDB);
  auto *prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  auto *partition_rule = table_definition->mutable_table_partition();
  auto *part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");

  for (int i = 0; i < partition_num; i++) {
    auto *part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_table_id);
    part->mutable_range()->set_start_key(client_v2::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client_v2::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  return request;
}

int64_t SendCreateTable(const std::string &table_name, int partition_num) {
  auto request = BuildCreateTableRequest(table_name, partition_num);
  if (request.table_id().entity_id() == 0) {
    DINGO_LOG(WARNING) << "BuildCreateTableRequest failed";
    return 0;
  }

  dingodb::pb::meta::CreateTableResponse response;
  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "CreateTable", request, response);

  DINGO_LOG(INFO) << "response=" << response.DebugString();
  return response.table_id().entity_id();
}

void SendDropTable(int64_t table_id) {
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  auto *mut_table_id = request.mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  mut_table_id->set_entity_id(table_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "DropTable", request, response);
}

butil::Status SendGetTableByName(const std::string &table_name, int64_t &table_id) {
  dingodb::pb::meta::GetTableByNameRequest request;
  dingodb::pb::meta::GetTableByNameResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_table_name(table_name);

  auto status =
      InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTableByName", request, response);
  table_id = response.table_definition_with_id().table_id().entity_id();
  return status;
}

butil::Status SendGetTableByName(const std::string &table_name,
                                 dingodb::pb::meta::TableDefinitionWithId &table_definition) {
  dingodb::pb::meta::GetTableByNameRequest request;
  dingodb::pb::meta::GetTableByNameResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_table_name(table_name);

  auto status =
      InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTableByName", request, response);
  table_definition.CopyFrom(response.table_definition_with_id());
  return status;
}

std::vector<int64_t> SendGetTablesBySchema() {
  dingodb::pb::meta::GetTablesBySchemaRequest request;
  dingodb::pb::meta::GetTablesBySchemaResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  InteractionManager::GetInstance().SendRequestWithoutContext("MetaService", "GetTablesBySchema", request, response);

  std::vector<int64_t> table_ids;
  for (const auto &id : response.table_definition_with_ids()) {
    table_ids.push_back(id.table_id().entity_id());
  }

  return table_ids;
}

butil::Status SendGetSchema(int64_t tenant_id, int64_t schema_id, dingodb::pb::meta::Schema &schema) {
  dingodb::pb::meta::GetSchemaRequest request;
  dingodb::pb::meta::GetSchemaResponse response;
  request.set_tenant_id(tenant_id);

  auto *id = request.mutable_schema_id();
  id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  id->set_parent_entity_id(dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  id->set_entity_id(schema_id);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetSchema", request, response);
  schema = response.schema();
  return status;
}

butil::Status SendGetSchemas(int64_t tenant_id, std::vector<dingodb::pb::meta::Schema> &schemas) {
  dingodb::pb::meta::GetSchemasRequest request;
  dingodb::pb::meta::GetSchemasResponse response;

  request.set_tenant_id(tenant_id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetSchemas", request,
                                                                                                  response);
  schemas = dingodb::Helper::PbRepeatedToVector(response.schemas());
  return status;
}

// 740000000000000000+m+tenants|tenant:{tenantId}|schema:{schemaId}|table:{tableId}+h+tenant:{tenantId}|schema:{schemaIdId}|table:{tableId}|index:{indexId}
void ParseKey(const std::string &key, std::string &type, int64_t &parent_id, int64_t &entity_id) {
  // 740000000000000000+m
  int start_pos = 10;
  int ret;
  {
    std::string output;
    ret = Helper::DecodeBytes(key.substr(start_pos, key.size()), output);
    CHECK(ret != -1) << fmt::format("decode key({}) failed.", dingodb::Helper::StringToHex(key));
    std::vector<std::string> results;
    dingodb::Helper::SplitString(output, ':', results);
    CHECK((results.size() == 1 || results.size() == 2)) << "split string size wrong, output: " << output;
    parent_id = (results.size() == 2) ? std::stoll(results[1]) : 0;
  }

  {
    start_pos += ret + 1 + 8;
    std::string output;
    ret = Helper::DecodeBytes(key.substr(start_pos, key.size()), output);
    CHECK(ret != -1) << fmt::format("decode key({}) failed.", dingodb::Helper::StringToHex(key));
    std::vector<std::string> results;
    dingodb::Helper::SplitString(output, ':', results);
    CHECK(results.size() == 2) << "split string size wrong, output: " << output;
    type = results[0];
    entity_id = std::stoll(results[1]);
  }
}

struct MetaItem {
  std::string type;
  int64_t parent_id;
  int64_t entity_id;
  std::string value;
};

// get excutor table meta from store
// get meta region
// scan region
// decode key and value
butil::Status GetSqlMeta(std::vector<MetaItem> &metas) {
  // get meta region
  dingodb::pb::coordinator::ScanRegionsRequest request;
  dingodb::pb::coordinator::ScanRegionsResponse response;

  request.set_key(dingodb::Helper::HexToString("740000000000000000"));
  request.set_range_end(dingodb::Helper::HexToString("740000000000000001"));

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("ScanRegions", request, response);
  if (!status.ok()) {
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  // scan region
  std::vector<dingodb::pb::common::KeyValue> kvs;
  for (const auto &region_info : response.regions()) {
    dingodb::pb::common::Region region = SendQueryRegion(region_info.region_id());
    if (region.id() == 0) {
      DINGO_LOG(ERROR) << "GetRegion failed." << std::endl;
      continue;
    }

    auto response = SendTxnScanByStreamMode(region, region_info.range(), 1000, INT64_MAX, 0, false, false);
    for (const auto &kv : response.kvs()) {
      kvs.push_back(kv);
    }
  }

  // decode key and value
  for (const auto &kv : kvs) {
    MetaItem meta;
    ParseKey(kv.key(), meta.type, meta.parent_id, meta.entity_id);
    meta.value = kv.value();
    metas.push_back(meta);
  }

  return butil::Status::OK();
}

butil::Status GetSqlTableOrIndexMeta(int64_t table_id, dingodb::pb::meta::TableDefinitionWithId &table_definition) {
  std::vector<MetaItem> metas;
  butil::Status status = GetSqlMeta(metas);
  if (!status.ok()) {
    return status;
  }

  for (auto &meta : metas) {
    if ((meta.type == "Table" || meta.type == "Index") && meta.entity_id == table_id) {
      bool ret = table_definition.ParseFromArray(meta.value.data(), meta.value.size());
      CHECK(ret) << "parse table definition failed.";
      break;
    }
  }
  return butil::Status::OK();
}

butil::Status GetSqlTableOrIndexMeta(std::string table_name, int64_t schema_id,
                                     dingodb::pb::meta::TableDefinitionWithId &table_definition) {
  std::vector<MetaItem> metas;
  butil::Status status = GetSqlMeta(metas);
  if (!status.ok()) {
    return status;
  }

  for (auto &meta : metas) {
    dingodb::pb::meta::TableDefinitionWithId temp_definition;
    if ((meta.type == "Table" || meta.type == "Index") && meta.parent_id == schema_id) {
      bool ret = temp_definition.ParseFromArray(meta.value.data(), meta.value.size());
      CHECK(ret) << "parse table definition failed.";
    }
    if (temp_definition.table_definition().name() == table_name) {
      table_definition.CopyFrom(temp_definition);
      break;
    }
  }

  return butil::Status::OK();
}

butil::Status GetSqlSchemaMeta(int64_t tenant_id, int64_t schema_id, dingodb::pb::meta::Schema &schema) {
  std::vector<MetaItem> metas;
  butil::Status status = GetSqlMeta(metas);
  if (!status.ok()) {
    return status;
  }
  for (auto &meta : metas) {
    if (meta.type == "DB" && meta.entity_id == schema_id) {
      // {"tenantId":0,"name":"MYSQL","schemaId":50001,"schemaState":"PUBLIC"}
      auto schema_json = nlohmann::json::parse(meta.value);
      if (tenant_id == schema_json["tenantId"].get<int64_t>()) {
        auto *id = schema.mutable_id();
        id->set_entity_id(meta.entity_id);
        id->set_parent_entity_id(meta.parent_id);
        id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
        schema.set_name(schema_json["name"].get<std::string>());
        schema.set_tenant_id(schema_json["tenantId"].get<int64_t>());
        for (auto &meta : metas) {
          if (meta.type == "Table" && meta.parent_id == schema_id) {
            auto *table_ids = schema.add_table_ids();
            table_ids->set_entity_id(meta.entity_id);
            table_ids->set_parent_entity_id(meta.parent_id);
            table_ids->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
          } else if (meta.type == "Index" && meta.parent_id == schema_id) {
            auto *index_ids = schema.add_index_ids();
            index_ids->set_entity_id(meta.entity_id);
            index_ids->set_parent_entity_id(meta.parent_id);
            index_ids->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
          }
        }
        break;
      }
    }
  }

  return butil::Status::OK();
}

butil::Status GetSqlSchemasMeta(int64_t tenant_id, std::vector<dingodb::pb::meta::Schema> &schemas) {
  std::vector<MetaItem> metas;
  butil::Status status = GetSqlMeta(metas);
  if (!status.ok()) {
    return status;
  }
  for (auto &meta : metas) {
    if (meta.type == "DB") {
      // {"tenantId":0,"name":"MYSQL","schemaId":50001,"schemaState":"PUBLIC"}
      auto schema_json = nlohmann::json::parse(meta.value);
      if (tenant_id == schema_json["tenantId"].get<int64_t>()) {
        dingodb::pb::meta::Schema schema;
        auto *id = schema.mutable_id();
        id->set_entity_id(meta.entity_id);
        id->set_parent_entity_id(meta.parent_id);
        id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
        schema.set_name(schema_json["name"].get<std::string>());
        schema.set_tenant_id(schema_json["tenantId"].get<int64_t>());
        int64_t schema_id = schema_json["schemaId"].get<int64_t>();
        for (auto &meta : metas) {
          if (meta.type == "Table" && meta.parent_id == schema_id) {
            auto *table_ids = schema.add_table_ids();
            table_ids->set_entity_id(meta.entity_id);
            table_ids->set_parent_entity_id(meta.parent_id);
            table_ids->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
          } else if (meta.type == "Index" && meta.parent_id == schema_id) {
            auto *index_ids = schema.add_index_ids();
            index_ids->set_entity_id(meta.entity_id);
            index_ids->set_parent_entity_id(meta.parent_id);
            index_ids->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
          }
        }
        schemas.push_back(schema);
      }
    }
  }

  return butil::Status::OK();
}
butil::Status GetTableOrIndexDefinition(int64_t id, dingodb::pb::meta::TableDefinition &table_definition) {
  dingodb::pb::meta::TableDefinitionWithId table_definition_with_id;
  auto status = GetSqlTableOrIndexMeta(id, table_definition_with_id);
  if (!status.ok()) {
    return status;
  }

  if (table_definition_with_id.table_id().entity_id() > 0) {
    table_definition = table_definition_with_id.table_definition();
    return butil::Status::OK();
  }

  table_definition_with_id = SendGetTable(id);
  if (table_definition_with_id.table_definition().name().empty()) {
    table_definition_with_id = SendGetIndex(id);
  }
  table_definition = table_definition_with_id.table_definition();
  return butil::Status::OK();
}

butil::Status GetTableOrIndexDefinition(int64_t id,
                                        dingodb::pb::meta::TableDefinitionWithId &table_definition_with_id) {
  auto status = GetSqlTableOrIndexMeta(id, table_definition_with_id);
  if (!status.ok()) {
    return status;
  }

  if (table_definition_with_id.table_id().entity_id() > 0) {
    return butil::Status::OK();
  }

  table_definition_with_id = SendGetTable(id);
  if (table_definition_with_id.table_id().entity_id() == 0) {
    table_definition_with_id = SendGetIndex(id);
  }
  if (table_definition_with_id.table_id().entity_id() == 0) {
    return butil::Status(dingodb::pb::error::ETABLE_NOT_FOUND, "Not find table");
  }
  return butil::Status::OK();
}

butil::Status GetTableOrIndexDefinition(std::string table_name, int64_t schema_id,
                                        dingodb::pb::meta::TableDefinitionWithId &table_definition_with_id) {
  auto status = GetSqlTableOrIndexMeta(table_name, schema_id, table_definition_with_id);
  if (!status.ok()) {
    return status;
  }

  if (table_definition_with_id.table_id().entity_id() > 0) {
    return butil::Status::OK();
  }

  SendGetTableByName(table_name, table_definition_with_id);
  if (table_definition_with_id.table_id().entity_id() == 0) {
    return butil::Status(dingodb::pb::error::ETABLE_NOT_FOUND, "Not find table");
  }
  return butil::Status::OK();
}

butil::Status GetSchemaDefinition(int64_t tenant_id, int64_t schema_id, dingodb::pb::meta::Schema &schema) {
  auto status = GetSqlSchemaMeta(tenant_id, schema_id, schema);
  if (!status.ok()) {
    return status;
  }

  if (schema.id().entity_id() > 0) {
    return butil::Status::OK();
  }

  status = SendGetSchema(tenant_id, schema_id, schema);
  if (!status.ok()) {
    return status;
  }
  if (schema.id().entity_id() == 0) {
    return butil::Status(dingodb::pb::error::ESCHEMA_NOT_FOUND, "Not find schema");
  }
  return butil::Status::OK();
}
butil::Status GetSchemasDefinition(int64_t tenant_id, std::vector<dingodb::pb::meta::Schema> &schemas) {
  auto status = GetSqlSchemasMeta(tenant_id, schemas);
  if (!status.ok()) {
    return status;
  }

  if (schemas.size() > 0) {
    return butil::Status::OK();
  }

  status = SendGetSchemas(tenant_id, schemas);
  if (schemas.size() == 0) {
    return butil::Status(dingodb::pb::error::ESCHEMA_NOT_FOUND, "Not find schema");
  }
  return butil::Status::OK();
}
void SetUpMetaHello(CLI::App &app) {
  auto opt = std::make_shared<MetaHelloOptions>();
  auto *cmd = app.add_subcommand("MetaHello", "Meta hello")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->callback([opt]() { RunMetaHello(*opt); });
}

void RunMetaHello(MetaHelloOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::HelloRequest request;
  dingodb::pb::meta::HelloResponse response;

  std::string const key = "Hello";

  request.set_hello(0);
  request.set_get_memory_info(true);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("Hello", request, response);
  DINGO_LOG(INFO) << "SendRequest status: " << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetTenant(CLI::App &app) {
  auto opt = std::make_shared<GetTenantOptions>();
  auto *cmd = app.add_subcommand("GetTenant", "Get tenant")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--source", opt->source, "tenant source 0 means from coordinator, 1 means from executor")
      ->default_val(0);
  cmd->add_option("--tenant_id", opt->tenant_id, "tenant id, 0 means query all tenants")->default_val(0);
  cmd->callback([opt]() { RunGetTenant(*opt); });
}

void RunGetTenant(GetTenantOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  if (opt.source == 1) {
    std::vector<MetaItem> metas;
    auto status = GetSqlMeta(metas);
    if (!status.ok()) {
      Pretty::ShowError(status);
      return;
    }
    std::vector<Pretty::TenantInfo> tenants;
    for (const auto &meta : metas) {
      if (meta.type == "tenant") {
        // {"id":0,"name":"root","comment":null,"createdTime":0,"updatedTime":0}
        auto tenant_json = nlohmann::json::parse(meta.value);
        auto name = tenant_json["name"].is_null() ? "" : tenant_json["name"].get<std::string>();
        auto comment = tenant_json["comment"].is_null() ? "" : tenant_json["comment"].get<std::string>();
        auto crate_time = tenant_json["createdTime"].is_null() ? 0 : tenant_json["createdTime"].get<int64_t>();
        auto update_time = tenant_json["updatedTime"].is_null() ? 0 : tenant_json["updatedTime"].get<int64_t>();

        if (opt.tenant_id == 0) {
          tenants.push_back({tenant_json["id"].get<int64_t>(), name, comment, crate_time, update_time});
        } else if (opt.tenant_id == tenant_json["id"].get<int64_t>()) {
          tenants.push_back({tenant_json["id"].get<int64_t>(), name, comment, crate_time, update_time});
        }
      } else if (meta.type == "DB") {
        // {"tenantId":0,"name":"MYSQL","schemaId":50001,"schemaState":"PUBLIC"}
        auto schema_json = nlohmann::json::parse(meta.value);
      }
    }

    Pretty::Show(tenants);
  } else {
    dingodb::pb::meta::GetTenantsRequest request;
    dingodb::pb::meta::GetTenantsResponse response;

    if (opt.tenant_id > 0) {
      request.add_tenant_ids(opt.tenant_id);
    }
    auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetTenants",
                                                                                                    request, response);
    Pretty::Show(response);
  }
}

void SetUpGetSchema(CLI::App &app) {
  auto opt = std::make_shared<GetSchemaOptions>();
  auto *cmd = app.add_subcommand("GetSchema", "Get schema ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")->default_val(0);
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->required();
  cmd->callback([opt]() { RunGetSchema(*opt); });
}

void RunGetSchema(GetSchemaOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::Schema schema;
  auto status = GetSchemaDefinition(opt.tenant_id, opt.schema_id, schema);
  if (Pretty::ShowError(status)) {
    return;
  }
  Pretty::ShowSchemas({schema});
}

void SetUpGetSchemas(CLI::App &app) {
  auto opt = std::make_shared<GetSchemasOptions>();
  auto *cmd = app.add_subcommand("GetSchemas", "Get schemas")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")->default_val(0);
  cmd->callback([opt]() { RunGetSchemas(*opt); });
}

void RunGetSchemas(GetSchemasOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  std::vector<dingodb::pb::meta::Schema> schemas;
  auto status = GetSchemasDefinition(opt.tenant_id, schemas);
  if (Pretty::ShowError(status)) {
    return;
  }
  Pretty::ShowSchemas(schemas);
}

void SetUpGetSchemaByName(CLI::App &app) {
  auto opt = std::make_shared<GetSchemaByNameOptions>();
  auto *cmd = app.add_subcommand("GetSchemaByName", "Get schema by name")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")->default_val(0);
  cmd->add_option("--name", opt->name, "Request parameter schema name")->required();
  cmd->callback([opt]() { RunGetSchemaByName(*opt); });
}

void RunGetSchemaByName(GetSchemaByNameOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetSchemaByNameRequest request;
  dingodb::pb::meta::GetSchemaByNameResponse response;

  request.set_schema_name(opt.name);
  request.set_tenant_id(opt.tenant_id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetSchemaByName",
                                                                                                  request, response);
  Pretty::Show(response);
}

void SetUpGetTablesBySchema(CLI::App &app) {
  auto opt = std::make_shared<GetTablesBySchemaOptions>();
  auto *cmd = app.add_subcommand("GetTablesBySchema", "Get schema by name")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->required();

  cmd->callback([opt]() { RunGetTablesBySchema(*opt); });
}

void RunGetTablesBySchema(GetTablesBySchemaOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetTablesBySchemaRequest request;
  dingodb::pb::meta::GetTablesBySchemaResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (opt.schema_id != 0) {
    schema_id->set_entity_id(opt.schema_id);
  }

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetTablesBySchema",
                                                                                                  request, response);
  Pretty::Show(response);
  std::cout << "\n Summary: table_count=" << response.table_definition_with_ids_size() << std::endl;
}

void SetUpGetTablesCount(CLI::App &app) {
  auto opt = std::make_shared<GetTablesCountOptions>();
  auto *cmd = app.add_subcommand("GetTablesCount", "Get tables count")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->required();

  cmd->callback([opt]() { RunGetTablesCount(*opt); });
}

void RunGetTablesCount(GetTablesCountOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetTablesCountRequest request;
  dingodb::pb::meta::GetTablesCountResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  schema_id->set_entity_id(opt.schema_id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetTablesCount",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "table_count=" << response.tables_count();
}

void SetUpCreateTable(CLI::App &app) {
  auto opt = std::make_shared<CreateTableOptions>();
  auto *cmd = app.add_subcommand("CreateTable", "Create table")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--name", opt->name, "Request parameter table name")->required();
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")
      ->default_val(1)
      ->check(CLI::Range(0, std::numeric_limits<int32_t>::max()));
  cmd->add_flag("--enable_rocks_engine", opt->enable_rocks_engine, "Request parameter enable rocks engine for store")
      ->default_val(false);
  cmd->add_flag("--with_increment", opt->with_increment, "Request parameter with_increment")->default_val(false);
  cmd->add_option("--engine", opt->engine, "Request parameter engine, Must be rocksdb|bdb")->default_val("rocksdb");
  cmd->add_option("--replica", opt->replica, "Request parameter replica")
      ->default_val(3)
      ->check(CLI::Range(0, std::numeric_limits<int32_t>::max()));

  cmd->callback([opt]() { RunCreateTable(*opt); });
}

void RunCreateTable(CreateTableOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateTableRequest request;
  dingodb::pb::meta::CreateTableResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  if (opt.schema_id > 0) {
    schema_id->set_entity_id(opt.schema_id);
  }

  uint32_t part_count = opt.part_count;
  if (part_count == 0) {
    part_count = 1;
  }
  std::vector<int64_t> new_ids;
  int ret = client_v2::Helper::GetCreateTableIds(CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta(),
                                                 1 + opt.part_count, new_ids);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    std::cout << "GetCreateTableIds failed";
    return;
  }
  if (new_ids.empty()) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    std::cout << "GetCreateTableIds failed, new_ids is empty";
    return;
  }
  if (new_ids.size() != 1 + opt.part_count) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    std::cout << "GetCreateTableIds failed, new_ids size not equal part count";
    return;
  }

  int64_t new_table_id = new_ids.at(0);
  DINGO_LOG(INFO) << "table_id = " << new_table_id;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = new_ids.at(1 + i);
    part_ids.push_back(new_part_id);
  }

  for (const auto &id : part_ids) {
    DINGO_LOG(INFO) << "part_id = " << id;
  }

  // setup table_id
  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(schema_id->entity_id());
  table_id->set_entity_id(new_table_id);

  // string name = 1;
  auto *table_definition = request.mutable_table_definition();
  table_definition->set_name(Helper::ToUpperCase(opt.name));

  table_definition->set_replica(opt.replica);
  if (opt.enable_rocks_engine) {
    table_definition->set_store_engine(::dingodb::pb::common::StorageEngine::STORE_ENG_MONO_STORE);
  } else {
    table_definition->set_store_engine(::dingodb::pb::common::StorageEngine::STORE_ENG_RAFT_STORE);
  }
  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto *column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type("BIGINT");
    column->set_element_type("BIGINT");
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");

    if (opt.with_increment && i == 0) {
      column->set_is_auto_increment(true);
    }
  }
  if (opt.with_increment) {
    table_definition->set_auto_increment(100);
  }

  // map<string, Index> indexes = 3;
  // uint32 version = 4;
  table_definition->set_version(1);
  // uint64 ttl = 5;
  table_definition->set_ttl(0);
  // PartitionRule table_partition = 6;
  // Engine engine = 7;
  if (opt.engine != "rocksdb" && opt.engine != "bdb") {
    DINGO_LOG(ERROR) << "engine must be rocksdb or bdb";
    std::cout << "engine must be rocksdb or bdb";
    return;
  }
  table_definition->set_engine(client_v2::Helper::GetEngine(opt.engine));
  // map<string, string> properties = 8;
  auto *prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add partition_rule
  auto *partition_rule = table_definition->mutable_table_partition();
  auto *part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");

  for (int i = 0; i < part_count; i++) {
    auto *part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_table_id);
    part->mutable_range()->set_start_key(client_v2::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client_v2::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  request.mutable_request_info()->set_request_id(1024);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateTable",
                                                                                                  request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "create table failed, error: "
                     << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name()
                     << " " << response.error().errmsg();
    std::cout << "create table failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "create table success, table_id==" << response.table_id().entity_id() << std::endl;
}

void SetUpCreateTableIds(CLI::App &app) {
  auto opt = std::make_shared<CreateTableIdsOptions>();
  auto *cmd = app.add_subcommand("CreateTableIds", "Create tableIds")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")->default_val(1)->required();

  cmd->callback([opt]() { RunCreateTableIds(*opt); });
}

void RunCreateTableIds(CreateTableIdsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateTableIdsRequest request;
  dingodb::pb::meta::CreateTableIdsResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_count(opt.part_count);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateTableIds",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
  DINGO_LOG(INFO) << "count = " << response.table_ids_size();
}

void SetUpCreateTableId(CLI::App &app) {
  auto opt = std::make_shared<CreateTableIdOptions>();
  auto *cmd = app.add_subcommand("CreateTableIds", "Create tableIds")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->callback([opt]() { RunCreateTableId(*opt); });
}

void RunCreateTableId(CreateTableIdOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateTableIdRequest request;
  dingodb::pb::meta::CreateTableIdResponse response;
  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateTableId",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDropTable(CLI::App &app) {
  auto opt = std::make_shared<DropTableOptions>();
  auto *cmd = app.add_subcommand("DropTable", "Drop Table")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->required();
  cmd->callback([opt]() { RunDropTable(*opt); });
}

void RunDropTable(DropTableOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (opt.schema_id > 0) {
    table_id->set_parent_entity_id(opt.schema_id);
  } else {
    table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  }

  table_id->set_entity_id(opt.id);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("DropTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "drop schema  " << opt.schema_id << " table " << opt.id << " failed, error: "
                     << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name()
                     << " " << response.error().errmsg();
  } else {
    DINGO_LOG(INFO) << "drop schema " << opt.schema_id << " table  " << opt.id << " success";
    std::cout << "drop schema " << opt.schema_id << " table " << opt.id << "  success" << std::endl;
  }
}

void SetUpCreateSchema(CLI::App &app) {
  auto opt = std::make_shared<CreateSchemaOptions>();
  auto *cmd = app.add_subcommand("CreateSchema", "Create schema")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--name", opt->name, "Request parameter schema name")->required();
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")->required();
  cmd->callback([opt]() { RunCreateSchema(*opt); });
}

void RunCreateSchema(CreateSchemaOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateSchemaRequest request;
  dingodb::pb::meta::CreateSchemaResponse response;

  request.set_tenant_id(opt.tenant_id);

  auto *parent_schema_id = request.mutable_parent_schema_id();
  parent_schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  parent_schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  parent_schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_schema_name(opt.name);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateSchema",
                                                                                                  request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "create schema failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Create schema success." << std::endl;
}

void SetUpDropSchema(CLI::App &app) {
  auto opt = std::make_shared<DropSchemaOptions>();
  auto *cmd = app.add_subcommand("DropSchema", "Drop schema")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->required();
  cmd->callback([opt]() { RunDropSchema(*opt); });
}

void RunDropSchema(DropSchemaOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::DropSchemaRequest request;
  dingodb::pb::meta::DropSchemaResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(opt.schema_id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("DropSchema", request,
                                                                                                  response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "drop schema failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Drop schema: " << opt.schema_id << " success." << std::endl;
}

void SetUpGetTable(CLI::App &app) {
  auto opt = std::make_shared<GetTableOptions>();
  auto *cmd = app.add_subcommand("GetTable", "Get table")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")
      ->check(CLI::Range(1, std::numeric_limits<int32_t>::max()))
      ->required();
  cmd->add_option("--is_index", opt->is_index, "Request parameter is_index")->default_val(false)->default_str("false");
  cmd->callback([opt]() { RunGetTable(*opt); });
}

void RunGetTable(GetTableOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::TableDefinitionWithId table_definition_with_id;
  auto status = GetTableOrIndexDefinition(opt.id, table_definition_with_id);
  if (Pretty::ShowError(status)) {
    return;
  }
  Pretty::Show(table_definition_with_id);
}

void SetUpGetTableByName(CLI::App &app) {
  auto opt = std::make_shared<GetTableByNameOptions>();
  auto *cmd = app.add_subcommand("GetTableByName", "Get table by name")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema_id")->required();
  cmd->add_option("--name", opt->name, "Request parameter name")->required()->transform([](const std::string &str) {
    return Helper::ToUpperCase(str);
  });
  cmd->callback([opt]() { RunGetTableByName(*opt); });
}

void RunGetTableByName(GetTableByNameOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::TableDefinitionWithId table_definition_with_id;
  auto status = GetTableOrIndexDefinition(opt.name, opt.schema_id, table_definition_with_id);
  if (Pretty::ShowError(status)) {
    return;
  }
  Pretty::Show(table_definition_with_id);
}

void SetUpGetTableRange(CLI::App &app) {
  auto opt = std::make_shared<GetTableRangeOptions>();
  auto *cmd = app.add_subcommand("GetTableRange", "Get table range")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->callback([opt]() { RunGetTableRange(*opt); });
}

void RunGetTableRange(GetTableRangeOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetTableRangeRequest request;
  dingodb::pb::meta::GetTableRangeResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  table_id->set_entity_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetTableRange",
                                                                                                  request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "get table failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }

  for (const auto &it : response.table_range().range_distribution()) {
    std::cout << "region_id=[" << it.id().entity_id() << "]" << "range=["
              << dingodb::Helper::StringToHex(it.range().start_key()) << ","
              << dingodb::Helper::StringToHex(it.range().end_key()) << "]" << " leader=[" << it.leader().host() << ":"
              << it.leader().port() << "]" << std::endl;
  }
}

void SetUpGetTableMetrics(CLI::App &app) {
  auto opt = std::make_shared<GetTableMetricsOptions>();
  auto *cmd = app.add_subcommand("GetTableMetrics", "Get table metrics")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->callback([opt]() { RunGetTableMetrics(*opt); });
}

void RunGetTableMetrics(GetTableMetricsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetTableMetricsRequest request;
  dingodb::pb::meta::GetTableMetricsResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  table_id->set_entity_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetTableMetrics",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSwitchAutoSplit(CLI::App &app) {
  auto opt = std::make_shared<SwitchAutoSplitOptions>();
  auto *cmd = app.add_subcommand("SwitchAutoSplit", "Switch auto split")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->required();
  cmd->add_flag("--auto_split", opt->auto_split, "Request parameter auto_split")->required();
  cmd->callback([opt]() { RunSwitchAutoSplit(*opt); });
}

void RunSwitchAutoSplit(SwitchAutoSplitOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::SwitchAutoSplitRequest request;
  dingodb::pb::meta::SwitchAutoSplitResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  table_id->set_parent_entity_id(opt.schema_id);
  table_id->set_entity_id(opt.id);

  request.set_auto_split(opt.auto_split);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("SwitchAutoSplit",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpGetDeletedTable(CLI::App &app) {
  auto opt = std::make_shared<GetDeletedTableOptions>();
  auto *cmd = app.add_subcommand("GetDeletedTable", "Get deleted table")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->callback([opt]() { RunGetDeletedTable(*opt); });
}

void RunGetDeletedTable(GetDeletedTableOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetDeletedTableRequest request;
  dingodb::pb::meta::GetDeletedTableResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  table_id->set_entity_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetDeletedTable",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto &table : response.table_definition_with_ids()) {
    DINGO_LOG(INFO) << "table_id=[" << table.table_id().entity_id() << "]" << "table_name=["
                    << table.table_definition().name() << "]" << " detail: " << table.ShortDebugString();
  }
  DINGO_LOG(INFO) << "Deleted table count=" << response.table_definition_with_ids_size();
}

void SetUpGetDeletedIndex(CLI::App &app) {
  auto opt = std::make_shared<GetDeletedIndexOptions>();
  auto *cmd = app.add_subcommand("GetDeletedIndex", "Get deleted index")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->callback([opt]() { RunGetDeletedIndex(*opt); });
}

void RunGetDeletedIndex(GetDeletedIndexOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetDeletedIndexRequest request;
  dingodb::pb::meta::GetDeletedIndexResponse response;

  auto *index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  index_id->set_entity_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetDeletedIndex",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto &index : response.table_definition_with_ids()) {
    DINGO_LOG(INFO) << "index_id=[" << index.table_id().entity_id() << "]" << "index_name=["
                    << index.table_definition().name() << "]" << " detail: " << index.ShortDebugString();
  }
  DINGO_LOG(INFO) << "Deleted index count=" << response.table_definition_with_ids_size();
}

void SetUpCleanDeletedTable(CLI::App &app) {
  auto opt = std::make_shared<CleanDeletedTableOptions>();
  auto *cmd = app.add_subcommand("CleanDeletedTable", "Clean deleted table")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->callback([opt]() { RunCleanDeletedTable(*opt); });
}

void RunCleanDeletedTable(CleanDeletedTableOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CleanDeletedTableRequest request;
  dingodb::pb::meta::CleanDeletedTableResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  table_id->set_entity_id(opt.id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CleanDeletedTable",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpCleanDeletedIndex(CLI::App &app) {
  auto opt = std::make_shared<CleanDeletedIndexOptions>();
  auto *cmd = app.add_subcommand("CleanDeletedIndex", "Clean deleted index")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter index id")->required();
  cmd->callback([opt]() { RunCleanDeletedIndex(*opt); });
}

void RunCleanDeletedIndex(CleanDeletedIndexOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CleanDeletedIndexRequest request;
  dingodb::pb::meta::CleanDeletedIndexResponse response;

  auto *index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  index_id->set_entity_id(opt.id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CleanDeletedIndex",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpCreateTenant(CLI::App &app) {
  auto opt = std::make_shared<CreateTenantOptions>();
  auto *cmd = app.add_subcommand("CreateTenant", "Create tenant")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--name", opt->name, "Request parameter name")->required();
  cmd->add_option("--comment", opt->comment, "Request parameter comment")->required();
  cmd->add_option("--id", opt->tenant_id, "Request parameter tenant id")->required();
  cmd->callback([opt]() { RunCreateTenant(*opt); });
}

void RunCreateTenant(CreateTenantOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateTenantRequest request;
  dingodb::pb::meta::CreateTenantResponse response;
  request.mutable_tenant()->set_id(opt.tenant_id);
  request.mutable_tenant()->set_name(opt.name);
  request.mutable_tenant()->set_comment(opt.comment);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateTenant",
                                                                                                  request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "create tenant failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Create tenant: " << opt.tenant_id << " , success." << std::endl;
}

void SetUpUpdateTenant(CLI::App &app) {
  auto opt = std::make_shared<UpdateTenantOptions>();
  auto *cmd = app.add_subcommand("UpdateTenant", "Update tenant")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--name", opt->name, "Request parameter name")->required();
  cmd->add_option("--comment", opt->comment, "Request parameter comment")->required();
  cmd->add_option("--id", opt->tenant_id, "Request parameter tenant id")->required();
  cmd->callback([opt]() { RunUpdateTenant(*opt); });
}

void RunUpdateTenant(UpdateTenantOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::UpdateTenantRequest request;
  dingodb::pb::meta::UpdateTenantResponse response;
  request.mutable_tenant()->set_id(opt.tenant_id);
  request.mutable_tenant()->set_name(opt.name);
  request.mutable_tenant()->set_comment(opt.comment);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("UpdateTenant",
                                                                                                  request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "update tenant failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Update tenant: " << opt.tenant_id << " , success." << std::endl;
}

void SetUpDropTenant(CLI::App &app) {
  auto opt = std::make_shared<DropTenantOptions>();
  auto *cmd = app.add_subcommand("DropTenant", "Drop tenant")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->tenant_id, "Request parameter tenant id")->required();
  cmd->callback([opt]() { RunDropTenant(*opt); });
}

void RunDropTenant(DropTenantOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::DropTenantRequest request;
  dingodb::pb::meta::DropTenantResponse response;

  request.set_tenant_id(opt.tenant_id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("DropTenant", request,
                                                                                                  response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "drop tanent failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Drop tenant: " << opt.tenant_id << " , success." << std::endl;
}

void SetUpGetIndexes(CLI::App &app) {
  auto opt = std::make_shared<GetIndexesOptions>();
  auto *cmd = app.add_subcommand("GetIndexes", "Get index")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->required();
  cmd->callback([opt]() { RunGetIndexes(*opt); });
}

void RunGetIndexes(GetIndexesOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetIndexesRequest request;
  dingodb::pb::meta::GetIndexesResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  schema_id->set_entity_id(opt.schema_id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetIndexes", request,
                                                                                                  response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  // DINGO_LOG(INFO) << response.DebugString();

  for (const auto &index_definition_with_id : response.index_definition_with_ids()) {
    DINGO_LOG(INFO) << "index_id=[" << index_definition_with_id.index_id().entity_id() << "]" << "index_name=["
                    << index_definition_with_id.index_definition().name() << "], index_type=["
                    << dingodb::pb::common::IndexType_Name(
                           index_definition_with_id.index_definition().index_parameter().index_type())
                    << "]";
  }

  DINGO_LOG(INFO) << "index_count=" << response.index_definition_with_ids_size();
}

void SetUpGetIndexesCount(CLI::App &app) {
  auto opt = std::make_shared<GetIndexesCountOptions>();
  auto *cmd = app.add_subcommand("GetIndexesCount", "Get index count")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->required();
  cmd->callback([opt]() { RunGetIndexesCount(*opt); });
}

void RunGetIndexesCount(GetIndexesCountOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetIndexesCountRequest request;
  dingodb::pb::meta::GetIndexesCountResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  schema_id->set_entity_id(opt.schema_id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetIndexesCount",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "index_count=" << response.indexes_count();
}

void SetUpCreateIndexId(CLI::App &app) {
  auto opt = std::make_shared<CreateIndexIdOptions>();
  auto *cmd = app.add_subcommand("CreateIndexId", "Create index id ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->callback([opt]() { RunCreateIndexId(*opt); });
}
void RunCreateIndexId(CreateIndexIdOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateIndexIdRequest request;
  dingodb::pb::meta::CreateIndexIdResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateIndexId",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateIndex(CLI::App &app) {
  auto opt = std::make_shared<UpdateIndexOptions>();
  auto *cmd = app.add_subcommand("UpdateIndex", "Update Index")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter index id");
  cmd->add_option("--max_elements", opt->max_elements, "Request parameter max_elements");
  cmd->callback([opt]() { RunUpdateIndex(*opt); });
}
void RunUpdateIndex(UpdateIndexOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetIndexRequest get_request;
  dingodb::pb::meta::GetIndexResponse get_response;

  auto *index_id = get_request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  index_id->set_entity_id(opt.id);

  if (opt.max_elements <= 0) {
    DINGO_LOG(WARNING) << "max_elements is empty, this max_elements";
    return;
  }

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest(
      "GetIndex", get_request, get_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << get_response.DebugString();

  DINGO_LOG(INFO) << "index_id=[" << get_response.index_definition_with_id().index_id().entity_id() << "]"
                  << "index_name=[" << get_response.index_definition_with_id().index_definition().name()
                  << "], index_type=["
                  << dingodb::pb::common::IndexType_Name(
                         get_response.index_definition_with_id().index_definition().index_parameter().index_type())
                  << "]";
  auto old_count = get_response.index_definition_with_id()
                       .index_definition()
                       .index_parameter()
                       .vector_index_parameter()
                       .hnsw_parameter()
                       .max_elements();

  DINGO_LOG(INFO) << "index_count=" << old_count;

  if (old_count <= 0) {
    DINGO_LOG(WARNING) << "old_count is illegal, stop to update";
    return;
  }

  dingodb::pb::meta::UpdateIndexRequest update_request;
  dingodb::pb::meta::UpdateIndexResponse update_response;

  *(update_request.mutable_index_id()) = get_response.index_definition_with_id().index_id();
  *(update_request.mutable_new_index_definition()) = get_response.index_definition_with_id().index_definition();
  update_request.mutable_new_index_definition()
      ->mutable_index_parameter()
      ->mutable_vector_index_parameter()
      ->mutable_hnsw_parameter()
      ->set_max_elements(opt.max_elements);

  status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest(
      "UpdateIndex", update_request, update_response);

  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << update_response.DebugString();
}

void SetUpDropIndex(CLI::App &app) {
  auto opt = std::make_shared<DropIndexOptions>();
  auto *cmd = app.add_subcommand("DropIndex", "Drop Index")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter index id");
  cmd->callback([opt]() { RunDropIndex(*opt); });
}
void RunDropIndex(DropIndexOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::DropIndexRequest request;
  dingodb::pb::meta::DropIndexResponse response;

  auto *index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  index_id->set_entity_id(opt.id);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("DropIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetIndex(CLI::App &app) {
  auto opt = std::make_shared<GetIndexOptions>();
  auto *cmd = app.add_subcommand("GetIndex", "Get Index")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter index id");
  cmd->callback([opt]() { RunGetIndex(*opt); });
}
void RunGetIndex(GetIndexOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetIndexRequest request;
  dingodb::pb::meta::GetIndexResponse response;

  auto *index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  index_id->set_entity_id(opt.id);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetIndexByName(CLI::App &app) {
  auto opt = std::make_shared<GetIndexByNameOptions>();
  auto *cmd = app.add_subcommand("GetIndexByName", "Get index by name")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--name", opt->name, "Request parameter index name");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id");
  cmd->callback([opt]() { RunGetIndexByName(*opt); });
}
void RunGetIndexByName(GetIndexByNameOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetIndexByNameRequest request;
  dingodb::pb::meta::GetIndexByNameResponse response;

  request.set_index_name(opt.name);
  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_id(opt.schema_id);
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetIndexByName",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetIndexRange(CLI::App &app) {
  auto opt = std::make_shared<GetIndexRangeOptions>();
  auto *cmd = app.add_subcommand("GetIndexRange", "Get index range ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter index id");
  cmd->callback([opt]() { RunGetIndexRange(*opt); });
}
void RunGetIndexRange(GetIndexRangeOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetIndexRangeRequest request;
  dingodb::pb::meta::GetIndexRangeResponse response;

  auto *index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  index_id->set_entity_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetIndexRange",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  for (const auto &it : response.index_range().range_distribution()) {
    DINGO_LOG(INFO) << "region_id=[" << it.id().entity_id() << "]" << "range=["
                    << dingodb::Helper::StringToHex(it.range().start_key()) << ","
                    << dingodb::Helper::StringToHex(it.range().end_key()) << "]" << " leader=[" << it.leader().host()
                    << ":" << it.leader().port() << "]";
  }
}

void SetUpGetIndexMetrics(CLI::App &app) {
  auto opt = std::make_shared<GetIndexMetricsOptions>();
  auto *cmd = app.add_subcommand("GetIndexMetrics", "Get index metrics")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter index id");
  cmd->callback([opt]() { RunGetIndexMetrics(*opt); });
}
void RunGetIndexMetrics(GetIndexMetricsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetIndexMetricsRequest request;
  dingodb::pb::meta::GetIndexMetricsResponse response;

  auto *index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  index_id->set_entity_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetIndexMetrics",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGenerateTableIds(CLI::App &app) {
  auto opt = std::make_shared<GenerateTableIdsOptions>();
  auto *cmd = app.add_subcommand("GenerateTableIds", "Generate table ids ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id");
  cmd->callback([opt]() { RunGenerateTableIds(*opt); });
}
void RunGenerateTableIds(GenerateTableIdsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GenerateTableIdsRequest request;
  dingodb::pb::meta::GenerateTableIdsResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (opt.schema_id > 0) {
    schema_id->set_entity_id(opt.schema_id);
  }

  dingodb::pb::meta::TableWithPartCount *count = request.mutable_count();
  count->set_has_table(true);
  count->set_table_part_count(3);
  uint32_t index_count = 2;
  count->set_index_count(index_count);
  for (int i = 0; i < index_count; i++) {
    count->add_index_part_count(4);
  }

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GenerateTableIds",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpCreateTables(CLI::App &app) {
  auto opt = std::make_shared<CreateTablesOptions>();
  auto *cmd = app.add_subcommand("GenerateTableIds", "Generate table ids ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id");
  cmd->add_option("--name", opt->name, "Request parameter name")->required();
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")->default_val(1)->required();
  cmd->add_option("--engine", opt->engine, "Request parameter engine, Must be rocksdb|bdb")->required();
  cmd->add_option("--replica", opt->replica, "Request parameter replica")->default_val(3)->required();
  cmd->callback([opt]() { RunCreateTables(*opt); });
}
void RunCreateTables(CreateTablesOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  bool with_table_id = false;
  bool with_increment = false;

  dingodb::pb::meta::CreateTablesRequest request;
  dingodb::pb::meta::CreateTablesResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  if (opt.schema_id > 0) {
    schema_id->set_entity_id(opt.schema_id);
  }

  uint32_t part_count = opt.part_count;

  std::vector<int64_t> new_ids;
  int ret = client_v2::Helper::GetCreateTableIds(CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta(),
                                                 1 + opt.part_count, new_ids);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }
  if (new_ids.empty()) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }
  if (new_ids.size() != 1 + opt.part_count) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }

  int64_t new_table_id = new_ids.at(0);
  DINGO_LOG(INFO) << "table_id = " << new_table_id;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = new_ids.at(1 + i);
    part_ids.push_back(new_part_id);
  }

  auto *definition_with_id = request.add_table_definition_with_ids();
  auto *table_id = definition_with_id->mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(schema_id->entity_id());
  table_id->set_entity_id(new_table_id);

  // string name = 1;
  auto *table_definition = definition_with_id->mutable_table_definition();
  table_definition->set_name(opt.name);

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto *column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type("BIGINT");
    column->set_element_type("BIGINT");
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");

    if (with_increment && i == 0) {
      column->set_is_auto_increment(true);
    }
  }
  if (with_increment) {
    table_definition->set_auto_increment(100);
  }

  // map<string, Index> indexes = 3;
  // uint32 version = 4;
  table_definition->set_version(1);
  // uint64 ttl = 5;
  table_definition->set_ttl(0);
  // PartitionRule table_partition = 6;
  // Engine engine = 7;
  if (opt.engine != "rocksdb" && opt.engine != "bdb") {
    DINGO_LOG(ERROR) << "engine must be rocksdb or bdb";
    return;
  }
  table_definition->set_engine(client_v2::Helper::GetEngine(opt.engine));
  // map<string, string> properties = 8;
  auto *prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add partition_rule
  // repeated string columns = 1;
  // PartitionStrategy strategy = 2;
  auto *partition_rule = table_definition->mutable_table_partition();
  auto *part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  for (int i = 0; i < part_count; i++) {
    auto *part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_table_id);
    part->mutable_range()->set_start_key(client_v2::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client_v2::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateTables",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateTables(CLI::App &app) {
  auto opt = std::make_shared<UpdateTablesOptions>();
  auto *cmd = app.add_subcommand("UpdateTables", "Update tables ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--table_id", opt->table_id, "Request parameter table id")->required();
  cmd->add_option("--name", opt->name, "Request parameter table name")->required();
  cmd->add_option("--def_version", opt->def_version, "Request parameter version")->required();
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")->default_val(1)->required();
  cmd->add_option("--replica", opt->replica, "Request parameter replica num, must greater than 0")
      ->default_val(3)
      ->required();
  cmd->add_flag("--is_updating_index", opt->is_updating_index, "Request parameter replica num, must greater than 0")
      ->default_val(false)
      ->required();
  cmd->add_option("--engine", opt->engine, "Request parameter engine, Must be rocksdb|bdb")->required();

  cmd->callback([opt]() { RunUpdateTables(*opt); });
}
void RunUpdateTables(UpdateTablesOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  bool with_increment = false;

  dingodb::pb::meta::UpdateTablesRequest request;
  dingodb::pb::meta::UpdateTablesResponse response;

  uint32_t part_count = opt.part_count;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = 0;
    int ret = client_v2::Helper::GetCreateTableId(CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta(),
                                                  new_part_id);
    if (ret != 0) {
      DINGO_LOG(WARNING) << "GetCreateTableId failed";
      return;
    }
    part_ids.push_back(new_part_id);
  }

  auto *definition_with_id = request.mutable_table_definition_with_id();
  auto *table_id = definition_with_id->mutable_table_id();
  if (!opt.is_updating_index) {
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  } else {
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  }
  table_id->set_entity_id(opt.table_id);

  // string name = 1;
  auto *table_definition = definition_with_id->mutable_table_definition();
  table_definition->set_name(opt.name);

  if (opt.replica > 0) {
    table_definition->set_replica(opt.replica);
  }

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto *column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type("BIGINT");
    column->set_element_type("BIGINT");
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");

    if (with_increment && i == 0) {
      column->set_is_auto_increment(true);
    }
  }
  if (with_increment) {
    table_definition->set_auto_increment(100);
  }

  // map<string, Index> indexes = 3;
  // uint32 version = 4;
  table_definition->set_version(opt.def_version);
  // uint64 ttl = 5;
  table_definition->set_ttl(0);
  // PartitionRule table_partition = 6;
  // Engine engine = 7;
  if (opt.engine != "rocksdb" && opt.engine != "bdb") {
    DINGO_LOG(ERROR) << "engine must be rocksdb or bdb";
    return;
  }
  table_definition->set_engine(client_v2::Helper::GetEngine(opt.engine));
  // map<string, string> properties = 8;
  auto *prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add partition_rule
  // repeated string columns = 1;
  // PartitionStrategy strategy = 2;
  auto *partition_rule = table_definition->mutable_table_partition();
  auto *part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  for (int i = 0; i < part_count; i++) {
    auto *part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(opt.table_id);
    part->mutable_range()->set_start_key(client_v2::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client_v2::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("UpdateTables",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpAddIndexOnTable(CLI::App &app) {
  auto opt = std::make_shared<AddIndexOnTableOptions>();
  auto *cmd = app.add_subcommand("AddIndexOnTable", "Add index on table")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--table_id", opt->table_id, "Request parameter table id")->required();
  cmd->add_option("--index_id", opt->index_id, "Request parameter index id")->required();
  cmd->add_option("--name", opt->name, "Request parameter table name")->required();
  cmd->add_option("--def_version", opt->def_version, "Request parameter version")->required();
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")->default_val(1)->required();
  cmd->add_option("--replica", opt->replica, "Request parameter replica num, must greater than 0")
      ->default_val(3)
      ->required();
  cmd->add_flag("--is_updating_index", opt->is_updating_index, "Request parameter replica num, must greater than 0")
      ->default_val(false)
      ->required();
  cmd->add_option("--engine", opt->engine, "Request parameter engine, Must be rocksdb|bdb")->required();

  cmd->callback([opt]() { RunAddIndexOnTable(*opt); });
}
void RunAddIndexOnTable(AddIndexOnTableOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  bool with_increment = false;

  dingodb::pb::meta::AddIndexOnTableRequest request;
  dingodb::pb::meta::AddIndexOnTableResponse response;

  uint32_t part_count = opt.part_count;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = 0;
    int ret = client_v2::Helper::GetCreateTableId(CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta(),
                                                  new_part_id);
    if (ret != 0) {
      DINGO_LOG(WARNING) << "GetCreateTableId failed";
      return;
    }
    part_ids.push_back(new_part_id);
  }

  request.mutable_table_id()->set_entity_id(opt.table_id);
  request.mutable_table_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  auto *definition_with_id = request.mutable_table_definition_with_id();
  auto *index_id = definition_with_id->mutable_table_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_entity_id(opt.index_id);

  // string name = 1;
  auto *table_definition = definition_with_id->mutable_table_definition();
  table_definition->set_name(opt.name);

  if (opt.replica > 0) {
    table_definition->set_replica(opt.replica);
  }

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto *column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type("BIGINT");
    column->set_element_type("BIGINT");
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");

    if (with_increment && i == 0) {
      column->set_is_auto_increment(true);
    }
  }
  if (with_increment) {
    table_definition->set_auto_increment(100);
  }

  // map<string, Index> indexes = 3;
  // uint32 version = 4;
  table_definition->set_version(opt.def_version);
  // uint64 ttl = 5;
  table_definition->set_ttl(0);
  // PartitionRule table_partition = 6;
  // Engine engine = 7;
  if (opt.engine != "rocksdb" && opt.engine != "bdb") {
    DINGO_LOG(ERROR) << "engine must be rocksdb or bdb";
    return;
  }
  table_definition->set_engine(client_v2::Helper::GetEngine(opt.engine));
  // map<string, string> properties = 8;
  auto *prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add index_parameter
  auto *index_parameter = table_definition->mutable_index_parameter();
  index_parameter->set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_SCALAR);
  index_parameter->mutable_scalar_index_parameter()->set_scalar_index_type(
      ::dingodb::pb::common::ScalarIndexType::SCALAR_INDEX_TYPE_LSM);

  // add partition_rule
  // repeated string columns = 1;
  // PartitionStrategy strategy = 2;
  auto *partition_rule = table_definition->mutable_table_partition();
  auto *part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  for (int i = 0; i < part_count; i++) {
    auto *part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(opt.table_id);
    part->mutable_range()->set_start_key(client_v2::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client_v2::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("AddIndexOnTable",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDropIndexOnTable(CLI::App &app) {
  auto opt = std::make_shared<DropIndexOnTableOptions>();
  auto *cmd = app.add_subcommand("DropIndexOnTable", "Drop index on table")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--table_id", opt->table_id, "Request parameter table id")->required();
  cmd->add_option("--index_id", opt->index_id, "Request parameter index id")->required();
  cmd->callback([opt]() { RunDropIndexOnTable(*opt); });
}
void RunDropIndexOnTable(DropIndexOnTableOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::DropIndexOnTableRequest request;
  dingodb::pb::meta::DropIndexOnTableResponse response;

  request.mutable_table_id()->set_entity_id(opt.table_id);
  request.mutable_table_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  request.mutable_index_id()->set_entity_id(opt.index_id);
  request.mutable_index_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("DropIndexOnTable",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetTables(CLI::App &app) {
  auto opt = std::make_shared<GetTablesOptions>();
  auto *cmd = app.add_subcommand("GetTables", "Get tables")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->callback([opt]() { RunGetTables(*opt); });
}

void RunGetTables(GetTablesOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  table_id->set_entity_id(opt.id);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpDropTables(CLI::App &app) {
  auto opt = std::make_shared<DropTablesOptions>();
  auto *cmd = app.add_subcommand("DropTables", "drop tables")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->required();
  cmd->callback([opt]() { RunDropTables(*opt); });
}

void RunDropTables(DropTablesOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::DropTablesRequest request;
  dingodb::pb::meta::DropTablesResponse response;

  auto *table_id = request.add_table_ids();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(opt.schema_id);
  table_id->set_entity_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("DropTables", request,
                                                                                                  response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpGetAutoIncrements(CLI::App &app) {
  auto opt = std::make_shared<GetAutoIncrementsOptions>();
  auto *cmd = app.add_subcommand("GetAutoIncrements", "Get auto increments ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->callback([opt]() { RunGetAutoIncrements(*opt); });
}

void RunGetAutoIncrements(GetAutoIncrementsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetAutoIncrementsRequest request;
  dingodb::pb::meta::GetAutoIncrementsResponse response;

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetAutoIncrements",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<GetAutoIncrementOptions>();
  auto *cmd = app.add_subcommand("GetAutoIncrement", "Get auto increment ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->callback([opt]() { RunGetAutoIncrement(*opt); });
}

void RunGetAutoIncrement(GetAutoIncrementOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetAutoIncrementRequest request;
  dingodb::pb::meta::GetAutoIncrementResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_entity_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("GetAutoIncrement",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpCreateAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<CreateAutoIncrementOptions>();
  auto *cmd = app.add_subcommand("CreateAutoIncrement", "Create auto increment ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->add_option("--incr_start_id", opt->incr_start_id, "Request parameter incr start id")->default_val(1)->required();
  cmd->callback([opt]() { RunCreateAutoIncrement(*opt); });
}

void RunCreateAutoIncrement(CreateAutoIncrementOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateAutoIncrementRequest request;
  dingodb::pb::meta::CreateAutoIncrementResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  table_id->set_entity_id(opt.id);

  request.set_start_id(opt.incr_start_id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("CreateAutoIncrement",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<UpdateAutoIncrementOptions>();
  auto *cmd = app.add_subcommand("UpdateAutoIncrement", "Update auto increment ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->add_option("--incr_start_id", opt->incr_start_id, "Request parameter incr start id")->default_val(1)->required();
  cmd->add_flag("--force", opt->force, "Request parameter force")->default_val(true)->required();
  cmd->callback([opt]() { RunUpdateAutoIncrement(*opt); });
}

void RunUpdateAutoIncrement(UpdateAutoIncrementOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::UpdateAutoIncrementRequest request;
  dingodb::pb::meta::UpdateAutoIncrementResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  table_id->set_entity_id(opt.id);

  request.set_start_id(opt.incr_start_id);
  request.set_force(opt.force);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("UpdateAutoIncrement",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGenerateAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<GenerateAutoIncrementOptions>();
  auto *cmd = app.add_subcommand("GenerateAutoIncrement", "Generate create auto increment ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();
  cmd->add_option("--generate_count", opt->generate_count, "Generate auto increment id count")
      ->default_val(10000)
      ->required();
  cmd->add_option("--auto_increment_offset", opt->auto_increment_offset, "Request parameter auto increment offset")
      ->default_val(1)
      ->required();
  cmd->add_option("--auto_increment_increment", opt->auto_increment_increment,
                  "Request parameter auto increment increment")
      ->default_val(1)
      ->required();
  cmd->callback([opt]() { RunGenerateAutoIncrement(*opt); });
}

void RunGenerateAutoIncrement(GenerateAutoIncrementOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GenerateAutoIncrementRequest request;
  dingodb::pb::meta::GenerateAutoIncrementResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  table_id->set_entity_id(opt.id);

  request.set_count(opt.generate_count);
  request.set_auto_increment_increment(opt.auto_increment_increment);
  request.set_auto_increment_offset(opt.auto_increment_offset);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest(
      "GenerateAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDeleteAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<DeleteAutoIncrementOptions>();
  auto *cmd = app.add_subcommand("DeleteAutoIncrement", "Delete auto increment ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required();

  cmd->callback([opt]() { RunDeleteAutoIncrement(*opt); });
}

void RunDeleteAutoIncrement(DeleteAutoIncrementOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::DeleteAutoIncrementRequest request;
  dingodb::pb::meta::DeleteAutoIncrementResponse response;

  auto *table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  table_id->set_entity_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("DeleteAutoIncrement",
                                                                                                  request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpListWatch(CLI::App &app) {
  auto opt = std::make_shared<ListWatchOptions>();
  auto *cmd = app.add_subcommand("ListWatch", "List watch")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--watch_id", opt->watch_id, "Request parameter watch id")->required();

  cmd->callback([opt]() { RunListWatch(*opt); });
}

void RunListWatch(ListWatchOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::ListWatchRequest request;
  dingodb::pb::meta::ListWatchResponse response;
  request.set_watch_id(opt.watch_id);

  DINGO_LOG(INFO) << "SendRequest watch_id=" << opt.watch_id;

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("ListWatch", request,
                                                                                                  response, 600000);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  for (const auto &node : response.watch_nodes()) {
    DINGO_LOG(INFO) << "watch_id: " << node.watch_id()
                    << ", last_send_time: " << dingodb::Helper::FormatMsTime(node.last_send_timestamp_ms())
                    << ", watched_revision: " << node.watched_revision();
  }
}

void SetUpCreateWatch(CLI::App &app) {
  auto opt = std::make_shared<CreateWatchOptions>();
  auto *cmd = app.add_subcommand("CreateWatch", "Create watch")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--watch_id", opt->watch_id, "Request parameter watch id")->required();
  cmd->add_option("--start_revision", opt->start_revision, "Request parameter start revision")->required();
  cmd->add_option("--watch_type", opt->watch_type,
                  "Request parameter watch type must be all|region|table|index|schema|table_index")
      ->required();
  cmd->callback([opt]() { RunCreateWatch(*opt); });
}

void RunCreateWatch(CreateWatchOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::WatchRequest request;
  dingodb::pb::meta::WatchResponse response;

  auto *create_request = request.mutable_create_request();
  create_request->set_watch_id(opt.watch_id);
  create_request->set_start_revision(opt.start_revision);

  if (opt.watch_type == "all") {
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_SCHEMA_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_SCHEMA_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_SCHEMA_DELETE);

    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_DELETE);

    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_INDEX_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_INDEX_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_INDEX_DELETE);

    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_DELETE);

    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_REGION_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_REGION_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_REGION_DELETE);
  } else if (opt.watch_type == "region") {
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_REGION_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_REGION_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_REGION_DELETE);
  } else if (opt.watch_type == "table") {
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_DELETE);
  } else if (opt.watch_type == "index") {
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_INDEX_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_INDEX_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_INDEX_DELETE);
  } else if (opt.watch_type == "schema") {
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_SCHEMA_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_SCHEMA_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_SCHEMA_DELETE);
  } else if (opt.watch_type == "table_index") {
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_CREATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_UPDATE);
    create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_DELETE);
  } else {
    DINGO_LOG(ERROR) << "watch_type is invalid, please input [all, region, table, index, schema, table_index]";
    return;
  }

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("Watch", request,
                                                                                                  response, 600000);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpCancelWatch(CLI::App &app) {
  auto opt = std::make_shared<CancelWatchOptions>();
  auto *cmd = app.add_subcommand("CancelWatch", "Cancel watch")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--watch_id", opt->watch_id, "Request parameter watch id")->required();
  cmd->add_option("--start_revision", opt->start_revision, "Request parameter start revision")->required();
  cmd->callback([opt]() { RunCancelWatch(*opt); });
}

void RunCancelWatch(CancelWatchOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::WatchRequest request;
  dingodb::pb::meta::WatchResponse response;

  auto *cancel_request = request.mutable_cancel_request();
  cancel_request->set_watch_id(opt.watch_id);

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("Watch", request,
                                                                                                  response, 600000);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpProgressWatch(CLI::App &app) {
  auto opt = std::make_shared<ProgressWatchOptions>();
  auto *cmd = app.add_subcommand("ProgressWatch", "Progress watch")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--watch_id", opt->watch_id, "Request parameter watch id")->required();
  cmd->callback([opt]() { RunProgressWatch(*opt); });
}

void RunProgressWatch(ProgressWatchOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::WatchRequest request;
  dingodb::pb::meta::WatchResponse response;
  auto *progress_request = request.mutable_progress_request();
  progress_request->set_watch_id(opt.watch_id);

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  for (uint64_t i = 0;; ++i) {
    auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("Watch", request,
                                                                                                    response, 600000);
    DINGO_LOG(INFO)
        << "SendRequest i: " << i << ", status=" << status
        << "========================================================================================================";
    DINGO_LOG(INFO) << "event_size: " << response.events_size();
    DINGO_LOG(INFO) << response.DebugString();

    if (response.error().errcode() != 0) {
      break;
    }
  }
}

void SetUpGenTso(CLI::App &app) {
  auto opt = std::make_shared<GenTsoOptions>();
  auto *cmd = app.add_subcommand("GenTso", "Generate tso ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->callback([opt]() { RunGenTso(*opt); });
}

void RunGenTso(GenTsoOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::TsoRequest request;
  dingodb::pb::meta::TsoResponse response;

  request.set_op_type(::dingodb::pb::meta::TsoOpType::OP_GEN_TSO);
  request.set_count(10);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("TsoService", request,
                                                                                                  response);
  Pretty::Show(response);
}

void SetUpResetTso(CLI::App &app) {
  auto opt = std::make_shared<ResetTsoOptions>();
  auto *cmd = app.add_subcommand("ResetTso", "Reset tso ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--watch_id", opt->tso_new_physical, "Request parameter tso_new_physical")->required();
  cmd->add_option("--tso_save_physical", opt->tso_save_physical, "Request parameter watch id")->default_val(0);
  cmd->add_option("--watch_id", opt->tso_new_logical, "Request parameter watch id")->default_val(0);

  cmd->callback([opt]() { RunResetTso(*opt); });
}

void RunResetTso(ResetTsoOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::TsoRequest request;
  dingodb::pb::meta::TsoResponse response;
  int64_t tso_save_physical = opt.tso_save_physical;
  if (tso_save_physical == 0) {
    DINGO_LOG(WARNING) << "tso_save_physical is empty, use tso_new_physical as tso_save_physical";
    tso_save_physical = opt.tso_new_physical;
  }

  if (opt.tso_new_logical == 0) {
    DINGO_LOG(WARNING) << "tso_new_logical is empty, use 0";
  }

  request.set_op_type(::dingodb::pb::meta::TsoOpType::OP_RESET_TSO);
  request.set_save_physical(tso_save_physical);
  request.mutable_current_timestamp()->set_physical(opt.tso_new_physical);
  request.mutable_current_timestamp()->set_logical(opt.tso_new_logical);
  request.set_force(true);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("TsoService", request,
                                                                                                  response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpUpdateTso(CLI::App &app) {
  auto opt = std::make_shared<UpdateTsoOptions>();
  auto *cmd = app.add_subcommand("UpdateTso", "Update tso ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--watch_id", opt->tso_new_physical, "Request parameter tso_new_physical")->required();
  cmd->add_option("--tso_save_physical", opt->tso_save_physical, "Request parameter watch id")->default_val(0);
  cmd->add_option("--watch_id", opt->tso_new_logical, "Request parameter watch id")->default_val(0);

  cmd->callback([opt]() { RunUpdateTso(*opt); });
}

void RunUpdateTso(UpdateTsoOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::TsoRequest request;
  dingodb::pb::meta::TsoResponse response;
  int64_t tso_save_physical = opt.tso_save_physical;
  if (tso_save_physical == 0) {
    DINGO_LOG(WARNING) << "tso_save_physical is empty, use tso_new_physical as tso_save_physical";
    tso_save_physical = opt.tso_new_physical;
  }

  if (opt.tso_new_logical == 0) {
    DINGO_LOG(WARNING) << "tso_new_logical is empty, use 0";
  }

  request.set_op_type(::dingodb::pb::meta::TsoOpType::OP_UPDATE_TSO);
  request.set_save_physical(tso_save_physical);
  request.mutable_current_timestamp()->set_physical(opt.tso_new_physical);
  request.mutable_current_timestamp()->set_logical(opt.tso_new_logical);
  request.set_force(true);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("TsoService", request,
                                                                                                  response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpGetRegionByTable(CLI::App &app) {
  auto opt = std::make_shared<GetRegionByTableOptions>();
  auto *cmd = app.add_subcommand("GetRegionByTable", "Get table")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--table_id", opt->table_id, "Request parameter table id")
      ->check(CLI::Range(1, std::numeric_limits<int32_t>::max()));
  cmd->add_option("--table_name", opt->table_name, "Request parameter table name")
      ->transform([](const std::string &str) { return Helper::ToUpperCase(str); });
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")->default_val(0);
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->default_val(0);
  cmd->callback([opt]() { RunGetRegionByTable(*opt); });
}

void RunGetRegionByTable(GetRegionByTableOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  if (opt.table_id == 0 && opt.table_name.empty()) {
    std::cout << "Must set table_id or table_name." << std::endl;
    return;
  }
  int64_t table_id = opt.table_id;
  if (opt.table_id == 0) {
    if (opt.schema_id == 0) {
      std::cout << "Must schema_id." << std::endl;
      return;
    }
    dingodb::pb::meta::TableDefinitionWithId table_definition_with_id;
    auto status = GetTableOrIndexDefinition(opt.table_name, opt.schema_id, table_definition_with_id);
    if (Pretty::ShowError(status)) {
      return;
    }
    table_id = table_definition_with_id.table_id().entity_id();
    // GetTableOrIndexDefinition(opt.table_id, )
  }
  // get regionmap
  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.set_epoch(1);
  request.set_tenant_id(opt.tenant_id);
  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetRegionMap", request, response);

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Get region map failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg() << std::endl;
    return;
  }
  std::vector<dingodb::pb::common::Region> regions;
  for (auto const &region : response.regionmap().regions()) {
    if (region.definition().table_id() == table_id || region.definition().index_id() == table_id) {
      regions.push_back(region);
    }
  }
  if (regions.empty()) {
    std::cout << "Not find region." << std::endl;
    return;
  }
  Pretty::Show(regions);
}

void SetUpCreateIds(CLI::App &app) {
  auto opt = std::make_shared<CreateIdsOptions>();
  auto *cmd = app.add_subcommand("CreateIds", "Create ids")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--count", opt->count, "Request parameter count")->required();
  cmd->add_option(
         "--epoch_type", opt->epoch_type,
         "Request parameter epoch_type, ID_NEXT_TABLE|ID_NEXT_SCHEMA|ID_SCHEMA_VERSION|ID_DDL_JOB|ID_NEXT_TENANT")
      ->required();

  cmd->callback([opt]() { RunCreateIds(*opt); });
}

void RunCreateIds(CreateIdsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::CreateIdsRequest request;
  dingodb::pb::coordinator::CreateIdsResponse response;

  if (opt.epoch_type.empty()) {
    std::cout
        << "epoch_type is empty, like ID_NEXT_TABLE, ID_NEXT_SCHEMA, ID_SCHEMA_VERSION, ID_DDL_JOB, ID_NEXT_TENANT"
        << std::endl;
    return;
  }

  if (opt.count <= 0) {
    std::cout << "count must be greater than 0" << std::endl;
    return;
  }

  const google::protobuf::EnumDescriptor *descriptor =
      google::protobuf::GetEnumDescriptor<dingodb::pb::coordinator::IdEpochType>();
  const google::protobuf::EnumValueDescriptor *enum_value = descriptor->FindValueByName(opt.epoch_type);
  if (enum_value == nullptr) {
    std::cout << "id_epoch_type is invalid, like ID_NEXT_TABLE, ID_NEXT_SCHEMA, ID_SCHEMA_VERSION, ID_DDL_JOB, "
                 "ID_NEXT_TENANT"
              << std::endl;
    return;
  }

  request.set_id_epoch_type(static_cast<dingodb::pb::coordinator::IdEpochType>(enum_value->number()));
  request.set_count(opt.count);

  CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CreateIds", request, response);

  Pretty::Show(response);
}

void SetUpImportMeta(CLI::App &app) {
  auto opt = std::make_shared<ImportMetaOptions>();
  auto *cmd = app.add_subcommand("ImportMeta", "Import meta ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--meta_file_dir", opt->dir_meta_file, "Request parameter meta_file_dir")->required();
  cmd->callback([opt]() { RunImportMeta(*opt); });
}

void RunImportMeta(const ImportMetaOptions &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::ImportMetaRequest request;
  dingodb::pb::meta::ImportMetaResponse response;

  std::map<std::string, std::string> internal_coordinator_sdk_meta_kvs;

  {
    // open RocksDB
    rocksdb::Options options;
    options.create_if_missing = true;

    // open SST
    std::string sst_file_path = opt.dir_meta_file;
    rocksdb::SstFileReader sst_reader(options);
    rocksdb::ReadOptions read_options;

    sst_reader.Open(sst_file_path);

    // get content
    rocksdb::Iterator *iter = sst_reader.NewIterator(read_options);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      std::string key(iter->key().data(), iter->key().size());
      std::string value(iter->value().data(), iter->value().size());
      internal_coordinator_sdk_meta_kvs.emplace(std::move(key), std::move(value));
    }

    delete iter;
  }

  // find kCoordinatorSdkMetaKeyName
  auto iter = internal_coordinator_sdk_meta_kvs.find(dingodb::Constant::kCoordinatorSdkMetaKeyName);
  if (iter == internal_coordinator_sdk_meta_kvs.end()) {
    std::string s =
        fmt::format("not found {} in coordinator.sdk.meta file.", dingodb::Constant::kCoordinatorSdkMetaKeyName);
    DINGO_LOG(ERROR) << s;
    return;
  }
  dingodb::pb::meta::MetaALL meta_all;
  if (!meta_all.ParseFromString(iter->second)) {
    std::string s =
        fmt::format("parse dingodb::pb::meta::MetaALL failed : {}", dingodb::Constant::kCoordinatorSdkMetaKeyName);
    DINGO_LOG(ERROR) << s;
    return;
  }
  request.mutable_meta_all()->CopyFrom(meta_all);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("ImportMeta", request,
                                                                                                  response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpExportMeta(CLI::App &app) {
  auto opt = std::make_shared<ExportMetaOptions>();
  auto *cmd = app.add_subcommand("ExportMeta", "Export meta ")->group("Meta Command");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--meta_file_dir", opt->dir_meta_file, "Request parameter meta_file_dir")->required();
  cmd->callback([opt]() { RunExportMeta(*opt); });
}

void RunExportMeta(const ExportMetaOptions &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::ExportMetaRequest request;
  dingodb::pb::meta::ExportMetaResponse response;

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteractionMeta()->SendRequest("ExportMeta", request,
                                                                                                  response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  std::map<std::string, std::string> kvs;

  kvs.emplace(dingodb::Constant::kCoordinatorSdkMetaKeyName, response.meta_all().SerializeAsString());
  {
    // open RocksDB
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::SstFileWriter sst_wirter(rocksdb::EnvOptions(), options, nullptr, true);
    rocksdb::WriteOptions write_options;

    auto status = sst_wirter.Open(opt.dir_meta_file);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.ToString();
    }

    // write SST
    for (const auto &[key, value] : kvs) {
      status = sst_wirter.Put(key, value);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.ToString();
      }
    }

    status = sst_wirter.Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.ToString();
    }
  }
}

}  // namespace client_v2
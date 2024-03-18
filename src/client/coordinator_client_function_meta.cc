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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client/client_helper.h"
#include "client/coordinator_client_function.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "coordinator/tso_control.h"
#include "gflags/gflags_declare.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"

DECLARE_bool(log_each_request);
DECLARE_int64(timeout_ms);
DECLARE_string(id);
DECLARE_string(name);
DECLARE_string(comment);
DECLARE_int64(schema_id);
DECLARE_int64(table_id);
DECLARE_int64(index_id);
DECLARE_int64(replica);
DECLARE_int32(max_elements);
DECLARE_int32(dimension);
DECLARE_int32(efconstruction);
DECLARE_int32(nlinks);
DECLARE_bool(with_auto_increment);
DECLARE_string(vector_index_type);
DECLARE_bool(auto_split);
DECLARE_int32(part_count);
DECLARE_int32(ncentroids);
DECLARE_string(metrics_type);
DECLARE_int64(def_version);
DECLARE_int32(nsubvector);
DECLARE_int32(nbits_per_idx);
DECLARE_int64(count);
DECLARE_string(engine);

DECLARE_int64(tso_save_physical);
DECLARE_int64(tso_new_physical);
DECLARE_int64(tso_new_logical);

DECLARE_int64(tenant_id);

DEFINE_bool(is_updating_index, false, "is index");
DEFINE_bool(is_index, false, "is index");

dingodb::pb::common::Engine GetEngine(const std::string& engine_name) {
  if (engine_name == "rocksdb") {
    return dingodb::pb::common::Engine::ENG_ROCKSDB;
  } else if (engine_name == "bdb") {
    return dingodb::pb::common::Engine::ENG_BDB;
  } else {
    DINGO_LOG(FATAL) << "engine_name is illegal, please input -engine=[rocksdb, bdb]";
  }
}

// meta hello
void SendMetaHello(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::HelloRequest request;
  dingodb::pb::meta::HelloResponse response;

  std::string const key = "Hello";
  // const char* op = nullptr;
  request.set_hello(0);
  request.set_get_memory_info(true);

  auto status = coordinator_interaction->SendRequest("Hello", request, response);
  DINGO_LOG(INFO) << "SendRequest status: " << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetSchemas(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetSchemasRequest request;
  dingodb::pb::meta::GetSchemasResponse response;

  request.set_tenant_id(FLAGS_tenant_id);

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = coordinator_interaction->SendRequest("GetSchemas", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  for (const auto& schema : response.schemas()) {
    DINGO_LOG(INFO) << "schema_id=[" << schema.id().entity_id() << "]"
                    << "schema_name=[" << schema.name() << "]"
                    << "child_table_count=" << schema.table_ids_size();
    for (const auto& child_table_id : schema.table_ids()) {
      DINGO_LOG(INFO) << "child table_id=[" << child_table_id.entity_id() << "]";
    }
  }
}

void SendGetSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetSchemaRequest request;
  dingodb::pb::meta::GetSchemaResponse response;

  request.set_tenant_id(FLAGS_tenant_id);

  if (FLAGS_schema_id <= 0) {
    DINGO_LOG(WARNING) << "schema_id is empty";
    return;
  }

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(FLAGS_schema_id);

  auto status = coordinator_interaction->SendRequest("GetSchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  DINGO_LOG(INFO) << "tenant_id=[" << response.schema().tenant_id() << "]"
                  << "schema_id=[" << response.schema().id().entity_id() << "]"
                  << "schema_name=[" << response.schema().name() << "]"
                  << "child_table_count=" << response.schema().table_ids_size();
  for (const auto& child_table_id : response.schema().table_ids()) {
    DINGO_LOG(INFO) << "child table_id=[" << child_table_id.entity_id() << "]";
  }
  for (const auto& child_table_id : response.schema().index_ids()) {
    DINGO_LOG(INFO) << "child index_id=[" << child_table_id.entity_id() << "]";
  }
}

void SendGetSchemaByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetSchemaByNameRequest request;
  dingodb::pb::meta::GetSchemaByNameResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty, this is schema_name";
    return;
  }
  request.set_schema_name(FLAGS_name);

  if (FLAGS_tenant_id > 0) {
    request.set_tenant_id(FLAGS_tenant_id);
  }

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = coordinator_interaction->SendRequest("GetSchemaByName", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  DINGO_LOG(INFO) << "tenant_id=[" << response.schema().tenant_id() << "]"
                  << "schema_id=[" << response.schema().id().entity_id() << "]"
                  << "schema_name=[" << response.schema().name() << "]"
                  << "child_table_count=" << response.schema().table_ids_size();
  for (const auto& child_table_id : response.schema().table_ids()) {
    DINGO_LOG(INFO) << "child table_id=[" << child_table_id.entity_id() << "]";
  }
}

void SendGetTablesCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTablesCountRequest request;
  dingodb::pb::meta::GetTablesCountResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  auto status = coordinator_interaction->SendRequest("GetTablesCount", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "table_count=" << response.tables_count();
}

void SendGetTablesBySchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTablesBySchemaRequest request;
  dingodb::pb::meta::GetTablesBySchemaResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  auto status = coordinator_interaction->SendRequest("GetTablesBySchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  // DINGO_LOG(INFO) << response.DebugString();

  for (const auto& table_definition_with_id : response.table_definition_with_ids()) {
    DINGO_LOG(INFO) << "table_id=[" << table_definition_with_id.table_id().entity_id() << "]"
                    << "table_name=[" << table_definition_with_id.table_definition().name() << "], column_count=["
                    << table_definition_with_id.table_definition().columns_size() << "]";
  }

  DINGO_LOG(INFO) << "table_count=" << response.table_definition_with_ids_size();
}

void SendGetTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTableRequest request;
  dingodb::pb::meta::GetTableResponse response;

  auto* table_id = request.mutable_table_id();
  if (FLAGS_is_index) {
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  } else {
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  }
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, this table_id";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetTableByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTableByNameRequest request;
  dingodb::pb::meta::GetTableByNameResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }
  if (FLAGS_schema_id == 0) {
    DINGO_LOG(WARNING) << "schema_id is empty";
    return;
  }

  request.set_table_name(FLAGS_name);
  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(FLAGS_schema_id);

  auto status = coordinator_interaction->SendRequest("GetTableByName", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetTableRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTableRangeRequest request;
  dingodb::pb::meta::GetTableRangeResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetTableRange", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  for (const auto& it : response.table_range().range_distribution()) {
    DINGO_LOG(INFO) << "region_id=[" << it.id().entity_id() << "]"
                    << "range=[" << dingodb::Helper::StringToHex(it.range().start_key()) << ","
                    << dingodb::Helper::StringToHex(it.range().end_key()) << "]"
                    << " leader=[" << it.leader().host() << ":" << it.leader().port() << "]";
  }
}

void SendCreateTableId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CreateTableIdRequest request;
  dingodb::pb::meta::CreateTableIdResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  auto status = coordinator_interaction->SendRequest("CreateTableId", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

int GetCreateTableId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, int64_t& table_id) {
  dingodb::pb::meta::CreateTableIdRequest request;
  dingodb::pb::meta::CreateTableIdResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  auto status = coordinator_interaction->SendRequest("CreateTableId", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.has_table_id()) {
    table_id = response.table_id().entity_id();
    return 0;
  } else {
    return -1;
  }
}

int GetCreateTableIds(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, int64_t count,
                      std::vector<int64_t>& table_ids) {
  dingodb::pb::meta::CreateTableIdsRequest request;
  dingodb::pb::meta::CreateTableIdsResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_count(count);

  auto status = coordinator_interaction->SendRequest("CreateTableIds", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.table_ids_size() > 0) {
    for (const auto& id : response.table_ids()) {
      table_ids.push_back(id.entity_id());
    }
    return 0;
  } else {
    return -1;
  }
}

void SendCreateTableIds(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CreateTableIdsRequest request;
  dingodb::pb::meta::CreateTableIdsResponse response;

  if (FLAGS_count <= 0) {
    DINGO_LOG(WARNING) << "count is empty";
    return;
  }

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_count(FLAGS_count);

  auto status = coordinator_interaction->SendRequest("CreateTableIds", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
  DINGO_LOG(INFO) << "count = " << response.table_ids_size();
}

void SendCreateTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, bool with_increment) {
  dingodb::pb::meta::CreateTableRequest request;
  dingodb::pb::meta::CreateTableResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  if (FLAGS_part_count == 0) {
    FLAGS_part_count = 1;
  }
  uint32_t part_count = FLAGS_part_count;

  std::vector<int64_t> new_ids;
  int ret = GetCreateTableIds(coordinator_interaction, 1 + FLAGS_part_count, new_ids);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }
  if (new_ids.empty()) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }
  if (new_ids.size() != 1 + FLAGS_part_count) {
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

  for (const auto& id : part_ids) {
    DINGO_LOG(INFO) << "part_id = " << id;
  }

  // setup table_id
  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(schema_id->entity_id());
  table_id->set_entity_id(new_table_id);

  // string name = 1;
  auto* table_definition = request.mutable_table_definition();
  table_definition->set_name(FLAGS_name);

  if (FLAGS_replica > 0) {
    table_definition->set_replica(FLAGS_replica);
  }

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
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
  if (FLAGS_engine != "rocksdb" && FLAGS_engine != "bdb") {
    DINGO_LOG(ERROR) << "engine must be rocksdb or bdb";
    return;
  }
  table_definition->set_engine(GetEngine(FLAGS_engine));
  // map<string, string> properties = 8;
  auto* prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add partition_rule
  auto* partition_rule = table_definition->mutable_table_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");

  for (int i = 0; i < part_count; i++) {
    auto* part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_table_id);
    part->mutable_range()->set_start_key(client::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  request.mutable_request_info()->set_request_id(1024);

  auto status = coordinator_interaction->SendRequest("CreateTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
  if (response.error().errcode() == 0) {
    DINGO_LOG(INFO) << "create table success, table_id==" << response.table_id().entity_id();
  }
}

void SendDropTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("DropTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendDropSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::DropSchemaRequest request;
  dingodb::pb::meta::DropSchemaResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  schema_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("DropSchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendCreateSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CreateSchemaRequest request;
  dingodb::pb::meta::CreateSchemaResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  request.set_tenant_id(FLAGS_tenant_id);

  auto* parent_schema_id = request.mutable_parent_schema_id();
  parent_schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  parent_schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  parent_schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_schema_name(FLAGS_name);

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = coordinator_interaction->SendRequest("CreateSchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetTableMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTableMetricsRequest request;
  dingodb::pb::meta::GetTableMetricsResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetTableMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetIndexsCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetIndexesCountRequest request;
  dingodb::pb::meta::GetIndexesCountResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  auto status = coordinator_interaction->SendRequest("GetIndexsCount", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "index_count=" << response.indexes_count();
}

void SendGetIndexs(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetIndexesRequest request;
  dingodb::pb::meta::GetIndexesResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  auto status = coordinator_interaction->SendRequest("GetIndexs", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  // DINGO_LOG(INFO) << response.DebugString();

  for (const auto& index_definition_with_id : response.index_definition_with_ids()) {
    DINGO_LOG(INFO) << "index_id=[" << index_definition_with_id.index_id().entity_id() << "]"
                    << "index_name=[" << index_definition_with_id.index_definition().name() << "], index_type=["
                    << dingodb::pb::common::IndexType_Name(
                           index_definition_with_id.index_definition().index_parameter().index_type())
                    << "]";
  }

  DINGO_LOG(INFO) << "index_count=" << response.index_definition_with_ids_size();
}

void SendUpdateIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetIndexRequest get_request;
  dingodb::pb::meta::GetIndexResponse get_response;

  auto* index_id = get_request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, this index_id";
    return;
  }
  index_id->set_entity_id(std::stol(FLAGS_id));

  if (FLAGS_max_elements <= 0) {
    DINGO_LOG(WARNING) << "max_elements is empty, this max_elements";
    return;
  }

  auto status = coordinator_interaction->SendRequest("GetIndex", get_request, get_response);
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
      ->set_max_elements(FLAGS_max_elements);

  status = coordinator_interaction->SendRequest("UpdateIndex", update_request, update_response);

  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << update_response.DebugString();
}

void SendGetIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetIndexRequest request;
  dingodb::pb::meta::GetIndexResponse response;

  auto* index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, this index_id";
    return;
  }
  index_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetIndexByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetIndexByNameRequest request;
  dingodb::pb::meta::GetIndexByNameResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  if (FLAGS_schema_id == 0) {
    DINGO_LOG(WARNING) << "schema_id is empty";
    return;
  }

  request.set_index_name(FLAGS_name);
  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_id(FLAGS_schema_id);
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);

  auto status = coordinator_interaction->SendRequest("GetIndexByName", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetIndexRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetIndexRangeRequest request;
  dingodb::pb::meta::GetIndexRangeResponse response;

  auto* index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  index_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetIndexRange", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  for (const auto& it : response.index_range().range_distribution()) {
    DINGO_LOG(INFO) << "region_id=[" << it.id().entity_id() << "]"
                    << "range=[" << dingodb::Helper::StringToHex(it.range().start_key()) << ","
                    << dingodb::Helper::StringToHex(it.range().end_key()) << "]"
                    << " leader=[" << it.leader().host() << ":" << it.leader().port() << "]";
  }
}

void SendCreateIndexId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CreateIndexIdRequest request;
  dingodb::pb::meta::CreateIndexIdResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  auto status = coordinator_interaction->SendRequest("CreateIndexId", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendCreateIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CreateIndexRequest request;
  dingodb::pb::meta::CreateIndexResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  if (FLAGS_part_count == 0) {
    FLAGS_part_count = 1;
  }
  uint32_t part_count = FLAGS_part_count;

  std::vector<int64_t> new_ids;
  int ret = GetCreateTableIds(coordinator_interaction, 1 + FLAGS_part_count, new_ids);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }
  if (new_ids.empty()) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }
  if (new_ids.size() != 1 + FLAGS_part_count) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }

  int64_t new_index_id = new_ids.at(0);
  DINGO_LOG(INFO) << "index_id = " << new_index_id;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = new_ids.at(1 + i);
    part_ids.push_back(new_part_id);
  }

  for (const auto& id : part_ids) {
    DINGO_LOG(INFO) << "part_id = " << id;
  }

  // setup index_id
  auto* index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(schema_id->entity_id());
  index_id->set_entity_id(new_index_id);

  // string name = 1;
  auto* index_definition = request.mutable_index_definition();
  index_definition->set_name(FLAGS_name);

  if (FLAGS_replica > 0) {
    index_definition->set_replica(FLAGS_replica);
  }

  if (FLAGS_with_auto_increment) {
    index_definition->set_with_auto_incrment(true);
    index_definition->set_auto_increment(1024);
  }

  // vector index parameter
  index_definition->mutable_index_parameter()->set_index_type(dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
  auto* vector_index_parameter = index_definition->mutable_index_parameter()->mutable_vector_index_parameter();

  if (FLAGS_vector_index_type.empty()) {
    DINGO_LOG(WARNING) << "vector_index_type is empty";
    return;
  }

  if (FLAGS_vector_index_type == "hnsw") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
  } else if (FLAGS_vector_index_type == "flat") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
  } else if (FLAGS_vector_index_type == "bruteforce") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BRUTEFORCE);
  } else if (FLAGS_vector_index_type == "ivf_flat") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT);
  } else if (FLAGS_vector_index_type == "ivf_pq") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
  } else {
    DINGO_LOG(WARNING) << "vector_index_type is invalid, now only support hnsw and flat";
    return;
  }

  if (FLAGS_dimension == 0) {
    DINGO_LOG(WARNING) << "dimension is empty";
    return;
  }

  dingodb::pb::common::MetricType metric_type;

  if (FLAGS_metrics_type == "L2" || FLAGS_metrics_type == "l2") {
    metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_L2;
  } else if (FLAGS_metrics_type == "IP" || FLAGS_metrics_type == "ip") {
    metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
  } else if (FLAGS_metrics_type == "COSINE" || FLAGS_metrics_type == "cosine") {
    metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE;
  } else {
    DINGO_LOG(WARNING) << "metrics_type is invalid, now only support L2, IP and COSINE";
    return;
  }

  if (FLAGS_vector_index_type == "hnsw") {
    if (FLAGS_max_elements < 0) {
      DINGO_LOG(WARNING) << "max_elements is negative";
      return;
    }
    if (FLAGS_efconstruction == 0) {
      DINGO_LOG(WARNING) << "efconstruction is empty";
      return;
    }
    if (FLAGS_nlinks == 0) {
      DINGO_LOG(WARNING) << "nlinks is empty";
      return;
    }

    DINGO_LOG(INFO) << "max_elements=" << FLAGS_max_elements << ", dimension=" << FLAGS_dimension;

    auto* hsnw_index_parameter = vector_index_parameter->mutable_hnsw_parameter();

    hsnw_index_parameter->set_dimension(FLAGS_dimension);
    hsnw_index_parameter->set_metric_type(metric_type);
    hsnw_index_parameter->set_efconstruction(FLAGS_efconstruction);
    hsnw_index_parameter->set_nlinks(FLAGS_nlinks);
    hsnw_index_parameter->set_max_elements(FLAGS_max_elements);
  } else if (FLAGS_vector_index_type == "flat") {
    auto* flat_index_parameter = vector_index_parameter->mutable_flat_parameter();
    flat_index_parameter->set_dimension(FLAGS_dimension);
    flat_index_parameter->set_metric_type(metric_type);
  } else if (FLAGS_vector_index_type == "bruteforce") {
    auto* bruteforce_index_parameter = vector_index_parameter->mutable_bruteforce_parameter();
    bruteforce_index_parameter->set_dimension(FLAGS_dimension);
    bruteforce_index_parameter->set_metric_type(metric_type);
  } else if (FLAGS_vector_index_type == "ivf_flat") {
    auto* ivf_flat_index_parameter = vector_index_parameter->mutable_ivf_flat_parameter();
    ivf_flat_index_parameter->set_dimension(FLAGS_dimension);
    ivf_flat_index_parameter->set_metric_type(metric_type);
    ivf_flat_index_parameter->set_ncentroids(FLAGS_ncentroids);
  } else if (FLAGS_vector_index_type == "ivf_pq") {
    auto* ivf_pq_index_parameter = vector_index_parameter->mutable_ivf_pq_parameter();
    ivf_pq_index_parameter->set_dimension(FLAGS_dimension);
    ivf_pq_index_parameter->set_metric_type(metric_type);
    ivf_pq_index_parameter->set_ncentroids(FLAGS_ncentroids);
    ivf_pq_index_parameter->set_nsubvector(FLAGS_nsubvector);
    ivf_pq_index_parameter->set_nbits_per_idx(FLAGS_nbits_per_idx);
  }

  index_definition->set_version(1);

  auto* partition_rule = index_definition->mutable_index_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");

  for (int i = 0; i < part_count; i++) {
    auto* part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_index_id);
    part->mutable_range()->set_start_key(client::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  if (FLAGS_with_auto_increment) {
    DINGO_LOG(INFO) << "with_auto_increment";
    index_definition->set_auto_increment(100);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  auto status = coordinator_interaction->SendRequest("CreateIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
  if (response.error().errcode() == 0) {
    DINGO_LOG(INFO) << "create index success, index_id==" << response.index_id().entity_id();
  }
}

void SendDropIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::DropIndexRequest request;
  dingodb::pb::meta::DropIndexResponse response;

  auto* index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  index_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("DropIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetIndexMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetIndexMetricsRequest request;
  dingodb::pb::meta::GetIndexMetricsResponse response;

  auto* index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  index_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetIndexMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetIndexesCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetIndexesCountRequest request;
  dingodb::pb::meta::GetIndexesCountResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  auto status = coordinator_interaction->SendRequest("GetIndexesCount", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "index_count=" << response.indexes_count();
}

void SendGetIndexes(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetIndexesRequest request;
  dingodb::pb::meta::GetIndexesResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  auto status = coordinator_interaction->SendRequest("GetIndexes", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  // DINGO_LOG(INFO) << response.DebugString();

  for (const auto& index_definition_with_id : response.index_definition_with_ids()) {
    DINGO_LOG(INFO) << "index_id=[" << index_definition_with_id.index_id().entity_id() << "]"
                    << "index_name=[" << index_definition_with_id.index_definition().name() << "], index_type=["
                    << dingodb::pb::common::IndexType_Name(
                           index_definition_with_id.index_definition().index_parameter().index_type())
                    << "]";
  }

  DINGO_LOG(INFO) << "index_count=" << response.index_definition_with_ids_size();
}

void SendGenerateTableIds(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GenerateTableIdsRequest request;
  dingodb::pb::meta::GenerateTableIdsResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  dingodb::pb::meta::TableWithPartCount* count = request.mutable_count();
  count->set_has_table(true);
  count->set_table_part_count(3);
  uint32_t index_count = 2;
  count->set_index_count(index_count);
  for (int i = 0; i < index_count; i++) {
    count->add_index_part_count(4);
  }

  auto status = coordinator_interaction->SendRequest("GenerateTableIds", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SendCreateTables(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  bool with_table_id = false;
  bool with_increment = false;

  dingodb::pb::meta::CreateTablesRequest request;
  dingodb::pb::meta::CreateTablesResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  if (FLAGS_part_count == 0) {
    FLAGS_part_count = 1;
  }
  uint32_t part_count = FLAGS_part_count;

  std::vector<int64_t> new_ids;
  int ret = GetCreateTableIds(coordinator_interaction, 1 + FLAGS_part_count, new_ids);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }
  if (new_ids.empty()) {
    DINGO_LOG(WARNING) << "GetCreateTableIds failed";
    return;
  }
  if (new_ids.size() != 1 + FLAGS_part_count) {
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

  auto* definition_with_id = request.add_table_definition_with_ids();
  auto* table_id = definition_with_id->mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(schema_id->entity_id());
  table_id->set_entity_id(new_table_id);

  // string name = 1;
  auto* table_definition = definition_with_id->mutable_table_definition();
  table_definition->set_name(FLAGS_name);

  if (FLAGS_replica > 0) {
    table_definition->set_replica(FLAGS_replica);
  }

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
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
  if (FLAGS_engine != "rocksdb" && FLAGS_engine != "bdb") {
    DINGO_LOG(ERROR) << "engine must be rocksdb or bdb";
    return;
  }
  table_definition->set_engine(GetEngine(FLAGS_engine));
  // map<string, string> properties = 8;
  auto* prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add partition_rule
  // repeated string columns = 1;
  // PartitionStrategy strategy = 2;
  auto* partition_rule = table_definition->mutable_table_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  for (int i = 0; i < part_count; i++) {
    auto* part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_table_id);
    part->mutable_range()->set_start_key(client::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  auto status = coordinator_interaction->SendRequest("CreateTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetTables(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SendDropTables(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::DropTablesRequest request;
  dingodb::pb::meta::DropTablesResponse response;

  auto* table_id = request.add_table_ids();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("DropTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SendSwitchAutoSplit(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::SwitchAutoSplitRequest request;
  dingodb::pb::meta::SwitchAutoSplitResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, please provides --schema-id=xxx --id=xxx and --auto_split=true/false";
    return;
  }
  if (FLAGS_schema_id == 0) {
    DINGO_LOG(WARNING) << "schema-id is empty, please provides --schema-id=xxx --id=xxx and --auto_split=true/false";
    return;
  }

  table_id->set_parent_entity_id(FLAGS_schema_id);
  table_id->set_entity_id(std::stol(FLAGS_id));

  request.set_auto_split(FLAGS_auto_split);

  auto status = coordinator_interaction->SendRequest("SwitchAutoSplit", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SendGenTso(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::TsoRequest request;
  dingodb::pb::meta::TsoResponse response;

  request.set_op_type(::dingodb::pb::meta::TsoOpType::OP_GEN_TSO);
  request.set_count(10);

  auto status = coordinator_interaction->SendRequest("TsoService", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();

  auto lambda_tso_2_timestamp_function = [](const ::dingodb::pb::meta::TsoTimestamp& tso) {
    return (tso.physical() << ::dingodb::kLogicalBits) + tso.logical();
  };

  for (int i = 0; i < 10; i++) {
    dingodb::pb::meta::TsoTimestamp tso;
    tso.set_physical(response.start_timestamp().physical());
    tso.set_logical(response.start_timestamp().logical() + i);
    int64_t time_safe_ts = lambda_tso_2_timestamp_function(tso);
    DINGO_LOG(INFO) << "time_safe_ts  : " << time_safe_ts;
  }
}

void SendResetTso(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::TsoRequest request;
  dingodb::pb::meta::TsoResponse response;

  if (FLAGS_tso_new_physical == 0) {
    DINGO_LOG(ERROR) << "tso_new_physical is empty";
    return;
  }

  if (FLAGS_tso_save_physical == 0) {
    DINGO_LOG(WARNING) << "tso_save_physical is empty, use tso_new_physical as tso_save_physical";
    FLAGS_tso_save_physical = FLAGS_tso_new_physical;
  }

  if (FLAGS_tso_new_logical == 0) {
    DINGO_LOG(WARNING) << "tso_new_logical is empty, use 0";
  }

  request.set_op_type(::dingodb::pb::meta::TsoOpType::OP_RESET_TSO);
  request.set_save_physical(FLAGS_tso_save_physical);
  request.mutable_current_timestamp()->set_physical(FLAGS_tso_new_physical);
  request.mutable_current_timestamp()->set_logical(FLAGS_tso_new_logical);
  request.set_force(true);

  auto status = coordinator_interaction->SendRequest("TsoService", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SendUpdateTso(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::TsoRequest request;
  dingodb::pb::meta::TsoResponse response;

  if (FLAGS_tso_new_physical == 0) {
    DINGO_LOG(ERROR) << "tso_new_physical is empty";
    return;
  }

  if (FLAGS_tso_save_physical == 0) {
    DINGO_LOG(WARNING) << "tso_save_physical is empty, use tso_new_physical as tso_save_physical";
    FLAGS_tso_save_physical = FLAGS_tso_new_physical;
  }

  if (FLAGS_tso_new_logical == 0) {
    DINGO_LOG(WARNING) << "tso_new_logical is empty, use 0";
  }

  request.set_op_type(::dingodb::pb::meta::TsoOpType::OP_RESET_TSO);
  request.set_save_physical(FLAGS_tso_save_physical);
  request.mutable_current_timestamp()->set_physical(FLAGS_tso_new_physical);
  request.mutable_current_timestamp()->set_logical(FLAGS_tso_new_logical);
  request.set_force(false);

  auto status = coordinator_interaction->SendRequest("TsoService", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SendGetDeletedTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetDeletedTableRequest request;
  dingodb::pb::meta::GetDeletedTableResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (!FLAGS_id.empty()) {
    table_id->set_entity_id(std::stol(FLAGS_id));
  }

  auto status = coordinator_interaction->SendRequest("GetDeletedTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto& table : response.table_definition_with_ids()) {
    DINGO_LOG(INFO) << "table_id=[" << table.table_id().entity_id() << "]"
                    << "table_name=[" << table.table_definition().name() << "]"
                    << " detail: " << table.ShortDebugString();
  }
  DINGO_LOG(INFO) << "Deleted table count=" << response.table_definition_with_ids_size();
}

void SendGetDeletedIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetDeletedIndexRequest request;
  dingodb::pb::meta::GetDeletedIndexResponse response;

  auto* index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (!FLAGS_id.empty()) {
    index_id->set_entity_id(std::stol(FLAGS_id));
  }

  auto status = coordinator_interaction->SendRequest("GetDeletedIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto& index : response.table_definition_with_ids()) {
    DINGO_LOG(INFO) << "index_id=[" << index.table_id().entity_id() << "]"
                    << "index_name=[" << index.table_definition().name() << "]"
                    << " detail: " << index.ShortDebugString();
  }
  DINGO_LOG(INFO) << "Deleted index count=" << response.table_definition_with_ids_size();
}

void SendCleanDeletedTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CleanDeletedTableRequest request;
  dingodb::pb::meta::CleanDeletedTableResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (!FLAGS_id.empty()) {
    table_id->set_entity_id(std::stol(FLAGS_id));
  }

  auto status = coordinator_interaction->SendRequest("CleanDeletedTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SendCleanDeletedIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CleanDeletedIndexRequest request;
  dingodb::pb::meta::CleanDeletedIndexResponse response;

  auto* index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (!FLAGS_id.empty()) {
    index_id->set_entity_id(std::stol(FLAGS_id));
  }

  auto status = coordinator_interaction->SendRequest("CleanDeletedIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SendUpdateTables(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  bool with_increment = false;

  dingodb::pb::meta::UpdateTablesRequest request;
  dingodb::pb::meta::UpdateTablesResponse response;

  if (FLAGS_table_id == 0) {
    DINGO_LOG(WARNING) << "table_id is empty";
    return;
  }

  if (FLAGS_def_version == 0) {
    DINGO_LOG(WARNING) << "version is empty";
    return;
  }

  if (FLAGS_part_count == 0) {
    FLAGS_part_count = 1;
  }
  uint32_t part_count = FLAGS_part_count;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = 0;
    int ret = GetCreateTableId(coordinator_interaction, new_part_id);
    if (ret != 0) {
      DINGO_LOG(WARNING) << "GetCreateTableId failed";
      return;
    }
    part_ids.push_back(new_part_id);
  }

  auto* definition_with_id = request.mutable_table_definition_with_id();
  auto* table_id = definition_with_id->mutable_table_id();
  if (!FLAGS_is_updating_index) {
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  } else {
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  }
  table_id->set_entity_id(FLAGS_table_id);

  // string name = 1;
  auto* table_definition = definition_with_id->mutable_table_definition();
  table_definition->set_name(FLAGS_name);

  if (FLAGS_replica > 0) {
    table_definition->set_replica(FLAGS_replica);
  }

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
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
  table_definition->set_version(FLAGS_def_version);
  // uint64 ttl = 5;
  table_definition->set_ttl(0);
  // PartitionRule table_partition = 6;
  // Engine engine = 7;
  if (FLAGS_engine != "rocksdb" && FLAGS_engine != "bdb") {
    DINGO_LOG(ERROR) << "engine must be rocksdb or bdb";
    return;
  }
  table_definition->set_engine(GetEngine(FLAGS_engine));
  // map<string, string> properties = 8;
  auto* prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add partition_rule
  // repeated string columns = 1;
  // PartitionStrategy strategy = 2;
  auto* partition_rule = table_definition->mutable_table_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  for (int i = 0; i < part_count; i++) {
    auto* part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(FLAGS_table_id);
    part->mutable_range()->set_start_key(client::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  auto status = coordinator_interaction->SendRequest("UpdateTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendAddIndexOnTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  bool with_increment = false;

  dingodb::pb::meta::AddIndexOnTableRequest request;
  dingodb::pb::meta::AddIndexOnTableResponse response;

  if (FLAGS_table_id == 0) {
    DINGO_LOG(WARNING) << "table_id is empty";
    return;
  }

  if (FLAGS_index_id == 0) {
    DINGO_LOG(WARNING) << "index_id is empty, please use CreateTableIds to get one id";
    return;
  }

  if (FLAGS_def_version == 0) {
    DINGO_LOG(WARNING) << "def_version is empty, must > 0";
    return;
  }

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  if (FLAGS_part_count == 0) {
    FLAGS_part_count = 1;
  }
  uint32_t part_count = FLAGS_part_count;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = 0;
    int ret = GetCreateTableId(coordinator_interaction, new_part_id);
    if (ret != 0) {
      DINGO_LOG(WARNING) << "GetCreateTableId failed";
      return;
    }
    part_ids.push_back(new_part_id);
  }

  request.mutable_table_id()->set_entity_id(FLAGS_table_id);
  request.mutable_table_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  auto* definition_with_id = request.mutable_table_definition_with_id();
  auto* index_id = definition_with_id->mutable_table_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_entity_id(FLAGS_index_id);

  // string name = 1;
  auto* table_definition = definition_with_id->mutable_table_definition();
  table_definition->set_name(FLAGS_name);

  if (FLAGS_replica > 0) {
    table_definition->set_replica(FLAGS_replica);
  }

  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
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
  table_definition->set_version(FLAGS_def_version);
  // uint64 ttl = 5;
  table_definition->set_ttl(0);
  // PartitionRule table_partition = 6;
  // Engine engine = 7;
  if (FLAGS_engine != "rocksdb" && FLAGS_engine != "bdb") {
    DINGO_LOG(ERROR) << "engine must be rocksdb or bdb";
    return;
  }
  table_definition->set_engine(GetEngine(FLAGS_engine));
  // map<string, string> properties = 8;
  auto* prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add index_parameter
  auto* index_parameter = table_definition->mutable_index_parameter();
  index_parameter->set_index_type(::dingodb::pb::common::IndexType::INDEX_TYPE_SCALAR);
  index_parameter->mutable_scalar_index_parameter()->set_scalar_index_type(
      ::dingodb::pb::common::ScalarIndexType::SCALAR_INDEX_TYPE_LSM);

  // add partition_rule
  // repeated string columns = 1;
  // PartitionStrategy strategy = 2;
  auto* partition_rule = table_definition->mutable_table_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  for (int i = 0; i < part_count; i++) {
    auto* part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(FLAGS_table_id);
    part->mutable_range()->set_start_key(client::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  auto status = coordinator_interaction->SendRequest("AddIndexOnTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendDropIndexOnTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  bool with_increment = false;

  dingodb::pb::meta::DropIndexOnTableRequest request;
  dingodb::pb::meta::DropIndexOnTableResponse response;

  if (FLAGS_table_id == 0) {
    DINGO_LOG(WARNING) << "table_id is empty";
    return;
  }

  if (FLAGS_index_id == 0) {
    DINGO_LOG(WARNING) << "index_id is empty";
    return;
  }

  request.mutable_table_id()->set_entity_id(FLAGS_table_id);
  request.mutable_table_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  request.mutable_index_id()->set_entity_id(FLAGS_index_id);
  request.mutable_index_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);

  auto status = coordinator_interaction->SendRequest("DropIndexOnTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendCreateTenant(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CreateTenantRequest request;
  dingodb::pb::meta::CreateTenantResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  if (FLAGS_comment.empty()) {
    DINGO_LOG(WARNING) << "comment is empty";
    return;
  }

  request.mutable_tenant()->set_id(FLAGS_tenant_id);
  request.mutable_tenant()->set_name(FLAGS_name);
  request.mutable_tenant()->set_comment(FLAGS_comment);

  auto status = coordinator_interaction->SendRequest("CreateTenant", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendUpdateTenant(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::UpdateTenantRequest request;
  dingodb::pb::meta::UpdateTenantResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  if (FLAGS_comment.empty()) {
    DINGO_LOG(WARNING) << "comment is empty";
    return;
  }

  request.mutable_tenant()->set_id(FLAGS_tenant_id);
  request.mutable_tenant()->set_name(FLAGS_name);
  request.mutable_tenant()->set_comment(FLAGS_comment);

  auto status = coordinator_interaction->SendRequest("UpdateTenant", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendDropTenant(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::DropTenantRequest request;
  dingodb::pb::meta::DropTenantResponse response;

  request.set_tenant_id(FLAGS_tenant_id);

  auto status = coordinator_interaction->SendRequest("DropTenant", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetTenants(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTenantsRequest request;
  dingodb::pb::meta::GetTenantsResponse response;

  if (FLAGS_tenant_id > 0) {
    request.add_tenant_ids(FLAGS_tenant_id);
  }

  auto status = coordinator_interaction->SendRequest("GetTenants", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

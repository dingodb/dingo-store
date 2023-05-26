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

#include <memory>
#include <string>

#include "client/coordinator_client_function.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "gflags/gflags_declare.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"

DECLARE_bool(log_each_request);
DECLARE_int32(timeout_ms);
DECLARE_string(id);
DECLARE_string(name);
DECLARE_int64(schema_id);
DECLARE_int64(replica);

void SendGetSchemas(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetSchemasRequest request;
  dingodb::pb::meta::GetSchemasResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  auto status = coordinator_interaction->SendRequest("GetSchemas", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();

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

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, this is schema_id";
    return;
  }
  schema_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetSchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();

  DINGO_LOG(INFO) << "schema_id=[" << response.schema().id().entity_id() << "]"
                  << "schema_name=[" << response.schema().name() << "]"
                  << "child_table_count=" << response.schema().table_ids_size();
  for (const auto& child_table_id : response.schema().table_ids()) {
    DINGO_LOG(INFO) << "child table_id=[" << child_table_id.entity_id() << "]";
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

  auto status = coordinator_interaction->SendRequest("GetSchemaByName", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();

  DINGO_LOG(INFO) << "schema_id=[" << response.schema().id().entity_id() << "]"
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

  auto status = coordinator_interaction->SendRequest("GetTablesCount", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << "table_count=" << response.tables_count();
}

void SendGetTables(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_schema_id > 0) {
    schema_id->set_entity_id(FLAGS_schema_id);
  }

  auto status = coordinator_interaction->SendRequest("GetTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  // DINGO_LOG_INFO << response.DebugString();

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
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, this table_id";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendGetTableByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetTableByNameRequest request;
  dingodb::pb::meta::GetTableByNameResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  request.set_table_name(FLAGS_name);

  auto status = coordinator_interaction->SendRequest("GetTableByName", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
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
  DINGO_LOG_INFO << response.DebugString();

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
  DINGO_LOG_INFO << response.DebugString();
}

void SendCreateTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, bool with_table_id,
                     bool with_increment) {
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

  if (with_table_id) {
    auto* table_id = request.mutable_table_id();
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
    table_id->set_parent_entity_id(schema_id->entity_id());
    if (FLAGS_id.empty()) {
      DINGO_LOG(WARNING) << "id is empty";
      return;
    }
    table_id->set_entity_id(std::stol(FLAGS_id));
  }

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
    column->set_sql_type(::dingodb::pb::meta::SqlType::SQL_TYPE_BIGINT);
    column->set_element_type(::dingodb::pb::meta::ElementType::ELEM_TYPE_INT64);
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
  table_definition->set_engine(::dingodb::pb::common::Engine::ENG_ROCKSDB);
  // map<string, string> properties = 8;
  auto* prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add partition_rule
  // repeated string columns = 1;
  // PartitionStrategy strategy = 2;
  // RangePartition range_partition = 3;
  // HashPartition hash_partition = 4;
  auto* partition_rule = table_definition->mutable_table_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  auto* range_partition = partition_rule->mutable_range_partition();

  if (with_table_id) {
    auto* part_range = range_partition->add_ranges();
    part_range->set_start_key(EncodeUint64(std::stol(FLAGS_id)));
    part_range->set_end_key(EncodeUint64(1 + std::stol(FLAGS_id)));
  } else {
    for (int i = 0; i < 2; i++) {
      auto* part_range = range_partition->add_ranges();
      auto* part_range_start = part_range->mutable_start_key();
      part_range_start->assign(std::to_string(i * 100));
      auto* part_range_end = part_range->mutable_end_key();
      part_range_end->assign(std::to_string((i + 1) * 100));
    }
  }

  auto status = coordinator_interaction->SendRequest("CreateTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
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
  DINGO_LOG_INFO << response.DebugString();
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
  DINGO_LOG_INFO << response.DebugString();
}

void SendCreateSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CreateSchemaRequest request;
  dingodb::pb::meta::CreateSchemaResponse response;

  if (FLAGS_name.empty()) {
    DINGO_LOG(WARNING) << "name is empty";
    return;
  }

  auto* parent_schema_id = request.mutable_parent_schema_id();
  parent_schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  parent_schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  parent_schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_schema_name(FLAGS_name);

  auto status = coordinator_interaction->SendRequest("CreateSchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
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
  DINGO_LOG_INFO << response.DebugString();
}
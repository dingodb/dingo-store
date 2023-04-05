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

#include <string>

#include "client/coordinator_client_function.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"

DECLARE_bool(log_each_request);
DECLARE_int32(timeout_ms);
DECLARE_string(id);

void SendGetSchemas(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetSchemasRequest request;
  dingodb::pb::meta::GetSchemasResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  stub.GetSchemas(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " schema_id=" << request.schema_id().entity_id() << " schema_count=" << response.schemas_size()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    // DINGO_LOG(INFO) << response.DebugString();
    for (const auto& schema : response.schemas()) {
      DINGO_LOG(INFO) << "schema_id=[" << schema.id().entity_id() << "]"
                      << "schema_name=[" << schema.name() << "]"
                      << "child_schema_count=" << schema.schema_ids_size()
                      << "child_table_count=" << schema.table_ids_size();
      for (const auto& child_schema_id : schema.schema_ids()) {
        DINGO_LOG(INFO) << "child schema_id=[" << child_schema_id.entity_id() << "]";
      }
      for (const auto& child_table_id : schema.table_ids()) {
        DINGO_LOG(INFO) << "child table_id=[" << child_table_id.entity_id() << "]";
      }
    }

    // for (int32_t i = 0; i < response.schemas_size(); i++) {
    //   DINGO_LOG(INFO) << "schema_id=[" << response.schemas(i).id().entity_id() << "]"
    //             << "child_schema_count=" << response.schemas(i).schema_ids_size()
    //             << "child_table_count=" << response.schemas(i).table_ids_size();
    //   for (int32_t j = 0; j < response.schemas(i).schema_ids_size(); j++) {
    //     DINGO_LOG(INFO) << "child schema_id=[" << response.schemas(i).schema_ids(j).entity_id() << "]";
    //   }
    //   for (int32_t j = 0; j < response.schemas(i).table_ids_size(); j++) {
    //     DINGO_LOG(INFO) << "child table_id=[" << response.schemas(i).table_ids(j).entity_id() << "]";
    //   }
    // }
  }
}

void SendGetTablesCount(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  stub.GetTables(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " table_count=" << response.table_definition_with_ids_size()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
  }
}

void SendGetTables(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  stub.GetTables(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " table_count=" << response.table_definition_with_ids_size()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    // DINGO_LOG(INFO) << response.DebugString();
    for (const auto& table_definition_with_id : response.table_definition_with_ids()) {
      DINGO_LOG(INFO) << "table_id=[" << table_definition_with_id.table_id().entity_id() << "]"
                      << "table_name=" << table_definition_with_id.table_definition().name();
    }
  }
}

void SendGetTable(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetTableRequest request;
  dingodb::pb::meta::GetTableResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  stub.GetTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " table_id=" << request.table_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetTableRange(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
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

  stub.GetTableRange(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " table_id=" << request.table_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendCreateTableId(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::CreateTableIdRequest request;
  dingodb::pb::meta::CreateTableIdResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  stub.CreateTableId(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << " request=" << request.DebugString();
    DINGO_LOG(INFO) << " response=" << response.DebugString();
  }
}

void SendCreateTable(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub, bool with_table_id) {
  dingodb::pb::meta::CreateTableRequest request;
  dingodb::pb::meta::CreateTableResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  if (with_table_id) {
    auto* table_id = request.mutable_table_id();
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
    table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
    if (FLAGS_id.empty()) {
      DINGO_LOG(WARNING) << "id is empty";
      return;
    }
    table_id->set_entity_id(std::stol(FLAGS_id));
  }

  // string name = 1;
  auto* table_definition = request.mutable_table_definition();
  table_definition->set_name("t_test1");
  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type(::dingodb::pb::meta::SqlType::SQL_TYPE_INTEGER);
    column->set_element_type(::dingodb::pb::meta::ElementType::ELEM_TYPE_BYTES);
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");
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

  for (int i = 0; i < 3; i++) {
    auto* part_range = range_partition->add_ranges();
    auto* part_range_start = part_range->mutable_start_key();
    part_range_start->assign(std::to_string(i * 100));
    auto* part_range_end = part_range->mutable_start_key();
    part_range_end->assign(std::to_string((i + 1) * 100));
  }

  stub.CreateTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << " request=" << request.DebugString();
    DINGO_LOG(INFO) << " response=" << response.DebugString();
  }
}

void SendDropTable(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
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

  stub.DropTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.table_id().parent_entity_id()
                    << " request table_id=" << request.table_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendDropSchema(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
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

  stub.DropSchema(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
    return;
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request parent_schema_id=" << request.schema_id().parent_entity_id()
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendCreateSchema(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::CreateSchemaRequest request;
  dingodb::pb::meta::CreateSchemaResponse response;

  auto* parent_schema_id = request.mutable_parent_schema_id();
  parent_schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  parent_schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  parent_schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_schema_name("test_create_schema");
  stub.CreateSchema(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
    return;
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request parent_schema_id=" << request.parent_schema_id().entity_id()
                    << " request schema_name=" << request.schema_name()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetTableMetrics(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
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

  stub.GetTableMetrics(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.table_id().parent_entity_id()
                    << " request table_id=" << request.table_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

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

#include "meta.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>

#include "client_v2/helper.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "coordinator/tso_control.h"

namespace client_v2 {

void SetUpMetaSubCommands(CLI::App &app) {
  SetUpCreateTable(app);
  SetUpGetTable(app);
  SetUpGetTableRange(app);
  SetUpGetTableByName(app);
  SetUpGenTso(app);
}

void SetUpMetaHello(CLI::App &app) {
  auto opt = std::make_shared<MetaHelloOptions>();
  auto *cmd = app.add_subcommand("MetaHello", "Meta hello")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("Hello", request, response);
  DINGO_LOG(INFO) << "SendRequest status: " << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetSchema(CLI::App &app) {
  auto opt = std::make_shared<GetSchemaOptions>();
  auto *cmd = app.add_subcommand("GetSchema", "Get schema ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetSchema(*opt); });
}

void RunGetSchema(GetSchemaOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetSchemaRequest request;
  dingodb::pb::meta::GetSchemaResponse response;
  request.set_tenant_id(opt.tenant_id);

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(opt.schema_id);

  auto status = coordinator_interaction_meta->SendRequest("GetSchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  DINGO_LOG(INFO) << "tenant_id=[" << response.schema().tenant_id() << "]"
                  << "schema_id=[" << response.schema().id().entity_id() << "]"
                  << "schema_name=[" << response.schema().name() << "]"
                  << "child_table_count=" << response.schema().table_ids_size();
  for (const auto &child_table_id : response.schema().table_ids()) {
    DINGO_LOG(INFO) << "child table_id=[" << child_table_id.entity_id() << "]";
  }
  for (const auto &child_table_id : response.schema().index_ids()) {
    DINGO_LOG(INFO) << "child index_id=[" << child_table_id.entity_id() << "]";
  }
}

void SetUpGetSchemas(CLI::App &app) {
  auto opt = std::make_shared<GetSchemasOptions>();
  auto *cmd = app.add_subcommand("GetSchema", "Get schema ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetSchemas(*opt); });
}

void RunGetSchemas(GetSchemasOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetSchemasRequest request;
  dingodb::pb::meta::GetSchemasResponse response;

  request.set_tenant_id(opt.tenant_id);

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = coordinator_interaction_meta->SendRequest("GetSchemas", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  for (const auto &schema : response.schemas()) {
    DINGO_LOG(INFO) << "schema_id=[" << schema.id().entity_id() << "]"
                    << "schema_name=[" << schema.name() << "]"
                    << "child_table_count=" << schema.table_ids_size();
    for (const auto &child_table_id : schema.table_ids()) {
      DINGO_LOG(INFO) << "child table_id=[" << child_table_id.entity_id() << "]";
    }
  }
}

void SetUpGetSchemaByName(CLI::App &app) {
  auto opt = std::make_shared<GetSchemaByNameOptions>();
  auto *cmd = app.add_subcommand("GetSchemaByName", "Get schema by name")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter schema name")
      ->required()
      ->group("Coordinator Manager Commands");
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

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = coordinator_interaction_meta->SendRequest("GetSchemaByName", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  DINGO_LOG(INFO) << "tenant_id=[" << response.schema().tenant_id() << "]"
                  << "schema_id=[" << response.schema().id().entity_id() << "]"
                  << "schema_name=[" << response.schema().name() << "]"
                  << "child_table_count=" << response.schema().table_ids_size();
  for (const auto &child_table_id : response.schema().table_ids()) {
    DINGO_LOG(INFO) << "child table_id=[" << child_table_id.entity_id() << "]";
  }
}

void SetUpGetTablesBySchema(CLI::App &app) {
  auto opt = std::make_shared<GetTablesBySchemaOptions>();
  auto *cmd = app.add_subcommand("GetSchemaByName", "Get schema by name")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");

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

  schema_id->set_entity_id(opt.schema_id);
  auto status = coordinator_interaction_meta->SendRequest("GetTablesBySchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto &table_definition_with_id : response.table_definition_with_ids()) {
    DINGO_LOG(INFO) << "table_id=[" << table_definition_with_id.table_id().entity_id() << "]"
                    << "table_name=[" << table_definition_with_id.table_definition().name() << "], column_count=["
                    << table_definition_with_id.table_definition().columns_size() << "]";
  }

  DINGO_LOG(INFO) << "table_count=" << response.table_definition_with_ids_size();
}

void SetUpGetTablesCount(CLI::App &app) {
  auto opt = std::make_shared<GetTablesCountOptions>();
  auto *cmd = app.add_subcommand("GetTablesCount", "Get tables count")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");

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

  auto status = coordinator_interaction_meta->SendRequest("GetTablesCount", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "table_count=" << response.tables_count();
}

void SetUpCreateTable(CLI::App &app) {
  auto opt = std::make_shared<CreateTableOptions>();
  auto *cmd = app.add_subcommand("CreateTable", "Create table")->group("Coordinator Manager Commands");
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
  int ret = client_v2::Helper::GetCreateTableIds(coordinator_interaction_meta, 1 + opt.part_count, new_ids);
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
  table_definition->set_name(opt.name);

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

  auto status = coordinator_interaction_meta->SendRequest("CreateTable", request, response);
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
  auto *cmd = app.add_subcommand("CreateTableIds", "Create tableIds")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");

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

  auto status = coordinator_interaction_meta->SendRequest("CreateTableIds", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
  DINGO_LOG(INFO) << "count = " << response.table_ids_size();
}

void SetUpCreateTableId(CLI::App &app) {
  auto opt = std::make_shared<CreateTableIdOptions>();
  auto *cmd = app.add_subcommand("CreateTableIds", "Create tableIds")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("CreateTableId", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDropTable(CLI::App &app) {
  auto opt = std::make_shared<DropTableOptions>();
  auto *cmd = app.add_subcommand("DropTable", "Drop Table")->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("DropTable", request, response);
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
  auto *cmd = app.add_subcommand("CreateSchema", "Create schema")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter schema name")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")
      ->required()
      ->group("Coordinator Manager Commands");
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

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = coordinator_interaction_meta->SendRequest("CreateSchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDropSchema(CLI::App &app) {
  auto opt = std::make_shared<DropSchemaOptions>();
  auto *cmd = app.add_subcommand("DropSchema", "Drop schema")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("DropSchema", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetTable(CLI::App &app) {
  auto opt = std::make_shared<GetTableOptions>();
  auto *cmd = app.add_subcommand("GetTable", "Get table")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_flag("--is_index", opt->is_index, "Request parameter is_index");
  cmd->add_option("--id", opt->id, "Request parameter table id")
      ->check(CLI::Range(1, std::numeric_limits<int32_t>::max()))
      ->required();
  cmd->callback([opt]() { RunGetTable(*opt); });
}

void RunGetTable(GetTableOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetTableRequest request;
  dingodb::pb::meta::GetTableResponse response;

  auto *table_id = request.mutable_table_id();
  if (opt.is_index) {
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  } else {
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  }
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  table_id->set_entity_id(opt.id);
  auto status = coordinator_interaction_meta->SendRequest("GetTable", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "get table failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "table_definition_with_id { " << std::endl;
  std::cout << "  tenant_id: " << response.table_definition_with_id().tenant_id() << std::endl;

  std::cout << "  table_id {" << std::endl;
  std::cout << "\t entity_type: "
            << dingodb::pb::meta::EntityType_Name(response.table_definition_with_id().table_id().entity_type())
            << std::endl;

  std::cout << "\t parent_entity_id: " << response.table_definition_with_id().table_id().parent_entity_id()
            << std::endl;

  std::cout << "\t entity_id: " << response.table_definition_with_id().table_id().entity_id() << " }" << std::endl;
  std::cout << "  }" << std::endl;

  std::cout << "  table_definition {" << std::endl;
  std::cout << "\t name: " << response.table_definition_with_id().table_definition().name() << "\n"
            << "\t version: " << response.table_definition_with_id().table_definition().version() << "\n"
            << "\t ttl: " << response.table_definition_with_id().table_definition().ttl() << "\n"
            << "\t auto_increment: " << response.table_definition_with_id().table_definition().auto_increment() << "\n"
            << "\t create_sql: " << response.table_definition_with_id().table_definition().create_sql() << "\n"
            << "\t charset: " << response.table_definition_with_id().table_definition().charset() << "\n"
            << "\t collate: " << response.table_definition_with_id().table_definition().collate() << "\n"
            << "\t replica: " << response.table_definition_with_id().table_definition().replica() << "\n"
            << "\t engine"
            << dingodb::pb::common::Engine_Name(response.table_definition_with_id().table_definition().engine()) << "\n"
            << "\t create_timestamp: " << response.table_definition_with_id().table_definition().create_timestamp()
            << "\n"
            << "\t update_timestamp: " << response.table_definition_with_id().table_definition().update_timestamp()
            << "\n"
            << "\t delete_timestamp:" << response.table_definition_with_id().table_definition().delete_timestamp()
            << "\n"
            << "\t revision: " << response.table_definition_with_id().table_definition().revision() << "\n";
  for (auto const &columns : response.table_definition_with_id().table_definition().columns()) {
    std::cout << "\t columns { \n";
    std::cout << "\t \t name:" << columns.name() << "\n"
              << "\t \t sql_type:" << columns.sql_type() << "\n"
              << "\t \t element_type:" << columns.element_type() << "\n"
              << "\t \t precision: " << columns.precision() << "\n"
              << "\t \t scale: " << columns.scale() << "\n"
              << "\t \t nullable: " << columns.nullable() << "\n"
              << "\t \t indexOfKey: " << columns.indexofkey() << "\n"
              << "\t \t has_default_val: " << columns.has_default_val() << "\n"
              << "\t \t default_val: " << columns.default_val() << "\n"
              << "\t \t is_auto_increment: " << columns.is_auto_increment() << "\n"
              << "\t \t state: " << columns.state() << "\n"
              << "\t \t comment: " << columns.comment() << "\n"
              << "\t \t create_version: " << columns.create_version() << "\n"
              << "\t \t update_version: " << columns.update_version() << "\n"
              << "\t \t delete_version: " << columns.delete_version() << "\n"
              << "\t }\n";
  }
  for (auto const &t : response.table_definition_with_id().table_definition().properties()) {
    std::cout << "\t properties { key:" << t.first << " value:" << t.second << "} \n";
  }
  if (response.table_definition_with_id().table_definition().index_parameter().index_type() ==
      dingodb::pb::common::INDEX_TYPE_VECTOR) {
    const auto &vector_index_parameter =
        response.table_definition_with_id().table_definition().index_parameter().vector_index_parameter();
    std::cout << "\t vector_index_parameter{ \n";
    std::cout << "\t \t vector_index_type: "
              << dingodb::pb::common::VectorIndexType_Name(vector_index_parameter.vector_index_type()) << "\n";
    if (vector_index_parameter.has_flat_parameter()) {
      std::cout << "\t \t flat_param: { dimension: " << vector_index_parameter.flat_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.flat_parameter().metric_type())
                << " }\n";
    } else if (vector_index_parameter.has_ivf_flat_parameter()) {
      std::cout << "\t \t ivf_flat_param: { dimension: " << vector_index_parameter.ivf_flat_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.ivf_flat_parameter().metric_type())
                << " , ncentroids: " << vector_index_parameter.ivf_flat_parameter().ncentroids() << " }\n";
    } else if (vector_index_parameter.has_ivf_pq_parameter()) {
      std::cout << "\t \t ivf_pg_param: { dimension: " << vector_index_parameter.ivf_pq_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.ivf_pq_parameter().metric_type())
                << " , ncentroids: " << vector_index_parameter.ivf_pq_parameter().ncentroids()
                << " , nsubvector: " << vector_index_parameter.ivf_pq_parameter().nsubvector()
                << " , bucket_init_size: " << vector_index_parameter.ivf_pq_parameter().bucket_init_size()
                << " , bucket_max_size: " << vector_index_parameter.ivf_pq_parameter().bucket_max_size()
                << " , nbits_per_idx: " << vector_index_parameter.ivf_pq_parameter().nbits_per_idx() << " }\n";
    } else if (vector_index_parameter.has_hnsw_parameter()) {
      std::cout << "\t \t hnsw_param: { dimension: " << vector_index_parameter.hnsw_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.hnsw_parameter().metric_type())
                << " , ef_construction: " << vector_index_parameter.hnsw_parameter().efconstruction()
                << " , nlinks: " << vector_index_parameter.hnsw_parameter().nlinks()
                << " , max_elements: " << vector_index_parameter.hnsw_parameter().max_elements() << " }\n";
    } else if (vector_index_parameter.has_diskann_parameter()) {
#if 0
      std::cout << "\t \t diskann_param: { dimension: " << vector_index_parameter.diskann_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.diskann_parameter().metric_type())
                << " , num_trees: " << vector_index_parameter.diskann_parameter().num_trees()
                << " , num_neighbors: " << vector_index_parameter.diskann_parameter().num_neighbors()
                << " , num_threads: " << vector_index_parameter.diskann_parameter().num_trees() << " }\n";
#endif
    } else if (vector_index_parameter.has_bruteforce_parameter()) {
      std::cout << "\t \t bruteforce_param: { dimension: " << vector_index_parameter.bruteforce_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.bruteforce_parameter().metric_type())
                << " }\n";
    }
    std::cout << "\t \t scalar_schema:{\n";
    for (auto const &t : vector_index_parameter.scalar_schema().fields()) {
      std::cout << "\t \t \t key: " << t.key()
                << " , field_type: " << dingodb::pb::common::ScalarFieldType_Name(t.field_type())
                << " , enable_speed_up: " << t.enable_speed_up() << "\n";
    }
    std::cout << "\t \t }\n";

    std::cout << "\t } \n";
  } else if (response.table_definition_with_id().table_definition().index_parameter().index_type() ==
             dingodb::pb::common::INDEX_TYPE_DOCUMENT) {
    const auto &document_index_parameter =
        response.table_definition_with_id().table_definition().index_parameter().document_index_parameter();
    std::cout << "\t document_index_parameter{ \n";
    std::cout << "\t \t json_parameter: " << document_index_parameter.json_parameter() << "\n";
    std::cout << "\t \t scalar_schema:{\n";
    for (auto const &t : document_index_parameter.scalar_schema().fields()) {
      std::cout << "\t \t \t key: " << t.key()
                << " , field_type: " << dingodb::pb::common::ScalarFieldType_Name(t.field_type())
                << " , enable_speed_up: " << t.enable_speed_up() << "\n";
    }
    std::cout << "\t \t }\n";
    std::cout << "\t }\n";
  }
  std::cout << "  }" << std::endl;
}

void SetUpGetTableByName(CLI::App &app) {
  auto opt = std::make_shared<GetTableByNameOptions>();
  auto *cmd = app.add_subcommand("GetTableByName", "Get table by name")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema_id")->required();
  cmd->add_option("--name", opt->name, "Request parameter name")->required();
  cmd->callback([opt]() { RunGetTableByName(*opt); });
}

void RunGetTableByName(GetTableByNameOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetTableByNameRequest request;
  dingodb::pb::meta::GetTableByNameResponse response;

  request.set_table_name(opt.name);
  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(opt.schema_id);

  auto status = coordinator_interaction_meta->SendRequest("GetTableByName", request, response);

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "get table by name failed, error: "
                     << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name()
                     << " " << response.error().errmsg();
    std::cout << "get table by name failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }

  std::cout << "table_definition_with_id { " << std::endl;
  std::cout << "  tenant_id: " << response.table_definition_with_id().tenant_id() << std::endl;

  std::cout << "  table_id {" << std::endl;
  std::cout << "\t entity_type: "
            << dingodb::pb::meta::EntityType_Name(response.table_definition_with_id().table_id().entity_type())
            << std::endl;

  std::cout << "\t parent_entity_id: " << response.table_definition_with_id().table_id().parent_entity_id()
            << std::endl;

  std::cout << "\t entity_id: " << response.table_definition_with_id().table_id().entity_id() << " }" << std::endl;
  std::cout << "  }" << std::endl;

  std::cout << "  table_definition {" << std::endl;
  std::cout << "\t name: " << response.table_definition_with_id().table_definition().name() << "\n"
            << "\t version: " << response.table_definition_with_id().table_definition().version() << "\n"
            << "\t ttl: " << response.table_definition_with_id().table_definition().ttl() << "\n"
            << "\t auto_increment: " << response.table_definition_with_id().table_definition().auto_increment() << "\n"
            << "\t create_sql: " << response.table_definition_with_id().table_definition().create_sql() << "\n"
            << "\t charset: " << response.table_definition_with_id().table_definition().charset() << "\n"
            << "\t collate: " << response.table_definition_with_id().table_definition().collate() << "\n"
            << "\t replica: " << response.table_definition_with_id().table_definition().replica() << "\n"
            << "\t engine"
            << dingodb::pb::common::Engine_Name(response.table_definition_with_id().table_definition().engine()) << "\n"
            << "\t create_timestamp: " << response.table_definition_with_id().table_definition().create_timestamp()
            << "\n"
            << "\t update_timestamp: " << response.table_definition_with_id().table_definition().update_timestamp()
            << "\n"
            << "\t delete_timestamp:" << response.table_definition_with_id().table_definition().delete_timestamp()
            << "\n"
            << "\t revision: " << response.table_definition_with_id().table_definition().revision() << "\n";
  for (auto const &columns : response.table_definition_with_id().table_definition().columns()) {
    std::cout << "\t columns { \n";
    std::cout << "\t \t name:" << columns.name() << "\n"
              << "\t \t sql_type:" << columns.sql_type() << "\n"
              << "\t \t element_type:" << columns.element_type() << "\n"
              << "\t \t precision: " << columns.precision() << "\n"
              << "\t \t scale: " << columns.scale() << "\n"
              << "\t \t nullable: " << columns.nullable() << "\n"
              << "\t \t indexOfKey: " << columns.indexofkey() << "\n"
              << "\t \t has_default_val: " << columns.has_default_val() << "\n"
              << "\t \t default_val: " << columns.default_val() << "\n"
              << "\t \t is_auto_increment: " << columns.is_auto_increment() << "\n"
              << "\t \t state: " << columns.state() << "\n"
              << "\t \t comment: " << columns.comment() << "\n"
              << "\t \t create_version: " << columns.create_version() << "\n"
              << "\t \t update_version: " << columns.update_version() << "\n"
              << "\t \t delete_version: " << columns.delete_version() << "\n"
              << "\t }\n";
  }
  for (auto const &t : response.table_definition_with_id().table_definition().properties()) {
    std::cout << "\t properties { key:" << t.first << " value:" << t.second << "} \n";
  }
  if (response.table_definition_with_id().table_definition().index_parameter().index_type() ==
      dingodb::pb::common::INDEX_TYPE_VECTOR) {
    const auto &vector_index_parameter =
        response.table_definition_with_id().table_definition().index_parameter().vector_index_parameter();
    std::cout << "\t vector_index_parameter{ \n";
    std::cout << "\t \t vector_index_type: "
              << dingodb::pb::common::VectorIndexType_Name(vector_index_parameter.vector_index_type()) << "\n";
    if (vector_index_parameter.has_flat_parameter()) {
      std::cout << "\t \t flat_param: { dimension: " << vector_index_parameter.flat_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.flat_parameter().metric_type())
                << " }\n";
    } else if (vector_index_parameter.has_ivf_flat_parameter()) {
      std::cout << "\t \t ivf_flat_param: { dimension: " << vector_index_parameter.ivf_flat_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.ivf_flat_parameter().metric_type())
                << " , ncentroids: " << vector_index_parameter.ivf_flat_parameter().ncentroids() << " }\n";
    } else if (vector_index_parameter.has_ivf_pq_parameter()) {
      std::cout << "\t \t ivf_pg_param: { dimension: " << vector_index_parameter.ivf_pq_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.ivf_pq_parameter().metric_type())
                << " , ncentroids: " << vector_index_parameter.ivf_pq_parameter().ncentroids()
                << " , nsubvector: " << vector_index_parameter.ivf_pq_parameter().nsubvector()
                << " , bucket_init_size: " << vector_index_parameter.ivf_pq_parameter().bucket_init_size()
                << " , bucket_max_size: " << vector_index_parameter.ivf_pq_parameter().bucket_max_size()
                << " , nbits_per_idx: " << vector_index_parameter.ivf_pq_parameter().nbits_per_idx() << " }\n";
    } else if (vector_index_parameter.has_hnsw_parameter()) {
      std::cout << "\t \t hnsw_param: { dimension: " << vector_index_parameter.hnsw_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.hnsw_parameter().metric_type())
                << " , ef_construction: " << vector_index_parameter.hnsw_parameter().efconstruction()
                << " , nlinks: " << vector_index_parameter.hnsw_parameter().nlinks()
                << " , max_elements: " << vector_index_parameter.hnsw_parameter().max_elements() << " }\n";
    } else if (vector_index_parameter.has_diskann_parameter()) {
#if 0
      std::cout << "\t \t diskann_param: { dimension: " << vector_index_parameter.diskann_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.diskann_parameter().metric_type())
                << " , num_trees: " << vector_index_parameter.diskann_parameter().num_trees()
                << " , num_neighbors: " << vector_index_parameter.diskann_parameter().num_neighbors()
                << " , num_threads: " << vector_index_parameter.diskann_parameter().num_trees() << " }\n";
#endif
    } else if (vector_index_parameter.has_bruteforce_parameter()) {
      std::cout << "\t \t bruteforce_param: { dimension: " << vector_index_parameter.bruteforce_parameter().dimension()
                << " , metric_type: "
                << dingodb::pb::common::MetricType_Name(vector_index_parameter.bruteforce_parameter().metric_type())
                << " }\n";
    }
    std::cout << "\t \t scalar_schema:{\n";
    for (auto const &t : vector_index_parameter.scalar_schema().fields()) {
      std::cout << "\t \t \t key: " << t.key()
                << " , field_type: " << dingodb::pb::common::ScalarFieldType_Name(t.field_type())
                << " , enable_speed_up: " << t.enable_speed_up() << "\n";
    }
    std::cout << "\t \t }\n";

    std::cout << "\t } \n";
  } else if (response.table_definition_with_id().table_definition().index_parameter().index_type() ==
             dingodb::pb::common::INDEX_TYPE_DOCUMENT) {
    const auto &document_index_parameter =
        response.table_definition_with_id().table_definition().index_parameter().document_index_parameter();
    std::cout << "\t document_index_parameter{ \n";
    std::cout << "\t \t json_parameter: " << document_index_parameter.json_parameter() << "\n";
    std::cout << "\t \t scalar_schema:{\n";
    for (auto const &t : document_index_parameter.scalar_schema().fields()) {
      std::cout << "\t \t \t key: " << t.key()
                << " , field_type: " << dingodb::pb::common::ScalarFieldType_Name(t.field_type())
                << " , enable_speed_up: " << t.enable_speed_up() << "\n";
    }
    std::cout << "\t \t }\n";
    std::cout << "\t }\n";
  }
  std::cout << "  }" << std::endl;
}

void SetUpGetTableRange(CLI::App &app) {
  auto opt = std::make_shared<GetTableRangeOptions>();
  auto *cmd = app.add_subcommand("GetTableRange", "Get table range")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetTableRange", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "get table failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }

  for (const auto &it : response.table_range().range_distribution()) {
    std::cout << "region_id=[" << it.id().entity_id() << "]"
              << "range=[" << dingodb::Helper::StringToHex(it.range().start_key()) << ","
              << dingodb::Helper::StringToHex(it.range().end_key()) << "]"
              << " leader=[" << it.leader().host() << ":" << it.leader().port() << "]" << std::endl;
  }
}

void SetUpGetTableMetrics(CLI::App &app) {
  auto opt = std::make_shared<GetTableMetricsOptions>();
  auto *cmd = app.add_subcommand("GetTableByName", "Get table by name")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetTableMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSwitchAutoSplit(CLI::App &app) {
  auto opt = std::make_shared<SwitchAutoSplitOptions>();
  auto *cmd = app.add_subcommand("SwitchAutoSplit", "Switch auto split")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--auto_split", opt->auto_split, "Request parameter auto_split")
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("SwitchAutoSplit", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpGetDeletedTable(CLI::App &app) {
  auto opt = std::make_shared<GetDeletedTableOptions>();
  auto *cmd = app.add_subcommand("GetDeletedTable", "Get deleted table")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetDeletedTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto &table : response.table_definition_with_ids()) {
    DINGO_LOG(INFO) << "table_id=[" << table.table_id().entity_id() << "]"
                    << "table_name=[" << table.table_definition().name() << "]"
                    << " detail: " << table.ShortDebugString();
  }
  DINGO_LOG(INFO) << "Deleted table count=" << response.table_definition_with_ids_size();
}

void SetUpGetDeletedIndex(CLI::App &app) {
  auto opt = std::make_shared<GetDeletedIndexOptions>();
  auto *cmd = app.add_subcommand("GetDeletedIndex", "Get deleted index")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetDeletedIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto &index : response.table_definition_with_ids()) {
    DINGO_LOG(INFO) << "index_id=[" << index.table_id().entity_id() << "]"
                    << "index_name=[" << index.table_definition().name() << "]"
                    << " detail: " << index.ShortDebugString();
  }
  DINGO_LOG(INFO) << "Deleted index count=" << response.table_definition_with_ids_size();
}

void SetUpCleanDeletedTable(CLI::App &app) {
  auto opt = std::make_shared<CleanDeletedTableOptions>();
  auto *cmd = app.add_subcommand("CleanDeletedTable", "Clean deleted table")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
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
  auto status = coordinator_interaction_meta->SendRequest("CleanDeletedTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpCleanDeletedIndex(CLI::App &app) {
  auto opt = std::make_shared<CleanDeletedIndexOptions>();
  auto *cmd = app.add_subcommand("CleanDeletedIndex", "Clean deleted index")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter index id")->required()->group("Coordinator Manager Commands");
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
  auto status = coordinator_interaction_meta->SendRequest("CleanDeletedIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpCreateTenant(CLI::App &app) {
  auto opt = std::make_shared<CreateTenantOptions>();
  auto *cmd = app.add_subcommand("CreateTenant", "Create tenant")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter name")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--comment", opt->comment, "Request parameter comment")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->tenant_id, "Request parameter tenant id")
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("CreateTenant", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateTenant(CLI::App &app) {
  auto opt = std::make_shared<UpdateTenantOptions>();
  auto *cmd = app.add_subcommand("UpdateTenant", "Update tenant")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter name")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--comment", opt->comment, "Request parameter comment")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->tenant_id, "Request parameter tenant id")
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("UpdateTenant", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDropTenant(CLI::App &app) {
  auto opt = std::make_shared<DropTenantOptions>();
  auto *cmd = app.add_subcommand("DropTenant", "Drop tenant")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->tenant_id, "Request parameter tenant id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunDropTenant(*opt); });
}

void RunDropTenant(DropTenantOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::DropTenantRequest request;
  dingodb::pb::meta::DropTenantResponse response;

  request.set_tenant_id(opt.tenant_id);

  auto status = coordinator_interaction_meta->SendRequest("DropTenant", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetTenant(CLI::App &app) {
  auto opt = std::make_shared<GetTenantOptions>();
  auto *cmd = app.add_subcommand("GetTenant", "Get tenant")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->tenant_id, "Request parameter tenant id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetTenant(*opt); });
}

void RunGetTenant(GetTenantOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetTenantsRequest request;
  dingodb::pb::meta::GetTenantsResponse response;

  request.add_tenant_ids(opt.tenant_id);
  auto status = coordinator_interaction_meta->SendRequest("GetTenants", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetIndexes(CLI::App &app) {
  auto opt = std::make_shared<GetIndexesOptions>();
  auto *cmd = app.add_subcommand("GetIndexs", "Get index")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");
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
  auto status = coordinator_interaction_meta->SendRequest("GetIndexes", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  // DINGO_LOG(INFO) << response.DebugString();

  for (const auto &index_definition_with_id : response.index_definition_with_ids()) {
    DINGO_LOG(INFO) << "index_id=[" << index_definition_with_id.index_id().entity_id() << "]"
                    << "index_name=[" << index_definition_with_id.index_definition().name() << "], index_type=["
                    << dingodb::pb::common::IndexType_Name(
                           index_definition_with_id.index_definition().index_parameter().index_type())
                    << "]";
  }

  DINGO_LOG(INFO) << "index_count=" << response.index_definition_with_ids_size();
}

void SetUpGetIndexesCount(CLI::App &app) {
  auto opt = std::make_shared<GetIndexesCountOptions>();
  auto *cmd = app.add_subcommand("GetIndexesCount", "Get index count")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");
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
  auto status = coordinator_interaction_meta->SendRequest("GetIndexesCount", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "index_count=" << response.indexes_count();
}

void SetUpCreateIndex(CLI::App &app) {
  auto opt = std::make_shared<CreateIndexOptions>();
  auto *cmd = app.add_subcommand("CreateIndex", "Create index ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter region name")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--with_auto_increment", opt->with_auto_increment, "Request parameter with_auto_increment")
      ->default_val(true)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--with_scalar_schema", opt->with_scalar_schema, "Request parameter with_scalar_schema")
      ->default_val(true)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--replica", opt->replica, "Request parameter replica num, must greater than 0")
      ->default_val(3)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--vector_index_type", opt->vector_index_type,
                  "Request parameter vector_index_type, hnsw|flat|ivf_flat|ivf_pq")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->group("Coordinator Manager Commands");
  cmd->add_option("--metrics_type", opt->metrics_type, "Request parameter metrics_type, L2|IP|COSINE")
      ->ignore_case()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--max_elements", opt->max_elements, "Request parameter max_elements")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--efconstruction", opt->efconstruction, "Request parameter efconstruction")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--nlinks", opt->nlinks, "Request parameter nlinks")->group("Coordinator Manager Commands");
  cmd->add_option("--ncentroids", opt->ncentroids, "Request parameter ncentroids, ncentroids default 10")
      ->default_val(10)
      ->group("Coordinator Manager Commands");
  cmd->add_option("--nsubvector", opt->nsubvector, "Request parameter nsubvector, ivf pq default subvector nums 8")
      ->default_val(8)
      ->group("Coordinator Manager Commands");
  cmd->add_option("--nbits_per_idx", opt->nbits_per_idx,
                  "Request parameter nbits_per_idx, ivf pq default nbits_per_idx 8")
      ->default_val(8)
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunCreateIndex(*opt); });
}

void RunCreateIndex(CreateIndexOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateIndexRequest request;
  dingodb::pb::meta::CreateIndexResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  schema_id->set_entity_id(opt.schema_id);

  uint32_t part_count = opt.part_count;

  std::vector<int64_t> new_ids;
  int ret = client_v2::Helper::GetCreateTableIds(coordinator_interaction_meta, 1 + opt.part_count, new_ids);
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

  int64_t new_index_id = new_ids.at(0);
  DINGO_LOG(INFO) << "index_id = " << new_index_id;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = new_ids.at(1 + i);
    part_ids.push_back(new_part_id);
  }

  for (const auto &id : part_ids) {
    DINGO_LOG(INFO) << "part_id = " << id;
  }

  // setup index_id
  auto *index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(schema_id->entity_id());
  index_id->set_entity_id(new_index_id);

  // string name = 1;
  auto *index_definition = request.mutable_index_definition();
  index_definition->set_name(opt.name);

  if (opt.replica > 0) {
    index_definition->set_replica(opt.replica);
  }

  if (opt.with_auto_increment) {
    index_definition->set_with_auto_incrment(true);
    index_definition->set_auto_increment(1024);
  }

  // vector index parameter
  index_definition->mutable_index_parameter()->set_index_type(dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);
  auto *vector_index_parameter = index_definition->mutable_index_parameter()->mutable_vector_index_parameter();

  if (opt.vector_index_type == "hnsw") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
  } else if (opt.vector_index_type == "flat") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
  } else if (opt.vector_index_type == "bruteforce") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BRUTEFORCE);
  } else if (opt.vector_index_type == "ivf_flat") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT);
  } else if (opt.vector_index_type == "ivf_pq") {
    vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
  } else {
    DINGO_LOG(WARNING) << "vector_index_type is invalid, now only support hnsw and flat";
    return;
  }

  if (opt.dimension == 0) {
    DINGO_LOG(WARNING) << "dimension is empty";
    return;
  }

  dingodb::pb::common::MetricType metric_type;

  if (opt.metrics_type == "L2" || opt.metrics_type == "l2") {
    metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_L2;
  } else if (opt.metrics_type == "IP" || opt.metrics_type == "ip") {
    metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
  } else if (opt.metrics_type == "COSINE" || opt.metrics_type == "cosine") {
    metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE;
  } else {
    DINGO_LOG(WARNING) << "metrics_type is invalid, now only support L2, IP and COSINE";
    return;
  }

  if (opt.vector_index_type == "hnsw") {
    if (opt.max_elements < 0) {
      DINGO_LOG(WARNING) << "max_elements is negative";
      return;
    }
    if (opt.efconstruction == 0) {
      DINGO_LOG(WARNING) << "efconstruction is empty";
      return;
    }
    if (opt.nlinks == 0) {
      DINGO_LOG(WARNING) << "nlinks is empty";
      return;
    }

    DINGO_LOG(INFO) << "max_elements=" << opt.max_elements << ", dimension=" << opt.dimension;

    auto *hsnw_index_parameter = vector_index_parameter->mutable_hnsw_parameter();

    hsnw_index_parameter->set_dimension(opt.dimension);
    hsnw_index_parameter->set_metric_type(metric_type);
    hsnw_index_parameter->set_efconstruction(opt.efconstruction);
    hsnw_index_parameter->set_nlinks(opt.nlinks);
    hsnw_index_parameter->set_max_elements(opt.max_elements);
  } else if (opt.vector_index_type == "flat") {
    auto *flat_index_parameter = vector_index_parameter->mutable_flat_parameter();
    flat_index_parameter->set_dimension(opt.dimension);
    flat_index_parameter->set_metric_type(metric_type);
  } else if (opt.vector_index_type == "bruteforce") {
    auto *bruteforce_index_parameter = vector_index_parameter->mutable_bruteforce_parameter();
    bruteforce_index_parameter->set_dimension(opt.dimension);
    bruteforce_index_parameter->set_metric_type(metric_type);
  } else if (opt.vector_index_type == "ivf_flat") {
    auto *ivf_flat_index_parameter = vector_index_parameter->mutable_ivf_flat_parameter();
    ivf_flat_index_parameter->set_dimension(opt.dimension);
    ivf_flat_index_parameter->set_metric_type(metric_type);
    ivf_flat_index_parameter->set_ncentroids(opt.ncentroids);
  } else if (opt.vector_index_type == "ivf_pq") {
    auto *ivf_pq_index_parameter = vector_index_parameter->mutable_ivf_pq_parameter();
    ivf_pq_index_parameter->set_dimension(opt.dimension);
    ivf_pq_index_parameter->set_metric_type(metric_type);
    ivf_pq_index_parameter->set_ncentroids(opt.ncentroids);
    ivf_pq_index_parameter->set_nsubvector(opt.nsubvector);
    ivf_pq_index_parameter->set_nbits_per_idx(opt.nbits_per_idx);
  }

  index_definition->set_version(1);

  auto *partition_rule = index_definition->mutable_index_partition();
  auto *part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");

  for (int i = 0; i < part_count; i++) {
    auto *part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_index_id);
    part->mutable_range()->set_start_key(client_v2::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client_v2::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  if (opt.with_auto_increment) {
    DINGO_LOG(INFO) << "with_auto_increment";
    index_definition->set_auto_increment(100);
  }

  // scalar key speed up
  if (opt.with_scalar_schema) {
    auto *scalar_parameter = vector_index_parameter->mutable_scalar_schema();

    auto *field = scalar_parameter->add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("speedup_key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(true);

    field = scalar_parameter->add_fields();
    field->set_key("key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(false);

    field = scalar_parameter->add_fields();
    field->set_key("key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(false);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  auto status = coordinator_interaction_meta->SendRequest("CreateIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
  if (response.error().errcode() == 0) {
    DINGO_LOG(INFO) << "create index success, index_id==" << response.index_id().entity_id();
  }
}

void SetUpCreateDocumentIndex(CLI::App &app) {
  auto opt = std::make_shared<CreateDocumentIndexOptions>();
  auto *cmd =
      app.add_subcommand("CreateDocumentIndex", "Create document index ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter region name")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--with_auto_increment", opt->with_auto_increment, "Request parameter with_auto_increment")
      ->default_val(true)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--use_json_parameter", opt->use_json_parameter, "Request parameter use_json_parameter")
      ->default_val(true)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--replica", opt->replica, "Request parameter replica num, must greater than 0")
      ->default_val(3)
      ->required()
      ->group("Coordinator Manager Commands");

  cmd->callback([opt]() { RunCreateDocumentIndex(*opt); });
}
void RunCreateDocumentIndex(CreateDocumentIndexOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::CreateIndexRequest request;
  dingodb::pb::meta::CreateIndexResponse response;

  auto *schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  if (opt.schema_id > 0) {
    schema_id->set_entity_id(opt.schema_id);
  }

  uint32_t part_count = opt.part_count;

  std::vector<int64_t> new_ids;
  int ret = client_v2::Helper::GetCreateTableIds(coordinator_interaction_meta, 1 + opt.part_count, new_ids);
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

  int64_t new_index_id = new_ids.at(0);
  DINGO_LOG(INFO) << "index_id = " << new_index_id;

  std::vector<int64_t> part_ids;
  for (int i = 0; i < part_count; i++) {
    int64_t new_part_id = new_ids.at(1 + i);
    part_ids.push_back(new_part_id);
  }

  for (const auto &id : part_ids) {
    DINGO_LOG(INFO) << "part_id = " << id;
  }

  // setup index_id
  auto *index_id = request.mutable_index_id();
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id->set_parent_entity_id(schema_id->entity_id());
  index_id->set_entity_id(new_index_id);

  // string name = 1;
  auto *index_definition = request.mutable_index_definition();
  index_definition->set_name(opt.name);

  if (opt.replica > 0) {
    index_definition->set_replica(opt.replica);
  }

  if (opt.with_auto_increment) {
    index_definition->set_with_auto_incrment(true);
    index_definition->set_auto_increment(1024);
  }

  std::string multi_type_column_json =
      R"({"col1": { "tokenizer": { "type": "chinese"}}, "col2": { "tokenizer": {"type": "i64", "indexed": true }}, "col3": { "tokenizer": {"type": "f64", "indexed": true }}, "col4": { "tokenizer": {"type": "chinese"}} })";

  // document index parameter
  index_definition->mutable_index_parameter()->set_index_type(dingodb::pb::common::IndexType::INDEX_TYPE_DOCUMENT);
  auto *document_index_parameter = index_definition->mutable_index_parameter()->mutable_document_index_parameter();

  if (opt.use_json_parameter) {
    document_index_parameter->set_json_parameter(multi_type_column_json);
  }

  auto *scalar_schema = document_index_parameter->mutable_scalar_schema();
  auto *field_col1 = scalar_schema->add_fields();
  field_col1->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
  field_col1->set_key("col1");
  auto *field_col2 = scalar_schema->add_fields();
  field_col2->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
  field_col2->set_key("col2");
  auto *field_col3 = scalar_schema->add_fields();
  field_col3->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
  field_col3->set_key("col3");
  auto *field_col4 = scalar_schema->add_fields();
  field_col4->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
  field_col4->set_key("col4");

  index_definition->set_version(1);

  auto *partition_rule = index_definition->mutable_index_partition();
  auto *part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");

  for (int i = 0; i < part_count; i++) {
    auto *part = partition_rule->add_partitions();
    part->mutable_id()->set_entity_id(part_ids[i]);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_index_id);
    part->mutable_range()->set_start_key(client_v2::Helper::EncodeRegionRange(part_ids[i]));
    part->mutable_range()->set_end_key(client_v2::Helper::EncodeRegionRange(part_ids[i] + 1));
  }

  if (opt.with_auto_increment) {
    DINGO_LOG(INFO) << "with_auto_increment";
    index_definition->set_auto_increment(100);
  }

  DINGO_LOG(INFO) << "Request: " << request.DebugString();

  auto status = coordinator_interaction_meta->SendRequest("CreateIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
  if (response.error().errcode() == 0) {
    DINGO_LOG(INFO) << "create index success, index_id==" << response.index_id().entity_id();
  }
}

void SetUpCreateIndexId(CLI::App &app) {
  auto opt = std::make_shared<CreateIndexIdOptions>();
  auto *cmd = app.add_subcommand("CreateIndexId", "Create index id ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("CreateIndexId", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateIndex(CLI::App &app) {
  auto opt = std::make_shared<UpdateIndexOptions>();
  auto *cmd = app.add_subcommand("UpdateIndex", "Update Index")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter index id")->group("Coordinator Manager Commands");
  cmd->add_option("--max_elements", opt->max_elements, "Request parameter max_elements")
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetIndex", get_request, get_response);
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

  status = coordinator_interaction_meta->SendRequest("UpdateIndex", update_request, update_response);

  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << update_response.DebugString();
}

void SetUpDropIndex(CLI::App &app) {
  auto opt = std::make_shared<DropIndexOptions>();
  auto *cmd = app.add_subcommand("DropIndex", "Drop Index")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter index id")->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("DropIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetIndex(CLI::App &app) {
  auto opt = std::make_shared<GetIndexOptions>();
  auto *cmd = app.add_subcommand("GetIndex", "Get Index")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter index id")->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetIndexByName(CLI::App &app) {
  auto opt = std::make_shared<GetIndexByNameOptions>();
  auto *cmd = app.add_subcommand("GetIndexByName", "Get index by name")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter index name")->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetIndexByName", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetIndexRange(CLI::App &app) {
  auto opt = std::make_shared<GetIndexRangeOptions>();
  auto *cmd = app.add_subcommand("GetIndexRange", "Get index range ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter index id")->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetIndexRange", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  for (const auto &it : response.index_range().range_distribution()) {
    DINGO_LOG(INFO) << "region_id=[" << it.id().entity_id() << "]"
                    << "range=[" << dingodb::Helper::StringToHex(it.range().start_key()) << ","
                    << dingodb::Helper::StringToHex(it.range().end_key()) << "]"
                    << " leader=[" << it.leader().host() << ":" << it.leader().port() << "]";
  }
}

void SetUpGetIndexMetrics(CLI::App &app) {
  auto opt = std::make_shared<GetIndexMetricsOptions>();
  auto *cmd = app.add_subcommand("GetIndexMetrics", "Get index metrics")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter index id")->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetIndexMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGenerateTableIds(CLI::App &app) {
  auto opt = std::make_shared<GenerateTableIdsOptions>();
  auto *cmd = app.add_subcommand("GenerateTableIds", "Generate table ids ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GenerateTableIds", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpCreateTables(CLI::App &app) {
  auto opt = std::make_shared<CreateTablesOptions>();
  auto *cmd = app.add_subcommand("GenerateTableIds", "Generate table ids ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter name")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--engine", opt->engine, "Request parameter engine, Must be rocksdb|bdb")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--replica", opt->replica, "Request parameter replica")
      ->default_val(3)
      ->required()
      ->group("Coordinator Manager Commands");
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
  int ret = client_v2::Helper::GetCreateTableIds(coordinator_interaction_meta, 1 + opt.part_count, new_ids);
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

  auto status = coordinator_interaction_meta->SendRequest("CreateTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateTables(CLI::App &app) {
  auto opt = std::make_shared<UpdateTablesOptions>();
  auto *cmd = app.add_subcommand("UpdateTables", "Update tables ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--table_id", opt->table_id, "Request parameter table id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter table name")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--def_version", opt->def_version, "Request parameter version")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--replica", opt->replica, "Request parameter replica num, must greater than 0")
      ->default_val(3)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--is_updating_index", opt->is_updating_index, "Request parameter replica num, must greater than 0")
      ->default_val(false)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--engine", opt->engine, "Request parameter engine, Must be rocksdb|bdb")
      ->required()
      ->group("Coordinator Manager Commands");

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
    int ret = client_v2::Helper::GetCreateTableId(coordinator_interaction_meta, new_part_id);
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

  auto status = coordinator_interaction_meta->SendRequest("UpdateTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpAddIndexOnTable(CLI::App &app) {
  auto opt = std::make_shared<AddIndexOnTableOptions>();
  auto *cmd = app.add_subcommand("AddIndexOnTable", "Add index on table")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--table_id", opt->table_id, "Request parameter table id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--index_id", opt->index_id, "Request parameter index id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter table name")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--def_version", opt->def_version, "Request parameter version")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--part_count", opt->part_count, "Request parameter part count")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--replica", opt->replica, "Request parameter replica num, must greater than 0")
      ->default_val(3)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--is_updating_index", opt->is_updating_index, "Request parameter replica num, must greater than 0")
      ->default_val(false)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--engine", opt->engine, "Request parameter engine, Must be rocksdb|bdb")
      ->required()
      ->group("Coordinator Manager Commands");

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
    int ret = client_v2::Helper::GetCreateTableId(coordinator_interaction_meta, new_part_id);
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

  auto status = coordinator_interaction_meta->SendRequest("AddIndexOnTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDropIndexOnTable(CLI::App &app) {
  auto opt = std::make_shared<DropIndexOnTableOptions>();
  auto *cmd = app.add_subcommand("DropIndexOnTable", "Drop index on table")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--table_id", opt->table_id, "Request parameter table id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--index_id", opt->index_id, "Request parameter index id")
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("DropIndexOnTable", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetTables(CLI::App &app) {
  auto opt = std::make_shared<GetTablesOptions>();
  auto *cmd = app.add_subcommand("GetTables", "Get tables")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpDropTables(CLI::App &app) {
  auto opt = std::make_shared<DropTablesOptions>();
  auto *cmd = app.add_subcommand("DropTables", "drop tables")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--schema_id", opt->schema_id, "Request parameter schema id")
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("DropTables", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpGetAutoIncrements(CLI::App &app) {
  auto opt = std::make_shared<GetAutoIncrementsOptions>();
  auto *cmd = app.add_subcommand("GetAutoIncrements", "Get auto increments ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetAutoIncrements(*opt); });
}

void RunGetAutoIncrements(GetAutoIncrementsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::meta::GetAutoIncrementsRequest request;
  dingodb::pb::meta::GetAutoIncrementsResponse response;

  auto status = coordinator_interaction_meta->SendRequest("GetAutoIncrements", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<GetAutoIncrementOptions>();
  auto *cmd = app.add_subcommand("GetAutoIncrement", "Get auto increment ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GetAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpCreateAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<CreateAutoIncrementOptions>();
  auto *cmd =
      app.add_subcommand("CreateAutoIncrement", "Create auto increment ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--incr_start_id", opt->incr_start_id, "Request parameter incr start id")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("CreateAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<UpdateAutoIncrementOptions>();
  auto *cmd =
      app.add_subcommand("UpdateAutoIncrement", "Update auto increment ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--incr_start_id", opt->incr_start_id, "Request parameter incr start id")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--force", opt->force, "Request parameter force")
      ->default_val(true)
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("UpdateAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGenerateAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<GenerateAutoIncrementOptions>();
  auto *cmd = app.add_subcommand("GenerateAutoIncrement", "Generate create auto increment ")
                  ->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--generate_count", opt->generate_count, "Generate auto increment id count")
      ->default_val(10000)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--auto_increment_offset", opt->auto_increment_offset, "Request parameter auto increment offset")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--auto_increment_increment", opt->auto_increment_increment,
                  "Request parameter auto increment increment")
      ->default_val(1)
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("GenerateAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDeleteAutoIncrement(CLI::App &app) {
  auto opt = std::make_shared<DeleteAutoIncrementOptions>();
  auto *cmd = app.add_subcommand("GenerateAutoIncrement", "Generate create auto increment ")
                  ->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter table id")->required()->group("Coordinator Manager Commands");

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

  auto status = coordinator_interaction_meta->SendRequest("DeleteAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpListWatch(CLI::App &app) {
  auto opt = std::make_shared<ListWatchOptions>();
  auto *cmd = app.add_subcommand("ListWatch", "List watch")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--watch_id", opt->watch_id, "Request parameter watch id")
      ->required()
      ->group("Coordinator Manager Commands");

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

  auto status = coordinator_interaction_meta->SendRequest("ListWatch", request, response, 600000);
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
  auto *cmd = app.add_subcommand("CreateWatch", "Create watch")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--watch_id", opt->watch_id, "Request parameter watch id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--start_revision", opt->start_revision, "Request parameter start revision")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--watch_type", opt->watch_type,
                  "Request parameter watch type must be all|region|table|index|schema|table_index")
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("Watch", request, response, 600000);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpCancelWatch(CLI::App &app) {
  auto opt = std::make_shared<CancelWatchOptions>();
  auto *cmd = app.add_subcommand("CancelWatch", "Cancel watch")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--watch_id", opt->watch_id, "Request parameter watch id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--start_revision", opt->start_revision, "Request parameter start revision")
      ->required()
      ->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("Watch", request, response, 600000);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpProgressWatch(CLI::App &app) {
  auto opt = std::make_shared<ProgressWatchOptions>();
  auto *cmd = app.add_subcommand("ProgressWatch", "Progress watch")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--watch_id", opt->watch_id, "Request parameter watch id")
      ->required()
      ->group("Coordinator Manager Commands");
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
    auto status = coordinator_interaction_meta->SendRequest("Watch", request, response, 600000);
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
  auto *cmd = app.add_subcommand("GenTso", "Generate tso ")->group("Coordinator Manager Commands");
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

  auto status = coordinator_interaction_meta->SendRequest("TsoService", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "gen tso failed, error: "
                     << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name()
                     << " " << response.error().errmsg();
    std::cout << "gen tso failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  auto lambda_tso_2_timestamp_function = [](const ::dingodb::pb::meta::TsoTimestamp &tso) {
    return (tso.physical() << ::dingodb::kLogicalBits) + tso.logical();
  };

  for (int i = 0; i < 10; i++) {
    dingodb::pb::meta::TsoTimestamp tso;
    tso.set_physical(response.start_timestamp().physical());
    tso.set_logical(response.start_timestamp().logical() + i);
    int64_t time_safe_ts = lambda_tso_2_timestamp_function(tso);
    std::cout << "time_safe_ts: " << time_safe_ts << std::endl;
  }
}

void SetUpResetTso(CLI::App &app) {
  auto opt = std::make_shared<ResetTsoOptions>();
  auto *cmd = app.add_subcommand("ResetTso", "Reset tso ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--watch_id", opt->tso_new_physical, "Request parameter tso_new_physical")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--tso_save_physical", opt->tso_save_physical, "Request parameter watch id")
      ->default_val(0)
      ->group("Coordinator Manager Commands");
  cmd->add_option("--watch_id", opt->tso_new_logical, "Request parameter watch id")
      ->default_val(0)
      ->group("Coordinator Manager Commands");

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

  auto status = coordinator_interaction_meta->SendRequest("TsoService", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

void SetUpUpdateTso(CLI::App &app) {
  auto opt = std::make_shared<UpdateTsoOptions>();
  auto *cmd = app.add_subcommand("ResetTso", "Reset tso ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--watch_id", opt->tso_new_physical, "Request parameter tso_new_physical")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--tso_save_physical", opt->tso_save_physical, "Request parameter watch id")
      ->default_val(0)
      ->group("Coordinator Manager Commands");
  cmd->add_option("--watch_id", opt->tso_new_logical, "Request parameter watch id")
      ->default_val(0)
      ->group("Coordinator Manager Commands");

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

  auto status = coordinator_interaction_meta->SendRequest("TsoService", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "RESPONSE =" << response.DebugString();
}

}  // namespace client_v2
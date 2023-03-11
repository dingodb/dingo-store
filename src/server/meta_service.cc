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

#include "server/meta_service.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "brpc/controller.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

void MetaServiceImpl::GetSchemas(google::protobuf::RpcController * /*controller*/,
                                 const pb::meta::GetSchemasRequest *request, pb::meta::GetSchemasResponse *response,
                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control->IsLeader()) {
    return RedirectResponse(response);
  }

  LOG(INFO) << "GetSchemas request:  schema_id = [" << request->schema_id().entity_id() << "]";

  std::vector<pb::meta::Schema> schemas;
  this->coordinator_control->GetSchemas(request->schema_id().entity_id(), schemas);

  for (auto &schema : schemas) {
    auto *new_schema = response->add_schemas();
    new_schema->CopyFrom(schema);
  }
}

void MetaServiceImpl::GetTables(google::protobuf::RpcController * /*controller*/,
                                const pb::meta::GetTablesRequest *request, pb::meta::GetTablesResponse *response,
                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control->IsLeader()) {
    return RedirectResponse(response);
  }

  LOG(INFO) << "GetTables request:  schema_id = [" << request->schema_id().entity_id() << "]";

  std::vector<pb::meta::TableDefinitionWithId> table_definition_with_ids;
  this->coordinator_control->GetTables(request->schema_id().entity_id(), table_definition_with_ids);

  if (table_definition_with_ids.empty()) {
    LOG(INFO) << "meta_service GetTables no tables, schema_id=" << request->schema_id().entity_id();
    return;
  }

  // add table_definition_with_id
  for (auto &table_definition_with_id : table_definition_with_ids) {
    auto *table_def_with_id = response->add_table_definition_with_ids();
    table_def_with_id->CopyFrom(table_definition_with_id);
  }
}

void MetaServiceImpl::GetTable(google::protobuf::RpcController * /*controller*/,
                               const pb::meta::GetTableRequest *request, pb::meta::GetTableResponse *response,
                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control->IsLeader()) {
    return RedirectResponse(response);
  }

  LOG(INFO) << "GetTable request:  table_id = [" << request->table_id().entity_id() << "]";

  auto *table = response->mutable_table();
  this->coordinator_control->GetTable(request->table_id().parent_entity_id(), request->table_id().entity_id(), *table);
}

void MetaServiceImpl::CreateTable(google::protobuf::RpcController * /*controller*/,
                                  const pb::meta::CreateTableRequest *request, pb::meta::CreateTableResponse *response,
                                  google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control->IsLeader()) {
    return RedirectResponse(response);
  }

  LOG(INFO) << "CreatTable request:  schema_id = [" << request->schema_id().entity_id() << "]";
  LOG(INFO) << request->DebugString();

  pb::coordinator_internal::MetaIncrement meta_increment;

  uint64_t new_table_id;
  int ret = this->coordinator_control->CreateTable(request->schema_id().entity_id(), request->table_definition(),
                                                   new_table_id, meta_increment);
  if (ret < 0) {
    LOG(ERROR) << "CreateTable failed in meta_service";
    return;
  }
  LOG(INFO) << "CreateTable new_table_id=" << new_table_id;

  auto *table_id = response->mutable_table_id();
  table_id->set_entity_id(new_table_id);
  table_id->set_parent_entity_id(request->schema_id().entity_id());
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  LOG(INFO) << "CreateTable Success in meta_service table_name =" << request->table_definition().name();
}

void MetaServiceImpl::CreateSchema(google::protobuf::RpcController *controller,
                                   const pb::meta::CreateSchemaRequest *request,
                                   pb::meta::CreateSchemaResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control->IsLeader()) {
    return RedirectResponse(response);
  }

  LOG(INFO) << "CreatSchema request:  parent_schema_id = [" << request->parent_schema_id().entity_id() << "]";
  LOG(INFO) << request->DebugString();

  pb::coordinator_internal::MetaIncrement meta_increment;

  uint64_t new_schema_id;
  int ret = this->coordinator_control->CreateSchema(request->parent_schema_id().entity_id(), request->schema_name(),
                                                    new_schema_id, meta_increment);
  if (ret) {
    LOG(INFO) << "CreateSchema failed ret = " << ret;
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(ret, "create schema failed");
    return;
  }

  auto *schema = response->mutable_schema();
  schema->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema->mutable_id()->set_entity_id(new_schema_id);
  schema->mutable_id()->set_parent_entity_id(request->parent_schema_id().entity_id());
  schema->set_name(request->schema_name());
}

void MetaServiceImpl::DropTable(google::protobuf::RpcController * /*controller*/,
                                const pb::meta::DropTableRequest *request, pb::meta::DropTableResponse *response,
                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control->IsLeader()) {
    return RedirectResponse(response);
  }

  LOG(INFO) << "DropTable request:  schema_id = [" << request->table_id().parent_entity_id() << "]"
            << " table_id = [" << request->table_id().entity_id() << "]";
  LOG(INFO) << request->DebugString();
}

}  // namespace dingodb
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
#include "proto/meta.pb.h"

namespace dingodb {

void MetaServiceImpl::GetSchemas(
    google::protobuf::RpcController * /*controller*/,
    const pb::meta::GetSchemasRequest *request,
    pb::meta::GetSchemasResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetSchemas request:  schema_id = [" << request->schema_id()
            << "]";

  std::vector<pb::common::Schema> schemas;
  this->coordinator_control->GetSchemas(request->schema_id(), schemas);

  for (auto &schema : schemas) {
    auto *new_schema = response->add_schemas();
    new_schema->CopyFrom(schema);
  }
}

void MetaServiceImpl::GetTables(
    google::protobuf::RpcController * /*controller*/,
    const pb::meta::GetTablesRequest *request,
    pb::meta::GetTablesResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetTables request:  schema_id = [" << request->schema_id()
            << "]";

  // add table_definition_with_id
  for (int i = 10; i < 15; i++) {
    auto *table_def_with_id = response->add_table_definition_with_ids();
    table_def_with_id->set_table_id(i);
  }
}

void MetaServiceImpl::CreateTable(
    google::protobuf::RpcController * /*controller*/,
    const pb::meta::CreateTableRequest *request,
    pb::meta::CreateTableResponse * /*response*/,
    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "CreatTable request:  schema_id = [" << request->schema_id()
            << "]";
  LOG(INFO) << request->DebugString();
}

void MetaServiceImpl::CreateSchema(google::protobuf::RpcController *controller,
                                   const pb::meta::CreateSchemaRequest *request,
                                   pb::meta::CreateSchemaResponse *response,
                                   google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "CreatSchema request:  parent_schema_id = ["
            << request->parent_schema_id() << "]";
  LOG(INFO) << request->DebugString();

  uint64_t new_schema_id;
  int ret = this->coordinator_control->CreateSchema(
      request->parent_schema_id(), request->schema_name(), new_schema_id);
  if (ret) {
    LOG(INFO) << "CreateSchema failed ret = " << ret;
    brpc::Controller *brpc_controller =
        static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(ret, "create schema failed");
    return;
  }

  auto *schema = response->mutable_schema();
  schema->set_id(new_schema_id);
  schema->set_name(request->schema_name());
}

void MetaServiceImpl::DropTable(
    google::protobuf::RpcController * /*controller*/,
    const pb::meta::DropTableRequest *request,
    pb::meta::DropTableResponse * /*response*/,
    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "DropTable request:  schema_id = [" << request->schema_id()
            << "]"
            << " table_id = [" << request->table_id() << "]";
  LOG(INFO) << request->DebugString();
}

}  // namespace dingodb
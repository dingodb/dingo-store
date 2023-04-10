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
#include "common/constant.h"
#include "common/logging.h"
#include "coordinator/coordinator_closure.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"

using dingodb::pb::error::Errno;

namespace dingodb {

void MetaServiceImpl::GetSchemas(google::protobuf::RpcController * /*controller*/,
                                 const pb::meta::GetSchemasRequest *request, pb::meta::GetSchemasResponse *response,
                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetSchemas request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  std::vector<pb::meta::Schema> schemas;
  this->coordinator_control_->GetSchemas(request->schema_id().entity_id(), schemas);

  for (auto &schema : schemas) {
    auto *new_schema = response->add_schemas();
    new_schema->CopyFrom(schema);
  }
}

void MetaServiceImpl::GetSchema(google::protobuf::RpcController * /*controller*/,
                                const pb::meta::GetSchemaRequest *request, pb::meta::GetSchemaResponse *response,
                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetSchema request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *schema = response->mutable_schema();
  this->coordinator_control_->GetSchema(request->schema_id().entity_id(), *schema);
}

void MetaServiceImpl::GetSchemaByName(google::protobuf::RpcController * /*controller*/,
                                      const pb::meta::GetSchemaByNameRequest *request,
                                      pb::meta::GetSchemaByNameResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetSchemaByName request:  schema_name = [" << request->schema_name() << "]";

  if (request->schema_name().empty()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *schema = response->mutable_schema();
  this->coordinator_control_->GetSchemaByName(request->schema_name(), *schema);
}

void MetaServiceImpl::GetTables(google::protobuf::RpcController * /*controller*/,
                                const pb::meta::GetTablesRequest *request, pb::meta::GetTablesResponse *response,
                                google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTables request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  std::vector<pb::meta::TableDefinitionWithId> table_definition_with_ids;
  this->coordinator_control_->GetTables(request->schema_id().entity_id(), table_definition_with_ids);

  if (table_definition_with_ids.empty()) {
    DINGO_LOG(INFO) << "meta_service GetTables no tables, schema_id=" << request->schema_id().entity_id();
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

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  DINGO_LOG(DEBUG) << "GetTable request:  table_id = [" << request->table_id().entity_id() << "]";

  auto *table = response->mutable_table_definition_with_id();
  this->coordinator_control_->GetTable(request->table_id().parent_entity_id(), request->table_id().entity_id(), *table);
}

void MetaServiceImpl::GetTableByName(google::protobuf::RpcController * /*controller*/,
                                     const pb::meta::GetTableByNameRequest *request,
                                     pb::meta::GetTableByNameResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTableByName request:  schema_id = [" << request->schema_id().entity_id() << "]"
                   << " table_name = [" << request->table_name() << "]";

  if (request->table_name().empty()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *table = response->mutable_table_definition_with_id();
  this->coordinator_control_->GetTableByName(request->schema_id().entity_id(), request->table_name(), *table);
}

void MetaServiceImpl::GetTableRange(google::protobuf::RpcController * /*controller*/,
                                    const pb::meta::GetTableRangeRequest *request,
                                    pb::meta::GetTableRangeResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTable request:  table_id = [" << request->table_id().entity_id() << "]";

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *table_range = response->mutable_table_range();
  this->coordinator_control_->GetTableRange(request->table_id().parent_entity_id(), request->table_id().entity_id(),
                                            *table_range);
}

void MetaServiceImpl::GetTableMetrics(google::protobuf::RpcController * /*controller*/,
                                      const pb::meta::GetTableMetricsRequest *request,
                                      pb::meta::GetTableMetricsResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTableMetrics request:  table_id = [" << request->table_id().entity_id() << "]";

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *table_metrics = response->mutable_table_metrics();
  this->coordinator_control_->GetTableMetrics(request->table_id().parent_entity_id(), request->table_id().entity_id(),
                                              *table_metrics);
}

void MetaServiceImpl::CreateTableId(google::protobuf::RpcController *controller,
                                    const pb::meta::CreateTableIdRequest *request,
                                    pb::meta::CreateTableIdResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateTableId request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->DebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  uint64_t new_table_id;
  int ret = this->coordinator_control_->CreateTableId(request->schema_id().entity_id(), new_table_id, meta_increment);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "CreateTableId failed in meta_service";
    return;
  }
  DINGO_LOG(INFO) << "CreateTableId new_table_id=" << new_table_id;

  auto *table_id = response->mutable_table_id();
  table_id->set_entity_id(new_table_id);
  table_id->set_parent_entity_id(request->schema_id().entity_id());
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  // prepare for raft process
  CoordinatorClosure<pb::meta::CreateTableIdRequest, pb::meta::CreateTableIdResponse> *meta_put_closure =
      new CoordinatorClosure<pb::meta::CreateTableIdRequest, pb::meta::CreateTableIdResponse>(request, response,
                                                                                              done_guard.release());

  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);

  DINGO_LOG(INFO) << "CreateTableId Success in meta_service table_d =" << new_table_id;
}

void MetaServiceImpl::CreateTable(google::protobuf::RpcController *controller,
                                  const pb::meta::CreateTableRequest *request, pb::meta::CreateTableResponse *response,
                                  google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateTable request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->DebugString();

  if (!request->has_schema_id() || !request->has_table_definition()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  uint64_t new_table_id = 0;
  if (request->has_table_id()) {
    if (request->table_id().entity_id() > 0) {
      new_table_id = request->table_id().entity_id();
      DINGO_LOG(INFO) << "CreateTable table_id is given[" << new_table_id << "] request:  schema_id = ["
                      << request->schema_id().entity_id() << "]";
    }
  }

  int ret = this->coordinator_control_->CreateTable(request->schema_id().entity_id(), request->table_definition(),
                                                    new_table_id, meta_increment);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "CreateTable failed in meta_service";
    return;
  }
  DINGO_LOG(INFO) << "CreateTable new_table_id=" << new_table_id;

  auto *table_id = response->mutable_table_id();
  table_id->set_entity_id(new_table_id);
  table_id->set_parent_entity_id(request->schema_id().entity_id());
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);

  // prepare for raft process
  CoordinatorClosure<pb::meta::CreateTableRequest, pb::meta::CreateTableResponse> *meta_put_closure =
      new CoordinatorClosure<pb::meta::CreateTableRequest, pb::meta::CreateTableResponse>(request, response,
                                                                                          done_guard.release());

  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);

  DINGO_LOG(INFO) << "CreateTable Success in meta_service table_name =" << request->table_definition().name();
}

void MetaServiceImpl::DropSchema(google::protobuf::RpcController *controller,
                                 const pb::meta::DropSchemaRequest *request, pb::meta::DropSchemaResponse *response,
                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(WARNING) << "DropSchema request:  parent_schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(INFO) << request->DebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  uint64_t schema_id = request->schema_id().entity_id();
  uint64_t parent_schema_id = request->schema_id().parent_entity_id();
  int ret = this->coordinator_control_->DropSchema(parent_schema_id, schema_id, meta_increment);
  if (ret) {
    DINGO_LOG(ERROR) << "DropSchema failed, schema_id=" << schema_id << " ret = " << ret;
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(ret, "drop schema failed");
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::DropSchemaRequest, pb::meta::DropSchemaResponse> *meta_put_closure =
      new CoordinatorClosure<pb::meta::DropSchemaRequest, pb::meta::DropSchemaResponse>(request, response,
                                                                                        done_guard.release());

  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void MetaServiceImpl::CreateSchema(google::protobuf::RpcController *controller,
                                   const pb::meta::CreateSchemaRequest *request,
                                   pb::meta::CreateSchemaResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreatSchema request:  parent_schema_id = [" << request->parent_schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->DebugString();

  if (!request->has_parent_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  uint64_t new_schema_id;
  int ret = this->coordinator_control_->CreateSchema(request->parent_schema_id().entity_id(), request->schema_name(),
                                                     new_schema_id, meta_increment);
  if (ret) {
    DINGO_LOG(ERROR) << "CreateSchema schema_id = " << new_schema_id
                     << " parent_schema_id=" << request->parent_schema_id().entity_id() << " failed ret = " << ret;
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(ret, "create schema failed");
    return;
  }

  auto *schema = response->mutable_schema();
  schema->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema->mutable_id()->set_entity_id(new_schema_id);
  schema->mutable_id()->set_parent_entity_id(request->parent_schema_id().entity_id());
  schema->set_name(request->schema_name());

  // prepare for raft process
  CoordinatorClosure<pb::meta::CreateSchemaRequest, pb::meta::CreateSchemaResponse> *meta_put_closure =
      new CoordinatorClosure<pb::meta::CreateSchemaRequest, pb::meta::CreateSchemaResponse>(request, response,
                                                                                            done_guard.release());

  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void MetaServiceImpl::DropTable(google::protobuf::RpcController *controller, const pb::meta::DropTableRequest *request,
                                pb::meta::DropTableResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(WARNING) << "DropTable request:  schema_id = [" << request->table_id().parent_entity_id() << "]"
                     << " table_id = [" << request->table_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->DebugString();

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int ret = this->coordinator_control_->DropTable(request->table_id().parent_entity_id(),
                                                  request->table_id().entity_id(), meta_increment);
  if (ret < 0) {
    response->mutable_error()->set_errcode(Errno::EINTERNAL);
    DINGO_LOG(ERROR) << "DropTable failed in meta_service, table_id=" << request->table_id().entity_id();
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::DropTableRequest, pb::meta::DropTableResponse> *meta_put_closure =
      new CoordinatorClosure<pb::meta::DropTableRequest, pb::meta::DropTableResponse>(request, response,
                                                                                      done_guard.release());

  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

}  // namespace dingodb

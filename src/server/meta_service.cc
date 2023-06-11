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
#include "butil/containers/flat_map.h"
#include "common/constant.h"
#include "common/logging.h"
#include "coordinator/coordinator_closure.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"

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
  auto ret = this->coordinator_control_->GetSchemas(request->schema_id().entity_id(), schemas);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

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
  auto ret = this->coordinator_control_->GetSchema(request->schema_id().entity_id(), *schema);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
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
  auto ret = this->coordinator_control_->GetSchemaByName(request->schema_name(), *schema);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void MetaServiceImpl::GetTablesCount(google::protobuf::RpcController * /*controller*/,
                                     const pb::meta::GetTablesCountRequest *request,
                                     pb::meta::GetTablesCountResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTablesCount request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  uint64_t tables_count = 0;
  auto ret = this->coordinator_control_->GetTablesCount(request->schema_id().entity_id(), tables_count);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  response->set_tables_count(tables_count);
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
  auto ret = this->coordinator_control_->GetTables(request->schema_id().entity_id(), table_definition_with_ids);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

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
  auto ret = this->coordinator_control_->GetTable(request->table_id().parent_entity_id(),
                                                  request->table_id().entity_id(), *table);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
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
  auto ret =
      this->coordinator_control_->GetTableByName(request->schema_id().entity_id(), request->table_name(), *table);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
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
  auto ret = this->coordinator_control_->GetTableRange(request->table_id().parent_entity_id(),
                                                       request->table_id().entity_id(), *table_range);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
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
  auto ret = coordinator_control_->GetTableMetrics(request->table_id().parent_entity_id(),
                                                   request->table_id().entity_id(), *table_metrics);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }
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
  auto ret = this->coordinator_control_->CreateTableId(request->schema_id().entity_id(), new_table_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CreateTableId failed in meta_service";
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
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

  auto ret = this->coordinator_control_->CreateTable(request->schema_id().entity_id(), request->table_definition(),
                                                     new_table_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CreateTable failed in meta_service, error code=" << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
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
  auto ret = this->coordinator_control_->DropSchema(parent_schema_id, schema_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "DropSchema failed, schema_id=" << schema_id << " ret = " << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
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
  auto ret = this->coordinator_control_->CreateSchema(request->parent_schema_id().entity_id(), request->schema_name(),
                                                      new_schema_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CreateSchema schema_id = " << new_schema_id
                     << " parent_schema_id=" << request->parent_schema_id().entity_id() << " failed ret = " << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
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

  auto ret = this->coordinator_control_->DropTable(request->table_id().parent_entity_id(),
                                                   request->table_id().entity_id(), meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "DropTable failed in meta_service, table_id=" << request->table_id().entity_id();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
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

void MetaServiceImpl::GetAutoIncrements(google::protobuf::RpcController * /*controller*/,
                                        const pb::meta::GetAutoIncrementsRequest *request,
                                        pb::meta::GetAutoIncrementsResponse *response,
                                        google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control_->IsLeader()) {
    return RedirectAutoIncrementResponse(response);
  }

  DINGO_LOG(DEBUG) << request->DebugString();

  butil::FlatMap<uint64_t, uint64_t> auto_increments;
  auto_increments.init(1024);
  auto ret = auto_increment_control_->GetAutoIncrements(auto_increments);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "get auto increments failed";
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  for (auto it : auto_increments) {
    auto *auto_increment = response->add_table_increments();
    auto_increment->set_table_id(it.first);
    auto_increment->set_start_id(it.second);
  }
}

void MetaServiceImpl::GetAutoIncrement(google::protobuf::RpcController * /*controller*/,
                                       const pb::meta::GetAutoIncrementRequest *request,
                                       pb::meta::GetAutoIncrementResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control_->IsLeader()) {
    return RedirectAutoIncrementResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  uint64_t table_id = request->table_id().entity_id();
  uint64_t start_id = 0;
  auto ret = auto_increment_control_->GetAutoIncrement(table_id, start_id);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "get auto increment failed, " << table_id;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  response->set_start_id(start_id);
}

void MetaServiceImpl::CreateAutoIncrement(google::protobuf::RpcController *controller,
                                          const pb::meta::CreateAutoIncrementRequest *request,
                                          pb::meta::CreateAutoIncrementResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control_->IsLeader()) {
    return RedirectAutoIncrementResponse(response);
  }
  DINGO_LOG(INFO) << request->ShortDebugString();

  uint64_t table_id = request->table_id().entity_id();
  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret = auto_increment_control_->CreateAutoIncrement(table_id, request->start_id(), meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "failed, " << table_id << " | " << request->start_id();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *closure = new CoordinatorClosure<pb::meta::CreateAutoIncrementRequest, pb::meta::CreateAutoIncrementResponse>(
      request, response, done_guard.release());
  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), closure, response);
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);
  // this is a async operation will be block by closure
  auto ret2 = engine_->MetaPut(ctx, meta_increment);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "failed, " << table_id << " | " << request->start_id() << " | " << ret2.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret2.error_code()));
    response->mutable_error()->set_errmsg(ret2.error_str());

    if (ret2.error_code() == pb::error::Errno::ERAFT_NOTLEADER) {
      RedirectAutoIncrementResponse(response);
    }
    return;
  }
}

void MetaServiceImpl::UpdateAutoIncrement(google::protobuf::RpcController *controller,
                                          const ::dingodb::pb::meta::UpdateAutoIncrementRequest *request,
                                          pb::meta::UpdateAutoIncrementResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control_->IsLeader()) {
    return RedirectAutoIncrementResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  uint64_t table_id = request->table_id().entity_id();
  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret =
      auto_increment_control_->UpdateAutoIncrement(table_id, request->start_id(), request->force(), meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "update auto increment failed, " << table_id << " | " << request->start_id() << " | "
                     << request->force() << " | " << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *closure = new CoordinatorClosure<pb::meta::UpdateAutoIncrementRequest, pb::meta::UpdateAutoIncrementResponse>(
      request, response, done_guard.release());
  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), closure);
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);
  // this is a async operation will be block by closure
  auto ret2 = engine_->MetaPut(ctx, meta_increment);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "update auto increment failed, " << table_id << " | " << request->start_id() << " | "
                     << request->force();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret2.error_code()));
    response->mutable_error()->set_errmsg(ret2.error_str());

    if (ret2.error_code() == pb::error::Errno::ERAFT_NOTLEADER) {
      RedirectAutoIncrementResponse(response);
    }
    return;
  }
}

void MetaServiceImpl::GenerateAutoIncrement(google::protobuf::RpcController *controller,
                                            const ::dingodb::pb::meta::GenerateAutoIncrementRequest *request,
                                            pb::meta::GenerateAutoIncrementResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control_->IsLeader()) {
    return RedirectAutoIncrementResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  uint64_t table_id = request->table_id().entity_id();
  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret =
      auto_increment_control_->GenerateAutoIncrement(table_id, request->count(), request->auto_increment_increment(),
                                                     request->auto_increment_offset(), meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "generate auto increment failed, " << ret << " | " << request->DebugString();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *closure =
      new CoordinatorClosure<pb::meta::GenerateAutoIncrementRequest, pb::meta::GenerateAutoIncrementResponse>(
          request, response, done_guard.release());
  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), closure, response);
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);
  // this is a async operation will be block by closure
  auto ret2 = engine_->MetaPut(ctx, meta_increment);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "generate auto increment failed, " << ret << " | " << request->DebugString() << " | "
                     << ret2.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret2.error_code()));
    response->mutable_error()->set_errmsg(ret2.error_str());

    if (ret2.error_code() == pb::error::Errno::ERAFT_NOTLEADER) {
      RedirectAutoIncrementResponse(response);
    }
    return;
  }
}

void MetaServiceImpl::DeleteAutoIncrement(google::protobuf::RpcController *controller,
                                          const ::dingodb::pb::meta::DeleteAutoIncrementRequest *request,
                                          pb::meta::DeleteAutoIncrementResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control_->IsLeader()) {
    return RedirectAutoIncrementResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  uint64_t table_id = request->table_id().entity_id();
  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret = auto_increment_control_->DeleteAutoIncrement(table_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "delete auto increment failed, " << table_id;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *closure = new CoordinatorClosure<pb::meta::DeleteAutoIncrementRequest, pb::meta::DeleteAutoIncrementResponse>(
      request, response, done_guard.release());
  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), closure);
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);
  // this is a async operation will be block by closure
  auto ret2 = engine_->MetaPut(ctx, meta_increment);
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "delete auto increment failed, " << table_id << " | " << ret2.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret2.error_code()));
    response->mutable_error()->set_errmsg(ret2.error_str());

    if (ret2.error_code() == pb::error::Errno::ERAFT_NOTLEADER) {
      RedirectAutoIncrementResponse(response);
    }
    return;
  }
}

void MetaServiceImpl::GetIndexesCount(google::protobuf::RpcController * /*controller*/,
                                      const pb::meta::GetIndexesCountRequest *request,
                                      pb::meta::GetIndexesCountResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndexsCount request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  uint64_t indexes_count = 0;
  auto ret = this->coordinator_control_->GetIndexsCount(request->schema_id().entity_id(), indexes_count);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  response->set_indexes_count(indexes_count);
}

void MetaServiceImpl::GetIndexes(google::protobuf::RpcController * /*controller*/,
                                 const pb::meta::GetIndexesRequest *request, pb::meta::GetIndexesResponse *response,
                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndexs request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  std::vector<pb::meta::IndexDefinitionWithId> index_definition_with_ids;
  auto ret = this->coordinator_control_->GetIndexs(request->schema_id().entity_id(), index_definition_with_ids);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (index_definition_with_ids.empty()) {
    DINGO_LOG(INFO) << "meta_service GetIndexs no indexs, schema_id=" << request->schema_id().entity_id();
    return;
  }

  // add index_definition_with_id
  for (auto &index_definition_with_id : index_definition_with_ids) {
    auto *index_def_with_id = response->add_index_definition_with_ids();
    index_def_with_id->CopyFrom(index_definition_with_id);
  }
}

void MetaServiceImpl::GetIndex(google::protobuf::RpcController * /*controller*/,
                               const pb::meta::GetIndexRequest *request, pb::meta::GetIndexResponse *response,
                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  DINGO_LOG(DEBUG) << "GetIndex request:  index_id = [" << request->index_id().entity_id() << "]";

  auto *index = response->mutable_index_definition_with_id();
  auto ret = this->coordinator_control_->GetIndex(request->index_id().parent_entity_id(),
                                                  request->index_id().entity_id(), *index);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void MetaServiceImpl::GetIndexByName(google::protobuf::RpcController * /*controller*/,
                                     const pb::meta::GetIndexByNameRequest *request,
                                     pb::meta::GetIndexByNameResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndexByName request:  schema_id = [" << request->schema_id().entity_id() << "]"
                   << " index_name = [" << request->index_name() << "]";

  if (request->index_name().empty()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *index = response->mutable_index_definition_with_id();
  auto ret =
      this->coordinator_control_->GetIndexByName(request->schema_id().entity_id(), request->index_name(), *index);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void MetaServiceImpl::GetIndexRange(google::protobuf::RpcController * /*controller*/,
                                    const pb::meta::GetIndexRangeRequest *request,
                                    pb::meta::GetIndexRangeResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndex request:  index_id = [" << request->index_id().entity_id() << "]";

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *index_range = response->mutable_index_range();
  auto ret = this->coordinator_control_->GetIndexRange(request->index_id().parent_entity_id(),
                                                       request->index_id().entity_id(), *index_range);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void MetaServiceImpl::GetIndexMetrics(google::protobuf::RpcController * /*controller*/,
                                      const pb::meta::GetIndexMetricsRequest *request,
                                      pb::meta::GetIndexMetricsResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndexMetrics request:  index_id = [" << request->index_id().entity_id() << "]";

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *index_metrics = response->mutable_index_metrics();
  auto ret = coordinator_control_->GetIndexMetrics(request->index_id().parent_entity_id(),
                                                   request->index_id().entity_id(), *index_metrics);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }
}

void MetaServiceImpl::CreateIndexId(google::protobuf::RpcController *controller,
                                    const pb::meta::CreateIndexIdRequest *request,
                                    pb::meta::CreateIndexIdResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateIndexId request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->DebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  uint64_t new_index_id;
  auto ret = this->coordinator_control_->CreateIndexId(request->schema_id().entity_id(), new_index_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CreateIndexId failed in meta_service";
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
  DINGO_LOG(INFO) << "CreateIndexId new_index_id=" << new_index_id;

  auto *index_id = response->mutable_index_id();
  index_id->set_entity_id(new_index_id);
  index_id->set_parent_entity_id(request->schema_id().entity_id());
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);

  // prepare for raft process
  CoordinatorClosure<pb::meta::CreateIndexIdRequest, pb::meta::CreateIndexIdResponse> *meta_put_closure =
      new CoordinatorClosure<pb::meta::CreateIndexIdRequest, pb::meta::CreateIndexIdResponse>(request, response,
                                                                                              done_guard.release());

  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);

  DINGO_LOG(INFO) << "CreateIndexId Success in meta_service index_d =" << new_index_id;
}

void MetaServiceImpl::CreateIndex(google::protobuf::RpcController *controller,
                                  const pb::meta::CreateIndexRequest *request, pb::meta::CreateIndexResponse *response,
                                  google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateIndex request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->DebugString();

  if (!request->has_schema_id() || !request->has_index_definition()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  uint64_t new_index_id = 0;
  if (request->has_index_id()) {
    if (request->index_id().entity_id() > 0) {
      new_index_id = request->index_id().entity_id();
      DINGO_LOG(INFO) << "CreateIndex index_id is given[" << new_index_id << "] request:  schema_id = ["
                      << request->schema_id().entity_id() << "]";
    }
  }

  auto ret = this->coordinator_control_->CreateIndex(request->schema_id().entity_id(), request->index_definition(),
                                                     new_index_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CreateIndex failed in meta_service, error code=" << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
  DINGO_LOG(INFO) << "CreateIndex new_index_id=" << new_index_id;

  auto *index_id = response->mutable_index_id();
  index_id->set_entity_id(new_index_id);
  index_id->set_parent_entity_id(request->schema_id().entity_id());
  index_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);

  // prepare for raft process
  CoordinatorClosure<pb::meta::CreateIndexRequest, pb::meta::CreateIndexResponse> *meta_put_closure =
      new CoordinatorClosure<pb::meta::CreateIndexRequest, pb::meta::CreateIndexResponse>(request, response,
                                                                                          done_guard.release());

  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);

  DINGO_LOG(INFO) << "CreateIndex Success in meta_service index_name =" << request->index_definition().name();
}

void MetaServiceImpl::DropIndex(google::protobuf::RpcController *controller, const pb::meta::DropIndexRequest *request,
                                pb::meta::DropIndexResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(WARNING) << "DropIndex request:  schema_id = [" << request->index_id().parent_entity_id() << "]"
                     << " index_id = [" << request->index_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->DebugString();

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = this->coordinator_control_->DropIndex(request->index_id().parent_entity_id(),
                                                   request->index_id().entity_id(), meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "DropIndex failed in meta_service, index_id=" << request->index_id().entity_id();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::DropIndexRequest, pb::meta::DropIndexResponse> *meta_put_closure =
      new CoordinatorClosure<pb::meta::DropIndexRequest, pb::meta::DropIndexResponse>(request, response,
                                                                                      done_guard.release());

  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_put_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

}  // namespace dingodb

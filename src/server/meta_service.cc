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
#include <map>
#include <memory>
#include <vector>

#include "brpc/controller.h"
#include "butil/containers/flat_map.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "coordinator/auto_increment_control.h"
#include "coordinator/coordinator_closure.h"
#include "coordinator/tso_control.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "server/service_helper.h"

namespace dingodb {

DECLARE_uint64(max_hnsw_memory_size_of_region);
DECLARE_uint32(max_hnsw_nlinks_of_region);
DECLARE_uint64(max_partition_num_of_table);

DEFINE_uint32(max_check_region_state_count, 600, "max check region state count");
DEFINE_uint32(max_table_definition_count_in_create_tables, 100, "max table definition count in create tables");
DEFINE_bool(async_create_table, false, "async create table");

static void MetaServiceDone(std::atomic<bool> *done) { done->store(true, std::memory_order_release); }

void MetaServiceImpl::TableDefinitionToIndexDefinition(const pb::meta::TableDefinition &table_definition,
                                                       pb::meta::IndexDefinition &index_definition) {
  index_definition.set_name(table_definition.name());
  index_definition.set_version(table_definition.version());
  *(index_definition.mutable_index_partition()) = table_definition.table_partition();
  index_definition.set_replica(table_definition.replica());
  *(index_definition.mutable_index_parameter()) = table_definition.index_parameter();
  index_definition.set_with_auto_incrment(table_definition.auto_increment() > 0);
  index_definition.set_auto_increment(table_definition.auto_increment());
}

void MetaServiceImpl::IndexDefinitionToTableDefinition(const pb::meta::IndexDefinition &index_definition,
                                                       pb::meta::TableDefinition &table_definition) {
  table_definition.set_name(index_definition.name());
  table_definition.set_version(index_definition.version());
  table_definition.set_auto_increment(index_definition.auto_increment());
  *(table_definition.mutable_table_partition()) = index_definition.index_partition();
  table_definition.set_replica(index_definition.replica());
  if ((index_definition.index_parameter().index_type() == pb::common::IndexType::INDEX_TYPE_SCALAR) &&
      (index_definition.index_parameter().scalar_index_parameter().scalar_index_type() ==
       pb::common::ScalarIndexType::SCALAR_INDEX_TYPE_BTREE)) {
    table_definition.set_engine(pb::common::Engine::ENG_BTREE);
  } else {
    table_definition.set_engine(pb::common::Engine::ENG_ROCKSDB);
  }
  *(table_definition.mutable_index_parameter()) = index_definition.index_parameter();
}

void DoGetSchemas(google::protobuf::RpcController * /*controller*/, const pb::meta::GetSchemasRequest *request,
                  pb::meta::GetSchemasResponse *response, google::protobuf::Closure *done,
                  std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetSchemas request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  std::vector<pb::meta::Schema> schemas;
  auto ret = coordinator_control->GetSchemas(request->schema_id().entity_id(), schemas);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  for (auto &schema : schemas) {
    auto *new_schema = response->add_schemas();
    *new_schema = schema;
  }
}

void DoGetSchema(google::protobuf::RpcController * /*controller*/, const pb::meta::GetSchemaRequest *request,
                 pb::meta::GetSchemaResponse *response, google::protobuf::Closure *done,
                 std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetSchema request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *schema = response->mutable_schema();
  auto ret = coordinator_control->GetSchema(request->schema_id().entity_id(), *schema);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void DoGetSchemaByName(google::protobuf::RpcController * /*controller*/,
                       const pb::meta::GetSchemaByNameRequest *request, pb::meta::GetSchemaByNameResponse *response,
                       google::protobuf::Closure *done, std::shared_ptr<CoordinatorControl> coordinator_control,
                       std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetSchemaByName request:  schema_name = [" << request->schema_name() << "]";

  if (request->schema_name().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *schema = response->mutable_schema();
  auto ret = coordinator_control->GetSchemaByName(request->schema_name(), *schema);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void DoGetTablesCount(google::protobuf::RpcController * /*controller*/, const pb::meta::GetTablesCountRequest *request,
                      pb::meta::GetTablesCountResponse *response, google::protobuf::Closure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control,
                      std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTablesCount request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  int64_t tables_count = 0;
  auto ret = coordinator_control->GetTablesCount(request->schema_id().entity_id(), tables_count);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  response->set_tables_count(tables_count);
}

void DoGetTablesBySchema(google::protobuf::RpcController * /*controller*/,
                         const pb::meta::GetTablesBySchemaRequest *request,
                         pb::meta::GetTablesBySchemaResponse *response, google::protobuf::Closure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control,
                         std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTablesBySchema request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  std::vector<pb::meta::TableDefinitionWithId> table_definition_with_ids;
  auto ret = coordinator_control->GetTables(request->schema_id().entity_id(), table_definition_with_ids);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (table_definition_with_ids.empty()) {
    DINGO_LOG(INFO) << "meta_service GetTablesBySchema no tables, schema_id=" << request->schema_id().entity_id();
    return;
  }

  // add table_definition_with_id
  for (auto &table_definition_with_id : table_definition_with_ids) {
    auto *table_def_with_id = response->add_table_definition_with_ids();
    *table_def_with_id = table_definition_with_id;
  }
}

void DoGetTable(google::protobuf::RpcController * /*controller*/, const pb::meta::GetTableRequest *request,
                pb::meta::GetTableResponse *response, google::protobuf::Closure *done,
                std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  DINGO_LOG(DEBUG) << "GetTable request:  table_id = [" << request->table_id().entity_id() << "]";

  auto *table = response->mutable_table_definition_with_id();
  auto ret =
      coordinator_control->GetTable(request->table_id().parent_entity_id(), request->table_id().entity_id(), *table);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void DoGetTableByName(google::protobuf::RpcController * /*controller*/, const pb::meta::GetTableByNameRequest *request,
                      pb::meta::GetTableByNameResponse *response, google::protobuf::Closure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control,
                      std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTableByName request:  schema_id = [" << request->schema_id().entity_id() << "]"
                   << " table_name = [" << request->table_name() << "]";

  if (request->table_name().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *table = response->mutable_table_definition_with_id();
  auto ret = coordinator_control->GetTableByName(request->schema_id().entity_id(), request->table_name(), *table);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void DoGetTableRange(google::protobuf::RpcController * /*controller*/, const pb::meta::GetTableRangeRequest *request,
                     pb::meta::GetTableRangeResponse *response, google::protobuf::Closure *done,
                     std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTable request:  table_id = [" << request->table_id().entity_id() << "]";

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *table_range = response->mutable_table_range();
  auto ret = coordinator_control->GetTableRange(request->table_id().parent_entity_id(), request->table_id().entity_id(),
                                                *table_range);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void DoGetTableMetrics(google::protobuf::RpcController * /*controller*/,
                       const pb::meta::GetTableMetricsRequest *request, pb::meta::GetTableMetricsResponse *response,
                       google::protobuf::Closure *done, std::shared_ptr<CoordinatorControl> coordinator_control,
                       std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTableMetrics request:  table_id = [" << request->table_id().entity_id() << "]";

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *table_metrics = response->mutable_table_metrics();
  auto ret = coordinator_control->GetTableMetrics(request->table_id().parent_entity_id(),
                                                  request->table_id().entity_id(), *table_metrics);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }
}

void DoCreateTableId(google::protobuf::RpcController *controller, const pb::meta::CreateTableIdRequest *request,
                     pb::meta::CreateTableIdResponse *response, google::protobuf::Closure *done,
                     std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateTableId request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int64_t new_table_id;
  auto ret = coordinator_control->CreateTableId(request->schema_id().entity_id(), new_table_id, meta_increment);
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
  CoordinatorClosure<pb::meta::CreateTableIdRequest, pb::meta::CreateTableIdResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::CreateTableIdRequest, pb::meta::CreateTableIdResponse>(request, response,
                                                                                              done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CreateTableId failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }

  DINGO_LOG(INFO) << "CreateTableId Success in meta_service table_d =" << new_table_id;
}

void DoCreateTableIds(google::protobuf::RpcController *controller, const pb::meta::CreateTableIdsRequest *request,
                      pb::meta::CreateTableIdsResponse *response, google::protobuf::Closure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateTableIds request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("schema_id must be set");
    return;
  }

  if (request->count() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("count must be greater than 0");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  std::vector<int64_t> new_table_ids;
  auto ret = coordinator_control->CreateTableIds(request->schema_id().entity_id(), request->count(), new_table_ids,
                                                 meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CreateTableIds failed in meta_service";
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
  DINGO_LOG(INFO) << "CreateTableIds new_table_id count=" << new_table_ids.size();

  for (const auto id : new_table_ids) {
    auto *table_id = response->add_table_ids();
    table_id->set_entity_id(id);
    table_id->set_parent_entity_id(request->schema_id().entity_id());
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::CreateTableIdsRequest, pb::meta::CreateTableIdsResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::CreateTableIdsRequest, pb::meta::CreateTableIdsResponse>(request, response,
                                                                                                done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CreateTableIds failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }

  DINGO_LOG(INFO) << "CreateTableIds Success in meta_service table_ids count =" << new_table_ids.size();
}

void DoCreateTable(google::protobuf::RpcController * /*controller*/, const pb::meta::CreateTableRequest *request,
                   pb::meta::CreateTableResponse *response, google::protobuf::Closure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateTable request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(INFO) << request->ShortDebugString();

  int64_t start_ms = butil::gettimeofday_ms();

  if (!request->has_schema_id() || !request->has_table_definition()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int64_t new_table_id = 0;
  if (request->has_table_id()) {
    if (request->table_id().entity_id() > 0) {
      new_table_id = request->table_id().entity_id();
      DINGO_LOG(INFO) << "CreateTable table_id is given[" << new_table_id << "] request:  schema_id = ["
                      << request->schema_id().entity_id() << "]";
    }
  }

  std::vector<int64_t> region_ids;
  auto ret = coordinator_control->CreateTable(request->schema_id().entity_id(), request->table_definition(),
                                              new_table_id, region_ids, meta_increment);
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

  {
    auto ret1 = coordinator_control->SubmitMetaIncrementSync(meta_increment);
    if (!ret1.ok()) {
      DINGO_LOG(ERROR) << "CreateTable failed in meta_service, error code=" << ret1.error_code()
                       << ", error str=" << ret1.error_str();
      response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret1.error_code()));
      response->mutable_error()->set_errmsg(ret1.error_str());
      return;
    }

    if (!FLAGS_async_create_table) {
      DINGO_LOG(INFO) << "CreateTable wait for raft done. table_id: " << new_table_id;
    }

    if ((!region_ids.empty()) && (!FLAGS_async_create_table)) {
      std::map<int64_t, bool> region_status;
      for (const auto &id : region_ids) {
        region_status[id] = false;
      }

      DINGO_LOG(INFO) << "start to check region_status, table_id: " << new_table_id
                      << ", region_id_count: " << region_ids.size();

      uint32_t max_check_region_state_count = FLAGS_max_check_region_state_count;

      while (!region_status.empty()) {
        std::vector<int64_t> id_to_erase;
        for (const auto &it : region_status) {
          DINGO_LOG(INFO) << "CreateTable region_id=" << it.first << " status=" << it.second;
          pb::common::Region region;
          auto ret2 = coordinator_control->QueryRegion(it.first, region);
          if (ret2.ok()) {
            if (region.state() == pb::common::RegionState::REGION_NORMAL) {
              DINGO_LOG(INFO) << "region is NORMAL, region_id=" << it.first;
              id_to_erase.push_back(it.first);
            } else {
              DINGO_LOG(INFO) << "CreateTable region_id=" << it.first << " status=" << it.second
                              << " region=" << region.ShortDebugString();
            }
          } else {
            DINGO_LOG(INFO) << "CreateTable region_id=" << it.first
                            << " QueryRegion fail, error_code: " << ret2.error_code()
                            << ", error_str: " << ret2.error_str();
          }
        }

        for (const auto &id : id_to_erase) {
          region_status.erase(id);
        }

        DINGO_LOG(INFO) << "continue to check region_status, table_id: " << new_table_id
                        << ", region_id_count: " << region_ids.size()
                        << ", left max_check_region_state_count: " << max_check_region_state_count;

        if (max_check_region_state_count-- <= 0) {
          DINGO_LOG(ERROR) << "CreateTable check region state timeout, table_id: " << new_table_id
                           << ", region_id_count: " << region_ids.size();
          response->mutable_error()->set_errcode(pb::error::Errno::EINTERNAL);
          response->mutable_error()->set_errmsg("CreateTable check region state timeout");
          break;
        }

        bthread_usleep(500 * 1000);
      }
    } else {
      DINGO_LOG(INFO) << "CreateTable region_ids is empty, table_id: " << new_table_id;
    }
  }

  DINGO_LOG(INFO) << "CreateTable Success in meta_service table_name: " << request->table_definition().name()
                  << ", id: " << new_table_id << ", time_cost_ms: " << (butil::gettimeofday_ms() - start_ms);
}

void DoDropSchema(google::protobuf::RpcController *controller, const pb::meta::DropSchemaRequest *request,
                  pb::meta::DropSchemaResponse *response, google::protobuf::Closure *done,
                  std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(WARNING) << "DropSchema request:  parent_schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int64_t schema_id = request->schema_id().entity_id();
  int64_t parent_schema_id = request->schema_id().parent_entity_id();
  auto ret = coordinator_control->DropSchema(parent_schema_id, schema_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "DropSchema failed, schema_id=" << schema_id << " ret = " << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::DropSchemaRequest, pb::meta::DropSchemaResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::DropSchemaRequest, pb::meta::DropSchemaResponse>(request, response,
                                                                                        done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DropSchema failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }
}

void DoCreateSchema(google::protobuf::RpcController *controller, const pb::meta::CreateSchemaRequest *request,
                    pb::meta::CreateSchemaResponse *response, google::protobuf::Closure *done,
                    std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreatSchema request:  parent_schema_id = [" << request->parent_schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_parent_schema_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int64_t new_schema_id;
  auto ret = coordinator_control->CreateSchema(request->parent_schema_id().entity_id(), request->schema_name(),
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
  CoordinatorClosure<pb::meta::CreateSchemaRequest, pb::meta::CreateSchemaResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::CreateSchemaRequest, pb::meta::CreateSchemaResponse>(request, response,
                                                                                            done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CreateSchema failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }
}

void DoDropTable(google::protobuf::RpcController *controller, const pb::meta::DropTableRequest *request,
                 pb::meta::DropTableResponse *response, google::protobuf::Closure *done,
                 std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(WARNING) << "DropTable request:  schema_id = [" << request->table_id().parent_entity_id() << "]"
                     << " table_id = [" << request->table_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = coordinator_control->DropTable(request->table_id().parent_entity_id(), request->table_id().entity_id(),
                                            meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "DropTable failed in meta_service, table_id=" << request->table_id().entity_id();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::DropTableRequest, pb::meta::DropTableResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::DropTableRequest, pb::meta::DropTableResponse>(request, response,
                                                                                      done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DropTable failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }
}

void DoGetAutoIncrements(google::protobuf::RpcController * /*controller*/,
                         const pb::meta::GetAutoIncrementsRequest *request,
                         pb::meta::GetAutoIncrementsResponse *response, google::protobuf::Closure *done,
                         std::shared_ptr<AutoIncrementControl> auto_increment_control,
                         std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control->IsLeader()) {
    return auto_increment_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << request->ShortDebugString();

  butil::FlatMap<int64_t, int64_t> auto_increments;
  auto_increments.init(1024);
  auto ret = auto_increment_control->GetAutoIncrements(auto_increments);
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

void DoGetAutoIncrement(google::protobuf::RpcController * /*controller*/,
                        const pb::meta::GetAutoIncrementRequest *request, pb::meta::GetAutoIncrementResponse *response,
                        google::protobuf::Closure *done, std::shared_ptr<AutoIncrementControl> auto_increment_control,
                        std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control->IsLeader()) {
    return auto_increment_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  int64_t table_id = request->table_id().entity_id();
  int64_t start_id = 0;
  auto ret = auto_increment_control->GetAutoIncrement(table_id, start_id);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "get auto increment failed, " << table_id;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  response->set_start_id(start_id);
}

void DoCreateAutoIncrement(google::protobuf::RpcController *controller,
                           const pb::meta::CreateAutoIncrementRequest *request,
                           pb::meta::CreateAutoIncrementResponse *response, google::protobuf::Closure *done,
                           std::shared_ptr<AutoIncrementControl> auto_increment_control,
                           std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control->IsLeader()) {
    return auto_increment_control->RedirectResponse(response);
  }
  DINGO_LOG(INFO) << request->ShortDebugString();

  int64_t table_id = request->table_id().entity_id();
  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret = auto_increment_control->CreateAutoIncrement(table_id, request->start_id(), meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "failed, " << table_id << " | " << request->start_id();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *meta_closure =
      new CoordinatorClosure<pb::meta::CreateAutoIncrementRequest, pb::meta::CreateAutoIncrementResponse>(
          request, response, done_guard.release());
  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure, response);
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);
  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "failed, " << table_id << " | " << request->start_id() << " | " << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);

    if (ret2.error_code() == pb::error::Errno::ERAFT_NOTLEADER) {
      auto_increment_control->RedirectResponse(response);
    }
    return;
  }
}

void DoUpdateAutoIncrement(google::protobuf::RpcController *controller,
                           const ::dingodb::pb::meta::UpdateAutoIncrementRequest *request,
                           pb::meta::UpdateAutoIncrementResponse *response, google::protobuf::Closure *done,
                           std::shared_ptr<AutoIncrementControl> auto_increment_control,
                           std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control->IsLeader()) {
    return auto_increment_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  int64_t table_id = request->table_id().entity_id();
  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret =
      auto_increment_control->UpdateAutoIncrement(table_id, request->start_id(), request->force(), meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "update auto increment failed, " << table_id << " | " << request->start_id() << " | "
                     << request->force() << " | " << ret.error_str();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *meta_closure =
      new CoordinatorClosure<pb::meta::UpdateAutoIncrementRequest, pb::meta::UpdateAutoIncrementResponse>(
          request, response, done_guard.release());
  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);
  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "update auto increment failed, " << table_id << " | " << request->start_id() << " | "
                     << request->force();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);

    if (ret2.error_code() == pb::error::Errno::ERAFT_NOTLEADER) {
      auto_increment_control->RedirectResponse(response);
    }
    return;
  }
}

void DoGenerateAutoIncrement(google::protobuf::RpcController *controller,
                             const ::dingodb::pb::meta::GenerateAutoIncrementRequest *request,
                             pb::meta::GenerateAutoIncrementResponse *response, google::protobuf::Closure *done,
                             std::shared_ptr<AutoIncrementControl> auto_increment_control,
                             std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control->IsLeader()) {
    return auto_increment_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  int64_t table_id = request->table_id().entity_id();
  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret =
      auto_increment_control->GenerateAutoIncrement(table_id, request->count(), request->auto_increment_increment(),
                                                    request->auto_increment_offset(), meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "generate auto increment failed, " << ret << " | " << request->ShortDebugString();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *meta_closure =
      new CoordinatorClosure<pb::meta::GenerateAutoIncrementRequest, pb::meta::GenerateAutoIncrementResponse>(
          request, response, done_guard.release());
  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure, response);
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);
  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "generate auto increment failed, " << ret << " | " << request->ShortDebugString() << " | "
                     << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);

    if (ret2.error_code() == pb::error::Errno::ERAFT_NOTLEADER) {
      auto_increment_control->RedirectResponse(response);
    }
    return;
  }
}

void DoDeleteAutoIncrement(google::protobuf::RpcController *controller,
                           const ::dingodb::pb::meta::DeleteAutoIncrementRequest *request,
                           pb::meta::DeleteAutoIncrementResponse *response, google::protobuf::Closure *done,
                           std::shared_ptr<AutoIncrementControl> auto_increment_control,
                           std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control->IsLeader()) {
    return auto_increment_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  int64_t table_id = request->table_id().entity_id();
  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret = auto_increment_control->DeleteAutoIncrement(table_id, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "delete auto increment failed, " << table_id;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *meta_closure =
      new CoordinatorClosure<pb::meta::DeleteAutoIncrementRequest, pb::meta::DeleteAutoIncrementResponse>(
          request, response, done_guard.release());
  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kAutoIncrementRegionId);
  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "delete auto increment failed, " << table_id << " | " << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);

    if (ret2.error_code() == pb::error::Errno::ERAFT_NOTLEADER) {
      auto_increment_control->RedirectResponse(response);
    }
    return;
  }
}

void DoGetIndexesCount(google::protobuf::RpcController * /*controller*/,
                       const pb::meta::GetIndexesCountRequest *request, pb::meta::GetIndexesCountResponse *response,
                       google::protobuf::Closure *done, std::shared_ptr<CoordinatorControl> coordinator_control,
                       std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndexesCount request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  int64_t indexes_count = 0;
  auto ret = coordinator_control->GetIndexesCount(request->schema_id().entity_id(), indexes_count);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  response->set_indexes_count(indexes_count);
}

void DoGetIndexes(google::protobuf::RpcController * /*controller*/, const pb::meta::GetIndexesRequest *request,
                  pb::meta::GetIndexesResponse *response, google::protobuf::Closure *done,
                  std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndexs request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  std::vector<pb::meta::TableDefinitionWithId> table_definition_with_ids;
  auto ret = coordinator_control->GetIndexes(request->schema_id().entity_id(), table_definition_with_ids);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  if (table_definition_with_ids.empty()) {
    DINGO_LOG(INFO) << "meta_service GetIndexs no indexs, schema_id=" << request->schema_id().entity_id();
    return;
  }

  // add index_definition_with_id
  for (const auto &temp_definition : table_definition_with_ids) {
    auto *index_def_with_id = response->add_index_definition_with_ids();
    *(index_def_with_id->mutable_index_id()) = temp_definition.table_id();
    MetaServiceImpl::TableDefinitionToIndexDefinition(temp_definition.table_definition(),
                                                      *(index_def_with_id->mutable_index_definition()));
  }
}

void DoGetIndex(google::protobuf::RpcController * /*controller*/, const pb::meta::GetIndexRequest *request,
                pb::meta::GetIndexResponse *response, google::protobuf::Closure *done,
                std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  DINGO_LOG(DEBUG) << "GetIndex request:  index_id = [" << request->index_id().entity_id() << "]";

  pb::meta::TableDefinitionWithId table_definition_with_id;
  auto ret = coordinator_control->GetIndex(request->index_id().parent_entity_id(), request->index_id().entity_id(),
                                           true, table_definition_with_id);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *index = response->mutable_index_definition_with_id();
  *(index->mutable_index_id()) = table_definition_with_id.table_id();
  MetaServiceImpl::TableDefinitionToIndexDefinition(table_definition_with_id.table_definition(),
                                                    *(index->mutable_index_definition()));
}

void DoGetIndexByName(google::protobuf::RpcController * /*controller*/, const pb::meta::GetIndexByNameRequest *request,
                      pb::meta::GetIndexByNameResponse *response, google::protobuf::Closure *done,
                      std::shared_ptr<CoordinatorControl> coordinator_control,
                      std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndexByName request:  schema_id = [" << request->schema_id().entity_id() << "]"
                   << " index_name = [" << request->index_name() << "]";

  if (request->index_name().empty()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::meta::TableDefinitionWithId table_definition_with_id;
  auto ret = coordinator_control->GetIndexByName(request->schema_id().entity_id(), request->index_name(),
                                                 table_definition_with_id);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  auto *index = response->mutable_index_definition_with_id();
  *(index->mutable_index_id()) = table_definition_with_id.table_id();
  MetaServiceImpl::TableDefinitionToIndexDefinition(table_definition_with_id.table_definition(),
                                                    *(index->mutable_index_definition()));
}

void DoGetIndexRange(google::protobuf::RpcController * /*controller*/, const pb::meta::GetIndexRangeRequest *request,
                     pb::meta::GetIndexRangeResponse *response, google::protobuf::Closure *done,
                     std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndex request:  index_id = [" << request->index_id().entity_id() << "]";

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *index_range = response->mutable_index_range();
  auto ret = coordinator_control->GetIndexRange(request->index_id().parent_entity_id(), request->index_id().entity_id(),
                                                *index_range);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void DoGetIndexMetrics(google::protobuf::RpcController * /*controller*/,
                       const pb::meta::GetIndexMetricsRequest *request, pb::meta::GetIndexMetricsResponse *response,
                       google::protobuf::Closure *done, std::shared_ptr<CoordinatorControl> coordinator_control,
                       std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndexMetrics request:  index_id = [" << request->index_id().entity_id() << "]";

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  auto *index_metrics = response->mutable_index_metrics();
  auto ret = coordinator_control->GetIndexMetrics(request->index_id().parent_entity_id(),
                                                  request->index_id().entity_id(), *index_metrics);
  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
  }
}

void DoCreateIndexId(google::protobuf::RpcController *controller, const pb::meta::CreateIndexIdRequest *request,
                     pb::meta::CreateIndexIdResponse *response, google::protobuf::Closure *done,
                     std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateIndexId request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int64_t new_index_id;
  auto ret = coordinator_control->CreateIndexId(request->schema_id().entity_id(), new_index_id, meta_increment);
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
  CoordinatorClosure<pb::meta::CreateIndexIdRequest, pb::meta::CreateIndexIdResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::CreateIndexIdRequest, pb::meta::CreateIndexIdResponse>(request, response,
                                                                                              done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "CreateIndexId failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }

  DINGO_LOG(INFO) << "CreateIndexId Success in meta_service index_d =" << new_index_id;
}

void DoCreateIndex(google::protobuf::RpcController * /*controller*/, const pb::meta::CreateIndexRequest *request,
                   pb::meta::CreateIndexResponse *response, google::protobuf::Closure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateIndex request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(INFO) << request->ShortDebugString();

  int64_t start_ms = butil::gettimeofday_ms();

  if (!request->has_schema_id() || !request->has_index_definition()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  int64_t new_index_id = 0;
  if (request->has_index_id()) {
    if (request->index_id().entity_id() > 0) {
      new_index_id = request->index_id().entity_id();
      DINGO_LOG(INFO) << "CreateIndex index_id is given[" << new_index_id << "] request:  schema_id = ["
                      << request->schema_id().entity_id() << "]";
    }
  }

  pb::meta::TableDefinition table_definition;
  MetaServiceImpl::IndexDefinitionToTableDefinition(request->index_definition(), table_definition);

  // check if hnsw max_element is too big
  if (table_definition.index_parameter().vector_index_parameter().vector_index_type() ==
      pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    auto *hnsw_parameter =
        table_definition.mutable_index_parameter()->mutable_vector_index_parameter()->mutable_hnsw_parameter();
    if (hnsw_parameter->dimension() <= 0) {
      DINGO_LOG(ERROR) << "CreateIndex failed in meta_service, hnsw dimension is too small, dimension="
                       << hnsw_parameter->dimension();
      response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
      response->mutable_error()->set_errmsg("hnsw dimension is too small");
      return;
    }
    auto max_elements_limit = Helper::CalcHnswCountFromMemory(FLAGS_max_hnsw_memory_size_of_region,
                                                              hnsw_parameter->dimension(), hnsw_parameter->nlinks());
    if (hnsw_parameter->max_elements() > max_elements_limit) {
      DINGO_LOG(WARNING) << "CreateIndex warning in meta_service, hnsw max_elements is too big, max_elements="
                         << hnsw_parameter->max_elements() << ", dimension=" << hnsw_parameter->dimension()
                         << ", max_memory_size_of_region=" << FLAGS_max_hnsw_memory_size_of_region
                         << ", max elements in this dimention=" << max_elements_limit;
      hnsw_parameter->set_max_elements(max_elements_limit);
    }

    if (hnsw_parameter->nlinks() > FLAGS_max_hnsw_nlinks_of_region) {
      DINGO_LOG(WARNING) << "CreateIndex warning in meta_service, hnsw nlinks is too big, nlinks="
                         << hnsw_parameter->nlinks() << ", max_nlinks=" << FLAGS_max_hnsw_nlinks_of_region;
      hnsw_parameter->set_nlinks(FLAGS_max_hnsw_nlinks_of_region);
    }
  }

  std::vector<int64_t> region_ids;
  auto ret = coordinator_control->CreateIndex(request->schema_id().entity_id(), table_definition, 0, new_index_id,
                                              region_ids, meta_increment);
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

  {
    auto ret1 = coordinator_control->SubmitMetaIncrementSync(meta_increment);
    if (!ret1.ok()) {
      DINGO_LOG(ERROR) << "CreateIndex failed in meta_service, error code=" << ret1.error_code()
                       << ", error str=" << ret1.error_str();
      response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret1.error_code()));
      response->mutable_error()->set_errmsg(ret1.error_str());
      return;
    }

    if (!FLAGS_async_create_table) {
      DINGO_LOG(INFO) << "CreateIndex wait for raft done. table_id: " << new_index_id;
    }

    if ((!region_ids.empty()) && (!FLAGS_async_create_table)) {
      std::map<int64_t, bool> region_status;
      for (const auto &id : region_ids) {
        region_status[id] = false;
      }

      DINGO_LOG(INFO) << "start to check region_status, index_id: " << new_index_id
                      << ", region_id_count: " << region_ids.size();

      uint32_t max_check_region_state_count = FLAGS_max_check_region_state_count;

      while (!region_status.empty()) {
        std::vector<int64_t> id_to_erase;
        for (const auto &it : region_status) {
          DINGO_LOG(INFO) << "CreateIndex region_id=" << it.first << " status=" << it.second;
          pb::common::Region region;
          auto ret2 = coordinator_control->QueryRegion(it.first, region);
          if (ret2.ok()) {
            if (region.state() == pb::common::RegionState::REGION_NORMAL) {
              DINGO_LOG(INFO) << "region is NORMAL, region_id=" << it.first;
              id_to_erase.push_back(it.first);
            } else {
              DINGO_LOG(INFO) << "CreateIndex region_id=" << it.first << " status=" << it.second
                              << " region=" << region.ShortDebugString();
            }
          } else {
            DINGO_LOG(INFO) << "CreateIndex region_id=" << it.first
                            << " QueryRegion fail, error_code: " << ret2.error_code()
                            << ", error_str: " << ret2.error_str();
          }
        }

        for (const auto &id : id_to_erase) {
          region_status.erase(id);
        }

        DINGO_LOG(INFO) << "continue to check region_status, table_id: " << new_index_id
                        << ", region_id_count: " << region_ids.size()
                        << ", left max_check_region_state_count: " << max_check_region_state_count;

        if (max_check_region_state_count-- <= 0) {
          DINGO_LOG(ERROR) << "CreateIndex check region state timeout, table_id: " << new_index_id
                           << ", region_id_count: " << region_ids.size();
          response->mutable_error()->set_errcode(pb::error::Errno::EINTERNAL);
          response->mutable_error()->set_errmsg("CreateIndex check region state timeout");
          break;
        }

        bthread_usleep(500 * 1000);
      }
    } else {
      DINGO_LOG(INFO) << "CreateIndex region_ids is empty, table_id: " << new_index_id;
    }
  }

  DINGO_LOG(INFO) << "CreateIndex Success in meta_service index_name: " << request->index_definition().name()
                  << ", id: " << new_index_id << ", time_cost_ms: " << (butil::gettimeofday_ms() - start_ms);
}

void DoUpdateIndex(google::protobuf::RpcController *controller, const pb::meta::UpdateIndexRequest *request,
                   pb::meta::UpdateIndexResponse *response, google::protobuf::Closure *done,
                   std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "UpdateIndex request:  schema_id = [" << request->index_id().parent_entity_id() << "]"
                  << " index_id = [" << request->index_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_index_id() || !request->has_new_index_definition()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;
  pb::meta::TableDefinition table_definition;
  MetaServiceImpl::IndexDefinitionToTableDefinition(request->new_index_definition(), table_definition);

  // check if hnsw max_element is too big
  if (table_definition.index_parameter().vector_index_parameter().vector_index_type() ==
      pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    auto *hnsw_parameter =
        table_definition.mutable_index_parameter()->mutable_vector_index_parameter()->mutable_hnsw_parameter();
    if (hnsw_parameter->dimension() <= 0) {
      DINGO_LOG(ERROR) << "UpdateIndex failed in meta_service, hnsw dimension is too small, dimension="
                       << hnsw_parameter->dimension();
      response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
      response->mutable_error()->set_errmsg("hnsw dimension is too small");
    }
    auto max_elements_limit = Helper::CalcHnswCountFromMemory(FLAGS_max_hnsw_memory_size_of_region,
                                                              hnsw_parameter->dimension(), hnsw_parameter->nlinks());
    if (hnsw_parameter->max_elements() > max_elements_limit) {
      DINGO_LOG(ERROR) << "UpdateIndex warning in meta_service, hnsw max_elements is too big, max_elements="
                       << hnsw_parameter->max_elements() << ", dimension=" << hnsw_parameter->dimension()
                       << ", max_memory_size_of_region=" << FLAGS_max_hnsw_memory_size_of_region
                       << ", max elements in this dimention=" << max_elements_limit;
      response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
      response->mutable_error()->set_errmsg("hnsw max_elements is too big");
    }

    if (hnsw_parameter->nlinks() > FLAGS_max_hnsw_nlinks_of_region) {
      DINGO_LOG(ERROR) << "UpdateIndex warning in meta_service, hnsw nlinks is too big, nlinks="
                       << hnsw_parameter->nlinks() << ", max_nlinks=" << FLAGS_max_hnsw_nlinks_of_region;
      response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
      response->mutable_error()->set_errmsg("hnsw nlinks is too big");
    }
  }

  auto ret = coordinator_control->UpdateIndex(request->index_id().parent_entity_id(), request->index_id().entity_id(),
                                              table_definition, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "UpdateIndex failed in meta_service, error code=" << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
  DINGO_LOG(INFO) << "UpdateIndex new_index_id=" << request->index_id().entity_id();

  // prepare for raft process
  CoordinatorClosure<pb::meta::UpdateIndexRequest, pb::meta::UpdateIndexResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::UpdateIndexRequest, pb::meta::UpdateIndexResponse>(request, response,
                                                                                          done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "UpdateIndex failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }

  DINGO_LOG(INFO) << "UpdateIndex Success in meta_service index_name =" << request->new_index_definition().name();
}

void DoDropIndex(google::protobuf::RpcController *controller, const pb::meta::DropIndexRequest *request,
                 pb::meta::DropIndexResponse *response, google::protobuf::Closure *done,
                 std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(WARNING) << "DropIndex request:  schema_id = [" << request->index_id().parent_entity_id() << "]"
                     << " index_id = [" << request->index_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = coordinator_control->DropIndex(request->index_id().parent_entity_id(), request->index_id().entity_id(),
                                            true, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "DropIndex failed in meta_service, index_id=" << request->index_id().entity_id();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::DropIndexRequest, pb::meta::DropIndexResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::DropIndexRequest, pb::meta::DropIndexResponse>(request, response,
                                                                                      done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DropIndex failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }
}

void DoGenerateTableIds(google::protobuf::RpcController *controller, const pb::meta::GenerateTableIdsRequest *request,
                        pb::meta::GenerateTableIdsResponse *response, google::protobuf::Closure *done,
                        std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_schema_id() || !request->has_count()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("schema id or count.");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;
  butil::Status ret = coordinator_control->GenerateTableIds(request->schema_id().entity_id(), request->count(),
                                                            meta_increment, response);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "GenerateTableIds failed.";
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::GenerateTableIdsRequest, pb::meta::GenerateTableIdsResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::GenerateTableIdsRequest, pb::meta::GenerateTableIdsResponse>(
          request, response, done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "GenerateTableIds failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }

  DINGO_LOG(INFO) << "GenerateTableIds Success.";
}

void DoCreateTables(google::protobuf::RpcController * /*controller*/, const pb::meta::CreateTablesRequest *request,
                    pb::meta::CreateTablesResponse *response, google::protobuf::Closure *done,
                    std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  int64_t start_ms = butil::gettimeofday_ms();

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_schema_id() || request->table_definition_with_ids_size() == 0) {
    DINGO_LOG(ERROR) << request->has_schema_id() << " | " << request->table_definition_with_ids_size();
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("schema id or definition size.");
    return;
  }

  if (request->table_definition_with_ids_size() > FLAGS_max_table_definition_count_in_create_tables) {
    DINGO_LOG(ERROR) << "table_definition_with_ids_size is too big, size=" << request->table_definition_with_ids_size()
                     << ", max=" << FLAGS_max_table_definition_count_in_create_tables;
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table_definition_with_ids_size is too big");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  bool find_table_type = false;
  int64_t new_table_id = 0;
  butil::Status ret;
  std::vector<int64_t> region_ids;
  std::vector<std::string> table_name_to_rollback;
  std::vector<std::string> index_name_to_rollback;

  std::atomic<bool> table_create_success(false);
  ON_SCOPE_EXIT([&]() {
    if (table_create_success.load() == false) {
      for (const auto &table_name : table_name_to_rollback) {
        coordinator_control->RollbackCreateTable(request->schema_id().entity_id(), table_name);
      }
      for (const auto &index_name : index_name_to_rollback) {
        coordinator_control->RollbackCreateIndex(request->schema_id().entity_id(), index_name);
      }
    }
  });

  // process table type
  for (const auto &temp_with_id : request->table_definition_with_ids()) {
    const auto &table_id = temp_with_id.table_id();
    if (table_id.entity_type() == pb::meta::EntityType::ENTITY_TYPE_TABLE) {
      if (find_table_type) {
        DINGO_LOG(ERROR) << "found more then one table.";
        response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
        response->mutable_error()->set_errmsg("found more then one table.");
        return;
      }

      find_table_type = true;
      new_table_id = table_id.entity_id();
      const auto &definition = temp_with_id.table_definition();

      ret = coordinator_control->CreateTable(request->schema_id().entity_id(), definition, new_table_id, region_ids,
                                             meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "CreateTables failed in meta_service, error code=" << ret;
        response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
        response->mutable_error()->set_errmsg(ret.error_str());
        return;
      }

      table_name_to_rollback.push_back(definition.name());

      auto *table_id_ptr = response->add_table_ids();
      table_id_ptr->set_entity_id(new_table_id);
      table_id_ptr->set_parent_entity_id(request->schema_id().entity_id());
      table_id_ptr->set_entity_type(table_id.entity_type());

      DINGO_LOG(INFO) << "type: " << table_id.entity_type() << ", new_id=" << new_table_id;
    }
  }

  // process index type
  pb::coordinator_internal::TableIndexInternal table_index_internal;
  for (const auto &temp_with_id : request->table_definition_with_ids()) {
    const auto &table_id = temp_with_id.table_id();
    if (table_id.entity_type() == pb::meta::EntityType::ENTITY_TYPE_TABLE) {
      continue;
    }

    const auto &definition = temp_with_id.table_definition();
    int64_t new_index_id = table_id.entity_id();

    if (table_id.entity_type() == pb::meta::EntityType::ENTITY_TYPE_INDEX) {
      if (definition.columns_size() == 0) {
        DINGO_LOG(ERROR) << "index column not found.";
        response->mutable_error()->set_errcode(pb::error::Errno::EINDEX_COLUMN_NOT_FOUND);
        response->mutable_error()->set_errmsg("index column not found.");
        return;
      }

      ret = coordinator_control->CreateIndex(request->schema_id().entity_id(), definition, new_table_id, new_index_id,
                                             region_ids, meta_increment);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << "CreateTables failed in meta_service, error code=" << ret;
        response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
        response->mutable_error()->set_errmsg(ret.error_str());
        return;
      }

      index_name_to_rollback.push_back(definition.name());

    } else {
      ret = butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "entity type is illegal");
    }

    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "CreateTables failed in meta_service, error code=" << ret;
      response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      response->mutable_error()->set_errmsg(ret.error_str());
      return;
    }

    auto *table_id_ptr = response->add_table_ids();
    table_id_ptr->set_entity_id(new_index_id);
    table_id_ptr->set_parent_entity_id(table_id.parent_entity_id());
    table_id_ptr->set_entity_type(table_id.entity_type());

    if (find_table_type) {
      *(table_index_internal.add_table_ids()) = *table_id_ptr;
    }

    DINGO_LOG(INFO) << "type: " << table_id.entity_type() << ", new_id=" << new_index_id;
  }

  if (find_table_type) {
    table_index_internal.set_id(new_table_id);
    coordinator_control->CreateTableIndexesMap(table_index_internal, meta_increment);
    DINGO_LOG(INFO) << "CreateTableIndexesMap new_table_id=" << new_table_id;
  }

  {
    auto ret1 = coordinator_control->SubmitMetaIncrementSync(meta_increment);
    if (!ret1.ok()) {
      DINGO_LOG(ERROR) << "CreateTables failed in meta_service, error code=" << ret1.error_code()
                       << ", error str=" << ret1.error_str();
      response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret1.error_code()));
      response->mutable_error()->set_errmsg(ret1.error_str());
      return;
    }

    table_create_success.store(true);

    if (!FLAGS_async_create_table) {
      DINGO_LOG(INFO) << "CreateTables wait for raft done. table_id: " << new_table_id;
    }

    if ((!region_ids.empty()) && (!FLAGS_async_create_table)) {
      std::map<int64_t, bool> region_status;
      for (const auto &id : region_ids) {
        region_status[id] = false;
      }

      DINGO_LOG(INFO) << "start to check region_status, table_id: " << new_table_id
                      << ", region_id_count: " << region_ids.size();

      uint32_t max_check_region_state_count = FLAGS_max_check_region_state_count;

      while (!region_status.empty()) {
        std::vector<int64_t> id_to_erase;
        for (const auto &it : region_status) {
          DINGO_LOG(INFO) << "CreateTable region_id=" << it.first << " status=" << it.second;
          pb::common::Region region;
          auto ret2 = coordinator_control->QueryRegion(it.first, region);
          if (ret2.ok()) {
            if (region.state() == pb::common::RegionState::REGION_NORMAL) {
              DINGO_LOG(INFO) << "region is NORMAL, region_id=" << it.first;
              id_to_erase.push_back(it.first);
            } else {
              DINGO_LOG(INFO) << "CreateTable region_id=" << it.first << " status=" << it.second
                              << " region=" << region.ShortDebugString();
            }
          } else {
            DINGO_LOG(INFO) << "CreateTable region_id=" << it.first
                            << " QueryRegion fail, error_code: " << ret2.error_code()
                            << ", error_str: " << ret2.error_str();
          }
        }

        for (const auto &id : id_to_erase) {
          region_status.erase(id);
        }

        DINGO_LOG(INFO) << "continue to check region_status, table_id: " << new_table_id
                        << ", region_id_count: " << region_ids.size()
                        << ", left max_check_region_state_count: " << max_check_region_state_count;

        if (max_check_region_state_count-- <= 0) {
          DINGO_LOG(ERROR) << "CreateTables check region state timeout, table_id: " << new_table_id
                           << ", region_id_count: " << region_ids.size();
          response->mutable_error()->set_errcode(pb::error::Errno::EINTERNAL);
          response->mutable_error()->set_errmsg("CreateTables check region state timeout");
          break;
        }

        bthread_usleep(500 * 1000);
      }
    } else {
      DINGO_LOG(INFO) << "CreateTables region_ids is empty, table_id: " << new_table_id;
    }
  }

  DINGO_LOG(INFO) << "CreateTables Success. id: " << new_table_id
                  << ", name: " << request->table_definition_with_ids(0).table_definition().name()
                  << ", time_cost_ms: " << (butil::gettimeofday_ms() - start_ms);
}

void DoGetTables(google::protobuf::RpcController * /*controller*/, const pb::meta::GetTablesRequest *request,
                 pb::meta::GetTablesResponse *response, google::protobuf::Closure *done,
                 std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table id not found.");
    return;
  }

  butil::Status ret;
  if (request->table_id().entity_type() == pb::meta::EntityType::ENTITY_TYPE_INDEX) {
    auto *definition_with_id = response->add_table_definition_with_ids();
    ret = coordinator_control->GetIndex(request->table_id().parent_entity_id(), request->table_id().entity_id(), false,
                                        *definition_with_id);
  } else if (request->table_id().entity_type() == pb::meta::EntityType::ENTITY_TYPE_TABLE) {
    ret = coordinator_control->GetTableIndexes(request->table_id().parent_entity_id(), request->table_id().entity_id(),
                                               response);
  } else {
    ret = butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "entity type is illegal");
  }

  if (!ret.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }
}

void DoDropTables(google::protobuf::RpcController *controller, const pb::meta::DropTablesRequest *request,
                  pb::meta::DropTablesResponse *response, google::protobuf::Closure *done,
                  std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  butil::Status ret;
  pb::coordinator_internal::MetaIncrement meta_increment;
  for (const auto &table_id : request->table_ids()) {
    if (table_id.entity_type() == pb::meta::EntityType::ENTITY_TYPE_TABLE) {
      ret = coordinator_control->DropTableIndexes(table_id.parent_entity_id(), table_id.entity_id(), meta_increment);
    } else if (table_id.entity_type() == pb::meta::EntityType::ENTITY_TYPE_INDEX) {
      ret = coordinator_control->RemoveTableIndex(table_id.parent_entity_id(), table_id.entity_id(), meta_increment);
    } else {
      ret = butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "entity type is illegal");
    }

    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "drop failed, parent_entity_id: " << table_id.parent_entity_id()
                       << ", entity_id: " << table_id.entity_id();
      response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      response->mutable_error()->set_errmsg(ret.error_str());
      return;
    }
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::DropTablesRequest, pb::meta::DropTablesResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::DropTablesRequest, pb::meta::DropTablesResponse>(request, response,
                                                                                        done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DropTables failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }

  DINGO_LOG(INFO) << "DropTables Success.";
}

void DoUpdateTables(google::protobuf::RpcController *controller, const pb::meta::UpdateTablesRequest *request,
                    pb::meta::UpdateTablesResponse *response, google::protobuf::Closure *done,
                    std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  int64_t start_ms = butil::gettimeofday_ms();

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_table_definition_with_id() || !request->table_definition_with_id().has_table_id()) {
    DINGO_LOG(ERROR) << "table_definition_with_id or table_id not found.";
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table_definition_with_id or table_id not found.");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // process table type
  const auto &table_id = request->table_definition_with_id().table_id();
  if (table_id.entity_type() == pb::meta::EntityType::ENTITY_TYPE_TABLE) {
    auto table_id_to_update = table_id.entity_id();
    const auto &definition = request->table_definition_with_id().table_definition();

    auto ret = coordinator_control->UpdateTableDefinition(table_id_to_update, false, definition, meta_increment);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "UpdateTableDefinition failed in meta_service, error code=" << ret;
      response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      response->mutable_error()->set_errmsg(ret.error_str());
      return;
    }
  } else if (table_id.entity_type() == pb::meta::EntityType::ENTITY_TYPE_INDEX) {
    auto index_id_to_update = table_id.entity_id();
    const auto &definition = request->table_definition_with_id().table_definition();

    auto ret = coordinator_control->UpdateTableDefinition(index_id_to_update, true, definition, meta_increment);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << "UpdateIndexDefinition failed in meta_service, error code=" << ret;
      response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      response->mutable_error()->set_errmsg(ret.error_str());
      return;
    }
  } else {
    DINGO_LOG(ERROR) << "entity type is illegal, entity_type=" << table_id.entity_type();
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("entity type is illegal");
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::UpdateTablesRequest, pb::meta::UpdateTablesResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::UpdateTablesRequest, pb::meta::UpdateTablesResponse>(request, response,
                                                                                            done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "UpdateTables failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }
}

void DoAddIndexOnTable(google::protobuf::RpcController *controller, const pb::meta::AddIndexOnTableRequest *request,
                       pb::meta::AddIndexOnTableResponse *response, google::protobuf::Closure *done,
                       std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  int64_t start_ms = butil::gettimeofday_ms();

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_table_id() || !request->has_table_definition_with_id() || request->table_id().entity_id() == 0 ||
      request->table_definition_with_id().table_id().entity_id() == 0) {
    DINGO_LOG(ERROR) << "table_id, index_id or index_definition not found.";
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table_id, index_id or index_definition not found.");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = coordinator_control->AddIndexOnTable(
      request->table_id().entity_id(), request->table_definition_with_id().table_id().entity_id(),
      request->table_definition_with_id().table_definition(), meta_increment);

  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "AddIndexOnTable failed in meta_service, error code=" << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::AddIndexOnTableRequest, pb::meta::AddIndexOnTableResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::AddIndexOnTableRequest, pb::meta::AddIndexOnTableResponse>(request, response,
                                                                                                  done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);

  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "AddIndexOnTable failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }
}

void DoDropIndexOnTable(google::protobuf::RpcController *controller, const pb::meta::DropIndexOnTableRequest *request,
                        pb::meta::DropIndexOnTableResponse *response, google::protobuf::Closure *done,
                        std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  int64_t start_ms = butil::gettimeofday_ms();

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (request->table_id().entity_id() == 0 || request->index_id().entity_id() == 0) {
    DINGO_LOG(ERROR) << "table_id or index_id not found.";
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table_id or index_id not found.");
    return;
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  auto ret = coordinator_control->DropIndexOnTable(request->table_id().entity_id(), request->index_id().entity_id(),
                                                   meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "DropIndexOnTable failed in meta_service, error code=" << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::DropIndexOnTableRequest, pb::meta::DropIndexOnTableResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::DropIndexOnTableRequest, pb::meta::DropIndexOnTableResponse>(
          request, response, done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);

  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "DropIndexOnTable failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }
}

void DoSwitchAutoSplit(google::protobuf::RpcController *controller,
                       const ::dingodb::pb::meta::SwitchAutoSplitRequest *request,
                       pb::meta::SwitchAutoSplitResponse *response, google::protobuf::Closure *done,
                       std::shared_ptr<CoordinatorControl> coordinator_control, std::shared_ptr<Engine> raft_engine) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  butil::Status ret;
  pb::coordinator_internal::MetaIncrement meta_increment;
  ret = coordinator_control->SwitchAutoSplit(request->table_id().parent_entity_id(), request->table_id().entity_id(),
                                             request->auto_split(), meta_increment);

  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "switch auto split failed, auto_split: " << request->auto_split();
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::meta::SwitchAutoSplitRequest, pb::meta::SwitchAutoSplitResponse> *meta_closure =
      new CoordinatorClosure<pb::meta::SwitchAutoSplitRequest, pb::meta::SwitchAutoSplitResponse>(request, response,
                                                                                                  done_guard.release());

  std::shared_ptr<Context> ctx = std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_closure);
  ctx->SetRegionId(Constant::kMetaRegionId);

  // this is a async operation will be block by closure
  auto ret2 = raft_engine->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!ret2.ok()) {
    DINGO_LOG(ERROR) << "SwitchAutoSplit failed in meta_service, error code=" << ret2.error_code()
                     << ", error str=" << ret2.error_str();
    ServiceHelper::SetError(response->mutable_error(), ret2.error_code(), ret2.error_str());
    brpc::ClosureGuard done_guard(meta_closure);
    return;
  }

  DINGO_LOG(INFO) << "SwitchAutoSplit Success.";
}

void DoTsoService(google::protobuf::RpcController *controller, const ::dingodb::pb::meta::TsoRequest *request,
                  pb::meta::TsoResponse *response, google::protobuf::Closure *done,
                  std::shared_ptr<TsoControl> tso_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!tso_control->IsLeader()) {
    return tso_control->RedirectResponse(response);
  }
  DINGO_LOG(INFO) << request->ShortDebugString();

  if (request->op_type() == pb::meta::TsoOpType::OP_NONE) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("tso op_type not found.");
    return;
  }

  tso_control->Process(controller, request, response, done_guard.release());
}

void DoGetDeletedTable(google::protobuf::RpcController * /*controller*/,
                       const pb::meta::GetDeletedTableRequest *request, pb::meta::GetDeletedTableResponse *response,
                       google::protobuf::Closure *done, std::shared_ptr<CoordinatorControl> coordinator_control,
                       std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  std::vector<pb::meta::TableDefinitionWithId> table_definition_with_ids;
  auto ret = coordinator_control->GetDeletedTable(request->table_id().entity_id(), table_definition_with_ids);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "GetDeletedTable failed in meta_service, error code=" << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  for (const auto &table : table_definition_with_ids) {
    auto *table_definition_with_id = response->add_table_definition_with_ids();
    *table_definition_with_id = table;
  }
}

void DoGetDeletedIndex(google::protobuf::RpcController * /*controller*/,
                       const pb::meta::GetDeletedIndexRequest *request, pb::meta::GetDeletedIndexResponse *response,
                       google::protobuf::Closure *done, std::shared_ptr<CoordinatorControl> coordinator_control,
                       std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  std::vector<pb::meta::TableDefinitionWithId> table_definition_with_ids;
  auto ret = coordinator_control->GetDeletedIndex(request->index_id().entity_id(), table_definition_with_ids);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "GetDeletedIndex failed in meta_service, error code=" << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  for (const auto &table : table_definition_with_ids) {
    auto *table_definition_with_id = response->add_table_definition_with_ids();
    *table_definition_with_id = table;
  }
}

void DoCleanDeletedTable(google::protobuf::RpcController * /*controller*/,
                         const pb::meta::CleanDeletedTableRequest *request,
                         pb::meta::CleanDeletedTableResponse *response, google::protobuf::Closure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control,
                         std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret = coordinator_control->CleanDeletedTable(request->table_id().entity_id());
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CleanDeletedTable failed in meta_service, error code=" << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  DINGO_LOG(INFO) << "CleanDeletedTable Success.";
}

void DoCleanDeletedIndex(google::protobuf::RpcController * /*controller*/,
                         const pb::meta::CleanDeletedIndexRequest *request,
                         pb::meta::CleanDeletedIndexResponse *response, google::protobuf::Closure *done,
                         std::shared_ptr<CoordinatorControl> coordinator_control,
                         std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control->IsLeader()) {
    return coordinator_control->RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  pb::coordinator_internal::MetaIncrement meta_increment;
  auto ret = coordinator_control->CleanDeletedIndex(request->index_id().entity_id());
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "CleanDeletedIndex failed in meta_service, error code=" << ret;
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
    response->mutable_error()->set_errmsg(ret.error_str());
    return;
  }

  DINGO_LOG(INFO) << "CleanDeletedIndex Success.";
}

void DoHello(google::protobuf::RpcController * /*controller*/, const pb::meta::HelloRequest *request,
             pb::meta::HelloResponse *response, google::protobuf::Closure *done,
             std::shared_ptr<AutoIncrementControl> auto_increment_control, std::shared_ptr<Engine> /*raft_engine*/) {
  brpc::ClosureGuard const done_guard(done);
  DINGO_LOG(DEBUG) << "Hello request: " << request->hello();

  if (!auto_increment_control->IsLeader()) {
    return auto_increment_control->RedirectResponse(response);
  }

  if (request->get_memory_info()) {
    auto *memory_info = response->mutable_memory_info();
    auto_increment_control->GetMemoryInfo(*memory_info);
  }
}

void MetaServiceImpl::GetSchemas(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetSchemas(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetSchema(google::protobuf::RpcController *controller, const pb::meta::GetSchemaRequest *request,
                                pb::meta::GetSchemaResponse *response, google::protobuf::Closure *done) {
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetSchema(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetSchemaByName(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetSchemaByName(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetTablesCount(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetTablesCount(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetTablesBySchema(google::protobuf::RpcController *controller,
                                        const pb::meta::GetTablesBySchemaRequest *request,
                                        pb::meta::GetTablesBySchemaResponse *response,
                                        google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetTablesBySchema request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetTablesBySchema(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetTable(google::protobuf::RpcController *controller, const pb::meta::GetTableRequest *request,
                               pb::meta::GetTableResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  DINGO_LOG(DEBUG) << "GetTable request:  table_id = [" << request->table_id().entity_id() << "]";

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetTable(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetTableByName(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetTableByName(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetTableRange(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetTableRange(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetTableMetrics(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetTableMetrics(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
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
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateTableId(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::CreateTableIds(google::protobuf::RpcController *controller,
                                     const pb::meta::CreateTableIdsRequest *request,
                                     pb::meta::CreateTableIdsResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateTableIds request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateTableIds(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::CreateTable(google::protobuf::RpcController *controller,
                                  const pb::meta::CreateTableRequest *request, pb::meta::CreateTableResponse *response,
                                  google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateTable request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(INFO) << request->ShortDebugString();

  int64_t start_ms = butil::gettimeofday_ms();

  if (!request->has_schema_id() || !request->has_table_definition()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateTable(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::DropSchema(google::protobuf::RpcController *controller,
                                 const pb::meta::DropSchemaRequest *request, pb::meta::DropSchemaResponse *response,
                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(WARNING) << "DropSchema request:  parent_schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDropSchema(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::CreateSchema(google::protobuf::RpcController *controller,
                                   const pb::meta::CreateSchemaRequest *request,
                                   pb::meta::CreateSchemaResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreatSchema request:  parent_schema_id = [" << request->parent_schema_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_parent_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateSchema(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::DropTable(google::protobuf::RpcController *controller, const pb::meta::DropTableRequest *request,
                                pb::meta::DropTableResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(WARNING) << "DropTable request:  schema_id = [" << request->table_id().parent_entity_id() << "]"
                     << " table_id = [" << request->table_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDropTable(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetAutoIncrements(google::protobuf::RpcController *controller,
                                        const pb::meta::GetAutoIncrementsRequest *request,
                                        pb::meta::GetAutoIncrementsResponse *response,
                                        google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control_->IsLeader()) {
    return RedirectAutoIncrementResponse(response);
  }

  DINGO_LOG(DEBUG) << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetAutoIncrements(controller, request, response, svr_done, auto_increment_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetAutoIncrement(google::protobuf::RpcController *controller,
                                       const pb::meta::GetAutoIncrementRequest *request,
                                       pb::meta::GetAutoIncrementResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!auto_increment_control_->IsLeader()) {
    return RedirectAutoIncrementResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetAutoIncrement(controller, request, response, svr_done, auto_increment_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateAutoIncrement(controller, request, response, svr_done, auto_increment_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoUpdateAutoIncrement(controller, request, response, svr_done, auto_increment_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGenerateAutoIncrement(controller, request, response, svr_done, auto_increment_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDeleteAutoIncrement(controller, request, response, svr_done, auto_increment_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetIndexesCount(google::protobuf::RpcController *controller,
                                      const pb::meta::GetIndexesCountRequest *request,
                                      pb::meta::GetIndexesCountResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(DEBUG) << "GetIndexesCount request:  schema_id = [" << request->schema_id().entity_id() << "]";

  if (!request->has_schema_id() || request->schema_id().entity_id() <= 0) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetIndexesCount(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetIndexes(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetIndexes(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetIndex(google::protobuf::RpcController *controller, const pb::meta::GetIndexRequest *request,
                               pb::meta::GetIndexResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  DINGO_LOG(DEBUG) << "GetIndex request:  index_id = [" << request->index_id().entity_id() << "]";

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetIndex(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetIndexByName(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetIndexByName(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetIndexRange(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetIndexRange(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetIndexMetrics(google::protobuf::RpcController *controller,
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

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetIndexMetrics(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
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
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_schema_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateIndexId(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::CreateIndex(google::protobuf::RpcController *controller,
                                  const pb::meta::CreateIndexRequest *request, pb::meta::CreateIndexResponse *response,
                                  google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "CreateIndex request:  schema_id = [" << request->schema_id().entity_id() << "]";
  DINGO_LOG(INFO) << request->ShortDebugString();

  int64_t start_ms = butil::gettimeofday_ms();

  if (!request->has_schema_id() || !request->has_index_definition()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateIndex(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::UpdateIndex(google::protobuf::RpcController *controller,
                                  const pb::meta::UpdateIndexRequest *request, pb::meta::UpdateIndexResponse *response,
                                  google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << "UpdateIndex request:  schema_id = [" << request->index_id().parent_entity_id() << "]"
                  << " index_id = [" << request->index_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_index_id() || !request->has_new_index_definition()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoUpdateIndex(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::DropIndex(google::protobuf::RpcController *controller, const pb::meta::DropIndexRequest *request,
                                pb::meta::DropIndexResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(WARNING) << "DropIndex request:  schema_id = [" << request->index_id().parent_entity_id() << "]"
                     << " index_id = [" << request->index_id().entity_id() << "]";
  DINGO_LOG(DEBUG) << request->ShortDebugString();

  if (!request->has_index_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDropIndex(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GenerateTableIds(google::protobuf::RpcController *controller,
                                       const pb::meta::GenerateTableIdsRequest *request,
                                       pb::meta::GenerateTableIdsResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_schema_id() || !request->has_count()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("schema id or count.");
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGenerateTableIds(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::CreateTables(google::protobuf::RpcController *controller,
                                   const pb::meta::CreateTablesRequest *request,
                                   pb::meta::CreateTablesResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  int64_t start_ms = butil::gettimeofday_ms();

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_schema_id() || request->table_definition_with_ids_size() == 0) {
    DINGO_LOG(ERROR) << request->has_schema_id() << " | " << request->table_definition_with_ids_size();
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("schema id or definition size.");
    return;
  }

  if (request->table_definition_with_ids_size() > FLAGS_max_table_definition_count_in_create_tables) {
    DINGO_LOG(ERROR) << "table_definition_with_ids_size is too big, size=" << request->table_definition_with_ids_size()
                     << ", max=" << FLAGS_max_table_definition_count_in_create_tables;
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table_definition_with_ids_size is too big");
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCreateTables(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetTables(google::protobuf::RpcController *controller, const pb::meta::GetTablesRequest *request,
                                pb::meta::GetTablesResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_table_id()) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table id not found.");
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetTables(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::DropTables(google::protobuf::RpcController *controller,
                                 const pb::meta::DropTablesRequest *request, pb::meta::DropTablesResponse *response,
                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDropTables(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::UpdateTables(google::protobuf::RpcController *controller,
                                   const pb::meta::UpdateTablesRequest *request,
                                   pb::meta::UpdateTablesResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  int64_t start_ms = butil::gettimeofday_ms();

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_table_definition_with_id() || !request->table_definition_with_id().has_table_id()) {
    DINGO_LOG(ERROR) << "table_definition_with_id or table_id not found.";
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table_definition_with_id or table_id not found.");
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoUpdateTables(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::AddIndexOnTable(google::protobuf::RpcController *controller,
                                      const pb::meta::AddIndexOnTableRequest *request,
                                      pb::meta::AddIndexOnTableResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  int64_t start_ms = butil::gettimeofday_ms();

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (!request->has_table_id() || !request->has_table_definition_with_id() || request->table_id().entity_id() == 0 ||
      request->table_definition_with_id().table_id().entity_id() == 0) {
    DINGO_LOG(ERROR) << "table_id, index_id or index_definition not found.";
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table_id, index_id or index_definition not found.");
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoAddIndexOnTable(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::DropIndexOnTable(google::protobuf::RpcController *controller,
                                       const pb::meta::DropIndexOnTableRequest *request,
                                       pb::meta::DropIndexOnTableResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  int64_t start_ms = butil::gettimeofday_ms();

  DINGO_LOG(INFO) << request->ShortDebugString();

  if (request->table_id().entity_id() == 0 || request->index_id().entity_id() == 0) {
    DINGO_LOG(ERROR) << "table_id or index_id not found.";
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("table_id or index_id not found.");
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoDropIndexOnTable(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::SwitchAutoSplit(google::protobuf::RpcController *controller,
                                      const ::dingodb::pb::meta::SwitchAutoSplitRequest *request,
                                      pb::meta::SwitchAutoSplitResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoSwitchAutoSplit(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::TsoService(google::protobuf::RpcController *controller,
                                 const ::dingodb::pb::meta::TsoRequest *request, pb::meta::TsoResponse *response,
                                 google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!tso_control_->IsLeader()) {
    return RedirectResponseTso(response);
  }
  DINGO_LOG(INFO) << request->ShortDebugString();

  if (request->op_type() == pb::meta::TsoOpType::OP_NONE) {
    response->mutable_error()->set_errcode(Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("tso op_type not found.");
    return;
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoTsoService(controller, request, response, svr_done, tso_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetDeletedTable(google::protobuf::RpcController *controller,
                                      const pb::meta::GetDeletedTableRequest *request,
                                      pb::meta::GetDeletedTableResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetDeletedTable(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::GetDeletedIndex(google::protobuf::RpcController *controller,
                                      const pb::meta::GetDeletedIndexRequest *request,
                                      pb::meta::GetDeletedIndexResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetDeletedIndex(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::CleanDeletedTable(google::protobuf::RpcController *controller,
                                        const pb::meta::CleanDeletedTableRequest *request,
                                        pb::meta::CleanDeletedTableResponse *response,
                                        google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCleanDeletedTable(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::CleanDeletedIndex(google::protobuf::RpcController *controller,
                                        const pb::meta::CleanDeletedIndexRequest *request,
                                        pb::meta::CleanDeletedIndexResponse *response,
                                        google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  if (!coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  DINGO_LOG(INFO) << request->ShortDebugString();

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCleanDeletedIndex(controller, request, response, svr_done, coordinator_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

void MetaServiceImpl::Hello(google::protobuf::RpcController *controller, const pb::meta::HelloRequest *request,
                            pb::meta::HelloResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "Hello request: " << request->hello();

  if (!this->auto_increment_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  // Run in queue.
  auto *svr_done = new CoordinatorServiceClosure(__func__, done_guard.release(), request, response);
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoHello(controller, request, response, svr_done, auto_increment_control_, engine_);
  });
  bool ret = worker_set_->ExecuteRR(task);
  if (!ret) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL, "Commit execute queue failed");
  }
}

}  // namespace dingodb

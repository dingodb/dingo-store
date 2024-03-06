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

#ifndef DINGODB_META_SERVICE_H_
#define DINGODB_META_SERVICE_H_

#include <memory>

#include "coordinator/auto_increment_control.h"
#include "coordinator/coordinator_control.h"
#include "coordinator/tso_control.h"
#include "engine/engine.h"
#include "proto/meta.pb.h"

namespace dingodb {

class MetaServiceImpl : public pb::meta::MetaService {
  using Errno = pb::error::Errno;

 public:
  MetaServiceImpl() = default;

  template <typename T>
  void RedirectResponse(T response) {
    pb::common::Location leader_location;
    this->coordinator_control_->GetLeaderLocation(leader_location);

    auto* error_in_response = response->mutable_error();
    *(error_in_response->mutable_leader_location()) = leader_location;
    error_in_response->set_errcode(Errno::ERAFT_NOTLEADER);
    error_in_response->set_errmsg("not leader, new leader location is " + leader_location.DebugString());
  }

  template <typename T>
  void RedirectAutoIncrementResponse(T response) {
    pb::common::Location leader_location;
    this->auto_increment_control_->GetLeaderLocation(leader_location);

    auto* error_in_response = response->mutable_error();
    *(error_in_response->mutable_leader_location()) = leader_location;
    error_in_response->set_errcode(Errno::ERAFT_NOTLEADER);
    error_in_response->set_errmsg("not leader, new leader location is " + leader_location.DebugString());
  }

  template <typename T>
  void RedirectResponseTso(T response) {
    pb::common::Location leader_location;
    this->tso_control_->GetLeaderLocation(leader_location);

    auto* error_in_response = response->mutable_error();
    *(error_in_response->mutable_leader_location()) = leader_location;
    error_in_response->set_errcode(Errno::ERAFT_NOTLEADER);
    error_in_response->set_errmsg("not leader, new leader location is " + leader_location.DebugString());
  }

  void SetKvEngine(std::shared_ptr<Engine> engine) { engine_ = engine; };
  std::shared_ptr<Engine> GetKvEngine() { return engine_; };

  void SetControl(std::shared_ptr<CoordinatorControl> coordinator_control) {
    coordinator_control_ = coordinator_control;
  };
  std::shared_ptr<CoordinatorControl> GetCoordinatorControl() { return coordinator_control_; };

  void SetAutoIncrementControl(std::shared_ptr<AutoIncrementControl>& auto_increment_control) {
    auto_increment_control_ = auto_increment_control;
  };
  std::shared_ptr<AutoIncrementControl> GetAutoIncrementControl() { return auto_increment_control_; };

  void SetTsoControl(std::shared_ptr<TsoControl> tso_control) { tso_control_ = tso_control; };
  std::shared_ptr<TsoControl> GetTsoControl() { return tso_control_; };

  void GetSchemas(google::protobuf::RpcController* controller, const pb::meta::GetSchemasRequest* request,
                  pb::meta::GetSchemasResponse* response, google::protobuf::Closure* done) override;
  void GetSchema(google::protobuf::RpcController* controller, const pb::meta::GetSchemaRequest* request,
                 pb::meta::GetSchemaResponse* response, google::protobuf::Closure* done) override;
  void GetSchemaByName(google::protobuf::RpcController* controller, const pb::meta::GetSchemaByNameRequest* request,
                       pb::meta::GetSchemaByNameResponse* response, google::protobuf::Closure* done) override;

  void GetTablesBySchema(google::protobuf::RpcController* controller, const pb::meta::GetTablesBySchemaRequest* request,
                         pb::meta::GetTablesBySchemaResponse* response, google::protobuf::Closure* done) override;
  void GetTablesCount(google::protobuf::RpcController* controller, const pb::meta::GetTablesCountRequest* request,
                      pb::meta::GetTablesCountResponse* response, google::protobuf::Closure* done) override;
  void GetTable(google::protobuf::RpcController* controller, const pb::meta::GetTableRequest* request,
                pb::meta::GetTableResponse* response, google::protobuf::Closure* done) override;
  void GetTableByName(google::protobuf::RpcController* controller, const pb::meta::GetTableByNameRequest* request,
                      pb::meta::GetTableByNameResponse* response, google::protobuf::Closure* done) override;
  void GetTableRange(google::protobuf::RpcController* controller, const pb::meta::GetTableRangeRequest* request,
                     pb::meta::GetTableRangeResponse* response, google::protobuf::Closure* done) override;
  void GetTableMetrics(google::protobuf::RpcController* controller, const pb::meta::GetTableMetricsRequest* request,
                       pb::meta::GetTableMetricsResponse* response, google::protobuf::Closure* done) override;
  void CreateTable(google::protobuf::RpcController* controller, const pb::meta::CreateTableRequest* request,
                   pb::meta::CreateTableResponse* response, google::protobuf::Closure* done) override;
  void DropTable(google::protobuf::RpcController* controller, const pb::meta::DropTableRequest* request,
                 pb::meta::DropTableResponse* response, google::protobuf::Closure* done) override;
  void CreateTableId(google::protobuf::RpcController* controller, const pb::meta::CreateTableIdRequest* request,
                     pb::meta::CreateTableIdResponse* response, google::protobuf::Closure* done) override;
  void CreateTableIds(google::protobuf::RpcController* controller, const pb::meta::CreateTableIdsRequest* request,
                      pb::meta::CreateTableIdsResponse* response, google::protobuf::Closure* done) override;

  void GetIndexes(google::protobuf::RpcController* controller, const pb::meta::GetIndexesRequest* request,
                  pb::meta::GetIndexesResponse* response, google::protobuf::Closure* done) override;
  void GetIndexesCount(google::protobuf::RpcController* controller, const pb::meta::GetIndexesCountRequest* request,
                       pb::meta::GetIndexesCountResponse* response, google::protobuf::Closure* done) override;
  void GetIndex(google::protobuf::RpcController* controller, const pb::meta::GetIndexRequest* request,
                pb::meta::GetIndexResponse* response, google::protobuf::Closure* done) override;
  void GetIndexByName(google::protobuf::RpcController* controller, const pb::meta::GetIndexByNameRequest* request,
                      pb::meta::GetIndexByNameResponse* response, google::protobuf::Closure* done) override;
  void GetIndexRange(google::protobuf::RpcController* controller, const pb::meta::GetIndexRangeRequest* request,
                     pb::meta::GetIndexRangeResponse* response, google::protobuf::Closure* done) override;
  void GetIndexMetrics(google::protobuf::RpcController* controller, const pb::meta::GetIndexMetricsRequest* request,
                       pb::meta::GetIndexMetricsResponse* response, google::protobuf::Closure* done) override;
  void CreateIndex(google::protobuf::RpcController* controller, const pb::meta::CreateIndexRequest* request,
                   pb::meta::CreateIndexResponse* response, google::protobuf::Closure* done) override;
  void UpdateIndex(google::protobuf::RpcController* controller, const pb::meta::UpdateIndexRequest* request,
                   pb::meta::UpdateIndexResponse* response, google::protobuf::Closure* done) override;
  void DropIndex(google::protobuf::RpcController* controller, const pb::meta::DropIndexRequest* request,
                 pb::meta::DropIndexResponse* response, google::protobuf::Closure* done) override;
  void CreateIndexId(google::protobuf::RpcController* controller, const pb::meta::CreateIndexIdRequest* request,
                     pb::meta::CreateIndexIdResponse* response, google::protobuf::Closure* done) override;

  void CreateSchema(google::protobuf::RpcController* controller, const pb::meta::CreateSchemaRequest* request,
                    pb::meta::CreateSchemaResponse* response, google::protobuf::Closure* done) override;
  void DropSchema(google::protobuf::RpcController* controller, const pb::meta::DropSchemaRequest* request,
                  pb::meta::DropSchemaResponse* response, google::protobuf::Closure* done) override;

  void GenerateTableIds(google::protobuf::RpcController* controller, const pb::meta::GenerateTableIdsRequest* request,
                        pb::meta::GenerateTableIdsResponse* response, google::protobuf::Closure* done) override;
  void CreateTables(google::protobuf::RpcController* controller, const pb::meta::CreateTablesRequest* request,
                    pb::meta::CreateTablesResponse* response, google::protobuf::Closure* done) override;
  void GetTables(google::protobuf::RpcController* controller, const pb::meta::GetTablesRequest* request,
                 pb::meta::GetTablesResponse* response, google::protobuf::Closure* done) override;
  void DropTables(google::protobuf::RpcController* controller, const pb::meta::DropTablesRequest* request,
                  pb::meta::DropTablesResponse* response, google::protobuf::Closure* done) override;
  void UpdateTables(google::protobuf::RpcController* controller, const pb::meta::UpdateTablesRequest* request,
                    pb::meta::UpdateTablesResponse* response, google::protobuf::Closure* done) override;
  void AddIndexOnTable(google::protobuf::RpcController* controller, const pb::meta::AddIndexOnTableRequest* request,
                       pb::meta::AddIndexOnTableResponse* response, google::protobuf::Closure* done) override;
  void DropIndexOnTable(google::protobuf::RpcController* controller, const pb::meta::DropIndexOnTableRequest* request,
                        pb::meta::DropIndexOnTableResponse* response, google::protobuf::Closure* done) override;

  void GetAutoIncrements(google::protobuf::RpcController* controller, const pb::meta::GetAutoIncrementsRequest* request,
                         pb::meta::GetAutoIncrementsResponse* response, google::protobuf::Closure* done) override;
  void GetAutoIncrement(google::protobuf::RpcController* controller, const pb::meta::GetAutoIncrementRequest* request,
                        pb::meta::GetAutoIncrementResponse* response, google::protobuf::Closure* done) override;
  void CreateAutoIncrement(google::protobuf::RpcController* controller,
                           const pb::meta::CreateAutoIncrementRequest* request,
                           pb::meta::CreateAutoIncrementResponse* response, google::protobuf::Closure* done) override;
  void UpdateAutoIncrement(google::protobuf::RpcController* controller,
                           const ::dingodb::pb::meta::UpdateAutoIncrementRequest* request,
                           pb::meta::UpdateAutoIncrementResponse* response, google::protobuf::Closure* done) override;
  void GenerateAutoIncrement(google::protobuf::RpcController* controller,
                             const ::dingodb::pb::meta::GenerateAutoIncrementRequest* request,
                             pb::meta::GenerateAutoIncrementResponse* response,
                             google::protobuf::Closure* done) override;
  void DeleteAutoIncrement(google::protobuf::RpcController* controller,
                           const ::dingodb::pb::meta::DeleteAutoIncrementRequest* request,
                           pb::meta::DeleteAutoIncrementResponse* response, google::protobuf::Closure* done) override;

  void SwitchAutoSplit(google::protobuf::RpcController* controller,
                       const ::dingodb::pb::meta::SwitchAutoSplitRequest* request,
                       pb::meta::SwitchAutoSplitResponse* response, google::protobuf::Closure* done) override;

  void TsoService(google::protobuf::RpcController* controller, const pb::meta::TsoRequest* request,
                  pb::meta::TsoResponse* response, google::protobuf::Closure* done) override;

  void GetDeletedTable(google::protobuf::RpcController* controller, const pb::meta::GetDeletedTableRequest* request,
                       pb::meta::GetDeletedTableResponse* response, google::protobuf::Closure* done) override;
  void GetDeletedIndex(google::protobuf::RpcController* controller, const pb::meta::GetDeletedIndexRequest* request,
                       pb::meta::GetDeletedIndexResponse* response, google::protobuf::Closure* done) override;
  void CleanDeletedTable(google::protobuf::RpcController* controller, const pb::meta::CleanDeletedTableRequest* request,
                         pb::meta::CleanDeletedTableResponse* response, google::protobuf::Closure* done) override;
  void CleanDeletedIndex(google::protobuf::RpcController* controller, const pb::meta::CleanDeletedIndexRequest* request,
                         pb::meta::CleanDeletedIndexResponse* response, google::protobuf::Closure* done) override;

  // Tenant
  void CreateTenant(google::protobuf::RpcController* controller, const pb::meta::CreateTenantRequest* request,
                    pb::meta::CreateTenantResponse* response, google::protobuf::Closure* done) override;
  void UpdateTenant(google::protobuf::RpcController* controller, const pb::meta::UpdateTenantRequest* request,
                    pb::meta::UpdateTenantResponse* response, google::protobuf::Closure* done) override;
  void DropTenant(google::protobuf::RpcController* controller, const pb::meta::DropTenantRequest* request,
                  pb::meta::DropTenantResponse* response, google::protobuf::Closure* done) override;
  void GetTenants(google::protobuf::RpcController* controller, const pb::meta::GetTenantsRequest* request,
                  pb::meta::GetTenantsResponse* response, google::protobuf::Closure* done) override;

  // hello
  void Hello(google::protobuf::RpcController* controller, const pb::meta::HelloRequest* request,
             pb::meta::HelloResponse* response, google::protobuf::Closure* done) override;
  void GetMemoryInfo(google::protobuf::RpcController* controller, const pb::meta::HelloRequest* request,
                     pb::meta::HelloResponse* response, google::protobuf::Closure* done) override;
  void GetTsoInfo(google::protobuf::RpcController* controller, const pb::meta::TsoRequest* request,
                  pb::meta::TsoResponse* response, google::protobuf::Closure* done) override;

  // meta watch
  void Watch(google::protobuf::RpcController* controller, const pb::meta::WatchRequest* request,
             pb::meta::WatchResponse* response, google::protobuf::Closure* done) override;

  void ListWatch(google::protobuf::RpcController* controller, const pb::meta::ListWatchRequest* request,
                 pb::meta::ListWatchResponse* response, google::protobuf::Closure* done) override;

  void SetWorkSet(PriorWorkerSetPtr worker_set) { worker_set_ = worker_set; }

  // table and index definition convertor
  static void TableDefinitionToIndexDefinition(const pb::meta::TableDefinition& table_definition,
                                               pb::meta::IndexDefinition& index_definition);
  static void IndexDefinitionToTableDefinition(const pb::meta::IndexDefinition& index_definition,
                                               pb::meta::TableDefinition& table_definition);

 private:
  std::shared_ptr<CoordinatorControl> coordinator_control_;
  std::shared_ptr<AutoIncrementControl> auto_increment_control_;
  std::shared_ptr<TsoControl> tso_control_;
  std::shared_ptr<Engine> engine_;
  // Run service request.
  PriorWorkerSetPtr worker_set_;
};

}  // namespace dingodb

#endif  // DINGODB_META_SERVICE_H_

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

#include "brpc/controller.h"
#include "brpc/server.h"
#include "coordinator/auto_increment_control.h"
#include "coordinator/coordinator_control.h"
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
    error_in_response->mutable_leader_location()->CopyFrom(leader_location);
    error_in_response->set_errcode(Errno::ERAFT_NOTLEADER);
  }

  template <typename T>
  void RedirectAutoIncrementResponse(T response) {
    pb::common::Location leader_location;
    this->auto_increment_control_->GetLeaderLocation(leader_location);

    auto* error_in_response = response->mutable_error();
    error_in_response->mutable_leader_location()->CopyFrom(leader_location);
    error_in_response->set_errcode(Errno::ERAFT_NOTLEADER);
  }

  void SetKvEngine(std::shared_ptr<Engine> engine) { engine_ = engine; };
  void SetControl(std::shared_ptr<CoordinatorControl> coordinator_control) {
    this->coordinator_control_ = coordinator_control;
  };

  void SetAutoIncrementControl(std::shared_ptr<AutoIncrementControl>& auto_increment_control) {
    auto_increment_control_ = auto_increment_control;
  };

  void GetSchemas(google::protobuf::RpcController* controller, const pb::meta::GetSchemasRequest* request,
                  pb::meta::GetSchemasResponse* response, google::protobuf::Closure* done) override;
  void GetSchema(google::protobuf::RpcController* controller, const pb::meta::GetSchemaRequest* request,
                 pb::meta::GetSchemaResponse* response, google::protobuf::Closure* done) override;
  void GetSchemaByName(google::protobuf::RpcController* controller, const pb::meta::GetSchemaByNameRequest* request,
                       pb::meta::GetSchemaByNameResponse* response, google::protobuf::Closure* done) override;

  void GetTables(google::protobuf::RpcController* controller, const pb::meta::GetTablesRequest* request,
                 pb::meta::GetTablesResponse* response, google::protobuf::Closure* done) override;
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
  void DropIndex(google::protobuf::RpcController* controller, const pb::meta::DropIndexRequest* request,
                 pb::meta::DropIndexResponse* response, google::protobuf::Closure* done) override;
  void CreateIndexId(google::protobuf::RpcController* controller, const pb::meta::CreateIndexIdRequest* request,
                     pb::meta::CreateIndexIdResponse* response, google::protobuf::Closure* done) override;

  void CreateSchema(google::protobuf::RpcController* controller, const pb::meta::CreateSchemaRequest* request,
                    pb::meta::CreateSchemaResponse* response, google::protobuf::Closure* done) override;
  void DropSchema(google::protobuf::RpcController* controller, const pb::meta::DropSchemaRequest* request,
                  pb::meta::DropSchemaResponse* response, google::protobuf::Closure* done) override;

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

 private:
  std::shared_ptr<CoordinatorControl> coordinator_control_;
  std::shared_ptr<AutoIncrementControl> auto_increment_control_;
  std::shared_ptr<Engine> engine_;
};

}  // namespace dingodb

#endif  // DINGODB_META_SERVICE_H_

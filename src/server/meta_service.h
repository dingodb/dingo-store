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

#ifndef DINGODB_meta_SERVICE_H_
#define DINGODB_meta_SERVICE_H_

#include <memory>

#include "brpc/controller.h"
#include "brpc/server.h"
#include "coordinator/coordinator_control.h"
#include "proto/meta.pb.h"

namespace dingodb {

class MetaServiceImpl : public pb::meta::MetaService {
 public:
  MetaServiceImpl() = default;

  void SetControl(std::shared_ptr<CoordinatorControl> coordinator_control) {
    this->coordinator_control = coordinator_control;
  };

  void GetSchemas(google::protobuf::RpcController* controller, const pb::meta::GetSchemasRequest* request,
                  pb::meta::GetSchemasResponse* response, google::protobuf::Closure* done) override;
  void GetTables(google::protobuf::RpcController* controller, const pb::meta::GetTablesRequest* request,
                 pb::meta::GetTablesResponse* response, google::protobuf::Closure* done) override;
  void GetTable(google::protobuf::RpcController* controller, const pb::meta::GetTableRequest* request,
                pb::meta::GetTableResponse* response, google::protobuf::Closure* done) override;
  void CreateTable(google::protobuf::RpcController* controller, const pb::meta::CreateTableRequest* request,
                   pb::meta::CreateTableResponse* response, google::protobuf::Closure* done) override;
  void DropTable(google::protobuf::RpcController* controller, const pb::meta::DropTableRequest* request,
                 pb::meta::DropTableResponse* response, google::protobuf::Closure* done) override;

  void CreateSchema(google::protobuf::RpcController* controller, const pb::meta::CreateSchemaRequest* request,
                    pb::meta::CreateSchemaResponse* response, google::protobuf::Closure* done) override;

  std::shared_ptr<CoordinatorControl> coordinator_control;
};

}  // namespace dingodb

#endif  // DINGODB_meta_SERVICE_H_

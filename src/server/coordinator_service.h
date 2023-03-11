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

#ifndef DINGODB_COORDINATOR_SERVICE_H_
#define DINGODB_COORDINATOR_SERVICE_H_

#include <memory>

#include "brpc/controller.h"
#include "brpc/server.h"
#include "coordinator/coordinator_control.h"
#include "engine/engine.h"
#include "proto/coordinator.pb.h"

namespace dingodb {

class CoordinatorServiceImpl : public pb::coordinator::CoordinatorService {
 public:
  CoordinatorServiceImpl() = default;

  template <typename T>
  void RedirectResponse(T response) {
    pb::common::Location leader_location;
    this->coordinator_control->GetLeaderLocation(leader_location);

    auto* error_in_response = response->mutable_error();
    error_in_response->mutable_leader_location()->CopyFrom(leader_location);
    error_in_response->set_errcode(::dingodb::pb::error::Errno::ERAFT_NOTLEADER);
  }

  void SetKvEngine(std::shared_ptr<Engine> engine) { engine_ = engine; };
  void SetControl(std::shared_ptr<CoordinatorControl> coordinator_control) {
    this->coordinator_control = coordinator_control;
  };

  void Hello(google::protobuf::RpcController* controller, const pb::coordinator::HelloRequest* request,
             pb::coordinator::HelloResponse* response, google::protobuf::Closure* done) override;
  void StoreHeartbeat(google::protobuf::RpcController* controller,
                      const pb::coordinator::StoreHeartbeatRequest* request,
                      pb::coordinator::StoreHeartbeatResponse* response, google::protobuf::Closure* done) override;
  void GetRegionMap(google::protobuf::RpcController* controller, const pb::coordinator::GetRegionMapRequest* request,
                    pb::coordinator::GetRegionMapResponse* response, google::protobuf::Closure* done) override;
  void GetStoreMap(google::protobuf::RpcController* controller, const pb::coordinator::GetStoreMapRequest* request,
                   pb::coordinator::GetStoreMapResponse* response, google::protobuf::Closure* done) override;
  void CreateStore(google::protobuf::RpcController* controller, const pb::coordinator::CreateStoreRequest* request,
                   pb::coordinator::CreateStoreResponse* response, google::protobuf::Closure* done) override;

  void GetCoordinatorMap(google::protobuf::RpcController* controller,
                         const pb::coordinator::GetCoordinatorMapRequest* request,
                         pb::coordinator::GetCoordinatorMapResponse* response,
                         google::protobuf::Closure* done) override;

  std::shared_ptr<CoordinatorControl> coordinator_control;

 private:
  std::shared_ptr<Engine> engine_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_SERVICE_H_

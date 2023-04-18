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
  using Errno = pb::error::Errno;

 public:
  CoordinatorServiceImpl() = default;

  template <typename T>
  void RedirectResponse(T response) {
    pb::common::Location leader_location;
    this->coordinator_control_->GetLeaderLocation(leader_location);

    auto* error_in_response = response->mutable_error();
    error_in_response->mutable_leader_location()->CopyFrom(leader_location);
    error_in_response->set_errcode(Errno::ERAFT_NOTLEADER);
  }

  void SetKvEngine(std::shared_ptr<Engine> engine) { engine_ = engine; };
  void SetControl(std::shared_ptr<CoordinatorControl> coordinator_control) {
    this->coordinator_control_ = coordinator_control;
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
  void GetStoreMetrics(google::protobuf::RpcController* controller,
                       const pb::coordinator::GetStoreMetricsRequest* request,
                       pb::coordinator::GetStoreMetricsResponse* response, google::protobuf::Closure* done) override;

  void CreateStore(google::protobuf::RpcController* controller, const pb::coordinator::CreateStoreRequest* request,
                   pb::coordinator::CreateStoreResponse* response, google::protobuf::Closure* done) override;
  void DeleteStore(google::protobuf::RpcController* controller, const pb::coordinator::DeleteStoreRequest* request,
                   pb::coordinator::DeleteStoreResponse* response, google::protobuf::Closure* done) override;

  void ExecutorHeartbeat(google::protobuf::RpcController* controller,
                         const pb::coordinator::ExecutorHeartbeatRequest* request,
                         pb::coordinator::ExecutorHeartbeatResponse* response,
                         google::protobuf::Closure* done) override;
  void CreateExecutor(google::protobuf::RpcController* controller,
                      const pb::coordinator::CreateExecutorRequest* request,
                      pb::coordinator::CreateExecutorResponse* response, google::protobuf::Closure* done) override;
  void DeleteExecutor(google::protobuf::RpcController* controller,
                      const pb::coordinator::DeleteExecutorRequest* request,
                      pb::coordinator::DeleteExecutorResponse* response, google::protobuf::Closure* done) override;
  void GetExecutorMap(google::protobuf::RpcController* controller,
                      const pb::coordinator::GetExecutorMapRequest* request,
                      pb::coordinator::GetExecutorMapResponse* response, google::protobuf::Closure* done) override;
  void CreateExecutorUser(google::protobuf::RpcController* controller,
                          const pb::coordinator::CreateExecutorUserRequest* request,
                          pb::coordinator::CreateExecutorUserResponse* response,
                          google::protobuf::Closure* done) override;
  void UpdateExecutorUser(google::protobuf::RpcController* controller,
                          const pb::coordinator::UpdateExecutorUserRequest* request,
                          pb::coordinator::UpdateExecutorUserResponse* response,
                          google::protobuf::Closure* done) override;
  void DeleteExecutorUser(google::protobuf::RpcController* controller,
                          const pb::coordinator::DeleteExecutorUserRequest* request,
                          pb::coordinator::DeleteExecutorUserResponse* response,
                          google::protobuf::Closure* done) override;
  void GetExecutorUserMap(google::protobuf::RpcController* controller,
                          const pb::coordinator::GetExecutorUserMapRequest* request,
                          pb::coordinator::GetExecutorUserMapResponse* response,
                          google::protobuf::Closure* done) override;

  void GetCoordinatorMap(google::protobuf::RpcController* controller,
                         const pb::coordinator::GetCoordinatorMapRequest* request,
                         pb::coordinator::GetCoordinatorMapResponse* response,
                         google::protobuf::Closure* done) override;
  // Region service
  void QueryRegion(google::protobuf::RpcController* controller, const pb::coordinator::QueryRegionRequest* request,
                   pb::coordinator::QueryRegionResponse* response, google::protobuf::Closure* done) override;
  void CreateRegion(google::protobuf::RpcController* controller, const pb::coordinator::CreateRegionRequest* request,
                    pb::coordinator::CreateRegionResponse* response, google::protobuf::Closure* done) override;
  void DropRegion(google::protobuf::RpcController* controller, const pb::coordinator::DropRegionRequest* request,
                  pb::coordinator::DropRegionResponse* response, google::protobuf::Closure* done) override;
  void DropRegionPermanently(google::protobuf::RpcController* controller,
                             const pb::coordinator::DropRegionPermanentlyRequest* request,
                             pb::coordinator::DropRegionPermanentlyResponse* response,
                             google::protobuf::Closure* done) override;
  void SplitRegion(google::protobuf::RpcController* controller, const pb::coordinator::SplitRegionRequest* request,
                   pb::coordinator::SplitRegionResponse* response, google::protobuf::Closure* done) override;
  void MergeRegion(google::protobuf::RpcController* controller, const pb::coordinator::MergeRegionRequest* request,
                   pb::coordinator::MergeRegionResponse* response, google::protobuf::Closure* done) override;
  void ChangePeerRegion(google::protobuf::RpcController* controller,
                        const pb::coordinator::ChangePeerRegionRequest* request,
                        pb::coordinator::ChangePeerRegionResponse* response, google::protobuf::Closure* done) override;

  // StoreOperation service
  void GetStoreOperation(google::protobuf::RpcController* controller,
                         const pb::coordinator::GetStoreOperationRequest* request,
                         pb::coordinator::GetStoreOperationResponse* response,
                         google::protobuf::Closure* done) override;
  void CleanStoreOperation(google::protobuf::RpcController* controller,
                           const pb::coordinator::CleanStoreOperationRequest* request,
                           pb::coordinator::CleanStoreOperationResponse* response,
                           google::protobuf::Closure* done) override;
  void AddStoreOperation(google::protobuf::RpcController* controller,
                         const pb::coordinator::AddStoreOperationRequest* request,
                         pb::coordinator::AddStoreOperationResponse* response,
                         google::protobuf::Closure* done) override;
  void RemoveStoreOperation(google::protobuf::RpcController* controller,
                            const pb::coordinator::RemoveStoreOperationRequest* request,
                            pb::coordinator::RemoveStoreOperationResponse* response,
                            google::protobuf::Closure* done) override;

 private:
  std::shared_ptr<CoordinatorControl> coordinator_control_;
  std::shared_ptr<Engine> engine_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_SERVICE_H_

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

#include "server/coordinator_service.h"

#include "brpc/controller.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"

namespace dingodb {

void CoordinatorServiceImpl::Hello(google::protobuf::RpcController *controller,
                                   const pb::coordinator::HelloRequest *request,
                                   pb::coordinator::HelloResponse *response,
                                   google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "Hello request: " << request->hello();

  response->set_status(static_cast<pb::common::CoordinatorStatus>(0));
  response->set_status_detail("OK");

  // brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
  // cntl->SetFailed(0, "Error is %s", "Failed");
}

void CoordinatorServiceImpl::StoreHearbeat(
    google::protobuf::RpcController * /*controller*/,
    const pb::coordinator::StoreHeartbeatRequest *request,
    pb::coordinator::StoreHeartbeatResponse *response,
    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "StoreHearbeat request: storemap_epoch ["
            << request->self_storemap_epoch() << "] regionmap_epoch ["
            << request->self_regionmap_epoch() << "]";

  response->set_storemap_epoch(1);
  response->set_regionmap_epoch(1);
}

void CoordinatorServiceImpl::GetStoreMap(
    google::protobuf::RpcController * /*controller*/,
    const pb::coordinator::GetStoreMapRequest *request,
    pb::coordinator::GetStoreMapResponse *response,
    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetStoreMap request: _epoch [" << request->epoch() << "]";

  response->set_epoch(1);
}

void CoordinatorServiceImpl::GetRegionMap(
    google::protobuf::RpcController * /*controller*/,
    const pb::coordinator::GetRegionMapRequest *request,
    pb::coordinator::GetRegionMapResponse *response,
    google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetRegionMap request: _epoch [" << request->epoch() << "]";

  response->set_epoch(1);
}

}  // namespace dingodb

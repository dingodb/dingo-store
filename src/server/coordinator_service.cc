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

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "common/constant.h"
#include "coordinator/coordinator_closure.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

void CoordinatorServiceImpl::Hello(google::protobuf::RpcController * /*controller*/,
                                   const pb::coordinator::HelloRequest *request,
                                   pb::coordinator::HelloResponse *response, google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "Hello request: " << request->hello();

  if (!this->coordinator_control_->IsLeader()) {
    return RedirectResponse(response);
  }

  response->set_state(static_cast<pb::common::CoordinatorState>(0));
  response->set_status_detail("OK");
}

void CoordinatorServiceImpl::CreateStore(google::protobuf::RpcController *controller,
                                         const pb::coordinator::CreateStoreRequest *request,
                                         pb::coordinator::CreateStoreResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  LOG(INFO) << "Receive Create Store Request: IsLeader:" << is_leader << ", Request: " << format_request;

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create store
  uint64_t store_id = 0;
  std::string password;
  auto local_ctl = this->coordinator_control_;
  int const ret = local_ctl->CreateStore(request->cluster_id(), store_id, password, meta_increment);
  if (ret == 0) {
    response->set_store_id(store_id);
    response->set_password(password);
  } else {
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(pb::error::EILLEGAL_PARAMTETERS, "Need legal cluster_id");
    return;
  }

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::CreateStoreRequest, pb::coordinator::CreateStoreResponse>
      *meta_create_store_closure =
          new CoordinatorClosure<pb::coordinator::CreateStoreRequest, pb::coordinator::CreateStoreResponse>(
              request, response, done_guard.release());

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_create_store_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
}

void CoordinatorServiceImpl::StoreHeartbeat(google::protobuf::RpcController *controller,
                                            const pb::coordinator::StoreHeartbeatRequest *request,
                                            pb::coordinator::StoreHeartbeatResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  LOG(INFO) << "Receive Store Heartbeat Request, IsLeader:" << is_leader << ", Request:" << format_request;

  if (!is_leader) {
    return RedirectResponse(response);
  }

  pb::coordinator_internal::MetaIncrement meta_increment;

  // update store map
  int const new_storemap_epoch = this->coordinator_control_->UpdateStoreMap(request->store(), meta_increment);

  // update region map
  std::vector<pb::common::Region> regions;
  regions.resize(request->regions_size());
  for (int i = 0; i < request->regions_size(); i++) {
    regions.push_back(request->regions(i));
  }
  uint64_t const new_regionmap_epoch = this->coordinator_control_->UpdateRegionMap(regions, meta_increment);

  // prepare for raft process
  CoordinatorClosure<pb::coordinator::StoreHeartbeatRequest, pb::coordinator::StoreHeartbeatResponse>
      *meta_create_store_closure =
          new CoordinatorClosure<pb::coordinator::StoreHeartbeatRequest, pb::coordinator::StoreHeartbeatResponse>(
              request, response, done_guard.release(), new_regionmap_epoch, new_storemap_epoch,
              this->coordinator_control_);

  std::shared_ptr<Context> const ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_create_store_closure);
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);

  // // setup response
  // LOG(INFO) << "set epoch id to response " << new_storemap_epoch << " " << new_regionmap_epoch;
  // response->set_storemap_epoch(new_storemap_epoch);
  // response->set_regionmap_epoch(new_regionmap_epoch);

  // auto *new_regionmap = response->mutable_regionmap();
  // this->coordinator_control_->GetRegionMap(*new_regionmap);

  // auto *new_storemap = response->mutable_storemap();
  // this->coordinator_control_->GetStoreMap(*new_storemap);
}

void CoordinatorServiceImpl::GetStoreMap(google::protobuf::RpcController * /*controller*/,
                                         const pb::coordinator::GetStoreMapRequest *request,
                                         pb::coordinator::GetStoreMapResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard const done_guard(done);

  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  LOG(INFO) << "Receive Get StoreMap Request, IsLeader:" << is_leader << ", Request:" << format_request;

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  pb::common::StoreMap storemap;
  this->coordinator_control_->GetStoreMap(storemap);
  response->mutable_storemap()->CopyFrom(storemap);
  response->set_epoch(storemap.epoch());
}

void CoordinatorServiceImpl::GetRegionMap(google::protobuf::RpcController * /*controller*/,
                                          const pb::coordinator::GetRegionMapRequest *request,
                                          pb::coordinator::GetRegionMapResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);

  auto format_request = Helper::MessageToJsonString(*request);
  auto is_leader = this->coordinator_control_->IsLeader();
  LOG(INFO) << "Receive Get RegionMap Request, IsLeader:" << is_leader << ", Request:" << format_request;

  if (!is_leader) {
    RedirectResponse(response);
    return;
  }

  pb::common::RegionMap regionmap;
  this->coordinator_control_->GetRegionMap(regionmap);

  response->mutable_regionmap()->CopyFrom(regionmap);
  response->set_epoch(regionmap.epoch());
}

void CoordinatorServiceImpl::GetCoordinatorMap(google::protobuf::RpcController * /*controller*/,
                                               const pb::coordinator::GetCoordinatorMapRequest *request,
                                               pb::coordinator::GetCoordinatorMapResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  auto format_request = Helper::MessageToJsonString(*request);
  LOG(INFO) << "Receive Get CoordinatorMap Request:" << format_request;

  uint64_t epoch;
  pb::common::Location leader_location;
  std::vector<pb::common::Location> locations;
  this->coordinator_control_->GetCoordinatorMap(request->cluster_id(), epoch, leader_location, locations);

  response->set_epoch(epoch);

  auto *leader_location_resp = response->mutable_leader_location();
  leader_location_resp->CopyFrom(leader_location);

  for (const auto &member_location : locations) {
    auto *location = response->add_coordinator_locations();
    location->CopyFrom(member_location);
  }
}

}  // namespace dingodb

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

  response->set_state(static_cast<pb::common::CoordinatorState>(0));
  response->set_status_detail("OK");
}

void CoordinatorServiceImpl::CreateStore(google::protobuf::RpcController *controller,
                                         const pb::coordinator::CreateStoreRequest *request,
                                         pb::coordinator::CreateStoreResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "CreateStore request cluster_id = : " << request->cluster_id();
  LOG(INFO) << request->DebugString();

  pb::coordinator_internal::MetaIncrement meta_increment;

  // create store
  uint64_t store_id = 0;
  std::string password;
  int ret = this->coordinator_control->CreateStore(request->cluster_id(), store_id, password, meta_increment);
  if (ret == 0) {
    response->set_store_id(store_id);
    response->set_password(password);
  } else {
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(pb::error::EILLEGAL_PARAMTETERS, "Need legal cluster_id");
  }

  std::shared_ptr<CreateStoreClosure> meta_create_store_closure =
      std::make_shared<CreateStoreClosure>(request, response, done_guard.release());
  std::shared_ptr<Context> ctx =
      std::make_shared<Context>(static_cast<brpc::Controller *>(controller), meta_create_store_closure.get());
  ctx->set_region_id(Constant::kCoordinatorRegionId);
  ctx->SetMetaController(static_cast<MetaControl *>(coordinator_control));

  // this is a async operation will be block by closure
  engine_->MetaPut(ctx, meta_increment);
  return;
}

void CoordinatorServiceImpl::StoreHeartbeat(google::protobuf::RpcController * /*controller*/,
                                            const pb::coordinator::StoreHeartbeatRequest *request,
                                            pb::coordinator::StoreHeartbeatResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "StoreHearbeat request: storemap_epoch [" << request->self_storemap_epoch() << "] regionmap_epoch ["
            << request->self_regionmap_epoch() << "]";

  LOG(INFO) << request->DebugString();

  pb::coordinator_internal::MetaIncrement meta_increment;

  // update store map
  int new_storemap_epoch = this->coordinator_control->UpdateStoreMap(request->store(), meta_increment);

  // update region map
  LOG(INFO) << " region size = " << request->regions_size();
  std::vector<pb::common::Region> regions;
  regions.resize(request->regions_size());
  for (int i = 0; i < request->regions_size(); i++) {
    regions.push_back(request->regions(i));
  }
  int new_regionmap_epoch = this->coordinator_control->UpdateRegionMap(regions, meta_increment);

  // setup response
  LOG(INFO) << "set epoch id to response " << new_storemap_epoch << " " << new_regionmap_epoch;
  response->set_storemap_epoch(new_storemap_epoch);
  response->set_regionmap_epoch(new_regionmap_epoch);

  auto *new_regionmap = response->mutable_regionmap();
  this->coordinator_control->GetRegionMap(*new_regionmap);

  auto *new_storemap = response->mutable_storemap();
  this->coordinator_control->GetStoreMap(*new_storemap);

  LOG(INFO) << "end reponse build " << response->DebugString();
}

void CoordinatorServiceImpl::GetStoreMap(google::protobuf::RpcController * /*controller*/,
                                         const pb::coordinator::GetStoreMapRequest *request,
                                         pb::coordinator::GetStoreMapResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetStoreMap request: _epoch [" << request->epoch() << "]";

  pb::common::StoreMap storemap;
  this->coordinator_control->GetStoreMap(storemap);

  response->mutable_storemap()->CopyFrom(storemap);
  response->set_epoch(storemap.epoch());
}

void CoordinatorServiceImpl::GetRegionMap(google::protobuf::RpcController * /*controller*/,
                                          const pb::coordinator::GetRegionMapRequest *request,
                                          pb::coordinator::GetRegionMapResponse *response,
                                          google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetRegionMap request: _epoch [" << request->epoch() << "]";

  pb::common::RegionMap regionmap;
  this->coordinator_control->GetRegionMap(regionmap);

  response->mutable_regionmap()->CopyFrom(regionmap);
  response->set_epoch(regionmap.epoch());
}

void CoordinatorServiceImpl::GetCoordinatorMap(google::protobuf::RpcController * /*controller*/,
                                               const pb::coordinator::GetCoordinatorMapRequest *request,
                                               pb::coordinator::GetCoordinatorMapResponse *response,
                                               google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetCoordinatorMap request: _epoch [" << request->cluster_id() << "]";

  uint64_t epoch;
  pb::common::Location leader_location;
  std::vector<pb::common::Location> locations;
  this->coordinator_control->GetCoordinatorMap(request->cluster_id(), epoch, leader_location, locations);

  response->set_epoch(epoch);
}

}  // namespace dingodb

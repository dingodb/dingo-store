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
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
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

  // create store
  uint64_t store_id = 0;
  std::string password;
  int ret = this->coordinator_control->CreateStore(request->cluster_id(), store_id, password);

  if (ret == 0) {
    response->set_store_id(store_id);
    response->set_password(password);
  } else {
    brpc::Controller *brpc_controller = static_cast<brpc::Controller *>(controller);
    brpc_controller->SetFailed(pb::error::EILLEGAL_PARAMTETERS, "Need legal cluster_id");
  }
}

void CoordinatorServiceImpl::StoreHeartbeat(google::protobuf::RpcController * /*controller*/,
                                            const pb::coordinator::StoreHeartbeatRequest *request,
                                            pb::coordinator::StoreHeartbeatResponse *response,
                                            google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "StoreHearbeat request: storemap_epoch [" << request->self_storemap_epoch() << "] regionmap_epoch ["
            << request->self_regionmap_epoch() << "]";

  LOG(INFO) << request->DebugString();

  // update store map
  int new_storemap_epoch = this->coordinator_control->UpdateStoreMap(request->store());

  // update region map
  LOG(INFO) << " region size = " << request->regions_size();
  std::vector<pb::common::Region> regions;
  regions.resize(request->regions_size());
  for (int i = 0; i < request->regions_size(); i++) {
    regions.push_back(request->regions(i));
  }
  int new_regionmap_epoch = this->coordinator_control->UpdateRegionMapMulti(regions);

  // setup response
  LOG(INFO) << "set epoch id to response " << new_storemap_epoch << " " << new_regionmap_epoch;
  response->set_storemap_epoch(new_storemap_epoch);
  response->set_regionmap_epoch(new_regionmap_epoch);

  auto *new_regionmap = response->mutable_regionmap();
  new_regionmap->CopyFrom(this->coordinator_control->GetRegionMap());

  auto *new_storemap = response->mutable_storemap();
  new_storemap->CopyFrom(this->coordinator_control->GetStoreMap());

  LOG(INFO) << "end reponse build " << response->DebugString();
}

void CoordinatorServiceImpl::GetStoreMap(google::protobuf::RpcController * /*controller*/,
                                         const pb::coordinator::GetStoreMapRequest *request,
                                         pb::coordinator::GetStoreMapResponse *response,
                                         google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "GetStoreMap request: _epoch [" << request->epoch() << "]";

  pb::common::StoreMap storemap;
  storemap = this->coordinator_control->GetStoreMap();

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
  regionmap = this->coordinator_control->GetRegionMap();

  response->mutable_regionmap()->CopyFrom(regionmap);
  response->set_epoch(regionmap.epoch());
}

}  // namespace dingodb

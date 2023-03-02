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

#include "server/heartbeat.h"

#include "coordinator/coordinator_interaction.h"

namespace dingodb {

Heartbeat::Heartbeat() {}

Heartbeat::~Heartbeat() {}

void Heartbeat::SendStoreHeartbeat(void* arg) {
  LOG(INFO) << "SendStoreHeartbeat...";
  CoordinatorInteraction* coordinator_interaction =
      static_cast<CoordinatorInteraction*>(arg);

  pb::coordinator::StoreHeartbeatRequest request;
  auto store_meta = Server::GetInstance()->get_store_meta_manager();

  request.set_self_storemap_epoch(store_meta->GetServerEpoch());
  request.set_self_regionmap_epoch(store_meta->GetRegionEpoch());
  auto store = request.mutable_store();
  *store = *store_meta->GetStore();

  for (auto _region : store_meta->GetAllRegion()) {
    auto region = request.add_regions();
    *region = *_region;
  }

  pb::coordinator::StoreHeartbeatResponse response;
  coordinator_interaction->SendRequest<pb::coordinator::StoreHeartbeatRequest,
                                       pb::coordinator::StoreHeartbeatResponse>(
      "StoreHearbeat", request, response);
  HandleStoreHeartbeatResponse(store_meta, response);
}

void Heartbeat::HandleStoreHeartbeatResponse(
    std::shared_ptr<dingodb::StoreMetaManager> store_meta,
    const pb::coordinator::StoreHeartbeatResponse& response) {
  // check region, if has new region, add region.
  LOG(INFO) << "HandleStoreHeartbeatResponse...";
}

}  // namespace dingodb
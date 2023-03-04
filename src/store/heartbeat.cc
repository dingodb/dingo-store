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

#include "store/heartbeat.h"

#include "common/helper.h"
#include "coordinator/coordinator_interaction.h"

namespace dingodb {

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

  for (auto& it : store_meta->GetAllRegion()) {
    auto region = request.add_regions();
    *region = *(it.second);
  }

  pb::coordinator::StoreHeartbeatResponse response;
  coordinator_interaction->SendRequest<pb::coordinator::StoreHeartbeatRequest,
                                       pb::coordinator::StoreHeartbeatResponse>(
      "StoreHearbeat", request, response);
  HandleStoreHeartbeatResponse(store_meta, response);
}

static std::vector<std::shared_ptr<pb::common::Region> > GetNewRegion(
    std::map<uint64_t, std::shared_ptr<pb::common::Region> > local_regions,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Region>&
        remote_regions) {
  std::vector<std::shared_ptr<pb::common::Region> > new_regions;
  for (auto& remote_region : remote_regions) {
    if (local_regions.find(remote_region.id()) == local_regions.end()) {
      new_regions.push_back(
          std::make_shared<pb::common::Region>(remote_region));
    }
  }

  return new_regions;
}

static std::vector<std::shared_ptr<pb::common::Region> > GetChangedPeerRegion(
    std::map<uint64_t, std::shared_ptr<pb::common::Region> > local_regions,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Region>&
        remote_regions) {
  std::vector<std::shared_ptr<pb::common::Region> > changedPeers;
  for (auto& remote_region : remote_regions) {
    auto it = local_regions.find(remote_region.id());
    if (it != local_regions.end() &&
        remote_region.epoch() != it->second->epoch()) {
      std::vector<pb::common::Peer> local_peers(it->second->peers().begin(),
                                                it->second->peers().end());
      std::vector<pb::common::Peer> remote_peers(remote_region.peers().begin(),
                                                 remote_region.peers().end());

      Helper::SortPeers(local_peers);
      Helper::SortPeers(remote_peers);
      if (Helper::IsDifferencePeers(local_peers, remote_peers)) {
        changedPeers.push_back(
            std::make_shared<pb::common::Region>(remote_region));
      }
    }
  }

  return changedPeers;
}

static std::vector<std::shared_ptr<pb::common::Region> > GetDeleteRegion(
    std::map<uint64_t, std::shared_ptr<pb::common::Region> > local_regions,
    const google::protobuf::RepeatedPtrField<pb::common::Region>&
        remote_regions) {
  auto to_region_map_func =
      [remote_regions]() -> std::map<uint64_t, pb::common::Region> {
    std::map<uint64_t, pb::common::Region> regionMap;
    for (auto& remote_region : remote_regions) {
      regionMap[remote_region.id()] = remote_region;
    }

    return regionMap;
  };

  std::vector<std::shared_ptr<pb::common::Region> > regions;
  std::map<uint64_t, pb::common::Region> remote_region_map =
      to_region_map_func();
  for (auto& local_region : local_regions) {
    if (remote_region_map.find(local_region.first) == remote_region_map.end()) {
      regions.push_back(local_region.second);
    }
  }

  return regions;
}

void Heartbeat::HandleStoreHeartbeatResponse(
    std::shared_ptr<dingodb::StoreMetaManager> store_meta,
    const pb::coordinator::StoreHeartbeatResponse& response) {
  // check region, if has new region, add region.
  LOG(INFO) << "HandleStoreHeartbeatResponse...";
  auto local_regions = store_meta->GetAllRegion();
  auto store_control = Server::GetInstance()->get_store_control();

  // If has new region, add region.
  auto new_regions =
      GetNewRegion(local_regions, response.regionmap().regions());
  LOG(INFO) << "new regions size: " << new_regions.size();
  if (new_regions.size() > 0) {
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    // store_control->AddRegions(ctx, new_regions);
  }

  // Check for change peers region.
  auto changed_peer_regions =
      GetChangedPeerRegion(local_regions, response.regionmap().regions());
  LOG(INFO) << "change peer regions size: " << changed_peer_regions.size();
  for (auto region : changed_peer_regions) {
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    // store_control->ChangeRegion(ctx, region);
  }

  // Check for delete region.
  auto delete_regions =
      GetDeleteRegion(local_regions, response.regionmap().regions());
  LOG(INFO) << "delete regions size: " << delete_regions.size();
  for (auto region : delete_regions) {
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    // store_control->DeleteRegion(ctx, region->id());
  }
}

}  // namespace dingodb
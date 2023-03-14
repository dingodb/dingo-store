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
  CoordinatorInteraction* coordinator_interaction = static_cast<CoordinatorInteraction*>(arg);

  pb::coordinator::StoreHeartbeatRequest request;
  auto store_meta = Server::GetInstance()->GetStoreMetaManager();

  request.set_self_storemap_epoch(store_meta->GetServerEpoch());
  request.set_self_regionmap_epoch(store_meta->GetRegionEpoch());

  request.mutable_store()->CopyFrom(*store_meta->GetStore(Server::GetInstance()->Id()));

  auto store_id = Server::GetInstance()->Id();
  for (auto& it : store_meta->GetAllRegion()) {
    if (it.second->leader_store_id() == store_id) {
      request.add_regions()->CopyFrom(*(it.second));
    }
  }

  pb::coordinator::StoreHeartbeatResponse response;
  auto status = coordinator_interaction->SendRequest("StoreHeartbeat", request, response);
  if (status.ok()) {
    HandleStoreHeartbeatResponse(store_meta, response);
  }
}

static std::vector<std::shared_ptr<pb::common::Store> > GetNewStore(
    std::map<uint64_t, std::shared_ptr<pb::common::Store> > local_stores,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Store>& remote_stores) {
  std::vector<std::shared_ptr<pb::common::Store> > new_stores;
  for (const auto& remote_store : remote_stores) {
    if (local_stores.find(remote_store.id()) == local_stores.end()) {
      new_stores.push_back(std::make_shared<pb::common::Store>(remote_store));
    }
  }

  return new_stores;
}

static std::vector<std::shared_ptr<pb::common::Store> > GetChangedStore(
    std::map<uint64_t, std::shared_ptr<pb::common::Store> > local_stores,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Store>& remote_stores) {
  std::vector<std::shared_ptr<pb::common::Store> > changed_stores;
  for (const auto& remote_store : remote_stores) {
    if (remote_store.id() == 0) {
      continue;
    }
    auto it = local_stores.find(remote_store.id());
    if (it != local_stores.end()) {
      if (it->second->raft_location().host() != remote_store.raft_location().host() ||
          it->second->raft_location().port() != remote_store.raft_location().port()) {
        changed_stores.push_back(std::make_shared<pb::common::Store>(remote_store));
      }
    }
  }

  return changed_stores;
}

static std::vector<std::shared_ptr<pb::common::Store> > GetDeletedStore(
    std::map<uint64_t, std::shared_ptr<pb::common::Store> > local_stores,
    const google::protobuf::RepeatedPtrField<pb::common::Store>& remote_stores) {
  std::vector<std::shared_ptr<pb::common::Store> > stores;
  for (auto& store : remote_stores) {
    if (store.state() != pb::common::STORE_OFFLINE && store.state() != pb::common::STORE_OUT) {
      continue;
    }

    auto it = local_stores.find(store.id());
    if (it != local_stores.end()) {
      stores.push_back(it->second);
    }
  }

  return stores;
}

static std::vector<std::shared_ptr<pb::common::Region> > GetNewRegion(
    std::map<uint64_t, std::shared_ptr<pb::common::Region> > local_regions,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Region>& remote_regions) {
  std::vector<std::shared_ptr<pb::common::Region> > new_regions;
  for (const auto& remote_region : remote_regions) {
    if (local_regions.find(remote_region.id()) == local_regions.end()) {
      new_regions.push_back(std::make_shared<pb::common::Region>(remote_region));
    }
  }

  return new_regions;
}

static std::vector<std::shared_ptr<pb::common::Region> > GetChangedPeerRegion(
    std::map<uint64_t, std::shared_ptr<pb::common::Region> > local_regions,
    const google::protobuf::RepeatedPtrField<dingodb::pb::common::Region>& remote_regions) {
  std::vector<std::shared_ptr<pb::common::Region> > changed_peers;
  for (const auto& remote_region : remote_regions) {
    if (remote_region.id() == 0) {
      continue;
    }
    auto it = local_regions.find(remote_region.id());
    if (it != local_regions.end() && remote_region.epoch() != it->second->epoch()) {
      std::vector<pb::common::Peer> local_peers(it->second->peers().begin(), it->second->peers().end());
      std::vector<pb::common::Peer> remote_peers(remote_region.peers().begin(), remote_region.peers().end());

      Helper::SortPeers(local_peers);
      Helper::SortPeers(remote_peers);
      if (Helper::IsDifferencePeers(local_peers, remote_peers)) {
        changed_peers.push_back(std::make_shared<pb::common::Region>(remote_region));
      }
    }
  }

  return changed_peers;
}

static std::vector<std::shared_ptr<pb::common::Region> > GetDeleteRegion(
    std::map<uint64_t, std::shared_ptr<pb::common::Region> > local_regions,
    const google::protobuf::RepeatedPtrField<pb::common::Region>& remote_regions) {
  std::vector<std::shared_ptr<pb::common::Region> > regions;
  for (auto& region : remote_regions) {
    if (region.id() == 0 || region.state() != pb::common::REGION_DELETE) {
      continue;
    }

    auto it = local_regions.find(region.id());
    if (it != local_regions.end()) {
      regions.push_back(it->second);
    }
  }

  return regions;
}

void Heartbeat::HandleStoreHeartbeatResponse(std::shared_ptr<dingodb::StoreMetaManager> store_meta,
                                             const pb::coordinator::StoreHeartbeatResponse& response) {
  // Handle store meta data.
  auto local_stores = store_meta->GetAllStore();
  auto remote_stores = response.storemap().stores();

  auto new_stores = GetNewStore(local_stores, remote_stores);
  LOG(INFO) << "new store size: " << new_stores.size() << " / " << local_stores.size();
  for (auto store : new_stores) {
    store_meta->AddStore(store);
  }

  auto changed_stores = GetChangedStore(local_stores, remote_stores);
  LOG(INFO) << "changed store size: " << changed_stores.size() << " / " << local_stores.size();
  for (auto store : changed_stores) {
    store_meta->UpdateStore(store);
  }

  auto deleted_stores = GetDeletedStore(local_stores, remote_stores);
  LOG(INFO) << "deleted store size: " << deleted_stores.size() << " / " << local_stores.size();
  for (auto store : deleted_stores) {
    store_meta->DeleteStore(store->id());
  }

  // check region, if has new region, add region.
  auto store_control = Server::GetInstance()->GetStoreControl();
  auto local_regions = store_meta->GetAllRegion();
  auto remote_regions = response.regionmap().regions();

  // If has new region, add region.
  auto new_regions = GetNewRegion(local_regions, remote_regions);
  LOG(INFO) << "new regions size: " << new_regions.size() << " / " << local_regions.size();
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  for (auto region : new_regions) {
    store_control->AddRegion(ctx, region);
  }

  // Check for change peers region.
  auto changed_peer_regions = GetChangedPeerRegion(local_regions, remote_regions);
  LOG(INFO) << "change peer regions size: " << changed_peer_regions.size() << " / " << local_regions.size();
  for (auto region : changed_peer_regions) {
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    // store_control->ChangeRegion(ctx, region);
  }

  // Check for delete region.
  auto delete_regions = GetDeleteRegion(local_regions, remote_regions);
  LOG(INFO) << "delete regions size: " << delete_regions.size() << " / " << local_regions.size();
  for (auto region : delete_regions) {
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    // store_control->DeleteRegion(ctx, region->id());
  }
}

}  // namespace dingodb
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

#include <map>

#include "butil/scoped_lock.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/push.pb.h"
#include "server/server.h"

namespace dingodb {

// this is for coordinator
void Heartbeat::CalculateTableMetrics(void* arg) {
  CoordinatorControl* coordinator_control = static_cast<CoordinatorControl*>(arg);

  if (!coordinator_control->IsLeader()) {
    // DINGO_LOG(INFO) << "SendCoordinatorPushToStore... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "CalculateTableMetrics... this is leader";

  coordinator_control->CalculateTableMetrics();
}

// this is for coordinator
void Heartbeat::SendCoordinatorPushToStore(void* arg) {
  CoordinatorControl* coordinator_control = static_cast<CoordinatorControl*>(arg);

  if (!coordinator_control->IsLeader()) {
    // DINGO_LOG(INFO) << "SendCoordinatorPushToStore... this is follower";
    return;
  }
  DINGO_LOG(DEBUG) << "SendCoordinatorPushToStore... this is leader";

  butil::FlatMap<uint64_t, pb::common::Store> store_to_push;
  store_to_push.init(1000, 80);  // notice: FlagMap must init before use
  coordinator_control->GetPushStoreMap(store_to_push);

  if (store_to_push.empty()) {
    // DINGO_LOG(INFO) << "SendCoordinatorPushToStore... No store to push";
    return;
  }

  // generate new heartbeat response
  pb::coordinator::StoreHeartbeatResponse heartbeat_response;
  {
    auto* new_regionmap = heartbeat_response.mutable_regionmap();
    coordinator_control->GetRegionMap(*new_regionmap);

    auto* new_storemap = heartbeat_response.mutable_storemap();
    coordinator_control->GetStoreMap(*new_storemap);

    heartbeat_response.set_storemap_epoch(new_storemap->epoch());
    heartbeat_response.set_regionmap_epoch(new_regionmap->epoch());

    DINGO_LOG(INFO) << "SendCoordinatorPushToStore will send to store with response:"
                    << heartbeat_response.DebugString();
  }

  // prepare request and response
  pb::push::PushHeartbeatRequest request;
  pb::push::PushHeartbeatResponse response;

  auto* heart_response_to_send = request.mutable_heartbeat_response();
  heart_response_to_send->CopyFrom(heartbeat_response);

  // send heartbeat to all stores need to push
  for (const auto& store_pair : store_to_push) {
    const pb::common::Store& store_need_send = store_pair.second;
    const pb::common::Location& store_server_location = store_need_send.server_location();

    if (store_server_location.host().length() <= 0 || store_server_location.port() <= 0) {
      DINGO_LOG(ERROR) << "SendCoordinatorPushToStore illegal store_server_location=" << store_server_location.host()
                       << ":" << store_server_location.port();
      return;
    }

    // build send location string
    auto store_server_location_string =
        store_server_location.host() + ":" + std::to_string(store_server_location.port());

    // send rpc
    braft::PeerId remote_node(store_server_location_string);

    // rpc
    brpc::Channel channel;
    if (channel.Init(remote_node.addr, nullptr) != 0) {
      DINGO_LOG(ERROR) << "SendCoordinatorPushToStore Fail to init channel to " << remote_node;
      return;
    }

    pb::push::PushService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(500L);

    stub.PushHeartbeat(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
      DINGO_LOG(WARNING) << "SendCoordinatorPushToStore Fail to send request to : " << cntl.ErrorText();
      return;
    }

    DINGO_LOG(DEBUG) << "SendCoordinatorPushToStore to " << store_server_location_string
                     << " response latency=" << cntl.latency_us() << " msg=" << response.DebugString();
  }
}

void Heartbeat::SendStoreHeartbeat(void* arg) {
  // DINGO_LOG(INFO) << "SendStoreHeartbeat...";
  CoordinatorInteraction* coordinator_interaction = static_cast<CoordinatorInteraction*>(arg);

  auto store_control = Server::GetInstance()->GetStoreControl();
  if (!store_control->CheckNeedToHeartbeat()) {
    // DINGO_LOG(INFO) << "CheckNeedToHeartbeat is false, no need to hearbeat";
    return;
  }
  DINGO_LOG(INFO) << "CheckNeedToHeartbeat is true, start to SendStoreHeartbeat";

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
  for (const auto& store : remote_stores) {
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
    // Only state is new, create new region.
    if (remote_region.state() != pb::common::REGION_NEW) {
      continue;
    }
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
    if (remote_region.state() != pb::common::REGION_NORMAL && remote_region.state() != pb::common::REGION_EXPAND &&
        remote_region.state() != pb::common::REGION_SHRINK && remote_region.state() != pb::common::REGION_DEGRADED &&
        remote_region.state() != pb::common::REGION_DANGER) {
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
  for (const auto& region : remote_regions) {
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
  BAIDU_SCOPED_LOCK(*(store_meta->GetHeartbeatUpdateMutexRef()));

  // Handle store meta data.
  auto local_stores = store_meta->GetAllStore();
  auto remote_stores = response.storemap().stores();

  auto new_stores = GetNewStore(local_stores, remote_stores);
  DINGO_LOG(INFO) << "new store size: " << new_stores.size() << " / " << local_stores.size();
  for (const auto& store : new_stores) {
    store_meta->AddStore(store);
  }

  auto changed_stores = GetChangedStore(local_stores, remote_stores);
  DINGO_LOG(INFO) << "changed store size: " << changed_stores.size() << " / " << local_stores.size();
  for (const auto& store : changed_stores) {
    store_meta->UpdateStore(store);
  }

  auto deleted_stores = GetDeletedStore(local_stores, remote_stores);
  DINGO_LOG(INFO) << "deleted store size: " << deleted_stores.size() << " / " << local_stores.size();
  for (const auto& store : deleted_stores) {
    store_meta->DeleteStore(store->id());
  }

  // check region, if has new region, add region.
  auto store_control = Server::GetInstance()->GetStoreControl();
  auto local_regions = store_meta->GetAllRegion();
  auto remote_regions = response.regionmap().regions();

  // If has new region, add region.
  auto new_regions = GetNewRegion(local_regions, remote_regions);
  DINGO_LOG(INFO) << "new regions size: " << new_regions.size() << " / " << local_regions.size();
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  for (const auto& region : new_regions) {
    store_control->AddRegion(ctx, region);
  }

  // Check for change peers region.
  auto changed_peer_regions = GetChangedPeerRegion(local_regions, remote_regions);
  DINGO_LOG(INFO) << "change peer regions size: " << changed_peer_regions.size() << " / " << local_regions.size();
  for (auto region : changed_peer_regions) {
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    // store_control->ChangeRegion(ctx, region);
  }

  // Check for delete region.
  auto delete_regions = GetDeleteRegion(local_regions, remote_regions);
  DINGO_LOG(INFO) << "delete regions size: " << delete_regions.size() << " / " << local_regions.size();
  for (auto region : delete_regions) {
    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    // store_control->DeleteRegion(ctx, region->id());
  }
}

}  // namespace dingodb

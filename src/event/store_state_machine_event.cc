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

#include "event/store_state_machine_event.h"

#include <string_view>
#include <vector>

#include "braft/errno.pb.h"
#include "butil/endpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "handler/raft_snapshot_handler.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "server/server.h"
#include "store/heartbeat.h"

namespace dingodb {

void SmApplyEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmApplyEvent>(event);

  // Dispatch
  auto* done = dynamic_cast<StoreClosure*>(the_event->done);
  auto ctx = done ? done->GetCtx() : nullptr;
  for (const auto& req : the_event->raft_cmd->requests()) {
    auto handler = handler_collection_->GetHandler(static_cast<HandlerType>(req.cmd_type()));
    if (handler) {
      handler->Handle(ctx, the_event->region, the_event->engine, req, the_event->region_metrics, the_event->term_id,
                      the_event->log_id);
    } else {
      DINGO_LOG(ERROR) << "Unknown raft cmd type " << req.cmd_type();
    }
  }
}

void SmSnapshotSaveEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmSnapshotSaveEvent>(event);

  if (handler_) {
    handler_->Handle(the_event->node_id, the_event->engine, the_event->writer, the_event->done);
  }
}

void SmSnapshotLoadEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmSnapshotLoadEvent>(event);

  if (handler_) {
    handler_->Handle(the_event->node_id, the_event->engine, the_event->reader);
  }
}

void SmLeaderStartEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmLeaderStartEvent>(event);

  // Update region meta
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  if (store_region_meta) {
    store_region_meta->UpdateLeaderId(the_event->node_id, Server::GetInstance()->Id());
  }

  // trigger heartbeat
  Heartbeat::TriggerStoreHeartbeat(the_event->node_id);
}

void SmConfigurationCommittedEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmConfigurationCommittedEvent>(event);

  // Update region definition peers
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  if (store_region_meta == nullptr) {
    return;
  }
  auto region = store_region_meta->GetRegion(the_event->node_id);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("region {} is null", the_event->node_id);
    return;
  }
  const auto& old_peers = region->Peers();

  // Get last peer from braft configuration.
  std::vector<braft::PeerId> new_peers;
  the_event->conf.list_peers(&new_peers);

  // Check and get changed peers.
  auto get_changed_peers = [new_peers, old_peers](std::vector<pb::common::Peer>& changed_peers) -> bool {
    bool has_new_peer = false;
    for (const auto& peer_id : new_peers) {
      bool is_exist = false;
      for (const auto& pb_peer : old_peers) {
        if (std::string_view(pb_peer.raft_location().host()) ==
                std::string_view(butil::ip2str(peer_id.addr.ip).c_str()) &&
            pb_peer.raft_location().port() == peer_id.addr.port) {
          is_exist = true;
          changed_peers.push_back(pb_peer);
        }
      }

      // Not exist, get peer info used by api.
      if (!is_exist) {
        has_new_peer = true;
        changed_peers.push_back(Helper::GetPeerInfo(peer_id.addr));
      }
    }

    return has_new_peer || changed_peers.size() != old_peers.size();
  };

  std::vector<pb::common::Peer> changed_peers;
  if (get_changed_peers(changed_peers)) {
    DINGO_LOG(DEBUG) << "Peers have changed, update region definition peer";
    Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->UpdatePeers(region, changed_peers);
    // Notify coordinator
    Heartbeat::TriggerStoreHeartbeat(the_event->node_id);
  }
}

void SmStartFollowingEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmStartFollowingEvent>(event);
  // Update region meta
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  if (store_region_meta) {
    store_region_meta->UpdateLeaderId(the_event->node_id, 0);
  }
}

void SmStopFollowingEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmStopFollowingEvent>(event);
}

std::shared_ptr<EventListenerCollection> StoreSmEventListenerFactory::Build() {
  auto listener_collection = std::make_shared<EventListenerCollection>();

  auto handler_factory = std::make_shared<RaftApplyHandlerFactory>();
  listener_collection->Register(std::make_shared<SmApplyEventListener>(handler_factory->Build()));

  listener_collection->Register(std::make_shared<SmShutdownEventListener>());
  listener_collection->Register(
      std::make_shared<SmSnapshotSaveEventListener>(std::make_shared<RaftSaveSnapshotHanler>()));
  listener_collection->Register(
      std::make_shared<SmSnapshotLoadEventListener>(std::make_shared<RaftLoadSnapshotHanler>()));
  listener_collection->Register(std::make_shared<SmLeaderStartEventListener>());
  listener_collection->Register(std::make_shared<SmLeaderStopEventListener>());
  listener_collection->Register(std::make_shared<SmErrorEventListener>());
  listener_collection->Register(std::make_shared<SmConfigurationCommittedEventListener>());
  listener_collection->Register(std::make_shared<SmStartFollowingEventListener>());
  listener_collection->Register(std::make_shared<SmStopFollowingEventListener>());

  return listener_collection;
}

}  // namespace dingodb
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

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "butil/endpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config_helper.h"
#include "fmt/core.h"
#include "handler/raft_snapshot_handler.h"
#include "handler/raft_vote_handler.h"
#include "proto/common.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
#include "store/heartbeat.h"

namespace dingodb {

int SmApplyEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmApplyEvent>(event);

  // Dispatch
  auto* done = dynamic_cast<BaseClosure*>(the_event->done);
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

  return 0;
}

int SmSnapshotSaveEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmSnapshotSaveEvent>(event);

  if (handler_) {
    handler_->Handle(the_event->region, the_event->engine, the_event->term, the_event->log_index, the_event->writer,
                     the_event->done);
  }

  return 0;
}

int SmSnapshotLoadEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmSnapshotLoadEvent>(event);

  if (handler_) {
    int ret = handler_->Handle(the_event->region, the_event->engine, the_event->reader);
    if (ret != 0) {
      return ret;
    }
  }

  return 0;
}

int SmLeaderStartEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmLeaderStartEvent>(event);
  auto region = the_event->region;

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  auto node = raft_store_engine->GetNode(region->Id());
  if (node != nullptr) {
    uint32_t election_timeout_ms = ConfigHelper::GetElectionTimeout() * 1000;
    if (node->ElectionTimeout() != election_timeout_ms) {
      node->ResetElectionTimeout(election_timeout_ms, 1000);
    }
  }

  auto store_region_meta = GET_STORE_REGION_META;
  // Update region meta
  if (store_region_meta != nullptr) {
    store_region_meta->UpdateLeaderId(region, Server::GetInstance().Id());
  }

  // trigger heartbeat
  Heartbeat::TriggerStoreHeartbeat({region->Id()});

  // Invoke handler
  auto handlers = handler_collection_->GetHandlers();
  for (auto& handle : handlers) {
    handle->Handle(the_event->region, the_event->term);
  }

  return 0;
}

int SmLeaderStopEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmLeaderStopEvent>(event);

  // Invoke handler
  auto handlers = handler_collection_->GetHandlers();
  for (auto& handle : handlers) {
    handle->Handle(the_event->region, the_event->status);
  }

  return 0;
}

int SmConfigurationCommittedEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmConfigurationCommittedEvent>(event);

  // Update region definition peers
  auto store_region_meta = GET_STORE_REGION_META;
  if (store_region_meta == nullptr) {
    return 0;
  }
  auto region = store_region_meta->GetRegion(the_event->node_id);
  if (region == nullptr) {
    return 0;
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
    DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] peers have changed, peers({})", region->Id(),
                                   Helper::PeersToString(changed_peers));
    region->SetPeers(changed_peers);
    store_region_meta->UpdateEpochConfVersion(region, region->Epoch().conf_version());
    // Notify coordinator
    Heartbeat::TriggerStoreHeartbeat({the_event->node_id});
  }

  return 0;
}

int SmStartFollowingEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmStartFollowingEvent>(event);
  auto region = the_event->region;

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  auto node = raft_store_engine->GetNode(region->Id());
  if (node != nullptr) {
    uint32_t election_timeout_ms = ConfigHelper::GetElectionTimeout() * 1000;
    if (node->ElectionTimeout() != election_timeout_ms) {
      node->ResetElectionTimeout(election_timeout_ms, 1000);
    }
  }

  auto store_region_meta = GET_STORE_REGION_META;
  // Update region meta
  if (store_region_meta != nullptr) {
    store_region_meta->UpdateLeaderId(region, 0);
  }

  // Invoke handler
  auto handlers = handler_collection_->GetHandlers();
  for (auto& handle : handlers) {
    handle->Handle(the_event->region, the_event->ctx);
  }

  return 0;
}

int SmStopFollowingEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmStopFollowingEvent>(event);

  // Invoke handler
  auto handlers = handler_collection_->GetHandlers();
  for (auto& handle : handlers) {
    handle->Handle(the_event->region, the_event->ctx);
  }

  return 0;
}

std::shared_ptr<EventListenerCollection> StoreSmEventListenerFactory::Build() {
  auto listener_collection = std::make_shared<EventListenerCollection>();

  std::shared_ptr<HandlerFactory> handler_factory;
  handler_factory = std::make_shared<RaftApplyHandlerFactory>();
  listener_collection->Register(std::make_shared<SmApplyEventListener>(handler_factory->Build()));

  listener_collection->Register(std::make_shared<SmShutdownEventListener>());
  listener_collection->Register(
      std::make_shared<SmSnapshotSaveEventListener>(std::make_shared<RaftSaveSnapshotHandler>()));
  listener_collection->Register(
      std::make_shared<SmSnapshotLoadEventListener>(std::make_shared<RaftLoadSnapshotHandler>()));

  handler_factory = std::make_shared<LeaderStartHandlerFactory>();
  listener_collection->Register(std::make_shared<SmLeaderStartEventListener>(handler_factory->Build()));
  handler_factory = std::make_shared<LeaderStopHandlerFactory>();
  listener_collection->Register(std::make_shared<SmLeaderStopEventListener>(handler_factory->Build()));

  listener_collection->Register(std::make_shared<SmErrorEventListener>());
  listener_collection->Register(std::make_shared<SmConfigurationCommittedEventListener>());

  handler_factory = std::make_shared<FollowerStartHandlerFactory>();
  listener_collection->Register(std::make_shared<SmStartFollowingEventListener>(handler_factory->Build()));
  handler_factory = std::make_shared<FollowerStopHandlerFactory>();
  listener_collection->Register(std::make_shared<SmStopFollowingEventListener>(handler_factory->Build()));

  return listener_collection;
}

}  // namespace dingodb
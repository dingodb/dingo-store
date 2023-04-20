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

#include "handler/raft_snapshot_handler.h"
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
      handler->Handle(ctx, the_event->region, the_event->engine, req);
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
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  if (store_meta_manager != nullptr) {
    auto store_region_meta = store_meta_manager->GetStoreRegionMeta();
    if (store_region_meta) {
      store_region_meta->UpdateLeaderId(the_event->node_id, Server::GetInstance()->Id());
    }

    // trigger heartbeat
    Heartbeat::TriggerStoreHeartbeat(nullptr);
  }
}

void SmStartFollowingEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmStartFollowingEvent>(event);
  // Update region meta
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  if (store_meta_manager != nullptr) {
    auto store_region_meta = store_meta_manager->GetStoreRegionMeta();
    if (store_region_meta) {
      store_region_meta->UpdateLeaderId(the_event->node_id, 0);
    }
  }
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
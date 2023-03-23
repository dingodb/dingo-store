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

namespace dingodb {

void SmApplyEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmApplyEvent>(event);

  // Dispatch
  auto done = dynamic_cast<StoreClosure*>(the_event->done_);
  auto ctx = done ? done->GetCtx() : nullptr;
  for (const auto& req : the_event->raft_cmd_->requests()) {
    auto handler = handler_collection_->GetHandler(static_cast<HandlerType>(req.cmd_type()));
    if (handler) {
      handler->Handle(ctx, the_event->engine_, req);
    } else {
      LOG(ERROR) << "Unknown raft cmd type " << req.cmd_type();
    }
  }
}

void SmLeaderStartEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmLeaderStartEvent>(event);

  // Update region meta
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  auto region = store_meta_manager->GetRegion(the_event->node_id_);
  region->set_leader_store_id(Server::GetInstance()->Id());
  region->set_state(pb::common::REGION_NORMAL);

  // trigger heartbeat
  auto store_control = Server::GetInstance()->GetStoreControl();
  store_control->TriggerHeartbeat();
}

void SmStartFollowingEventListener::OnEvent(std::shared_ptr<Event> event) {
  auto the_event = std::dynamic_pointer_cast<SmStartFollowingEvent>(event);
  // Update region meta
  auto store_meta_manager = Server::GetInstance()->GetStoreMetaManager();
  auto region = store_meta_manager->GetRegion(the_event->node_id_);
  region->set_leader_store_id(0);
  region->set_state(pb::common::REGION_NORMAL);
}

std::shared_ptr<EventListenerCollection> StoreSmEventListenerFactory::Build() {
  auto listener_collection_ = std::make_shared<EventListenerCollection>();

  auto handlerFactory = std::make_shared<RaftApplyHandlerFactory>();
  listener_collection_->Register(std::make_shared<SmApplyEventListener>(handlerFactory->Build()));

  listener_collection_->Register(std::make_shared<SmShutdownEventListener>());
  listener_collection_->Register(std::make_shared<SmSnapshotSaveEventListener>());
  listener_collection_->Register(std::make_shared<SmSnapshotLoadEventListener>());
  listener_collection_->Register(std::make_shared<SmLeaderStartEventListener>());
  listener_collection_->Register(std::make_shared<SmLeaderStopEventListener>());
  listener_collection_->Register(std::make_shared<SmErrorEventListener>());
  listener_collection_->Register(std::make_shared<SmConfigurationCommittedEventListener>());
  listener_collection_->Register(std::make_shared<SmStartFollowingEventListener>());
  listener_collection_->Register(std::make_shared<SmStopFollowingEventListener>());

  return listener_collection_;
}
}  // namespace dingodb
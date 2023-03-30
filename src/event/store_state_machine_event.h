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

#ifndef DINGODB_EVENT_STATE_MACHINE_EVENT_H_
#define DINGODB_EVENT_STATE_MACHINE_EVENT_H_

#include "event/event.h"
#include "handler/handler.h"
#include "handler/raft_handler.h"
#include "raft/store_state_machine.h"
#include "server/server.h"

namespace dingodb {

// State Machine apply event
struct SmApplyEvent : public Event {
  SmApplyEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_APPLY) {}
  ~SmApplyEvent() override = default;

  std::shared_ptr<RawEngine> engine;
  braft::Closure* done;
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd;
};

class SmApplyEventListener : public EventListener {
 public:
  SmApplyEventListener(std::shared_ptr<HandlerCollection> handler_collection)
      : handler_collection_(handler_collection) {}
  ~SmApplyEventListener() override = default;

  EventType GetType() override { return EventType::SM_APPLY; }
  void OnEvent(std::shared_ptr<Event> event) override;

 private:
  std::shared_ptr<HandlerCollection> handler_collection_;
};

// State Machine Shutdown
struct SmShutdownEvent : public Event {
  SmShutdownEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_SHUTDOWN) {}
  ~SmShutdownEvent() override = default;
};

class SmShutdownEventListener : public EventListener {
 public:
  SmShutdownEventListener() = default;
  ~SmShutdownEventListener() override = default;

  EventType GetType() override { return EventType::SM_SHUTDOWN; }
  void OnEvent(std::shared_ptr<Event> event) override {}
};

// State Machine SnapshotSave
struct SmSnapshotSaveEvent : public Event {
  SmSnapshotSaveEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_SNAPSHOT_SAVE) {}
  ~SmSnapshotSaveEvent() override = default;

  std::shared_ptr<RawEngine> engine;
  braft::SnapshotWriter* writer;
  braft::Closure* done;
  uint64_t node_id;
};

class SmSnapshotSaveEventListener : public EventListener {
 public:
  SmSnapshotSaveEventListener(std::shared_ptr<Handler> handler) : handler_(handler) {}
  ~SmSnapshotSaveEventListener() override = default;

  EventType GetType() override { return EventType::SM_SNAPSHOT_SAVE; }
  void OnEvent(std::shared_ptr<Event> event) override;

 private:
  std::shared_ptr<Handler> handler_;
};

// State Machine SnapshotLoad
struct SmSnapshotLoadEvent : public Event {
  SmSnapshotLoadEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_SNAPSHOT_LOAD) {}
  ~SmSnapshotLoadEvent() override = default;

  std::shared_ptr<RawEngine> engine;
  braft::SnapshotReader* reader;
  uint64_t node_id;
};

class SmSnapshotLoadEventListener : public EventListener {
 public:
  SmSnapshotLoadEventListener(std::shared_ptr<Handler> handler) : handler_(handler) {}
  ~SmSnapshotLoadEventListener() override = default;

  EventType GetType() override { return EventType::SM_SNAPSHOT_LOAD; }
  void OnEvent(std::shared_ptr<Event> event) override;

 private:
  std::shared_ptr<Handler> handler_;
};

// State Machine LeaderStart
struct SmLeaderStartEvent : public Event {
  SmLeaderStartEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_LEADER_START) {}
  ~SmLeaderStartEvent() override = default;

  int64_t term;
  int64_t node_id;
};

class SmLeaderStartEventListener : public EventListener {
 public:
  SmLeaderStartEventListener() = default;
  ~SmLeaderStartEventListener() override = default;

  EventType GetType() override { return EventType::SM_LEADER_START; }
  void OnEvent(std::shared_ptr<Event> event) override;
};

// State Machine LeaderStop
struct SmLeaderStopEvent : public Event {
  SmLeaderStopEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_LEADER_STOP) {}
  ~SmLeaderStopEvent() override = default;

  butil::Status status;
};

class SmLeaderStopEventListener : public EventListener {
 public:
  SmLeaderStopEventListener() = default;
  ~SmLeaderStopEventListener() override = default;

  EventType GetType() override { return EventType::SM_LEADER_STOP; }
  void OnEvent(std::shared_ptr<Event> event) override {}
};

// State Machine Error
struct SmErrorEvent : public Event {
  SmErrorEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_ERROR) {}
  ~SmErrorEvent() override = default;

  braft::Error e;
};

class SmErrorEventListener : public EventListener {
 public:
  SmErrorEventListener() = default;
  ~SmErrorEventListener() override = default;

  EventType GetType() override { return EventType::SM_ERROR; }
  void OnEvent(std::shared_ptr<Event> event) override {}
};

// State Machine ConfigurationCommitted
struct SmConfigurationCommittedEvent : public Event {
  SmConfigurationCommittedEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_CONFIGURATION_COMMITTED) {}
  ~SmConfigurationCommittedEvent() override = default;

  braft::Configuration conf;
};

class SmConfigurationCommittedEventListener : public EventListener {
 public:
  SmConfigurationCommittedEventListener() = default;
  ~SmConfigurationCommittedEventListener() override = default;

  EventType GetType() override { return EventType::SM_CONFIGURATION_COMMITTED; }
  void OnEvent(std::shared_ptr<Event> event) override {}
};

// State Machine StartFollowing
struct SmStartFollowingEvent : public Event {
  SmStartFollowingEvent(const braft::LeaderChangeContext& ctx)
      : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_START_FOLLOWING), ctx(ctx) {}
  ~SmStartFollowingEvent() override = default;

  const braft::LeaderChangeContext& ctx;
  uint64_t node_id;
};

class SmStartFollowingEventListener : public EventListener {
 public:
  SmStartFollowingEventListener() = default;
  ~SmStartFollowingEventListener() override = default;

  EventType GetType() override { return EventType::SM_START_FOLLOWING; }
  void OnEvent(std::shared_ptr<Event> event) override;
};

// State Machine StopFollowing
struct SmStopFollowingEvent : public Event {
  SmStopFollowingEvent(const braft::LeaderChangeContext& ctx)
      : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_STOP_FOLLOWING), ctx(ctx) {}
  ~SmStopFollowingEvent() override = default;

  const braft::LeaderChangeContext& ctx;
};

class SmStopFollowingEventListener : public EventListener {
 public:
  SmStopFollowingEventListener() = default;
  ~SmStopFollowingEventListener() override = default;

  EventType GetType() override { return EventType::SM_STOP_FOLLOWING; }
  void OnEvent(std::shared_ptr<Event> event) override {}
};

class StoreSmEventListenerFactory : public EventListenerFactory {
 public:
  StoreSmEventListenerFactory() = default;
  ~StoreSmEventListenerFactory() override = default;

  std::shared_ptr<EventListenerCollection> Build() override;
};

}  // namespace dingodb

#endif  // DINGODB_EVENT_STATE_MACHINE_EVENT_H_
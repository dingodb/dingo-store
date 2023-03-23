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
  ~SmApplyEvent() = default;

  std::shared_ptr<RawEngine> engine_;
  braft::Closure* done_;
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd_;
};

class SmApplyEventListener : public EventListener {
 public:
  SmApplyEventListener(std::shared_ptr<HandlerCollection> handler_collection)
      : EventListener(), handler_collection_(handler_collection) {}
  ~SmApplyEventListener() = default;

  EventType GetType() { return EventType::SM_APPLY; }
  void OnEvent(std::shared_ptr<Event> event) override;

 private:
  std::shared_ptr<HandlerCollection> handler_collection_;
};

// State Machine Shutdown
struct SmShutdownEvent : public Event {
  SmShutdownEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_SHUTDOWN) {}
  ~SmShutdownEvent() = default;
};

class SmShutdownEventListener : public EventListener {
 public:
  SmShutdownEventListener() = default;
  ~SmShutdownEventListener() = default;

  EventType GetType() { return EventType::SM_SHUTDOWN; }
  void OnEvent(std::shared_ptr<Event> event) {}
};

// State Machine SnapshotSave
struct SmSnapshotSaveEvent : public Event {
  SmSnapshotSaveEvent(braft::SnapshotWriter* writer, braft::Closure* done)
      : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_SNAPSHOT_SAVE), writer_(writer), done_(done) {}
  ~SmSnapshotSaveEvent() = default;

  braft::SnapshotWriter* writer_;
  braft::Closure* done_;
};

class SmSnapshotSaveEventListener : public EventListener {
 public:
  SmSnapshotSaveEventListener() = default;
  ~SmSnapshotSaveEventListener() = default;

  EventType GetType() { return EventType::SM_SNAPSHOT_SAVE; }
  void OnEvent(std::shared_ptr<Event> event) {}
};

// State Machine SnapshotLoad
struct SmSnapshotLoadEvent : public Event {
  SmSnapshotLoadEvent(braft::SnapshotReader* reader)
      : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_SNAPSHOT_LOAD), reader_(reader) {}
  ~SmSnapshotLoadEvent() = default;

  braft::SnapshotReader* reader_;
};

class SmSnapshotLoadEventListener : public EventListener {
 public:
  SmSnapshotLoadEventListener() = default;
  ~SmSnapshotLoadEventListener() = default;

  EventType GetType() { return EventType::SM_SNAPSHOT_LOAD; }
  void OnEvent(std::shared_ptr<Event> event) {}
};

// State Machine LeaderStart
struct SmLeaderStartEvent : public Event {
  SmLeaderStartEvent() : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_LEADER_START) {}
  ~SmLeaderStartEvent() = default;

  int64_t term_;
  int64_t node_id_;
};

class SmLeaderStartEventListener : public EventListener {
 public:
  SmLeaderStartEventListener() = default;
  ~SmLeaderStartEventListener() = default;

  EventType GetType() { return EventType::SM_LEADER_START; }
  void OnEvent(std::shared_ptr<Event> event) override;
};

// State Machine LeaderStop
struct SmLeaderStopEvent : public Event {
  SmLeaderStopEvent(const butil::Status& status)
      : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_LEADER_STOP), status_(status) {}
  ~SmLeaderStopEvent() = default;

  const butil::Status& status_;
};

class SmLeaderStopEventListener : public EventListener {
 public:
  SmLeaderStopEventListener() = default;
  ~SmLeaderStopEventListener() = default;

  EventType GetType() { return EventType::SM_LEADER_STOP; }
  void OnEvent(std::shared_ptr<Event> event) {}
};

// State Machine Error
struct SmErrorEvent : public Event {
  SmErrorEvent(const braft::Error& e) : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_ERROR), e_(e) {}
  ~SmErrorEvent() = default;

  const braft::Error& e_;
};

class SmErrorEventListener : public EventListener {
 public:
  SmErrorEventListener() = default;
  ~SmErrorEventListener() = default;

  EventType GetType() { return EventType::SM_ERROR; }
  void OnEvent(std::shared_ptr<Event> event) {}
};

// State Machine ConfigurationCommitted
struct SmConfigurationCommittedEvent : public Event {
  SmConfigurationCommittedEvent(const braft::Configuration& conf)
      : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_CONFIGURATION_COMMITTED), conf_(conf) {}
  ~SmConfigurationCommittedEvent() = default;

  const braft::Configuration& conf_;
};

class SmConfigurationCommittedEventListener : public EventListener {
 public:
  SmConfigurationCommittedEventListener() = default;
  ~SmConfigurationCommittedEventListener() = default;

  EventType GetType() { return EventType::SM_CONFIGURATION_COMMITTED; }
  void OnEvent(std::shared_ptr<Event> event) {}
};

// State Machine StartFollowing
struct SmStartFollowingEvent : public Event {
  SmStartFollowingEvent(const braft::LeaderChangeContext& ctx)
      : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_START_FOLLOWING), ctx_(ctx) {}
  ~SmStartFollowingEvent() = default;

  const braft::LeaderChangeContext& ctx_;
  uint64_t node_id_;
};

class SmStartFollowingEventListener : public EventListener {
 public:
  SmStartFollowingEventListener() = default;
  ~SmStartFollowingEventListener() = default;

  EventType GetType() { return EventType::SM_START_FOLLOWING; }
  void OnEvent(std::shared_ptr<Event> event) override;
};

// State Machine StopFollowing
struct SmStopFollowingEvent : public Event {
  SmStopFollowingEvent(const braft::LeaderChangeContext& ctx)
      : Event(EventSource::RAFT_STATE_MACHINE, EventType::SM_STOP_FOLLOWING), ctx_(ctx) {}
  ~SmStopFollowingEvent() = default;

  const braft::LeaderChangeContext& ctx_;
};

class SmStopFollowingEventListener : public EventListener {
 public:
  SmStopFollowingEventListener() = default;
  ~SmStopFollowingEventListener() = default;

  EventType GetType() { return EventType::SM_STOP_FOLLOWING; }
  void OnEvent(std::shared_ptr<Event> event) {}
};

class StoreSmEventListenerFactory : public EventListenerFactory {
 public:
  StoreSmEventListenerFactory() = default;
  ~StoreSmEventListenerFactory() = default;

  std::shared_ptr<EventListenerCollection> Build();
};

}  // namespace dingodb

#endif  // DINGODB_EVENT_STATE_MACHINE_EVENT_H_
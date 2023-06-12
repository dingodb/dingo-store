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

#include <cstdint>

#include "event/event.h"
#include "handler/handler.h"
#include "handler/raft_handler.h"
#include "metrics/store_metrics_manager.h"
#include "raft/store_state_machine.h"
#include "server/server.h"

namespace dingodb {

// State Machine apply event
struct SmApplyEvent : public Event {
  SmApplyEvent() : Event(EventSource::kRaftStateMachine, EventType::kSmApply) {}
  ~SmApplyEvent() override = default;

  store::RegionPtr region;
  store::RegionMetricsPtr region_metrics;
  std::shared_ptr<RawEngine> engine;
  braft::Closure* done;
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd;
  uint64_t term_id;
  uint64_t log_id;
};

class SmApplyEventListener : public EventListener {
 public:
  SmApplyEventListener(std::shared_ptr<HandlerCollection> handler_collection)
      : handler_collection_(handler_collection) {}
  ~SmApplyEventListener() override = default;

  EventType GetType() override { return EventType::kSmApply; }
  void OnEvent(std::shared_ptr<Event> event) override;

 private:
  std::shared_ptr<HandlerCollection> handler_collection_;
};

// State Machine Shutdown
struct SmShutdownEvent : public Event {
  SmShutdownEvent() : Event(EventSource::kRaftStateMachine, EventType::kSmShutdown) {}
  ~SmShutdownEvent() override = default;
};

class SmShutdownEventListener : public EventListener {
 public:
  SmShutdownEventListener() = default;
  ~SmShutdownEventListener() override = default;

  EventType GetType() override { return EventType::kSmShutdown; }
  void OnEvent(std::shared_ptr<Event> event) override {}
};

// State Machine SnapshotSave
struct SmSnapshotSaveEvent : public Event {
  SmSnapshotSaveEvent() : Event(EventSource::kRaftStateMachine, EventType::kSmSnapshotSave) {}
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

  EventType GetType() override { return EventType::kSmSnapshotSave; }
  void OnEvent(std::shared_ptr<Event> event) override;

 private:
  std::shared_ptr<Handler> handler_;
};

// State Machine SnapshotLoad
struct SmSnapshotLoadEvent : public Event {
  SmSnapshotLoadEvent() : Event(EventSource::kRaftStateMachine, EventType::kSmSnapshotLoad) {}
  ~SmSnapshotLoadEvent() override = default;

  std::shared_ptr<RawEngine> engine;
  braft::SnapshotReader* reader;
  uint64_t node_id;
};

class SmSnapshotLoadEventListener : public EventListener {
 public:
  SmSnapshotLoadEventListener(std::shared_ptr<Handler> handler) : handler_(handler) {}
  ~SmSnapshotLoadEventListener() override = default;

  EventType GetType() override { return EventType::kSmSnapshotLoad; }
  void OnEvent(std::shared_ptr<Event> event) override;

 private:
  std::shared_ptr<Handler> handler_;
};

// State Machine LeaderStart
struct SmLeaderStartEvent : public Event {
  SmLeaderStartEvent() : Event(EventSource::kRaftStateMachine, EventType::kSmLeaderStart) {}
  ~SmLeaderStartEvent() override = default;

  int64_t term;
  int64_t node_id;
};

class SmLeaderStartEventListener : public EventListener {
 public:
  SmLeaderStartEventListener() = default;
  ~SmLeaderStartEventListener() override = default;

  EventType GetType() override { return EventType::kSmLeaderStart; }
  void OnEvent(std::shared_ptr<Event> event) override;
};

// State Machine LeaderStop
struct SmLeaderStopEvent : public Event {
  SmLeaderStopEvent() : Event(EventSource::kRaftStateMachine, EventType::kSmLeaderStop) {}
  ~SmLeaderStopEvent() override = default;

  butil::Status status;
};

class SmLeaderStopEventListener : public EventListener {
 public:
  SmLeaderStopEventListener() = default;
  ~SmLeaderStopEventListener() override = default;

  EventType GetType() override { return EventType::kSmLeaderStop; }
  void OnEvent(std::shared_ptr<Event> event) override {}
};

// State Machine Error
struct SmErrorEvent : public Event {
  SmErrorEvent() : Event(EventSource::kRaftStateMachine, EventType::kSmError) {}
  ~SmErrorEvent() override = default;

  braft::Error e;
};

class SmErrorEventListener : public EventListener {
 public:
  SmErrorEventListener() = default;
  ~SmErrorEventListener() override = default;

  EventType GetType() override { return EventType::kSmError; }
  void OnEvent(std::shared_ptr<Event> event) override {}
};

// State Machine ConfigurationCommitted
struct SmConfigurationCommittedEvent : public Event {
  SmConfigurationCommittedEvent() : Event(EventSource::kRaftStateMachine, EventType::kSmConfigurationCommited) {}
  ~SmConfigurationCommittedEvent() override = default;

  uint64_t node_id;
  braft::Configuration conf;
};

class SmConfigurationCommittedEventListener : public EventListener {
 public:
  SmConfigurationCommittedEventListener() = default;
  ~SmConfigurationCommittedEventListener() override = default;

  EventType GetType() override { return EventType::kSmConfigurationCommited; }
  void OnEvent(std::shared_ptr<Event> event) override;
};

// State Machine StartFollowing
struct SmStartFollowingEvent : public Event {
  SmStartFollowingEvent(const braft::LeaderChangeContext& ctx)
      : Event(EventSource::kRaftStateMachine, EventType::kSmStartFollowing), ctx(ctx) {}
  ~SmStartFollowingEvent() override = default;

  const braft::LeaderChangeContext& ctx;
  uint64_t node_id;
};

class SmStartFollowingEventListener : public EventListener {
 public:
  SmStartFollowingEventListener() = default;
  ~SmStartFollowingEventListener() override = default;

  EventType GetType() override { return EventType::kSmStartFollowing; }
  void OnEvent(std::shared_ptr<Event> event) override;
};

// State Machine StopFollowing
struct SmStopFollowingEvent : public Event {
  SmStopFollowingEvent(const braft::LeaderChangeContext& ctx)
      : Event(EventSource::kRaftStateMachine, EventType::kSmStopFollowing), ctx(ctx) {}
  ~SmStopFollowingEvent() override = default;

  const braft::LeaderChangeContext& ctx;
  uint64_t node_id;
};

class SmStopFollowingEventListener : public EventListener {
 public:
  SmStopFollowingEventListener() = default;
  ~SmStopFollowingEventListener() override = default;

  EventType GetType() override { return EventType::kSmStopFollowing; }
  void OnEvent(std::shared_ptr<Event> event) override;
};

class StoreSmEventListenerFactory : public EventListenerFactory {
 public:
  StoreSmEventListenerFactory() = default;
  ~StoreSmEventListenerFactory() override = default;

  std::shared_ptr<EventListenerCollection> Build() override;
};

}  // namespace dingodb

#endif  // DINGODB_EVENT_STATE_MACHINE_EVENT_H_
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

#ifndef DINGODB_EVENT_EVENT_H_
#define DINGODB_EVENT_EVENT_H_

#include "braft/raft.h"
#include "brpc/controller.h"
#include "common/helper.h"

namespace dingodb {

//                                  Event model
//
//                                          ┌────────────────┐
//                                ┌────────▶│ Event listener │
//                                │         └────────────────┘           ┌────────────┐
//                                │                               ┌─────▶│   Handler  │
//                                │                               │      └────────────┘
//                                │                               │
// ┌─────────────────────┐ deliver event    ┌────────────────┐    │      ┌────────────┐
// │    Event source     ├────────┼────────▶│ Event listener ├────┼─────▶│   Handler  │
// └─────────────────────┘        │         └────────────────┘    │      └────────────┘
//                                │                               │
//                                │                               │      ┌────────────┐
//                                │                               └─────▶│   Handler  │
//                                │         ┌────────────────┐           └────────────┘
//                                └────────▶│ Event listener │
//                                          └────────────────┘

// Event source, like raft state machine.
enum class EventSource {
  kRaftStateMachine,
};

// Event type
enum class EventType {
  // Raft state machine event.
  kSmApply,
  kSmShutdown,
  kSmSnapshotSave,
  kSmSnapshotLoad,
  kSmLeaderStart,
  kSmLeaderStop,
  kSmError,
  kSmConfigurationCommited,
  kSmStartFollowing,
  kSmStopFollowing,
};

// Event abstract class.
struct Event {
  Event(EventSource source, EventType type) : id(Helper::GenId()), source(source), type(type) {}
  virtual ~Event() = default;

  int64_t GetID() const { return id; }
  EventSource GetSource() const { return source; }
  EventType GetType() const { return type; }

  int64_t id;
  EventSource source;
  EventType type;
};

// Event listerner abstract class.
class EventListener {
 public:
  EventListener() : id_(Helper::GenId()) {}
  virtual ~EventListener() = default;
  EventListener(const EventListener &) = delete;
  const EventListener &operator=(const EventListener &) = delete;

  int64_t GetID() const { return id_; }
  virtual EventType GetType() = 0;
  virtual int OnEvent(std::shared_ptr<Event> event) = 0;

 private:
  int64_t id_;
};

// Event listener collection
class EventListenerCollection {
 public:
  using EventListenerChain = std::vector<std::shared_ptr<EventListener>>;

  EventListenerCollection() = default;
  ~EventListenerCollection() = default;
  EventListenerCollection(const EventListenerCollection &) = delete;
  const EventListenerCollection &operator=(const EventListenerCollection &) = delete;

  void Register(std::shared_ptr<EventListener> listener);
  EventListenerChain Get(EventType type);

 private:
  std::unordered_map<EventType, EventListenerChain> listeners_;
};

// Event listerner abstract factory.
class EventListenerFactory {
 public:
  EventListenerFactory() = default;
  virtual ~EventListenerFactory() = default;
  EventListenerFactory(const EventListenerFactory &) = delete;
  const EventListenerFactory &operator=(const EventListenerFactory &) = delete;

  virtual std::shared_ptr<EventListenerCollection> Build() = 0;
};

}  // namespace dingodb

#endif  // DINGODB_EVENT_EVENT_H_
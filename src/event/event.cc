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

#include "event/event.h"

namespace dingodb {

void EventListenerCollection::Register(std::shared_ptr<EventListener> listener) {
  auto it = listeners_.find(listener->GetType());
  if (it == listeners_.end()) {
    EventListenerChain chain;
    listeners_.insert(std::make_pair(listener->GetType(), chain));
  }
  listeners_[listener->GetType()].push_back(listener);
}

EventListenerCollection::EventListenerChain EventListenerCollection::Get(EventType type) {
  auto it = listeners_.find(type);
  if (it == listeners_.end()) {
    return {};
  }

  return it->second;
}

}  // namespace dingodb
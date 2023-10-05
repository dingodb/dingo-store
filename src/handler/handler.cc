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

#include "handler/handler.h"

#include "common/logging.h"

namespace dingodb {

void HandlerCollection::Register(std::shared_ptr<Handler> handler) {
  if (handlers_.find(handler->GetType()) != handlers_.end()) {
    DINGO_LOG(ERROR) << "Handler already exist " << static_cast<int>(handler->GetType());
    return;
  }

  handlers_.insert(std::make_pair(handler->GetType(), handler));
}

std::shared_ptr<Handler> HandlerCollection::GetHandler(HandlerType type) {
  auto it = handlers_.find(type);
  if (it == handlers_.end()) {
    return nullptr;
  }
  return it->second;
}

std::vector<std::shared_ptr<Handler>> HandlerCollection::GetHandlers() {
  std::vector<std::shared_ptr<Handler>> handlers;
  handlers.reserve(handlers_.size());
  for (auto [_, handler] : handlers_) {
    handlers.push_back(handler);
  }

  return handlers;
}

}  // namespace dingodb

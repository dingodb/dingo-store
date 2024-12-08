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

#ifndef DINGODB_BR_INTERATION_MANAGER_H_
#define DINGODB_BR_INTERATION_MANAGER_H_

#include <utility>

#include "br/interation.h"
#include "butil/status.h"

namespace br {
class InteractionManager {
 public:
  static InteractionManager& GetInstance();

  void SetCoordinatorInteraction(ServerInteractionPtr interaction);
  void SetStoreInteraction(ServerInteractionPtr interaction);
  void SetIndexInteraction(ServerInteractionPtr interaction);
  void SetDocumentInteraction(ServerInteractionPtr interaction);

  ServerInteractionPtr GetCoordinatorInteraction();
  ServerInteractionPtr GetStoreInteraction();
  ServerInteractionPtr GetIndexInteraction();
  ServerInteractionPtr GetDocumentInteraction();

  std::pair<butil::Status, ServerInteractionPtr> CloneCoordinatorInteraction();
  std::pair<butil::Status, ServerInteractionPtr> CloneStoreInteraction();
  std::pair<butil::Status, ServerInteractionPtr> CloneIndexInteraction();
  std::pair<butil::Status, ServerInteractionPtr> CloneDocumentInteraction();

 private:
  static std::pair<butil::Status, ServerInteractionPtr> CreateInteraction(const std::vector<std::string>& addrs);

  InteractionManager();
  ~InteractionManager();

  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  ServerInteractionPtr index_interaction_;
  ServerInteractionPtr document_interaction_;

  bthread_mutex_t mutex_;
};

}  // namespace br

#endif  // DINGODB_BR_INTERATION_MANAGER_H_
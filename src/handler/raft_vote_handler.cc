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

#include "handler/raft_vote_handler.h"

#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

void VectorIndexLeaderStartHandler::Handle(store::RegionPtr region, uint64_t) {
  // Load vector index.
  if (region != nullptr) {
    auto vector_index_manager = Server::GetInstance()->GetVectorIndexManager();
    auto vector_index = vector_index_manager->GetVectorIndex(region->Id());
    if (vector_index != nullptr) {
      DINGO_LOG(WARNING) << fmt::format("Vector index {} already exist, don't need load again.", region->Id());
    } else {
      auto status = vector_index_manager->LoadOrBuildVectorIndex(region);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Load or build vector index failed, region {} error: {}", region->Id(),
                                        status.error_str());
      }
    }
  }
}

void VectorIndexLeaderStopHandler::Handle(store::RegionPtr region, butil::Status) {
  if (region != nullptr) {
    Server::GetInstance()->GetVectorIndexManager()->DeleteVectorIndex(region->Id());
  }
}

std::shared_ptr<HandlerCollection> LeaderStartHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  if (Server::GetInstance()->GetRole() == pb::common::INDEX) {
    handler_collection->Register(std::make_shared<VectorIndexLeaderStartHandler>());
  }

  return handler_collection;
}

std::shared_ptr<HandlerCollection> LeaderStopHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  if (Server::GetInstance()->GetRole() == pb::common::INDEX) {
    handler_collection->Register(std::make_shared<VectorIndexLeaderStopHandler>());
  }

  return handler_collection;
}

}  // namespace dingodb
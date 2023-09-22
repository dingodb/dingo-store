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
#include "vector/vector_index_snapshot_manager.h"

namespace dingodb {

int VectorIndexLeaderStartHandler::Handle(store::RegionPtr region, uint64_t) {
  if (region == nullptr) {
    return 0;
  }

  // Load vector index.
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper->IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("Vector index {} already exist, don't need load again.", region->Id());
  } else {
    auto snapshot_set = vector_index_wrapper->SnapshotSet();
    auto status = VectorIndexSnapshotManager::PullLastSnapshotFromPeers(snapshot_set);
    if (!status.ok()) {
      if (status.error_code() != pb::error::EVECTOR_SNAPSHOT_EXIST) {
        DINGO_LOG(ERROR) << fmt::format("Pull vector index {} last snapshot failed, error: {}", region->Id(),
                                        status.error_str());
      }
    }

    status = VectorIndexManager::LoadOrBuildVectorIndex(vector_index_wrapper);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Load or build vector index failed, region {} error: {}", region->Id(),
                                      status.error_str());
    }
  }

  return 0;
}

int VectorIndexLeaderStopHandler::Handle(store::RegionPtr region, butil::Status) {
  auto config = Server::GetInstance()->GetConfig();
  if (config == nullptr) {
    return 0;
  }
  if (config->GetBool("vector.enable_follower_hold_index")) {
    return 0;
  }
  if (region == nullptr) {
    return 0;
  }

  if (!region->NeedBootstrapDoSnapshot()) {
    // Delete vector index.
    region->VectorIndexWrapper()->ClearVectorIndex();
  }

  return 0;
}

int VectorIndexFollowerStartHandler::Handle(store::RegionPtr region, const braft::LeaderChangeContext &) {
  auto config = Server::GetInstance()->GetConfig();
  if (config == nullptr) {
    return 0;
  }
  if (!config->GetBool("vector.enable_follower_hold_index")) {
    return 0;
  }
  if (region == nullptr) {
    return 0;
  }

  // Load vector index.
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper->IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("Vector index {} already exist, don't need load again.", region->Id());
  } else {
    auto status = VectorIndexManager::LoadOrBuildVectorIndex(vector_index_wrapper);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Load or build vector index failed, region {} error: {}", region->Id(),
                                      status.error_str());
    }
  }

  return 0;
}

int VectorIndexFollowerStopHandler::Handle(store::RegionPtr region, const braft::LeaderChangeContext &ctx) { return 0; }

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

std::shared_ptr<HandlerCollection> FollowerStartHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  if (Server::GetInstance()->GetRole() == pb::common::INDEX) {
    handler_collection->Register(std::make_shared<VectorIndexFollowerStartHandler>());
  }

  return handler_collection;
}

std::shared_ptr<HandlerCollection> FollowerStopHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  if (Server::GetInstance()->GetRole() == pb::common::INDEX) {
    handler_collection->Register(std::make_shared<VectorIndexFollowerStopHandler>());
  }

  return handler_collection;
}

}  // namespace dingodb
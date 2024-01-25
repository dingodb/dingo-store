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

#include "common/role.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

DECLARE_int64(vector_fast_build_log_gap);

int VectorIndexLeaderStartHandler::Handle(store::RegionPtr region, int64_t) {
  if (region == nullptr) {
    return 0;
  }

  // Load vector index.
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper->IsReady()) {
    DINGO_LOG(INFO) << fmt::format("[raft.handle][region({})] vector index already exist, don't need load again.",
                                   region->Id());
  } else {
    bool is_fast_load = false;

    if (region->Epoch().version() == 1) {
      auto raft_meta = Server::GetInstance().GetRaftMeta(region->Id());
      int64_t applied_index = -1;
      if (raft_meta != nullptr) {
        applied_index = raft_meta->AppliedId();

        if (applied_index < FLAGS_vector_fast_build_log_gap) {
          // use fast load
          is_fast_load = true;
        }
      }
    }

    VectorIndexManager::LaunchLoadAsyncBuildVectorIndex(vector_index_wrapper, false, is_fast_load, 0, "being leader");
  }

  return 0;
}

int VectorIndexLeaderStopHandler::Handle(store::RegionPtr region, butil::Status) {
  if (region == nullptr) {
    return 0;
  }
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[raft.handle][region({})] vector index wrapper is null.", region->Id());
    return -1;
  }
  if (vector_index_wrapper->IsPermanentHoldVectorIndex(vector_index_wrapper->Id()) ||
      vector_index_wrapper->IsTempHoldVectorIndex()) {
    return 0;
  }

  // Delete vector index.
  region->VectorIndexWrapper()->ClearVectorIndex("stop leader");

  return 0;
}

int VectorIndexFollowerStartHandler::Handle(store::RegionPtr region, const braft::LeaderChangeContext &) {
  if (region == nullptr) {
    return 0;
  }
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[raft.handle][region({})] vector index wrapper is null.", region->Id());
    return -1;
  }
  if (!vector_index_wrapper->IsPermanentHoldVectorIndex(vector_index_wrapper->Id()) &&
      !vector_index_wrapper->IsTempHoldVectorIndex()) {
    return 0;
  }

  // Load vector index.
  if (vector_index_wrapper->IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[raft.handle][region({})] vector index already exist, don't need load again.",
                                      region->Id());
  } else {
    bool is_fast_load = false;

    if (region->Epoch().version() == 1) {
      auto raft_meta = Server::GetInstance().GetRaftMeta(region->Id());
      int64_t applied_index = -1;
      if (raft_meta != nullptr) {
        applied_index = raft_meta->AppliedId();

        if (applied_index < FLAGS_vector_fast_build_log_gap) {
          // use fast load
          is_fast_load = true;
        }
      }
    }

    VectorIndexManager::LaunchLoadAsyncBuildVectorIndex(vector_index_wrapper, false, is_fast_load, 0, "being follower");
  }

  return 0;
}

int VectorIndexFollowerStopHandler::Handle(store::RegionPtr /*region*/, const braft::LeaderChangeContext & /*ctx*/) {
  return 0;
}

std::shared_ptr<HandlerCollection> LeaderStartHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  if (GetRole() == pb::common::INDEX) {
    handler_collection->Register(std::make_shared<VectorIndexLeaderStartHandler>());
  }

  return handler_collection;
}

std::shared_ptr<HandlerCollection> LeaderStopHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  if (GetRole() == pb::common::INDEX) {
    handler_collection->Register(std::make_shared<VectorIndexLeaderStopHandler>());
  }

  return handler_collection;
}

std::shared_ptr<HandlerCollection> FollowerStartHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  if (GetRole() == pb::common::INDEX) {
    handler_collection->Register(std::make_shared<VectorIndexFollowerStartHandler>());
  }

  return handler_collection;
}

std::shared_ptr<HandlerCollection> FollowerStopHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  if (GetRole() == pb::common::INDEX) {
    handler_collection->Register(std::make_shared<VectorIndexFollowerStopHandler>());
  }

  return handler_collection;
}

}  // namespace dingodb
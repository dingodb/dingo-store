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

#include "raft/raft_node_manager.h"

#include "common/logging.h"
#include "fmt/core.h"

namespace dingodb {

RaftNodeManager::RaftNodeManager() { bthread_mutex_init(&mutex_, nullptr); }

RaftNodeManager::~RaftNodeManager() { bthread_mutex_destroy(&mutex_); }

bool RaftNodeManager::IsExist(int64_t node_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return nodes_.find(node_id) != nodes_.end();
}

std::shared_ptr<RaftNode> RaftNodeManager::GetNode(int64_t node_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    DINGO_LOG(WARNING) << fmt::format("node {} not exist!", node_id);
    return nullptr;
  }

  return it->second;
}

std::vector<std::shared_ptr<RaftNode>> RaftNodeManager::GetAllNode() {
  BAIDU_SCOPED_LOCK(mutex_);

  std::vector<std::shared_ptr<RaftNode>> nodes;
  nodes.reserve(nodes_.size());
  for (auto& [_, node] : nodes_) {
    nodes.push_back(node);
  }

  return nodes;
}

void RaftNodeManager::AddNode(int64_t node_id, std::shared_ptr<RaftNode> node) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (nodes_.find(node_id) != nodes_.end()) {
    DINGO_LOG(WARNING) << fmt::format("node {} already exist!", node_id);
    return;
  }

  nodes_.insert(std::make_pair(node_id, node));
}

void RaftNodeManager::DeleteNode(int64_t node_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  nodes_.erase(node_id);
}

}  // namespace dingodb
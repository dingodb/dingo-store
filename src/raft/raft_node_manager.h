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

#ifndef DINGODB_RAFT_RAFT_NODE_MANAGER_H_
#define DINGODB_RAFT_RAFT_NODE_MANAGER_H_

#include <map>
#include <memory>
#include <shared_mutex>

#include "raft/raft_node.h"

namespace dingodb {

// raft node manager
class RaftNodeManager {
 public:
  RaftNodeManager();
  ~RaftNodeManager();

  bool IsExist(uint64_t node_id);
  std::shared_ptr<RaftNode> GetNode(uint64_t node_id);
  void AddNode(uint64_t node_id, std::shared_ptr<RaftNode> node);
  void DeleteNode(uint64_t node_id);
  RaftNodeManager(const RaftNodeManager &) = delete;
  const RaftNodeManager &operator=(const RaftNodeManager &) = delete;

 private:
  std::shared_mutex mutex_;
  std::map<uint64_t, std::shared_ptr<RaftNode> > nodes_;
};

}  // namespace dingodb

#endif  // DINGODB_RAFT_RAFT_NODE_MANAGER_H_

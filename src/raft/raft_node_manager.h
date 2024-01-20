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

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "raft/raft_node.h"

namespace dingodb {

// raft node manager
class RaftNodeManager {
 public:
  RaftNodeManager();
  ~RaftNodeManager();

  RaftNodeManager(const RaftNodeManager &) = delete;
  const RaftNodeManager &operator=(const RaftNodeManager &) = delete;

  bool IsExist(int64_t node_id);
  std::shared_ptr<RaftNode> GetNode(int64_t node_id);
  std::vector<std::shared_ptr<RaftNode>> GetAllNode();
  void AddNode(int64_t node_id, std::shared_ptr<RaftNode> node);
  void DeleteNode(int64_t node_id);

 private:
  bthread_mutex_t mutex_;

  std::map<int64_t, std::shared_ptr<RaftNode>> nodes_;
};

}  // namespace dingodb

#endif  // DINGODB_RAFT_RAFT_NODE_MANAGER_H_

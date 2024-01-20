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

#ifndef DINGODB_META_CONTROL_H_
#define DINGODB_META_CONTROL_H_

#include <cstdint>

#include "engine/snapshot.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

class MetaControl {
 public:
  virtual ~MetaControl() = default;
  MetaControl() = default;
  virtual bool IsLeader() = 0;
  virtual void SetLeaderTerm(int64_t term) = 0;
  virtual void OnLeaderStart(int64_t term) = 0;
  virtual void OnLeaderStop() = 0;
  virtual int GetAppliedTermAndIndex(int64_t &term, int64_t &index) = 0;

  // Get raft leader's server location for sdk use
  virtual void GetLeaderLocation(pb::common::Location &leader_server_location) = 0;

  // set raft_node to coordinator_control
  virtual void SetRaftNode(std::shared_ptr<RaftNode> raft_node) = 0;
  virtual std::shared_ptr<RaftNode> GetRaftNode() = 0;

  // on_apply callback
  virtual void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool is_leader, int64_t term,
                                  int64_t index, google::protobuf::Message *response) = 0;

  // prepare snapshot for raft snapshot
  // return: Snapshot
  virtual std::shared_ptr<Snapshot> PrepareRaftSnapshot() = 0;

  // LoadMetaToSnapshotFile
  virtual bool LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                                      pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) = 0;

  // LoadMetaFromSnapshotFile
  virtual bool LoadMetaFromSnapshotFile(pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) = 0;
};

}  // namespace dingodb

#endif  // DINGODB_META_CONTROL_H_

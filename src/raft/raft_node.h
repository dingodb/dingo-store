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

#ifndef DINGODB_RAFT_NODE_H_
#define DINGODB_RAFT_NODE_H_

#include <braft/raft.h>
#include <braft/util.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "common/context.h"
#include "log/segment_log_storage.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/raft.pb.h"
#include "raft/state_machine.h"

namespace dingodb {

struct SnapshotContext;

// Encapsulation braft node
class RaftNode {
 public:
  RaftNode(int64_t node_id, const std::string& raft_group_name, braft::PeerId peer_id,
           std::shared_ptr<BaseStateMachine> fsm, std::shared_ptr<SegmentLogStorage> log_storage);
  ~RaftNode();

  int Init(store::RegionPtr region, const std::string& init_conf, const std::string& raft_path,
           int election_timeout_ms);
  void Stop();
  void Destroy();

  std::string GetRaftGroupName() const { return raft_group_name_; }
  int64_t GetNodeId() const { return node_id_; }

  butil::Status Commit(std::shared_ptr<Context> ctx, std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd);

  bool IsLeader();
  bool IsLeaderLeaseValid();
  bool HasLeader();
  braft::PeerId GetLeaderId();
  braft::PeerId GetPeerId();

  uint32_t ElectionTimeout() const;
  void ResetElectionTimeout(int election_timeout_ms, int max_clock_drift_ms);

  void Shutdown(braft::Closure* done);
  void Join();

  // Only leader can list peers.
  butil::Status ListPeers(std::vector<braft::PeerId>* peers);
  void AddPeer(const braft::PeerId& peer, braft::Closure* done);
  void RemovePeer(const braft::PeerId& peer, braft::Closure* done);
  void ChangePeers(const std::vector<pb::common::Peer>& peers, braft::Closure* done);
  butil::Status ResetPeers(const braft::Configuration& new_peers);
  int TransferLeadershipTo(const braft::PeerId& peer);

  butil::Status Snapshot(std::shared_ptr<Context> ctx, bool force);

  std::shared_ptr<pb::common::BRaftStatus> GetStatus();

  std::shared_ptr<BaseStateMachine> GetStateMachine();
  std::shared_ptr<SnapshotContext> MakeSnapshotContext();

  void SetDisableSaveSnapshot(bool disable);
  bool DisableSaveSnapshot();

 private:
  std::string path_;
  int64_t node_id_;
  std::string str_node_id_;
  std::string raft_group_name_;

  uint32_t election_timeout_ms_;

  std::shared_ptr<BaseStateMachine> fsm_;
  std::shared_ptr<SegmentLogStorage> log_storage_;
  std::unique_ptr<braft::Node> node_;

  std::atomic<bool> disable_save_snapshot_;
};

}  // namespace dingodb

#endif  // DINGODB_RAFT_NODE_H_

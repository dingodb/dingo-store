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

#include "raft/raft_node.h"

#include "butil/strings/stringprintf.h"
#include "common/helper.h"
#include "config/config_manager.h"
#include "proto/common.pb.h"
#include "raft/state_machine.h"

namespace dingodb {

RaftNode::RaftNode(pb::common::ClusterRole role, uint64_t node_id, braft::PeerId peer_id, braft::StateMachine* fsm)
    : role_(role), node_id_(node_id), node_(new braft::Node(std::to_string(node_id), peer_id)), fsm_(fsm) {}

RaftNode::~RaftNode() {
  if (fsm_) {
    delete fsm_;
    fsm_ = nullptr;
  }
}

// init_conf: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
int RaftNode::Init(const std::string& init_conf) {
  LOG(INFO) << "raft init node_id: " << node_id_ << " init_conf: " << init_conf;
  braft::NodeOptions node_options;
  if (node_options.initial_conf.parse_from(init_conf) != 0) {
    LOG(ERROR) << "Fail to parse configuration";
    return -1;
  }

  auto config = ConfigManager::GetInstance()->GetConfig(role_);

  node_options.election_timeout_ms = config->GetInt("raft.electionTimeout");
  node_options.fsm = fsm_;
  node_options.node_owns_fsm = false;
  node_options.snapshot_interval_s = config->GetInt("raft.snapshotInterval");

  std::string const path = butil::StringPrintf("%s/%ld", config->GetString("raft.path").c_str(), node_id_);
  node_options.log_uri = "local://" + path + "/log";
  node_options.raft_meta_uri = "local://" + path + "/raft_meta";
  node_options.snapshot_uri = "local://" + path + "/snapshot";
  node_options.disable_cli = false;

  if (node_->init(node_options) != 0) {
    LOG(ERROR) << "Fail to init raft node " << node_id_;
    return -1;
  }

  return 0;
}

void RaftNode::Destroy() {}

// Commit message to raft
pb::error::Errno RaftNode::Commit(std::shared_ptr<Context> ctx, std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd) {
  if (!IsLeader()) {
    return pb::error::ERAFT_NOTLEADER;
  }
  LOG(INFO) << "Commit raft cmd to " << node_id_;
  butil::IOBuf data;
  butil::IOBufAsZeroCopyOutputStream wrapper(&data);
  raft_cmd->SerializeToZeroCopyStream(&wrapper);

  braft::Task task;
  task.data = &data;
  task.done = new StoreClosure(ctx);
  node_->apply(task);

  return pb::error::OK;
}

bool RaftNode::IsLeader() { return node_->is_leader(); }

bool RaftNode::IsLeaderLeaseValid() { return node_->is_leader_lease_valid(); }

braft::PeerId RaftNode::GetLeaderId() { return node_->leader_id(); }

void RaftNode::Shutdown(braft::Closure* done) { node_->shutdown(done); }
void RaftNode::Join() { node_->join(); }

butil::Status RaftNode::ListPeers(std::vector<braft::PeerId>* peers) { return node_->list_peers(peers); }

void RaftNode::AddPeer(const braft::PeerId& peer, braft::Closure* done) { node_->add_peer(peer, done); }

void RaftNode::RemovePeer(const braft::PeerId& peer, braft::Closure* done) { node_->remove_peer(peer, done); }

void RaftNode::ChangePeers(const std::vector<pb::common::Peer>& peers, braft::Closure* done) {
  braft::Configuration config;
  for (const auto& peer : peers) {
    butil::EndPoint endpoint = Helper::LocationToEndPoint(peer.raft_location());
    config.add_peer(braft::PeerId(endpoint));
  }

  node_->change_peers(config, done);
}

butil::Status RaftNode::ResetPeers(const braft::Configuration& new_peers) { return node_->reset_peers(new_peers); }

}  // namespace dingodb

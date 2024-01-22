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

#include <memory>
#include <string>
#include <utility>

#include "bthread/bthread.h"
#include "butil/memory/ref_counted.h"
#include "butil/status.h"
#include "common/failpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "log/segment_log_storage.h"
#include "metrics/store_bvar_metrics.h"
#include "proto/common.pb.h"
#include "raft/dingo_filesystem_adaptor.h"
#include "raft/store_state_machine.h"

DEFINE_int32(node_destroy_wait_time_ms, 3000, "wait time on node destroy");

namespace dingodb {

RaftNode::RaftNode(int64_t node_id, const std::string& raft_group_name, braft::PeerId peer_id,
                   std::shared_ptr<BaseStateMachine> fsm, std::shared_ptr<SegmentLogStorage> log_storage)
    : node_id_(node_id),
      str_node_id_(std::to_string(node_id)),
      raft_group_name_(raft_group_name),
      node_(new braft::Node(raft_group_name, peer_id)),
      fsm_(fsm),
      log_storage_(log_storage),
      disable_save_snapshot_(false) {
  DINGO_LOG(DEBUG) << fmt::format("[new.RaftNode][id({})]", node_id);
}

RaftNode::~RaftNode() { DINGO_LOG(DEBUG) << fmt::format("[delete.RaftNode][id({})]", node_id_); }

// init_conf: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
int RaftNode::Init(store::RegionPtr region, const std::string& init_conf, const std::string& raft_path,
                   int election_timeout_ms) {
  DINGO_LOG(INFO) << fmt::format("[raft.node][node_id({})] raft init init_conf: {}", node_id_, init_conf);
  election_timeout_ms_ = election_timeout_ms;

  braft::NodeOptions node_options;
  if (node_options.initial_conf.parse_from(init_conf) != 0) {
    DINGO_LOG(ERROR) << "Fail to parse configuration";
    return -1;
  }
  node_options.election_timeout_ms = election_timeout_ms;
  node_options.fsm = fsm_.get();
  node_options.node_owns_fsm = false;
  // Disable braft snapshot trigger
  node_options.snapshot_interval_s = 0;

  path_ = fmt::format("{}/{}", raft_path, node_id_);
  node_options.raft_meta_uri = "local://" + path_ + "/raft_meta";
  node_options.snapshot_uri = "local://" + path_ + "/snapshot";
  node_options.disable_cli = false;

  node_options.log_storage = new SegmentLogStorageWrapper(log_storage_);
  node_options.node_owns_log_storage = true;

  // coordinator's region does not have store_region_meta, so coordinator will pass nullptr to call AddNode.
  // only store/index's region has store_region_meta, its region != nullptr, we used our own snapshot adaptor.
  if (region != nullptr) {
    region->snapshot_adaptor = new DingoFileSystemAdaptor(region->Id());
    node_options.snapshot_file_system_adaptor = &region->snapshot_adaptor;
  }

  if (node_->init(node_options) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[raft.node][node_id({})] init raft node failed.", node_id_);
    return -1;
  }

  return 0;
}

void RaftNode::Stop() {
  DINGO_LOG(INFO) << fmt::format("[raft.node][node_id({})] stop raft node shutdown.", node_id_);
  node_->shutdown(nullptr);
  node_->join();
  DINGO_LOG(INFO) << fmt::format("[raft.node][node_id({})] stop raft node shutdown finish.", node_id_);
}

void RaftNode::Destroy() {
  Stop();

  // Wait braft node finish.
  if (FLAGS_node_destroy_wait_time_ms > 0) {
    bthread_usleep(FLAGS_node_destroy_wait_time_ms * 1000);
  }

  // Delete file directory
  // Braft maybe save raft_meta file after shutdown, so retry remove.
  for (int i = 0; i < 10; ++i) {
    if (Helper::RemoveAllFileOrDirectory(path_)) {
      break;
    }
    bthread_usleep(100000);
  }

  DINGO_LOG(INFO) << fmt::format("[raft.node][node_id({})] delete file directory", node_id_);
}

// Commit message to raft
butil::Status RaftNode::Commit(std::shared_ptr<Context> ctx, std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd) {
  if (!IsLeader()) {
    return butil::Status(pb::error::ERAFT_NOTLEADER, GetLeaderId().to_string());
  }
  butil::IOBuf data;
  butil::IOBufAsZeroCopyOutputStream wrapper(&data);
  raft_cmd->SerializeToZeroCopyStream(&wrapper);

  FAIL_POINT("before_raft_commit");

  auto tracker = ctx->Tracker();
  if (tracker) {
    tracker->SetPrepairCommitTime();
  }

  braft::Task task;
  task.data = &data;
  task.done = new BaseClosure(ctx, raft_cmd);
  node_->apply(task);

  StoreBvarMetrics::GetInstance().IncCommitCountPerSecond(str_node_id_);

  FAIL_POINT("after_raft_commit");

  return butil::Status();
}

bool RaftNode::IsLeader() { return node_->is_leader(); }

bool RaftNode::IsLeaderLeaseValid() { return node_->is_leader_lease_valid(); }

bool RaftNode::HasLeader() { return node_->leader_id().to_string() != "0.0.0.0:0:0"; }
braft::PeerId RaftNode::GetLeaderId() { return node_->leader_id(); }
braft::PeerId RaftNode::GetPeerId() { return node_->node_id().peer_id; }

uint32_t RaftNode::ElectionTimeout() const { return election_timeout_ms_; }

void RaftNode::ResetElectionTimeout(int election_timeout_ms, int max_clock_drift_ms) {
  if (election_timeout_ms != election_timeout_ms_) {
    DINGO_LOG(INFO) << fmt::format("[raft.node][node_id({})] reset election time({}) max_clock_drift({})", node_id_,
                                   election_timeout_ms, max_clock_drift_ms);
    election_timeout_ms_ = election_timeout_ms;
    node_->reset_election_timeout_ms(election_timeout_ms, max_clock_drift_ms);
  }
}

void RaftNode::Shutdown(braft::Closure* done) { node_->shutdown(done); }
void RaftNode::Join() { node_->join(); }

butil::Status RaftNode::ListPeers(std::vector<braft::PeerId>* peers) {
  if (!IsLeader()) {
    return butil::Status();
  }
  auto status = node_->list_peers(peers);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.node][node_id({})] list peers failed, error: {}", node_id_,
                                    status.error_str());
  }
  return status;
}

void RaftNode::AddPeer(const braft::PeerId& peer, braft::Closure* done) { node_->add_peer(peer, done); }

void RaftNode::RemovePeer(const braft::PeerId& peer, braft::Closure* done) { node_->remove_peer(peer, done); }

void RaftNode::ChangePeers(const std::vector<pb::common::Peer>& peers, braft::Closure* done) {
  braft::Configuration config;
  for (const auto& peer : peers) {
    butil::EndPoint const endpoint = Helper::LocationToEndPoint(peer.raft_location());
    config.add_peer(braft::PeerId(endpoint, peer.raft_location().index()));
  }

  node_->change_peers(config, done);
}

butil::Status RaftNode::ResetPeers(const braft::Configuration& new_peers) { return node_->reset_peers(new_peers); }

int RaftNode::TransferLeadershipTo(const braft::PeerId& peer) { return node_->transfer_leadership_to(peer); }

butil::Status RaftNode::Snapshot(std::shared_ptr<Context> ctx, bool force) {
  if (disable_save_snapshot_.load()) {
    return butil::Status(pb::error::ERAFT_DISABLE_SAVE_SNAPSHOT, "disable save snapshot");
  }
  if (!force && !fsm_->MaySaveSnapshot()) {
    return butil::Status(pb::error::ERAFT_NOT_NEED_SNAPSHOT, "not need save snapshot");
  }

  node_->snapshot(new RaftSnapshotClosure(ctx));

  return butil::Status();
}

std::shared_ptr<pb::common::BRaftStatus> RaftNode::GetStatus() {
  braft::NodeStatus status;
  node_->get_status(&status);

  auto braft_status = std::make_shared<pb::common::BRaftStatus>();
  braft_status->set_raft_state(static_cast<pb::common::RaftNodeState>(status.state));
  braft_status->set_peer_id(status.peer_id.to_string());
  braft_status->set_leader_peer_id(status.leader_id.to_string());
  braft_status->set_readonly(status.readonly);
  braft_status->set_term(status.term);
  braft_status->set_committed_index(status.committed_index);
  braft_status->set_known_applied_index(status.known_applied_index);
  braft_status->set_pending_index(status.pending_index);
  braft_status->set_pending_queue_size(status.pending_queue_size);
  braft_status->set_applying_index(status.applying_index);
  braft_status->set_first_index(status.first_index);
  braft_status->set_last_index(status.last_index);
  braft_status->set_disk_index(status.disk_index);

  auto* stable_follower = braft_status->mutable_stable_followers();
  for (auto [peer_id, peer_status] : status.stable_followers) {
    pb::common::RaftPeerStatus braft_peer_status;
    braft_peer_status.set_valid(peer_status.valid);
    braft_peer_status.set_installing_snapshot(peer_status.installing_snapshot);
    braft_peer_status.set_next_index(peer_status.next_index);
    braft_peer_status.set_last_rpc_send_timestamp(peer_status.last_rpc_send_timestamp);
    braft_peer_status.set_flying_append_entries_size(peer_status.flying_append_entries_size);
    braft_peer_status.set_readonly_index(peer_status.readonly_index);
    braft_peer_status.set_consecutive_error_times(peer_status.consecutive_error_times);
    braft_peer_status.set_valid(peer_status.valid);

    stable_follower->insert({peer_id.to_string(), braft_peer_status});
  }

  auto* unstable_follower = braft_status->mutable_unstable_followers();
  for (auto [peer_id, peer_status] : status.unstable_followers) {
    pb::common::RaftPeerStatus braft_peer_status;
    braft_peer_status.set_valid(peer_status.valid);
    braft_peer_status.set_installing_snapshot(peer_status.installing_snapshot);
    braft_peer_status.set_next_index(peer_status.next_index);
    braft_peer_status.set_last_rpc_send_timestamp(peer_status.last_rpc_send_timestamp);
    braft_peer_status.set_flying_append_entries_size(peer_status.flying_append_entries_size);
    braft_peer_status.set_readonly_index(peer_status.readonly_index);
    braft_peer_status.set_consecutive_error_times(peer_status.consecutive_error_times);
    braft_peer_status.set_valid(peer_status.valid);

    unstable_follower->insert({peer_id.to_string(), braft_peer_status});
  }

  return braft_status;
}

std::shared_ptr<BaseStateMachine> RaftNode::GetStateMachine() { return fsm_; }

std::shared_ptr<SnapshotContext> RaftNode::MakeSnapshotContext() {
  auto fsm = std::dynamic_pointer_cast<StoreStateMachine>(fsm_);
  return fsm != nullptr ? fsm->MakeSnapshotContext() : nullptr;
}

void RaftNode::SetDisableSaveSnapshot(bool disable) { return disable_save_snapshot_.store(disable); }

bool RaftNode::DisableSaveSnapshot() { return disable_save_snapshot_.load(); }

}  // namespace dingodb

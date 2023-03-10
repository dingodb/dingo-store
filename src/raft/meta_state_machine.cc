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

#include "raft/meta_state_machine.h"

#include <memory>

#include "braft/util.h"
#include "butil/strings/stringprintf.h"
#include "coordinator/coordinator_control.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

MetaStateMachine::MetaStateMachine(std::shared_ptr<Engine> engine, MetaControl* meta_control)
    : engine_(engine), meta_control_(meta_control) {}

void MetaStateMachine::DispatchRequest(bool is_leader, const pb::raft::RaftCmdRequest& raft_cmd) {
  for (const auto& req : raft_cmd.requests()) {
    switch (req.cmd_type()) {
      case pb::raft::CmdType::META_WRITE:
        HandleMetaProcess(is_leader, raft_cmd);
        break;
      default:
        LOG(ERROR) << "Unknown raft cmd type " << req.cmd_type();
    }
  }
}

void MetaStateMachine::HandleMetaProcess(bool is_leader, const pb::raft::RaftCmdRequest& raft_cmd) {
  // return response about diffrent Closure
  // todo
  // std::shared_ptr<Context> const ctx = done->GetCtx();
  // brpc::ClosureGuard const done_guard(ctx->done());

  CoordinatorControl* controller = dynamic_cast<CoordinatorControl*>(meta_control_);
  if (raft_cmd.requests_size() > 0) {
    auto meta_increment = raft_cmd.requests(0).meta_req().meta_increment();
    controller->ApplyMetaIncrement(meta_increment, is_leader);
  }
}

void MetaStateMachine::on_apply(braft::Iterator& iter) {
  LOG(INFO) << "on_apply...";
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard const done_guard(iter.done());

    // Leader Node, then we should apply the data to memory and rocksdb
    bool is_leader = false;
    pb::raft::RaftCmdRequest raft_cmd;
    if (iter.done()) {
      StoreClosure* store_closure = dynamic_cast<StoreClosure*>(iter.done());
      raft_cmd = *(store_closure->GetRequest());
      is_leader = true;
    } else {
      butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
      CHECK(raft_cmd.ParseFromZeroCopyStream(&wrapper));
    }

    LOG(INFO) << butil::StringPrintf("raft commited log region(%ld) term(%ld) index(%ld)",
                                     raft_cmd.header().region_id(), iter.term(), iter.index());
    LOG(INFO) << "raft_cmd: " << raft_cmd.ShortDebugString();

    // Follower only write data ?
    // DispatchRequest(dynamic_cast<StoreClosure*>(iter.done()), is_leader, raft_cmd);
    DispatchRequest(is_leader, raft_cmd);
  }
}

void MetaStateMachine::on_shutdown() { LOG(INFO) << "on_shutdown..."; }

void MetaStateMachine::on_snapshot_save(braft::SnapshotWriter* /*writer*/, braft::Closure* /*done*/) {
  LOG(INFO) << "on_snapshot_save...";
}

int MetaStateMachine::on_snapshot_load([[maybe_unused]] braft::SnapshotReader* reader) {
  LOG(INFO) << "on_snapshot_load...";
  return -1;
}

void MetaStateMachine::on_leader_start() { LOG(INFO) << "on_leader_start..."; }

void MetaStateMachine::on_leader_start(int64_t term) { LOG(INFO) << "on_leader_start term: " << term; }

void MetaStateMachine::on_leader_stop(const butil::Status& status) {
  LOG(INFO) << "on_leader_stop: " << status.error_code() << " " << status.error_str();
}

void MetaStateMachine::on_error(const ::braft::Error& e) {
  LOG(INFO) << butil::StringPrintf("on_error type(%d) %d %s", e.type(), e.status().error_code(),
                                   e.status().error_cstr());
}

void MetaStateMachine::on_configuration_committed(const ::braft::Configuration& conf) {
  LOG(INFO) << "on_configuration_committed...";
  // std::vector<braft::PeerId> peers;
  // conf.list_peers(&peers);
}

void MetaStateMachine::on_start_following(const ::braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "on_start_following...";
}

void MetaStateMachine::on_stop_following(const ::braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "on_stop_following...";
}

}  // namespace dingodb

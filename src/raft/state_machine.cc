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

#include "raft/state_machine.h"

#include "braft/util.h"
#include "butil/strings/stringprintf.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"

namespace dingodb {

void StoreClosure::Run() {
  LOG(INFO) << "Closure run...";
  brpc::ClosureGuard done_guard(done_);
  if (!status().ok()) {
    cntl_->SetFailed(status().error_code(), "%s", status().error_cstr());
    LOG(ERROR) << butil::StringPrintf(
        "raft log commit failed, region[%ld] %d:%s", region_id_,
        status().error_code(), status().error_cstr());
  }
}

StoreStateMachine::StoreStateMachine(std::shared_ptr<Engine> engine)
    : engine_(engine) {}

void StoreStateMachine::dispatchRequest(
    const StoreClosure* done, const pb::raft::RaftCmdRequest& raft_cmd) {
  for (auto& req : raft_cmd.requests()) {
    switch (req.cmd_type()) {
      case pb::raft::CmdType::PUT:
        handlePutRequest(done, req.put());
        break;
      case pb::raft::CmdType::PUTIFABSENT:
        handlePutIfAbsentRequest(done, req.put_if_absent());
        break;
      default:
        LOG(ERROR) << "Unknown raft cmd type " << req.cmd_type();
    }
  }
}

void StoreStateMachine::handlePutRequest(const StoreClosure* done,
                                         const pb::raft::PutRequest& request) {
  LOG(INFO) << "handlePutRequest ...";
  // todo: write data
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  for (auto& kv : request.kvs()) {
    engine_->KvPut(ctx, kv);
  }
}

void StoreStateMachine::handlePutIfAbsentRequest(
    const StoreClosure* done, const pb::raft::PutIfAbsentRequest& request) {
  // todo: write data
  LOG(INFO) << "handlePutIfAbsentRequest ...";
}

void StoreStateMachine::on_apply(braft::Iterator& iter) {
  LOG(INFO) << "on_apply...";
  for (; iter.valid(); iter.next()) {
    brpc::ClosureGuard done_guard(iter.done());

    butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
    pb::raft::RaftCmdRequest raft_cmd;
    CHECK(raft_cmd.ParseFromZeroCopyStream(&wrapper));
    LOG(INFO) << butil::StringPrintf("raft log %ld:%ld:%ld commited",
                                     raft_cmd.header().region_id(), iter.term(),
                                     iter.index());

    LOG(INFO) << "raft_cmd: " << raft_cmd.ShortDebugString();
    dispatchRequest(dynamic_cast<StoreClosure*>(iter.done()), raft_cmd);
  }
}

void StoreStateMachine::on_shutdown() { LOG(INFO) << "on_shutdown..."; }

void StoreStateMachine::on_snapshot_save(braft::SnapshotWriter* writer,
                                         braft::Closure* done) {
  LOG(INFO) << "on_snapshot_save...";
}

int StoreStateMachine::on_snapshot_load(
    [[maybe_unused]] braft::SnapshotReader* reader) {
  LOG(INFO) << "on_snapshot_load...";
  return -1;
}

void StoreStateMachine::on_leader_start() { LOG(INFO) << "on_leader_start..."; }

void StoreStateMachine::on_leader_start(int64_t term) {
  LOG(INFO) << "on_leader_start term: " << term;
}

void StoreStateMachine::on_leader_stop() { LOG(INFO) << "on_leader_stop..."; }

void StoreStateMachine::on_leader_stop(const butil::Status& status) {
  LOG(INFO) << "on_leader_stop: " << status.error_code() << " "
            << status.error_str();
}

void StoreStateMachine::on_error(const ::braft::Error& e) {
  LOG(INFO) << butil::StringPrintf("on_error type(%d) %d %s", e.type(),
                                   e.status().error_code(),
                                   e.status().error_cstr());
}

void StoreStateMachine::on_configuration_committed(
    const ::braft::Configuration& conf) {
  LOG(INFO) << "on_configuration_committed...";
  // std::vector<braft::PeerId> peers;
  // conf.list_peers(&peers);
}

void StoreStateMachine::on_start_following(
    const ::braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "on_start_following...";
}
void StoreStateMachine::on_stop_following(
    const ::braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "on_stop_following...";
}

}  // namespace dingodb
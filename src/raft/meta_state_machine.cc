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

#include <braft/protobuf_file.h>  // braft::ProtoBufFile
#include <braft/storage.h>        // braft::SnapshotWriter
#include <braft/util.h>           // braft::AsyncClosureGuard

#include <cstdint>
#include <memory>

#include "common/logging.h"
#include "common/meta_control.h"
#include "engine/snapshot.h"
#include "fmt/core.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

MetaStateMachine::MetaStateMachine(std::shared_ptr<MetaControl> meta_control, bool is_volatile)
    : meta_control_(meta_control), is_volatile_state_machine_(is_volatile), last_snapshot_index_(0) {}

void MetaStateMachine::DispatchRequest(bool is_leader, int64_t term, int64_t index,
                                       const pb::raft::RaftCmdRequest& raft_cmd, google::protobuf::Message* response) {
  for (const auto& req : raft_cmd.requests()) {
    switch (req.cmd_type()) {
      case pb::raft::CmdType::META_WRITE:
        HandleMetaProcess(is_leader, term, index, raft_cmd, response);
        break;
      default:
        DINGO_LOG(ERROR) << "Unknown raft cmd type " << req.cmd_type();
    }
  }
}

void MetaStateMachine::HandleMetaProcess(bool is_leader, int64_t term, int64_t index,
                                         const pb::raft::RaftCmdRequest& raft_cmd,
                                         google::protobuf::Message* response) {
  // return response about diffrent Closure
  // todo
  // std::shared_ptr<Context> const ctx = done->GetCtx();
  // brpc::ClosureGuard const done_guard(ctx->done());

  // CoordinatorControl* controller = dynamic_cast<CoordinatorControl*>(meta_control_);
  if (raft_cmd.requests_size() > 0) {
    auto meta_increment = raft_cmd.requests(0).meta_req().meta_increment();
    meta_control_->ApplyMetaIncrement(meta_increment, is_leader, term, index, response);
  }
}

void MetaStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard const done_guard(iter.done());

    // Leader Node, then we should apply the data to memory and rocksdb
    bool is_leader = false;
    google::protobuf::Message* response = nullptr;
    pb::raft::RaftCmdRequest raft_cmd;
    if (iter.done()) {
      BaseClosure* store_closure = dynamic_cast<BaseClosure*>(iter.done());
      response = store_closure->GetCtx()->Response();
      raft_cmd = *(store_closure->GetRequest());
      is_leader = true;
    } else {
      butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
      CHECK(raft_cmd.ParseFromZeroCopyStream(&wrapper));
    }

    DINGO_LOG(DEBUG) << fmt::format("raft apply log on region[{}-term:{}-index:{}] cmd:[{}]",
                                    raft_cmd.header().region_id(), iter.term(), iter.index(), raft_cmd.DebugString());

    DispatchRequest(is_leader, iter.term(), iter.index(), raft_cmd, response);

    applied_term_ = iter.term();
    applied_index_ = iter.index();
  }
}

void MetaStateMachine::on_shutdown() { DINGO_LOG(INFO) << "on_shutdown..."; }

struct SnapshotArg {
  int64_t value;
  std::shared_ptr<MetaControl> control;
  std::shared_ptr<Snapshot> snapshot;
  braft::SnapshotWriter* writer;
  braft::Closure* done;
};

static void* SaveSnapshot(void* arg) {
  SnapshotArg* sa = (SnapshotArg*)arg;
  std::unique_ptr<SnapshotArg> arg_guard(sa);
  // Serialize StateMachine to the snapshot
  brpc::ClosureGuard done_guard(sa->done);
  std::string snapshot_path = sa->writer->get_path() + "/data";
  DINGO_LOG(INFO) << "Saving snapshot to " << snapshot_path;
  // Use protobuf to store the snapshot for backward compatibility.
  pb::coordinator_internal::MetaSnapshotFile s;
  bool ret = sa->control->LoadMetaToSnapshotFile(sa->snapshot, s);
  if (!ret) {
    sa->done->status().set_error(EIO, "Fail to add file to writer, LoadMetaToSnapshotFile return false");
    return nullptr;
  }

  braft::ProtoBufFile pb_file(snapshot_path);
  if (pb_file.save(&s, true) != 0) {
    sa->done->status().set_error(EIO, "Fail to save pb_file");
    return nullptr;
  }
  // Snapshot is a set of files in raft. Add the only file into the
  // writer here.
  if (sa->writer->add_file("data") != 0) {
    sa->done->status().set_error(EIO, "Fail to add file to writer");
    return nullptr;
  }
  return nullptr;
}

void MetaStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
  DINGO_LOG(INFO) << "on_snapshot_save...";
  // Save current StateMachine in memory and starts a new bthread to avoid
  // blocking StateMachine since it's a bit slow to write data to disk
  // file.
  SnapshotArg* arg = new SnapshotArg;
  // arg->value = _value.load(butil::memory_order_relaxed);
  arg->control = this->meta_control_;
  arg->snapshot = this->meta_control_->PrepareRaftSnapshot();
  arg->writer = writer;
  arg->done = done;
  bthread_t tid;
  bthread_start_urgent(&tid, nullptr, SaveSnapshot, arg);

  last_snapshot_index_ = applied_index_;
}

int MetaStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
  DINGO_LOG(INFO) << "on_snapshot_load...";
  // Load snasphot from reader, replacing the running StateMachine
  if (!is_volatile_state_machine_) {
    CHECK(!this->meta_control_->IsLeader()) << "Leader is not supposed to load snapshot";
  }
  if (reader->get_file_meta("data", nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to find `data' on " << reader->get_path();
    return -1;
  }

  // load snapshot meta
  braft::SnapshotMeta snapshot_meta;
  auto ret = reader->load_meta(&snapshot_meta);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "Fail to load snapshot meta from " << reader->get_path();
    return -1;
  }

  // if last_include_index < last_applied_index, we should not load snapshot
  // because we have already applied logs after last_include_index
  int64_t term = 0;
  int64_t index = 0;
  ret = this->meta_control_->GetAppliedTermAndIndex(term, index);
  if (ret < 0) {
    DINGO_LOG(WARNING) << "Fail to GetAppliedTermAndIndex, need snapshot install, when load snapshot from "
                       << reader->get_path();
  }

  DINGO_LOG(INFO) << "on_snapshot_load last_include_index:" << snapshot_meta.last_included_index()
                  << " last_applied_index:" << index << " last_include_term:" << snapshot_meta.last_included_term()
                  << " last_applied_term:" << term;

  if (term >= snapshot_meta.last_included_term() && index >= snapshot_meta.last_included_index()) {
    DINGO_LOG(WARNING) << "skip to load snapshot from " << reader->get_path()
                       << " because last_include_index"
                          " < last_applied_index and last_include_term < last_applied_term";
    DINGO_LOG(WARNING) << "skip to load snapshot last_include_index:" << snapshot_meta.last_included_index()
                       << " last_applied_index:" << index << " last_include_term:" << snapshot_meta.last_included_term()
                       << " last_applied_term:" << term;
    return 0;
  }

  std::string snapshot_path = reader->get_path() + "/data";
  braft::ProtoBufFile pb_file(snapshot_path);
  pb::coordinator_internal::MetaSnapshotFile s;
  if (pb_file.load(&s) != 0) {
    DINGO_LOG(ERROR) << "Fail to load snapshot from " << snapshot_path;
    return -1;
  }

  bool bool_ret = this->meta_control_->LoadMetaFromSnapshotFile(s);
  if (!bool_ret) {
    DINGO_LOG(ERROR) << "Fail to load snapshot from " << snapshot_path << " LoadMetaFromSnapshotFile return false";
    return -1;
  }

  applied_term_ = snapshot_meta.last_included_term();
  applied_index_ = snapshot_meta.last_included_index();
  last_snapshot_index_ = snapshot_meta.last_included_index();
  return 0;
}

void MetaStateMachine::on_leader_start(int64_t term) {
  DINGO_LOG(INFO) << "on_leader_start term: " << term;
  meta_control_->SetLeaderTerm(term);
  meta_control_->OnLeaderStart(term);
}

void MetaStateMachine::on_leader_stop(const butil::Status& status) {
  DINGO_LOG(INFO) << "on_leader_stop: " << status.error_code() << " " << status.error_str();
  meta_control_->SetLeaderTerm(-1);
  meta_control_->OnLeaderStop();
}

void MetaStateMachine::on_error(const ::braft::Error& e) {
  DINGO_LOG(INFO) << fmt::format("on_error type({}) {} {}", static_cast<int>(e.type()), e.status().error_code(),
                                 e.status().error_str());
}

void MetaStateMachine::on_configuration_committed(const ::braft::Configuration& /*conf*/) {
  DINGO_LOG(INFO) << "on_configuration_committed...";
  // std::vector<braft::PeerId> peers;
  // conf.list_peers(&peers);
}

void MetaStateMachine::on_start_following(const ::braft::LeaderChangeContext& /*ctx*/) {
  DINGO_LOG(INFO) << "on_start_following...";
}

void MetaStateMachine::on_stop_following(const ::braft::LeaderChangeContext& /*ctx*/) {
  DINGO_LOG(INFO) << "on_stop_following...";
}

int64_t MetaStateMachine::GetAppliedIndex() const { return applied_index_; }

int64_t MetaStateMachine::GetLastSnapshotIndex() const { return last_snapshot_index_; }

}  // namespace dingodb

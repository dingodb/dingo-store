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

#include "raft/auto_increment_state_machine.h"

#include <fstream>

#include "butil/strings/stringprintf.h"
#include "coordinator/auto_increment_control.h"
#include "proto/common.pb.h"
#include "raft/store_state_machine.h"

namespace dingodb {

AutoIncrementStateMachine::AutoIncrementStateMachine(std::shared_ptr<MetaControl> meta_control) :
	meta_control_(meta_control) {
}

void AutoIncrementStateMachine::on_apply(braft::Iterator& iter) {
  DINGO_LOG(INFO) << "on apply...";
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard const done_guard(iter.done());

    bool is_leader = false;
    google::protobuf::Message* response = nullptr;
    pb::raft::RaftCmdRequest raft_cmd;
    if (iter.done()) {
      is_leader = true;
      StoreClosure* store_closure = dynamic_cast<StoreClosure*>(iter.done()); 
      response = store_closure->GetCtx()->Response();
      raft_cmd = *(store_closure->GetRequest());
    } else {
      butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
      CHECK(raft_cmd.ParseFromZeroCopyStream(&wrapper));
    }
    DINGO_LOG(DEBUG) << butil::StringPrintf("raft apply log on auto increment, region[%ld-term:%ld-index:%ld] cmd:[%s]",
                                            raft_cmd.header().region_id(), iter.term(), iter.index(),
                                            raft_cmd.DebugString().c_str());
    DispatchRequest(is_leader, iter.term(), iter.index(), response, raft_cmd);
  }
}

void AutoIncrementStateMachine::on_shutdown() {
  DINGO_LOG(INFO) << "on shutdown...";
}

void AutoIncrementStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
  DINGO_LOG(INFO) << "on snapshot save";
  
  std::string auto_increment_data;
  auto control = std::dynamic_pointer_cast<AutoIncrementControl>(meta_control_);
  if (control->SaveAutoIncrement(auto_increment_data) < 0) {
    DINGO_LOG(ERROR) << "on snapshot save failed.";
    return;
  }

  std::function<void()> save_snapshot_function = [this, done, writer, auto_increment_data]() {
    SaveSnapshot(done, writer, auto_increment_data);
  };
  Bthread bth(&BTHREAD_ATTR_SMALL);
  bth.run(save_snapshot_function);
}

int AutoIncrementStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
  DINGO_LOG(INFO) << "on snapshot load";
  std::vector<std::string> files;
  reader->list_files(&files);
  for (auto& file : files) {
      if (file == kSnapshotFile) {
          DINGO_LOG(INFO) << "snapshot load file: " << file;
          std::string auto_increment_file = reader->get_path() + kSnapshotFile;
          auto control = std::dynamic_pointer_cast<AutoIncrementControl>(meta_control_);
          if (control->LoadAutoIncrement(auto_increment_file) != 0) {
              DINGO_LOG(ERROR) << "snapshot load file failed.";
              return -1;
          }
          break;
      }
  }
  return 0;
}

void AutoIncrementStateMachine::on_leader_start(int64_t term) {
  DINGO_LOG(INFO) << "AutoIncrementStateMachine on_leader_start term: " << term << ".";
  meta_control_->SetLeaderTerm(term);
  meta_control_->OnLeaderStart(term);
}

void AutoIncrementStateMachine::on_leader_stop(const butil::Status& status) {
  DINGO_LOG(INFO) << " on leader stop: " << status.error_code() << " " << status.error_str();
  meta_control_->SetLeaderTerm(-1);
}

void AutoIncrementStateMachine::on_error(const ::braft::Error& e) {
  DINGO_LOG(ERROR) << butil::StringPrintf("auto increment state machine on_error type(%d) %d %s.", 
    e.type(), e.status().error_code(), e.status().error_cstr());
}

void AutoIncrementStateMachine::on_configuration_committed(const ::braft::Configuration& /*conf*/) {
  DINGO_LOG(INFO) << "on configuration committed...";
}

void AutoIncrementStateMachine::on_start_following(const ::braft::LeaderChangeContext& /*ctx*/) {
  DINGO_LOG(INFO) << "on start following...";
}

void AutoIncrementStateMachine::on_stop_following(const ::braft::LeaderChangeContext& /*ctx*/) {
  DINGO_LOG(INFO) << "on stop following...";
}

void AutoIncrementStateMachine::SaveSnapshot(braft::Closure* done, braft::SnapshotWriter* writer,
  const std::string& auto_increment_data) {
  brpc::ClosureGuard done_guard(done);
  std::string snapshot_path = writer->get_path();
  std::string auto_increment_path = snapshot_path + kSnapshotFile;
  std::ofstream auto_increment_fs(auto_increment_path, std::ofstream::out | std::ofstream::trunc);
  auto_increment_fs.write(auto_increment_data.data(), auto_increment_data.size());
  auto_increment_fs.close();
  if (writer->add_file(kSnapshotFile) != 0) {
    done->status().set_error(EINVAL, "Fail to add file");
    DINGO_LOG(ERROR) << "Error while adding file to writer.";
    return;
  }
}

void AutoIncrementStateMachine::DispatchRequest(bool is_leader, uint64_t term, uint64_t index,
      google::protobuf::Message* response, const pb::raft::RaftCmdRequest& raft_cmd) {
  size_t request_index = 0;
  for (const auto& req : raft_cmd.requests()) {
    switch (req.cmd_type()) {
      case pb::raft::CmdType::META_WRITE:
        HandleAutoIncrementProcess(is_leader, term, index, request_index, response, raft_cmd);
        break;
      default:
        DINGO_LOG(ERROR) << "Unknown raft cmd type " << req.cmd_type();
    }
    ++request_index;
  }
}

void AutoIncrementStateMachine::HandleAutoIncrementProcess(bool is_leader, uint64_t term, uint64_t index,
      size_t request_index, google::protobuf::Message* response, const pb::raft::RaftCmdRequest& raft_cmd) {
  CHECK(request_index < raft_cmd.requests_size());
  auto meta_increment = raft_cmd.requests(request_index).meta_req().meta_increment();
  meta_control_->ApplyMetaIncrement(meta_increment, is_leader, term, index, response);
}

}  // namespace dingodb

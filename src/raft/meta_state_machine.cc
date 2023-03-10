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

#include "braft/util.h"
#include "butil/strings/stringprintf.h"
#include "proto/error.pb.h"

namespace dingodb {

using RaftCreateSchemaRequest = pb::raft::RaftCreateSchemaRequest;

void MetaClosure::Run() {
  LOG(INFO) << "Closure run...";

  // Set response error
  auto set_error_func = [](butil::Status& status, google::protobuf::Message* message) {
    const google::protobuf::Reflection* reflection = message->GetReflection();
    const google::protobuf::Descriptor* desc = message->GetDescriptor();

    const google::protobuf::FieldDescriptor* error_field = desc->FindFieldByName("error");
    google::protobuf::Message* error = reflection->MutableMessage(message, error_field);
    const google::protobuf::Reflection* error_ref = error->GetReflection();
    const google::protobuf::Descriptor* error_desc = error->GetDescriptor();
    const google::protobuf::FieldDescriptor* errcode_field = error_desc->FindFieldByName("errcode");
    error_ref->SetEnumValue(error, errcode_field, status.error_code());
    const google::protobuf::FieldDescriptor* errmsg_field = error_desc->FindFieldByName("errmsg");
    error_ref->SetString(error, errmsg_field, status.error_str());
  };

  brpc::ClosureGuard done_guard(ctx_->done());
  if (!status().ok()) {
    LOG(ERROR) << butil::StringPrintf("raft log commit failed, region[%ld] %d:%s", ctx_->region_id(),
                                      status().error_code(), status().error_cstr());
    set_error_func(status(), ctx_->response());
  }
}

MetaStateMachine::MetaStateMachine(std::shared_ptr<Engine> engine) : engine_(engine) {}

void MetaStateMachine::DispatchRequest(MetaClosure* done, const pb::raft::RaftCmdRequest& raft_cmd) {
  for (const auto& req : raft_cmd.requests()) {
    switch (req.cmd_type()) {
      case pb::raft::CmdType::META_WRITE:
        // HandleCreateSchema(done, request);
        break;
      default:
        LOG(ERROR) << "Unknown raft cmd type " << req.cmd_type();
    }
  }
}

void MetaStateMachine::HandleCreateSchema(MetaClosure* done, const RaftCreateSchemaRequest& request) {}

void MetaStateMachine::on_apply(braft::Iterator& iter) {
  LOG(INFO) << "on_apply...";
  for (; iter.valid(); iter.next()) {
    brpc::ClosureGuard done_guard(iter.done());

    butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
    pb::raft::RaftCmdRequest raft_cmd;
    CHECK(raft_cmd.ParseFromZeroCopyStream(&wrapper));
    LOG(INFO) << butil::StringPrintf("raft commited log region(%ld) term(%ld) index(%ld)",
                                     raft_cmd.header().region_id(), iter.term(), iter.index());

    LOG(INFO) << "raft_cmd: " << raft_cmd.ShortDebugString();
    DispatchRequest(dynamic_cast<MetaClosure*>(iter.done()), raft_cmd);
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

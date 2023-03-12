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

#include "raft/store_state_machine.h"

#include "braft/util.h"
#include "butil/strings/stringprintf.h"
#include "common/helper.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"

namespace dingodb {

void StoreClosure::Run() {
  LOG(INFO) << "Closure run...";

  brpc::ClosureGuard done_guard(ctx_->IsSyncMode() ? nullptr : ctx_->Done());
  if (!status().ok()) {
    LOG(ERROR) << butil::StringPrintf("raft log commit failed, region[%ld] %d:%s", ctx_->RegionId(),
                                      status().error_code(), status().error_cstr());

    ctx_->SetStatus(status());
  }

  if (ctx_->IsSyncMode()) {
    ctx_->Cond()->DecreaseSignal();
  } else {
    ctx_->WriteCb()(ctx_->Status());
  }
}

StoreStateMachine::StoreStateMachine(std::shared_ptr<RawEngine> engine) : engine_(engine) {}

void StoreStateMachine::DispatchRequest(StoreClosure* done, const pb::raft::RaftCmdRequest& raft_cmd) {
  for (const auto& req : raft_cmd.requests()) {
    switch (req.cmd_type()) {
      case pb::raft::CmdType::PUT:
        HandlePutRequest(done, req.put());
        break;
      case pb::raft::CmdType::PUTIFABSENT:
        HandlePutIfAbsentRequest(done, req.put_if_absent());
        break;
      case pb::raft::CmdType::DELETERANGE:
        HandleDeleteRangeRequest(done, req.delete_range());
        break;
      default:
        LOG(ERROR) << "Unknown raft cmd type " << req.cmd_type();
    }
  }
}

void StoreStateMachine::HandlePutRequest(StoreClosure* done, const pb::raft::PutRequest& request) {
  LOG(INFO) << "handlePutRequest ...";
  butil::Status status;
  auto writer = engine_->NewWriter(request.cf_name());
  if (request.kvs().size() == 1) {
    status = writer->KvPut(request.kvs().Get(0));
  } else {
    status = writer->KvBatchPut(Helper::PbRepeatedToVector(request.kvs()));
  }

  if (done != nullptr) {
    std::shared_ptr<Context> ctx = done->GetCtx();
    if (ctx) {
      ctx->SetStatus(status);
    }
  }
}

void StoreStateMachine::HandlePutIfAbsentRequest(StoreClosure* done, const pb::raft::PutIfAbsentRequest& request) {
  LOG(INFO) << "handlePutIfAbsentRequest ...";

  butil::Status status;
  auto writer = engine_->NewWriter(request.cf_name());
  if (request.kvs().size() == 1) {
    status = writer->KvPutIfAbsent(request.kvs().Get(0));
  } else {
    std::vector<std::string> put_keys;
    status = writer->KvBatchPutIfAbsent(Helper::PbRepeatedToVector(request.kvs()), put_keys, true);
  }

  if (done != nullptr) {
    std::shared_ptr<Context> ctx = done->GetCtx();
    if (ctx) {
      ctx->SetStatus(status);
    }
  }
}

void StoreStateMachine::HandleDeleteRangeRequest([[maybe_unused]] StoreClosure* done,
                                                 [[maybe_unused]] const pb::raft::DeleteRangeRequest& request) {
  LOG(INFO) << "HandleDeleteRangeRequest ...";

  butil::Status status;
  auto writer = engine_->NewWriter(request.cf_name());
  for (auto& range : request.ranges()) {
    status = writer->KvDeleteRange(range);
    if (!status.ok()) {
      break;
    }
  }

  if (done != nullptr) {
    std::shared_ptr<Context> ctx = done->GetCtx();
    if (ctx) {
      ctx->SetStatus(status);
    }
  }
}

void StoreStateMachine::on_apply(braft::Iterator& iter) {
  LOG(INFO) << "on_apply...";
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard done_guard(iter.done());

    butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
    pb::raft::RaftCmdRequest raft_cmd;
    CHECK(raft_cmd.ParseFromZeroCopyStream(&wrapper));
    LOG(INFO) << butil::StringPrintf("raft commited log region(%ld) term(%ld) index(%ld)",
                                     raft_cmd.header().region_id(), iter.term(), iter.index());

    LOG(INFO) << "raft_cmd: " << raft_cmd.ShortDebugString();
    DispatchRequest(dynamic_cast<StoreClosure*>(iter.done()), raft_cmd);
  }
}

void StoreStateMachine::on_shutdown() { LOG(INFO) << "on_shutdown..."; }

void StoreStateMachine::on_snapshot_save([[maybe_unused]] braft::SnapshotWriter* writer,
                                         [[maybe_unused]] braft::Closure* done) {
  LOG(INFO) << "on_snapshot_save...";
}

int StoreStateMachine::on_snapshot_load([[maybe_unused]] braft::SnapshotReader* reader) {
  LOG(INFO) << "on_snapshot_load...";
  return -1;
}

void StoreStateMachine::on_leader_start() { LOG(INFO) << "on_leader_start..."; }

void StoreStateMachine::on_leader_start(int64_t term) { LOG(INFO) << "on_leader_start term: " << term; }

void StoreStateMachine::on_leader_stop(const butil::Status& status) {
  LOG(INFO) << "on_leader_stop: " << status.error_code() << " " << status.error_str();
}

void StoreStateMachine::on_error(const ::braft::Error& e) {
  LOG(INFO) << butil::StringPrintf("on_error type(%d) %d %s", e.type(), e.status().error_code(),
                                   e.status().error_cstr());
}

void StoreStateMachine::on_configuration_committed([[maybe_unused]] const ::braft::Configuration& conf) {
  LOG(INFO) << "on_configuration_committed...";
  // std::vector<braft::PeerId> peers;
  // conf.list_peers(&peers);
}

void StoreStateMachine::on_start_following([[maybe_unused]] const ::braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "on_start_following...";
}
void StoreStateMachine::on_stop_following([[maybe_unused]] const ::braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "on_stop_following...";
}

}  // namespace dingodb

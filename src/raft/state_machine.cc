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
#include "common/helper.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"

namespace dingodb {

void StoreClosure::Run() {
  LOG(INFO) << "Closure run...";

  brpc::ClosureGuard done_guard(ctx_->IsSyncMode() ? nullptr : ctx_->done());
  if (!status().ok()) {
    LOG(ERROR) << butil::StringPrintf("raft log commit failed, region[%ld] %d:%s", ctx_->region_id(),
                                      status().error_code(), status().error_cstr());
    if (!ctx_->IsSyncMode()) {
      // todo
      Helper::SetPbMessageError(status().error_code(), status().error_str(), ctx_->response());
    }
  }

  if (ctx_->IsSyncMode()) {
    ctx_->SetStatus(status());
    ctx_->Cond()->DecreaseSignal();
  }
}

StoreStateMachine::StoreStateMachine(std::shared_ptr<Engine> engine) : engine_(engine) {}

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

void StoreStateMachine::HandlePutRequest([[maybe_unused]] StoreClosure* done, const pb::raft::PutRequest& request) {
  LOG(INFO) << "handlePutRequest ...";
  std::shared_ptr<Context> ctx =
      (done != nullptr && done->GetCtx() != nullptr) ? done->GetCtx() : std::make_shared<Context>();
  ctx->set_cf_name(request.cf_name());

  pb::error::Errno errcode = pb::error::OK;
  if (request.kvs().size() == 1) {
    errcode = engine_->KvPut(ctx, request.kvs().Get(0));
  } else {
    errcode = engine_->KvBatchPut(ctx, Helper::PbRepeatedToVector(request.kvs()));
  }

  if (errcode != pb::error::OK && done != nullptr && done->GetCtx() != nullptr && !done->GetCtx()->IsSyncMode()) {
    Helper::SetPbMessageError(errcode, "Put failed.", done->GetCtx()->response());
  }
}

void StoreStateMachine::HandlePutIfAbsentRequest(StoreClosure* done, const pb::raft::PutIfAbsentRequest& request) {
  LOG(INFO) << "handlePutIfAbsentRequest ...";
  std::shared_ptr<Context> ctx =
      (done != nullptr && done->GetCtx() != nullptr) ? done->GetCtx() : std::make_shared<Context>();
  ctx->set_cf_name(request.cf_name());

  if (request.kvs().size() == 1) {
    auto errcode = engine_->KvPutIfAbsent(ctx, request.kvs().Get(0));
    if (errcode != pb::error::OK && done != nullptr && done->GetCtx() != nullptr) {
      Helper::SetPbMessageError(errcode, "Put if absent failed.", done->GetCtx()->response());
    }
  } else {
    std::vector<std::string> put_keys;
    auto errcode = engine_->KvBatchPutIfAbsentNonAtomic(ctx, Helper::PbRepeatedToVector(request.kvs()), put_keys);
    if (done != nullptr && done->GetCtx() != nullptr) {
      if (errcode != pb::error::OK) {
        Helper::SetPbMessageError(errcode, "Batch put if absent failed.", done->GetCtx()->response());
      } else {
        auto response = dynamic_cast<pb::store::KvBatchPutIfAbsentResponse*>(done->GetCtx()->response());
        for (auto& key : put_keys) {
          response->add_put_keys(key);
        }
      }
    }
  }
}

void StoreStateMachine::HandleDeleteRangeRequest([[maybe_unused]] StoreClosure* done,
                                                 [[maybe_unused]] const pb::raft::DeleteRangeRequest& request) {
  LOG(INFO) << "HandleDeleteRangeRequest ...";
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

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

#include "coordinator/tso_control.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "brpc/closure_guard.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "engine/snapshot.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

void TsoClosure::Run() {
  // DINGO_LOG(INFO) << "TsoClosure run";
  if (!status().ok()) {
    if (response) {
      response->mutable_error()->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);
    }
    DINGO_LOG(ERROR) << "meta server closure fail, error_code: " << status().error_code()
                     << ", error_mas: " << status().error_cstr();
  }
  if (sync_cond) {
    sync_cond->DecreaseSignal();
  }
  if (done != nullptr) {
    done->Run();
  }
  delete this;
}

int TsoTimer::init(TsoControl* tso_control, int timeout_ms) {
  int ret = RepeatedTimerTask::init(timeout_ms);
  this->tso_control = tso_control;
  return ret;
}

void TsoTimer::run() { this->tso_control->UpdateTimestamp(); }

void TsoControl::UpdateTimestamp() {
  // DINGO_LOG(INFO) << "update timestamp";

  if (!IsLeader()) {
    return;
  }
  int64_t now = ClockRealtimeMs();
  int64_t prev_physical = 0;
  int64_t prev_logical = 0;
  int64_t last_save = 0;
  {
    BAIDU_SCOPED_LOCK(tso_mutex_);
    prev_physical = tso_obj_.current_timestamp.physical();
    prev_logical = tso_obj_.current_timestamp.logical();
    last_save = tso_obj_.last_save_physical;
  }
  int64_t delta = now - prev_physical;
  if (delta < 0) {
    DINGO_LOG(WARNING) << "physical time slow now: " << now << ", prev: " << prev_physical;
  }
  int64_t next = now;
  if (delta > kUpdateTimestampGuardMs) {
    next = now;
  } else if (prev_logical > kMaxLogical / 2) {
    next = prev_physical + kUpdateTimestampGuardMs;
  } else {
    DINGO_LOG(WARNING) << "don't need update timestamp prev: " << prev_physical << ", now: " << now
                       << ", save: " << last_save;
    return;
  }
  int64_t save = last_save;
  if (save - next <= kUpdateTimestampGuardMs) {
    save = next + kSaveIntervalMs;
  }
  pb::meta::TsoTimestamp tp;
  tp.set_physical(next);
  tp.set_logical(0);
  SyncTimestamp(tp, save);
}

int TsoControl::SyncTimestamp(const pb::meta::TsoTimestamp& current_timestamp, int64_t save_physical) {
  pb::meta::TsoRequest request;
  pb::meta::TsoResponse response;
  request.set_op_type(pb::meta::OP_UPDATE_TSO);
  auto* timestamp = request.mutable_current_timestamp();
  *timestamp = current_timestamp;
  request.set_save_physical(save_physical);

  DINGO_LOG(DEBUG) << "sync timestamp request: " << request.ShortDebugString();

  BthreadCond sync_cond;
  TsoClosure* c = new TsoClosure(&sync_cond);
  c->response = &response;
  c->done = nullptr;
  c->tso_control = this;
  sync_cond.Increase();

  pb::coordinator_internal::MetaIncrement meta_increment;
  auto* tso = meta_increment.add_timestamp_oracles();
  *(tso->mutable_tso_request()) = request;
  this->SubmitMetaIncrementAsync(c, &response, meta_increment);

  sync_cond.Wait();

  if (response.error().errcode() != pb::error::OK) {
    DINGO_LOG(ERROR) << "sync timestamp failed, request: " << request.ShortDebugString()
                     << ", response: " << response.ShortDebugString();
    return -1;
  }

  DINGO_LOG(DEBUG) << "sync timestamp ok, request: " << request.ShortDebugString()
                   << ", response: " << response.ShortDebugString();

  return 0;
}

void TsoControl::GenTso(const pb::meta::TsoRequest* request, pb::meta::TsoResponse* response) {
  int64_t count = request->count();
  response->set_op_type(request->op_type());
  if (count == 0) {
    response->mutable_error()->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);
    response->mutable_error()->set_errmsg("tso count should be positive");
    return;
  }
  if (!is_healty_) {
    DINGO_LOG(ERROR) << "TSO has wrong status, retry later";
    response->mutable_error()->set_errcode(pb::error::Errno::ERETRY_LATER);
    response->mutable_error()->set_errmsg("timestamp not ok, retry later");
    return;
  }
  pb::meta::TsoTimestamp current;
  bool need_retry = false;
  for (size_t i = 0; i < 50; i++) {
    {
      BAIDU_SCOPED_LOCK(tso_mutex_);
      int64_t physical = tso_obj_.current_timestamp.physical();
      if (physical != 0) {
        int64_t new_logical = tso_obj_.current_timestamp.logical() + count;
        if (new_logical < kMaxLogical) {
          current = tso_obj_.current_timestamp;
          tso_obj_.current_timestamp.set_logical(new_logical);
          need_retry = false;
        } else {
          DINGO_LOG(WARNING) << "logical part outside of max logical interval, retry later, please check ntp time";
          need_retry = true;
        }
      } else {
        DINGO_LOG(WARNING) << "timestamp not ok physical == 0, retry later";
        need_retry = true;
      }
    }
    if (!need_retry) {
      break;
    } else {
      bthread_usleep(kUpdateTimestampIntervalMs * 1000LL);
    }
  }
  if (need_retry) {
    response->mutable_error()->set_errcode(pb::error::Errno::EEXEC_FAIL);
    response->mutable_error()->set_errmsg("gen tso failed");
    DINGO_LOG(ERROR) << "gen tso failed";
    return;
  }
  DINGO_LOG(INFO) << "gen tso current: (" << current.physical() << ", " << current.logical() << ")";
  auto* timestamp = response->mutable_start_timestamp();
  *timestamp = current;
  response->set_count(count);
}

// This method is called by the gRPC server.
// This method is used to process the request from the client.
// The response is filled by the state machine and sent back to the client.
// The done callback is used to notify the gRPC server that the request is
void TsoControl::Process(google::protobuf::RpcController* controller, const pb::meta::TsoRequest* request,
                         pb::meta::TsoResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  if (request->op_type() == pb::meta::OP_QUERY_TSO_INFO) {
    response->mutable_error()->set_errcode(pb::error::Errno::OK);
    response->mutable_error()->set_errmsg("success");
    response->set_op_type(request->op_type());
    // response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
    response->set_system_time(ClockRealtimeMs());
    response->set_save_physical(tso_obj_.last_save_physical);
    auto* timestamp = response->mutable_start_timestamp();
    *timestamp = tso_obj_.current_timestamp;
    return;
  }
  brpc::Controller* cntl = (brpc::Controller*)controller;
  int64_t log_id = 0;
  if (cntl->has_log_id()) {
    log_id = cntl->log_id();
  }
  const auto& remote_side_tmp = butil::endpoint2str(cntl->remote_side());
  const char* remote_side = remote_side_tmp.c_str();
  if (!IsLeader()) {
    response->mutable_error()->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);
    response->mutable_error()->set_errmsg("not leader");
    response->set_op_type(request->op_type());
    // response->set_leader(butil::endpoint2str(_node.leader_id().addr).c_str());
    DINGO_LOG(WARNING) << "state machine not leader, request: " << request->ShortDebugString()
                       << ", remote_side: " << remote_side << ", log_id: " << log_id;
    return;
  }
  // gen tso out of raft state machine
  if (request->op_type() == pb::meta::OP_GEN_TSO) {
    GenTso(request, response);
    return;
  }
  butil::IOBuf data;
  butil::IOBufAsZeroCopyOutputStream wrapper(&data);
  if (!request->SerializeToZeroCopyStream(&wrapper)) {
    cntl->SetFailed(brpc::EREQUEST, "Fail to serialize request");
    return;
  }
  TsoClosure* closure = new TsoClosure;
  closure->cntl = cntl;
  closure->response = response;
  closure->done = done_guard.release();
  closure->tso_control = this;

  pb::coordinator_internal::MetaIncrement meta_increment;
  auto* tso = meta_increment.add_timestamp_oracles();
  *(tso->mutable_tso_request()) = (*request);
  this->SubmitMetaIncrementSync(closure, response, meta_increment);
}

// If request.force == false, ResetTso is equal to UpdateTso
// Else ResetTso will force update tso
void TsoControl::ResetTso(const pb::meta::TsoRequest& request, pb::meta::TsoResponse* response) {
  if (request.has_current_timestamp() && request.save_physical() > 0) {
    int64_t physical = request.save_physical();
    const pb::meta::TsoTimestamp& current = request.current_timestamp();
    if (physical < tso_obj_.last_save_physical || current.physical() < tso_obj_.current_timestamp.physical()) {
      if (!request.force()) {
        DINGO_LOG(WARNING) << "time fallback save_physical:(" << physical << ", " << tso_obj_.last_save_physical
                           << ") current:(" << current.physical() << ", " << tso_obj_.current_timestamp.physical()
                           << ", " << current.logical() << ", " << tso_obj_.current_timestamp.logical() << ")";
        if (response) {
          response->mutable_error()->set_errcode(pb::error::Errno::EINTERNAL);
          response->mutable_error()->set_errmsg("time can't fallback");
          auto* timestamp = response->mutable_start_timestamp();
          *timestamp = tso_obj_.current_timestamp;
          response->set_save_physical(tso_obj_.last_save_physical);
        }
        return;
      }
    }
    is_healty_ = true;
    DINGO_LOG(WARNING) << "reset tso save_physical: " << physical << " current: (" << current.physical() << ", "
                       << current.logical() << ")";
    {
      BAIDU_SCOPED_LOCK(tso_mutex_);
      tso_obj_.last_save_physical = physical;
      tso_obj_.current_timestamp = current;
    }
    if (response) {
      response->set_save_physical(physical);
      auto* timestamp = response->mutable_start_timestamp();
      *timestamp = current;
      response->mutable_error()->set_errcode(pb::error::Errno::OK);
      response->mutable_error()->set_errmsg("SUCCESS");
    }
  }
}

void TsoControl::UpdateTso(const pb::meta::TsoRequest& request, pb::meta::TsoResponse* response) {
  int64_t physical = request.save_physical();
  const pb::meta::TsoTimestamp& current = request.current_timestamp();
  // can't rollback
  if (physical < tso_obj_.last_save_physical || current.physical() < tso_obj_.current_timestamp.physical()) {
    DINGO_LOG(WARNING) << "time fallback save_physical:(" << physical << ", " << tso_obj_.last_save_physical
                       << ") current:(" << current.physical() << ", " << tso_obj_.current_timestamp.physical() << ", "
                       << current.logical() << ", " << tso_obj_.current_timestamp.logical() << ")";
    if (response) {
      response->mutable_error()->set_errcode(pb::error::Errno::EINTERNAL);
      response->mutable_error()->set_errmsg("time can't fallback");
    }
    return;
  }
  {
    BAIDU_SCOPED_LOCK(tso_mutex_);
    tso_obj_.last_save_physical = physical;
    tso_obj_.current_timestamp = current;
  }

  if (response) {
    response->mutable_error()->set_errcode(pb::error::Errno::OK);
    response->mutable_error()->set_errmsg("SUCCESS");
  }
}

TsoControl::TsoControl() {
  // init bthread mutex
  bthread_mutex_init(&tso_mutex_, nullptr);

  leader_term_.store(-1, butil::memory_order_release);
}

// tso_update_timer_ is a timer to update timestamp
// tso_update_timer_ is started when OnLeaderStart
// and is stopped in OnLeaderStop
bool TsoControl::Init() {
  DINGO_LOG(INFO) << "init";
  tso_update_timer_.init(this, kUpdateTimestampIntervalMs);
  tso_obj_.current_timestamp.set_physical(0);
  tso_obj_.current_timestamp.set_logical(0);
  tso_obj_.last_save_physical = 0;

  return true;
}

bool TsoControl::Recover() {
  DINGO_LOG(INFO) << "recover";
  return true;
}

void TsoControl::GetLeaderLocation(pb::common::Location& leader_server_location) {
  if (raft_node_ == nullptr) {
    DINGO_LOG(ERROR) << "raft_node_ is nullptr";
    return;
  }

  // parse leader raft location from string
  auto leader_string = raft_node_->GetLeaderId().to_string();

  pb::common::Location leader_raft_location;
  int ret = Helper::PeerIdToLocation(raft_node_->GetLeaderId(), leader_raft_location);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "get raft leader failed, ret: " << ret << ".";
    return;
  }

  // GetServerLocation
  GetServerLocation(leader_raft_location, leader_server_location);
}

void TsoControl::GetServerLocation(pb::common::Location& raft_location, pb::common::Location& server_location) {
  // find in cache
  auto raft_location_string = raft_location.host() + ":" + std::to_string(raft_location.port());
  auto it = tso_location_cache_.find(raft_location_string);
  if (it != tso_location_cache_.end()) {
    server_location = it->second;
    DINGO_LOG(DEBUG) << "Cache Hit raft_location=" << raft_location.host() << ":" << raft_location.port();
    return;
  }

  Helper::GetServerLocation(raft_location, server_location);

  // add to cache if get server_location
  if (server_location.host().length() > 0 && server_location.port() > 0) {
    DINGO_LOG(INFO) << " Cache Miss, add new cache raft_location=" << raft_location.host() << ":"
                    << raft_location.port();
    tso_location_cache_[raft_location_string] = server_location;
  } else {
    DINGO_LOG(INFO) << " Cache Miss, can't get server_location, raft_location=" << raft_location.host() << ":"
                    << raft_location.port();
  }
}

bool TsoControl::IsLeader() { return leader_term_.load(butil::memory_order_acquire) > 0; }

void TsoControl::SetLeaderTerm(int64_t term) { leader_term_.store(term, butil::memory_order_release); }

void TsoControl::OnLeaderStart(int64_t term) {
  DINGO_LOG(INFO) << "OnLeaderStart_TSO, term=" << term;
  int64_t now = ClockRealtimeMs();
  pb::meta::TsoTimestamp current;
  current.set_physical(now);
  current.set_logical(0);
  int64_t last_save = tso_obj_.last_save_physical;
  if (now < last_save + kUpdateTimestampIntervalMs) {
    DINGO_LOG(WARNING) << "time maybe fallback, now: " << now << ", last_save: " << last_save
                       << ", kUpdateTimestampIntervalMs: " << kUpdateTimestampIntervalMs;
    current.set_physical(last_save + kUpdateTimestampGuardMs);
  }
  last_save = current.physical() + kSaveIntervalMs;
  auto func = [this, last_save, current]() {
    LOG(INFO) << "leader_start current(phy:" << current.physical() << ",log:" << current.logical()
              << ") save:" << last_save;
    int ret = SyncTimestamp(current, last_save);
    if (ret < 0) {
      LOG(INFO) << "sync timestamp failed";
      is_healty_ = false;
    }
    LOG(INFO) << "sync timestamp ok";
    tso_update_timer_.start();
  };
  Bthread bth;
  bth.Run(func);
}

// only leader will update timestamp
// so follower will stop timer
void TsoControl::OnLeaderStop() {
  DINGO_LOG(INFO) << "OnLeaderStop";
  tso_update_timer_.stop();
}

// set raft_node to coordinator_control
void TsoControl::SetRaftNode(std::shared_ptr<RaftNode> raft_node) { raft_node_ = raft_node; }

// on_apply callback
void TsoControl::ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement& meta_increment, bool is_leader,
                                    int64_t /*term*/, int64_t /*index*/, google::protobuf::Message* response) {
  for (int i = 0; i < meta_increment.timestamp_oracles_size(); i++) {
    const auto& tso = meta_increment.timestamp_oracles(i);

    const auto& tso_request = tso.tso_request();

    pb::meta::TsoResponse* tso_response = nullptr;
    if (is_leader && response != nullptr) {
      tso_response = static_cast<pb::meta::TsoResponse*>(response);
      tso_response->set_op_type(tso_request.op_type());
      // DINGO_LOG(INFO) << "tso_request: " << tso_request.ShortDebugString();
    }

    switch (tso_request.op_type()) {
      case pb::meta::OP_RESET_TSO: {
        ResetTso(tso_request, tso_response);
        break;
      }
      case pb::meta::OP_UPDATE_TSO: {
        UpdateTso(tso_request, tso_response);
        break;
      }
      default: {
        DINGO_LOG(ERROR) << "unsupport request type, type: " << tso_request.op_type();
        if (tso_response) {
          tso_response->mutable_error()->set_errcode(pb::error::Errno::EUNSUPPORT_REQ_TYPE);
          tso_response->mutable_error()->set_errmsg("unsupport request type");
        }
      }
    }
  }
}

int TsoControl::GetAppliedTermAndIndex(int64_t& term, int64_t& index) {
  term = 0;
  index = 0;
  return 0;
}

// tso's snapshot is only a int64_t value
// just reuse the snapshot logic of coordinator and auto increment controller
std::shared_ptr<Snapshot> TsoControl::PrepareRaftSnapshot() {
  int64_t* save_physical = new (std::nothrow) int64_t;
  {
    BAIDU_SCOPED_LOCK(tso_mutex_);
    *save_physical = tso_obj_.last_save_physical;
  }

  return std::make_shared<TsoSnapshot>(save_physical);
}

bool TsoControl::LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                                        pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
  DINGO_LOG(INFO) << "TsoControl start to LoadMetaToSnapshotFile";

  auto tso_snapshot = std::dynamic_pointer_cast<TsoSnapshot>(snapshot);
  if (tso_snapshot == nullptr) {
    DINGO_LOG(ERROR) << "Failed to dynamic cast snapshot to auto increment snapshot";
    return false;
  }

  const auto* save_physical_of_snapshot = tso_snapshot->GetSnapshot();

  auto* tso_storage = meta_snapshot_file.mutable_tso_storage();
  tso_storage->set_physical(*save_physical_of_snapshot);

  DINGO_LOG(INFO) << "TsoControl LoadMetaToSnapshotFile success, save_physical=" << *save_physical_of_snapshot;

  return true;
}

bool TsoControl::LoadMetaFromSnapshotFile(pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
  DINGO_LOG(INFO) << "TsoControl start to LoadMetaFromSnapshotFile";

  const auto& storage = meta_snapshot_file.tso_storage();

  BAIDU_SCOPED_LOCK(tso_mutex_);
  tso_obj_.last_save_physical = storage.physical();

  DINGO_LOG(INFO) << "TsoControl LoadMetaFromSnapshotFile success, last_save_physical=" << storage.physical();

  return true;
}

// SubmitMetaIncrement
// commit meta increment to raft meta engine, with no closure
void LogMetaIncrementTso(pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (meta_increment.ByteSizeLong() > 0) {
    DINGO_LOG(DEBUG) << "meta_increment byte_size=" << meta_increment.ByteSizeLong();
  } else {
    return;
  }
  if (meta_increment.timestamp_oracles_size() > 0) {
    DINGO_LOG(DEBUG) << "0.timestamp_oracles_size=" << meta_increment.timestamp_oracles_size();
  }

  DINGO_LOG(DEBUG) << meta_increment.DebugString();
}

butil::Status TsoControl::SubmitMetaIncrementAsync(google::protobuf::Closure* done, pb::meta::TsoResponse* response,
                                                   pb::coordinator_internal::MetaIncrement& meta_increment) {
  LogMetaIncrementTso(meta_increment);

  std::shared_ptr<Context> const ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kTsoRegionId);

  if (response != nullptr) {
    ctx->SetResponse(response);
  }

  if (done != nullptr) {
    ctx->SetDone(done);
  }

  auto status = engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "ApplyMetaIncrement failed, errno=" << status.error_code() << " errmsg=" << status.error_str();
    if (done != nullptr) {
      brpc::ClosureGuard fail_done(done);
    }
    return status;
  }
  return butil::Status::OK();
}

butil::Status TsoControl::SubmitMetaIncrementSync(google::protobuf::Closure* done, pb::meta::TsoResponse* response,
                                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  LogMetaIncrementTso(meta_increment);

  std::shared_ptr<Context> const ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kTsoRegionId);

  if (response != nullptr) {
    ctx->SetResponse(response);
  }

  if (done != nullptr) {
    ctx->SetDone(done);
  }

  auto status = engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "ApplyMetaIncrement failed, errno=" << status.error_code() << " errmsg=" << status.error_str();
    if (done != nullptr) {
      brpc::ClosureGuard fail_done(done);
    }
    return status;
  }
  return butil::Status::OK();
}

}  // namespace dingodb

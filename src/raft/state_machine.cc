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

#include <memory>

#include "common/logging.h"
#include "fmt/core.h"

DEFINE_int64(save_raft_snapshot_log_gap_num, 64, "save raft snapshot log gap");

namespace dingodb {

void BaseClosure::Run() {
  // Delete self after run
  std::unique_ptr<BaseClosure> self_guard(this);
  brpc::ClosureGuard const done_guard(ctx_->Done());

  if (!status().ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.sm][region({})] raft log commit failed, error: {} {}", ctx_->RegionId(),
                                    status().error_code(), status().error_str());

    ctx_->SetStatus(butil::Status(pb::error::ERAFT_COMMITLOG, status().error_str()));
  }

  // if sync_mode_cond exists, it means a sync mode call is in progress.
  // now sync mode call does not support write callback function.
  auto sync_mode_cond = ctx_->SyncModeCond();
  if (sync_mode_cond) {
    sync_mode_cond->DecreaseSignal();
  } else {
    if (ctx_->WriteCb()) {
      ctx_->WriteCb()(ctx_, ctx_->Status());
    }
  }
}

void RaftSnapshotClosure::Run() {
  // Delete self after run
  std::unique_ptr<RaftSnapshotClosure> self_guard(this);
  brpc::ClosureGuard const done_guard(ctx_->Done());

  if (!status().ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.sm][region({})] do raft snapshot failed, error: {} {}", ctx_->RegionId(),
                                    status().error_code(), status().error_str());

    auto* done = dynamic_cast<braft::Closure*>(ctx_->Done());
    if (done != nullptr) {
      done->status().set_error(status().error_code(), status().error_str());
    }
    ctx_->SetStatus(status());
  }

  // if sync_mode_cond exists, it means a sync mode call is in progress.
  // now sync mode call does not support write callback function.
  auto sync_mode_cond = ctx_->SyncModeCond();
  if (sync_mode_cond) {
    sync_mode_cond->DecreaseSignal();
  } else {
    if (ctx_->WriteCb()) {
      ctx_->WriteCb()(ctx_, ctx_->Status());
    }
  }
}

bool BaseStateMachine::MaySaveSnapshot() {
  return (GetAppliedIndex() - GetLastSnapshotIndex()) >= FLAGS_save_raft_snapshot_log_gap_num;
}

}  // namespace dingodb
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
#include "event/store_state_machine_event.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"

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
    if (ctx_->WriteCb()) {
      ctx_->WriteCb()(ctx_->Status());
    }
  }
}

StoreStateMachine::StoreStateMachine(std::shared_ptr<RawEngine> engine, uint64_t node_id,
                                     std::shared_ptr<EventListenerCollection> listeners)
    : engine_(engine), node_id_(node_id), listeners_(listeners) {}

void StoreStateMachine::on_apply(braft::Iterator& iter) {
  LOG(INFO) << "on_apply...";
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard done_guard(iter.done());

    auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
    if (iter.done()) {
      StoreClosure* store_closure = dynamic_cast<StoreClosure*>(iter.done());
      raft_cmd = store_closure->GetRequest();
    } else {
      butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
      CHECK(raft_cmd->ParseFromZeroCopyStream(&wrapper));
    }

    std::string str_raft_cmd = Helper::MessageToJsonString(*raft_cmd.get());
    LOG(INFO) << butil::StringPrintf("raft apply log on region[%ld-term:%ld-index:%ld] cmd:[%s]",
                                     raft_cmd->header().region_id(), iter.term(), iter.index(), str_raft_cmd.c_str());
    // Build event
    auto event = std::make_shared<SmApplyEvent>();
    event->engine_ = engine_;
    event->done_ = iter.done();
    event->raft_cmd_ = raft_cmd;

    for (auto listener : listeners_->Get(EventType::SM_APPLY)) {
      listener->OnEvent(event);
    }
  }
}

void StoreStateMachine::on_shutdown() {
  LOG(INFO) << "on_shutdown...";
  auto event = std::make_shared<SmShutdownEvent>();

  for (auto listener : listeners_->Get(EventType::SM_SNAPSHOT_SAVE)) {
    listener->OnEvent(event);
  }
}

void StoreStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
  LOG(INFO) << "on_snapshot_save...";
  auto event = std::make_shared<SmSnapshotSaveEvent>(writer, done);

  for (auto listener : listeners_->Get(EventType::SM_SNAPSHOT_SAVE)) {
    listener->OnEvent(event);
  }
}

int StoreStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
  LOG(INFO) << "on_snapshot_load...";
  auto event = std::make_shared<SmSnapshotLoadEvent>(reader);

  for (auto listener : listeners_->Get(EventType::SM_SNAPSHOT_LOAD)) {
    listener->OnEvent(event);
  }
  return -1;
}

void StoreStateMachine::on_leader_start(int64_t term) {
  LOG(INFO) << "on_leader_start term: " << term;

  auto event = std::make_shared<SmLeaderStartEvent>();
  event->term_ = term;
  event->node_id_ = node_id_;

  for (auto listener : listeners_->Get(EventType::SM_LEADER_START)) {
    listener->OnEvent(event);
  }
}

void StoreStateMachine::on_leader_stop(const butil::Status& status) {
  LOG(INFO) << "on_leader_stop: " << status.error_code() << " " << status.error_str();
  auto event = std::make_shared<SmLeaderStopEvent>(status);

  for (auto listener : listeners_->Get(EventType::SM_LEADER_STOP)) {
    listener->OnEvent(event);
  }
}

void StoreStateMachine::on_error(const braft::Error& e) {
  LOG(INFO) << butil::StringPrintf("on_error type(%d) %d %s", e.type(), e.status().error_code(),
                                   e.status().error_cstr());

  auto event = std::make_shared<SmErrorEvent>(e);

  for (auto listener : listeners_->Get(EventType::SM_ERROR)) {
    listener->OnEvent(event);
  }
}

void StoreStateMachine::on_configuration_committed(const braft::Configuration& conf) {
  LOG(INFO) << "on_configuration_committed...";

  auto event = std::make_shared<SmConfigurationCommittedEvent>(conf);

  for (auto listener : listeners_->Get(EventType::SM_CONFIGURATION_COMMITTED)) {
    listener->OnEvent(event);
  }
}

void StoreStateMachine::on_start_following(const braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "on_start_following...";
  auto event = std::make_shared<SmStartFollowingEvent>(ctx);
  event->node_id_ = node_id_;

  for (auto listener : listeners_->Get(EventType::SM_START_FOLLOWING)) {
    listener->OnEvent(event);
  }
}

void StoreStateMachine::on_stop_following(const braft::LeaderChangeContext& ctx) {
  LOG(INFO) << "on_stop_following...";
  auto event = std::make_shared<SmStopFollowingEvent>(ctx);
  for (auto listener : listeners_->Get(EventType::SM_STOP_FOLLOWING)) {
    listener->OnEvent(event);
  }
}

}  // namespace dingodb

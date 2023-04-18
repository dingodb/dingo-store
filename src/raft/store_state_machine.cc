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

#include <memory>

#include "braft/util.h"
#include "butil/strings/stringprintf.h"
#include "common/helper.h"
#include "common/logging.h"
#include "event/store_state_machine_event.h"
#include "meta/store_meta_manager.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"

const int kSaveAppliedIndexStep = 10;

namespace dingodb {

void StoreClosure::Run() {
  // Delete self after run
  std::unique_ptr<StoreClosure> self_guard(this);
  brpc::ClosureGuard const done_guard(ctx_->IsSyncMode() ? nullptr : ctx_->Done());
  if (!status().ok()) {
    DINGO_LOG(ERROR) << butil::StringPrintf("raft log commit failed, region[%ld] %d:%s", ctx_->RegionId(),
                                            status().error_code(), status().error_cstr());

    ctx_->SetStatus(status());
  }

  if (ctx_->IsSyncMode()) {
    ctx_->Cond()->DecreaseSignal();
  } else {
    if (ctx_->WriteCb()) {
      ctx_->WriteCb()(ctx_, ctx_->Status());
    }
  }
}

StoreStateMachine::StoreStateMachine(std::shared_ptr<RawEngine> engine, uint64_t node_id,
                                     std::shared_ptr<EventListenerCollection> listeners)
    : engine_(engine), node_id_(node_id), listeners_(listeners), applied_term_(0), applied_index_(0) {}

bool StoreStateMachine::Init() {
  // Recover applied index
  auto store_raft_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta();
  auto raft_meta = store_raft_meta->GetRaftMeta(node_id_);
  if (raft_meta != nullptr) {
    DINGO_LOG(INFO) << "applied_index: " << raft_meta->applied_index();
    applied_index_ = raft_meta->applied_index();

  } else {
    raft_meta = StoreRaftMeta::NewRaftMeta(node_id_);
    store_raft_meta->AddRaftMeta(raft_meta);
  }

  raft_meta_ = raft_meta;

  return true;
}

void StoreStateMachine::DispatchEvent(dingodb::EventType event_type, std::shared_ptr<dingodb::Event> event) {
  for (auto& listener : listeners_->Get(event_type)) {
    listener->OnEvent(event);
  }
}

void StoreStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard done_guard(iter.done());
    if (iter.index() <= applied_index_) {
      continue;
    }

    auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
    if (iter.done()) {
      StoreClosure* store_closure = dynamic_cast<StoreClosure*>(iter.done());
      raft_cmd = store_closure->GetRequest();
    } else {
      butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
      CHECK(raft_cmd->ParseFromZeroCopyStream(&wrapper));
    }

    DINGO_LOG(DEBUG) << butil::StringPrintf(
        "raft apply log on region[%ld-term:%ld-index:%ld] applied_index[%ld] cmd:[%s]", raft_cmd->header().region_id(),
        iter.term(), iter.index(), applied_index_, raft_cmd->ShortDebugString().c_str());
    // Build event
    auto event = std::make_shared<SmApplyEvent>();
    event->engine = engine_;
    event->done = iter.done();
    event->raft_cmd = raft_cmd;

    DispatchEvent(EventType::kSmApply, event);
    applied_term_ = iter.term();
    applied_index_ = iter.index();
  }

  // Persistence applied index
  // If operation is idempotent, it's ok.
  // If not, must be stored with the data.
  if (applied_index_ % kSaveAppliedIndexStep == 0) {
    raft_meta_->set_term(applied_term_);
    raft_meta_->set_applied_index(applied_index_);
    Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta()->UpdateRaftMeta(raft_meta_);
  }
}

void StoreStateMachine::on_shutdown() {
  DINGO_LOG(INFO) << "on_shutdown, region: " << node_id_;
  auto event = std::make_shared<SmShutdownEvent>();
  DispatchEvent(EventType::kSmShutdown, event);
}

void StoreStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
  DINGO_LOG(INFO) << "on_snapshot_save, region: " << node_id_;
  auto event = std::make_shared<SmSnapshotSaveEvent>();
  event->engine = engine_;
  event->writer = writer;
  event->done = done;
  event->node_id = node_id_;

  DispatchEvent(EventType::kSmSnapshotSave, event);
  DINGO_LOG(INFO) << "on_snapshot_save done, region: " << node_id_;
}

// Load snapshot timing
// 1. server restart.
//    on_snapshot_load:
//      1>. max_index = max(applied_index, snapshot_index)
//      2>. not load snapshot files
//      3>. applied_index = max_index
// 2. add peer.
//    install_snapshot
//    on_snapshot_load:
//      1>. max_index = max(applied_index, snapshot_index)
//      2>. load snapshot files
//      3>. applied_index = max_index
// 3. follower log fall behind leader and leader log already deleted.
//    install_snapshot
//    on_snapshot_load:
//      1>. max_index = max(applied_index, snapshot_index)
//      2>. load snapshot files
//      3>. applied_index = max_index
int StoreStateMachine::on_snapshot_load(braft::SnapshotReader* reader) {
  DINGO_LOG(INFO) << "on_snapshot_load, region: " << node_id_;
  braft::SnapshotMeta meta;
  reader->load_meta(&meta);
  DINGO_LOG(INFO) << butil::StringPrintf("load snapshot(%ld-%ld) applied_index(%lu)", meta.last_included_term(),
                                         meta.last_included_index(), applied_index_);

  if (meta.last_included_index() > applied_index_) {
    auto event = std::make_shared<SmSnapshotLoadEvent>();
    event->engine = engine_;
    event->reader = reader;
    event->node_id = node_id_;
    DispatchEvent(EventType::kSmSnapshotLoad, event);
    applied_index_ = meta.last_included_index();
  }

  return 0;
}

void StoreStateMachine::on_leader_start(int64_t term) {
  DINGO_LOG(INFO) << "on_leader_start, region: " << node_id_ << " term: " << term;

  auto event = std::make_shared<SmLeaderStartEvent>();
  event->term = term;
  event->node_id = node_id_;

  DispatchEvent(EventType::kSmLeaderStart, event);
}

void StoreStateMachine::on_leader_stop(const butil::Status& status) {
  DINGO_LOG(INFO) << "on_leader_stop, region: " << node_id_ << " error: " << status.error_code() << " "
                  << status.error_str();
  auto event = std::make_shared<SmLeaderStopEvent>();
  event->status = status;

  DispatchEvent(EventType::kSmLeaderStop, event);
}

void StoreStateMachine::on_error(const braft::Error& e) {
  DINGO_LOG(INFO) << butil::StringPrintf("on_error region: %ld type(%d) %d %s", node_id_, e.type(),
                                         e.status().error_code(), e.status().error_cstr());

  auto event = std::make_shared<SmErrorEvent>();
  event->e = e;

  DispatchEvent(EventType::kSmError, event);
}

void StoreStateMachine::on_configuration_committed(const braft::Configuration& conf) {
  DINGO_LOG(INFO) << "on_configuration_committed, region: " << node_id_;

  auto event = std::make_shared<SmConfigurationCommittedEvent>();
  event->conf = conf;

  DispatchEvent(EventType::kSmConfigurationCommited, event);
}

void StoreStateMachine::on_start_following(const braft::LeaderChangeContext& ctx) {
  DINGO_LOG(INFO) << "on_start_following, region: " << node_id_;
  auto event = std::make_shared<SmStartFollowingEvent>(ctx);
  event->node_id = node_id_;

  DispatchEvent(EventType::kSmStartFollowing, event);
}

void StoreStateMachine::on_stop_following(const braft::LeaderChangeContext& ctx) {
  DINGO_LOG(INFO) << "on_stop_following, region: " << node_id_;
  auto event = std::make_shared<SmStopFollowingEvent>(ctx);

  DispatchEvent(EventType::kSmStopFollowing, event);
}

}  // namespace dingodb

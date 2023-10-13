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

#include <atomic>
#include <memory>
#include <string>

#include "braft/util.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "event/store_state_machine_event.h"
#include "fmt/core.h"
#include "meta/meta_writer.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_bvar_metrics.h"
#include "proto/common.pb.h"
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
    DINGO_LOG(ERROR) << fmt::format("raft log commit failed, region[{}] {}:{}", ctx_->RegionId(), status().error_code(),
                                    status().error_str());

    ctx_->SetStatus(butil::Status(pb::error::ERAFT_COMMITLOG, status().error_str()));
  }

  if (ctx_->IsSyncMode()) {
    ctx_->Cond()->DecreaseSignal();
  } else {
    if (ctx_->WriteCb()) {
      ctx_->WriteCb()(ctx_, ctx_->Status());
    }
  }
}

StoreStateMachine::StoreStateMachine(std::shared_ptr<RawEngine> engine, store::RegionPtr region,
                                     std::shared_ptr<pb::store_internal::RaftMeta> raft_meta,
                                     store::RegionMetricsPtr region_metrics,
                                     std::shared_ptr<EventListenerCollection> listeners, bool is_restart)
    : engine_(engine),
      region_(region),
      str_node_id_(std::to_string(region->Id())),
      raft_meta_(raft_meta),
      region_metrics_(region_metrics),
      listeners_(listeners),
      applied_term_(raft_meta->term()),
      applied_index_(raft_meta->applied_index()),
      is_restart_for_load_snapshot_(is_restart) {}

bool StoreStateMachine::Init() { return true; }

int StoreStateMachine::DispatchEvent(dingodb::EventType event_type, std::shared_ptr<dingodb::Event> event) {
  if (listeners_ == nullptr) return -1;

  for (auto& listener : listeners_->Get(event_type)) {
    int ret = listener->OnEvent(event);
    if (ret != 0) {
      return ret;
    }
  }

  return 0;
}

void StoreStateMachine::on_apply(braft::Iterator& iter) {
  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard done_guard(iter.done());
    if (iter.index() <= applied_index_) {
      continue;
    }

    // region is STANDBY state, don't apply.
    while (region_->State() == pb::common::StoreRegionState::STANDBY) {
      DINGO_LOG(WARNING) << fmt::format("[raft.sm][region({})] region is standby for spliting, waiting...",
                                        region_->Id());
      bthread_usleep(1000 * 1000);
    }

    if (region_->State() == pb::common::StoreRegionState::DELETING ||
        region_->State() == pb::common::StoreRegionState::DELETED) {
      DINGO_LOG(WARNING) << fmt::format("[raft.sm][region({})] region is deleting/deleted, abandon apply log.",
                                        region_->Id());
      break;
    }

    auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
    if (iter.done()) {
      StoreClosure* store_closure = dynamic_cast<StoreClosure*>(iter.done());
      raft_cmd = store_closure->GetRequest();
    } else {
      butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
      CHECK(raft_cmd->ParseFromZeroCopyStream(&wrapper));
    }

    // DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] apply log {}:{} applied_index({})",
    //                                raft_cmd->header().region_id(), iter.term(), iter.index(), applied_index_);
    // Build event
    auto event = std::make_shared<SmApplyEvent>();
    event->region = region_;
    event->engine = engine_;
    event->done = iter.done();
    event->raft_cmd = raft_cmd;
    event->region_metrics = region_metrics_;
    event->term_id = iter.term();
    event->log_id = iter.index();

    DispatchEvent(EventType::kSmApply, event);

    applied_term_ = iter.term();
    applied_index_ = iter.index();

    raft_meta_->set_term(iter.term());
    raft_meta_->set_applied_index(iter.index());

    // bvar metrics
    StoreBvarMetrics::GetInstance().IncApplyCountPerSecond(str_node_id_);
  }

  // Persistence applied index
  // If operation is idempotent, it's ok.
  // If not, must be stored with the data.
  if (applied_index_ % kSaveAppliedIndexStep == 0) {
    Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta()->UpdateRaftMeta(raft_meta_);
  }
}

void StoreStateMachine::on_shutdown() {
  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_shutdown", region_->Id());
  auto event = std::make_shared<SmShutdownEvent>();
  DispatchEvent(EventType::kSmShutdown, event);
  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_shutdown done", region_->Id());
}

void StoreStateMachine::on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_snapshot_save", region_->Id());
  auto event = std::make_shared<SmSnapshotSaveEvent>();
  event->engine = engine_;
  event->writer = writer;
  event->done = done;
  event->region = region_;
  event->term = applied_term_;
  event->log_index = applied_index_;

  DispatchEvent(EventType::kSmSnapshotSave, event);

  if (raft_meta_ != nullptr) {
    Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta()->UpdateRaftMeta(raft_meta_);
  }

  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_snapshot_save done", region_->Id());
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
  braft::SnapshotMeta meta;
  int ret = reader->load_meta(&meta);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("[raft.sm][region({})] load meta failed, ret: {}", region_->Id(), ret);
    return -1;
  }

  pb::store_internal::RaftSnapshotRegionMeta business_meta;
  auto status = Helper::ParseRaftSnapshotRegionMeta(reader->get_path(), business_meta);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.sm][region({})] parse business snapshot meta failed, error: {}",
                                    region_->Id(), status.error_str());
    return -1;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[raft.sm][region({})] on_snapshot_load snapshot({}-{}) business meta({}-{}) applied_index({})", region_->Id(),
      meta.last_included_term(), meta.last_included_index(), business_meta.term(), business_meta.log_index(),
      applied_index_);

  if (region_->State() == pb::common::STANDBY) {
    DINGO_LOG(WARNING) << fmt::format("[raft.sm][region({})] region is STANDBY state, ignore load snapshot.",
                                      region_->Id());
    if (business_meta.log_index() > applied_index_) {
      DINGO_LOG(ERROR) << fmt::format("[raft.sm][region({})] region is STANDBY state, kill self.", region_->Id());
      return -1;
    }
    return 0;
  }

  if (business_meta.log_index() > applied_index_) {
    auto event = std::make_shared<SmSnapshotLoadEvent>();
    event->engine = engine_;
    event->reader = reader;
    event->region = region_;

    int ret = DispatchEvent(EventType::kSmSnapshotLoad, event);
    if (ret != 0) {
      return ret;
    }

    // Update applied term and index
    applied_term_ = meta.last_included_term();
    applied_index_ = meta.last_included_index();

    if (raft_meta_ != nullptr) {
      raft_meta_->set_term(meta.last_included_term());
      raft_meta_->set_applied_index(meta.last_included_index());
      Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta()->UpdateRaftMeta(raft_meta_);
    }
  }

  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_snapshot_load done", region_->Id());

  return 0;
}

void StoreStateMachine::on_leader_start(int64_t term) {
  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_leader_start term({})", region_->Id(), term);

  auto event = std::make_shared<SmLeaderStartEvent>();
  event->term = term;
  event->region = region_;

  DispatchEvent(EventType::kSmLeaderStart, event);

  // bvar metrics
  StoreBvarMetrics::GetInstance().UpdateLeaderSwitchCount(str_node_id_, term);
  StoreBvarMetrics::GetInstance().UpdateLeaderSwitchTime(str_node_id_);
}

void StoreStateMachine::on_leader_stop(const butil::Status& status) {
  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_leader_stop, error: {} {}", region_->Id(),
                                 status.error_code(), status.error_str());
  auto event = std::make_shared<SmLeaderStopEvent>();
  event->status = status;
  event->region = region_;

  DispatchEvent(EventType::kSmLeaderStop, event);
}

void StoreStateMachine::on_error(const braft::Error& e) {
  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_error type({}) {} {}", region_->Id(),
                                 static_cast<int>(e.type()), e.status().error_code(), e.status().error_str());

  auto event = std::make_shared<SmErrorEvent>();
  event->e = e;

  DispatchEvent(EventType::kSmError, event);
}

void StoreStateMachine::on_configuration_committed(const braft::Configuration& conf) {
  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_configuration_committed, peers: {}", region_->Id(),
                                 Helper::FormatPeers(conf));

  auto event = std::make_shared<SmConfigurationCommittedEvent>();
  event->node_id = region_->Id();
  event->conf = conf;

  DispatchEvent(EventType::kSmConfigurationCommited, event);
}

void StoreStateMachine::on_start_following(const braft::LeaderChangeContext& ctx) {
  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_start_following, leader_id: {} error: {} {}", region_->Id(),
                                 ctx.leader_id().to_string(), ctx.status().error_code(), ctx.status().error_str());
  auto event = std::make_shared<SmStartFollowingEvent>(ctx);
  event->region = region_;

  DispatchEvent(EventType::kSmStartFollowing, event);

  // bvar metrics
  StoreBvarMetrics::GetInstance().UpdateLeaderSwitchCount(str_node_id_, ctx.term());
  StoreBvarMetrics::GetInstance().UpdateLeaderSwitchTime(str_node_id_);
}

void StoreStateMachine::on_stop_following(const braft::LeaderChangeContext& ctx) {
  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_stop_following, leader_id: {} error: {} {}", region_->Id(),
                                 ctx.leader_id().to_string(), ctx.status().error_code(), ctx.status().error_str());
  auto event = std::make_shared<SmStopFollowingEvent>(ctx);
  event->region = region_;

  DispatchEvent(EventType::kSmStopFollowing, event);
}

}  // namespace dingodb

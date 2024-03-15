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

#include <cstdint>
#include <memory>
#include <string>

#include "braft/util.h"
#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "event/store_state_machine_event.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "raft/dingo_filesystem_adaptor.h"
#include "server/server.h"

const int kSaveAppliedIndexStep = 10;

namespace dingodb {

StoreStateMachine::StoreStateMachine(std::shared_ptr<RawEngine> engine, store::RegionPtr region,
                                     store::RaftMetaPtr raft_meta, store::RegionMetricsPtr region_metrics,
                                     std::shared_ptr<EventListenerCollection> listeners,
                                     PriorWorkerSetPtr raft_apply_worker_set)
    : raw_engine_(engine),
      region_(region),
      str_node_id_(std::to_string(region->Id())),
      raft_meta_(raft_meta),
      region_metrics_(region_metrics),
      listeners_(listeners),
      applied_term_(raft_meta->Term()),
      applied_index_(raft_meta->AppliedId()),
      last_snapshot_index_(0),
      raft_apply_worker_set_(raft_apply_worker_set) {
  bthread_mutex_init(&apply_mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.StoreStateMachine][id({})]", str_node_id_);
}

StoreStateMachine::~StoreStateMachine() {
  DINGO_LOG(DEBUG) << fmt::format("[delete.StoreStateMachine][id({})]", str_node_id_);
  bthread_mutex_destroy(&apply_mutex_);
}

bool StoreStateMachine::Init() { return true; }

void DoDispatchEvent(int64_t region_id, std::shared_ptr<EventListenerCollection> listeners,
                     dingodb::EventType event_type, std::shared_ptr<dingodb::Event> event, BthreadCondPtr cond) {
  DEFER(if (BAIDU_LIKELY(cond != nullptr)) { cond->DecreaseSignal(); });

  if (listeners == nullptr) return;

  for (auto& listener : listeners->Get(event_type)) {
    int ret = listener->OnEvent(event);
    if (ret != 0) {
      DINGO_LOG(FATAL) << fmt::format("[raft.sm][region({})] dispatch event({}) failed, ret: {}", region_id,
                                      static_cast<int>(event_type), ret);
      return;
    }
  }
}

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
  BAIDU_SCOPED_LOCK(apply_mutex_);

  for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard done_guard(iter.done());

    if (iter.index() <= applied_index_) {
      continue;
    }

    auto* done = dynamic_cast<BaseClosure*>(iter.done());
    auto ctx = done ? done->GetCtx() : nullptr;
    auto tracker = ctx ? ctx->Tracker() : nullptr;
    if (tracker != nullptr) {
      tracker->SetRaftCommitTime();
    }

    // Region is STANDBY state, wait to apply.
    while (region_->State() == pb::common::StoreRegionState::STANDBY) {
      DINGO_LOG(WARNING) << fmt::format("[raft.sm][region({})] region is standby for spliting, waiting...",
                                        region_->Id());
      bthread_usleep(1000 * 1000);
    }

    // Parse raft command
    auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
    if (iter.done()) {
      BaseClosure* store_closure = dynamic_cast<BaseClosure*>(iter.done());
      raft_cmd = store_closure->GetRequest();
    } else {
      butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
      CHECK(raft_cmd->ParseFromZeroCopyStream(&wrapper));
    }

    bool need_apply = true;
    // Check region state
    auto region_state = region_->State();
    if (BAIDU_UNLIKELY(region_state == pb::common::StoreRegionState::DELETING ||
                       region_state == pb::common::StoreRegionState::DELETED ||
                       region_state == pb::common::StoreRegionState::TOMBSTONE)) {
      std::string s = fmt::format("Region({}) is {} state, abandon apply log", region_->Id(),
                                  pb::common::StoreRegionState_Name(region_state));
      DINGO_LOG(WARNING) << fmt::format("[raft.sm][region({})] {}", region_->Id(), s);
      if (ctx != nullptr) {
        ctx->SetStatus(butil::Status(pb::error::EREGION_UNAVAILABLE, s));
      }
      need_apply = false;
    }

    // Check region epoch
    if (BAIDU_UNLIKELY(need_apply && !Helper::IsEqualRegionEpoch(raft_cmd->header().epoch(), region_->Epoch()))) {
      std::string s = fmt::format("Region({}) epoch is not match, region_epoch({}) raft_cmd_epoch({})", region_->Id(),
                                  region_->EpochToString(), Helper::RegionEpochToString(raft_cmd->header().epoch()));
      DINGO_LOG(WARNING) << fmt::format("[raft.sm][region({})] {}", region_->Id(), s);
      if (ctx != nullptr) {
        ctx->SetStatus(butil::Status(pb::error::EREGION_VERSION, s));
      }
      need_apply = false;
    }

    DINGO_LOG(DEBUG) << fmt::format(
        "[raft.sm][region({}).epoch({})] apply log {}:{} applied_index({}) cmd_type({})",
        raft_cmd->header().region_id(), Helper::RegionEpochToString(raft_cmd->header().epoch()), iter.term(),
        iter.index(), applied_index_,
        raft_cmd->requests().empty() ? "" : pb::raft::CmdType_Name(raft_cmd->requests().at(0).cmd_type()));

    if (need_apply) {
      // Build event
      auto event = std::make_shared<SmApplyEvent>();
      event->region = region_;
      event->engine = raw_engine_;
      event->done = iter.done();
      event->raft_cmd = raft_cmd;
      event->region_metrics = region_metrics_;
      event->term_id = iter.term();
      event->log_id = iter.index();

      if (BAIDU_LIKELY(raft_apply_worker_set_ != nullptr)) {
        // Run in queue.
        auto cond = std::make_shared<BthreadCond>();

        auto task = std::make_shared<DispatchEventTask>([this, event, cond, tracker]() {
          if (tracker != nullptr) {
            tracker->SetRaftQueueWaitTime();
          }
          DoDispatchEvent(region_->Id(), listeners_, EventType::kSmApply, event, cond);
        });

        bool ret = raft_apply_worker_set_->ExecuteRR(task);
        if (BAIDU_UNLIKELY(!ret)) {
          DINGO_LOG(FATAL) << fmt::format(
              "[raft.sm][region({})] execute apply task failed, downgrade to in_place execute", region_->Id());
          DispatchEvent(EventType::kSmApply, event);
        } else {
          cond->IncreaseWait();
        }
      } else {
        DispatchEvent(EventType::kSmApply, event);
      }
    }

    if (tracker != nullptr) {
      tracker->SetRaftApplyTime();
    }

    applied_term_ = iter.term();
    applied_index_ = iter.index();
    raft_meta_->SetTermAndAppliedId(applied_term_, applied_index_);

    // bvar metrics
    StoreBvarMetrics::GetInstance().IncApplyCountPerSecond(str_node_id_);

    // Persistence applied index
    // If operation is idempotent, it's ok.
    // If not, must be stored with the data.
    if (applied_index_ % kSaveAppliedIndexStep == 0) {
      Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta()->UpdateRaftMeta(raft_meta_);
    }
  }
}

int32_t StoreStateMachine::CatchUpApplyLog(const std::vector<pb::raft::LogEntry>& entries) {
  if (entries.empty()) {
    return 0;
  }
  if (entries[entries.size() - 1].index() <= applied_index_) {
    return 0;
  }

  uint64_t start_time = Helper::TimestampMs();
  int32_t actual_apply_log_count = 0;
  int64_t start_applied_id = 0;
  {
    BAIDU_SCOPED_LOCK(apply_mutex_);
    start_applied_id = applied_index_;

    for (const auto& entry : entries) {
      if (entry.index() <= applied_index_) {
        continue;
      }

      auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
      CHECK(raft_cmd->ParsePartialFromArray(entry.data().data(), entry.data().size()));

      DINGO_LOG(INFO) << fmt::format(
          "[raft.sm][region({}).epoch({})] apply log {}:{} applied_index({}) cmd_type({})",
          raft_cmd->header().region_id(), Helper::RegionEpochToString(raft_cmd->header().epoch()), entry.term(),
          entry.index(), applied_index_,
          raft_cmd->requests().empty() ? "" : pb::raft::CmdType_Name(raft_cmd->requests().at(0).cmd_type()));

      auto event = std::make_shared<SmApplyEvent>();
      event->region = region_;
      event->engine = raw_engine_;
      event->raft_cmd = raft_cmd;
      event->region_metrics = region_metrics_;
      event->term_id = entry.term();
      event->log_id = entry.index();

      DispatchEvent(EventType::kSmApply, event);

      applied_term_ = entry.term();
      applied_index_ = entry.index();

      raft_meta_->SetTermAndAppliedId(applied_term_, applied_index_);

      // bvar metrics
      StoreBvarMetrics::GetInstance().IncApplyCountPerSecond(str_node_id_);

      if (applied_index_ % kSaveAppliedIndexStep == 0) {
        Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta()->UpdateRaftMeta(raft_meta_);
      }

      ++actual_apply_log_count;
    }
  }

  DINGO_LOG(INFO) << fmt::format(
      "[raft.sm][region({})] catch up apply log finish, start_applied_id({}), apply_log_count({}/{}) elapsed "
      "time({})",
      region_->Id(), start_applied_id, actual_apply_log_count, entries.size(), Helper::TimestampMs() - start_time);

  return actual_apply_log_count;
}

std::shared_ptr<SnapshotContext> StoreStateMachine::MakeSnapshotContext() {
  BAIDU_SCOPED_LOCK(apply_mutex_);

  auto snapshot_ctx = std::make_shared<SnapshotContext>(raw_engine_);
  snapshot_ctx->applied_term = applied_term_;
  snapshot_ctx->applied_index = applied_index_;
  snapshot_ctx->region_epoch = region_->Epoch();
  snapshot_ctx->range = region_->Range();

  return snapshot_ctx;
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
  event->engine = raw_engine_;
  event->writer = writer;
  event->done = done;
  event->region = region_;
  event->term = applied_term_;
  event->log_index = applied_index_;

  DispatchEvent(EventType::kSmSnapshotSave, event);

  last_snapshot_index_ = applied_index_;

  if (raft_meta_ != nullptr) {
    Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta()->UpdateRaftMeta(raft_meta_);
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

  DINGO_LOG(INFO) << fmt::format("[raft.sm][region({})] on_snapshot_load snapshot({}-{}) applied_index({})",
                                 region_->Id(), meta.last_included_term(), meta.last_included_index(), applied_index_);

  std::string flag_filepath = reader->get_path() + "/" + Constant::kRaftSnapshotRegionMetaFileName;
  if (!Helper::IsExistPath(flag_filepath)) {
    DINGO_LOG(INFO) << fmt::format(
        "[raft.sm][region({})] on_snapshot_load, region_meta is not exists, this is a local snapshot, just skip load",
        region_->Id());
    return 0;
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
    event->engine = raw_engine_;
    event->reader = reader;
    event->region = region_;

    int ret = DispatchEvent(EventType::kSmSnapshotLoad, event);
    if (ret != 0) {
      return ret;
    }

    // Update applied term and index
    applied_term_ = meta.last_included_term();
    applied_index_ = meta.last_included_index();
    last_snapshot_index_ = meta.last_included_index();

    if (raft_meta_ != nullptr) {
      raft_meta_->SetTermAndAppliedId(meta.last_included_term(), meta.last_included_index());
      Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta()->UpdateRaftMeta(raft_meta_);
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

void StoreStateMachine::UpdateAppliedIndex(int64_t applied_index) { applied_index_ = applied_index; }

int64_t StoreStateMachine::GetAppliedIndex() const { return applied_index_; }

int64_t StoreStateMachine::GetLastSnapshotIndex() const { return last_snapshot_index_; }

}  // namespace dingodb

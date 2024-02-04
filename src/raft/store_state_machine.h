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

#ifndef DINGODB_RAFT_STATE_MACHINE_H_
#define DINGODB_RAFT_STATE_MACHINE_H_

#include <cstdint>
#include <string>
#include <vector>

#include "braft/raft.h"
#include "common/runnable.h"
#include "engine/raw_engine.h"
#include "event/event.h"
#include "meta/store_meta_manager.h"
#include "metrics/store_metrics_manager.h"
#include "proto/raft.pb.h"
#include "raft/state_machine.h"

namespace dingodb {

struct SnapshotContext;

class DispatchEventTask : public TaskRunnable {
 public:
  using Handler = std::function<void(void)>;
  DispatchEventTask(Handler handle) : handle_(handle) {}
  ~DispatchEventTask() override = default;

  std::string Type() override { return "STATE_MACHINE_TASK"; }

  void Run() override { handle_(); }

 private:
  Handler handle_;
};

// Execute order on restart: on_snapshot_load
//                           on_configuration_committed
//                           on_leader_start|on_start_following
//                           on_apply
class StoreStateMachine : public BaseStateMachine {
 public:
  explicit StoreStateMachine(std::shared_ptr<RawEngine> engine, store::RegionPtr region, store::RaftMetaPtr raft_meta,
                             store::RegionMetricsPtr region_metrics, std::shared_ptr<EventListenerCollection> listeners,
                             PriorWorkerSetPtr raft_apply_worker_set);
  ~StoreStateMachine() override;

  static bool Init();

  void on_apply(braft::Iterator& iter) override;
  void on_shutdown() override;
  void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override;
  int on_snapshot_load(braft::SnapshotReader* reader) override;
  void on_leader_start(int64_t term) override;
  void on_leader_stop(const butil::Status& status) override;
  void on_error(const braft::Error& e) override;
  void on_configuration_committed(const braft::Configuration& conf) override;
  void on_start_following(const braft::LeaderChangeContext& ctx) override;
  void on_stop_following(const braft::LeaderChangeContext& ctx) override;

  void UpdateAppliedIndex(int64_t applied_index);
  int64_t GetAppliedIndex() const override;

  int64_t GetLastSnapshotIndex() const override;

  int32_t CatchUpApplyLog(const std::vector<pb::raft::LogEntry>& entries);

  std::shared_ptr<SnapshotContext> MakeSnapshotContext();

 private:
  int DispatchEvent(dingodb::EventType, std::shared_ptr<dingodb::Event> event);

  store::RegionPtr region_;
  std::string str_node_id_;
  std::shared_ptr<RawEngine> raw_engine_;
  std::shared_ptr<EventListenerCollection> listeners_;

  int64_t applied_term_;
  int64_t applied_index_;
  int64_t last_snapshot_index_;
  store::RaftMetaPtr raft_meta_;

  store::RegionMetricsPtr region_metrics_;

  // Protect apply serial
  bthread_mutex_t apply_mutex_;

  // raft_apply_worker_set
  PriorWorkerSetPtr raft_apply_worker_set_;
};

}  // namespace dingodb

#endif  // DINGODB_RAFT_STATE_MACHINE_H_

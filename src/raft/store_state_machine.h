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

#include "braft/raft.h"
#include "brpc/controller.h"
#include "common/context.h"
#include "engine/raw_engine.h"
#include "event/event.h"
#include "proto/raft.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

class StoreClosure : public braft::Closure {
 public:
  StoreClosure(std::shared_ptr<Context> ctx, std::shared_ptr<pb::raft::RaftCmdRequest> request)
      : ctx_(ctx), request_(request) {}
  ~StoreClosure() override = default;

  void Run() override;

  std::shared_ptr<Context> GetCtx() { return ctx_; }
  std::shared_ptr<pb::raft::RaftCmdRequest> GetRequest() { return request_; }

 private:
  std::shared_ptr<Context> ctx_;
  std::shared_ptr<pb::raft::RaftCmdRequest> request_;
};

// Execute order on restart: on_snapshot_load
//                           on_configuration_committed
//                           on_leader_start|on_start_following
//                           on_apply
class StoreStateMachine : public braft::StateMachine {
 public:
  explicit StoreStateMachine(std::shared_ptr<RawEngine> engine, uint64_t node_id,
                             std::shared_ptr<EventListenerCollection> listeners);
  ~StoreStateMachine() = default;

  bool Init();

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

 private:
  void DispatchEvent(dingodb::EventType, std::shared_ptr<dingodb::Event> event);

  uint64_t node_id_;
  std::shared_ptr<RawEngine> engine_;
  std::shared_ptr<EventListenerCollection> listeners_;

  bool is_restart_;
  int64_t applied_index_;
  std::shared_ptr<pb::store_internal::RaftMeta> raft_meta_;
};

}  // namespace dingodb

#endif  // DINGODB_RAFT_STATE_MACHINE_H_

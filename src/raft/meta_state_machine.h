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

#ifndef DINGODB_META_STATE_MACHINE_H_
#define DINGODB_META_STATE_MACHINE_H_

#include <cstdint>
#include <memory>

#include "braft/raft.h"
#include "common/meta_control.h"
#include "proto/common.pb.h"
#include "proto/raft.pb.h"
#include "raft/state_machine.h"

namespace dingodb {

class MetaStateMachine : public BaseStateMachine {
 public:
  MetaStateMachine(std::shared_ptr<MetaControl> meta_control, bool is_volatile = false);
  void on_apply(braft::Iterator& iter) override;
  void on_shutdown() override;
  void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) override;
  int on_snapshot_load(braft::SnapshotReader* reader) override;
  void on_leader_start(int64_t term) override;
  void on_leader_stop(const butil::Status& status) override;
  void on_error(const ::braft::Error& e) override;
  void on_configuration_committed(const ::braft::Configuration& conf) override;
  void on_start_following(const ::braft::LeaderChangeContext& ctx) override;
  void on_stop_following(const ::braft::LeaderChangeContext& ctx) override;

  void SetVolatile(bool is_volatile_state_machine) { is_volatile_state_machine_ = is_volatile_state_machine; }

  int64_t GetAppliedIndex() const override;
  int64_t GetLastSnapshotIndex() const override;

 private:
  void DispatchRequest(bool is_leader, int64_t term, int64_t index, const pb::raft::RaftCmdRequest& raft_cmd,
                       google::protobuf::Message* response);
  void HandleMetaProcess(bool is_leader, int64_t term, int64_t index, const pb::raft::RaftCmdRequest& raft_cmd,
                         google::protobuf::Message* response);
  std::shared_ptr<MetaControl> meta_control_;
  bool is_volatile_state_machine_ = false;

  int64_t applied_term_;
  int64_t applied_index_;
  int64_t last_snapshot_index_;
};

}  // namespace dingodb

#endif  // DINGODB_META_STATE_MACHINE_H_

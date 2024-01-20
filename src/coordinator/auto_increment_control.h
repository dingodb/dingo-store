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

#ifndef DINGODB_AUTO_INCREMENT_CONTROL_H_
#define DINGODB_AUTO_INCREMENT_CONTROL_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "butil/containers/flat_map.h"
#include "common/meta_control.h"
#include "engine/engine.h"
#include "proto/coordinator_internal.pb.h"

namespace dingodb {
class AutoIncrementSnapshot : public dingodb::Snapshot {
 public:
  explicit AutoIncrementSnapshot(const butil::FlatMap<int64_t, int64_t> *snapshot) : snapshot_(snapshot) {}
  ~AutoIncrementSnapshot() override { delete snapshot_; };

  const void *Inner() override { return snapshot_; }
  const butil::FlatMap<int64_t, int64_t> *GetSnapshot() { return snapshot_; }

 private:
  const butil::FlatMap<int64_t, int64_t> *snapshot_;
};

class AutoIncrementControl : public MetaControl {
 public:
  AutoIncrementControl();
  ~AutoIncrementControl() override = default;

  template <typename T>
  void RedirectResponse(T response) {
    pb::common::Location leader_location;
    this->GetLeaderLocation(leader_location);

    auto *error_in_response = response->mutable_error();
    *(error_in_response->mutable_leader_location()) = leader_location;
    error_in_response->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);
    error_in_response->set_errmsg("not leader, new leader location is " + leader_location.DebugString());
  }

  static bool Init();
  static bool Recover();

  butil::Status GetAutoIncrements(butil::FlatMap<int64_t, int64_t> &auto_increments);
  butil::Status GetAutoIncrement(int64_t table_id, int64_t &start_id);
  butil::Status CreateAutoIncrement(int64_t table_id, int64_t start_id,
                                    pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status SyncCreateAutoIncrement(int64_t table_id, int64_t start_id);
  butil::Status SyncDeleteAutoIncrement(int64_t table_id);

  butil::Status UpdateAutoIncrement(int64_t table_id, int64_t start_id, bool force,
                                    pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status GenerateAutoIncrement(int64_t table_id, uint32_t count, uint32_t auto_increment_increment,
                                      uint32_t auto_increment_offset,
                                      pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status DeleteAutoIncrement(int64_t table_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // Get raft leader's server location
  void GetLeaderLocation(pb::common::Location &leader_server_location) override;

  // use raft_location to get server_location
  // in: raft_location
  // out: server_location
  void GetServerLocation(pb::common::Location &raft_location, pb::common::Location &server_location);

  // functions below are for raft fsm
  bool IsLeader() override;
  void SetLeaderTerm(int64_t term) override;
  void OnLeaderStart(int64_t term) override;
  void OnLeaderStop() override;

  // set raft_node to auto_increment_control
  void SetRaftNode(std::shared_ptr<RaftNode> raft_node) override;
  std::shared_ptr<RaftNode> GetRaftNode() override { return raft_node_; }

  // on_apply callback
  void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool is_leader, int64_t term,
                          int64_t index, google::protobuf::Message *response) override;

  int GetAppliedTermAndIndex(int64_t &term, int64_t &index) override;
  std::shared_ptr<Snapshot> PrepareRaftSnapshot() override;
  bool LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                              pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;
  bool LoadMetaFromSnapshotFile(pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;

  int SaveAutoIncrement(std::string &auto_increment_data);
  int LoadAutoIncrement(const std::string &auto_increment_file);

  static butil::Status CheckAutoIncrementInTableDefinition(const pb::meta::TableDefinition &table_definition,
                                                           bool &has_auto_increment_column);

  static butil::Status SyncSendCreateAutoIncrementInternal(int64_t table_id, int64_t auto_increment);
  static void AsyncSendUpdateAutoIncrementInternal(int64_t table_id, int64_t auto_increment);
  static void AsyncSendDeleteAutoIncrementInternal(int64_t table_id);

  void GetMemoryInfo(pb::coordinator::CoordinatorMemoryInfo &memory_info);

  void SetKvEngine(std::shared_ptr<Engine> engine) { engine_ = engine; };

 private:
  static int64_t GetGenerateEndId(int64_t start_id, uint32_t count, uint32_t increment, uint32_t offset);
  static int64_t GetRealStartId(int64_t start_id, uint32_t auto_increment_increment, uint32_t auto_increment_offset);

  butil::FlatMap<int64_t, int64_t> auto_increment_map_;
  bthread_mutex_t auto_increment_map_mutex_;

  // node is leader or not
  butil::atomic<int64_t> leader_term_;

  // raft node
  std::shared_ptr<RaftNode> raft_node_;

  // raft kv engine
  std::shared_ptr<Engine> engine_;

  // coordinator raft_location to server_location cache
  std::map<std::string, pb::common::Location> auto_increment_location_cache_;

  inline static const uint32_t kAutoIncrementGenerateCountMax = 100000;
  inline static const uint32_t kAutoIncrementOffsetMax = 65535;
};

}  // namespace dingodb

#endif  // DINGODB_AUTO_INCREMENT_CONTROL_H_

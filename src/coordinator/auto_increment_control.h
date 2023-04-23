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
#include <shared_mutex>
#include <string>

#include "common/meta_control.h"

namespace dingodb {

class AutoIncrementControl : public MetaControl {
 public:
  AutoIncrementControl();
  ~AutoIncrementControl() override {};

  bool Init();
  bool Recover();

  pb::error::Errno GetAutoIncrement(uint64_t table_id, uint64_t& start_id);
  pb::error::Errno CreateAutoIncrement(uint64_t table_id,
                        uint64_t start_id,
                        pb::coordinator_internal::MetaIncrement &meta_increment);
  pb::error::Errno UpdateAutoIncrement(uint64_t table_id,
                            uint64_t start_id,
                            bool force,
                            pb::coordinator_internal::MetaIncrement &meta_increment);
  pb::error::Errno GenerateAutoIncrement(uint64_t table_id,
                            uint32_t count,
                            uint32_t auto_increment_increment,
                            uint32_t auto_increment_offset,
                            pb::coordinator_internal::MetaIncrement &meta_increment);
  pb::error::Errno DeleteAutoIncrement(uint64_t table_id,
                            pb::coordinator_internal::MetaIncrement &meta_increment);

  // Get raft leader's server location
  void GetLeaderLocation(pb::common::Location* leader_server_location_ptr);

  // use raft_location to get server_location
  // in: raft_location
  // out: server_location
  void GetServerLocation(pb::common::Location &raft_location, pb::common::Location &server_location);

  // functions below are for raft fsm
  bool IsLeader() override;
  void SetLeaderTerm(int64_t term) override;
  void OnLeaderStart(int64_t term) override;

  // set raft_node to auto_increment_control
  void SetRaftNode(std::shared_ptr<RaftNode> raft_node) override;
  std::shared_ptr<RaftNode> GetRaftNode() override {
    return raft_node_;
  }

  // on_apply callback
  void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool is_leader,
                          uint64_t term, uint64_t index, google::protobuf::Message* response) override;

  int GetAppliedTermAndIndex(uint64_t &term, uint64_t &index) override {return 0;}
  std::shared_ptr<Snapshot> PrepareRaftSnapshot() override {return nullptr;}
  bool LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
       pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override {return true;}
  bool LoadMetaFromSnapshotFile(pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override {return true;}

  int SaveAutoIncrement(std::string& auto_increment_data);
  int LoadAutoIncrement(const std::string& auto_increment_file);

  static pb::error::Errno CheckAutoIncrementInTableDefinition(const pb::meta::TableDefinition& table_definition,
      bool& has_auto_increment_column);

  static butil::Status SendCreateAutoIncrementInternal(const uint64_t table_id, const uint64_t auto_increment);
  static void SendUpdateAutoIncrementInternal(const uint64_t table_id, const uint64_t auto_increment);
  static void SendDeleteAutoIncrementInternal(const uint64_t table_id);

 private:
  uint64_t GetGenerateEndId(uint64_t start_id, uint32_t count, uint32_t increment, uint32_t offset);
  uint64_t GetRealStartId(uint64_t start_id,
                          uint32_t auto_increment_increment,
                          uint32_t auto_increment_offset);

  butil::FlatMap<uint64_t, uint64_t> auto_increment_map_;
  bthread_mutex_t auto_increment_map_mutex_;

    // node is leader or not
  butil::atomic<int64_t> leader_term_;

  // raft node
  std::shared_ptr<RaftNode> raft_node_;

  // coordinator raft_location to server_location cache
  std::map<std::string, pb::common::Location> auto_increment_location_cache_;

  inline static const uint32_t kAutoIncrementGenerateCountMax = 100000;
  inline static const uint32_t kAutoIncrementOffsetMax = 65535;
};

}  // namespace dingodb

#endif  // DINGODB_AUTO_INCREMENT_CONTROL_H_


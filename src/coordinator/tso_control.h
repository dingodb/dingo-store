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

#ifndef DINGODB_TSO_CONTROL_H_
#define DINGODB_TSO_CONTROL_H_

#include <braft/repeated_timer_task.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "common/meta_control.h"
#include "engine/engine.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

constexpr int64_t kUpdateTimestampIntervalMs = 50LL;   // 50ms
constexpr int64_t kUpdateTimestampGuardMs = 1LL;       // 1ms
constexpr int64_t kSaveIntervalMs = 3000LL;            // 3000ms
constexpr int64_t kBaseTimestampMs = 1577808000000LL;  // 2020-01-01 12:00:00
constexpr int kLogicalBits = 18;
constexpr int64_t kMaxLogical = 1 << kLogicalBits;

inline int64_t ClockRealtimeMs() {
  struct timespec tp;
  ::clock_gettime(CLOCK_REALTIME, &tp);
  return tp.tv_sec * 1000ULL + tp.tv_nsec / 1000000ULL - kBaseTimestampMs;
}

inline uint32_t GetTimestampInternal(int64_t offset) { return ((offset >> 18) + kBaseTimestampMs) / 1000; }

class TimeCost {
 public:
  TimeCost() { start_ = butil::gettimeofday_us(); }

  ~TimeCost() = default;

  void Reset() { start_ = butil::gettimeofday_us(); }

  int64_t GetTime() const { return butil::gettimeofday_us() - start_; }

 private:
  int64_t start_;
};

class TsoControl;
struct TsoClosure : public braft::Closure {
  TsoClosure() : sync_cond(nullptr){};
  TsoClosure(BthreadCond *cond) : sync_cond(cond){};
  void Run() override;

  brpc::Controller *cntl;
  TsoControl *tso_control;
  google::protobuf::Closure *done;
  pb::meta::TsoResponse *response;
  int64_t raft_time_cost;
  int64_t total_time_cost;
  TimeCost time_cost;
  bool is_sync = false;
  BthreadCond *sync_cond;
};

class TsoTimer : public braft::RepeatedTimerTask {
 public:
  TsoTimer() = default;
  ~TsoTimer() override = default;
  int init(TsoControl *tso_control, int timeout_ms);
  void run() override;

 protected:
  void on_destroy() override {}
  TsoControl *tso_control{};
};

struct TsoObj {
  pb::meta::TsoTimestamp current_timestamp;
  int64_t last_save_physical;
};

class TsoSnapshot : public dingodb::Snapshot {
 public:
  explicit TsoSnapshot(const int64_t *snapshot) : snapshot_(snapshot) {}
  ~TsoSnapshot() override { delete snapshot_; };

  const void *Inner() override { return snapshot_; }
  const int64_t *GetSnapshot() { return snapshot_; }

 private:
  const int64_t *snapshot_;
};

class TsoControl : public MetaControl {
 public:
  TsoControl();
  ~TsoControl() override = default;

  template <typename T>
  void RedirectResponse(T response) {
    pb::common::Location leader_location;
    GetLeaderLocation(leader_location);

    auto *error_in_response = response->mutable_error();
    *(error_in_response->mutable_leader_location()) = leader_location;
    error_in_response->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);
    error_in_response->set_errmsg("not leader, new leader location is " + leader_location.DebugString());
  }

  void SetKvEngine(std::shared_ptr<Engine> engine) { engine_ = engine; };

  bool Init();
  static bool Recover();

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

  // set raft_node to tso_control
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

  // SubmitMetaIncrement
  // in:  meta_increment
  // return: 0 or -1
  butil::Status SubmitMetaIncrementAsync(google::protobuf::Closure *done, pb::meta::TsoResponse *response,
                                         pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status SubmitMetaIncrementSync(google::protobuf::Closure *done, pb::meta::TsoResponse *response,
                                        pb::coordinator_internal::MetaIncrement &meta_increment);

  void Process(google::protobuf::RpcController *controller, const pb::meta::TsoRequest *request,
               pb::meta::TsoResponse *response, google::protobuf::Closure *done);

  void GenTso(const pb::meta::TsoRequest *request, pb::meta::TsoResponse *response);
  void ResetTso(const pb::meta::TsoRequest &request, pb::meta::TsoResponse *response);
  void UpdateTso(const pb::meta::TsoRequest &request, pb::meta::TsoResponse *response);

  int SyncTimestamp(const pb::meta::TsoTimestamp &current_timestamp, int64_t save_physical);
  void UpdateTimestamp();
  void OnApply(braft::Iterator &iter);

 private:
  TsoTimer tso_update_timer_;
  TsoObj tso_obj_;
  bthread_mutex_t tso_mutex_;  // for tso_obj_
  bool is_healty_ = true;

  // node is leader or not
  butil::atomic<int64_t> leader_term_;

  // raft node
  std::shared_ptr<RaftNode> raft_node_;

  // coordinator raft_location to server_location cache
  std::map<std::string, pb::common::Location> tso_location_cache_;

  // raft kv engine
  std::shared_ptr<Engine> engine_;
};

}  // namespace dingodb

#endif  // DINGODB_TSO_CONTROL_H_

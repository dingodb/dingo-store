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

#ifndef DINGODB_KV_CONTROL_H_
#define DINGODB_KV_CONTROL_H_

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "butil/status.h"
#include "common/meta_control.h"
#include "common/safe_map.h"
#include "coordinator/coordinator_meta_storage.h"
#include "engine/engine.h"
#include "engine/snapshot.h"
#include "google/protobuf/stubs/callback.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "proto/version.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

struct KvWatchNode {
  KvWatchNode(uint64_t closure_id_in)
      : closure_id(closure_id_in),
        start_revision(0),
        no_put_event(false),
        no_delete_event(false),
        need_prev_kv(false) {}

  KvWatchNode(uint64_t closure_id_in, int64_t start_revision, bool no_put_event, bool no_delete_event,
              bool need_prev_kv)
      : closure_id(closure_id_in),
        start_revision(start_revision),
        no_put_event(no_put_event),
        no_delete_event(no_delete_event),
        need_prev_kv(need_prev_kv) {}

  uint64_t closure_id;
  int64_t start_revision;
  bool no_put_event;
  bool no_delete_event;
  bool need_prev_kv;
};

class DeferDone {
 public:
  DeferDone() {
    // bthread_mutex_init(&mutex_, nullptr);
    create_time_ = butil::gettimeofday_ms();
  }
  DeferDone(uint64_t closure_id, const std::string &watch_key, google::protobuf::Closure *done,
            pb::version::WatchResponse *response)
      : closure_id_(closure_id), watch_key_(watch_key), done_(done), response_(response) {
    create_time_ = butil::gettimeofday_ms();
    // bthread_mutex_init(&mutex_, nullptr);
  }

  ~DeferDone() = default;

  bool IsDone() {
    // bthread_mutex_lock(&mutex_);
    bool is_done = done_ == nullptr;
    // bthread_mutex_unlock(&mutex_);
    return is_done;
  }

  std::string GetWatchKey() { return watch_key_; }
  pb::version::WatchResponse *GetResponse() {
    // bthread_mutex_lock(&mutex_);
    auto *response = response_;
    // bthread_mutex_unlock(&mutex_);
    return response;
  }

  int64_t GetStartTime() const { return create_time_; }

  void Done() {
    // bthread_mutex_lock(&mutex_);
    if (done_) {
      braft::AsyncClosureGuard done_guard(done_);
      done_ = nullptr;
      response_ = nullptr;
      DINGO_LOG(INFO) << "Deferred done_first closure_id=" << closure_id_ << " watch_key=" << watch_key_
                      << " cost: " << butil::gettimeofday_ms() - create_time_ << " ms, create_time: " << create_time_;
    } else {
      DINGO_LOG(INFO) << "Deferred done_again closure_id=" << closure_id_ << " watch_key=" << watch_key_
                      << " cost: " << butil::gettimeofday_ms() - create_time_ << " ms, create_time: " << create_time_;
    }
    // bthread_mutex_unlock(&mutex_);
  }

 private:
  uint64_t closure_id_;
  std::string watch_key_;
  bthread_mutex_t mutex_;
  google::protobuf::Closure *done_;
  pb::version::WatchResponse *response_;
  int64_t create_time_;
};

struct KvLeaseWithKeys {
  pb::coordinator_internal::LeaseInternal lease;
  std::set<std::string> keys;
};

class KvControl : public MetaControl {
 public:
  KvControl(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer,
            std::shared_ptr<RawEngine> raw_engine_of_meta);
  ~KvControl() override;
  bool Recover();
  void SetKvEngine(std::shared_ptr<Engine> engine) { engine_ = engine; };

  static bool Init() {
    DINGO_LOG(INFO) << "KvControl init";
    return true;
  }

  template <typename T>
  void RedirectResponse(T response) {
    pb::common::Location leader_location;
    GetLeaderLocation(leader_location);

    auto *error_in_response = response->mutable_error();
    *(error_in_response->mutable_leader_location()) = leader_location;
    error_in_response->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);
    error_in_response->set_errmsg("not leader, new leader location is " + leader_location.DebugString());
  }

  // SubmitMetaIncrement
  // in:  meta_increment
  // return: 0 or -1
  butil::Status SubmitMetaIncrementAsync(google::protobuf::Closure *done,
                                         pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status SubmitMetaIncrementSync(pb::coordinator_internal::MetaIncrement &meta_increment);

  // GetMemoryInfo
  void GetMemoryInfo(pb::coordinator::CoordinatorMemoryInfo &memory_info);

  // Get raft leader's server location for sdk use
  void GetLeaderLocation(pb::common::Location &leader_server_location) override;

  // use raft_location to get server_location
  // in: raft_location
  // out: server_location
  void GetServerLocation(pb::common::Location &raft_location, pb::common::Location &server_location);
  void GetRaftLocation(pb::common::Location &server_location, pb::common::Location &raft_location);

  // get next id/epoch
  int64_t GetNextId(const pb::coordinator_internal::IdEpochType &key,
                    pb::coordinator_internal::MetaIncrement &meta_increment);

  // get present id/epoch
  int64_t GetPresentId(const pb::coordinator_internal::IdEpochType &key);

  // update present id/epoch
  int64_t UpdatePresentId(const pb::coordinator_internal::IdEpochType &key, int64_t new_id,
                          pb::coordinator_internal::MetaIncrement &meta_increment);

  // init ids
  void InitIds();

  // functions below are for raft fsm
  bool IsLeader() override;                                            // for raft fsm
  void SetLeaderTerm(int64_t term) override;                           // for raft fsm
  void OnLeaderStart(int64_t term) override;                           // for raft fsm
  void OnLeaderStop() override;                                        // for raft fsm
  int GetAppliedTermAndIndex(int64_t &term, int64_t &index) override;  // for raft fsm

  void BuildTempMaps();

  // set raft_node to kv_control
  void SetRaftNode(std::shared_ptr<RaftNode> raft_node) override;  // for raft fsm
  std::shared_ptr<RaftNode> GetRaftNode() override;                // for raft fsm

  // on_apply callback
  void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool is_leader, int64_t term,
                          int64_t index, google::protobuf::Message *response) override;  // for raft fsm

  // prepare snapshot for raft snapshot
  // return: Snapshot
  std::shared_ptr<Snapshot> PrepareRaftSnapshot() override;  // for raft fsm

  // LoadMetaToSnapshotFile
  bool LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                              pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;  // for raft fsm

  // LoadMetaFromSnapshotFile
  bool LoadMetaFromSnapshotFile(
      pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;  // for raft fsm

  static void KvLogMetaIncrementSize(pb::coordinator_internal::MetaIncrement &meta_increment);

  // lease timeout/revoke task
  void LeaseTask();

  // lease timeout/revoke task
  void CompactionTask();

  // lease
  butil::Status LeaseGrant(int64_t lease_id, int64_t ttl_seconds, int64_t &granted_id, int64_t &granted_ttl_seconds,
                           pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status LeaseRenew(int64_t lease_id, int64_t &ttl_seconds,
                           pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status LeaseRevoke(int64_t lease_id, pb::coordinator_internal::MetaIncrement &meta_increment,
                            bool has_mutex_locked = false);
  butil::Status ListLeases(std::vector<pb::coordinator_internal::LeaseInternal> &leases);
  butil::Status LeaseQuery(int64_t lease_id, bool get_keys, int64_t &granted_ttl_seconds,
                           int64_t &remaining_ttl_seconds, std::set<std::string> &keys);
  void BuildLeaseToKeyMap();
  butil::Status LeaseAddKeys(int64_t lease_id, std::set<std::string> &keys);
  butil::Status LeaseRemoveKeys(int64_t lease_id, std::set<std::string> &keys);
  butil::Status LeaseRemoveMultiLeaseKeys(std::map<int64_t, std::set<std::string>> &lease_to_keys);

  // revision encode and decode
  static std::string RevisionToString(const pb::coordinator_internal::RevisionInternal &revision);
  static pb::coordinator_internal::RevisionInternal StringToRevision(const std::string &input_string);

  // raw kv functions
  butil::Status RangeRawKvIndex(const std::string &key, const std::string &range_end,
                                std::vector<pb::coordinator_internal::KvIndexInternal> &kv_index_values);
  butil::Status GetRawKvIndex(const std::string &key, pb::coordinator_internal::KvIndexInternal &kv_index);
  butil::Status PutRawKvIndex(const std::string &key, const pb::coordinator_internal::KvIndexInternal &kv_index);
  butil::Status DeleteRawKvIndex(const std::string &key);
  butil::Status GetRawKvRev(const pb::coordinator_internal::RevisionInternal &revision,
                            pb::coordinator_internal::KvRevInternal &kv_rev);
  butil::Status PutRawKvRev(const pb::coordinator_internal::RevisionInternal &revision,
                            const pb::coordinator_internal::KvRevInternal &kv_rev);
  butil::Status DeleteRawKvRev(const pb::coordinator_internal::RevisionInternal &revision);

  // kv functions for api
  // KvRange is the get function
  // in:  key
  // in:  range_end
  // in:  limit
  // in:  keys_only
  // in:  count_only
  // out: kv
  // return: errno
  butil::Status KvRange(const std::string &key, const std::string &range_end, int64_t limit, bool keys_only,
                        bool count_only, std::vector<pb::version::Kv> &kv, int64_t &return_count, bool &has_more);

  // kv functions for internal use
  // KvRange is the get function
  // in:  key
  // in:  range_end
  // out: keys
  // return: errno
  butil::Status KvRangeRawKeys(const std::string &key, const std::string &range_end, std::vector<std::string> &keys);

  // KvPut is the put function
  // in:  key_value
  // in:  lease_id
  // in:  prev_kv
  // in:  igore_value
  // in:  ignore_lease
  // out:  prev_kv
  // return: errno
  butil::Status KvPut(const pb::common::KeyValue &key_value_in, int64_t lease_id, bool need_prev_kv, bool ignore_value,
                      bool ignore_lease, pb::version::Kv &prev_kv, int64_t &lease_grant_id,
                      pb::coordinator_internal::MetaIncrement &meta_increment);

  // KvDeleteRange is the delete function
  // in:  key
  // in:  range_end
  // in:  prev_key
  // in:  need_lease_remove_keys
  // out:  deleted_count
  // out:  prev_kvs
  // return: errno
  butil::Status KvDeleteRange(const std::string &key, const std::string &range_end, bool need_prev_kv,
                              bool need_lease_remove_keys, int64_t &deleted_count,
                              std::vector<pb::version::Kv> &prev_kvs,
                              pb::coordinator_internal::MetaIncrement &meta_increment);

  // KvPutApply is the apply function for put
  butil::Status KvPutApply(const std::string &key, const pb::coordinator_internal::RevisionInternal &op_revision,
                           bool ignore_lease, int64_t lease_id, bool ignore_value, const std::string &value);

  // KvDeleteRangeApply is the apply function for delete
  butil::Status KvDeleteApply(const std::string &key, const pb::coordinator_internal::RevisionInternal &op_revision);

  // KvCompact is the compact function
  butil::Status KvCompact(const std::vector<std::string> &keys,
                          const pb::coordinator_internal::RevisionInternal &compact_revision);

  // KvCompactApply is the apply function for delete
  butil::Status KvCompactApply(const std::string &key,
                               const pb::coordinator_internal::RevisionInternal &compact_revision);

  // watch functions for api
  butil::Status OneTimeWatch(const std::string &watch_key, int64_t start_revision, bool no_put_event,
                             bool no_delete_event, bool need_prev_kv, bool wait_on_not_exist_key,
                             google::protobuf::Closure *done, pb::version::WatchResponse *response,
                             brpc::Controller *cntl);

  // add watch to map
  butil::Status AddOneTimeWatch(const std::string &watch_key, int64_t start_revision, bool no_put_event,
                                bool no_delete_event, bool need_prev_kv, uint64_t closure_id);
  // remove watch from map
  butil::Status RemoveOneTimeWatch();
  butil::Status RemoveOneTimeWatch(uint64_t closure_id);
  butil::Status RemoveOneTimeWatchWithLock(uint64_t closure_id);
  butil::Status CancelOneTimeWatchClosure(uint64_t closure_id);

  // watch functions for raft fsm
  butil::Status TriggerOneWatch(const std::string &key, pb::version::Event::EventType event_type,
                                pb::version::Kv &new_kv, pb::version::Kv &prev_kv);

 private:
  // deprecated, will removed in the future
  // ids_epochs_temp (out of state machine, only for leader use)
  // DingoSafeIdEpochMap id_epoch_map_safe_temp_;

  // 0.ids_epochs
  // TableInternal is combination of Table & TableDefinition
  DingoSafeIdEpochMap id_epoch_map_;
  MetaMemMapFlat<pb::coordinator_internal::IdEpochInternal> *id_epoch_meta_;

  // 14.lease
  DingoSafeMap<int64_t, pb::coordinator_internal::LeaseInternal> kv_lease_map_;
  MetaMemMapFlat<pb::coordinator_internal::LeaseInternal> *kv_lease_meta_;
  std::map<int64_t, KvLeaseWithKeys>
      lease_to_key_map_temp_;  // storage lease_id to key map, this map is built in on_leader_start
  bthread_mutex_t lease_to_key_map_temp_mutex_;

  // 15.version kv with lease
  DingoSafeStdMap<std::string, pb::coordinator_internal::KvIndexInternal> kv_index_map_;
  MetaMemMapStd<pb::coordinator_internal::KvIndexInternal> *kv_index_meta_;

  // 16.version kv multi revision
  MetaDiskMap<pb::coordinator_internal::KvRevInternal> *kv_rev_meta_;

  // one time watch map
  // this map on work on leader, is out of state machine
  std::map<std::string, std::map<uint64_t, KvWatchNode>> one_time_watch_map_;
  bthread_mutex_t one_time_watch_map_mutex_;
  std::map<uint64_t, DeferDone> one_time_watch_closure_map_;
  std::atomic<uint64_t> one_time_watch_closure_seq_{1000};  // used to generate unique closure id
  DingoSafeStdMap<uint64_t, bool> one_time_watch_closure_status_map_;

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  // node is leader or not
  butil::atomic<int64_t> leader_term_;

  // raft node
  std::shared_ptr<RaftNode> raft_node_;

  // coordinator raft_location to server_location cache
  std::map<std::string, pb::common::Location> coordinator_location_cache_;

  // raw_engine for state_machine storage
  std::shared_ptr<RawEngine> raw_engine_of_meta_;

  // raft kv engine
  std::shared_ptr<Engine> engine_;
};

}  // namespace dingodb

#endif  // DINGODB_KV_CONTROL_H_

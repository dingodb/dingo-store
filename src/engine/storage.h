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

#ifndef DINGODB_ENGINE_STORAGE_H_
#define DINGODB_ENGINE_STORAGE_H_

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/context.h"
#include "engine/engine.h"
#include "engine/raft_store_engine.h"
#include "memory"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

class Storage {
 public:
  Storage(std::shared_ptr<Engine> engine);
  ~Storage();

  static Snapshot* GetSnapshot();
  void ReleaseSnapshot();

  // kv
  butil::Status KvGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                      std::vector<pb::common::KeyValue>& kvs);

  butil::Status KvPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs);

  butil::Status KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                              bool is_atomic);

  butil::Status KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys);

  butil::Status KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range);

  butil::Status KvCompareAndSet(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                const std::vector<std::string>& expect_values, bool is_atomic);

  butil::Status KvScanBegin(std::shared_ptr<Context> ctx, const std::string& cf_name, int64_t region_id,
                            const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                            bool disable_auto_release, bool disable_coprocessor,
                            const pb::store::Coprocessor& coprocessor, std::string* scan_id,
                            std::vector<pb::common::KeyValue>* kvs);

  static butil::Status KvScanContinue(std::shared_ptr<Context> ctx, const std::string& scan_id, int64_t max_fetch_cnt,
                                      std::vector<pb::common::KeyValue>* kvs);

  static butil::Status KvScanRelease(std::shared_ptr<Context> ctx, const std::string& scan_id);

  // txn
  butil::Status TxnBatchGet(std::shared_ptr<Context> ctx, uint64_t start_ts, const std::vector<std::string>& keys,
                            pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs);
  butil::Status TxnScan(std::shared_ptr<Context> ctx, uint64_t start_ts, const pb::common::Range& range, uint64_t limit,
                        bool key_only, bool is_reverse, bool disable_coprocessor,
                        const pb::store::Coprocessor& coprocessor, pb::store::TxnResultInfo& txn_result_info,
                        std::vector<pb::common::KeyValue>& kvs, bool& has_more, std::string& end_key);
  // store prewrite
  butil::Status TxnPrewrite(std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations,
                            const std::string& primary_lock, uint64_t start_ts, uint64_t lock_ttl, uint64_t txn_size,
                            bool try_one_pc, uint64_t max_commit_ts, pb::store::TxnResultInfo& txn_result_info,
                            std::vector<std::string> already_exist, uint64_t& one_pc_commit_ts);
  // index prewrite
  butil::Status TxnPrewrite(std::shared_ptr<Context> ctx, const std::vector<pb::index::Mutation>& mutations,
                            const std::string& primary_lock, uint64_t start_ts, uint64_t lock_ttl, uint64_t txn_size,
                            bool try_one_pc, uint64_t max_commit_ts, pb::store::TxnResultInfo& txn_result_info,
                            std::vector<std::string> already_exist, uint64_t& one_pc_commit_ts);
  butil::Status TxnCommit(std::shared_ptr<Context> ctx, uint64_t start_ts, uint64_t commit_ts,
                          const std::vector<std::string>& keys, pb::store::TxnResultInfo& txn_result_info,
                          uint64_t& committed_ts);
  butil::Status TxnCheckTxnStatus(std::shared_ptr<Context> ctx, const std::string& primary_key, uint64_t lock_ts,
                                  uint64_t caller_start_ts, uint64_t current_ts,
                                  pb::store::TxnResultInfo& txn_result_info, uint64_t& lock_ttl, uint64_t& commit_ts,
                                  pb::store::Action& action, pb::store::LockInfo& lock_info);
  butil::Status TxnResolveLock(std::shared_ptr<Context> ctx, uint64_t start_ts, uint64_t commit_ts,
                               std::vector<std::string>& keys, pb::store::TxnResultInfo& txn_result_info);
  butil::Status TxnBatchRollback(std::shared_ptr<Context> ctx, uint64_t start_ts, const std::vector<std::string>& keys,
                                 pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs);
  butil::Status TxnScanLock(std::shared_ptr<Context> ctx, uint64_t max_ts, const std::string& start_key, uint64_t limit,
                            const std::string& end_key, pb::store::TxnResultInfo& txn_result_info,
                            std::vector<pb::store::LockInfo>& locks);
  butil::Status TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock, uint64_t start_ts,
                             uint64_t advise_lock_ttl, pb::store::TxnResultInfo& txn_result_info, uint64_t& lock_ttl);
  butil::Status TxnGc(std::shared_ptr<Context> ctx, uint64_t safe_point_ts, pb::store::TxnResultInfo& txn_result_info);
  butil::Status TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key);
  butil::Status TxnDump(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                        uint64_t start_ts, uint64_t end_ts, pb::store::TxnResultInfo& txn_result_info,
                        std::vector<pb::store::TxnWriteKey>& txn_write_keys,
                        std::vector<pb::store::TxnWriteValue>& txn_write_values,
                        std::vector<pb::store::TxnLockKey>& txn_lock_keys,
                        std::vector<pb::store::TxnLockValue>& txn_lock_values,
                        std::vector<pb::store::TxnDataKey>& txn_data_keys,
                        std::vector<pb::store::TxnDataValue>& txn_data_values);

  // vector index
  butil::Status VectorAdd(std::shared_ptr<Context> ctx, const std::vector<pb::common::VectorWithId>& vectors);
  butil::Status VectorDelete(std::shared_ptr<Context> ctx, const std::vector<int64_t>& ids);

  butil::Status VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                 std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                  std::vector<pb::index::VectorWithDistanceResult>& results);
  butil::Status VectorGetBorderId(int64_t region_id, const pb::common::Range& region_range, bool get_min,
                                  int64_t& vector_id);
  butil::Status VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status VectorGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                       VectorIndexWrapperPtr vector_index_wrapper,
                                       pb::common::VectorIndexMetrics& region_metrics);

  butil::Status VectorCount(int64_t region_id, const pb::common::Range& range, int64_t& count);

  static butil::Status VectorCalcDistance(
      const ::dingodb::pb::index::VectorCalcDistanceRequest& request,
      std::vector<std::vector<float>>& distances,                            // NOLINT
      std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,    // NOLINT
      std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors);  // NOLINT

  // This function is for testing only
  butil::Status VectorBatchSearchDebug(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                       std::vector<pb::index::VectorWithDistanceResult>& results,
                                       int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                       int64_t& search_time_us);

  butil::Status ValidateLeader(int64_t region_id);

  bool ExecuteRR(int64_t /*region_id*/, TaskRunnablePtr task);
  bool ExecuteHash(int64_t region_id, TaskRunnablePtr task);

  // increase the number of tasks in workers_
  void IncTaskCount();
  // decrease the number of tasks in workers_
  void DecTaskCount();

 private:
  std::shared_ptr<Engine> engine_;
  std::atomic<uint64_t> active_worker_id_;
  std::vector<WorkerPtr> workers_;           // this is for long-time request processing, for instance VectorBatchSearch
  bvar::Adder<int64_t> workers_task_count_;  // this is used to monitor the number of tasks in workers_
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_STORAGE_H

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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/context.h"
#include "engine/engine.h"
#include "engine/raft_store_engine.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

class Storage {
 public:
  Storage(std::shared_ptr<Engine> engine);
  ~Storage() = default;

  std::shared_ptr<Engine> GetEngine();
  std::shared_ptr<RaftStoreEngine> GetRaftStoreEngine();

  static Snapshot* GetSnapshot();
  void ReleaseSnapshot();

  // kv read
  butil::Status KvGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                      std::vector<pb::common::KeyValue>& kvs);

  butil::Status KvScanBegin(std::shared_ptr<Context> ctx, const std::string& cf_name, int64_t region_id,
                            const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                            bool disable_auto_release, bool disable_coprocessor,
                            const pb::store::Coprocessor& coprocessor, std::string* scan_id,
                            std::vector<pb::common::KeyValue>* kvs);

  static butil::Status KvScanContinue(std::shared_ptr<Context> ctx, const std::string& scan_id, int64_t max_fetch_cnt,
                                      std::vector<pb::common::KeyValue>* kvs, bool& has_more);

  static butil::Status KvScanRelease(std::shared_ptr<Context> ctx, const std::string& scan_id);

  butil::Status KvScanBeginV2(std::shared_ptr<Context> ctx, const std::string& cf_name, int64_t region_id,
                              const pb::common::Range& range, int64_t max_fetch_cnt, bool key_only,
                              bool disable_auto_release, bool disable_coprocessor,
                              const pb::common::CoprocessorV2& coprocessor, int64_t scan_id,
                              std::vector<pb::common::KeyValue>* kvs);

  static butil::Status KvScanContinueV2(std::shared_ptr<Context> ctx, int64_t scan_id, int64_t max_fetch_cnt,
                                        std::vector<pb::common::KeyValue>* kvs, bool& has_more);

  static butil::Status KvScanReleaseV2(std::shared_ptr<Context> ctx, int64_t scan_id);

  // kv write
  butil::Status KvPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs);

  butil::Status KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                              bool is_atomic, std::vector<bool>& key_states);

  butil::Status KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys);

  butil::Status KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range);

  butil::Status KvCompareAndSet(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                const std::vector<std::string>& expect_values, bool is_atomic,
                                std::vector<bool>& key_states);

  // txn reader
  butil::Status TxnBatchGet(std::shared_ptr<Context> ctx, int64_t start_ts, const std::vector<std::string>& keys,
                            const std::set<int64_t>& resolved_locks, pb::store::TxnResultInfo& txn_result_info,
                            std::vector<pb::common::KeyValue>& kvs);
  butil::Status TxnScan(std::shared_ptr<Context> ctx, int64_t start_ts, const pb::common::Range& range, int64_t limit,
                        bool key_only, bool is_reverse, const std::set<int64_t>& resolved_locks,
                        pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,
                        bool& has_more, std::string& end_scan_key, bool disable_coprocessor,
                        const pb::common::CoprocessorV2& coprocessor);
  butil::Status TxnScanLock(std::shared_ptr<Context> ctx, int64_t max_ts, const pb::common::Range& range, int64_t limit,
                            pb::store::TxnResultInfo& txn_result_info, std::vector<pb::store::LockInfo>& lock_infos,
                            bool& has_more, std::string& end_scan_key);
  butil::Status TxnDump(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                        int64_t start_ts, int64_t end_ts, pb::store::TxnResultInfo& txn_result_info,
                        std::vector<pb::store::TxnWriteKey>& txn_write_keys,
                        std::vector<pb::store::TxnWriteValue>& txn_write_values,
                        std::vector<pb::store::TxnLockKey>& txn_lock_keys,
                        std::vector<pb::store::TxnLockValue>& txn_lock_values,
                        std::vector<pb::store::TxnDataKey>& txn_data_keys,
                        std::vector<pb::store::TxnDataValue>& txn_data_values);
  // txn writer
  butil::Status TxnPessimisticLock(std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations,
                                   const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl,
                                   int64_t for_update_ts);
  butil::Status TxnPessimisticRollback(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t for_update_ts,
                                       const std::vector<std::string>& keys);
  butil::Status TxnPrewrite(std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation>& mutations,
                            const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size,
                            bool try_one_pc, int64_t max_commit_ts, const std::vector<int64_t>& pessimistic_checks,
                            const std::map<int64_t, int64_t>& for_update_ts_checks,
                            const std::map<int64_t, std::string>& lock_extra_datas);
  butil::Status TxnCommit(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                          const std::vector<std::string>& keys);
  butil::Status TxnBatchRollback(std::shared_ptr<Context> ctx, int64_t start_ts, const std::vector<std::string>& keys);
  butil::Status TxnCheckTxnStatus(std::shared_ptr<Context> ctx, const std::string& primary_key, int64_t lock_ts,
                                  int64_t caller_start_ts, int64_t current_ts);
  butil::Status TxnResolveLock(std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                               const std::vector<std::string>& keys);
  butil::Status TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock, int64_t start_ts,
                             int64_t advise_lock_ttl);
  butil::Status TxnGc(std::shared_ptr<Context> ctx, int64_t safe_point_ts);
  butil::Status TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key);

  // vector index
  butil::Status VectorAdd(std::shared_ptr<Context> ctx, bool is_sync,
                          const std::vector<pb::common::VectorWithId>& vectors);
  butil::Status VectorDelete(std::shared_ptr<Context> ctx, bool is_sync, const std::vector<int64_t>& ids);

  butil::Status VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                 std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                  std::vector<pb::index::VectorWithDistanceResult>& results);
  butil::Status VectorGetBorderId(store::RegionPtr region, bool get_min, int64_t& vector_id);
  butil::Status VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status VectorGetRegionMetrics(store::RegionPtr region, VectorIndexWrapperPtr vector_index_wrapper,
                                       pb::common::VectorIndexMetrics& region_metrics);

  butil::Status VectorCount(store::RegionPtr region, pb::common::Range range, int64_t& count);

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
  bool IsLeader(int64_t region_id);

  butil::Status PrepareMerge(std::shared_ptr<Context> ctx, int64_t job_id,
                             const pb::common::RegionDefinition& region_definition, int64_t min_applied_log_id);
  butil::Status CommitMerge(std::shared_ptr<Context> ctx, int64_t job_id,
                            const pb::common::RegionDefinition& region_definition, int64_t prepare_merge_log_id,
                            const std::vector<pb::raft::LogEntry>& entries);

 private:
  std::shared_ptr<Engine> engine_;
};

using StoragePtr = std::shared_ptr<Storage>;

}  // namespace dingodb

#endif  // DINGODB_ENGINE_STORAGE_H

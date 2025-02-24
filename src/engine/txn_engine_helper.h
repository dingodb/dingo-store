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

#ifndef DINGODB_TXN_ENGINE_HELPER_H_
#define DINGODB_TXN_ENGINE_HELPER_H_

#include <sys/stat.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "engine/snapshot.h"
#include "meta/store_meta_manager.h"
#include "proto/store.pb.h"

namespace dingodb {

class TxnReader {
 public:
  TxnReader(RawEnginePtr raw_engine) : raw_engine_(raw_engine) {}

  TxnReader(RawEnginePtr raw_engine, SnapshotPtr snapshot) : raw_engine_(raw_engine), snapshot_(snapshot) {}

  ~TxnReader() = default;

  butil::Status Init();
  butil::Status GetLockInfo(const std::string &key, pb::store::LockInfo &lock_info);
  butil::Status GetDataValue(const std::string &key, std::string &value);
  butil::Status GetWriteInfo(int64_t min_commit_ts, int64_t max_commit_ts, int64_t start_ts, const std::string &key,
                             bool include_rollback, bool include_delete, bool include_put,
                             pb::store::WriteInfo &write_info, int64_t &commit_ts);
  butil::Status GetRollbackInfo(int64_t start_ts, const std::string &key, pb::store::WriteInfo &write_info);

  butil::Status GetOldValue(const std::string &key, int64_t start_ts, bool prev_write_load,
                            pb::store::WriteInfo &write_info, std::vector<pb::common::KeyValue> &kvs);
  std::shared_ptr<Iterator> GetWriteIter() { return write_iter_; }
  SnapshotPtr GetSnapshot() { return snapshot_; }

 private:
  bool is_initialized_{false};
  RawEnginePtr raw_engine_;
  SnapshotPtr snapshot_;
  RawEngine::ReaderPtr reader_;

  std::shared_ptr<Iterator> write_iter_;
};

class TxnIterator {
 public:
  TxnIterator(RawEnginePtr raw_engine, const pb::common::Range &range, int64_t start_ts,
              pb::store::IsolationLevel isolation_level, const std::set<int64_t> &resolved_locks)
      : raw_engine_(raw_engine),
        range_(range),
        isolation_level_(isolation_level),
        start_ts_(start_ts),
        resolved_locks_(resolved_locks) {
    if (isolation_level == pb::store::IsolationLevel::ReadCommitted) {
      seek_ts_ = Constant::kMaxVer;
    } else {
      seek_ts_ = start_ts;
    }
  }

  ~TxnIterator() = default;
  butil::Status Init();
  butil::Status Seek(const std::string &key);
  butil::Status InnerSeek(const std::string &key);
  butil::Status Next();
  butil::Status InnerNext();
  bool Valid(pb::store::TxnResultInfo &txn_result_info);
  std::string Key();
  std::string Value();

  std::string GetLastLockKey() { return last_lock_key_; }
  std::string GetLastWriteKey() { return last_write_key_; }

  static butil::Status GetUserValueInWriteIter(std::shared_ptr<Iterator> write_iter, RawEngine::ReaderPtr reader,
                                               pb::store::IsolationLevel isolation_level, int64_t seek_ts,
                                               int64_t start_ts, const std::string &user_key,
                                               std::string &last_write_key, bool &is_value_found,
                                               std::string &user_value);
  static std::string GetUserKey(std::shared_ptr<Iterator> write_iter);
  static butil::Status GotoNextUserKeyInWriteIter(std::shared_ptr<Iterator> write_iter, std::string prev_user_key,
                                                  std::string &last_write_key);

 private:
  butil::Status GetCurrentValue();

  RawEnginePtr raw_engine_;
  pb::common::Range range_;
  int64_t start_ts_;
  int64_t seek_ts_;
  pb::store::IsolationLevel isolation_level_;

  SnapshotPtr snapshot_;
  RawEngine::ReaderPtr reader_;
  std::shared_ptr<Iterator> write_iter_;
  std::shared_ptr<Iterator> lock_iter_;
  std::string last_lock_key_{};
  std::string last_write_key_{};
  pb::store::TxnResultInfo txn_result_info_;

  std::string key_{};
  std::string value_{};

  // The resolved locks are used to check the lock conflict.
  // If the lock is resolved, there will not be a conflict for provided resolved_locks.
  std::set<int64_t> resolved_locks_;
};
using TxnIteratorPtr = std::shared_ptr<TxnIterator>;

class TxnEngineHelper {
 public:
  static bool CheckLockConflict(const pb::store::LockInfo &lock_info, pb::store::IsolationLevel isolation_level,
                                int64_t start_ts, const std::set<int64_t> &resolved_locks,
                                pb::store::TxnResultInfo &txn_result_info);

  static butil::Status ScanLockInfo(StreamPtr stream, RawEnginePtr raw_engine, int64_t min_lock_ts, int64_t max_lock_ts,
                                    const pb::common::Range &range, int64_t limit,
                                    std::vector<pb::store::LockInfo> &lock_infos, bool &has_more,
                                    std::string &end_scan_key);

  static butil::Status BatchGet(RawEnginePtr raw_engine, const pb::store::IsolationLevel &isolation_level,
                                int64_t start_ts, const std::vector<std::string> &keys,
                                const std::set<int64_t> &resolved_locks, pb::store::TxnResultInfo &txn_result_info,
                                std::vector<pb::common::KeyValue> &kvs);

  static butil::Status Scan(StreamPtr stream, RawEnginePtr raw_engine, const pb::store::IsolationLevel &isolation_level,
                            int64_t start_ts, const pb::common::Range &range, int64_t limit, bool key_only,
                            bool is_reverse, const std::set<int64_t> &resolved_locks, bool disable_coprocessor,
                            const pb::common::CoprocessorV2 &coprocessor, pb::store::TxnResultInfo &txn_result_info,
                            std::vector<pb::common::KeyValue> &kvs, bool &has_more, std::string &end_scan_key);

  // txn write functions
  static butil::Status DoTxnCommit(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, store::RegionPtr region,
                                   const std::vector<pb::store::LockInfo> &lock_infos, int64_t start_ts,
                                   int64_t commit_ts);

  static butil::Status DoRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                  std::shared_ptr<Context> ctx, std::vector<std::string> &keys_to_rollback_with_data,
                                  std::vector<std::string> &keys_to_rollback_without_data, int64_t start_ts);

  static butil::Status DoUpdateLock(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                                    const pb::store::LockInfo &lock_info);

  static butil::Status PessimisticLock(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                       std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation> &mutations,
                                       const std::string &primary_lock, int64_t start_ts, int64_t lock_ttl,
                                       int64_t for_update_ts, bool return_values,
                                       std::vector<pb::common::KeyValue> &kvs);

  static butil::Status PessimisticRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                           std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
                                           int64_t for_update_ts, const std::vector<std::string> &keys);

  static butil::Status Prewrite(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                std::shared_ptr<Context> ctx, store::RegionPtr region,
                                const std::vector<pb::store::Mutation> &mutations, const std::string &primary_lock,
                                int64_t start_ts, int64_t lock_ttl, int64_t txn_size, bool try_one_pc,
                                int64_t min_commit_ts, int64_t max_commit_ts,
                                const std::vector<int64_t> &pessimistic_checks,
                                const std::map<int64_t, int64_t> &for_update_ts_checks,
                                const std::map<int64_t, std::string> &lock_extra_datas,
                                const std::vector<std::string> &secondaries);

  static butil::Status Commit(RawEnginePtr raw_engine, std::shared_ptr<Engine> engine, std::shared_ptr<Context> ctx,
                              store::RegionPtr region, int64_t start_ts, int64_t commit_ts,
                              const std::vector<std::string> &keys);

  static butil::Status BatchRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                     std::shared_ptr<Context> ctx, int64_t start_ts,
                                     const std::vector<std::string> &keys);

  static butil::Status CheckTxnStatus(RawEnginePtr raw_engine, std::shared_ptr<Engine> engine,
                                      std::shared_ptr<Context> ctx, const std::string &primary_key, int64_t lock_ts,
                                      int64_t caller_start_ts, int64_t current_ts, bool force_sync_commit);

  static butil::Status TxnCheckSecondaryLocks(RawEnginePtr raw_engine, std::shared_ptr<Context> ctx,
                                              store::RegionPtr region, int64_t start_ts,
                                              const std::vector<std::string> &keys);

  static butil::Status ResolveLock(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                   const std::vector<std::string> &keys);

  static butil::Status HeartBeat(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                 std::shared_ptr<Context> ctx, const std::string &primary_lock, int64_t start_ts,
                                 int64_t advise_lock_ttl);

  static butil::Status DeleteRange(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, const std::string &start_key,
                                   const std::string &end_key);

  static butil::Status Gc(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                          int64_t safe_point_ts);

  static butil::Status DoGc(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                            int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                            const std::string &region_start_key, const std::string &region_end_key);

  static butil::Status DoGcCoreTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, pb::common::RegionType type, int64_t safe_point_ts,
                                   std::shared_ptr<GCSafePoint> gc_safe_point, const std::string &region_start_key,
                                   const std::string &region_end_key);

  static butil::Status DoGcCoreNonTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                      std::shared_ptr<Context> ctx, pb::common::RegionType type, int64_t safe_point_ts,
                                      std::shared_ptr<GCSafePoint> gc_safe_point, const std::string &region_start_key,
                                      const std::string &region_end_key);

  static butil::Status DoGcForStoreTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                       std::shared_ptr<Context> ctx, pb::common::RegionType type, int64_t safe_point_ts,
                                       std::shared_ptr<GCSafePoint> gc_safe_point, const std::string &region_start_key,
                                       const std::string &region_end_key);

  static butil::Status DoGcForStoreNonTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                          std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                          int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                          const std::string &region_start_key, const std::string &region_end_key);

  static butil::Status DoGcForIndexTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                       std::shared_ptr<Context> ctx, pb::common::RegionType type, int64_t safe_point_ts,
                                       std::shared_ptr<GCSafePoint> gc_safe_point, const std::string &region_start_key,
                                       const std::string &region_end_key);

  static butil::Status DoGcForIndexNonTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                          std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                          int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                          const std::string &region_start_key, const std::string &region_end_key);

  static butil::Status DoGcForDocumentTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                          std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                          int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                          const std::string &region_start_key, const std::string &region_end_key);

  static butil::Status DoGcForDocumentNonTxn(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                             std::shared_ptr<Context> ctx, pb::common::RegionType type,
                                             int64_t safe_point_ts, std::shared_ptr<GCSafePoint> gc_safe_point,
                                             const std::string &region_start_key, const std::string &region_end_key);

  static butil::Status CheckLockForTxnGc(RawEngine::ReaderPtr reader, std::shared_ptr<Snapshot> snapshot,
                                         const std::string &start_key, const std::string &end_key,
                                         int64_t safe_point_ts, int64_t region_id, int64_t tenant_id,
                                         pb::common::RegionType type);

  static butil::Status RaftEngineWriteForTxnGc(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                                               const std::vector<std::string> &kv_deletes_lock,
                                               const std::vector<std::string> &kv_deletes_data,
                                               const std::vector<std::string> &kv_deletes_write, int64_t tenant_id,
                                               pb::common::RegionType type);

  static butil::Status RaftEngineWriteForNonTxnStoreAndDocumentGc(std::shared_ptr<Engine> raft_engine,
                                                                  std::shared_ptr<Context> ctx,
                                                                  const std::vector<std::string> &kv_deletes_default,
                                                                  int64_t tenant_id, pb::common::RegionType type);

  static butil::Status RaftEngineWriteForNonTxnIndexGc(std::shared_ptr<Engine> raft_engine,
                                                       std::shared_ptr<Context> ctx,
                                                       const std::vector<std::string> &kv_deletes_default,
                                                       const std::vector<std::string> &kv_deletes_scalar,
                                                       const std::vector<std::string> &kv_deletes_table,
                                                       const std::vector<std::string> &kv_deletes_scalar_speedup,
                                                       int64_t tenant_id, pb::common::RegionType type);

  static butil::Status RaftEngineWrite(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                                       pb::raft::TxnRaftRequest &txn_raft_request, int64_t tenant_id,
                                       pb::common::RegionType type, const std::string &name);

  static butil::Status DoFinalWorkForTxnGc(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                                           RawEngine::ReaderPtr reader, std::shared_ptr<Snapshot> snapshot,
                                           const std::string &write_key, int64_t tenant_id, pb::common::RegionType type,
                                           int64_t safe_point_ts,
                                           std::vector<std::string> &kv_deletes_lock,                    // NOLINT
                                           std::vector<std::string> &kv_deletes_data,                    // NOLINT
                                           std::vector<std::string> &kv_deletes_write,                   // NOLINT
                                           std::string &lock_start_key,                                  // NOLINT
                                           std::string &lock_end_key, std::string &last_lock_start_key,  // NOLINT
                                           std::string &last_lock_end_key);                              // NOLINT

  static butil::Status DoFinalWorkForNonTxnGc(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                                              int64_t tenant_id, pb::common::RegionType type,
                                              std::vector<std::string> &kv_deletes_default,
                                              std::vector<std::string> &kv_deletes_scalar,
                                              std::vector<std::string> &kv_deletes_table,
                                              std::vector<std::string> &kv_deletes_scalar_speedup);

  static void RegularUpdateSafePointTsHandler(void *arg);
  static void RegularDoGcHandler(void *arg);

  static void GenFinalMinCommitTs(int64_t region_id, std::string key, int64_t region_max_ts, int64_t start_ts,
                                  int64_t for_update_ts, int64_t lock_min_commit_ts, int64_t max_commit_ts,
                                  int64_t &final_min_commit_ts);

  static butil::Status GenPrewriteDataAndLock(
      store::RegionPtr region, const pb::store::Mutation &mutation, const pb::store::LockInfo &prev_lock_info,
      const pb::store::WriteInfo &write_info, const std::string &primary_lock, int64_t start_ts, int64_t for_update_ts,
      int64_t lock_ttl, int64_t txn_size, const std::string &lock_extra_data, int64_t min_commit_ts,
      int64_t max_commit_ts, bool need_check_pessimistic_lock, bool &try_one_pc, bool &use_async_commit,
      const std::vector<std::string> &secondaries, std::vector<pb::common::KeyValue> &kv_puts_data,
      std::vector<pb::common::KeyValue> &kv_puts_lock,
      std::vector<std::tuple<std::string, std::string, pb::store::LockInfo, bool>> &locks_for_1pc,
      int64_t &final_min_commit_ts);

  static void FallbackTo1PCLocks(
      std::vector<pb::common::KeyValue> &kv_puts_lock,
      std::vector<std::tuple<std::string, std::string, pb::store::LockInfo, bool>> &locks_for_1pc);

  static butil::Status OnePCommit(
      std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx, store::RegionPtr region, int64_t start_ts,
      int64_t final_commit_ts,
      std::vector<std::tuple<std::string, std::string, pb::store::LockInfo, bool>> &locks_for_1pc,
      std::vector<pb::common::KeyValue> &kv_puts_data);
  static butil::Status DoPreWrite(std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx, int64_t region_id,
                                  int64_t start_ts, int64_t mutation_size,
                                  std::vector<pb::common::KeyValue> &kv_puts_data,
                                  std::vector<pb::common::KeyValue> &kv_puts_lock);

  // backup & restore
  static butil::Status BackupData(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine, store::RegionPtr region,
                                  const pb::common::RegionType &region_type, std::string backup_ts, int64_t backup_tso,
                                  const std::string &storage_path, const pb::common::StorageBackend &storage_backend,
                                  const pb::common::CompressionType &compression_type, int32_t compression_level,
                                  dingodb::pb::store::BackupDataResponse *response);

  static butil::Status DoBackupDataCoreTxn(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine,
                                           store::RegionPtr region, const pb::common::RegionType &region_type,
                                           int64_t backup_tso, std::map<std::string, std::string> &kv_data,
                                           std::map<std::string, std::string> &kv_write);

  static butil::Status DoBackupDataCoreNonTxn(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine,
                                              store::RegionPtr region, const pb::common::RegionType &region_type,
                                              int64_t backup_tso, std::map<std::string, std::string> &kv_default,
                                              std::map<std::string, std::string> &kv_scalar,
                                              std::map<std::string, std::string> &kv_table,
                                              std::map<std::string, std::string> &kv_scalar_speedup);

  static butil::Status DoBackupDataForStoreTxn(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine,
                                               store::RegionPtr region, const pb::common::RegionType &region_type,
                                               int64_t backup_tso, std::map<std::string, std::string> &kv_data,
                                               std::map<std::string, std::string> &kv_write);

  static butil::Status DoBackupDataForStoreNonTxn(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine,
                                                  store::RegionPtr region, const pb::common::RegionType &region_type,
                                                  int64_t backup_tso, std::map<std::string, std::string> &kv_default,
                                                  std::map<std::string, std::string> &kv_scalar,
                                                  std::map<std::string, std::string> &kv_table,
                                                  std::map<std::string, std::string> &kv_scalar_speedup);

  static butil::Status DoBackupDataForIndexTxn(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine,
                                               store::RegionPtr region, const pb::common::RegionType &region_type,
                                               int64_t backup_tso, std::map<std::string, std::string> &kv_data,
                                               std::map<std::string, std::string> &kv_write);

  static butil::Status DoBackupDataForIndexNonTxn(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine,
                                                  store::RegionPtr region, const pb::common::RegionType &region_type,
                                                  int64_t backup_tso, std::map<std::string, std::string> &kv_default,
                                                  std::map<std::string, std::string> &kv_scalar,
                                                  std::map<std::string, std::string> &kv_table,
                                                  std::map<std::string, std::string> &kv_scalar_speedup);

  static butil::Status DoBackupDataForDocumentTxn(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine,
                                                  store::RegionPtr region, const pb::common::RegionType &region_type,
                                                  int64_t backup_tso, std::map<std::string, std::string> &kv_data,
                                                  std::map<std::string, std::string> &kv_write);

  static butil::Status DoBackupDataForDocumentNonTxn(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine,
                                                     store::RegionPtr region, const pb::common::RegionType &region_type,
                                                     int64_t backup_tso, std::map<std::string, std::string> &kv_default,
                                                     std::map<std::string, std::string> &kv_scalar,
                                                     std::map<std::string, std::string> &kv_table,
                                                     std::map<std::string, std::string> &kv_scalar_speedup);

  static butil::Status DoWriteDataAndCheckForTxn(RawEngine::ReaderPtr reader, std::shared_ptr<Snapshot> snapshot,
                                                 int64_t region_id, const pb::common::RegionType &region_type,
                                                 const pb::store::WriteInfo &write_info,
                                                 std::string_view write_iter_key, std::string_view write_iter_value,
                                                 const std::string &write_key, int64_t start_ts, int64_t write_ts,
                                                 std::map<std::string, std::string> &kv_data,
                                                 std::map<std::string, std::string> &kv_write);

  static butil::Status WriteSstFileForTxn(store::RegionPtr region, int64_t instance_id,
                                          const pb::common::RegionType &region_type,
                                          const pb::common::StorageBackend &storage_backend,
                                          const std::string &backup_file_prefix, const std::string &region_type_name,
                                          const std::map<std::string, std::string> &kv_data,
                                          const std::map<std::string, std::string> &kv_write,
                                          pb::common::BackupDataFileValueSstMetaGroup *sst_meta_group);

  static butil::Status WriteSstFileForNonTxn(store::RegionPtr region, int64_t instance_id,
                                             const pb::common::RegionType &region_type,
                                             const pb::common::StorageBackend &storage_backend,
                                             const std::string &backup_file_prefix, const std::string &region_type_name,
                                             const std::map<std::string, std::string> &kv_default,
                                             const std::map<std::string, std::string> &kv_scalar,
                                             const std::map<std::string, std::string> &kv_table,
                                             const std::map<std::string, std::string> &kv_scalar_speedup,
                                             pb::common::BackupDataFileValueSstMetaGroup *sst_meta_group);

  static butil::Status DoWriteSstFile(store::RegionPtr region, int64_t instance_id,
                                      const pb::common::RegionType &region_type,
                                      const pb::common::StorageBackend &storage_backend,
                                      const std::string &backup_file_prefix, const std::string &region_type_name,
                                      const std::string &cf, const std::map<std::string, std::string> &kvs,
                                      pb::common::BackupDataFileValueSstMetaGroup *sst_meta_group);

  static butil::Status BackupMeta(std::shared_ptr<Context> ctx, RawEnginePtr raw_engine, store::RegionPtr region,
                                  const pb::common::RegionType &region_type, std::string backup_ts, int64_t backup_tso,
                                  const std::string &storage_path, const pb::common::StorageBackend &storage_backend,
                                  const pb::common::CompressionType &compression_type, int32_t compression_level,
                                  dingodb::pb::store::BackupMetaResponse *response);

  static butil::Status RestoreMeta(std::shared_ptr<Context> ctx, std::shared_ptr<Engine> raft_engine,
                                   store::RegionPtr region, std::string backup_ts, int64_t backup_tso,
                                   const pb::common::StorageBackend &storage_backend,
                                   const dingodb::pb::common::BackupDataFileValueSstMetaGroup &sst_metas);

  static butil::Status RestoreData(std::shared_ptr<Context> ctx, std::shared_ptr<Engine> raft_engine,
                                   store::RegionPtr region, std::string backup_ts, int64_t backup_tso,
                                   const pb::common::StorageBackend &storage_backend,
                                   const dingodb::pb::common::BackupDataFileValueSstMetaGroup &sst_metas);

  static butil::Status ReadSstFile(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                   std::shared_ptr<Engine> raft_engine,
                                   const pb::common::StorageBackend &storage_backend,
                                   const pb::common::BackupDataFileValueSstMetaGroup &sst_meta_group);

  static butil::Status DoReadSstFileForTxn(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                           std::shared_ptr<Engine> raft_engine,
                                           const pb::common::StorageBackend &storage_backend,
                                           const pb::common::BackupDataFileValueSstMetaGroup &sst_meta_group);

  static butil::Status DoReadSstFileForNonTxn(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                              std::shared_ptr<Engine> raft_engine,
                                              const pb::common::StorageBackend &storage_backend,
                                              const pb::common::BackupDataFileValueSstMetaGroup &sst_meta_group);

  static butil::Status RestoreTxn(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                  std::shared_ptr<Engine> raft_engine,
                                  const std::vector<pb::common::KeyValue> &kv_puts_data,
                                  const std::vector<pb::common::KeyValue> &kv_puts_write);

  static butil::Status RestoreNonTxnStore(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                          std::shared_ptr<Engine> raft_engine,
                                          const std::vector<pb::common::KeyValue> &kv_default);

  static butil::Status RestoreNonTxnDocument(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                             std::shared_ptr<Engine> raft_engine,
                                             const std::vector<pb::common::KeyValue> &kv_default);

  static butil::Status OptimizePreProcessVectorIndex(const std::vector<pb::common::KeyValue> &kv_default,
                                                     const std::vector<pb::common::KeyValue> &kv_scalar,
                                                     const std::vector<pb::common::KeyValue> &kv_table,
                                                     const std::vector<pb::common::KeyValue> &kv_scalar_speed_up,
                                                     std::vector<pb::common::VectorWithId> &vector_with_ids);

  static butil::Status PreProcessVectorIndex(const std::vector<pb::common::KeyValue> &kv_default,
                                             const std::vector<pb::common::KeyValue> &kv_scalar,
                                             const std::vector<pb::common::KeyValue> &kv_table,
                                             const std::vector<pb::common::KeyValue> &kv_scalar_speed_up,
                                             std::vector<pb::common::VectorWithId> &vector_with_ids);

  static butil::Status RestoreNonTxnIndex(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                          std::shared_ptr<Engine> raft_engine,
                                          const std::vector<pb::common::KeyValue> &kv_default,
                                          const std::vector<pb::common::KeyValue> &kv_scalar,
                                          const std::vector<pb::common::KeyValue> &kv_table,
                                          const std::vector<pb::common::KeyValue> &kv_scalar_speed_up);

  static butil::Status RestoreNonTxn(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                     std::shared_ptr<Engine> raft_engine,
                                     const std::vector<pb::common::KeyValue> &kv_default,
                                     const std::vector<pb::common::KeyValue> &kv_scalar,
                                     const std::vector<pb::common::KeyValue> &kv_table,
                                     const std::vector<pb::common::KeyValue> &kv_scalar_speed_up);
};

}  // namespace dingodb

#endif  // DINGODB_TXN_ENGINE_HELPER_H_  // NOLINT

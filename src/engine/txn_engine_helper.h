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

#include <memory>
#include <vector>

#include "butil/status.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "meta/store_meta_manager.h"
#include "proto/store.pb.h"

namespace dingodb {

class TxnEngineHelper {
 public:
  // txn read functions
  static butil::Status GetLockInfo(const std::shared_ptr<RawEngine::Reader> &reader, const std::string &key,
                                   pb::store::LockInfo &lock_info);

  static butil::Status ScanLockInfo(const std::shared_ptr<RawEngine> &engine, int64_t min_lock_ts, int64_t max_lock_ts,
                                    const std::string &start_key, const std::string &end_key, uint32_t limit,
                                    std::vector<pb::store::LockInfo> &lock_infos);

  //   static butil::Status Commit(const std::shared_ptr<RawEngine> &engine, std::vector<pb::store::LockInfo>
  //   &lock_infos,
  //                               int64_t commit_ts);

  static butil::Status BatchGet(const std::shared_ptr<RawEngine> &engine,
                                const pb::store::IsolationLevel &isolation_level, int64_t start_ts,
                                const std::vector<std::string> &keys, std::vector<pb::common::KeyValue> &kvs,
                                pb::store::TxnResultInfo &txn_result_info);

  static butil::Status ScanGetNextKeyValue(std::shared_ptr<RawEngine::Reader> data_reader,
                                           std::shared_ptr<Iterator> write_iter, std::shared_ptr<Iterator> lock_iter,
                                           int64_t start_ts, const std::string &start_iter_key,
                                           std::string &last_lock_key, std::string &last_write_key,
                                           pb::store::TxnResultInfo &txn_result_info, std::string &iter_key,
                                           std::string &data_value);

  static butil::Status Scan(const std::shared_ptr<RawEngine> &engine, const pb::store::IsolationLevel &isolation_level,
                            int64_t start_ts, const pb::common::Range &range, int64_t limit, bool key_only,
                            bool is_reverse, bool disable_coprocessor, const pb::store::Coprocessor &coprocessor,
                            pb::store::TxnResultInfo &txn_result_info, std::vector<pb::common::KeyValue> &kvs,
                            bool &has_more, std::string &end_key);

  static butil::Status GetWriteInfo(const std::shared_ptr<RawEngine> &engine, int64_t min_commit_ts,
                                    int64_t max_commit_ts, int64_t start_ts, const std::string &key,
                                    bool include_rollback, bool include_delete, bool include_put,
                                    pb::store::WriteInfo &write_info, int64_t &commit_ts);

  static butil::Status GetRollbackInfo(const std::shared_ptr<RawEngine::Reader> &write_reader, int64_t start_ts,
                                       const std::string &key, pb::store::WriteInfo &write_info);

  // txn write functions
  static butil::Status DoTxnCommit(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, store::RegionPtr region,
                                   const std::vector<pb::store::LockInfo> &lock_infos, int64_t start_ts,
                                   int64_t commit_ts);

  static butil::Status DoRollback(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> raft_engine,
                                  std::shared_ptr<Context> ctx, std::vector<std::string> &keys_to_rollback_with_data,
                                  std::vector<std::string> &keys_to_rollback_without_data, int64_t start_ts);

  static butil::Status Prewrite(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> raft_engine,
                                std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation> &mutations,
                                const std::string &primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size,
                                bool try_one_pc, int64_t max_commit_ts);

  static butil::Status Commit(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> engine,
                              std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                              const std::vector<std::string> &keys);

  static butil::Status BatchRollback(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> raft_engine,
                                     std::shared_ptr<Context> ctx, int64_t start_ts,
                                     const std::vector<std::string> &keys);

  static butil::Status CheckTxnStatus(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> engine,
                                      std::shared_ptr<Context> ctx, const std::string &primary_key, int64_t lock_ts,
                                      int64_t caller_start_ts, int64_t current_ts);

  static butil::Status ResolveLock(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                   const std::vector<std::string> &keys);

  static butil::Status HeartBeat(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> raft_engine,
                                 std::shared_ptr<Context> ctx, const std::string &primary_lock, int64_t start_ts,
                                 int64_t advise_lock_ttl);

  static butil::Status DeleteRange(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, const std::string &start_key,
                                   const std::string &end_key);

  static butil::Status Gc(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<Engine> raft_engine,
                          std::shared_ptr<Context> ctx, int64_t safe_point_ts);
};

}  // namespace dingodb

#endif  // DINGODB_TXN_ENGINE_HELPER_H_
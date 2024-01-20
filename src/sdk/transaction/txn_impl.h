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

#ifndef DINGODB_SDK_TRANSACTION_IMPL_H_
#define DINGODB_SDK_TRANSACTION_IMPL_H_

#include <cstdint>
#include <memory>

#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "sdk/client.h"
#include "sdk/client_stub.h"
#include "sdk/region.h"
#include "sdk/store/store_rpc.h"
#include "sdk/transaction/txn_buffer.h"

namespace dingodb {
namespace sdk {

enum TransactionState : uint8_t {
  kInit,
  kActive,
  kRollbacking,
  kRollbackted,
  kPreCommitting,
  kPreCommitted,
  kCommitting,
  kCommitted
};  // NOLINT

static const char* TransactionState2Str(TransactionState state) {
  switch (state) {
    case kInit:
      return "INIT";
    case kActive:
      return "ACTIVE";
    case kRollbacking:
      return "ROLLBACKING";
    case kRollbackted:
      return "ROLLBACKTED";
    case kPreCommitting:
      return "PRECOMMITTING";
    case kPreCommitted:
      return "PRECOMMITTED";
    case kCommitting:
      return "COMMITTING";
    case kCommitted:
      return "COMMITTED";
    default:
      CHECK(false) << "unknow transaction state";
  }
}

// TODO: support read only txn
class Transaction::TxnImpl {
 public:
  TxnImpl(const TxnImpl&) = delete;
  const TxnImpl& operator=(const TxnImpl&) = delete;

  explicit TxnImpl(const ClientStub& stub, const TransactionOptions& options);

  ~TxnImpl() = default;

  Status Begin();

  Status Get(const std::string& key, std::string& value);

  Status BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs);

  Status Put(const std::string& key, const std::string& value);

  Status BatchPut(const std::vector<KVPair>& kvs);

  Status PutIfAbsent(const std::string& key, const std::string& value);

  Status BatchPutIfAbsent(const std::vector<KVPair>& kvs);

  Status Delete(const std::string& key);

  Status BatchDelete(const std::vector<std::string>& keys);

  Status Scan(const std::string& start_key, const std::string& end_key, uint64_t limit, std::vector<KVPair>& kvs);

  Status PreCommit();

  Status Commit();

  Status Rollback();

  TransactionState TEST_GetTransactionState() { return state_; }         // NOLINT
  int64_t TEST_GetStartTs() { return start_ts_; }                        // NOLINT
  int64_t TEST_GetCommitTs() { return commit_ts_; }                      // NOLINT
  int64_t TEST_MutationsSize() { return buffer_->MutationsSize(); }      // NOLINT
  std::string TEST_GetPrimaryKey() { return buffer_->GetPrimaryKey(); }  // NOLINT

 private:
  struct TxnSubTask {
    Rpc* rpc;
    std::shared_ptr<Region> region;
    Status status;
    std::vector<KVPair> result_kvs;

    TxnSubTask(Rpc* p_rpc, std::shared_ptr<Region> p_region) : rpc(p_rpc), region(std::move(p_region)) {}
  };

  // txn get
  std::unique_ptr<TxnGetRpc> PrepareTxnGetRpc(const std::shared_ptr<Region>& region) const;
  Status DoTxnGet(const std::string& key, std::string& value);

  // txn batch get
  std::unique_ptr<TxnBatchGetRpc> PrepareTxnBatchGetRpc(const std::shared_ptr<Region>& region) const;
  void ProcessTxnBatchGetSubTask(TxnSubTask* sub_task);
  Status DoTxnBatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs);

  // txn commit
  std::unique_ptr<TxnPrewriteRpc> PrepareTxnPrewriteRpc(const std::shared_ptr<Region>& region) const;
  void CheckAndLogPreCommitPrimaryKeyResponse(const pb::store::TxnPrewriteResponse* response) const;
  Status TryResolveTxnPrewriteLockConflict(const pb::store::TxnPrewriteResponse* response) const;
  Status PreCommitPrimaryKey();
  void ProcessTxnPrewriteSubTask(TxnSubTask* sub_task);

  std::unique_ptr<TxnCommitRpc> PrepareTxnCommitRpc(const std::shared_ptr<Region>& region) const;
  Status ProcessTxnCommitResponse(const pb::store::TxnCommitResponse* response, bool is_primary) const;
  Status CommitPrimaryKey();
  void ProcessTxnCommitSubTask(TxnSubTask* sub_task);

  // txn rollback
  std::unique_ptr<TxnBatchRollbackRpc> PrepareTxnBatchRollbackRpc(const std::shared_ptr<Region>& region) const;
  void CheckAndLogTxnBatchRollbackResponse(const pb::store::TxnBatchRollbackResponse* response) const;
  void ProcessBatchRollbackSubTask(TxnSubTask* sub_task);

  Status HeartBeat();

  static bool NeedRetryAndInc(int& times);

  static void DelayRetry(int64_t delay_ms);

  const ClientStub& stub_;
  const TransactionOptions options_;
  TransactionState state_;
  std::unique_ptr<TxnBuffer> buffer_;

  pb::meta::TsoTimestamp start_tso_;
  int64_t start_ts_;

  pb::meta::TsoTimestamp commit_tso_;
  int64_t commit_ts_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_IMPL_H_
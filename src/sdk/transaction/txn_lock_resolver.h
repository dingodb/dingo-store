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

#ifndef DINGODB_SDK_TRANSACTION_LOCK_RESOLVER_H_
#define DINGODB_SDK_TRANSACTION_LOCK_RESOLVER_H_

#include <cstdint>
#include "fmt/core.h"
#include "proto/store.pb.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

class ClientStub;

struct TxnStatus {
  int64_t lock_ttl;
  int64_t commit_ts;

  explicit TxnStatus() : lock_ttl(-1), commit_ts(-1) {}

  explicit TxnStatus(int64_t p_lock_ttl, int64_t p_commit_ts) : lock_ttl(p_lock_ttl), commit_ts(p_commit_ts) {}

  bool IsCommitted() const { return commit_ts > 0; }

  bool IsRollbacked() const { return lock_ttl == 0 && commit_ts == 0; }

  bool IsLocked() const { return lock_ttl > 0; }

  std::string ToString() const { return fmt::format("(lock_ttl:{}, commit_ts:{})", lock_ttl, commit_ts); }
};

class TxnLockResolver {
 public:
  explicit TxnLockResolver(const ClientStub& stub);

  virtual ~TxnLockResolver() = default;

  virtual Status ResolveLock(const pb::store::LockInfo& lock_info, int64_t caller_start_ts);

 private:
  Status CheckTxnStatus(int64_t txn_start_ts, const std::string& txn_primary_key, int64_t caller_start_ts, TxnStatus& txn_status);

  static Status ProcessTxnCheckStatusResponse(const pb::store::TxnCheckTxnStatusResponse& response,
                                              TxnStatus& txn_status);

  Status ResolveLockKey(int64_t txn_start_ts, const std::string& key, int64_t commit_ts);

  static Status ProcessTxnResolveLockResponse(const pb::store::TxnResolveLockResponse& response);

  const ClientStub& stub_;
};
}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TRANSACTION_LOCK_RESOLVER_H_
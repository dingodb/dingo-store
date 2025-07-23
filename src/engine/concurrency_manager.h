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

#ifndef DINGODB_COMMON_CONCURRENCY_MANAGER_H_
#define DINGODB_COMMON_CONCURRENCY_MANAGER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/synchronization.h"
#include "proto/store.pb.h"

namespace dingodb {
class ConcurrencyManager {
 public:
  ConcurrencyManager() = default;
  ~ConcurrencyManager() = default;

  ConcurrencyManager(const ConcurrencyManager&) = delete;
  void operator=(const ConcurrencyManager&) = delete;

  struct LockEntry {
    LockEntry() = default;
    ~LockEntry() = default;

    LockEntry(const LockEntry&) = delete;
    LockEntry& operator=(const LockEntry&) = delete;

    pb::store::LockInfo lock_info;
    RWLock rw_lock;
    std::atomic<bool> is_deleted{false};  // Whether the marker has been is_deleted (i.e., removed from the map)
  };
  
  using LockEntryPtr = std::shared_ptr<LockEntry>;

  void LockKey(const std::string& key, LockEntryPtr lock_entry);

  void UnlockKeys(const std::vector<std::string>& keys);

  bool CheckKeys(const std::vector<std::string>& keys, pb::store::IsolationLevel isolation_level, int64_t start_ts,
                 const std::set<int64_t>& resolved_locks, pb::store::TxnResultInfo& txn_result_info);

  // Range read operation: Check whether the keys within the range are locked
  bool CheckRange(const std::string& start_key, const std::string& end_key, pb::store::IsolationLevel isolation_level,
                  int64_t start_ts, const std::set<int64_t>& resolved_locks, pb::store::TxnResultInfo& txn_result_info);

 private:
  // key->lock_info  pb::store::LockInfo
  std::map<std::string, LockEntryPtr>
      lock_table_;  // Ordered storage of locked keys (supporting range queries)
  RWLock rw_lock_;
};

}  // namespace dingodb

#endif
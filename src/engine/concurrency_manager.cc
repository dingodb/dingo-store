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

#include "engine/concurrency_manager.h"

#include "engine/txn_engine_helper.h"
#include "proto/store.pb.h"

namespace dingodb {

// lock_entry.rw_lock has already write locked
void ConcurrencyManager::LockKey(const std::string& key, LockEntryPtr lock_entry) {
  RWLockWriteGuard guard(&rw_lock_);
  lock_table_[key] = lock_entry;
}

void ConcurrencyManager::UnlockKeys(const std::vector<std::string>& keys) {
  RWLockWriteGuard guard(&rw_lock_);
  for (auto const& key : keys) {
    auto it = lock_table_.find(key);
    if (it != lock_table_.end()) {
      it->second->is_deleted.store(true, std::memory_order_release);
      lock_table_.erase(it);
    }
  }
}

bool ConcurrencyManager::CheckKeys(const std::vector<std::string>& keys, pb::store::IsolationLevel isolation_level,
                                   int64_t start_ts, const std::set<int64_t>& resolved_locks,
                                   pb::store::TxnResultInfo& txn_result_info) {
  std::vector<LockEntryPtr> lock_entrys;
  {
    RWLockReadGuard guard(&rw_lock_);
    for (auto const& key : keys) {
      auto it = lock_table_.find(key);
      if (it != lock_table_.end()) {
        lock_entrys.push_back(it->second);
      }
    }
  }

  for (auto& lock_entry : lock_entrys) {
    if (lock_entry->is_deleted.load(std::memory_order_acquire)) {
      continue;
    }
    RWLockReadGuard guard(&lock_entry->rw_lock);
    if (TxnEngineHelper::CheckLockConflict(lock_entry->lock_info, isolation_level, start_ts, resolved_locks,
                                           txn_result_info)) {
      return true;
    }
  }

  return false;
}

bool ConcurrencyManager::CheckRange(const std::string& start_key, const std::string& end_key,
                                    pb::store::IsolationLevel isolation_level, int64_t start_ts,
                                    const std::set<int64_t>& resolved_locks,
                                    pb::store::TxnResultInfo& txn_result_info) {
  std::vector<LockEntryPtr> lock_entrys;
  {
    RWLockReadGuard guard(&rw_lock_);
    auto it = lock_table_.lower_bound(start_key);
    while (it != lock_table_.end() && it->first <= end_key) {
      lock_entrys.push_back(it->second);
    }
  }

  for (auto& lock_entry : lock_entrys) {
    if (lock_entry->is_deleted.load(std::memory_order_acquire)) {
      continue;
    }
    RWLockReadGuard guard(&lock_entry->rw_lock);
    if (TxnEngineHelper::CheckLockConflict(lock_entry->lock_info, isolation_level, start_ts, resolved_locks,
                                           txn_result_info)) {
      return true;
    }
  }

  return false;
}

}  // namespace dingodb

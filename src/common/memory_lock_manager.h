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

#ifndef DINGODB_COMMON_MEMORY_LOCK_MANAGER_H_
#define DINGODB_COMMON_MEMORY_LOCK_MANAGER_H_

#include <set>
#include <string>
#include <vector>

#include "bthread/mutex.h"

namespace dingodb {

class MemoryLockManager {
 public:
  MemoryLockManager() = default;
  ~MemoryLockManager() = default;

  MemoryLockManager(const MemoryLockManager&) = delete;
  void operator=(const MemoryLockManager&) = delete;

  void Lock(const std::string& key) {
    BAIDU_SCOPED_LOCK(mutex_);
    lock_table_.insert(key);
  }

  void Unlock(const std::string& key) {
    BAIDU_SCOPED_LOCK(mutex_);
    lock_table_.erase(key);
  }

  // Return the locked key; if it is not locked, return an empty string
  std::string CheckKey(const std::string& key) {
    BAIDU_SCOPED_LOCK(mutex_);
    auto it = lock_table_.find(key);
    if (it != lock_table_.end()) {
      return *it;
    }
    return "";
  }

  // Range read operation: Check whether the keys within the range are locked
  // Return the locked key; if it is not locked, return an empty string
  std::string CheckRange(const std::string& start_key, const std::string& end_key) {
    BAIDU_SCOPED_LOCK(mutex_);

    auto it = lock_table_.lower_bound(start_key);

    while (it != lock_table_.end() && *it <= end_key) {
      return *it;
    }

    return "";
  }

 private:
  std::set<std::string> lock_table_;  // Ordered storage of locked keys (supporting range queries)
  bthread_mutex_t mutex_;
};

}  // namespace dingodb

#endif
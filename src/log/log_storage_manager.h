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

#ifndef DINGODB_LOG_STORAGE_MANAGER_H_
#define DINGODB_LOG_STORAGE_MANAGER_H_

#include <cstdint>
#include <map>
#include <memory>

#include "log/segment_log_storage.h"

namespace dingodb {

class LogStorageManager {
 public:
  LogStorageManager() { bthread_mutex_init(&mutex_, nullptr); }
  ~LogStorageManager() { bthread_mutex_destroy(&mutex_); }

  void AddLogStorage(int64_t region_id, std::shared_ptr<SegmentLogStorage> log_storage);
  void DeleteStorage(int64_t region_id);
  std::shared_ptr<SegmentLogStorage> GetLogStorage(int64_t region_id);

 private:
  bthread_mutex_t mutex_;
  std::map<int64_t, std::shared_ptr<SegmentLogStorage>> log_storages_;
};

}  // namespace dingodb

#endif  // DINGODB_LOG_STORAGE_MANAGER_H_
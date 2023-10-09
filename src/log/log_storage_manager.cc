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

#include "log/log_storage_manager.h"

#include <utility>

namespace dingodb {

void LogStorageManager::AddLogStorage(int64_t region_id, std::shared_ptr<SegmentLogStorage> log_storage) {
  BAIDU_SCOPED_LOCK(mutex_);

  log_storages_.insert(std::make_pair(region_id, log_storage));
}

void LogStorageManager::DeleteStorage(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  log_storages_.erase(region_id);
}

std::shared_ptr<SegmentLogStorage> LogStorageManager::GetLogStorage(int64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = log_storages_.find(region_id);
  if (it == log_storages_.end()) {
    return nullptr;
  }

  return it->second;
}

}  // namespace dingodb
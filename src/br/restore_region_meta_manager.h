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

#ifndef DINGODB_BR_RESTORE_REGION_META_MANAGER_H_
#define DINGODB_BR_RESTORE_REGION_META_MANAGER_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "bthread/butex.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class RestoreRegionMetaManager : public std::enable_shared_from_this<RestoreRegionMetaManager> {
 public:
  RestoreRegionMetaManager(
      ServerInteractionPtr coordinator_interaction, uint32_t concurrency, int64_t replica_num,
      const std::string& storage_internal,
      std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> id_and_region_kvs,
      const std::string& backup_meta_region_name, int64_t create_region_timeout_s);
  ~RestoreRegionMetaManager();

  RestoreRegionMetaManager(const RestoreRegionMetaManager&) = delete;
  const RestoreRegionMetaManager& operator=(const RestoreRegionMetaManager&) = delete;
  RestoreRegionMetaManager(RestoreRegionMetaManager&&) = delete;
  RestoreRegionMetaManager& operator=(RestoreRegionMetaManager&&) = delete;

  std::shared_ptr<RestoreRegionMetaManager> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

  int64_t GetRegions();

 protected:
 private:
  butil::Status DoAsyncRestoreRegionMeta(uint32_t thread_no);
  butil::Status DoRestoreRegionInternal(ServerInteractionPtr coordinator_interaction, uint32_t thread_no);
  butil::Status FormatBackupMetaRegionName(std::vector<std::string>& backup_meta_region_names);
  static butil::Status PaddingBackupMetaRegionName(std::vector<std::string>& backup_meta_region_names);
  ServerInteractionPtr coordinator_interaction_;
  uint32_t concurrency_;
  int64_t replica_num_;
  std::string storage_internal_;

  std::string backup_meta_region_name_;
  int64_t create_region_timeout_s_;

  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> id_and_region_kvs_;

  std::vector<std::shared_ptr<dingodb::pb::common::Region>> regions_;

  // notify other threads to exit
  std::atomic<bool> is_need_exit_;

  bthread_mutex_t mutex_;

  std::atomic<int64_t> already_restore_region_metas_;

  // last error
  butil::Status last_error_;

  // 0 : not exit; 1 : exit
  std::vector<uint32_t> thread_exit_flags_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_REGION_META_MANAGER_H_
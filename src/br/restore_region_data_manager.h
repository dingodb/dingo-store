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

#ifndef DINGODB_BR_RESTORE_REGION_DATA_MANAGER_H_
#define DINGODB_BR_RESTORE_REGION_DATA_MANAGER_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "bthread/butex.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class RestoreRegionDataManager : public std::enable_shared_from_this<RestoreRegionDataManager> {
 public:
  RestoreRegionDataManager(
      ServerInteractionPtr coordinator_interaction, ServerInteractionPtr interaction, uint32_t concurrency,
      int64_t replica_num, const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
      const std::string& storage_internal,
      std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>
          id_and_sst_meta_group_kvs,
      const std::string& backup_meta_region_cf_name, const std::string& group_belongs_to_whom,
      int64_t restore_region_timeout_s,
      std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> id_and_region_kvs);
  ~RestoreRegionDataManager();

  RestoreRegionDataManager(const RestoreRegionDataManager&) = delete;
  const RestoreRegionDataManager& operator=(const RestoreRegionDataManager&) = delete;
  RestoreRegionDataManager(RestoreRegionDataManager&&) = delete;
  RestoreRegionDataManager& operator=(RestoreRegionDataManager&&) = delete;

  std::shared_ptr<RestoreRegionDataManager> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

 protected:
 private:
  butil::Status DoAsyncRestoreRegionData(uint32_t thread_no);
  butil::Status DoRestoreRegionDataInternal(ServerInteractionPtr coordinator_interaction,
                                            ServerInteractionPtr interaction, uint32_t thread_no);
  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr interaction_;
  uint32_t concurrency_;
  int64_t replica_num_;
  std::string restorets_;
  int64_t restoretso_internal_;
  std::string storage_;
  std::string storage_internal_;

  std::string backup_meta_region_cf_name_;
  std::string group_belongs_to_whom_;
  int64_t restore_region_timeout_s_;
  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>> id_and_region_kvs_;
  std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>
      id_and_sst_meta_group_kvs_;

  std::vector<std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>> sst_meta_groups_;

  // notify other threads to exit
  std::atomic<bool> is_need_exit_;

  bthread_mutex_t mutex_;

  std::atomic<int64_t> already_restore_region_datas_;

  // last error
  butil::Status last_error_;

  // 0 : not exit; 1 : exit
  std::vector<uint32_t> thread_exit_flags_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_REGION_DATA_MANAGER_H_
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

#ifndef DINGODB_BR_BACKUP_DATA_H_
#define DINGODB_BR_BACKUP_DATA_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/backup_sdk_data.h"
#include "br/backup_sql_data.h"
#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

class BackupData : public std::enable_shared_from_this<BackupData> {
 public:
  BackupData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
             ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
             const std::string& backupts, int64_t backuptso_internal, const std::string& storage,
             const std::string& storage_internal);
  ~BackupData();

  BackupData(const BackupData&) = delete;
  const BackupData& operator=(const BackupData&) = delete;
  BackupData(BackupData&&) = delete;
  BackupData& operator=(BackupData&&) = delete;

  std::shared_ptr<BackupData> GetSelf();

  butil::Status Init(const std::vector<int64_t>& meta_region_list);

  butil::Status Run();

  butil::Status Finish();

  std::shared_ptr<dingodb::pb::common::RegionMap> GetRegionMap() const { return region_map_; }

  std::shared_ptr<dingodb::pb::common::BackupMeta> GetBackupMeta();

 protected:
 private:
  butil::Status GetAllRegionMapFromCoordinator();
  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  ServerInteractionPtr index_interaction_;
  ServerInteractionPtr document_interaction_;
  std::string backupts_;
  int64_t backuptso_internal_;
  std::string storage_;
  std::string storage_internal_;
  std::shared_ptr<dingodb::pb::common::RegionMap> region_map_;

  std::shared_ptr<BackupSqlData> backup_sql_data_;
  std::shared_ptr<BackupSdkData> backup_sdk_data_;

  std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta_;
};

}  // namespace br

#endif  // DINGODB_BR_BACKUP_DATA_H_
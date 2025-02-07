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

#ifndef DINGODB_BR_BACKUP_META_H_
#define DINGODB_BR_BACKUP_META_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "br/backup_sdk_meta.h"
#include "br/backup_sql_meta.h"
#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/meta.pb.h"

namespace br {

class BackupMeta : public std::enable_shared_from_this<BackupMeta> {
 public:
  BackupMeta(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
             ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
             const std::string& backupts, int64_t backuptso_internal, const std::string& storage,
             const std::string& storage_internal);
  ~BackupMeta();

  BackupMeta(const BackupMeta&) = delete;
  const BackupMeta& operator=(const BackupMeta&) = delete;
  BackupMeta(BackupMeta&&) = delete;
  BackupMeta& operator=(BackupMeta&&) = delete;

  std::shared_ptr<BackupMeta> GetSelf();

  butil::Status Init();

  butil::Status Run(std::shared_ptr<dingodb::pb::common::RegionMap> region_map);

  butil::Status Finish();

  std::vector<int64_t> GetSqlMetaRegionList();

  std::shared_ptr<dingodb::pb::common::BackupMeta> GetBackupMeta();

  std::pair<butil::Status, std::shared_ptr<dingodb::pb::meta::IdEpochTypeAndValue>> GetIdEpochTypeAndValue();

  std::pair<butil::Status, std::shared_ptr<dingodb::pb::meta::TableIncrementGroup>> GetAllTableIncrement();

 protected:
 private:
  butil::Status GetPresentIdsFromMeta();
  butil::Status GetAllTableIncrementFromMeta();
  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  ServerInteractionPtr index_interaction_;
  ServerInteractionPtr document_interaction_;
  std::string backupts_;
  int64_t backuptso_internal_;
  std::string storage_;
  std::string storage_internal_;
  std::shared_ptr<dingodb::pb::meta::IdEpochTypeAndValue> id_epoch_type_and_value_;
  std::shared_ptr<dingodb::pb::meta::TableIncrementGroup> table_increment_group_;

  std::shared_ptr<BackupSqlMeta> backup_sql_meta_;
  std::shared_ptr<BackupSdkMeta> backup_sdk_meta_;

  std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta_;
};

}  // namespace br

#endif  // DINGODB_BR_BACKUP_META_H_
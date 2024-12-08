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

#ifndef DINGODB_BR_BACKUP_SQL_META_H_
#define DINGODB_BR_BACKUP_SQL_META_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/backup_meta_base.h"
#include "br/interation.h"
#include "butil/status.h"
#include "fmt/core.h"

namespace br {

class BackupSqlMeta : public BackupMetaBase, public std::enable_shared_from_this<BackupSqlMeta> {
 public:
  BackupSqlMeta(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                const std::string& backupts, int64_t backuptso_internal, const std::string& storage,
                 const std::string& storage_internal);
  ~BackupSqlMeta() override;

  BackupSqlMeta(const BackupSqlMeta&) = delete;
  const BackupSqlMeta& operator=(const BackupSqlMeta&) = delete;
  BackupSqlMeta(BackupSqlMeta&&) = delete;
  BackupSqlMeta& operator=(BackupSqlMeta&&) = delete;

  std::shared_ptr<BackupSqlMeta> GetSelf();

  butil::Status GetSqlMetaRegionFromCoordinator();

  void GetSqlMetaRegionList(std::vector<int64_t>& region_list);

 protected:
 private:
  std::shared_ptr<std::vector<dingodb::pb::coordinator::ScanRegionInfo>> scan_region_infos_;
};

}  // namespace br

#endif  // DINGODB_BR_BACKUP_SQL_META_H_
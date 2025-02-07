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

#ifndef DINGODB_BR_RESTORE_META_H_
#define DINGODB_BR_RESTORE_META_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "br/restore_sdk_meta.h"
#include "br/restore_sql_meta.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/meta.pb.h"

namespace br {

class RestoreMeta : public std::enable_shared_from_this<RestoreMeta> {
 public:
  RestoreMeta(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
              const std::string &restorets, int64_t restoretso_internal, const std::string &storage,
              const std::string &storage_internal, std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta,
              std::shared_ptr<dingodb::pb::meta::IdEpochTypeAndValue> id_epoch_type_and_value,
              std::shared_ptr<dingodb::pb::meta::TableIncrementGroup> table_increment_group,
              uint32_t create_region_concurrency, uint32_t restore_region_concurrency, int64_t create_region_timeout_s,
              int64_t restore_region_timeout_s, int32_t replica_num);
  ~RestoreMeta();

  RestoreMeta(const RestoreMeta &) = delete;
  const RestoreMeta &operator=(const RestoreMeta &) = delete;
  RestoreMeta(RestoreMeta &&) = delete;
  RestoreMeta &operator=(RestoreMeta &&) = delete;

  std::shared_ptr<RestoreMeta> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

  butil::Status ImportIdEpochTypeToMeta();
  butil::Status CreateAutoIncrementsToMeta();

 protected:
 private:
  butil::Status CheckBackupMeta();
  butil::Status ExtractFromBackupMeta();

  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;

  std::string restorets_;
  int64_t restoretso_internal_;
  std::string storage_;
  std::string storage_internal_;

  std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta_;
  std::shared_ptr<dingodb::pb::meta::IdEpochTypeAndValue> id_epoch_type_and_value_;
  std::shared_ptr<dingodb::pb::meta::TableIncrementGroup> table_increment_group_;
  uint32_t create_region_concurrency_;
  uint32_t restore_region_concurrency_;
  int64_t create_region_timeout_s_;
  int64_t restore_region_timeout_s_;
  int32_t replica_num_;

  std::shared_ptr<RestoreSqlMeta> restore_sql_meta_;
  std::shared_ptr<RestoreSdkMeta> restore_sdk_meta_;

  std::map<std::string, std::string> backupmeta_schema_kvs_;

  std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sql_meta_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sql_meta_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> coordinator_sdk_meta_sst_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_META_H_
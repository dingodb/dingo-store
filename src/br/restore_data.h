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

#ifndef DINGODB_BR_RESTORE_DATA_H_
#define DINGODB_BR_RESTORE_DATA_H_

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "br/restore_sdk_data.h"
#include "br/restore_sql_data.h"
#include "butil/status.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

class RestoreData : public std::enable_shared_from_this<RestoreData> {
 public:
  RestoreData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
              ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
              const std::string &restorets, int64_t restoretso_internal, const std::string &storage,
              const std::string &storage_internal, std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta,
              uint32_t create_region_concurrency, uint32_t restore_region_concurrency, int64_t create_region_timeout_s,
              int64_t restore_region_timeout_s, int32_t replica_num);
  ~RestoreData();

  RestoreData(const RestoreData &) = delete;
  const RestoreData &operator=(const RestoreData &) = delete;
  RestoreData(RestoreData &&) = delete;
  RestoreData &operator=(RestoreData &&) = delete;

  std::shared_ptr<RestoreData> GetSelf();

  butil::Status Init();

  butil::Status Run();

  butil::Status Finish();

  std::pair<int64_t, int64_t> GetRegions();

 protected:
 private:
  butil::Status CheckBackupMeta();
  butil::Status CheckBackupMetaDatafileKvs();
  butil::Status ExtractFromBackupMeta();
  ServerInteractionPtr coordinator_interaction_;
  ServerInteractionPtr store_interaction_;
  ServerInteractionPtr index_interaction_;
  ServerInteractionPtr document_interaction_;

  std::string restorets_;
  int64_t restoretso_internal_;
  std::string storage_;
  std::string storage_internal_;

  std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta_;
  uint32_t create_region_concurrency_;
  uint32_t restore_region_concurrency_;
  int64_t create_region_timeout_s_;
  int64_t restore_region_timeout_s_;
  int32_t replica_num_;

  std::map<std::string, std::string> backupmeta_datafile_kvs_;

  // sql maybe nullptr. if no any data.
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sql_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sql_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_sql_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_sql_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_sql_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_sql_data_sst_;

  // sdk maybe nullptr. if no any data.
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_sdk_data_sst_;
  std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_sdk_data_sst_;

  std::shared_ptr<RestoreSqlData> restore_sql_data_;
  std::shared_ptr<RestoreSdkData> restore_sdk_data_;
};

}  // namespace br

#endif  // DINGODB_BR_RESTORE_DATA_H_
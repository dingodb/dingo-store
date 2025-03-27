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

#include "br/restore_data.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/interation.h"
#include "br/sst_file_reader.h"
#include "br/utils.h"
#include "common/constant.h"
#include "common/logging.h"
#include "fmt/core.h"

namespace br {

RestoreData::RestoreData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                         ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                         const std::string &restorets, int64_t restoretso_internal, const std::string &storage,
                         const std::string &storage_internal,
                         std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta,
                         uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
                         int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      index_interaction_(index_interaction),
      document_interaction_(document_interaction),
      restorets_(restorets),
      restoretso_internal_(restoretso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      backup_meta_(backup_meta),
      create_region_concurrency_(create_region_concurrency),
      restore_region_concurrency_(restore_region_concurrency),
      create_region_timeout_s_(create_region_timeout_s),
      restore_region_timeout_s_(restore_region_timeout_s),
      replica_num_(replica_num) {}

RestoreData::~RestoreData() = default;

std::shared_ptr<RestoreData> RestoreData::GetSelf() { return shared_from_this(); }

butil::Status RestoreData::Init() {
  butil::Status status;

  if (backup_meta_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << backup_meta_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "backup_meta_ = nullptr";
  }

  status = CheckBackupMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromBackupMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  // double check
  if (!store_region_sql_data_sst_ && store_cf_sst_meta_sql_data_sst_) {
    std::string s = "store_cf_sst_meta_sql_data_sst_ is not null, but store_region_sql_data_sst_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  if (!index_region_sql_data_sst_ && index_cf_sst_meta_sql_data_sst_) {
    std::string s = "index_cf_sst_meta_sql_data_sst_ is not null, but index_region_sql_data_sst_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  if (!document_region_sql_data_sst_ && document_cf_sst_meta_sql_data_sst_) {
    std::string s = "document_cf_sst_meta_sql_data_sst_ is not null, but document_region_sql_data_sst_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  if (!store_region_sdk_data_sst_ && store_cf_sst_meta_sdk_data_sst_) {
    std::string s = "store_cf_sst_meta_sdk_data_sst_ is not null, but store_region_sdk_data_sst_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  if (!index_region_sdk_data_sst_ && index_cf_sst_meta_sdk_data_sst_) {
    std::string s = "index_cf_sst_meta_sdk_data_sst_ is not null, but index_region_sdk_data_sst_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  if (!document_region_sdk_data_sst_ && document_cf_sst_meta_sdk_data_sst_) {
    std::string s = "document_cf_sst_meta_sdk_data_sst_ is not null, but document_region_sdk_data_sst_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  // init sql data
  {
    if (!restore_sql_data_ && backup_meta_ &&
        (store_region_sql_data_sst_ || store_cf_sst_meta_sql_data_sst_ || index_region_sql_data_sst_ ||
         index_cf_sst_meta_sql_data_sst_ || document_region_sql_data_sst_ || document_cf_sst_meta_sql_data_sst_)) {
      std::vector<std::string> coordinator_addrs = coordinator_interaction_->GetAddrs();
      std::vector<std::string> store_addrs = store_interaction_->GetAddrs();
      std::vector<std::string> index_addrs = index_interaction_->GetAddrs();
      std::vector<std::string> document_addrs = document_interaction_->GetAddrs();

      std::shared_ptr<br::ServerInteraction> internal_coordinator_interaction;
      status = ServerInteraction::CreateInteraction(coordinator_addrs, internal_coordinator_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      std::shared_ptr<br::ServerInteraction> internal_store_interaction;
      status = ServerInteraction::CreateInteraction(store_addrs, internal_store_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      std::shared_ptr<br::ServerInteraction> internal_index_interaction;
      status = ServerInteraction::CreateInteraction(index_addrs, internal_index_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      std::shared_ptr<br::ServerInteraction> internal_document_interaction;
      status = ServerInteraction::CreateInteraction(document_addrs, internal_document_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      restore_sql_data_ = std::make_shared<RestoreSqlData>(
          internal_coordinator_interaction, internal_store_interaction, internal_index_interaction,
          internal_document_interaction, restorets_, restoretso_internal_, storage_, storage_internal_,
          store_region_sql_data_sst_, store_cf_sst_meta_sql_data_sst_, index_region_sql_data_sst_,
          index_cf_sst_meta_sql_data_sst_, document_region_sql_data_sst_, document_cf_sst_meta_sql_data_sst_,
          create_region_concurrency_, restore_region_concurrency_, create_region_timeout_s_, restore_region_timeout_s_,
          replica_num_);

      status = restore_sql_data_->Init();
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }
    }
  }

  // init sdk data
  {
    if (!restore_sdk_data_ && backup_meta_ &&
        (store_region_sdk_data_sst_ || store_cf_sst_meta_sdk_data_sst_ || index_region_sdk_data_sst_ ||
         index_cf_sst_meta_sdk_data_sst_ || document_region_sdk_data_sst_ || document_cf_sst_meta_sdk_data_sst_)) {
      std::vector<std::string> coordinator_addrs = coordinator_interaction_->GetAddrs();
      std::vector<std::string> store_addrs = store_interaction_->GetAddrs();
      std::vector<std::string> index_addrs = index_interaction_->GetAddrs();
      std::vector<std::string> document_addrs = document_interaction_->GetAddrs();

      std::shared_ptr<br::ServerInteraction> internal_coordinator_interaction;
      status = ServerInteraction::CreateInteraction(coordinator_addrs, internal_coordinator_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      std::shared_ptr<br::ServerInteraction> internal_store_interaction;
      status = ServerInteraction::CreateInteraction(store_addrs, internal_store_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      std::shared_ptr<br::ServerInteraction> internal_index_interaction;
      status = ServerInteraction::CreateInteraction(index_addrs, internal_index_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      std::shared_ptr<br::ServerInteraction> internal_document_interaction;
      status = ServerInteraction::CreateInteraction(document_addrs, internal_document_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      restore_sdk_data_ = std::make_shared<RestoreSdkData>(
          internal_coordinator_interaction, internal_store_interaction, internal_index_interaction,
          internal_document_interaction, restorets_, restoretso_internal_, storage_, storage_internal_,
          store_region_sdk_data_sst_, store_cf_sst_meta_sdk_data_sst_, index_region_sdk_data_sst_,
          index_cf_sst_meta_sdk_data_sst_, document_region_sdk_data_sst_, document_cf_sst_meta_sdk_data_sst_,
          create_region_concurrency_, restore_region_concurrency_, create_region_timeout_s_, restore_region_timeout_s_,
          replica_num_);

      status = restore_sdk_data_->Init();
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreData::Run() {
  butil::Status status;

  if (restore_sql_data_) {
    status = restore_sql_data_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << "restore_sql_data_ is nullptr. ignore run";
  }

  if (restore_sdk_data_) {
    status = restore_sdk_data_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << "restore_sdk_data_ is nullptr. ignore run";
  }

  return butil::Status::OK();
}

butil::Status RestoreData::Finish() {
  butil::Status status;

  if (restore_sql_data_) {
    status = restore_sql_data_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (restore_sdk_data_) {
    status = restore_sdk_data_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  return butil::Status::OK();
}

std::pair<int64_t, int64_t> RestoreData::GetRegions() {
  std::pair<int64_t, int64_t> region_metas = std::pair<int64_t, int64_t>(0, 0);
  std::pair<int64_t, int64_t> region_datas = std::pair<int64_t, int64_t>(0, 0);

  if (restore_sql_data_) {
    region_metas = restore_sql_data_->GetRegions();
  }

  if (restore_sdk_data_) {
    region_datas = restore_sdk_data_->GetRegions();
  }

  return std::pair<int64_t, int64_t>(region_metas.first + region_datas.first,
                                     region_metas.second + region_datas.second);
}

butil::Status RestoreData::CheckBackupMeta() {
  butil::Status status;

  if (backup_meta_) {
    std::string file_path = storage_internal_ + "/" + dingodb::Constant::kBackupMetaDataFileName;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = Utils::CheckBackupMeta(backup_meta_, storage_internal_, dingodb::Constant::kBackupMetaDataFileName, "",
                                    dingodb::Constant::kBackupRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }  // if (backup_meta_)

  return butil::Status::OK();
}

butil::Status RestoreData::CheckBackupMetaDatafileKvs() {
  butil::Status status;

  // find store_cf_sst_meta_sql_data.sst
  auto iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kStoreCfSstMetaSqlDataSstName);
  if (iter != backupmeta_datafile_kvs_.end()) {
    // find store_region_sql_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kStoreRegionSqlDataSstName);
    if (iter == backupmeta_datafile_kvs_.end()) {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file. but {} already in backupmeta.datafile file.",
                      dingodb::Constant::kStoreRegionSqlDataSstName, dingodb::Constant::kStoreCfSstMetaSqlDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_NOT_FOUND_KEY_IN_FILE, s);
    }
  }

  // find index_cf_sst_meta_sql_data.sst
  iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kIndexCfSstMetaSqlDataSstName);
  if (iter != backupmeta_datafile_kvs_.end()) {
    // find index_region_sql_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kIndexRegionSqlDataSstName);
    if (iter == backupmeta_datafile_kvs_.end()) {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file. but {} already in backupmeta.datafile file.",
                      dingodb::Constant::kIndexRegionSqlDataSstName, dingodb::Constant::kIndexCfSstMetaSqlDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_NOT_FOUND_KEY_IN_FILE, s);
    }
  }

  // find document_cf_sst_meta_sql_data.sst
  iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kDocumentCfSstMetaSqlDataSstName);
  if (iter != backupmeta_datafile_kvs_.end()) {
    // find document_region_sql_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kDocumentRegionSqlDataSstName);
    if (iter == backupmeta_datafile_kvs_.end()) {
      std::string s = fmt::format(
          "not found {} in backupmeta.datafile file. but {} already in backupmeta.datafile file.",
          dingodb::Constant::kDocumentRegionSqlDataSstName, dingodb::Constant::kDocumentCfSstMetaSqlDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_NOT_FOUND_KEY_IN_FILE, s);
    }
  }

  // find store_cf_sst_meta_sdk_data.sst
  iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
  if (iter != backupmeta_datafile_kvs_.end()) {
    // find store_region_sdk_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kStoreRegionSdkDataSstName);
    if (iter == backupmeta_datafile_kvs_.end()) {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file. but {} already in backupmeta.datafile file.",
                      dingodb::Constant::kStoreRegionSdkDataSstName, dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_NOT_FOUND_KEY_IN_FILE, s);
    }
  }

  // find index_cf_sst_meta_sdk_data.sst
  iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kIndexCfSstMetaSdkDataSstName);
  if (iter != backupmeta_datafile_kvs_.end()) {
    // find index_region_sdk_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kIndexRegionSdkDataSstName);
    if (iter == backupmeta_datafile_kvs_.end()) {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file. but {} already in backupmeta.datafile file.",
                      dingodb::Constant::kIndexRegionSdkDataSstName, dingodb::Constant::kIndexCfSstMetaSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_NOT_FOUND_KEY_IN_FILE, s);
    }
  }

  // find document_cf_sst_meta_sdk_data.sst
  iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kDocumentCfSstMetaSdkDataSstName);
  if (iter != backupmeta_datafile_kvs_.end()) {
    // find document_region_sdk_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kDocumentRegionSdkDataSstName);
    if (iter == backupmeta_datafile_kvs_.end()) {
      std::string s = fmt::format(
          "not found {} in backupmeta.datafile file. but {} already in backupmeta.datafile file.",
          dingodb::Constant::kDocumentRegionSdkDataSstName, dingodb::Constant::kDocumentCfSstMetaSdkDataSstName);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_NOT_FOUND_KEY_IN_FILE, s);
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreData::ExtractFromBackupMeta() {
  butil::Status status;

  if (backup_meta_) {
    std::string file_path = storage_internal_ + "/" + backup_meta_->file_name();

    SstFileReader sst_file_reader;
    status = sst_file_reader.ReadFile(file_path, backupmeta_datafile_kvs_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = CheckBackupMetaDatafileKvs();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    // find store_region_sql_data.sst
    auto iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kStoreRegionSqlDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_store_region_sql_data_sst;
      auto ret = internal_store_region_sql_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kStoreRegionSqlDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      store_region_sql_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_store_region_sql_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kStoreRegionSqlDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find store_cf_sst_meta_sql_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kStoreCfSstMetaSqlDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_store_cf_sst_meta_sql_data_sst;
      auto ret = internal_store_cf_sst_meta_sql_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kStoreCfSstMetaSqlDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      store_cf_sst_meta_sql_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_store_cf_sst_meta_sql_data_sst));
    }

    // find index_region_sql_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kIndexRegionSqlDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_index_region_sql_data_sst;
      auto ret = internal_index_region_sql_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kIndexRegionSqlDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      index_region_sql_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_index_region_sql_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kIndexRegionSqlDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find index_cf_sst_meta_sql_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kIndexCfSstMetaSqlDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_index_cf_sst_meta_sql_data_sst;
      auto ret = internal_index_cf_sst_meta_sql_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kIndexCfSstMetaSqlDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      index_cf_sst_meta_sql_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_index_cf_sst_meta_sql_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kIndexCfSstMetaSqlDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find document_region_sql_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kDocumentRegionSqlDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_document_region_sql_data_sst;
      auto ret = internal_document_region_sql_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kDocumentRegionSqlDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      document_region_sql_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_document_region_sql_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kDocumentRegionSqlDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find document_cf_sst_meta_sql_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kDocumentCfSstMetaSqlDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_document_cf_sst_meta_sql_data_sst;
      auto ret = internal_document_cf_sst_meta_sql_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kDocumentCfSstMetaSqlDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      document_cf_sst_meta_sql_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_document_cf_sst_meta_sql_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kDocumentCfSstMetaSqlDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find store_region_sdk_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kStoreRegionSdkDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_store_region_sdk_data_sst;
      auto ret = internal_store_region_sdk_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kStoreRegionSdkDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      store_region_sdk_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_store_region_sdk_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kStoreRegionSdkDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find store_cf_sst_meta_sdk_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_store_cf_sst_meta_sdk_data_sst;
      auto ret = internal_store_cf_sst_meta_sdk_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      store_cf_sst_meta_sdk_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_store_cf_sst_meta_sdk_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kStoreCfSstMetaSdkDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find index_region_sdk_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kIndexRegionSdkDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_index_region_sdk_data_sst;
      auto ret = internal_index_region_sdk_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kIndexRegionSdkDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      index_region_sdk_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_index_region_sdk_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kIndexRegionSdkDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find index_cf_sst_meta_sdk_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kIndexCfSstMetaSdkDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_index_cf_sst_meta_sdk_data_sst;
      auto ret = internal_index_cf_sst_meta_sdk_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kIndexCfSstMetaSdkDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      index_cf_sst_meta_sdk_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_index_cf_sst_meta_sdk_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kIndexCfSstMetaSdkDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find document_region_sdk_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kDocumentRegionSdkDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_document_region_sdk_data_sst;
      auto ret = internal_document_region_sdk_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kDocumentRegionSdkDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      document_region_sdk_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_document_region_sdk_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kDocumentRegionSdkDataSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find document_cf_sst_meta_sdk_data.sst
    iter = backupmeta_datafile_kvs_.find(dingodb::Constant::kDocumentCfSstMetaSdkDataSstName);
    if (iter != backupmeta_datafile_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_document_cf_sst_meta_sdk_data_sst;
      auto ret = internal_document_cf_sst_meta_sdk_data_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed : {}",
                                    dingodb::Constant::kDocumentCfSstMetaSdkDataSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      document_cf_sst_meta_sdk_data_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_document_cf_sst_meta_sdk_data_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta.datafile file.", dingodb::Constant::kDocumentCfSstMetaSdkDataSstName);
      DINGO_LOG(WARNING) << s;
    }

  }  // if(backup_meta_)

  return butil::Status::OK();
}

}  // namespace br
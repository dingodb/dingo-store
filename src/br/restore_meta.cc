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

#include "br/restore_meta.h"

#include <memory>
#include <string>

#include "br/helper.h"
#include "br/sst_file_reader.h"
#include "br/utils.h"
#include "common/constant.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/meta.pb.h"

namespace br {

RestoreMeta::RestoreMeta(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                         const std::string &restorets, int64_t restoretso_internal, const std::string &storage,
                         const std::string &storage_internal,
                         std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta,
                         std::shared_ptr<dingodb::pb::meta::IdEpochTypeAndValue> id_epoch_type_and_value,
                         std::shared_ptr<dingodb::pb::meta::TableIncrementGroup> table_increment_group,
                         uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
                         int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      restorets_(restorets),
      restoretso_internal_(restoretso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      backup_meta_(backup_meta),
      id_epoch_type_and_value_(id_epoch_type_and_value),
      table_increment_group_(table_increment_group),
      create_region_concurrency_(create_region_concurrency),
      restore_region_concurrency_(restore_region_concurrency),
      create_region_timeout_s_(create_region_timeout_s),
      restore_region_timeout_s_(restore_region_timeout_s),
      replica_num_(replica_num) {}

RestoreMeta::~RestoreMeta() = default;

std::shared_ptr<RestoreMeta> RestoreMeta::GetSelf() { return shared_from_this(); }

butil::Status RestoreMeta::Init() {
  butil::Status status;

  if (backup_meta_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << backup_meta_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "backup_meta_ = nullptr";
  }

  if (id_epoch_type_and_value_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << id_epoch_type_and_value_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "id_epoch_type_and_value_ = nullptr";
  }

  if (table_increment_group_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << table_increment_group_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "table_increment_group_ = nullptr";
  }

  status = CheckBackupMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = ExtractFromBackupMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // double check
  if (!store_region_sql_meta_sst_ && store_cf_sst_meta_sql_meta_sst_) {
    std::string s = "store_cf_sst_meta_sql_meta_sst_ is not null, but store_region_sql_meta_sst_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  if (store_region_sql_meta_sst_ || store_cf_sst_meta_sql_meta_sst_) {
    std::vector<std::string> coordinator_addrs = coordinator_interaction_->GetAddrs();
    std::vector<std::string> store_addrs = store_interaction_->GetAddrs();

    std::shared_ptr<br::ServerInteraction> internal_coordinator_interaction;
    status = ServerInteraction::CreateInteraction(coordinator_addrs, internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> internal_store_interaction;
    status = ServerInteraction::CreateInteraction(store_addrs, internal_store_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    restore_sql_meta_ = std::make_shared<RestoreSqlMeta>(
        internal_coordinator_interaction, internal_store_interaction, restorets_, restoretso_internal_, storage_,
        storage_internal_, store_region_sql_meta_sst_, store_cf_sst_meta_sql_meta_sst_, create_region_concurrency_,
        restore_region_concurrency_, create_region_timeout_s_, restore_region_timeout_s_, replica_num_);

    status = restore_sql_meta_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  if (coordinator_sdk_meta_sst_) {
    std::vector<std::string> coordinator_addrs = coordinator_interaction_->GetAddrs();

    std::shared_ptr<br::ServerInteraction> internal_coordinator_interaction;
    status = ServerInteraction::CreateInteraction(coordinator_addrs, internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
    restore_sdk_meta_ =
        std::make_shared<RestoreSdkMeta>(internal_coordinator_interaction, restorets_, restoretso_internal_, storage_,
                                         storage_internal_, coordinator_sdk_meta_sst_);

    status = restore_sdk_meta_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreMeta::Run() {
  butil::Status status;

  if (restore_sql_meta_) {
    status = restore_sql_meta_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << "restore_sql_meta_ = nullptr. ignore run";
  }  // if (restore_sql_meta_) {

  if (restore_sdk_meta_) {
    status = restore_sdk_meta_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << "restore_sdk_meta_ = nullptr. ignore run";
  }  // if (restore_sdk_meta_) {

  return butil::Status::OK();
}

butil::Status RestoreMeta::Finish() {
  butil::Status status;

  if (restore_sql_meta_) {
    status = restore_sql_meta_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  if (restore_sdk_meta_) {
    status = restore_sdk_meta_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreMeta::ImportIdEpochTypeToMeta() {
  if (id_epoch_type_and_value_ && id_epoch_type_and_value_->items_size() > 0) {
    dingodb::pb::meta::ImportIdEpochTypeRequest request;
    dingodb::pb::meta::ImportIdEpochTypeResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.mutable_id_epoch_type_and_value()->CopyFrom(*id_epoch_type_and_value_);

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

    auto status = coordinator_interaction_->SendRequest("MetaService", "ImportIdEpochType", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << response.error().errmsg();
      return butil::Status(response.error().errcode(), response.error().errmsg());
    }

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();
  } else {
    DINGO_LOG(WARNING) << "id_epoch_type_and_value is empty, skip import id epoch type.";
  }

  return butil::Status::OK();
}

butil::Status RestoreMeta::CreateOrUpdateAutoIncrementsToMeta() {
  if (table_increment_group_ && table_increment_group_->table_increments_size() > 0) {
    dingodb::pb::meta::CreateOrUpdateAutoIncrementsRequest request;
    dingodb::pb::meta::CreateOrUpdateAutoIncrementsResponse response;

    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
    request.mutable_table_increment_group()->CopyFrom(*table_increment_group_);

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

    auto status =
        coordinator_interaction_->SendRequest("MetaService", "CreateOrUpdateAutoIncrements", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_code() << " " << status.error_cstr();
      return status;
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << response.error().errcode() << " " << response.error().errmsg();
      return butil::Status(response.error().errcode(), response.error().errmsg());
    }

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();
  } else {
    DINGO_LOG(WARNING) << "table_increment_group is empty, skip create or update auto increments.";
  }

  return butil::Status::OK();
}

std::pair<int64_t, int64_t> RestoreMeta::GetRegions() {
  int64_t region_metas = 0;
  int64_t region_datas = 0;

  if (restore_sql_meta_) {
    auto regions = restore_sql_meta_->GetRegions();
    region_metas += regions.first;
    region_datas += regions.second;
  }

  if (restore_sdk_meta_) {
    auto regions = restore_sdk_meta_->GetRegions();
    region_metas += regions.first;
    region_datas += regions.second;
  }

  return std::pair<int64_t, int64_t>(region_metas, region_datas);
}

butil::Status RestoreMeta::CheckBackupMeta() {
  butil::Status status;

  if (backup_meta_) {
    std::string file_path = storage_internal_ + "/" + dingodb::Constant::kBackupMetaSchemaName;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = Utils::CheckBackupMeta(backup_meta_, storage_internal_, dingodb::Constant::kBackupMetaSchemaName, "",
                                    dingodb::Constant::kBackupRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }  //   if (backup_meta_) {

  return butil::Status::OK();
}

butil::Status RestoreMeta::ExtractFromBackupMeta() {
  butil::Status status;

  if (backup_meta_) {
    std::string file_path = storage_internal_ + "/" + backup_meta_->file_name();

    SstFileReader sst_file_reader;
    status = sst_file_reader.ReadFile(file_path, backupmeta_schema_kvs_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    // find store_region_sql_meta.sst
    auto iter = backupmeta_schema_kvs_.find(dingodb::Constant::kStoreRegionSqlMetaSstName);
    if (iter != backupmeta_schema_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_store_region_sql_meta_sst;
      auto ret = internal_store_region_sql_meta_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("{} parse dingodb::pb::common::BackupParam failed",
                                    dingodb::Constant::kStoreRegionSqlMetaSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      store_region_sql_meta_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_store_region_sql_meta_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta file. ignore.", dingodb::Constant::kStoreRegionSqlMetaSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find store_cf_sst_meta_sql_meta.sst
    iter = backupmeta_schema_kvs_.find(dingodb::Constant::kStoreCfSstMetaSqlMetaSstName);
    if (iter != backupmeta_schema_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_store_cf_sst_meta_sql_meta_sst;
      auto ret = internal_store_cf_sst_meta_sql_meta_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("{} parse dingodb::pb::common::BackupParam failed",
                                    dingodb::Constant::kStoreRegionSqlMetaSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      store_cf_sst_meta_sql_meta_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_store_cf_sst_meta_sql_meta_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta file. ignore.", dingodb::Constant::kStoreCfSstMetaSqlMetaSstName);
      DINGO_LOG(WARNING) << s;
    }

    // find coordinator_sdk_meta.sst
    iter = backupmeta_schema_kvs_.find(dingodb::Constant::kCoordinatorSdkMetaSstName);
    if (iter != backupmeta_schema_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_coordinator_sdk_meta_sst;
      auto ret = internal_coordinator_sdk_meta_sst.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("{} parse dingodb::pb::common::BackupParam failed",
                                    dingodb::Constant::kStoreRegionSqlMetaSstName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      coordinator_sdk_meta_sst_ =
          std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_coordinator_sdk_meta_sst));
    } else {
      std::string s =
          fmt::format("not found {} in backupmeta file. ignore.", dingodb::Constant::kCoordinatorSdkMetaSstName);
      DINGO_LOG(WARNING) << s;
    }

  }  //  if (backup_meta_) {

  return butil::Status::OK();
}

}  // namespace br
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

#include "br/restore_sql_meta.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/parameter.h"
#include "br/sst_file_reader.h"
#include "br/utils.h"
#include "common/constant.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

RestoreSqlMeta::RestoreSqlMeta(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                               const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
                               const std::string& storage_internal,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_sql_meta_sst,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_sql_meta_sst,
                               uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
                               int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      restorets_(restorets),
      restoretso_internal_(restoretso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      store_region_sql_meta_sst_(store_region_sql_meta_sst),
      store_cf_sst_meta_sql_meta_sst_(store_cf_sst_meta_sql_meta_sst),
      create_region_concurrency_(create_region_concurrency),
      restore_region_concurrency_(restore_region_concurrency),
      create_region_timeout_s_(create_region_timeout_s),
      restore_region_timeout_s_(restore_region_timeout_s),
      replica_num_(replica_num) {}

RestoreSqlMeta::~RestoreSqlMeta() = default;

std::shared_ptr<RestoreSqlMeta> RestoreSqlMeta::GetSelf() { return shared_from_this(); }

butil::Status RestoreSqlMeta::Init() {
  butil::Status status;

  if (store_region_sql_meta_sst_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << store_region_sql_meta_sst_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "store_region_sql_meta_sst_ = nullptr";
  }

  if (store_cf_sst_meta_sql_meta_sst_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << store_cf_sst_meta_sql_meta_sst_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "store_cf_sst_meta_sql_meta_sst_ = nullptr";
  }

  status = CheckStoreRegionSqlMetaSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = CheckStoreCfSstMetaSqlMetaSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromStoreRegionSqlMetaSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromStoreCfSstMetaSqlMetaSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  // double check
  if (id_and_sst_meta_group_kvs_ && !id_and_region_kvs_) {
    std::string s = "id_and_sst_meta_group_kvs_ is not null, but id_and_region_kvs_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  if (id_and_region_kvs_) {
    ServerInteractionPtr internal_coordinator_interaction;

    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    restore_region_meta_manager_ = std::make_shared<RestoreRegionMetaManager>(
        internal_coordinator_interaction, create_region_concurrency_, replica_num_, storage_internal_,
        id_and_region_kvs_, dingodb::Constant::kStoreRegionSqlMetaSstName, create_region_timeout_s_);

    status = restore_region_meta_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (id_and_sst_meta_group_kvs_ && id_and_region_kvs_) {
    ServerInteractionPtr internal_coordinator_interaction;

    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    ServerInteractionPtr internal_store_interaction;

    status = ServerInteraction::CreateInteraction(store_interaction_->GetAddrs(), internal_store_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    restore_region_data_manager_ = std::make_shared<RestoreRegionDataManager>(
        internal_coordinator_interaction, internal_store_interaction, restore_region_concurrency_, replica_num_, restorets_,
        restoretso_internal_, storage_, storage_internal_, id_and_sst_meta_group_kvs_,
        dingodb::Constant::kStoreCfSstMetaSqlMetaSstName, dingodb::Constant::kRestoreMeta, restore_region_timeout_s_,
        id_and_region_kvs_);

    status = restore_region_data_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreSqlMeta::Run() {
  butil::Status status;

  if (restore_region_meta_manager_) {
    status = restore_region_meta_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << "restore_region_meta_manager_ = nullptr. ignore run";
  }  // if (restore_region_meta_manager_)

  if (restore_region_data_manager_) {
    status = restore_region_data_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << "restore_region_data_manager_ = nullptr. ignore run";
  }
  // if (restore_region_data_manager_)

  return butil::Status::OK();
}

butil::Status RestoreSqlMeta::Finish() {
  butil::Status status;

  if (restore_region_meta_manager_) {
    status = restore_region_meta_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }  // if (restore_region_meta_manager_)

  if (restore_region_data_manager_) {
    status = restore_region_data_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }  //  if (restore_region_data_manager_) {

  return butil::Status::OK();
}

butil::Status RestoreSqlMeta::CheckStoreRegionSqlMetaSst() {
  butil::Status status;

  if (store_region_sql_meta_sst_) {
    std::string file_name = dingodb::Constant::kStoreRegionSqlMetaSstName;
    std::string file_path = storage_internal_ + "/" + file_name;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = Utils::CheckBackupMeta(store_region_sql_meta_sst_, storage_internal_,
                                    dingodb::Constant::kStoreRegionSqlMetaSstName, "",
                                    dingodb::Constant::kCoordinatorRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }  // if (store_region_sql_meta_sst_)

  return butil::Status::OK();
}

butil::Status RestoreSqlMeta::CheckStoreCfSstMetaSqlMetaSst() {
  butil::Status status;

  if (store_cf_sst_meta_sql_meta_sst_) {
    std::string file_name = dingodb::Constant::kStoreCfSstMetaSqlMetaSstName;
    std::string file_path = storage_internal_ + "/" + file_name;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = Utils::CheckBackupMeta(store_cf_sst_meta_sql_meta_sst_, storage_internal_,
                                    dingodb::Constant::kStoreCfSstMetaSqlMetaSstName, "",
                                    dingodb::Constant::kStoreRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }  // if (store_cf_sst_meta_sql_meta_sst_)

  return butil::Status::OK();
}

butil::Status RestoreSqlMeta::ExtractFromStoreRegionSqlMetaSst() {
  butil::Status status;

  if (store_region_sql_meta_sst_) {
    std::string file_path = storage_internal_ + "/" + store_region_sql_meta_sst_->file_name();

    SstFileReader sst_file_reader;
    std::map<std::string, std::string> internal_id_and_region_kvs;
    status = sst_file_reader.ReadFile(file_path, internal_id_and_region_kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    id_and_region_kvs_ = std::make_shared<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>>();
    for (const auto& [internal_id, region_str] : internal_id_and_region_kvs) {
      dingodb::pb::common::Region region;
      if (!region.ParseFromString(region_str)) {
        std::string s = fmt::format("parse dingodb::pb::common::Region failed : {}", internal_id);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      id_and_region_kvs_->emplace(std::stoll(internal_id),
                                  std::make_shared<dingodb::pb::common::Region>(std::move(region)));
    }
  }  // if (store_region_sql_meta_sst_)

  return butil::Status::OK();
}

butil::Status RestoreSqlMeta::ExtractFromStoreCfSstMetaSqlMetaSst() {
  butil::Status status;

  if (store_cf_sst_meta_sql_meta_sst_) {
    std::string file_path = storage_internal_ + "/" + store_cf_sst_meta_sql_meta_sst_->file_name();

    SstFileReader sst_file_reader;
    std::map<std::string, std::string> internal_id_and_sst_meta_group_kvs;
    status = sst_file_reader.ReadFile(file_path, internal_id_and_sst_meta_group_kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    id_and_sst_meta_group_kvs_ =
        std::make_shared<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>();
    for (const auto& [internal_id, group_str] : internal_id_and_sst_meta_group_kvs) {
      dingodb::pb::common::BackupDataFileValueSstMetaGroup group;
      if (!group.ParseFromString(group_str)) {
        std::string s =
            fmt::format("parse dingodb::pb::common::BackupDataFileValueSstMetaGroup failed : {}", internal_id);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      id_and_sst_meta_group_kvs_->emplace(
          std::stoll(internal_id),
          std::make_shared<dingodb::pb::common::BackupDataFileValueSstMetaGroup>(std::move(group)));
    }
  }  // if (store_cf_sst_meta_sql_meta_sst_)

  return butil::Status::OK();
}

std::pair<int64_t, int64_t> RestoreSqlMeta::GetRegions() {
  int64_t region_metas = 0;
  int64_t region_datas = 0;

  if (restore_region_meta_manager_) {
    region_metas = restore_region_meta_manager_->GetRegions();
  }

  if (restore_region_data_manager_) {
    region_datas = restore_region_data_manager_->GetRegions();
  }

  return std::pair<int64_t, int64_t>(region_metas, region_datas);
}

}  // namespace br
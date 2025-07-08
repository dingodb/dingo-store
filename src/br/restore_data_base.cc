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

#include "br/restore_data_base.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/sst_file_reader.h"
#include "br/utils.h"
#include "common/constant.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

#ifndef ENABLE_RESTORE_SQL_DATA_PTHREAD
#define ENABLE_RESTORE_SQL_DATA_PTHREAD
#endif

// #undef ENABLE_RESTORE_SQL_DATA_PTHREAD

RestoreDataBase::RestoreDataBase(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                                 ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                                 const std::string& restorets, int64_t restoretso_internal, const std::string& storage,
                                 const std::string& storage_internal,
                                 std::shared_ptr<dingodb::pb::common::BackupMeta> store_region_data_sst,
                                 std::shared_ptr<dingodb::pb::common::BackupMeta> store_cf_sst_meta_data_sst,
                                 std::shared_ptr<dingodb::pb::common::BackupMeta> index_region_data_sst,
                                 std::shared_ptr<dingodb::pb::common::BackupMeta> index_cf_sst_meta_data_sst,
                                 std::shared_ptr<dingodb::pb::common::BackupMeta> document_region_data_sst,
                                 std::shared_ptr<dingodb::pb::common::BackupMeta> document_cf_sst_meta_data_sst,
                                 uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
                                 int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num,
                                 const std::string& type_name)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      index_interaction_(index_interaction),
      document_interaction_(document_interaction),
      restorets_(restorets),
      restoretso_internal_(restoretso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      store_region_data_sst_(store_region_data_sst),
      store_cf_sst_meta_data_sst_(store_cf_sst_meta_data_sst),
      index_region_data_sst_(index_region_data_sst),
      index_cf_sst_meta_data_sst_(index_cf_sst_meta_data_sst),
      document_region_data_sst_(document_region_data_sst),
      document_cf_sst_meta_data_sst_(document_cf_sst_meta_data_sst),
      create_region_concurrency_(create_region_concurrency),
      restore_region_concurrency_(restore_region_concurrency),
      create_region_timeout_s_(create_region_timeout_s),
      restore_region_timeout_s_(restore_region_timeout_s),
      replica_num_(replica_num),
      type_name_(type_name) {}

RestoreDataBase::~RestoreDataBase() = default;

butil::Status RestoreDataBase::Init() {
  butil::Status status;

  if (store_region_data_sst_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << store_region_data_sst_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "store_region_data_sst_ = nullptr";
  }

  if (store_cf_sst_meta_data_sst_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << store_cf_sst_meta_data_sst_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "store_cf_sst_meta_data_sst_ = nullptr";
  }

  if (index_region_data_sst_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << index_region_data_sst_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "index_region_data_sst_ = nullptr";
  }

  if (index_cf_sst_meta_data_sst_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << index_cf_sst_meta_data_sst_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "index_cf_sst_meta_data_sst_ = nullptr";
  }

  if (document_region_data_sst_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << document_region_data_sst_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "document_region_data_sst_ = nullptr";
  }

  if (document_cf_sst_meta_data_sst_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << document_cf_sst_meta_data_sst_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "document_cf_sst_meta_data_sst_ = nullptr";
  }

  status = CheckStoreRegionDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = CheckStoreCfSstMetaDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = CheckIndexRegionDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = CheckIndexCfSstMetaDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = CheckDocumentRegionDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = CheckDocumentCfSstMetaDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromStoreRegionDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromStoreCfSstMetaDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromIndexRegionDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromIndexCfSstMetaDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromDocumentRegionDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromDocumentCfSstMetaDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  // double check
  if (store_id_and_sst_meta_group_kvs_ && !store_id_and_region_kvs_) {
    std::string s = "store_id_and_sst_meta_group_kvs_ is not null, but store_id_and_region_kvs_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  if (index_id_and_sst_meta_group_kvs_ && !index_id_and_region_kvs_) {
    std::string s = "index_id_and_sst_meta_group_kvs_ is not null, but index_id_and_region_kvs_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  if (document_id_and_sst_meta_group_kvs_ && !document_id_and_region_kvs_) {
    std::string s = "document_id_and_sst_meta_group_kvs_ is not null, but document_id_and_region_kvs_ is null";
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_META_NOT_FOUND, s);
  }

  // store region meta manager
  if (store_id_and_region_kvs_) {
    if (store_interaction_ == nullptr || (store_interaction_ != nullptr && store_interaction_->IsEmpty())) {
      std::string s = "store_id_and_region_kvs_ is not null, need store node, but store_interaction_ is null or empty";
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_STORE_NODE_NOT_EXIST, s);
    }

    ServerInteractionPtr internal_coordinator_interaction;

    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    std::string backup_meta_region_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_name = dingodb::Constant::kStoreRegionSdkDataSstName;
    } else {
      backup_meta_region_name = dingodb::Constant::kStoreRegionSqlDataSstName;
    }

    store_restore_region_meta_manager_ = std::make_shared<RestoreRegionMetaManager>(
        internal_coordinator_interaction, create_region_concurrency_, replica_num_, storage_internal_,
        store_id_and_region_kvs_, backup_meta_region_name, create_region_timeout_s_);

    status = store_restore_region_meta_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // store region data manager
  if (store_id_and_sst_meta_group_kvs_ && store_id_and_region_kvs_) {
    if (store_interaction_ == nullptr || (store_interaction_ != nullptr && store_interaction_->IsEmpty())) {
      std::string s =
          "store_id_and_sst_meta_group_kvs_ && store_id_and_region_kvs_ is not null, need store node, but "
          "store_interaction_ is null or empty";
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_STORE_NODE_NOT_EXIST, s);
    }

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

    std::string backup_meta_region_cf_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_cf_name = dingodb::Constant::kStoreCfSstMetaSdkDataSstName;
    } else {
      backup_meta_region_cf_name = dingodb::Constant::kStoreCfSstMetaSqlDataSstName;
    }

    store_restore_region_data_manager_ = std::make_shared<RestoreRegionDataManager>(
        internal_coordinator_interaction, internal_store_interaction, restore_region_concurrency_, replica_num_,
        restorets_, restoretso_internal_, storage_, storage_internal_, store_id_and_sst_meta_group_kvs_,
        backup_meta_region_cf_name, dingodb::Constant::kRestoreData, restore_region_timeout_s_,
        store_id_and_region_kvs_);

    status = store_restore_region_data_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // index region meta manager
  if (index_id_and_region_kvs_) {
    if (index_interaction_ == nullptr || (index_interaction_ != nullptr && index_interaction_->IsEmpty())) {
      std::string s = "index_id_and_region_kvs_ is not null, need index node, but index_interaction_ is null or empty";
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_INDEX_NODE_NOT_EXIST, s);
    }
    ServerInteractionPtr internal_coordinator_interaction;
    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    std::string backup_meta_region_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_name = dingodb::Constant::kIndexRegionSdkDataSstName;
    } else {
      backup_meta_region_name = dingodb::Constant::kIndexRegionSqlDataSstName;
    }

    index_restore_region_meta_manager_ = std::make_shared<RestoreRegionMetaManager>(
        internal_coordinator_interaction, create_region_concurrency_, replica_num_, storage_internal_,
        index_id_and_region_kvs_, backup_meta_region_name, create_region_timeout_s_);

    status = index_restore_region_meta_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // index region data manager
  if (index_id_and_sst_meta_group_kvs_ && index_id_and_region_kvs_) {
    if (index_interaction_ == nullptr || (index_interaction_ != nullptr && index_interaction_->IsEmpty())) {
      std::string s =
          "index_id_and_sst_meta_group_kvs_ && index_id_and_region_kvs_ is not null, need index node, but "
          "index_interaction_ is null or empty";
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_INDEX_NODE_NOT_EXIST, s);
    }

    ServerInteractionPtr internal_coordinator_interaction;
    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    ServerInteractionPtr internal_index_interaction;
    status = ServerInteraction::CreateInteraction(index_interaction_->GetAddrs(), internal_index_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    std::string backup_meta_region_cf_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_cf_name = dingodb::Constant::kIndexCfSstMetaSdkDataSstName;
    } else {
      backup_meta_region_cf_name = dingodb::Constant::kIndexCfSstMetaSqlDataSstName;
    }

    index_restore_region_data_manager_ = std::make_shared<RestoreRegionDataManager>(
        internal_coordinator_interaction, internal_index_interaction, restore_region_concurrency_, replica_num_,
        restorets_, restoretso_internal_, storage_, storage_internal_, index_id_and_sst_meta_group_kvs_,
        backup_meta_region_cf_name, dingodb::Constant::kRestoreData, restore_region_timeout_s_,
        index_id_and_region_kvs_);

    status = index_restore_region_data_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // document region meta manager
  if (document_id_and_region_kvs_) {
    if (document_interaction_ == nullptr || (document_interaction_ != nullptr && document_interaction_->IsEmpty())) {
      std::string s =
          "document_id_and_region_kvs_ is not null, need document node, but document_interaction_ is null or empty";
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_DOCUMENT_NODE_NOT_EXIST, s);
    }

    ServerInteractionPtr internal_coordinator_interaction;
    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    std::string backup_meta_region_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_name = dingodb::Constant::kDocumentRegionSdkDataSstName;
    } else {
      backup_meta_region_name = dingodb::Constant::kDocumentRegionSqlDataSstName;
    }

    document_restore_region_meta_manager_ = std::make_shared<RestoreRegionMetaManager>(
        internal_coordinator_interaction, create_region_concurrency_, replica_num_, storage_internal_,
        document_id_and_region_kvs_, backup_meta_region_name, create_region_timeout_s_);

    status = document_restore_region_meta_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // document region data manager
  if (document_id_and_sst_meta_group_kvs_ && document_id_and_region_kvs_) {
    if (document_interaction_ == nullptr || (document_interaction_ != nullptr && document_interaction_->IsEmpty())) {
      std::string s =
          "document_id_and_sst_meta_group_kvs_ && document_id_and_region_kvs_ is not null, need document node, but "
          "document_interaction_ is null or empty";
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::ERESTORE_DOCUMENT_NODE_NOT_EXIST, s);
    }

    ServerInteractionPtr internal_coordinator_interaction;
    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    ServerInteractionPtr internal_document_interaction;
    status = ServerInteraction::CreateInteraction(document_interaction_->GetAddrs(), internal_document_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    std::string backup_meta_region_cf_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_cf_name = dingodb::Constant::kDocumentCfSstMetaSdkDataSstName;
    } else {
      backup_meta_region_cf_name = dingodb::Constant::kDocumentCfSstMetaSqlDataSstName;
    }

    document_restore_region_data_manager_ = std::make_shared<RestoreRegionDataManager>(
        internal_coordinator_interaction, internal_document_interaction, restore_region_concurrency_, replica_num_,
        restorets_, restoretso_internal_, storage_, storage_internal_, document_id_and_sst_meta_group_kvs_,
        backup_meta_region_cf_name, dingodb::Constant::kRestoreData, restore_region_timeout_s_,
        document_id_and_region_kvs_);

    status = document_restore_region_data_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::Run() {
  butil::Status status;

  // store region meta manager
  if (store_restore_region_meta_manager_) {
    status = store_restore_region_meta_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << type_name_ << " store_restore_region_meta_manager_ = nullptr. ignore run";
  }

  // store region data manager
  if (store_restore_region_data_manager_) {
    status = store_restore_region_data_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << type_name_ << " store_restore_region_data_manager_ = nullptr. ignore run";
  }

  // index region meta manager
  if (index_restore_region_meta_manager_) {
    status = index_restore_region_meta_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << type_name_ << " index_restore_region_meta_manager_ = nullptr. ignore run";
  }

  // index region data manager
  if (index_restore_region_data_manager_) {
    status = index_restore_region_data_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << type_name_ << " index_restore_region_data_manager_ = nullptr. ignore run";
  }

  // document region meta manager
  if (document_restore_region_meta_manager_) {
    status = document_restore_region_meta_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << type_name_ << " document_restore_region_meta_manager_ = nullptr. ignore run";
  }

  // document region data manager
  if (document_restore_region_data_manager_) {
    status = document_restore_region_data_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << type_name_ << " document_restore_region_data_manager_ = nullptr. ignore run";
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::Finish() {
  butil::Status status;

  // store region meta manager
  if (store_restore_region_meta_manager_) {
    status = store_restore_region_meta_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // store region data manager
  if (store_restore_region_data_manager_) {
    status = store_restore_region_data_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // index region meta manager
  if (index_restore_region_meta_manager_) {
    status = index_restore_region_meta_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // index region data manager
  if (index_restore_region_data_manager_) {
    status = index_restore_region_data_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // document region meta manager
  if (document_restore_region_meta_manager_) {
    status = document_restore_region_meta_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // document region data manager
  if (document_restore_region_data_manager_) {
    status = document_restore_region_data_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  return butil::Status::OK();
}

std::pair<int64_t, int64_t> RestoreDataBase::GetRegions() {
  int64_t region_metas = 0;
  int64_t region_datas = 0;

  if (store_restore_region_meta_manager_) {
    region_metas = store_restore_region_meta_manager_->GetRegions();
  }

  if (store_restore_region_data_manager_) {
    region_datas = store_restore_region_data_manager_->GetRegions();
  }

  if (index_restore_region_meta_manager_) {
    region_metas += index_restore_region_meta_manager_->GetRegions();
  }

  if (index_restore_region_data_manager_) {
    region_datas += index_restore_region_data_manager_->GetRegions();
  }

  if (document_restore_region_meta_manager_) {
    region_metas += document_restore_region_meta_manager_->GetRegions();
  }

  if (document_restore_region_data_manager_) {
    region_datas += document_restore_region_data_manager_->GetRegions();
  }

  return std::pair<int64_t, int64_t>(region_metas, region_datas);
}

butil::Status RestoreDataBase::CheckStoreRegionDataSst() {
  butil::Status status;

  if (store_region_data_sst_) {
    std::string backup_meta_region_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_name = dingodb::Constant::kStoreRegionSdkDataSstName;
    } else {
      backup_meta_region_name = dingodb::Constant::kStoreRegionSqlDataSstName;
    }

    std::string file_path = storage_internal_ + "/" + backup_meta_region_name;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = CheckRegionDataSst(store_region_data_sst_, backup_meta_region_name);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }  // if (store_region_data_sst_)

  return butil::Status::OK();
}
butil::Status RestoreDataBase::CheckStoreCfSstMetaDataSst() {
  butil::Status status;

  if (store_cf_sst_meta_data_sst_) {
    std::string backup_meta_region_cf_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_cf_name = dingodb::Constant::kStoreCfSstMetaSdkDataSstName;
    } else {
      backup_meta_region_cf_name = dingodb::Constant::kStoreCfSstMetaSqlDataSstName;
    }

    std::string file_path = storage_internal_ + "/" + backup_meta_region_cf_name;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = CheckCfSstMetaDataSst(store_cf_sst_meta_data_sst_, backup_meta_region_cf_name,
                                   dingodb::Constant::kStoreRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

  }  // if (store_cf_sst_meta_data_sst_)

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckIndexRegionDataSst() {
  butil::Status status;

  if (index_region_data_sst_) {
    std::string backup_meta_region_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_name = dingodb::Constant::kIndexRegionSdkDataSstName;
    } else {
      backup_meta_region_name = dingodb::Constant::kIndexRegionSqlDataSstName;
    }

    std::string file_path = storage_internal_ + "/" + backup_meta_region_name;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = CheckRegionDataSst(index_region_data_sst_, backup_meta_region_name);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

  }  // if (index_region_data_sst_)

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckIndexCfSstMetaDataSst() {
  butil::Status status;

  if (index_cf_sst_meta_data_sst_) {
    std::string backup_meta_region_cf_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_cf_name = dingodb::Constant::kIndexCfSstMetaSdkDataSstName;
    } else {
      backup_meta_region_cf_name = dingodb::Constant::kIndexCfSstMetaSqlDataSstName;
    }

    std::string file_path = storage_internal_ + "/" + backup_meta_region_cf_name;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = CheckCfSstMetaDataSst(index_cf_sst_meta_data_sst_, backup_meta_region_cf_name,
                                   dingodb::Constant::kIndexRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

  }  // if (index_cf_sst_meta_data_sst_)

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckDocumentRegionDataSst() {
  butil::Status status;

  if (document_region_data_sst_) {
    std::string backup_meta_region_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_name = dingodb::Constant::kDocumentRegionSdkDataSstName;
    } else {
      backup_meta_region_name = dingodb::Constant::kDocumentRegionSqlDataSstName;
    }

    std::string file_path = storage_internal_ + "/" + backup_meta_region_name;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = CheckRegionDataSst(document_region_data_sst_, backup_meta_region_name);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

  }  // if (document_region_data_sst_)

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckDocumentCfSstMetaDataSst() {
  butil::Status status;

  if (document_cf_sst_meta_data_sst_) {
    std::string backup_meta_region_cf_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_cf_name = dingodb::Constant::kDocumentCfSstMetaSdkDataSstName;
    } else {
      backup_meta_region_cf_name = dingodb::Constant::kDocumentCfSstMetaSqlDataSstName;
    }

    std::string file_path = storage_internal_ + "/" + backup_meta_region_cf_name;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = CheckCfSstMetaDataSst(document_cf_sst_meta_data_sst_, backup_meta_region_cf_name,
                                   dingodb::Constant::kDocumentRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

  }  // document_cf_sst_meta_data_sst_

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckRegionDataSst(std::shared_ptr<dingodb::pb::common::BackupMeta> region_data_sst,
                                                  const std::string& file_name) {
  butil::Status status;

  status = Utils::CheckBackupMeta(region_data_sst, storage_internal_, file_name, "",
                                  dingodb::Constant::kCoordinatorRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckCfSstMetaDataSst(
    std::shared_ptr<dingodb::pb::common::BackupMeta> cf_sst_meta_data_sst, const std::string& file_name,
    const std::string& exec_node) {
  butil::Status status;

  status = Utils::CheckBackupMeta(cf_sst_meta_data_sst, storage_internal_, file_name, "", exec_node);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::ExtractFromStoreRegionDataSst() {
  return ExtractFromRegionDataSst(store_region_data_sst_, store_id_and_region_kvs_);
}
butil::Status RestoreDataBase::ExtractFromStoreCfSstMetaDataSst() {
  return ExtractFromCfSstMetaDataSst(store_cf_sst_meta_data_sst_, store_id_and_sst_meta_group_kvs_);
}

butil::Status RestoreDataBase::ExtractFromIndexRegionDataSst() {
  return ExtractFromRegionDataSst(index_region_data_sst_, index_id_and_region_kvs_);
}
butil::Status RestoreDataBase::ExtractFromIndexCfSstMetaDataSst() {
  return ExtractFromCfSstMetaDataSst(index_cf_sst_meta_data_sst_, index_id_and_sst_meta_group_kvs_);
}

butil::Status RestoreDataBase::ExtractFromDocumentRegionDataSst() {
  return ExtractFromRegionDataSst(document_region_data_sst_, document_id_and_region_kvs_);
}
butil::Status RestoreDataBase::ExtractFromDocumentCfSstMetaDataSst() {
  return ExtractFromCfSstMetaDataSst(document_cf_sst_meta_data_sst_, document_id_and_sst_meta_group_kvs_);
}

butil::Status RestoreDataBase::ExtractFromRegionDataSst(
    std::shared_ptr<dingodb::pb::common::BackupMeta> region_data_sst,
    std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>>& id_and_region_kvs) {
  butil::Status status;

  if (region_data_sst) {
    std::string file_path = storage_internal_ + "/" + region_data_sst->file_name();

    SstFileReader sst_file_reader;
    std::map<std::string, std::string> internal_id_and_region_kvs;
    status = sst_file_reader.ReadFile(file_path, internal_id_and_region_kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    id_and_region_kvs = std::make_shared<std::map<int64_t, std::shared_ptr<dingodb::pb::common::Region>>>();
    for (const auto& [internal_id, region_str] : internal_id_and_region_kvs) {
      dingodb::pb::common::Region region;
      if (!region.ParseFromString(region_str)) {
        std::string s = fmt::format("parse dingodb::pb::common::Region failed : {}", internal_id);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      id_and_region_kvs->emplace(std::stoll(internal_id),
                                 std::make_shared<dingodb::pb::common::Region>(std::move(region)));
    }

  }  // if(region_data_sst)

  return butil::Status::OK();
}

butil::Status RestoreDataBase::ExtractFromCfSstMetaDataSst(
    std::shared_ptr<dingodb::pb::common::BackupMeta> region_data_sst,
    std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>&
        id_and_sst_meta_group_kvs) {
  butil::Status status;

  if (region_data_sst) {
    std::string file_path = storage_internal_ + "/" + region_data_sst->file_name();

    SstFileReader sst_file_reader;
    std::map<std::string, std::string> internal_id_and_sst_meta_group_kvs;
    status = sst_file_reader.ReadFile(file_path, internal_id_and_sst_meta_group_kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    id_and_sst_meta_group_kvs =
        std::make_shared<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>();
    for (const auto& [internal_id, group_str] : internal_id_and_sst_meta_group_kvs) {
      dingodb::pb::common::BackupDataFileValueSstMetaGroup group;
      if (!group.ParseFromString(group_str)) {
        std::string s =
            fmt::format("parse dingodb::pb::common::BackupDataFileValueSstMetaGroup failed : {}", internal_id);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      id_and_sst_meta_group_kvs->emplace(
          std::stoll(internal_id),
          std::make_shared<dingodb::pb::common::BackupDataFileValueSstMetaGroup>(std::move(group)));
    }

  }  // region_data_sst

  return butil::Status::OK();
}

}  // namespace br
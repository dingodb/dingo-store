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

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << store_region_data_sst_->DebugString();
  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << store_region_data_sst_->DebugString();

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << index_region_data_sst_->DebugString();
  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << index_cf_sst_meta_data_sst_->DebugString();

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << document_region_data_sst_->DebugString();
  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << document_cf_sst_meta_data_sst_->DebugString();

  status = CheckStoreRegionDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckStoreCfSstMetaDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckIndexRegionDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckIndexCfSstMetaDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckDocumentRegionDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = CheckDocumentCfSstMetaDataSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
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
    ServerInteractionPtr internal_coordinator_interaction;

    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), coordinator_interaction_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
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
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // store region data manager
  if (store_id_and_sst_meta_group_kvs_ && store_id_and_region_kvs_) {
    ServerInteractionPtr internal_coordinator_interaction;

    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), coordinator_interaction_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    ServerInteractionPtr internal_interaction;

    status = ServerInteraction::CreateInteraction(store_interaction_->GetAddrs(), internal_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::string backup_meta_region_cf_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_cf_name = dingodb::Constant::kStoreCfSstMetaSdkDataSstName;
    } else {
      backup_meta_region_cf_name = dingodb::Constant::kStoreCfSstMetaSqlDataSstName;
    }

    store_restore_region_data_manager_ = std::make_shared<RestoreRegionDataManager>(
        internal_coordinator_interaction, internal_interaction, restore_region_concurrency_, replica_num_, restorets_,
        restoretso_internal_, storage_, storage_internal_, store_id_and_sst_meta_group_kvs_, backup_meta_region_cf_name,
        dingodb::Constant::kRestoreData, restore_region_timeout_s_, store_id_and_region_kvs_);

    status = store_restore_region_data_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // index region meta manager
  if (index_id_and_region_kvs_) {
    ServerInteractionPtr internal_coordinator_interaction;

    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), coordinator_interaction_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
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
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // index region data manager
  if (index_id_and_sst_meta_group_kvs_ && index_id_and_region_kvs_) {
    ServerInteractionPtr internal_coordinator_interaction;

    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), coordinator_interaction_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    ServerInteractionPtr internal_interaction;

    status = ServerInteraction::CreateInteraction(index_interaction_->GetAddrs(), internal_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::string backup_meta_region_cf_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_cf_name = dingodb::Constant::kIndexCfSstMetaSdkDataSstName;
    } else {
      backup_meta_region_cf_name = dingodb::Constant::kIndexCfSstMetaSqlDataSstName;
    }

    index_restore_region_data_manager_ = std::make_shared<RestoreRegionDataManager>(
        internal_coordinator_interaction, internal_interaction, restore_region_concurrency_, replica_num_, restorets_,
        restoretso_internal_, storage_, storage_internal_, index_id_and_sst_meta_group_kvs_, backup_meta_region_cf_name,
        dingodb::Constant::kRestoreData, restore_region_timeout_s_, index_id_and_region_kvs_);

    status = index_restore_region_data_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // document region meta manager
  if (document_id_and_region_kvs_) {
    ServerInteractionPtr internal_coordinator_interaction;

    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), coordinator_interaction_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
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
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // document region data manager
  if (document_id_and_sst_meta_group_kvs_ && document_id_and_region_kvs_) {
    ServerInteractionPtr internal_coordinator_interaction;

    butil::Status status =
        ServerInteraction::CreateInteraction(coordinator_interaction_->GetAddrs(), coordinator_interaction_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    ServerInteractionPtr internal_interaction;

    status = ServerInteraction::CreateInteraction(document_interaction_->GetAddrs(), internal_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::string backup_meta_region_cf_name;

    if (type_name_ == dingodb::Constant::kSdkData) {
      backup_meta_region_cf_name = dingodb::Constant::kDocumentCfSstMetaSdkDataSstName;
    } else {
      backup_meta_region_cf_name = dingodb::Constant::kDocumentCfSstMetaSqlDataSstName;
    }

    document_restore_region_data_manager_ = std::make_shared<RestoreRegionDataManager>(
        internal_coordinator_interaction, internal_interaction, restore_region_concurrency_, replica_num_, restorets_,
        restoretso_internal_, storage_, storage_internal_, document_id_and_sst_meta_group_kvs_,
        backup_meta_region_cf_name, dingodb::Constant::kRestoreData, restore_region_timeout_s_,
        document_id_and_region_kvs_);

    status = document_restore_region_data_manager_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
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
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // store region data manager
  if (store_restore_region_data_manager_) {
    status = store_restore_region_data_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // index region meta manager
  if (index_restore_region_meta_manager_) {
    status = index_restore_region_meta_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // index region data manager
  if (index_restore_region_data_manager_) {
    status = index_restore_region_data_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // document region meta manager
  if (document_restore_region_meta_manager_) {
    status = document_restore_region_meta_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // document region data manager
  if (document_restore_region_data_manager_) {
    status = document_restore_region_data_manager_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::Finish() {
  butil::Status status;

  // store region meta manager
  if (store_restore_region_meta_manager_) {
    status = store_restore_region_meta_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // store region data manager
  if (store_restore_region_data_manager_) {
    status = store_restore_region_data_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // index region meta manager
  if (index_restore_region_meta_manager_) {
    status = index_restore_region_meta_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // index region data manager
  if (index_restore_region_data_manager_) {
    status = index_restore_region_data_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // document region meta manager
  if (document_restore_region_meta_manager_) {
    status = document_restore_region_meta_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // document region data manager
  if (document_restore_region_data_manager_) {
    status = document_restore_region_data_manager_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckStoreRegionDataSst() {
  butil::Status status;

  std::string backup_meta_region_name;

  if (type_name_ == dingodb::Constant::kSdkData) {
    backup_meta_region_name = dingodb::Constant::kStoreRegionSdkDataSstName;
  } else {
    backup_meta_region_name = dingodb::Constant::kStoreRegionSqlDataSstName;
  }

  std::string file_path = storage_internal_ + "/" + backup_meta_region_name;
  status = Utils::FileExistsAndRegular(file_path);
  if (status.ok()) {
    status = CheckRegionDataSst(store_region_data_sst_, backup_meta_region_name);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}
butil::Status RestoreDataBase::CheckStoreCfSstMetaDataSst() {
  butil::Status status;

  std::string backup_meta_region_cf_name;

  if (type_name_ == dingodb::Constant::kSdkData) {
    backup_meta_region_cf_name = dingodb::Constant::kStoreCfSstMetaSdkDataSstName;
  } else {
    backup_meta_region_cf_name = dingodb::Constant::kStoreCfSstMetaSqlDataSstName;
  }

  std::string file_path = storage_internal_ + "/" + backup_meta_region_cf_name;
  status = Utils::FileExistsAndRegular(file_path);
  if (status.ok()) {
    status = CheckCfSstMetaDataSst(store_cf_sst_meta_data_sst_, backup_meta_region_cf_name,
                                   dingodb::Constant::kStoreRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckIndexRegionDataSst() {
  butil::Status status;

  std::string backup_meta_region_name;

  if (type_name_ == dingodb::Constant::kSdkData) {
    backup_meta_region_name = dingodb::Constant::kIndexRegionSdkDataSstName;
  } else {
    backup_meta_region_name = dingodb::Constant::kIndexRegionSqlDataSstName;
  }

  std::string file_path = storage_internal_ + "/" + backup_meta_region_name;
  status = Utils::FileExistsAndRegular(file_path);
  if (status.ok()) {
    status = CheckRegionDataSst(index_region_data_sst_, backup_meta_region_name);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckIndexCfSstMetaDataSst() {
  butil::Status status;

  std::string backup_meta_region_cf_name;

  if (type_name_ == dingodb::Constant::kSdkData) {
    backup_meta_region_cf_name = dingodb::Constant::kIndexCfSstMetaSdkDataSstName;
  } else {
    backup_meta_region_cf_name = dingodb::Constant::kIndexCfSstMetaSqlDataSstName;
  }

  std::string file_path = storage_internal_ + "/" + backup_meta_region_cf_name;
  status = Utils::FileExistsAndRegular(file_path);
  if (status.ok()) {
    status = CheckCfSstMetaDataSst(index_cf_sst_meta_data_sst_, backup_meta_region_cf_name,
                                   dingodb::Constant::kIndexRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckDocumentRegionDataSst() {
  butil::Status status;

  std::string backup_meta_region_name;

  if (type_name_ == dingodb::Constant::kSdkData) {
    backup_meta_region_name = dingodb::Constant::kDocumentRegionSdkDataSstName;
  } else {
    backup_meta_region_name = dingodb::Constant::kDocumentRegionSqlDataSstName;
  }

  std::string file_path = storage_internal_ + "/" + backup_meta_region_name;
  status = Utils::FileExistsAndRegular(file_path);
  if (status.ok()) {
    status = CheckRegionDataSst(document_region_data_sst_, backup_meta_region_name);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }
  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckDocumentCfSstMetaDataSst() {
  butil::Status status;

  std::string backup_meta_region_cf_name;

  if (type_name_ == dingodb::Constant::kSdkData) {
    backup_meta_region_cf_name = dingodb::Constant::kDocumentCfSstMetaSdkDataSstName;
  } else {
    backup_meta_region_cf_name = dingodb::Constant::kDocumentCfSstMetaSqlDataSstName;
  }

  std::string file_path = storage_internal_ + "/" + backup_meta_region_cf_name;
  status = Utils::FileExistsAndRegular(file_path);
  if (status.ok()) {
    status = CheckCfSstMetaDataSst(document_cf_sst_meta_data_sst_, backup_meta_region_cf_name,
                                   dingodb::Constant::kDocumentRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::CheckRegionDataSst(std::shared_ptr<dingodb::pb::common::BackupMeta> region_data_sst,
                                                  const std::string& file_name) {
  butil::Status status;

  status = Utils::CheckBackupMeta(region_data_sst, storage_internal_, file_name, "",
                                  dingodb::Constant::kCoordinatorRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
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
    DINGO_LOG(ERROR) << status.error_cstr();
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

  std::string file_path = storage_internal_ + "/" + region_data_sst->file_name();

  status = Utils::FileExistsAndRegular(file_path);
  if (status.ok()) {
    SstFileReader sst_file_reader;
    std::map<std::string, std::string> internal_id_and_region_kvs;
    status = sst_file_reader.ReadFile(file_path, internal_id_and_region_kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
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
  }

  return butil::Status::OK();
}

butil::Status RestoreDataBase::ExtractFromCfSstMetaDataSst(
    std::shared_ptr<dingodb::pb::common::BackupMeta> region_data_sst,
    std::shared_ptr<std::map<int64_t, std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup>>>&
        id_and_sst_meta_group_kvs) {
  butil::Status status;

  std::string file_path = storage_internal_ + "/" + region_data_sst->file_name();

  status = Utils::FileExistsAndRegular(file_path);
  if (status.ok()) {
    SstFileReader sst_file_reader;
    std::map<std::string, std::string> internal_id_and_sst_meta_group_kvs;
    status = sst_file_reader.ReadFile(file_path, internal_id_and_sst_meta_group_kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
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
  }

  return butil::Status::OK();
}

}  // namespace br
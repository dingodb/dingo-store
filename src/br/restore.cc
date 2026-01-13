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

#include "br/restore.h"

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/sst_file_reader.h"
#include "br/utils.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "common/uuid.h"
#include "common/version.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

namespace br {

#ifndef ENABLE_RESTORE_PTHREAD
#define ENABLE_RESTORE_PTHREAD
#endif

// #undef ENABLE_RESTORE_PTHREAD

Restore::Restore(const RestoreParams& params, uint32_t create_region_concurrency, uint32_t restore_region_concurrency,
                 int64_t create_region_timeout_s, int64_t restore_region_timeout_s, int32_t replica_num)
    : coor_url_(br::InteractionManager::GetInstance().GetCoordinatorInteraction()->GetAddrsAsString()),
      store_url_(br::InteractionManager::GetInstance().GetStoreInteraction() != nullptr
                     ? br::InteractionManager::GetInstance().GetStoreInteraction()->GetAddrsAsString()
                     : std::string()),
      index_url_(br::InteractionManager::GetInstance().GetIndexInteraction() != nullptr
                     ? br::InteractionManager::GetInstance().GetIndexInteraction()->GetAddrsAsString()
                     : std::string()),
      document_url_(br::InteractionManager::GetInstance().GetDocumentInteraction() != nullptr
                        ? br::InteractionManager::GetInstance().GetDocumentInteraction()->GetAddrsAsString()
                        : std::string()),
      br_type_(params.br_type),
      br_restore_type_(params.br_restore_type),
      storage_(params.storage),
      storage_internal_(params.storage_internal),
      create_region_concurrency_(create_region_concurrency),
      restore_region_concurrency_(restore_region_concurrency),
      create_region_timeout_s_(create_region_timeout_s),
      restore_region_timeout_s_(restore_region_timeout_s),
      replica_num_(replica_num),
      is_gc_stop_(false),
      is_gc_enable_after_finish_(false),
      is_need_exit_(false),
      is_already_register_restore_to_coordinator_(false),
      is_exit_register_restore_to_coordinator_thread_(true),
      region_auto_split_enable_after_finish_(false),
      region_auto_merge_enable_after_finish_(false),
      balance_leader_enable_after_finish_(false),
      balance_region_enable_after_finish_(false),
      start_time_ms_(dingodb::Helper::TimestampMs()),
      end_time_ms_(0) {
  bthread_mutex_init(&mutex_, nullptr);
}

Restore::~Restore() { bthread_mutex_destroy(&mutex_); };

std::shared_ptr<Restore> Restore::GetSelf() { return shared_from_this(); }

butil::Status Restore::Init() {
  butil::Status status;
  status = ParamsCheck();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  // find backup ts
  auto iter = backupmeta_file_kvs_.find(dingodb::Constant::kBackupBackupParamKey);
  if (iter == backupmeta_file_kvs_.end()) {
    std::string s = fmt::format("not found {} in backupmeta file.", dingodb::Constant::kBackupBackupParamKey);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_NOT_FOUND_KEY_IN_FILE, s);
  }

  dingodb::pb::common::BackupParam backup_param;
  auto ret = backup_param.ParseFromString(iter->second);
  if (!ret) {
    std::string s = fmt::format("parse dingodb::pb::common::BackupParam failed");
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << backup_param.DebugString();

  restorets_ = backup_param.backupts();
  restoretso_internal_ = backup_param.backuptso_internal();

  // find version
  iter = backupmeta_file_kvs_.find(dingodb::Constant::kBackupVersionKey);
  if (iter == backupmeta_file_kvs_.end()) {
    std::string s = fmt::format("not found {} in backupmeta file.", dingodb::Constant::kBackupVersionKey);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_NOT_FOUND_KEY_IN_FILE, s);
  }

  ServerInteractionPtr internal_coordinator_interaction;

  status = ServerInteraction::CreateInteraction(coor_url_, internal_coordinator_interaction);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  dingodb::pb::common::VersionInfo version_info_in_backup;
  ret = version_info_in_backup.ParseFromString(iter->second);
  if (!ret) {
    std::string s = fmt::format("parse dingodb::pb::common::VersionInfo failed");
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  // version compare
  dingodb::pb::common::VersionInfo version_info_local = dingodb::GetVersionInfo();

  dingodb::pb::common::VersionInfo version_info_remote;
  status = GetVersionFromCoordinator(internal_coordinator_interaction, version_info_remote);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = CompareVersion(version_info_local, version_info_remote, version_info_in_backup);
  if (!status.ok()) {
    if (FLAGS_restore_strict_version_comparison) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    } else {
      std::cout << "ignore version compare" << std::endl;
      DINGO_LOG(INFO) << "ignore version compare";
    }
  }

  std::cout << "version compare ok" << std::endl;
  DINGO_LOG(INFO) << "version compare ok";

  status = CheckGcSafePoint();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::cout << "safe point ts check ok" << std::endl;
  DINGO_LOG(INFO) << "safe point ts check ok";

  status = Restore::GetAllRegionMapFromCoordinator(internal_coordinator_interaction);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::cout << "dingo store is an empty database  ok" << std::endl;
  DINGO_LOG(INFO) << "dingo store is an empty database  ok";

  restore_task_id_ = dingodb::UUIDGenerator::GenerateUUID();

  auto lambda_exit_function = [this, &status]() {
    if (!status.ok()) {
      DoFinish();
      last_error_ = status;
    };
  };

  std::cout << "restore_task_id : " << restore_task_id_ << std::endl;
  DINGO_LOG(INFO) << "restore_task_id : " << restore_task_id_ << std::endl;

  dingodb::ON_SCOPE_EXIT(lambda_exit_function);

  // is backup
  status = RegisterBackupStatusToCoordinator(internal_coordinator_interaction);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::cout << "not found register backup To coordinator ok" << std::endl;
  DINGO_LOG(INFO) << "not found register backup To coordinator ok";

  // try to register restore task
  bool is_first = true;
  status = RegisterRestoreToCoordinator(is_first, internal_coordinator_interaction);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  is_already_register_restore_to_coordinator_ = true;

  std::cout << "register restore To coordinator ok" << std::endl;
  DINGO_LOG(INFO) << "register restore To coordinator ok";

  // set gc stop
  if (!is_gc_stop_) {
    status = SetGcStop();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
    std::cout << "gc set stopped ok" << std::endl;
    DINGO_LOG(INFO) << "gc set stopped ok";
  } else {
    std::cout << "gc already stopped. ignore" << std::endl;
    DINGO_LOG(INFO) << "gc already stopped. ignore";
  }

  status = DisableBalanceToCoordinator(internal_coordinator_interaction);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (balance_leader_enable_after_finish_) {
    std::cout << "balance leader set stopped ok" << std::endl;
    DINGO_LOG(INFO) << "balance leader set stopped ok";
  } else {
    std::cout << "balance leader already stopped. ignore" << std::endl;
    DINGO_LOG(INFO) << "balance leader already stopped. ignore";
  }

  if (balance_region_enable_after_finish_) {
    std::cout << "balance region set stopped ok" << std::endl;
    DINGO_LOG(INFO) << "balance region set stopped ok";
  } else {
    std::cout << "balance region already stopped. ignore" << std::endl;
    DINGO_LOG(INFO) << "balance region already stopped. ignore";
  }

  ServerInteractionPtr internal_store_interaction;
  if (!store_url_.empty()) {
    status = ServerInteraction::CreateInteraction(store_url_, internal_store_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  ServerInteractionPtr internal_index_interaction;
  if (!index_url_.empty()) {
    status = ServerInteraction::CreateInteraction(index_url_, internal_index_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  status = DisableSplitAndMergeToStoreAndIndex(internal_store_interaction, internal_index_interaction);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (region_auto_split_enable_after_finish_) {
    std::cout << "region auto split set stopped ok" << std::endl;
    DINGO_LOG(INFO) << "region auto split set stopped ok";
  } else {
    std::cout << "region auto split already stopped. ignore" << std::endl;
    DINGO_LOG(INFO) << "region auto split already stopped. ignore";
  }

  if (region_auto_merge_enable_after_finish_) {
    std::cout << "region auto merge set stopped ok" << std::endl;
    DINGO_LOG(INFO) << "region auto merge set stopped ok";
  } else {
    std::cout << "region auto merge already stopped. ignore" << std::endl;
    DINGO_LOG(INFO) << "region auto merge already stopped. ignore";
  }

  DINGO_LOG(INFO) << "Restore::Init " << " success";

  return butil::Status::OK();
}

butil::Status Restore::Run() {
  butil::Status status;

  auto lambda_exit_function = [this, &status]() {
    if (!status.ok()) {
      DoFinish();
      last_error_ = status;
    };
  };

  dingodb::ON_SCOPE_EXIT(lambda_exit_function);

  // create register restore task to coordinator
  {
    std::shared_ptr<br::ServerInteraction> internal_coordinator_interaction;
    status = ServerInteraction::CreateInteraction(coor_url_, internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    // register restore task to coordinator
    status = DoAsyncRegisterRestoreToCoordinator(internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  std::cout << "Restore task " << restore_task_id_ << " is registered to coordinator Periodicity." << std::endl;
  DINGO_LOG(INFO) << "Restore task " << restore_task_id_ << " is registered to coordinator Periodicity.";

  return DoRun();
}

butil::Status Restore::Finish() {
  butil::Status status;
  status = DoFinish();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  end_time_ms_ = dingodb::Helper::TimestampMs();

  const auto& [region_metas, region_datas] = GetRegions();

  std::string s = fmt::format(
      "[Full Restore success summary][total-range-metas={}]"
      "[total-range-datas={}][total-take={} s] ",
      region_metas, region_datas, (end_time_ms_ - start_time_ms_) / 1000.0);
  std::cout << s << std::endl;
  DINGO_LOG(INFO) << s;
  return butil::Status::OK();
}

butil::Status Restore::DoRun() {
  butil::Status status;

  // create restore meta
  {
    std::shared_ptr<br::ServerInteraction> internal_coordinator_interaction;
    status = ServerInteraction::CreateInteraction(coor_url_, internal_coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    std::shared_ptr<br::ServerInteraction> internal_store_interaction;
    if (!store_url_.empty()) {
      status = ServerInteraction::CreateInteraction(store_url_, internal_store_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }
    }

    // find meta meta
    std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta_meta;
    auto iter = backupmeta_file_kvs_.find(dingodb::Constant::kBackupMetaSchemaName);
    if (iter != backupmeta_file_kvs_.end()) {
      dingodb::pb::common::BackupMeta internal_backup_meta_meta;
      auto ret = internal_backup_meta_meta.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupMeta failed");
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
      backup_meta_meta = std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_backup_meta_meta));
    } else {
      std::string s = fmt::format("not found {} in backupmeta file.", dingodb::Constant::kBackupMetaSchemaName);
      DINGO_LOG(WARNING) << s;
    }

    // find IdEpochTypeAndValueKey
    std::shared_ptr<dingodb::pb::meta::IdEpochTypeAndValue> id_epoch_type_and_value;
    iter = backupmeta_file_kvs_.find(dingodb::Constant::kIdEpochTypeAndValueKey);
    if (iter != backupmeta_file_kvs_.end()) {
      dingodb::pb::meta::IdEpochTypeAndValue internal_id_epoch_type_and_value;
      auto ret = internal_id_epoch_type_and_value.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::meta::IdEpochTypeAndValue failed");
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      id_epoch_type_and_value =
          std::make_shared<dingodb::pb::meta::IdEpochTypeAndValue>(std::move(internal_id_epoch_type_and_value));
    } else {
      std::string s = fmt::format("not found {} in backupmeta file.", dingodb::Constant::kIdEpochTypeAndValueKey);
      DINGO_LOG(WARNING) << s;
    }

    // find TableIncrementKey
    std::shared_ptr<dingodb::pb::meta::TableIncrementGroup> table_increment_group;
    iter = backupmeta_file_kvs_.find(dingodb::Constant::kTableIncrementKey);
    if (iter != backupmeta_file_kvs_.end()) {
      dingodb::pb::meta::TableIncrementGroup internal_table_increment_group;
      auto ret = internal_table_increment_group.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::meta::TableIncrementGroup failed");
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      table_increment_group =
          std::make_shared<dingodb::pb::meta::TableIncrementGroup>(std::move(internal_table_increment_group));
    } else {
      std::string s = fmt::format("not found {} in backupmeta file.", dingodb::Constant::kTableIncrementKey);
      DINGO_LOG(WARNING) << s;
    }

    if (backup_meta_meta || id_epoch_type_and_value || table_increment_group) {
      restore_meta_ = std::make_shared<RestoreMeta>(internal_coordinator_interaction, internal_store_interaction,
                                                    restorets_, restoretso_internal_, storage_, storage_internal_,
                                                    backup_meta_meta, id_epoch_type_and_value, table_increment_group,
                                                    create_region_concurrency_, restore_region_concurrency_,
                                                    create_region_timeout_s_, restore_region_timeout_s_, replica_num_);
    }
  }

  // create restore data
  {
    // find data meta
    std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta_data;
    auto iter = backupmeta_file_kvs_.find(dingodb::Constant::kBackupMetaDataFileName);
    if (iter != backupmeta_file_kvs_.end()) {
      std::shared_ptr<br::ServerInteraction> internal_coordinator_interaction;
      status = ServerInteraction::CreateInteraction(coor_url_, internal_coordinator_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      std::shared_ptr<br::ServerInteraction> internal_store_interaction;
      if (!store_url_.empty()) {
        status = ServerInteraction::CreateInteraction(store_url_, internal_store_interaction);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
          return status;
        }
      }

      std::shared_ptr<br::ServerInteraction> internal_index_interaction;
      if (!index_url_.empty()) {
        status = ServerInteraction::CreateInteraction(index_url_, internal_index_interaction);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
          return status;
        }
      }

      std::shared_ptr<br::ServerInteraction> internal_document_interaction;
      if (!document_url_.empty()) {
        status = ServerInteraction::CreateInteraction(document_url_, internal_document_interaction);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
          return status;
        }
      }

      dingodb::pb::common::BackupMeta internal_backup_meta_data;
      auto ret = internal_backup_meta_data.ParseFromString(iter->second);
      if (!ret) {
        std::string s = fmt::format("parse dingodb::pb::common::BackupMeta failed");
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      backup_meta_data = std::make_shared<dingodb::pb::common::BackupMeta>(std::move(internal_backup_meta_data));

      restore_data_ = std::make_shared<RestoreData>(
          internal_coordinator_interaction, internal_store_interaction, internal_index_interaction,
          internal_document_interaction, restorets_, restoretso_internal_, storage_, storage_internal_,
          backup_meta_data, create_region_concurrency_, restore_region_concurrency_, create_region_timeout_s_,
          restore_region_timeout_s_, replica_num_);
    } else {
      std::string s = fmt::format("not found {} in backupmeta file.", dingodb::Constant::kBackupMetaDataFileName);
      DINGO_LOG(WARNING) << s;
    }  // if (iter != backupmeta_file_kvs_.end())
  }

  // double check
  if (!restore_meta_ && restore_data_) {
    std::string s = fmt::format("restore_meta_ is nullptr, but restore_data_ is not nullptr");
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  if (restore_meta_) {
    status = restore_meta_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  std::cout << "Restore Meta Init ok" << std::endl;
  DINGO_LOG(INFO) << "Restore Meta Init ok";

  if (restore_data_) {
    status = restore_data_->Init();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  std::cout << "Restore Data Init ok" << std::endl;
  DINGO_LOG(INFO) << "Restore Data Init ok";

  if (restore_meta_) {
    status = restore_meta_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (restore_data_) {
    status = restore_data_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (restore_meta_) {
    status = restore_meta_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  if (restore_data_) {
    status = restore_data_->Finish();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }

  // import id epoch type to meta
  status = restore_meta_->ImportIdEpochTypeToMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  // import table increment to meta
  status = restore_meta_->CreateOrUpdateAutoIncrementsToMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  return butil::Status::OK();
}

butil::Status Restore::ParamsCheck() {
  butil::Status status;
  status = ParamsCheckForStorage();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }
  return butil::Status::OK();
}

butil::Status Restore::ParamsCheckForStorage() {
  butil::Status status;
  status = Utils::DirExists(storage_internal_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::string lock_path = storage_internal_ + "/" + kBackupFileLock;
  status = Utils::FileExistsAndRegular(lock_path);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << Utils::FormatStatusError(status);
  }

  std::string backupmeta_encryption_path = storage_internal_ + "/" + dingodb::Constant::kBackupMetaEncryptionName;
  status = Utils::FileExistsAndRegular(backupmeta_encryption_path);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::ifstream reader;
  status = Utils::ReadFile(reader, backupmeta_encryption_path);
  if (!status.ok()) {
    if (reader.is_open()) {
      reader.close();
    }
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::string hash_code;
  reader >> hash_code;

  if (reader.is_open()) {
    reader.close();
  }

  std::string backupmeta_path = storage_internal_ + "/" + dingodb::Constant::kBackupMetaName;
  status = Utils::FileExistsAndRegular(backupmeta_encryption_path);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  std::string calc_hash_code;
  status = dingodb::Helper::CalSha1CodeWithFileEx(backupmeta_path, calc_hash_code);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  // compare hash_code and calc_hash_code
  if (hash_code != calc_hash_code) {
    std::string s = fmt::format("hash_code : {}({}) != calc_hash_code : {}({})", hash_code, backupmeta_encryption_path,
                                calc_hash_code, backupmeta_path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_FILE_CHECKSUM_NOT_MATCH, s);
  }

  SstFileReader sst_file_reader;
  status = sst_file_reader.ReadFile(backupmeta_path, backupmeta_file_kvs_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  return butil::Status::OK();
}

butil::Status Restore::CheckGcSafePoint() {
  dingodb::pb::coordinator::GetGCSafePointRequest request;
  dingodb::pb::coordinator::GetGCSafePointResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_get_all_tenant(true);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get GC safe point, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to get GC safe point, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();
  if (FLAGS_br_log_switch_restore_detail) {
    DINGO_LOG(INFO) << "";
    DINGO_LOG(INFO) << "tenant id : " << dingodb::Constant::kDefaultTenantId
                    << " safe point : " << response.safe_point() << "("
                    << Utils::ConvertTsoToDateTime(response.safe_point()) << ")";

    for (const auto& [id, safe_point] : response.tenant_safe_points()) {
      DINGO_LOG(INFO) << "tenant id : " << id << " safe point : " << safe_point << "("
                      << Utils::ConvertTsoToDateTime(safe_point) << ")";
    }

    DINGO_LOG(INFO) << "";

    DINGO_LOG(INFO) << "tenant id : " << dingodb::Constant::kDefaultTenantId
                    << " resolve lock safe point : " << response.resolve_lock_safe_point() << "("
                    << Utils::ConvertTsoToDateTime(response.resolve_lock_safe_point()) << ")";

    for (const auto& [id, safe_point] : response.tenant_resolve_lock_safe_points()) {
      DINGO_LOG(INFO) << "tenant id : " << id << " resolve lock safe point : " << safe_point << "("
                      << Utils::ConvertTsoToDateTime(safe_point) << ")";
    }
    DINGO_LOG(INFO) << "";
  }

  if (response.safe_point() > 0) {
    std::string s = fmt::format("GC safe point is not 0, safe point : {}", response.safe_point());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_DINGO_STORE_NOT_EMPTY, s);
  }

  if (response.resolve_lock_safe_point() > 0) {
    std::string s =
        fmt::format("GC resolve lock safe point is not 0, safe point : {}", response.resolve_lock_safe_point());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_DINGO_STORE_NOT_EMPTY, s);
  }

  if (response.tenant_safe_points().size() != 0) {
    std::string s = fmt::format("GC tenant safe points is not empty, size : {}", response.tenant_safe_points().size());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_DINGO_STORE_NOT_EMPTY, s);
  }

  if (response.tenant_resolve_lock_safe_points().size() != 0) {
    std::string s = fmt::format("GC tenant resolve lock safe points is not empty, size : {}",
                                response.tenant_resolve_lock_safe_points().size());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_DINGO_STORE_NOT_EMPTY, s);
  }

  if (response.gc_stop()) {
    is_gc_stop_ = true;
    is_gc_enable_after_finish_ = false;
    DINGO_LOG(INFO) << "GC is already stopped. Restore will not enable  if restore is finished.";
  }

  return butil::Status::OK();
}

butil::Status Restore::SetGcStop() {
  if (is_gc_stop_) {
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << "Set GC stop ...";

  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_STOP);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  is_gc_stop_ = true;
  is_gc_enable_after_finish_ = true;

  DINGO_LOG(INFO) << "GC is stopped. Restore will enable GC.  if restore is finished.";

  return butil::Status::OK();
}

butil::Status Restore::SetGcStart() {
  if (!is_gc_enable_after_finish_) {
    return butil::Status::OK();
  }
  DINGO_LOG(INFO) << "Set GC start ...";

  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_START);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  is_gc_stop_ = false;
  is_gc_enable_after_finish_ = false;

  DINGO_LOG(INFO) << "Set GC start success.";

  return butil::Status::OK();
}

butil::Status Restore::DisableBalanceToCoordinator(ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::ControlConfigRequest request;
  dingodb::pb::coordinator::ControlConfigResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  dingodb::pb::common::ControlConfigVariable config_balance_leader;
  config_balance_leader.set_name("FLAGS_enable_balance_leader");
  config_balance_leader.set_value("false");
  request.mutable_control_config_variable()->Add(std::move(config_balance_leader));

  dingodb::pb::common::ControlConfigVariable config_balance_region;
  config_balance_region.set_name("FLAGS_enable_balance_region");
  config_balance_region.set_value("false");
  request.mutable_control_config_variable()->Add(std::move(config_balance_region));

  std::vector<std::string> addrs = coordinator_interaction->GetAddrs();

  for (const auto& addr : addrs) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();
    response.Clear();

    std::shared_ptr<ServerInteraction> interaction;
    butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = interaction->SendRequest("CoordinatorService", "ControlConfig", request, response);
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
      return butil::Status(response.error().errcode(), response.error().errmsg());
    }

    for (const auto& config : response.control_config_variable()) {
      if (config.is_error_occurred()) {
        DINGO_LOG(ERROR) << "ControlConfig not support variable: " << config.name() << " skip.";
        return butil::Status(dingodb::pb::error::EINTERNAL, "ControlConfig not support variable: %s skip.",
                             config.name().c_str());
      }

      if (!config.is_already_set() && config.name() == "FLAGS_enable_balance_leader") {
        balance_leader_enable_after_finish_ = true;
      }

      if (!config.is_already_set() && config.name() == "FLAGS_enable_balance_region") {
        balance_region_enable_after_finish_ = true;
      }
    }
  }

  return butil::Status::OK();
}

butil::Status Restore::EnableBalanceToCoordinator(ServerInteractionPtr coordinator_interaction) const {
  dingodb::pb::coordinator::ControlConfigRequest request;
  dingodb::pb::coordinator::ControlConfigResponse response;

  if (balance_leader_enable_after_finish_) {
    dingodb::pb::common::ControlConfigVariable config_balance_leader;
    config_balance_leader.set_name("FLAGS_enable_balance_leader");
    config_balance_leader.set_value("true");
    request.mutable_control_config_variable()->Add(std::move(config_balance_leader));
  }

  if (balance_region_enable_after_finish_) {
    dingodb::pb::common::ControlConfigVariable config_balance_region;
    config_balance_region.set_name("FLAGS_enable_balance_region");
    config_balance_region.set_value("true");
    request.mutable_control_config_variable()->Add(std::move(config_balance_region));
  }

  if (!request.control_config_variable().empty()) {
    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

    std::vector<std::string> addrs = coordinator_interaction->GetAddrs();

    for (const auto& addr : addrs) {
      DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();
      response.Clear();

      std::shared_ptr<ServerInteraction> interaction;
      butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      status = interaction->SendRequest("CoordinatorService", "ControlConfig", request, response);
      DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      if (response.error().errcode() != dingodb::pb::error::OK) {
        DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
        return butil::Status(response.error().errcode(), response.error().errmsg());
      }
    }
  }

  return butil::Status::OK();
}

butil::Status Restore::DisableSplitAndMergeToStoreAndIndex(ServerInteractionPtr store_interaction,
                                                           ServerInteractionPtr index_interaction) {
  bool is_exist_store = (store_interaction != nullptr ? !store_interaction->IsEmpty() : false);
  bool is_exist_index = (index_interaction != nullptr ? !index_interaction->IsEmpty() : false);

  if (!is_exist_store && !is_exist_index) {
    DINGO_LOG(INFO) << "Store and Index not exist, skip DisableSplitAndMergeToStoreAndIndex";
    return butil::Status::OK();
  }

  dingodb::pb::store::ControlConfigRequest request;
  dingodb::pb::store::ControlConfigResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  dingodb::pb::common::ControlConfigVariable config_auto_split;
  config_auto_split.set_name("FLAGS_region_enable_auto_split");
  config_auto_split.set_value("false");
  request.mutable_control_config_variable()->Add(std::move(config_auto_split));

  dingodb::pb::common::ControlConfigVariable config_auto_merge;
  config_auto_merge.set_name("FLAGS_region_enable_auto_merge");
  config_auto_merge.set_value("false");
  request.mutable_control_config_variable()->Add(std::move(config_auto_merge));

  // store exist
  if (is_exist_store) {
    std::vector<std::string> addrs = store_interaction->GetAddrs();
    for (const auto& addr : addrs) {
      DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();
      response.Clear();

      std::shared_ptr<ServerInteraction> interaction;
      butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      status = interaction->SendRequest("StoreService", "ControlConfig", request, response);
      DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      if (response.error().errcode() != dingodb::pb::error::OK) {
        DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
        return butil::Status(response.error().errcode(), response.error().errmsg());
      }

      for (const auto& config : response.control_config_variable()) {
        if (config.is_error_occurred()) {
          DINGO_LOG(ERROR) << "ControlConfig not support variable: " << config.name() << " skip.";
          return butil::Status(dingodb::pb::error::EINTERNAL, "ControlConfig not support variable: %s skip.",
                               config.name().c_str());
        }

        if (!config.is_already_set() && config.name() == "FLAGS_region_enable_auto_split") {
          region_auto_split_enable_after_finish_ = true;
        }

        if (!config.is_already_set() && config.name() == "FLAGS_region_enable_auto_merge") {
          region_auto_merge_enable_after_finish_ = true;
        }
      }
    }
  }  //   if (is_exist_store) {

  // index exist
  if (is_exist_index) {
    std::vector<std::string> addrs = index_interaction->GetAddrs();
    for (const auto& addr : addrs) {
      DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();
      response.Clear();
      std::shared_ptr<ServerInteraction> interaction;

      butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      status = interaction->SendRequest("IndexService", "ControlConfig", request, response);
      DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
        return status;
      }

      if (response.error().errcode() != dingodb::pb::error::OK) {
        DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
        return butil::Status(response.error().errcode(), response.error().errmsg());
      }

      for (const auto& config : response.control_config_variable()) {
        if (config.is_error_occurred()) {
          DINGO_LOG(ERROR) << "ControlConfig not support variable: " << config.name() << " skip.";
          return butil::Status(dingodb::pb::error::EINTERNAL, "ControlConfig not support variable: %s skip.",
                               config.name().c_str());
        }

        if (!config.is_already_set() && config.name() == "FLAGS_region_enable_auto_split") {
          region_auto_split_enable_after_finish_ = true;
        }

        if (!config.is_already_set() && config.name() == "FLAGS_region_enable_auto_merge") {
          region_auto_merge_enable_after_finish_ = true;
        }
      }
    }

  }  //   if (is_exist_index) {

  return butil::Status::OK();
}

butil::Status Restore::EnableSplitAndMergeToStoreAndIndex(ServerInteractionPtr store_interaction,
                                                          ServerInteractionPtr index_interaction) const {
  bool is_exist_store = (store_interaction != nullptr ? !store_interaction->IsEmpty() : false);
  bool is_exist_index = (index_interaction != nullptr ? !index_interaction->IsEmpty() : false);

  if (!is_exist_store && !is_exist_index) {
    DINGO_LOG(INFO) << "Store and Index not exist, skip EnableSplitAndMergeToStoreAndIndex";
    return butil::Status::OK();
  }

  dingodb::pb::store::ControlConfigRequest request;
  dingodb::pb::store::ControlConfigResponse response;

  if (region_auto_split_enable_after_finish_) {
    dingodb::pb::common::ControlConfigVariable config_auto_split;
    config_auto_split.set_name("FLAGS_region_enable_auto_split");
    config_auto_split.set_value("true");
    request.mutable_control_config_variable()->Add(std::move(config_auto_split));
  }

  if (region_auto_merge_enable_after_finish_) {
    dingodb::pb::common::ControlConfigVariable config_auto_merge;
    config_auto_merge.set_name("FLAGS_region_enable_auto_merge");
    config_auto_merge.set_value("true");
    request.mutable_control_config_variable()->Add(std::move(config_auto_merge));
  }

  if (!request.control_config_variable().empty()) {
    request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

    if (is_exist_store) {
      std::vector<std::string> addrs = store_interaction->GetAddrs();
      for (const auto& addr : addrs) {
        DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();
        response.Clear();

        std::shared_ptr<ServerInteraction> interaction;
        butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
          return status;
        }

        status = interaction->SendRequest("StoreService", "ControlConfig", request, response);
        DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();
        if (!status.ok()) {
          DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
          return status;
        }

        if (response.error().errcode() != dingodb::pb::error::OK) {
          DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
          return butil::Status(response.error().errcode(), response.error().errmsg());
        }
      }
    }  // if (is_exist_store) {

    if (is_exist_index) {
      std::vector<std::string> addrs = index_interaction->GetAddrs();
      for (const auto& addr : addrs) {
        DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();
        response.Clear();
        std::shared_ptr<ServerInteraction> interaction;

        butil::Status status = ServerInteraction::CreateInteraction({addr}, interaction);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
          return status;
        }

        status = interaction->SendRequest("IndexService", "ControlConfig", request, response);
        DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();
        if (!status.ok()) {
          DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
          return status;
        }
        if (response.error().errcode() != dingodb::pb::error::OK) {
          DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
          return butil::Status(response.error().errcode(), response.error().errmsg());
        }
      }
    }
  }
  return butil::Status::OK();
}

butil::Status Restore::RegisterBackupStatusToCoordinator(ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::RegisterBackupStatusRequest request;
  dingodb::pb::coordinator::RegisterBackupStatusResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "RegisterBackupStatus", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set RegisterBackupStatus, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set RegisterBackupStatus, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  if (response.is_backing_up()) {
    std::string s = fmt::format("dingo store is backing up");
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_DINGO_STORE_IS_BACKING_UP, s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

butil::Status Restore::RegisterRestoreToCoordinator(bool is_first, ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::RegisterRestoreRequest request;
  dingodb::pb::coordinator::RegisterRestoreResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_restore_name(restore_task_id_);
  request.set_restore_path(storage_internal_);
  int64_t current_now_s = dingodb::Helper::Timestamp();
  if (is_first) {
    request.set_restore_start_timestamp(current_now_s);
  }
  request.set_restore_current_timestamp(current_now_s);
  request.set_restore_timeout_s(FLAGS_restore_task_timeout_s);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "RegisterRestore", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set RegisterRestore, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set RegisterRestore, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status Restore::UnregisterRestoreToCoordinator(ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::UnRegisterRestoreRequest request;
  dingodb::pb::coordinator::UnRegisterRestoreResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_restore_name(restore_task_id_);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "UnRegisterRestore", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set UnRegisterRestore, status={}", Utils::FormatStatusError(status));
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set UnRegisterRestore, error={}", Utils::FormatResponseError(response));
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

butil::Status Restore::DoAsyncRegisterRestoreToCoordinator(ServerInteractionPtr coordinator_interaction) {
  std::shared_ptr<Restore> self = GetSelf();
  auto lambda_call = [self, coordinator_interaction]() {
    self->DoRegisterRestoreToCoordinatorInternal(coordinator_interaction);
  };

#if defined(ENABLE_RESTORE_PTHREAD)
  std::thread th(lambda_call);
  th.detach();
#else

  std::function<void()>* call = new std::function<void()>;
  *call = lambda_call;
  bthread_t th;

  int ret = bthread_start_background(
      &th, nullptr,
      [](void* arg) -> void* {
        auto* call = static_cast<std::function<void()>*>(arg);
        (*call)();
        delete call;
        return nullptr;
      },
      call);
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("bthread_start_background fail");
    return butil::Status(dingodb::pb::error::EINTERNAL, "bthread_start_background fail");
  }
#endif  // #if defined(ENABLE_BACKUP_PTHREAD)

  return butil::Status::OK();
}

butil::Status Restore::DoRegisterRestoreToCoordinatorInternal(ServerInteractionPtr coordinator_interaction) {
  butil::Status status;
  bool is_first = false;
  is_exit_register_restore_to_coordinator_thread_ = false;
  while (!is_need_exit_) {
    bool is_error_occur = true;
    uint32_t retry_times = FLAGS_restore_task_max_retry;
    do {
      status = RegisterRestoreToCoordinator(is_first, coordinator_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      } else {  // success
        is_error_occur = false;
        break;
      }
      sleep(FLAGS_restore_watch_interval_s);
    } while (!is_need_exit_ && retry_times-- > 0);

    if (is_error_occur) {
      if (!is_need_exit_) {
        is_need_exit_ = true;
      }
      {
        BAIDU_SCOPED_LOCK(mutex_);
        last_error_ = status;
      }
      break;
    }

    sleep(FLAGS_restore_watch_interval_s);
  }

  is_exit_register_restore_to_coordinator_thread_ = true;
  DINGO_LOG(INFO) << "exit register restore  thread.";

  return butil::Status::OK();
}

butil::Status Restore::GetAllRegionMapFromCoordinator(ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_tenant_id(-1);  // get all tenants region map

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  auto status = coordinator_interaction->SendRequest("CoordinatorService", "GetRegionMap", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  if (!response.regionmap().regions().empty()) {
    std::string s = fmt::format("region map is not empty, region size : {}", response.regionmap().regions_size());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_DINGO_STORE_NOT_EMPTY, s);
  }

  return butil::Status::OK();
}

butil::Status Restore::GetVersionFromCoordinator(ServerInteractionPtr coordinator_interaction,
                                                 dingodb::pb::common::VersionInfo& version_info) {
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_is_just_version_info(true);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  butil::Status status = coordinator_interaction->SendRequest("CoordinatorService", "Hello", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  version_info = response.version_info();

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

butil::Status Restore::CompareVersion(const dingodb::pb::common::VersionInfo& version_info_local,
                                      const dingodb::pb::common::VersionInfo& version_info_remote,
                                      const dingodb::pb::common::VersionInfo& version_info_in_backup) {
  DINGO_LOG(INFO) << "local version info : " << version_info_local.DebugString() << std::endl;
  DINGO_LOG(INFO) << "remote version info : " << version_info_remote.DebugString() << std::endl;
  DINGO_LOG(INFO) << "backup version info : " << version_info_in_backup.DebugString() << std::endl;

  if (version_info_local.git_commit_hash() != version_info_in_backup.git_commit_hash()) {
    std::string s = fmt::format("git_commit_hash is different. local : {} backup : {}",
                                version_info_local.git_commit_hash(), version_info_in_backup.git_commit_hash());
    if (FLAGS_restore_strict_version_comparison) {
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::EBACKUP_VERSION_NOT_MATCH, s);
    } else {
      DINGO_LOG(WARNING) << s;
      return butil::Status(dingodb::pb::error::EBACKUP_VERSION_NOT_MATCH, s);
    }
  }

  if (version_info_local.git_commit_hash() != version_info_remote.git_commit_hash()) {
    std::string s = fmt::format("git_commit_hash is different. local : {} remote : {}",
                                version_info_local.git_commit_hash(), version_info_remote.git_commit_hash());
    if (FLAGS_restore_strict_version_comparison) {
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::EBACKUP_VERSION_NOT_MATCH, s);
    } else {
      DINGO_LOG(WARNING) << s;
      return butil::Status(dingodb::pb::error::EBACKUP_VERSION_NOT_MATCH, s);
    }
  }

  return butil::Status::OK();
}

butil::Status Restore::DoFinish() {
  if (is_already_register_restore_to_coordinator_) {
    if (!is_need_exit_) {
      is_need_exit_ = true;
    }
    std::cerr << "Waiting for register restore thread exit. (Do not use kill -9 or Ctrl-C to exit.) <";
    DINGO_LOG(INFO) << "Waiting for register restore thread exit. (Do not use kill -9 or Ctrl-C to exit.) <";
    std::string s;
    while (true) {
      if (is_exit_register_restore_to_coordinator_thread_) {
        break;
      }
      sleep(1);
      std::cerr << "-";
      s += "-";
    }
    std::cout << ">" << std::endl;
    DINGO_LOG(INFO) << s << ">";
    UnregisterRestoreToCoordinator(br::InteractionManager::GetInstance().GetCoordinatorInteraction());
  }

  // gc start
  if (is_gc_enable_after_finish_) {
    SetGcStart();
    std::cout << "gc start ok" << std::endl;
    DINGO_LOG(INFO) << "gc start ok";
  }

  butil::Status status = EnableBalanceToCoordinator(br::InteractionManager::GetInstance().GetCoordinatorInteraction());
  if (status.ok()) {
    if (balance_leader_enable_after_finish_) {
      std::cout << "balance leader set start ok" << std::endl;
      DINGO_LOG(INFO) << "balance leader set start ok";
    }

    if (balance_region_enable_after_finish_) {
      std::cout << "balance region set start ok" << std::endl;
      DINGO_LOG(INFO) << "balance region set start ok";
    }
  }

  status = EnableSplitAndMergeToStoreAndIndex(br::InteractionManager::GetInstance().GetStoreInteraction(),
                                              br::InteractionManager::GetInstance().GetIndexInteraction());
  if (status.ok()) {
    if (region_auto_split_enable_after_finish_) {
      std::cout << "region auto split set start ok" << std::endl;
      DINGO_LOG(INFO) << "region auto split set start ok";
    }

    if (region_auto_merge_enable_after_finish_) {
      std::cout << "region auto merge set start ok" << std::endl;
      DINGO_LOG(INFO) << "region auto merge set start ok";
    }
  }

  return butil::Status::OK();
}

std::pair<int64_t, int64_t> Restore::GetRegions() {
  std::pair<int64_t, int64_t> region_metas = std::pair<int64_t, int64_t>(0, 0);
  std::pair<int64_t, int64_t> region_datas = std::pair<int64_t, int64_t>(0, 0);

  if (restore_meta_) {
    region_metas = restore_meta_->GetRegions();
  }

  if (restore_data_) {
    region_datas = restore_data_->GetRegions();
  }

  return std::pair<int64_t, int64_t>(region_metas.first + region_datas.first,
                                     region_metas.second + region_datas.second);
}

}  // namespace br
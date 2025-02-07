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

#include "br/backup.h"

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "br/helper.h"
#include "br/interaction_manager.h"
#include "br/sst_file_writer.h"
#include "br/utils.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "common/uuid.h"
#include "common/version.h"
#include "fmt/core.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

namespace br {

#ifndef ENABLE_BACKUP_PTHREAD
#define ENABLE_BACKUP_PTHREAD
#endif

// #undef ENABLE_BACKUP_PTHREAD

Backup::Backup(const BackupParams& params)
    : is_gc_stop_(false),
      is_gc_enable_after_finish_(false),
      is_need_exit_(false),
      is_already_register_backup_to_coordinator_(false),
      is_exit_register_backup_to_coordinator_thread_(true),
      region_auto_split_enable_after_finish_(false),
      region_auto_merge_enable_after_finish_(false),
      balance_leader_enable_after_finish_(false),
      balance_region_enable_after_finish_(false),
      start_time_ms_(dingodb::Helper::TimestampMs()),
      end_time_ms_(0) {
  coor_url_ = params.coor_url;
  br_type_ = params.br_type;
  br_backup_type_ = params.br_backup_type;
  backupts_ = params.backupts;
  backuptso_internal_ = params.backuptso_internal;
  storage_ = params.storage;
  storage_internal_ = params.storage_internal;

  bthread_mutex_init(&mutex_, nullptr);
}

Backup::~Backup() { bthread_mutex_destroy(&mutex_); };

std::shared_ptr<Backup> Backup::GetSelf() { return shared_from_this(); }

butil::Status Backup::Init() {
  butil::Status status;
  status = ParamsCheck();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // version match check
  dingodb::pb::common::VersionInfo version_info_remote;
  status =
      GetVersionFromCoordinator(br::InteractionManager::GetInstance().GetCoordinatorInteraction(), version_info_remote);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  dingodb::pb::common::VersionInfo version_info_local = dingodb::GetVersionInfo();
  status = CompareVersion(version_info_local, version_info_remote);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  std::cout << "version compare ok" << std::endl;
  DINGO_LOG(INFO) << "version compare ok";

  status = GetGcSafePoint();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  std::cout << "safe point ts check ok" << std::endl;
  DINGO_LOG(INFO) << "safe point ts check ok";

  backup_task_id_ = dingodb::UUIDGenerator::GenerateUUID();

  auto lambda_exit_function = [this, &status]() {
    if (!status.ok()) {
      DoFinish();
      last_error_ = status;
    };
  };

  std::cout << "backup_task_id : " << backup_task_id_ << std::endl;
  DINGO_LOG(INFO) << "backup_task_id : " << backup_task_id_ << std::endl;

  dingodb::ON_SCOPE_EXIT(lambda_exit_function);

  // try to register backup task
  bool is_first = true;
  status = RegisterBackupToCoordinator(is_first, br::InteractionManager::GetInstance().GetCoordinatorInteraction());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  is_already_register_backup_to_coordinator_ = true;

  std::cout << "register backup To coordinator ok" << std::endl;
  DINGO_LOG(INFO) << "register backup To coordinator ok";

  // set gc stop
  if (!is_gc_stop_) {
    status = SetGcStop();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
    std::cout << "gc set stopped ok" << std::endl;
    DINGO_LOG(INFO) << "gc set stopped ok";
  } else {
    std::cout << "gc already stopped. ignore" << std::endl;
    DINGO_LOG(INFO) << "gc already stopped. ignore";
  }

  status = DisableBalanceToCoordinator(br::InteractionManager::GetInstance().GetCoordinatorInteraction());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
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

  status = DisableSplitAndMergeToStoreAndIndex(br::InteractionManager::GetInstance().GetStoreInteraction(),
                                               br::InteractionManager::GetInstance().GetIndexInteraction());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
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

  status = Utils::DirExists(storage_internal_);
  if (status.ok()) {
    // clean backup dir
    status = Utils::ClearDir(storage_internal_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
    DINGO_LOG(INFO) << "backup dir : " << storage_internal_ << " already exist. clear dir.";
  } else if (dingodb::pb::error::EFILE_NOT_EXIST == status.error_code()) {
    // create backup dir
    status = Utils::CreateDirRecursion(storage_internal_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
    DINGO_LOG(INFO) << "backup dir : " << storage_internal_ << " not exist. create recursion dir.";
  } else {  // error
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // create backup lock file
  std::ofstream writer;
  std::string lock_path = storage_internal_ + "/" + kBackupFileLock;
  status = Utils::CreateFile(writer, lock_path);
  if (!status.ok()) {
    if (writer.is_open()) {
      writer.close();
    }
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  writer << "DO NOT DELETE" << std::endl;
  writer << "This file exists to remind other backup jobs won't use this path" << std::endl;

  if (writer.is_open()) {
    writer.close();
  }

  DINGO_LOG(INFO) << "write backup lock file : " << lock_path << " success";

  DINGO_LOG(INFO) << "Backup::Init " << " success";

  return butil::Status::OK();
}

butil::Status Backup::Run() {
  butil::Status status;

  auto lambda_exit_function = [this, &status]() {
    if (!status.ok()) {
      DoFinish();
      last_error_ = status;
    };
  };

  dingodb::ON_SCOPE_EXIT(lambda_exit_function);

  std::vector<std::string> coordinator_addrs =
      br::InteractionManager::GetInstance().GetCoordinatorInteraction()->GetAddrs();

  // create register backup task to coordinator
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = ServerInteraction::CreateInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    // register backup task to coordinator
    status = DoAsyncRegisterBackupToCoordinator(coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  std::cout << "Backup task " << backup_task_id_ << " is registered to coordinator Periodicity." << std::endl;
  DINGO_LOG(INFO) << "Backup task " << backup_task_id_ << " is registered to coordinator Periodicity.";

  status = DoRun();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  return butil::Status::OK();
}

butil::Status Backup::DoRun() {
  butil::Status status;
  std::vector<std::string> coordinator_addrs =
      br::InteractionManager::GetInstance().GetCoordinatorInteraction()->GetAddrs();

  std::vector<std::string> store_addrs = br::InteractionManager::GetInstance().GetStoreInteraction()->GetAddrs();

  std::vector<std::string> index_addrs = br::InteractionManager::GetInstance().GetIndexInteraction()->GetAddrs();

  std::vector<std::string> document_addrs = br::InteractionManager::GetInstance().GetDocumentInteraction()->GetAddrs();

  // create backup meta
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = ServerInteraction::CreateInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> store_interaction;
    status = ServerInteraction::CreateInteraction(store_addrs, store_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> index_interaction;
    status = ServerInteraction::CreateInteraction(index_addrs, index_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> document_interaction;
    status = ServerInteraction::CreateInteraction(document_addrs, document_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    backup_meta_ =
        std::make_shared<BackupMeta>(coordinator_interaction, store_interaction, index_interaction,
                                     document_interaction, backupts_, backuptso_internal_, storage_, storage_internal_);
  }

  status = backup_meta_->Init();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  std::cout << "Back Meta Init ok" << std::endl;
  DINGO_LOG(INFO) << "Back Meta Init ok";

  // create backup data
  {
    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = ServerInteraction::CreateInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> store_interaction;
    status = ServerInteraction::CreateInteraction(store_addrs, store_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> index_interaction;
    status = ServerInteraction::CreateInteraction(index_addrs, index_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::shared_ptr<br::ServerInteraction> document_interaction;
    status = ServerInteraction::CreateInteraction(document_addrs, document_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    backup_data_ =
        std::make_shared<BackupData>(coordinator_interaction, store_interaction, index_interaction,
                                     document_interaction, backupts_, backuptso_internal_, storage_, storage_internal_);
  }

  std::vector<int64_t> meta_region_list = backup_meta_->GetSqlMetaRegionList();
  if (FLAGS_br_log_switch_backup_detail) {
    DINGO_LOG(INFO) << "sql meta region list size : " << meta_region_list.size();
    for (const auto& region_id : meta_region_list) {
      DINGO_LOG(INFO) << "sql meta region id : " << region_id;
    }
  }
  status = backup_data_->Init(meta_region_list);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  std::cout << "Back Data Init ok" << std::endl;
  DINGO_LOG(INFO) << "Back Data Init ok";

  status = backup_data_->Run();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = backup_data_->Finish();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = backup_meta_->Run(backup_data_->GetRegionMap());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = backup_meta_->Finish();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  //
  std::shared_ptr<dingodb::pb::common::BackupMeta> meta_meta = backup_meta_->GetBackupMeta();
  std::shared_ptr<dingodb::pb::common::BackupMeta> meta_data = backup_data_->GetBackupMeta();

  const auto& [status2, id_epoch_type_and_value] = backup_meta_->GetIdEpochTypeAndValue();
  if (!status2.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  const auto& [status3, table_increment_group] = backup_meta_->GetAllTableIncrement();
  if (!status3.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // write backup meta
  {
    std::string file_name = dingodb::Constant::kBackupMetaName;
    std::string file_path = storage_internal_ + "/" + file_name;

    std::map<std::string, std::string> kvs;

    if (meta_meta) {
      kvs.emplace(meta_meta->file_name(), meta_meta->SerializeAsString());
    }

    if (meta_data) {
      kvs.emplace(meta_data->file_name(), meta_data->SerializeAsString());
    }

    if (id_epoch_type_and_value) {
      kvs.emplace(dingodb::Constant::kIdEpochTypeAndValueKey, id_epoch_type_and_value->SerializeAsString());
    }

    if (table_increment_group) {
      kvs.emplace(dingodb::Constant::kTableIncrementKey, table_increment_group->SerializeAsString());
    }

    dingodb::pb::common::VersionInfo version_info = dingodb::GetVersionInfo();

    { kvs.emplace(dingodb::Constant::kBackupVersionKey, version_info.SerializeAsString()); }

    dingodb::pb::common::BackupParam backup_param;
    backup_param.set_coor_addr(coor_url_);
    backup_param.set_store_addr(dingodb::Helper::VectorToString(store_addrs));
    backup_param.set_store_addr(dingodb::Helper::VectorToString(index_addrs));
    backup_param.set_store_addr(dingodb::Helper::VectorToString(document_addrs));
    backup_param.set_br_type(br_type_);
    backup_param.set_br_backup_type(br_backup_type_);
    backup_param.set_backupts(backupts_);
    backup_param.set_backuptso_internal(backuptso_internal_);
    backup_param.set_storage(storage_);
    backup_param.set_storage_internal(storage_internal_);
    { kvs.emplace(dingodb::Constant::kBackupBackupParamKey, backup_param.SerializeAsString()); }

    rocksdb::Options options;
    std::shared_ptr<SstFileWriter> sst = std::make_shared<SstFileWriter>(options);

    status = sst->SaveFile(kvs, file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    std::string hash_code;
    status = dingodb::Helper::CalSha1CodeWithFileEx(file_path, hash_code);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    // create backupmeta encryption file
    std::ofstream writer;
    std::string backupmeta_encryption_path = storage_internal_ + "/" + dingodb::Constant::kBackupMetaEncryptionName;
    status = Utils::CreateFile(writer, backupmeta_encryption_path);
    if (!status.ok()) {
      if (writer.is_open()) {
        writer.close();
      }
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    writer << hash_code;

    if (writer.is_open()) {
      writer.close();
    }

    if (FLAGS_br_log_switch_backup_detail) {
      DINGO_LOG(INFO) << dingodb::Constant::kBackupMetaName
                      << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<";
      DINGO_LOG(INFO) << dingodb::Constant::kBackupVersionKey << " :";
      DINGO_LOG(INFO) << version_info.DebugString() << std::endl;

      DINGO_LOG(INFO) << dingodb::Constant::kBackupBackupParamKey << " :";
      DINGO_LOG(INFO) << backup_param.DebugString() << std::endl;

      if (id_epoch_type_and_value) {
        DINGO_LOG(INFO) << dingodb::Constant::kIdEpochTypeAndValueKey << " :";
        DINGO_LOG(INFO) << id_epoch_type_and_value->DebugString() << std::endl;
      }

      if (table_increment_group) {
        DINGO_LOG(INFO) << dingodb::Constant::kTableIncrementKey << " :";
        DINGO_LOG(INFO) << table_increment_group->DebugString() << std::endl;
      }

      if (meta_meta) {
        DINGO_LOG(INFO) << meta_meta->file_name() << " :";
        DINGO_LOG(INFO) << meta_meta->DebugString() << std::endl;
      }

      if (meta_data) {
        DINGO_LOG(INFO) << meta_data->file_name() << " :";
        DINGO_LOG(INFO) << meta_data->DebugString() << std::endl;
      }
      DINGO_LOG(INFO) << dingodb::Constant::kBackupMetaName
                      << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<";
    }

    // create backupmeta debug file
    std::string backupmeta_debug_path = storage_internal_ + "/" + dingodb::Constant::kBackupMetaDebugName;
    status = Utils::CreateFile(writer, backupmeta_debug_path);
    if (!status.ok()) {
      if (writer.is_open()) {
        writer.close();
      }
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    writer << "This is Debug File" << std::endl;

    writer << dingodb::Constant::kBackupVersionKey << " : " << std::endl;
    writer << version_info.DebugString() << std::endl;

    writer << dingodb::Constant::kBackupBackupParamKey << " : " << std::endl;
    writer << backup_param.DebugString() << std::endl;

    if (id_epoch_type_and_value) {
      writer << dingodb::Constant::kIdEpochTypeAndValueKey << " : " << std::endl;
      writer << id_epoch_type_and_value->DebugString() << std::endl;
    }

    if (table_increment_group) {
      writer << dingodb::Constant::kTableIncrementKey << " : " << std::endl;
      writer << table_increment_group->DebugString() << std::endl;
    }

    if (meta_meta) {
      writer << meta_meta->file_name() << " : " << std::endl;
      writer << meta_meta->DebugString() << std::endl;
    }

    if (meta_data) {
      writer << meta_data->file_name() << " : " << std::endl;
      writer << meta_data->DebugString() << std::endl;
    }

    if (writer.is_open()) {
      writer.close();
    }
  }

  return butil::Status::OK();
}

butil::Status Backup::Finish() {
  butil::Status status;
  status = DoFinish();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  end_time_ms_ = dingodb::Helper::TimestampMs();

  std::string s =
      fmt::format("[Full Backup success summary][backup-total-ranges={}] [backup-sql-meta-ranges={}][total-take={}s]",
                  backup_data_->GetRegionMap()->regions_size(), backup_meta_->GetSqlMetaRegionList().size(),
                  (end_time_ms_ - start_time_ms_) / 1000.0);
  std::cout << s << std::endl;
  DINGO_LOG(INFO) << s;
  return butil::Status::OK();
}

butil::Status Backup::ParamsCheck() {
  butil::Status status;
  status = ParamsCheckForStorage();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  return butil::Status::OK();
}

butil::Status Backup::ParamsCheckForStorage() {
  butil::Status status;
  status = Utils::DirExists(storage_internal_);
  if (status.ok()) {
    std::string lock_path = storage_internal_ + "/" + kBackupFileLock;
    status = Utils::FileExistsAndRegular(lock_path);
    if (status.ok()) {
      std::string s = fmt::format("Backup may be running, please wait or delete lock file : {}", lock_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::EFILE_EXIST, s);
    } else if (status.error_code() != dingodb::pb::error::EFILE_NOT_EXIST) {
      std::string s = fmt::format("Check lock file : {} failed: {}", lock_path, status.error_cstr());
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::EINTERNAL, s);
    }

  } else if (status.error_code() != dingodb::pb::error::EFILE_NOT_EXIST) {
    std::string s = fmt::format("Check storage : {} storage_internal_ : {} failed: {}", storage_, storage_internal_,
                                status.error_cstr());
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }
  return butil::Status::OK();
}

butil::Status Backup::GetGcSafePoint() {
  dingodb::pb::coordinator::GetGCSafePointRequest request;
  dingodb::pb::coordinator::GetGCSafePointResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_get_all_tenant(true);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "GetGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to get GC safe point, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to get GC safe point, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();
  if (FLAGS_br_log_switch_backup_detail) {
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

  int64_t max_tenant_safe_points;
  int64_t min_tenant_resolve_lock_safe_points;

  max_tenant_safe_points = response.safe_point();
  min_tenant_resolve_lock_safe_points = response.resolve_lock_safe_point();

  for (const auto& [id, safe_point] : response.tenant_safe_points()) {
    if (safe_point > max_tenant_safe_points) {
      max_tenant_safe_points = safe_point;
    }
  }

  for (const auto& [id, safe_point] : response.tenant_resolve_lock_safe_points()) {
    if (safe_point < min_tenant_resolve_lock_safe_points) {
      min_tenant_resolve_lock_safe_points = safe_point;
    }
  }

  // compare safe points
  if (backuptso_internal_ > max_tenant_safe_points && backuptso_internal_ <= min_tenant_resolve_lock_safe_points) {
    DINGO_LOG(INFO) << "Backup safe point is " << backuptso_internal_ << " "
                    << Utils::ConvertTsoToDateTime(backuptso_internal_);
  } else {
    std::string s = fmt::format(
        "Backup safe point is {}({}), but max tenant safe point is {}({}), min tenant resolve lock safe point is "
        "{}({})",
        backuptso_internal_, Utils::ConvertTsoToDateTime(backuptso_internal_), max_tenant_safe_points,
        Utils::ConvertTsoToDateTime(max_tenant_safe_points), min_tenant_resolve_lock_safe_points,
        Utils::ConvertTsoToDateTime(min_tenant_resolve_lock_safe_points));
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EILLEGAL_PARAMTETERS, s);
  }

  std::string s = fmt::format(
      "# max tenant safe points : {}({}) min tenant resolve lock safe points : {}({}) backuptso(internal) : {}({})",
      max_tenant_safe_points, Utils::ConvertTsoToDateTime(max_tenant_safe_points), min_tenant_resolve_lock_safe_points,
      Utils::ConvertTsoToDateTime(min_tenant_resolve_lock_safe_points), backuptso_internal_,
      Utils::ConvertTsoToDateTime(backuptso_internal_));
  std::cout << s << std::endl;
  DINGO_LOG(INFO) << s;

  if (response.gc_stop()) {
    is_gc_stop_ = true;
    is_gc_enable_after_finish_ = false;
    DINGO_LOG(INFO) << "GC is already stopped. Backup will not enable  if backup is finished.";
  }

  return butil::Status::OK();
}

butil::Status Backup::SetGcStop() {
  if (is_gc_stop_) {
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << "Set GC stop ...";

  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_STOP);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

  is_gc_stop_ = true;
  is_gc_enable_after_finish_ = true;

  DINGO_LOG(INFO) << "GC is stopped. Backup will enable GC.  if backup is finished.";

  return butil::Status::OK();
}

butil::Status Backup::SetGcStart() {
  if (!is_gc_enable_after_finish_) {
    return butil::Status::OK();
  }
  DINGO_LOG(INFO) << "Set GC start ...";

  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_gc_flag(
      ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_START);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  butil::Status status = br::InteractionManager::GetInstance().GetCoordinatorInteraction()->SendRequest(
      "CoordinatorService", "UpdateGCSafePoint", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set GC stop, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set GC stop, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

  is_gc_stop_ = false;
  is_gc_enable_after_finish_ = false;

  DINGO_LOG(INFO) << "Set GC start success.";

  return butil::Status::OK();
}

butil::Status Backup::RegisterBackupToCoordinator(bool is_first, ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::RegisterBackupRequest request;
  dingodb::pb::coordinator::RegisterBackupResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_backup_name(backup_task_id_);
  request.set_backup_path(storage_internal_);
  int64_t current_now_s = dingodb::Helper::Timestamp();
  if (is_first) {
    request.set_backup_start_timestamp(current_now_s);
  }
  request.set_backup_current_timestamp(current_now_s);
  request.set_backup_timeout_s(FLAGS_backup_task_timeout_s);

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "RegisterBackup", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set RegisterBackup, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set RegisterBackup, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  return butil::Status::OK();
}

butil::Status Backup::UnregisterBackupToCoordinator(ServerInteractionPtr coordinator_interaction) {
  dingodb::pb::coordinator::UnRegisterBackupRequest request;
  dingodb::pb::coordinator::UnRegisterBackupResponse response;
  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  request.set_backup_name(backup_task_id_);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "UnRegisterBackup", request, response);
  if (!status.ok()) {
    std::string s = fmt::format("Fail to set UnRegisterBackup, status={}", status.error_cstr());
    DINGO_LOG(ERROR) << s;
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    std::string s = fmt::format("Fail to set UnRegisterBackup, error={}", response.error().errmsg());
    DINGO_LOG(ERROR) << s;
    return butil::Status(response.error().errcode(), s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

butil::Status Backup::DoAsyncRegisterBackupToCoordinator(ServerInteractionPtr coordinator_interaction) {
  std::shared_ptr<Backup> self = GetSelf();
  auto lambda_call = [self, coordinator_interaction]() {
    self->DoRegisterBackupToCoordinatorInternal(coordinator_interaction);
  };

#if defined(ENABLE_BACKUP_PTHREAD)
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

butil::Status Backup::DoRegisterBackupToCoordinatorInternal(ServerInteractionPtr coordinator_interaction) {
  butil::Status status;
  bool is_first = false;
  is_exit_register_backup_to_coordinator_thread_ = false;
  while (!is_need_exit_) {
    bool is_error_occur = true;
    uint32_t retry_times = FLAGS_backup_task_max_retry;
    do {
      status = RegisterBackupToCoordinator(is_first, coordinator_interaction);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
      } else {  // success
        is_error_occur = false;
        break;
      }
      sleep(FLAGS_backup_watch_interval_s);
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

    sleep(FLAGS_backup_watch_interval_s);
  }

  is_exit_register_backup_to_coordinator_thread_ = true;
  DINGO_LOG(INFO) << "exit register backup  thread.";

  return butil::Status::OK();
}

butil::Status Backup::DisableBalanceToCoordinator(ServerInteractionPtr coordinator_interaction) {
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

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  butil::Status status =
      coordinator_interaction->AllSendRequest("CoordinatorService", "ControlConfig", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (status.error_code() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return butil::Status(status.error_code(), "%s", status.error_cstr());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

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

  return butil::Status::OK();
}

butil::Status Backup::EnableBalanceToCoordinator(ServerInteractionPtr coordinator_interaction) const {
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

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

    butil::Status status =
        coordinator_interaction->AllSendRequest("CoordinatorService", "ControlConfig", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return butil::Status(status.error_code(), "%s", status.error_cstr());
    }

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();
  }

  return butil::Status::OK();
}

butil::Status Backup::DisableSplitAndMergeToStoreAndIndex(ServerInteractionPtr store_interaction,
                                                          ServerInteractionPtr index_interaction) {
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

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  butil::Status status = store_interaction->AllSendRequest("StoreService", "ControlConfig", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (status.error_code() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return butil::Status(status.error_code(), "%s", status.error_cstr());
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

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  status = index_interaction->AllSendRequest("IndexService", "ControlConfig", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (status.error_code() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return butil::Status(status.error_code(), "%s", status.error_cstr());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

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

  return butil::Status::OK();
}
butil::Status Backup::EnableSplitAndMergeToStoreAndIndex(ServerInteractionPtr store_interaction,
                                                         ServerInteractionPtr index_interaction) const {
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

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

    butil::Status status = store_interaction->AllSendRequest("StoreService", "ControlConfig", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return butil::Status(status.error_code(), "%s", status.error_cstr());
    }

    status = index_interaction->AllSendRequest("IndexService", "ControlConfig", request, response);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    if (status.error_code() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return butil::Status(status.error_code(), "%s", status.error_cstr());
    }

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();
  }
  return butil::Status::OK();
}

butil::Status Backup::GetVersionFromCoordinator(ServerInteractionPtr coordinator_interaction,
                                                dingodb::pb::common::VersionInfo& version_info) {
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_is_just_version_info(true);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  butil::Status status = coordinator_interaction->SendRequest("CoordinatorService", "Hello", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (status.error_code() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return butil::Status(status.error_code(), "%s", status.error_cstr());
  }

  version_info = response.version_info();

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

butil::Status Backup::CompareVersion(const dingodb::pb::common::VersionInfo& version_info_local,
                                     const dingodb::pb::common::VersionInfo& version_info_remote) {
  DINGO_LOG(INFO) << "local version info : " << version_info_local.DebugString() << std::endl;
  DINGO_LOG(INFO) << "remote version info : " << version_info_remote.DebugString() << std::endl;

  if (version_info_local.git_commit_hash() != version_info_remote.git_commit_hash()) {
    std::string s = fmt::format("git_commit_hash is different. local : {} remote : {}",
                                version_info_local.git_commit_hash(), version_info_remote.git_commit_hash());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EBACKUP_VERSION_NOT_MATCH, s);
  }

  return butil::Status::OK();
}

butil::Status Backup::DoFinish() {
  if (is_already_register_backup_to_coordinator_) {
    if (!is_need_exit_) {
      is_need_exit_ = true;
    }
    std::cerr << "Waiting for register backup thread exit. (Do not use kill -9 or Ctrl-C to exit.) <";
    DINGO_LOG(INFO) << "Waiting for register backup thread exit. (Do not use kill -9 or Ctrl-C to exit.) <";
    std::string s;
    while (true) {
      if (is_exit_register_backup_to_coordinator_thread_) {
        break;
      }
      // sleep(FLAGS_backup_watch_interval_s);
      sleep(1);
      std::cerr << "-";
      s += "-";
    }
    std::cout << ">" << std::endl;
    DINGO_LOG(INFO) << s << ">";
    UnregisterBackupToCoordinator(br::InteractionManager::GetInstance().GetCoordinatorInteraction());
  }

  if (is_gc_enable_after_finish_) {
    SetGcStart();
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

}  // namespace br
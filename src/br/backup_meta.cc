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

#include "br/backup_meta.h"

#include <memory>
#include <string>

#include "br/helper.h"
#include "br/sst_file_writer.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"

namespace br {

BackupMeta::BackupMeta(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                       ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                       const std::string& backupts, int64_t backuptso_internal, const std::string& storage,
                       const std::string& storage_internal)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      index_interaction_(index_interaction),
      document_interaction_(document_interaction),
      backupts_(backupts),
      backuptso_internal_(backuptso_internal),
      storage_(storage),
      storage_internal_(storage_internal) {}

BackupMeta::~BackupMeta() = default;

std::shared_ptr<BackupMeta> BackupMeta::GetSelf() { return shared_from_this(); }

butil::Status BackupMeta::Init() {
  butil::Status status;
  if (!backup_sql_meta_) {
    std::vector<std::string> coordinator_addrs = coordinator_interaction_->GetAddrs();
    std::vector<std::string> store_addrs = store_interaction_->GetAddrs();

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

    backup_sql_meta_ = std::make_shared<BackupSqlMeta>(coordinator_interaction, store_interaction, backupts_,
                                                       backuptso_internal_, storage_, storage_internal_);
  }

  status = backup_sql_meta_->GetSqlMetaRegionFromCoordinator();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  DINGO_LOG(INFO) << "GetSqlMetaRegionFromCoordinator success";

  if (!backup_sdk_meta_) {
    std::vector<std::string> coordinator_addrs = coordinator_interaction_->GetAddrs();

    std::shared_ptr<br::ServerInteraction> coordinator_interaction;
    status = ServerInteraction::CreateInteraction(coordinator_addrs, coordinator_interaction);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
    backup_sdk_meta_ = std::make_shared<BackupSdkMeta>(coordinator_interaction, storage_internal_);
  }

  status = backup_sdk_meta_->GetSdkMetaFromCoordinator();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  DINGO_LOG(INFO) << "GetSdkMetaFromCoordinator success";

  return butil::Status::OK();
}

butil::Status BackupMeta::Run(std::shared_ptr<dingodb::pb::common::RegionMap> region_map) {
  butil::Status status;

  // backup sql meta
  {
    backup_sql_meta_->SetRegionMap(region_map);

    std::vector<int64_t> region_list;
    backup_sql_meta_->GetSqlMetaRegionList(region_list);
    status = backup_sql_meta_->ReserveSqlMeta(region_list);

    status = backup_sql_meta_->Filter();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = backup_sql_meta_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = backup_sql_meta_->Backup();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // backup sdk meta
  {
    status = backup_sdk_meta_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = backup_sdk_meta_->Backup();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status BackupMeta::Finish() {
  butil::Status status;

  std::string file_name = dingodb::Constant::kBackupMetaSchemaName;
  std::string file_path = storage_internal_ + "/" + file_name;

  std::map<std::string, std::string> kvs;

  std::shared_ptr<std::vector<dingodb::pb::common::BackupMeta>> sql_meta = backup_sql_meta_->GetBackupMeta();
  std::shared_ptr<dingodb::pb::common::BackupMeta> sdk_meta = backup_sdk_meta_->GetBackupMeta();
  for (const auto& meta : *sql_meta) {
    kvs.emplace(meta.file_name(), meta.SerializeAsString());
  }

  { kvs.emplace(sdk_meta->file_name(), sdk_meta->SerializeAsString()); }

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

  if (!backup_meta_) {
    backup_meta_ = std::make_shared<dingodb::pb::common::BackupMeta>();
  }

  backup_meta_->set_remark("backup sdk meta and sql meta. save dingodb::pb::common::BackupDataFileValueSstMetaGroup. ");
  backup_meta_->set_exec_node(dingodb::Constant::kBackupRegionName);
  backup_meta_->set_dir_name("");
  backup_meta_->set_file_size(sst->GetSize());
  backup_meta_->set_encryption(hash_code);
  backup_meta_->set_file_name(file_name);

  if (FLAGS_br_log_switch_backup_detail) {
    DINGO_LOG(INFO) << backup_meta_->DebugString() << std::endl;
  }

  return butil::Status::OK();
}

std::vector<int64_t> BackupMeta::GetSqlMetaRegionList() {
  std::vector<int64_t> region_list;
  backup_sql_meta_->GetSqlMetaRegionList(region_list);
  return region_list;
}

std::shared_ptr<dingodb::pb::common::BackupMeta> BackupMeta::GetBackupMeta() { return backup_meta_; }

std::pair<butil::Status, std::shared_ptr<dingodb::pb::meta::IdEpochTypeAndValue>> BackupMeta::GetIdEpochTypeAndValue() {
  butil::Status status;
  status = GetPresentIdsFromCoordinator();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return {status, nullptr};
  }
  return {butil::Status::OK(), id_epoch_type_and_value_};
}

std::pair<butil::Status, std::shared_ptr<dingodb::pb::meta::TableIncrementGroup>> BackupMeta::GetAllTableIncrement() {
  butil::Status status;
  status = GetAllTableIncrementFromMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return {status, nullptr};
  }
  return {butil::Status::OK(), table_increment_group_};
}

butil::Status BackupMeta::GetPresentIdsFromCoordinator() {
  dingodb::pb::meta::SaveIdEpochTypeRequest request;
  dingodb::pb::meta::SaveIdEpochTypeResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("MetaService", "SaveIdEpochType", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << response.error().errmsg();
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

  id_epoch_type_and_value_ =
      std::make_shared<dingodb::pb::meta::IdEpochTypeAndValue>(response.id_epoch_type_and_value());

  return butil::Status::OK();
}

butil::Status BackupMeta::GetAllTableIncrementFromMeta() {
  dingodb::pb::meta::GetAutoIncrementsRequest request;
  dingodb::pb::meta::GetAutoIncrementsResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("MetaService", "GetAutoIncrements", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << response.error().errmsg();
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

  table_increment_group_ = std::make_shared<dingodb::pb::meta::TableIncrementGroup>();
  for (const auto& table_increment : response.table_increments()) {
    table_increment_group_->add_table_increments()->CopyFrom(table_increment);
  }

  return butil::Status::OK();
}

}  // namespace br
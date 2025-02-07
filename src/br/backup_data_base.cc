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

#include "br/backup_data_base.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/helper.h"
#include "br/sst_file_writer.h"
#include "common/constant.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace br {

BackupDataBase::BackupDataBase(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                               ServerInteractionPtr index_interaction, ServerInteractionPtr document_interaction,
                               const std::string& backupts, int64_t backuptso_internal, const std::string& storage,
                               const std::string& storage_internal, const std::string& name)
    : coordinator_interaction_(coordinator_interaction),
      store_interaction_(store_interaction),
      index_interaction_(index_interaction),
      document_interaction_(document_interaction),
      backupts_(backupts),
      backuptso_internal_(backuptso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      is_need_exit_(false),
      already_handle_regions_(0),
      already_handle_store_regions_(0),
      already_handle_index_regions_(0),
      already_handle_document_regions_(0),
      name_(name) {
  bthread_mutex_init(&mutex_, nullptr);
}

BackupDataBase::~BackupDataBase() { bthread_mutex_destroy(&mutex_); }

void BackupDataBase::SetRegionMap(std::shared_ptr<dingodb::pb::common::RegionMap> region_map) {
  region_map_ = region_map;
}

butil::Status BackupDataBase::Filter() { return butil::Status::OK(); }

butil::Status BackupDataBase::Run() { return butil::Status::OK(); }

butil::Status BackupDataBase::Backup() {
  butil::Status status;

  if (!backup_data_base_) {
    backup_data_base_ = std::make_shared<std::vector<dingodb::pb::common::BackupMeta>>();
  }

  status = BackupRegion();
  if (!status.ok()) {
    is_need_exit_ = true;
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  status = BackupCfSstMeta();
  if (!status.ok()) {
    is_need_exit_ = true;
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

std::shared_ptr<std::vector<dingodb::pb::common::BackupMeta>> BackupDataBase::GetBackupMeta() {
  return backup_data_base_;
}

butil::Status BackupDataBase::BackupRegion() {
  butil::Status status;
  std::string file_name;

  if (name_ == dingodb::Constant::kSdkData) {
    file_name = dingodb::Constant::kStoreRegionSdkDataSstName;
  } else {
    file_name = dingodb::Constant::kStoreRegionSqlDataSstName;
  }
  status = DoBackupRegion(wait_for_handle_store_regions_, file_name);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (name_ == dingodb::Constant::kSdkData) {
    file_name = dingodb::Constant::kIndexRegionSdkDataSstName;
  } else {
    file_name = dingodb::Constant::kIndexRegionSqlDataSstName;
  }
  status = DoBackupRegion(wait_for_handle_index_regions_, file_name);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (name_ == dingodb::Constant::kSdkData) {
    file_name = dingodb::Constant::kDocumentRegionSdkDataSstName;
  } else {
    file_name = dingodb::Constant::kDocumentRegionSqlDataSstName;
  }
  status = DoBackupRegion(wait_for_handle_document_regions_, file_name);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status BackupDataBase::BackupCfSstMeta() {
  butil::Status status;
  std::string file_name;

  if (name_ == dingodb::Constant::kSdkData) {
    file_name = dingodb::Constant::kStoreCfSstMetaSdkDataSstName;
  } else {
    file_name = dingodb::Constant::kStoreCfSstMetaSqlDataSstName;
  }
  status = DoBackupCfSstMeta(save_store_region_map_, file_name, dingodb::Constant::kStoreRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (name_ == dingodb::Constant::kSdkData) {
    file_name = dingodb::Constant::kIndexCfSstMetaSdkDataSstName;
  } else {
    file_name = dingodb::Constant::kIndexCfSstMetaSqlDataSstName;
  }
  status = DoBackupCfSstMeta(save_index_region_map_, file_name, dingodb::Constant::kIndexRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (name_ == dingodb::Constant::kSdkData) {
    file_name = dingodb::Constant::kDocumentCfSstMetaSdkDataSstName;
  } else {
    file_name = dingodb::Constant::kDocumentCfSstMetaSqlDataSstName;
  }
  status = DoBackupCfSstMeta(save_document_region_map_, file_name, dingodb::Constant::kDocumentRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status BackupDataBase::DoBackupRegion(
    std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_regions, const std::string& file_name) {
  butil::Status status;

  std::string file_path = storage_internal_ + "/" + file_name;

  if (!wait_for_handle_regions->empty()) {
    std::map<std::string, std::string> kvs;
    for (const auto& region : *wait_for_handle_regions) {
      kvs.emplace(std::to_string(region.id()), region.SerializeAsString());
    }

    rocksdb::Options options;
    std::shared_ptr<SstFileWriter> sst = std::make_shared<SstFileWriter>(options);

    status = sst->SaveFile(kvs, file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    save_region_files_.push_back(file_path);

    std::string hash_code;
    status = dingodb::Helper::CalSha1CodeWithFileEx(file_path, hash_code);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    dingodb::pb::common::BackupMeta backup_meta;

    if (name_ == dingodb::Constant::kSdkData) {
      backup_meta.set_remark("python sdk create region meta info. save dingodb::pb::common::Region. ");
    } else {
      backup_meta.set_remark("executor create region meta info. save dingodb::pb::common::Region. ");
    }
    backup_meta.set_exec_node(dingodb::Constant::kCoordinatorRegionName);
    backup_meta.set_dir_name("");
    backup_meta.set_file_size(sst->GetSize());
    backup_meta.set_encryption(hash_code);
    backup_meta.set_file_name(file_name);

    if (FLAGS_br_log_switch_backup_detail) {
      DINGO_LOG(INFO) << backup_meta.DebugString() << std::endl;
    }

    backup_data_base_->push_back(std::move(backup_meta));
  }
  return butil::Status::OK();
}

butil::Status BackupDataBase::DoBackupCfSstMeta(
    std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_region_map,
    const std::string& file_name, const std::string& region_from) {
  butil::Status status;
  std::string file_path = storage_internal_ + "/" + file_name;

  if (!save_region_map->empty()) {
    std::map<std::string, std::string> kvs;
    for (const auto& [region_id, group] : *save_region_map) {
      kvs.emplace(std::to_string(region_id), group.SerializeAsString());
    }

    rocksdb::Options options;
    std::shared_ptr<SstFileWriter> sst = std::make_shared<SstFileWriter>(options);

    status = sst->SaveFile(kvs, file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    save_cf_sst_meta_files_.push_back(file_path);

    std::string hash_code;
    status = dingodb::Helper::CalSha1CodeWithFileEx(file_path, hash_code);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    dingodb::pb::common::BackupMeta backup_meta;

    if (name_ == dingodb::Constant::kSdkData) {
      backup_meta.set_remark(
          "python sdk create region sst meta info. save dingodb::pb::common::BackupDataFileValueSstMetaGroup. ");

    } else {
      backup_meta.set_remark(
          "executor create region sst meta info. save dingodb::pb::common::BackupDataFileValueSstMetaGroup. ");
    }
    backup_meta.set_exec_node(region_from);
    backup_meta.set_dir_name("");
    backup_meta.set_file_size(sst->GetSize());
    backup_meta.set_encryption(hash_code);
    backup_meta.set_file_name(file_name);

    if (FLAGS_br_log_switch_backup_detail) {
      DINGO_LOG(INFO) << backup_meta.DebugString() << std::endl;
    }

    backup_data_base_->push_back(std::move(backup_meta));
  }
  return butil::Status::OK();
}

butil::Status BackupDataBase::DoBackupRegionInternal(
    ServerInteractionPtr interaction, const std::string& service_name,
    std::shared_ptr<std::vector<dingodb::pb::common::Region>> wait_for_handle_regions,
    std::atomic<int64_t>& already_handle_regions,
    std::shared_ptr<std::map<int64_t, dingodb::pb::common::BackupDataFileValueSstMetaGroup>> save_region_map,
    std::atomic<bool>& is_thread_exit) {
  butil::Status status;
  is_thread_exit = false;
  for (const auto& region : *wait_for_handle_regions) {
    if (is_need_exit_) {
      break;
    }
    dingodb::pb::store::BackupDataRequest request;
    dingodb::pb::store::BackupDataResponse response;

    request.mutable_request_info()->set_request_id(Helper::GetRandInt());
    request.mutable_context()->set_region_id(region.id());
    request.mutable_context()->mutable_region_epoch()->CopyFrom(region.definition().epoch());
    request.set_start_key(region.definition().range().start_key());
    request.set_end_key(region.definition().range().end_key());
    request.set_need_leader(true);
    request.set_region_type(region.region_type());
    request.set_backup_ts(backupts_);
    request.set_backup_tso(backuptso_internal_);
    request.set_storage_path(storage_);
    request.mutable_storage_backend()->mutable_local()->set_path(storage_internal_);

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << name_ << " " << request.DebugString();

    status = interaction->SendRequest(service_name, "BackupData", request, response);
    if (!status.ok()) {
      is_need_exit_ = true;
      std::string s = fmt::format("Fail to backup region, region_id={}, status={}", region.id(), status.error_cstr());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(status.error_code(), s);
      break;
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      is_need_exit_ = true;
      std::string s =
          fmt::format("Fail to backup region, region_id={}, error={}", region.id(), response.error().errmsg());
      DINGO_LOG(ERROR) << s;
      status = butil::Status(response.error().errcode(), s);
      break;
    }

    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << name_ << " " << response.DebugString();

    if (response.sst_metas().backup_data_file_value_sst_metas_size() > 0) {
      save_region_map->insert({region.id(), response.sst_metas()});
    }
    already_handle_regions++;
  }

  is_thread_exit = true;
  {
    BAIDU_SCOPED_LOCK(mutex_);
    last_error_ = status;
  }

  return status;
}

}  // namespace br
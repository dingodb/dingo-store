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

#include "br/backup_data.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/helper.h"
#include "br/interation.h"
#include "br/parameter.h"
#include "br/sst_file_writer.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/coordinator.pb.h"

namespace br {

BackupData::BackupData(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
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

BackupData::~BackupData() = default;

std::shared_ptr<BackupData> BackupData::GetSelf() { return shared_from_this(); }

butil::Status BackupData::Init(const std::vector<int64_t>& meta_region_list) {
  butil::Status status = GetAllRegionMapFromCoordinator();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  DINGO_LOG(INFO) << "GetAllRegionMapFromCoordinator success";

  // init sql data
  {
    if (!backup_sql_data_) {
      std::vector<std::string> coordinator_addrs = coordinator_interaction_->GetAddrs();
      std::vector<std::string> store_addrs = store_interaction_->GetAddrs();
      std::vector<std::string> index_addrs = index_interaction_->GetAddrs();
      std::vector<std::string> document_addrs = document_interaction_->GetAddrs();

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
      backup_sql_data_ = std::make_shared<BackupSqlData>(coordinator_interaction, store_interaction, index_interaction,
                                                         document_interaction, backupts_, backuptso_internal_, storage_,
                                                         storage_internal_);
    }

    backup_sql_data_->SetRegionMap(region_map_);

    status = backup_sql_data_->RemoveSqlMeta(meta_region_list);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = backup_sql_data_->Filter();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // init sdk data
  {
    if (!backup_sdk_data_) {
      std::vector<std::string> coordinator_addrs = coordinator_interaction_->GetAddrs();
      std::vector<std::string> store_addrs = store_interaction_->GetAddrs();
      std::vector<std::string> index_addrs = index_interaction_->GetAddrs();
      std::vector<std::string> document_addrs = document_interaction_->GetAddrs();

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

      backup_sdk_data_ = std::make_shared<BackupSdkData>(coordinator_interaction, store_interaction, index_interaction,
                                                         document_interaction, backupts_, backuptso_internal_, storage_,
                                                         storage_internal_);
    }

    backup_sdk_data_->SetRegionMap(region_map_);

    status = backup_sdk_data_->Filter();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status BackupData::Run() {
  butil::Status status;

  // backup sql data
  {
    status = backup_sql_data_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = backup_sql_data_->Backup();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  // backup sdk data
  {
    status = backup_sdk_data_->Run();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    status = backup_sdk_data_->Backup();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status BackupData::Finish() {
  butil::Status status;

  std::string file_name = dingodb::Constant::kBackupMetaDataFileName;
  std::string file_path = storage_internal_ + "/" + file_name;

  std::map<std::string, std::string> kvs;

  if (backup_sql_data_) {
    std::shared_ptr<std::vector<dingodb::pb::common::BackupMeta>> sql_backup_meta = backup_sql_data_->GetBackupMeta();
    for (const auto& meta : *sql_backup_meta) {
      kvs.emplace(meta.file_name(), meta.SerializeAsString());
    }
  }

  if (backup_sdk_data_) {
    std::shared_ptr<std::vector<dingodb::pb::common::BackupMeta>> sdk_backup_meta = backup_sdk_data_->GetBackupMeta();
    for (const auto& meta : *sdk_backup_meta) {
      kvs.emplace(meta.file_name(), meta.SerializeAsString());
    }
  }

  if (!kvs.empty()) {
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

    backup_meta_->set_remark(
        "backup sdk data and sql data. save dingodb::pb::common::BackupDataFileValueSstMetaGroup. ");
    backup_meta_->set_exec_node(dingodb::Constant::kBackupRegionName);
    backup_meta_->set_dir_name("");
    backup_meta_->set_file_size(sst->GetSize());
    backup_meta_->set_encryption(hash_code);
    backup_meta_->set_file_name(file_name);

    if (FLAGS_br_log_switch_backup_detail) {
      DINGO_LOG(INFO) << backup_meta_->DebugString() << std::endl;
    }
  }  // if (!kvs.empty()) {

  return butil::Status::OK();
}

std::shared_ptr<dingodb::pb::common::BackupMeta> BackupData::GetBackupMeta() { return backup_meta_; }

butil::Status BackupData::GetAllRegionMapFromCoordinator() {
  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_tenant_id(-1);  // get all tenants region map

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("CoordinatorService", "GetRegionMap", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << response.error().errmsg();
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  for (const auto& region : response.regionmap().regions()) {
    if (region.id() == 80081) {
      if (region.definition().has_index_parameter()) {
        DINGO_LOG(INFO) << "has_index_parameter ***************meta store region id : " << region.id()
                        << ",  name : " << region.definition().name()
                        << ", index_parameter : " << region.definition().index_parameter().DebugString();
      } else {
        DINGO_LOG(INFO) << "no_index_parameter ***************meta store region id : " << region.id()
                        << ",  name : " << region.definition().name()
                        << ", index_parameter : " << region.definition().index_parameter().DebugString();
      }
    }
  }

  region_map_ = std::make_shared<dingodb::pb::common::RegionMap>(response.regionmap());

  if (FLAGS_br_log_switch_backup_detail) {
    DINGO_LOG(INFO) << "===================================================";
    DINGO_LOG(INFO) << "GetRegionMap region size = " << response.regionmap().regions_size();
    int i = 0;
    std::string s;
    for (const auto& region : response.regionmap().regions()) {
      if (0 != i++) {
        s += ", ";
      }
      s += std::to_string(region.id());
      if (i == 10) {
        DINGO_LOG(INFO) << "region_id=[" << s << "]";
        s.clear();
        i = 0;
      }
    }
    if (!s.empty()) {
      DINGO_LOG(INFO) << "region_id=[" << s << "]";
    }
    DINGO_LOG(INFO) << "===================================================";
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

}  // namespace br
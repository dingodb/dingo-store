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

#include "br/backup_sdk_meta.h"

#include <memory>
#include <string>

#include "br/helper.h"
#include "br/sst_file_writer.h"
#include "common/constant.h"
#include "common/helper.h"
#include "fmt/core.h"

namespace br {

BackupSdkMeta::BackupSdkMeta(ServerInteractionPtr coordinator_interaction, const std::string &storage_internal)
    : coordinator_interaction_(coordinator_interaction), storage_internal_(storage_internal) {}

BackupSdkMeta::~BackupSdkMeta() = default;

std::shared_ptr<BackupSdkMeta> BackupSdkMeta::GetSelf() { return shared_from_this(); }

butil::Status BackupSdkMeta::GetSdkMetaFromCoordinator() {
  dingodb::pb::meta::ExportMetaRequest request;
  dingodb::pb::meta::ExportMetaResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("MetaService", "ExportMeta", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << response.error().errmsg();
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  meta_all_ = std::make_shared<dingodb::pb::meta::MetaALL>(response.meta_all());

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << meta_all_->DebugString();

  return butil::Status::OK();
}

butil::Status BackupSdkMeta::Run() {
  std::cerr << "Full Backup Sdk Meta " << "<";
  DINGO_LOG(INFO) << "Full Backup Sdk Meta " << "<";

  std::cerr << "-";
  DINGO_LOG(INFO) << "-";

  std::cerr << ">" << " 100.00%";
  DINGO_LOG(INFO) << ">" << " 100.00%";

  std::cout << std::endl;
  return butil::Status::OK();
}

butil::Status BackupSdkMeta::Backup() {
  butil::Status status;

  std::map<std::string, std::string> kvs;

  kvs.emplace(dingodb::Constant::kCoordinatorSdkMetaKeyName, meta_all_->SerializeAsString());

  rocksdb::Options options;
  std::shared_ptr<SstFileWriter> sst = std::make_shared<SstFileWriter>(options);

  std::string file_name = dingodb::Constant::kCoordinatorSdkMetaSstName;
  std::string file_path = storage_internal_ + "/" + file_name;

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

  if (!backup_sdk_meta_) {
    backup_sdk_meta_ = std::make_shared<dingodb::pb::common::BackupMeta>();
  }

  backup_sdk_meta_->set_remark(
      "python sdk create tenants, schema, table, index etc. meta info. save dingodb::pb::meta::MetaALL.");
  backup_sdk_meta_->set_exec_node(dingodb::Constant::kCoordinatorRegionName);
  backup_sdk_meta_->set_dir_name("");
  backup_sdk_meta_->set_file_size(sst->GetSize());
  backup_sdk_meta_->set_encryption(hash_code);
  backup_sdk_meta_->set_file_name(file_name);

  if (FLAGS_br_log_switch_backup_detail) {
    DINGO_LOG(INFO) << backup_sdk_meta_->DebugString() << std::endl;
  }

  return butil::Status::OK();
}

std::shared_ptr<dingodb::pb::common::BackupMeta> BackupSdkMeta::GetBackupMeta() { return backup_sdk_meta_; }

}  // namespace br
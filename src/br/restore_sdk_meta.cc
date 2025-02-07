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

#include "br/restore_sdk_meta.h"

#include <memory>
#include <string>

#include "br/helper.h"
#include "br/sst_file_reader.h"
#include "br/utils.h"
#include "common/constant.h"
#include "common/helper.h"
#include "fmt/core.h"

namespace br {

RestoreSdkMeta::RestoreSdkMeta(ServerInteractionPtr coordinator_interaction, const std::string& restorets,
                               int64_t restoretso_internal, const std::string& storage,
                               const std::string& storage_internal,
                               std::shared_ptr<dingodb::pb::common::BackupMeta> coordinator_sdk_meta_sst)
    : coordinator_interaction_(coordinator_interaction),
      restorets_(restorets),
      restoretso_internal_(restoretso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      coordinator_sdk_meta_sst_(coordinator_sdk_meta_sst) {}

RestoreSdkMeta::~RestoreSdkMeta() = default;

std::shared_ptr<RestoreSdkMeta> RestoreSdkMeta::GetSelf() { return shared_from_this(); }

butil::Status RestoreSdkMeta::Init() {
  butil::Status status;

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << coordinator_sdk_meta_sst_->DebugString();

  status = CheckCoordinatorSdkMetaSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = ExtractFromCoordinatorSdkMetaSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::Run() {
  butil::Status status;

  status = ImportSdkMetaToCoordinator();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::Finish() { return butil::Status::OK(); }

butil::Status RestoreSdkMeta::CheckCoordinatorSdkMetaSst() {
  butil::Status status;

  status = Utils::CheckBackupMeta(coordinator_sdk_meta_sst_, storage_internal_,
                                  dingodb::Constant::kCoordinatorSdkMetaSstName, "",
                                  dingodb::Constant::kCoordinatorRegionName);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::ExtractFromCoordinatorSdkMetaSst() {
  butil::Status status;

  std::string file_path = storage_internal_ + "/" + coordinator_sdk_meta_sst_->file_name();

  SstFileReader sst_file_reader;
  std::map<std::string, std::string> internal_coordinator_sdk_meta_kvs;
  status = sst_file_reader.ReadFile(file_path, internal_coordinator_sdk_meta_kvs);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // find kCoordinatorSdkMetaKeyName
  auto iter = internal_coordinator_sdk_meta_kvs.find(dingodb::Constant::kCoordinatorSdkMetaKeyName);
  if (iter == internal_coordinator_sdk_meta_kvs.end()) {
    std::string s =
        fmt::format("not found {} in coordinator.sdk.meta file.", dingodb::Constant::kCoordinatorSdkMetaKeyName);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_NOT_FOUND_KEY_IN_FILE, s);
  }

  dingodb::pb::meta::MetaALL meta_all;
  if (!meta_all.ParseFromString(iter->second)) {
    std::string s =
        fmt::format("parse dingodb::pb::meta::MetaALL failed : {}", dingodb::Constant::kCoordinatorSdkMetaKeyName);
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  meta_all_ = std::make_shared<dingodb::pb::meta::MetaALL>(std::move(meta_all));

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::ImportSdkMetaToCoordinator() {
  dingodb::pb::meta::ImportMetaRequest request;
  dingodb::pb::meta::ImportMetaResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.mutable_meta_all()->CopyFrom(*meta_all_);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("MetaService", "ImportMeta", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << response.error().errmsg();
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

}  // namespace br
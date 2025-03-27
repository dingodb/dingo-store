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
#include "proto/meta.pb.h"

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

  if (coordinator_sdk_meta_sst_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << coordinator_sdk_meta_sst_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "coordinator_sdk_meta_sst_ = nullptr";
  }

  status = CheckCoordinatorSdkMetaSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = ExtractFromCoordinatorSdkMetaSst();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (meta_all_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << meta_all_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "meta_all_ = nullptr";
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::Run() {
  butil::Status status;

  if (meta_all_) {
    status = ImportSdkMetaToCoordinator();
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  } else {
    DINGO_LOG(WARNING) << "meta_all_ = nullptr. ignore run";
  }  // if (meta_all_)

  std::cerr << "Full Restore Sdk Meta" << " <--->" << " 100.00%";
  DINGO_LOG(INFO) << "Full Restore Sdk Meta" << " <--->" << " 100.00%";

  std::cout << std::endl;

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::Finish() { return butil::Status::OK(); }

std::pair<int64_t, int64_t> RestoreSdkMeta::GetRegions() { return std::pair<int64_t, int64_t>(0, 0); }

butil::Status RestoreSdkMeta::CheckCoordinatorSdkMetaSst() {
  butil::Status status;

  if (coordinator_sdk_meta_sst_) {
    std::string file_name = dingodb::Constant::kCoordinatorSdkMetaSstName;
    std::string file_path = storage_internal_ + "/" + file_name;
    status = Utils::FileExistsAndRegular(file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    status = Utils::CheckBackupMeta(coordinator_sdk_meta_sst_, storage_internal_,
                                    dingodb::Constant::kCoordinatorSdkMetaSstName, "",
                                    dingodb::Constant::kCoordinatorRegionName);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }
  }  // if (coordinator_sdk_meta_sst_)

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::ExtractFromCoordinatorSdkMetaSst() {
  butil::Status status;

  if (coordinator_sdk_meta_sst_) {
    std::string file_path = storage_internal_ + "/" + coordinator_sdk_meta_sst_->file_name();

    SstFileReader sst_file_reader;
    std::map<std::string, std::string> internal_coordinator_sdk_meta_kvs;
    status = sst_file_reader.ReadFile(file_path, internal_coordinator_sdk_meta_kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
      return status;
    }

    // find kCoordinatorSdkMetaKeyName
    auto iter = internal_coordinator_sdk_meta_kvs.find(dingodb::Constant::kCoordinatorSdkMetaKeyName);
    if (iter != internal_coordinator_sdk_meta_kvs.end()) {
      dingodb::pb::meta::MetaALL meta_all;
      if (!meta_all.ParseFromString(iter->second)) {
        std::string s =
            fmt::format("parse dingodb::pb::meta::MetaALL failed : {}", dingodb::Constant::kCoordinatorSdkMetaKeyName);
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }

      meta_all_ = std::make_shared<dingodb::pb::meta::MetaALL>(std::move(meta_all));
    } else {
      std::string s = fmt::format("coordinator_sdk_meta.sst = nullptr");
      DINGO_LOG(WARNING) << s;
    }  // if (coordinator_sdk_meta_sst_)
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::ImportSdkMetaToCoordinator() {
  butil::Status status;

  status = CreateTenantsToCoordinator();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = CreateSchemasToCoordinator();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  status = CreateIndexMetasToCoordinator();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::ImportSdkMetaToCoordinatorDeprecated() {
  dingodb::pb::meta::ImportMetaRequest request;
  dingodb::pb::meta::ImportMetaResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.mutable_meta_all()->CopyFrom(*meta_all_);

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("MetaService", "ImportMeta", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::CreateTenantsToCoordinator() {
  dingodb::pb::meta::CreateTenantsRequest request;
  dingodb::pb::meta::CreateTenantsResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.mutable_tenants()->CopyFrom(meta_all_->tenants());

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("MetaService", "CreateTenants", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  return butil::Status::OK();
}
butil::Status RestoreSdkMeta::CreateSchemasToCoordinator() {
  dingodb::pb::meta::CreateSchemasRequest request;
  dingodb::pb::meta::CreateSchemasResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.mutable_schemas()->insert(meta_all_->schemas().begin(), meta_all_->schemas().end());

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("MetaService", "CreateSchemas", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

butil::Status RestoreSdkMeta::CreateIndexMetasToCoordinator() {
  dingodb::pb::meta::CreateIndexMetasRequest request;
  dingodb::pb::meta::CreateIndexMetasResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.mutable_tables_and_indexes()->insert(meta_all_->tables_and_indexes().begin(),
                                               meta_all_->tables_and_indexes().end());

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("MetaService", "CreateIndexMetas", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

}  // namespace br
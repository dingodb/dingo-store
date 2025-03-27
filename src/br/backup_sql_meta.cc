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

#include "br/backup_sql_meta.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/parameter.h"
#include "br/utils.h"
#include "common/helper.h"
#include "fmt/core.h"

namespace br {

BackupSqlMeta::BackupSqlMeta(ServerInteractionPtr coordinator_interaction, ServerInteractionPtr store_interaction,
                             const std::string& backupts, int64_t backuptso_internal, const std::string& storage,
                             const std::string& storage_internal)
    : BackupMetaBase(coordinator_interaction, store_interaction, backupts, backuptso_internal, storage,
                     storage_internal) {}

BackupSqlMeta::~BackupSqlMeta() = default;

std::shared_ptr<BackupSqlMeta> BackupSqlMeta::GetSelf() { return shared_from_this(); }

butil::Status BackupSqlMeta::GetSqlMetaRegionFromCoordinator() {  // get meta region
  dingodb::pb::coordinator::ScanRegionsRequest request;
  dingodb::pb::coordinator::ScanRegionsResponse response;

  request.set_key(dingodb::Helper::HexToString("740000000000000000"));
  request.set_range_end(dingodb::Helper::HexToString("740000000000000001"));

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << request.DebugString();

  auto status = coordinator_interaction_->SendRequest("CoordinatorService", "ScanRegions", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << Utils::FormatResponseError(response);
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  scan_region_infos_ = std::make_shared<std::vector<dingodb::pb::coordinator::ScanRegionInfo>>();
  scan_region_infos_->insert(scan_region_infos_->begin(), response.regions().begin(), response.regions().end());

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_backup_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

void BackupSqlMeta::GetSqlMetaRegionList(std::vector<int64_t>& region_list) {
  region_list.clear();

  for (const auto& region_info : *scan_region_infos_) {
    region_list.push_back(region_info.region_id());
  }
}

}  // namespace br
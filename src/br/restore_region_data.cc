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

#include "br/restore_region_data.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/helper.h"
#include "br/parameter.h"
#include "br/utils.h"
#include "common/constant.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"

namespace br {

RestoreRegionData::RestoreRegionData(
    ServerInteractionPtr coordinator_interaction, ServerInteractionPtr interaction,
    std::shared_ptr<dingodb::pb::common::Region> region, const std::string& restorets, int64_t restoretso_internal,
    const std::string& storage, const std::string& storage_internal,
    std::shared_ptr<dingodb::pb::common::BackupDataFileValueSstMetaGroup> backup_data_file_value_sst_meta_group,
    const std::string& backup_meta_region_cf_name, const std::string& group_belongs_to_whom,
    int64_t restore_region_timeout_s)
    : coordinator_interaction_(coordinator_interaction),
      interaction_(interaction),
      region_(region),
      restorets_(restorets),
      restoretso_internal_(restoretso_internal),
      storage_(storage),
      storage_internal_(storage_internal),
      backup_data_file_value_sst_meta_group_(backup_data_file_value_sst_meta_group),
      backup_meta_region_cf_name_(backup_meta_region_cf_name),
      group_belongs_to_whom_(group_belongs_to_whom),
      restore_region_timeout_s_(restore_region_timeout_s) {}

RestoreRegionData::~RestoreRegionData() = default;

std::shared_ptr<RestoreRegionData> RestoreRegionData::GetSelf() { return shared_from_this(); }

butil::Status RestoreRegionData::Init() {
  butil::Status status;

  if (backup_data_file_value_sst_meta_group_) {
    DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << backup_data_file_value_sst_meta_group_->DebugString();
  } else {
    DINGO_LOG(WARNING) << "backup_data_file_value_sst_meta_group_ = nullptr";
  }

  group_debug_info_ =
      fmt::format("backup_meta_region_cf_name:{} group_belongs_to_whom:{} region_name:{} region_id:{}",
                  backup_meta_region_cf_name_, group_belongs_to_whom_, region_->definition().name(), region_->id());

  // Avoid first failure
  region_epoch_.set_conf_version(1);
  region_epoch_.set_version(1);

  return butil::Status::OK();
}

butil::Status RestoreRegionData::Run() {
  if (backup_data_file_value_sst_meta_group_) {
    switch (region_->region_type()) {
      case dingodb::pb::common::RegionType::STORE_REGION:
        if (group_belongs_to_whom_ == dingodb::Constant::kRestoreMeta) {
          return RegionMetaToStore();
        } else {
          return RegionDataToStore();
        }
      case dingodb::pb::common::RegionType::INDEX_REGION:
        return RegionDataToIndex();
      case dingodb::pb::common::RegionType::DOCUMENT_REGION:
        return RegionDataToDocument();
      default:
        std::string s = group_debug_info_ +
                        " unknown region type : " + dingodb::pb::common::RegionType_Name(region_->region_type());
        DINGO_LOG(ERROR) << s;
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }
  }  // if (backup_data_file_value_sst_meta_group_) {

  return butil::Status::OK();
}

butil::Status RestoreRegionData::Finish() { return butil::Status::OK(); }

butil::Status RestoreRegionData::RegionDataToStore() {
  return SendRegionRequest<dingodb::pb::store::RestoreDataRequest, dingodb::pb::store::RestoreDataResponse>(
      "StoreService", "RestoreData");
}
butil::Status RestoreRegionData::RegionDataToIndex() {
  return SendRegionRequest<dingodb::pb::store::RestoreDataRequest, dingodb::pb::store::RestoreDataResponse>(
      "IndexService", "RestoreData");
}
butil::Status RestoreRegionData::RegionDataToDocument() {
  return SendRegionRequest<dingodb::pb::store::RestoreDataRequest, dingodb::pb::store::RestoreDataResponse>(
      "DocumentService", "RestoreData");
}
butil::Status RestoreRegionData::RegionMetaToStore() {
  return SendRegionRequest<dingodb::pb::store::RestoreMetaRequest, dingodb::pb::store::RestoreMetaResponse>(
      "StoreService", "RestoreMeta");
}

butil::Status RestoreRegionData::GetRegionEpoch() {
  butil::Status status;

  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  request.set_region_id(region_->id());

  status = coordinator_interaction_->SendRequest("CoordinatorService", "QueryRegion", request, response);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << Utils::FormatStatusError(status);
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << group_debug_info_ << " " << Utils::FormatResponseError(response);
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  region_epoch_ = response.region().definition().epoch();

  return butil::Status::OK();
}

template <typename Request, typename Response>
butil::Status RestoreRegionData::SendRegionRequest(const std::string& service_name, const std::string& api_name) {
  butil::Status status;

  if (backup_data_file_value_sst_meta_group_) {
    while (true) {
      Request request;
      Response response;

      request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());

      request.mutable_context()->set_region_id(region_->id());
      request.mutable_context()->mutable_region_epoch()->CopyFrom(region_epoch_);

      request.set_start_key(region_->definition().range().start_key());
      request.set_end_key(region_->definition().range().end_key());

      request.set_need_leader(true);

      request.set_region_type(region_->region_type());

      request.set_backup_ts(restorets_);

      request.set_backup_tso(restoretso_internal_);

      request.set_storage_path(storage_internal_);
      request.mutable_storage_backend()->mutable_local()->set_path(storage_internal_);

      request.mutable_sst_metas()->CopyFrom(*backup_data_file_value_sst_meta_group_);

      DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << request.DebugString();
      // DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

      auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
      auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      };

      auto restore_region_data_start_ms = lambda_time_now_function();
      status = interaction_->SendRequest(service_name, api_name, request, response, restore_region_timeout_s_ * 1000);
      auto restore_region_data_end_ms = lambda_time_now_function();
      auto restore_region_data_diff_ms =
          lambda_time_diff_microseconds_function(restore_region_data_start_ms, restore_region_data_end_ms);
      DINGO_LOG(INFO) << fmt::format("{}::{} region id:{} cost time:{} ", service_name, api_name, region_->id(),
                                     Utils::FormatTimeMs(restore_region_data_diff_ms));

      DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << response.DebugString();
      // DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

      if (status.ok()) {
        if (response.error().errcode() == dingodb::pb::error::OK) {
          break;
        } else if (response.error().errcode() != dingodb::pb::error::EREGION_VERSION) {
          std::string s = group_debug_info_ + " " + Utils::FormatResponseError(response);
          DINGO_LOG(ERROR) << s;
          return butil::Status(response.error().errcode(), s);
        }
      } else {
        if (status.error_code() != dingodb::pb::error::EREGION_VERSION) {
          std::string s = group_debug_info_ + " " + Utils::FormatStatusError(status);
          DINGO_LOG(ERROR) << s;
          return butil::Status(status.error_code(), s);
        }
      }

      // try get region epoch again
      status = GetRegionEpoch();
      if (!status.ok()) {
        std::string s = group_debug_info_ + " " + Utils::FormatStatusError(status);
        DINGO_LOG(ERROR) << s;
        return butil::Status(status.error_code(), s);
      }

      // try next loop
    }

  }  // if (backup_data_file_value_sst_meta_group_)

  return butil::Status::OK();
}

}  // namespace br
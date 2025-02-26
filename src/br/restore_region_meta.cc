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

#include "br/restore_region_meta.h"

#include <cstdint>
#include <memory>
#include <string>

#include "br/helper.h"
#include "br/parameter.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"

namespace br {

RestoreRegionMeta::RestoreRegionMeta(ServerInteractionPtr coordinator_interaction,
                                     std::shared_ptr<dingodb::pb::common::Region> region, int64_t replica_num,
                                     const std::string& backup_meta_region_name, int64_t create_region_timeout_s)
    : coordinator_interaction_(coordinator_interaction),
      region_(region),
      replica_num_(replica_num),
      backup_meta_region_name_(backup_meta_region_name),
      create_region_timeout_s_(create_region_timeout_s) {}

RestoreRegionMeta::~RestoreRegionMeta() = default;

std::shared_ptr<RestoreRegionMeta> RestoreRegionMeta::GetSelf() { return shared_from_this(); }

butil::Status RestoreRegionMeta::Init() {
  butil::Status status;

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail) << region_->DebugString();

  region_debug_info_ =
      fmt::format("{} : {}({}):", backup_meta_region_name_, region_->definition().name(), region_->id());

  return butil::Status::OK();
}

butil::Status RestoreRegionMeta::Run() { return CreateRegionToCoordinator(); }

butil::Status RestoreRegionMeta::Finish() { return butil::Status::OK(); }

butil::Status RestoreRegionMeta::CreateRegionToCoordinator() {
  butil::Status status;

  dingodb::pb::coordinator::CreateRegionRequest request;
  dingodb::pb::coordinator::CreateRegionResponse response;

  request.mutable_request_info()->set_request_id(br::Helper::GetRandInt());
  request.set_region_name(region_->definition().name());
  // ignore resource_tag
  request.set_replica_num(replica_num_);
  request.mutable_range()->CopyFrom(region_->definition().range());
  request.set_raw_engine(region_->definition().raw_engine());
  request.set_store_engine(region_->definition().store_engine());
  request.set_region_id(region_->id());
  request.set_use_region_name_direct(true);

  request.set_schema_id(region_->definition().schema_id());
  request.set_table_id(region_->definition().table_id());
  request.set_index_id(region_->definition().index_id());
  request.set_part_id(region_->definition().part_id());
  request.set_tenant_id(region_->definition().tenant_id());

  // ignore store_ids
  // ignore split_from_region_id
  request.set_region_type(region_->region_type());
  if (region_->definition().has_index_parameter() &&
      region_->definition().index_parameter().index_type() != dingodb::pb::common::IndexType ::INDEX_TYPE_NONE) {
    request.mutable_index_parameter()->CopyFrom(region_->definition().index_parameter());
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << request.DebugString();

  status = coordinator_interaction_->SendRequest("CoordinatorService", "CreateRegion", request, response,
                                                 create_region_timeout_s_ * 1000);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (response.error().errcode() != dingodb::pb::error::OK) {
    DINGO_LOG(ERROR) << region_debug_info_ << " " << response.error().errmsg();
    return butil::Status(response.error().errcode(), region_debug_info_ + " " + response.error().errmsg());
  }

  // double check
  if (response.region_id() != region_->id()) {
    std::string s =
        fmt::format("response region id : {} not match request region id : {}", response.region_id(), region_->id());
    DINGO_LOG(ERROR) << region_debug_info_ + " " + s;
    return butil::Status(dingodb::pb::error::ERESTORE_REGION_ID_NOT_MATCH, region_debug_info_ + " " + s);
  }

  DINGO_LOG_IF(INFO, FLAGS_br_log_switch_restore_detail_detail) << response.DebugString();

  return butil::Status::OK();
}

}  // namespace br
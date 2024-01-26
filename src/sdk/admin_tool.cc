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

#include "sdk/admin_tool.h"

#include "common/logging.h"
#include "glog/logging.h"
#include "proto/coordinator.pb.h"
#include "sdk/common/common.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

AdminTool::AdminTool(std::shared_ptr<CoordinatorProxy> coordinator_proxy) : coordinator_proxy_(coordinator_proxy) {}

Status AdminTool::GetCurrentTsoTimeStamp(pb::meta::TsoTimestamp& timestamp) {
  pb::meta::TsoRequest request;
  pb::meta::TsoResponse response;

  request.set_op_type(pb::meta::TsoOpType::OP_GEN_TSO);
  request.set_count(1);

  auto status = coordinator_proxy_->TsoService(request, response);
  if (!status.IsOK()) {
    DINGO_LOG(WARNING) << "Fail tsoService request fail, status:" << status.ToString()
                       << ", response:" << response.DebugString();
  } else {
    CHECK(response.has_start_timestamp());
    timestamp = response.start_timestamp();
    DINGO_LOG(DEBUG) << "tso timestamp: " << timestamp.DebugString();
  }

  return status;
}

Status AdminTool::GetCurrentTimeStamp(int64_t& timestamp) {
  pb::meta::TsoTimestamp tso;
  DINGO_RETURN_NOT_OK(GetCurrentTsoTimeStamp(tso));
  timestamp = Tso2Timestamp(tso);
  return Status::OK();
}

Status AdminTool::IsCreateRegionInProgress(int64_t region_id, bool& out_create_in_progress) {
  pb::coordinator::QueryRegionRequest req;
  req.set_region_id(region_id);

  pb::coordinator::QueryRegionResponse resp;
  DINGO_RETURN_NOT_OK(coordinator_proxy_->QueryRegion(req, resp));

  CHECK(resp.has_region()) << "query region internal error, req:" << req.DebugString()
                           << ", resp:" << resp.DebugString();
  CHECK_EQ(resp.region().id(), region_id);
  out_create_in_progress = (resp.region().state() == pb::common::REGION_NEW);

  return Status::OK();
}

Status AdminTool::DropRegion(int64_t region_id) {
  pb::coordinator::DropRegionRequest req;
  req.set_region_id(region_id);
  pb::coordinator::DropRegionResponse resp;
  Status ret = coordinator_proxy_->DropRegion(req, resp);

  if (ret.IsNotFound()) {
    ret = Status::OK();
  }

  return ret;
}

Status AdminTool::CreateTableIds(int64_t count, std::vector<int64_t>& out_table_ids) {
  CHECK(count > 0) << "count must greater 0";
  pb::meta::CreateTableIdsRequest request;
  pb::meta::CreateTableIdsResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_count(count);
  DINGO_RETURN_NOT_OK(coordinator_proxy_->CreateTableIds(request, response));
  CHECK_EQ(response.table_ids_size(), count);

  for (const auto& id : response.table_ids()) {
    out_table_ids.push_back(id.entity_id());
  }

  return Status::OK();
}

Status AdminTool::DropIndex(int64_t index_id) {
  if (index_id <= 0) {
    return Status::InvalidArgument("index_id must greater than 0");
  }

  pb::meta::DropIndexRequest request;
  pb::meta::DropIndexResponse response;

  auto* index_pb = request.mutable_index_id();
  index_pb->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_pb->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  index_pb->set_entity_id(index_id);

  return coordinator_proxy_->DropIndex(request, response);
}

}  // namespace sdk
}  // namespace dingodb
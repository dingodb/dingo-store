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

#include "glog/logging.h"
#include "proto/coordinator.pb.h"
#include "sdk/common.h"
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
    VLOG(1) << "tso timestamp: " << timestamp.DebugString();
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

}  // namespace sdk
}  // namespace dingodb
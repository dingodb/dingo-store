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

#include "server/push_service.h"

#include <cstdint>

#include "butil/status.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/push.pb.h"
#include "server/server.h"
#include "server/service_helper.h"
#include "store/heartbeat.h"
#include "store/region_controller.h"

namespace dingodb {

PushServiceImpl::PushServiceImpl() = default;

void PushServiceImpl::PushHeartbeat(google::protobuf::RpcController* controller,
                                    const dingodb::pb::push::PushHeartbeatRequest* request,
                                    dingodb::pb::push::PushHeartbeatResponse* response,
                                    google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard const done_guard(svr_done);

  // call HandleStoreHeartbeatResponse
  const auto& heartbeat_response = request->heartbeat_response();
  auto store_meta = Server::GetInstance().GetStoreMetaManager();
  HeartbeatTask::HandleStoreHeartbeatResponse(store_meta, heartbeat_response);
}

void PushServiceImpl::PushStoreOperation(google::protobuf::RpcController* controller,
                                         const dingodb::pb::push::PushStoreOperationRequest* request,
                                         dingodb::pb::push::PushStoreOperationResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard const done_guard(svr_done);

  if (request->store_operation().id() != Server::GetInstance().Id()) {
    DINGO_LOG(ERROR) << fmt::format("[push.store] store id not match, req/local store id({} / {})",
                                    request->store_operation().id(), Server::GetInstance().Id());
    return;
  }

  auto error_func = [response](int64_t command_id, ::dingodb::pb::coordinator::RegionCmdType region_cmd_type,
                               butil::Status status) {
    auto* result = response->add_region_cmd_results();
    result->set_region_cmd_id(command_id);
    result->set_region_cmd_type(region_cmd_type);

    if (!status.ok()) {
      auto* mut_err = result->mutable_error();
      mut_err->set_errcode(static_cast<pb::error::Errno>(status.error_code()));
      if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
        mut_err->set_errmsg("Not leader, please redirect leader.");
        ServiceHelper::RedirectLeader(status.error_str(), result);
      } else {
        mut_err->set_errmsg(status.error_str());
      }
    }
  };

  auto region_controller = Server::GetInstance().GetRegionController();
  for (const auto& command : request->store_operation().region_cmds()) {
    butil::Status status;
    auto store_meta_manager = Server::GetInstance().GetStoreMetaManager();

    auto validate_func = RegionController::GetValidater(command.region_cmd_type());
    status = (validate_func != nullptr) ? validate_func(command)
                                        : butil::Status(pb::error::EINTERNAL, "Unknown region command");
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[push.store] validate failed, error: {} command: {}",
                                      Helper::PrintStatus(status), command.ShortDebugString());
      error_func(command.id(), command.region_cmd_type(), status);
      continue;
    }

    std::shared_ptr<Context> ctx = std::make_shared<Context>();
    status =
        region_controller->DispatchRegionControlCommand(ctx, std::make_shared<pb::coordinator::RegionCmd>(command));
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[push.store] dispatch failed, error: {} command: {}", status.error_str(),
                                      command.ShortDebugString());
    }
    // coordinator need to get all region_cmd results, so add all results to response here
    error_func(command.id(), command.region_cmd_type(), status);
  }

  if (!response->region_cmd_results().empty()) {
    for (const auto& cmd_result : response->region_cmd_results()) {
      if (cmd_result.error().errcode() != 0) {
        *(response->mutable_error()) = cmd_result.error();
        break;
      }
    }
    DINGO_LOG(INFO) << fmt::format("[push.store] response: {} request: {}", response->ShortDebugString(),
                                   request->ShortDebugString());
  }
}

}  // namespace dingodb

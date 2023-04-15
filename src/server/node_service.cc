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

#include "server/node_service.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "common/logging.h"
#include "coordinator/coordinator_closure.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/node.pb.h"

namespace dingodb {
using pb::error::Errno;
using pb::node::LogDetail;
using pb::node::LogLevel;

void NodeServiceImpl::SetServer(dingodb::Server* server) { this->server_ = server; }

void NodeServiceImpl::GetNodeInfo(google::protobuf::RpcController* /*controller*/,
                                  const pb::node::GetNodeInfoRequest* request, pb::node::GetNodeInfoResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);

  if (request->cluster_id() < 0) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EILLEGAL_PARAMTETERS);
  }

  auto* node_info = response->mutable_node_info();

  node_info->set_id(server_->Id());
  node_info->set_role(server_->GetRole());

  // parse server location
  auto* server_location = node_info->mutable_server_location();
  auto* server_host = server_location->mutable_host();
  auto host_str = butil::ip2str(server_->ServerEndpoint().ip);
  server_host->assign(std::string(host_str.c_str()));
  server_location->set_port(server_->ServerEndpoint().port);

  // parse raft location
  auto* raft_location = node_info->mutable_raft_location();
  auto* raft_host = raft_location->mutable_host();
  auto raft_host_str = butil::ip2str(server_->RaftEndpoint().ip);
  raft_host->assign(std::string(host_str.c_str()));
  raft_location->set_port(server_->RaftEndpoint().port);
}

void NodeServiceImpl::GetLogLevel(google::protobuf::RpcController* controller,
                                  const pb::node::GetLogLevelRequest* request, pb::node::GetLogLevelResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);

  DINGO_LOG(INFO) << "GetLogLevel receive Request:" << request->DebugString();

  auto* log_detail = response->mutable_log_detail();
  log_detail->set_log_buf_secs(DingoLogger::GetLogBuffSecs());
  log_detail->set_max_log_size(DingoLogger::GetMaxLogSize());
  log_detail->set_stop_logging_if_full_disk(DingoLogger::GetStoppingWhenDiskFull());

  int const min_log_level = DingoLogger::GetMinLogLevel();
  int const min_verbose_level = DingoLogger::GetMinVerboseLevel();

  if (min_log_level > pb::node::FATAL) {
    DINGO_LOG(ERROR) << "Invalid Log Level:" << min_log_level;
    response->mutable_error()->set_errcode(::dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
    return;
  }

  if (min_log_level == 0 && min_verbose_level > 1) {
    response->set_log_level(static_cast<LogLevel>(0));
  } else {
    response->set_log_level(static_cast<LogLevel>(min_log_level + 1));
  }
}

void NodeServiceImpl::ChangeLogLevel(google::protobuf::RpcController* /* controller */,
                                     const pb::node::ChangeLogLevelRequest* request,
                                     pb::node::ChangeLogLevelResponse* /*response*/, google::protobuf::Closure* done) {
  brpc::ClosureGuard const done_guard(done);

  DINGO_LOG(INFO) << "ChangeLogLevel=>Receive Request:" << request->DebugString();

  const LogLevel log_level = request->log_level();
  if (log_level == pb::node::DEBUG) {
    DingoLogger::SetMinLogLevel(0);
    DingoLogger::SetMinVerboseLevel(kGlobalValueOfDebug);
  } else {
    DingoLogger::SetMinLogLevel(static_cast<int>(log_level) - 1);
    DingoLogger::SetMinVerboseLevel(1);
  }

  const LogDetail& log_detail = request->log_detail();
  DingoLogger::SetLogBuffSecs(log_detail.log_buf_secs());
  DingoLogger::SetMaxLogSize(log_detail.max_log_size());
  DingoLogger::SetStoppingWhenDiskFull(log_detail.stop_logging_if_full_disk());
}

}  // namespace dingodb

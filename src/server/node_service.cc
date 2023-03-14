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
#include "coordinator/coordinator_closure.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/node.pb.h"

namespace dingodb {

void NodeServiceImpl::SetServer(dingodb::Server* server) { this->server_ = server; }

void NodeServiceImpl::GetNodeInfo(google::protobuf::RpcController* /*controller*/,
                                  const pb::node::GetNodeInfoRequest* request, pb::node::GetNodeInfoResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);

  if (request->cluster_id() < 0) {
    auto* error = response->mutable_error();
    error->set_errcode(::dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);
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

}  // namespace dingodb
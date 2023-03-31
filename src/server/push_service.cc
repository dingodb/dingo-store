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

#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/push.pb.h"
#include "server/server.h"
#include "store/heartbeat.h"

namespace dingodb {

PushServiceImpl::PushServiceImpl() = default;

void PushServiceImpl::PushHeartbeat(google::protobuf::RpcController* controller,
                                    const dingodb::pb::push::PushHeartbeatRequest* request,
                                    dingodb::pb::push::PushHeartbeatResponse* /*response*/,
                                    google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard const done_guard(done);
  DINGO_LOG(INFO) << "PushHeartbeat request: " << dingodb::Helper::MessageToJsonString(*request);

  // call HandleStoreHeartbeatResponse
  auto heartbeat_response = request->heartbeat_response();
  auto store_meta = Server::GetInstance()->GetStoreMetaManager();
  Heartbeat::HandleStoreHeartbeatResponse(store_meta, heartbeat_response);
}

}  // namespace dingodb

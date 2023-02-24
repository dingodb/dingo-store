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

#include "server/coordinator_service.h"

#include "proto/common.pb.h"

namespace dingodb {

void CoordinatorServiceImpl::Hello(google::protobuf::RpcController *controller,
                              const pb::coordinator::HelloRequest *request,
                              pb::coordinator::HelloResponse *response,
                              google::protobuf::Closure *done) {
  brpc::ClosureGuard done_guard(done);
  LOG(INFO) << "Hello request: " << request->hello();

  response->set_status(static_cast<pb::common::CoordinatorStatus>(0));
  response->set_status_detail("OK");
}

} // namespace dingodb

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

#ifndef DINGODB_PUSH_SERVICE_H_
#define DINGODB_PUSH_SERVICE_H_

#include "proto/push.pb.h"

namespace dingodb {

class PushServiceImpl : public pb::push::PushService {
 public:
  PushServiceImpl();
  void PushHeartbeat(google::protobuf::RpcController* controller,
                     const dingodb::pb::push::PushHeartbeatRequest* request,
                     dingodb::pb::push::PushHeartbeatResponse* /*response*/, google::protobuf::Closure* done) override;
  void PushStoreOperation(google::protobuf::RpcController* controller,
                          const dingodb::pb::push::PushStoreOperationRequest* request,
                          dingodb::pb::push::PushStoreOperationResponse* /*response*/,
                          google::protobuf::Closure* done) override;
};

}  // namespace dingodb

#endif  // DINGODB_PUSH_SERVICE_H_

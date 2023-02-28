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

#ifndef DINGODB_SERVER_HEARTBEAT_H_
#define DINGODB_SERVER_HEARTBEAT_H_

#include "brpc/channel.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "server/server.h"

namespace dingodb {

class Heartbeat {
 public:
  Heartbeat();
  ~Heartbeat();

  static void SendStoreHeartbeat(void* arg);

  static void HandleStoreHeartbeatResponse(
      std::shared_ptr<StoreMetaManager> store_meta,
      const pb::coordinator::StoreHeartbeatResponse& response);
};

}  // namespace dingodb

#endif  // DINGODB_SERVER_HEARTBEAT_H_
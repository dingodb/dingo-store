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

#ifndef DINGODB_SDK_RPC_CLIENT_H_
#define DINGODB_SDK_RPC_CLIENT_H_

#include "rpc.h"
#include "sdk/common/param_config.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

struct RpcClientOptions {
  int32_t connect_timeout_ms;
  int32_t timeout_ms;
  int max_retry;

  RpcClientOptions()
      : connect_timeout_ms(FLAGS_rpc_channel_timeout_ms),
        timeout_ms(FLAGS_rpc_channel_connect_timeout_ms),
        max_retry(FLAGS_rpc_max_retry) {}
};

class RpcClient {
 public:
  RpcClient(const RpcClient &) = delete;
  const RpcClient &operator=(const RpcClient &) = delete;

  RpcClient(const RpcClientOptions &options) : m_options(options) {}

  virtual ~RpcClient() = default;

  virtual void Open() {}

  virtual void SendRpc(Rpc &rpc, RpcCallback cb) = 0;

 protected:
  RpcClientOptions m_options;
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_RPC_CLIENT_H_
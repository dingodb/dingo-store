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

#include "sdk/rpc/brpc/brpc_rpc_client.h"
#include <memory>

#include "glog/logging.h"
#include "sdk/rpc/brpc/unary_rpc.h"

namespace dingodb {
namespace sdk {

void BrpcRpcClient::SendRpc(Rpc& rpc, RpcCallback cb) {
  auto endpoint = rpc.GetEndPoint();
  CHECK(endpoint.IsValid()) << "rpc endpoint not valid: " << endpoint.ToString();

  std::shared_ptr<brpc::Channel> channel = std::make_shared<brpc::Channel>();
  {
    std::lock_guard<std::mutex> guard(lock_);
    auto ch = channel_map_.find(endpoint);
    if (ch == channel_map_.end()) {
      brpc::ChannelOptions options;
      options.timeout_ms = m_options.timeout_ms;
      options.connect_timeout_ms = m_options.connect_timeout_ms;
      options.max_retry = m_options.max_retry;

      int ret = channel->Init(endpoint.Host().c_str(), endpoint.Port(), &options);

      CHECK_EQ(ret, 0) << "Fail init channel endpoint:" << endpoint.ToString();
      channel_map_.insert(std::make_pair(endpoint, channel));
    } else {
      channel = CHECK_NOTNULL(ch->second);
    }
  }

  CHECK_NOTNULL(channel.get());
  auto ctx = std::make_unique<BrpcContext>();
  ctx->cb = std::move(cb);
  ctx->channel = channel;
  rpc.Call(ctx.release());
}

}  // namespace sdk
};  // namespace dingodb
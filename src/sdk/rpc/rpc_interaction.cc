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

#include "sdk/rpc/rpc_interaction.h"

#include <memory>
#include <mutex>
#include <utility>

#include "brpc/channel.h"
#include "butil/endpoint.h"
#include "glog/logging.h"

namespace dingodb {
namespace sdk {

void RpcInteraction::SendRpc(Rpc& rpc, RpcCallback cb) {
  auto endpoint = rpc.GetEndPoint();
  CHECK(endpoint.ip != butil::IP_ANY) << "rpc endpoint not set";
  CHECK(endpoint.port != 0) << "rpc endpoint port should not 0";

  std::shared_ptr<brpc::Channel> channel = std::make_shared<brpc::Channel>();
  {
    std::lock_guard<std::mutex> guard(lock_);
    auto ch = channel_map_.find(endpoint);
    if (ch == channel_map_.end()) {
      int ret = channel->Init(endpoint, &options_);
      CHECK_EQ(ret, 0) << "Fail init channel endpoint:" << butil::endpoint2str(endpoint).c_str();
      channel_map_.insert(std::make_pair(endpoint, channel));
    } else {
      channel = CHECK_NOTNULL(ch->second);
    }
  }

  CHECK_NOTNULL(channel.get());
  rpc.Call(channel.get(), std::move(cb));
}

}  // namespace sdk
};  // namespace dingodb
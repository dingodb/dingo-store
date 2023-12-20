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

#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>

#include "brpc/channel.h"
#include "butil/endpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

Status RpcInteraction::SendRpc(Rpc& rpc, google::protobuf::Closure* done) {
  auto endpoint = rpc.GetEndPoint();
  CHECK(endpoint.ip != butil::IP_ANY) << "rpc endpoint not set";
  CHECK(endpoint.port != 0) << "rpc endpoint port should not 0";

  std::shared_ptr<brpc::Channel> channel = nullptr;
  {
    std::lock_guard<std::mutex> guard(lock_);
    auto ch = channel_map_.find(endpoint);
    if (ch == channel_map_.end()) {
      Status init = InitChannel(endpoint, channel);
      if (!init.IsOK()) {
        return init;
      }

      channel_map_.insert(std::make_pair(endpoint, channel));
    } else {
      channel = CHECK_NOTNULL(ch->second);
    }
  }

  CHECK_NOTNULL(channel.get());
  rpc.Call(channel.get(), done);

  return Status::OK();
}

Status RpcInteraction::InitChannel(const butil::EndPoint& server_addr_and_port, std::shared_ptr<brpc::Channel>& channel) {
  std::shared_ptr<brpc::Channel> tmp = std::make_shared<brpc::Channel>();
  int ret = tmp->Init(server_addr_and_port, &options_);
  // TODO: maybe we should check ret is always 0
  if (ret != 0) {
    std::string msg = fmt::format("channel Init fail, please check endpoint_addr: {} and channel options, ret:{}",
                                  butil::endpoint2str(server_addr_and_port).c_str(), ret);
    return Status::Uninitialized(msg);
  }

  channel = tmp;

  return Status::OK();
}

}  // namespace sdk
};  // namespace dingodb
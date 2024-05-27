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

#include "sdk/rpc/grpc/grpc_rpc_client.h"

#include <mutex>

#include "glog/logging.h"
#include "grpcpp/grpcpp.h"
#include "sdk/common/param_config.h"
#include "sdk/rpc/grpc/unary_rpc.h"
#include "sdk/rpc/rpc.h"
#include "sdk/utils/net_util.h"

namespace dingodb {
namespace sdk {

void GrpcRpcClient::Open() {
  std::unique_lock<std::mutex> lg(lock_);
  if (!opened_) {
    for (int i = 0; i < FLAGS_grpc_poll_thread_num; ++i) {
      auto cq = std::make_unique<grpc::CompletionQueue>();
      workers_.emplace_back(
          [&](grpc::CompletionQueue* cq) -> void {
            void* tag;
            bool ok;
            while (cq->Next(&tag, &ok)) {
              CHECK(ok) << "expect ok is always true";
              auto* rpc = static_cast<Rpc*>(tag);
              rpc->OnRpcDone();
            }
          },
          cq.get());

      cqs_.emplace_back(std::move(cq));
    }

    opened_ = true;
  }
}

void GrpcRpcClient::Close() {
  std::unique_lock<std::mutex> lg(lock_);
  if (opened_) {
    for (auto& cq : cqs_) {
      cq->Shutdown();
    }

    for (auto& worker : workers_) {
      worker.join();
    }

    opened_ = false;
  }
}

void GrpcRpcClient::SendRpc(Rpc& rpc, RpcCallback cb) {
  CHECK(opened_) << "grpc rpc client not opened";
  const auto& endpoint = rpc.GetEndPoint();
  CHECK(endpoint.IsValid()) << "rpc endpoint not valid: " << endpoint.ToString();

  auto ctx = std::make_unique<GrpcContext>();

  std::shared_ptr<grpc::Channel> channel;
  {
    std::lock_guard<std::mutex> guard(lock_);
    auto ch = channel_map_.find(endpoint);
    if (ch == channel_map_.end()) {
      // TODO: maybe use custome channel
      channel = grpc::CreateChannel(endpoint.StringAddr(), grpc::InsecureChannelCredentials());
      channel_map_.insert(std::make_pair(endpoint, channel));
    } else {
      channel = CHECK_NOTNULL(ch->second);
    }

    ctx->cq = cqs_[next_cq_index_ % cqs_.size()].get();
    next_cq_index_++;
  }

  CHECK_NOTNULL(channel.get());
  ctx->channel = std::move(channel);
  ctx->cb = std::move(cb);

  rpc.Call(ctx.release());
}

}  // namespace sdk
}  // namespace dingodb
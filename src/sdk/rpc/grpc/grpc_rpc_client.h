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

#ifndef DINGODB_SDK_GRPC_RPC_CLIENT_H_
#define DINGODB_SDK_GRPC_RPC_CLIENT_H_

#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "grpcpp/channel.h"
#include "grpcpp/completion_queue.h"
#include "sdk/rpc/rpc_client.h"

namespace dingodb {
namespace sdk {

class GrpcRpcClient : public RpcClient {
 public:
  GrpcRpcClient(const RpcClientOptions &options) : RpcClient(options) {}

  ~GrpcRpcClient() override { Close(); }

  void Open() override;

  void SendRpc(Rpc &rpc, RpcCallback cb) override;

 private:
  void Close();

  std::mutex lock_;
  std::vector<std::unique_ptr<grpc::CompletionQueue>> cqs_;
  std::vector<std::thread> workers_;
  std::map<EndPoint, std::shared_ptr<grpc::Channel>> channel_map_;
  bool opened_{false};
  int64_t next_cq_index_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_GRPC_RPC_CLIENT_H_
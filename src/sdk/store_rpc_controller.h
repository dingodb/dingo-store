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

#ifndef DINGODB_SDK_STORE_RPC_CONTROLLER_H_
#define DINGODB_SDK_STORE_RPC_CONTROLLER_H_

#include <memory>

#include "butil/endpoint.h"
#include "proto/error.pb.h"
#include "sdk/client_stub.h"

namespace dingodb {
namespace sdk {
class DingoStub;
// TODO: support backoff strategy
class StoreRpcController {
 public:
  explicit StoreRpcController(const ClientStub& stub, Rpc& rpc, std::shared_ptr<Region> region);

  virtual ~StoreRpcController();

  // NOTE: only support sync
  // TODO: support async
  Status Call(google::protobuf::Closure* done = nullptr);

  void ResetRegion(std::shared_ptr<Region> region);

 protected:
  virtual bool IsRpcFailed(const brpc::Controller* cntl);

 private:
  bool PickNextLeader(butil::EndPoint& leader);

  Status PrepareAndSendRpc(google::protobuf::Closure* done = nullptr);

  bool Failed(const butil::EndPoint& addr);

  void SetFailed(butil::EndPoint addr);

  std::shared_ptr<Region> ProcessStoreRegionInfo(const dingodb::pb::error::StoreRegionInfo& store_region_info);

  bool NeedRetry() const;

  static const pb::error::Error& GetResponseError(Rpc& rpc);

  const ClientStub& stub_;
  Rpc& rpc_;
  std::shared_ptr<Region> region_;

  int rpc_retry_times_;

  std::set<butil::EndPoint> failed_addrs_;
  int next_replica_index_;
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_STORE_RPC_CONTROLLER_H_
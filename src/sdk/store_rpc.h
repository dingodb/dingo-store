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

#ifndef DINGODB_SDK_STORE_RPC_H_
#define DINGODB_SDK_STORE_RPC_H_

#include "proto/store.pb.h"
#include "sdk/rpc.h"

namespace dingodb {
namespace sdk {

using pb::store::KvGetRequest;
using pb::store::KvGetResponse;
using pb::store::KvPutRequest;
using pb::store::KvPutResponse;
using pb::store::StoreService;
using pb::store::StoreService_Stub;

class KvGetRpc final : public ClientRpc<KvGetRequest, KvGetResponse, StoreService, StoreService_Stub> {
 public:
  KvGetRpc(const KvGetRpc &) = delete;
  const KvGetRpc &operator=(const KvGetRpc &) = delete;

  explicit KvGetRpc();

  explicit KvGetRpc(const std::string &cmd);

  ~KvGetRpc() override;

  std::string Method() const override { return ConstMethod(); }

  void Send(StoreService_Stub &stub, google::protobuf::Closure *done) override;

  static std::string ConstMethod();
};

class KvPutRpc final : public ClientRpc<KvPutRequest, KvPutResponse, StoreService, StoreService_Stub> {
 public:
  KvPutRpc(const KvPutRpc &) = delete;
  const KvPutRpc &operator=(const KvPutRpc &) = delete;

  explicit KvPutRpc();

  explicit KvPutRpc(const std::string &cmd);

  ~KvPutRpc() override;

  std::string Method() const override { return ConstMethod(); }

  void Send(StoreService_Stub &stub, google::protobuf::Closure *done) override;

  static std::string ConstMethod();
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_STORE_RPC_H_
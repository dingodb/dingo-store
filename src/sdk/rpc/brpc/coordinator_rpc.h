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

#ifndef DINGODB_SDK_BRPC_COORDINATOR_RPC_H_
#define DINGODB_SDK_BRPC_COORDINATOR_RPC_H_

#include "proto/coordinator.pb.h"
#include "proto/meta.pb.h"
#include "sdk/rpc/brpc/unary_rpc.h"

namespace dingodb {
namespace sdk {

#define DECLARE_COORDINATOR_RPC(METHOD) DECLARE_UNARY_RPC(pb::coordinator, CoordinatorService, METHOD)

#define DECLARE_META_RPC(METHOD) DECLARE_UNARY_RPC(pb::meta, MetaService, METHOD)

namespace coordinator {
DECLARE_COORDINATOR_RPC(Hello);
};

DECLARE_COORDINATOR_RPC(QueryRegion);
DECLARE_COORDINATOR_RPC(CreateRegion);
DECLARE_COORDINATOR_RPC(DropRegion);
DECLARE_COORDINATOR_RPC(ScanRegions);

DECLARE_META_RPC(GenerateAutoIncrement);
DECLARE_META_RPC(CreateIndex);
DECLARE_META_RPC(GetIndexByName);
DECLARE_META_RPC(DropIndex);
DECLARE_META_RPC(CreateTableIds);
DECLARE_META_RPC(GetIndex);

class TsoServiceRpc final
    : public UnaryRpc<pb::meta::TsoRequest, pb::meta::TsoResponse, pb::meta::MetaService, pb::meta::MetaService_Stub> {
 public:
  TsoServiceRpc(const TsoServiceRpc&) = delete;
  TsoServiceRpc& operator=(const TsoServiceRpc&) = delete;
  explicit TsoServiceRpc();
  explicit TsoServiceRpc(const std ::string& cmd);
  ~TsoServiceRpc() override;
  std ::string Method() const override { return ConstMethod(); }
  void Send(pb::meta::MetaService_Stub& stub, google::protobuf::Closure* done) override;
  static std ::string ConstMethod();
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_BRPC_COORDINATOR_RPC_H_
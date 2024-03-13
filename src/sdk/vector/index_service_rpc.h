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

#ifndef DINGODB_SDK_INDEX_SERVICE_RPC_H_
#define DINGODB_SDK_INDEX_SERVICE_RPC_H_

#include "proto/index.pb.h"
#include "sdk/rpc/rpc.h"

namespace dingodb {
namespace sdk {

using pb::index::IndexService;
using pb::index::IndexService_Stub;

#define DECLARE_INDEX_SERVICE_RPC(METHOD)                                                                            \
  class METHOD##Rpc final                                                                                            \
      : public ClientRpc<pb::index::METHOD##Request, pb::index::METHOD##Response, IndexService, IndexService_Stub> { \
   public:                                                                                                           \
    METHOD##Rpc(const METHOD##Rpc &) = delete;                                                                       \
    METHOD##Rpc &operator=(const METHOD##Rpc &) = delete;                                                            \
    explicit METHOD##Rpc();                                                                                          \
    explicit METHOD##Rpc(const std::string &cmd);                                                                    \
    ~METHOD##Rpc() override;                                                                                         \
    std::string Method() const override { return ConstMethod(); }                                                    \
    void Send(IndexService_Stub &stub, google::protobuf::Closure *done) override;                                    \
    static std::string ConstMethod();                                                                                \
  };

DECLARE_INDEX_SERVICE_RPC(Hello);
DECLARE_INDEX_SERVICE_RPC(VectorAdd);
DECLARE_INDEX_SERVICE_RPC(VectorSearch);
DECLARE_INDEX_SERVICE_RPC(VectorDelete);
DECLARE_INDEX_SERVICE_RPC(VectorBatchQuery);
DECLARE_INDEX_SERVICE_RPC(VectorGetBorderId);
DECLARE_INDEX_SERVICE_RPC(VectorScanQuery);
DECLARE_INDEX_SERVICE_RPC(VectorGetRegionMetrics);
DECLARE_INDEX_SERVICE_RPC(VectorCount);

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_INDEX_SERVICE_RPC_H_
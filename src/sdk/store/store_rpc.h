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
#include "sdk/rpc/rpc.h"

namespace dingodb {
namespace sdk {

using pb::store::StoreService;
using pb::store::StoreService_Stub;

#define DECLARE_STORE_RPC(METHOD) \
class METHOD##Rpc final : public ClientRpc<pb::store::METHOD##Request, pb::store::METHOD##Response, StoreService, StoreService_Stub> { \
public: \
  METHOD##Rpc(const METHOD##Rpc &) = delete;\
  METHOD##Rpc &operator=(const METHOD##Rpc &) = delete;\
  explicit METHOD##Rpc(); \
  explicit METHOD##Rpc(const std::string &cmd); \
  ~METHOD##Rpc() override; \
  std::string Method() const override { return ConstMethod(); } \
  void Send(StoreService_Stub &stub, google::protobuf::Closure *done) override;\
  static std::string ConstMethod(); \
};

DECLARE_STORE_RPC(KvGet);
DECLARE_STORE_RPC(KvBatchGet);
DECLARE_STORE_RPC(KvPut);
DECLARE_STORE_RPC(KvBatchPut);
DECLARE_STORE_RPC(KvPutIfAbsent);
DECLARE_STORE_RPC(KvBatchPutIfAbsent);
DECLARE_STORE_RPC(KvBatchDelete);
DECLARE_STORE_RPC(KvDeleteRange);
DECLARE_STORE_RPC(KvCompareAndSet);
DECLARE_STORE_RPC(KvBatchCompareAndSet);

DECLARE_STORE_RPC(KvScanBegin);
DECLARE_STORE_RPC(KvScanContinue);
DECLARE_STORE_RPC(KvScanRelease);

DECLARE_STORE_RPC(TxnGet);
DECLARE_STORE_RPC(TxnBatchGet);
DECLARE_STORE_RPC(TxnPrewrite);
DECLARE_STORE_RPC(TxnCommit);
DECLARE_STORE_RPC(TxnBatchRollback);
DECLARE_STORE_RPC(TxnScan);

DECLARE_STORE_RPC(TxnHeartBeat);
DECLARE_STORE_RPC(TxnCheckTxnStatus);
DECLARE_STORE_RPC(TxnResolveLock);

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_STORE_RPC_H_
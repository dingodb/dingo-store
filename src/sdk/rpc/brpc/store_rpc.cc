//
// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "sdk/rpc/brpc/store_rpc.h"

namespace dingodb {
namespace sdk {

#define DEFINE_STORE_RPC(METHOD) DEFINE_UNAEY_RPC(pb::store, StoreService, METHOD)

DEFINE_STORE_RPC(KvGet);
DEFINE_STORE_RPC(KvBatchGet);
DEFINE_STORE_RPC(KvPut);
DEFINE_STORE_RPC(KvBatchPut);
DEFINE_STORE_RPC(KvPutIfAbsent);
DEFINE_STORE_RPC(KvBatchPutIfAbsent);
DEFINE_STORE_RPC(KvBatchDelete);
DEFINE_STORE_RPC(KvDeleteRange);
DEFINE_STORE_RPC(KvCompareAndSet);
DEFINE_STORE_RPC(KvBatchCompareAndSet);

DEFINE_STORE_RPC(KvScanBegin);
DEFINE_STORE_RPC(KvScanContinue);
DEFINE_STORE_RPC(KvScanRelease);

DEFINE_STORE_RPC(TxnGet);
DEFINE_STORE_RPC(TxnBatchGet);
DEFINE_STORE_RPC(TxnPrewrite);
DEFINE_STORE_RPC(TxnCommit);
DEFINE_STORE_RPC(TxnBatchRollback);
DEFINE_STORE_RPC(TxnScan);

DEFINE_STORE_RPC(TxnHeartBeat);
DEFINE_STORE_RPC(TxnCheckTxnStatus);
DEFINE_STORE_RPC(TxnResolveLock);

}  // namespace sdk
}  // namespace dingodb
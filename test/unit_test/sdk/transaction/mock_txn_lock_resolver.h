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

#ifndef DINGODB_SDK_TEST_MOCK_TXN_RESOLVER_H_
#define DINGODB_SDK_TEST_MOCK_TXN_RESOLVER_H_

#include "sdk/client_stub.h"
#include "gmock/gmock.h"
#include "sdk/status.h"
#include "sdk/transaction/txn_lock_resolver.h"

namespace dingodb {
namespace sdk {

class MockTxnLockResolver final : public TxnLockResolver {
 public:
  explicit MockTxnLockResolver(const ClientStub& stub) : sdk::TxnLockResolver(stub){};

  ~MockTxnLockResolver() override = default;

  MOCK_METHOD(Status, ResolveLock, (const pb::store::LockInfo& lock_info, int64_t caller_start_ts), (override));
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TEST_MOCK_TXN_RESOLVER_H_
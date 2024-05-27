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

#ifndef DINGODB_SDK_TEST_MOCK_CLIENT_STUB_H_
#define DINGODB_SDK_TEST_MOCK_CLIENT_STUB_H_

#include "gmock/gmock.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/coordinator_rpc_controller.h"

namespace dingodb {
namespace sdk {
class MockClientStub final : public ClientStub {
 public:
  explicit MockClientStub() = default;

  ~MockClientStub() override = default;

  MOCK_METHOD(std::shared_ptr<CoordinatorRpcController>, GetCoordinatorRpcController, (), (const, override));
  MOCK_METHOD(std::shared_ptr<CoordinatorRpcController>, GetMetaRpcController, (), (const, override));
  MOCK_METHOD(std::shared_ptr<MetaCache>, GetMetaCache, (), (const, override));
  MOCK_METHOD(std::shared_ptr<RpcClient>, GetStoreRpcClient, (), (const, override));
  MOCK_METHOD(std::shared_ptr<RegionScannerFactory>, GetRawKvRegionScannerFactory, (), (const, override));
  MOCK_METHOD(std::shared_ptr<AdminTool>, GetAdminTool, (), (const, override));
  MOCK_METHOD(std::shared_ptr<TxnLockResolver>, GetTxnLockResolver, (), (const, override));
  MOCK_METHOD(std::shared_ptr<Actuator>, GetActuator, (), (const, override));
  MOCK_METHOD(std::shared_ptr<VectorIndexCache>, GetVectorIndexCache, (), (const, override));
  MOCK_METHOD(std::shared_ptr<AutoIncrementerManager>, GetAutoIncrementerManager, (), (const, override));

  // std::shared_ptr<AutoIncrementerManager>  auto_increment_manager_;
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TEST_MOCK_CLIENT_STUB_H_
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

#ifndef DINGODB_SDK_TEST_MOCK_STORE_RPC_CONTROLLER_H_
#define DINGODB_SDK_TEST_MOCK_STORE_RPC_CONTROLLER_H_

#include "sdk/client_stub.h"
#include "gmock/gmock.h"
#include "sdk/store/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

class MockStoreRpcController final : public StoreRpcController {
 public:
  MockStoreRpcController(const ClientStub& stub, Rpc& rpc, std::shared_ptr<Region> region)
      : StoreRpcController(stub, rpc, region) {
  }

  ~MockStoreRpcController() override = default;
};

}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_TEST_MOCK_STORE_RPC_CONTROLLER_H_
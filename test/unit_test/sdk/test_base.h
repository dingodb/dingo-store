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

#ifndef DINGODB_SDK_TEST_TEST_BASE_H_
#define DINGODB_SDK_TEST_TEST_BASE_H_

#include <memory>

#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock_client_stub.h"
#include "mock_coordinator_proxy.h"
#include "mock_region_scanner.h"
#include "mock_rpc_interaction.h"
#include "sdk/admin_tool.h"
#include "sdk/client.h"
#include "sdk/client_internal_data.h"
#include "sdk/meta_cache.h"
#include "sdk/transaction/txn_impl.h"
#include "sdk/utils/actuator.h"
#include "sdk/utils/thread_pool_actuator.h"
#include "test_common.h"
#include "transaction/mock_txn_lock_resolver.h"

namespace dingodb {
namespace sdk {

class TestBase : public ::testing::Test {
 public:
  TestBase() {
    std::unique_ptr<MockClientStub> tmp = std::make_unique<MockClientStub>();
    stub = tmp.get();

    coordinator_proxy = std::make_shared<MockCoordinatorProxy>();
    ON_CALL(*stub, GetCoordinatorProxy).WillByDefault(testing::Return(coordinator_proxy));
    EXPECT_CALL(*stub, GetCoordinatorProxy).Times(testing::AnyNumber());
    ON_CALL(*coordinator_proxy, ScanRegions).WillByDefault(testing::Return(Status::OK()));

    meta_cache = std::make_shared<MetaCache>(coordinator_proxy);
    ON_CALL(*stub, GetMetaCache).WillByDefault(testing::Return(meta_cache));
    EXPECT_CALL(*stub, GetMetaCache).Times(testing::AnyNumber());

    brpc::ChannelOptions options;
    options.connect_timeout_ms = 3000;
    options.timeout_ms = 5000;
    store_rpc_interaction = std::make_shared<MockRpcInteraction>(options);
    ON_CALL(*stub, GetStoreRpcInteraction).WillByDefault(testing::Return(store_rpc_interaction));
    EXPECT_CALL(*stub, GetStoreRpcInteraction).Times(testing::AnyNumber());

    region_scanner_factory = std::make_shared<MockRegionScannerFactory>();
    ON_CALL(*stub, GetRawKvRegionScannerFactory).WillByDefault(testing::Return(region_scanner_factory));
    EXPECT_CALL(*stub, GetRawKvRegionScannerFactory).Times(testing::AnyNumber());

    admin_tool = std::make_shared<AdminTool>(coordinator_proxy);
    ON_CALL(*stub, GetAdminTool).WillByDefault(testing::Return(admin_tool));
    EXPECT_CALL(*stub, GetAdminTool).Times(testing::AnyNumber());

    txn_lock_resolver = std::make_shared<MockTxnLockResolver>(*stub);
    ON_CALL(*stub, GetTxnLockResolver).WillByDefault(testing::Return(txn_lock_resolver));
    EXPECT_CALL(*stub, GetTxnLockResolver).Times(testing::AnyNumber());

    actuator.reset(new ThreadPoolActuator());
    actuator->Start(FLAGS_actuator_thread_num);
    ON_CALL(*stub, GetActuator).WillByDefault(testing::Return(actuator));
    EXPECT_CALL(*stub, GetActuator).Times(testing::AnyNumber());

    client = new Client();
    client->data_->stub = std::move(tmp);
  }

  ~TestBase() override {
    store_rpc_interaction.reset();
    meta_cache.reset();
    delete client;
  }

  void SetUp() override { PreFillMetaCache(); }

  std::unique_ptr<Transaction::TxnImpl> NewTransactionImpl(const TransactionOptions& options) const {
    auto txn = std::make_unique<Transaction::TxnImpl>(*stub, options);
    CHECK_NOTNULL(txn.get());
    CHECK(txn->Begin().ok());
    return std::move(txn);
  }

  std::shared_ptr<MockCoordinatorProxy> coordinator_proxy;
  std::shared_ptr<MetaCache> meta_cache;
  std::shared_ptr<MockRpcInteraction> store_rpc_interaction;
  std::shared_ptr<MockRegionScannerFactory> region_scanner_factory;
  std::shared_ptr<AdminTool> admin_tool;
  std::shared_ptr<MockTxnLockResolver> txn_lock_resolver;
  std::shared_ptr<Actuator> actuator;

  // client own stub
  MockClientStub* stub;
  Client* client;

 private:
  void PreFillMetaCache() {
    meta_cache->MaybeAddRegion(RegionA2C());
    meta_cache->MaybeAddRegion(RegionC2E());
    meta_cache->MaybeAddRegion(RegionE2G());
  }
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TEST_TEST_BASE_H_
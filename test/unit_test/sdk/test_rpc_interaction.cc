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

#include "common/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock_rpc.h"
#include "mock_rpc_interaction.h"
#include "proto/store.pb.h"
#include "test_common.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

class RpcInteractionTest : public testing::Test {
 public:
  void SetUp() override {
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 3000;
    options.timeout_ms = 5000;
    interaction = std::make_shared<MockRpcInteraction>(options);

    ON_CALL(*interaction, SendRpc).WillByDefault(testing::Invoke(interaction.get(), &MockRpcInteraction::RealSendRpc));
    ON_CALL(*interaction, InitChannel)
        .WillByDefault(testing::Invoke(interaction.get(), &MockRpcInteraction::RealInitChannel));
  }

  void TearDown() override { interaction.reset(); }

  std::shared_ptr<MockRpcInteraction> interaction;
};

TEST_F(RpcInteractionTest, InitFail) {
  pb::store::KvGetRequest request;
  pb::store::KvGetResponse response;
  MockRpc rpc(&request, &response);

  butil::EndPoint end_point;
  butil::str2endpoint(kAddrOne.c_str(), &end_point);
  rpc.SetEndPoint(end_point);

  EXPECT_CALL(*interaction, SendRpc).Times(1);

  EXPECT_CALL(*interaction, InitChannel)
      .WillOnce(testing::Invoke([&](butil::EndPoint server_addr_and_port, std::shared_ptr<brpc::Channel>& channel) {
        (void)server_addr_and_port;
        (void)channel;
        return Status::Uninitialized("init fail");
      }));

  auto sent = interaction->SendRpcSync(rpc);
  EXPECT_TRUE(sent.IsUninitialized());
}

TEST_F(RpcInteractionTest, InitOK) {
  pb::store::KvGetRequest request;
  pb::store::KvGetResponse response;
  MockRpc rpc(&request, &response);

  butil::EndPoint end_point;
  butil::str2endpoint(kAddrOne.c_str(), &end_point);
  rpc.SetEndPoint(end_point);

  EXPECT_CALL(*interaction, InitChannel).Times(1);
  EXPECT_CALL(*interaction, SendRpc).Times(1);
  EXPECT_CALL(rpc, Call).Times(1);

  auto sent = interaction->SendRpcSync(rpc);
  EXPECT_TRUE(sent.IsOK());
}

TEST_F(RpcInteractionTest, ChannelFromPool) {
  {
    pb::store::KvGetRequest request;
    pb::store::KvGetResponse response;
    MockRpc rpc(&request, &response);

    butil::EndPoint end_point;
    butil::str2endpoint(kAddrOne.c_str(), &end_point);
    rpc.SetEndPoint(end_point);

    EXPECT_CALL(rpc, Call).Times(1);
    EXPECT_CALL(*interaction, InitChannel).Times(1);
    EXPECT_CALL(*interaction, SendRpc).Times(1);

    auto sent = interaction->SendRpcSync(rpc);
    EXPECT_TRUE(sent.IsOK());
  }

  {
    pb::store::KvGetRequest request;
    pb::store::KvGetResponse response;
    MockRpc rpc(&request, &response);

    butil::EndPoint end_point;
    butil::str2endpoint(kAddrOne.c_str(), &end_point);
    rpc.SetEndPoint(end_point);

    EXPECT_CALL(*interaction, SendRpc).Times(1);
    EXPECT_CALL(rpc, Call).Times(1);
    EXPECT_CALL(*interaction, InitChannel).Times(0);

    auto sent = interaction->SendRpcSync(rpc);
    EXPECT_TRUE(sent.IsOK());
  }
}

}  // namespace sdk
}  // namespace dingodb
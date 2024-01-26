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

#ifndef DINGODB_SDK_TEST_MOCK_RPC_H_
#define DINGODB_SDK_TEST_MOCK_RPC_H_

#include "gmock/gmock.h"
#include "google/protobuf/message.h"
#include "sdk/rpc/rpc.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

class MockRpc final : public Rpc {
 public:
  MockRpc(google::protobuf::Message* request, google::protobuf::Message* response)
      : Rpc("MockRpc"), request_(request), response_(response) {}

  ~MockRpc() override = default;

  google::protobuf::Message* RawMutableRequest() override { return request_; }

  const google::protobuf::Message* RawRequest() const override { return request_; }

  google::protobuf::Message* RawMutableResponse() override { return response_; }

  const google::protobuf::Message* RawResponse() const override { return response_; }

  MOCK_METHOD(std::string, ServiceName, (), (override));

  MOCK_METHOD(std::string, ServiceFullName, (), (override));

  MOCK_METHOD(std::string, Method, (), (const, override));

  MOCK_METHOD(void, Call, (brpc::Channel * channel, RpcCallback cb), (override));

  MOCK_METHOD(void, Reset, (), (override));

 private:
  google::protobuf::Message* request_;
  google::protobuf::Message* response_;
};
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TEST_MOCK_RPC_H_
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

#ifndef DINGODB_INTEGRATION_TEST_ENVIROMENT_
#define DINGODB_INTEGRATION_TEST_ENVIROMENT_

#include <iostream>

#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "sdk/client.h"
#include "sdk/coordinator_proxy.h"

DECLARE_string(coordinator_url);

namespace dingodb {

namespace integration_test {

class Environment : public testing::Environment {
 public:
  Environment() : coordinator_proxy_(std::make_shared<sdk::CoordinatorProxy>()) {}
  static Environment& GetInstance() {
    static Environment environment;
    return environment;
  }

  void SetUp() override {
    auto status = coordinator_proxy_->Open(FLAGS_coordinator_url);
    CHECK(status.IsOK()) << "Open coordinator proxy failed, please check parameter --url=" << FLAGS_coordinator_url;

    status = sdk::Client::Build(FLAGS_coordinator_url, client_);
    CHECK(status.IsOK()) << fmt::format("Build sdk client failed, error: {}", status.ToString());
  }
  void TearDown() override {}

  std::shared_ptr<sdk::CoordinatorProxy> GetCoordinatorProxy() { return coordinator_proxy_; }
  std::shared_ptr<sdk::Client> GetClient() { return client_; }

 private:
  std::shared_ptr<sdk::CoordinatorProxy> coordinator_proxy_;
  std::shared_ptr<sdk::Client> client_;
};

}  // namespace integration_test

}  // namespace dingodb

#endif  // DINGODB_INTEGRATION_TEST_ENVIROMENT_

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
#include <mutex>

#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "proto/common.pb.h"
#include "sdk/client.h"
#include "sdk/coordinator_proxy.h"

DECLARE_string(coordinator_url);

namespace dingodb {

namespace integration_test {

class Environment : public testing::Environment {
 public:
  Environment() : coordinator_proxy_(std::make_shared<sdk::CoordinatorProxy>()) {}

  static Environment& GetInstance() {
    static Environment* environment = nullptr;

    static std::once_flag flag;
    std::call_once(flag, [&]() {
      // gtest free
      environment = new Environment();
    });

    return *environment;
  }

  void SetUp() override {
    auto status = coordinator_proxy_->Open(FLAGS_coordinator_url);
    CHECK(status.IsOK()) << "Open coordinator proxy failed, please check parameter --url=" << FLAGS_coordinator_url;

    sdk::Client* tmp;
    status = sdk::Client::Build(FLAGS_coordinator_url, &tmp);
    CHECK(status.IsOK()) << fmt::format("Build sdk client failed, error: {}", status.ToString());
    client_.reset(tmp);

    // Get dingo-store version info
    version_info_ = GetVersionInfo();
    PrintVersionInfo(version_info_);
  }

  void TearDown() override {}

  // TODO: remove this
  std::shared_ptr<sdk::CoordinatorProxy> GetCoordinatorProxy() { return coordinator_proxy_; }
  std::shared_ptr<sdk::Client> GetClient() { return client_; }

  pb::common::VersionInfo VersionInfo() { return version_info_; }

 private:
  pb::common::VersionInfo GetVersionInfo() {
    pb::coordinator::HelloRequest request;
    pb::coordinator::HelloResponse response;

    request.set_is_just_version_info(true);

    LOG(INFO) << "Hello request: " << request.ShortDebugString();

    auto status = coordinator_proxy_->Hello(request, response);
    CHECK(status.IsOK()) << fmt::format("Hello failed, {}", status.ToString());

    return response.version_info();
  }

  static void PrintVersionInfo(const pb::common::VersionInfo& version_info) {
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_commit_hash", version_info.git_commit_hash());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_tag_name", version_info.git_tag_name());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_commit_user", version_info.git_commit_user());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_commit_mail", version_info.git_commit_mail());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_commit_time", version_info.git_commit_time());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "major_version", version_info.major_version());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "minor_version", version_info.minor_version());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "dingo_build_type", version_info.dingo_build_type());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "dingo_contrib_build_type", version_info.dingo_contrib_build_type());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_mkl", (version_info.use_mkl() ? "true" : "false"));
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_openblas", (version_info.use_openblas() ? "true" : "false"));
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_tcmalloc", (version_info.use_tcmalloc() ? "true" : "false"));
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_profiler", (version_info.use_profiler() ? "true" : "false"));
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_sanitizer", (version_info.use_sanitizer() ? "true" : "false"));
  }

  std::shared_ptr<sdk::CoordinatorProxy> coordinator_proxy_;
  std::shared_ptr<sdk::Client> client_;

  pb::common::VersionInfo version_info_;
};

}  // namespace integration_test

}  // namespace dingodb

#endif  // DINGODB_INTEGRATION_TEST_ENVIROMENT_

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

#ifndef DINGODB_INTEGRATION_TEST_REPORT_WEB_
#define DINGODB_INTEGRATION_TEST_REPORT_WEB_

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "proto/common.pb.h"

namespace dingodb::report::web {

class Web {
 public:
  Web() = default;
  ~Web() = default;

  static void GenIntegrationTestReport(const testing::UnitTest* unit_test, const pb::common::VersionInfo& version_info,
                                       const std::string& allure_url, const std::string& directory_path);

  static void GenUnitTestReport(const testing::UnitTest* unit_test, const pb::common::VersionInfo& version_info,
                                const std::string& allure_url, const std::string& coverage_url,
                                const std::string& directory_path);

 private:
  static std::string GenVersionContent(const pb::common::VersionInfo& version_info);
  static std::string GenTestResultContent(const testing::UnitTest* unit_test);
  static std::string GenAllureLinkContent(const std::string& allure_url);
  static std::string GenCoverageLinkContent(const std::string& url);
};

}  // namespace dingodb::report::web

#endif  // DINGODB_INTEGRATION_TEST_REPORT_WEB_
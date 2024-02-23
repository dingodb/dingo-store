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

#include <iostream>
#include <string>
#include <vector>

#include "common/helper.h"
#include "common/uuid.h"
#include "environment.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "helper.h"
#include "report/allure.h"
#include "report/web.h"

DEFINE_string(coordinator_url, "file://./coor_list", "coordinator url");
DEFINE_int32(create_region_wait_time_s, 3, "create region wait time");

DEFINE_string(allure_report, "", "allure report directory");
DEFINE_string(allure_url, "", "jenkins allure url");
DEFINE_string(web_report, "", "web report directory");

int main(int argc, char* argv[]) {
  testing::AddGlobalTestEnvironment(&dingodb::integration_test::Environment::GetInstance());
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);

  int ret = RUN_ALL_TESTS();

  // Generate allure report.
  if (!FLAGS_allure_report.empty()) {
    dingodb::report::allure::Allure::GenReport(testing::UnitTest::GetInstance(),
                                               dingodb::integration_test::Environment::GetInstance().VersionInfo(),
                                               FLAGS_allure_report);
  }

  // Generate web report.
  if (!FLAGS_web_report.empty()) {
    dingodb::report::web::Web::GenIntegrationTestReport(
        testing::UnitTest::GetInstance(), dingodb::integration_test::Environment::GetInstance().VersionInfo(),
        FLAGS_allure_url, FLAGS_web_report);
  }

  return ret;
}
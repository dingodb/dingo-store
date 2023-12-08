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

#include "environment.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

DEFINE_string(coordinator_url, "file://./coor_list", "coordinator url");
DEFINE_int32(create_region_wait_time_s, 3, "create region wait time");

int main(int argc, char* argv[]) {
  testing::AddGlobalTestEnvironment(&dingodb::integration_test::Environment::GetInstance());
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);

  int ret = RUN_ALL_TESTS();

  // LOG(INFO) << fmt::format(
  //     "Test summary: test_suite(total({})/success({})/fail({})) test_case(total({})/success({})/fail({})/skip({})) ",
  //     testing::UnitTest::GetInstance()->total_test_suite_count(),
  //     testing::UnitTest::GetInstance()->successful_test_suite_count(),
  //     testing::UnitTest::GetInstance()->failed_test_suite_count(),
  //     testing::UnitTest::GetInstance()->total_test_count(),
  //     testing::UnitTest::GetInstance()->successful_test_count(),
  //     testing::UnitTest::GetInstance()->failed_test_count(), testing::UnitTest::GetInstance()->skipped_test_count());

  // int total_count = testing::UnitTest::GetInstance()->total_test_suite_count();
  // for (int i = 0; i < total_count; ++i) {
  //   const auto* test_suite = testing::UnitTest::GetInstance()->GetTestSuite(i);
  //   int total_case_count = test_suite->total_test_count();
  //   for (int j = 0; j < total_case_count; ++j) {
  //     const auto* test_info = test_suite->GetTestInfo(j);
  //     test_info->result();
  //   }
  // }
  // const auto& test_result = testing::UnitTest::GetInstance()->ad_hoc_test_result();

  return ret;
}
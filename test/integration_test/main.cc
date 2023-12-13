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

DEFINE_string(coordinator_url, "file://./coor_list", "coordinator url");
DEFINE_int32(create_region_wait_time_s, 3, "create region wait time");

DEFINE_bool(generate_allure, false, "generate allure");
DEFINE_string(allure_report, "./allure_report", "allure report");

static std::string TransformStatus(const testing::TestResult* test_case_result) {
  if (test_case_result->Passed()) {
    return "passed";
  } else if (test_case_result->Failed()) {
    return "failed";
  } else if (test_case_result->Skipped()) {
    return "skipped";
  } else if (test_case_result->HasFatalFailure()) {
    return "broken";
  }

  return "unknown";
}

static std::string TransformStatus(const testing::TestPartResult& test_case_part_result) {
  if (test_case_part_result.passed()) {
    return "passed";
  } else if (test_case_part_result.failed()) {
    return "failed";
  } else if (test_case_part_result.skipped()) {
    return "skipped";
  } else if (test_case_part_result.fatally_failed()) {
    return "broken";
  }

  return "unknown";
}

static std::string GetPropertyValue(const std::map<std::string, std::string>& properties, const std::string& key) {
  auto it = properties.find(key);
  return it == properties.end() ? "" : it->second;
}

void GenAllureReport() {
  LOG(INFO) << fmt::format(
      "Test summary: test_suite(total({})/success({})/fail({})) test_case(total({})/success({})/fail({})/skip({})) ",
      testing::UnitTest::GetInstance()->total_test_suite_count(),
      testing::UnitTest::GetInstance()->successful_test_suite_count(),
      testing::UnitTest::GetInstance()->failed_test_suite_count(), testing::UnitTest::GetInstance()->total_test_count(),
      testing::UnitTest::GetInstance()->successful_test_count(), testing::UnitTest::GetInstance()->failed_test_count(),
      testing::UnitTest::GetInstance()->skipped_test_count());

  std::vector<dingodb::integration_test::allure::TestSuite> allure_test_suites;
  int total_count = testing::UnitTest::GetInstance()->total_test_suite_count();
  for (int i = 0; i < total_count; ++i) {
    dingodb::integration_test::allure::TestSuite allure_test_suite;
    const auto* test_suite = testing::UnitTest::GetInstance()->GetTestSuite(i);
    allure_test_suite.uuid = dingodb::UUIDGenerator::GenerateUUID();
    allure_test_suite.history_id = dingodb::UUIDGenerator::GenerateUUIDV3(test_suite->name());
    allure_test_suite.name = test_suite->name();
    allure_test_suite.start = test_suite->start_timestamp();
    allure_test_suite.stop = test_suite->start_timestamp() + test_suite->elapsed_time();

    int total_case_count = test_suite->total_test_count();
    for (int j = 0; j < total_case_count; ++j) {
      dingodb::integration_test::allure::TestCase allure_test_case;
      const auto* test_case_info = test_suite->GetTestInfo(j);
      const auto* test_case_result = test_case_info->result();

      // Generate property map
      std::map<std::string, std::string> properties;
      int total_property_count = test_case_result->test_property_count();
      for (int k = 0; k < total_property_count; ++k) {
        const auto& property = test_case_result->GetTestProperty(k);
        properties[property.key()] = property.value();
      }

      allure_test_case.uuid = dingodb::UUIDGenerator::GenerateUUID();
      allure_test_case.history_id = dingodb::UUIDGenerator::GenerateUUIDV3(test_case_info->name());
      allure_test_case.test_case_id = dingodb::UUIDGenerator::GenerateUUIDV3(test_case_info->name());
      allure_test_case.full_name = fmt::format("{}.{}", test_suite->name(), test_case_info->name());
      allure_test_case.name = test_case_info->name();
      allure_test_case.status = TransformStatus(test_case_result);
      allure_test_case.start = test_case_result->start_timestamp();
      allure_test_case.stop = test_case_result->start_timestamp() + test_case_result->elapsed_time();
      allure_test_case.labels = {
          {"framework", "gtest"},
          {"language", "c++"},
          {"suite", test_suite->name()},
          {"subSuite", test_case_info->name()},
          {"testMethod", test_case_info->name()},
      };
      allure_test_case.description = GetPropertyValue(properties, "description");

      int total_part_count = test_case_result->total_part_count();
      for (int k = 0; k < total_part_count; ++k) {
        dingodb::integration_test::allure::Step allure_test_step;
        const auto& test_case_part_result = test_case_result->GetTestPartResult(k);
        allure_test_step.name = fmt::format("{}.{}", test_case_info->name(), test_case_part_result.line_number());
        allure_test_step.stage = "finished";
        allure_test_step.status = TransformStatus(test_case_part_result);
        allure_test_step.status_details.known = true;
        allure_test_step.status_details.message = test_case_part_result.message();
        allure_test_step.status_details.trace = test_case_part_result.summary();
        allure_test_case.steps.push_back(allure_test_step);
      }

      allure_test_suite.test_cases.push_back(allure_test_case);
    }

    allure_test_suites.push_back(allure_test_suite);
  }

  dingodb::Helper::RemoveAllFileOrDirectory(FLAGS_allure_report);
  dingodb::Helper::CreateDirectory(FLAGS_allure_report);

  dingodb::integration_test::allure::Allure allure(allure_test_suites);
  allure.GenReport(FLAGS_allure_report, dingodb::integration_test::Helper::TransformVersionInfo(
                                            dingodb::integration_test::Environment::GetInstance().VersionInfo()));
}

int main(int argc, char* argv[]) {
  testing::AddGlobalTestEnvironment(&dingodb::integration_test::Environment::GetInstance());
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);

  int ret = RUN_ALL_TESTS();

  if (FLAGS_generate_allure) {
    GenAllureReport();
  }

  return ret;
}
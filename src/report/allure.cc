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

#include "report/allure.h"

#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/helper.h"
#include "common/uuid.h"
#include "fmt/core.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace dingodb::report::allure {

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

static std::vector<std::pair<std::string, std::string>> TransformVersionInfo(
    const pb::common::VersionInfo& version_info) {
  std::vector<std::pair<std::string, std::string>> result = {
      std::make_pair("git_commit_hash", version_info.git_commit_hash()),
      std::make_pair("git_tag_name", version_info.git_tag_name()),
      std::make_pair("git_commit_user", version_info.git_commit_user()),
      std::make_pair("git_commit_mail", version_info.git_commit_mail()),
      std::make_pair("git_commit_time", version_info.git_commit_time()),
      std::make_pair("major_version", version_info.major_version()),
      std::make_pair("minor_version", version_info.minor_version()),
      std::make_pair("dingo_build_type", version_info.dingo_build_type()),
      std::make_pair("dingo_contrib_build_type", version_info.dingo_contrib_build_type()),
      std::make_pair("use_mkl", version_info.use_mkl() ? "true" : "false"),
      std::make_pair("use_openblas", version_info.use_openblas() ? "true" : "false"),
      std::make_pair("use_openblas", version_info.use_openblas() ? "true" : "false"),
      std::make_pair("use_tcmalloc", version_info.use_tcmalloc() ? "true" : "false"),
      std::make_pair("use_profiler", version_info.use_profiler() ? "true" : "false"),
      std::make_pair("use_sanitizer", version_info.use_sanitizer() ? "true" : "false"),
  };

  return result;
}

void Allure::GenReport(const testing::UnitTest* unit_test, const pb::common::VersionInfo& version_info,
                       const std::string& directory_path) {
  std::vector<dingodb::report::allure::TestSuite> allure_test_suites;
  int total_count = unit_test->total_test_suite_count();
  for (int i = 0; i < total_count; ++i) {
    dingodb::report::allure::TestSuite allure_test_suite;
    const auto* test_suite = unit_test->GetTestSuite(i);
    allure_test_suite.uuid = dingodb::UUIDGenerator::GenerateUUID();
    allure_test_suite.history_id = dingodb::UUIDGenerator::GenerateUUIDV3(test_suite->name());
    allure_test_suite.name = test_suite->name();
    allure_test_suite.start = test_suite->start_timestamp();
    allure_test_suite.stop = test_suite->start_timestamp() + test_suite->elapsed_time();

    int total_case_count = test_suite->total_test_count();
    for (int j = 0; j < total_case_count; ++j) {
      dingodb::report::allure::TestCase allure_test_case;
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
        dingodb::report::allure::Step allure_test_step;
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

  if (dingodb::Helper::IsExistPath(directory_path)) {
    dingodb::Helper::RemoveAllFileOrDirectory(directory_path);
  }
  dingodb::Helper::CreateDirectory(directory_path);

  GenTestResultFile(allure_test_suites, directory_path);
  GenContainerFile(allure_test_suites, directory_path);
  GenCategoriesFile(allure_test_suites, directory_path);
  GenEnvironmentFile(allure_test_suites, directory_path, TransformVersionInfo(version_info));
  GenHistoryFile(allure_test_suites, directory_path);
}

void Allure::GenTestResultFile(std::vector<TestSuite>& test_suites, const std::string& directory_path) {
  for (const auto& test_suite : test_suites) {
    for (const auto& test_case : test_suite.test_cases) {
      rapidjson::Document doc;
      doc.SetObject();
      rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

      doc.AddMember("uuid", rapidjson::StringRef(test_case.uuid.c_str()), allocator);
      doc.AddMember("historyId", rapidjson::StringRef(test_case.history_id.c_str()), allocator);
      doc.AddMember("testCaseId", rapidjson::StringRef(test_case.test_case_id.c_str()), allocator);
      doc.AddMember("fullName", rapidjson::StringRef(test_case.full_name.c_str()), allocator);
      doc.AddMember("name", rapidjson::StringRef(test_case.name.c_str()), allocator);
      doc.AddMember("description", rapidjson::StringRef(test_case.description.c_str()), allocator);

      if (!test_case.links.empty()) {
        rapidjson::Value array_value(rapidjson::kArrayType);
        for (const auto& link : test_case.links) {
          rapidjson::Value obj_value(rapidjson::kObjectType);
          obj_value.AddMember("type", rapidjson::StringRef(link.type.c_str()), allocator);
          obj_value.AddMember("name", rapidjson::StringRef(link.name.c_str()), allocator);
          obj_value.AddMember("url", rapidjson::StringRef(link.url.c_str()), allocator);
          array_value.PushBack(obj_value, allocator);
        }
        doc.AddMember("links", array_value, allocator);
      }

      if (!test_case.labels.empty()) {
        rapidjson::Value array_value(rapidjson::kArrayType);
        for (const auto& label : test_case.labels) {
          rapidjson::Value obj_value(rapidjson::kObjectType);
          obj_value.AddMember("name", rapidjson::StringRef(label.name.c_str()), allocator);
          obj_value.AddMember("value", rapidjson::StringRef(label.value.c_str()), allocator);
          array_value.PushBack(obj_value, allocator);
        }
        doc.AddMember("labels", array_value, allocator);
      }

      if (!test_case.steps.empty()) {
        rapidjson::Value array_value(rapidjson::kArrayType);
        for (const auto& step : test_case.steps) {
          rapidjson::Value obj_value(rapidjson::kObjectType);
          obj_value.AddMember("name", rapidjson::StringRef(step.name.c_str()), allocator);
          obj_value.AddMember("status", rapidjson::StringRef(step.status.c_str()), allocator);
          obj_value.AddMember("start", step.start, allocator);
          obj_value.AddMember("stop", step.stop, allocator);
          array_value.PushBack(obj_value, allocator);
        }
        doc.AddMember("steps", array_value, allocator);
      }

      doc.AddMember("status", rapidjson::StringRef(test_case.status.c_str()), allocator);
      doc.AddMember("start", test_case.start, allocator);
      doc.AddMember("stop", test_case.stop, allocator);

      rapidjson::StringBuffer str_buf;
      rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);
      doc.Accept(writer);

      std::string filepath = fmt::format("{}/{}-result.json", directory_path, dingodb::UUIDGenerator::GenerateUUID());
      dingodb::Helper::SaveFile(filepath, str_buf.GetString());
    }
  }
}

void Allure::GenContainerFile(std::vector<TestSuite>& test_suites, const std::string& directory_path) {
  for (const auto& test_suite : test_suites) {
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
    doc.AddMember("uuid", rapidjson::StringRef(test_suite.uuid.c_str()), allocator);
    doc.AddMember("start", test_suite.start, allocator);
    doc.AddMember("stop", test_suite.stop, allocator);

    if (!test_suite.test_cases.empty()) {
      rapidjson::Value array_value(rapidjson::kArrayType);
      for (const auto& test_case : test_suite.test_cases) {
        array_value.PushBack(rapidjson::StringRef(test_case.uuid.c_str()), allocator);
      }
      doc.AddMember("children", array_value, allocator);
    }

    if (!test_suite.setup_steps.empty()) {
      rapidjson::Value array_value(rapidjson::kArrayType);
      for (const auto& step : test_suite.setup_steps) {
        rapidjson::Value obj_value(rapidjson::kObjectType);
        obj_value.AddMember("name", rapidjson::StringRef(step.name.c_str()), allocator);
        obj_value.AddMember("status", rapidjson::StringRef(step.status.c_str()), allocator);
        obj_value.AddMember("start", step.start, allocator);
        obj_value.AddMember("stop", step.stop, allocator);

        array_value.PushBack(obj_value, allocator);
      }
      doc.AddMember("befores", array_value, allocator);
    }

    if (!test_suite.teardown_steps.empty()) {
      rapidjson::Value array_value(rapidjson::kArrayType);
      for (const auto& step : test_suite.teardown_steps) {
        rapidjson::Value obj_value(rapidjson::kObjectType);
        obj_value.AddMember("name", rapidjson::StringRef(step.name.c_str()), allocator);
        obj_value.AddMember("status", rapidjson::StringRef(step.status.c_str()), allocator);
        obj_value.AddMember("start", step.start, allocator);
        obj_value.AddMember("stop", step.stop, allocator);

        array_value.PushBack(obj_value, allocator);
      }
      doc.AddMember("afters", array_value, allocator);
    }

    rapidjson::StringBuffer str_buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);
    doc.Accept(writer);

    std::string filepath = fmt::format("{}/{}-container.json", directory_path, dingodb::UUIDGenerator::GenerateUUID());
    dingodb::Helper::SaveFile(filepath, str_buf.GetString());
  }
}

void Allure::GenCategoriesFile(std::vector<TestSuite>& test_suites, const std::string& directory_path) {}

void Allure::GenEnvironmentFile(std::vector<TestSuite>&, const std::string& directory_path,
                                const std::vector<std::pair<std::string, std::string>>& properties) {
  std::string data;
  for (const auto& pair : properties) {
    data += fmt::format("{}: {}\n", pair.first, pair.second);
  }

  if (!data.empty()) {
    std::string filepath = fmt::format("{}/environment.properties", directory_path);
    dingodb::Helper::SaveFile(filepath, data);
  }
}

void Allure::GenHistoryFile(std::vector<TestSuite>& test_suites, const std::string& directory_path) {}

}  // namespace dingodb::report::allure
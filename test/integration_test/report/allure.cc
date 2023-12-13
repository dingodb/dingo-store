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

#include "common/uuid.h"
#include "fmt/core.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace dingodb {

namespace integration_test {

namespace allure {

void Allure::GenReport(const std::string& directory_path,
                       const std::vector<std::pair<std::string, std::string>>& properties) {
  GenTestResultFile(directory_path);
  GenContainerFile(directory_path);
  GenCategoriesFile(directory_path);
  GenEnvironmentFile(directory_path, properties);
  GenHistoryFile(directory_path);
}

void Allure::GenTestResultFile(const std::string& directory_path) {
  for (const auto& test_suite : test_suites_) {
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
      SaveFile(filepath, str_buf.GetString());
    }
  }
}

void Allure::GenContainerFile(const std::string& directory_path) {
  for (const auto& test_suite : test_suites_) {
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
    SaveFile(filepath, str_buf.GetString());
  }
}

void Allure::GenCategoriesFile(const std::string& directory_path) {}

void Allure::GenEnvironmentFile(const std::string& directory_path,
                                const std::vector<std::pair<std::string, std::string>>& properties) {
  std::string data;
  for (const auto& pair : properties) {
    data += fmt::format("{}: {}\n", pair.first, pair.second);
  }

  if (!data.empty()) {
    std::string filepath = fmt::format("{}/environment.properties", directory_path);
    SaveFile(filepath, data);
  }
}

void Allure::GenHistoryFile(const std::string& directory_path) {}

bool Allure::SaveFile(const std::string& filepath, const std::string& data) {
  std::ofstream file(filepath);
  if (!file.is_open()) {
    return false;
  }

  file << data;
  file.close();

  return true;
}

}  // namespace allure

}  // namespace integration_test

}  // namespace dingodb
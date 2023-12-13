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

#ifndef DINGODB_INTEGRATION_TEST_REPORT_ALLURE_
#define DINGODB_INTEGRATION_TEST_REPORT_ALLURE_

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace dingodb {

namespace integration_test {

namespace allure {

struct Parameter {
  std::string name;
  std::string value;
  bool excluded;
  std::string mode;
};

struct Attachment {
  std::string name;
  std::string source;
  std::string type;
};

struct Link {
  std::string type;
  std::string name;
  std::string url;
};

struct Label {
  std::string name;
  std::string value;
};

struct StatusDetails {
  bool known;
  bool muted;
  bool flaky;
  std::string message;
  std::string trace;
};

struct Step {
  std::string name;
  std::vector<Parameter> parameters;
  std::vector<Attachment> attachments;
  std::string status;
  StatusDetails status_details;
  std::string stage;
  int64_t start;
  int64_t stop;
};

struct TestCase {
  std::string uuid;
  std::string history_id;
  std::string test_case_id;
  std::string full_name;
  std::string name;
  std::string description;
  std::string status;
  int64_t start;
  int64_t stop;
  std::vector<Step> steps;
  std::vector<Label> labels;
  std::vector<Link> links;
};

struct TestSuite {
  std::string uuid;
  std::string history_id;
  std::string name;
  int64_t start;
  int64_t stop;
  std::vector<TestCase> test_cases;
  std::vector<Step> setup_steps;
  std::vector<Step> teardown_steps;
};

class Allure {
 public:
  Allure(std::vector<TestSuite> test_suites) : test_suites_(test_suites) {}
  ~Allure() = default;

  void GenReport(const std::string& directory_path, const std::vector<std::pair<std::string, std::string>>& properties);

 private:
  void GenTestResultFile(const std::string& directory_path);
  void GenContainerFile(const std::string& directory_path);
  void GenCategoriesFile(const std::string& directory_path);
  void GenEnvironmentFile(const std::string& directory_path,
                          const std::vector<std::pair<std::string, std::string>>& properties);
  void GenHistoryFile(const std::string& directory_path);

  static bool SaveFile(const std::string& filepath, const std::string& data);

  std::vector<TestSuite> test_suites_;
};

}  // namespace allure

}  // namespace integration_test

}  // namespace dingodb

#endif  // DINGODB_INTEGRATION_TEST_REPORT_ALLURE_
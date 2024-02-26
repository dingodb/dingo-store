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

#include "report/web.h"

#include <fstream>
#include <string>

#include "common/helper.h"
#include "fmt/core.h"

namespace dingodb::report::web {

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

std::string Web::GenVersionContent(const pb::common::VersionInfo& version_info) {
  std::string content;

  content += "<h2>Version Information:</h2>";
  content += "<ul>";
  content += fmt::format("<li>{}: {}</li>", "commit_hash", version_info.git_commit_hash());
  content += fmt::format("<li>{}: {}</li>", "commit_user", version_info.git_commit_user());
  content += fmt::format("<li>{}: {}</li>", "commit_mail", version_info.git_commit_mail());
  content += fmt::format("<li>{}: {}</li>", "commit_time", version_info.git_commit_time());
  content += fmt::format("<li>{}: {}</li>", "tag_name", version_info.git_tag_name());
  content += fmt::format("<li>{}: {}</li>", "major_version", version_info.major_version());
  content += fmt::format("<li>{}: {}</li>", "minor_version", version_info.minor_version());
  content += fmt::format("<li>{}: {}</li>", "build_type", version_info.dingo_build_type());
  content += fmt::format("<li>{:<24}: {:>64}</li>", "contrib_build_type", version_info.dingo_contrib_build_type());
  content +=
      fmt::format("<li>flags: use_mkl({}) use_openblas({}) use_tcmalloc({}) use_profiler({}) use_sanitizer({})</li>",
                  (version_info.use_mkl() ? "true" : "false"), (version_info.use_openblas() ? "true" : "false"),
                  (version_info.use_tcmalloc() ? "true" : "false"), (version_info.use_profiler() ? "true" : "false"),
                  (version_info.use_sanitizer() ? "true" : "false"));
  content += "</ul>";

  return content;
}

std::string Web::GenTestResultContent(const testing::UnitTest* unit_test) {
  std::string content = R"(
    <h2>Test Result Information:</h2>
    <div style="padding-left: 28px;">
    <table style="text-indent: 8px;border-collapse: collapse; width: 80%;" border="1">
    <tbody>
      <tr style="height: 32px;background-color: #2EA9DF;">
        <td style="width: 20%;"><strong>Test Suite</strong></td>
        <td style="width: 30%;"><strong>Test Case</strong></td>
        <td style="width: 10%;"><strong>Result</strong></td>
        <td style="width: 10%;"><strong>Elapsed Time(ms)</strong></td>
      </tr>
  )";

  int total_count = unit_test->total_test_suite_count();
  for (int i = 0; i < total_count; ++i) {
    const auto* test_suite = unit_test->GetTestSuite(i);

    int total_case_count = test_suite->total_test_count();
    for (int j = 0; j < total_case_count; ++j) {
      const auto* test_case_info = test_suite->GetTestInfo(j);
      const auto* test_case_result = test_case_info->result();

      std::string status = TransformStatus(test_case_result);
      content += R"(<tr style="height: 32px;)";
      if (status == "failed") {
        content += "background-color: #CB1B45;";
      } else if (status == "broken") {
        content += "background-color: #F596AA;";
      }
      content += R"(">)";
      if (j == 0) {
        content += R"(<td style="width: 16.6667%;" rowspan=")";
        content += std::to_string(total_case_count);
        content += R"(">)";
        content += test_suite->name();
        content += R"(<br></td>)";
      }

      content += R"(<td style="width: 16.6667%;">)";
      content += test_case_info->name();
      content += R"(</td>)";

      content += R"(<td style="width: 16.6667%;">)";
      content += status;
      content += R"(</td>)";

      content += R"(<td style="width: 16.6667%;">)";
      content += std::to_string(test_case_result->elapsed_time());
      content += R"(</td>)";

      content += R"(</tr>)";
    }
  }

  content += R"(</tbody>)";
  content += R"(</table>)";
  content += R"(</div>)";

  return content;
}

std::string Web::GenAllureLinkContent(const std::string& allure_url) {
  std::string content;
  content += "<div>";
  content += "<p>";
  content += "Report Details: " + fmt::format("<a href=\"{}\">dingo-store test report</a>", allure_url);
  content += "</p>";
  content += "</div>";

  return content;
}

std::string Web::GenCoverageLinkContent(const std::string& url) {
  std::string content;
  content += "<div>";
  content += "<p>";
  content += "Code Coverage Details: " + fmt::format("<a href=\"{}\">dingo-store unit test coverage report</a>", url);
  content += "</p>";
  content += "</div>";

  return content;
}

void Web::GenIntegrationTestReport(const testing::UnitTest* unit_test, const pb::common::VersionInfo& version_info,
                                   const std::string& allure_url, const std::string& directory_path) {
  std::string html = R"(
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Dingo-Store Test Report</title>
    </head>
    <body>
  )";

  html += "<h1>Dingo-Store Integration Test Report</h1>";
  html += "<div>" + GenVersionContent(version_info) + "</div>";
  html += "<div>" + GenTestResultContent(unit_test) + "</div>";
  html += "<div>" + GenAllureLinkContent(allure_url) + "</div>";

  html += R"(
    </body>
    </html>
  )";

  if (dingodb::Helper::IsExistPath(directory_path)) {
    dingodb::Helper::RemoveAllFileOrDirectory(directory_path);
  }
  dingodb::Helper::CreateDirectory(directory_path);

  std::string filepath = fmt::format("{}/integration_test.html", directory_path);
  dingodb::Helper::SaveFile(filepath, html);
}

void Web::GenUnitTestReport(const testing::UnitTest* unit_test, const pb::common::VersionInfo& version_info,
                            const std::string& allure_url, const std::string& coverage_url,
                            const std::string& directory_path) {
  std::string html = R"(
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Dingo-Store Test Report</title>
    </head>
    <body>
  )";

  html += "<h1>Dingo-Store Unit Test Report</h1>";
  html += "<div>" + GenVersionContent(version_info) + "</div>";
  html += "<div>" + GenTestResultContent(unit_test) + "</div>";
  html += "<div>" + GenAllureLinkContent(allure_url) + "</div>";
  html += "<div>" + GenCoverageLinkContent(coverage_url) + "</div>";

  html += R"(
    </body>
    </html>
  )";

  if (dingodb::Helper::IsExistPath(directory_path)) {
    dingodb::Helper::RemoveAllFileOrDirectory(directory_path);
  }
  dingodb::Helper::CreateDirectory(directory_path);

  std::string filepath = fmt::format("{}/unit_test.html", directory_path);
  dingodb::Helper::SaveFile(filepath, html);
}

}  // namespace dingodb::report::web
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

#include <gtest/gtest.h>

#include "common/helper.h"
#include "common/version.h"
#include "glog/logging.h"
#include "report/allure.h"
#include "report/web.h"

DEFINE_string(allure_report, "", "allure report directory");
DEFINE_string(allure_url, "", "jenkins allure url");
DEFINE_string(web_report, "", "web report directory");
DEFINE_string(coverage_url, "", "coverage url");

void InitLog(const std::string& log_dir) {
  if (!dingodb::Helper::IsExistPath(log_dir)) {
    dingodb::Helper::CreateDirectories(log_dir);
  }

  FLAGS_logbufsecs = 0;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;

  std::string program_name = "dingodb_unit_test";

  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(google::GLOG_INFO, fmt::format("{}/{}.info.log.", log_dir, program_name).c_str());
  google::SetLogDestination(google::GLOG_WARNING, fmt::format("{}/{}.warn.log.", log_dir, program_name).c_str());
  google::SetLogDestination(google::GLOG_ERROR, fmt::format("{}/{}.error.log.", log_dir, program_name).c_str());
  google::SetLogDestination(google::GLOG_FATAL, fmt::format("{}/{}.fatal.log.", log_dir, program_name).c_str());
  google::SetStderrLogging(google::GLOG_FATAL);
}

int main(int argc, char* argv[]) {
  InitLog("./log");

  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (testing::FLAGS_gtest_filter == "*") {
    std::string default_run_case;
    default_run_case += "ConfigTest.*";
    default_run_case += ":HelperTest.*";
    default_run_case += ":DingoLatchTest.*";
    default_run_case += ":DingoLoggerTest.*";
    default_run_case += ":StorePbTest.*";
    default_run_case += ":WriteDataBuilderTest.*";
    default_run_case += ":FailPointTest.*";
    default_run_case += ":CoprocessorTest.*";
    default_run_case += ":CoprocessorUtilsTest.*";
    default_run_case += ":CoprocessorAggregationManagerTest.*";

    default_run_case += ":DingoSafeMapTest.*";
    default_run_case += ":SegmentLogStorageTest.*";
    default_run_case += ":DingoSerialListTypeTest.*";
    default_run_case += ":DingoSerialTest.*";
    default_run_case += ":ServiceHelperTest.*";
    default_run_case += ":SplitCheckerTest.*";

    default_run_case += ":ScanTest.*";
    default_run_case += ":ScanV2Test.*";
    default_run_case += ":ScanWithCoprocessor.*";
    default_run_case += ":ScanWithCoprocessorV2.*";

    // default_run_case += ":StoreRegionMetaTest.*";
    // default_run_case += ":StoreRegionMetricsTest.*";

    default_run_case += ":VectorIndexWrapperTest.*";
    default_run_case += ":VectorIndexUtilsTest.*";
    default_run_case += ":VectorIndexSnapshotTest.*";
    default_run_case += ":VectorIndexRawIvfPqTest.*";
    default_run_case += ":VectorIndexRawIvfPqBoundaryTest.*";
    default_run_case += ":VectorIndexMemoryTest.*";
    default_run_case += ":VectorIndexMemoryHnswTest.*";
    default_run_case += ":VectorIndexMemoryFlatTest.*";
    default_run_case += ":VectorIndexIvfPqTest.*";
    default_run_case += ":VectorIndexIvfFlatTest.*";
    default_run_case += ":VectorIndexHnswTest.*";
    default_run_case += ":VectorIndexHnswSearchParamTest.*";
    default_run_case += ":VectorIndexFlatTest.*";
    default_run_case += ":VectorIndexFlatSearchParamTest.*";
    default_run_case += ":VectorIndexFlatSearchParamLimitTest.*";
    default_run_case += ":TxnGcTest.*";

    // sdk
    default_run_case += ":MetaCacheTest.*";
    default_run_case += ":RegionTest.*";
    default_run_case += ":StoreRpcControllerTest.*";
    default_run_case += ":ThreadPoolActuatorTest.*";
    default_run_case += ":VectorCommonTest.*";
    default_run_case += ":VectorIndexCacheKeyTest.*";
    default_run_case += ":VectorIndexTest.*";
    default_run_case += ":TxnBufferTest.*";
    default_run_case += ":TxnImplTest.*";
    default_run_case += ":TxnLockResolverTest.*";
    default_run_case += ":RawKvRegionScannerImplTest.*";
    default_run_case += ":RawKVTest.*";

    testing::GTEST_FLAG(filter) = default_run_case;
  }

  int ret = RUN_ALL_TESTS();

  // Generate allure report.
  if (!FLAGS_allure_report.empty()) {
    dingodb::report::allure::Allure::GenReport(testing::UnitTest::GetInstance(), dingodb::GetVersionInfo(),
                                               FLAGS_allure_report);
  }

  // Generate web report.
  if (!FLAGS_web_report.empty()) {
    dingodb::report::web::Web::GenUnitTestReport(testing::UnitTest::GetInstance(), dingodb::GetVersionInfo(),
                                                 FLAGS_allure_url, FLAGS_coverage_url, FLAGS_web_report);
  }

  return ret;
}
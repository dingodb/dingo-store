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

#include <iostream>
#include <string>

#include "common/helper.h"

class HelperTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// TEST_F(HelperTest, GetDiskCapacity) {
//   std::string path = "/";
//   std::map<std::string, uint64_t> output;

//   EXPECT_EQ(true, dingodb::Helper::GetDiskCapacity(path, output));
//   std::cout << output["TotalSpace"] << " " << output["FreeSpace"] << std::endl;
// }

TEST_F(HelperTest, FormatTime) {
  auto format_time = dingodb::Helper::FormatTime(1681970908, "%Y-%m-%d %H:%M:%S");
  std::cout << format_time << std::endl;

  EXPECT_EQ("2023-04-20 14:08:28", format_time);

  // auto format_ms_time = dingodb::Helper::FormatMsTime(1681970908001, "%Y-%m-%d %H:%M:%S");
  // std::cout << format_ms_time << std::endl;

  // EXPECT_EQ("2023-04-20 14:08:28.001", format_ms_time);

  std::cout << dingodb::Helper::GetNowFormatMsTime();
}

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
#include <iterator>
#include <memory>
#include <string>

#include "common/helper.h"
#include "server/service_helper.h"

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

TEST_F(HelperTest, TimestampNs) {
  std::shared_ptr<int> abc;
  if (abc == nullptr) {
    std::cout << dingodb::Helper::TimestampNs() << std::endl;
  }
}

TEST_F(HelperTest, TransformRangeWithOptions) {
  dingodb::pb::common::Range region_range;
  char start_key[] = {0x61, 0x64};
  char end_key[] = {0x78, 0x65};
  region_range.set_start_key(start_key, 2);
  region_range.set_end_key(end_key, 2);

  std::cout << "region_range: " << dingodb::Helper::StringToHex(region_range.start_key()) << " "
            << dingodb::Helper::StringToHex(region_range.end_key()) << std::endl;

  {
    // [0x61, 0x78]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61};
    char end_key[] = {0x78};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);
    std::cout << "uniform_range: " << dingodb::Helper::StringToHex(uniform_range.start_key()) << " "
              << dingodb::Helper::StringToHex(uniform_range.end_key()) << std::endl;

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x61, 0x78)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61};
    char end_key[] = {0x78};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x61, 0x78]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61};
    char end_key[] = {0x78};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x61, 0x78)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61};
    char end_key[] = {0x78};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x60, 0x77]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x60};
    char end_key[] = {0x77};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x60, 0x77]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x60};
    char end_key[] = {0x77};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x60, 0x77)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x60};
    char end_key[] = {0x77};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x60, 0x77)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x60};
    char end_key[] = {0x77};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x62, 0x79)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x62};
    char end_key[] = {0x79};
    scan_range.mutable_range()->set_start_key(start_key, 1);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 1);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  // ==================================================
  {
    // [0x6164, 0x7865]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x6164, 0x7865)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x6164, 0x7865]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x6164, 0x7865)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x6163, 0x7865]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x63};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x6163, 0x7865)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x63};
    char end_key[] = {0x78, 0x65};
    scan_range.mutable_range()->set_start_key(start_key, 2);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 2);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  // ========================================
  {
    // [0x616461, 0x786563]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x65, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // [0x616461, 0x786563)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x65, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x616461, 0x786563]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x65, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }

  {
    // (0x616461, 0x786563)
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x65, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(false);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(false);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }
  {
    // [0x616461, 0x786463]
    dingodb::pb::common::RangeWithOptions scan_range;
    char start_key[] = {0x61, 0x64, 0x61};
    char end_key[] = {0x78, 0x64, 0x63};
    scan_range.mutable_range()->set_start_key(start_key, 3);
    scan_range.set_with_start(true);
    scan_range.mutable_range()->set_end_key(end_key, 3);
    scan_range.set_with_end(true);
    auto uniform_range = dingodb::Helper::TransformRangeWithOptions(scan_range);

    EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(region_range, uniform_range).ok());
  }
}

TEST_F(HelperTest, hello) {
  butil::EndPoint endpoint;
  std::cout << endpoint.ip << endpoint.port;
}
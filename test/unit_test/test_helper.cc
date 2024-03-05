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

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include "common/helper.h"
#include "fmt/core.h"
#include "server/service_helper.h"

class HelperTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// TEST_F(HelperTest, GetDiskCapacity) {
//   std::string path = "/";
//   std::map<std::string, int64_t> output;

//   EXPECT_EQ(true, dingodb::Helper::GetDiskCapacity(path, output));
//   LOG(INFO) << output["TotalSpace"] << " " << output["FreeSpace"];
// }

TEST_F(HelperTest, FormatTime) {
  auto format_time = dingodb::Helper::FormatTime(1681970908, "%Y-%m-%d %H:%M:%S");
  LOG(INFO) << format_time;

  EXPECT_EQ("2023-04-20 14:08:28", format_time);

  auto format_ms_time = dingodb::Helper::FormatMsTime(1681970908001, "%Y-%m-%d %H:%M:%S");
  LOG(INFO) << format_ms_time;

  EXPECT_EQ("2023-04-20 14:08:28.1", format_ms_time);
}

TEST_F(HelperTest, TransformRangeWithOptions) {
  dingodb::pb::common::Range region_range;
  char start_key[] = {0x61, 0x64};
  char end_key[] = {0x78, 0x65};
  region_range.set_start_key(start_key, 2);
  region_range.set_end_key(end_key, 2);

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

TEST_F(HelperTest, ToUpper) {
  std::string value = "hello world";
  EXPECT_EQ("HELLO WORLD", dingodb::Helper::ToUpper(value));
}

TEST_F(HelperTest, ToLower) {
  std::string value = "HELLO WORLD";
  EXPECT_EQ("hello world", dingodb::Helper::ToLower(value));
}

TEST_F(HelperTest, TraverseDirectory) {
  std::string path = "/tmp/unit_test_traverse_directory";

  std::filesystem::create_directories(path);
  std::filesystem::create_directories(fmt::format("{}/a1", path));
  std::filesystem::create_directories(fmt::format("{}/a1/b1", path));
  std::filesystem::create_directories(fmt::format("{}/a1/b2", path));
  std::filesystem::create_directories(fmt::format("{}/a2", path));
  std::filesystem::create_directories(fmt::format("{}/a2/b3", path));

  auto filenames = dingodb::Helper::TraverseDirectory(path);
  for (const auto& filename : filenames) {
    LOG(INFO) << "filename: " << filename;
  }

  std::filesystem::remove_all(path);
  EXPECT_EQ(2, filenames.size());
}

TEST_F(HelperTest, TraverseDirectoryByPrefix) {
  std::string path = "/tmp/unit_test_traverse_directory_prefix";

  std::filesystem::create_directories(path);
  std::filesystem::create_directories(fmt::format("{}/a1", path));
  std::filesystem::create_directories(fmt::format("{}/a1/b1", path));
  std::filesystem::create_directories(fmt::format("{}/a1/b2", path));
  std::filesystem::create_directories(fmt::format("{}/a2", path));
  std::filesystem::create_directories(fmt::format("{}/a2/b3", path));

  auto filenames = dingodb::Helper::TraverseDirectory(path, std::string("a1"));
  for (const auto& filename : filenames) {
    LOG(INFO) << "filename: " << filename;
  }

  std::filesystem::remove_all(path);
  EXPECT_EQ(1, filenames.size());
}

TEST_F(HelperTest, GetSystemMemoryInfo) {
  std::map<std::string, int64_t> output;
  auto ret = dingodb::Helper::GetSystemMemoryInfo(output);
  EXPECT_EQ(true, ret);

  for (auto& it : output) {
    LOG(INFO) << it.first << ": " << it.second;
  }
}

TEST_F(HelperTest, GetSystemCpuUsage) {
  std::map<std::string, int64_t> output;
  auto ret = dingodb::Helper::GetSystemCpuUsage(output);
  EXPECT_EQ(true, ret);

  for (auto& it : output) {
    LOG(INFO) << it.first << ": " << it.second;
  }
}

TEST_F(HelperTest, GetSystemDiskIoUtil) {
  GTEST_SKIP() << "Skip GetSystemDiskIoUtil...";

  std::map<std::string, int64_t> output;
  std::string device_name = "sda";
  auto ret = dingodb::Helper::GetSystemDiskIoUtil(device_name, output);
  EXPECT_EQ(true, ret);

  for (auto& it : output) {
    LOG(INFO) << it.first << ": " << it.second;
  }
}

TEST_F(HelperTest, GetProcessMemoryInfo) {
  std::map<std::string, int64_t> output;
  auto ret = dingodb::Helper::GetProcessMemoryInfo(output);
  EXPECT_EQ(true, ret);

  for (auto& it : output) {
    LOG(INFO) << it.first << ": " << it.second;
  }
}

// TEST_F(HelperTest, CleanFirstSlash) {
//   EXPECT_EQ("", dingodb::Helper::CleanFirstSlash(""));
//   EXPECT_EQ("hello.txt", dingodb::Helper::CleanFirstSlash("hello.txt"));
//   EXPECT_EQ("hello.txt", dingodb::Helper::CleanFirstSlash("/hello.txt"));
// }

TEST_F(HelperTest, PaddingUserKey) {
  std::string a = "abc";
  std::string pa = dingodb::Helper::HexToString("6162630000000000FA");

  EXPECT_EQ(pa, dingodb::Helper::PaddingUserKey(a));
  EXPECT_EQ(a, dingodb::Helper::UnpaddingUserKey(pa));

  a = "aaaabbbb";
  pa = dingodb::Helper::HexToString("6161616162626262FF0000000000000000F7");

  EXPECT_EQ(pa, dingodb::Helper::PaddingUserKey(a));
  EXPECT_EQ(a, dingodb::Helper::UnpaddingUserKey(pa));

  a = "aaaabbbbc";
  pa = dingodb::Helper::HexToString("6161616162626262FF6300000000000000F8");

  EXPECT_EQ(pa, dingodb::Helper::PaddingUserKey(a));
  EXPECT_EQ(a, dingodb::Helper::UnpaddingUserKey(pa));

  a = "aaaabbbbaaaabbbbcc";
  pa = dingodb::Helper::HexToString("6161616162626262FF6161616162626262FF6363000000000000F9");

  EXPECT_EQ(pa, dingodb::Helper::PaddingUserKey(a));
  EXPECT_EQ(a, dingodb::Helper::UnpaddingUserKey(pa));
}

TEST_F(HelperTest, IsContinuous) {
  // Test with an empty set
  std::set<int64_t> empty_set;
  EXPECT_TRUE(dingodb::Helper::IsContinuous(empty_set));

  // Test with a set of continuous numbers
  std::set<int64_t> continuous_set = {1, 2, 3, 4, 5};
  EXPECT_TRUE(dingodb::Helper::IsContinuous(continuous_set));

  // Test with a set of non-continuous numbers
  std::set<int64_t> non_continuous_set = {1, 2, 4, 5};
  EXPECT_FALSE(dingodb::Helper::IsContinuous(non_continuous_set));

  // Test with a large set of continuous numbers
  std::set<int64_t> large_continuous_set;
  for (int64_t i = 0; i < 10000; ++i) {
    large_continuous_set.insert(i);
  }
  EXPECT_TRUE(dingodb::Helper::IsContinuous(large_continuous_set));

  // Test with a large set of non-continuous numbers
  std::set<int64_t> large_non_continuous_set;
  for (int64_t i = 0; i < 10000; ++i) {
    if (i != 5000) {  // Skip one number to make the set non-continuous
      large_non_continuous_set.insert(i);
    }
  }
  EXPECT_FALSE(dingodb::Helper::IsContinuous(large_non_continuous_set));

  // Test with a set of continuous negative numbers
  std::set<int64_t> negative_continuous_set = {-5, -4, -3, -2, -1, 0};
  EXPECT_TRUE(dingodb::Helper::IsContinuous(negative_continuous_set));

  // Test with a set of non-continuous negative numbers
  std::set<int64_t> negative_non_continuous_set = {-5, -4, -2, -1, 0};
  EXPECT_FALSE(dingodb::Helper::IsContinuous(negative_non_continuous_set));

  // Test with a set of continuous numbers that includes both positive and negative numbers
  std::set<int64_t> mixed_continuous_set = {-2, -1, 0, 1, 2};
  EXPECT_TRUE(dingodb::Helper::IsContinuous(mixed_continuous_set));

  // Test with a set of non-continuous numbers that includes both positive and negative numbers
  std::set<int64_t> mixed_non_continuous_set = {-2, -1, 1, 2};
  EXPECT_FALSE(dingodb::Helper::IsContinuous(mixed_non_continuous_set));
}

TEST_F(HelperTest, IsConflictRange) {
  // Test with two ranges that do not intersect
  dingodb::pb::common::Range range1;
  range1.set_start_key("hello");
  range1.set_end_key("hello0000");
  dingodb::pb::common::Range range2;
  range2.set_start_key("hello0000");
  range2.set_end_key("hello00000000");
  EXPECT_FALSE(dingodb::Helper::IsConflictRange(range1, range2));

  // Test with two ranges that intersect
  range2.set_start_key("hello000");
  range2.set_end_key("hello000000");
  EXPECT_TRUE(dingodb::Helper::IsConflictRange(range1, range2));

  // Test with two ranges that are the same
  range2.set_start_key("hello");
  range2.set_end_key("hello0000");
  EXPECT_TRUE(dingodb::Helper::IsConflictRange(range1, range2));

  // Test with two ranges where one is inside the other
  range2.set_start_key("hello0");
  range2.set_end_key("hello000");
  EXPECT_TRUE(dingodb::Helper::IsConflictRange(range1, range2));

  // Test with a large number of ranges
  for (int i = 0; i < 1000; ++i) {
    range1.set_start_key("hello" + std::to_string(i));
    range1.set_end_key("hello" + std::to_string(i + 1));
    range2.set_start_key("hello" + std::to_string(i + 1));
    range2.set_end_key("hello" + std::to_string(i + 2));
    EXPECT_FALSE(dingodb::Helper::IsConflictRange(range1, range2));
  }

  // Test with a large number of ranges that intersect
  for (int i = 900; i < 996; ++i) {
    range1.set_start_key("hello" + std::to_string(i));
    range1.set_end_key("hello" + std::to_string(i + 2));
    range2.set_start_key("hello" + std::to_string(i + 1));
    range2.set_end_key("hello" + std::to_string(i + 3));
    EXPECT_TRUE(dingodb::Helper::IsConflictRange(range1, range2));
  }
}

TEST_F(HelperTest, GetMemComparableRange) {
  std::string start_key = "hello";
  std::string end_key = "hello0000";
  dingodb::pb::common::Range range;
  range.set_start_key(start_key);
  range.set_end_key(end_key);

  auto mem_comparable_range = dingodb::Helper::GetMemComparableRange(range);

  DINGO_LOG(INFO) << "start_key: " << dingodb::Helper::StringToHex(range.start_key()) << " "
                  << dingodb::Helper::StringToHex(mem_comparable_range.start_key());
  DINGO_LOG(INFO) << "end_key: " << dingodb::Helper::StringToHex(range.end_key()) << " "
                  << dingodb::Helper::StringToHex(mem_comparable_range.end_key());

  EXPECT_EQ(dingodb::Helper::PaddingUserKey(range.start_key()), mem_comparable_range.start_key());
  EXPECT_EQ(dingodb::Helper::PaddingUserKey(range.end_key()), mem_comparable_range.end_key());
}

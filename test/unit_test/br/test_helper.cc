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
#include <fstream>
#include <string>
#include <vector>

#include "br/helper.h"
#include "br/utils.h"
#include "fmt/core.h"

class BrHelperTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(BrHelperTest, Ltrim) {
  std::string s = "  abc";
  std::string delete_str = " ";
  std::string result = br::Helper::Ltrim(s, delete_str);
  EXPECT_EQ(result, "abc");
}

TEST_F(BrHelperTest, Rtrim) {
  std::string s = "abc  ";
  std::string delete_str = " ";
  std::string result = br::Helper::Rtrim(s, delete_str);
  EXPECT_EQ(result, "abc");
}

TEST_F(BrHelperTest, Trim) {
  std::string s = "  abc  ";
  std::string delete_str = " ";
  std::string result = br::Helper::Trim(s, delete_str);
  EXPECT_EQ(result, "abc");
}

TEST_F(BrHelperTest, GetRandInt) {
  int result = br::Helper::GetRandInt();
  result = br::Helper::GetRandInt();
  result = br::Helper::GetRandInt();
}

TEST_F(BrHelperTest, StringToEndpoints) {
  std::string str = "127.0.0.1:1025, 127.0.0.1:1026, 127.0.0.1:1027";
  std::vector<butil::EndPoint> end_points = br::Helper::StringToEndpoints(str);
  EXPECT_EQ(end_points.size(), 3);

  str = "127.0.0.1:1025,127.0.0.1:1026,127.0.0.1:1027";
  end_points = br::Helper::StringToEndpoints(str);
  EXPECT_EQ(end_points.size(), 3);
}

TEST_F(BrHelperTest, VectorToEndpoints) {
  std::vector<std::string> addrs;
  addrs.push_back("127.0.0.1:1025");
  addrs.push_back("127.0.0.1:1026");
  addrs.push_back("127.0.0.1:1027");

  std::vector<butil::EndPoint> end_points = br::Helper::VectorToEndpoints(addrs);
  EXPECT_EQ(end_points.size(), 3);

  addrs.clear();
  addrs.push_back(" 127.0.0.1:1025 ");
  addrs.push_back(" 127.0.0.1:1026 ");
  addrs.push_back(" 127.0.0.1:1027 ");

  end_points = br::Helper::VectorToEndpoints(addrs);
  EXPECT_EQ(end_points.size(), 3);
}

TEST_F(BrHelperTest, GetAddrsFromFile) {
  std::string path = "./coor_list";
  std::ofstream writer;
  auto status = br::Utils::CreateFile(writer, path);
  writer << "# dingo-store coordinators" << std::endl;
  writer << "172.30.14.11:32001" << std::endl;
  writer << "172.30.14.11:32002" << std::endl;
  writer << "172.30.14.11:32003" << std::endl;

  std::vector<std::string> addrs = br::Helper::GetAddrsFromFile(path);
  EXPECT_EQ(addrs.size(), 3);

  std::filesystem::remove(path);
}

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
#include <regex>
#include <string>
#include <vector>

#include "common/failpoint.h"

class FailPointTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(FailPointTest, ParseConfig) {
  {
    auto actual = dingodb::FailPoint::ParseConfig("50%10*sleep(10)->60%6*panic");

    auto expect = std::vector<std::string>{"50", "10", "sleep", "10", "60", "6", "panic", ""};

    for (int i = 0; i < expect.size(); ++i) {
      EXPECT_EQ(expect[i], actual[i]);
    }
  }

  {
    auto actual = dingodb::FailPoint::ParseConfig("10*sleep(10)->60%6*panic");

    auto expect = std::vector<std::string>{"", "10", "sleep", "10", "60", "6", "panic", ""};

    for (int i = 0; i < expect.size(); ++i) {
      EXPECT_EQ(expect[i], actual[i]);
    }
  }

  {
    auto actual = dingodb::FailPoint::ParseConfig("50%sleep(10)->60%6*panic");

    auto expect = std::vector<std::string>{"50", "", "sleep", "10", "60", "6", "panic", ""};

    for (int i = 0; i < expect.size(); ++i) {
      EXPECT_EQ(expect[i], actual[i]);
    }
  }

  {
    auto actual = dingodb::FailPoint::ParseConfig("50%10*sleep(10)->60%6*panic->");

    auto expect = std::vector<std::string>{"50", "10", "sleep", "10", "60", "6", "panic", ""};

    for (int i = 0; i < expect.size(); ++i) {
      EXPECT_EQ(expect[i], actual[i]);
    }
  }

  {
    auto actual = dingodb::FailPoint::ParseConfig("50%10*sleep(10)->60%6*print(hello world)");

    auto expect = std::vector<std::string>{"50", "10", "sleep", "10", "60", "6", "print", "hello world"};

    for (int i = 0; i < expect.size(); ++i) {
      EXPECT_EQ(expect[i], actual[i]);
    }
  }
}

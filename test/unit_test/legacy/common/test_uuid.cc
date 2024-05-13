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
#include <vector>

#include "common/helper.h"
#include "common/uuid.h"
#include "fmt/core.h"

class UUIDGeneratorTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(UUIDGeneratorTest, GenerateUUID) {
  // uuid format: xxxx-xx-xx-xx-xxxxxx
  std::string uuid = dingodb::UUIDGenerator::GenerateUUID();
  ASSERT_EQ(36, uuid.size());

  // validate format
  std::vector<std::string> parts;
  dingodb::Helper::SplitString(uuid, '-', parts);
  ASSERT_EQ(5, parts.size());
  ASSERT_EQ(8, parts[0].size());
  ASSERT_EQ(4, parts[1].size());
  ASSERT_EQ(4, parts[2].size());
  ASSERT_EQ(4, parts[3].size());
  ASSERT_EQ(12, parts[4].size());
}

TEST_F(UUIDGeneratorTest, GenerateUUIDV3) {
  // uuid format: xxxx-xx-xx-xx-xxxxxx
  // specified seed
  std::string uuid = dingodb::UUIDGenerator::GenerateUUIDV3("hell world");
  ASSERT_EQ("656c6c20-656c-656c-656c-656c6c20776f", uuid);
}
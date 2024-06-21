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

#include <cstdint>
#include <string>

#include "butil/status.h"
#include "proto/common.pb.h"
#include "server/service_helper.h"

namespace dingodb {  // NOLINT

class ServiceHelperTest : public testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

std::string GenString(std::vector<char> key) { return std::string(key.data(), key.size()); }

dingodb::pb::common::Range GenRange(std::vector<uint8_t> start_key, std::vector<uint8_t> end_key) {
  dingodb::pb::common::Range range;
  range.set_start_key(start_key.data(), start_key.size());
  range.set_end_key(end_key.data(), end_key.size());
  return range;
}

dingodb::pb::common::RangeWithOptions GenRangeWithOptions(std::vector<uint8_t> start_key, bool with_start,
                                                          std::vector<uint8_t> end_key, bool with_end) {
  dingodb::pb::common::RangeWithOptions range;
  range.mutable_range()->set_start_key(start_key.data(), start_key.size());
  range.mutable_range()->set_end_key(end_key.data(), end_key.size());
  range.set_with_start(with_start);
  range.set_with_end(with_end);

  return range;
}

TEST_F(ServiceHelperTest, ValidateRange) {
  EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRange(GenRange({}, {0x78, 0x65})).ok());
  EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRange(GenRange({0x61, 0x64}, {})).ok());

  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRange(GenRange({0x61, 0x64}, {0x78, 0x65})).ok());
  EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRange(GenRange({0x78, 0x65}, {0x61, 0x64})).ok());

  auto range = GenRangeWithOptions(
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x12, 0xff, 0x01, 0x80, 0x00, 0x00, 0x67, 0x01, 0x80, 0x00, 0x00, 0x5a},
      true, {0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x12, 0xff}, true);

  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRange(dingodb::Helper::TransformRangeWithOptions(range)).ok());
}

TEST_F(ServiceHelperTest, ValidateKeyInRange) {
  EXPECT_EQ(
      true,
      dingodb::ServiceHelper::ValidateKeyInRange(GenRange({0x61, 0x64}, {0x78, 0x65}), {GenString({0x61, 0x64})}).ok());

  EXPECT_EQ(true,
            dingodb::ServiceHelper::ValidateKeyInRange(GenRange({0x61, 0x64}, {0x78, 0x65}), {GenString({0x63})}).ok());
  EXPECT_EQ(false,
            dingodb::ServiceHelper::ValidateKeyInRange(GenRange({0x61, 0x64}, {0x78, 0x65}), {GenString({0x61})}).ok());
  EXPECT_EQ(
      false,
      dingodb::ServiceHelper::ValidateKeyInRange(GenRange({0x61, 0x64}, {0x78, 0x65}), {GenString({0x78, 0x65})}).ok());
  EXPECT_EQ(false,
            dingodb::ServiceHelper::ValidateKeyInRange(GenRange({0x61, 0x64}, {0x78, 0x65}), {GenString({0x79})}).ok());
}

TEST_F(ServiceHelperTest, ValidateRangeInRange) {
  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(GenRange({0x61, 0x64}, {0x78, 0x65}),
                                                               GenRange({0x61, 0x64}, {0x78, 0x65}))
                      .ok());
  EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeInRange(GenRange({0x61, 0x64}, {0x78, 0x65}),
                                                                GenRange({0x61, 0x64}, {0x79, 0x65}))
                       .ok());
  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(GenRange({0x60, 0x64}, {0x78, 0x65}),
                                                               GenRange({0x61, 0x64}, {0x78, 0x65}))
                      .ok());
  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(GenRange({0x61, 0x64}, {0x78, 0x65}),
                                                               GenRange({0x63, 0x64}, {0x77, 0x65}))
                      .ok());

  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(GenRange({0x61, 0x64}, {0x78, 0x65}),
                                                               GenRange({0x61, 0x64, 0x11}, {0x78, 0x64, 0x11}))
                      .ok());
  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(GenRange({0x61, 0x64}, {0x78, 0x65}),
                                                               GenRange({0x61, 0x64}, {0x78}))
                      .ok());
  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(
                      GenRange({0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0xd2},
                               {0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0xd3}),
                      GenRange({0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0xd2, 0x01, 0x25, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0xf8, 0x01, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x00, 0x00, 0x00, 0xfb},
                               {0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0xd2, 0x01, 0x25, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0xf8, 0x01, 0x72, 0x6f, 0x6f, 0x74, 0x00, 0x00, 0x00, 0x00, 0xfc}))
                      .ok());

  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeInRange(
                      GenRange({0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x13, 0x5f, 0x33, 0x30, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0xf9, 0xc0, 0x3e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                               {0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x13, 0x5f, 0x36, 0x30, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0xf9, 0xc0, 0x4e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
                      GenRange({0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x13, 0x5f, 0x33, 0x30, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0xf9, 0xc0, 0x3e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                               {0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x13, 0x5f, 0x33, 0x35, 0x00, 0x00, 0x00, 0x00,
                                0x00, 0x00, 0xfa}))
                      .ok());
}

}  // namespace dingodb

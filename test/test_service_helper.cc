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

TEST_F(ServiceHelperTest, ValidateRangeWithOptions) {
  // ok  start_key == end_key and with_start = true and with_end = true
  {
    std::string start_key = "1234";
    std::string end_key = "1234";
    pb::common::RangeWithOptions range;
    range.mutable_range()->set_start_key(start_key);
    range.mutable_range()->set_end_key(end_key);
    range.set_with_start(true);
    range.set_with_end(true);
    butil::Status ok = ServiceHelper::ValidateRangeWithOptions(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // failed  start_key == end_key and with_start = false and with_end = true
  {
    std::string start_key = "1234";
    std::string end_key = "1234";
    pb::common::RangeWithOptions range;
    range.mutable_range()->set_start_key(start_key);
    range.mutable_range()->set_end_key(end_key);
    range.set_with_start(false);
    range.set_with_end(true);
    butil::Status ok = ServiceHelper::ValidateRangeWithOptions(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // failed  start_key == end_key and with_start = true and with_end = false
  {
    std::string start_key = "1234";
    std::string end_key = "1234";
    pb::common::RangeWithOptions range;
    range.mutable_range()->set_start_key(start_key);
    range.mutable_range()->set_end_key(end_key);
    range.set_with_start(true);
    range.set_with_end(false);
    butil::Status ok = ServiceHelper::ValidateRangeWithOptions(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // failed  start_key == end_key and with_start = false and with_end = false
  {
    std::string start_key = "1234";
    std::string end_key = "1234";
    pb::common::RangeWithOptions range;
    range.mutable_range()->set_start_key(start_key);
    range.mutable_range()->set_end_key(end_key);
    range.set_with_start(false);
    range.set_with_end(false);
    butil::Status ok = ServiceHelper::ValidateRangeWithOptions(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // failed  start_key empty
  {
    std::string start_key;
    std::string end_key = "1234";
    pb::common::RangeWithOptions range;
    range.mutable_range()->set_start_key(start_key);
    range.mutable_range()->set_end_key(end_key);
    range.set_with_start(false);
    range.set_with_end(false);
    butil::Status ok = ServiceHelper::ValidateRangeWithOptions(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // failed  end_key empty
  {
    std::string start_key = "1234";
    std::string end_key;
    pb::common::RangeWithOptions range;
    range.mutable_range()->set_start_key(start_key);
    range.mutable_range()->set_end_key(end_key);
    range.set_with_start(false);
    range.set_with_end(false);
    butil::Status ok = ServiceHelper::ValidateRangeWithOptions(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // failed prefix start_key > prefix end_key ignore with_start with_end
  {
    std::string start_key = "1244";
    std::string end_key = "12341";
    pb::common::RangeWithOptions range;
    range.mutable_range()->set_start_key(start_key);
    range.mutable_range()->set_end_key(end_key);
    range.set_with_start(false);
    range.set_with_end(false);
    butil::Status ok = ServiceHelper::ValidateRangeWithOptions(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // ok prefix start_key == prefix end_key ignore with_start with_end
  {
    std::string start_key = "12345";
    std::string end_key = "1234";
    pb::common::RangeWithOptions range;
    range.mutable_range()->set_start_key(start_key);
    range.mutable_range()->set_end_key(end_key);
    range.set_with_start(false);
    range.set_with_end(false);
    butil::Status ok = ServiceHelper::ValidateRangeWithOptions(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok prefix start_key < prefix end_key ignore with_start with_end
  {
    std::string start_key = "12345";
    std::string end_key = "1237";
    pb::common::RangeWithOptions range;
    range.mutable_range()->set_start_key(start_key);
    range.mutable_range()->set_end_key(end_key);
    range.set_with_start(false);
    range.set_with_end(false);
    butil::Status ok = ServiceHelper::ValidateRangeWithOptions(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

std::string GenString(std::vector<char> key) { return std::string(key.data(), key.size()); }

dingodb::pb::common::Range GenRange(std::vector<char> start_key, std::vector<char> end_key) {
  dingodb::pb::common::Range range;
  range.set_start_key(start_key.data(), start_key.size());
  range.set_end_key(end_key.data(), end_key.size());
  return range;
}

dingodb::pb::common::RangeWithOptions GenRangeWithOptions(std::vector<char> start_key, bool with_start,
                                                          std::vector<char> end_key, bool with_end) {
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
}

TEST_F(ServiceHelperTest, ValidateRangeWithOptions2) {
  EXPECT_EQ(false,
            dingodb::ServiceHelper::ValidateRangeWithOptions(GenRangeWithOptions({}, true, {0x78, 0x65}, true)).ok());
  EXPECT_EQ(false,
            dingodb::ServiceHelper::ValidateRangeWithOptions(GenRangeWithOptions({0x61, 0x64}, true, {}, true)).ok());

  EXPECT_EQ(true, dingodb::ServiceHelper::ValidateRangeWithOptions(
                      GenRangeWithOptions({0x61, 0x64}, true, {0x78, 0x65}, true))
                      .ok());
  EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeWithOptions(
                       GenRangeWithOptions({0x78, 0x65}, true, {0x61, 0x64}, true))
                       .ok());

  EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeWithOptions(
                       GenRangeWithOptions({0x61, 0x64}, true, {0x61, 0x64}, false))
                       .ok());
  EXPECT_EQ(false, dingodb::ServiceHelper::ValidateRangeWithOptions(
                       GenRangeWithOptions({0x61, 0x64}, false, {0x61, 0x64}, true))
                       .ok());
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
}

}  // namespace dingodb

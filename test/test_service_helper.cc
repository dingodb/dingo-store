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

}  // namespace dingodb

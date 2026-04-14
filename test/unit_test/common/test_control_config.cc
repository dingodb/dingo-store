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

// Unit tests for Helper::HandleBoolControlConfigVariable covering:
//   [fix][store] Optimized some code.                   (query mode added)
//   [feat][br]   Add a toggle switch for RocksDB disk synchronization.

#include <gtest/gtest.h>

#include <string>

#include "common/helper.h"
#include "proto/common.pb.h"

namespace dingodb {

class HandleBoolControlConfigVariableTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  // Build a ControlConfigVariable with the given name and value.
  static pb::common::ControlConfigVariable MakeVar(const std::string& name, const std::string& value) {
    pb::common::ControlConfigVariable var;
    var.set_name(name);
    var.set_value(value);
    return var;
  }
};

// ============================================================
// Query mode — introduced in [fix][store] Optimized some code.
// ============================================================

TEST_F(HandleBoolControlConfigVariableTest, Query_WhenTrue_ReturnsTrueNoError) {
  pb::common::ControlConfigVariable config;
  bool flag = true;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "query"), config, flag);

  EXPECT_EQ("true", config.value());
  EXPECT_FALSE(config.is_error_occurred());
  // gflags variable must NOT be modified
  EXPECT_TRUE(flag);
}

TEST_F(HandleBoolControlConfigVariableTest, Query_WhenFalse_ReturnsFalseNoError) {
  pb::common::ControlConfigVariable config;
  bool flag = false;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "query"), config, flag);

  EXPECT_EQ("false", config.value());
  EXPECT_FALSE(config.is_error_occurred());
  EXPECT_FALSE(flag);
}

TEST_F(HandleBoolControlConfigVariableTest, Query_UpperCase_Accepted) {
  pb::common::ControlConfigVariable config;
  bool flag = true;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "QUERY"), config, flag);

  EXPECT_EQ("true", config.value());
  EXPECT_FALSE(config.is_error_occurred());
  EXPECT_TRUE(flag);  // not modified
}

TEST_F(HandleBoolControlConfigVariableTest, Query_MixedCase_Accepted) {
  pb::common::ControlConfigVariable config;
  bool flag = false;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "Query"), config, flag);

  EXPECT_EQ("false", config.value());
  EXPECT_FALSE(config.is_error_occurred());
  EXPECT_FALSE(flag);  // not modified
}

TEST_F(HandleBoolControlConfigVariableTest, Query_DoesNotModifyGflagsVar) {
  pb::common::ControlConfigVariable config;
  bool flag = true;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "query"), config, flag);

  // flag must remain true — query is read-only
  EXPECT_TRUE(flag);
}

// ============================================================
// Set to true
// ============================================================

TEST_F(HandleBoolControlConfigVariableTest, SetTrue_WhenAlreadyTrue_IsAlreadySet) {
  pb::common::ControlConfigVariable config;
  bool flag = true;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "true"), config, flag);

  EXPECT_TRUE(flag);
  EXPECT_TRUE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableTest, SetTrue_WhenFalse_SetsToTrue) {
  pb::common::ControlConfigVariable config;
  bool flag = false;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "true"), config, flag);

  EXPECT_TRUE(flag);
  EXPECT_FALSE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableTest, SetTrue_UpperCase_SetsToTrue) {
  pb::common::ControlConfigVariable config;
  bool flag = false;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "TRUE"), config, flag);

  EXPECT_TRUE(flag);
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableTest, SetTrue_One_SetsToTrue) {
  pb::common::ControlConfigVariable config;
  bool flag = false;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "1"), config, flag);

  EXPECT_TRUE(flag);
  EXPECT_FALSE(config.is_error_occurred());
}

// ============================================================
// Set to false
// ============================================================

TEST_F(HandleBoolControlConfigVariableTest, SetFalse_WhenAlreadyFalse_IsAlreadySet) {
  pb::common::ControlConfigVariable config;
  bool flag = false;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "false"), config, flag);

  EXPECT_FALSE(flag);
  EXPECT_TRUE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableTest, SetFalse_WhenTrue_SetsToFalse) {
  pb::common::ControlConfigVariable config;
  bool flag = true;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "false"), config, flag);

  EXPECT_FALSE(flag);
  EXPECT_FALSE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableTest, SetFalse_Zero_SetsToFalse) {
  pb::common::ControlConfigVariable config;
  bool flag = true;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "0"), config, flag);

  EXPECT_FALSE(flag);
  EXPECT_FALSE(config.is_error_occurred());
}

// ============================================================
// Invalid value
// ============================================================

TEST_F(HandleBoolControlConfigVariableTest, InvalidValue_SetsErrorOccurred) {
  pb::common::ControlConfigVariable config;
  bool flag = true;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", "invalid_value"), config, flag);

  EXPECT_TRUE(config.is_error_occurred());
  // gflags variable must NOT be modified on error
  EXPECT_TRUE(flag);
  // current value must be returned even on error
  EXPECT_EQ("true", config.value());
}

TEST_F(HandleBoolControlConfigVariableTest, InvalidValue_EmptyString_SetsErrorOccurred) {
  pb::common::ControlConfigVariable config;
  bool flag = false;

  Helper::HandleBoolControlConfigVariable(MakeVar("FLAGS_raft_sync", ""), config, flag);

  EXPECT_TRUE(config.is_error_occurred());
  EXPECT_FALSE(flag);
  EXPECT_EQ("false", config.value());
}

// ============================================================
// StringConvertTrue / StringConvertFalse (used internally)
// ============================================================

TEST_F(HandleBoolControlConfigVariableTest, StringConvertTrue_AllVariants) {
  EXPECT_TRUE(Helper::StringConvertTrue("true"));
  EXPECT_TRUE(Helper::StringConvertTrue("TRUE"));
  EXPECT_TRUE(Helper::StringConvertTrue("True"));
  EXPECT_TRUE(Helper::StringConvertTrue("1"));
  EXPECT_FALSE(Helper::StringConvertTrue("false"));
  EXPECT_FALSE(Helper::StringConvertTrue("0"));
  EXPECT_FALSE(Helper::StringConvertTrue("query"));
  EXPECT_FALSE(Helper::StringConvertTrue(""));
}

TEST_F(HandleBoolControlConfigVariableTest, StringConvertFalse_AllVariants) {
  EXPECT_TRUE(Helper::StringConvertFalse("false"));
  EXPECT_TRUE(Helper::StringConvertFalse("FALSE"));
  EXPECT_TRUE(Helper::StringConvertFalse("False"));
  EXPECT_TRUE(Helper::StringConvertFalse("0"));
  EXPECT_FALSE(Helper::StringConvertFalse("true"));
  EXPECT_FALSE(Helper::StringConvertFalse("1"));
  EXPECT_FALSE(Helper::StringConvertFalse("query"));
  EXPECT_FALSE(Helper::StringConvertFalse(""));
}

}  // namespace dingodb

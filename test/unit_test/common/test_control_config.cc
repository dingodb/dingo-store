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
#include "gflags/gflags.h"
#include "proto/common.pb.h"

// These flags live at global scope (see coordinator/balance_region.cc and
// config/config_helper.cc). The int64/double handlers apply values through
// google::SetCommandLineOption so registered validators run; tests therefore
// exercise the real flags instead of a detached local variable.
DECLARE_int64(balance_region_default_store_region_size);
DECLARE_double(balance_region_limit_score_diff);

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

// ============================================================
// Int64 / double variants — added for runtime control of the
// balance region knobs (limit_score_diff, default_store_region_size).
// Same contract as the bool handler: "query" reports without touching
// the flag, an unparsable value sets is_error_occurred and keeps the
// flag, an identical value sets is_already_set.
// ============================================================

class HandleInt64ControlConfigVariableTest : public testing::Test {
 protected:
  static pb::common::ControlConfigVariable MakeVar(const std::string& name, const std::string& value) {
    pb::common::ControlConfigVariable var;
    var.set_name(name);
    var.set_value(value);
    return var;
  }
};

TEST_F(HandleInt64ControlConfigVariableTest, Set_UpdatesFlagNoError) {
  const int64_t old_value = FLAGS_balance_region_default_store_region_size;
  pb::common::ControlConfigVariable config;

  Helper::HandleInt64ControlConfigVariable(MakeVar("FLAGS_balance_region_default_store_region_size", "4294967296"),
                                           config, FLAGS_balance_region_default_store_region_size);

  EXPECT_EQ(4294967296LL, FLAGS_balance_region_default_store_region_size);
  EXPECT_FALSE(config.is_error_occurred());
  EXPECT_FALSE(config.is_already_set());

  FLAGS_balance_region_default_store_region_size = old_value;
}

// The RPC path must run the registered gflags validator (NonNegativeInteger):
// a negative size would otherwise silently land in the scheduler.
TEST_F(HandleInt64ControlConfigVariableTest, RpcPathRejectsNegative) {
  const int64_t old_value = FLAGS_balance_region_default_store_region_size;
  pb::common::ControlConfigVariable config;

  Helper::HandleInt64ControlConfigVariable(MakeVar("FLAGS_balance_region_default_store_region_size", "-1"), config,
                                           FLAGS_balance_region_default_store_region_size);

  EXPECT_TRUE(config.is_error_occurred());
  EXPECT_EQ(old_value, FLAGS_balance_region_default_store_region_size);
}

TEST_F(HandleInt64ControlConfigVariableTest, SameValue_SetsAlreadySet) {
  pb::common::ControlConfigVariable config;
  int64_t flag = 5;

  Helper::HandleInt64ControlConfigVariable(MakeVar("FLAGS_x", "5"), config, flag);

  EXPECT_EQ(5, flag);
  EXPECT_TRUE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleInt64ControlConfigVariableTest, Query_ReturnsCurrentWithoutModify) {
  pb::common::ControlConfigVariable config;
  int64_t flag = 5;

  Helper::HandleInt64ControlConfigVariable(MakeVar("FLAGS_x", "query"), config, flag);

  EXPECT_EQ("5", config.value());
  EXPECT_EQ(5, flag);
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleInt64ControlConfigVariableTest, Invalid_SetsErrorKeepsFlag) {
  pb::common::ControlConfigVariable config;
  int64_t flag = 5;

  Helper::HandleInt64ControlConfigVariable(MakeVar("FLAGS_x", "12abc"), config, flag);

  EXPECT_EQ(5, flag);
  EXPECT_TRUE(config.is_error_occurred());
}

class HandleDoubleControlConfigVariableTest : public testing::Test {
 protected:
  static pb::common::ControlConfigVariable MakeVar(const std::string& name, const std::string& value) {
    pb::common::ControlConfigVariable var;
    var.set_name(name);
    var.set_value(value);
    return var;
  }
};

TEST_F(HandleDoubleControlConfigVariableTest, Set_UpdatesFlagNoError) {
  const double old_value = FLAGS_balance_region_limit_score_diff;
  pb::common::ControlConfigVariable config;

  Helper::HandleDoubleControlConfigVariable(MakeVar("FLAGS_balance_region_limit_score_diff", "7.5"), config,
                                            FLAGS_balance_region_limit_score_diff);

  EXPECT_DOUBLE_EQ(7.5, FLAGS_balance_region_limit_score_diff);
  EXPECT_FALSE(config.is_error_occurred());
  EXPECT_FALSE(config.is_already_set());

  FLAGS_balance_region_limit_score_diff = old_value;
}

// The RPC path must run the registered gflags validator (ValidatePositiveDouble):
// zero/negative would defeat the score-diff hysteresis every inspection round.
TEST_F(HandleDoubleControlConfigVariableTest, RpcPathRejectsNonPositive) {
  const double old_value = FLAGS_balance_region_limit_score_diff;
  for (const char* bad : {"0", "-3"}) {
    pb::common::ControlConfigVariable config;
    Helper::HandleDoubleControlConfigVariable(MakeVar("FLAGS_balance_region_limit_score_diff", bad), config,
                                              FLAGS_balance_region_limit_score_diff);
    EXPECT_TRUE(config.is_error_occurred()) << bad;
    EXPECT_DOUBLE_EQ(old_value, FLAGS_balance_region_limit_score_diff) << bad;
  }
}

// strtod happily parses "inf"/"nan"; NaN would make the score-diff comparison
// constantly false and +inf would silently disable balancing, so both must be
// rejected before they reach the flag.
TEST_F(HandleDoubleControlConfigVariableTest, RpcPathRejectsNonFinite) {
  const double old_value = FLAGS_balance_region_limit_score_diff;
  for (const char* bad : {"inf", "-inf", "nan", "nan(123)"}) {
    pb::common::ControlConfigVariable config;
    Helper::HandleDoubleControlConfigVariable(MakeVar("FLAGS_balance_region_limit_score_diff", bad), config,
                                              FLAGS_balance_region_limit_score_diff);
    EXPECT_TRUE(config.is_error_occurred()) << bad;
    EXPECT_DOUBLE_EQ(old_value, FLAGS_balance_region_limit_score_diff) << bad;
  }
}

// A name that is not a registered gflag must be reported as an error instead of
// silently mutating whatever variable the caller happened to pass.
TEST_F(HandleDoubleControlConfigVariableTest, UnknownFlagName_SetsError) {
  pb::common::ControlConfigVariable config;
  double flag = 7.5;

  Helper::HandleDoubleControlConfigVariable(MakeVar("FLAGS_no_such_flag_in_binary", "9.5"), config, flag);

  EXPECT_TRUE(config.is_error_occurred());
  EXPECT_DOUBLE_EQ(7.5, flag);
}

TEST_F(HandleDoubleControlConfigVariableTest, SameValue_SetsAlreadySet) {
  pb::common::ControlConfigVariable config;
  double flag = 7.5;

  Helper::HandleDoubleControlConfigVariable(MakeVar("FLAGS_x", "7.5"), config, flag);

  EXPECT_DOUBLE_EQ(7.5, flag);
  EXPECT_TRUE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleDoubleControlConfigVariableTest, Query_ReturnsCurrentWithoutModify) {
  pb::common::ControlConfigVariable config;
  double flag = 7.5;

  Helper::HandleDoubleControlConfigVariable(MakeVar("FLAGS_x", "query"), config, flag);

  EXPECT_EQ("7.5", config.value());
  EXPECT_DOUBLE_EQ(7.5, flag);
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleDoubleControlConfigVariableTest, Invalid_SetsErrorKeepsFlag) {
  pb::common::ControlConfigVariable config;
  double flag = 7.5;

  Helper::HandleDoubleControlConfigVariable(MakeVar("FLAGS_x", "abc"), config, flag);

  EXPECT_DOUBLE_EQ(7.5, flag);
  EXPECT_TRUE(config.is_error_occurred());
}

}  // namespace dingodb

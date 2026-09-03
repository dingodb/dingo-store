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

// Unit tests for ControlConfig value parsing and registry-based bool updates.

#include <gtest/gtest.h>

#include <string>

#include "common/helper.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"

// These flags live at global scope (see coordinator/balance_region.cc and
// config/config_helper.cc). The int64/double handlers apply values through
// google::SetCommandLineOption so registered validators run; tests therefore
// exercise the real flags instead of detached local variables.
DECLARE_int64(balance_region_default_store_region_size);
DECLARE_double(balance_region_limit_score_diff);

namespace dingodb {

DEFINE_bool(control_config_test_bool, false, "bool flag used by ControlConfig unit tests");
DEFINE_bool(control_config_reject_true, false, "validated bool flag used by ControlConfig unit tests");

static bool RejectTrue(const char*, bool value) { return !value; }
DEFINE_validator(control_config_reject_true, &RejectTrue);

class HandleBoolControlConfigVariableByNameTest : public testing::Test {
 protected:
  void SetUp() override {
    saved_test_bool_ = FLAGS_control_config_test_bool;
    saved_reject_true_ = FLAGS_control_config_reject_true;
    FLAGS_control_config_test_bool = false;
    FLAGS_control_config_reject_true = false;
  }

  void TearDown() override {
    FLAGS_control_config_test_bool = saved_test_bool_;
    FLAGS_control_config_reject_true = saved_reject_true_;
  }

  static pb::common::ControlConfigVariable MakeVar(const std::string& name, const std::string& value) {
    pb::common::ControlConfigVariable variable;
    variable.set_name(name);
    variable.set_value(value);
    return variable;
  }

  static pb::common::ControlConfigVariable Call(const std::string& name, const std::string& value) {
    const auto variable = MakeVar(name, value);
    pb::common::ControlConfigVariable config;
    config.set_name(name);
    config.set_value(value);
    Helper::HandleBoolControlConfigVariableByName(variable, config);
    return config;
  }

  bool saved_test_bool_{};
  bool saved_reject_true_{};
};

TEST_F(HandleBoolControlConfigVariableByNameTest, QueryReturnsCurrentValueWithoutMutation) {
  FLAGS_control_config_test_bool = true;

  const auto config = Call("FLAGS_control_config_test_bool", "query");

  EXPECT_TRUE(FLAGS_control_config_test_bool);
  EXPECT_EQ("true", config.value());
  EXPECT_FALSE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableByNameTest, UnknownFlagReturnsError) {
  const auto config = Call("FLAGS_control_config_missing_bool", "true");

  EXPECT_FALSE(config.is_already_set());
  EXPECT_TRUE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableByNameTest, InvalidValueReturnsCurrentValueAndError) {
  FLAGS_control_config_test_bool = true;

  const auto config = Call("FLAGS_control_config_test_bool", "abc");

  EXPECT_TRUE(FLAGS_control_config_test_bool);
  EXPECT_EQ("true", config.value());
  EXPECT_FALSE(config.is_already_set());
  EXPECT_TRUE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableByNameTest, IdenticalValueIsAlreadySet) {
  FLAGS_control_config_test_bool = true;

  const auto config = Call("FLAGS_control_config_test_bool", "true");

  EXPECT_TRUE(FLAGS_control_config_test_bool);
  EXPECT_TRUE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableByNameTest, NumericValuesUpdateRegisteredFlag) {
  auto config = Call("FLAGS_control_config_test_bool", "1");
  EXPECT_TRUE(FLAGS_control_config_test_bool);
  EXPECT_FALSE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());

  config = Call("FLAGS_control_config_test_bool", "0");
  EXPECT_FALSE(FLAGS_control_config_test_bool);
  EXPECT_FALSE(config.is_already_set());
  EXPECT_FALSE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableByNameTest, ValidatorRejectionReturnsError) {
  const auto config = Call("FLAGS_control_config_reject_true", "true");

  EXPECT_FALSE(FLAGS_control_config_reject_true);
  EXPECT_EQ("false", config.value());
  EXPECT_FALSE(config.is_already_set());
  EXPECT_TRUE(config.is_error_occurred());
}

TEST_F(HandleBoolControlConfigVariableByNameTest, StringConvertTrue_AllVariants) {
  EXPECT_TRUE(Helper::StringConvertTrue("true"));
  EXPECT_TRUE(Helper::StringConvertTrue("TRUE"));
  EXPECT_TRUE(Helper::StringConvertTrue("True"));
  EXPECT_TRUE(Helper::StringConvertTrue("1"));
  EXPECT_FALSE(Helper::StringConvertTrue("false"));
  EXPECT_FALSE(Helper::StringConvertTrue("0"));
  EXPECT_FALSE(Helper::StringConvertTrue("query"));
  EXPECT_FALSE(Helper::StringConvertTrue(""));
}

TEST_F(HandleBoolControlConfigVariableByNameTest, StringConvertFalse_AllVariants) {
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

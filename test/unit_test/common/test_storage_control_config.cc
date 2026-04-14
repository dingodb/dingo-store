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

// Unit tests for Storage::ControlConfig (static method) covering commit:
//   [feat][br] Add a toggle switch for RocksDB disk synchronization (5f38bad3)
//      Added FLAGS_raft_sync dispatch in Storage::ControlConfig
//   [fix][store] Optimized some code (5210ca61)
//      HandleBoolControlConfigVariable with query mode
//
// Storage::ControlConfig is a purely static, network-free method that
// dispatches ControlConfigVariable requests to HandleBoolControlConfigVariable.
// ctx is unused (passed as nullptr in tests).

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "engine/storage.h"
#include "gflags/gflags_declare.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace braft {
DECLARE_bool(raft_sync);
}

namespace dingodb {

// Flags defined in external translation units that we exercise through
// Storage::ControlConfig.
DECLARE_bool(region_enable_auto_split);
DECLARE_bool(region_enable_auto_merge);

// ---------------------------------------------------------------------------
// Helper: build a single ControlConfigVariable protobuf message.
// ---------------------------------------------------------------------------
static pb::common::ControlConfigVariable MakeVar(const std::string& name, const std::string& value) {
  pb::common::ControlConfigVariable v;
  v.set_name(name);
  v.set_value(value);
  return v;
}

// ---------------------------------------------------------------------------
// Fixture: save and restore all flags touched by the tests so that test
// execution order does not matter.
// ---------------------------------------------------------------------------
class StorageControlConfigTest : public testing::Test {
 protected:
  bool saved_auto_split{};
  bool saved_auto_merge{};
  bool saved_raft_sync{};

  void SetUp() override {
    saved_auto_split = FLAGS_region_enable_auto_split;
    saved_auto_merge = FLAGS_region_enable_auto_merge;
    saved_raft_sync = braft::FLAGS_raft_sync;
  }

  void TearDown() override {
    FLAGS_region_enable_auto_split = saved_auto_split;
    FLAGS_region_enable_auto_merge = saved_auto_merge;
    braft::FLAGS_raft_sync = saved_raft_sync;
  }

  // Invoke Storage::ControlConfig with a single variable.
  static pb::store::ControlConfigResponse CallOne(const std::string& name, const std::string& value) {
    pb::store::ControlConfigResponse response;
    const std::vector<pb::common::ControlConfigVariable> vars = {MakeVar(name, value)};
    const butil::Status s = Storage::ControlConfig(nullptr, vars, &response);
    EXPECT_TRUE(s.ok()) << s.error_str();
    return response;
  }
};

// ============================================================
// Basic status: Storage::ControlConfig always returns OK
// ============================================================

TEST_F(StorageControlConfigTest, AlwaysReturnsOkStatus) {
  pb::store::ControlConfigResponse response;
  const std::vector<pb::common::ControlConfigVariable> vars = {MakeVar("FLAGS_region_enable_auto_split", "true")};
  const butil::Status s = Storage::ControlConfig(nullptr, vars, &response);
  EXPECT_TRUE(s.ok());
}

TEST_F(StorageControlConfigTest, EmptyVariableList_ReturnsOkStatus) {
  pb::store::ControlConfigResponse response;
  const std::vector<pb::common::ControlConfigVariable> vars;
  const butil::Status s = Storage::ControlConfig(nullptr, vars, &response);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, response.control_config_variable_size());
}

// ============================================================
// FLAGS_region_enable_auto_split
// ============================================================

TEST_F(StorageControlConfigTest, AutoSplit_SetTrue) {
  FLAGS_region_enable_auto_split = false;
  auto resp = CallOne("FLAGS_region_enable_auto_split", "true");
  ASSERT_EQ(1, resp.control_config_variable_size());
  const auto& cfg = resp.control_config_variable(0);
  EXPECT_EQ("FLAGS_region_enable_auto_split", cfg.name());
  EXPECT_TRUE(FLAGS_region_enable_auto_split) << "flag should have been set to true";
  EXPECT_FALSE(cfg.is_already_set()) << "flag was false, so setting true should not be already_set";
  EXPECT_FALSE(cfg.is_error_occurred());
}

TEST_F(StorageControlConfigTest, AutoSplit_SetFalse) {
  FLAGS_region_enable_auto_split = true;
  auto resp = CallOne("FLAGS_region_enable_auto_split", "false");
  ASSERT_EQ(1, resp.control_config_variable_size());
  EXPECT_FALSE(FLAGS_region_enable_auto_split) << "flag should have been set to false";
  EXPECT_FALSE(resp.control_config_variable(0).is_already_set())
      << "flag was true, so setting false should not be already_set";
  EXPECT_FALSE(resp.control_config_variable(0).is_error_occurred());
}

TEST_F(StorageControlConfigTest, AutoSplit_Query) {
  const bool original = FLAGS_region_enable_auto_split;
  auto resp = CallOne("FLAGS_region_enable_auto_split", "query");
  ASSERT_EQ(1, resp.control_config_variable_size());
  const auto& cfg = resp.control_config_variable(0);
  // After a query the flag must remain unchanged.
  EXPECT_EQ(original, FLAGS_region_enable_auto_split);
  EXPECT_EQ("FLAGS_region_enable_auto_split", cfg.name());
  // Query mode is read-only; is_already_set is not used and is set to false.
  EXPECT_FALSE(cfg.is_already_set());
  EXPECT_EQ(original ? "true" : "false", cfg.value());
  EXPECT_FALSE(cfg.is_error_occurred());
}

// ============================================================
// FLAGS_region_enable_auto_merge
// ============================================================

TEST_F(StorageControlConfigTest, AutoMerge_SetTrue) {
  FLAGS_region_enable_auto_merge = false;
  auto resp = CallOne("FLAGS_region_enable_auto_merge", "true");
  ASSERT_EQ(1, resp.control_config_variable_size());
  EXPECT_TRUE(FLAGS_region_enable_auto_merge);
  EXPECT_FALSE(resp.control_config_variable(0).is_already_set());
  EXPECT_FALSE(resp.control_config_variable(0).is_error_occurred());
}

TEST_F(StorageControlConfigTest, AutoMerge_SetFalse) {
  FLAGS_region_enable_auto_merge = true;
  auto resp = CallOne("FLAGS_region_enable_auto_merge", "false");
  ASSERT_EQ(1, resp.control_config_variable_size());
  EXPECT_FALSE(FLAGS_region_enable_auto_merge);
  EXPECT_FALSE(resp.control_config_variable(0).is_already_set());
  EXPECT_FALSE(resp.control_config_variable(0).is_error_occurred());
}

TEST_F(StorageControlConfigTest, AutoMerge_Query) {
  const bool original = FLAGS_region_enable_auto_merge;
  auto resp = CallOne("FLAGS_region_enable_auto_merge", "query");
  ASSERT_EQ(1, resp.control_config_variable_size());
  EXPECT_EQ(original, FLAGS_region_enable_auto_merge);
  EXPECT_FALSE(resp.control_config_variable(0).is_already_set());
  EXPECT_EQ(original ? "true" : "false", resp.control_config_variable(0).value());
  EXPECT_FALSE(resp.control_config_variable(0).is_error_occurred());
}

// ============================================================
// FLAGS_raft_sync  (commit 5f38bad3 — new variable added to ControlConfig)
// ============================================================

TEST_F(StorageControlConfigTest, RaftSync_SetTrue) {
  braft::FLAGS_raft_sync = false;
  auto resp = CallOne("FLAGS_raft_sync", "true");
  ASSERT_EQ(1, resp.control_config_variable_size());
  EXPECT_TRUE(braft::FLAGS_raft_sync) << "FLAGS_raft_sync should be true after set";
  EXPECT_FALSE(resp.control_config_variable(0).is_already_set());
  EXPECT_FALSE(resp.control_config_variable(0).is_error_occurred());
}

TEST_F(StorageControlConfigTest, RaftSync_SetFalse) {
  braft::FLAGS_raft_sync = true;
  auto resp = CallOne("FLAGS_raft_sync", "false");
  ASSERT_EQ(1, resp.control_config_variable_size());
  EXPECT_FALSE(braft::FLAGS_raft_sync) << "FLAGS_raft_sync should be false after unset";
  EXPECT_FALSE(resp.control_config_variable(0).is_already_set());
  EXPECT_FALSE(resp.control_config_variable(0).is_error_occurred());
}

TEST_F(StorageControlConfigTest, RaftSync_Query_DoesNotMutateFlag) {
  const bool original = braft::FLAGS_raft_sync;
  auto resp = CallOne("FLAGS_raft_sync", "query");
  ASSERT_EQ(1, resp.control_config_variable_size());
  // Flag must remain unchanged after a query.
  EXPECT_EQ(original, braft::FLAGS_raft_sync) << "query must not change FLAGS_raft_sync";
  EXPECT_FALSE(resp.control_config_variable(0).is_already_set());
  EXPECT_EQ(original ? "true" : "false", resp.control_config_variable(0).value());
  EXPECT_FALSE(resp.control_config_variable(0).is_error_occurred());
}

TEST_F(StorageControlConfigTest, RaftSync_QueryCaseInsensitive_Query) {
  // HandleBoolControlConfigVariable accepts "Query" and "QUERY" as well.
  const bool original = braft::FLAGS_raft_sync;
  pb::store::ControlConfigResponse response;
  const std::vector<pb::common::ControlConfigVariable> vars = {MakeVar("FLAGS_raft_sync", "Query")};
  const butil::Status s = Storage::ControlConfig(nullptr, vars, &response);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(original, braft::FLAGS_raft_sync);
}

// ============================================================
// Unknown variable names → is_error_occurred=true, no crash
// ============================================================

TEST_F(StorageControlConfigTest, UnknownVariable_SetsErrorFlag) {
  auto resp = CallOne("FLAGS_nonexistent_variable", "true");
  ASSERT_EQ(1, resp.control_config_variable_size());
  const auto& cfg = resp.control_config_variable(0);
  EXPECT_TRUE(cfg.is_error_occurred()) << "unknown variable should set is_error_occurred";
  EXPECT_FALSE(cfg.is_already_set());
}

TEST_F(StorageControlConfigTest, EmptyVariableName_SetsErrorFlag) {
  auto resp = CallOne("", "true");
  ASSERT_EQ(1, resp.control_config_variable_size());
  EXPECT_TRUE(resp.control_config_variable(0).is_error_occurred());
}

// ============================================================
// Multiple variables in one call
// ============================================================

TEST_F(StorageControlConfigTest, MultipleVariables_AllProcessed) {
  FLAGS_region_enable_auto_split = false;
  FLAGS_region_enable_auto_merge = false;
  braft::FLAGS_raft_sync = false;

  pb::store::ControlConfigResponse response;
  const std::vector<pb::common::ControlConfigVariable> vars = {
      MakeVar("FLAGS_region_enable_auto_split", "true"),
      MakeVar("FLAGS_region_enable_auto_merge", "true"),
      MakeVar("FLAGS_raft_sync", "true"),
  };
  const butil::Status s = Storage::ControlConfig(nullptr, vars, &response);
  EXPECT_TRUE(s.ok());
  ASSERT_EQ(3, response.control_config_variable_size());

  for (int i = 0; i < response.control_config_variable_size(); ++i) {
    EXPECT_FALSE(response.control_config_variable(i).is_error_occurred())
        << "variable[" << i << "] should not have errors";
    EXPECT_FALSE(response.control_config_variable(i).is_already_set())
        << "variable[" << i << "] should not be already_set because it flips from false->true";
  }

  EXPECT_TRUE(FLAGS_region_enable_auto_split);
  EXPECT_TRUE(FLAGS_region_enable_auto_merge);
  EXPECT_TRUE(braft::FLAGS_raft_sync);
}

TEST_F(StorageControlConfigTest, MultipleVariables_MixedValidAndInvalid) {
  FLAGS_region_enable_auto_split = false;

  pb::store::ControlConfigResponse response;
  const std::vector<pb::common::ControlConfigVariable> vars = {
      MakeVar("FLAGS_region_enable_auto_split", "true"),
      MakeVar("FLAGS_invalid_flag", "true"),
  };
  const butil::Status s = Storage::ControlConfig(nullptr, vars, &response);
  EXPECT_TRUE(s.ok());  // always OK even with errors inside
  ASSERT_EQ(2, response.control_config_variable_size());

  EXPECT_FALSE(response.control_config_variable(0).is_already_set());
  EXPECT_FALSE(response.control_config_variable(0).is_error_occurred());

  EXPECT_TRUE(response.control_config_variable(1).is_error_occurred());
  EXPECT_FALSE(response.control_config_variable(1).is_already_set());
}

TEST_F(StorageControlConfigTest, MultipleVariables_ResponsePreservesVariableNames) {
  pb::store::ControlConfigResponse response;
  const std::vector<pb::common::ControlConfigVariable> vars = {
      MakeVar("FLAGS_region_enable_auto_split", "query"),
      MakeVar("FLAGS_region_enable_auto_merge", "query"),
      MakeVar("FLAGS_raft_sync", "query"),
  };
  Storage::ControlConfig(nullptr, vars, &response);
  ASSERT_EQ(3, response.control_config_variable_size());
  EXPECT_EQ("FLAGS_region_enable_auto_split", response.control_config_variable(0).name());
  EXPECT_EQ("FLAGS_region_enable_auto_merge", response.control_config_variable(1).name());
  EXPECT_EQ("FLAGS_raft_sync", response.control_config_variable(2).name());
}

}  // namespace dingodb

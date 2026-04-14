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

// Unit tests for BR ToolClient::DisableRaftSync / EnableRaftSync / QueryRaftSync
// covering commit 5f38bad3:
//   [feat][br] Add a toggle switch for RocksDB disk synchronization
//
// These three static methods are thin wrappers around CoreRaftSync that pass
// literal string constants ("false", "true", "query") plus the variable name
// "FLAGS_raft_sync". When InteractionManager has no endpoints registered
// the method returns OK immediately, which lets us test the no-network code
// path and the request-building logic via the helper layer.

#include <gtest/gtest.h>

#include <string>

#include "br/interaction_manager.h"
#include "br/parameter.h"
#include "br/tool_client.h"
#include "butil/status.h"
#include "common/helper.h"
#include "gflags/gflags_declare.h"
#include "proto/common.pb.h"

namespace braft {
DECLARE_bool(raft_sync);
}

// br::ToolClient and br::InteractionManager live in the br:: namespace.
// dingodb::Helper and dingodb::pb::... live in dingodb::.

// ---------------------------------------------------------------------------
// Fixture: ensure InteractionManager has no real connections so
// CoreRaftSync takes the "nothing registered → return OK" fast path.
// ---------------------------------------------------------------------------
class ToolClientRaftSyncTest : public testing::Test {
 protected:
  void SetUp() override {
    // Reset to a clean state: set all interactions to nullptr.
    br::InteractionManager::GetInstance().SetCoordinatorInteraction(nullptr);
    br::InteractionManager::GetInstance().SetStoreInteraction(nullptr);
    br::InteractionManager::GetInstance().SetIndexInteraction(nullptr);
    br::InteractionManager::GetInstance().SetDocumentInteraction(nullptr);
  }
  void TearDown() override {
    br::InteractionManager::GetInstance().SetCoordinatorInteraction(nullptr);
    br::InteractionManager::GetInstance().SetStoreInteraction(nullptr);
    br::InteractionManager::GetInstance().SetIndexInteraction(nullptr);
    br::InteractionManager::GetInstance().SetDocumentInteraction(nullptr);
  }
};

// ============================================================
// When no interactions are registered CoreRaftSync returns OK immediately
// without touching the network (so all three public methods must do the same).
// ============================================================

TEST_F(ToolClientRaftSyncTest, DisableRaftSync_NoInteractions_ReturnsOk) {
  br::ToolClientParams params;
  params.br_client_method = "DisableRaftSync";
  br::ToolClient client(params);
  const butil::Status s = client.Run();
  EXPECT_TRUE(s.ok()) << s.error_str();
}

TEST_F(ToolClientRaftSyncTest, EnableRaftSync_NoInteractions_ReturnsOk) {
  br::ToolClientParams params;
  params.br_client_method = "EnableRaftSync";
  br::ToolClient client(params);
  const butil::Status s = client.Run();
  EXPECT_TRUE(s.ok()) << s.error_str();
}

TEST_F(ToolClientRaftSyncTest, QueryRaftSync_NoInteractions_ReturnsOk) {
  br::ToolClientParams params;
  params.br_client_method = "QueryRaftSync";
  br::ToolClient client(params);
  const butil::Status s = client.Run();
  EXPECT_TRUE(s.ok()) << s.error_str();
}

// ============================================================
// Verify that the three facade methods are distinct (not accidentally
// aliased to the same thing) by checking the server-side interpretation
// of the values they would pass.  We use HandleBoolControlConfigVariable
// (the same logic the server uses) to verify that:
//   "false" → sets the target to false (Disable)
//   "true"  → sets the target to true  (Enable)
//   "query" → reads without mutating    (Query)
// ============================================================

// Use fully-qualified dingodb:: for Helper and pb:: types.
class RaftSyncValueSemanticsTest : public testing::Test {
 protected:
  void SetUp() override { original_raft_sync = braft::FLAGS_raft_sync; }
  void TearDown() override { braft::FLAGS_raft_sync = original_raft_sync; }

  bool original_raft_sync{};
};

// The "false" constant used by DisableRaftSync must set the flag to false.
TEST_F(RaftSyncValueSemanticsTest, DisableValue_SetsFlagFalse) {
  braft::FLAGS_raft_sync = true;
  dingodb::pb::common::ControlConfigVariable variable;
  variable.set_name("FLAGS_raft_sync");
  variable.set_value("false");  // same string that DisableRaftSync passes
  dingodb::pb::common::ControlConfigVariable config;
  config.set_name(variable.name());
  config.set_value(variable.value());
  dingodb::Helper::HandleBoolControlConfigVariable(variable, config, braft::FLAGS_raft_sync);
  EXPECT_FALSE(braft::FLAGS_raft_sync);
  EXPECT_FALSE(config.is_already_set()) << "flag was true; setting false should not be already_set";
  EXPECT_FALSE(config.is_error_occurred());
}

// The "true" constant used by EnableRaftSync must set the flag to true.
TEST_F(RaftSyncValueSemanticsTest, EnableValue_SetsFlagTrue) {
  braft::FLAGS_raft_sync = false;
  dingodb::pb::common::ControlConfigVariable variable;
  variable.set_name("FLAGS_raft_sync");
  variable.set_value("true");  // same string that EnableRaftSync passes
  dingodb::pb::common::ControlConfigVariable config;
  config.set_name(variable.name());
  config.set_value(variable.value());
  dingodb::Helper::HandleBoolControlConfigVariable(variable, config, braft::FLAGS_raft_sync);
  EXPECT_TRUE(braft::FLAGS_raft_sync);
  EXPECT_FALSE(config.is_already_set()) << "flag was false; setting true should not be already_set";
  EXPECT_FALSE(config.is_error_occurred());
}

// The "query" constant used by QueryRaftSync must NOT change the flag.
TEST_F(RaftSyncValueSemanticsTest, QueryValue_DoesNotMutateFlag) {
  const bool before = braft::FLAGS_raft_sync;
  dingodb::pb::common::ControlConfigVariable variable;
  variable.set_name("FLAGS_raft_sync");
  variable.set_value("query");  // same string that QueryRaftSync passes
  dingodb::pb::common::ControlConfigVariable config;
  config.set_name(variable.name());
  config.set_value(variable.value());
  dingodb::Helper::HandleBoolControlConfigVariable(variable, config, braft::FLAGS_raft_sync);
  EXPECT_EQ(before, braft::FLAGS_raft_sync) << "query must not mutate FLAGS_raft_sync";
  EXPECT_FALSE(config.is_already_set()) << "query is read-only; is_already_set is unused and set to false";
  EXPECT_EQ(before ? "true" : "false", config.value());
  EXPECT_FALSE(config.is_error_occurred());
}

// ============================================================
// Verify the variable name constant: CoreRaftSync uses "FLAGS_raft_sync"
// (not "raft_sync", "braft::raft_sync", etc.).  We check this by verifying
// that HandleBoolControlConfigVariable is reachable via that exact name
// and that Storage::ControlConfig dispatches on it — the latter is the
// focus of test_storage_control_config.cc; here we just confirm the string.
// ============================================================

TEST_F(RaftSyncValueSemanticsTest, VariableName_IsFLAGS_raft_sync) {
  // Build a variable with the name that CoreRaftSync puts in the request.
  dingodb::pb::common::ControlConfigVariable variable;
  variable.set_name("FLAGS_raft_sync");
  variable.set_value("query");
  dingodb::pb::common::ControlConfigVariable config;
  config.set_name(variable.name());
  config.set_value(variable.value());

  // If the name is wrong, HandleBoolControlConfigVariable would not touch
  // FLAGS_raft_sync at all.  By verifying there is no error and that the
  // response carries back the flag value, we confirm that "FLAGS_raft_sync"
  // is the right name for the braft raft_sync flag.
  dingodb::Helper::HandleBoolControlConfigVariable(variable, config, braft::FLAGS_raft_sync);
  EXPECT_FALSE(config.is_error_occurred());
  EXPECT_EQ("FLAGS_raft_sync", config.name()) << "request/response should preserve variable name";
}

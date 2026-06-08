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

// Unit tests for BR ToolClient::DisableRaftMetaForceNoSync /
// EnableRaftMetaForceNoSync / QueryRaftMetaForceNoSync.
//
// Scope and a deliberate limitation:
//   ServerInteraction::SendRequest is a non-virtual template member and
//   ServerInteraction::CreateInteraction is static, so the network broadcast in
//   CoreControlConfig cannot be mocked at the unit-test layer. These tests
//   therefore cover what IS reachable without a live cluster:
//     * the "no endpoints registered -> return OK" fast path of every method;
//     * the --confirm_dangerous / TTY gate that guards EnableRaftMetaForceNoSync.
//   The server-side semantics of the FLAGS_raft_meta_force_no_sync ControlConfig
//   variable (true/false/query, case-insensitive query) are covered end-to-end
//   in test/unit_test/common/test_storage_control_config.cc via
//   Storage::ControlConfig, so they are intentionally NOT duplicated here.
//   The full multi-node broadcast / partial-failure aggregation needs an
//   integration test against a real cluster.

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdio>
#include <string>

#include "br/interaction_manager.h"
#include "br/parameter.h"
#include "br/tool_client.h"
#include "butil/status.h"

// br::ToolClient and br::InteractionManager live in the br:: namespace.

// ---------------------------------------------------------------------------
// Fixture: ensure InteractionManager has no real connections so the methods
// take the "nothing registered -> return OK" fast path, and acknowledge the
// dangerous EnableRaftMetaForceNoSync up front (via --confirm_dangerous) so the
// fast-path tests do not block on / get refused by the confirmation gate. Both
// the interactions and the flag are saved and restored so test ordering does
// not matter.
// ---------------------------------------------------------------------------
class ToolClientRaftMetaForceNoSyncTest : public testing::Test {
 protected:
  void SetUp() override {
    saved_confirm_dangerous = br::FLAGS_confirm_dangerous;
    br::FLAGS_confirm_dangerous = true;
    ClearInteractions();
  }
  void TearDown() override {
    br::FLAGS_confirm_dangerous = saved_confirm_dangerous;
    ClearInteractions();
  }

  static void ClearInteractions() {
    br::InteractionManager::GetInstance().SetCoordinatorInteraction(nullptr);
    br::InteractionManager::GetInstance().SetStoreInteraction(nullptr);
    br::InteractionManager::GetInstance().SetIndexInteraction(nullptr);
    br::InteractionManager::GetInstance().SetDocumentInteraction(nullptr);
  }

  bool saved_confirm_dangerous{};
};

// ============================================================
// When no interactions are registered the methods return OK immediately
// without touching the network.
// ============================================================

TEST_F(ToolClientRaftMetaForceNoSyncTest, DisableRaftMetaForceNoSync_NoInteractions_ReturnsOk) {
  br::ToolClientParams params;
  params.br_client_method = "DisableRaftMetaForceNoSync";
  br::ToolClient client(params);
  const butil::Status s = client.Run();
  EXPECT_TRUE(s.ok()) << s.error_str();
}

TEST_F(ToolClientRaftMetaForceNoSyncTest, EnableRaftMetaForceNoSync_NoInteractions_ReturnsOk) {
  // --confirm_dangerous is set by the fixture, so the gate is satisfied and the
  // method falls through to the no-network fast path.
  br::ToolClientParams params;
  params.br_client_method = "EnableRaftMetaForceNoSync";
  br::ToolClient client(params);
  const butil::Status s = client.Run();
  EXPECT_TRUE(s.ok()) << s.error_str();
}

TEST_F(ToolClientRaftMetaForceNoSyncTest, QueryRaftMetaForceNoSync_NoInteractions_ReturnsOk) {
  br::ToolClientParams params;
  params.br_client_method = "QueryRaftMetaForceNoSync";
  br::ToolClient client(params);
  const butil::Status s = client.Run();
  EXPECT_TRUE(s.ok()) << s.error_str();
}

// ============================================================
// EnableRaftMetaForceNoSync is DANGEROUS (it disables fsync of raft meta). When
// --confirm_dangerous is NOT set and there is no interactive TTY to confirm, it
// must be REFUSED with a non-OK status so scripts/CI cannot silently weaken
// durability. Disable / Query are safe and have no such gate.
// ============================================================

TEST_F(ToolClientRaftMetaForceNoSyncTest, EnableRaftMetaForceNoSync_NonInteractiveWithoutConfirm_Refused) {
  if (isatty(fileno(stdin))) {
    GTEST_SKIP() << "stdin is a TTY; the interactive confirmation path would block and is not unit-testable here.";
  }
  br::FLAGS_confirm_dangerous = false;  // override the fixture default for this test
  br::ToolClientParams params;
  params.br_client_method = "EnableRaftMetaForceNoSync";
  br::ToolClient client(params);
  const butil::Status s = client.Run();
  EXPECT_FALSE(s.ok()) << "Enable without --confirm_dangerous in non-interactive mode must be refused";
}

TEST_F(ToolClientRaftMetaForceNoSyncTest, DisableRaftMetaForceNoSync_NoConfirmNeeded_ReturnsOk) {
  // Disable is the safe direction: it must succeed even without --confirm_dangerous.
  br::FLAGS_confirm_dangerous = false;
  br::ToolClientParams params;
  params.br_client_method = "DisableRaftMetaForceNoSync";
  br::ToolClient client(params);
  const butil::Status s = client.Run();
  EXPECT_TRUE(s.ok()) << s.error_str();
}

TEST_F(ToolClientRaftMetaForceNoSyncTest, QueryRaftMetaForceNoSync_NoConfirmNeeded_ReturnsOk) {
  // Query is read-only: it must succeed even without --confirm_dangerous.
  br::FLAGS_confirm_dangerous = false;
  br::ToolClientParams params;
  params.br_client_method = "QueryRaftMetaForceNoSync";
  br::ToolClient client(params);
  const butil::Status s = client.Run();
  EXPECT_TRUE(s.ok()) << s.error_str();
}

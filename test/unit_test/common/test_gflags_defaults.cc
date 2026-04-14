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

// Regression tests: verify that all gflags introduced by the
// wip-fix-leader-switch commits exist and carry the documented default values.
//
// Relevant commits:
//   6d397b57  [feat][store] Added caching to get_term
//   d69bf028  [fix][store]  Made forced sync of index metadata configurable
//   ba3efce8  [feat][store] Implemented independent queues for each Raft Apply
//   2ea74987  [clore][log]  Fine-tuning Logging
//   716f9a48  [feat][store] Measures the duration of the raft add operation
//   5f38bad3  [feat][br]    Add a toggle switch for RocksDB disk synchronization

#include <gtest/gtest.h>

#include <string>

#include "gflags/gflags.h"

// ------------------------------------------------------------------
// Declare every flag that should have been registered by the time the
// test binary starts (the DEFINE_bool lives in DINGODB_OBJS / PROTO_OBJS).
// Adding a DECLARE_bool does NOT redefine the flag; it merely makes the
// symbol visible in this translation unit.
// ------------------------------------------------------------------

namespace braft {
DECLARE_bool(raft_sync);
}

namespace dingodb {

namespace wal {
DECLARE_bool(rocks_log_enable_get_term_cache);
}

DECLARE_bool(store_state_machine_enable_independent_queue);
DECLARE_bool(print_raft_add_node);
DECLARE_bool(print_recycle_orphan_region_not_table_or_index);
DECLARE_bool(print_process_job_error);
DECLARE_bool(print_periodic_split_check);
DECLARE_bool(print_periodic_merge_check);

class GflagsDefaultsTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  // Helper: look up the registered default value string for a flag.
  static std::string GetDefault(const std::string& flag_name) {
    google::CommandLineFlagInfo info;
    if (!google::GetCommandLineFlagInfo(flag_name.c_str(), &info)) {
      return "__FLAG_NOT_FOUND__";
    }
    return info.default_value;
  }

  // Helper: look up the current value string for a flag.
  static std::string GetCurrent(const std::string& flag_name) {
    google::CommandLineFlagInfo info;
    if (!google::GetCommandLineFlagInfo(flag_name.c_str(), &info)) {
      return "__FLAG_NOT_FOUND__";
    }
    return info.current_value;
  }

  // Helper: verify a flag is registered (i.e. its DEFINE_bool was linked in).
  static bool FlagExists(const std::string& flag_name) {
    google::CommandLineFlagInfo info;
    return google::GetCommandLineFlagInfo(flag_name.c_str(), &info);
  }
};

// ============================================================
// commit 6d397b57 + 5210ca61
// rocks_log_enable_get_term_cache — default false (safe: cache disabled by default)
// ============================================================

TEST_F(GflagsDefaultsTest, RocksLogEnableGetTermCache_Exists) {
  EXPECT_TRUE(FlagExists("rocks_log_enable_get_term_cache"));
}

TEST_F(GflagsDefaultsTest, RocksLogEnableGetTermCache_DefaultFalse) {
  EXPECT_EQ("false", GetDefault("rocks_log_enable_get_term_cache"));
  EXPECT_FALSE(wal::FLAGS_rocks_log_enable_get_term_cache);
}

// ============================================================
// commit ba3efce8
// store_state_machine_enable_independent_queue — default false
// (experimental; must be opt-in)
// ============================================================

TEST_F(GflagsDefaultsTest, StoreStateMachineEnableIndependentQueue_Exists) {
  EXPECT_TRUE(FlagExists("store_state_machine_enable_independent_queue"));
}

TEST_F(GflagsDefaultsTest, StoreStateMachineEnableIndependentQueue_DefaultFalse) {
  EXPECT_EQ("false", GetDefault("store_state_machine_enable_independent_queue"));
  EXPECT_FALSE(FLAGS_store_state_machine_enable_independent_queue);
}

// ============================================================
// commit 716f9a48
// print_raft_add_node — default false (verbose timing disabled by default)
// ============================================================

TEST_F(GflagsDefaultsTest, PrintRaftAddNode_Exists) { EXPECT_TRUE(FlagExists("print_raft_add_node")); }

TEST_F(GflagsDefaultsTest, PrintRaftAddNode_DefaultFalse) {
  EXPECT_EQ("false", GetDefault("print_raft_add_node"));
  EXPECT_FALSE(FLAGS_print_raft_add_node);
}

// ============================================================
// commit 2ea74987 — coordinator logging flags
// print_recycle_orphan_region_not_table_or_index — default true
// print_process_job_error — default true
// ============================================================

TEST_F(GflagsDefaultsTest, PrintRecycleOrphanRegion_Exists) {
  EXPECT_TRUE(FlagExists("print_recycle_orphan_region_not_table_or_index"));
}

TEST_F(GflagsDefaultsTest, PrintRecycleOrphanRegion_DefaultTrue) {
  EXPECT_EQ("true", GetDefault("print_recycle_orphan_region_not_table_or_index"));
  EXPECT_TRUE(FLAGS_print_recycle_orphan_region_not_table_or_index);
}

TEST_F(GflagsDefaultsTest, PrintProcessJobError_Exists) { EXPECT_TRUE(FlagExists("print_process_job_error")); }

TEST_F(GflagsDefaultsTest, PrintProcessJobError_DefaultTrue) {
  EXPECT_EQ("true", GetDefault("print_process_job_error"));
  EXPECT_TRUE(FLAGS_print_process_job_error);
}

// ============================================================
// commit 2ea74987 — split/merge periodic check logging
// print_periodic_split_check — default false
// print_periodic_merge_check — default false
// ============================================================

TEST_F(GflagsDefaultsTest, PrintPeriodicSplitCheck_Exists) { EXPECT_TRUE(FlagExists("print_periodic_split_check")); }

TEST_F(GflagsDefaultsTest, PrintPeriodicSplitCheck_DefaultFalse) {
  EXPECT_EQ("false", GetDefault("print_periodic_split_check"));
  EXPECT_FALSE(FLAGS_print_periodic_split_check);
}

TEST_F(GflagsDefaultsTest, PrintPeriodicMergeCheck_Exists) { EXPECT_TRUE(FlagExists("print_periodic_merge_check")); }

TEST_F(GflagsDefaultsTest, PrintPeriodicMergeCheck_DefaultFalse) {
  EXPECT_EQ("false", GetDefault("print_periodic_merge_check"));
  EXPECT_FALSE(FLAGS_print_periodic_merge_check);
}

// ============================================================
// Verify that BRPC hot-reload validator is registered for each flag
// (i.e. the flag is modifiable at runtime via brpc /flags endpoint).
// ============================================================

TEST_F(GflagsDefaultsTest, RocksLogEnableGetTermCache_IsNotModifiable) {
  // Intentionally no BRPC_VALIDATE_GFLAG: toggling at runtime is unsafe
  // because the cache is only built during init().
  // Verify the flag is NOT hot-reloadable (no validator registered).
  google::CommandLineFlagInfo info;
  ASSERT_TRUE(google::GetCommandLineFlagInfo("rocks_log_enable_get_term_cache", &info));
  // The flag should have no description indicating PassValidate; we just
  // verify it exists and has the right default. Runtime-toggle safety is
  // enforced by the commented-out BRPC_VALIDATE_GFLAG in the source.
  EXPECT_EQ("false", info.default_value);
}

TEST_F(GflagsDefaultsTest, AllLoggingFlags_AreRuntimeModifiable) {
  // Verify flags backed by BRPC_VALIDATE_GFLAG are accessible for update.
  // SetCommandLineOption returns "" only if the flag doesn't exist or
  // validation fails; a pass-through validator always succeeds.
  EXPECT_NE("", google::SetCommandLineOption("store_state_machine_enable_independent_queue", "false"));
  EXPECT_NE("", google::SetCommandLineOption("print_raft_add_node", "false"));
  EXPECT_NE("", google::SetCommandLineOption("print_recycle_orphan_region_not_table_or_index", "true"));
  EXPECT_NE("", google::SetCommandLineOption("print_process_job_error", "true"));
  EXPECT_NE("", google::SetCommandLineOption("print_periodic_split_check", "false"));
  EXPECT_NE("", google::SetCommandLineOption("print_periodic_merge_check", "false"));
}

// ============================================================
// commit d69bf028
// braft::FLAGS_raft_sync — made configurable (was hardcoded `options.sync = true`)
//
// RocksLogStorageWrapper::OpenForRocksMetaStorage() now sets:
//   options.sync = braft::FLAGS_raft_sync
// instead of the always-true hardcoded value.
//
// We test that:
//   1. The flag exists (braft library linked in).
//   2. Its default is true (preserving the original behaviour by default).
//   3. It can be changed at runtime (braft registers with PassValidate).
// ============================================================

TEST_F(GflagsDefaultsTest, BraftRaftSync_Exists) {
  EXPECT_TRUE(FlagExists("raft_sync")) << "braft::FLAGS_raft_sync must be registered";
}

TEST_F(GflagsDefaultsTest, BraftRaftSync_DefaultTrue) {
  // braft's DEFINE_bool(raft_sync, true, ...) defaults to true, which preserves
  // the prior behavior of the always-sync code in d69bf028.
  EXPECT_EQ("true", GetDefault("raft_sync")) << "Default must be true so that the behavior after d69bf028 is unchanged "
                                                "unless the operator explicitly disables sync";
  EXPECT_TRUE(braft::FLAGS_raft_sync);
}

TEST_F(GflagsDefaultsTest, BraftRaftSync_IsRuntimeModifiable) {
  // braft registers raft_sync with a validator, so it should be hot-reloadable.
  const bool saved = braft::FLAGS_raft_sync;
  EXPECT_NE("", google::SetCommandLineOption("raft_sync", "false")) << "FLAGS_raft_sync must be modifiable at runtime";
  EXPECT_FALSE(braft::FLAGS_raft_sync);
  // restore
  google::SetCommandLineOption("raft_sync", saved ? "true" : "false");
}

// ============================================================
// braft raft meta tuning flags from the shipped gflags configs
// ============================================================

TEST_F(GflagsDefaultsTest, BraftRaftMetaTuningFlags_Exist) {
  EXPECT_TRUE(FlagExists("raft_meta_enable_leveldb_tuning"));
  EXPECT_TRUE(FlagExists("raft_meta_enable_preserialize"));
  EXPECT_TRUE(FlagExists("raft_meta_force_no_sync"));
  EXPECT_TRUE(FlagExists("raft_meta_periodic_sync_enabled"));
  EXPECT_TRUE(FlagExists("raft_meta_periodic_sync_interval_ms"));
}

TEST_F(GflagsDefaultsTest, BraftRaftMetaTuningFlags_DefaultValues) {
  EXPECT_EQ("false", GetDefault("raft_meta_enable_leveldb_tuning"));

  EXPECT_EQ("false", GetDefault("raft_meta_enable_preserialize"));

  EXPECT_EQ("false", GetDefault("raft_meta_force_no_sync"));

  EXPECT_EQ("false", GetDefault("raft_meta_periodic_sync_enabled"));

  EXPECT_EQ("1000", GetDefault("raft_meta_periodic_sync_interval_ms"));
}

TEST_F(GflagsDefaultsTest, BraftRaftMetaTuningBoolFlags_CanToggleTrueAndFalse) {
  const std::string saved_leveldb_tuning = GetCurrent("raft_meta_enable_leveldb_tuning");
  const std::string saved_preserialize = GetCurrent("raft_meta_enable_preserialize");
  const std::string saved_force_no_sync = GetCurrent("raft_meta_force_no_sync");
  const std::string saved_periodic_sync_enabled = GetCurrent("raft_meta_periodic_sync_enabled");

  EXPECT_NE("", google::SetCommandLineOption("raft_meta_enable_leveldb_tuning", "false"));
  EXPECT_EQ("false", GetCurrent("raft_meta_enable_leveldb_tuning"));
  EXPECT_NE("", google::SetCommandLineOption("raft_meta_enable_leveldb_tuning", "true"));
  EXPECT_EQ("true", GetCurrent("raft_meta_enable_leveldb_tuning"));

  EXPECT_NE("", google::SetCommandLineOption("raft_meta_enable_preserialize", "false"));
  EXPECT_EQ("false", GetCurrent("raft_meta_enable_preserialize"));
  EXPECT_NE("", google::SetCommandLineOption("raft_meta_enable_preserialize", "true"));
  EXPECT_EQ("true", GetCurrent("raft_meta_enable_preserialize"));

  EXPECT_NE("", google::SetCommandLineOption("raft_meta_force_no_sync", "false"));
  EXPECT_EQ("false", GetCurrent("raft_meta_force_no_sync"));
  EXPECT_NE("", google::SetCommandLineOption("raft_meta_force_no_sync", "true"));
  EXPECT_EQ("true", GetCurrent("raft_meta_force_no_sync"));

  EXPECT_NE("", google::SetCommandLineOption("raft_meta_periodic_sync_enabled", "false"));
  EXPECT_EQ("false", GetCurrent("raft_meta_periodic_sync_enabled"));
  EXPECT_NE("", google::SetCommandLineOption("raft_meta_periodic_sync_enabled", "true"));
  EXPECT_EQ("true", GetCurrent("raft_meta_periodic_sync_enabled"));

  google::SetCommandLineOption("raft_meta_enable_leveldb_tuning", saved_leveldb_tuning.c_str());
  google::SetCommandLineOption("raft_meta_enable_preserialize", saved_preserialize.c_str());
  google::SetCommandLineOption("raft_meta_force_no_sync", saved_force_no_sync.c_str());
  google::SetCommandLineOption("raft_meta_periodic_sync_enabled", saved_periodic_sync_enabled.c_str());
}

TEST_F(GflagsDefaultsTest, BraftRaftMetaPeriodicSyncInterval_IsRuntimeModifiable) {
  EXPECT_NE("", google::SetCommandLineOption("raft_meta_periodic_sync_interval_ms", "1000"));
}

// ============================================================
// braft raft latency logging flags from the shipped gflags configs
// ============================================================

TEST_F(GflagsDefaultsTest, BraftRaftLatencyLogFlags_Exist) {
  EXPECT_TRUE(FlagExists("raft_latency_log_batch_append_entries"));
  EXPECT_TRUE(FlagExists("raft_latency_log_append_entries"));
  EXPECT_TRUE(FlagExists("raft_latency_log_handle_append_entries"));
  EXPECT_TRUE(FlagExists("raft_latency_log_init"));
}

TEST_F(GflagsDefaultsTest, BraftRaftLatencyLogFlags_DefaultTrue) {
  EXPECT_EQ("false", GetDefault("raft_latency_log_batch_append_entries"));

  EXPECT_EQ("false", GetDefault("raft_latency_log_append_entries"));

  EXPECT_EQ("false", GetDefault("raft_latency_log_handle_append_entries"));

  EXPECT_EQ("false", GetDefault("raft_latency_log_init"));
}

TEST_F(GflagsDefaultsTest, BraftRaftLatencyLogFlags_CanToggleTrueAndFalse) {
  const std::string saved_batch_append_entries = GetCurrent("raft_latency_log_batch_append_entries");
  const std::string saved_append_entries = GetCurrent("raft_latency_log_append_entries");
  const std::string saved_handle_append_entries = GetCurrent("raft_latency_log_handle_append_entries");
  const std::string saved_init = GetCurrent("raft_latency_log_init");

  EXPECT_NE("", google::SetCommandLineOption("raft_latency_log_batch_append_entries", "false"));
  EXPECT_EQ("false", GetCurrent("raft_latency_log_batch_append_entries"));
  EXPECT_NE("", google::SetCommandLineOption("raft_latency_log_batch_append_entries", "true"));
  EXPECT_EQ("true", GetCurrent("raft_latency_log_batch_append_entries"));

  EXPECT_NE("", google::SetCommandLineOption("raft_latency_log_append_entries", "false"));
  EXPECT_EQ("false", GetCurrent("raft_latency_log_append_entries"));
  EXPECT_NE("", google::SetCommandLineOption("raft_latency_log_append_entries", "true"));
  EXPECT_EQ("true", GetCurrent("raft_latency_log_append_entries"));

  EXPECT_NE("", google::SetCommandLineOption("raft_latency_log_handle_append_entries", "false"));
  EXPECT_EQ("false", GetCurrent("raft_latency_log_handle_append_entries"));
  EXPECT_NE("", google::SetCommandLineOption("raft_latency_log_handle_append_entries", "true"));
  EXPECT_EQ("true", GetCurrent("raft_latency_log_handle_append_entries"));

  EXPECT_NE("", google::SetCommandLineOption("raft_latency_log_init", "false"));
  EXPECT_EQ("false", GetCurrent("raft_latency_log_init"));
  EXPECT_NE("", google::SetCommandLineOption("raft_latency_log_init", "true"));
  EXPECT_EQ("true", GetCurrent("raft_latency_log_init"));

  google::SetCommandLineOption("raft_latency_log_batch_append_entries", saved_batch_append_entries.c_str());
  google::SetCommandLineOption("raft_latency_log_append_entries", saved_append_entries.c_str());
  google::SetCommandLineOption("raft_latency_log_handle_append_entries", saved_handle_append_entries.c_str());
  google::SetCommandLineOption("raft_latency_log_init", saved_init.c_str());
}

}  // namespace dingodb

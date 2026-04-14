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

// Regression tests for commit ef81b9f2:
//   "Update configuration file to resolve Leader switching issues."
//
// That commit added / updated entries in all four conf files:
//   conf/coordinator-gflags.conf
//   conf/document-gflags.conf
//   conf/index-gflags.conf
//   conf/store-gflags.conf
//
// Each test reads the conf file as plain text and verifies that:
//   * Required entries are present with the correct values.
//   * Key values that were CHANGED (braft_use_align_hearbeat: true→false) are
//     at their new values.
//   * Files are mutually consistent for flags that must match across all roles.
//
// Conf-file format: one flag per line, leading dash, e.g. "-flagname=value"
// Blank lines and lines not starting with '-' are ignored.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

namespace dingodb {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Compute the project root directory from the path of *this* source file.
// __FILE__ == .../dingo-store/test/unit_test/common/test_conf_files.cc
// Up 4 levels gives .../dingo-store/
static std::filesystem::path ProjectRoot() {
  const std::filesystem::path src = std::filesystem::path(__FILE__);
  return src
      .parent_path()   // common/
      .parent_path()   // unit_test/
      .parent_path()   // test/
      .parent_path();  // dingo-store/
}

// Parse a gflags conf file into a map<name, value>.
// Lines have the form: -name=value
// Lines not starting with '-' are skipped.
static std::map<std::string, std::string> ParseConfFile(const std::filesystem::path& path) {
  std::map<std::string, std::string> result;
  std::ifstream f(path);
  if (!f.is_open()) {
    return result;
  }
  std::string line;
  while (std::getline(f, line)) {
    if (line.empty() || line[0] != '-') continue;
    auto eq = line.find('=');
    if (eq == std::string::npos) continue;
    const std::string name = line.substr(1, eq - 1);  // strip leading '-'
    const std::string value = line.substr(eq + 1);
    result[name] = value;
  }
  return result;
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

class GflagsConfFileTest : public testing::Test {
 protected:
  std::filesystem::path conf_dir;

  void SetUp() override { conf_dir = ProjectRoot() / "conf"; }

  // Verify that the conf file at `conf_dir/filename` can be opened.
  bool ConfFileExists(const std::string& filename) { return std::filesystem::exists(conf_dir / filename); }

  std::map<std::string, std::string> Read(const std::string& filename) { return ParseConfFile(conf_dir / filename); }

  // Assert that `map` contains `key` with expected `value`.
  static void ExpectEntry(const std::map<std::string, std::string>& m, const std::string& key,
                          const std::string& expected_value, const std::string& context) {
    auto it = m.find(key);
    ASSERT_NE(m.end(), it) << context << ": entry '" << key << "' not found";
    EXPECT_EQ(expected_value, it->second) << context << ": wrong value for '" << key << "'";
  }
};

// ============================================================
// Verify the conf files exist (sanity check)
// ============================================================

TEST_F(GflagsConfFileTest, AllFourConfFilesExist) {
  EXPECT_TRUE(ConfFileExists("coordinator-gflags.conf")) << conf_dir;
  EXPECT_TRUE(ConfFileExists("document-gflags.conf")) << conf_dir;
  EXPECT_TRUE(ConfFileExists("index-gflags.conf")) << conf_dir;
  EXPECT_TRUE(ConfFileExists("store-gflags.conf")) << conf_dir;
}

// ============================================================
// braft_use_align_hearbeat changed from true → false in ef81b9f2
// This is the primary bug-fix: misaligned heartbeats caused leader switches.
// ============================================================

TEST_F(GflagsConfFileTest, CoordinatorConf_BraftAlignHearbeat_IsFalse) {
  if (!ConfFileExists("coordinator-gflags.conf")) GTEST_SKIP();
  auto m = Read("coordinator-gflags.conf");
  ExpectEntry(m, "braft_use_align_hearbeat", "false", "coordinator-gflags.conf");
}

TEST_F(GflagsConfFileTest, DocumentConf_BraftAlignHearbeat_IsFalse) {
  if (!ConfFileExists("document-gflags.conf")) GTEST_SKIP();
  auto m = Read("document-gflags.conf");
  ExpectEntry(m, "braft_use_align_hearbeat", "false", "document-gflags.conf");
}

TEST_F(GflagsConfFileTest, IndexConf_BraftAlignHearbeat_IsFalse) {
  if (!ConfFileExists("index-gflags.conf")) GTEST_SKIP();
  auto m = Read("index-gflags.conf");
  ExpectEntry(m, "braft_use_align_hearbeat", "false", "index-gflags.conf");
}

TEST_F(GflagsConfFileTest, StoreConf_BraftAlignHearbeat_IsFalse) {
  if (!ConfFileExists("store-gflags.conf")) GTEST_SKIP();
  auto m = Read("store-gflags.conf");
  ExpectEntry(m, "braft_use_align_hearbeat", "false", "store-gflags.conf");
}

// ============================================================
// raft_sync=true must be explicitly set in all 4 conf files
// (ef81b9f2 added this entry; d69bf028 made sync code configurable)
// ============================================================

TEST_F(GflagsConfFileTest, AllConfFiles_RaftSyncIsTrue) {
  for (const auto& name :
       {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf", "store-gflags.conf"}) {
    if (!ConfFileExists(name)) continue;
    auto m = Read(name);
    ExpectEntry(m, "raft_sync", "true", name);
  }
}

// ============================================================
// rocks_log_enable_get_term_cache=true in deployment conf
// (default in code is false; deployment enables it for performance)
// ============================================================

TEST_F(GflagsConfFileTest, AllConfFiles_GetTermCacheEnabled) {
  for (const auto& name :
       {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf", "store-gflags.conf"}) {
    if (!ConfFileExists(name)) continue;
    auto m = Read(name);
    ExpectEntry(m, "rocks_log_enable_get_term_cache", "true", name);
  }
}

// ============================================================
// store_state_machine_enable_independent_queue=false
// Experimental feature — must remain disabled in deployment conf.
// ============================================================

TEST_F(GflagsConfFileTest, AllConfFiles_IndependentQueueDisabled) {
  for (const auto& name :
       {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf", "store-gflags.conf"}) {
    if (!ConfFileExists(name)) continue;
    auto m = Read(name);
    ExpectEntry(m, "store_state_machine_enable_independent_queue", "false", name);
  }
}

// ============================================================
// print_recycle_orphan_region_not_table_or_index=false
// Code default is true; deployment overrides to false to reduce log noise.
// ============================================================

TEST_F(GflagsConfFileTest, AllConfFiles_RecycleOrphanPrintDisabled) {
  for (const auto& name :
       {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf", "store-gflags.conf"}) {
    if (!ConfFileExists(name)) continue;
    auto m = Read(name);
    ExpectEntry(m, "print_recycle_orphan_region_not_table_or_index", "false", name);
  }
}

// ============================================================
// print_process_job_error=true
// ============================================================

TEST_F(GflagsConfFileTest, AllConfFiles_PrintProcessJobErrorEnabled) {
  for (const auto& name :
       {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf", "store-gflags.conf"}) {
    if (!ConfFileExists(name)) continue;
    auto m = Read(name);
    ExpectEntry(m, "print_process_job_error", "true", name);
  }
}

// ============================================================
// print_periodic_merge_check=false and print_periodic_split_check=false
// ============================================================

TEST_F(GflagsConfFileTest, AllConfFiles_PeriodicCheckPrintDisabled) {
  for (const auto& name :
       {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf", "store-gflags.conf"}) {
    if (!ConfFileExists(name)) continue;
    auto m = Read(name);
    ExpectEntry(m, "print_periodic_merge_check", "false", name);
    ExpectEntry(m, "print_periodic_split_check", "false", name);
  }
}

// ============================================================
// print_raft_add_node=true in deployment
// (code default is false; deployment enables timing for diagnostics)
// ============================================================

TEST_F(GflagsConfFileTest, AllConfFiles_PrintRaftAddNodeEnabled) {
  for (const auto& name :
       {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf", "store-gflags.conf"}) {
    if (!ConfFileExists(name)) continue;
    auto m = Read(name);
    ExpectEntry(m, "print_raft_add_node", "true", name);
  }
}

// ============================================================
// Raft meta storage / latency logging knobs added in ef81b9f2
// ============================================================

TEST_F(GflagsConfFileTest, AllConfFiles_RaftMetaAndLatencyFlagsPresent) {
  for (const auto& name :
       {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf", "store-gflags.conf"}) {
    if (!ConfFileExists(name)) continue;
    auto m = Read(name);

    ExpectEntry(m, "raft_meta_enable_leveldb_tuning", "true", name);
    ExpectEntry(m, "raft_meta_enable_preserialize", "true", name);
    ExpectEntry(m, "raft_meta_force_no_sync", "true", name);
    ExpectEntry(m, "raft_meta_periodic_sync_enabled", "false", name);
    ExpectEntry(m, "raft_meta_periodic_sync_interval_ms", "1000", name);

    ExpectEntry(m, "raft_latency_log_batch_append_entries", "true", name);
    ExpectEntry(m, "raft_latency_log_append_entries", "true", name);
    ExpectEntry(m, "raft_latency_log_handle_append_entries", "true", name);
    ExpectEntry(m, "raft_latency_log_init", "true", name);
    ExpectEntry(m, "raft_latency_log_threshold_ms", "1000", name);
  }
}

TEST_F(GflagsConfFileTest, AllConfFiles_RestoreLimitsPresent) {
  for (const auto& name :
       {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf", "store-gflags.conf"}) {
    if (!ConfFileExists(name)) continue;
    auto m = Read(name);
    ExpectEntry(m, "max_restore_data_memory_size", "10485760", name);
    ExpectEntry(m, "max_restore_count", "32768", name);
  }
}

// ============================================================
// Cross-file consistency: all 4 conf files must agree on every
// flag that was added/changed by ef81b9f2.
// ============================================================

TEST_F(GflagsConfFileTest, AllConfFiles_NewFlagsAreConsistentAcrossRoles) {
  const std::vector<std::string> files = {"coordinator-gflags.conf", "document-gflags.conf", "index-gflags.conf",
                                          "store-gflags.conf"};
  const std::vector<std::string> flags = {
      "braft_use_align_hearbeat",
      "raft_sync",
      "rocks_log_enable_get_term_cache",
      "store_state_machine_enable_independent_queue",
      "print_recycle_orphan_region_not_table_or_index",
      "print_process_job_error",
      "print_periodic_merge_check",
      "print_periodic_split_check",
      "print_raft_add_node",
      "raft_meta_enable_leveldb_tuning",
      "raft_meta_enable_preserialize",
      "raft_meta_force_no_sync",
      "raft_meta_periodic_sync_enabled",
      "raft_meta_periodic_sync_interval_ms",
      "raft_latency_log_batch_append_entries",
      "raft_latency_log_append_entries",
      "raft_latency_log_handle_append_entries",
      "raft_latency_log_init",
      "raft_latency_log_threshold_ms",
      "max_restore_data_memory_size",
      "max_restore_count",
  };

  // Read all files that exist.
  std::map<std::string, std::map<std::string, std::string>> all_maps;
  for (const auto& f : files) {
    if (ConfFileExists(f)) {
      all_maps[f] = Read(f);
    }
  }
  if (all_maps.empty()) GTEST_SKIP();

  // For each flag, verify all files agree.
  for (const auto& flag : flags) {
    std::string reference_value;
    std::string reference_file;
    for (const auto& [filename, m] : all_maps) {
      auto it = m.find(flag);
      if (it == m.end()) continue;
      if (reference_file.empty()) {
        reference_value = it->second;
        reference_file = filename;
      } else {
        EXPECT_EQ(reference_value, it->second) << "Flag '" << flag << "' mismatch: " << reference_file << "="
                                               << reference_value << " vs " << filename << "=" << it->second;
      }
    }
  }
}

}  // namespace dingodb

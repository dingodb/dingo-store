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

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/constant.h"
#include "common/helper.h"
#include "common/role.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "engine/txn_auto_compaction_checker.h"
#include "engine/txn_mvcc_properties_collector.h"
#include "fmt/core.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

DECLARE_bool(gc_enable_compaction_filter);
DECLARE_bool(gc_enable_safe_point_read_check);
DECLARE_int64(gc_auto_compaction_min_tombstones);
DECLARE_int32(gc_auto_compaction_tombstones_percent);
DECLARE_int64(gc_auto_compaction_min_redundant_versions);
DECLARE_int32(gc_auto_compaction_redundant_versions_percent);

// ---------------- pure scoring logic ----------------

using Checker = TxnAutoCompactionChecker;

TEST(TxnAutoCompactionScoreTest, InterpolateCountBounds) {
  // no data / absent ts bounds
  EXPECT_EQ(Checker::InterpolateCount(0, 10, 20, 15), 0);
  EXPECT_EQ(Checker::InterpolateCount(100, 0, 20, 15), 0);
  EXPECT_EQ(Checker::InterpolateCount(100, 10, 0, 15), 0);
  EXPECT_EQ(Checker::InterpolateCount(100, 20, 10, 15), 0);
  // safe point below / above the whole segment
  EXPECT_EQ(Checker::InterpolateCount(100, 10, 20, 9), 0);
  EXPECT_EQ(Checker::InterpolateCount(100, 10, 20, 20), 100);
  EXPECT_EQ(Checker::InterpolateCount(100, 10, 20, 1000), 100);
  // single-ts segment fully below the safe point counts (unlike TiKV)
  EXPECT_EQ(Checker::InterpolateCount(7, 15, 15, 20), 7);
  EXPECT_EQ(Checker::InterpolateCount(7, 15, 15, 14), 0);
  // linear interpolation in between
  EXPECT_EQ(Checker::InterpolateCount(100, 10, 20, 15), 50);
  EXPECT_EQ(Checker::InterpolateCount(100, 10, 20, 10), 0);
  EXPECT_EQ(Checker::InterpolateCount(100, 10, 20, 19), 90);
}

TEST(TxnAutoCompactionScoreTest, EstimateDiscardableAddsBothSegments) {
  TxnMvccProperties mvcc;
  mvcc.num_rows = 10;
  mvcc.num_versions = 110;  // 100 shadowed versions
  mvcc.oldest_stale_version_ts = 10;
  mvcc.newest_stale_version_ts = 20;
  mvcc.num_deletes = 40;
  mvcc.oldest_delete_ts = 30;
  mvcc.newest_delete_ts = 40;

  // sp=15: half the stale segment, none of the delete segment
  EXPECT_EQ(Checker::EstimateDiscardable(mvcc, 15), 50);
  // sp=35: full stale segment + half the delete segment
  EXPECT_EQ(Checker::EstimateDiscardable(mvcc, 35), 120);
  // sp beyond everything: all of both
  EXPECT_EQ(Checker::EstimateDiscardable(mvcc, 1000), 140);
}

class TxnAutoCompactionScoreFlagTest : public testing::Test {
 protected:
  void SetUp() override {
    FLAGS_gc_auto_compaction_min_tombstones = 10000;
    FLAGS_gc_auto_compaction_tombstones_percent = 30;
    FLAGS_gc_auto_compaction_min_redundant_versions = 50000;
    FLAGS_gc_auto_compaction_redundant_versions_percent = 20;
  }
};

TEST_F(TxnAutoCompactionScoreFlagTest, FilterOffUsesTombstoneRule) {
  TxnMvccRangeStats stats;
  stats.total_entries = 100000;
  stats.num_files_with_props = 1;

  // below both thresholds -> 0
  stats.tombstones = 9999;
  EXPECT_EQ(Checker::ComputeScore(stats, 100, false).score, 0.0);
  // absolute threshold alone triggers ("or" semantics)
  stats.tombstones = 10000;
  EXPECT_GT(Checker::ComputeScore(stats, 100, false).score, 0.0);
  // ratio threshold alone triggers
  stats.tombstones = 40;
  stats.total_entries = 100;
  auto detail = Checker::ComputeScore(stats, 100, false);
  EXPECT_DOUBLE_EQ(detail.score, 40.0 * 0.4);
}

TEST_F(TxnAutoCompactionScoreFlagTest, FilterOnUsesRedundancyRule) {
  TxnMvccRangeStats stats;
  stats.total_entries = 100;
  stats.num_files_with_props = 1;
  stats.mvcc.num_rows = 20;
  stats.mvcc.num_versions = 100;  // 80 shadowed
  stats.mvcc.oldest_stale_version_ts = 10;
  stats.mvcc.newest_stale_version_ts = 20;

  // sp beyond the stale segment: discardable=80, ratio=0.8 -> triggers
  auto detail = Checker::ComputeScore(stats, 100, true);
  EXPECT_EQ(detail.discardable, 80);
  EXPECT_DOUBLE_EQ(detail.score, 80.0 * 0.8);
  // sp below the stale segment: nothing reclaimable yet
  EXPECT_EQ(Checker::ComputeScore(stats, 5, true).score, 0.0);
}

TEST_F(TxnAutoCompactionScoreFlagTest, FilterOnStillHonorsTombstoneMountains) {
  // Deliberate deviation from TiKV: legacy tombstones with zero MVCC
  // redundancy must still trigger in filter mode.
  TxnMvccRangeStats stats;
  stats.total_entries = 100;
  stats.tombstones = 60;
  stats.num_files_with_props = 1;
  stats.mvcc.num_rows = 40;
  stats.mvcc.num_versions = 40;  // no shadowed versions

  auto detail = Checker::ComputeScore(stats, 100, true);
  EXPECT_DOUBLE_EQ(detail.score, 60.0 * 0.6);
}

TEST_F(TxnAutoCompactionScoreFlagTest, NoPropsFilesNeverLookLikeGarbage) {
  TxnMvccRangeStats stats;
  stats.total_entries = 1000000;  // pre-collector files, denominators only
  stats.num_files_with_props = 0;
  EXPECT_EQ(Checker::ComputeScore(stats, 100, true).score, 0.0);
  // empty range
  TxnMvccRangeStats empty;
  EXPECT_EQ(Checker::ComputeScore(empty, 100, true).score, 0.0);
}

// ---------------- engine-level end to end ----------------

class TxnAutoCompactionEngineTest : public testing::Test {
 protected:
  inline static const std::string kRootPath = "./unit_test_txn_auto_compaction";
  inline static const std::vector<std::string> kAllCFs = {Constant::kTxnWriteCF, Constant::kTxnDataCF,
                                                          Constant::kTxnLockCF, Constant::kStoreDataCF};
  inline static std::atomic<int64_t> safe_point{0};

  static void SetUpTestSuite() {
    SetRole("store");
    Helper::CreateDirectories(kRootPath);
  }

  static void TearDownTestSuite() { Helper::RemoveAllFileOrDirectory(kRootPath); }

  void SetUp() override {
    FLAGS_gc_enable_compaction_filter = true;
    FLAGS_gc_enable_safe_point_read_check = true;
    safe_point.store(0);

    const std::string store_path = kRootPath + "/" + ::testing::UnitTest::GetInstance()->current_test_info()->name();
    Helper::CreateDirectories(store_path);

    const std::string yaml_config = "store:\n  path: " + store_path + "\n";
    auto config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(yaml_config));

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine->Init(config, kAllCFs));
    engine->SetGcSafePointProvider([]() -> int64_t { return safe_point.load(); });
  }

  void TearDown() override {
    engine->Close();
    engine->Destroy();
    engine = nullptr;
    FLAGS_gc_enable_compaction_filter = true;
    FLAGS_gc_enable_safe_point_read_check = true;
  }

  void PutWrite(const std::string& user_key, int64_t commit_ts, int64_t start_ts) {
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(pb::store::Op::Put);
    write_info.set_short_value("v");

    pb::common::KeyValue kv;
    kv.set_key(mvcc::Codec::EncodeKey(user_key, commit_ts));
    kv.set_value(write_info.SerializeAsString());
    ASSERT_TRUE(engine->Writer()->KvPut(Constant::kTxnWriteCF, kv).ok());
  }

  static TxnAutoCompactionChecker::RangeEntry Entry(int64_t region_id, const std::string& plain_start,
                                                    const std::string& plain_end) {
    TxnAutoCompactionChecker::RangeEntry entry;
    entry.region_id = region_id;
    entry.encoded_range.set_start_key(mvcc::Codec::EncodeKey(plain_start, Constant::kMaxVer));
    entry.encoded_range.set_end_key(mvcc::Codec::EncodeKey(plain_end, Constant::kMaxVer));
    return entry;
  }

  std::set<std::pair<std::string, int64_t>> WriteKeys() {
    std::vector<pb::common::KeyValue> kvs;
    std::string start(1, '\x01');
    std::string end(32, '\xff');
    EXPECT_TRUE(engine->Reader()->KvScan(Constant::kTxnWriteCF, start, end, kvs).ok());

    std::set<std::pair<std::string, int64_t>> result;
    for (const auto& kv : kvs) {
      std::string user_key;
      int64_t ts = 0;
      EXPECT_TRUE(mvcc::Codec::DecodeKey(kv.key(), user_key, ts));
      result.emplace(user_key, ts);
    }
    return result;
  }

  static int64_t ProviderValue() { return safe_point.load(); }

  std::shared_ptr<RocksRawEngine> engine;
};

// Put-headed garbage below the safe point: RunForRanges scores the range,
// compacts it, the gc filter physically drops the shadowed versions, and the
// follow-up evaluation goes quiet.
TEST_F(TxnAutoCompactionEngineTest, GarbageRangeCompactedThenQuiet) {
  for (int i = 0; i < 12; i++) {
    std::string key = fmt::format("ka{:02d}", i);
    for (int64_t ts = 10; ts <= 50; ts += 10) {
      PutWrite(key, ts, ts - 5);
    }
  }
  engine->Flush(Constant::kTxnWriteCF);
  safe_point.store(100);

  auto attempted = TxnAutoCompactionChecker::RunForRanges(engine, {Entry(7, "ka", "kb")}, ProviderValue,
                                                          /*budget_ms=*/60 * 1000);
  ASSERT_EQ(attempted, std::vector<int64_t>({7}));

  // Only the newest version of each key survives.
  auto keys = WriteKeys();
  EXPECT_EQ(keys.size(), 12);
  for (const auto& [key, ts] : keys) {
    EXPECT_EQ(ts, 50) << key;
  }

  // The garbage is gone; the same range no longer qualifies.
  auto again = TxnAutoCompactionChecker::RunForRanges(engine, {Entry(7, "ka", "kb")}, ProviderValue,
                                                      /*budget_ms=*/60 * 1000);
  EXPECT_TRUE(again.empty());
}

// The compaction stays inside the requested encoded range: a disjoint key
// space in its own SST keeps every version.
TEST_F(TxnAutoCompactionEngineTest, CompactionScopedToRange) {
  for (int64_t ts = 10; ts <= 50; ts += 10) {
    PutWrite("ka", ts, ts - 5);
  }
  engine->Flush(Constant::kTxnWriteCF);
  for (int64_t ts = 10; ts <= 50; ts += 10) {
    PutWrite("zz", ts, ts - 5);
  }
  engine->Flush(Constant::kTxnWriteCF);
  safe_point.store(100);

  auto attempted =
      TxnAutoCompactionChecker::RunForRanges(engine, {Entry(1, "ka", "kb")}, ProviderValue, /*budget_ms=*/60 * 1000);
  ASSERT_EQ(attempted.size(), 1);

  auto keys = WriteKeys();
  int zz_versions = 0;
  for (const auto& [key, ts] : keys) {
    if (key == "zz") zz_versions++;
  }
  EXPECT_EQ(zz_versions, 5);  // untouched neighbor
  EXPECT_EQ(keys.size(), static_cast<size_t>(1 + 5));
}

// Guard rails: zero budget stops before any compaction, zero safe point
// skips the round entirely.
TEST_F(TxnAutoCompactionEngineTest, BudgetAndSafePointGuards) {
  for (int64_t ts = 10; ts <= 50; ts += 10) {
    PutWrite("ka", ts, ts - 5);
  }
  engine->Flush(Constant::kTxnWriteCF);

  safe_point.store(0);
  EXPECT_TRUE(
      TxnAutoCompactionChecker::RunForRanges(engine, {Entry(1, "ka", "kb")}, ProviderValue, 60 * 1000).empty());

  safe_point.store(100);
  EXPECT_TRUE(TxnAutoCompactionChecker::RunForRanges(engine, {Entry(1, "ka", "kb")}, ProviderValue, 0).empty());
  EXPECT_EQ(WriteKeys().size(), 5);  // nothing was compacted
}

}  // namespace dingodb

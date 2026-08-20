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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/constant.h"
#include "common/helper.h"
#include "common/role.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "engine/txn_mvcc_properties_collector.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

// Behavior tests for the write-CF MVCC properties collector: a standalone
// RocksRawEngine per test, real Flush (the collector runs at table-file
// creation), assertions through the public GetTxnMvccPropertiesInRange
// aggregation — rocksdb::DB is intentionally unreachable from tests.
class TxnMvccPropertiesCollectorTest : public testing::Test {
 protected:
  inline static const std::string kRootPath = "./unit_test_txn_mvcc_props";
  inline static const std::vector<std::string> kAllCFs = {Constant::kTxnWriteCF, Constant::kTxnDataCF,
                                                          Constant::kTxnLockCF, Constant::kStoreDataCF};

  static void SetUpTestSuite() {
    SetRole("store");
    Helper::CreateDirectories(kRootPath);
  }

  static void TearDownTestSuite() { Helper::RemoveAllFileOrDirectory(kRootPath); }

  void SetUp() override {
    const std::string store_path = kRootPath + "/" + ::testing::UnitTest::GetInstance()->current_test_info()->name();
    Helper::CreateDirectories(store_path);

    const std::string yaml_config = "store:\n  path: " + store_path + "\n";
    auto config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(yaml_config));

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine->Init(config, kAllCFs));
  }

  void TearDown() override {
    engine->Close();
    engine->Destroy();
    engine = nullptr;
  }

  // literal-friendly wrapper: Codec::EncodeKey(const char*, ts) is ambiguous
  static std::string EK(const std::string& user_key, int64_t ts) { return mvcc::Codec::EncodeKey(user_key, ts); }

  void PutWrite(const std::string& user_key, int64_t commit_ts, pb::store::Op op, int64_t start_ts) {
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(op);

    pb::common::KeyValue kv;
    kv.set_key(mvcc::Codec::EncodeKey(user_key, commit_ts));
    kv.set_value(write_info.SerializeAsString());
    ASSERT_TRUE(engine->Writer()->KvPut(Constant::kTxnWriteCF, kv).ok());
  }

  void PutRawWrite(const std::string& raw_key, const std::string& raw_value) {
    pb::common::KeyValue kv;
    kv.set_key(raw_key);
    kv.set_value(raw_value);
    ASSERT_TRUE(engine->Writer()->KvPut(Constant::kTxnWriteCF, kv).ok());
  }

  void Flush() { engine->Flush(Constant::kTxnWriteCF); }

  TxnMvccRangeStats Query(const std::string& plain_start, const std::string& plain_end) {
    pb::common::Range range;
    range.set_start_key(mvcc::Codec::EncodeKey(plain_start, Constant::kMaxVer));
    range.set_end_key(mvcc::Codec::EncodeKey(plain_end, Constant::kMaxVer));
    TxnMvccRangeStats stats;
    EXPECT_TRUE(engine->GetTxnMvccPropertiesInRange(range, stats).ok());
    return stats;
  }

  std::shared_ptr<RocksRawEngine> engine;
};

// Newest-first version groups: shadowed versions feed the stale ts range,
// the per-file row/version counters add up.
TEST_F(TxnMvccPropertiesCollectorTest, BasicCountsSingleSst) {
  PutWrite("ka", 30, pb::store::Op::Put, 25);
  PutWrite("ka", 20, pb::store::Op::Put, 15);
  PutWrite("ka", 10, pb::store::Op::Put, 5);
  PutWrite("kb", 40, pb::store::Op::Put, 35);
  Flush();

  auto stats = Query("j", "z");
  EXPECT_EQ(stats.num_files, 1);
  EXPECT_EQ(stats.num_files_with_props, 1);
  EXPECT_EQ(stats.total_entries, 4);
  EXPECT_EQ(stats.tombstones, 0);
  EXPECT_EQ(stats.range_deletions, 0);
  EXPECT_EQ(stats.mvcc.num_rows, 2);
  EXPECT_EQ(stats.mvcc.num_versions, 4);
  EXPECT_EQ(stats.mvcc.num_puts, 4);
  EXPECT_EQ(stats.mvcc.num_deletes, 0);
  EXPECT_EQ(stats.mvcc.num_rollbacks, 0);
  EXPECT_EQ(stats.mvcc.max_row_versions, 3);
  EXPECT_EQ(stats.mvcc.min_ts, 10);
  EXPECT_EQ(stats.mvcc.max_ts, 40);
  EXPECT_EQ(stats.mvcc.oldest_stale_version_ts, 10);
  EXPECT_EQ(stats.mvcc.newest_stale_version_ts, 20);
  EXPECT_EQ(stats.mvcc.oldest_delete_ts, 0);
  EXPECT_EQ(stats.mvcc.newest_delete_ts, 0);
  EXPECT_EQ(stats.mvcc.num_errors, 0);
}

// Delete marks and rollbacks are classified off WriteInfo.op; delete marks
// additionally carve their own ts range.
TEST_F(TxnMvccPropertiesCollectorTest, OpClassification) {
  PutWrite("ka", 30, pb::store::Op::Delete, 25);
  PutWrite("ka", 20, pb::store::Op::Put, 15);
  PutWrite("ka", 10, pb::store::Op::Rollback, 10);
  Flush();

  auto stats = Query("j", "z");
  EXPECT_EQ(stats.mvcc.num_rows, 1);
  EXPECT_EQ(stats.mvcc.num_versions, 3);
  EXPECT_EQ(stats.mvcc.num_puts, 1);
  EXPECT_EQ(stats.mvcc.num_deletes, 1);
  EXPECT_EQ(stats.mvcc.num_rollbacks, 1);
  EXPECT_EQ(stats.mvcc.oldest_delete_ts, 30);
  EXPECT_EQ(stats.mvcc.newest_delete_ts, 30);
  EXPECT_EQ(stats.mvcc.oldest_stale_version_ts, 10);
  EXPECT_EQ(stats.mvcc.newest_stale_version_ts, 20);
  EXPECT_EQ(stats.mvcc.num_errors, 0);
}

// rocksdb point tombstones surface through the built-in num_deletions and
// still feed min/max_ts, but stay out of num_versions.
TEST_F(TxnMvccPropertiesCollectorTest, RocksdbTombstoneCounted) {
  PutWrite("ka", 10, pb::store::Op::Put, 5);
  Flush();
  ASSERT_TRUE(engine->Writer()->KvDelete(Constant::kTxnWriteCF, EK("kx", 77)).ok());
  Flush();

  auto stats = Query("j", "z");
  EXPECT_EQ(stats.num_files, 2);
  EXPECT_EQ(stats.num_files_with_props, 2);
  EXPECT_EQ(stats.total_entries, 2);
  EXPECT_EQ(stats.tombstones, 1);
  EXPECT_EQ(stats.mvcc.num_versions, 1);
  EXPECT_EQ(stats.mvcc.num_rows, 1);
  EXPECT_EQ(stats.mvcc.min_ts, 10);
  EXPECT_EQ(stats.mvcc.max_ts, 77);
}

// Aggregation over multiple SSTs is the Add() semantics: sums, min/max
// folds, max_row_versions takes the largest.
TEST_F(TxnMvccPropertiesCollectorTest, MultiSstAggregation) {
  PutWrite("ka", 30, pb::store::Op::Put, 25);
  PutWrite("ka", 20, pb::store::Op::Put, 15);
  Flush();
  PutWrite("kb", 50, pb::store::Op::Put, 45);
  Flush();

  auto stats = Query("j", "z");
  EXPECT_EQ(stats.num_files, 2);
  EXPECT_EQ(stats.num_files_with_props, 2);
  EXPECT_EQ(stats.mvcc.num_rows, 2);
  EXPECT_EQ(stats.mvcc.num_versions, 3);
  EXPECT_EQ(stats.mvcc.max_row_versions, 2);
  EXPECT_EQ(stats.mvcc.min_ts, 20);
  EXPECT_EQ(stats.mvcc.max_ts, 50);
  EXPECT_EQ(stats.mvcc.oldest_stale_version_ts, 20);
  EXPECT_EQ(stats.mvcc.newest_stale_version_ts, 20);
}

// Unparseable keys/values only bump num_errors; the SST build survives and
// the remaining fields stay correct.
TEST_F(TxnMvccPropertiesCollectorTest, DefensiveBadInput) {
  PutRawWrite("x", "short-key-entry");                                     // key < 17 bytes
  PutRawWrite(EK("ka", 30), "garbage-not-writeinfo");  // valid key, bad value
  PutWrite("kb", 20, pb::store::Op::Put, 15);
  Flush();

  auto stats = Query("j", "z");
  EXPECT_EQ(stats.total_entries, 3);
  EXPECT_EQ(stats.mvcc.num_errors, 2);
  // The bad-value entry still has a parsable key: counted as a version/row.
  EXPECT_EQ(stats.mvcc.num_versions, 2);
  EXPECT_EQ(stats.mvcc.num_rows, 2);
  EXPECT_EQ(stats.mvcc.num_puts, 1);
}

// Encode/Decode is all-or-nothing: any missing or malformed key rejects the
// whole block, so a damaged block degrades like an absent one.
TEST_F(TxnMvccPropertiesCollectorTest, DecodeRoundTripAndRejects) {
  TxnMvccProperties props;
  props.min_ts = 7;
  props.max_ts = 99;
  props.num_rows = 3;
  props.num_puts = 4;
  props.num_deletes = 1;
  props.num_rollbacks = 2;
  props.num_versions = 7;
  props.max_row_versions = 5;
  props.oldest_stale_version_ts = 8;
  props.newest_stale_version_ts = 88;
  props.oldest_delete_ts = 9;
  props.newest_delete_ts = 90;
  props.num_errors = 6;

  rocksdb::UserCollectedProperties encoded;
  props.EncodeTo(&encoded);

  TxnMvccProperties decoded;
  ASSERT_TRUE(TxnMvccProperties::DecodeFrom(encoded, &decoded));
  EXPECT_EQ(decoded.min_ts, 7);
  EXPECT_EQ(decoded.max_ts, 99);
  EXPECT_EQ(decoded.num_rows, 3);
  EXPECT_EQ(decoded.num_puts, 4);
  EXPECT_EQ(decoded.num_deletes, 1);
  EXPECT_EQ(decoded.num_rollbacks, 2);
  EXPECT_EQ(decoded.num_versions, 7);
  EXPECT_EQ(decoded.max_row_versions, 5);
  EXPECT_EQ(decoded.oldest_stale_version_ts, 8);
  EXPECT_EQ(decoded.newest_stale_version_ts, 88);
  EXPECT_EQ(decoded.oldest_delete_ts, 9);
  EXPECT_EQ(decoded.newest_delete_ts, 90);
  EXPECT_EQ(decoded.num_errors, 6);

  TxnMvccProperties ignored;
  EXPECT_FALSE(TxnMvccProperties::DecodeFrom({}, &ignored));

  auto missing_key = encoded;
  missing_key.erase(kMvccPropNumVersions);
  EXPECT_FALSE(TxnMvccProperties::DecodeFrom(missing_key, &ignored));

  auto bad_number = encoded;
  bad_number[kMvccPropNumRows] = "not-a-number";
  EXPECT_FALSE(TxnMvccProperties::DecodeFrom(bad_number, &ignored));
}

// GetPropertiesOfTablesInRange is SST-overlap granular: a narrow query still
// returns whole-file counters for every overlapping SST.
TEST_F(TxnMvccPropertiesCollectorTest, SubRangeQueryIsSstGranular) {
  PutWrite("ka", 10, pb::store::Op::Put, 5);
  PutWrite("kz", 20, pb::store::Op::Put, 15);
  Flush();

  auto narrow = Query("ka", "kb");
  EXPECT_EQ(narrow.num_files, 1);
  EXPECT_EQ(narrow.total_entries, 2);
  EXPECT_EQ(narrow.mvcc.num_rows, 2);
}

}  // namespace dingodb

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
#include <chrono>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/constant.h"
#include "common/helper.h"
#include "common/role.h"
#include "config/config.h"
#include "config/config_manager.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "engine/txn_gc_compaction_filter.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

DECLARE_bool(gc_enable_compaction_filter);
DECLARE_bool(gc_enable_safe_point_read_check);

// End-to-end behavior tests for the txn gc compaction filter: a standalone
// RocksRawEngine per test (no server singleton — keeps this suite
// order-independent in the shared test binary), a fixed safe point provider,
// real Flush + CompactRange, byte-exact assertions on the survivors.
class TxnGcCompactionFilterTest : public testing::Test {
 protected:
  inline static const std::string kRootPath = "./unit_test_txn_gc_filter";
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
    // The S0 read guard is an enforced precondition: without it the factory
    // refuses to create filters.
    FLAGS_gc_enable_safe_point_read_check = true;
    safe_point.store(0);

    const std::string store_path =
        kRootPath + "/" + ::testing::UnitTest::GetInstance()->current_test_info()->name();
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

  void PutWrite(const std::string& user_key, int64_t commit_ts, pb::store::Op op, int64_t start_ts,
                const std::string& short_value = "") {
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(op);
    if (!short_value.empty()) {
      write_info.set_short_value(short_value);
    }

    pb::common::KeyValue kv;
    kv.set_key(mvcc::Codec::EncodeKey(user_key, commit_ts));
    kv.set_value(write_info.SerializeAsString());
    ASSERT_TRUE(engine->Writer()->KvPut(Constant::kTxnWriteCF, kv).ok());
  }

  void PutRawWrite(const std::string& user_key, int64_t commit_ts, const std::string& raw_value) {
    pb::common::KeyValue kv;
    kv.set_key(mvcc::Codec::EncodeKey(user_key, commit_ts));
    kv.set_value(raw_value);
    ASSERT_TRUE(engine->Writer()->KvPut(Constant::kTxnWriteCF, kv).ok());
  }

  void PutData(const std::string& user_key, int64_t start_ts) {
    pb::common::KeyValue kv;
    kv.set_key(mvcc::Codec::EncodeKey(user_key, start_ts));
    kv.set_value("value-" + user_key);
    ASSERT_TRUE(engine->Writer()->KvPut(Constant::kTxnDataCF, kv).ok());
  }

  void Compact() {
    engine->Flush(Constant::kTxnWriteCF);
    engine->Flush(Constant::kTxnDataCF);
    ASSERT_TRUE(engine->Compact(Constant::kTxnWriteCF).ok());
  }

  using KeyTs = std::pair<std::string, int64_t>;

  std::set<KeyTs> CfKeys(const std::string& cf_name) {
    std::vector<pb::common::KeyValue> kvs;
    std::string start(1, '\x01');
    std::string end(32, '\xff');
    EXPECT_TRUE(engine->Reader()->KvScan(cf_name, start, end, kvs).ok());

    std::set<KeyTs> result;
    for (const auto& kv : kvs) {
      std::string user_key;
      int64_t ts = 0;
      EXPECT_TRUE(mvcc::Codec::DecodeKey(kv.key(), user_key, ts));
      result.emplace(user_key, ts);
    }
    return result;
  }

  std::set<KeyTs> WriteKeys() { return CfKeys(Constant::kTxnWriteCF); }
  std::set<KeyTs> DataKeys() { return CfKeys(Constant::kTxnDataCF); }

  // The inline orphan path completes within CompactRange, but a stalled write
  // may fall back to the cleaner thread; bound the wait instead of assuming.
  void WaitDataKeys(const std::set<KeyTs>& expect) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (DataKeys() != expect && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(DataKeys(), expect);
  }

  std::shared_ptr<RocksRawEngine> engine;
};

// The newest Put at or below the safe point survives unconditionally; every
// older version is physically dropped, and their data CF rows go with them.
TEST_F(TxnGcCompactionFilterTest, HeadPutKeptOlderRemoved) {
  safe_point.store(100);
  PutWrite("ka", 30, pb::store::Op::Put, 29);
  PutWrite("ka", 20, pb::store::Op::Put, 19);
  PutWrite("ka", 10, pb::store::Op::Put, 9);
  PutData("ka", 29);
  PutData("ka", 19);
  PutData("ka", 9);

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 30}}));
  WaitDataKeys({{"ka", 29}});
}

// A head-position rollback is KEPT (a computed 1PC/async commit_ts may
// collide with its encoded key — removal would destroy that live commit) and
// does not take the head slot: the Put below is still the head and survives,
// versions under the head still go.
TEST_F(TxnGcCompactionFilterTest, HeadRollbackKeptWithoutTakingHead) {
  safe_point.store(100);
  PutWrite("ka", 40, pb::store::Op::Rollback, 40);
  PutWrite("ka", 30, pb::store::Op::Put, 29, "short");
  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 40}, {"ka", 30}}));
}

// Below a kept Put head a rollback is shadowed garbage and is removed.
TEST_F(TxnGcCompactionFilterTest, RollbackBelowHeadRemoved) {
  safe_point.store(100);
  PutWrite("ka", 50, pb::store::Op::Put, 49, "short");
  PutWrite("ka", 40, pb::store::Op::Rollback, 40);
  PutWrite("ka", 30, pb::store::Op::Put, 29, "short");

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 50}}));
}

// With the S0 read guard off, enabling the filter flag must be refused: reads
// below the safe point would have no defense.
TEST_F(TxnGcCompactionFilterTest, ReadGuardOffRefusesToFilter) {
  safe_point.store(100);
  FLAGS_gc_enable_safe_point_read_check = false;
  PutWrite("ka", 30, pb::store::Op::Put, 29, "short");
  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 30}, {"ka", 20}}));
}

// A Delete-headed group is kept whole: delete marks belong to scan GC, which
// removes the group atomically via raft.
TEST_F(TxnGcCompactionFilterTest, DeleteGroupKeptWhole) {
  safe_point.store(100);
  PutWrite("ka", 30, pb::store::Op::Delete, 29);
  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");
  PutWrite("ka", 10, pb::store::Op::Put, 9, "short");

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 30}, {"ka", 20}, {"ka", 10}}));
}

// A shadowed Put whose value lives inline (short_value) must not issue a data
// CF delete: a data row at the same (key, start_ts) belongs to someone else.
TEST_F(TxnGcCompactionFilterTest, ShortValuePutExemptFromOrphanDelete) {
  safe_point.store(100);
  PutWrite("ka", 30, pb::store::Op::Put, 29, "short");
  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");
  PutData("ka", 19);

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 30}}));
  WaitDataKeys({{"ka", 19}});
}

// Provider returning 0 (no/stopped safe point) disables filtering entirely.
TEST_F(TxnGcCompactionFilterTest, SafePointZeroKeepsEverything) {
  safe_point.store(0);
  PutWrite("ka", 30, pb::store::Op::Put, 29, "short");
  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");
  PutWrite("ka", 10, pb::store::Op::Rollback, 10);

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 30}, {"ka", 20}, {"ka", 10}}));
}

// Flag off keeps everything even with a valid safe point; turning it on takes
// effect on the next compaction of the SAME open DB — no reopen.
TEST_F(TxnGcCompactionFilterTest, RuntimeToggleNoReopen) {
  safe_point.store(100);
  FLAGS_gc_enable_compaction_filter = false;
  PutWrite("ka", 30, pb::store::Op::Put, 29, "short");
  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");

  Compact();
  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 30}, {"ka", 20}}));

  FLAGS_gc_enable_compaction_filter = true;
  Compact();
  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 30}}));
}

// Versions above the safe point are kept unparsed and never decide the group
// head; deliberately no scan-GC "A rule": the newest Put <= safe point
// survives even with live Puts above it.
TEST_F(TxnGcCompactionFilterTest, VersionsAboveSafePointUntouched) {
  safe_point.store(100);
  PutWrite("ka", 200, pb::store::Op::Put, 199, "short");
  PutWrite("ka", 150, pb::store::Op::Delete, 149);
  PutWrite("ka", 90, pb::store::Op::Put, 89, "short");
  PutWrite("ka", 80, pb::store::Op::Put, 79, "short");

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 200}, {"ka", 150}, {"ka", 90}}));
}

// ts=1 successor boundary: skip_until = EncodeKey(key, 0) = trailing 8 bytes
// of 0xff; removal must stay inside this user key.
TEST_F(TxnGcCompactionFilterTest, TsOneSuccessorBoundary) {
  safe_point.store(100);
  PutWrite("ka", 2, pb::store::Op::Put, 1, "short");
  PutWrite("ka", 1, pb::store::Op::Put, 1, "short");
  PutWrite("kb", 1, pb::store::Op::Put, 1, "short");

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 2}, {"kb", 1}}));
}

// Adjacent user keys have independent group state; removals in one key must
// not skip into or influence the next.
TEST_F(TxnGcCompactionFilterTest, AdjacentUserKeysIndependent) {
  safe_point.store(100);
  PutWrite("ka", 30, pb::store::Op::Put, 29, "short");
  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");
  PutWrite("kb", 25, pb::store::Op::Delete, 24);
  PutWrite("kb", 15, pb::store::Op::Put, 14, "short");
  PutWrite("kc", 5, pb::store::Op::Put, 4, "short");
  PutWrite("kc", 3, pb::store::Op::Rollback, 3);

  Compact();

  EXPECT_EQ(WriteKeys(),
            (std::set<KeyTs>{{"ka", 30}, {"kb", 25}, {"kb", 15}, {"kc", 5}}));
}

// Partial-view convergence: a compaction that only sees older versions keeps
// its local newest (cannot know a newer head exists elsewhere); once a later
// compaction sees old and new together, the shadowed ones go. The phase-2
// batch spans keys on both sides of the phase-1 file so the two files' ranges
// overlap — otherwise RocksDB trivial-moves the new file without ever running
// the filter, and the versions never meet in one compaction input.
TEST_F(TxnGcCompactionFilterTest, TwoPhasePartialCompactionConverges) {
  safe_point.store(100);
  PutWrite("ka", 10, pb::store::Op::Put, 9, "short");
  PutWrite("kc", 5, pb::store::Op::Put, 4, "short");
  Compact();
  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 10}, {"kc", 5}}));

  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");
  PutWrite("kc", 7, pb::store::Op::Put, 6, "short");
  Compact();
  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 20}, {"kc", 7}}));
}

// Ops that scan GC treats as invalid in the write CF (e.g. Lock) are kept.
TEST_F(TxnGcCompactionFilterTest, UnexpectedOpKept) {
  safe_point.store(100);
  PutWrite("ka", 30, pb::store::Op::Lock, 29);
  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");
  PutWrite("ka", 10, pb::store::Op::Put, 9, "short");

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 30}, {"ka", 20}}));
}

// An unparseable head could be a delete mark; the whole group must survive.
TEST_F(TxnGcCompactionFilterTest, UnparseableHeadKeepsGroup) {
  safe_point.store(100);
  PutRawWrite("ka", 30, "\xde\xad\xbe\xef garbage that is not a WriteInfo");
  PutWrite("ka", 20, pb::store::Op::Put, 19, "short");
  PutWrite("ka", 10, pb::store::Op::Put, 9, "short");

  Compact();

  EXPECT_EQ(WriteKeys(), (std::set<KeyTs>{{"ka", 30}, {"ka", 20}, {"ka", 10}}));
}

}  // namespace dingodb

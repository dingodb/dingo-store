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
#include <set>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/stream.h"
#include "common/tracker.h"
#include "config/config.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "engine/txn_engine_helper.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

namespace {

const std::string kRootPath = "./unit_test_txn_scan_lock_collection";
const std::string kLogPath = kRootPath + "/log";
const std::string kStorePath = kRootPath + "/db";

const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: 12345\n"
    "  coordinators: 127.0.0.1:19190,127.0.0.1:19191,127.0.0.1:19192\n"
    "  keyring: TO_BE_CONTINUED\n"
    "server:\n"
    "  host: 127.0.0.1\n"
    "  port: 23000\n"
    "log:\n"
    "  path: " +
    kLogPath +
    "\n"
    "store:\n"
    "  path: " +
    kStorePath + "\n";

struct ScanOutput {
  butil::Status status;
  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::common::KeyValue> kvs;
  std::vector<pb::store::TxnScanEntry> entries;
  bool has_more{false};
  std::string end_key;
  StreamPtr stream;
};

class TxnScanLockCollectionTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    Helper::CreateDirectories(kStorePath);

    std::shared_ptr<Config> config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  static inline std::shared_ptr<RocksRawEngine> engine;

 private:
  static const std::vector<std::string> kAllCFs;
};

const std::vector<std::string> TxnScanLockCollectionTest::kAllCFs = {
    Constant::kTxnWriteCF,
    Constant::kTxnDataCF,
    Constant::kTxnLockCF,
    "default",
};

static void ClearTxnPrefix(const std::shared_ptr<RocksRawEngine> &engine, const std::string &prefix) {
  pb::common::Range range;
  range.set_start_key(mvcc::Codec::EncodeKey(prefix, Constant::kMaxVer));
  range.set_end_key(mvcc::Codec::EncodeKey(Helper::PrefixNext(prefix), 0));

  auto writer = engine->Writer();
  ASSERT_EQ(writer->KvDeleteRange(Constant::kTxnWriteCF, range).error_code(), pb::error::OK);
  ASSERT_EQ(writer->KvDeleteRange(Constant::kTxnDataCF, range).error_code(), pb::error::OK);
  ASSERT_EQ(writer->KvDeleteRange(Constant::kTxnLockCF, range).error_code(), pb::error::OK);
}

static std::string TestKey(const std::string &prefix, const std::string &suffix) { return prefix + suffix; }

static void PutCommitted(const std::shared_ptr<RocksRawEngine> &engine, const std::string &key, const std::string &value,
                         int64_t start_ts, int64_t commit_ts) {
  pb::store::WriteInfo write_info;
  write_info.set_start_ts(start_ts);
  write_info.set_op(pb::store::Op::Put);

  pb::common::KeyValue write_kv;
  write_kv.set_key(mvcc::Codec::EncodeKey(key, commit_ts));
  write_kv.set_value(write_info.SerializeAsString());
  ASSERT_EQ(engine->Writer()->KvPut(Constant::kTxnWriteCF, write_kv).error_code(), pb::error::OK);

  pb::common::KeyValue data_kv;
  data_kv.set_key(mvcc::Codec::EncodeKey(key, start_ts));
  data_kv.set_value(value);
  ASSERT_EQ(engine->Writer()->KvPut(Constant::kTxnDataCF, data_kv).error_code(), pb::error::OK);
}

static void PutLock(const std::shared_ptr<RocksRawEngine> &engine, const std::string &key, int64_t lock_ts,
                    int64_t min_commit_ts = 0, pb::store::Op lock_type = pb::store::Op::Put,
                    int64_t for_update_ts = 0) {
  pb::store::LockInfo lock_info;
  lock_info.set_primary_lock(key);
  lock_info.set_lock_ts(lock_ts);
  lock_info.set_key(key);
  lock_info.set_lock_ttl(1000);
  lock_info.set_lock_type(lock_type);
  lock_info.set_min_commit_ts(min_commit_ts);
  lock_info.set_for_update_ts(for_update_ts);

  pb::common::KeyValue lock_kv;
  lock_kv.set_key(mvcc::Codec::EncodeKey(key, Constant::kLockVer));
  lock_kv.set_value(lock_info.SerializeAsString());
  ASSERT_EQ(engine->Writer()->KvPut(Constant::kTxnLockCF, lock_kv).error_code(), pb::error::OK);
}

static ScanOutput RunScan(const std::shared_ptr<RocksRawEngine> &engine, const std::string &prefix, int64_t scan_ts,
                          bool enable_lock_collection, const std::set<int64_t> &resolved_locks = {},
                          StreamPtr stream = nullptr, uint32_t stream_limit = 1000, bool key_only = false) {
  ScanOutput output;
  output.stream = stream == nullptr ? Stream::New(stream_limit) : stream;

  auto ctx = std::make_shared<Context>();
  pb::common::RequestInfo request_info;
  ctx->SetTracker(Tracker::New(request_info));

  pb::common::Range range;
  range.set_start_key(prefix);
  range.set_end_key(Helper::PrefixNext(prefix));

  pb::common::CoprocessorV2 coprocessor;
  output.status = TxnEngineHelper::Scan(ctx, output.stream, engine, pb::store::IsolationLevel::SnapshotIsolation,
                                        scan_ts, range, stream_limit, key_only, false, resolved_locks, true, coprocessor,
                                        enable_lock_collection, output.txn_result_info, output.kvs, output.entries,
                                        output.has_more, output.end_key);
  return output;
}

static void ExpectKvEntry(const pb::store::TxnScanEntry &entry, const std::string &key, const std::string &value) {
  ASSERT_TRUE(entry.has_kv()) << entry.ShortDebugString();
  EXPECT_EQ(entry.kv().key(), key);
  EXPECT_EQ(entry.kv().value(), value);
}

static void ExpectKeyOnlyEntry(const pb::store::TxnScanEntry &entry, const std::string &key) {
  ASSERT_TRUE(entry.has_kv()) << entry.ShortDebugString();
  EXPECT_EQ(entry.kv().key(), key);
  EXPECT_TRUE(entry.kv().value().empty());
}

static void ExpectLockedEntry(const pb::store::TxnScanEntry &entry, const std::string &key, int64_t lock_ts) {
  ASSERT_TRUE(entry.has_locked()) << entry.ShortDebugString();
  EXPECT_EQ(entry.locked().key(), key);
  EXPECT_EQ(entry.locked().lock_ts(), lock_ts);
}

}  // namespace

TEST_F(TxnScanLockCollectionTest, EmptyRange) {
  const std::string prefix = "txn_scan_lc_empty_";
  ClearTxnPrefix(engine, prefix);

  auto output = RunScan(engine, prefix, 100, true);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_FALSE(output.has_more);
  EXPECT_EQ(output.kvs.size(), 0);
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  EXPECT_EQ(output.entries.size(), 0);
}

TEST_F(TxnScanLockCollectionTest, NoLockReturnsEntriesInOrder) {
  const std::string prefix = "txn_scan_lc_no_lock_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  const std::string k3 = TestKey(prefix, "003");
  PutCommitted(engine, k1, "v1", 10, 20);
  PutCommitted(engine, k2, "v2", 11, 21);
  PutCommitted(engine, k3, "v3", 12, 22);

  auto output = RunScan(engine, prefix, 100, true);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_FALSE(output.has_more);
  EXPECT_EQ(output.kvs.size(), 0);
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  ASSERT_EQ(output.entries.size(), 3);
  ExpectKvEntry(output.entries[0], k1, "v1");
  ExpectKvEntry(output.entries[1], k2, "v2");
  ExpectKvEntry(output.entries[2], k3, "v3");
}

TEST_F(TxnScanLockCollectionTest, KeyOnlyOmitsValuesButKeepsLockedEntries) {
  const std::string prefix = "txn_scan_lc_key_only_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  const std::string k3 = TestKey(prefix, "003");
  PutCommitted(engine, k1, "v1", 10, 20);
  PutLock(engine, k2, 50);
  PutCommitted(engine, k3, "v3", 12, 22);

  auto output = RunScan(engine, prefix, 100, true, {}, nullptr, 1000, true);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_FALSE(output.has_more);
  EXPECT_EQ(output.kvs.size(), 0);
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  ASSERT_EQ(output.entries.size(), 3);
  ExpectKeyOnlyEntry(output.entries[0], k1);
  ExpectLockedEntry(output.entries[1], k2, 50);
  ExpectKeyOnlyEntry(output.entries[2], k3);
}

TEST_F(TxnScanLockCollectionTest, OnlyLocks) {
  const std::string prefix = "txn_scan_lc_only_lock_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  const std::string k3 = TestKey(prefix, "003");
  PutLock(engine, k1, 50);
  PutLock(engine, k2, 51);
  PutLock(engine, k3, 52);

  auto output = RunScan(engine, prefix, 100, true);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_FALSE(output.has_more);
  EXPECT_EQ(output.kvs.size(), 0);
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  ASSERT_EQ(output.entries.size(), 3);
  ExpectLockedEntry(output.entries[0], k1, 50);
  ExpectLockedEntry(output.entries[1], k2, 51);
  ExpectLockedEntry(output.entries[2], k3, 52);
}

TEST_F(TxnScanLockCollectionTest, MiddleKeyLockedOrderPreserved) {
  const std::string prefix = "txn_scan_lc_mid_lock_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  const std::string k3 = TestKey(prefix, "003");
  const std::string k4 = TestKey(prefix, "004");
  const std::string k5 = TestKey(prefix, "005");
  PutCommitted(engine, k1, "v1", 10, 20);
  PutLock(engine, k2, 50);
  PutLock(engine, k3, 51);
  PutLock(engine, k4, 52);
  PutCommitted(engine, k5, "v5", 14, 24);

  auto output = RunScan(engine, prefix, 100, true);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_FALSE(output.has_more);
  EXPECT_EQ(output.kvs.size(), 0);
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  ASSERT_EQ(output.entries.size(), 5);
  ExpectKvEntry(output.entries[0], k1, "v1");
  ExpectLockedEntry(output.entries[1], k2, 50);
  ExpectLockedEntry(output.entries[2], k3, 51);
  ExpectLockedEntry(output.entries[3], k4, 52);
  ExpectKvEntry(output.entries[4], k5, "v5");
}

TEST_F(TxnScanLockCollectionTest, LockedKeyAlsoHasWriteReturnsLockedOnly) {
  const std::string prefix = "txn_scan_lc_lock_with_write_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  PutCommitted(engine, k1, "old_v1", 10, 20);
  PutLock(engine, k1, 50);
  PutCommitted(engine, k2, "v2", 11, 21);

  auto output = RunScan(engine, prefix, 100, true);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_EQ(output.kvs.size(), 0);
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  ASSERT_EQ(output.entries.size(), 2);
  ExpectLockedEntry(output.entries[0], k1, 50);
  ExpectKvEntry(output.entries[1], k2, "v2");
}

TEST_F(TxnScanLockCollectionTest, LockPlusWriteWriteIterExhausted) {
  const std::string prefix = "txn_scan_lc_lock_with_write_end_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  PutCommitted(engine, k1, "old_v1", 10, 20);
  PutLock(engine, k1, 50);

  auto output = RunScan(engine, prefix, 100, true);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_FALSE(output.has_more);
  EXPECT_EQ(output.kvs.size(), 0);
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  ASSERT_EQ(output.entries.size(), 1);
  ExpectLockedEntry(output.entries[0], k1, 50);
}

TEST_F(TxnScanLockCollectionTest, ResolvedLockIgnoredReturnsVisibleValue) {
  const std::string prefix = "txn_scan_lc_resolved_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  PutCommitted(engine, k1, "old_v1", 10, 20);
  PutLock(engine, k1, 50);
  PutCommitted(engine, k2, "v2", 11, 21);

  auto output = RunScan(engine, prefix, 100, true, {50});

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  ASSERT_EQ(output.entries.size(), 2);
  ExpectKvEntry(output.entries[0], k1, "old_v1");
  ExpectKvEntry(output.entries[1], k2, "v2");
}

TEST_F(TxnScanLockCollectionTest, MinCommitTsGreaterThanStartTsIgnored) {
  const std::string prefix = "txn_scan_lc_min_commit_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  PutCommitted(engine, k1, "old_v1", 10, 20);
  PutLock(engine, k1, 50, 101);
  PutCommitted(engine, k2, "v2", 11, 21);

  auto output = RunScan(engine, prefix, 100, true);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  ASSERT_EQ(output.entries.size(), 2);
  ExpectKvEntry(output.entries[0], k1, "old_v1");
  ExpectKvEntry(output.entries[1], k2, "v2");
}

TEST_F(TxnScanLockCollectionTest, DisabledReturnsTxnResult) {
  const std::string prefix = "txn_scan_lc_disabled_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  PutLock(engine, k1, 50);
  PutCommitted(engine, k2, "v2", 11, 21);

  auto output = RunScan(engine, prefix, 100, false);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_TRUE(output.has_more);
  EXPECT_EQ(output.entries.size(), 0);
  EXPECT_EQ(output.kvs.size(), 0);
  ASSERT_TRUE(output.txn_result_info.has_locked()) << output.txn_result_info.ShortDebugString();
  EXPECT_EQ(output.txn_result_info.locked().key(), k1);
  EXPECT_EQ(output.txn_result_info.locked().lock_ts(), 50);
}

TEST_F(TxnScanLockCollectionTest, CoprocessorRejected) {
  const std::string prefix = "txn_scan_lc_coprocessor_reject_";
  ClearTxnPrefix(engine, prefix);

  auto stream = Stream::New(1000);
  auto ctx = std::make_shared<Context>();
  ctx->SetTracker(Tracker::New(pb::common::RequestInfo()));

  pb::common::Range range;
  range.set_start_key(prefix);
  range.set_end_key(Helper::PrefixNext(prefix));

  ScanOutput output;
  output.stream = stream;
  pb::common::CoprocessorV2 coprocessor;
  output.status = TxnEngineHelper::Scan(
      ctx, output.stream, engine, pb::store::IsolationLevel::SnapshotIsolation, 100, range, 1000, false, false, {}, false,
      coprocessor, true, output.txn_result_info, output.kvs, output.entries, output.has_more, output.end_key);

  EXPECT_FALSE(output.status.ok());
  EXPECT_EQ(output.status.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  EXPECT_EQ(output.entries.size(), 0);
  EXPECT_EQ(output.kvs.size(), 0);
}

TEST_F(TxnScanLockCollectionTest, PaginationEndKeyAndContinue) {
  const std::string prefix = "txn_scan_lc_page_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  const std::string k3 = TestKey(prefix, "003");
  PutLock(engine, k1, 50);
  PutCommitted(engine, k2, "v2", 11, 21);
  PutCommitted(engine, k3, "v3", 12, 22);

  auto first = RunScan(engine, prefix, 100, true, {}, nullptr, 2);

  ASSERT_TRUE(first.status.ok()) << first.status.error_str();
  EXPECT_TRUE(first.has_more);
  EXPECT_EQ(first.end_key, k2);
  ASSERT_EQ(first.entries.size(), 2);
  ExpectLockedEntry(first.entries[0], k1, 50);
  ExpectKvEntry(first.entries[1], k2, "v2");

  auto second = RunScan(engine, prefix, 100, true, {}, first.stream, 2);

  ASSERT_TRUE(second.status.ok()) << second.status.error_str();
  EXPECT_FALSE(second.has_more);
  ASSERT_EQ(second.entries.size(), 1);
  ExpectKvEntry(second.entries[0], k3, "v3");
}

TEST_F(TxnScanLockCollectionTest, PaginationEndKeyCanBeLockedEntry) {
  const std::string prefix = "txn_scan_lc_page_locked_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  const std::string k3 = TestKey(prefix, "003");
  PutCommitted(engine, k1, "v1", 10, 20);
  PutLock(engine, k2, 50);
  PutCommitted(engine, k3, "v3", 12, 22);

  auto first = RunScan(engine, prefix, 100, true, {}, nullptr, 2);

  ASSERT_TRUE(first.status.ok()) << first.status.error_str();
  EXPECT_TRUE(first.has_more);
  EXPECT_EQ(first.end_key, k2);
  ASSERT_EQ(first.entries.size(), 2);
  ExpectKvEntry(first.entries[0], k1, "v1");
  ExpectLockedEntry(first.entries[1], k2, 50);

  auto second = RunScan(engine, prefix, 100, true, {}, first.stream, 2);

  ASSERT_TRUE(second.status.ok()) << second.status.error_str();
  EXPECT_FALSE(second.has_more);
  ASSERT_EQ(second.entries.size(), 1);
  ExpectKvEntry(second.entries[0], k3, "v3");
}

TEST_F(TxnScanLockCollectionTest, StreamRejectsModeChange) {
  const std::string prefix = "txn_scan_lc_mode_change_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  PutLock(engine, k1, 50);
  PutCommitted(engine, k2, "v2", 11, 21);

  auto first = RunScan(engine, prefix, 100, true, {}, nullptr, 1);

  ASSERT_TRUE(first.status.ok()) << first.status.error_str();
  EXPECT_TRUE(first.has_more);
  ASSERT_EQ(first.entries.size(), 1);
  ExpectLockedEntry(first.entries[0], k1, 50);

  auto second = RunScan(engine, prefix, 100, false, {}, first.stream, 1);

  EXPECT_FALSE(second.status.ok());
  EXPECT_EQ(second.status.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
}

TEST_F(TxnScanLockCollectionTest, StreamDoesNotRefreshResolvedLocks) {
  const std::string prefix = "txn_scan_lc_no_refresh_resolved_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  const std::string k3 = TestKey(prefix, "003");
  PutCommitted(engine, k1, "v1", 10, 20);
  PutCommitted(engine, k2, "old_v2", 11, 21);
  PutLock(engine, k2, 50);
  PutCommitted(engine, k3, "v3", 12, 22);

  auto first = RunScan(engine, prefix, 100, true, {}, nullptr, 1);

  ASSERT_TRUE(first.status.ok()) << first.status.error_str();
  EXPECT_TRUE(first.has_more);
  ASSERT_EQ(first.entries.size(), 1);
  ExpectKvEntry(first.entries[0], k1, "v1");

  auto second = RunScan(engine, prefix, 100, true, {50}, first.stream, 1);

  ASSERT_TRUE(second.status.ok()) << second.status.error_str();
  EXPECT_TRUE(second.has_more);
  ASSERT_EQ(second.entries.size(), 1);
  ExpectLockedEntry(second.entries[0], k2, 50);
}

TEST_F(TxnScanLockCollectionTest, ReadCommitted) {
  const std::string prefix = "txn_scan_lc_rc_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  PutCommitted(engine, k1, "v1", 10, 20);

  // In RC mode, only pessimistic prewrite locks (for_update_ts > 0, lock_type != Lock)
  // trigger conflicts. Set up a pessimistic prewrite lock with for_update_ts < start_ts.
  {
    pb::store::LockInfo lock_info;
    lock_info.set_primary_lock(k2);
    lock_info.set_lock_ts(50);
    lock_info.set_key(k2);
    lock_info.set_lock_ttl(1000);
    lock_info.set_lock_type(pb::store::Op::Put);
    lock_info.set_for_update_ts(30);

    pb::common::KeyValue lock_kv;
    lock_kv.set_key(mvcc::Codec::EncodeKey(k2, Constant::kLockVer));
    lock_kv.set_value(lock_info.SerializeAsString());
    ASSERT_EQ(engine->Writer()->KvPut(Constant::kTxnLockCF, lock_kv).error_code(), pb::error::OK);
  }

  auto stream = Stream::New(1000);
  auto ctx = std::make_shared<Context>();
  ctx->SetTracker(Tracker::New(pb::common::RequestInfo()));

  pb::common::Range range;
  range.set_start_key(prefix);
  range.set_end_key(Helper::PrefixNext(prefix));

  ScanOutput output;
  output.stream = stream;
  pb::common::CoprocessorV2 coprocessor;
  output.status = TxnEngineHelper::Scan(
      ctx, output.stream, engine, pb::store::IsolationLevel::ReadCommitted, 100, range, 1000, false, false, {}, true,
      coprocessor, true, output.txn_result_info, output.kvs, output.entries, output.has_more, output.end_key);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_FALSE(output.has_more);
  ASSERT_EQ(output.entries.size(), 2);
  ExpectKvEntry(output.entries[0], k1, "v1");
  ExpectLockedEntry(output.entries[1], k2, 50);
}

TEST_F(TxnScanLockCollectionTest, ReadCommittedIgnoresNonConflictingLocks) {
  const std::string prefix = "txn_scan_lc_rc_ignore_";
  ClearTxnPrefix(engine, prefix);

  const std::string k1 = TestKey(prefix, "001");
  const std::string k2 = TestKey(prefix, "002");
  const std::string k3 = TestKey(prefix, "003");
  const std::string k4 = TestKey(prefix, "004");
  PutCommitted(engine, k1, "v1", 10, 20);
  PutCommitted(engine, k2, "old_v2", 11, 21);
  PutCommitted(engine, k3, "old_v3", 12, 22);
  PutCommitted(engine, k4, "old_v4", 13, 23);
  PutLock(engine, k2, 50, 0, pb::store::Op::Lock, 30);
  PutLock(engine, k3, 51, 0, pb::store::Op::Put, 100);
  PutLock(engine, k4, 52);

  auto stream = Stream::New(1000);
  auto ctx = std::make_shared<Context>();
  ctx->SetTracker(Tracker::New(pb::common::RequestInfo()));

  pb::common::Range range;
  range.set_start_key(prefix);
  range.set_end_key(Helper::PrefixNext(prefix));

  ScanOutput output;
  output.stream = stream;
  pb::common::CoprocessorV2 coprocessor;
  output.status = TxnEngineHelper::Scan(
      ctx, output.stream, engine, pb::store::IsolationLevel::ReadCommitted, 100, range, 1000, false, false, {}, true,
      coprocessor, true, output.txn_result_info, output.kvs, output.entries, output.has_more, output.end_key);

  ASSERT_TRUE(output.status.ok()) << output.status.error_str();
  EXPECT_FALSE(output.has_more);
  EXPECT_EQ(output.txn_result_info.ByteSizeLong(), 0);
  ASSERT_EQ(output.entries.size(), 4);
  ExpectKvEntry(output.entries[0], k1, "v1");
  ExpectKvEntry(output.entries[1], k2, "old_v2");
  ExpectKvEntry(output.entries[2], k3, "old_v3");
  ExpectKvEntry(output.entries[3], k4, "old_v4");
}

}  // namespace dingodb

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
#include <utility>
#include <vector>

#include "common/constant.h"
#include "common/helper.h"
#include "config/config.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "engine/txn_engine_helper.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

DECLARE_int64(txn_scan_next_seek_bound);

const std::string kSeekBoundTestRootPath = "./unit_test_txn_scan_seek_bound";
const std::string kSeekBoundTestLogPath = kSeekBoundTestRootPath + "/log";
const std::string kSeekBoundTestStorePath = kSeekBoundTestRootPath + "/db";

const std::string kSeekBoundTestYaml =
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
    kSeekBoundTestLogPath +
    "\n"
    "store:\n"
    "  path: " +
    kSeekBoundTestStorePath + "\n";

// Snapshot-read version-group crawl: a snapshot older than a long run of newer
// versions must not walk the run version by version. These tests drive
// TxnIterator directly and observe the Next-probe accounting exposed by
// GetSkippedVersions().
class TxnScanSeekBoundTest : public testing::Test {
 protected:
  // The snapshot the SI scan reads at; every "newer" version commits after it.
  static constexpr int64_t kSnapshotTs = 500;
  static constexpr int64_t kSeekBound = 8;
  static constexpr int64_t kNewerKa = 50;
  static constexpr int64_t kNewerKb = 30;
  static constexpr int64_t kNewerKc = 25;

  static void SetUpTestSuite() {
    Helper::CreateDirectories(kSeekBoundTestStorePath);

    std::shared_ptr<Config> config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kSeekBoundTestYaml));

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, {Constant::kTxnWriteCF, Constant::kTxnDataCF, Constant::kTxnLockCF, "default"}));

    PrepareData();
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory(kSeekBoundTestRootPath);
  }

  static void PutWrite(const std::string &user_key, int64_t commit_ts, const std::string &short_value) {
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(commit_ts - 10);
    write_info.set_op(pb::store::Op::Put);
    write_info.set_short_value(short_value);

    pb::common::KeyValue kv;
    kv.set_key(mvcc::Codec::EncodeKey(user_key, commit_ts));
    kv.set_value(write_info.SerializeAsString());
    ASSERT_EQ(engine->Writer()->KvPut(Constant::kTxnWriteCF, kv).error_code(), pb::error::Errno::OK);
  }

  static void PrepareData() {
    // "ka": buried visible version, but as the scan's FIRST key its newer run is
    // jumped by the initial Seek(EncodeKey(key, seek_ts)) — never crawled.
    PutWrite("ka", 100, "va");
    for (int64_t i = 0; i < kNewerKa; i++) {
      PutWrite("ka", 1001 + i, "ka-newer");
    }
    // "kb": every version newer than the snapshot, so SI must skip the whole key.
    for (int64_t i = 0; i < kNewerKb; i++) {
      PutWrite("kb", 1001 + i, "kb-newer");
    }
    // "kc": visible version buried under a newer run, reached mid-scan.
    PutWrite("kc", 200, "vc");
    for (int64_t i = 0; i < kNewerKc; i++) {
      PutWrite("kc", 1001 + i, "kc-newer");
    }
  }

  static std::vector<std::pair<std::string, std::string>> SiScan(int64_t seek_bound, int64_t &skipped) {
    int64_t saved_bound = FLAGS_txn_scan_next_seek_bound;
    FLAGS_txn_scan_next_seek_bound = seek_bound;

    pb::common::Range range;
    range.set_start_key("ka");
    range.set_end_key("kz");

    std::vector<std::pair<std::string, std::string>> kvs;
    auto iter = std::make_shared<TxnIterator>(engine, range, kSnapshotTs,
                                              pb::store::IsolationLevel::SnapshotIsolation, std::set<int64_t>{});
    EXPECT_EQ(iter->Init().error_code(), pb::error::Errno::OK);
    EXPECT_EQ(iter->Seek(range.start_key()).error_code(), pb::error::Errno::OK);

    pb::store::TxnResultInfo txn_result_info;
    while (iter->Valid(txn_result_info)) {
      kvs.emplace_back(iter->Key(), iter->Value());
      EXPECT_EQ(iter->Next().error_code(), pb::error::Errno::OK);
    }
    skipped = iter->GetSkippedVersions();

    FLAGS_txn_scan_next_seek_bound = saved_bound;
    return kvs;
  }

  static inline std::shared_ptr<RocksRawEngine> engine;
};

// The seek fallback is a pure access-path change: bound 0 (Next-only crawl) and
// the bounded seek must yield identical snapshot-read results.
TEST_F(TxnScanSeekBoundTest, SnapshotScanResultIndependentOfSeekBound) {
  std::vector<std::pair<std::string, std::string>> expect = {{"ka", "va"}, {"kc", "vc"}};

  int64_t skipped = 0;
  EXPECT_EQ(SiScan(0, skipped), expect);
  EXPECT_EQ(SiScan(kSeekBound, skipped), expect);
}

// After kSeekBound Next probes inside a newer-than-snapshot run, one Seek to
// EncodeKey(user_key, start_ts) must jump the rest of the run — straight on
// the next user key ("kb" has nothing visible) or landing on the visible
// version ("kc"). Probe accounting is the observable: bounded scan spends at
// most kSeekBound probes per run instead of the run length.
TEST_F(TxnScanSeekBoundTest, SeekFallbackCapsNewerVersionCrawl) {
  int64_t skipped_next_only = 0;
  int64_t skipped_with_seek = 0;
  auto next_only = SiScan(0, skipped_next_only);
  auto with_seek = SiScan(kSeekBound, skipped_with_seek);
  EXPECT_EQ(next_only, with_seek);

  // Next-only crawls the full "kb" and "kc" runs version by version. "ka"'s
  // run never counts: the initial Seek(EncodeKey(key, seek_ts)) already lands
  // below it (seek_ts == start_ts under SI) — pinned by the upper bound here.
  EXPECT_GE(skipped_next_only, kNewerKb + kNewerKc);
  EXPECT_LE(skipped_next_only, kNewerKb + kNewerKc + 4);

  // Bounded: kSeekBound probes per newer-run ("kb", "kc") plus one tail Next
  // crossing off "ka" and "kc" after their visible Put => 2 * kSeekBound + 2;
  // small slack to avoid pinning exact bookkeeping.
  EXPECT_LT(skipped_with_seek, skipped_next_only);
  EXPECT_LE(skipped_with_seek, 2 * kSeekBound + 4);
}

}  // namespace dingodb

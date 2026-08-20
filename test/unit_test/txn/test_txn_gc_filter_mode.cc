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
#include <string>
#include <vector>

#include "common/helper.h"
#include "engine/txn_engine_helper.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {

DECLARE_bool(gc_enable_compaction_filter);
DECLARE_bool(gc_enable_safe_point_read_check);

// Pure-logic tests for the filter-mode scan-GC classification (S2): scan GC
// narrowed to delete-mark groups (removed as one raft range per user key)
// plus rollbacks before a head. The raft/apply plumbing is exercised
// end-to-end against a real cluster; these tests pin the decision table and
// the range byte math.
class TxnGcFilterModeTest : public testing::Test {
 protected:
  using State = TxnEngineHelper::GcFilterModeKeyState;
  using Action = TxnEngineHelper::GcFilterModeAction;

  // literal-friendly wrapper: Codec::EncodeKey(const char*) is ambiguous
  static std::string EK(const std::string &user_key, int64_t ts) { return mvcc::Codec::EncodeKey(user_key, ts); }

  static Action Classify(State &state, pb::store::Op op, const std::string &user_key, int64_t ts) {
    encoded_key_holder = EK(user_key, ts);
    return TxnEngineHelper::ClassifyForGcFilterMode(state, op, std::string_view(encoded_key_holder));
  }

  inline static std::string encoded_key_holder;
};

// Newest Put <= safe point: the filter owns it and everything below it.
TEST_F(TxnGcFilterModeTest, PutHeadedGroupFullySkipped) {
  State state;
  EXPECT_EQ(Classify(state, pb::store::Op::Put, "ka", 30), Action::kSkip);
  EXPECT_TRUE(state.head_decided);
  EXPECT_FALSE(state.under_delete_mark);
  EXPECT_EQ(Classify(state, pb::store::Op::Put, "ka", 20), Action::kSkip);
  EXPECT_EQ(Classify(state, pb::store::Op::Delete, "ka", 10), Action::kSkip);
  EXPECT_EQ(Classify(state, pb::store::Op::Rollback, "ka", 5), Action::kSkip);
  EXPECT_TRUE(state.pending_range_start.empty());
}

// Delete-mark head: range recorded (deferred), every member skipped — data
// CF cleanup for the covered versions happens replica-locally when the raft
// range is applied, never from the leader snapshot.
TEST_F(TxnGcFilterModeTest, DeleteMarkGroupBecomesRange) {
  State state;
  EXPECT_EQ(Classify(state, pb::store::Op::Delete, "ka", 30), Action::kSkip);
  EXPECT_TRUE(state.head_decided);
  EXPECT_TRUE(state.under_delete_mark);
  EXPECT_EQ(state.pending_range_start, EK("ka", 30));

  EXPECT_EQ(Classify(state, pb::store::Op::Put, "ka", 20), Action::kSkip);
  EXPECT_EQ(Classify(state, pb::store::Op::Delete, "ka", 15), Action::kSkip);
  EXPECT_EQ(Classify(state, pb::store::Op::Rollback, "ka", 10), Action::kSkip);
  EXPECT_EQ(Classify(state, pb::store::Op::Put, "ka", 5), Action::kSkip);
}

// Rollbacks before any head are scan GC's (the filter keeps head rollbacks:
// computed-commit_ts twin hazard), and they do not occupy the head slot.
TEST_F(TxnGcFilterModeTest, RollbacksBeforeHeadCollected) {
  State state;
  EXPECT_EQ(Classify(state, pb::store::Op::Rollback, "ka", 40), Action::kCollectWriteExact);
  EXPECT_FALSE(state.head_decided);
  EXPECT_EQ(Classify(state, pb::store::Op::Rollback, "ka", 35), Action::kCollectWriteExact);
  EXPECT_EQ(Classify(state, pb::store::Op::Put, "ka", 30), Action::kSkip);
  EXPECT_TRUE(state.head_decided);
  EXPECT_EQ(Classify(state, pb::store::Op::Rollback, "ka", 20), Action::kSkip);
}

// Rollback-only keys keep collecting (no leak even though the compaction
// filter keeps every head-position rollback).
TEST_F(TxnGcFilterModeTest, RollbackOnlyKeyNeverLeaks) {
  State state;
  for (int64_t ts = 50; ts > 0; ts -= 10) {
    EXPECT_EQ(Classify(state, pb::store::Op::Rollback, "ka", ts), Action::kCollectWriteExact);
  }
  EXPECT_FALSE(state.head_decided);
}

// Alien ops decide nothing and collect nothing (mirror legacy "ignore").
TEST_F(TxnGcFilterModeTest, UnexpectedOpSkippedWithoutStateChange) {
  State state;
  EXPECT_EQ(Classify(state, pb::store::Op::Lock, "ka", 40), Action::kSkip);
  EXPECT_FALSE(state.head_decided);
  EXPECT_EQ(Classify(state, pb::store::Op::Put, "ka", 30), Action::kSkip);
  EXPECT_TRUE(state.head_decided);
}

// The emitted range is [mark, PrefixNext(encoded user prefix)): every
// version at or below the mark, nothing above it, nothing of neighbors.
TEST_F(TxnGcFilterModeTest, RangeCoversMarkAndOlderOnly) {
  State state;
  std::vector<pb::common::Range> ranges;
  Classify(state, pb::store::Op::Delete, "ka", 30);
  TxnEngineHelper::FlushGcFilterModePendingRange(state, ranges);

  ASSERT_EQ(ranges.size(), 1);
  const auto &range = ranges[0];
  EXPECT_EQ(range.start_key(), EK("ka", 30));
  EXPECT_EQ(range.end_key(), Helper::PrefixNext(mvcc::Codec::EncodeBytes("ka")));
  EXPECT_TRUE(state.pending_range_start.empty());

  auto inside = [&](const std::string &key) { return key >= range.start_key() && key < range.end_key(); };
  // mark itself and every older version die
  EXPECT_TRUE(inside(EK("ka", 30)));
  EXPECT_TRUE(inside(EK("ka", 29)));
  EXPECT_TRUE(inside(EK("ka", 1)));
  // newer versions and the live world above the safe point survive
  EXPECT_FALSE(inside(EK("ka", 31)));
  EXPECT_FALSE(inside(EK("ka", INT64_MAX)));
  // neighbor user keys (including extensions of the same plain prefix) survive
  EXPECT_FALSE(inside(EK("kb", 1)));
  EXPECT_FALSE(inside(EK("ka0", 1)));
  EXPECT_FALSE(inside(EK("k", 1)));
}

// ts=1 mark: the range still bottoms out inside this user key.
TEST_F(TxnGcFilterModeTest, RangeTsOneBoundary) {
  State state;
  std::vector<pb::common::Range> ranges;
  Classify(state, pb::store::Op::Delete, "ka", 1);
  TxnEngineHelper::FlushGcFilterModePendingRange(state, ranges);

  ASSERT_EQ(ranges.size(), 1);
  EXPECT_TRUE(EK("ka", 0) >= ranges[0].start_key());
  EXPECT_TRUE(EK("ka", 0) < ranges[0].end_key());
  EXPECT_FALSE(EK("kb", 1) < ranges[0].end_key());
}

// No mark, no range; flush is idempotent; malformed pending never emits.
TEST_F(TxnGcFilterModeTest, FlushGuards) {
  State state;
  std::vector<pb::common::Range> ranges;
  TxnEngineHelper::FlushGcFilterModePendingRange(state, ranges);
  EXPECT_TRUE(ranges.empty());

  state.pending_range_start = "short";
  TxnEngineHelper::FlushGcFilterModePendingRange(state, ranges);
  EXPECT_TRUE(ranges.empty());
  EXPECT_TRUE(state.pending_range_start.empty());
}

// Gate: narrowing requires BOTH flags (without the read guard the filter
// refuses to run and Put garbage would be owned by nobody) AND a known
// rocksdb region; every failure falls back to the legacy full scan.
TEST_F(TxnGcFilterModeTest, GateRequiresBothFlagsAndKnownRegion) {
  FLAGS_gc_enable_compaction_filter = false;
  FLAGS_gc_enable_safe_point_read_check = false;
  EXPECT_FALSE(TxnEngineHelper::IsGcFilterMode(999999));

  FLAGS_gc_enable_compaction_filter = true;  // read guard still off -> refused
  EXPECT_FALSE(TxnEngineHelper::IsGcFilterMode(999999));

  FLAGS_gc_enable_safe_point_read_check = true;  // both on, unknown region -> refused
  EXPECT_FALSE(TxnEngineHelper::IsGcFilterMode(999999));

  FLAGS_gc_enable_compaction_filter = true;
  FLAGS_gc_enable_safe_point_read_check = true;
}

}  // namespace dingodb

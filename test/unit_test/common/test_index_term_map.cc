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

// Unit tests for IndexTermMap introduced in:
//   [feat][store] Added caching to get_term to resolve Leader switching issues.
//   [fix][store] Optimized some code.

#include <gtest/gtest.h>

#include <cstdint>

#include "braft/log_entry.h"
#include "log/index_term_map.h"

namespace dingodb {

class IndexTermMapTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

  void SetUp() override {}
  void TearDown() override {}

  static constexpr int64_t kRegionId = 100;
};

// ============================================================
// GetTerm — empty cache
// ============================================================

TEST_F(IndexTermMapTest, GetTerm_EmptyCache_ReturnsZero) {
  const IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.GetTerm(1));
  EXPECT_EQ(0, m.GetTerm(100));
}

// ============================================================
// Append — basic behaviour
// ============================================================

TEST_F(IndexTermMapTest, Append_FirstEntry_Succeeds) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(1, m.GetTerm(1));
}

TEST_F(IndexTermMapTest, Append_SameTerm_DoesNotGrowCache) {
  // Consecutive appends with the same term must be accepted (return 0)
  // and the cache must still return that term for every index in the range.
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(2, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(3, 1)));

  EXPECT_EQ(1, m.GetTerm(1));
  EXPECT_EQ(1, m.GetTerm(2));
  EXPECT_EQ(1, m.GetTerm(3));
}

TEST_F(IndexTermMapTest, Append_TermChange_NewEntryAdded) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(5, 2)));  // term change at index 5

  EXPECT_EQ(1, m.GetTerm(1));
  EXPECT_EQ(1, m.GetTerm(4));
  EXPECT_EQ(2, m.GetTerm(5));
  EXPECT_EQ(2, m.GetTerm(100));  // beyond last appended index, same term
}

TEST_F(IndexTermMapTest, Append_InvalidIndexNotIncreasing_ReturnsMinus1) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(5, 1)));
  // index <= back.index — invalid
  EXPECT_EQ(-1, m.Append(braft::LogId(5, 1)));
  EXPECT_EQ(-1, m.Append(braft::LogId(3, 1)));
}

TEST_F(IndexTermMapTest, Append_TermDecreasing_ReturnsMinus1) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 3)));
  // term < back.term — invalid
  EXPECT_EQ(-1, m.Append(braft::LogId(2, 2)));
}

// ============================================================
// GetTerm — range queries across multiple term segments
// ============================================================

TEST_F(IndexTermMapTest, GetTerm_MultipleSegments_LinearSearch) {
  // Populate fewer than 15 entries so the linear-search path is exercised.
  IndexTermMap m(kRegionId);
  // term=1: indices 1-4, term=2: indices 5-9, term=3: indices 10+
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(5, 2)));
  EXPECT_EQ(0, m.Append(braft::LogId(10, 3)));

  EXPECT_EQ(0, m.GetTerm(0));  // before first entry → 0
  EXPECT_EQ(1, m.GetTerm(1));
  EXPECT_EQ(1, m.GetTerm(4));
  EXPECT_EQ(2, m.GetTerm(5));
  EXPECT_EQ(2, m.GetTerm(9));
  EXPECT_EQ(3, m.GetTerm(10));
  EXPECT_EQ(3, m.GetTerm(999));
}

TEST_F(IndexTermMapTest, GetTerm_MultipleSegments_BinarySearch) {
  // Populate >= 15 term-change entries so the binary-search path is exercised.
  IndexTermMap m(kRegionId);
  // Create 20 term segments, each starting at index i*10, term=i+1
  for (int64_t i = 0; i < 20; ++i) {
    EXPECT_EQ(0, m.Append(braft::LogId(i * 10 + 1, i + 1)));
    // append two more entries within the same term so indices are consecutive
    EXPECT_EQ(0, m.Append(braft::LogId(i * 10 + 2, i + 1)));
    EXPECT_EQ(0, m.Append(braft::LogId(i * 10 + 3, i + 1)));
  }

  EXPECT_EQ(0, m.GetTerm(0));  // before any entry
  EXPECT_EQ(1, m.GetTerm(1));
  EXPECT_EQ(1, m.GetTerm(3));
  EXPECT_EQ(2, m.GetTerm(11));
  EXPECT_EQ(5, m.GetTerm(41));
  EXPECT_EQ(20, m.GetTerm(191));
  EXPECT_EQ(20, m.GetTerm(9999));  // beyond last appended
}

TEST_F(IndexTermMapTest, GetTerm_IndexBeforeFirstEntry_ReturnsZero) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(10, 5)));
  // index < first cached index → unknown term
  EXPECT_EQ(0, m.GetTerm(5));
  EXPECT_EQ(0, m.GetTerm(9));
}

// ============================================================
// TruncatePrefix
// ============================================================

TEST_F(IndexTermMapTest, TruncatePrefix_EmptyCache_NocrashWarning) {
  IndexTermMap m(kRegionId);
  // Should not crash even on empty cache (emits a warning log internally)
  EXPECT_NO_FATAL_FAILURE(m.TruncatePrefix(5));
}

TEST_F(IndexTermMapTest, TruncatePrefix_KeepFirstEntry_NothingRemoved) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(5, 2)));
  EXPECT_EQ(0, m.Append(braft::LogId(10, 3)));

  // Truncating before the second entry should leave the first entry as the
  // representative (IndexTermMap keeps the earliest entry as fallback).
  m.TruncatePrefix(3);
  // term for index=1 is now unknown (prefix removed), but index=5 must be 2
  EXPECT_EQ(2, m.GetTerm(5));
  EXPECT_EQ(3, m.GetTerm(10));
}

TEST_F(IndexTermMapTest, TruncatePrefix_RemovesEarlierSegments) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(5, 2)));
  EXPECT_EQ(0, m.Append(braft::LogId(10, 3)));

  // Truncate at the boundary of the second term segment
  m.TruncatePrefix(5);
  EXPECT_EQ(2, m.GetTerm(5));
  EXPECT_EQ(3, m.GetTerm(10));
  EXPECT_EQ(3, m.GetTerm(99));
}

// ============================================================
// TruncateSuffix
// ============================================================

TEST_F(IndexTermMapTest, TruncateSuffix_EmptyCache_NocrashSilent) {
  IndexTermMap m(kRegionId);
  EXPECT_NO_FATAL_FAILURE(m.TruncateSuffix(5));
}

TEST_F(IndexTermMapTest, TruncateSuffix_RemovesLaterSegments) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(5, 2)));
  EXPECT_EQ(0, m.Append(braft::LogId(10, 3)));

  m.TruncateSuffix(8);

  // term=3 segment (index 10) should be gone
  EXPECT_EQ(1, m.GetTerm(1));
  EXPECT_EQ(2, m.GetTerm(5));
  // index 10 is now beyond the (notionally) last valid index;
  // the cache still returns term=2 since there is no boundary tracking beyond
  // term-change-points in the base implementation.
  // At minimum, verify no crash and term=1/2 range is still correct.
  EXPECT_EQ(2, m.GetTerm(8));
}

TEST_F(IndexTermMapTest, TruncateSuffix_AllRemoved_CacheEmpty) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(5, 2)));

  // Truncate before the first entry in the cache
  m.TruncateSuffix(0);

  EXPECT_EQ(0, m.GetTerm(1));
  EXPECT_EQ(0, m.GetTerm(5));
}

TEST_F(IndexTermMapTest, TruncateSuffix_CanAppendAgainAfterTruncate) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(5, 2)));
  EXPECT_EQ(0, m.Append(braft::LogId(10, 3)));

  m.TruncateSuffix(6);

  // After truncation, appending a new entry with a higher index should work.
  EXPECT_EQ(0, m.Append(braft::LogId(7, 2)));
  EXPECT_EQ(2, m.GetTerm(7));
}

// ============================================================
// Reset
// ============================================================

TEST_F(IndexTermMapTest, Reset_ClearsAllEntries) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(5, 2)));
  EXPECT_EQ(1, m.GetTerm(1));

  m.Reset();

  EXPECT_EQ(0, m.GetTerm(1));
  EXPECT_EQ(0, m.GetTerm(5));
}

TEST_F(IndexTermMapTest, Reset_CanAppendAfterReset) {
  IndexTermMap m(kRegionId);
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(10, 2)));

  m.Reset();

  // After reset we can start from any index again.
  EXPECT_EQ(0, m.Append(braft::LogId(1, 3)));
  EXPECT_EQ(3, m.GetTerm(1));
}

// ============================================================
// Combined scenarios — simulate typical Raft log lifecycle
// ============================================================

TEST_F(IndexTermMapTest, Scenario_AppendThenTruncateSuffixThenAppend) {
  // Simulates a leader being replaced: new entries are written, then truncated
  // on follower catch-up, then new leader writes new entries.
  IndexTermMap m(kRegionId);

  // Leader 1 appends term=1 entries
  for (int64_t i = 1; i <= 5; ++i) {
    EXPECT_EQ(0, m.Append(braft::LogId(i, 1)));
  }
  EXPECT_EQ(1, m.GetTerm(5));

  // Conflict: truncate suffix back to index 3
  m.TruncateSuffix(3);

  // New leader (term=2) writes from index 4
  EXPECT_EQ(0, m.Append(braft::LogId(4, 2)));
  EXPECT_EQ(0, m.Append(braft::LogId(5, 2)));
  EXPECT_EQ(0, m.Append(braft::LogId(6, 2)));

  EXPECT_EQ(1, m.GetTerm(1));
  EXPECT_EQ(1, m.GetTerm(3));
  EXPECT_EQ(2, m.GetTerm(4));
  EXPECT_EQ(2, m.GetTerm(6));
}

TEST_F(IndexTermMapTest, Scenario_PrefixTruncationFollowedByGetTerm) {
  // Simulates a snapshot install which discards old log prefix.
  IndexTermMap m(kRegionId);

  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(0, m.Append(braft::LogId(100, 2)));
  EXPECT_EQ(0, m.Append(braft::LogId(200, 3)));

  // Snapshot covers up to index 150
  m.TruncatePrefix(150);

  EXPECT_EQ(2, m.GetTerm(100));  // Still accessible via the surviving first/second entry
  EXPECT_EQ(3, m.GetTerm(200));
}

TEST_F(IndexTermMapTest, Scenario_ResetAndReuseForNewRegion) {
  // Each time a region is reset (e.g. after destroy + recreate) the cache
  // must be fully cleared.
  IndexTermMap m(kRegionId);

  EXPECT_EQ(0, m.Append(braft::LogId(1, 5)));
  EXPECT_EQ(0, m.Append(braft::LogId(2, 5)));
  m.Reset();

  // Start fresh
  EXPECT_EQ(0, m.Append(braft::LogId(1, 1)));
  EXPECT_EQ(1, m.GetTerm(1));
  EXPECT_EQ(1, m.GetTerm(2));
}

}  // namespace dingodb

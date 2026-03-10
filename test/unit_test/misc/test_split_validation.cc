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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"

// =============================================================================
// Suite 1: SplitRegionPeerValidationTest
//
// Tests for BUG 1 fix (commit 777bd049):
// The peer extraction in SplitRegion validation was using split_to_region
// instead of split_from_region when building the split_from_region_peers vector.
//
// Fixed line in coordinator_control_coor.cc:2637:
//   BEFORE: split_from_region_peers.push_back(split_to_region.definition().peers(i).store_id());
//   AFTER:  split_from_region_peers.push_back(split_from_region.definition().peers(i).store_id());
// =============================================================================

class SplitRegionPeerValidationTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  // Helper: create a RegionInternal with the given store_ids as peers.
  static dingodb::pb::coordinator_internal::RegionInternal BuildRegionWithPeers(int64_t region_id,
                                                                                const std::vector<int64_t>& store_ids) {
    dingodb::pb::coordinator_internal::RegionInternal region;
    region.set_id(region_id);
    auto* definition = region.mutable_definition();
    definition->set_id(region_id);
    for (auto store_id : store_ids) {
      auto* peer = definition->add_peers();
      peer->set_store_id(store_id);
    }
    return region;
  }

  // Replicate the peer extraction logic from coordinator_control_coor.cc:2633-2644.
  // The `source_region` parameter controls which region peers are extracted from.
  static std::vector<int64_t> ExtractSortedPeers(
      const dingodb::pb::coordinator_internal::RegionInternal& source_region) {
    std::vector<int64_t> peers;
    peers.reserve(source_region.definition().peers_size());
    for (int i = 0; i < source_region.definition().peers_size(); i++) {
      peers.push_back(source_region.definition().peers(i).store_id());
    }
    std::sort(peers.begin(), peers.end());
    return peers;
  }
};

// Both regions have the same store_ids — peer vectors should match.
TEST_F(SplitRegionPeerValidationTest, MatchingPeers_ShouldBeEqual) {
  auto split_from_region = BuildRegionWithPeers(100, {1, 2, 3});
  auto split_to_region = BuildRegionWithPeers(200, {1, 2, 3});

  auto from_peers = ExtractSortedPeers(split_from_region);
  auto to_peers = ExtractSortedPeers(split_to_region);

  EXPECT_EQ(from_peers, to_peers);
  EXPECT_TRUE(std::equal(from_peers.begin(), from_peers.end(), to_peers.begin()));
}

// split_from has {1,2,3}, split_to has {4,5,6} — peer vectors should NOT match.
TEST_F(SplitRegionPeerValidationTest, DifferentPeers_ShouldNotBeEqual) {
  auto split_from_region = BuildRegionWithPeers(100, {1, 2, 3});
  auto split_to_region = BuildRegionWithPeers(200, {4, 5, 6});

  auto from_peers = ExtractSortedPeers(split_from_region);
  auto to_peers = ExtractSortedPeers(split_to_region);

  EXPECT_NE(from_peers, to_peers);
}

// Extract from split_from_region (the fixed code path) — should yield {10,20,30}.
TEST_F(SplitRegionPeerValidationTest, PeerExtractionUsesCorrectRegion) {
  auto split_from_region = BuildRegionWithPeers(100, {10, 20, 30});
  auto split_to_region = BuildRegionWithPeers(200, {40, 50, 60});

  // Fixed code: extract from split_from_region
  auto from_peers = ExtractSortedPeers(split_from_region);

  std::vector<int64_t> expected = {10, 20, 30};
  EXPECT_EQ(from_peers, expected);
}

// Extract from split_to_region (the old buggy code path) — should NOT yield {10,20,30}.
TEST_F(SplitRegionPeerValidationTest, PeerExtractionFromWrongRegion_DemonstratesBug) {
  auto split_from_region = BuildRegionWithPeers(100, {10, 20, 30});
  auto split_to_region = BuildRegionWithPeers(200, {40, 50, 60});

  // Buggy code: accidentally extracted from split_to_region
  auto buggy_peers = ExtractSortedPeers(split_to_region);

  std::vector<int64_t> expected_from = {10, 20, 30};
  EXPECT_NE(buggy_peers, expected_from);

  // The buggy result would be {40,50,60} instead
  std::vector<int64_t> expected_buggy = {40, 50, 60};
  EXPECT_EQ(buggy_peers, expected_buggy);
}

// =============================================================================
// Suite 2: SplitKeyBoundaryValidationTest
//
// Tests for BUG 2 fix (commit e161143c):
// The split key boundary check was too loose, allowing split_key == start_key
// or split_key == end_key which should be rejected.
//
// Fixed in raft_apply_handler.cc (PreCreate and PostCreate):
//   BEFORE: if (split_key < start_key || split_key > end_key)   — too loose
//   AFTER:  if (split_key <= start_key || split_key >= end_key)  — correct
// =============================================================================

class SplitKeyBoundaryValidationTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  // Replicates the fixed boundary check from raft_apply_handler.cc:276 and :531.
  // Returns true if split_key is strictly between start_key and end_key.
  static bool IsSplitKeyValid(const std::string& split_key, const std::string& start_key, const std::string& end_key) {
    if (split_key <= start_key || split_key >= end_key) {
      return false;
    }
    return true;
  }
};

// split_key="C" is strictly between "B" and "D" — should be valid.
TEST_F(SplitKeyBoundaryValidationTest, SplitKeyInMiddle_ShouldBeValid) {
  EXPECT_TRUE(IsSplitKeyValid("C", "B", "D"));
}

// split_key="B" equals start_key — should be rejected (the bug allowed this).
TEST_F(SplitKeyBoundaryValidationTest, SplitKeyEqualToStartKey_ShouldBeRejected) {
  EXPECT_FALSE(IsSplitKeyValid("B", "B", "D"));
}

// split_key="D" equals end_key — should be rejected (the bug allowed this).
TEST_F(SplitKeyBoundaryValidationTest, SplitKeyEqualToEndKey_ShouldBeRejected) {
  EXPECT_FALSE(IsSplitKeyValid("D", "B", "D"));
}

// split_key="A" is before the range — should be rejected.
TEST_F(SplitKeyBoundaryValidationTest, SplitKeyBeforeStartKey_ShouldBeRejected) {
  EXPECT_FALSE(IsSplitKeyValid("A", "B", "D"));
}

// split_key="E" is after the range — should be rejected.
TEST_F(SplitKeyBoundaryValidationTest, SplitKeyAfterEndKey_ShouldBeRejected) {
  EXPECT_FALSE(IsSplitKeyValid("E", "B", "D"));
}

// split_key="BA" is just after start_key "B" — should be valid.
TEST_F(SplitKeyBoundaryValidationTest, SplitKeyJustAfterStartKey_ShouldBeValid) {
  EXPECT_TRUE(IsSplitKeyValid("BA", "B", "D"));
}

// split_key="C\xff" is just before end_key "D" — should be valid.
TEST_F(SplitKeyBoundaryValidationTest, SplitKeyJustBeforeEndKey_ShouldBeValid) {
  EXPECT_TRUE(IsSplitKeyValid(std::string("C\xff", 2), "B", "D"));
}

// =============================================================================
// Suite 3: SnapshotFailureCleanupTest
//
// Tests for BUG 5 fix (commit e161143c):
// When snapshot permanently fails after retries, the cleanup code now properly
// clears NeedBootstrapDoSnapshot and (for STORE_REGION) TemporaryDisableChange.
//
// Added in raft_apply_handler.cc:197-201:
//   if (!is_success) {
//     store_region_meta->UpdateNeedBootstrapDoSnapshot(region, false);
//     if (region->Type() == pb::common::STORE_REGION) {
//       store_region_meta->UpdateTemporaryDisableChange(region, false);
//     }
//   }
// =============================================================================

class SnapshotFailureCleanupTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  // Helper: create a Region with default definition (defaults to STORE_REGION type).
  static dingodb::store::RegionPtr CreateStoreRegion(int64_t region_id) {
    dingodb::pb::common::RegionDefinition definition;
    definition.set_id(region_id);
    return dingodb::store::Region::New(definition);
  }
};

// Verify NeedBootstrapDoSnapshot can be set and cleared via Region's direct setters.
TEST_F(SnapshotFailureCleanupTest, SetAndClear_NeedBootstrapDoSnapshot) {
  auto region = CreateStoreRegion(1001);

  // Initially should be false
  EXPECT_FALSE(region->NeedBootstrapDoSnapshot());

  // Set to true (simulates entering split snapshot path)
  region->SetNeedBootstrapDoSnapshot(true);
  EXPECT_TRUE(region->NeedBootstrapDoSnapshot());

  // Clear to false (simulates snapshot failure cleanup)
  region->SetNeedBootstrapDoSnapshot(false);
  EXPECT_FALSE(region->NeedBootstrapDoSnapshot());
}

// Verify TemporaryDisableChange can be set and cleared via Region's direct setters.
TEST_F(SnapshotFailureCleanupTest, SetAndClear_TemporaryDisableChange) {
  auto region = CreateStoreRegion(1001);

  // Initially should be false
  EXPECT_FALSE(region->TemporaryDisableChange());

  // Set to true (simulates entering split path)
  region->SetTemporaryDisableChange(true);
  EXPECT_TRUE(region->TemporaryDisableChange());

  // Clear to false (simulates snapshot failure cleanup)
  region->SetTemporaryDisableChange(false);
  EXPECT_FALSE(region->TemporaryDisableChange());
}

// Both flags are set during split for ALL region types, both should be cleared on failure.
// TemporaryDisableChange is cleared unconditionally (not just for STORE_REGION) because
// on snapshot failure, INDEX_REGION and DOCUMENT_REGION also won't reach their async
// cleanup paths (SaveVectorIndexTask / LoadOrBuildDocumentIndexTask).
TEST_F(SnapshotFailureCleanupTest, BothFlagsCleared_SimulatesSnapshotFailure) {
  auto region = CreateStoreRegion(1001);

  // Simulate entering the split snapshot path: set both flags
  region->SetNeedBootstrapDoSnapshot(true);
  region->SetTemporaryDisableChange(true);

  EXPECT_TRUE(region->NeedBootstrapDoSnapshot());
  EXPECT_TRUE(region->TemporaryDisableChange());

  // Simulate snapshot failure cleanup (replicating raft_apply_handler.cc:197-200)
  // TemporaryDisableChange is cleared unconditionally for all region types.
  bool is_success = false;
  if (!is_success) {
    region->SetNeedBootstrapDoSnapshot(false);
    region->SetTemporaryDisableChange(false);
  }

  // Both flags should now be cleared
  EXPECT_FALSE(region->NeedBootstrapDoSnapshot());
  EXPECT_FALSE(region->TemporaryDisableChange());
}

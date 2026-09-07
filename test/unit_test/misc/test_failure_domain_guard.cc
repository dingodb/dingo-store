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
#include <string>
#include <vector>

#include "coordinator/coordinator_control.h"
#include "proto/error.pb.h"

// Unit tests for the pure decisions behind the ChangePeer failure-domain guard:
//   - CoordinatorControl::CheckFailureDomainNoWorse, shared by ChangePeerRegionWithJob and
//     ChangePairPeerRegionWithJob;
//   - CoordinatorControl::BuildEffectiveStoreIds, which turns ChangePeerRegionWithJob's final
//     diff vectors into the peer set the guard must judge.
//
// The guard answers one question: does the new peer set pack more replicas into a single
// failure domain (host) than the current one? It is deliberately "no worse", not "must be
// legal": regions created before domain awareness may already sit on one host and have to
// stay repairable, so a neutral change must pass and only a regression is rejected.
//
// Host labels mirror the production cluster: four machines, five store instances each.

class FailureDomainGuardTest : public testing::Test {
 protected:
  using ShadowDecision = dingodb::CoordinatorControl::ShadowDecision;

  static butil::Status Check(const std::vector<std::string>& old_keys, const std::vector<std::string>& new_keys) {
    return dingodb::CoordinatorControl::CheckFailureDomainNoWorse(kRegionId, old_keys, new_keys);
  }

  static std::vector<int64_t> Build(const std::vector<int64_t>& old_ids, const std::vector<int64_t>& diff_less,
                                    const std::vector<int64_t>& diff_more) {
    return dingodb::CoordinatorControl::BuildEffectiveStoreIds(old_ids, diff_less, diff_more);
  }

  static constexpr int64_t kRegionId = 80001;
  static constexpr int64_t kStoreA = 1;
  static constexpr int64_t kStoreB = 2;
  static constexpr int64_t kStoreC = 3;
  static constexpr int64_t kStoreD = 4;
  static const std::string kHost25;
  static const std::string kHost26;
  static const std::string kHost28;
};

const std::string FailureDomainGuardTest::kHost25 = "10.220.68.25";
const std::string FailureDomainGuardTest::kHost26 = "10.220.68.26";
const std::string FailureDomainGuardTest::kHost28 = "10.220.68.28";

// Production layout (1002,1005,1007): two replicas on .25, one on .26 -> profile [2,1].

TEST_F(FailureDomainGuardTest, SpreadingOutIsAccepted) {
  // 1005(.25) -> 1019(.28): [2,1] -> [1,1,1]
  auto status = Check({kHost25, kHost25, kHost26}, {kHost25, kHost26, kHost28});
  EXPECT_TRUE(status.ok()) << status.error_str();
}

TEST_F(FailureDomainGuardTest, NeutralSameHostSwapIsAccepted) {
  // 1005(.25) -> 1003(.25): [2,1] -> [2,1]. Useless but harmless; rejecting it would also
  // block legitimate same-host maintenance moves.
  auto status = Check({kHost25, kHost25, kHost26}, {kHost25, kHost25, kHost26});
  EXPECT_TRUE(status.ok()) << status.error_str();
}

TEST_F(FailureDomainGuardTest, PackingOntoOneHostIsRejected) {
  // 1007(.26) -> 1003(.25): [2,1] -> [3]. One machine would hold every replica.
  auto status = Check({kHost25, kHost25, kHost26}, {kHost25, kHost25, kHost25});
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(dingodb::pb::error::Errno::ECHANGE_PEER_FAILURE_DOMAIN_WORSE, status.error_code());
}

TEST_F(FailureDomainGuardTest, AlreadySpreadCannotRegress) {
  // [1,1,1] -> [2,1]
  auto status = Check({kHost25, kHost26, kHost28}, {kHost25, kHost25, kHost26});
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(dingodb::pb::error::Errno::ECHANGE_PEER_FAILURE_DOMAIN_WORSE, status.error_code());
}

TEST_F(FailureDomainGuardTest, LegacyPackedLayoutStaysRepairable) {
  // A region that is already [3] must not be frozen: a same-host swap is neutral, and any
  // move off the host is an improvement.
  EXPECT_TRUE(Check({kHost25, kHost25, kHost25}, {kHost25, kHost25, kHost25}).ok());
  EXPECT_TRUE(Check({kHost25, kHost25, kHost25}, {kHost25, kHost25, kHost26}).ok());
}

TEST_F(FailureDomainGuardTest, ErrorMessageCarriesBothProfiles) {
  auto status = Check({kHost25, kHost25, kHost26}, {kHost25, kHost25, kHost25});
  ASSERT_FALSE(status.ok());
  const std::string msg = status.error_str();
  EXPECT_NE(std::string::npos, msg.find("before:[2,1]")) << msg;
  EXPECT_NE(std::string::npos, msg.find("after:[3]")) << msg;
  EXPECT_NE(std::string::npos, msg.find("region_id:80001")) << msg;
}

TEST_F(FailureDomainGuardTest, ComparesProfilesNotHostIdentities) {
  // Only the shape matters, not which host holds what: swapping two peers between hosts keeps
  // the profile at [2,1] and must pass, even though the per-host counts changed.
  EXPECT_TRUE(Check({kHost25, kHost25, kHost26}, {kHost26, kHost26, kHost25}).ok());

  // And a region that moves entirely to other machines while staying spread is fine.
  EXPECT_TRUE(Check({kHost25, kHost26, kHost28}, {kHost26, kHost28, kHost25}).ok());
}

// === BuildEffectiveStoreIds: the peer set ChangePeerRegionWithJob hands to the guard ===

TEST_F(FailureDomainGuardTest, EffectiveStoreIdsLegacyAdd) {
  // Legacy path, add one brand-new store: old + 1.
  EXPECT_EQ((std::vector<int64_t>{kStoreA, kStoreB, kStoreC, kStoreD}),
            Build({kStoreA, kStoreB, kStoreC}, {}, {kStoreD}));
}

TEST_F(FailureDomainGuardTest, EffectiveStoreIdsLegacyRemove) {
  // Legacy path, drop one peer: old - 1, order of the survivors preserved.
  EXPECT_EQ((std::vector<int64_t>{kStoreA, kStoreC}), Build({kStoreA, kStoreB, kStoreC}, {kStoreB}, {}));
}

TEST_F(FailureDomainGuardTest, EffectiveStoreIdsNoDiffIsIdentity) {
  EXPECT_EQ((std::vector<int64_t>{kStoreA, kStoreB, kStoreC}), Build({kStoreA, kStoreB, kStoreC}, {}, {}));
}

TEST_F(FailureDomainGuardTest, EffectiveStoreIdsShadowActivationIsIdempotent) {
  // verify_peer_on_store kAddShadow: the target is still registered in the region definition,
  // so "adding" it must not grow the set or duplicate the id. This is the input the guard saw
  // as N+1 replicas and wrongly pushed into the add quota.
  auto effective = Build({kStoreA, kStoreB, kStoreC}, {}, {kStoreC});
  EXPECT_EQ((std::vector<int64_t>{kStoreA, kStoreB, kStoreC}), effective);
  EXPECT_EQ(3u, effective.size());
  EXPECT_EQ(1, std::count(effective.begin(), effective.end(), kStoreC));
}

TEST_F(FailureDomainGuardTest, EffectiveStoreIdsShadowCleanupRemoves) {
  // verify_peer_on_store kRemoveShadow: the shadow leaves the set.
  EXPECT_EQ((std::vector<int64_t>{kStoreA, kStoreB}), Build({kStoreA, kStoreB, kStoreC}, {kStoreC}, {}));
}

// === Shadow maintenance end to end through the pure functions ===

TEST_F(FailureDomainGuardTest, ShadowActivationPassesGuardOnSpreadRegion) {
  // Verify mode: peers ABC registered, C is a shadow (not running on its store) -> activate C.
  auto decision = dingodb::CoordinatorControl::DecideShadowAction(
      kRegionId, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB}, {kStoreC});
  ASSERT_EQ(ShadowDecision::kAddShadow, decision.action);
  ASSERT_EQ(kStoreC, decision.target_store_id);

  // ChangePeerRegionWithJob turns this into diff_more = {C}; C is already a peer, so the
  // effective set must have the same size as before.
  const std::vector<int64_t> old_ids = {kStoreA, kStoreB, kStoreC};
  auto effective = Build(old_ids, {}, {decision.target_store_id});
  ASSERT_EQ(old_ids, effective);

  // One store per host: the same-size guard sees [1,1,1] -> [1,1,1] and accepts.
  const std::vector<std::string> old_keys = {kHost25, kHost26, kHost28};
  EXPECT_TRUE(Check(old_keys, old_keys).ok());

  // Regression: counting the activated shadow twice yields N+1 keys, which routes the request
  // into the add quota. On a four-host cluster quota = ceil(4/4) = 1, so [2,1,1] is rejected.
  const std::vector<std::string> duplicated_keys = {kHost25, kHost26, kHost28, kHost28};
  auto status = dingodb::CoordinatorControl::CheckFailureDomainQuota(kRegionId, old_keys, duplicated_keys,
                                                                     /*domain_count=*/4);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(dingodb::pb::error::Errno::ECHANGE_PEER_FAILURE_DOMAIN_WORSE, status.error_code());
}

TEST_F(FailureDomainGuardTest, ShadowCleanupShrinksPeerSet) {
  // Verify mode: peers ABC registered, C is a shadow, the operator asks for AB -> clean up C.
  auto decision = dingodb::CoordinatorControl::DecideShadowAction(kRegionId, {kStoreA, kStoreB},
                                                                  {kStoreA, kStoreB, kStoreC}, {kStoreA, kStoreB},
                                                                  {kStoreC});
  ASSERT_EQ(ShadowDecision::kRemoveShadow, decision.action);
  ASSERT_EQ(kStoreC, decision.target_store_id);

  // diff_less = {C}: the set shrinks, which the guard always accepts (removing a replica never
  // packs more of them together).
  auto effective = Build({kStoreA, kStoreB, kStoreC}, {decision.target_store_id}, {});
  EXPECT_EQ((std::vector<int64_t>{kStoreA, kStoreB}), effective);
  EXPECT_LT(effective.size(), 3u);
}

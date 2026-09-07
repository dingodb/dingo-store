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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "coordinator/balance_leader.h"
#include "coordinator/balance_region.h"
#include "coordinator/coordinator_control.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"

namespace dingodb {
// Defined in coordinator_control_coor.cc inside namespace dingodb.
DECLARE_bool(enable_failure_domain_guard);
DECLARE_bool(enable_failure_domain_placement);
}  // namespace dingodb

// Failure-domain awareness across the placement paths that decide where replicas live:
//   - SelectStore's best-effort spread (PickStoresAcrossFailureDomains)
//   - the add-only ChangePeer quota (CheckFailureDomainQuota)
//   - balance region's move pre-check (BalanceRegionScheduler::IsFailureDomainNoWorse)
//   - balance leader's per-domain leader quota
//
// Host labels mirror the production cluster: four machines, five store instances each.

namespace {

const std::string kHost25 = "10.220.68.25";
const std::string kHost26 = "10.220.68.26";
const std::string kHost27 = "10.220.68.27";
const std::string kHost28 = "10.220.68.28";

dingodb::pb::common::Store MakeStore(int64_t store_id, const std::string& host) {
  dingodb::pb::common::Store store;
  store.set_id(store_id);
  store.set_state(dingodb::pb::common::StoreState::STORE_NORMAL);
  store.mutable_server_location()->set_host(host);
  store.mutable_server_location()->set_port(20000 + static_cast<int32_t>(store_id % 5));
  return store;
}

dingodb::pb::common::Peer MakePeer(int64_t store_id, const std::string& host) {
  dingodb::pb::common::Peer peer;
  peer.set_store_id(store_id);
  peer.set_role(dingodb::pb::common::PeerRole::VOTER);
  peer.mutable_server_location()->set_host(host);
  peer.mutable_server_location()->set_port(20000 + static_cast<int32_t>(store_id % 5));
  return peer;
}

std::vector<int64_t> Ids(const std::vector<dingodb::pb::common::Store>& stores) {
  std::vector<int64_t> ids;
  ids.reserve(stores.size());
  for (const auto& store : stores) {
    ids.push_back(store.id());
  }
  return ids;
}

std::vector<int32_t> Profile(const std::vector<dingodb::pb::common::Store>& stores) {
  std::vector<std::string> keys;
  keys.reserve(stores.size());
  for (const auto& store : stores) {
    keys.push_back(dingodb::Helper::FailureDomainKey(store));
  }
  return dingodb::Helper::FailureDomainProfile(keys);
}

using RegionInternal = dingodb::pb::coordinator_internal::RegionInternal;

class MockCoordinatorControl : public dingodb::CoordinatorControl {
 public:
  explicit MockCoordinatorControl(std::map<int64_t, RegionInternal> regions)
      : dingodb::CoordinatorControl(nullptr, nullptr, nullptr), regions_(std::move(regions)) {}
  ~MockCoordinatorControl() override = default;

  RegionInternal GetRegion(int64_t region_id) override {
    auto it = regions_.find(region_id);
    return (it != regions_.end()) ? it->second : RegionInternal{};
  }

 private:
  std::map<int64_t, RegionInternal> regions_;
};

}  // namespace

class FailureDomainPlacementTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

// === PickStoresAcrossFailureDomains ===

TEST_F(FailureDomainPlacementTest, PickSpreadsOverHostsWhenEnough) {
  // Weight order puts three .25 instances first, exactly the shape that produced (1002,1005,1007).
  std::vector<dingodb::pb::common::Store> candidates = {
      MakeStore(1002, kHost25), MakeStore(1005, kHost25), MakeStore(1001, kHost25), MakeStore(1007, kHost26),
      MakeStore(1012, kHost27), MakeStore(1019, kHost28), MakeStore(1006, kHost26),
  };

  auto pick = dingodb::CoordinatorControl::PickStoresAcrossFailureDomains(candidates, 3);

  EXPECT_EQ(4, pick.domain_count);
  EXPECT_EQ(1, pick.max_per_domain);
  EXPECT_EQ((std::vector<int32_t>{1, 1, 1}), Profile(pick.stores));
  // Best-weighted store of each chosen host, in weight order.
  EXPECT_EQ((std::vector<int64_t>{1002, 1007, 1012}), Ids(pick.stores));
}

TEST_F(FailureDomainPlacementTest, PickStacksOnlyAfterEveryHostHoldsOne) {
  // Three hosts, four replicas: [2,1,1], never [2,2].
  std::vector<dingodb::pb::common::Store> candidates = {
      MakeStore(1001, kHost25), MakeStore(1002, kHost25), MakeStore(1006, kHost26),
      MakeStore(1007, kHost26), MakeStore(1011, kHost27),
  };

  auto pick = dingodb::CoordinatorControl::PickStoresAcrossFailureDomains(candidates, 4);

  EXPECT_EQ(3, pick.domain_count);
  EXPECT_EQ(2, pick.max_per_domain);
  EXPECT_EQ((std::vector<int32_t>{2, 1, 1}), Profile(pick.stores));
  EXPECT_EQ((std::vector<int64_t>{1001, 1006, 1011, 1002}), Ids(pick.stores));
}

TEST_F(FailureDomainPlacementTest, PickDegradesInsteadOfFailing) {
  // Two hosts, three replicas: 2+1 is the best possible and must not be refused.
  std::vector<dingodb::pb::common::Store> two_hosts = {
      MakeStore(1001, kHost25), MakeStore(1002, kHost25), MakeStore(1006, kHost26),
  };
  auto pick = dingodb::CoordinatorControl::PickStoresAcrossFailureDomains(two_hosts, 3);
  EXPECT_EQ(3u, pick.stores.size());
  EXPECT_EQ((std::vector<int32_t>{2, 1}), Profile(pick.stores));

  // One host: plain top-N, identical to the legacy behaviour.
  std::vector<dingodb::pb::common::Store> one_host = {
      MakeStore(1001, kHost25), MakeStore(1002, kHost25), MakeStore(1003, kHost25), MakeStore(1004, kHost25),
  };
  pick = dingodb::CoordinatorControl::PickStoresAcrossFailureDomains(one_host, 3);
  EXPECT_EQ((std::vector<int64_t>{1001, 1002, 1003}), Ids(pick.stores));
  EXPECT_EQ(3, pick.max_per_domain);
}

TEST_F(FailureDomainPlacementTest, PickExhaustsSmallHostsThenFillsFromLargeOne) {
  // {A:4, B:1, C:1}, five replicas: passes stop at cap 3 with [3,1,1].
  std::vector<dingodb::pb::common::Store> candidates = {
      MakeStore(1001, kHost25), MakeStore(1002, kHost25), MakeStore(1003, kHost25),
      MakeStore(1004, kHost25), MakeStore(1006, kHost26), MakeStore(1011, kHost27),
  };

  auto pick = dingodb::CoordinatorControl::PickStoresAcrossFailureDomains(candidates, 5);

  EXPECT_EQ(5u, pick.stores.size());
  EXPECT_EQ(3, pick.max_per_domain);
  EXPECT_EQ((std::vector<int32_t>{3, 1, 1}), Profile(pick.stores));
}

TEST_F(FailureDomainPlacementTest, PickNeverReturnsMoreThanCandidates) {
  std::vector<dingodb::pb::common::Store> candidates = {MakeStore(1001, kHost25), MakeStore(1006, kHost26)};
  auto pick = dingodb::CoordinatorControl::PickStoresAcrossFailureDomains(candidates, 3);
  EXPECT_EQ(2u, pick.stores.size());
}

// === CheckFailureDomainQuota (add-only ChangePeer) ===

TEST_F(FailureDomainPlacementTest, QuotaRejectsStackingWhenAFreeHostExists) {
  // (1002,1005,1007) = [2,1] on a four-host cluster; adding 1003 on .25 would make [3,1].
  auto status = dingodb::CoordinatorControl::CheckFailureDomainQuota(80001, {kHost25, kHost25, kHost26},
                                                                     {kHost25, kHost25, kHost26, kHost25}, 4);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(dingodb::pb::error::Errno::ECHANGE_PEER_FAILURE_DOMAIN_WORSE, status.error_code());
}

TEST_F(FailureDomainPlacementTest, QuotaAcceptsAddingOnAFreshHost) {
  // [2,1] -> [2,1,1]: the max does not grow even though 2 already exceeds the quota of 1.
  auto status = dingodb::CoordinatorControl::CheckFailureDomainQuota(80001, {kHost25, kHost25, kHost26},
                                                                     {kHost25, kHost25, kHost26, kHost28}, 4);
  EXPECT_TRUE(status.ok()) << status.error_str();
}

TEST_F(FailureDomainPlacementTest, QuotaAcceptsUnavoidableStackingOnSmallCluster) {
  // Two hosts, growing 2 -> 3 replicas: [1,1] -> [2,1] is the best a two-host cluster can do.
  auto status =
      dingodb::CoordinatorControl::CheckFailureDomainQuota(80001, {kHost25, kHost26}, {kHost25, kHost26, kHost25}, 2);
  EXPECT_TRUE(status.ok()) << status.error_str();

  // Same change on a four-host cluster is a regression.
  status =
      dingodb::CoordinatorControl::CheckFailureDomainQuota(80001, {kHost25, kHost26}, {kHost25, kHost26, kHost25}, 4);
  EXPECT_FALSE(status.ok());
}

// === balance region move check ===

namespace {

dingodb::pb::common::RegionDefinition MakeDefinition(const std::vector<std::pair<int64_t, std::string>>& peers) {
  dingodb::pb::common::RegionDefinition definition;
  definition.set_id(80001);
  for (const auto& [store_id, host] : peers) {
    *definition.add_peers() = MakePeer(store_id, host);
  }
  return definition;
}

}  // namespace

TEST_F(FailureDomainPlacementTest, BalanceMoveRejectsPackingOntoAPeerHost) {
  // Production shape (1002,1005,1007): .25 twice, .26 once.
  auto definition = MakeDefinition({{1002, kHost25}, {1005, kHost25}, {1007, kHost26}});
  using dingodb::balanceregion::BalanceRegionScheduler;

  // Moving the .26 replica onto .25 would put all three on one machine.
  EXPECT_FALSE(BalanceRegionScheduler::IsFailureDomainNoWorse(definition, 1007, kHost25));
  // Moving a .25 replica out to a free host is the improvement we want.
  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(definition, 1005, kHost28));
  // Moving a .25 replica onto .26 is also fine: [2,1] -> [1,2] is the same profile.
  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(definition, 1005, kHost26));
}

TEST_F(FailureDomainPlacementTest, BalanceMoveAllowsNeutralSameHostMove) {
  // This is the case a source-blind filter gets wrong. On a cluster whose host count equals the
  // replica count, a same-host move is the only move left; refusing it makes balance region a
  // no-op for every already-spread region.
  auto spread = MakeDefinition({{1002, kHost25}, {1007, kHost26}, {1012, kHost27}});
  using dingodb::balanceregion::BalanceRegionScheduler;

  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(spread, 1002, kHost25))
      << "1002 -> another store on .25 leaves the profile at [1,1,1]";
  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(spread, 1012, kHost27));
  // But collapsing onto a host that already holds a peer is still refused.
  EXPECT_FALSE(BalanceRegionScheduler::IsFailureDomainNoWorse(spread, 1002, kHost26));
}

TEST_F(FailureDomainPlacementTest, BalanceMoveKeepsLegacyPackedRegionsMovable) {
  // Every replica on one host: any move is neutral or better, so nothing may be refused.
  auto packed = MakeDefinition({{1001, kHost25}, {1002, kHost25}, {1003, kHost25}});
  using dingodb::balanceregion::BalanceRegionScheduler;

  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(packed, 1001, kHost25));
  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(packed, 1001, kHost28));
}

TEST_F(FailureDomainPlacementTest, BalanceMoveHandlesEmptyDefinition) {
  dingodb::pb::common::RegionDefinition empty;
  EXPECT_TRUE(dingodb::balanceregion::BalanceRegionScheduler::IsFailureDomainNoWorse(empty, 1001, kHost25));
}

TEST_F(FailureDomainPlacementTest, BalanceMoveStaysGuardedWhileCommitGuardIsOn) {
  // The pre-check exists so balance region never proposes a move the commit-side guard will
  // reject; otherwise the same rejected task is regenerated every round. Turning off only the
  // placement flag therefore must not disable it while the guard is still on.
  using dingodb::balanceregion::BalanceRegionScheduler;
  auto spread = MakeDefinition({{1002, kHost25}, {1007, kHost26}, {1012, kHost27}});

  const bool saved_guard = dingodb::FLAGS_enable_failure_domain_guard;
  const bool saved_placement = dingodb::FLAGS_enable_failure_domain_placement;

  dingodb::FLAGS_enable_failure_domain_placement = false;
  dingodb::FLAGS_enable_failure_domain_guard = true;
  EXPECT_FALSE(BalanceRegionScheduler::IsFailureDomainNoWorse(spread, 1002, kHost26))
      << "placement off, guard on: [1,1,1] -> [2,1] would be rejected at commit, so refuse it here";
  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(spread, 1002, kHost25))
      << "a neutral same-host move is still fine";

  // The other way round is guarded too: placement on keeps balance region spread-aware even
  // when the commit-side guard is off.
  dingodb::FLAGS_enable_failure_domain_placement = true;
  dingodb::FLAGS_enable_failure_domain_guard = false;
  EXPECT_FALSE(BalanceRegionScheduler::IsFailureDomainNoWorse(spread, 1002, kHost26))
      << "placement on, guard off: still refuses to pack replicas";

  // Only when both are off does balance region fall back to the legacy, domain-blind behaviour.
  dingodb::FLAGS_enable_failure_domain_placement = false;
  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(spread, 1002, kHost26));

  dingodb::FLAGS_enable_failure_domain_guard = saved_guard;
  dingodb::FLAGS_enable_failure_domain_placement = saved_placement;
}

TEST_F(FailureDomainPlacementTest, BalanceMoveResolvesPeerHostsFromStoreMap) {
  // The peer locations cached in a region definition are written once (create / add peer) and
  // never refreshed. The commit-side guard reads the store map first, so the pre-check must too,
  // or a store that re-registered on another host makes the two disagree and balance region spins.
  using dingodb::balanceregion::BalanceRegionScheduler;
  auto definition = MakeDefinition({{1002, kHost25}, {1007, kHost26}, {1012, kHost27}});

  // 1012 has since moved to .28 according to the store map.
  BalanceRegionScheduler::StoreDomainMap domain_by_store = {{1002, kHost25}, {1007, kHost26}, {1012, kHost28}};

  // Moving 1002 onto .28 now lands next to 1012: [1,1,1] -> [2,1], refuse. The cached-only view
  // would wrongly accept it because it still believes 1012 is on .27.
  EXPECT_FALSE(BalanceRegionScheduler::IsFailureDomainNoWorse(definition, 1002, kHost28, domain_by_store));
  // Moving 1002 onto .27 is now fine: nobody is there any more.
  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(definition, 1002, kHost27, domain_by_store));

  // A peer whose store is missing from the map falls back to the cached location.
  BalanceRegionScheduler::StoreDomainMap partial = {{1002, kHost25}};
  EXPECT_FALSE(BalanceRegionScheduler::IsFailureDomainNoWorse(definition, 1002, kHost27, partial));
  EXPECT_TRUE(BalanceRegionScheduler::IsFailureDomainNoWorse(definition, 1002, kHost28, partial));
}

// === balance leader failure-domain quota ===

TEST_F(FailureDomainPlacementTest, LeaderQuotaFollowsEvenShareWithTolerance) {
  // 2000 leaders over four hosts: share 500, quota ceil(500 * 1.1) = 550.
  std::map<std::string, int64_t> leaders = {{kHost25, 1344}, {kHost26, 604}, {kHost27, 51}, {kHost28, 1}};

  EXPECT_TRUE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost25, 0.1));
  EXPECT_TRUE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost26, 0.1));
  EXPECT_FALSE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost27, 0.1));
  EXPECT_FALSE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost28, 0.1));

  // Exactly at the boundary: 549 + 1 == 550 is still allowed, 550 + 1 is not.
  leaders = {{kHost25, 549}, {kHost26, 549}, {kHost27, 451}, {kHost28, 451}};
  EXPECT_FALSE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost25, 0.1));
  leaders[kHost25] = 550;
  leaders[kHost27] = 450;  // move one leader so the total stays 2000 and the quota stays 550
  EXPECT_TRUE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost25, 0.1));

  // Tolerance 0 is a strict even share.
  leaders = {{kHost25, 500}, {kHost26, 500}, {kHost27, 500}, {kHost28, 500}};
  EXPECT_TRUE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost25, 0.0));
  EXPECT_FALSE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost25, 0.1));

  // The quota must be exact integer arithmetic: in doubles 50 * 1.1 is 55.000000000000007, whose
  // ceil is 56, and a domain holding 55 of 100 leaders would wrongly look like it has room.
  leaders = {{kHost25, 55}, {kHost26, 45}};
  EXPECT_TRUE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost25, 0.1));
  leaders = {{kHost25, 54}, {kHost26, 46}};
  EXPECT_FALSE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost25, 0.1));
}

TEST_F(FailureDomainPlacementTest, LeaderQuotaNeverBlocksAnEmptyCluster) {
  std::map<std::string, int64_t> none;
  EXPECT_FALSE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(none, kHost25, 0.1));

  // A domain unknown to the map counts as zero leaders.
  std::map<std::string, int64_t> leaders = {{kHost25, 10}, {kHost26, 10}};
  EXPECT_FALSE(dingodb::balance::BalanceLeaderScheduler::IsFailureDomainLeaderFull(leaders, kHost27, 0.1));
}

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
#include <unordered_set>
#include <vector>

#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/balance_leader.h"
#include "proto/common.pb.h"

class CandidateStoresTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

class BalanceLeaderSchedulerTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

using RegionInternal = dingodb::pb::coordinator_internal::RegionInternal;

class MockCoordinatorControl : public dingodb::CoordinatorControl {
 public:
  MockCoordinatorControl(std::map<int64_t, RegionInternal> region_internal_map)
      : dingodb::CoordinatorControl(nullptr, nullptr, nullptr), region_internal_map_(region_internal_map){};
  ~MockCoordinatorControl() override = default;

  RegionInternal GetRegion(int64_t region_id) override {
    auto it = region_internal_map_.find(region_id);
    return (it != region_internal_map_.end()) ? it->second : RegionInternal{};
  }

 private:
  std::map<int64_t, RegionInternal> region_internal_map_;
};

// for generate region
struct RegionDistribution {
  int64_t region_id;
  std::vector<int64_t> store_ids;  // first item is leader, other item is follower
};

int64_t GetNextId(bool is_reset = false) {
  static int64_t id = 0;

  if (is_reset) {
    id = 0;
  }

  return ++id;
}

dingodb::pb::common::Store GenerateStore(int64_t store_id) {
  dingodb::pb::common::Store store;
  store.set_id(store_id);
  store.set_state(dingodb::pb::common::StoreState::STORE_NORMAL);
  store.set_leader_num_weight(1);

  return store;
}

dingodb::pb::common::StoreMap GenerateStoreMap(std::vector<int64_t> store_ids) {
  dingodb::pb::common::StoreMap store_map;
  for (auto store_id : store_ids) {
    *store_map.add_stores() = GenerateStore(store_id);
  }

  return store_map;
}

dingodb::pb::common::StoreMap GenerateStoreMap(std::vector<RegionDistribution> region_distributions) {
  std::set<int64_t> unique_store_ids;
  for (auto& region_distribution : region_distributions) {
    unique_store_ids.insert(region_distribution.store_ids.begin(), region_distribution.store_ids.end());
  }

  std::vector<int64_t> store_ids(unique_store_ids.begin(), unique_store_ids.end());

  return GenerateStoreMap(store_ids);
}

dingodb::pb::common::Region GenerateRegion(int64_t region_id, std::vector<int64_t> store_ids) {
  dingodb::pb::common::Region region;
  region.set_id(region_id);
  region.set_region_type(dingodb::pb::common::RegionType::STORE_REGION);
  region.set_state(dingodb::pb::common::RegionState::REGION_NORMAL);
  region.set_leader_store_id(store_ids[0]);

  dingodb::pb::common::RegionDefinition definition;
  definition.set_id(region.id());
  definition.set_raw_engine(dingodb::pb::common::RAW_ENG_ROCKSDB);
  definition.set_store_engine(dingodb::pb::common::STORE_ENG_RAFT_STORE);

  for (auto store_id : store_ids) {
    auto* peer = definition.add_peers();
    peer->set_store_id(store_id);
    peer->set_role(dingodb::pb::common::PeerRole::VOTER);
  }

  *region.mutable_definition() = definition;

  return region;
}

dingodb::pb::common::RegionMap GenerateRegionMap(std::vector<RegionDistribution> region_distributions) {
  dingodb::pb::common::RegionMap region_map;

  for (auto& region_distribution : region_distributions) {
    *region_map.add_regions() = GenerateRegion(region_distribution.region_id, region_distribution.store_ids);
  }

  return region_map;
}

RegionInternal GenerateRegionInternal(int64_t region_id, std::vector<int64_t> store_ids) {
  RegionInternal region;

  region.set_id(region_id);
  region.set_epoch(1);
  region.set_region_type(dingodb::pb::common::STORE_REGION);
  region.set_state(dingodb::pb::common::RegionState::REGION_NORMAL);

  dingodb::pb::common::RegionDefinition definition;
  definition.set_id(region.id());
  definition.set_raw_engine(dingodb::pb::common::RAW_ENG_ROCKSDB);
  definition.set_store_engine(dingodb::pb::common::STORE_ENG_RAFT_STORE);

  for (auto store_id : store_ids) {
    auto* peer = definition.add_peers();
    peer->set_store_id(store_id);
    peer->set_role(dingodb::pb::common::PeerRole::VOTER);
  }

  *region.mutable_definition() = definition;

  return region;
}

std::map<int64_t, RegionInternal> GenerateRegionInternalMap(std::vector<RegionDistribution> region_distributions) {
  std::map<int64_t, RegionInternal> region_internal_map;
  for (auto& region_distribution : region_distributions) {
    region_internal_map[region_distribution.region_id] =
        GenerateRegionInternal(region_distribution.region_id, region_distribution.store_ids);
  }

  return region_internal_map;
}

std::vector<dingodb::balance::StoreEntryPtr> GenerateStoreEntries(int store_num) {
  std::vector<dingodb::balance::StoreEntryPtr> stores;

  for (int i = 0; i < store_num; ++i) {
    std::vector<int64_t> leader_region_ids;
    std::vector<int64_t> follower_region_ids;

    stores.push_back(
        dingodb::balance::StoreEntry::New(GenerateStore(1000 + GetNextId()), leader_region_ids, follower_region_ids));
  }

  return stores;
}

void DistributeRandomRegionToStore(int region_num, int replicate_num,
                                   std::vector<dingodb::balance::StoreEntryPtr>& store_entries) {
  uint32_t store_size = store_entries.size();

  for (int i = 0; i < region_num; ++i) {
    int64_t region_id = 60000 + GetNextId(i == 0);

    std::unordered_set<int> store_offsetes;
    for (;;) {
      int offset = dingodb::Helper::GenerateRealRandomInteger(0, store_size - 1);
      if (store_offsetes.find(offset) == store_offsetes.end()) {
        store_offsetes.insert(offset);
      }
      if (store_offsetes.size() == replicate_num) {
        break;
      }
    }

    bool is_used_leader = false;
    for (auto offset : store_offsetes) {
      auto store_entry = store_entries[offset];
      if (!is_used_leader) {
        is_used_leader = true;
        store_entry->TestAddLeader(region_id);
      } else {
        store_entry->TestAddFollower(region_id);
      }
    }
  }
}

// region | store-0 | store-1 | store-2
// 60001  | L       | F       | F
// 60002  | L       | F       | F
// 60003  | L       | F       | F
// 60004  | L       | F       | F
// 60005  | L       | F       | F
// 60006  | L       | F       | F
void DistributeRegionToStore(std::vector<dingodb::balance::StoreEntryPtr>& store_entries) {
  {
    int64_t region_id = 60001;
    store_entries[0]->TestAddLeader(region_id);
    store_entries[1]->TestAddFollower(region_id);
    store_entries[2]->TestAddFollower(region_id);
  }

  {
    int64_t region_id = 60002;
    store_entries[0]->TestAddLeader(region_id);
    store_entries[1]->TestAddFollower(region_id);
    store_entries[2]->TestAddFollower(region_id);
  }

  {
    int64_t region_id = 60003;
    store_entries[0]->TestAddLeader(region_id);
    store_entries[1]->TestAddFollower(region_id);
    store_entries[2]->TestAddFollower(region_id);
  }

  {
    int64_t region_id = 60004;
    store_entries[0]->TestAddLeader(region_id);
    store_entries[1]->TestAddFollower(region_id);
    store_entries[2]->TestAddFollower(region_id);
  }

  {
    int64_t region_id = 60005;
    store_entries[0]->TestAddLeader(region_id);
    store_entries[1]->TestAddFollower(region_id);
    store_entries[2]->TestAddFollower(region_id);
  }

  {
    int64_t region_id = 60006;
    store_entries[0]->TestAddLeader(region_id);
    store_entries[1]->TestAddFollower(region_id);
    store_entries[2]->TestAddFollower(region_id);
  }
}

TEST_F(CandidateStoresTest, Build1) {
  GTEST_SKIP() << "skip...";
  std::vector<dingodb::balance::StoreEntryPtr> stores = GenerateStoreEntries(3);
  // DistributeRegionToStore(stores);
  DistributeRandomRegionToStore(10, 3, stores);

  auto source_candidate_stores = dingodb::balance::CandidateStores::New(stores, true);
  std::cout << "leader score: " << source_candidate_stores->ToString() << std::endl;
}

TEST_F(BalanceLeaderSchedulerTest, Schedule) {
  GTEST_SKIP() << "skip...";
  {
    // region | store-1 | stcore-2 | store-3
    // 60001  | L       | F       | F
    // 60002  | L       | F       | F
    // 60003  | L       | F       | F
    // 60004  | L       | F       | F
    // 60005  | L       | F       | F
    // 60006  | L       | F       | F
    std::vector<RegionDistribution> region_distributions = {
        {60001, {1001, 1002, 1003}}, {60002, {1001, 1002, 1003}}, {60003, {1001, 1002, 1003}},
        {60004, {1001, 1002, 1003}}, {60005, {1001, 1002, 1003}}, {60006, {1001, 1002, 1003}},
    };

    dingodb::pb::common::StoreMap store_map = GenerateStoreMap(region_distributions);
    dingodb::pb::common::RegionMap region_map = GenerateRegionMap(region_distributions);
    std::map<int64_t, RegionInternal> region_internal_map = GenerateRegionInternalMap(region_distributions);

    std::shared_ptr<dingodb::CoordinatorControl> coordinator_control =
        std::make_shared<MockCoordinatorControl>(region_internal_map);

    std::vector<dingodb::balance::FilterPtr> store_filters;
    std::vector<dingodb::balance::FilterPtr> region_filters;
    std::vector<dingodb::balance::FilterPtr> task_filters;
    std::vector<dingodb::balance::FilterPtr> resource_filters;
    auto balance_leader_scheduler = dingodb::balance::BalanceLeaderScheduler::New(
        coordinator_control, nullptr, store_filters, region_filters, task_filters, resource_filters, nullptr);

    balance_leader_scheduler->Schedule(region_map, store_map);

    std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  {
    // region | store-1 | store-2 | store-3 | store-4 | store-5
    // 60001  | L       | F       | F       | F       |
    // 60002  | L       | F       | F       | F       |
    // 60003  | L       | F       | F       | F       |
    // 60004  | L       | F       | F       |         | F
    // 60005  | L       | F       | F       |         | F
    // 60006  | L       | F       | F       |         | F
    // 60007  | L       | F       | F       | F       |
    // 60008  | L       | F       | F       |         | F
    // 60009  | L       | F       | F       |         | F
    // 60010  | L       | F       | F       |         | F
    // 60011  | L       | F       | F       | F       |
    // 60012  | L       | F       | F       | F       |
    // 60013  | L       | F       | F       | F       |
    std::vector<RegionDistribution> region_distributions = {
        {60001, {1001, 1002, 1003, 1004}},  {60002, {1001, 1002, 1003, 1004}}, {60003, {1001, 1002, 1003, 1004}},
        {60004, {1001, 1002, 1003, 1005}},  {60005, {1001, 1002, 1003, 1005}}, {60006, {1001, 1002, 1003, 1005}},
        {60007, {1001, 1002, 1003, 1004}},  {60008, {1001, 1002, 1003, 1005}}, {60009, {1001, 1002, 1003, 1005}},
        {60010, {1001, 1002, 1003, 1005}},  {60011, {1001, 1002, 1003, 1004}}, {600012, {1001, 1002, 1003, 1004}},
        {600013, {1001, 1002, 1003, 1004}},
    };

    dingodb::pb::common::StoreMap store_map = GenerateStoreMap(region_distributions);
    dingodb::pb::common::RegionMap region_map = GenerateRegionMap(region_distributions);
    std::map<int64_t, RegionInternal> region_internal_map = GenerateRegionInternalMap(region_distributions);

    std::shared_ptr<dingodb::CoordinatorControl> coordinator_control =
        std::make_shared<MockCoordinatorControl>(region_internal_map);

    std::vector<dingodb::balance::FilterPtr> store_filters;
    std::vector<dingodb::balance::FilterPtr> region_filters;
    std::vector<dingodb::balance::FilterPtr> task_filters;
    std::vector<dingodb::balance::FilterPtr> resource_filters;
    auto balance_leader_scheduler = dingodb::balance::BalanceLeaderScheduler::New(
        coordinator_control, nullptr, store_filters, region_filters, task_filters, resource_filters, nullptr);

    balance_leader_scheduler->Schedule(region_map, store_map);

    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

TEST_F(BalanceLeaderSchedulerTest, ParseTimePeriod) {
  {
    auto time_periods = dingodb::balance::BalanceLeaderScheduler::TestParseTimePeriod("2,3");
    ASSERT_EQ(1, time_periods.size());
    ASSERT_EQ(2, time_periods[0].first);
    ASSERT_EQ(3, time_periods[0].second);
  }

  {
    auto time_periods = dingodb::balance::BalanceLeaderScheduler::TestParseTimePeriod("2,");
    ASSERT_EQ(1, time_periods.size());
    ASSERT_EQ(2, time_periods[0].first);
    ASSERT_EQ(23, time_periods[0].second);
  }

  {
    auto time_periods = dingodb::balance::BalanceLeaderScheduler::TestParseTimePeriod("2,3;4,7");
    ASSERT_EQ(2, time_periods.size());
    ASSERT_EQ(2, time_periods[0].first);
    ASSERT_EQ(3, time_periods[0].second);
    ASSERT_EQ(4, time_periods[1].first);
    ASSERT_EQ(7, time_periods[1].second);
  }

  {
    auto time_periods = dingodb::balance::BalanceLeaderScheduler::TestParseTimePeriod("2,3;4,18");
    ASSERT_EQ(2, time_periods.size());
    ASSERT_EQ(2, time_periods[0].first);
    ASSERT_EQ(3, time_periods[0].second);
    ASSERT_EQ(4, time_periods[1].first);
    ASSERT_EQ(18, time_periods[1].second);
  }

  {
    auto time_periods = dingodb::balance::BalanceLeaderScheduler::TestParseTimePeriod("2,;4,7");
    ASSERT_EQ(2, time_periods.size());
    ASSERT_EQ(2, time_periods[0].first);
    ASSERT_EQ(23, time_periods[0].second);
    ASSERT_EQ(4, time_periods[1].first);
    ASSERT_EQ(7, time_periods[1].second);
  }

  {
    auto time_periods = dingodb::balance::BalanceLeaderScheduler::TestParseTimePeriod("");
    ASSERT_EQ(0, time_periods.size());
  }

  {
    auto time_periods = dingodb::balance::BalanceLeaderScheduler::TestParseTimePeriod("a,");
    ASSERT_EQ(0, time_periods.size());
  }

  {
    auto time_periods = dingodb::balance::BalanceLeaderScheduler::TestParseTimePeriod("2,a");
    ASSERT_EQ(0, time_periods.size());
  }

  {
    auto time_periods = dingodb::balance::BalanceLeaderScheduler::TestParseTimePeriod(" 2 , 4 ");
    ASSERT_EQ(1, time_periods.size());
    ASSERT_EQ(2, time_periods[0].first);
    ASSERT_EQ(4, time_periods[0].second);
  }
}
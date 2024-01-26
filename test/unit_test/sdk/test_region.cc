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

#include "gtest/gtest.h"
#include "sdk/region.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

class RegionTest : public testing::Test {
 protected:
  void SetUp() override { InitRegion(); }

  void TearDown() override {}

  std::shared_ptr<Region> region;

 private:
  void InitRegion() {
    pb::common::Range range;
    range.set_start_key("a");
    range.set_end_key("b");

    pb::common::RegionEpoch epoch;
    epoch.set_version(1);
    epoch.set_conf_version(1);

    pb::common::RegionType type = pb::common::STORE_REGION;

    std::vector<Replica> replicas;
    replicas.reserve(kInitReplica.size());
    for (const auto& entry : kInitReplica) {
      butil::EndPoint end_point;
      butil::str2endpoint(entry.first.c_str(), &end_point);
      replicas.push_back({end_point, entry.second});
    }

    region.reset(new Region(1, range, epoch, type, replicas));
  }
};

TEST_F(RegionTest, TestInit) {
  EXPECT_EQ(region->RegionId(), 1);
  EXPECT_EQ(region->Range().start_key(), "a");
  EXPECT_EQ(region->Range().end_key(), "b");
  EXPECT_EQ(region->Epoch().version(), 1);
  EXPECT_EQ(region->Epoch().conf_version(), 1);
  EXPECT_EQ(region->RegionType(), pb::common::STORE_REGION);

  auto end_points = region->ReplicaEndPoint();

  for (const auto& end : end_points) {
    EXPECT_TRUE(kInitReplica.find(Helper::EndPointToStr(end)) != kInitReplica.end());
  }

  butil::EndPoint leader;
  Status got = region->GetLeader(leader);
  EXPECT_TRUE(got.IsOK());
  EXPECT_EQ(Helper::EndPointToStr(leader), kAddrOne);
  EXPECT_TRUE(region->IsStale());
}

TEST_F(RegionTest, TestMark) {
  butil::EndPoint end;
  butil::str2endpoint(kAddrOne.c_str(), &end);
  region->MarkFollower(end);
  butil::EndPoint leader;
  Status got = region->GetLeader(leader);
  EXPECT_TRUE(got.IsNotFound());

  butil::str2endpoint(kAddrTwo.c_str(), &end);
  region->MarkLeader(end);
  got = region->GetLeader(leader);
  EXPECT_TRUE(got.IsOK());
  EXPECT_EQ(Helper::EndPointToStr(leader), kAddrTwo);

  {
    // test mark and unmark stale
    EXPECT_TRUE(region->IsStale());
    region->TEST_UnMarkStale();
    EXPECT_FALSE(region->IsStale());

    region->TEST_MarkStale();
    EXPECT_TRUE(region->IsStale());
  }
}

}  // namespace sdk
}  // namespace dingodb
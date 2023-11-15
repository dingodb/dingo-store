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

#include "mock_meta_cache.h"

#include <memory>
#include <string>
#include <vector>

#include "butil/endpoint.h"
#include "butil/status.h"
#include "common/helper.h"
#include "coordinator_interaction.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "meta_cache.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "status.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

using ::testing::_;

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

class MetaCacheTest : public testing::Test {
 protected:
  void SetUp() override {
    auto coordinator_interaction = std::make_shared<CoordinatorInteraction>();
    meta_cache = std::make_shared<MockMetaCache>(coordinator_interaction);
  }

  void TearDown() override { meta_cache.reset(); }

  std::shared_ptr<MockMetaCache> meta_cache;  // NOLINT
};

TEST_F(MetaCacheTest, LookupRegionByKey) {
  auto region = RegionA2C();

  EXPECT_CALL(*meta_cache, SendScanRegionsRequest(_, _))
      .WillOnce(testing::Invoke(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "b");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          }));

  std::shared_ptr<Region> tmp;
  Status got = meta_cache->LookupRegionByKey("b", tmp);
  EXPECT_TRUE(got.IsOK());

  EXPECT_EQ(tmp->RegionId(), region->RegionId());
  EXPECT_EQ(tmp->Range().start_key(), region->Range().start_key());
  EXPECT_EQ(tmp->Range().end_key(), region->Range().end_key());
}

TEST_F(MetaCacheTest, ClearRange) {
  auto region = RegionA2C();

  EXPECT_CALL(*meta_cache, SendScanRegionsRequest(_, _))
      .WillOnce(testing::Invoke(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "b");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          }));

  {
    // clear exist
    std::shared_ptr<Region> tmp;
    Status got = meta_cache->LookupRegionByKey("b", tmp);
    EXPECT_TRUE(got.IsOK());

    meta_cache->ClearRange(tmp);
    EXPECT_TRUE(tmp->IsStale());

    got = meta_cache->TEST_FastLookUpRegionByKey("b", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  {
    // clear not exist
    std::shared_ptr<Region> tmp = RegionC2E();
    Status got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsNotFound());
    meta_cache->ClearRange(tmp);
  }
}

TEST_F(MetaCacheTest, AddRegion) {
  auto region = RegionA2C();

  EXPECT_CALL(*meta_cache, SendScanRegionsRequest(_, _))
      .WillOnce(testing::Invoke(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "b");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          }));

  std::shared_ptr<Region> tmp;
  Status got = meta_cache->LookupRegionByKey("b", tmp);
  EXPECT_TRUE(got.IsOK());

  {
    // add c2e
    got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsNotFound());

    region = RegionC2E();
    meta_cache->MaybeAddRegion(region);

    got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), region->RegionId());
  }

  {
    // add e2g
    got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsNotFound());

    region = RegionE2G();
    meta_cache->MaybeAddRegion(region);

    got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), region->RegionId());
  }
}

TEST_F(MetaCacheTest, AddInterleaveRegionFromSmall2Large) {
  std::shared_ptr<Region> a2c;
  {
    Status got = meta_cache->TEST_FastLookUpRegionByKey("b", a2c);
    EXPECT_TRUE(got.IsNotFound());

    meta_cache->MaybeAddRegion(RegionA2C());

    got = meta_cache->TEST_FastLookUpRegionByKey("b", a2c);
    EXPECT_TRUE(got.IsOK());
    EXPECT_FALSE(a2c->IsStale());
  }

  std::shared_ptr<Region> c2e;
  {
    Status got = meta_cache->TEST_FastLookUpRegionByKey("c", c2e);
    EXPECT_TRUE(got.IsNotFound());

    meta_cache->MaybeAddRegion(RegionC2E());

    got = meta_cache->TEST_FastLookUpRegionByKey("c", c2e);
    EXPECT_TRUE(got.IsOK());
    EXPECT_FALSE(c2e->IsStale());
  }

  std::shared_ptr<Region> e2g;
  {
    Status got = meta_cache->TEST_FastLookUpRegionByKey("e", e2g);
    EXPECT_TRUE(got.IsNotFound());

    meta_cache->MaybeAddRegion(RegionE2G());

    got = meta_cache->TEST_FastLookUpRegionByKey("e", e2g);
    EXPECT_TRUE(got.IsOK());
    EXPECT_FALSE(e2g->IsStale());
  }

  std::shared_ptr<Region> b2f = RegionB2F();
  {
    meta_cache->MaybeAddRegion(b2f);
    EXPECT_TRUE(a2c->IsStale());
    EXPECT_TRUE(c2e->IsStale());
    EXPECT_TRUE(e2g->IsStale());

    std::shared_ptr<Region> tmp;
    Status got = meta_cache->TEST_FastLookUpRegionByKey("b", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());
  }

  meta_cache->Dump();
}

TEST_F(MetaCacheTest, AddInterleaveRegionFromLarge2Small) {
  std::shared_ptr<Region> a2z = RegionA2Z();
  {
    std::shared_ptr<Region> tmp;
    Status got = meta_cache->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());

    meta_cache->MaybeAddRegion(a2z);

    got = meta_cache->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), a2z->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), a2z->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), a2z->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), a2z->RegionId());
  }

  std::shared_ptr<Region> b2f = RegionB2F();
  {
    meta_cache->MaybeAddRegion(b2f);
    EXPECT_TRUE(a2z->IsStale());

    std::shared_ptr<Region> tmp;
    Status got = meta_cache->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  std::shared_ptr<Region> a2c = RegionA2C();
  {
    meta_cache->MaybeAddRegion(a2c);
    EXPECT_TRUE(b2f->IsStale());

    std::shared_ptr<Region> tmp;
    Status got = meta_cache->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), a2c->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  std::shared_ptr<Region> c2e = RegionC2E();
  {
    meta_cache->MaybeAddRegion(c2e);

    std::shared_ptr<Region> tmp;
    Status got = meta_cache->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), a2c->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), c2e->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  std::shared_ptr<Region> e2g = RegionE2G();
  {
    meta_cache->MaybeAddRegion(e2g);

    std::shared_ptr<Region> tmp;
    Status got = meta_cache->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), a2c->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), c2e->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_EQ(tmp->RegionId(), e2g->RegionId());

    got = meta_cache->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  meta_cache->Dump();
}

TEST_F(MetaCacheTest, StaleRegion) {
  std::shared_ptr<Region> a2c;
  {
    Status got = meta_cache->TEST_FastLookUpRegionByKey("b", a2c);
    EXPECT_TRUE(got.IsNotFound());
    meta_cache->MaybeAddRegion(RegionA2C());

    got = meta_cache->TEST_FastLookUpRegionByKey("b", a2c);
    EXPECT_TRUE(got.IsOK());

    EXPECT_FALSE(a2c->IsStale());
  }

  std::shared_ptr<Region> a2c_version2;
  {
    a2c_version2 = RegionA2C(2, 1);
    meta_cache->MaybeAddRegion(a2c_version2);

    std::shared_ptr<Region> tmp;
    Status got = meta_cache->TEST_FastLookUpRegionByKey("b", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_FALSE(tmp->IsStale());
    EXPECT_EQ(tmp->Epoch().version(), a2c_version2->Epoch().version());
    EXPECT_EQ(tmp->Epoch().conf_version(), a2c_version2->Epoch().conf_version());

    EXPECT_TRUE(a2c->IsStale());
  }

  std::shared_ptr<Region> a2c_conf2;
  {
    a2c_conf2 = RegionA2C(2, 2);
    meta_cache->MaybeAddRegion(a2c_conf2);

    std::shared_ptr<Region> tmp;
    Status got = meta_cache->TEST_FastLookUpRegionByKey("b", tmp);
    EXPECT_TRUE(got.IsOK());
    EXPECT_FALSE(tmp->IsStale());
    EXPECT_EQ(tmp->Epoch().version(), a2c_conf2->Epoch().version());
    EXPECT_EQ(tmp->Epoch().conf_version(), a2c_conf2->Epoch().conf_version());

    EXPECT_TRUE(a2c->IsStale());
    EXPECT_TRUE(a2c_version2->IsStale());
  }
}

}  // namespace sdk
}  // namespace dingodb
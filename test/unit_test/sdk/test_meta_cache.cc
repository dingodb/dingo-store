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

#include <vector>

#include "gtest/gtest.h"
#include "mock_coordinator_proxy.h"
#include "sdk/coordinator_proxy.h"
#include "sdk/meta_cache.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

class MetaCacheTest : public testing::Test {
 protected:
  void SetUp() override {
    cooridnator_proxy = std::make_shared<MockCoordinatorProxy>();
    meta_cache = std::make_shared<MetaCache>(cooridnator_proxy);
  }

  void TearDown() override { meta_cache.reset(); }

  std::shared_ptr<MockCoordinatorProxy> cooridnator_proxy;
  std::shared_ptr<MetaCache> meta_cache;
};

TEST_F(MetaCacheTest, LookupRegionByKey) {
  auto region = RegionA2C();

  EXPECT_CALL(*cooridnator_proxy, ScanRegions)
      .WillOnce(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "b");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          });

  std::shared_ptr<Region> tmp;
  Status got = meta_cache->LookupRegionByKey("b", tmp);
  EXPECT_TRUE(got.IsOK());

  EXPECT_EQ(tmp->RegionId(), region->RegionId());
  EXPECT_EQ(tmp->Range().start_key(), region->Range().start_key());
  EXPECT_EQ(tmp->Range().end_key(), region->Range().end_key());
}

TEST_F(MetaCacheTest, ClearRange) {
  auto region = RegionA2C();

  EXPECT_CALL(*cooridnator_proxy, ScanRegions)
      .WillOnce(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "b");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          });

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

  EXPECT_CALL(*cooridnator_proxy, ScanRegions)
      .WillOnce(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "b");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          });

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

TEST_F(MetaCacheTest, LookupRegionBetweenRangeNotFound) {
  EXPECT_CALL(*cooridnator_proxy, ScanRegions)
      .WillOnce(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "a");
            EXPECT_EQ(request.range_end(), "d");
            return Status::OK();
          });

  std::shared_ptr<Region> tmp;
  Status got = meta_cache->LookupRegionBetweenRange("a", "d", tmp);
  EXPECT_TRUE(got.IsNotFound());
}

TEST_F(MetaCacheTest, LookupRegionBetweenRangeFromRemote) {
  auto region = RegionA2C();
  EXPECT_CALL(*cooridnator_proxy, ScanRegions)
      .WillOnce(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "a");
            EXPECT_EQ(request.range_end(), "d");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          });

  std::shared_ptr<Region> tmp;
  Status got = meta_cache->LookupRegionBetweenRange("a", "d", tmp);
  EXPECT_TRUE(got.IsOK());

  EXPECT_EQ(tmp->RegionId(), region->RegionId());
  EXPECT_EQ(tmp->Range().start_key(), region->Range().start_key());
  EXPECT_EQ(tmp->Range().end_key(), region->Range().end_key());
}

TEST_F(MetaCacheTest, LookupRegionBetweenRangeFromCache) {
  auto region = RegionA2C();
  meta_cache->MaybeAddRegion(region);

  EXPECT_CALL(*cooridnator_proxy, ScanRegions).Times(0);

  std::shared_ptr<Region> tmp;
  Status got = meta_cache->LookupRegionBetweenRange("a", "d", tmp);
  EXPECT_TRUE(got.IsOK());

  EXPECT_EQ(tmp->RegionId(), region->RegionId());
  EXPECT_EQ(tmp->Range().start_key(), region->Range().start_key());
  EXPECT_EQ(tmp->Range().end_key(), region->Range().end_key());
}

TEST_F(MetaCacheTest, LookupRegionBetweenRangePrefetch) {
  auto a2c = RegionA2C();
  auto e2g = RegionE2G();

  EXPECT_CALL(*cooridnator_proxy, ScanRegions)
      .WillOnce(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "a");
            EXPECT_EQ(request.range_end(), "f");
            EXPECT_GT(request.limit(), 1);
            Region2ScanRegionInfo(a2c, response.add_regions());
            Region2ScanRegionInfo(e2g, response.add_regions());
            return Status::OK();
          });

  std::shared_ptr<Region> tmp;
  Status got = meta_cache->LookupRegionBetweenRange("a", "f", tmp);
  EXPECT_TRUE(got.IsOK());

  EXPECT_EQ(tmp->RegionId(), a2c->RegionId());
  EXPECT_EQ(tmp->Range().start_key(), a2c->Range().start_key());
  EXPECT_EQ(tmp->Range().end_key(), a2c->Range().end_key());

  got = meta_cache->TEST_FastLookUpRegionByKey("e", tmp);
  EXPECT_TRUE(got.IsOK());
  EXPECT_EQ(tmp->RegionId(), e2g->RegionId());
  EXPECT_EQ(tmp->Range().start_key(), e2g->Range().start_key());
  EXPECT_EQ(tmp->Range().end_key(), e2g->Range().end_key());
}

TEST_F(MetaCacheTest, LookupRegionBetweenRangeNoPreetch) {
  auto a2c = RegionA2C();

  EXPECT_CALL(*cooridnator_proxy, ScanRegions)
      .WillOnce(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "a");
            EXPECT_EQ(request.range_end(), "f");
            EXPECT_EQ(request.limit(), 1);
            Region2ScanRegionInfo(a2c, response.add_regions());
            return Status::OK();
          });

  std::shared_ptr<Region> tmp;
  Status got = meta_cache->LookupRegionBetweenRangeNoPrefetch("a", "f", tmp);
  EXPECT_TRUE(got.IsOK());

  EXPECT_EQ(tmp->RegionId(), a2c->RegionId());
  EXPECT_EQ(tmp->Range().start_key(), a2c->Range().start_key());
  EXPECT_EQ(tmp->Range().end_key(), a2c->Range().end_key());
}

TEST_F(MetaCacheTest, ScanRegionsBetweenContinuousRangeNoRegion) {
  {
    EXPECT_CALL(*cooridnator_proxy, ScanRegions)
        .WillOnce(
            [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
              (void)request;
              (void)response;
              return Status::OK();
            });

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("c", "g", regions);
    EXPECT_TRUE(got.IsNotFound());
    EXPECT_EQ(regions.size(), 0);
  }

  {
    auto region = RegionA2C();
    meta_cache->MaybeAddRegion(region);

    EXPECT_CALL(*cooridnator_proxy, ScanRegions)
        .WillOnce(
            [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
              (void)request;
              (void)response;
              return Status::OK();
            });

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("c", "g", regions);
    EXPECT_TRUE(got.IsNotFound());
    EXPECT_EQ(regions.size(), 0);

    meta_cache->ClearCache();
  }

  {
    auto region = RegionB2F();
    meta_cache->MaybeAddRegion(region);

    EXPECT_CALL(*cooridnator_proxy, ScanRegions)
        .WillOnce(
            [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
              (void)request;
              (void)response;
              return Status::OK();
            });

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("c", "g", regions);
    EXPECT_TRUE(got.IsNotFound());
    EXPECT_EQ(regions.size(), 0);

    meta_cache->ClearCache();
  }

  {
    auto region = RegionE2G();
    meta_cache->MaybeAddRegion(region);
    region = RegionL2N();
    meta_cache->MaybeAddRegion(region);

    EXPECT_CALL(*cooridnator_proxy, ScanRegions)
        .WillOnce(
            [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
              (void)request;
              (void)response;
              return Status::OK();
            });

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("c", "e", regions);
    EXPECT_TRUE(got.IsNotFound());
    EXPECT_EQ(regions.size(), 0);

    meta_cache->ClearCache();
  }

  {
    auto region = RegionL2N();
    meta_cache->MaybeAddRegion(region);

    EXPECT_CALL(*cooridnator_proxy, ScanRegions)
        .WillOnce(
            [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
              (void)request;
              (void)response;
              return Status::OK();
            });

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("c", "g", regions);
    EXPECT_TRUE(got.IsNotFound());
    EXPECT_EQ(regions.size(), 0);

    meta_cache->ClearCache();
  }

  {
    auto region = RegionA2C();
    meta_cache->MaybeAddRegion(region);
    region = RegionE2G();
    meta_cache->MaybeAddRegion(region);

    EXPECT_CALL(*cooridnator_proxy, ScanRegions)
        .WillOnce(
            [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
              (void)request;
              (void)response;
              return Status::OK();
            });

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("a", "g", regions);
    EXPECT_TRUE(got.IsNotFound());
    EXPECT_EQ(regions.size(), 0);

    meta_cache->ClearCache();
  }
}

TEST_F(MetaCacheTest, ScanRegionsBetweenContinuousRangeRegionFromCache) {
  {
    auto region = RegionA2C();
    meta_cache->MaybeAddRegion(region);

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("a", "c", regions);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(regions.size(), 1);

    auto target = regions[0];
    EXPECT_EQ(target->RegionId(), region->RegionId());
    EXPECT_EQ(target->Range().start_key(), region->Range().start_key());

    EXPECT_EQ(target->Range().end_key(), region->Range().end_key());
  }

  {
    auto a2c = RegionA2C();
    meta_cache->MaybeAddRegion(a2c);
    auto c2e = RegionC2E();
    meta_cache->MaybeAddRegion(c2e);

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("c", "e", regions);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(regions.size(), 1);

    auto target = regions[0];
    EXPECT_EQ(target->RegionId(), c2e->RegionId());
    EXPECT_EQ(target->Range().start_key(), c2e->Range().start_key());

    EXPECT_EQ(target->Range().end_key(), c2e->Range().end_key());
  }

  {
    auto a2c = RegionA2C();
    meta_cache->MaybeAddRegion(a2c);
    auto c2e = RegionC2E();
    meta_cache->MaybeAddRegion(c2e);
    auto e2g = RegionE2G();
    meta_cache->MaybeAddRegion(e2g);

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("c", "e", regions);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(regions.size(), 1);

    auto target = regions[0];
    EXPECT_EQ(target->RegionId(), c2e->RegionId());
    EXPECT_EQ(target->Range().start_key(), c2e->Range().start_key());
    EXPECT_EQ(target->Range().end_key(), c2e->Range().end_key());
  }
}

TEST_F(MetaCacheTest, ScanRegionsBetweenContinuousRangeMultipleRegionFromCache) {
  {
    auto a2c = RegionA2C();
    meta_cache->MaybeAddRegion(a2c);
    auto c2e = RegionC2E();
    meta_cache->MaybeAddRegion(c2e);

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("a", "e", regions);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(regions.size(), 2);

    EXPECT_EQ(regions[0]->RegionId(), a2c->RegionId());
    EXPECT_EQ(regions[0]->Range().start_key(), a2c->Range().start_key());
    EXPECT_EQ(regions[0]->Range().end_key(), a2c->Range().end_key());

    EXPECT_EQ(regions[1]->RegionId(), c2e->RegionId());
    EXPECT_EQ(regions[1]->Range().start_key(), c2e->Range().start_key());
    EXPECT_EQ(regions[1]->Range().end_key(), c2e->Range().end_key());
  }

  {
    auto a2c = RegionA2C();
    meta_cache->MaybeAddRegion(a2c);
    auto c2e = RegionC2E();
    meta_cache->MaybeAddRegion(c2e);
    auto e2g = RegionE2G();
    meta_cache->MaybeAddRegion(e2g);

    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("c", "g", regions);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(regions.size(), 2);

    EXPECT_EQ(regions[0]->RegionId(), c2e->RegionId());
    EXPECT_EQ(regions[0]->Range().start_key(), c2e->Range().start_key());
    EXPECT_EQ(regions[0]->Range().end_key(), c2e->Range().end_key());

    EXPECT_EQ(regions[1]->RegionId(), e2g->RegionId());
    EXPECT_EQ(regions[1]->Range().start_key(), e2g->Range().start_key());
    EXPECT_EQ(regions[1]->Range().end_key(), e2g->Range().end_key());
  }
}

TEST_F(MetaCacheTest, ScanRegionsBetweenContinuousRangeMultipleRegionFromRemote) {
  auto a2c = RegionA2C();
  auto c2e = RegionC2E();
  auto e2g = RegionE2G();

  EXPECT_CALL(*cooridnator_proxy, ScanRegions)
      .WillOnce(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "a");
            EXPECT_EQ(request.range_end(), "g");
            EXPECT_EQ(request.limit(), 0);
            Region2ScanRegionInfo(a2c, response.add_regions());
            Region2ScanRegionInfo(c2e, response.add_regions());
            Region2ScanRegionInfo(e2g, response.add_regions());
            return Status::OK();
          });
  {
    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("a", "g", regions);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(regions.size(), 3);

    EXPECT_EQ(regions[0]->RegionId(), a2c->RegionId());
    EXPECT_EQ(regions[0]->Range().start_key(), a2c->Range().start_key());
    EXPECT_EQ(regions[0]->Range().end_key(), a2c->Range().end_key());

    EXPECT_EQ(regions[1]->RegionId(), c2e->RegionId());
    EXPECT_EQ(regions[1]->Range().start_key(), c2e->Range().start_key());
    EXPECT_EQ(regions[1]->Range().end_key(), c2e->Range().end_key());

    EXPECT_EQ(regions[2]->RegionId(), e2g->RegionId());
    EXPECT_EQ(regions[2]->Range().start_key(), e2g->Range().start_key());
    EXPECT_EQ(regions[2]->Range().end_key(), e2g->Range().end_key());
  }

  {
    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("a", "e", regions);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(regions.size(), 2);

    EXPECT_EQ(regions[0]->RegionId(), a2c->RegionId());
    EXPECT_EQ(regions[0]->Range().start_key(), a2c->Range().start_key());
    EXPECT_EQ(regions[0]->Range().end_key(), a2c->Range().end_key());

    EXPECT_EQ(regions[1]->RegionId(), c2e->RegionId());
    EXPECT_EQ(regions[1]->Range().start_key(), c2e->Range().start_key());
    EXPECT_EQ(regions[1]->Range().end_key(), c2e->Range().end_key());
  }

  {
    std::vector<std::shared_ptr<Region>> regions;
    Status got = meta_cache->ScanRegionsBetweenContinuousRange("c", "g", regions);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(regions.size(), 2);

    EXPECT_EQ(regions[0]->RegionId(), c2e->RegionId());
    EXPECT_EQ(regions[0]->Range().start_key(), c2e->Range().start_key());
    EXPECT_EQ(regions[0]->Range().end_key(), c2e->Range().end_key());

    EXPECT_EQ(regions[1]->RegionId(), e2g->RegionId());
    EXPECT_EQ(regions[1]->Range().start_key(), e2g->Range().start_key());
    EXPECT_EQ(regions[1]->Range().end_key(), e2g->Range().end_key());
  }
}

}  // namespace sdk
}  // namespace dingodb
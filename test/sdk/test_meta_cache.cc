#include "test_meta_cache.h"

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

namespace dingodb {
namespace sdk {

using ::testing::_;

const std::string kIpOne = "192.0.0.1";
const std::string kIpTwo = "192.0.0.2";
const std::string kIpThree = "192.0.0.3";

const int kPort = 20001;

const std::string kAddrOne = HostPortToAddrStr(kIpOne, kPort);
const std::string kAddrTwo = HostPortToAddrStr(kIpTwo, kPort);
const std::string kAddrThree = HostPortToAddrStr(kIpThree, kPort);

const std::map<std::string, RaftRole> kInitReplica = {
    {kAddrOne, kLeader}, {kAddrTwo, kFollower}, {kAddrThree, kFollower}};

class RegionTest : public testing::Test {
 protected:
  void SetUp() override { InitRegion(); }

  void TearDown() override {}

  std::shared_ptr<Region> region_;  // NOLINT

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

    region_.reset(new Region(1, range, epoch, type, replicas));
  }
};

TEST_F(RegionTest, TestInit) {
  EXPECT_EQ(region_->RegionId(), 1);
  EXPECT_EQ(region_->Range().start_key(), "a");
  EXPECT_EQ(region_->Range().end_key(), "b");
  EXPECT_EQ(region_->Epoch().version(), 1);
  EXPECT_EQ(region_->Epoch().conf_version(), 1);
  EXPECT_EQ(region_->RegionType(), pb::common::STORE_REGION);

  auto end_points = region_->ReplicaEndPoint();

  for (const auto& end : end_points) {
    EXPECT_TRUE(kInitReplica.find(Helper::EndPointToStr(end)) != kInitReplica.end());
  }

  butil::EndPoint leader;
  Status got = region_->GetLeader(leader);
  EXPECT_TRUE(got.ok());
  EXPECT_EQ(Helper::EndPointToStr(leader), kAddrOne);
  EXPECT_TRUE(region_->IsStale());
}

TEST_F(RegionTest, TestMark) {
  butil::EndPoint end;
  butil::str2endpoint(kAddrOne.c_str(), &end);
  region_->MarkFollower(end);
  butil::EndPoint leader;
  Status got = region_->GetLeader(leader);
  EXPECT_TRUE(got.IsNotFound());

  butil::str2endpoint(kAddrTwo.c_str(), &end);
  region_->MarkLeader(end);
  got = region_->GetLeader(leader);
  EXPECT_TRUE(got.ok());
  EXPECT_EQ(Helper::EndPointToStr(leader), kAddrTwo);

  {
    // test mark and unmark stale
    EXPECT_TRUE(region_->IsStale());
    region_->TEST_UnMarkStale();
    EXPECT_FALSE(region_->IsStale());

    region_->TEST_MarkStale();
    EXPECT_TRUE(region_->IsStale());
  }
}

class MetaCacheTest : public testing::Test {
 protected:
  void SetUp() override {
    auto coordinator_interaction = std::make_shared<CoordinatorInteraction>();
    meta_cache_ = std::make_shared<MockMetaCache>(coordinator_interaction);
  }

  void TearDown() override { meta_cache_.reset(); }

  std::shared_ptr<MockMetaCache> meta_cache_;  // NOLINT
};

static std::shared_ptr<Region> GenRegion(int64_t id, pb::common::Range range, pb::common::RegionEpoch epoch,
                                         pb::common::RegionType type) {
  std::vector<Replica> replicas;
  replicas.reserve(kInitReplica.size());
  for (const auto& entry : kInitReplica) {
    butil::EndPoint end_point;
    butil::str2endpoint(entry.first.c_str(), &end_point);
    replicas.push_back({end_point, entry.second});
  }
  return std::make_shared<Region>(id, range, epoch, type, replicas);
}

static std::shared_ptr<Region> RegionA2C(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'a';
  pb::common::Range range;
  range.set_start_key("a");
  range.set_end_key("c");
  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);
  return GenRegion(id, range, epoch, type);
}

static std::shared_ptr<Region> RegionC2E(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'c';
  pb::common::Range range;
  range.set_start_key("c");
  range.set_end_key("e");
  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);
  return GenRegion(id, range, epoch, type);
}

static std::shared_ptr<Region> RegionE2G(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'e';
  pb::common::Range range;
  range.set_start_key("e");
  range.set_end_key("g");
  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);

  return GenRegion(id, range, epoch, type);
}

static std::shared_ptr<Region> RegionB2F(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'b';
  pb::common::Range range;
  range.set_start_key("b");
  range.set_end_key("f");

  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);

  return GenRegion(id, range, epoch, type);
}

static std::shared_ptr<Region> RegionA2Z(int version = 1, int conf_version = 1,
                                         pb::common::RegionType type = pb::common::RegionType::STORE_REGION) {
  int64_t id = 'a';
  pb::common::Range range;
  range.set_start_key("a");
  range.set_end_key("z");

  pb::common::RegionEpoch epoch;
  epoch.set_version(version);
  epoch.set_conf_version(conf_version);

  return GenRegion(id, range, epoch, type);
}

static void Region2ScanRegionInfo(const std::shared_ptr<Region>& region,
                                  pb::coordinator::ScanRegionInfo* scan_region_info) {
  scan_region_info->set_region_id(region->RegionId());

  auto* range = scan_region_info->mutable_range();
  *range = region->Range();

  auto* epoch = scan_region_info->mutable_region_epoch();
  *epoch = region->Epoch();

  auto replicas = region->Replicas();
  for (const auto& r : replicas) {
    if (r.role == kLeader) {
      auto* leader = scan_region_info->mutable_leader();
      *leader = Helper::EndPointToLocation(r.end_point);
    } else {
      auto* voter = scan_region_info->add_voters();
      *voter = Helper::EndPointToLocation(r.end_point);
    }
  }
}

TEST_F(MetaCacheTest, LookupRegionByKey) {
  auto region = RegionA2C();

  EXPECT_CALL(*meta_cache_, SendScanRegionsRequest(_, _))
      .WillOnce(testing::Invoke(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "b");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          }));

  std::shared_ptr<Region> tmp;
  Status got = meta_cache_->LookupRegionByKey("b", tmp);
  EXPECT_TRUE(got.ok());

  EXPECT_EQ(tmp->RegionId(), region->RegionId());
  EXPECT_EQ(tmp->Range().start_key(), region->Range().start_key());
  EXPECT_EQ(tmp->Range().end_key(), region->Range().end_key());
}

TEST_F(MetaCacheTest, ClearRange) {
  auto region = RegionA2C();

  EXPECT_CALL(*meta_cache_, SendScanRegionsRequest(_, _))
      .WillOnce(testing::Invoke(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "b");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          }));

  {
    // clear exist
    std::shared_ptr<Region> tmp;
    Status got = meta_cache_->LookupRegionByKey("b", tmp);
    EXPECT_TRUE(got.ok());

    meta_cache_->ClearRange(tmp);
    EXPECT_TRUE(tmp->IsStale());

    got = meta_cache_->TEST_FastLookUpRegionByKey("b", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  {
    // clear not exist
    std::shared_ptr<Region> tmp = RegionC2E();
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsNotFound());
    meta_cache_->ClearRange(tmp);
  }
}

TEST_F(MetaCacheTest, AddRegion) {
  auto region = RegionA2C();

  EXPECT_CALL(*meta_cache_, SendScanRegionsRequest(_, _))
      .WillOnce(testing::Invoke(
          [&](const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response) {
            EXPECT_EQ(request.key(), "b");
            Region2ScanRegionInfo(region, response.add_regions());
            return Status::OK();
          }));

  std::shared_ptr<Region> tmp;
  Status got = meta_cache_->LookupRegionByKey("b", tmp);
  EXPECT_TRUE(got.ok());

  {
    // add c2e
    got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsNotFound());

    region = RegionC2E();
    meta_cache_->MaybeAddRegion(region);

    got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), region->RegionId());
  }

  {
    // add e2g
    got = meta_cache_->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsNotFound());

    region = RegionE2G();
    meta_cache_->MaybeAddRegion(region);

    got = meta_cache_->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), region->RegionId());
  }
}

TEST_F(MetaCacheTest, AddInterleaveRegionFromSmall2Large) {
  std::shared_ptr<Region> a2c;
  {
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("b", a2c);
    EXPECT_TRUE(got.IsNotFound());

    meta_cache_->MaybeAddRegion(RegionA2C());

    got = meta_cache_->TEST_FastLookUpRegionByKey("b", a2c);
    EXPECT_TRUE(got.ok());
    EXPECT_FALSE(a2c->IsStale());
  }

  std::shared_ptr<Region> c2e;
  {
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("c", c2e);
    EXPECT_TRUE(got.IsNotFound());

    meta_cache_->MaybeAddRegion(RegionC2E());

    got = meta_cache_->TEST_FastLookUpRegionByKey("c", c2e);
    EXPECT_TRUE(got.ok());
    EXPECT_FALSE(c2e->IsStale());
  }

  std::shared_ptr<Region> e2g;
  {
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("e", e2g);
    EXPECT_TRUE(got.IsNotFound());

    meta_cache_->MaybeAddRegion(RegionE2G());

    got = meta_cache_->TEST_FastLookUpRegionByKey("e", e2g);
    EXPECT_TRUE(got.ok());
    EXPECT_FALSE(e2g->IsStale());
  }

  std::shared_ptr<Region> b2f = RegionB2F();
  {
    meta_cache_->MaybeAddRegion(b2f);
    EXPECT_TRUE(a2c->IsStale());
    EXPECT_TRUE(c2e->IsStale());
    EXPECT_TRUE(e2g->IsStale());

    std::shared_ptr<Region> tmp;
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("b", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());
  }

  meta_cache_->Dump();
}

TEST_F(MetaCacheTest, AddInterleaveRegionFromLarge2Small) {
  std::shared_ptr<Region> a2z = RegionA2Z();
  {
    std::shared_ptr<Region> tmp;
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache_->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache_->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());

    meta_cache_->MaybeAddRegion(a2z);

    got = meta_cache_->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), a2z->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), a2z->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), a2z->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), a2z->RegionId());
  }

  std::shared_ptr<Region> b2f = RegionB2F();
  {
    meta_cache_->MaybeAddRegion(b2f);
    EXPECT_TRUE(a2z->IsStale());

    std::shared_ptr<Region> tmp;
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), b2f->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  std::shared_ptr<Region> a2c = RegionA2C();
  {
    meta_cache_->MaybeAddRegion(a2c);
    EXPECT_TRUE(b2f->IsStale());

    std::shared_ptr<Region> tmp;
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), a2c->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache_->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache_->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  std::shared_ptr<Region> c2e = RegionC2E();
  {
    meta_cache_->MaybeAddRegion(c2e);

    std::shared_ptr<Region> tmp;
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), a2c->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), c2e->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.IsNotFound());

    got = meta_cache_->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  std::shared_ptr<Region> e2g = RegionE2G();
  {
    meta_cache_->MaybeAddRegion(e2g);

    std::shared_ptr<Region> tmp;
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("a", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), a2c->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("c", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), c2e->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("e", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_EQ(tmp->RegionId(), e2g->RegionId());

    got = meta_cache_->TEST_FastLookUpRegionByKey("g", tmp);
    EXPECT_TRUE(got.IsNotFound());
  }

  meta_cache_->Dump();
}

TEST_F(MetaCacheTest, StaleRegion) {
  std::shared_ptr<Region> a2c;
  {
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("b", a2c);
    EXPECT_TRUE(got.IsNotFound());
    meta_cache_->MaybeAddRegion(RegionA2C());

    got = meta_cache_->TEST_FastLookUpRegionByKey("b", a2c);
    EXPECT_TRUE(got.ok());

    EXPECT_FALSE(a2c->IsStale());
  }

  std::shared_ptr<Region> a2c_version2;
  {
    a2c_version2 = RegionA2C(2, 1);
    meta_cache_->MaybeAddRegion(a2c_version2);

    std::shared_ptr<Region> tmp;
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("b", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_FALSE(tmp->IsStale());
    EXPECT_EQ(tmp->Epoch().version(), a2c_version2->Epoch().version());
    EXPECT_EQ(tmp->Epoch().conf_version(), a2c_version2->Epoch().conf_version());

    EXPECT_TRUE(a2c->IsStale());
  }

  std::shared_ptr<Region> a2c_conf2;
  {
    a2c_conf2 = RegionA2C(2, 2);
    meta_cache_->MaybeAddRegion(a2c_conf2);

    std::shared_ptr<Region> tmp;
    Status got = meta_cache_->TEST_FastLookUpRegionByKey("b", tmp);
    EXPECT_TRUE(got.ok());
    EXPECT_FALSE(tmp->IsStale());
    EXPECT_EQ(tmp->Epoch().version(), a2c_conf2->Epoch().version());
    EXPECT_EQ(tmp->Epoch().conf_version(), a2c_conf2->Epoch().conf_version());

    EXPECT_TRUE(a2c->IsStale());
    EXPECT_TRUE(a2c_version2->IsStale());
  }
}

}  // namespace sdk
}  // namespace dingodb
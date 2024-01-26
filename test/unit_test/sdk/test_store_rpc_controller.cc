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

#include <memory>

#include "brpc/channel.h"
#include "common/logging.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock_rpc_interaction.h"
#include "mock_store_rpc_controller.h"
#include "proto/error.pb.h"
#include "sdk/common/common.h"
#include "sdk/meta_cache.h"
#include "sdk/region.h"
#include "sdk/rpc/rpc.h"
#include "sdk/status.h"
#include "sdk/store/store_rpc.h"
#include "sdk/store/store_rpc_controller.h"
#include "test_base.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

class StoreRpcControllerTest : public TestBase {};

TEST_F(StoreRpcControllerTest, CallSuccess) {
  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  StoreRpcController controller(*stub, rpc, region);

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
    CHECK_NOTNULL(get_rpc);
    get_rpc->MutableResponse()->set_value("pong");

    cb();
  });

  Status call = controller.Call();
  EXPECT_TRUE(call.IsOK());

  EXPECT_EQ(rpc.Response()->value(), "pong");
}

TEST_F(StoreRpcControllerTest, RegionIsStale) {
  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  meta_cache->MaybeAddRegion(RegionB2F());
  EXPECT_TRUE(region->IsStale());

  StoreRpcController controller(*stub, rpc, region);

  Status call = controller.Call();
  EXPECT_TRUE(call.IsIncomplete());
}

TEST_F(StoreRpcControllerTest, AllReplicaFail) {
  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    rpc.SetStatus(Status::NetworkError("connect fail"));
    cb();
  });

  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  StoreRpcController controller(*stub, rpc, region);

  Status call = controller.Call();
  // expect all replica failed
  // TODO:: maybe add status sub code
  EXPECT_TRUE(call.IsAborted());
}

TEST_F(StoreRpcControllerTest, RaftNotLeaderWithOutHint) {
  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  MockStoreRpcController controller(*stub, rpc, region);

  Replica leader;
  for (const auto& replica : region->Replicas()) {
    if (replica.role == kLeader) {
      leader = replica;
      break;
    }
  }

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    auto* kv_get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
    CHECK_NOTNULL(kv_get_rpc);
    auto* response = kv_get_rpc->MutableResponse();
    response->mutable_error()->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);

    cb();
  });

  Status call = controller.Call();
  // expect retry rpc times exceed
  EXPECT_TRUE(call.IsAborted());

  for (const auto& replica : region->Replicas()) {
    if (replica.end_point == leader.end_point) {
      EXPECT_TRUE(replica.role == kFollower);
    }
  }

  EXPECT_FALSE(region->IsStale());
}

TEST_F(StoreRpcControllerTest, RaftNotLeaderWithHint) {
  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  MockStoreRpcController controller(*stub, rpc, region);

  Replica leader;
  Replica follower;
  for (const auto& replica : region->Replicas()) {
    if (replica.role == kFollower) {
      follower = replica;
    }

    if (replica.role == kLeader) {
      leader = replica;
    }
  }

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
        CHECK_NOTNULL(kv_get_rpc);
        auto* response = kv_get_rpc->MutableResponse();
        response->mutable_error()->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);
        auto* location = response->mutable_error()->mutable_leader_location();
        *location = Helper::EndPointToLocation(follower.end_point);

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        rpc.Reset();
        auto* kv_get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
        CHECK_NOTNULL(kv_get_rpc);
        kv_get_rpc->MutableResponse()->set_value("pong");
        cb();
      });

  Status call = controller.Call();
  EXPECT_TRUE(call.IsOK());

  EXPECT_EQ(rpc.Response()->value(), "pong");

  for (const auto& replica : region->Replicas()) {
    if (replica.end_point == leader.end_point) {
      EXPECT_TRUE(replica.role == kFollower);
    }

    if (replica.end_point == follower.end_point) {
      EXPECT_TRUE(replica.role == kLeader);
    }
  }

  EXPECT_FALSE(region->IsStale());
}

TEST_F(StoreRpcControllerTest, RegionVersionWithOutStoreRegionInfo) {
  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  MockStoreRpcController controller(*stub, rpc, region);

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* kv_get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
    CHECK_NOTNULL(kv_get_rpc);
    auto* response = kv_get_rpc->MutableResponse();
    response->mutable_error()->set_errcode(pb::error::EREGION_VERSION);

    cb();
  });

  Status call = controller.Call();
  EXPECT_TRUE(call.IsIncomplete());

  EXPECT_TRUE(region->IsStale());
}

TEST_F(StoreRpcControllerTest, RegionVersionWithStoreRegionInfo) {
  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  MockStoreRpcController controller(*stub, rpc, region);

  std::shared_ptr<Region> new_region = RegionC2E(2);

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* kv_get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
    CHECK_NOTNULL(kv_get_rpc);
    auto* response = kv_get_rpc->MutableResponse();
    response->mutable_error()->set_errcode(pb::error::EREGION_VERSION);
    auto* region_info = response->mutable_error()->mutable_store_region_info();

    Region2StoreRegionInfo(new_region, region_info);

    cb();
  });

  Status call = controller.Call();
  EXPECT_TRUE(call.IsIncomplete());

  EXPECT_TRUE(region->IsStale());

  got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());
  EXPECT_EQ(region->RegionId(), new_region->RegionId());
  EXPECT_EQ(EpochCompare(region->Epoch(), new_region->Epoch()), 0);
}

TEST_F(StoreRpcControllerTest, RegionNotFound) {
  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  MockStoreRpcController controller(*stub, rpc, region);

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* kv_get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
    CHECK_NOTNULL(kv_get_rpc);
    auto* response = kv_get_rpc->MutableResponse();
    response->mutable_error()->set_errcode(pb::error::Errno::EREGION_NOT_FOUND);
    cb();
  });

  Status call = controller.Call();
  EXPECT_TRUE(call.IsIncomplete());

  EXPECT_TRUE(region->IsStale());
}

TEST_F(StoreRpcControllerTest, KeyOutOfRange) {
  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  MockStoreRpcController controller(*stub, rpc, region);

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* kv_get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
    CHECK_NOTNULL(kv_get_rpc);
    auto* response = kv_get_rpc->MutableResponse();
    response->mutable_error()->set_errcode(pb::error::Errno::EKEY_OUT_OF_RANGE);
    cb();
  });

  Status call = controller.Call();
  EXPECT_TRUE(call.IsIncomplete());

  EXPECT_TRUE(region->IsStale());
}

TEST_F(StoreRpcControllerTest, RequestFull) {
  std::string key = "d";

  KvPutRpc rpc;
  auto* kv = rpc.MutableRequest()->mutable_kv();
  kv->set_key(key);
  kv->set_value("pong");

  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  MockStoreRpcController controller(*stub, rpc, region);

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    auto* kv_put_rpc = dynamic_cast<KvPutRpc*>(&rpc);
    CHECK_NOTNULL(kv_put_rpc);
    auto* response = kv_put_rpc->MutableResponse();
    response->mutable_error()->set_errcode(pb::error::EREQUEST_FULL);
    cb();
  });

  Status call = controller.Call();
  EXPECT_TRUE(call.IsAborted());

  EXPECT_FALSE(region->IsStale());
}

TEST_F(StoreRpcControllerTest, RequestFullThenSuccess) {
  KvGetRpc rpc;
  std::string key = "d";
  rpc.MutableRequest()->set_key(key);
  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  StoreRpcController controller(*stub, rpc, region);

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvGetRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        auto* response = kv_rpc->MutableResponse();
        response->mutable_error()->set_errcode(pb::error::EREQUEST_FULL);
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        rpc.Reset();
        auto* kv_rpc = dynamic_cast<KvGetRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);
        kv_rpc->MutableResponse()->set_value("pong");
        cb();
      });

  Status call = controller.Call();
  EXPECT_TRUE(call.IsOK());

  EXPECT_EQ(rpc.Response()->value(), "pong");
}

TEST_F(StoreRpcControllerTest, OtherErrorCode) {
  std::string key = "d";

  KvPutRpc rpc;
  auto* kv = rpc.MutableRequest()->mutable_kv();
  kv->set_key(key);
  kv->set_value("pong");

  std::shared_ptr<Region> region;
  Status got = meta_cache->LookupRegionByKey(key, region);
  EXPECT_TRUE(got.IsOK());
  EXPECT_FALSE(region->IsStale());

  MockStoreRpcController controller(*stub, rpc, region);

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* kv_put_rpc = dynamic_cast<KvPutRpc*>(&rpc);
    CHECK_NOTNULL(kv_put_rpc);
    auto* response = kv_put_rpc->MutableResponse();
    response->mutable_error()->set_errcode(pb::error::EINTERNAL);
    cb();
    return Status::OK();
  });

  Status call = controller.Call();
  EXPECT_TRUE(call.IsIncomplete());

  EXPECT_FALSE(region->IsStale());
}

}  // namespace sdk

}  // namespace dingodb

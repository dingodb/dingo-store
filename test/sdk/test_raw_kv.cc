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

#include "client.h"
#include "common.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "store_rpc.h"
#include "test_base.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {
class RawKVTest : public TestBase {
 public:
  RawKVTest() = default;

  ~RawKVTest() override = default;

  void SetUp() override {
    Status kv = NewRawKV(raw_kv);
    CHECK(kv.IsOK());
  }

  void TearDown() override { raw_kv.reset(); }

  std::shared_ptr<RawKV> raw_kv;
};

TEST_F(RawKVTest, Get) {
  std::string key = "b";
  std::string value;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(key, region).IsOK());
  CHECK_NOTNULL(region.get());

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_get_rpc = dynamic_cast<KvGetRpc*>(&rpc);
    CHECK_NOTNULL(kv_get_rpc);

    EXPECT_TRUE(kv_get_rpc->Request()->has_context());
    auto context = kv_get_rpc->Request()->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

    kv_get_rpc->MutableResponse()->set_value("pong");
    return Status::OK();
  });

  Status got = raw_kv->Get(key, value);
  EXPECT_TRUE(got.IsOK());
  EXPECT_EQ(value, "pong");
}

TEST_F(RawKVTest, Put) {
  std::string key = "d";
  std::string value = "pong";

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(key, region).IsOK());
  CHECK_NOTNULL(region.get());

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_put_rpc = dynamic_cast<KvPutRpc*>(&rpc);
    CHECK_NOTNULL(kv_put_rpc);

    EXPECT_TRUE(kv_put_rpc->Request()->has_context());
    auto context = kv_put_rpc->Request()->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

    auto kv = kv_put_rpc->MutableRequest()->kv();
    EXPECT_EQ(kv.key(), key);
    EXPECT_EQ(kv.value(), value);
    return Status::OK();
  });

  Status put = raw_kv->Put(key, value);
  EXPECT_TRUE(put.IsOK());
}

}  // namespace sdk
}  // namespace dingodb
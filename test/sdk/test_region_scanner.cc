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

#include <cstdint>
#include <memory>
#include <string>

#include "client.h"
#include "common.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock_region_scanner.h"
#include "param_config.h"
#include "proto/error.pb.h"
#include "region_scanner_impl.h"
#include "store_rpc.h"
#include "test_base.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

class RegionScannerImplTest : public TestBase {
 public:
  RegionScannerImplTest() = default;

  ~RegionScannerImplTest() override = default;

  void SetUp() override {
    meta_cache->MaybeAddRegion(RegionA2C());
    meta_cache->MaybeAddRegion(RegionE2G());
  }
};

TEST_F(RegionScannerImplTest, OpenCloseSuccess) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).IsOK());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        const auto* request = kv_rpc->Request();

        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_TRUE(request->has_range());
        const auto& range_with_option = request->range();
        EXPECT_TRUE(range_with_option.with_start());
        EXPECT_FALSE(range_with_option.with_end());

        EXPECT_TRUE(range_with_option.has_range());
        const auto& range = range_with_option.range();
        EXPECT_EQ(region->Range().start_key(), range.start_key());
        EXPECT_EQ(region->Range().end_key(), range.end_key());

        EXPECT_EQ(0, request->max_fetch_cnt());
        EXPECT_FALSE(request->key_only());
        EXPECT_FALSE(request->disable_auto_release());
        EXPECT_TRUE(request->disable_coprocessor());

        kv_rpc->MutableResponse()->set_scan_id(scan_id);

        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        const auto* request = kv_rpc->Request();

        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

        return Status::OK();
      });

  RegionScannerImpl scanner(*stub, region);
  Status open = scanner.Open();
  EXPECT_TRUE(open.IsOK());

  EXPECT_TRUE(scanner.HasMore());

  scanner.Close();
}

TEST_F(RegionScannerImplTest, OpenFail) {
  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).IsOK());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
    (void)done;
    auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    auto* error = kv_rpc->MutableResponse()->mutable_error();
    error->set_errcode(pb::error::EINTERNAL);

    return Status::OK();
  });

  RegionScannerImpl scanner(*stub, region);
  Status open = scanner.Open();
  EXPECT_FALSE(open.IsOK());

  EXPECT_FALSE(scanner.HasMore());
  EXPECT_FALSE(scanner.TEST_IsOpen());
}

TEST_F(RegionScannerImplTest, OpenSuccessCloseFail) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).IsOK());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        kv_rpc->MutableResponse()->set_scan_id(scan_id);
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

        auto* error = kv_rpc->MutableResponse()->mutable_error();
        error->set_errcode(pb::error::EINTERNAL);

        return Status::OK();
      });

  RegionScannerImpl scanner(*stub, region);
  Status open = scanner.Open();
  EXPECT_TRUE(open.IsOK());

  EXPECT_TRUE(scanner.HasMore());

  scanner.Close();

  EXPECT_FALSE(scanner.TEST_IsOpen());
}

TEST_F(RegionScannerImplTest, SetBatchSize) {
  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).IsOK());
  CHECK_NOTNULL(region.get());

  RegionScannerImpl scanner(*stub, region);

  EXPECT_EQ(scanner.GetBatchSize(), kScanBatchSize);

  scanner.SetBatchSize(0);
  EXPECT_EQ(scanner.GetBatchSize(), kMinScanBatchSize);

  scanner.SetBatchSize(INT64_MAX);
  EXPECT_EQ(scanner.GetBatchSize(), kMaxScanBatchSize);

  scanner.SetBatchSize(20);
  EXPECT_EQ(scanner.GetBatchSize(), 20);
}

TEST_F(RegionScannerImplTest, NextBatchFail) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).IsOK());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        kv_rpc->MutableResponse()->set_scan_id(scan_id);
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanContinueRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(kv_rpc->Request()->scan_id(), scan_id);
        EXPECT_EQ(kv_rpc->Request()->max_fetch_cnt(), kScanBatchSize);

        auto* error = kv_rpc->MutableResponse()->mutable_error();
        error->set_errcode(pb::error::EINTERNAL);

        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

        return Status::OK();
      });

  RegionScannerImpl scanner(*stub, region);
  Status ret = scanner.Open();
  EXPECT_TRUE(ret.IsOK());

  EXPECT_TRUE(scanner.HasMore());

  std::vector<KVPair> kvs;
  ret = scanner.NextBatch(kvs);
  EXPECT_FALSE(ret.IsOK());

  EXPECT_EQ(kvs.size(), 0);
}

TEST_F(RegionScannerImplTest, NextBatchNoData) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).IsOK());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        kv_rpc->MutableResponse()->set_scan_id(scan_id);
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanContinueRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(kv_rpc->Request()->scan_id(), scan_id);
        EXPECT_EQ(kv_rpc->Request()->max_fetch_cnt(), kScanBatchSize);

        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

        return Status::OK();
      });

  RegionScannerImpl scanner(*stub, region);
  Status ret = scanner.Open();
  EXPECT_TRUE(ret.IsOK());

  EXPECT_TRUE(scanner.HasMore());

  std::vector<KVPair> kvs;
  ret = scanner.NextBatch(kvs);
  EXPECT_TRUE(ret.IsOK());

  EXPECT_EQ(kvs.size(), 0);

  EXPECT_FALSE(scanner.HasMore());
}

TEST_F(RegionScannerImplTest, NextBatchWithData) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).IsOK());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";
  int64_t batch_size = 1;

  std::vector<std::string> fake_datas = {"a001", "a002", "a003"};

  int iter = 0;
  int iter_before = 0;

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        kv_rpc->MutableResponse()->set_scan_id(scan_id);
        return Status::OK();
      })
      .WillRepeatedly([&](Rpc& rpc, google::protobuf::Closure* done) {
        (void)done;
        auto* kv_rpc = dynamic_cast<KvScanContinueRpc*>(&rpc);
        if (kv_rpc == nullptr) {
          // KvScanReleaseRpc
          auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
          CHECK_NOTNULL(kv_rpc);

          EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

          return Status::OK();
        } else {
          // KvScanContinueRpc
          CHECK_NOTNULL(kv_rpc);

          EXPECT_EQ(kv_rpc->Request()->scan_id(), scan_id);
          EXPECT_EQ(kv_rpc->Request()->max_fetch_cnt(), batch_size);

          auto* response = kv_rpc->MutableResponse();
          if (iter < fake_datas.size()) {
            auto* kv = response->add_kvs();
            kv->set_key(fake_datas[iter]);
            kv->set_value(fake_datas[iter]);

            iter_before = iter;
            iter++;
          }

          return Status::OK();
        }
      });

  RegionScannerImpl scanner(*stub, region);
  Status ret = scanner.Open();
  EXPECT_TRUE(ret.IsOK());

  EXPECT_TRUE(scanner.HasMore());

  scanner.SetBatchSize(batch_size);
  EXPECT_EQ(scanner.GetBatchSize(), batch_size);

  while (iter < fake_datas.size()) {
    EXPECT_TRUE(scanner.HasMore());
    std::vector<KVPair> kvs;
    ret = scanner.NextBatch(kvs);
    EXPECT_TRUE(ret.IsOK());
    EXPECT_EQ(kvs.size(), batch_size);
    EXPECT_EQ(kvs.front().key, fake_datas.at(iter_before));
    EXPECT_EQ(kvs.front().value, fake_datas.at(iter_before));
  }

  EXPECT_EQ(iter, fake_datas.size());

  {
    // seek to last
    EXPECT_TRUE(scanner.HasMore());
    std::vector<KVPair> kvs;
    ret = scanner.NextBatch(kvs);
    EXPECT_TRUE(ret.IsOK());
    EXPECT_EQ(kvs.size(), 0);
    EXPECT_FALSE(scanner.HasMore());
  }
}

}  // namespace sdk

}  // namespace dingodb
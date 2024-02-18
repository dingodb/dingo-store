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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock_region_scanner.h"
#include "proto/error.pb.h"
#include "sdk/client.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/rawkv/raw_kv_region_scanner_impl.h"
#include "sdk/status.h"
#include "sdk/store/store_rpc.h"
#include "sdk/utils/async_util.h"
#include "test_base.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

class RawKvRegionScannerImplTest : public TestBase {
 public:
  RawKvRegionScannerImplTest() = default;

  ~RawKvRegionScannerImplTest() override = default;

  void SetUp() override {
    meta_cache->MaybeAddRegion(RegionA2C());
    meta_cache->MaybeAddRegion(RegionE2G());
  }
};

static Status OpenScanner(RawKvRegionScannerImpl& scanner) {
  Status open;
  Synchronizer sync;
  scanner.AsyncOpen(sync.AsStatusCallBack(open));
  sync.Wait();
  return open;
}

static Status CloseScanner(RawKvRegionScannerImpl& scanner) {
  Status close;
  Synchronizer sync;
  scanner.AsyncClose(sync.AsStatusCallBack(close));
  sync.Wait();
  return close;
}

TEST_F(RawKvRegionScannerImplTest, OpenCloseSuccess) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).ok());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
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

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        const auto* request = kv_rpc->Request();

        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

        cb();
      });

  RawKvRegionScannerImpl scanner(*stub, region, region->Range().start_key(), region->Range().end_key());
  Status open = OpenScanner(scanner);
  EXPECT_TRUE(open.ok());
  EXPECT_TRUE(scanner.HasMore());

  CloseScanner(scanner);
}

TEST_F(RawKvRegionScannerImplTest, OpenFail) {
  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).ok());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
    CHECK_NOTNULL(kv_rpc);

    auto* error = kv_rpc->MutableResponse()->mutable_error();
    error->set_errcode(pb::error::EINTERNAL);

    cb();
  });

  RawKvRegionScannerImpl scanner(*stub, region, region->Range().start_key(), region->Range().end_key());
  Status open = OpenScanner(scanner);
  EXPECT_FALSE(open.ok());

  EXPECT_FALSE(scanner.HasMore());
  EXPECT_FALSE(scanner.TEST_IsOpen());
}

TEST_F(RawKvRegionScannerImplTest, OpenSuccessCloseFail) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).ok());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        kv_rpc->MutableResponse()->set_scan_id(scan_id);
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

        auto* error = kv_rpc->MutableResponse()->mutable_error();
        error->set_errcode(pb::error::EINTERNAL);

        cb();
      });

  RawKvRegionScannerImpl scanner(*stub, region, region->Range().start_key(), region->Range().end_key());
  Status open = OpenScanner(scanner);
  EXPECT_TRUE(open.ok());
  EXPECT_TRUE(scanner.HasMore());

  CloseScanner(scanner);

  EXPECT_FALSE(scanner.TEST_IsOpen());
}

TEST_F(RawKvRegionScannerImplTest, SetBatchSize) {
  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).ok());
  CHECK_NOTNULL(region.get());

  RawKvRegionScannerImpl scanner(*stub, region, region->Range().start_key(), region->Range().end_key());

  EXPECT_EQ(scanner.GetBatchSize(), FLAGS_scan_batch_size);

  scanner.SetBatchSize(0);
  EXPECT_EQ(scanner.GetBatchSize(), kMinScanBatchSize);

  scanner.SetBatchSize(INT64_MAX);
  EXPECT_EQ(scanner.GetBatchSize(), kMaxScanBatchSize);

  scanner.SetBatchSize(20);
  EXPECT_EQ(scanner.GetBatchSize(), 20);
}

TEST_F(RawKvRegionScannerImplTest, NextBatchFail) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).ok());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        kv_rpc->MutableResponse()->set_scan_id(scan_id);
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanContinueRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(kv_rpc->Request()->scan_id(), scan_id);
        EXPECT_EQ(kv_rpc->Request()->max_fetch_cnt(), FLAGS_scan_batch_size);

        auto* error = kv_rpc->MutableResponse()->mutable_error();
        error->set_errcode(pb::error::EINTERNAL);

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

        cb();
      });

  RawKvRegionScannerImpl scanner(*stub, region, region->Range().start_key(), region->Range().end_key());
  Status open = OpenScanner(scanner);
  EXPECT_TRUE(open.ok());
  EXPECT_TRUE(scanner.HasMore());

  std::vector<KVPair> kvs;
  Status ret = scanner.NextBatch(kvs);
  EXPECT_FALSE(ret.ok());

  EXPECT_EQ(kvs.size(), 0);
}

TEST_F(RawKvRegionScannerImplTest, NextBatchNoData) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).ok());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        kv_rpc->MutableResponse()->set_scan_id(scan_id);
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanContinueRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(kv_rpc->Request()->scan_id(), scan_id);
        EXPECT_EQ(kv_rpc->Request()->max_fetch_cnt(), FLAGS_scan_batch_size);

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

        cb();
      });

  RawKvRegionScannerImpl scanner(*stub, region, region->Range().start_key(), region->Range().end_key());
  Status open = OpenScanner(scanner);
  EXPECT_TRUE(open.ok());
  EXPECT_TRUE(scanner.HasMore());

  std::vector<KVPair> kvs;
  Status ret = scanner.NextBatch(kvs);
  EXPECT_TRUE(ret.ok());

  EXPECT_EQ(kvs.size(), 0);

  EXPECT_FALSE(scanner.HasMore());
}

TEST_F(RawKvRegionScannerImplTest, NextBatchWithData) {
  testing::InSequence s;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionBetweenRange("a", "c", region).ok());
  CHECK_NOTNULL(region.get());

  std::string scan_id = "101";
  int64_t batch_size = 1;

  std::vector<std::string> fake_datas = {"a001", "a002", "a003"};

  int iter = 0;
  int iter_before = 0;

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanBeginRpc*>(&rpc);
        CHECK_NOTNULL(kv_rpc);

        kv_rpc->MutableResponse()->set_scan_id(scan_id);
        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        auto* kv_rpc = dynamic_cast<KvScanContinueRpc*>(&rpc);
        if (kv_rpc == nullptr) {
          // KvScanReleaseRpc
          auto* kv_rpc = dynamic_cast<KvScanReleaseRpc*>(&rpc);
          CHECK_NOTNULL(kv_rpc);

          EXPECT_EQ(scan_id, kv_rpc->Request()->scan_id());

          cb();
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

          cb();
        }
      });

  RawKvRegionScannerImpl scanner(*stub, region, region->Range().start_key(), region->Range().end_key());
  Status open = OpenScanner(scanner);
  EXPECT_TRUE(open.ok());
  EXPECT_TRUE(scanner.HasMore());

  scanner.SetBatchSize(batch_size);
  EXPECT_EQ(scanner.GetBatchSize(), batch_size);

  Status ret;
  while (iter < fake_datas.size()) {
    EXPECT_TRUE(scanner.HasMore());
    std::vector<KVPair> kvs;
    ret = scanner.NextBatch(kvs);
    EXPECT_TRUE(ret.ok());
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
    EXPECT_TRUE(ret.ok());
    EXPECT_EQ(kvs.size(), 0);
    EXPECT_FALSE(scanner.HasMore());
  }
}

}  // namespace sdk

}  // namespace dingodb
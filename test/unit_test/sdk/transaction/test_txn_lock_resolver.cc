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

#include "gtest/gtest.h"
#include "sdk/common/common.h"
#include "sdk/store/store_rpc.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "test_base.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

class TxnLockResolverTest : public TestBase {
 public:
  TxnLockResolverTest() = default;
  ~TxnLockResolverTest() override = default;

  void SetUp() override {
    TestBase::SetUp();

    lock_resolver = std::make_shared<TxnLockResolver>(*stub);
    init_tso = CurrentFakeTso();
  }

  std::shared_ptr<TxnLockResolver> lock_resolver;
  pb::meta::TsoTimestamp init_tso;
};

TEST_F(TxnLockResolverTest, TxnNotFound) {
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();
  EXPECT_CALL(*coordinator_proxy, TsoService)
      .WillOnce([&](const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response) {
        EXPECT_EQ(request.op_type(), pb::meta::OP_GEN_TSO);
        EXPECT_EQ(request.count(), 1);
        auto* ts = response.mutable_start_timestamp();
        *ts = fake_tso;

        return Status::OK();
      });

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
    CHECK_NOTNULL(txn_rpc);

    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());
    auto context = request->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

    EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
    EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
    EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));

    // txn_rpc->MutableResponse()->set_lock_ttl(10);
    auto* txn_result = txn_rpc->MutableResponse()->mutable_txn_result();
    auto* no_txn = txn_result->mutable_txn_not_found();
    no_txn->set_start_ts(request->lock_ts());
    no_txn->set_primary_key(request->primary_key());

    cb();
  });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(TxnLockResolverTest, Locked) {
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*coordinator_proxy, TsoService)
      .WillOnce([&](const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response) {
        EXPECT_EQ(request.op_type(), pb::meta::OP_GEN_TSO);
        EXPECT_EQ(request.count(), 1);
        auto* ts = response.mutable_start_timestamp();
        *ts = fake_tso;

        return Status::OK();
      });

  EXPECT_CALL(*store_rpc_interaction, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
    CHECK_NOTNULL(txn_rpc);

    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());
    auto context = request->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

    EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
    EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
    EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));

    txn_rpc->MutableResponse()->set_lock_ttl(10);

    cb();
  });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.IsTxnLockConflict());
}

TEST_F(TxnLockResolverTest, Committed) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*coordinator_proxy, TsoService)
      .WillOnce([&](const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response) {
        EXPECT_EQ(request.op_type(), pb::meta::OP_GEN_TSO);
        EXPECT_EQ(request.count(), 1);
        auto* ts = response.mutable_start_timestamp();
        *ts = fake_tso;

        return Status::OK();
      });

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        txn_rpc->MutableResponse()->set_commit_ts(request->current_ts());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve primary key
        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.primary_lock());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve conlict key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.key());

        cb();
      });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(TxnLockResolverTest, CommittedResolvePrimaryKeyFail) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*coordinator_proxy, TsoService)
      .WillOnce([&](const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response) {
        EXPECT_EQ(request.op_type(), pb::meta::OP_GEN_TSO);
        EXPECT_EQ(request.count(), 1);
        auto* ts = response.mutable_start_timestamp();
        *ts = fake_tso;

        return Status::OK();
      });

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        txn_rpc->MutableResponse()->set_commit_ts(request->current_ts());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve primary key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.primary_lock());

        auto* response = txn_rpc->MutableResponse();
        auto* error = response->mutable_error();
        error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);

        cb();
      });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(!s.ok());
}

TEST_F(TxnLockResolverTest, CommittedResolveConflictKeyFail) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*coordinator_proxy, TsoService)
      .WillOnce([&](const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response) {
        EXPECT_EQ(request.op_type(), pb::meta::OP_GEN_TSO);
        EXPECT_EQ(request.count(), 1);
        auto* ts = response.mutable_start_timestamp();
        *ts = fake_tso;

        return Status::OK();
      });

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        txn_rpc->MutableResponse()->set_commit_ts(request->current_ts());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve primary key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.primary_lock());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve primary key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.key());

        auto* response = txn_rpc->MutableResponse();
        auto* error = response->mutable_error();
        error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);

        cb();
      });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(!s.ok());
}

TEST_F(TxnLockResolverTest, Rollbacked) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*coordinator_proxy, TsoService)
      .WillOnce([&](const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response) {
        EXPECT_EQ(request.op_type(), pb::meta::OP_GEN_TSO);
        EXPECT_EQ(request.count(), 1);
        auto* ts = response.mutable_start_timestamp();
        *ts = fake_tso;

        return Status::OK();
      });

  EXPECT_CALL(*store_rpc_interaction, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        txn_rpc->MutableResponse()->set_lock_ttl(0);
        txn_rpc->MutableResponse()->set_commit_ts(0);

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve primary key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 0);
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.primary_lock());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve conlict key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(context.region_epoch(), region->Epoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 0);
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.key());

        cb();
      });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

}  // namespace sdk

}  // namespace dingodb
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
#include <memory>

#include "butil/status.h"
#include "common/constant.h"
#include "engine/gc_safe_point.h"
#include "engine/txn_engine_helper.h"
#include "proto/error.pb.h"

namespace dingodb {

DECLARE_bool(gc_enable_safe_point_read_check);

// Core-logic tests for the gc safe point read guard, dependency-injected with
// a standalone GCSafePointManager (no server singleton — keeps this suite
// order-independent from the other txn suites in the shared test binary).
// The three call-site wirings (BatchGet / Scan stream creation / Prewrite)
// are exercised end-to-end against a real cluster in the S3 verification.
class TxnSafePointGuardTest : public testing::Test {
 protected:
  void SetUp() override {
    FLAGS_gc_enable_safe_point_read_check = false;
    manager = std::make_shared<GCSafePointManager>();
  }

  void TearDown() override { FLAGS_gc_enable_safe_point_read_check = true; }

  static butil::Status Check(const std::shared_ptr<GCSafePointManager> &mgr, int64_t tenant_id, int64_t start_ts) {
    return TxnEngineHelper::CheckSafePointForRead(mgr, tenant_id, /*region_id=*/1, start_ts, "UnitTest");
  }

  std::shared_ptr<GCSafePointManager> manager;
};

// Flag off (the default): even ancient start_ts pass.
TEST_F(TxnSafePointGuardTest, FlagOffNoRejection) {
  manager->SetGcFlagAndSafePointTs({{Constant::kDefaultTenantId, 1000}}, false);
  EXPECT_TRUE(Check(manager, Constant::kDefaultTenantId, 1).ok());
}

// start_ts <= safe point is rejected; strictly newer passes.
TEST_F(TxnSafePointGuardTest, RejectAtOrBelowSafePoint) {
  FLAGS_gc_enable_safe_point_read_check = true;
  manager->SetGcFlagAndSafePointTs({{Constant::kDefaultTenantId, 1000}}, false);

  EXPECT_EQ(Check(manager, Constant::kDefaultTenantId, 999).error_code(), pb::error::Errno::ETXN_LT_GC_SAFE_POINT);
  EXPECT_EQ(Check(manager, Constant::kDefaultTenantId, 1000).error_code(), pb::error::Errno::ETXN_LT_GC_SAFE_POINT);
  EXPECT_TRUE(Check(manager, Constant::kDefaultTenantId, 1001).ok());
}

// gc_stop pauses garbage collection but does NOT make a stale snapshot safe:
// versions may already be gone. The guard must ignore the flag.
TEST_F(TxnSafePointGuardTest, GcStopDoesNotBypassGuard) {
  FLAGS_gc_enable_safe_point_read_check = true;
  manager->SetGcFlagAndSafePointTs({{Constant::kDefaultTenantId, 1000}}, true);

  EXPECT_EQ(Check(manager, Constant::kDefaultTenantId, 500).error_code(), pb::error::Errno::ETXN_LT_GC_SAFE_POINT);
}

// A tenant without any recorded safe point must never reject: the manager
// returns {true, 0} for unknown tenants and ts 0 disables the comparison.
TEST_F(TxnSafePointGuardTest, UnknownTenantNoRejection) {
  FLAGS_gc_enable_safe_point_read_check = true;
  manager->SetGcFlagAndSafePointTs({{7, 1000}}, false);

  EXPECT_TRUE(Check(manager, Constant::kDefaultTenantId, 1).ok());
}

// Per-tenant isolation: only the key's own tenant safe point applies.
TEST_F(TxnSafePointGuardTest, PerTenantSafePoint) {
  FLAGS_gc_enable_safe_point_read_check = true;
  manager->SetGcFlagAndSafePointTs({{Constant::kDefaultTenantId, 100}, {7, 1000}}, false);

  EXPECT_TRUE(Check(manager, Constant::kDefaultTenantId, 500).ok());
  EXPECT_EQ(Check(manager, 7, 500).error_code(), pb::error::Errno::ETXN_LT_GC_SAFE_POINT);
}

// A null manager (server not fully initialized) must fail open, not crash.
TEST_F(TxnSafePointGuardTest, NullManagerNoRejection) {
  FLAGS_gc_enable_safe_point_read_check = true;
  EXPECT_TRUE(Check(nullptr, Constant::kDefaultTenantId, 1).ok());
}

}  // namespace dingodb

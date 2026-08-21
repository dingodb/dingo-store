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

// ConfigHelper::GetBalanceRegionDefaultStoreRegionSize resolves in priority
// order: runtime gflag override (> 0) -> config file value -> built-in
// default. The gflag defaults to 0, which means "no override" and keeps the
// pre-existing config-file semantics untouched.

#include <gtest/gtest.h>

#include <cstdint>

#include "config/config_helper.h"
#include "gflags/gflags.h"

// Defined at global scope in config/config_helper.cc.
DECLARE_int64(balance_region_default_store_region_size);

namespace dingodb {

class BalanceRegionStoreRegionSizeTest : public testing::Test {
 protected:
  void SetUp() override { saved_flag_ = FLAGS_balance_region_default_store_region_size; }
  void TearDown() override { FLAGS_balance_region_default_store_region_size = saved_flag_; }

 private:
  int64_t saved_flag_{0};
};

TEST_F(BalanceRegionStoreRegionSizeTest, FlagOverridesConfigAndDefault) {
  FLAGS_balance_region_default_store_region_size = 0;
  const int64_t baseline = ConfigHelper::GetBalanceRegionDefaultStoreRegionSize();
  ASSERT_GT(baseline, 0);

  // 4GB deliberately exceeds int32 range: the override path must be 64-bit clean.
  FLAGS_balance_region_default_store_region_size = 4294967296LL;
  EXPECT_EQ(4294967296LL, ConfigHelper::GetBalanceRegionDefaultStoreRegionSize());
}

TEST_F(BalanceRegionStoreRegionSizeTest, FlagZeroDisablesOverride) {
  FLAGS_balance_region_default_store_region_size = 7777;
  ASSERT_EQ(7777, ConfigHelper::GetBalanceRegionDefaultStoreRegionSize());

  FLAGS_balance_region_default_store_region_size = 0;
  const int64_t value = ConfigHelper::GetBalanceRegionDefaultStoreRegionSize();
  EXPECT_NE(7777, value);
  EXPECT_GT(value, 0);
}

}  // namespace dingodb

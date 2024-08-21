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
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "engine/gc_safe_point.h"
#include "proto/common.pb.h"

namespace dingodb {

class GCSafePointManagerTest : public testing::Test {
 protected:
  static void SetUpTestSuite() { gc_safe_point_manager = std::make_shared<GCSafePointManager>(); }

  static void TearDownTestSuite() { gc_safe_point_manager.reset(); }

  void SetUp() override {}

  void TearDown() override {}

  static inline std::shared_ptr<GCSafePointManager> gc_safe_point_manager = nullptr;
};

TEST_F(GCSafePointManagerTest, SetGcFlagAndSafePointTs) {
  std::map<int64_t, int64_t> safe_point_ts_group;

  // start -> stop
  {
    // first [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();

    // second [0, 1, 2, 3] stop
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, true);
    safe_point_ts_group.clear();

    EXPECT_EQ(0, gc_safe_point_manager->GetAllGcFlagAndSafePointTs().size());
    gc_safe_point_manager->ClearAll();
  }

  // stop -> start
  {
    // first [0, 1, 2, 3] stop
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, true);
    safe_point_ts_group.clear();

    // second [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();

    auto result = gc_safe_point_manager->GetAllGcFlagAndSafePointTs();
    EXPECT_EQ(4, result.size());
    EXPECT_EQ(false, result[Constant::kDefaultTenantId].first);
    EXPECT_EQ(10, result[Constant::kDefaultTenantId].second);
    EXPECT_EQ(false, result[1].first);
    EXPECT_EQ(11, result[1].second);
    EXPECT_EQ(false, result[2].first);
    EXPECT_EQ(12, result[2].second);
    EXPECT_EQ(false, result[3].first);
    EXPECT_EQ(13, result[3].second);
    gc_safe_point_manager->ClearAll();
  }

  // start -> start
  {
    // first [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();

    // second [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();

    auto result = gc_safe_point_manager->GetAllGcFlagAndSafePointTs();
    EXPECT_EQ(4, result.size());
    EXPECT_EQ(false, result[Constant::kDefaultTenantId].first);
    EXPECT_EQ(10, result[Constant::kDefaultTenantId].second);
    EXPECT_EQ(false, result[1].first);
    EXPECT_EQ(11, result[1].second);
    EXPECT_EQ(false, result[2].first);
    EXPECT_EQ(12, result[2].second);
    EXPECT_EQ(false, result[3].first);
    EXPECT_EQ(13, result[3].second);
    gc_safe_point_manager->ClearAll();
  }

  // start -> start
  {
    // first [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();

    // second [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();

    auto result = gc_safe_point_manager->GetAllGcFlagAndSafePointTs();
    EXPECT_EQ(4, result.size());
    EXPECT_EQ(false, result[Constant::kDefaultTenantId].first);
    EXPECT_EQ(10, result[Constant::kDefaultTenantId].second);
    EXPECT_EQ(false, result[1].first);
    EXPECT_EQ(11, result[1].second);
    EXPECT_EQ(false, result[2].first);
    EXPECT_EQ(12, result[2].second);
    EXPECT_EQ(false, result[3].first);
    EXPECT_EQ(13, result[3].second);
    gc_safe_point_manager->ClearAll();
  }
}

TEST_F(GCSafePointManagerTest, GetGcFlagAndSafePointTs) {
  std::map<int64_t, int64_t> safe_point_ts_group;

  // start
  {
    // first [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();
    auto flag_ts = gc_safe_point_manager->GetGcFlagAndSafePointTs(1);
    EXPECT_EQ(false, flag_ts.first);
    EXPECT_EQ(11, flag_ts.second);
    gc_safe_point_manager->ClearAll();
  }

  // stop
  {
    // second [0, 1, 2, 3] stop
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, true);
    safe_point_ts_group.clear();
    auto flag_ts = gc_safe_point_manager->GetGcFlagAndSafePointTs(1);
    EXPECT_EQ(true, flag_ts.first);
    EXPECT_EQ(11, flag_ts.second);
    gc_safe_point_manager->ClearAll();
  }

  // none
  {
    auto flag_ts = gc_safe_point_manager->GetGcFlagAndSafePointTs(1);
    EXPECT_EQ(true, flag_ts.first);
    EXPECT_EQ(0, flag_ts.second);
  }
}

TEST_F(GCSafePointManagerTest, FindSafePoint) {
  std::map<int64_t, int64_t> safe_point_ts_group;

  // start
  {
    // first [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();
    auto gc_safe_point = gc_safe_point_manager->FindSafePoint(1);
    EXPECT_EQ(1, gc_safe_point->GetTenantId());
    auto flag_ts = gc_safe_point->GetGcFlagAndSafePointTs();
    EXPECT_EQ(false, flag_ts.first);
    EXPECT_EQ(11, flag_ts.second);
    EXPECT_EQ(false, gc_safe_point->GetGcStop());
    gc_safe_point_manager->ClearAll();
  }

  // stop
  {
    // second [0, 1, 2, 3] stop
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, true);
    safe_point_ts_group.clear();
    auto gc_safe_point = gc_safe_point_manager->FindSafePoint(1);
    EXPECT_EQ(1, gc_safe_point->GetTenantId());
    auto flag_ts = gc_safe_point->GetGcFlagAndSafePointTs();
    EXPECT_EQ(true, flag_ts.first);
    EXPECT_EQ(11, flag_ts.second);
    EXPECT_EQ(true, gc_safe_point->GetGcStop());
    gc_safe_point_manager->ClearAll();
  }

  // none
  {
    auto gc_safe_point = gc_safe_point_manager->FindSafePoint(1);
    EXPECT_EQ(nullptr, gc_safe_point.get());
  }
}

TEST_F(GCSafePointManagerTest, RemoveSafePoint) {
  std::map<int64_t, int64_t> safe_point_ts_group;

  // start
  {
    // first [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();
    gc_safe_point_manager->RemoveSafePoint(1);

    gc_safe_point_manager->ClearAll();
  }

  // stop
  {
    // second [0, 1, 2, 3] stop
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, true);
    safe_point_ts_group.clear();
    gc_safe_point_manager->RemoveSafePoint(1);

    gc_safe_point_manager->ClearAll();
  }

  // none
  { gc_safe_point_manager->RemoveSafePoint(1); }
}

TEST_F(GCSafePointManagerTest, GcStop) {
  std::map<int64_t, int64_t> safe_point_ts_group;

  // start
  {
    // first [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();

    gc_safe_point_manager->SetGcStop(1, true);
    EXPECT_EQ(true, gc_safe_point_manager->GetGcStop(1));
    gc_safe_point_manager->SetGcStop(1, false);
    EXPECT_EQ(false, gc_safe_point_manager->GetGcStop(1));

    gc_safe_point_manager->ClearAll();
  }

  // stop
  {
    // second [0, 1, 2, 3] stop
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, true);
    safe_point_ts_group.clear();
    gc_safe_point_manager->SetGcStop(1, true);
    EXPECT_EQ(true, gc_safe_point_manager->GetGcStop(1));
    gc_safe_point_manager->SetGcStop(1, false);
    EXPECT_EQ(false, gc_safe_point_manager->GetGcStop(1));
    gc_safe_point_manager->ClearAll();
  }

  // none
  {
    gc_safe_point_manager->SetGcStop(1, true);
    EXPECT_EQ(true, gc_safe_point_manager->GetGcStop(1));
    gc_safe_point_manager->SetGcStop(1, false);
    EXPECT_EQ(true, gc_safe_point_manager->GetGcStop(1));
  }
}

TEST_F(GCSafePointManagerTest, LastAccomplishedSafePointTs) {
  std::map<int64_t, int64_t> safe_point_ts_group;

  // start
  {
    // first [0, 1, 2, 3] start
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, false);
    safe_point_ts_group.clear();

    gc_safe_point_manager->SetLastAccomplishedSafePointTs(1, 1111);
    EXPECT_EQ(1111, gc_safe_point_manager->GetLastAccomplishedSafePointTs(1));

    gc_safe_point_manager->ClearAll();
  }

  // stop
  {
    // second [0, 1, 2, 3] stop
    safe_point_ts_group[Constant::kDefaultTenantId] = 10;
    safe_point_ts_group[1] = 11;
    safe_point_ts_group[2] = 12;
    safe_point_ts_group[3] = 13;

    gc_safe_point_manager->SetGcFlagAndSafePointTs(safe_point_ts_group, true);
    safe_point_ts_group.clear();
    gc_safe_point_manager->SetLastAccomplishedSafePointTs(1, 1111);
    EXPECT_EQ(1111, gc_safe_point_manager->GetLastAccomplishedSafePointTs(1));
    gc_safe_point_manager->ClearAll();
  }

  // none
  {
    gc_safe_point_manager->SetLastAccomplishedSafePointTs(1, 1111);
    EXPECT_EQ(0, gc_safe_point_manager->GetLastAccomplishedSafePointTs(1));
  }
}

}  // namespace dingodb

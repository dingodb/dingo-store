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

#include <memory>
#include <string>

#include "meta/store_meta_manager.h"

class StoreRegionMetaTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(StoreRegionMetaTest, ParseRegionId) {
  dingodb::StoreRegionMeta store_region_meta(nullptr, nullptr);

  int64_t region_id = store_region_meta.ParseRegionId("META_REGION_11111");

  EXPECT_EQ(11111, region_id);
}

TEST_F(StoreRegionMetaTest, AddRegion) {
  auto store_region_mata = std::make_shared<dingodb::StoreRegionMeta>(nullptr, nullptr);
  dingodb::pb::common::RegionDefinition definition;
  definition.set_id(1001);
  store_region_mata->AddRegion(dingodb::store::Region::New(definition));
  auto region = store_region_mata->GetRegion(1001);
  EXPECT_NE(nullptr, region);
  EXPECT_EQ(1001, region->Id());
}
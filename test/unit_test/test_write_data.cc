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

#include <string>

#include "engine/write_data.h"
#include "fmt/core.h"

class WriteDataBuilderTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(WriteDataBuilderTest, BuildWriteData) {
  std::vector<dingodb::pb::common::KeyValue> kvs;
  for (int i = 0; i < 1; ++i) {
    dingodb::pb::common::KeyValue kv;
    kv.set_key(fmt::format("key000000-{}", i));
    kv.set_value("value00000000-{}", i);
    kvs.push_back(kv);
  }

  auto writedata = dingodb::WriteDataBuilder::BuildWrite("default", kvs);
  for (auto& datum : writedata->Datums()) {
    auto* request = datum->TransformToRaft();
    delete request;
  }

  EXPECT_EQ(true, true);
}
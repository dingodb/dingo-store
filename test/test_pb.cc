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

#include <iostream>
#include <string>

#include "proto/store.pb.h"

class StorePbTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST(StorePbTest, GetTypeName) {
  dingodb::pb::store::KvBatchPutIfAbsentResponse kv_batch_put_if_absent_response;
  auto batch_put_if_absent_name = kv_batch_put_if_absent_response.GetTypeName();

  std::cout << "batch_put_if_absent_name : " << batch_put_if_absent_name << std::endl;

  EXPECT_EQ(batch_put_if_absent_name, "dingodb.pb.store.KvBatchPutIfAbsentResponse");

  dingodb::pb::store::KvPutIfAbsentResponse kv_put_if_absent_response;
  auto put_if_absent_name = kv_put_if_absent_response.GetTypeName();

  std::cout << "put_if_absent_name : " << put_if_absent_name << std::endl;
  EXPECT_EQ(put_if_absent_name, "dingodb.pb.store.KvPutIfAbsentResponse");
}

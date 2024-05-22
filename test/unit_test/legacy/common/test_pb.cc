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
#include <iostream>
#include <string>

#include "engine/write_data.h"
#include "proto/raft.pb.h"
#include "proto/store.pb.h"

class StorePbTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST(StorePbTest, GetTypeName) {
  dingodb::pb::store::KvBatchPutIfAbsentResponse kv_batch_put_if_absent_response;
  auto batch_put_if_absent_name = kv_batch_put_if_absent_response.GetTypeName();

  EXPECT_EQ(batch_put_if_absent_name, "dingodb.pb.store.KvBatchPutIfAbsentResponse");

  dingodb::pb::store::KvPutIfAbsentResponse kv_put_if_absent_response;
  auto put_if_absent_name = kv_put_if_absent_response.GetTypeName();

  EXPECT_EQ(put_if_absent_name, "dingodb.pb.store.KvPutIfAbsentResponse");
}

// uint64 id = 1;
// Vector vector = 2;  // vector data
// // scalar data of this vector, key: scalar key, value: scalar value data. meta data.
// VectorScalardata scalar_data = 3;
// VectorTableData table_data = 4;  // table data of this vector, only SQL can use this field

dingodb::pb::common::VectorWithId GenVector(uint16_t id, int dimension) {
  dingodb::pb::common::VectorWithId vector;

  vector.set_id(id);
  for (int i = 0; i < dimension; ++i) {
    vector.mutable_vector()->add_float_values(1.1111);
  }

  return vector;
}

TEST(StorePbTest, MemoryLeak) {
  GTEST_SKIP() << "Skip MemoryLeak...";
  // for (;;) {
  //   std::vector<dingodb::pb::common::VectorWithId> vectors;
  //   vectors.reserve(1024);

  //   for (int i = 0; i < 1024; ++i) {
  //     vectors.push_back(GenVector(i, 512));
  //   }
  //   auto write_data = dingodb::WriteDataBuilder::BuildWrite("default", vectors);

  //   auto raft_cmd = std::make_shared<dingodb::pb::raft::RaftCmdRequest>();

  //   dingodb::pb::raft::RequestHeader* header = raft_cmd->mutable_header();
  //   header->set_region_id(1111);

  //   auto* requests = raft_cmd->mutable_requests();
  //   for (auto& datum : write_data->Datums()) {
  //     requests->AddAllocated(datum->TransformToRaft());
  //   }
  // }
}

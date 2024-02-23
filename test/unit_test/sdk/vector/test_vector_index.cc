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

#include "common/logging.h"
#include "gtest/gtest.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_index.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

class VectorIndexTest : public testing::Test {
 protected:
  void SetUp() override { InitVectorIndex(); }

  void TearDown() override {}

  std::shared_ptr<VectorIndex> vector_index;

  std::string index_name{"test"};
  int64_t schema_id{2};
  std::vector<int64_t> index_and_part_ids{2, 3, 4, 5, 6};
  int64_t index_id = index_and_part_ids[0];
  std::vector<int64_t> range_seperator_ids = {5, 10, 20};
  FlatParam flat_param{1000, dingodb::sdk::MetricType::kL2};

 private:
  void InitVectorIndex() {
    pb::meta::IndexDefinitionWithId index_definition_with_id;
    FillVectorIndexId(index_definition_with_id.mutable_index_id(), index_id, schema_id);
    auto* defination = index_definition_with_id.mutable_index_definition();
    defination->set_name(index_name);
    FillRangePartitionRule(defination->mutable_index_partition(), range_seperator_ids, index_and_part_ids);
    defination->set_replica(3);

    auto* index_parameter = defination->mutable_index_parameter();
    index_parameter->set_index_type(pb::common::IndexType::INDEX_TYPE_VECTOR);
    FillFlatParmeter(index_parameter->mutable_vector_index_parameter(), flat_param);

    vector_index = std::make_shared<VectorIndex>(index_definition_with_id);
  }
};

TEST_F(VectorIndexTest, TestInit) {
  EXPECT_NE(vector_index, nullptr);

  EXPECT_EQ(vector_index->GetId(), index_id);
  EXPECT_EQ(vector_index->GetName(), index_name);
  EXPECT_EQ(vector_index->GetSchemaId(), schema_id);

  EXPECT_EQ(vector_index->GetVectorIndexType(), VectorIndexType::kFlat);
}

TEST_F(VectorIndexTest, TestGetPartitionId) {
  EXPECT_EQ(vector_index->GetPartitionId(1), index_and_part_ids[1]);
  EXPECT_EQ(vector_index->GetPartitionId(4), index_and_part_ids[1]);
  EXPECT_EQ(vector_index->GetPartitionId(5), index_and_part_ids[2]);
  EXPECT_EQ(vector_index->GetPartitionId(9), index_and_part_ids[2]);
  EXPECT_EQ(vector_index->GetPartitionId(10), index_and_part_ids[3]);
  EXPECT_EQ(vector_index->GetPartitionId(19), index_and_part_ids[3]);
  EXPECT_EQ(vector_index->GetPartitionId(20), index_and_part_ids[4]);
  EXPECT_EQ(vector_index->GetPartitionId(100), index_and_part_ids[4]);
}

}  // namespace sdk

}  // namespace dingodb
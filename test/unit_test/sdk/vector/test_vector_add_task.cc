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

#include "gtest/gtest.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_add_task.h"
#include "sdk/vector/vector_common.h"
#include "test_base.h"
namespace dingodb {
namespace sdk {

class SDKVectorAddTaskTest : public TestBase {
 public:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(SDKVectorAddTaskTest, EmptyVectors) {
  std::vector<VectorWithId> ids;
  VectorAddTask task(*stub, 1, ids);
  Status s = task.Run();
  EXPECT_TRUE(s.IsInvalidArgument());
}

static std::shared_ptr<VectorIndex> CreateFakeVectorIndex(int64_t start_id = 0) {
  std::shared_ptr<VectorIndex> vector_index;

  {
    std::string index_name{"test"};
    int64_t schema_id{2};
    std::vector<int64_t> index_and_part_ids{2, 3, 4, 5, 6};
    int64_t index_id = index_and_part_ids[0];
    std::vector<int64_t> range_seperator_ids = {5, 10, 20};
    FlatParam flat_param{1000, dingodb::sdk::MetricType::kL2};

    pb::meta::IndexDefinitionWithId index_definition_with_id;
    FillVectorIndexId(index_definition_with_id.mutable_index_id(), index_id, schema_id);
    auto* defination = index_definition_with_id.mutable_index_definition();
    defination->set_name(index_name);
    FillRangePartitionRule(defination->mutable_index_partition(), range_seperator_ids, index_and_part_ids);
    defination->set_replica(3);
    if (start_id > 0) {
      defination->set_with_auto_incrment(true);
      defination->set_auto_increment(start_id);
    }

    auto* index_parameter = defination->mutable_index_parameter();
    index_parameter->set_index_type(pb::common::IndexType::INDEX_TYPE_VECTOR);
    FillFlatParmeter(index_parameter->mutable_vector_index_parameter(), flat_param);

    vector_index = std::make_shared<VectorIndex>(index_definition_with_id);
  }

  return vector_index;
}

TEST_F(SDKVectorAddTaskTest, InitNoAutoIncreFail) {
  auto vector_index = CreateFakeVectorIndex();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GetIndexRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->index_id().entity_id(), vector_index->GetId());
    *(t_rpc->MutableResponse()->mutable_index_definition_with_id()) = vector_index->GetIndexDefWithId();
    return Status::OK();
  });

  std::vector<VectorWithId> ids;
  for (auto i = 0; i < 10; i++) {
    VectorWithId vector_with_id;
    vector_with_id.id = 0;
    ids.push_back(vector_with_id);
  }
  VectorAddTask task(*stub, vector_index->GetId(), ids);

  Status s = task.TEST_Init();
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST_F(SDKVectorAddTaskTest, InitNoAutoIncreSuccess) {
  auto vector_index = CreateFakeVectorIndex();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GetIndexRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->index_id().entity_id(), vector_index->GetId());
    *(t_rpc->MutableResponse()->mutable_index_definition_with_id()) = vector_index->GetIndexDefWithId();
    return Status::OK();
  });

  std::vector<VectorWithId> ids;
  for (auto i = 0; i < 10; i++) {
    VectorWithId vector_with_id;
    vector_with_id.id = i + 1;
    ids.push_back(vector_with_id);
  }
  VectorAddTask task(*stub, vector_index->GetId(), ids);

  Status s = task.TEST_Init();
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKVectorAddTaskTest, InitAutoIncreFail) {
  auto vector_index = CreateFakeVectorIndex(1);

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GetIndexRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->index_id().entity_id(), vector_index->GetId());
    *(t_rpc->MutableResponse()->mutable_index_definition_with_id()) = vector_index->GetIndexDefWithId();
    return Status::OK();
  });

  std::vector<VectorWithId> ids;
  for (auto i = 0; i < 10; i++) {
    VectorWithId vector_with_id;
    vector_with_id.id = (i % 2 ? 0 : i + 1);
    ids.push_back(vector_with_id);
  }
  VectorAddTask task(*stub, vector_index->GetId(), ids);

  Status s = task.TEST_Init();
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST_F(SDKVectorAddTaskTest, InitAutoIncreSuccess) {
  auto vector_index = CreateFakeVectorIndex(1);

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GetIndexRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->index_id().entity_id(), vector_index->GetId());
    *(t_rpc->MutableResponse()->mutable_index_definition_with_id()) = vector_index->GetIndexDefWithId();
    return Status::OK();
  });

  std::vector<VectorWithId> ids;
  for (auto i = 0; i < 10; i++) {
    VectorWithId vector_with_id;
    vector_with_id.id = (i + 1);
    ids.push_back(vector_with_id);
  }
  VectorAddTask task(*stub, vector_index->GetId(), ids);

  Status s = task.TEST_Init();
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKVectorAddTaskTest, InitAutoIncreUseGenerateid) {
  auto vector_index = CreateFakeVectorIndex(1);

  EXPECT_CALL(*meta_rpc_controller, SyncCall)
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GetIndexRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->index_id().entity_id(), vector_index->GetId());
        *(t_rpc->MutableResponse()->mutable_index_definition_with_id()) = vector_index->GetIndexDefWithId();
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        t_rpc->MutableResponse()->set_start_id(1);
        t_rpc->MutableResponse()->set_end_id(100);
        return Status::OK();
      });

  int64_t count = 10;
  std::vector<VectorWithId> ids;
  for (auto i = 0; i < count; i++) {
    VectorWithId vector_with_id;
    vector_with_id.id = 0;
    ids.push_back(vector_with_id);
  }

  VectorAddTask task(*stub, vector_index->GetId(), ids);
  Status s = task.TEST_Init();
  EXPECT_TRUE(s.ok());
  for (auto i = 0; i < count; i++) {
    EXPECT_EQ(ids[i].id, i + 1);
  }
}

}  // namespace sdk
}  // namespace dingodb
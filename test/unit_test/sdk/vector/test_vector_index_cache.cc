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
#include <vector>

#include "gtest/gtest.h"
#include "sdk/rpc/brpc/coordinator_rpc.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_index_cache.h"
#include "test_base.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

TEST(SDKVectorIndexCacheKeyTest, TestEncodeDecodeVectorIndexCacheKey) {
  int64_t schema_id = 123;
  std::string index_name = "test_index";

  VectorIndexCacheKey key = EncodeVectorIndexCacheKey(schema_id, index_name);

  int64_t decoded_schema_id;
  std::string decoded_index_name;
  DecodeVectorIndexCacheKey(key, decoded_schema_id, decoded_index_name);

  EXPECT_EQ(decoded_schema_id, schema_id);
  EXPECT_EQ(decoded_index_name, index_name);
}

class SDKVectorIndexCacheTest : public TestBase {
 protected:
  void SetUp() override { cache = std::make_shared<VectorIndexCache>(*stub); }

  void TearDown() override { cache.reset(); }

  int64_t schema_id{2};
  std::shared_ptr<VectorIndexCache> cache;
};

TEST_F(SDKVectorIndexCacheTest, GetIndexIdByNameNotOK) {
  std::string index_name = "test";

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GetIndexByNameRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->index_name(), index_name);
    return Status::RemoteError("mock error");
  });

  int64_t id = -1;
  Status status = cache->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), id);

  EXPECT_TRUE(!status.ok());
  EXPECT_EQ(id, -1);
}

TEST_F(SDKVectorIndexCacheTest, GetVectorIndexByKeyNotOK) {
  std::string index_name = "test";

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GetIndexByNameRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->index_name(), index_name);
    return Status::RemoteError("mock error");
  });

  std::shared_ptr<VectorIndex> index;
  Status status = cache->GetVectorIndexByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index);

  EXPECT_TRUE(!status.ok());
}

TEST_F(SDKVectorIndexCacheTest, GetVectorIndexByIdNotOK) {
  int64_t index_id = 2;

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GetIndexRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->index_id().entity_type(), pb::meta::EntityType::ENTITY_TYPE_INDEX);
    EXPECT_EQ(t_rpc->Request()->index_id().parent_entity_id(), ::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
    EXPECT_EQ(t_rpc->Request()->index_id().entity_id(), index_id);

    return Status::RemoteError("mock error");
  });

  std::shared_ptr<VectorIndex> index;
  Status status = cache->GetVectorIndexById(index_id, index);

  EXPECT_TRUE(!status.ok());
}

TEST_F(SDKVectorIndexCacheTest, GetVectorIndexByKeyOK) {
  std::string index_name = "test";
  std::vector<int64_t> index_and_part_ids{2, 3, 4, 5, 6};
  int64_t index_id = index_and_part_ids[0];
  std::vector<int64_t> range_seperator_ids = {5, 10, 20};
  FlatParam flat_param(1000, dingodb::sdk::MetricType::kL2);

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GetIndexByNameRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->index_name(), index_name);
    FillVectorIndexId(t_rpc->MutableResponse()->mutable_index_definition_with_id()->mutable_index_id(), index_id,
                      schema_id);
    auto* defination = t_rpc->MutableResponse()->mutable_index_definition_with_id()->mutable_index_definition();
    defination->set_name(index_name);
    FillRangePartitionRule(defination->mutable_index_partition(), range_seperator_ids, index_and_part_ids);
    defination->set_replica(3);

    auto* index_parameter = defination->mutable_index_parameter();
    index_parameter->set_index_type(pb::common::IndexType::INDEX_TYPE_VECTOR);
    FillFlatParmeter(index_parameter->mutable_vector_index_parameter(), flat_param);
    return Status::OK();
  });

  {
    std::shared_ptr<VectorIndex> index;
    Status status = cache->GetVectorIndexByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index);

    ASSERT_TRUE(status.ok());
    EXPECT_EQ(index->GetId(), index_id);
    EXPECT_EQ(index->GetName(), index_name);
    EXPECT_FALSE(index->IsStale());
    EXPECT_EQ(index->GetVectorIndexType(), flat_param.Type());
  }

  {
    std::shared_ptr<VectorIndex> index;
    Status status = cache->GetVectorIndexById(index_id, index);

    ASSERT_TRUE(status.ok());
    EXPECT_EQ(index->GetId(), index_id);
    EXPECT_EQ(index->GetName(), index_name);
    EXPECT_FALSE(index->IsStale());
    EXPECT_EQ(index->GetVectorIndexType(), flat_param.Type());
  }

  EXPECT_CALL(*meta_rpc_controller, SyncCall)
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GetIndexByNameRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->index_name(), index_name);
        return Status::RemoteError("mock error");
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GetIndexRpc*>(&rpc);

        EXPECT_EQ(t_rpc->Request()->index_id().entity_type(), pb::meta::EntityType::ENTITY_TYPE_INDEX);
        EXPECT_EQ(t_rpc->Request()->index_id().parent_entity_id(),
                  ::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
        EXPECT_EQ(t_rpc->Request()->index_id().entity_id(), index_id);

        return Status::RemoteError("mock error");
      });

  {
    cache->RemoveVectorIndexByKey(EncodeVectorIndexCacheKey(schema_id, index_name));

    {
      std::shared_ptr<VectorIndex> index;
      Status status = cache->GetVectorIndexByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index);
      EXPECT_TRUE(!status.ok());
    }

    {
      std::shared_ptr<VectorIndex> index;
      Status status = cache->GetVectorIndexById(index_id, index);
      EXPECT_TRUE(!status.ok());
    }
  }
}

TEST_F(SDKVectorIndexCacheTest, GetVectorIndexByIdOK) {
  std::string index_name = "test";
  std::vector<int64_t> index_and_part_ids{2, 3, 4, 5, 6};
  int64_t index_id = index_and_part_ids[0];
  std::vector<int64_t> range_seperator_ids = {5, 10, 20};
  FlatParam flat_param(1000, dingodb::sdk::MetricType::kL2);

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GetIndexRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->index_id().entity_type(), pb::meta::EntityType::ENTITY_TYPE_INDEX);
    EXPECT_EQ(t_rpc->Request()->index_id().parent_entity_id(), ::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
    EXPECT_EQ(t_rpc->Request()->index_id().entity_id(), index_id);

    FillVectorIndexId(t_rpc->MutableResponse()->mutable_index_definition_with_id()->mutable_index_id(), index_id,
                      schema_id);
    auto* defination = t_rpc->MutableResponse()->mutable_index_definition_with_id()->mutable_index_definition();
    defination->set_name(index_name);
    FillRangePartitionRule(defination->mutable_index_partition(), range_seperator_ids, index_and_part_ids);
    defination->set_replica(3);

    auto* index_parameter = defination->mutable_index_parameter();
    index_parameter->set_index_type(pb::common::IndexType::INDEX_TYPE_VECTOR);
    FillFlatParmeter(index_parameter->mutable_vector_index_parameter(), flat_param);
    return Status::OK();
  });

  {
    std::shared_ptr<VectorIndex> index;
    Status status = cache->GetVectorIndexById(index_id, index);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(index->GetId(), index_id);
    EXPECT_EQ(index->GetName(), index_name);
    EXPECT_FALSE(index->IsStale());
    EXPECT_EQ(index->GetVectorIndexType(), flat_param.Type());
  }

  {
    std::shared_ptr<VectorIndex> index;
    Status status = cache->GetVectorIndexByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(index->GetId(), index_id);
    EXPECT_EQ(index->GetName(), index_name);
    EXPECT_FALSE(index->IsStale());
    EXPECT_EQ(index->GetVectorIndexType(), flat_param.Type());
  }

  EXPECT_CALL(*meta_rpc_controller, SyncCall)
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GetIndexRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->index_id().entity_type(), pb::meta::EntityType::ENTITY_TYPE_INDEX);
        EXPECT_EQ(t_rpc->Request()->index_id().parent_entity_id(),
                  ::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
        EXPECT_EQ(t_rpc->Request()->index_id().entity_id(), index_id);

        return Status::RemoteError("mock error");
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GetIndexByNameRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->index_name(), index_name);
        return Status::RemoteError("mock error");
      });

  {
    cache->RemoveVectorIndexById(index_id);

    {
      std::shared_ptr<VectorIndex> index;
      Status status = cache->GetVectorIndexById(index_id, index);
      EXPECT_TRUE(!status.ok());
    }

    {
      std::shared_ptr<VectorIndex> index;
      Status status = cache->GetVectorIndexByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index);
      EXPECT_TRUE(!status.ok());
    }
  }
}

}  // namespace sdk

}  // namespace dingodb
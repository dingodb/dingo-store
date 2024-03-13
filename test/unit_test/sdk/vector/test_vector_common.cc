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

#include "gtest/gtest.h"
#include "sdk/vector/vector_common.h"

namespace dingodb {
namespace sdk {

TEST(VectorCommonTest, TestMetricType2InternalMetricTypePB) {
  EXPECT_EQ(MetricType2InternalMetricTypePB(MetricType::kL2), pb::common::MetricType::METRIC_TYPE_L2);

  EXPECT_EQ(MetricType2InternalMetricTypePB(MetricType::kInnerProduct),
            pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT);

  EXPECT_EQ(MetricType2InternalMetricTypePB(MetricType::kCosine), pb::common::MetricType::METRIC_TYPE_COSINE);
}

TEST(VectorCommonTest, TestInternalMetricTypePB2MetricType) {
  EXPECT_EQ(InternalMetricTypePB2MetricType(pb::common::MetricType::METRIC_TYPE_NONE), MetricType::kNoneMetricType);
  EXPECT_EQ(InternalMetricTypePB2MetricType(pb::common::MetricType::METRIC_TYPE_L2), MetricType::kL2);
  EXPECT_EQ(InternalMetricTypePB2MetricType(pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT),
            MetricType::kInnerProduct);
  EXPECT_EQ(InternalMetricTypePB2MetricType(pb::common::MetricType::METRIC_TYPE_COSINE), MetricType::kCosine);
}

TEST(VectorCommonTest, TestVectorIndexType2InternalVectorIndexTypePB) {
  EXPECT_EQ(VectorIndexType2InternalVectorIndexTypePB(VectorIndexType::kNoneIndexType),
            pb::common::VECTOR_INDEX_TYPE_NONE);

  EXPECT_EQ(VectorIndexType2InternalVectorIndexTypePB(VectorIndexType::kFlat), pb::common::VECTOR_INDEX_TYPE_FLAT);

  EXPECT_EQ(VectorIndexType2InternalVectorIndexTypePB(VectorIndexType::kIvfFlat),
            pb::common::VECTOR_INDEX_TYPE_IVF_FLAT);

  EXPECT_EQ(VectorIndexType2InternalVectorIndexTypePB(VectorIndexType::kIvfPq), pb::common::VECTOR_INDEX_TYPE_IVF_PQ);

  EXPECT_EQ(VectorIndexType2InternalVectorIndexTypePB(VectorIndexType::kHnsw), pb::common::VECTOR_INDEX_TYPE_HNSW);

  EXPECT_EQ(VectorIndexType2InternalVectorIndexTypePB(VectorIndexType::kDiskAnn),
            pb::common::VECTOR_INDEX_TYPE_DISKANN);

  EXPECT_EQ(VectorIndexType2InternalVectorIndexTypePB(VectorIndexType::kBruteForce),
            pb::common::VECTOR_INDEX_TYPE_BRUTEFORCE);
}

TEST(VectorCommonTest, TestInternalVectorIndexTypePB2VectorIndexType) {
  EXPECT_EQ(InternalVectorIndexTypePB2VectorIndexType(pb::common::VECTOR_INDEX_TYPE_NONE),
            VectorIndexType::kNoneIndexType);

  EXPECT_EQ(InternalVectorIndexTypePB2VectorIndexType(pb::common::VECTOR_INDEX_TYPE_FLAT), VectorIndexType::kFlat);

  EXPECT_EQ(InternalVectorIndexTypePB2VectorIndexType(pb::common::VECTOR_INDEX_TYPE_IVF_FLAT),
            VectorIndexType::kIvfFlat);

  EXPECT_EQ(InternalVectorIndexTypePB2VectorIndexType(pb::common::VECTOR_INDEX_TYPE_IVF_PQ), VectorIndexType::kIvfPq);

  EXPECT_EQ(InternalVectorIndexTypePB2VectorIndexType(pb::common::VECTOR_INDEX_TYPE_HNSW), VectorIndexType::kHnsw);

  EXPECT_EQ(InternalVectorIndexTypePB2VectorIndexType(pb::common::VECTOR_INDEX_TYPE_DISKANN),
            VectorIndexType::kDiskAnn);

  EXPECT_EQ(InternalVectorIndexTypePB2VectorIndexType(pb::common::VECTOR_INDEX_TYPE_BRUTEFORCE),
            VectorIndexType::kBruteForce);
}

TEST(VectorCommonTest, TestFillFlatParmeter) {
  pb::common::VectorIndexParameter parameter;
  FlatParam param{128, MetricType::kL2};

  FillFlatParmeter(&parameter, param);

  EXPECT_EQ(parameter.vector_index_type(), pb::common::VECTOR_INDEX_TYPE_FLAT);
  EXPECT_EQ(parameter.flat_parameter().dimension(), param.dimension);
  EXPECT_EQ(parameter.flat_parameter().metric_type(), MetricType2InternalMetricTypePB(param.metric_type));
}

TEST(VectorCommonTest, TestFillIvfFlatParmeter) {
  pb::common::VectorIndexParameter parameter;
  IvfFlatParam param{128, MetricType::kL2};

  FillIvfFlatParmeter(&parameter, param);

  EXPECT_EQ(parameter.vector_index_type(), pb::common::VECTOR_INDEX_TYPE_IVF_FLAT);
  EXPECT_EQ(parameter.ivf_flat_parameter().dimension(), param.dimension);
  EXPECT_EQ(parameter.ivf_flat_parameter().metric_type(), MetricType2InternalMetricTypePB(param.metric_type));
  EXPECT_EQ(parameter.ivf_flat_parameter().ncentroids(), param.ncentroids);
}

TEST(VectorCommonTest, TestFillIvfPqParmeter) {
  pb::common::VectorIndexParameter parameter;
  IvfPqParam param{128, MetricType::kL2};

  FillIvfPqParmeter(&parameter, param);

  EXPECT_EQ(parameter.vector_index_type(), pb::common::VECTOR_INDEX_TYPE_IVF_PQ);
  EXPECT_EQ(parameter.ivf_pq_parameter().dimension(), param.dimension);
  EXPECT_EQ(parameter.ivf_pq_parameter().metric_type(), MetricType2InternalMetricTypePB(param.metric_type));
  EXPECT_EQ(parameter.ivf_pq_parameter().ncentroids(), param.ncentroids);
  EXPECT_EQ(parameter.ivf_pq_parameter().nsubvector(), param.nsubvector);
}

TEST(VectorCommonTest, TestFillHnswParmeter) {
  pb::common::VectorIndexParameter parameter;
  HnswParam param{128, MetricType::kL2, 200};

  FillHnswParmeter(&parameter, param);

  EXPECT_EQ(parameter.vector_index_type(), pb::common::VECTOR_INDEX_TYPE_HNSW);
  EXPECT_EQ(parameter.hnsw_parameter().dimension(), param.dimension);
  EXPECT_EQ(parameter.hnsw_parameter().metric_type(), MetricType2InternalMetricTypePB(param.metric_type));
  EXPECT_EQ(parameter.hnsw_parameter().efconstruction(), param.ef_construction);
  EXPECT_EQ(parameter.hnsw_parameter().nlinks(), param.nlinks);
  EXPECT_EQ(parameter.hnsw_parameter().max_elements(), param.max_elements);
}

TEST(VectorCommonTest, TestFillButeForceParmeter) {
  pb::common::VectorIndexParameter parameter;
  BruteForceParam param{128, MetricType::kL2};

  FillButeForceParmeter(&parameter, param);

  EXPECT_EQ(parameter.vector_index_type(), pb::common::VECTOR_INDEX_TYPE_BRUTEFORCE);
  EXPECT_EQ(parameter.bruteforce_parameter().dimension(), param.dimension);
  EXPECT_EQ(parameter.bruteforce_parameter().metric_type(), MetricType2InternalMetricTypePB(param.metric_type));
}

TEST(VectorCommonTest, TestFillRangePartitionRule) {
  pb::meta::PartitionRule partition_rule;
  std::vector<int64_t> seperator_ids = {10, 20, 30};
  std::vector<int64_t> index_and_part_ids = {1, 2, 3, 4, 5};

  FillRangePartitionRule(&partition_rule, seperator_ids, index_and_part_ids);

  EXPECT_EQ(partition_rule.partitions_size(), seperator_ids.size() + 1);

  for (int i = 0; i < partition_rule.partitions_size(); i++) {
    const auto& part = partition_rule.partitions(i);
    EXPECT_EQ(part.id().entity_id(), index_and_part_ids[i + 1]);
    EXPECT_EQ(part.id().parent_entity_id(), index_and_part_ids[0]);
  }

  for (int i = 0; i < partition_rule.partitions_size(); i++) {
    const auto& part = partition_rule.partitions(i);

    int64_t start_id = VectorCodec::DecodeVectorId(part.range().start_key());
    if (i == 0) {
      EXPECT_EQ(start_id, 0);
    } else {
      EXPECT_EQ(start_id, seperator_ids[i - 1]);
    }

    int64_t end_id = VectorCodec::DecodeVectorId(part.range().end_key());
    EXPECT_EQ(end_id, 0);
  }

  for (int i = 0; i < partition_rule.partitions_size(); i++) {
    const auto& part = partition_rule.partitions(i);
    int64_t start_key_part_id = VectorCodec::DecodePartitionId(part.range().start_key());
    int64_t end_key_part_id = VectorCodec::DecodePartitionId(part.range().end_key());
    EXPECT_EQ(start_key_part_id, index_and_part_ids[i + 1]);
    EXPECT_EQ(end_key_part_id, index_and_part_ids[i + 1] + 1);
  }
}

TEST(VectorCommonTest, TestValueType2InternalValueTypePB) {
  EXPECT_EQ(ValueType2InternalValueTypePB(ValueType::kFloat), pb::common::ValueType::FLOAT);
  EXPECT_EQ(ValueType2InternalValueTypePB(ValueType::kUint8), pb::common::ValueType::UINT8);
}

TEST(VectorCommonTest, TestFillearchFlatParamPB) {
  SearchParam param;
  param.extra_params[SearchExtraParamType::kParallelOnQueries] = 1;

  pb::common::SearchFlatParam pb;
  FillSearchFlatParamPB(&pb, param);

  EXPECT_EQ(pb.parallel_on_queries(), 1);
}

TEST(VectorCommonTest, TestFillsVectorWithIdPB) {
  VectorWithId vector_with_id;
  vector_with_id.id = 100;
  vector_with_id.vector.dimension = 2;
  vector_with_id.vector.value_type = ValueType::kFloat;
  vector_with_id.vector.float_values = {1.0, 2.0};

  pb::common::VectorWithId pb;
  FillVectorWithIdPB(&pb, vector_with_id);

  EXPECT_EQ(pb.id(), 100);
  EXPECT_EQ(pb.vector().dimension(), 2);
  EXPECT_EQ(pb.vector().value_type(), pb::common::ValueType::FLOAT);
  ASSERT_EQ(pb.vector().float_values_size(), 2);
  EXPECT_EQ(pb.vector().float_values(0), 1.0);
  EXPECT_EQ(pb.vector().float_values(1), 2.0);
}

TEST(VectorCommonTest, TestInternalVectorIdPB2VectorWithId) {
  pb::common::VectorWithId pb;
  pb.set_id(100);
  auto* vector_pb = pb.mutable_vector();
  vector_pb->set_dimension(2);
  vector_pb->set_value_type(pb::common::ValueType::FLOAT);
  vector_pb->add_float_values(1.0);
  vector_pb->add_float_values(2.0);

  VectorWithId vector_with_id = InternalVectorIdPB2VectorWithId(pb);

  EXPECT_EQ(vector_with_id.id, 100);
  EXPECT_EQ(vector_with_id.vector.dimension, 2);
  EXPECT_EQ(vector_with_id.vector.value_type, ValueType::kFloat);
  ASSERT_EQ(vector_with_id.vector.float_values.size(), 2);
  EXPECT_EQ(vector_with_id.vector.float_values[0], 1.0);
  EXPECT_EQ(vector_with_id.vector.float_values[1], 2.0);
}

TEST(VectorCommonTest, TestInternalVectorWithDistance2VectorWithDistance) {
  pb::common::VectorWithDistance pb;
  auto* vector_with_id_pb = pb.mutable_vector_with_id();
  vector_with_id_pb->set_id(100);
  auto* vector_pb = vector_with_id_pb->mutable_vector();
  vector_pb->set_dimension(2);
  vector_pb->set_value_type(pb::common::ValueType::FLOAT);
  vector_pb->add_float_values(1.0);
  vector_pb->add_float_values(2.0);
  pb.set_distance(3.0);
  pb.set_metric_type(pb::common::MetricType::METRIC_TYPE_L2);

  VectorWithDistance vector_with_distance = InternalVectorWithDistance2VectorWithDistance(pb);

  EXPECT_EQ(vector_with_distance.vector_data.id, 100);
  EXPECT_EQ(vector_with_distance.vector_data.vector.dimension, 2);
  EXPECT_EQ(vector_with_distance.vector_data.vector.value_type, ValueType::kFloat);
  ASSERT_EQ(vector_with_distance.vector_data.vector.float_values.size(), 2);
  EXPECT_EQ(vector_with_distance.vector_data.vector.float_values[0], 1.0);
  EXPECT_EQ(vector_with_distance.vector_data.vector.float_values[1], 2.0);
  EXPECT_EQ(vector_with_distance.distance, 3.0);
  EXPECT_EQ(vector_with_distance.metric_type, MetricType::kL2);
}

TEST(VectorCommonTest, InternalVectorIndexMetrics2IndexMetricsResult) {
  pb::common::VectorIndexMetrics pb;
  pb.set_vector_index_type(pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
  pb.set_current_count(100);
  pb.set_deleted_count(10);
  pb.set_max_id(200);
  pb.set_min_id(1);
  pb.set_memory_bytes(5000);

  IndexMetricsResult result = InternalVectorIndexMetrics2IndexMetricsResult(pb);

  EXPECT_EQ(result.index_type, VectorIndexType::kFlat);
  EXPECT_EQ(result.count, 100);
  EXPECT_EQ(result.deleted_count, 10);
  EXPECT_EQ(result.max_vector_id, 200);
  EXPECT_EQ(result.min_vector_id, 1);
  EXPECT_EQ(result.memory_bytes, 5000);
}

TEST(VectorCommonTest, TestFillSearchIvfFlatParamPB) {
  SearchParam param;
  param.extra_params[SearchExtraParamType::kNprobe] = 10;
  param.extra_params[SearchExtraParamType::kParallelOnQueries] = 1;

  pb::common::SearchIvfFlatParam pb;
  FillSearchIvfFlatParamPB(&pb, param);

  EXPECT_EQ(pb.nprobe(), 10);
  EXPECT_EQ(pb.parallel_on_queries(), 1);
}

TEST(VectorCommonTest, TestFillSearchIvfPqParamPB) {
  SearchParam param;
  param.extra_params[SearchExtraParamType::kNprobe] = 10;
  param.extra_params[SearchExtraParamType::kParallelOnQueries] = 1;
  param.extra_params[SearchExtraParamType::kRecallNum] = 5;

  pb::common::SearchIvfPqParam pb;
  FillSearchIvfPqParamPB(&pb, param);

  EXPECT_EQ(pb.nprobe(), 10);
  EXPECT_EQ(pb.parallel_on_queries(), 1);
  EXPECT_EQ(pb.recall_num(), 5);
}

TEST(FillSearchHnswParamPBTest, TestFillSearchHnswParamPB) {
  SearchParam param;
  param.extra_params[SearchExtraParamType::kEfSearch] = 20;

  pb::common::SearchHNSWParam pb;
  FillSearchHnswParamPB(&pb, param);

  EXPECT_EQ(pb.efsearch(), 20);
}

}  // namespace sdk
}  // namespace dingodb
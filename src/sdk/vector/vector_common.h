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

#ifndef DINGODB_SDK_VECTOR_COMMON_H_
#define DINGODB_SDK_VECTOR_COMMON_H_

#include <cstdint>

#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"
#include "sdk/vector.h"
#include "vector/codec.h"

namespace dingodb {
namespace sdk {

static const char kVectorPrefix = 'r';

static pb::common::MetricType MetricType2InternalMetricTypePB(MetricType metric_type) {
  switch (metric_type) {
    case MetricType::kL2:
      return pb::common::MetricType::METRIC_TYPE_L2;
    case MetricType::kInnerProduct:
      return pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
    case MetricType::kCosine:
      return pb::common::MetricType::METRIC_TYPE_COSINE;
    default:
      CHECK(false) << "unsupported metric type:" << metric_type;
  }
}

static pb::common::VectorIndexType VectorIndexType2InternalVectorIndexTypePB(VectorIndexType type) {
  switch (type) {
    case VectorIndexType::kNoneIndexType:
      return pb::common::VECTOR_INDEX_TYPE_NONE;
    case VectorIndexType::kFlat:
      return pb::common::VECTOR_INDEX_TYPE_FLAT;
    case VectorIndexType::kIvfFlat:
      return pb::common::VECTOR_INDEX_TYPE_IVF_FLAT;
    case VectorIndexType::kIvfPq:
      return pb::common::VECTOR_INDEX_TYPE_IVF_PQ;
    case VectorIndexType::kHnsw:
      return pb::common::VECTOR_INDEX_TYPE_HNSW;
    case VectorIndexType::kDiskAnn:
      return pb::common::VECTOR_INDEX_TYPE_DISKANN;
    case VectorIndexType::kBruteForce:
      return pb::common::VECTOR_INDEX_TYPE_BRUTEFORCE;
    default:
      CHECK(false) << "unsupported vector index type:" << type;
  }
}

static VectorIndexType InternalVectorIndexTypePB2VectorIndexType(pb::common::VectorIndexType type) {
  switch (type) {
    case pb::common::VECTOR_INDEX_TYPE_NONE:
      return VectorIndexType::kNoneIndexType;
    case pb::common::VECTOR_INDEX_TYPE_FLAT:
      return VectorIndexType::kFlat;
    case pb::common::VECTOR_INDEX_TYPE_IVF_FLAT:
      return VectorIndexType::kIvfFlat;
    case pb::common::VECTOR_INDEX_TYPE_IVF_PQ:
      return VectorIndexType::kIvfPq;
    case pb::common::VECTOR_INDEX_TYPE_HNSW:
      return VectorIndexType::kHnsw;
    case pb::common::VECTOR_INDEX_TYPE_DISKANN:
      return VectorIndexType::kDiskAnn;
    case pb::common::VECTOR_INDEX_TYPE_BRUTEFORCE:
      return VectorIndexType::kBruteForce;
    default:
      CHECK(false) << "unsupported vector index type:" << pb::common::VectorIndexType_Name(type);
  }
}

static void FillRangePartitionRule(pb::meta::PartitionRule* partition_rule, const std::vector<int64_t>& seperator_ids,
                                   const std::vector<int64_t>& index_and_part_ids) {
  auto part_count = seperator_ids.size() + 1;
  CHECK(part_count == index_and_part_ids.size() - 1);

  int64_t new_index_id = index_and_part_ids[0];

  for (int i = 0; i < part_count; i++) {
    auto* part = partition_rule->add_partitions();
    int64_t part_id = index_and_part_ids[i + 1];  // 1st use for index id
    part->mutable_id()->set_entity_id(part_id);
    part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
    part->mutable_id()->set_parent_entity_id(new_index_id);
    std::string start;
    if (i == 0) {
      VectorCodec::EncodeVectorKey(kVectorPrefix, part_id, start);
    } else {
      VectorCodec::EncodeVectorKey(kVectorPrefix, part_id, index_and_part_ids[i - 1], start);
    }
    part->mutable_range()->set_start_key(start);
    std::string end;
    VectorCodec::EncodeVectorKey(kVectorPrefix, part_id + 1, end);
    part->mutable_range()->set_end_key(end);
  }
}

static void FillFlatParmeter(pb::common::VectorIndexParameter* parameter, const FlatParam& param) {
  parameter->set_vector_index_type(pb::common::VECTOR_INDEX_TYPE_FLAT);
  auto* flat = parameter->mutable_flat_parameter();
  flat->set_dimension(param.dimension);
  flat->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
}

static void FillIvfFlatParmeter(pb::common::VectorIndexParameter* parameter, const IvfFlatParam& param) {
  parameter->set_vector_index_type(pb::common::VECTOR_INDEX_TYPE_IVF_FLAT);
  auto* ivf_flat = parameter->mutable_ivf_flat_parameter();
  ivf_flat->set_dimension(param.dimension);
  ivf_flat->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
  ivf_flat->set_ncentroids(param.ncentroids);
}

static void FillIvfPqParmeter(pb::common::VectorIndexParameter* parameter, const IvfPqParam& param) {
  parameter->set_vector_index_type(pb::common::VECTOR_INDEX_TYPE_IVF_PQ);
  auto* ivf_pq = parameter->mutable_ivf_pq_parameter();
  ivf_pq->set_dimension(param.dimension);
  ivf_pq->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
  ivf_pq->set_ncentroids(param.ncentroids);
  ivf_pq->set_nsubvector(param.nsubvector);
  ivf_pq->set_nbits_per_idx(param.nbits_per_idx);
}

static void FillHnswParmeter(pb::common::VectorIndexParameter* parameter, const HnswParam& param) {
  parameter->set_vector_index_type(pb::common::VECTOR_INDEX_TYPE_HNSW);
  auto* hnsw = parameter->mutable_hnsw_parameter();
  hnsw->set_dimension(param.dimension);
  hnsw->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
  hnsw->set_efconstruction(param.ef_construction);
  hnsw->set_nlinks(param.nlinks);
  hnsw->set_max_elements(param.max_elements);
}

static void FillButeForceParmeter(pb::common::VectorIndexParameter* parameter, const BruteForceParam& param) {
  parameter->set_vector_index_type(pb::common::VECTOR_INDEX_TYPE_BRUTEFORCE);
  auto* bruteforce = parameter->mutable_bruteforce_parameter();
  bruteforce->set_dimension(param.dimension);
  bruteforce->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
}

using VectorIndexCacheKey = std::string;

static VectorIndexCacheKey GetVectorIndexCacheKey(int64_t schema_id, const std::string& index_name){
  DCHECK_GT(schema_id, 0);
  DCHECK(!index_name.empty());
  auto buf_size = sizeof(schema_id) + index_name.size();
  char buf[buf_size];
  memcpy(buf, &schema_id, sizeof(schema_id));
  memcpy(buf + sizeof(schema_id), index_name.data(), index_name.size());
  std::string tmp(buf, buf_size);
  return std::move(tmp);
}

static void DecodeVectorIndexCacheKey(const VectorIndexCacheKey& key, int64_t& schema_id, std::string& index_name){
  DCHECK_GE(key.size(), sizeof(schema_id));
  int64_t tmp_schema_id;
  memcpy(&tmp_schema_id, key.data(), sizeof(schema_id));
  schema_id = tmp_schema_id;
  index_name = std::string(key.data() + sizeof(schema_id), key.size() - sizeof(schema_id));
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_COMMON_H_
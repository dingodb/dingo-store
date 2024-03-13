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
    case MetricType::kNoneMetricType:
      return pb::common::MetricType::METRIC_TYPE_NONE;
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

static MetricType InternalMetricTypePB2MetricType(pb::common::MetricType metric_type) {
  switch (metric_type) {
    case pb::common::MetricType::METRIC_TYPE_NONE:
      return MetricType::kNoneMetricType;
    case pb::common::MetricType::METRIC_TYPE_L2:
      return MetricType::kL2;
    case pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT:
      return MetricType::kInnerProduct;
    case pb::common::MetricType::METRIC_TYPE_COSINE:
      return MetricType::kCosine;
    default:
      CHECK(false) << "unsupported metric type:" << pb::common::MetricType_Name(metric_type);
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
      VectorCodec::EncodeVectorKey(kVectorPrefix, part_id, seperator_ids[i - 1], start);
    }
    part->mutable_range()->set_start_key(start);
    std::string end;
    VectorCodec::EncodeVectorKey(kVectorPrefix, part_id + 1, end);
    part->mutable_range()->set_end_key(end);
  }
}

static pb::common::ValueType ValueType2InternalValueTypePB(ValueType value_type) {
  switch (value_type) {
    case ValueType::kFloat:
      return pb::common::ValueType::FLOAT;
    case ValueType::kUint8:
      return pb::common::ValueType::UINT8;
    default:
      CHECK(false) << "unsupported value type:" << value_type;
  }
}

static pb::common::ScalarValue TransformScalarValue(const sdk::ScalarValue& scalar_value) {
  pb::common::ScalarValue result;
  if (scalar_value.type == sdk::ScalarFieldType::kBool) {
    result.set_field_type(pb::common::ScalarFieldType::BOOL);
    for (const auto& field : scalar_value.fields) {
      result.add_fields()->set_bool_data(field.bool_data);
    }
  } else if (scalar_value.type == sdk::ScalarFieldType::kInt8) {
    result.set_field_type(pb::common::ScalarFieldType::INT8);
    for (const auto& field : scalar_value.fields) {
      result.add_fields()->set_int_data(field.int_data);
    }
  } else if (scalar_value.type == sdk::ScalarFieldType::kInt16) {
    result.set_field_type(pb::common::ScalarFieldType::INT16);
    for (const auto& field : scalar_value.fields) {
      result.add_fields()->set_int_data(field.int_data);
    }
  } else if (scalar_value.type == sdk::ScalarFieldType::kInt32) {
    result.set_field_type(pb::common::ScalarFieldType::INT32);
    for (const auto& field : scalar_value.fields) {
      result.add_fields()->set_int_data(field.int_data);
    }
  } else if (scalar_value.type == sdk::ScalarFieldType::kInt64) {
    result.set_field_type(pb::common::ScalarFieldType::INT64);
    for (const auto& field : scalar_value.fields) {
      result.add_fields()->set_long_data(field.long_data);
    }
  } else if (scalar_value.type == sdk::ScalarFieldType::kFloat32) {
    result.set_field_type(pb::common::ScalarFieldType::FLOAT32);
    for (const auto& field : scalar_value.fields) {
      result.add_fields()->set_float_data(field.float_data);
    }
  } else if (scalar_value.type == sdk::ScalarFieldType::kDouble) {
    result.set_field_type(pb::common::ScalarFieldType::DOUBLE);
    for (const auto& field : scalar_value.fields) {
      result.add_fields()->set_double_data(field.double_data);
    }
  } else if (scalar_value.type == sdk::ScalarFieldType::kString) {
    result.set_field_type(pb::common::ScalarFieldType::STRING);
    for (const auto& field : scalar_value.fields) {
      result.add_fields()->set_string_data(field.string_data);
    }
  } else if (scalar_value.type == sdk::ScalarFieldType::kBytes) {
    result.set_field_type(pb::common::ScalarFieldType::BYTES);
    for (const auto& field : scalar_value.fields) {
      result.add_fields()->set_bytes_data(field.bytes_data);
    }
  }

  return result;
}

static void FillVectorWithIdPB(pb::common::VectorWithId* pb, const VectorWithId& vector_with_id, bool with_id = true) {
  if (with_id) {
    pb->set_id(vector_with_id.id);
  }
  auto* vector_pb = pb->mutable_vector();
  const auto& vector = vector_with_id.vector;
  vector_pb->set_dimension(vector.dimension);
  vector_pb->set_value_type(ValueType2InternalValueTypePB(vector.value_type));
  // TODO: support uint
  for (const auto& float_value : vector.float_values) {
    vector_pb->add_float_values(float_value);
  }

  auto* scalar_data = pb->mutable_scalar_data();
  for (const auto& [key, value] : vector_with_id.scalar_data) {
    scalar_data->mutable_scalar_data()->insert({key, TransformScalarValue(value)});
  }
}

static VectorWithId InternalVectorIdPB2VectorWithId(const pb::common::VectorWithId& pb) {
  VectorWithId to_return;
  to_return.id = pb.id();

  const auto& vector_pb = pb.vector();
  to_return.vector.dimension = vector_pb.dimension();
  to_return.vector.value_type = ValueType::kFloat;
  // TODO: support uint
  for (const auto& float_value : vector_pb.float_values()) {
    to_return.vector.float_values.push_back(float_value);
  }
  return std::move(to_return);
}

static VectorWithDistance InternalVectorWithDistance2VectorWithDistance(const pb::common::VectorWithDistance& pb) {
  VectorWithDistance to_return;
  to_return.vector_data = std::move(InternalVectorIdPB2VectorWithId(pb.vector_with_id()));
  to_return.distance = pb.distance();
  to_return.metric_type = InternalMetricTypePB2MetricType(pb.metric_type());
  return std::move(to_return);
}

static IndexMetricsResult InternalVectorIndexMetrics2IndexMetricsResult(const pb::common::VectorIndexMetrics& pb) {
  IndexMetricsResult to_return;
  to_return.index_type = InternalVectorIndexTypePB2VectorIndexType(pb.vector_index_type());
  to_return.count = pb.current_count();
  to_return.deleted_count = pb.deleted_count();
  to_return.max_vector_id = pb.max_id();
  to_return.min_vector_id = pb.min_id();
  to_return.memory_bytes = pb.memory_bytes();

  return to_return;
}

static void FillSearchFlatParamPB(pb::common::SearchFlatParam* pb, const SearchParam& parameter) {
  if (parameter.extra_params.find(SearchExtraParamType::kParallelOnQueries) != parameter.extra_params.end()) {
    pb->set_parallel_on_queries(parameter.extra_params.at(SearchExtraParamType::kParallelOnQueries));
  }
}

static void FillSearchIvfFlatParamPB(pb::common::SearchIvfFlatParam* pb, const SearchParam& parameter) {
  if (parameter.extra_params.find(SearchExtraParamType::kNprobe) != parameter.extra_params.end()) {
    pb->set_nprobe(parameter.extra_params.at(SearchExtraParamType::kNprobe));
  }
  if (parameter.extra_params.find(SearchExtraParamType::kParallelOnQueries) != parameter.extra_params.end()) {
    pb->set_parallel_on_queries(parameter.extra_params.at(SearchExtraParamType::kParallelOnQueries));
  }
}

static void FillSearchIvfPqParamPB(pb::common::SearchIvfPqParam* pb, const SearchParam& parameter) {
  if (parameter.extra_params.find(SearchExtraParamType::kNprobe) != parameter.extra_params.end()) {
    pb->set_nprobe(parameter.extra_params.at(SearchExtraParamType::kNprobe));
  }
  if (parameter.extra_params.find(SearchExtraParamType::kParallelOnQueries) != parameter.extra_params.end()) {
    pb->set_parallel_on_queries(parameter.extra_params.at(SearchExtraParamType::kParallelOnQueries));
  }
  if (parameter.extra_params.find(SearchExtraParamType::kRecallNum) != parameter.extra_params.end()) {
    pb->set_recall_num(parameter.extra_params.at(SearchExtraParamType::kRecallNum));
  }
}

static void FillSearchHnswParamPB(pb::common::SearchHNSWParam* pb, const SearchParam& parameter) {
  if (parameter.extra_params.find(SearchExtraParamType::kEfSearch) != parameter.extra_params.end()) {
    pb->set_efsearch(parameter.extra_params.at(SearchExtraParamType::kEfSearch));
  }
}

// TODO: to support
static void FillSearchDiskAnnParamPB(pb::common::SearchDiskAnnParam* pb, const SearchParam& parameter) {}

static void FillInternalSearchParams(pb::common::VectorSearchParameter* internal_parameter, VectorIndexType type,
                                     const SearchParam& parameter) {
  internal_parameter->set_top_n(parameter.topk);
  internal_parameter->set_without_vector_data(!parameter.with_vector_data);
  internal_parameter->set_without_scalar_data(!parameter.with_scalar_data);
  if (parameter.with_scalar_data) {
    for (const auto& key : parameter.selected_keys) {
      internal_parameter->add_selected_keys(key);
    }
  }

  internal_parameter->set_without_table_data(!parameter.with_table_data);
  internal_parameter->set_enable_range_search(parameter.enable_range_search);
  switch (type) {
    case VectorIndexType::kFlat:
      FillSearchFlatParamPB(internal_parameter->mutable_flat(), parameter);
      break;
    case VectorIndexType::kBruteForce:
      break;
    case VectorIndexType::kIvfFlat:
      FillSearchIvfFlatParamPB(internal_parameter->mutable_ivf_flat(), parameter);
      break;
    case VectorIndexType::kIvfPq:
      FillSearchIvfPqParamPB(internal_parameter->mutable_ivf_pq(), parameter);
      break;
    case VectorIndexType::kHnsw:
      FillSearchHnswParamPB(internal_parameter->mutable_hnsw(), parameter);
      break;
    case VectorIndexType::kDiskAnn:
      FillSearchDiskAnnParamPB(internal_parameter->mutable_diskann(), parameter);
      break;
    default:
      CHECK(false) << "not support index type: " << static_cast<int>(type);
      break;
  }

  switch (parameter.filter_source) {
    case FilterSource::kNoneFilterSource:
      break;
    case FilterSource::kScalarFilter:
      internal_parameter->set_vector_filter(pb::common::VectorFilter::SCALAR_FILTER);
      break;
    case FilterSource::kTableFilter:
      internal_parameter->set_vector_filter(pb::common::VectorFilter::TABLE_FILTER);
      break;
    case FilterSource::kVectorIdFilter:
      internal_parameter->set_vector_filter(pb::common::VectorFilter::VECTOR_ID_FILTER);
      break;
    default:
      CHECK(false) << "not support filter source: " << static_cast<int>(parameter.filter_source);
      break;
  }

  switch (parameter.filter_type) {
    case FilterType::kNoneFilterType:
      break;
    case FilterType::kQueryPre:
      internal_parameter->set_vector_filter_type(pb::common::VectorFilterType::QUERY_PRE);
      break;
    case FilterType::kQueryPost:
      internal_parameter->set_vector_filter_type(pb::common::VectorFilterType::QUERY_POST);
      break;
    default:
      CHECK(false) << "not support filter type: " << static_cast<int>(parameter.filter_type);
      break;
  }

  // TODO: support coprocessor
  //   CoprocessorV2 vector_coprocessor = 23;
  for (const auto id : parameter.vector_ids) {
    internal_parameter->add_vector_ids(id);
  }

  internal_parameter->set_use_brute_force(parameter.use_brute_force);
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_COMMON_H_
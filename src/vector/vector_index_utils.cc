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

#include "vector/vector_index_utils.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "coprocessor/utils.h"
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
#include "document/codec.h"
#include "fmt/format.h"
#endif
#include "faiss/MetricType.h"
#include "faiss/utils/extra_distances-inl.h"
#include "fmt/core.h"
#include "hnswlib/hnswlib.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

DECLARE_bool(dingo_log_switch_scalar_speed_up_detail);

butil::Status VectorIndexUtils::CalcDistanceEntry(
    const ::dingodb::pb::index::VectorCalcDistanceRequest& request,
    std::vector<std::vector<float>>& distances,                             // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,     // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors) {  // NOLINT

  pb::index::AlgorithmType algorithm_type = request.algorithm_type();
  pb::common::MetricType metric_type = request.metric_type();
  const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors = request.op_left_vectors();
  const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors =
      request.op_right_vectors();

  bool is_return_normlize = request.is_return_normlize();

  switch (algorithm_type) {
    case pb::index::ALGORITHM_FAISS: {
      return CalcDistanceByFaiss(metric_type, op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                 result_op_left_vectors, result_op_right_vectors);
    }
    case pb::index::ALGORITHM_HNSWLIB: {
      return CalcDistanceByHnswlib(metric_type, op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                   result_op_left_vectors, result_op_right_vectors);
    }
    case pb::index::AlgorithmType_INT_MIN_SENTINEL_DO_NOT_USE_:
    case pb::index::AlgorithmType_INT_MAX_SENTINEL_DO_NOT_USE_:
    case pb::index::ALGORITHM_NONE: {
      std::string s = fmt::format("invalid algorithm type : ALGORITHM_NONE");
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
    }
  }

  return butil::Status();
}

butil::Status VectorIndexUtils::CalcDistanceCore(
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                           // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,   // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors,  // NOLINT
    DoCalcDistanceFunc do_calc_distance_func) {
  distances.clear();
  distances.resize(op_left_vectors.size());
  size_t i = 0;
  size_t j = 0;

  if (is_return_normlize) {
    result_op_left_vectors.clear();
    result_op_right_vectors.clear();
    result_op_left_vectors.resize(op_left_vectors.size());
    result_op_right_vectors.resize(op_right_vectors.size());
  }

  for (const auto& left_vector : op_left_vectors) {
    std::vector<float> distance;
    distance.resize(op_right_vectors.size());
    ::dingodb::pb::common::Vector result_op_left_vector;
    j = 0;
    for (const auto& right_vector : op_right_vectors) {
      float dis = 0.0f;
      ::dingodb::pb::common::Vector result_op_right_vector;
      do_calc_distance_func(left_vector, right_vector, is_return_normlize,
                            dis,                    // NOLINT
                            result_op_left_vector,  // NOLINT
                            result_op_right_vector);
      distance[j] = dis;
      if (is_return_normlize) result_op_right_vectors[j] = std::move(result_op_right_vector);
      j++;
    }
    distances[i] = std::move(distance);
    if (is_return_normlize) result_op_left_vectors[i] = std::move(result_op_left_vector);
    i++;
  }

  return butil::Status();
}

butil::Status VectorIndexUtils::CalcDistanceByFaiss(
    pb::common::MetricType metric_type,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                          // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,  // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors) {
  switch (metric_type) {
    case pb::common::METRIC_TYPE_L2: {
      return CalcL2DistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                   result_op_left_vectors, result_op_right_vectors);
    }
    case pb::common::METRIC_TYPE_INNER_PRODUCT: {
      return CalcIpDistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                   result_op_left_vectors, result_op_right_vectors);
    }
    case pb::common::METRIC_TYPE_COSINE: {
      return CalcCosineDistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                       result_op_left_vectors, result_op_right_vectors);
    }
    case pb::common::METRIC_TYPE_HAMMING: {
      return CalcHammingDistanceByFaiss(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                        result_op_left_vectors, result_op_right_vectors);
    }
    case pb::common::METRIC_TYPE_NONE:
    case pb::common::MetricType_INT_MIN_SENTINEL_DO_NOT_USE_:
    case pb::common::MetricType_INT_MAX_SENTINEL_DO_NOT_USE_: {
      std::string s = fmt::format("invalid metric_type type : METRIC_TYPE_NONE");
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
    }
  }
  return butil::Status();
}

butil::Status VectorIndexUtils::CalcDistanceByHnswlib(
    pb::common::MetricType metric_type,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                          // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,  // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors) {
  switch (metric_type) {
    case pb::common::METRIC_TYPE_L2: {
      return CalcL2DistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                     result_op_left_vectors, result_op_right_vectors);
    }
    case pb::common::METRIC_TYPE_INNER_PRODUCT: {
      return CalcIpDistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                     result_op_left_vectors, result_op_right_vectors);
    }
    case pb::common::METRIC_TYPE_COSINE: {
      return CalcCosineDistanceByHnswlib(op_left_vectors, op_right_vectors, is_return_normlize, distances,
                                         result_op_left_vectors, result_op_right_vectors);
    }
    case pb::common::METRIC_TYPE_NONE:
    case pb::common::MetricType_INT_MIN_SENTINEL_DO_NOT_USE_:
    case pb::common::MetricType_INT_MAX_SENTINEL_DO_NOT_USE_: {
      std::string s = fmt::format("invalid metric_type type : METRIC_TYPE_NONE");
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
    }
  }
  return butil::Status();
}

butil::Status VectorIndexUtils::CalcL2DistanceByFaiss(
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                          // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,  // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors) {
  return CalcDistanceCore(op_left_vectors, op_right_vectors, is_return_normlize, distances, result_op_left_vectors,
                          result_op_right_vectors, DoCalcL2DistanceByFaiss);
}

butil::Status VectorIndexUtils::CalcIpDistanceByFaiss(
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                           // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,   // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors)  // NOLINT
{                                                                         // NOLINT
  return CalcDistanceCore(op_left_vectors, op_right_vectors, is_return_normlize, distances, result_op_left_vectors,
                          result_op_right_vectors, DoCalcIpDistanceByFaiss);
}

butil::Status VectorIndexUtils::CalcCosineDistanceByFaiss(
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                           // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,   // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors)  // NOLINT
{                                                                         // NOLINT
  return CalcDistanceCore(op_left_vectors, op_right_vectors, is_return_normlize, distances, result_op_left_vectors,
                          result_op_right_vectors, DoCalcCosineDistanceByFaiss);
}

butil::Status VectorIndexUtils::CalcHammingDistanceByFaiss(
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                           // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,   // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors)  // NOLINT
{                                                                         // NOLINT
  return CalcDistanceCore(op_left_vectors, op_right_vectors, is_return_normlize, distances, result_op_left_vectors,
                          result_op_right_vectors, DoCalcHammingDistanceByFaiss);
}

butil::Status VectorIndexUtils::CalcL2DistanceByHnswlib(
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                           // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,   // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors)  // NOLINT
{                                                                         // NOLINT
  return CalcDistanceCore(op_left_vectors, op_right_vectors, is_return_normlize, distances, result_op_left_vectors,
                          result_op_right_vectors, DoCalcL2DistanceByHnswlib);
}

butil::Status VectorIndexUtils::CalcIpDistanceByHnswlib(
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                           // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,   // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors)  // NOLINT
{                                                                         // NOLINT
  return CalcDistanceCore(op_left_vectors, op_right_vectors, is_return_normlize, distances, result_op_left_vectors,
                          result_op_right_vectors, DoCalcIpDistanceByHnswlib);
}

butil::Status VectorIndexUtils::CalcCosineDistanceByHnswlib(
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_left_vectors,
    const google::protobuf::RepeatedPtrField<::dingodb::pb::common::Vector>& op_right_vectors, bool is_return_normlize,
    std::vector<std::vector<float>>& distances,                           // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,   // NOLINT
    std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors)  // NOLINT
{                                                                         // NOLINT
  return CalcDistanceCore(op_left_vectors, op_right_vectors, is_return_normlize, distances, result_op_left_vectors,
                          result_op_right_vectors, DoCalcCosineDistanceByHnswlib);
}

butil::Status VectorIndexUtils::DoCalcL2DistanceByFaiss(const ::dingodb::pb::common::Vector& op_left_vectors,
                                                        const ::dingodb::pb::common::Vector& op_right_vectors,
                                                        bool is_return_normlize,
                                                        float& distance,                                       // NOLINT
                                                        dingodb::pb::common::Vector& result_op_left_vectors,   // NOLINT
                                                        dingodb::pb::common::Vector& result_op_right_vectors)  // NOLINT
{                                                                                                              // NOLINT
  faiss::VectorDistance<faiss::MetricType::METRIC_L2> vector_distance;
  vector_distance.d = op_left_vectors.float_values().size();

  distance = vector_distance(op_left_vectors.float_values().data(), op_right_vectors.float_values().data());

  ResultOpVectorAssignmentWrapper(op_left_vectors, op_right_vectors, is_return_normlize, result_op_left_vectors,
                                  result_op_right_vectors);

  return butil::Status();
}

butil::Status VectorIndexUtils::DoCalcIpDistanceByFaiss(const ::dingodb::pb::common::Vector& op_left_vectors,
                                                        const ::dingodb::pb::common::Vector& op_right_vectors,
                                                        bool is_return_normlize,
                                                        float& distance,                                       // NOLINT
                                                        dingodb::pb::common::Vector& result_op_left_vectors,   // NOLINT
                                                        dingodb::pb::common::Vector& result_op_right_vectors)  // NOLINT
{                                                                                                              // NOLINT
  faiss::VectorDistance<faiss::MetricType::METRIC_INNER_PRODUCT> vector_distance;
  vector_distance.d = op_left_vectors.float_values().size();

  distance = vector_distance(op_left_vectors.float_values().data(), op_right_vectors.float_values().data());

  ResultOpVectorAssignmentWrapper(op_left_vectors, op_right_vectors, is_return_normlize, result_op_left_vectors,
                                  result_op_right_vectors);

  return butil::Status();
}

butil::Status VectorIndexUtils::DoCalcCosineDistanceByFaiss(
    const ::dingodb::pb::common::Vector& op_left_vectors, const ::dingodb::pb::common::Vector& op_right_vectors,
    bool is_return_normlize,
    float& distance,                                       // NOLINT
    dingodb::pb::common::Vector& result_op_left_vectors,   // NOLINT
    dingodb::pb::common::Vector& result_op_right_vectors)  // NOLINT
{                                                          // NOLINT
  faiss::VectorDistance<faiss::MetricType::METRIC_INNER_PRODUCT> vector_distance;
  vector_distance.d = op_left_vectors.float_values().size();

  dingodb::pb::common::Vector tmp_result_op_left_vectors = op_left_vectors;    // NOLINT
  dingodb::pb::common::Vector tmp_result_op_right_vectors = op_right_vectors;  // NOLINT

  NormalizeVectorForFaiss(const_cast<float*>(tmp_result_op_left_vectors.float_values().data()), vector_distance.d);
  NormalizeVectorForFaiss(const_cast<float*>(tmp_result_op_right_vectors.float_values().data()), vector_distance.d);

  distance = vector_distance(tmp_result_op_left_vectors.float_values().data(),
                             tmp_result_op_right_vectors.float_values().data());

  ResultOpVectorAssignmentWrapper(tmp_result_op_left_vectors, tmp_result_op_right_vectors, is_return_normlize,
                                  result_op_left_vectors, result_op_right_vectors);

  return butil::Status();
}

butil::Status VectorIndexUtils::DoCalcHammingDistanceByFaiss(
    const ::dingodb::pb::common::Vector& op_left_vectors, const ::dingodb::pb::common::Vector& op_right_vectors,
    bool is_return_normlize,
    float& distance,                                       // NOLINT
    dingodb::pb::common::Vector& result_op_left_vectors,   // NOLINT
    dingodb::pb::common::Vector& result_op_right_vectors)  // NOLINT
{                                                          // NOLINT
  faiss::VectorDistance<faiss::MetricType::METRIC_HAMMING> vector_distance;
  vector_distance.d = op_left_vectors.binary_values().size();

  std::vector<uint8_t> left_vectors = std::vector<uint8_t>(op_left_vectors.binary_values().size());
  for (int j = 0; j < op_left_vectors.binary_values().size(); j++) {
    left_vectors[j] = static_cast<uint8_t>(op_left_vectors.binary_values()[j][0]);
  }
  std::vector<uint8_t> right_vectors = std::vector<uint8_t>(op_right_vectors.binary_values().size());
  for (int j = 0; j < op_right_vectors.binary_values().size(); j++) {
    right_vectors[j] = static_cast<uint8_t>(op_right_vectors.binary_values()[j][0]);
  }

  distance = vector_distance(left_vectors.data(), right_vectors.data());

  ResultOpBinaryVectorAssignmentWrapper(op_left_vectors, op_right_vectors, is_return_normlize, result_op_left_vectors,
                                        result_op_right_vectors);

  return butil::Status();
}

butil::Status VectorIndexUtils::DoCalcL2DistanceByHnswlib(
    const ::dingodb::pb::common::Vector& op_left_vectors, const ::dingodb::pb::common::Vector& op_right_vectors,
    bool is_return_normlize,
    float& distance,                                       // NOLINT
    dingodb::pb::common::Vector& result_op_left_vectors,   // NOLINT
    dingodb::pb::common::Vector& result_op_right_vectors)  // NOLINT
{                                                          // NOLINT
  hnswlib::L2Space vector_distance(op_left_vectors.float_values().size());

  auto func = vector_distance.get_dist_func();
  distance = func(static_cast<const void*>(op_left_vectors.float_values().data()),
                  static_cast<const void*>(op_right_vectors.float_values().data()),
                  static_cast<const void*>(vector_distance.get_dist_func_param()));

  ResultOpVectorAssignmentWrapper(op_left_vectors, op_right_vectors, is_return_normlize, result_op_left_vectors,
                                  result_op_right_vectors);

  return butil::Status();
}

butil::Status VectorIndexUtils::DoCalcIpDistanceByHnswlib(
    const ::dingodb::pb::common::Vector& op_left_vectors, const ::dingodb::pb::common::Vector& op_right_vectors,
    bool is_return_normlize,
    float& distance,                                       // NOLINT
    dingodb::pb::common::Vector& result_op_left_vectors,   // NOLINT
    dingodb::pb::common::Vector& result_op_right_vectors)  // NOLINT
{                                                          // NOLINT
  hnswlib::InnerProductSpace vector_distance(op_left_vectors.float_values().size());

  auto func = vector_distance.get_dist_func();
  distance = func(static_cast<const void*>(op_left_vectors.float_values().data()),
                  static_cast<const void*>(op_right_vectors.float_values().data()),
                  static_cast<const void*>(vector_distance.get_dist_func_param()));

  ResultOpVectorAssignmentWrapper(op_left_vectors, op_right_vectors, is_return_normlize, result_op_left_vectors,
                                  result_op_right_vectors);

  return butil::Status();
}

butil::Status VectorIndexUtils::DoCalcCosineDistanceByHnswlib(
    const ::dingodb::pb::common::Vector& op_left_vectors, const ::dingodb::pb::common::Vector& op_right_vectors,
    bool is_return_normlize,
    float& distance,                                       // NOLINT
    dingodb::pb::common::Vector& result_op_left_vectors,   // NOLINT
    dingodb::pb::common::Vector& result_op_right_vectors)  // NOLINT
{                                                          // NOLINT
  hnswlib::InnerProductSpace vector_distance(op_left_vectors.float_values().size());

  auto func = vector_distance.get_dist_func();

  dingodb::pb::common::Vector tmp_result_op_left_vectors = op_left_vectors;    // NOLINT
  dingodb::pb::common::Vector tmp_result_op_right_vectors = op_right_vectors;  // NOLINT

  NormalizeVectorForHnsw(static_cast<const float*>(op_left_vectors.float_values().data()),
                         *static_cast<uint32_t*>(vector_distance.get_dist_func_param()),
                         const_cast<float*>(tmp_result_op_left_vectors.float_values().data()));

  NormalizeVectorForHnsw(static_cast<const float*>(op_right_vectors.float_values().data()),
                         *static_cast<uint32_t*>(vector_distance.get_dist_func_param()),
                         const_cast<float*>(tmp_result_op_right_vectors.float_values().data()));

  distance = func(static_cast<const void*>(tmp_result_op_left_vectors.float_values().data()),
                  static_cast<const void*>(tmp_result_op_right_vectors.float_values().data()),
                  static_cast<const void*>(vector_distance.get_dist_func_param()));

  ResultOpVectorAssignmentWrapper(tmp_result_op_left_vectors, tmp_result_op_right_vectors, is_return_normlize,
                                  result_op_left_vectors, result_op_right_vectors);

  return butil::Status();
}

void VectorIndexUtils::ResultOpVectorAssignment(dingodb::pb::common::Vector& result_op_vectors,
                                                const ::dingodb::pb::common::Vector& op_vectors) {
  result_op_vectors = op_vectors;
  result_op_vectors.set_dimension(result_op_vectors.float_values().size());
  result_op_vectors.set_value_type(::dingodb::pb::common::ValueType::FLOAT);
}

void VectorIndexUtils::ResultOpBinaryVectorAssignment(dingodb::pb::common::Vector& result_op_vectors,
                                                      const ::dingodb::pb::common::Vector& op_vectors) {
  result_op_vectors = op_vectors;
  result_op_vectors.set_dimension(result_op_vectors.binary_values().size() * CHAR_BIT);
  result_op_vectors.set_value_type(::dingodb::pb::common::ValueType::UINT8);
}

void VectorIndexUtils::ResultOpVectorAssignmentWrapper(const ::dingodb::pb::common::Vector& op_left_vectors,
                                                       const ::dingodb::pb::common::Vector& op_right_vectors,
                                                       bool is_return_normlize,
                                                       dingodb::pb::common::Vector& result_op_left_vectors,   // NOLINT
                                                       dingodb::pb::common::Vector& result_op_right_vectors)  // NOLINT
{                                                                                                             // NOLINT
  if (is_return_normlize) {
    if (result_op_left_vectors.float_values().empty()) {
      ResultOpVectorAssignment(result_op_left_vectors, op_left_vectors);
    }

    if (result_op_right_vectors.float_values().empty()) {
      ResultOpVectorAssignment(result_op_right_vectors, op_right_vectors);
    }
  }
}

void VectorIndexUtils::ResultOpBinaryVectorAssignmentWrapper(
    const ::dingodb::pb::common::Vector& op_left_vectors, const ::dingodb::pb::common::Vector& op_right_vectors,
    bool is_return_normlize,
    dingodb::pb::common::Vector& result_op_left_vectors,   // NOLINT
    dingodb::pb::common::Vector& result_op_right_vectors)  // NOLINT
{                                                          // NOLINT
  if (is_return_normlize) {
    if (result_op_left_vectors.binary_values().empty()) {
      ResultOpBinaryVectorAssignment(result_op_left_vectors, op_left_vectors);
    }

    if (result_op_right_vectors.binary_values().empty()) {
      ResultOpBinaryVectorAssignment(result_op_right_vectors, op_right_vectors);
    }
  }
}

void VectorIndexUtils::NormalizeVectorForFaiss(float* x, int32_t d) {
  static const float kFloatAccuracy = 0.00001;

  float norm_l2_sqr = faiss::fvec_norm_L2sqr(x, d);

  if (norm_l2_sqr > 0 && std::abs(1.0f - norm_l2_sqr) > kFloatAccuracy) {
    float norm_l2 = std::sqrt(norm_l2_sqr);
    for (int32_t i = 0; i < d; i++) {
      x[i] = x[i] / norm_l2;
    }
  }
}

void VectorIndexUtils::NormalizeVectorForHnsw(const float* data, uint32_t dimension, float* norm_array) {
  float norm = 0.0f;
  for (int i = 0; i < dimension; i++) norm += data[i] * data[i];

  norm = 1.0f / (sqrtf(norm) + 1e-30f);

  for (int i = 0; i < dimension; i++) norm_array[i] = data[i] * norm;
}

butil::Status VectorIndexUtils::CheckVectorDimension(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                     int dimension) {
  for (const auto& vector_with_id : vector_with_ids) {
    if (vector_with_id.vector().value_type() == pb::common::ValueType::FLOAT) {
      if (vector_with_id.vector().float_values().size() != dimension) {
        std::string s =
            fmt::format("vector dimension not match, {} {}", vector_with_id.vector().float_values_size(), dimension);
        return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
      }
    } else if (vector_with_id.vector().value_type() == pb::common::ValueType::UINT8) {
      if (vector_with_id.vector().binary_values().size() != dimension / CHAR_BIT) {
        std::string s = fmt::format("binary vector dimension not match, {} {}/bit",
                                    vector_with_id.vector().binary_values_size(), dimension);
        return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
      }
    } else {
      std::string s =
          fmt::format("invalid value type : {}", pb::common::ValueType_Name(vector_with_id.vector().value_type()));
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
    }
    if (vector_with_id.vector().dimension() != dimension) {
      std::string s = fmt::format("vector dimension not match, {} {}", vector_with_id.vector().dimension(), dimension);
      return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
    }
  }

  return butil::Status::OK();
}

std::unique_ptr<faiss::idx_t[]> VectorIndexUtils::CastVectorId(const std::vector<int64_t>& delete_ids) {
  std::unique_ptr<faiss::idx_t[]> ids = std::make_unique<faiss::idx_t[]>(delete_ids.size());
  for (size_t i = 0; i < delete_ids.size(); ++i) {
    ids.get()[i] = static_cast<faiss::idx_t>(delete_ids[i]);
  }

  return std::move(ids);
}

std::unique_ptr<faiss::idx_t[]> VectorIndexUtils::ExtractVectorId(
    const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  std::unique_ptr<faiss::idx_t[]> ids = std::make_unique<faiss::idx_t[]>(vector_with_ids.size());
  for (size_t i = 0; i < vector_with_ids.size(); ++i) {
    ids[i] = static_cast<faiss::idx_t>(vector_with_ids[i].id());
  }

  return std::move(ids);
}

butil::Status VectorIndexUtils::CheckVectorIdDuplicated(const std::unique_ptr<faiss::idx_t[]>& ids, size_t size) {
  std::unordered_set<faiss::idx_t> id_set;
  for (size_t i = 0; i < size; i++) {
    if (0 != id_set.count(ids[i])) {
      std::string s = fmt::format("vector id duplicated: {}", ids[i]);
      return butil::Status(pb::error::Errno::EVECTOR_ID_DUPLICATED, s);
    }
    id_set.insert(ids[i]);
  }
  return butil::Status::OK();
}

template <typename T>
std::unique_ptr<T[]> VectorIndexUtils::ExtractVectorValue(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                          faiss::idx_t dimension, bool normalize) {
  std::unique_ptr<T[]> vectors = nullptr;
  if constexpr (std::is_same<T, float>::value) {
    vectors = std::make_unique<T[]>(vector_with_ids.size() * dimension);
  } else if constexpr (std::is_same<T, uint8_t>::value) {
    vectors = std::make_unique<T[]>(vector_with_ids.size() * dimension / CHAR_BIT);
  } else {
    std::string s = fmt::format("invalid value typename type");
    DINGO_LOG(ERROR) << s;
    return nullptr;
  }

  for (size_t i = 0; i < vector_with_ids.size(); ++i) {
    if constexpr (std::is_same<T, float>::value) {
      if (vector_with_ids[i].vector().value_type() != pb::common::ValueType::FLOAT) {
        std::string s = fmt::format("template not match vectors value_type : {}",
                                    pb::common::ValueType_Name(vector_with_ids[i].vector().value_type()));
        DINGO_LOG(ERROR) << s;
        return nullptr;
      }
      const auto& vector_value = vector_with_ids[i].vector().float_values();
      memcpy(vectors.get() + i * dimension, vector_value.data(), dimension * sizeof(float));
      if (normalize) {
        VectorIndexUtils::NormalizeVectorForFaiss(reinterpret_cast<float*>(vectors.get()) + i * dimension, dimension);
      }
    } else if constexpr (std::is_same<T, uint8_t>::value) {
      if (vector_with_ids[i].vector().value_type() != pb::common::ValueType::UINT8) {
        std::string s = fmt::format("template not match vectors value_type : {}",
                                    pb::common::ValueType_Name(vector_with_ids[i].vector().value_type()));
        DINGO_LOG(ERROR) << s;
        return nullptr;
      }
      const auto& vector_value = vector_with_ids[i].vector().binary_values();
      for (int j = 0; j < vector_value.size(); j++) {
        vectors.get()[i * dimension / CHAR_BIT + j] = static_cast<uint8_t>(vector_value[j][0]);
      }
    } else {
      std::string s =
          fmt::format("invalid value type : {}", pb::common::ValueType_Name(vector_with_ids[i].vector().value_type()));
      DINGO_LOG(ERROR) << s;
      return nullptr;
    }
  }
  return std::move(vectors);
}

template <typename T>
butil::Status VectorIndexUtils::FillSearchResult(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                 uint32_t topk, const std::vector<T>& distances,
                                                 const std::vector<faiss::idx_t>& labels,
                                                 pb::common::MetricType metric_type, faiss::idx_t dimension,
                                                 std::vector<pb::index::VectorWithDistanceResult>& results) {
  for (size_t row = 0; row < vector_with_ids.size(); ++row) {
    auto& result = results.emplace_back();

    for (size_t i = 0; i < topk; i++) {
      size_t pos = row * topk + i;
      if (labels[pos] < 0) {
        continue;
      }
      auto* vector_with_distance = result.add_vector_with_distances();
      if (metric_type == pb::common::METRIC_TYPE_L2 || metric_type == pb::common::METRIC_TYPE_COSINE ||
          metric_type == pb::common::METRIC_TYPE_INNER_PRODUCT) {
        auto* vector_with_id = vector_with_distance->mutable_vector_with_id();
        vector_with_id->set_id(labels[pos]);
        vector_with_id->mutable_vector()->set_dimension(dimension);
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        if (metric_type == pb::common::MetricType::METRIC_TYPE_COSINE ||
            metric_type == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
          vector_with_distance->set_distance(1.0F - distances[pos]);
        } else {
          vector_with_distance->set_distance(distances[pos]);
        }

        vector_with_distance->set_metric_type(metric_type);
      } else if (metric_type == pb::common::METRIC_TYPE_HAMMING) {
        auto* vector_with_id = vector_with_distance->mutable_vector_with_id();
        vector_with_id->set_id(labels[pos]);
        vector_with_id->mutable_vector()->set_dimension(dimension);
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
        vector_with_distance->set_distance(static_cast<float>(distances[pos]));
        vector_with_distance->set_metric_type(metric_type);
      } else {
        std::string s = fmt::format("invalid metric_type type : {}", pb::common::MetricType_Name(metric_type));
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
      }
    }
  }
  return butil::Status::OK();
}

butil::Status VectorIndexUtils::FillRangeSearchResult(
    const std::unique_ptr<faiss::RangeSearchResult>& range_search_result, pb::common::MetricType metric_type,
    faiss::idx_t dimension, std::vector<pb::index::VectorWithDistanceResult>& results) {
  size_t off = 0;
  for (size_t row = 0; row < range_search_result->nq; ++row) {
    auto& result = results.emplace_back();

    // Don't worry, there will be no memory out of bounds here. Faiss has already processed it.
    size_t total = (range_search_result->lims[row + 1] - range_search_result->lims[row]);
    for (size_t i = 0; i < total; i++) {
      auto* vector_with_distance = result.add_vector_with_distances();

      auto* vector_with_id = vector_with_distance->mutable_vector_with_id();
      vector_with_id->set_id(range_search_result->labels[off + i]);
      vector_with_id->mutable_vector()->set_dimension(dimension);
      if (metric_type == pb::common::MetricType::METRIC_TYPE_HAMMING) {
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::UINT8);
        vector_with_distance->set_distance(range_search_result->distances[off + i]);
      } else if (metric_type == pb::common::MetricType::METRIC_TYPE_COSINE ||
                 metric_type == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        vector_with_distance->set_distance(1.0F - range_search_result->distances[off + i]);
      } else if (metric_type == pb::common::METRIC_TYPE_L2) {
        vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
        vector_with_distance->set_distance(range_search_result->distances[off + i]);
      } else {
        std::string s = fmt::format("invalid metric_type type : {}", pb::common::MetricType_Name(metric_type));
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, s);
      }

      vector_with_distance->set_metric_type(metric_type);
    }
    off += total;
  }

  return butil::Status::OK();
}

butil::Status VectorIndexUtils::CheckVectorIndexParameterCompatibility(const pb::common::VectorIndexParameter& source,
                                                                       const pb::common::VectorIndexParameter& target) {
  if (source.vector_index_type() != target.vector_index_type()) {
    DINGO_LOG(INFO) << "source.vector_index_type() != target.vector_index_type()";
    return butil::Status(pb::error::EMERGE_VECTOR_INDEX_TYPE_NOT_MATCH,
                         "source.vector_index_type() != target.vector_index_type()");
  }

  if (source.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT) {
    const auto& source_flat_parameter = source.flat_parameter();
    const auto& target_flat_parameter = target.flat_parameter();
    if (source_flat_parameter.dimension() != target_flat_parameter.dimension()) {
      DINGO_LOG(INFO) << "source_flat_parameter.dimension() != target_flat_parameter.dimension()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_flat_parameter.dimension() != target_flat_parameter.dimension()");
    }

    if (source_flat_parameter.metric_type() != target_flat_parameter.metric_type()) {
      DINGO_LOG(INFO) << "source_flat_parameter.metric_type() != target_flat_parameter.metric_type()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_flat_parameter.metric_type() != target_flat_parameter.metric_type()");
    }
    return butil::Status::OK();
  } else if (source.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    const auto& source_hnsw_parameter = source.hnsw_parameter();
    const auto& target_hnsw_parameter = target.hnsw_parameter();
    if (source_hnsw_parameter.dimension() != target_hnsw_parameter.dimension()) {
      DINGO_LOG(INFO) << "source_hnsw_parameter.dimension() != target_hnsw_parameter.dimension()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_hnsw_parameter.dimension() != target_hnsw_parameter.dimension()");
    }
    if (source_hnsw_parameter.metric_type() != target_hnsw_parameter.metric_type()) {
      DINGO_LOG(INFO) << "source_hnsw_parameter.metric_type() != target_hnsw_parameter.metric_type()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_hnsw_parameter.metric_type() != target_hnsw_parameter.metric_type()");
    }
    return butil::Status::OK();
  } else if (source.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT) {
    const auto& source_ivf_flat_parameter = source.ivf_flat_parameter();
    const auto& target_ivf_flat_parameter = target.ivf_flat_parameter();
    if (source_ivf_flat_parameter.dimension() != target_ivf_flat_parameter.dimension()) {
      DINGO_LOG(INFO) << "source_ivf_flat_parameter.dimension() != target_ivf_flat_parameter.dimension()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_ivf_flat_parameter.dimension() != target_ivf_flat_parameter.dimension()");
    }
    if (source_ivf_flat_parameter.metric_type() != target_ivf_flat_parameter.metric_type()) {
      DINGO_LOG(INFO) << "source_ivf_flat_parameter.metric_type() != target_ivf_flat_parameter.metric_type()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_ivf_flat_parameter.metric_type() != target_ivf_flat_parameter.metric_type()");
    }
    if (source_ivf_flat_parameter.ncentroids() != target_ivf_flat_parameter.ncentroids()) {
      DINGO_LOG(INFO) << "source_ivf_flat_parameter.ncentroids() != target_ivf_flat_parameter.ncentroids()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_ivf_flat_parameter.ncentroids() != target_ivf_flat_parameter.ncentroids()");
    }
    return butil::Status::OK();
  } else if (source.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ) {
    const auto& source_ivf_pq_parameter = source.ivf_pq_parameter();
    const auto& target_ivf_pq_parameter = target.ivf_pq_parameter();
    if (source_ivf_pq_parameter.dimension() != target_ivf_pq_parameter.dimension()) {
      DINGO_LOG(INFO) << "source_ivf_pq_parameter.dimension() != target_ivf_pq_parameter.dimension()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_ivf_pq_parameter.dimension() != target_ivf_pq_parameter.dimension()");
    }
    if (source_ivf_pq_parameter.metric_type() != target_ivf_pq_parameter.metric_type()) {
      DINGO_LOG(INFO) << "source_ivf_pq_parameter.metric_type() != target_ivf_pq_parameter.metric_type()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_ivf_pq_parameter.metric_type() != target_ivf_pq_parameter.metric_type()");
    }
    if (source_ivf_pq_parameter.ncentroids() != target_ivf_pq_parameter.ncentroids()) {
      DINGO_LOG(INFO) << "source_ivf_pq_parameter.ncentroids() != target_ivf_pq_parameter.ncentroids()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_ivf_pq_parameter.ncentroids() != target_ivf_pq_parameter.ncentroids()");
    }
    return butil::Status::OK();
  } else if (source.vector_index_type() == pb::common::VECTOR_INDEX_TYPE_BINARY_FLAT) {
    const auto& source_binary_flat_parameter = source.binary_flat_parameter();
    const auto& target_binary_flat_parameter = target.binary_flat_parameter();
    if (source_binary_flat_parameter.dimension() != target_binary_flat_parameter.dimension()) {
      DINGO_LOG(INFO) << "source_binary_flat_parameter.dimension() != target_binary_flat_parameter.dimension()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_binary_flat_parameter.dimension() != target_binary_flat_parameter.dimension()");
    }
    if (source_binary_flat_parameter.metric_type() != target_binary_flat_parameter.metric_type()) {
      DINGO_LOG(INFO) << "source_binary_flat_parameter.metric_type() != target_binary_flat_parameter.metric_type()";
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
                           "source_binary_flat_parameter.metric_type() != target_binary_flat_parameter.metric_type()");
    }
    return butil::Status::OK();
  } else if (source.vector_index_type() == pb::common::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT) {
    const auto& source_binary_ivf_flat_parameter = source.binary_ivf_flat_parameter();
    const auto& target_binary_ivf_flat_parameter = target.binary_ivf_flat_parameter();
    if (source_binary_ivf_flat_parameter.dimension() != target_binary_ivf_flat_parameter.dimension()) {
      DINGO_LOG(INFO) << "source_binary_ivf_flat_parameter.dimension() != target_binary_ivf_flat_parameter.dimension()";
      return butil::Status(
          pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
          "source_binary_ivf_flat_parameter.dimension() != target_binary_ivf_flat_parameter.dimension()");
    }
    if (source_binary_ivf_flat_parameter.metric_type() != target_binary_ivf_flat_parameter.metric_type()) {
      DINGO_LOG(INFO)
          << "source_binary_ivf_flat_parameter.metric_type() != target_binary_ivf_flat_parameter.metric_type()";
      return butil::Status(
          pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
          "source_binary_ivf_flat_parameter.metric_type() != target_binary_ivf_flat_parameter.metric_type()");
    }
    if (source_binary_ivf_flat_parameter.ncentroids() != target_binary_ivf_flat_parameter.ncentroids()) {
      DINGO_LOG(INFO)
          << "source_binary_ivf_flat_parameter.ncentroids() != target_binary_ivf_flat_parameter.ncentroids()";
      return butil::Status(
          pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH,
          "source_binary_ivf_flat_parameter.ncentroids() != target_binary_ivf_flat_parameter.ncentroids()");
    }
    return butil::Status::OK();
  } else if (source.vector_index_type() == pb::common::VECTOR_INDEX_TYPE_BRUTEFORCE) {
    const auto& source_bruteforce_parameter = source.bruteforce_parameter();
    const auto& target_bruteforce_parameter = target.bruteforce_parameter();
    if (source_bruteforce_parameter.dimension() != target_bruteforce_parameter.dimension()) {
      std::string s =
          fmt::format("source_bruteforce_parameter.dimension() : {} != target_bruteforce_parameter.dimension() : {}",
                      source_bruteforce_parameter.dimension(), target_bruteforce_parameter.dimension());
      DINGO_LOG(INFO) << s;
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH, s);
    }

    if (source_bruteforce_parameter.metric_type() != target_bruteforce_parameter.metric_type()) {
      std::string s = fmt::format(
          "source_bruteforce_parameter.metric_type() : {} != target_bruteforce_parameter.metric_type() : {}",
          pb::common::MetricType_Name(source_bruteforce_parameter.metric_type()),
          pb::common::MetricType_Name(target_bruteforce_parameter.metric_type()));
      DINGO_LOG(INFO) << s;
      return butil::Status(pb::error::EMERGE_VECTOR_INDEX_PARAMETER_NOT_MATCH, s);
    }
    return butil::Status::OK();

  } else {
    DINGO_LOG(ERROR) << "source.vector_index_type() is not supported";
    return butil::Status(pb::error::EMERGE_VECTOR_INDEX_TYPE_NOT_MATCH, "source.vector_index_type() is not supported");
  }
}

// validate vector index parameter
// in: vector_index_parameter
// return: errno
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
butil::Status VectorIndexUtils::ValidateVectorIndexParameter(
    const pb::common::VectorIndexParameter& vector_index_parameter, bool check_document) {
#else
butil::Status VectorIndexUtils::ValidateVectorIndexParameter(
    const pb::common::VectorIndexParameter& vector_index_parameter) {
#endif
  // check vector_index_parameter.index_type is not NONE
  if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE) {
    DINGO_LOG(ERROR) << "vector_index_parameter.index_type is NONE";
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "vector_index_parameter.index_type is NONE");
  }

  // if vector_index_type is HNSW, check hnsw_parameter is set
  if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    if (!vector_index_parameter.has_hnsw_parameter()) {
      DINGO_LOG(ERROR) << "vector_index_type is HNSW, but hnsw_parameter is not set";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "vector_index_type is HNSW, but hnsw_parameter is not set");
    }

    const auto& hnsw_parameter = vector_index_parameter.hnsw_parameter();

    // check hnsw_parameter.dimension
    // The dimension of the vector space. This parameter is required and must be greater than 0.
    if (hnsw_parameter.dimension() <= 0 || hnsw_parameter.dimension() > Constant::kVectorMaxDimension) {
      DINGO_LOG(ERROR) << "hnsw_parameter.dimension is illegal " << hnsw_parameter.dimension();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "hnsw_parameter.dimension is illegal " + std::to_string(hnsw_parameter.dimension()));
    }

    // check hnsw_parameter.metric_type
    // The distance metric used to calculate the similarity between vectors.
    // This parameter must be METRIC_TYPE_COSINE or METRIC_TYPE_INNER_PRODUCT or METRIC_TYPE_L2.
    if (!(hnsw_parameter.metric_type() == pb::common::METRIC_TYPE_COSINE) &&
        !(hnsw_parameter.metric_type() == pb::common::METRIC_TYPE_INNER_PRODUCT) &&
        !(hnsw_parameter.metric_type() == pb::common::METRIC_TYPE_L2)) {
      DINGO_LOG(ERROR) << "hnsw_parameter.metric_type is illegal " << hnsw_parameter.metric_type();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "hnsw_parameter.metric_type is illegal " + std::to_string(hnsw_parameter.metric_type()));
    }

    // check hnsw_parameter.ef_construction
    // The size of the dynamic list for the nearest neighbors during the construction of the graph. This parameter
    // affects the quality of the graph and the construction time. A larger value leads to a higher quality graph
    // but slower construction time. This parameter must be greater than 0.
    if (hnsw_parameter.efconstruction() <= 0) {
      DINGO_LOG(ERROR) << "hnsw_parameter.ef_construction is illegal " << hnsw_parameter.efconstruction();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "hnsw_parameter.ef_construction is illegal " +
                                                                       std::to_string(hnsw_parameter.efconstruction()));
    }

    // check hnsw_parameter.max_elements
    // The maximum number of elements that can be indexed. This parameter affects the memory usage of the index.
    // This parameter must be equal or greater than 0.
    if (hnsw_parameter.max_elements() < 0) {
      DINGO_LOG(ERROR) << "hnsw_parameter.max_elements is illegal " << hnsw_parameter.max_elements();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "hnsw_parameter.max_elements is illegal " + std::to_string(hnsw_parameter.max_elements()));
    }

    // check hnsw_parameter.nlinks
    // The number of links for each element in the graph. This parameter affects the quality of the graph and the
    // search time. A larger value leads to a higher quality graph but slower search time. This parameter must be
    // greater than 1.
    // In HNSW, there is a equation: mult_ = 1 / log(1.0 * M_), where M_ is the nlists
    // During latter processing, HNSW will malloc memory directly proportional to mult_, so when M_==1,  mult_ is
    // infinity, malloc will fail.
    if (hnsw_parameter.nlinks() <= 1) {
      DINGO_LOG(ERROR) << "hnsw_parameter.nlinks is illegal " << hnsw_parameter.nlinks();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "hnsw_parameter.nlinks is illegal " + std::to_string(hnsw_parameter.nlinks()));
    }
  }

  // if vector_index_type is FLAT, check flat_parameter is set
  if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT) {
    if (!vector_index_parameter.has_flat_parameter()) {
      DINGO_LOG(ERROR) << "vector_index_type is FLAT, but flat_parameter is not set";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "vector_index_type is FLAT, but flat_parameter is not set");
    }

    const auto& flat_parameter = vector_index_parameter.flat_parameter();

    // check flat_parameter.dimension
    if (flat_parameter.dimension() <= 0 || flat_parameter.dimension() > Constant::kVectorMaxDimension) {
      DINGO_LOG(ERROR) << "flat_parameter.dimension is illegal " << flat_parameter.dimension();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "flat_parameter.dimension is illegal " + std::to_string(flat_parameter.dimension()));
    }

    // check flat_parameter.metric_type
    if (!(flat_parameter.metric_type() == pb::common::METRIC_TYPE_COSINE) &&
        !(flat_parameter.metric_type() == pb::common::METRIC_TYPE_INNER_PRODUCT) &&
        !(flat_parameter.metric_type() == pb::common::METRIC_TYPE_L2)) {
      DINGO_LOG(ERROR) << "flat_parameter.metric_type is illegal " << flat_parameter.metric_type();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "flat_parameter.metric_type is illegal " + std::to_string(flat_parameter.metric_type()));
    }
  }

  // if vector_index_type is IVF_FLAT, check ivf_flat_parameter is set
  if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT) {
    if (!vector_index_parameter.has_ivf_flat_parameter()) {
      DINGO_LOG(ERROR) << "vector_index_type is IVF_FLAT, but ivf_flat_parameter is not set";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "vector_index_type is IVF_FLAT, but ivf_flat_parameter is not set");
    }

    const auto& ivf_flat_parameter = vector_index_parameter.ivf_flat_parameter();

    // check ivf_flat_parameter.dimension
    // The dimension of the vectors to be indexed. This parameter must be greater than 0.
    if (ivf_flat_parameter.dimension() <= 0 || ivf_flat_parameter.dimension() > Constant::kVectorMaxDimension) {
      DINGO_LOG(ERROR) << "ivf_flat_parameter.dimension is illegal " << ivf_flat_parameter.dimension();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "ivf_flat_parameter.dimension is illegal " + std::to_string(ivf_flat_parameter.dimension()));
    }

    // check ivf_flat_parameter.metric_type
    if (!(ivf_flat_parameter.metric_type() == pb::common::METRIC_TYPE_COSINE) &&
        !(ivf_flat_parameter.metric_type() == pb::common::METRIC_TYPE_INNER_PRODUCT) &&
        !(ivf_flat_parameter.metric_type() == pb::common::METRIC_TYPE_L2)) {
      DINGO_LOG(ERROR) << "ivf_flat_parameter.metric_type is illegal " << ivf_flat_parameter.metric_type();
      return butil::Status(
          pb::error::Errno::EILLEGAL_PARAMTETERS,
          "ivf_flat_parameter.metric_type is illegal " + std::to_string(ivf_flat_parameter.metric_type()));
    }

    // check ivf_flat_parameter.ncentroids
    // The number of centroids (clusters) used in the product quantization. This parameter affects the memory usage
    // of the index and the accuracy of the search. This parameter must be greater than 0.
    if (ivf_flat_parameter.ncentroids() <= 0) {
      std::string s = fmt::format("ivf_flat_parameter.ncentroids is illegal : {}  default : {}",
                                  ivf_flat_parameter.ncentroids(), Constant::kCreateIvfFlatParamNcentroids);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }
  }

  // if vector_index_type is IVF_PQ, check ivf_pq_parameter is set
  if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ) {
    if (!vector_index_parameter.has_ivf_pq_parameter()) {
      DINGO_LOG(ERROR) << "vector_index_type is IVF_PQ, but ivf_pq_parameter is not set";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "vector_index_type is IVF_PQ, but ivf_pq_parameter is not set");
    }

    const auto& ivf_pq_parameter = vector_index_parameter.ivf_pq_parameter();

    // check ivf_pq_parameter.dimension
    // The dimension of the vectors to be indexed. This parameter must be greater than 0.
    if (ivf_pq_parameter.dimension() <= 0 || ivf_pq_parameter.dimension() > Constant::kVectorMaxDimension) {
      DINGO_LOG(ERROR) << "ivf_pq_parameter.dimension is illegal " << ivf_pq_parameter.dimension();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "ivf_pq_parameter.dimension is illegal " + std::to_string(ivf_pq_parameter.dimension()));
    }

    // check ivf_pq_parameter.metric_type
    // The distance metric used to compute the distance between vectors. This parameter affects the accuracy of the
    // search. This parameter must be METRIC_TYPE_COSINE or METRIC_TYPE_INNER_PRODUCT or METRIC_TYPE_L2.
    if (!(ivf_pq_parameter.metric_type() == pb::common::METRIC_TYPE_COSINE) &&
        !(ivf_pq_parameter.metric_type() == pb::common::METRIC_TYPE_INNER_PRODUCT) &&
        !(ivf_pq_parameter.metric_type() == pb::common::METRIC_TYPE_L2)) {
      DINGO_LOG(ERROR) << "ivf_pq_parameter.metric_type is illegal " << ivf_pq_parameter.metric_type();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "ivf_pq_parameter.metric_type is illegal " + std::to_string(ivf_pq_parameter.metric_type()));
    }

    // check ivf_pq_parameter.nlist
    // The number of inverted lists (buckets) used in the index. This parameter affects the memory usage of the
    // index and the accuracy of the search. This parameter must be greater than 0.
    if (ivf_pq_parameter.ncentroids() <= 0) {
      std::string s = fmt::format("ivf_pq_parameter.ncentroids is illegal : {} default : {}",
                                  ivf_pq_parameter.ncentroids(), Constant::kCreateIvfPqParamNcentroids);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    // check ivf_pq_parameter.nsubvector
    // The number of subvectors used in the product quantization. This parameter affects the memory usage of the
    // index and the accuracy of the search. This parameter must be greater than 0.
    if (ivf_pq_parameter.nsubvector() <= 0) {
      std::string s = fmt::format("ivf_pq_parameter.nsubvector is illegal : {} default : {}",
                                  ivf_pq_parameter.nsubvector(), Constant::kCreateIvfPqParamNsubvector);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    // check ivf_pq_parameter.bucket_init_size
    // The number of bits used to represent each subvector in the index. This parameter affects the memory usage of
    // the index and the accuracy of the search. This parameter must be greater than 0.
    if (ivf_pq_parameter.bucket_init_size() < 0) {
      DINGO_LOG(ERROR) << "ivf_pq_parameter.bucket_init_size is illegal " << ivf_pq_parameter.bucket_init_size();
      return butil::Status(
          pb::error::Errno::EILLEGAL_PARAMTETERS,
          "ivf_pq_parameter.bucket_init_size is illegal " + std::to_string(ivf_pq_parameter.bucket_init_size()));
    }

    // check ivf_pq_parameter.bucket_max_size
    // The maximum number of vectors that can be added to each inverted list (bucket) in the index. This parameter
    // affects the memory usage of the index and the accuracy of the search. This parameter must be greater than 0.
    if (ivf_pq_parameter.bucket_max_size() < 0) {
      DINGO_LOG(ERROR) << "ivf_pq_parameter.bucket_max_size is illegal " << ivf_pq_parameter.bucket_max_size();
      return butil::Status(
          pb::error::Errno::EILLEGAL_PARAMTETERS,
          "ivf_pq_parameter.bucket_max_size is illegal " + std::to_string(ivf_pq_parameter.bucket_max_size()));
    }

    int32_t nsubvector = ivf_pq_parameter.nsubvector();

    uint32_t dimension = ivf_pq_parameter.dimension();
    if (0 != (dimension % nsubvector)) {
      std::string s =
          fmt::format("ivf_pq_parameter vector_index_parameter is illegal, dimension:{} / nsubvector:{} not divisible ",
                      dimension, nsubvector);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    int32_t nbits_per_idx = ivf_pq_parameter.nbits_per_idx();
    if (nbits_per_idx <= 0 || nbits_per_idx > 16) {
      std::string s = fmt::format("ivf_pq_parameter.nbits_per_idx is illegal : {} nbits_per_idx valid : (0, 16]",
                                  ivf_pq_parameter.nbits_per_idx());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }
  }

  butil::Status status = ValidateDiskannParameter(vector_index_parameter);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_FLAT) {
    if (!vector_index_parameter.has_binary_flat_parameter()) {
      DINGO_LOG(ERROR) << "vector_index_type is Binary FLAT, but has_binary_flat_parameter is not set";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "vector_index_type is Binary FLAT, but has_binary_flat_parameter is not set");
    }

    const auto& binary_flat_parameter = vector_index_parameter.binary_flat_parameter();

    // check binary_flat_parameter.dimension
    if (binary_flat_parameter.dimension() <= 0 || binary_flat_parameter.dimension() > Constant::kVectorMaxDimension ||
        binary_flat_parameter.dimension() % 8 != 0) {
      DINGO_LOG(ERROR) << "binary_flat_parameter.dimension is illegal " << binary_flat_parameter.dimension();
      return butil::Status(
          pb::error::Errno::EILLEGAL_PARAMTETERS,
          "binary_flat_parameter.dimension is illegal " + std::to_string(binary_flat_parameter.dimension()));
    }

    // check binary_flat_parameter.metric_type
    if (binary_flat_parameter.metric_type() != pb::common::METRIC_TYPE_HAMMING) {
      DINGO_LOG(ERROR) << "binary_flat_parameter.metric_type is illegal " << binary_flat_parameter.metric_type();
      return butil::Status(
          pb::error::Errno::EILLEGAL_PARAMTETERS,
          "binary_flat_parameter.metric_type is illegal " + std::to_string(binary_flat_parameter.metric_type()));
    }
  }

  if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BINARY_IVF_FLAT) {
    if (!vector_index_parameter.has_binary_ivf_flat_parameter()) {
      DINGO_LOG(ERROR) << "vector_binary_index_type is BINARY_IVF_FLAT, but binary_ivf_flat_parameter is not set";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "vector_index_type is BINARY_IVF_FLAT, but binary_ivf_flat_parameter is not set");
    }

    const auto& binary_ivf_flat_parameter = vector_index_parameter.binary_ivf_flat_parameter();

    // check binary_ivf_flat_parameter.dimension
    // The dimension of the vectors to be indexed. This parameter must be greater than 0.
    if (binary_ivf_flat_parameter.dimension() <= 0 ||
        binary_ivf_flat_parameter.dimension() > Constant::kVectorMaxDimension ||
        binary_ivf_flat_parameter.dimension() % 8 != 0) {
      DINGO_LOG(ERROR) << "binary_ivf_flat_parameter.dimension is illegal " << binary_ivf_flat_parameter.dimension();
      return butil::Status(
          pb::error::Errno::EILLEGAL_PARAMTETERS,
          "binary_ivf_flat_parameter.dimension is illegal " + std::to_string(binary_ivf_flat_parameter.dimension()));
    }

    // check binary_ivf_flat_parameter.metric_type
    // The distance metric used to compute the distance between vectors. This parameter affects the accuracy of the
    // search. This parameter must be METRIC_TYPE_HAMMING.
    if (binary_ivf_flat_parameter.metric_type() != pb::common::METRIC_TYPE_HAMMING) {
      DINGO_LOG(ERROR) << "ivf_flat_parameter.metric_type is illegal " << binary_ivf_flat_parameter.metric_type();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "binary_ivf_flat_parameter.metric_type is illegal " +
                               std::to_string(binary_ivf_flat_parameter.metric_type()));
    }

    // check binary_ivf_flat_parameter.ncentroids
    // The number of centroids (clusters) used in the product quantization. This parameter affects the memory usage
    // of the index and the accuracy of the search. This parameter must be greater than 0.
    if (binary_ivf_flat_parameter.ncentroids() <= 0) {
      std::string s =
          fmt::format("binary_ivf_flat_parameter.ncentroids is illegal : {}  default : {}",
                      binary_ivf_flat_parameter.ncentroids(), Constant::kCreateBinaryIvfFlatParamNcentroids);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }
  }

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  status = VectorIndexUtils::ValidateVectorScalarSchema(vector_index_parameter.scalar_schema());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  if (check_document) {
    return ValidateVectorScalarSchemaWithDocumentSpeedup(vector_index_parameter);
  }
  return butil::Status::OK();
#else
  return VectorIndexUtils::ValidateVectorScalarSchema(vector_index_parameter.scalar_schema());
#endif
}

butil::Status VectorIndexUtils::ValidateDiskannParameter(
    const pb::common::VectorIndexParameter& vector_index_parameter) {
  // if vector_index_type is diskann, check diskann_parameter is set
  if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN) {
    if (!vector_index_parameter.has_diskann_parameter()) {
      DINGO_LOG(ERROR) << "vector_index_type is DISKANN, but diskann_parameter is not set";
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "vector_index_type is DISKANN, but diskann_parameter is not set");
    }

    const auto& diskann_parameter = vector_index_parameter.diskann_parameter();

    // check diskann_parameter.dimension
    // The dimension of the vectors to be indexed. This parameter must be greater than 0.
    if (diskann_parameter.dimension() <= 0 || diskann_parameter.dimension() > Constant::kVectorMaxDimension) {
      DINGO_LOG(ERROR) << "diskann_parameter.dimension is illegal " << diskann_parameter.dimension();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS,
                           "diskann_parameter.dimension is illegal " + std::to_string(diskann_parameter.dimension()));
    }

    // check diskann_parameter.metric_type
    // The distance metric used to compute the distance between vectors. This parameter affects the accuracy of the
    // search. This parameter must be METRIC_TYPE_COSINE or METRIC_TYPE_INNER_PRODUCT or METRIC_TYPE_L2.
    if (!(diskann_parameter.metric_type() == pb::common::METRIC_TYPE_COSINE) &&
        !(diskann_parameter.metric_type() == pb::common::METRIC_TYPE_INNER_PRODUCT) &&
        !(diskann_parameter.metric_type() == pb::common::METRIC_TYPE_L2)) {
      DINGO_LOG(ERROR) << "diskann_parameter.metric_type is illegal " << diskann_parameter.metric_type();
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "diskann_parameter.metric_type is illegal " +
                                                                       std::to_string(diskann_parameter.metric_type()));
    }

    // check diskann_parameter.value_type
    // Note that we currently only support float.
    if (pb::common::ValueType::FLOAT != diskann_parameter.value_type()) {
      std::string s = fmt::format("diskann_parameter.value_type is illegal, only support float. but now is  {} {}",
                                  static_cast<int>(diskann_parameter.value_type()),
                                  pb::common::ValueType_Name(diskann_parameter.value_type()));
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    // check diskann_parameter.max_degree
    // the degree of the graph index, typically between 60 and 150.
    if (diskann_parameter.max_degree() < 60 || diskann_parameter.max_degree() > 150) {
      std::string s = fmt::format("diskann_parameter.max_degree is illegal : {} default : [60, 150]",
                                  diskann_parameter.max_degree());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    // check diskann_parameter.search_list_size
    // Typical values are between 75 to 200.
    if (diskann_parameter.search_list_size() < 75 || diskann_parameter.search_list_size() > 200) {
      std::string s = fmt::format("diskann_parameter.search_list_size is illegal : {} default : [75, 200]",
                                  diskann_parameter.search_list_size());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    // check diskann_parameter.max_degree and diskann_parameter.search_list_size
    if (diskann_parameter.max_degree() > diskann_parameter.search_list_size()) {
      std::string s = fmt::format(
          "diskann_parameter.max_degree : {} is greater than diskann_parameter.search_list_size : {}  is illegal",
          diskann_parameter.max_degree(), diskann_parameter.search_list_size());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    // check diskann_parameter.qd
    // check diskann_parameter.codebook_prefix
    // check diskann_parameter.pq_disk_bytes
    // check diskann_parameter.append_reorder_data
    // check diskann_parameter.build_pq_bytes
    // check diskann_parameter.use_opq
  }

  return butil::Status::OK();
}

butil::Status VectorIndexUtils::ValidateVectorScalarSchema(const pb::common::ScalarSchema& scalar_schema) {
  DINGO_LOG_IF(INFO, FLAGS_dingo_log_switch_scalar_speed_up_detail)
      << fmt::format("scalar_schema : {}", scalar_schema.ShortDebugString());
  std::set<std::string> keys;
  for (const auto& field : scalar_schema.fields()) {
    const std::string& key = field.key();
    if (key.empty()) {
      std::string s = fmt::format("in scalar_schema exist empty key scalar_schema : {}", scalar_schema.DebugString());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    if (0 != keys.count(key)) {
      std::string s =
          fmt::format("in scalar_schema exist repeated key : {} scalar_schema : {}", key, scalar_schema.DebugString());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    pb::common::ScalarFieldType scalar_field_type = field.field_type();
    switch (scalar_field_type) {
      case pb::common::ScalarFieldType::BOOL:
        [[fallthrough]];
      case pb::common::ScalarFieldType::INT8:
        [[fallthrough]];
      case pb::common::ScalarFieldType::INT16:
        [[fallthrough]];
      case pb::common::ScalarFieldType::INT32:
        [[fallthrough]];
      case pb::common::ScalarFieldType::INT64:
        [[fallthrough]];
      case pb::common::ScalarFieldType::FLOAT32:
        [[fallthrough]];
      case pb::common::ScalarFieldType::DOUBLE:
        [[fallthrough]];
      case pb::common::ScalarFieldType::STRING:
        [[fallthrough]];
      case pb::common::ScalarFieldType::BYTES:
        break;
      case pb::common::ScalarFieldType::DATETIME:
        break;
      case pb::common::ScalarFieldType_INT_MIN_SENTINEL_DO_NOT_USE_:
      case pb::common::ScalarFieldType_INT_MAX_SENTINEL_DO_NOT_USE_:
      case pb::common::NONE:
      default: {
        std::string s = fmt::format("invalid scalar field type : {} {}", static_cast<int>(scalar_field_type),
                                    pb::common::ScalarFieldType_Name(scalar_field_type));
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
      }
    }

    keys.insert(key);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexUtils::ValidateVectorScalarData(const pb::common::ScalarSchema& scalar_schema,
                                                         const pb::common::VectorScalardata& vector_scalar_data) {
  for (const auto& field : scalar_schema.fields()) {
    const std::string& key = field.key();
    auto iter = vector_scalar_data.scalar_data().find(key);
    if (field.enable_speed_up()) {
      if (iter == vector_scalar_data.scalar_data().end()) {
        std::string s = fmt::format(
            "in vector scalar data not find key : {}. that is setted speed up must be exist in vector_scalar_data.",
            key);
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
      }
    }

    if (iter != vector_scalar_data.scalar_data().end()) {
      if (field.field_type() != iter->second.field_type()) {
        std::string s = fmt::format("scalar_schema field_type : {} and vector_scalar_data field_type  {} not match.",
                                    pb::common::ScalarFieldType_Name(field.field_type()),
                                    pb::common::ScalarFieldType_Name(iter->second.field_type()));

        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
      }
    }
  }
  return butil::Status::OK();
}

butil::Status VectorIndexUtils::SplitVectorScalarData(
    const pb::common::ScalarSchema& scalar_schema, const pb::common::VectorScalardata& vector_scalar_data,
    std::vector<std::pair<std::string, pb::common::ScalarValue>>& scalar_key_value_pairs) {
  for (const auto& field : scalar_schema.fields()) {
    if (field.enable_speed_up()) {
      const std::string& key = field.key();
      auto iter = vector_scalar_data.scalar_data().find(key);
      if (iter != vector_scalar_data.scalar_data().end()) {
        scalar_key_value_pairs.push_back({iter->first, iter->second});
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexUtils::IsNeedToScanKeySpeedUpCF(const pb::common::ScalarSchema& scalar_schema,
                                                         const pb::common::CoprocessorV2& coprocessor_v2,
                                                         bool& is_need) {
  if (scalar_schema.fields().empty()) {
    is_need = false;
    return butil::Status::OK();
  }

  // not find speed up key.
  auto iter = std::find_if(scalar_schema.fields().begin(), scalar_schema.fields().end(),
                           [](const pb::common::ScalarSchemaItem& item) { return item.enable_speed_up(); });
  if (iter == scalar_schema.fields().end()) {
    is_need = false;
    return butil::Status::OK();
  }

  if (coprocessor_v2.original_schema().schema().empty()) {
    is_need = false;
    return butil::Status::OK();
  }

  if (coprocessor_v2.selection_columns().empty()) {
    is_need = false;
    return butil::Status::OK();
  }

  is_need = true;
  butil::Status status;
  for (int schema_member_index : coprocessor_v2.selection_columns()) {
    google::protobuf::internal::RepeatedPtrIterator<const dingodb::pb::common::Schema> iter_schema;
    status = Utils::FindSchemaInOriginalSchemaBySelectionColumnsIndex(coprocessor_v2, schema_member_index, iter_schema);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    const std::string& name = iter_schema->name();
    if (BAIDU_UNLIKELY(name.empty())) {
      std::string s = fmt::format("CoprocessorV2.original_schema.schema.name empty. not support. original_schema : {}",
                                  coprocessor_v2.original_schema().DebugString());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    auto iter_scalar = std::find_if(
        scalar_schema.fields().begin(), scalar_schema.fields().end(),
        [&name](const pb::common::ScalarSchemaItem& item) { return item.enable_speed_up() && name == item.key(); });
    if (iter_scalar == scalar_schema.fields().end()) {
      is_need = false;
      break;
    }

    status = Utils::CompareCoprocessorSchemaAndScalarSchema(iter_schema, iter_scalar);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexUtils::IsNeedToScanKeySpeedUpCF(const pb::common::ScalarSchema& scalar_schema,
                                                         const pb::common::VectorScalardata& vector_scalar_data,
                                                         bool& is_need) {  // NOLINT
  if (scalar_schema.fields().empty()) {
    is_need = false;
    return butil::Status::OK();
  }

  // not find speed up key.
  auto iter = std::find_if(scalar_schema.fields().begin(), scalar_schema.fields().end(),
                           [](const pb::common::ScalarSchemaItem& item) { return item.enable_speed_up(); });
  if (iter == scalar_schema.fields().end()) {
    is_need = false;
    return butil::Status::OK();
  }

  if (vector_scalar_data.scalar_data().empty()) {
    is_need = false;
    return butil::Status::OK();
  }

  is_need = true;
  for (const auto& [key, _] : vector_scalar_data.scalar_data()) {
    if (key.empty()) {
      std::string s = fmt::format("VectorScalardata.scalar_data.key empty. not support.");
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    // c++20 fix this bug.
    const auto& key2 = key;
    auto iter = std::find_if(
        scalar_schema.fields().begin(), scalar_schema.fields().end(),
        [&key2](const pb::common::ScalarSchemaItem& item) { return item.enable_speed_up() && key2 == item.key(); });
    if (iter == scalar_schema.fields().end()) {
      is_need = false;
      break;
    }
  }

  return butil::Status::OK();
}

template std::unique_ptr<float[]> VectorIndexUtils::ExtractVectorValue<float>(
    const std::vector<pb::common::VectorWithId>&, long, bool);

template std::unique_ptr<uint8_t[]> VectorIndexUtils::ExtractVectorValue<uint8_t>(
    const std::vector<pb::common::VectorWithId>&, long, bool);

template butil::Status VectorIndexUtils::FillSearchResult<float>(const std::vector<pb::common::VectorWithId>&,
                                                                 unsigned int, const std::vector<float>&,
                                                                 const std::vector<long>&, pb::common::MetricType, long,
                                                                 std::vector<pb::index::VectorWithDistanceResult>&);

template butil::Status VectorIndexUtils::FillSearchResult<int>(const std::vector<pb::common::VectorWithId>&,
                                                               unsigned int, const std::vector<int>&,
                                                               const std::vector<long>&, pb::common::MetricType, long,
                                                               std::vector<pb::index::VectorWithDistanceResult>&);

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
butil::Status VectorIndexUtils::AutoFillScalarSchemaWithDocumentSpeedup(
    pb::common::VectorIndexParameter& vector_index_parameter) {
  bool enable_scalar_speed_up_with_document = vector_index_parameter.enable_scalar_speed_up_with_document();
  if (!enable_scalar_speed_up_with_document) {
    return butil::Status::OK();
  }

  const auto& scalar_schema = vector_index_parameter.scalar_schema();

  ::dingodb::pb::common::DocumentIndexParameter* document_index_parameter =
      vector_index_parameter.mutable_document_index_parameter();

  // file document scalar_schema
  if (!document_index_parameter->has_scalar_schema()) {
    auto* document_scalar_schema = document_index_parameter->mutable_scalar_schema();
    for (const auto& field : scalar_schema.fields()) {
      if (field.enable_speed_up()) {
        pb::common::ScalarSchemaItem* new_field = document_scalar_schema->add_fields();
        new_field->CopyFrom(field);
      }
    }
  }

  // file json_parameter
  if (vector_index_parameter.document_index_parameter().json_parameter().empty()) {
    // create json_parameter
    // generate default json_parameter
    std::string error_message, json_parameter;
    std::map<std::string, dingodb::TokenizerType> column_tokenizer_parameter;

    for (const auto& field : vector_index_parameter.document_index_parameter().scalar_schema().fields()) {
      const auto field_type = field.field_type();

      switch (field_type) {
        case pb::common::ScalarFieldType::BOOL: {
          column_tokenizer_parameter[field.key()] = dingodb::TokenizerType::kTokenizerTypeBool;
          break;
        }
        case pb::common::ScalarFieldType::INT64: {
          column_tokenizer_parameter[field.key()] = dingodb::TokenizerType::kTokenizerTypeI64;
          break;
        }

        case pb::common::ScalarFieldType::DOUBLE: {
          column_tokenizer_parameter[field.key()] = dingodb::TokenizerType::kTokenizerTypeF64;
          break;
        }
        case pb::common::ScalarFieldType::STRING: {
          column_tokenizer_parameter[field.key()] = dingodb::TokenizerType::kTokenizerTypeText;
          break;
        }
        case pb::common::ScalarFieldType::DATETIME: {
          column_tokenizer_parameter[field.key()] = dingodb::TokenizerType::kTokenizerTypeDateTime;
          break;
        }
        case pb::common::ScalarFieldType::BYTES: {
          column_tokenizer_parameter[field.key()] = dingodb::TokenizerType::kTokenizerTypeBytes;
          break;
        }
        case pb::common::ScalarFieldType::NONE:
          [[fallthrough]];
        case pb::common::ScalarFieldType::INT8:
          [[fallthrough]];
        case pb::common::ScalarFieldType::INT16:
          [[fallthrough]];
        case pb::common::ScalarFieldType::INT32:
          [[fallthrough]];
        case pb::common::ScalarFieldType::FLOAT32:
          [[fallthrough]];
        case pb::common::ScalarFieldType_INT_MIN_SENTINEL_DO_NOT_USE_:
          [[fallthrough]];
        case pb::common::ScalarFieldType_INT_MAX_SENTINEL_DO_NOT_USE_:
          [[fallthrough]];
        default: {
          error_message =
              fmt::format("unsupported field type : {}", ::dingodb::pb::common::ScalarFieldType_Name(field_type));
          DINGO_LOG(ERROR) << error_message;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_message);
        }
      }
    }

    auto json_valid = DocumentCodec::GenDefaultTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
        column_tokenizer_parameter, json_parameter, error_message);

    if (!json_valid) {
      std::string s = fmt::format("gen default json_parameter is illegal, error_message: {}", error_message);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    vector_index_parameter.mutable_document_index_parameter()->set_json_parameter(json_parameter);
  }

  return butil::Status::OK();
}

butil::Status VectorIndexUtils::ValidateVectorScalarSchemaWithDocumentSpeedup(
    const pb::common::VectorIndexParameter& vector_index_parameter) {
  const auto& scalar_schema = vector_index_parameter.scalar_schema();

  bool enable_scalar_speed_up_with_document = vector_index_parameter.enable_scalar_speed_up_with_document();
  if (!enable_scalar_speed_up_with_document) {
    if (vector_index_parameter.has_document_index_parameter()) {
      std::string s = fmt::format(
          "vector_index_parameter enable_scalar_speed_up_with_document is false, but set document_index_parameter. "
          "vector_index_parameter: {}",
          vector_index_parameter.DebugString());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }
    return butil::Status::OK();
  }

  // not support diskann and bruteforce
  if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN ||
      vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_BRUTEFORCE) {
    std::string s = fmt::format(
        "vector_index_parameter enable_scalar_speed_up_with_document is true, but vector_index_type is {} not "
        "support. vector_index_parameter: {}",
        pb::common::VectorIndexType_Name(vector_index_parameter.vector_index_type()),
        vector_index_parameter.DebugString());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  // collect speed up fields.
  std::vector<pb::common::ScalarSchemaItem> scalar_schema_speed_up_fields;
  for (const auto& field : scalar_schema.fields()) {
    if (field.enable_speed_up()) {
      scalar_schema_speed_up_fields.push_back(field);
    }
  }

  if (!vector_index_parameter.has_document_index_parameter()) {
    std::string s = fmt::format(
        "vector_index_parameter enable_scalar_speed_up_with_document is true, but not set document_index_parameter. "
        "vector_index_parameter: {}",
        vector_index_parameter.DebugString());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  if (!vector_index_parameter.document_index_parameter().has_scalar_schema()) {
    std::string s = fmt::format(
        "vector_index_parameter enable_scalar_speed_up_with_document is true, but not set "
        "document_index_parameter.scalar_schema. vector_index_parameter: {}",
        vector_index_parameter.DebugString());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  if (vector_index_parameter.document_index_parameter().json_parameter().empty()) {
    std::string s = fmt::format(
        "vector_index_parameter enable_scalar_speed_up_with_document is true, but not set "
        "document_index_parameter.json_parameter. vector_index_parameter: {}",
        vector_index_parameter.DebugString());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  if (scalar_schema_speed_up_fields.size() !=
      vector_index_parameter.document_index_parameter().scalar_schema().fields().size()) {
    std::string s = fmt::format(
        "vector_index_parameter enable_scalar_speed_up_with_document is true, but "
        "scalar_schema speed up fields size : {} != document_index_parameter.scalar_schema.fields size : {}. "
        "vector_index_parameter: "
        "{}",
        scalar_schema_speed_up_fields.size(),
        vector_index_parameter.document_index_parameter().scalar_schema().fields().size(),
        vector_index_parameter.DebugString());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  if (0 == scalar_schema_speed_up_fields.size()) {
    std::string s = fmt::format(
        "vector_index_parameter enable_scalar_speed_up_with_document is true, but not find speed up fields. "
        "vector_index_parameter: {}",
        vector_index_parameter.DebugString());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  if (0 == vector_index_parameter.document_index_parameter().scalar_schema().fields().size()) {
    std::string s = fmt::format(
        "vector_index_parameter enable_scalar_speed_up_with_document is true, but "
        "document_index_parameter.scalar_schema.fields is empty. vector_index_parameter: {}",
        vector_index_parameter.DebugString());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  for (const auto& field : vector_index_parameter.document_index_parameter().scalar_schema().fields()) {
    const std::string& key = field.key();

    pb::common::ScalarFieldType scalar_field_type = field.field_type();

    auto it = std::find_if(scalar_schema_speed_up_fields.begin(), scalar_schema_speed_up_fields.end(),
                           [&key](const pb::common::ScalarSchemaItem& item) { return item.key() == key; });
    if (it == scalar_schema_speed_up_fields.end()) {
      std::string s = fmt::format(
          "vector_index_parameter enable_scalar_speed_up_with_document is true, but "
          "document_index_parameter.scalar_schema.fields key : {} not exist in vector_index_parameter.scalar_schema. "
          "vector_index_parameter: {}",
          key, vector_index_parameter.DebugString());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
    }

    switch (scalar_field_type) {
      case pb::common::ScalarFieldType::BOOL: {
        if (it->field_type() != pb::common::ScalarFieldType::BOOL) {
          std::string s = fmt::format(
              "vector_index_parameter enable_scalar_speed_up_with_document is true, but "
              "document_index_parameter.scalar_schema.fields key : {} field_type : {} not match "
              "vector_index_parameter "
              "scalar_schema field_type : {}. vector_index_parameter: {}",
              key, pb::common::ScalarFieldType_Name(scalar_field_type),
              pb::common::ScalarFieldType_Name(it->field_type()), vector_index_parameter.DebugString());
          DINGO_LOG(ERROR) << s;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
        }
        break;
      }
      case pb::common::ScalarFieldType::INT64: {
        if (it->field_type() != pb::common::ScalarFieldType::INT64) {
          std::string s = fmt::format(
              "vector_index_parameter enable_scalar_speed_up_with_document is true, but "
              "document_index_parameter.scalar_schema.fields key : {} field_type : {} not match "
              "vector_index_parameter "
              "scalar_schema field_type : {}. vector_index_parameter: {}",
              key, pb::common::ScalarFieldType_Name(scalar_field_type),
              pb::common::ScalarFieldType_Name(it->field_type()), vector_index_parameter.DebugString());
          DINGO_LOG(ERROR) << s;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
        }
        break;
      }
      case pb::common::ScalarFieldType::DOUBLE: {
        if (it->field_type() != pb::common::ScalarFieldType::DOUBLE) {
          std::string s = fmt::format(
              "vector_index_parameter enable_scalar_speed_up_with_document is true, but "
              "document_index_parameter.scalar_schema.fields key : {} field_type : {} not match "
              "vector_index_parameter "
              "scalar_schema field_type : {}. vector_index_parameter: {}",
              key, pb::common::ScalarFieldType_Name(scalar_field_type),
              pb::common::ScalarFieldType_Name(it->field_type()), vector_index_parameter.DebugString());
          DINGO_LOG(ERROR) << s;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
        }
        break;
      }
      case pb::common::ScalarFieldType::STRING: {
        if (it->field_type() != pb::common::ScalarFieldType::STRING) {
          std::string s = fmt::format(
              "vector_index_parameter enable_scalar_speed_up_with_document is true, but "
              "document_index_parameter.scalar_schema.fields key : {} field_type : {} not match "
              "vector_index_parameter "
              "scalar_schema field_type : {}. vector_index_parameter: {}",
              key, pb::common::ScalarFieldType_Name(scalar_field_type),
              pb::common::ScalarFieldType_Name(it->field_type()), vector_index_parameter.DebugString());
          DINGO_LOG(ERROR) << s;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
        }
        break;
      }
      case pb::common::ScalarFieldType::BYTES: {
        if (it->field_type() != pb::common::ScalarFieldType::BYTES) {
          std::string s = fmt::format(
              "vector_index_parameter enable_scalar_speed_up_with_document is true, but "
              "document_index_parameter.scalar_schema.fields key : {} field_type : {} not match "
              "vector_index_parameter "
              "scalar_schema field_type : {}. vector_index_parameter: {}",
              key, pb::common::ScalarFieldType_Name(scalar_field_type),
              pb::common::ScalarFieldType_Name(it->field_type()), vector_index_parameter.DebugString());
          DINGO_LOG(ERROR) << s;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
        }
        break;
      }
      case pb::common::ScalarFieldType::DATETIME: {
        if (it->field_type() != pb::common::ScalarFieldType::DATETIME) {
          std::string s = fmt::format(
              "vector_index_parameter enable_scalar_speed_up_with_document is true, but "
              "document_index_parameter.scalar_schema.fields key : {} field_type : {} not match "
              "vector_index_parameter "
              "scalar_schema field_type : {}. vector_index_parameter: {}",
              key, pb::common::ScalarFieldType_Name(scalar_field_type),
              pb::common::ScalarFieldType_Name(it->field_type()), vector_index_parameter.DebugString());
          DINGO_LOG(ERROR) << s;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
        }
        break;
      }
      case pb::common::ScalarFieldType::NONE:
        [[fallthrough]];
      case pb::common::ScalarFieldType::INT8:
        [[fallthrough]];
      case pb::common::ScalarFieldType::INT16:
        [[fallthrough]];
      case pb::common::ScalarFieldType::INT32:
        [[fallthrough]];
      case pb::common::ScalarFieldType::FLOAT32:
        [[fallthrough]];
      case pb::common::ScalarFieldType::ScalarFieldType_INT_MIN_SENTINEL_DO_NOT_USE_:
        [[fallthrough]];
      case pb::common::ScalarFieldType::ScalarFieldType_INT_MAX_SENTINEL_DO_NOT_USE_:
        [[fallthrough]];
      default: {
        std::string s = fmt::format("invalid scalar field type : {} {}", static_cast<int>(scalar_field_type),
                                    pb::common::ScalarFieldType_Name(scalar_field_type));
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
      }
    }
  }

  return ValidateVectorWithDocumentSpeedupJsonParameter(
      vector_index_parameter.document_index_parameter().scalar_schema(),
      vector_index_parameter.document_index_parameter().json_parameter());
}

butil::Status VectorIndexUtils::ValidateVectorWithDocumentSpeedupJsonParameter(
    const pb::common::ScalarSchema& scalar_schema, const std::string& json_parameter) {
  // validate json parameter

  std::map<std::string, TokenizerType> column_tokenizer_parameter;
  std::string error_message;

  auto json_valid = DocumentCodec::IsValidTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      json_parameter, column_tokenizer_parameter, error_message);

  if (!json_valid) {
    DINGO_LOG(ERROR) << "json_parameter is illegal, error_message:" << error_message;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_message);
  }

  if (!column_tokenizer_parameter.empty() && scalar_schema.fields().empty()) {
    std::string error_msg = "json_parameter is not consistent with scalar_schema, scalar_schema is empty";
    DINGO_LOG(ERROR) << error_msg;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_msg);
  }

  // check if json_parameter is consistent with scalar_schema
  for (const auto& field : scalar_schema.fields()) {
    // compatible executor
    std::string lower_str = field.key();
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                   [](char const& c) { return std::tolower(c); });

    if (column_tokenizer_parameter.find(field.key()) == column_tokenizer_parameter.end()) {
      if (column_tokenizer_parameter.find(lower_str) == column_tokenizer_parameter.end()) {
        std::string error_msg =
            fmt::format("json_parameter is not consistent with scalar_schema, field_name:{}", field.key());
        DINGO_LOG(ERROR) << error_msg;
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_msg);
      }
    }

    if ((column_tokenizer_parameter[field.key()] == dingodb::TokenizerType::kTokenizerTypeUnknown) &&
        (column_tokenizer_parameter[lower_str] == dingodb::TokenizerType::kTokenizerTypeUnknown)) {
      std::string error_msg =
          fmt::format("json_parameter is not consistent with scalar_schema, field_name:{}, type unknown", field.key());
      DINGO_LOG(ERROR) << error_msg;
      return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_msg);
    }

    switch (field.field_type()) {
      case pb::common::ScalarFieldType::BYTES:
        if ((column_tokenizer_parameter[field.key()] != dingodb::TokenizerType::kTokenizerTypeBytes) &&
            (column_tokenizer_parameter[lower_str] != dingodb::TokenizerType::kTokenizerTypeBytes)) {
          std::string error_msg = fmt::format(
              "json_parameter is not consistent with scalar_schema, field_name:{}, field_type:BYTES vs "
              "{} ",
              field.key(), DocumentCodec::GetTokenizerTypeString(column_tokenizer_parameter[field.key()]));
          DINGO_LOG(ERROR) << error_msg;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_msg);
        }
        break;
      case pb::common::ScalarFieldType::STRING:
        if ((column_tokenizer_parameter[field.key()] != dingodb::TokenizerType::kTokenizerTypeText) &&
            (column_tokenizer_parameter[lower_str] != dingodb::TokenizerType::kTokenizerTypeText)) {
          std::string error_msg =
              fmt::format("json_parameter is not consistent with scalar_schema, field_name:{}, field_type:STRING vs {}",
                          field.key(), DocumentCodec::GetTokenizerTypeString(column_tokenizer_parameter[field.key()]));
          DINGO_LOG(ERROR) << error_msg;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_msg);
        }
        break;
      case pb::common::ScalarFieldType::INT64:
        if ((column_tokenizer_parameter[field.key()] != dingodb::TokenizerType::kTokenizerTypeI64) &&
            (column_tokenizer_parameter[lower_str] != dingodb::TokenizerType::kTokenizerTypeI64)) {
          std::string error_msg =
              fmt::format("json_parameter is not consistent with scalar_schema, field_name:{}, field_type:INT64 vs {}",
                          field.key(), DocumentCodec::GetTokenizerTypeString(column_tokenizer_parameter[field.key()]));
          DINGO_LOG(ERROR) << error_msg;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_msg);
        }
        break;
      case pb::common::ScalarFieldType::DOUBLE:
        if ((column_tokenizer_parameter[field.key()] != dingodb::TokenizerType::kTokenizerTypeF64) &&
            (column_tokenizer_parameter[lower_str] != dingodb::TokenizerType::kTokenizerTypeF64)) {
          std::string error_msg =
              fmt::format("json_parameter is not consistent with scalar_schema, field_name:{}, field_type:DOUBLE vs {}",
                          field.key(), DocumentCodec::GetTokenizerTypeString(column_tokenizer_parameter[field.key()]));
          DINGO_LOG(ERROR) << error_msg;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_msg);
        }
        break;
      case pb::common::ScalarFieldType::DATETIME:
        if ((column_tokenizer_parameter[field.key()] != dingodb::TokenizerType::kTokenizerTypeDateTime) &&
            (column_tokenizer_parameter[lower_str] != dingodb::TokenizerType::kTokenizerTypeDateTime)) {
          std::string error_msg = fmt::format(
              "json_parameter is not consistent with scalar_schema, field_name:{}, field_type:datetime vs {}",
              field.key(), DocumentCodec::GetTokenizerTypeString(column_tokenizer_parameter[field.key()]));
          DINGO_LOG(ERROR) << error_msg;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_msg);
        }
        break;
      case pb::common::ScalarFieldType::BOOL:
        if ((column_tokenizer_parameter[field.key()] != dingodb::TokenizerType::kTokenizerTypeBool) &&
            (column_tokenizer_parameter[lower_str] != dingodb::TokenizerType::kTokenizerTypeBool)) {
          std::string error_msg =
              fmt::format("json_parameter is not consistent with scalar_schema, field_name:{}, field_type:bool vs {}",
                          field.key(), DocumentCodec::GetTokenizerTypeString(column_tokenizer_parameter[field.key()]));
          DINGO_LOG(ERROR) << error_msg;
          return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, error_msg);
        }
        break;
      default:
        DINGO_LOG(ERROR) << "field type is NONE";
        return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "field type is NONE");
        break;
    }
  }

  return butil::Status::OK();
}
#endif

}  // namespace dingodb

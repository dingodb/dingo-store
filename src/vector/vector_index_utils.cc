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

#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "common/logging.h"
#include "faiss/MetricType.h"
#include "faiss/utils/extra_distances-inl.h"
#include "fmt/core.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

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

std::pair<std::unique_ptr<faiss::idx_t[]>, butil::Status> VectorIndexUtils::CopyVectorId(
    const std::vector<int64_t>& delete_ids) {
  std::unique_ptr<faiss::idx_t[]> ids;
  try {
    ids = std::make_unique<faiss::idx_t[]>(delete_ids.size());  // do not modify reset method. this fast and safe.
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for ids: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return {nullptr, butil::Status(pb::error::Errno::EVECTOR_INVALID, s)};
  }

  for (size_t i = 0; i < delete_ids.size(); ++i) {
    ids.get()[i] = static_cast<faiss::idx_t>(delete_ids[i]);
  }

  return {std::move(ids), butil::Status::OK()};
}

std::pair<std::unique_ptr<faiss::idx_t[]>, butil::Status> VectorIndexUtils::CheckAndCopyVectorId(
    const std::vector<pb::common::VectorWithId>& vector_with_ids, faiss::idx_t dimension) {
  // check
  {
    size_t i = 0;
    for (const auto& vector_with_id : vector_with_ids) {
      uint32_t input_dimension = vector_with_id.vector().float_values_size();
      if (input_dimension != static_cast<size_t>(dimension)) {
        std::string s = fmt::format("id.no : {}: float size : {} not equal to  dimension(create) : {}", i,
                                    input_dimension, dimension);
        DINGO_LOG(ERROR) << s;
        return {nullptr, butil::Status(pb::error::Errno::EVECTOR_INVALID, s)};
      }
      i++;
    }
  }

  std::unique_ptr<faiss::idx_t[]> ids;
  try {
    ids = std::make_unique<faiss::idx_t[]>(vector_with_ids.size());  // do not modify reset method. this fast and safe.
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for ids: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return {nullptr, butil::Status(pb::error::Errno::EVECTOR_INVALID, s)};
  }

  for (size_t i = 0; i < vector_with_ids.size(); ++i) {
    ids[i] = static_cast<faiss::idx_t>(vector_with_ids[i].id());
  }

  return {std::move(ids), butil::Status::OK()};
}

std::pair<std::unique_ptr<float[]>, butil::Status> VectorIndexUtils::CopyVectorData(
    const std::vector<pb::common::VectorWithId>& vector_with_ids, faiss::idx_t dimension, bool normalize) {
  std::unique_ptr<float[]> vectors;
  try {
    vectors = std::make_unique<float[]>(vector_with_ids.size() *
                                        dimension);  // do not modify reset method. this fast and safe.
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for vectors: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return {nullptr, butil::Status(pb::error::Errno::EVECTOR_INVALID, s)};
  }

  for (size_t i = 0; i < vector_with_ids.size(); ++i) {
    const auto& vector = vector_with_ids[i].vector().float_values();
    memcpy(vectors.get() + i * dimension, vector.data(), dimension * sizeof(float));

    if (normalize) {
      VectorIndexUtils::NormalizeVectorForFaiss(vectors.get() + i * dimension, dimension);
    }
  }
  return {std::move(vectors), butil::Status::OK()};
}

std::pair<std::unique_ptr<float[]>, butil::Status> VectorIndexUtils::CheckAndCopyVectorData(
    const std::vector<pb::common::VectorWithId>& vector_with_ids, faiss::idx_t dimension, bool normalize) {
  std::unique_ptr<float[]> vectors;

  try {
    vectors = std::make_unique<float[]>(vector_with_ids.size() *
                                        dimension);  // do not modify reset method. this fast and safe.
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for vectors: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return {nullptr, butil::Status(pb::error::Errno::EVECTOR_INVALID, s)};
  }

  for (size_t i = 0; i < vector_with_ids.size(); ++i) {
    if (vector_with_ids[i].vector().float_values_size() != dimension) {
      std::string s = fmt::format(
          "vector dimension is not equal to index dimension, vector id : {}, float_value_size: {}, index dimension: {}",
          vector_with_ids[i].id(), vector_with_ids[i].vector().float_values_size(), dimension);

      DINGO_LOG(ERROR) << s;
      return {nullptr, butil::Status(pb::error::Errno::EVECTOR_INVALID, s)};
    } else {
      const auto& vector = vector_with_ids[i].vector().float_values();
      memcpy(vectors.get() + i * dimension, vector.data(), dimension * sizeof(float));

      if (normalize) {
        VectorIndexUtils::NormalizeVectorForFaiss(vectors.get() + i * dimension, dimension);
      }
    }
  }

  return {std::move(vectors), butil::Status::OK()};
}

butil::Status VectorIndexUtils::FillSearchResult(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                 uint32_t topk, const std::vector<faiss::Index::distance_t>& distances,
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
      vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      if (metric_type == pb::common::MetricType::METRIC_TYPE_COSINE ||
          metric_type == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
        vector_with_distance->set_distance(1.0F - range_search_result->distances[off + i]);
      } else {
        vector_with_distance->set_distance(range_search_result->distances[off + i]);
      }

      vector_with_distance->set_metric_type(metric_type);
    }
    off += total;
  }

  return butil::Status::OK();
}

}  // namespace dingodb

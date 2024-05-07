
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

#ifndef DINGODB_SDK_VECTOR_UTIL_H_
#define DINGODB_SDK_VECTOR_UTIL_H_

#include <cstdint>
#include <ostream>
#include <sstream>

#include "fmt/core.h"
#include "fmt/ranges.h"
#include "sdk/types.h"
#include "sdk/vector.h"

namespace dingodb {
namespace sdk {

std::string Vector::ToString() const {
  std::stringstream float_ss;
  for (size_t i = 0; i < float_values.size(); ++i) {
    float_ss << float_values[i];
    if (i != float_values.size() - 1) {
      float_ss << ", ";
    }
  }

  std::stringstream binary_ss;
  for (size_t i = 0; i < binary_values.size(); ++i) {
    binary_ss << binary_values[i];
    if (i != binary_values.size() - 1) {
      binary_ss << ", ";
    }
  }

  return fmt::format("Vector {{ dimension: {}, value_type: {}, float_values: [{}], binary_values: [{}] }}", dimension,
                     ValueTypeToString(value_type), float_ss.str(), binary_ss.str());
}

static std::string FieldToString(const Type& type, const ScalarField& field) {
  switch (type) {
    case Type::kBOOL:
      return std::to_string(field.bool_data);
    case Type::kINT64:
      return std::to_string(field.long_data);
    case Type::kDOUBLE:
      return std::to_string(field.double_data);
    case Type::kSTRING:
      return field.string_data;
    default:
      return "Unknown";
  }
}

std::string ScalarValue::ToString() const {
  std::stringstream ss;
  ss << "ScalarValue { type: " << TypeToString(type) << ", fields: [";

  for (const ScalarField& field : fields) {
    ss << " " << FieldToString(type, field) << ",";
  }

  if (!fields.empty()) {
    ss.seekp(-1, std::ios_base::end);  // Remove the trailing comma
  }

  ss << " ] }";
  return ss.str();
}

std::string VectorWithId::ToString() const {
  std::stringstream ss;
  ss << "VectorWithId { id: " << id << ", vector: " << vector.ToString();

  if (!scalar_data.empty()) {
    ss << ", scalar_data: {";
    for (const auto& pair : scalar_data) {
      ss << " " << pair.first << ": " << pair.second.ToString() << ",";
    }
    ss.seekp(-1, std::ios_base::end);  // Remove the trailing comma
    ss << " }";
  }

  ss << " }";
  return ss.str();
}

std::string VectorWithDistance::ToString() const {
  return fmt::format("VectorWithDistance {{ vector: {}, distance: {}, metric_type: {} }}", vector_data.ToString(),
                     distance, MetricTypeToString(metric_type));
}

std::string SearchResult::ToString() const {
  std::ostringstream oss;
  oss << "SearchResult { id: " << id.ToString() << ", vector_datas: [";
  for (const auto& vector_data : vector_datas) {
    oss << vector_data.ToString() << ", ";
  }
  oss << "]}";
  return oss.str();
}

std::string DeleteResult::ToString() const {
  return fmt::format("DeleteResult {{ vector_id: {}, deleted: {} }}", vector_id, (deleted ? "true" : "false"));
}

std::string QueryResult::ToString() const {
  std::ostringstream oss;
  oss << "QueryResult: {";
  oss << "vectors: [";
  for (const auto& vector : vectors) {
    oss << vector.ToString() << ", ";
  }
  oss << "]";
  oss << "}";
  return oss.str();
}

std::string ScanQueryResult::ToString() const {
  std::ostringstream oss;
  oss << "ScanQueryResult: {";
  oss << "vectors: [";
  for (const auto& vector : vectors) {
    oss << vector.ToString() << ", ";
  }
  oss << "]";
  oss << "}";
  return oss.str();
}

std::string VectorIndexTypeToString(VectorIndexType type) {
  switch (type) {
    case VectorIndexType::kNoneIndexType:
      return "NoneIndexType";
    case VectorIndexType::kFlat:
      return "Flat";
    case VectorIndexType::kIvfFlat:
      return "IvfFlat";
    case VectorIndexType::kIvfPq:
      return "IvfPq";
    case VectorIndexType::kHnsw:
      return "Hnsw";
    case VectorIndexType::kDiskAnn:
      return "DiskAnn";
    case VectorIndexType::kBruteForce:
      return "BruteForce";
    default:
      return "Unknown";
  }
}

std::string MetricTypeToString(MetricType type) {
  switch (type) {
    case MetricType::kNoneMetricType:
      return "NoneMetricType";
    case MetricType::kL2:
      return "L2";
    case MetricType::kInnerProduct:
      return "InnerProduct";
    case MetricType::kCosine:
      return "Cosine";
    default:
      return "Unknown";
  }
}

std::string ValueTypeToString(ValueType type) {
  switch (type) {
    case ValueType::kNoneValueType:
      return "NoneValueType";
    case ValueType::kFloat:
      return "Float";
    case ValueType::kUint8:
      return "UinT8";
    default:
      return "Unknown";
  }
}

std::string IndexMetricsResult::ToString() const {
  std::ostringstream oss;
  oss << "IndexMetricsResult: {";
  oss << "index_type: " << VectorIndexTypeToString(index_type) << ", ";
  oss << "count: " << count << ", ";
  oss << "deleted_count: " << deleted_count << ", ";
  oss << "max_vector_id: " << max_vector_id << ", ";
  oss << "min_vector_id: " << min_vector_id << ", ";
  oss << "memory_bytes: " << memory_bytes;
  oss << "}";
  return oss.str();
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_UTIL_H_

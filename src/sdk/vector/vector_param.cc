
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
#include "sdk/vector.h"

namespace dingodb {
namespace sdk {

std::string DumpToString(const Vector& obj) {
  std::string float_values = fmt::format("{}", fmt::join(obj.float_values, ", "));
  std::string binary_values = fmt::format("{}", fmt::join(obj.binary_values, ", "));
  return fmt::format("Vector {{ dimension: {}, value_type: {}, float_values: [{}], binary_values: [{}] }}",
                     obj.dimension, ValueTypeToString(obj.value_type), float_values, binary_values);
}

std::string DumpToString(const VectorWithId& obj) {
  return fmt::format("VectorWithId {{ id: {}, vector: {} }}", obj.id, DumpToString(obj.vector));
}

std::string DumpToString(const VectorWithDistance& obj) {
  return fmt::format("VectorWithDistance {{ vector: {}, distance: {}, metric_type: {} }}",
                     DumpToString(obj.vector_data), obj.distance, MetricTypeToString(obj.metric_type));
}

std::string DumpToString(const SearchResult& obj) {
  std::ostringstream oss;
  oss << "SearchResult { id: " << DumpToString(obj.id) << ", vector_datas: [";
  for (const auto& vector_data : obj.vector_datas) {
    oss << DumpToString(vector_data) << ", ";
  }
  oss << "]}";
  return oss.str();
}

std::string DumpToString(const DeleteResult& obj) {
  return fmt::format("DeleteResult {{ vector_id: {}, deleted: {} }}", obj.vector_id, (obj.deleted ? "true" : "false"));
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

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_UTIL_H_

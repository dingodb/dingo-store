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

#ifndef DINGODB_SDK_VECTOR_H_
#define DINGODB_SDK_VECTOR_H_

#include <cstdint>
#include <map>
#include <vector>

#include "sdk/status.h"

namespace dingodb {
namespace sdk {

class ClientStub;

enum VectorIndexType : uint8_t { kNoneIndexType, kFlat, kIvfFlat, kIvfPq, kHnsw, kDiskAnn, kBruteForce };

std::string VectorIndexTypeToString(VectorIndexType type);

enum MetricType : uint8_t { kNoneMetricType, kL2, kInnerProduct, kCosine };

std::string MetricTypeToString(MetricType type);

struct FlatParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;

  explicit FlatParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kFlat; }
};

struct IvfFlatParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;
  // Number of cluster centers Default 2048 required
  int32_t ncentroids{2048};

  explicit IvfFlatParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kIvfFlat; }
};

struct IvfPqParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;
  // Number of cluster centers Default 2048 required
  int32_t ncentroids{2048};
  // PQ split sub-vector size default 64 required
  int32_t nsubvector{64};
  // Inverted list (IVF) bucket initialization size default 1000 optional
  int32_t bucket_init_size{1000};
  // Inverted list (IVF) bucket maximum capacity default 1280000 optional
  int32_t bucket_max_size{1280000};
  // bit number of sub cluster center. default 8 required.  means 256.
  int32_t nbits_per_idx{8};

  explicit IvfPqParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kIvfPq; }
};

struct HnswParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;
  // the range traversed in the graph during the process of finding node neighbors when
  // composing the graph. The larger the value, the better the composition effect and the
  // longer the composition time. Default 40 required
  int32_t ef_construction{40};
  // Set the maximum number of elements. required
  int32_t max_elements = 4;
  // The number of node neighbors, the larger the value, the better the composition effect, and the
  // more memory it takes. Default 32. required .
  int32_t nlinks{32};

  explicit HnswParam(int32_t p_dimension, MetricType p_metric_type, int32_t p_max_elements)
      : dimension(p_dimension), metric_type(p_metric_type), max_elements(p_max_elements) {}

  static VectorIndexType Type() { return VectorIndexType::kHnsw; }
};

struct DiskAnnParam {
  // TODO: to support
};

struct BruteForceParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;

  explicit BruteForceParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kBruteForce; }
};

enum ValueType : uint8_t { kNoneValueType, kFloat, kUint8 };

std::string ValueTypeToString(ValueType type);

struct Vector {
  int32_t dimension;
  ValueType value_type;
  std::vector<float> float_values;
  std::vector<uint8_t> binary_values;

  explicit Vector() : value_type(kNoneValueType), dimension(0) {}

  explicit Vector(ValueType p_value_type, int32_t p_dimension) : value_type(p_value_type), dimension(p_dimension) {}

  Vector(Vector&& other) noexcept
      : dimension(other.dimension),
        value_type(other.value_type),
        float_values(std::move(other.float_values)),
        binary_values(std::move(other.binary_values)) {}

  Vector(const Vector& other) noexcept = default;

  Vector& operator=(Vector&& other) noexcept {
    dimension = other.dimension;
    value_type = other.value_type;
    float_values = std::move(other.float_values);
    binary_values = std::move(other.binary_values);
    return *this;
  }

  Vector& operator=(const Vector&) = default;

  uint32_t Size() const { return float_values.size() * 4 + binary_values.size() + 4; }

  std::string ToString() const;
};

enum class ScalarFieldType : uint8_t {
  kNone,
  kBool,
  kInt8,
  kInt16,
  kInt32,
  kInt64,
  kFloat32,
  kDouble,
  kString,
  kBytes
};

struct ScalarField {
  bool bool_data;
  int32_t int_data;
  int64_t long_data;
  float float_data;
  double double_data;
  std::string string_data;
  std::string bytes_data;
};

struct ScalarValue {
  ScalarFieldType type;
  std::vector<ScalarField> fields;
};

struct VectorWithId {
  int64_t id;
  Vector vector;
  //  TODO: scalar data and table data
  std::map<std::string, ScalarValue> scalar_data;

  explicit VectorWithId() : id(0) {}

  explicit VectorWithId(int64_t p_id, Vector p_vector) : id(p_id), vector(std::move(p_vector)) {}

  explicit VectorWithId(Vector p_vector) : id(0), vector(std::move(p_vector)) {}

  VectorWithId(VectorWithId&& other) noexcept
      : id(other.id), vector(std::move(other.vector)), scalar_data(std::move(other.scalar_data)) {}
  VectorWithId(const VectorWithId& other) = default;

  VectorWithId& operator=(VectorWithId&& other) noexcept {
    id = other.id;
    vector = std::move(other.vector);
    return *this;
  }

  VectorWithId& operator=(const VectorWithId&) = default;

  std::string ToString() const;
};

enum FilterSource : uint8_t {
  kNoneFilterSource,
  // filter vector scalar include post filter and pre filter
  kScalarFilter,
  // use coprocessor only include pre filter
  kTableFilter,
  // vector id search direct by ids. only include pre filter
  kVectorIdFilter
};

enum FilterType : uint8_t {
  kNoneFilterType,
  // first vector search, then filter
  kQueryPost,
  // first search from rocksdb, then search vector
  kQueryPre
};

enum SearchExtraParamType : uint8_t { kParallelOnQueries, kNprobe, kRecallNum, kEfSearch };

struct SearchParam {
  int32_t topk{0};
  bool with_vector_data{true};
  bool with_scalar_data{false};
  std::vector<std::string> selected_keys;
  bool with_table_data{false};      // Default false, if true, response without table data
  bool enable_range_search{false};  // if enable_range_search = true. top_n disabled.
  float radius{0.0f};
  FilterSource filter_source{kNoneFilterSource};
  FilterType filter_type{kNoneFilterType};
  // TODO: coprocessorv2
  std::vector<int64_t> vector_ids;  // vector id array vector_filter == VECTOR_ID_FILTER enable vector_ids
  bool use_brute_force{false};      // use brute-force search
  std::map<SearchExtraParamType, int32_t> extra_params;  // The search method to use

  explicit SearchParam() = default;

  SearchParam(SearchParam&& other) noexcept
      : topk(other.topk),
        with_vector_data(other.with_vector_data),
        with_scalar_data(other.with_scalar_data),
        with_table_data(other.with_table_data),
        enable_range_search(other.enable_range_search),
        radius(other.radius),
        filter_source(other.filter_source),
        filter_type(other.filter_type),
        vector_ids(std::move(other.vector_ids)),
        use_brute_force(other.use_brute_force),
        extra_params(std::move(other.extra_params)) {
    other.topk = 0;
    other.with_vector_data = true;
    other.with_scalar_data = false;
    other.with_table_data = false;
    other.enable_range_search = false;
    other.radius = 0.0f;
    other.filter_source = kNoneFilterSource;
    other.filter_type = kNoneFilterType;
    other.use_brute_force = false;
  }

  SearchParam& operator=(SearchParam&& other) noexcept {
    topk = other.topk;
    with_vector_data = other.with_vector_data;
    with_scalar_data = other.with_scalar_data;
    with_table_data = other.with_table_data;
    enable_range_search = other.enable_range_search;
    radius = other.radius;
    filter_source = other.filter_source;
    filter_type = other.filter_type;
    vector_ids = std::move(other.vector_ids);
    use_brute_force = other.use_brute_force;
    extra_params = std::move(other.extra_params);

    other.topk = 0;
    other.with_vector_data = true;
    other.with_scalar_data = false;
    other.with_table_data = false;
    other.enable_range_search = false;
    other.radius = 0.0f;
    other.filter_source = kNoneFilterSource;
    other.filter_type = kNoneFilterType;
    other.use_brute_force = false;

    return *this;
  }
};

struct VectorWithDistance {
  VectorWithId vector_data;
  float distance;
  MetricType metric_type{kNoneMetricType};

  explicit VectorWithDistance() = default;

  VectorWithDistance(VectorWithDistance&& other) noexcept
      : vector_data(std::move(other.vector_data)), distance(other.distance), metric_type(other.metric_type) {}

  VectorWithDistance& operator=(VectorWithDistance&& other) noexcept {
    vector_data = std::move(other.vector_data);
    distance = other.distance;
    metric_type = other.metric_type;
    return *this;
  }

  VectorWithDistance(const VectorWithDistance&) = default;
  VectorWithDistance& operator=(const VectorWithDistance&) = default;

  std::string ToString() const;
};

struct SearchResult {
  // TODO : maybe remove VectorWithId
  VectorWithId id;
  std::vector<VectorWithDistance> vector_datas;

  SearchResult() = default;

  explicit SearchResult(VectorWithId p_id) : id(std::move(p_id)) {}

  SearchResult(SearchResult&& other) noexcept : id(std::move(other.id)), vector_datas(std::move(other.vector_datas)) {}

  SearchResult& operator=(SearchResult&& other) noexcept {
    id = std::move(other.id);
    vector_datas = std::move(other.vector_datas);
    return *this;
  }

  SearchResult(const SearchResult&) = default;
  SearchResult& operator=(const SearchResult&) = default;

  std::string ToString() const;
};

struct DeleteResult {
  int64_t vector_id;
  bool deleted;

  std::string ToString() const;
};

struct QueryParam {
  std::vector<int64_t> vector_ids;
  // If true, response with vector data
  bool with_vector_data{true};
  // if true, response with scalar data
  bool with_scalar_data{false};
  // If with_scalar_data is true, selected_keys is used to select scalar data, and if this parameter is null, all scalar
  // data will be returned.
  std::vector<std::string> selected_keys;
  // if true, response witho table data
  bool with_table_data{false};
};

struct QueryResult {
  std::vector<VectorWithId> vectors;

  std::string ToString() const;
};

struct ScanQueryParam {
  int64_t vector_id_start;
  // the end id of scan
  // if is_reverse is true, vector_id_end must be less than vector_id_start
  // if is_reverse is false, vector_id_end must be greater than vector_id_start
  // the real range is [start, end], include start and end
  // if vector_id_end == 0, scan to the end of the region
  int64_t vector_id_end{0};
  int64_t max_scan_count;
  bool is_reverse{false};

  bool with_vector_data{true};
  bool with_scalar_data{false};
  // If with_scalar_data is true, selected_keys is used to select scalar data, and if this parameter is null, all scalar
  // data will be returned.
  std::vector<std::string> selected_keys;
  bool with_table_data{false};  // Default false, if true, response without table data

  // TODO: support use_scalar_filter
  bool use_scalar_filter{false};
  // std::map<std::string, ScalarValue> scalar_data;

  explicit ScanQueryParam() = default;

  ScanQueryParam(ScanQueryParam&& other) noexcept
      : vector_id_start(other.vector_id_start),
        vector_id_end(other.vector_id_end),
        max_scan_count(other.max_scan_count),
        is_reverse(other.is_reverse),
        with_vector_data(other.with_vector_data),
        with_scalar_data(other.with_scalar_data),
        selected_keys(std::move(other.selected_keys)),
        with_table_data(other.with_table_data),
        use_scalar_filter(other.use_scalar_filter) {}

  ScanQueryParam& operator=(ScanQueryParam&& other) noexcept {
    if (this != &other) {
      vector_id_start = other.vector_id_start;
      vector_id_end = other.vector_id_end;
      max_scan_count = other.max_scan_count;
      is_reverse = other.is_reverse;
      with_vector_data = other.with_vector_data;
      with_scalar_data = other.with_scalar_data;
      selected_keys = std::move(other.selected_keys);
      with_table_data = other.with_table_data;
      use_scalar_filter = other.use_scalar_filter;
      // You can add more fields here if you add more fields to the struct
    }
    return *this;
  }
};

struct ScanQueryResult {
  std::vector<VectorWithId> vectors;

  std::string ToString() const;
};

struct IndexMetricsResult {
  VectorIndexType index_type{kNoneIndexType};
  int64_t count{0};
  int64_t deleted_count{0};
  int64_t max_vector_id{0};
  int64_t min_vector_id{0};
  int64_t memory_bytes{0};

  std::string ToString() const;
};

class VectorIndexCreator {
 public:
  ~VectorIndexCreator();

  VectorIndexCreator& SetSchemaId(int64_t schema_id);

  VectorIndexCreator& SetName(const std::string& name);

  VectorIndexCreator& SetRangePartitions(std::vector<int64_t> separator_id);

  VectorIndexCreator& SetReplicaNum(int64_t num);

  // one of FlatParam/IvfFlatParam/HnswParam/DiskAnnParam/BruteForceParam, if set multiple, the last one will effective
  VectorIndexCreator& SetFlatParam(const FlatParam& params);
  VectorIndexCreator& SetIvfFlatParam(const IvfFlatParam& params);
  VectorIndexCreator& SetIvfPqParam(const IvfPqParam& params);
  VectorIndexCreator& SetHnswParam(const HnswParam& params);
  // VectorIndexCreator& SetDiskAnnParam(DiskAnnParam& params);
  VectorIndexCreator& SetBruteForceParam(const BruteForceParam& params);

  // VectorIndexCreator& SetAutoIncrement(bool auto_incr);

  // VectorIndexCreator& SetAutoIncrementStart(int64_t start_id);

  Status Create(int64_t& out_index_id);

 private:
  friend class Client;

  // own
  class Data;
  Data* data_;
  explicit VectorIndexCreator(Data* data);
};

class VectorClient {
 public:
  VectorClient(const VectorClient&) = delete;
  const VectorClient& operator=(const VectorClient&) = delete;

  ~VectorClient() = default;

  Status AddByIndexId(int64_t index_id, const std::vector<VectorWithId>& vectors, bool replace_deleted = false,
                      bool is_update = false);
  Status AddByIndexName(int64_t schema_id, const std::string& index_name, const std::vector<VectorWithId>& vectors,
                        bool replace_deleted = false, bool is_update = false);

  Status SearchByIndexId(int64_t index_id, const SearchParam& search_param,
                         const std::vector<VectorWithId>& target_vectors, std::vector<SearchResult>& out_result);
  Status SearchByIndexName(int64_t schema_id, const std::string& index_name, const SearchParam& search_param,
                           const std::vector<VectorWithId>& target_vectors, std::vector<SearchResult>& out_result);

  Status DeleteByIndexId(int64_t index_id, const std::vector<int64_t>& vector_ids,
                         std::vector<DeleteResult>& out_result);
  Status DeleteByIndexName(int64_t schema_id, const std::string& index_name, const std::vector<int64_t>& vector_ids,
                           std::vector<DeleteResult>& out_result);

  Status BatchQueryByIndexId(int64_t index_id, const QueryParam& query_param, QueryResult& out_result);
  Status BatchQueryByIndexName(int64_t schema_id, const std::string& index_name, const QueryParam& query_param,
                               QueryResult& out_result);

  Status GetBorderByIndexId(int64_t index_id, bool is_max, int64_t& out_vector_id);
  Status GetBorderByIndexName(int64_t schema_id, const std::string& index_name, bool is_max, int64_t& out_vector_id);

  Status ScanQueryByIndexId(int64_t index_id, const ScanQueryParam& query_param, ScanQueryResult& out_result);
  Status ScanQueryByIndexName(int64_t schema_id, const std::string& index_name, const ScanQueryParam& query_param,
                              ScanQueryResult& out_result);

  Status GetIndexMetricsByIndexId(int64_t index_id, IndexMetricsResult& out_result);
  Status GetIndexMetricsByIndexName(int64_t schema_id, const std::string& index_name, IndexMetricsResult& out_result);

  Status CountByIndexId(int64_t index_id, int64_t start_vector_id, int64_t end_vector_id, int64_t& out_count);
  Status CountByIndexName(int64_t schema_id, const std::string& index_name, int64_t start_vector_id, int64_t end_vector_id, int64_t& out_count);

 private:
  friend class Client;

  const ClientStub& stub_;

  explicit VectorClient(const ClientStub& stub);
};
}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_H_
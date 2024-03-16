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

#include "vector/vector_reader.h"

#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "coprocessor/coprocessor_scalar.h"
#include "coprocessor/coprocessor_v2.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_index.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_flat.h"

namespace dingodb {

#ifndef ENABLE_SCALAR_WITH_COPROCESSOR
#define ENABLE_SCALAR_WITH_COPROCESSOR
#endif
// #undef ENABLE_SCALAR_WITH_COPROCESSOR

DEFINE_int64(vector_index_max_range_search_result_count, 1024, "max range search result count");
DEFINE_int64(vector_index_bruteforce_batch_count, 2048, "bruteforce batch count");

bvar::LatencyRecorder g_bruteforce_search_latency("dingo_bruteforce_search_latency");
bvar::LatencyRecorder g_bruteforce_range_search_latency("dingo_bruteforce_range_search_latency");

butil::Status VectorReader::QueryVectorWithId(const pb::common::Range& region_range, int64_t partition_id,
                                              int64_t vector_id, bool with_vector_data,
                                              pb::common::VectorWithId& vector_with_id) {
  std::string key;
  VectorCodec::EncodeVectorKey(region_range.start_key()[0], partition_id, vector_id, key);

  std::string value;
  auto status = reader_->KvGet(Constant::kStoreDataCF, key, value);
  if (!status.ok()) {
    return status;
  }

  if (with_vector_data) {
    pb::common::Vector vector;
    if (!vector.ParseFromString(value)) {
      return butil::Status(pb::error::EINTERNAL, "Parse proto from string error");
    }
    vector_with_id.mutable_vector()->Swap(&vector);
  }

  vector_with_id.set_id(vector_id);

  return butil::Status();
}

butil::Status VectorReader::SearchVector(
    int64_t partition_id, VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "Empty vector with ids";
    return butil::Status();
  }

  auto vector_filter = parameter.vector_filter();
  auto vector_filter_type = parameter.vector_filter_type();

  bool with_vector_data = !(parameter.without_vector_data());
  std::vector<pb::index::VectorWithDistanceResult> tmp_results;

  // scalar post filter
  if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
      dingodb::pb::common::VectorFilterType::QUERY_POST == vector_filter_type) {
    uint32_t top_n = parameter.top_n();
    bool enable_range_search = parameter.enable_range_search();

    if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() == 0) &&
        !parameter.has_vector_coprocessor()) {
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(
          vector_index, region_range, vector_with_ids, parameter, vector_with_distance_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
    } else if (parameter.has_vector_coprocessor()) {
      if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() != 0)) {
        DINGO_LOG(WARNING) << "vector_with_ids[0].scalar_data() deprecated. use coprocessor.";
      }
      top_n *= 10;
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids,
                                                                       parameter, tmp_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }

      std::shared_ptr<RawCoprocessor> scalar_coprocessor = std::make_shared<CoprocessorScalar>();

      status = scalar_coprocessor->Open(CoprocessorPbWrapper{parameter.vector_coprocessor()});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << "scalar coprocessor::Open failed " << status.error_cstr();
        return status;
      }

      for (auto& vector_with_distance_result : tmp_results) {
        pb::index::VectorWithDistanceResult new_vector_with_distance_result;

        for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
          int64_t temp_id = temp_vector_with_distance.vector_with_id().id();
          bool compare_result = false;
          butil::Status status = CompareVectorScalarDataWithCoprocessor(region_range, partition_id, temp_id,
                                                                        scalar_coprocessor, compare_result);
          if (!status.ok()) {
            return status;
          }

          if (!compare_result) {
            continue;
          }

          new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
          // topk
          if (!enable_range_search) {
            if (new_vector_with_distance_result.vector_with_distances_size() >= parameter.top_n()) {
              break;
            }
          }
        }
        vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
      }

    } else {  //! parameter.has_vector_coprocessor() && vector_with_ids[0].scalar_data().scalar_data_size() != 0
      top_n *= 10;
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids,
                                                                       parameter, tmp_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }

      for (auto& vector_with_distance_result : tmp_results) {
        pb::index::VectorWithDistanceResult new_vector_with_distance_result;

        for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
          int64_t temp_id = temp_vector_with_distance.vector_with_id().id();
          bool compare_result = false;
          butil::Status status = CompareVectorScalarData(region_range, partition_id, temp_id,
                                                         vector_with_ids[0].scalar_data(), compare_result);
          if (!status.ok()) {
            return status;
          }
          if (!compare_result) {
            continue;
          }

          new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
          // topk
          if (!enable_range_search) {
            if (new_vector_with_distance_result.vector_with_distances_size() >= parameter.top_n()) {
              break;
            }
          }
        }
        vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
      }
    }
  } else if (dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER == vector_filter) {  // vector id array search
    butil::Status status = DoVectorSearchForVectorIdPreFilter(vector_index, vector_with_ids, parameter, region_range,
                                                              vector_with_distance_results);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForVectorIdPreFilter failed");
      return status;
    }
  } else if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
             dingodb::pb::common::VectorFilterType::QUERY_PRE == vector_filter_type) {  // scalar pre filter search

    butil::Status status = DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                            vector_with_distance_results);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForScalarPreFilter failed ");
      return status;
    }
  } else if (dingodb::pb::common::VectorFilter::TABLE_FILTER ==
             vector_filter) {  //  table coprocessor pre filter search. not impl
    butil::Status status = DoVectorSearchForTableCoprocessor(vector_index, region_range, vector_with_ids, parameter,
                                                             vector_with_distance_results);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForTableCoprocessor failed ");
      return status;
    }
  }

  // if vector index does not support restruct vector ,we restruct it using RocksDB
  if (with_vector_data) {
    for (auto& result : vector_with_distance_results) {
      for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
        if (vector_with_distance.vector_with_id().vector().float_values_size() > 0 ||
            vector_with_distance.vector_with_id().vector().binary_values_size() > 0) {
          continue;
        }

        pb::common::VectorWithId vector_with_id;
        auto status = QueryVectorWithId(region_range, partition_id, vector_with_distance.vector_with_id().id(), true,
                                        vector_with_id);
        if (!status.ok()) {
          return status;
        }
        vector_with_distance.mutable_vector_with_id()->Swap(&vector_with_id);
      }
    }
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorTableData(const pb::common::Range& region_range, int64_t partition_id,
                                                 pb::common::VectorWithId& vector_with_id) {
  std::string key, value;
  VectorCodec::EncodeVectorKey(region_range.start_key()[0], partition_id, vector_with_id.id(), key);

  auto status = reader_->KvGet(Constant::kVectorTableCF, key, value);
  if (!status.ok()) {
    return status;
  }

  pb::common::VectorTableData vector_table;
  if (!vector_table.ParseFromString(value)) {
    return butil::Status(pb::error::EINTERNAL, "Decode vector table data failed");
  }

  *(vector_with_id.mutable_table_data()) = vector_table;

  return butil::Status();
}

butil::Status VectorReader::QueryVectorTableData(const pb::common::Range& region_range, int64_t partition_id,
                                                 std::vector<pb::index::VectorWithDistanceResult>& results) {
  // get metadata by parameter
  for (auto& result : results) {
    for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
      pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
      QueryVectorTableData(region_range, partition_id, vector_with_id);
    }
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorTableData(const pb::common::Range& region_range, int64_t partition_id,
                                                 std::vector<pb::common::VectorWithDistance>& vector_with_distances) {
  // get metadata by parameter
  for (auto& vector_with_distance : vector_with_distances) {
    pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
    QueryVectorTableData(region_range, partition_id, vector_with_id);
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorScalarData(const pb::common::Range& region_range, int64_t partition_id,
                                                  std::vector<std::string> selected_scalar_keys,
                                                  pb::common::VectorWithId& vector_with_id) {
  std::string key, value;
  // VectorCodec::EncodeVectorScalar(partition_id, vector_with_id.id(), key);
  VectorCodec::EncodeVectorKey(region_range.start_key()[0], partition_id, vector_with_id.id(), key);

  auto status = reader_->KvGet(Constant::kVectorScalarCF, key, value);
  if (!status.ok()) {
    return status;
  }

  pb::common::VectorScalardata vector_scalar;
  if (!vector_scalar.ParseFromString(value)) {
    return butil::Status(pb::error::EINTERNAL, "Decode vector scalar data failed");
  }

  auto* scalar = vector_with_id.mutable_scalar_data()->mutable_scalar_data();
  for (const auto& [key, value] : vector_scalar.scalar_data()) {
    if (!selected_scalar_keys.empty() &&
        std::find(selected_scalar_keys.begin(), selected_scalar_keys.end(), key) == selected_scalar_keys.end()) {
      continue;
    }

    scalar->insert({key, value});
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorScalarData(const pb::common::Range& region_range, int64_t partition_id,
                                                  std::vector<std::string> selected_scalar_keys,
                                                  std::vector<pb::index::VectorWithDistanceResult>& results) {
  // get metadata by parameter
  for (auto& result : results) {
    for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
      pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
      QueryVectorScalarData(region_range, partition_id, selected_scalar_keys, vector_with_id);
    }
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorScalarData(const pb::common::Range& region_range, int64_t partition_id,
                                                  std::vector<std::string> selected_scalar_keys,
                                                  std::vector<pb::common::VectorWithDistance>& vector_with_distances) {
  // get metadata by parameter
  for (auto& vector_with_distance : vector_with_distances) {
    pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
    QueryVectorScalarData(region_range, partition_id, selected_scalar_keys, vector_with_id);
  }

  return butil::Status();
}

butil::Status VectorReader::CompareVectorScalarData(const pb::common::Range& region_range, int64_t partition_id,
                                                    int64_t vector_id,
                                                    const pb::common::VectorScalardata& source_scalar_data,
                                                    bool& compare_result) {
  compare_result = false;
  std::string key, value;

  VectorCodec::EncodeVectorKey(region_range.start_key()[0], partition_id, vector_id, key);

  auto status = reader_->KvGet(Constant::kVectorScalarCF, key, value);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("Get vector scalar data failed, vector_id: {} error: {} ", vector_id,
                                      status.error_str());
    return status;
  }

  pb::common::VectorScalardata vector_scalar;
  if (!vector_scalar.ParseFromString(value)) {
    return butil::Status(pb::error::EINTERNAL, "Decode vector scalar data failed");
  }

  for (const auto& [key, value] : source_scalar_data.scalar_data()) {
    auto it = vector_scalar.scalar_data().find(key);
    if (it == vector_scalar.scalar_data().end()) {
      compare_result = false;
      return butil::Status();
    }

    compare_result = Helper::IsEqualVectorScalarValue(value, it->second);
    if (!compare_result) {
      return butil::Status();
    }
  }

  compare_result = true;
  return butil::Status();
}

butil::Status VectorReader::CompareVectorScalarDataWithCoprocessor(
    const pb::common::Range& region_range, int64_t partition_id, int64_t vector_id,
    const std::shared_ptr<RawCoprocessor>& scalar_coprocessor, bool& compare_result) {
  compare_result = false;
  std::string key, value;

  VectorCodec::EncodeVectorKey(region_range.start_key()[0], partition_id, vector_id, key);

  auto status = reader_->KvGet(Constant::kVectorScalarCF, key, value);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("Get vector scalar data failed, vector_id: {} error: {} ", vector_id,
                                      status.error_str());
    return status;
  }

  pb::common::VectorScalardata vector_scalar;
  if (!vector_scalar.ParseFromString(value)) {
    return butil::Status(pb::error::EINTERNAL, "Decode vector scalar data failed");
  }

  auto lambda_scalar_compare_with_coprocessor_function =
      [&scalar_coprocessor](const pb::common::VectorScalardata& internal_vector_scalar) {
        bool is_reverse = false;
        butil::Status status = scalar_coprocessor->Filter(internal_vector_scalar, is_reverse);
        if (!status.ok()) {
          LOG(ERROR) << "[" << __PRETTY_FUNCTION__ << "] "
                     << "scalar coprocessor::Filter failed " << status.error_cstr();
          return false;
        }
        return is_reverse;
      };

  compare_result = lambda_scalar_compare_with_coprocessor_function(vector_scalar);

  return butil::Status();
}

butil::Status VectorReader::VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                              std::vector<pb::index::VectorWithDistanceResult>& results) {  // NOLINT
  // Search vectors by vectors
  auto status = SearchVector(ctx->partition_id, ctx->vector_index, ctx->region_range, ctx->vector_with_ids,
                             ctx->parameter, results);
  if (!status.ok()) {
    return status;
  }

  if (!ctx->parameter.without_scalar_data()) {
    // Get scalar data by parameter
    std::vector<std::string> selected_scalar_keys = Helper::PbRepeatedToVector(ctx->parameter.selected_keys());
    auto status = QueryVectorScalarData(ctx->region_range, ctx->partition_id, selected_scalar_keys, results);
    if (!status.ok()) {
      return status;
    }
  }

  if (!ctx->parameter.without_table_data()) {
    // Get table data by parameter
    auto status = QueryVectorTableData(ctx->region_range, ctx->partition_id, results);
    if (!status.ok()) {
      return status;
    }
  }

  return butil::Status();
}

butil::Status VectorReader::VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                             std::vector<pb::common::VectorWithId>& vector_with_ids) {
  for (auto vector_id : ctx->vector_ids) {
    pb::common::VectorWithId vector_with_id;
    auto status =
        QueryVectorWithId(ctx->region_range, ctx->partition_id, vector_id, ctx->with_vector_data, vector_with_id);
    if ((!status.ok()) && status.error_code() != pb::error::EKEY_NOT_FOUND) {
      DINGO_LOG(WARNING) << fmt::format("Query vector_with_id failed, vector_id: {} error: {}", vector_id,
                                        status.error_str());
    }

    // if the id is not exist, the vector_with_id will be empty, sdk client will handle this
    vector_with_ids.push_back(vector_with_id);
  }

  if (ctx->with_scalar_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status =
          QueryVectorScalarData(ctx->region_range, ctx->partition_id, ctx->selected_scalar_keys, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector scalar data failed, vector_id: {} error: {} ",
                                          vector_with_id.id(), status.error_str());
      }
    }
  }

  if (ctx->with_table_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status = QueryVectorTableData(ctx->region_range, ctx->partition_id, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector table data failed, vector_id: {} error: {} ",
                                          vector_with_id.id(), status.error_str());
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::VectorGetBorderId(const pb::common::Range& region_range, bool get_min, int64_t& vector_id) {
  auto status = GetBorderId(region_range, get_min, vector_id);
  if (!status.ok()) {
    DINGO_LOG(INFO) << "Get border vector id failed, error: " << status.error_str();
    return status;
  }

  return butil::Status();
}

butil::Status VectorReader::VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                            std::vector<pb::common::VectorWithId>& vector_with_ids) {
  DINGO_LOG(INFO) << fmt::format("Scan vector id, region_id: {} start_id: {} is_reverse: {} limit: {}", ctx->region_id,
                                 ctx->start_id, ctx->is_reverse, ctx->limit);

  // scan for ids
  std::vector<int64_t> vector_ids;
  auto status = ScanVectorId(ctx, vector_ids);
  if (!status.ok()) {
    DINGO_LOG(INFO) << "Scan vector id failed, error: " << status.error_str();
    return status;
  }

  DINGO_LOG(INFO) << "scan vector id count: " << vector_ids.size();

  if (vector_ids.empty()) {
    return butil::Status();
  }

  // query vector with id
  for (auto vector_id : vector_ids) {
    pb::common::VectorWithId vector_with_id;
    auto status =
        QueryVectorWithId(ctx->region_range, ctx->partition_id, vector_id, ctx->with_vector_data, vector_with_id);
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format("Query vector data failed, vector_id {} error: {}", vector_id,
                                        status.error_str());
    }

    // if the id is not exist, the vector_with_id will be empty, sdk client will handle this
    vector_with_ids.push_back(vector_with_id);
  }

  if (ctx->with_scalar_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status =
          QueryVectorScalarData(ctx->region_range, ctx->partition_id, ctx->selected_scalar_keys, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector scalar data failed, vector_id {} error: {}",
                                          vector_with_id.id(), status.error_str());
      }
    }
  }

  if (ctx->with_table_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status = QueryVectorTableData(ctx->region_range, ctx->partition_id, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector table data failed, vector_id {} error: {}", vector_with_id.id(),
                                          status.error_str());
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::VectorGetRegionMetrics(int64_t /*region_id*/, const pb::common::Range& region_range,
                                                   VectorIndexWrapperPtr vector_index,
                                                   pb::common::VectorIndexMetrics& region_metrics) {
  int64_t total_vector_count = 0;
  int64_t total_deleted_count = 0;
  int64_t total_memory_usage = 0;
  int64_t max_id = 0;
  int64_t min_id = 0;

  auto inner_vector_index = vector_index->GetOwnVectorIndex();
  if (inner_vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", vector_index->Id());
  }

  auto status = inner_vector_index->GetCount(total_vector_count);
  if (!status.ok()) {
    return status;
  }

  status = inner_vector_index->GetDeletedCount(total_deleted_count);
  if (!status.ok()) {
    return status;
  }

  status = inner_vector_index->GetMemorySize(total_memory_usage);
  if (!status.ok()) {
    return status;
  }

  status = GetBorderId(region_range, true, min_id);
  if (!status.ok()) {
    return status;
  }

  status = GetBorderId(region_range, false, max_id);
  if (!status.ok()) {
    return status;
  }

  region_metrics.set_current_count(total_vector_count);
  region_metrics.set_deleted_count(total_deleted_count);
  region_metrics.set_memory_bytes(total_memory_usage);
  region_metrics.set_max_id(max_id);
  region_metrics.set_min_id(min_id);

  return butil::Status();
}

butil::Status VectorReader::VectorCount(const pb::common::Range& range, int64_t& count) {
  const std::string& begin_key = range.start_key();
  const std::string& end_key = range.end_key();

  IteratorOptions options;
  options.upper_bound = end_key;
  auto iter = reader_->NewIterator(Constant::kStoreDataCF, options);
  for (iter->Seek(begin_key); iter->Valid(); iter->Next()) {
    ++count;
  }

  return butil::Status::OK();
}

// GetBorderId
butil::Status VectorReader::GetBorderId(const pb::common::Range& region_range, bool get_min, int64_t& vector_id) {
  const std::string& start_key = region_range.start_key();
  const std::string& end_key = region_range.end_key();

  if (get_min) {
    IteratorOptions options;
    options.lower_bound = start_key;
    options.upper_bound = end_key;
    auto iter = reader_->NewIterator(Constant::kStoreDataCF, options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                      Helper::StringToHex(region_range.start_key()),
                                      Helper::StringToHex(region_range.end_key()));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }

    iter->Seek(start_key);
    if (!iter->Valid()) {
      vector_id = 0;
      return butil::Status();
    }

    std::string key(iter->Key());
    vector_id = VectorCodec::DecodeVectorId(key);
  } else {
    IteratorOptions options;
    options.lower_bound = start_key;
    auto iter = reader_->NewIterator(Constant::kStoreDataCF, options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                      Helper::StringToHex(region_range.start_key()),
                                      Helper::StringToHex(region_range.end_key()));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }

    iter->SeekForPrev(end_key);
    if (iter->Valid()) {
      if (iter->Key() == end_key) {
        iter->Prev();
      }
    }

    if (!iter->Valid()) {
      vector_id = 0;
      return butil::Status();
    }

    std::string key(iter->Key());
    vector_id = VectorCodec::DecodeVectorId(key);
  }

  return butil::Status::OK();
}

// ScanVectorId
butil::Status VectorReader::ScanVectorId(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                         std::vector<int64_t>& vector_ids) {
  std::string seek_key;
  VectorCodec::EncodeVectorKey(ctx->region_range.start_key()[0], ctx->partition_id, ctx->start_id, seek_key);
  std::string range_start_key = ctx->region_range.start_key();
  std::string range_end_key = ctx->region_range.end_key();

  IteratorOptions options;
  if (!ctx->is_reverse) {
    if (seek_key < range_start_key) {
      seek_key = range_start_key;
    }

    if (seek_key >= range_end_key) {
      return butil::Status::OK();
    }

    options.lower_bound = range_start_key;
    options.upper_bound = range_end_key;
    auto iter = reader_->NewIterator(Constant::kStoreDataCF, options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                      Helper::StringToHex(ctx->region_range.start_key()),
                                      Helper::StringToHex(ctx->region_range.end_key()));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }
    for (iter->Seek(seek_key); iter->Valid(); iter->Next()) {
      pb::common::VectorWithId vector;

      std::string key(iter->Key());
      auto vector_id = VectorCodec::DecodeVectorId(key);
      if (vector_id == 0 || vector_id == INT64_MAX || vector_id < 0) {
        continue;
      }

      if (ctx->end_id != 0 && vector_id > ctx->end_id) {
        break;
      }

      if (ctx->use_scalar_filter) {
        bool compare_result = false;
        auto status = CompareVectorScalarData(ctx->region_range, ctx->partition_id, vector_id,
                                              ctx->scalar_data_for_filter, compare_result);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << " CompareVectorScalarData failed, vector_id: " << vector_id
                           << " error: " << status.error_str();
          return status;
        }
        if (!compare_result) {
          continue;
        }
      }

      vector_ids.push_back(vector_id);
      if (vector_ids.size() >= ctx->limit) {
        break;
      }
    }
  } else {
    if (seek_key > range_end_key) {
      seek_key = range_end_key;
    }

    if (seek_key < range_start_key) {
      return butil::Status::OK();
    }

    options.lower_bound = range_start_key;
    auto iter = reader_->NewIterator(Constant::kStoreDataCF, options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                      Helper::StringToHex(ctx->region_range.start_key()),
                                      Helper::StringToHex(ctx->region_range.end_key()));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }

    for (iter->SeekForPrev(seek_key); iter->Valid(); iter->Prev()) {
      if (iter->Key() == range_end_key) {
        continue;
      }

      pb::common::VectorWithId vector;

      std::string key(iter->Key());
      auto vector_id = VectorCodec::DecodeVectorId(key);
      if (vector_id == 0 || vector_id == INT64_MAX || vector_id < 0) {
        continue;
      }

      if (ctx->end_id != 0 && vector_id < ctx->end_id) {
        break;
      }

      if (ctx->use_scalar_filter) {
        bool compare_result = false;
        auto status = CompareVectorScalarData(ctx->region_range, ctx->partition_id, vector_id,
                                              ctx->scalar_data_for_filter, compare_result);
        if (!status.ok()) {
          return status;
        }
        if (!compare_result) {
          continue;
        }
      }

      vector_ids.push_back(vector_id);
      if (vector_ids.size() >= ctx->limit) {
        break;
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForVectorIdPreFilter(  // NOLINT
    VectorIndexWrapperPtr vector_index, const std::vector<pb::common::VectorWithId>& vector_with_ids,
    const pb::common::VectorSearchParameter& parameter, const pb::common::Range& region_range,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {
  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
  auto status =
      VectorReader::SetVectorIndexIdsFilter(vector_index, filters, Helper::PbRepeatedToVector(parameter.vector_ids()));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_str();
    return status;
  }

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForScalarPreFilter(
    VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {  // NOLINT
  // scalar pre filter search
  butil::Status status;
  bool use_coprocessor = parameter.has_vector_coprocessor();

  if (!use_coprocessor && vector_with_ids[0].scalar_data().scalar_data_size() == 0) {
    std::string s = fmt::format("vector_with_ids[0].scalar_data() empty not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  const auto& std_vector_scalar = vector_with_ids[0].scalar_data();
  auto lambda_scalar_compare_function =
      [&std_vector_scalar](const pb::common::VectorScalardata& internal_vector_scalar) {
        for (const auto& [key, value] : std_vector_scalar.scalar_data()) {
          auto it = internal_vector_scalar.scalar_data().find(key);
          if (it == internal_vector_scalar.scalar_data().end()) {
            return false;
          }

          bool compare_result = Helper::IsEqualVectorScalarValue(value, it->second);
          if (!compare_result) {
            return false;
          }
        }
        return true;
      };

  const auto& coprocessor = parameter.vector_coprocessor();

  std::shared_ptr<RawCoprocessor> scalar_coprocessor = std::make_shared<CoprocessorScalar>();

  if (use_coprocessor) {
    status = scalar_coprocessor->Open(CoprocessorPbWrapper{coprocessor});
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "scalar coprocessor::Open failed " << status.error_cstr();
      return status;
    }
  }

  auto lambda_scalar_compare_with_coprocessor_function =
      [&scalar_coprocessor](const pb::common::VectorScalardata& internal_vector_scalar) {
        bool is_reverse = false;
        butil::Status status = scalar_coprocessor->Filter(internal_vector_scalar, is_reverse);
        if (!status.ok()) {
          LOG(ERROR) << "[" << __PRETTY_FUNCTION__ << "] "
                     << "scalar coprocessor::Filter failed " << status.error_cstr();
          return false;
        }
        return is_reverse;
      };

  const std::string& start_key = region_range.start_key();
  const std::string& end_key = region_range.end_key();

  IteratorOptions options;
  options.upper_bound = end_key;

  auto iter = reader_->NewIterator(Constant::kVectorScalarCF, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                    Helper::StringToHex(region_range.start_key()),
                                    Helper::StringToHex(region_range.end_key()));
    return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
  }

  std::vector<int64_t> vector_ids;
  vector_ids.reserve(1024);
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    pb::common::VectorScalardata internal_vector_scalar;
    if (!internal_vector_scalar.ParseFromString(std::string(iter->Value()))) {
      return butil::Status(pb::error::EINTERNAL, "Internal error, decode VectorScalar failed");
    }

    bool compare_result = use_coprocessor ? lambda_scalar_compare_with_coprocessor_function(internal_vector_scalar)
                                          : lambda_scalar_compare_function(internal_vector_scalar);

    if (compare_result) {
      std::string key(iter->Key());
      int64_t internal_vector_id = VectorCodec::DecodeVectorId(key);
      if (0 == internal_vector_id) {
        std::string s = fmt::format("VectorCodec::DecodeVectorId failed key : {}", Helper::StringToHex(key));
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
      }
      vector_ids.push_back(internal_vector_id);
    }
  }

  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
  status = VectorReader::SetVectorIndexIdsFilter(vector_index, filters, vector_ids);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForTableCoprocessor(  // NOLINT(*static)
    [[maybe_unused]] VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    [[maybe_unused]] const std::vector<pb::common::VectorWithId>& vector_with_ids,
    [[maybe_unused]] const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {  // NOLINT
  //   std::string s = fmt::format("vector index search table filter for coprocessor not support now !!! ");
  //   DINGO_LOG(ERROR) << s;
  //   return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);

  DINGO_LOG(DEBUG) << "vector index search table filter for coprocessor support";

  // table pre filter search

  if (!parameter.has_vector_coprocessor()) {
    std::string s = fmt::format("vector_coprocessor empty not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  const auto& coprocessor = parameter.vector_coprocessor();

  std::shared_ptr<RawCoprocessor> table_coprocessor = std::make_shared<CoprocessorV2>();
  butil::Status status;
  status = table_coprocessor->Open(CoprocessorPbWrapper{coprocessor});
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "table coprocessor::Open failed " << status.error_cstr();
    return status;
  }

  auto lambda_table_compare_with_coprocessor_function = [&table_coprocessor](
                                                            const pb::common::VectorTableData& internal_vector_table) {
    bool is_reverse = false;
    butil::Status status =
        table_coprocessor->Filter(internal_vector_table.table_key(), internal_vector_table.table_value(), is_reverse);
    if (!status.ok()) {
      LOG(ERROR) << "[" << __PRETTY_FUNCTION__ << "] "
                 << "Scalar coprocessor::Filter failed " << status.error_cstr();
      return false;
    }
    return is_reverse;
  };

  const std::string& start_key = region_range.start_key();
  const std::string& end_key = region_range.end_key();

  IteratorOptions options;
  options.upper_bound = end_key;

  auto iter = reader_->NewIterator(Constant::kVectorTableCF, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                    Helper::StringToHex(region_range.start_key()),
                                    Helper::StringToHex(region_range.end_key()));
    return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
  }

  std::vector<int64_t> vector_ids;
  vector_ids.reserve(1024);
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    pb::common::VectorTableData internal_vector_table;
    if (!internal_vector_table.ParseFromString(std::string(iter->Value()))) {
      return butil::Status(pb::error::EINTERNAL, "Internal error, decode VectorTable failed");
    }

    bool compare_result = lambda_table_compare_with_coprocessor_function(internal_vector_table);
    if (compare_result) {
      std::string key(iter->Key());
      int64_t internal_vector_id = VectorCodec::DecodeVectorId(key);
      if (0 == internal_vector_id) {
        std::string s = fmt::format("VectorCodec::DecodeVectorId failed key : {}", Helper::StringToHex(key));
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
      }
      vector_ids.push_back(internal_vector_id);
    }
  }

  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
  status = VectorReader::SetVectorIndexIdsFilter(vector_index, filters, vector_ids);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorReader::VectorBatchSearchDebug(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                                   std::vector<pb::index::VectorWithDistanceResult>& results,
                                                   int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                                   int64_t& search_time_us) {  // NOLINT
  // Search vectors by vectors
  auto status =
      SearchVectorDebug(ctx->partition_id, ctx->vector_index, ctx->region_range, ctx->vector_with_ids, ctx->parameter,
                        results, deserialization_id_time_us, scan_scalar_time_us, search_time_us);
  if (!status.ok()) {
    return status;
  }

  if (!ctx->parameter.without_scalar_data()) {
    // Get scalar data by parameter
    std::vector<std::string> selected_scalar_keys = Helper::PbRepeatedToVector(ctx->parameter.selected_keys());
    auto status = QueryVectorScalarData(ctx->region_range, ctx->partition_id, selected_scalar_keys, results);
    if (!status.ok()) {
      return status;
    }
  }

  if (!ctx->parameter.without_table_data()) {
    // Get table data by parameter
    auto status = QueryVectorTableData(ctx->region_range, ctx->partition_id, results);
    if (!status.ok()) {
      return status;
    }
  }

  return butil::Status();
}

butil::Status VectorReader::SearchVectorDebug(
    int64_t partition_id, VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, int64_t& deserialization_id_time_us,
    int64_t& scan_scalar_time_us, int64_t& search_time_us) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "Empty vector with ids";
    return butil::Status();
  }

  auto vector_filter = parameter.vector_filter();
  auto vector_filter_type = parameter.vector_filter_type();

  bool with_vector_data = !(parameter.without_vector_data());
  std::vector<pb::index::VectorWithDistanceResult> tmp_results;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  // scalar post filter
  if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
      dingodb::pb::common::VectorFilterType::QUERY_POST == vector_filter_type) {
    uint32_t top_n = parameter.top_n();
    bool enable_range_search = parameter.enable_range_search();

    if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() == 0) &&
        !parameter.has_vector_coprocessor()) {
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(
          vector_index, region_range, vector_with_ids, parameter, vector_with_distance_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
    } else if (parameter.has_vector_coprocessor()) {
      auto start = lambda_time_now_function();
      if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() != 0)) {
        DINGO_LOG(WARNING) << "vector_with_ids[0].scalar_data() deprecated. use coprocessor.";
      }
      top_n *= 10;
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids,
                                                                       parameter, tmp_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
      auto end = lambda_time_now_function();
      search_time_us = lambda_time_diff_microseconds_function(start, end);

      auto start_kv_get = lambda_time_now_function();
      std::shared_ptr<RawCoprocessor> scalar_coprocessor = std::make_shared<CoprocessorScalar>();

      status = scalar_coprocessor->Open(CoprocessorPbWrapper{parameter.vector_coprocessor()});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << "scalar coprocessor::Open failed " << status.error_cstr();
        return status;
      }

      for (auto& vector_with_distance_result : tmp_results) {
        pb::index::VectorWithDistanceResult new_vector_with_distance_result;

        for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
          int64_t temp_id = temp_vector_with_distance.vector_with_id().id();
          bool compare_result = false;
          butil::Status status = CompareVectorScalarDataWithCoprocessor(region_range, partition_id, temp_id,
                                                                        scalar_coprocessor, compare_result);
          if (!status.ok()) {
            return status;
          }
          if (!compare_result) {
            continue;
          }

          new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
          if (!enable_range_search) {
            if (new_vector_with_distance_result.vector_with_distances_size() >= parameter.top_n()) {
              break;
            }
          }
        }
        vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
      }
      auto end_kv_get = lambda_time_now_function();
      scan_scalar_time_us = lambda_time_diff_microseconds_function(start_kv_get, end_kv_get);
    } else {
      top_n *= 10;
      butil::Status status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids,
                                                                       parameter, tmp_results, top_n, {});
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }

      for (auto& vector_with_distance_result : tmp_results) {
        pb::index::VectorWithDistanceResult new_vector_with_distance_result;

        for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
          int64_t temp_id = temp_vector_with_distance.vector_with_id().id();
          bool compare_result = false;
          butil::Status status = CompareVectorScalarData(region_range, partition_id, temp_id,
                                                         vector_with_ids[0].scalar_data(), compare_result);
          if (!status.ok()) {
            return status;
          }
          if (!compare_result) {
            continue;
          }

          new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
          // topk
          if (!enable_range_search) {
            if (new_vector_with_distance_result.vector_with_distances_size() >= parameter.top_n()) {
              break;
            }
          }
        }
        vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
      }
    }

  } else if (dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER == vector_filter) {  // vector id array search
    butil::Status status = DoVectorSearchForVectorIdPreFilterDebug(vector_index, vector_with_ids, parameter,
                                                                   region_range, vector_with_distance_results,
                                                                   deserialization_id_time_us, search_time_us);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForVectorIdPreFilterDebug failed");
      return status;
    }
  } else if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
             dingodb::pb::common::VectorFilterType::QUERY_PRE == vector_filter_type) {  // scalar pre filter search

    butil::Status status =
        DoVectorSearchForScalarPreFilterDebug(vector_index, region_range, vector_with_ids, parameter,
                                              vector_with_distance_results, scan_scalar_time_us, search_time_us);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForScalarPreFilterDebug failed ");
      return status;
    }
  } else if (dingodb::pb::common::VectorFilter::TABLE_FILTER ==
             vector_filter) {  //  table coprocessor pre filter search. not impl
    butil::Status status = DoVectorSearchForTableCoprocessor(vector_index, region_range, vector_with_ids, parameter,
                                                             vector_with_distance_results);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForTableCoprocessor failed ");
      return status;
    }
  }

  // if vector index does not support restruct vector ,we restruct it using RocksDB
  if (with_vector_data) {
    for (auto& result : vector_with_distance_results) {
      for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
        if (vector_with_distance.vector_with_id().vector().float_values_size() > 0 ||
            vector_with_distance.vector_with_id().vector().binary_values_size() > 0) {
          continue;
        }

        pb::common::VectorWithId vector_with_id;
        auto status = QueryVectorWithId(region_range, partition_id, vector_with_distance.vector_with_id().id(), true,
                                        vector_with_id);
        if (!status.ok()) {
          return status;
        }
        vector_with_distance.mutable_vector_with_id()->Swap(&vector_with_id);
      }
    }
  }

  return butil::Status();
}

butil::Status VectorReader::DoVectorSearchForVectorIdPreFilterDebug(  // NOLINT
    VectorIndexWrapperPtr vector_index, const std::vector<pb::common::VectorWithId>& vector_with_ids,
    const pb::common::VectorSearchParameter& parameter, const pb::common::Range& region_range,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, int64_t& deserialization_id_time_us,
    int64_t& search_time_us) {
  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
  auto start_ids = lambda_time_now_function();
  auto status =
      VectorReader::SetVectorIndexIdsFilter(vector_index, filters, Helper::PbRepeatedToVector(parameter.vector_ids()));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  auto end_ids = lambda_time_now_function();
  deserialization_id_time_us = lambda_time_diff_microseconds_function(start_ids, end_ids);

  auto start_search = lambda_time_now_function();

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  auto end_search = lambda_time_now_function();
  search_time_us = lambda_time_diff_microseconds_function(start_search, end_search);

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForScalarPreFilterDebug(
    VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, int64_t& scan_scalar_time_us,
    int64_t& search_time_us) {
  // scalar pre filter search
  butil::Status status;
#if !defined(ENABLE_SCALAR_WITH_COPROCESSOR)
  const auto& std_vector_scalar = vector_with_ids[0].scalar_data();
  auto lambda_scalar_compare_function =
      [&std_vector_scalar](const pb::common::VectorScalardata& internal_vector_scalar) {
        for (const auto& [key, value] : std_vector_scalar.scalar_data()) {
          auto it = internal_vector_scalar.scalar_data().find(key);
          if (it == internal_vector_scalar.scalar_data().end()) {
            return false;
          }

          bool compare_result = Helper::IsEqualVectorScalarValue(value, it->second);
          if (!compare_result) {
            return false;
          }
        }
        return true;
      };

#else   // ENABLE_SCALAR_WITH_COPROCESSOR
  if (!parameter.has_vector_coprocessor()) {
    std::string s = fmt::format("vector_coprocessor empty not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  const auto& coprocessor = parameter.vector_coprocessor();

  std::shared_ptr<RawCoprocessor> scalar_coprocessor = std::make_shared<CoprocessorScalar>();
  status = scalar_coprocessor->Open(CoprocessorPbWrapper{coprocessor});
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "scalar coprocessor::Open failed " << status.error_cstr();
    return status;
  }

  auto lambda_scalar_compare_with_coprocessor_function =
      [&scalar_coprocessor](const pb::common::VectorScalardata& internal_vector_scalar) {
        bool is_reverse = false;
        butil::Status status = scalar_coprocessor->Filter(internal_vector_scalar, is_reverse);
        if (!status.ok()) {
          LOG(ERROR) << "[" << __PRETTY_FUNCTION__ << "] "
                     << "scalar coprocessor::Filter failed " << status.error_cstr();
          return is_reverse;
        }
        return is_reverse;
      };
#endif  // #if !defined( ENABLE_SCALAR_WITH_COPROCESSOR)

  // std::string start_key = VectorCodec::FillVectorScalarPrefix(region_range.start_key());
  // std::string end_key = VectorCodec::FillVectorScalarPrefix(region_range.end_key());
  const std::string& start_key = region_range.start_key();
  const std::string& end_key = region_range.end_key();

  IteratorOptions options;
  options.upper_bound = end_key;

  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  auto start_iter = lambda_time_now_function();
  auto iter = reader_->NewIterator(Constant::kVectorScalarCF, options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                    Helper::StringToHex(region_range.start_key()),
                                    Helper::StringToHex(region_range.end_key()));
    return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
  }

  std::vector<int64_t> vector_ids;
  vector_ids.reserve(1024);
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    pb::common::VectorScalardata internal_vector_scalar;
    if (!internal_vector_scalar.ParseFromString(std::string(iter->Value()))) {
      return butil::Status(pb::error::EINTERNAL, "Internal error, decode VectorScalar failed");
    }

#if !defined(ENABLE_SCALAR_WITH_COPROCESSOR)
    bool compare_result = lambda_scalar_compare_function(internal_vector_scalar);
#else
    bool compare_result = lambda_scalar_compare_with_coprocessor_function(internal_vector_scalar);
#endif
    if (compare_result) {
      std::string key(iter->Key());
      int64_t internal_vector_id = VectorCodec::DecodeVectorId(key);
      if (0 == internal_vector_id) {
        std::string s = fmt::format("VectorCodec::DecodeVectorId failed key : {}", Helper::StringToHex(key));
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
      }
      vector_ids.push_back(internal_vector_id);
    }
  }
  auto end_iter = lambda_time_now_function();
  scan_scalar_time_us = lambda_time_diff_microseconds_function(start_iter, end_iter);

  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;

  status = VectorReader::SetVectorIndexIdsFilter(vector_index, filters, vector_ids);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  auto start_search = lambda_time_now_function();

  status = VectorReader::SearchAndRangeSearchWrapper(vector_index, region_range, vector_with_ids, parameter,
                                                     vector_with_distance_results, parameter.top_n(), filters);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  auto end_search = lambda_time_now_function();
  search_time_us = lambda_time_diff_microseconds_function(start_search, end_search);
  return butil::Status::OK();
}

butil::Status VectorReader::SetVectorIndexIdsFilter(VectorIndexWrapperPtr /*vector_index*/,
                                                    std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                                    const std::vector<int64_t>& vector_ids) {
  filters.push_back(std::make_shared<VectorIndex::ConcreteFilterFunctor>(vector_ids));
  return butil::Status::OK();
}

butil::Status VectorReader::SearchAndRangeSearchWrapper(
    VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, uint32_t topk,
    std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters) {
  bool with_vector_data = !(parameter.without_vector_data());
  bool enable_range_search = parameter.enable_range_search();
  float radius = parameter.radius();
  butil::Status status;

  // if vector index does not support restruct vector ,we restruct it using RocksDB
  // if use_brute_force is true, we use brute force search, else we call vector index search, if vector index not
  // support, then use brute force search again to get result
  if (parameter.use_brute_force()) {
    if (enable_range_search) {
      status = BruteForceRangeSearch(vector_index, vector_with_ids, radius, region_range, filters, with_vector_data,
                                     parameter, vector_with_distance_results);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("RangeSearch vector index failed, error: {} {}", status.error_code(),
                                        status.error_str());
        return status;
      }
    } else {
      status = BruteForceSearch(vector_index, vector_with_ids, topk, region_range, filters, with_vector_data, parameter,
                                vector_with_distance_results);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Search vector index failed, error: {} {}", status.error_code(),
                                        status.error_str());
        return status;
      }
    }
  } else {
    if (enable_range_search) {
      status = vector_index->RangeSearch(vector_with_ids, radius, region_range, filters, with_vector_data, parameter,
                                         vector_with_distance_results);
      if (status.error_code() == pb::error::Errno::EVECTOR_NOT_SUPPORT) {
        DINGO_LOG(INFO) << "RangeSearch vector index not support, try brute force, id: " << vector_index->Id();
        return BruteForceRangeSearch(vector_index, vector_with_ids, radius, region_range, filters, with_vector_data,
                                     parameter, vector_with_distance_results);
      } else if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("RangeSearch vector index failed, error: {} {}", status.error_code(),
                                        status.error_str());
        return status;
      }
    } else {
      status = vector_index->Search(vector_with_ids, topk, region_range, filters, with_vector_data, parameter,
                                    vector_with_distance_results);
      if (status.error_code() == pb::error::Errno::EVECTOR_NOT_SUPPORT) {
        DINGO_LOG(DEBUG) << "Search vector index not support, try brute force, id: " << vector_index->Id();
        return BruteForceSearch(vector_index, vector_with_ids, topk, region_range, filters, with_vector_data, parameter,
                                vector_with_distance_results);
      } else if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Search vector index failed, error: {} {}", status.error_code(),
                                        status.error_str());
        return status;
      }
    }
  }

  return butil::Status::OK();
}

// DistanceResult
// This class is used for priority queue to merge the search result from many batch scan data from raw engine.
class DistanceResult {
 public:
  float distance{0};
  pb::common::VectorWithDistance vector_with_distance{};
  DistanceResult() = default;
  DistanceResult(float distance, pb::common::VectorWithDistance vector_with_distance)
      : distance(distance), vector_with_distance(vector_with_distance) {}
};

// Overload the < operator.
bool operator<(const DistanceResult& result1, const DistanceResult& result2) {
  if (result1.distance != result2.distance) {
    return result1.distance < result2.distance;
  } else {
    return result1.vector_with_distance.vector_with_id().id() < result2.vector_with_distance.vector_with_id().id();
  }
}

// Overload the > operator.
bool operator>(const DistanceResult& result1, const DistanceResult& result2) {
  if (result1.distance != result2.distance) {
    return result1.distance > result2.distance;
  } else {
    return result1.vector_with_distance.vector_with_id().id() > result2.vector_with_distance.vector_with_id().id();
  }
}

// ScanData from raw engine, build vector index and search
butil::Status VectorReader::BruteForceSearch(VectorIndexWrapperPtr vector_index,
                                             const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                             uint32_t topk, const pb::common::Range& region_range,
                                             std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                             bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                             std::vector<pb::index::VectorWithDistanceResult>& results) {
  auto metric_type = vector_index->GetMetricType();
  auto dimension = vector_index->GetDimension();

  pb::common::RegionEpoch epoch;
  pb::common::VectorIndexParameter index_parameter;
  index_parameter.mutable_flat_parameter()->set_dimension(dimension);
  index_parameter.mutable_flat_parameter()->set_metric_type(metric_type);

  IteratorOptions options;
  options.lower_bound = region_range.start_key();
  options.upper_bound = region_range.end_key();
  auto iterator = reader_->NewIterator(Constant::kVectorDataCF, options);

  iterator->Seek(region_range.start_key());
  if (!iterator->Valid()) {
    return butil::Status();
  }

  BvarLatencyGuard bvar_guard(&g_bruteforce_search_latency);

  // topk results
  std::vector<std::priority_queue<DistanceResult>> top_results;
  top_results.resize(vector_with_ids.size());

  int64_t count = 0;
  std::vector<pb::common::VectorWithId> vector_with_id_batch;
  std::vector<pb::index::VectorWithDistanceResult> results_batch;

  // scan data from raw engine
  while (iterator->Valid()) {
    std::string key(iterator->Key());
    auto vector_id = VectorCodec::DecodeVectorId(key);
    if (vector_id == 0 || vector_id == INT64_MAX || vector_id < 0) {
      iterator->Next();
      continue;
    }

    auto value = iterator->Value();

    pb::common::Vector vector;
    if (!vector.ParseFromArray(value.data(), value.size())) {
      return butil::Status(pb::error::EINTERNAL, "Parse proto from string error");
    }
    pb::common::VectorWithId vector_with_id;
    vector_with_id.mutable_vector()->Swap(&vector);
    vector_with_id.set_id(vector_id);

    vector_with_id_batch.push_back(vector_with_id);

    if (vector_with_id_batch.size() == FLAGS_vector_index_bruteforce_batch_count) {
      auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
      auto flat_index = VectorIndexFactory::NewFlat(INT64_MAX, index_parameter, epoch, region_range, thread_pool);
      if (flat_index == nullptr) {
        DINGO_LOG(FATAL) << "flat_index is nullptr";
      }

      auto ret = flat_index->Add(vector_with_id_batch);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Add vector to flat index failed, error: {} {}", ret.error_code(),
                                        ret.error_str());
        return ret;
      }

      ret = flat_index->Search(vector_with_ids, topk, filters, reconstruct, parameter, results_batch);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Search vector index failed, error: {} {}", ret.error_code(), ret.error_str());
        return ret;
      }

      CHECK(results_batch.size() == vector_with_ids.size());

      for (int i = 0; i < results_batch.size(); i++) {
        auto& result = results_batch[i];
        auto& top_result = top_results[i];

        for (const auto& vector_with_distance : result.vector_with_distances()) {
          auto& top_result = top_results[i];
          if (top_result.size() < topk) {
            top_result.emplace(vector_with_distance.distance(), vector_with_distance);
          } else {
            if (top_result.top().distance > vector_with_distance.distance()) {
              top_result.pop();
              top_result.emplace(vector_with_distance.distance(), vector_with_distance);
            }
          }
        }
      }

      results_batch.clear();
      vector_with_id_batch.clear();
    }

    iterator->Next();
  }

  if (!vector_with_id_batch.empty()) {
    auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
    auto flat_index = VectorIndexFactory::NewFlat(INT64_MAX, index_parameter, epoch, region_range, thread_pool);
    if (flat_index == nullptr) {
      DINGO_LOG(FATAL) << "flat_index is nullptr";
    }

    auto ret = flat_index->Add(vector_with_id_batch);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Add vector to flat index failed, error: {} {}", ret.error_code(),
                                      ret.error_str());
      return ret;
    }

    ret = flat_index->Search(vector_with_ids, topk, filters, reconstruct, parameter, results_batch);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Search vector index failed, error: {} {}", ret.error_code(), ret.error_str());
      return ret;
    }

    CHECK(results_batch.size() == vector_with_ids.size());

    for (int i = 0; i < results_batch.size(); i++) {
      auto& result = results_batch[i];
      auto& top_result = top_results[i];

      for (const auto& vector_with_distance : result.vector_with_distances()) {
        auto& top_result = top_results[i];
        if (top_result.size() < topk) {
          top_result.emplace(vector_with_distance.distance(), vector_with_distance);
        } else {
          if (top_result.top().distance > vector_with_distance.distance()) {
            top_result.pop();
            top_result.emplace(vector_with_distance.distance(), vector_with_distance);
          }
        }
      }
    }

    results_batch.clear();
    vector_with_id_batch.clear();
  }

  // copy top_results to results
  // we don't do sorting by distance here
  // the client will do sorting by distance
  results.resize(top_results.size());

  for (int i = 0; i < top_results.size(); i++) {
    auto& top_result = top_results[i];
    auto& result = results[i];

    std::deque<pb::common::VectorWithDistance> vector_with_distances_deque;

    while (!top_result.empty()) {
      auto top = top_result.top();
      vector_with_distances_deque.emplace_front(top.vector_with_distance);
      top_result.pop();
    }

    while (!vector_with_distances_deque.empty()) {
      auto& vector_with_distance = *result.add_vector_with_distances();
      vector_with_distance.Swap(&vector_with_distances_deque.front());
      vector_with_distances_deque.pop_front();
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::BruteForceRangeSearch(VectorIndexWrapperPtr vector_index,
                                                  const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                  float radius, const pb::common::Range& region_range,
                                                  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                                                  bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                                  std::vector<pb::index::VectorWithDistanceResult>& results) {
  auto metric_type = vector_index->GetMetricType();
  auto dimension = vector_index->GetDimension();

  pb::common::RegionEpoch epoch;
  pb::common::VectorIndexParameter index_parameter;
  index_parameter.mutable_flat_parameter()->set_dimension(dimension);
  index_parameter.mutable_flat_parameter()->set_metric_type(metric_type);

  IteratorOptions options;
  options.lower_bound = region_range.start_key();
  options.upper_bound = region_range.end_key();
  auto iterator = reader_->NewIterator(Constant::kVectorDataCF, options);

  iterator->Seek(region_range.start_key());
  if (!iterator->Valid()) {
    return butil::Status();
  }

  BvarLatencyGuard bvar_guard(&g_bruteforce_range_search_latency);

  // range search results
  std::vector<std::vector<std::pair<float, pb::common::VectorWithDistance>>> range_rsults;
  range_rsults.resize(vector_with_ids.size());

  int64_t count = 0;
  std::vector<pb::common::VectorWithId> vector_with_id_batch;
  std::vector<pb::index::VectorWithDistanceResult> results_batch;

  // scan data from raw engine
  while (iterator->Valid()) {
    std::string key(iterator->Key());
    auto vector_id = VectorCodec::DecodeVectorId(key);
    if (vector_id == 0 || vector_id == INT64_MAX || vector_id < 0) {
      iterator->Next();
      continue;
    }

    auto value = iterator->Value();

    pb::common::Vector vector;
    if (!vector.ParseFromArray(value.data(), value.size())) {
      return butil::Status(pb::error::EINTERNAL, "Parse proto from string error");
    }
    pb::common::VectorWithId vector_with_id;
    vector_with_id.mutable_vector()->Swap(&vector);
    vector_with_id.set_id(vector_id);

    vector_with_id_batch.push_back(vector_with_id);

    if (vector_with_id_batch.size() == FLAGS_vector_index_bruteforce_batch_count) {
      auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
      auto flat_index = VectorIndexFactory::NewFlat(INT64_MAX, index_parameter, epoch, region_range, thread_pool);
      if (flat_index == nullptr) {
        DINGO_LOG(FATAL) << "flat_index is nullptr";
      }

      auto ret = flat_index->Add(vector_with_id_batch);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Add vector to flat index failed, error: {} {}", ret.error_code(),
                                        ret.error_str());
        return ret;
      }

      ret = flat_index->RangeSearch(vector_with_ids, radius, filters, reconstruct, parameter, results_batch);
      if (!ret.ok()) {
        DINGO_LOG(ERROR) << fmt::format("RangeSearch vector index failed, error: {} {}", ret.error_code(),
                                        ret.error_str());
        return ret;
      }

      CHECK(results_batch.size() == vector_with_ids.size());

      for (int i = 0; i < results_batch.size(); i++) {
        auto& result = results_batch[i];
        auto& top_result = range_rsults[i];

        for (const auto& vector_with_distance : result.vector_with_distances()) {
          auto& top_result = range_rsults[i];
          if (top_result.size() < FLAGS_vector_index_max_range_search_result_count) {
            top_result.emplace_back(vector_with_distance.distance(), vector_with_distance);
          } else {
            DINGO_LOG(WARNING) << fmt::format("RangeSearch result count exceed limit, limit: {}, actual: {}",
                                              FLAGS_vector_index_max_range_search_result_count, top_result.size() + 1);
            break;
          }
        }
      }

      results_batch.clear();
      vector_with_id_batch.clear();
    }

    iterator->Next();
  }

  if (!vector_with_id_batch.empty()) {
    auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
    auto flat_index = VectorIndexFactory::NewFlat(INT64_MAX, index_parameter, epoch, region_range, thread_pool);
    if (flat_index == nullptr) {
      DINGO_LOG(FATAL) << "flat_index is nullptr";
    }

    auto ret = flat_index->Add(vector_with_id_batch);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Add vector to flat index failed, error: {} {}", ret.error_code(),
                                      ret.error_str());
      return ret;
    }

    ret = flat_index->RangeSearch(vector_with_ids, radius, filters, reconstruct, parameter, results_batch);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("RangeSearch vector index failed, error: {} {}", ret.error_code(),
                                      ret.error_str());
      return ret;
    }

    CHECK(results_batch.size() == vector_with_ids.size());

    for (int i = 0; i < results_batch.size(); i++) {
      auto& result = results_batch[i];
      auto& top_result = range_rsults[i];

      for (const auto& vector_with_distance : result.vector_with_distances()) {
        auto& top_result = range_rsults[i];
        if (top_result.size() < FLAGS_vector_index_max_range_search_result_count) {
          top_result.emplace_back(vector_with_distance.distance(), vector_with_distance);
        } else {
          DINGO_LOG(WARNING) << fmt::format("RangeSearch result count exceed limit, limit: {}, actual: {}",
                                            FLAGS_vector_index_max_range_search_result_count, top_result.size() + 1);
          break;
        }
      }
    }

    results_batch.clear();
    vector_with_id_batch.clear();
  }

  // copy top_results to results
  // we don't do sorting by distance here
  // the client will do sorting by distance
  results.resize(range_rsults.size());
  for (int i = 0; i < range_rsults.size(); i++) {
    auto& top_result = range_rsults[i];
    auto& result = results[i];

    for (auto& top : top_result) {
      auto& vector_with_distance = *result.add_vector_with_distances();
      vector_with_distance.Swap(&top.second);
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb

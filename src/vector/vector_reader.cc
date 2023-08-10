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
#include <memory>
#include <string>
#include <vector>

#include "common/helper.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_index.h"

namespace dingodb {

butil::Status VectorReader::QueryVectorWithId(uint64_t partition_id, uint64_t vector_id, bool with_vector_data,
                                              pb::common::VectorWithId& vector_with_id) {
  std::string key;
  VectorCodec::EncodeVectorData(partition_id, vector_id, key);

  std::string value;
  auto status = reader_->KvGet(key, value);
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
    uint64_t partition_id, std::shared_ptr<VectorIndex> vector_index, pb::common::Range region_range,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "Empty vector with ids";
    return butil::Status();
  }

  uint32_t top_n = parameter.top_n();
  bool use_scalar_filter = parameter.use_scalar_filter();

  auto vector_filter = parameter.vector_filter();
  auto vector_filter_type = parameter.vector_filter_type();

  if (use_scalar_filter) {
    if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
        dingodb::pb::common::VectorFilterType::QUERY_POST == vector_filter_type) {
      if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() == 0)) {
        return butil::Status(
            pb::error::EVECTOR_SCALAR_DATA_NOT_FOUND,
            fmt::format("Not found vector scalar data, region: {}, vector id: {}", region_id, vector_with_ids[0].id()));
      }
      top_n *= 10;
    }
  }

  bool with_vector_data = !(parameter.without_vector_data());
  std::vector<pb::index::VectorWithDistanceResult> tmp_results;

  uint64_t min_vector_id = VectorCodec::DecodeVectorId(region_range.start_key());
  uint64_t max_vector_id = VectorCodec::DecodeVectorId(region_range.end_key());
  DINGO_LOG(INFO) << fmt::format("vector id range [{}-{})", min_vector_id, max_vector_id);
  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters;
  filters.push_back(std::make_shared<VectorIndex::RangeFilterFunctor>(min_vector_id, max_vector_id));
  vector_index->Search(vector_with_ids, top_n, filters, tmp_results, with_vector_data);

  if (use_scalar_filter) {
    // scalar post filter
    if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
        dingodb::pb::common::VectorFilterType::QUERY_POST == vector_filter_type) {
      vector_index->Search(vector_with_ids, top_n, tmp_results, with_vector_data);
      for (auto& vector_with_distance_result : tmp_results) {
        pb::index::VectorWithDistanceResult new_vector_with_distance_result;

        for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
          uint64_t temp_id = temp_vector_with_distance.vector_with_id().id();
          bool compare_result = false;
          butil::Status status =
              CompareVectorScalarData(partition_id, temp_id, vector_with_ids[0].scalar_data(), compare_result);
          if (!status.ok()) {
            return status;
          }
          if (!compare_result) {
            continue;
          }

          new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
        }

        vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
      }
    } else if (dingodb::pb::common::VectorFilter::VECTOR_ID_FILTER == vector_filter) {  // vector id array search
      butil::Status status =
          DoVectorSearchForVectorIdPreFilter(vector_index, vector_with_ids, parameter, vector_with_distance_results);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForVectorIdPreFilter failed");
        return status;
      }

    } else if (dingodb::pb::common::VectorFilter::SCALAR_FILTER == vector_filter &&
               dingodb::pb::common::VectorFilterType::QUERY_PRE == vector_filter_type) {  // scalar pre filter search

      butil::Status status = DoVectorSearchForScalarPreFilter(vector_index, partition_id, vector_with_ids, parameter,
                                                              vector_with_distance_results);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForScalarPreFilter failed ");
        return status;
      }

    } else if (dingodb::pb::common::VectorFilter::TABLE_FILTER ==
               vector_filter) {  //  table coprocessor pre filter search. not impl
      butil::Status status = DoVectorSearchForTableCoprocessor(vector_index, region_id, vector_with_ids, parameter,
                                                               vector_with_distance_results);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("DoVectorSearchForTableCoprocessor failed ");
        return status;
      }
    }
  } else {  // no filter
    vector_index->Search(vector_with_ids, top_n, tmp_results, with_vector_data);
    vector_with_distance_results.swap(tmp_results);
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
        auto status = QueryVectorWithId(partition_id, vector_with_distance.vector_with_id().id(), true, vector_with_id);
        if (!status.ok()) {
          return status;
        }
        vector_with_distance.mutable_vector_with_id()->Swap(&vector_with_id);
      }
    }
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorTableData(uint64_t partition_id, pb::common::VectorWithId& vector_with_id) {
  std::string key, value;
  VectorCodec::EncodeVectorTable(partition_id, vector_with_id.id(), key);

  auto status = reader_->KvGet(key, value);
  if (!status.ok()) {
    return status;
  }

  pb::common::VectorTableData vector_table;
  if (!vector_table.ParseFromString(value)) {
    return butil::Status(pb::error::EINTERNAL, "Decode vector table data failed");
  }

  vector_with_id.mutable_table_data()->CopyFrom(vector_table);

  return butil::Status();
}

butil::Status VectorReader::QueryVectorTableData(uint64_t partition_id,
                                                 std::vector<pb::index::VectorWithDistanceResult>& results) {
  // get metadata by parameter
  for (auto& result : results) {
    for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
      pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
      QueryVectorTableData(partition_id, vector_with_id);
    }
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorTableData(uint64_t partition_id,
                                                 std::vector<pb::common::VectorWithDistance>& vector_with_distances) {
  // get metadata by parameter
  for (auto& vector_with_distance : vector_with_distances) {
    pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
    QueryVectorTableData(partition_id, vector_with_id);
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorScalarData(uint64_t partition_id, std::vector<std::string> selected_scalar_keys,
                                                  pb::common::VectorWithId& vector_with_id) {
  std::string key, value;
  VectorCodec::EncodeVectorScalar(partition_id, vector_with_id.id(), key);

  auto status = reader_->KvGet(key, value);
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

butil::Status VectorReader::QueryVectorScalarData(uint64_t partition_id, std::vector<std::string> selected_scalar_keys,
                                                  std::vector<pb::index::VectorWithDistanceResult>& results) {
  // get metadata by parameter
  for (auto& result : results) {
    for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
      pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
      QueryVectorScalarData(partition_id, selected_scalar_keys, vector_with_id);
    }
  }

  return butil::Status();
}

butil::Status VectorReader::QueryVectorScalarData(uint64_t partition_id, std::vector<std::string> selected_scalar_keys,
                                                  std::vector<pb::common::VectorWithDistance>& vector_with_distances) {
  // get metadata by parameter
  for (auto& vector_with_distance : vector_with_distances) {
    pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
    QueryVectorScalarData(partition_id, selected_scalar_keys, vector_with_id);
  }

  return butil::Status();
}

butil::Status VectorReader::CompareVectorScalarData(uint64_t partition_id, uint64_t vector_id,
                                                    const pb::common::VectorScalardata& source_scalar_data,
                                                    bool& compare_result) {
  compare_result = false;
  std::string key, value;

  VectorCodec::EncodeVectorScalar(partition_id, vector_id, key);

  auto status = reader_->KvGet(key, value);
  if (!status.ok()) {
    return status;
  }

  pb::common::VectorScalardata vector_scalar;
  if (!vector_scalar.ParseFromString(value)) {
    return butil::Status(pb::error::EINTERNAL, "Decode vector scalar data failed");
  }

  for (const auto& [key, value] : source_scalar_data.scalar_data()) {
    auto it = vector_scalar.scalar_data().find(key);
    if (it == vector_scalar.scalar_data().end()) {
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

butil::Status VectorReader::VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                              std::vector<pb::index::VectorWithDistanceResult>& results) {  // NOLINT
  // Search vectors by vectors
  auto status = SearchVector(ctx->partition_id, ctx->vector_index, ctx->region_range, ctx->vector_with_ids,
                             ctx->parameter, results);
  if (!status.ok()) {
    return status;
  }

  if (ctx->parameter.with_scalar_data()) {
    // Get scalar data by parameter
    std::vector<std::string> selected_scalar_keys = Helper::PbRepeatedToVector(ctx->parameter.selected_keys());
    auto status = QueryVectorScalarData(ctx->partition_id, selected_scalar_keys, results);
    if (!status.ok()) {
      return status;
    }
  }

  if (ctx->parameter.with_table_data()) {
    // Get table data by parameter
    auto status = QueryVectorTableData(ctx->partition_id, results);
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
    auto status = QueryVectorWithId(ctx->partition_id, vector_id, ctx->with_vector_data, vector_with_id);
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format("Query vector_with_id failed, vector_id: {} error: {} ", vector_id,
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

      auto status = QueryVectorScalarData(ctx->partition_id, ctx->selected_scalar_keys, vector_with_id);
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

      auto status = QueryVectorTableData(ctx->partition_id, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector table data failed, vector_id: {} error: {} ",
                                          vector_with_id.id(), status.error_str());
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::VectorGetBorderId(const pb::common::Range& region_range, bool get_min,
                                              uint64_t& vector_id) {
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
  std::vector<uint64_t> vector_ids;
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
    auto status = QueryVectorWithId(ctx->partition_id, vector_id, ctx->with_vector_data, vector_with_id);
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

      auto status = QueryVectorScalarData(ctx->partition_id, ctx->selected_scalar_keys, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector scalar data failed, vector_id {} error: {]",
                                          vector_with_id.id(), status.error_str());
      }
    }
  }

  if (ctx->with_table_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status = QueryVectorTableData(ctx->partition_id, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("Query vector table data failed, vector_id {} error: {]", vector_with_id.id(),
                                          status.error_str());
      }
    }
  }

  return butil::Status::OK();
}

butil::Status VectorReader::VectorGetRegionMetrics(uint64_t region_id, const pb::common::Range& region_range,
                                                   std::shared_ptr<VectorIndex> vector_index,
                                                   pb::common::VectorIndexMetrics& region_metrics) {
  auto vector_index_manager = Server::GetInstance()->GetVectorIndexManager();
  if (vector_index_manager == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, fmt::format("Not found vector index mgr {}", region_id));
  }

  uint64_t total_vector_count = 0;
  uint64_t total_deleted_count = 0;
  uint64_t total_memory_usage = 0;
  uint64_t max_id = 0;
  uint64_t min_id = 0;

  vector_index->GetCount(total_vector_count);
  vector_index->GetDeletedCount(total_deleted_count);
  vector_index->GetMemorySize(total_memory_usage);

  GetBorderId(region_range, true, min_id);
  GetBorderId(region_range, false, max_id);

  region_metrics.set_current_count(total_vector_count);
  region_metrics.set_deleted_count(total_deleted_count);
  region_metrics.set_memory_bytes(total_memory_usage);
  region_metrics.set_max_id(max_id);
  region_metrics.set_min_id(min_id);

  return butil::Status();
}

// GetBorderId
butil::Status VectorReader::GetBorderId(const pb::common::Range& region_range, bool get_min, uint64_t& vector_id) {
  std::string start_key = VectorCodec::FillVectorDataPrefix(region_range.start_key());
  std::string end_key = VectorCodec::FillVectorDataPrefix(region_range.end_key());

  if (get_min) {
    IteratorOptions options;
    options.upper_bound = end_key;
    auto iter = reader_->NewIterator(options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                      Helper::StringToHex(region_range.start_key()),
                                      Helper::StringToHex(region_range.end_key()));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }

    iter->Seek(start_key);
    if (!iter->Valid()) {
      return butil::Status(pb::error::Errno::EINTERNAL, "Seek start_key failed");
    }

    std::string key(iter->Key());
    vector_id = VectorCodec::DecodeVectorId(key);
  } else {
    IteratorOptions options;
    options.lower_bound = start_key;
    auto iter = reader_->NewIterator(options);
    if (iter == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                      Helper::StringToHex(region_range.start_key()),
                                      Helper::StringToHex(region_range.end_key()));
      return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
    }
    iter->SeekForPrev(end_key);
    if (!iter->Valid()) {
      return butil::Status(pb::error::Errno::EINTERNAL, "Seek end_key failed");
    }

    std::string key(iter->Key());
    vector_id = VectorCodec::DecodeVectorId(key);
  }

  return butil::Status::OK();
}

// ScanVectorId
butil::Status VectorReader::ScanVectorId(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                         std::vector<uint64_t>& vector_ids) {
  std::string seek_key;
  VectorCodec::EncodeVectorData(ctx->partition_id, ctx->start_id, seek_key);

  IteratorOptions options;
  options.lower_bound = VectorCodec::FillVectorDataPrefix(ctx->region_range.start_key());
  options.upper_bound = VectorCodec::FillVectorDataPrefix(ctx->region_range.end_key());

  auto iter = reader_->NewIterator(options);
  if (iter == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("New iterator failed, region range [{}-{})",
                                    Helper::StringToHex(ctx->region_range.start_key()),
                                    Helper::StringToHex(ctx->region_range.end_key()));
    return butil::Status(pb::error::Errno::EINTERNAL, "New iterator failed");
  }

  if (!ctx->is_reverse) {
    for (iter->Seek(seek_key); iter->Valid(); iter->Next()) {
      pb::common::VectorWithId vector;

      std::string key(iter->Key());
      auto vector_id = VectorCodec::DecodeVectorId(key);

      if (ctx->use_scalar_filter) {
        bool compare_result = false;
        auto status =
            CompareVectorScalarData(ctx->partition_id, vector_id, ctx->scalar_data_for_filter, compare_result);
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
  } else {
    for (iter->SeekForPrev(seek_key); iter->Valid(); iter->Prev()) {
      pb::common::VectorWithId vector;

      std::string key(iter->Key());
      auto vector_id = VectorCodec::DecodeVectorId(key);
      if (vector_id == 0 || vector_id == UINT64_MAX) {
        continue;
      }

      if (ctx->use_scalar_filter) {
        bool compare_result = false;
        auto status =
            CompareVectorScalarData(ctx->partition_id, vector_id, ctx->scalar_data_for_filter, compare_result);
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

butil::Status VectorReader::DoVectorSearchForVectorIdPreFilter(
    std::shared_ptr<VectorIndex> vector_index, const std::vector<pb::common::VectorWithId>& vector_with_ids,
    const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {  // NOLINT

  uint32_t top_n = parameter.top_n();
  bool with_vector_data = !(parameter.without_vector_data());

  const ::google::protobuf::RepeatedField<uint64_t>& vector_ids = parameter.vector_ids();
  std::vector<uint64_t> internal_vector_ids(vector_ids.begin(), vector_ids.end());

  butil::Status status =
      vector_index->Search(vector_with_ids, top_n, vector_with_distance_results, with_vector_data, internal_vector_ids);
  if (!status.ok()) {
    std::string s = fmt::format("DoVectorSearchForVectorIdPreFilter::VectorIndex::Search failed");
    DINGO_LOG(ERROR) << s;
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForScalarPreFilter(
    std::shared_ptr<VectorIndex> vector_index, uint64_t partition_id,
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {  // NOLINT

  // scalar pre filter search

  uint32_t top_n = parameter.top_n();
  bool with_vector_data = !(parameter.without_vector_data());

  std::vector<uint64_t> vector_ids;
  vector_ids.reserve(1024);
  std::string start_key;
  std::string end_key;
  VectorCodec::EncodeVectorScalar(partition_id, 0, start_key);
  VectorCodec::EncodeVectorScalar(partition_id, UINT64_MAX, end_key);

  auto scalar_iter = reader_->NewIterator(start_key, end_key);

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

  scalar_iter->Start();
  while (scalar_iter->HasNext()) {
    std::string key;
    std::string value;
    scalar_iter->GetKV(key, value);

    uint64_t internal_vector_id = 0;

    internal_vector_id = VectorCodec::DecodeVectorId(key);
    if (0 == internal_vector_id) {
      std::string s = fmt::format("VectorCodec::DecodeVectorId failed key : {}", Helper::StringToHex(key));
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
    }

    pb::common::VectorScalardata internal_vector_scalar;
    if (!internal_vector_scalar.ParseFromString(value)) {
      return butil::Status(pb::error::EINTERNAL, "Internal error, decode VectorScalar failed");
    }

    bool compare_result = lambda_scalar_compare_function(internal_vector_scalar);
    if (compare_result) {
      vector_ids.push_back(internal_vector_id);
    }

    scalar_iter->Next();
  }

  butil::Status status =
      vector_index->Search(vector_with_ids, top_n, vector_with_distance_results, with_vector_data, vector_ids);
  if (!status.ok()) {
    std::string s = fmt::format("DoVectorSearchForScalarPreFilter::VectorIndex::Search failed");
    DINGO_LOG(ERROR) << s;
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorReader::DoVectorSearchForTableCoprocessor(
    [[maybe_unused]] std::shared_ptr<VectorIndex> vector_index, [[maybe_unused]] uint64_t partition_id,
    [[maybe_unused]] const std::vector<pb::common::VectorWithId>& vector_with_ids,
    [[maybe_unused]] const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {  // NOLINT
  std::string s = fmt::format("vector index search table filter for coprocessor not support now !!! ");
  DINGO_LOG(ERROR) << s;
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
}

}  // namespace dingodb
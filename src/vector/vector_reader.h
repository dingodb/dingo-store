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

#ifndef DINGODB_VECTOR_READER_H_
#define DINGODB_VECTOR_READER_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>

#include "butil/status.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

// Vector reader
class VectorReader {
 public:
  // VectorReader(std::shared_ptr<RawEngine::Reader> reader) : reader_(reader) {}
  VectorReader(std::shared_ptr<RawEngine::Reader> vector_data_reader,
               std::shared_ptr<RawEngine::Reader> vector_scalar_reader,
               std::shared_ptr<RawEngine::Reader> vector_table_reader)
      : vector_data_reader_(vector_data_reader),
        vector_scalar_reader_(vector_scalar_reader),
        vector_table_reader_(vector_table_reader) {}

  // static std::shared_ptr<VectorReader> New(std::shared_ptr<RawEngine::Reader> reader) {
  //   return std::make_shared<VectorReader>(reader);
  // }
  static std::shared_ptr<VectorReader> New(std::shared_ptr<RawEngine::Reader> vector_data_reader,
                                           std::shared_ptr<RawEngine::Reader> vector_scalar_reader,
                                           std::shared_ptr<RawEngine::Reader> vector_table_reader) {
    return std::make_shared<VectorReader>(vector_data_reader, vector_scalar_reader, vector_table_reader);
  }

  butil::Status VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                  std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                 std::vector<pb::common::VectorWithId>& vector_with_ids);

  butil::Status VectorGetBorderId(const pb::common::Range& region_range, bool get_min, int64_t& vector_id);

  butil::Status VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                std::vector<pb::common::VectorWithId>& vector_with_ids);

  butil::Status VectorGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                       VectorIndexWrapperPtr vector_index,
                                       pb::common::VectorIndexMetrics& region_metrics);

  butil::Status VectorCount(const pb::common::Range& range, int64_t& count);

  // This function is for testing only
  butil::Status VectorBatchSearchDebug(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                       std::vector<pb::index::VectorWithDistanceResult>& results,
                                       int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                       int64_t& search_time_us);

 private:
  butil::Status QueryVectorWithId(const pb::common::Range& region_range, int64_t partition_id, int64_t vector_id,
                                  bool with_vector_data, pb::common::VectorWithId& vector_with_id);
  butil::Status SearchVector(int64_t partition_id, VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
                             const std::vector<pb::common::VectorWithId>& vector_with_ids,
                             const pb::common::VectorSearchParameter& parameter,
                             std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

  butil::Status QueryVectorScalarData(const pb::common::Range& region_range, int64_t partition_id,
                                      std::vector<std::string> selected_scalar_keys,
                                      pb::common::VectorWithId& vector_with_id);
  butil::Status QueryVectorScalarData(const pb::common::Range& region_range, int64_t partition_id,
                                      std::vector<std::string> selected_scalar_keys,
                                      std::vector<pb::common::VectorWithDistance>& vector_with_distances);
  butil::Status QueryVectorScalarData(const pb::common::Range& region_range, int64_t partition_id,
                                      std::vector<std::string> selected_scalar_keys,
                                      std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status CompareVectorScalarData(const pb::common::Range& region_range, int64_t partition_id, int64_t vector_id,
                                        const pb::common::VectorScalardata& source_scalar_data, bool& compare_result);

  butil::Status QueryVectorTableData(const pb::common::Range& region_range, int64_t partition_id,
                                     pb::common::VectorWithId& vector_with_id);
  butil::Status QueryVectorTableData(const pb::common::Range& region_range, int64_t partition_id,
                                     std::vector<pb::common::VectorWithDistance>& vector_with_distances);
  butil::Status QueryVectorTableData(const pb::common::Range& region_range, int64_t partition_id,
                                     std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status GetBorderId(const pb::common::Range& region_range, bool get_min, int64_t& vector_id);
  butil::Status ScanVectorId(std::shared_ptr<Engine::VectorReader::Context> ctx, std::vector<int64_t>& vector_ids);

  butil::Status DoVectorSearchForVectorIdPreFilter(
      VectorIndexWrapperPtr vector_index, const std::vector<pb::common::VectorWithId>& vector_with_ids,
      const pb::common::VectorSearchParameter& parameter, const pb::common::Range& region_range,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

  butil::Status DoVectorSearchForScalarPreFilter(
      VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
      const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

  butil::Status DoVectorSearchForTableCoprocessor(
      [[maybe_unused]] VectorIndexWrapperPtr vector_index, [[maybe_unused]] int64_t partition_id,
      [[maybe_unused]] const std::vector<pb::common::VectorWithId>& vector_with_ids,
      [[maybe_unused]] const pb::common::VectorSearchParameter& parameter,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

  // This function is for testing only
  butil::Status SearchVectorDebug(int64_t partition_id, VectorIndexWrapperPtr vector_index,
                                  pb::common::Range region_range,
                                  const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                  const pb::common::VectorSearchParameter& parameter,
                                  std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results,
                                  int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                  int64_t& search_time_us);

  // This function is for testing only
  butil::Status DoVectorSearchForVectorIdPreFilterDebug(
      VectorIndexWrapperPtr vector_index, const std::vector<pb::common::VectorWithId>& vector_with_ids,
      const pb::common::VectorSearchParameter& parameter, const pb::common::Range& region_range,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results,
      int64_t& deserialization_id_time_us, int64_t& search_time_us);

  // This function is for testing only
  butil::Status DoVectorSearchForScalarPreFilterDebug(
      VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
      const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, int64_t& scan_scalar_time_us,
      int64_t& search_time_us);  // NOLINT

  // std::shared_ptr<RawEngine::Reader> reader_;
  std::shared_ptr<RawEngine::Reader> vector_data_reader_;
  std::shared_ptr<RawEngine::Reader> vector_scalar_reader_;
  std::shared_ptr<RawEngine::Reader> vector_table_reader_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_READER_H_
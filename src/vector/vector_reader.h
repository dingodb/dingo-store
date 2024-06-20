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
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "butil/status.h"
#include "coprocessor/raw_coprocessor.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "mvcc/reader.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

// Vector reader
class VectorReader {
 public:
  VectorReader(mvcc::ReaderPtr reader) : reader_(reader) {}

  static std::shared_ptr<VectorReader> New(mvcc::ReaderPtr reader) { return std::make_shared<VectorReader>(reader); }

  butil::Status VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                  std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                 std::vector<pb::common::VectorWithId>& vector_with_ids);

  butil::Status VectorGetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min, int64_t& vector_id);

  butil::Status VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                std::vector<pb::common::VectorWithId>& vector_with_ids);

  butil::Status VectorGetRegionMetrics(int64_t region_id, const pb::common::Range& region_range,
                                       VectorIndexWrapperPtr vector_index,
                                       pb::common::VectorIndexMetrics& region_metrics);

  butil::Status VectorCount(int64_t ts, const pb::common::Range& range, int64_t& count);

  // This function is for testing only
  butil::Status VectorBatchSearchDebug(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                       std::vector<pb::index::VectorWithDistanceResult>& results,
                                       int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                       int64_t& search_time_us);

 private:
  butil::Status QueryVectorWithId(int64_t ts, const pb::common::Range& region_range, int64_t partition_id,
                                  int64_t vector_id, bool with_vector_data, pb::common::VectorWithId& vector_with_id);
  butil::Status SearchVector(int64_t ts, int64_t partition_id, VectorIndexWrapperPtr vector_index,
                             pb::common::Range region_range,
                             const std::vector<pb::common::VectorWithId>& vector_with_ids,
                             const pb::common::VectorSearchParameter& parameter,
                             const pb::common::ScalarSchema& scalar_schema,
                             std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

  butil::Status QueryVectorScalarData(int64_t ts, const pb::common::Range& region_range, int64_t partition_id,
                                      std::vector<std::string> selected_scalar_keys,
                                      pb::common::VectorWithId& vector_with_id);
  butil::Status QueryVectorScalarData(int64_t ts, const pb::common::Range& region_range, int64_t partition_id,
                                      std::vector<std::string> selected_scalar_keys,
                                      std::vector<pb::common::VectorWithDistance>& vector_with_distances);
  butil::Status QueryVectorScalarData(int64_t ts, const pb::common::Range& region_range, int64_t partition_id,
                                      std::vector<std::string> selected_scalar_keys,
                                      std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status CompareVectorScalarData(int64_t ts, const pb::common::Range& region_range, int64_t partition_id,
                                        int64_t vector_id, const pb::common::VectorScalardata& source_scalar_data,
                                        bool& compare_result);

  butil::Status CompareVectorScalarDataWithCoprocessor(int64_t ts, const pb::common::Range& region_range,
                                                       int64_t partition_id, int64_t vector_id,
                                                       const std::shared_ptr<RawCoprocessor>& scalar_coprocessor,
                                                       bool& compare_result);

  butil::Status QueryVectorTableData(int64_t ts, const pb::common::Range& region_range, int64_t partition_id,
                                     pb::common::VectorWithId& vector_with_id);
  butil::Status QueryVectorTableData(int64_t ts, const pb::common::Range& region_range, int64_t partition_id,
                                     std::vector<pb::common::VectorWithDistance>& vector_with_distances);
  butil::Status QueryVectorTableData(int64_t ts, const pb::common::Range& region_range, int64_t partition_id,
                                     std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status GetBorderId(int64_t ts, const pb::common::Range& region_range, bool get_min, int64_t& vector_id);
  butil::Status ScanVectorId(std::shared_ptr<Engine::VectorReader::Context> ctx, std::vector<int64_t>& vector_ids);

  butil::Status DoVectorSearchForVectorIdPreFilter(
      VectorIndexWrapperPtr vector_index, const std::vector<pb::common::VectorWithId>& vector_with_ids,
      const pb::common::VectorSearchParameter& parameter, const pb::common::Range& region_range,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

 public:
  butil::Status DoVectorSearchForScalarPreFilter(
      VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
      const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
      const pb::common::ScalarSchema& scalar_schema,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

  static bool ScalarCompareCore(const pb::common::VectorScalardata& std_vector_scalar,
                                const pb::common::VectorScalardata& internal_vector_scalar);

  butil::Status InternalVectorSearchForScalarPreFilterWithScalarCF(
      pb::common::Range region_range, bool use_coprocessor, const std::shared_ptr<RawCoprocessor>& scalar_coprocessor,
      const pb::common::VectorScalardata& std_vector_scalar,
      std::vector<int64_t>& vector_ids);  // NOLINT

  static bool ScalarCompareWithCoprocessorCore(const std::shared_ptr<RawCoprocessor>& scalar_coprocessor,
                                               const pb::common::VectorScalardata& internal_vector_scalar);

  butil::Status InternalVectorSearchForScalarPreFilterWithScalarKeySpeedUpCF(
      pb::common::Range region_range, const std::set<std::string>& compare_keys, bool use_coprocessor,
      const std::shared_ptr<RawCoprocessor>& scalar_coprocessor, const pb::common::VectorScalardata& std_vector_scalar,
      std::vector<int64_t>& vector_ids);  // NOLINT
 private:
  butil::Status DoVectorSearchForTableCoprocessor(
      VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
      const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
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
      int64_t& search_time_us);

  static butil::Status SetVectorIndexIdsFilter(bool is_negation, bool is_sorted, std::vector<int64_t>& vector_ids,
                                               std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters);

  butil::Status SearchAndRangeSearchWrapper(
      VectorIndexWrapperPtr vector_index, pb::common::Range region_range,
      const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results, uint32_t topk,  // NOLINT
      std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters);

  butil::Status BruteForceSearch(VectorIndexWrapperPtr vector_index,
                                 const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                                 const pb::common::Range& region_range,
                                 std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters, bool reconstruct,
                                 const pb::common::VectorSearchParameter& parameter,
                                 std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status BruteForceRangeSearch(VectorIndexWrapperPtr vector_index,
                                      const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
                                      const pb::common::Range& region_range,
                                      std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                                      bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                      std::vector<pb::index::VectorWithDistanceResult>& results);

  mvcc::ReaderPtr reader_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_READER_H_
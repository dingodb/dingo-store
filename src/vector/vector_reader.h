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

#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

// Vector reader
class VectorReader {
 public:
  VectorReader(std::shared_ptr<RawEngine::Reader> reader) : reader_(reader) {}

  static std::shared_ptr<VectorReader> New(std::shared_ptr<RawEngine::Reader> reader) {
    return std::make_shared<VectorReader>(reader);
  }

  butil::Status VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                  std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                 std::vector<pb::common::VectorWithId>& vector_with_ids);

  butil::Status VectorGetBorderId(const pb::common::Range& region_range, bool get_min, uint64_t& vector_id);

  butil::Status VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                std::vector<pb::common::VectorWithId>& vector_with_ids);

  butil::Status VectorGetRegionMetrics(uint64_t region_id, const pb::common::Range& region_range,
                                       pb::common::VectorIndexMetrics& region_metrics);

 private:
  butil::Status QueryVectorWithId(uint64_t partition_id, uint64_t vector_id, bool with_vector_data,
                                  pb::common::VectorWithId& vector_with_id);
  butil::Status SearchVector(uint64_t partition_id, uint64_t region_id,
                             const std::vector<pb::common::VectorWithId>& vector_with_ids,
                             const pb::common::VectorSearchParameter& parameter,
                             std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

  butil::Status QueryVectorScalarData(uint64_t partition_id, std::vector<std::string> selected_scalar_keys,
                                      pb::common::VectorWithId& vector_with_id);
  butil::Status QueryVectorScalarData(uint64_t partition_id, std::vector<std::string> selected_scalar_keys,
                                      std::vector<pb::common::VectorWithDistance>& vector_with_distances);
  butil::Status QueryVectorScalarData(uint64_t partition_id, std::vector<std::string> selected_scalar_keys,
                                      std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status CompareVectorScalarData(uint64_t partition_id, uint64_t vector_id,
                                        const pb::common::VectorScalardata& source_scalar_data, bool& compare_result);

  butil::Status QueryVectorTableData(uint64_t partition_id, pb::common::VectorWithId& vector_with_id);
  butil::Status QueryVectorTableData(uint64_t partition_id,
                                     std::vector<pb::common::VectorWithDistance>& vector_with_distances);
  butil::Status QueryVectorTableData(uint64_t partition_id, std::vector<pb::index::VectorWithDistanceResult>& results);

  butil::Status GetBorderId(const pb::common::Range& region_range, bool get_min, uint64_t& vector_id);
  butil::Status ScanVectorId(std::shared_ptr<Engine::VectorReader::Context> ctx, std::vector<uint64_t>& vector_ids);

  butil::Status DoVectorSearchForVectorIdPreFilter(
      std::shared_ptr<VectorIndex> vector_index, const std::vector<pb::common::VectorWithId>& vector_with_ids,
      const pb::common::VectorSearchParameter& parameter,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

  butil::Status DoVectorSearchForScalarPreFilter(
      std::shared_ptr<VectorIndex> vector_index, uint64_t partition_id,
      const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::VectorSearchParameter& parameter,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);

  butil::Status DoVectorSearchForTableCoprocessor(
      [[maybe_unused]] std::shared_ptr<VectorIndex> vector_index, [[maybe_unused]] uint64_t partition_id,
      [[maybe_unused]] const std::vector<pb::common::VectorWithId>& vector_with_ids,
      [[maybe_unused]] const pb::common::VectorSearchParameter& parameter,
      std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results);
  std::shared_ptr<RawEngine::Reader> reader_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_READER_H_
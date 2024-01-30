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

#include "sdk/vector/vector_index.h"

#include <cstdint>
#include <sstream>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/meta.pb.h"
#include "sdk/common/param_config.h"
#include "sdk/vector/vector_common.h"
#include "vector/codec.h"

namespace dingodb {
namespace sdk {

VectorIndex::VectorIndex(pb::meta::IndexDefinitionWithId index_def_with_id)
    : id_(index_def_with_id.index_id().entity_id()),
      schema_id_(index_def_with_id.index_id().parent_entity_id()),
      name_(index_def_with_id.index_definition().name()),
      index_def_with_id_(std::move(index_def_with_id)) {
  CHECK_GT(index_def_with_id_.index_definition().index_partition().partitions_size(), 0);
  for (const auto& partition : index_def_with_id_.index_definition().index_partition().partitions()) {
    int64_t start_id = VectorCodec::DecodeVectorId(partition.range().start_key());
    int64_t part_id = partition.id().entity_id();
    CHECK_GE(start_id, 0);
    CHECK(start_key_to_part_id_.insert({start_id, part_id}).second);
    CHECK(part_id_to_range_.insert({part_id, partition.range()}).second);
  }
  VLOG(kSdkVlogLevel) << "Init:" << ToString();
}

VectorIndexType VectorIndex::GetVectorIndexType() const {
  return InternalVectorIndexTypePB2VectorIndexType(
      index_def_with_id_.index_definition().index_parameter().vector_index_parameter().vector_index_type());
}

int64_t VectorIndex::GetPartitionId(int64_t vector_id) const {
  CHECK_GT(vector_id, 0);
  VLOG(kSdkVlogLevel) << "query  vector_id:" << vector_id << ", cache:" << ToString();
  auto iter = start_key_to_part_id_.upper_bound(vector_id);
  CHECK(iter != start_key_to_part_id_.begin());
  iter--;
  return iter->second;
}

std::vector<int64_t> VectorIndex::GetPartitionIds() const {
  std::vector<int64_t> part_ids;
  part_ids.reserve(start_key_to_part_id_.size());
  for (const auto& [start_key, part_id] : start_key_to_part_id_) {
    part_ids.push_back(part_id);
  }

  return std::move(part_ids);
}

const pb::common::Range& VectorIndex::GetPartitionRange(int64_t part_id) const {
  auto iter = part_id_to_range_.find(part_id);
  CHECK(iter != part_id_to_range_.end());
  return iter->second;
}

std::string VectorIndex::ToString(bool verbose) const {
  std::ostringstream oss;
  for (const auto& [start_key, part_id] : start_key_to_part_id_) {
    oss << "[" << start_key << ":" << part_id << "]";
  }
  if (verbose) {
    return fmt::format("VectorIndex(id={}, schema_id={}, name={}, start_key_to_part_id={}, index_def_with_id={})", id_,
                       schema_id_, name_, oss.str(), index_def_with_id_.DebugString());
  } else {
    return fmt::format("VectorIndex(id={}, schema_id={}, name={}, start_key_to_part_id={})", id_, schema_id_, name_,
                       oss.str());
  }
}

}  // namespace sdk
}  // namespace dingodb
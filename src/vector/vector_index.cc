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

#include "vector/vector_index.h"

#include "butil/status.h"
#include "proto/common.pb.h"

namespace dingodb {

VectorIndex::~VectorIndex() = default;

pb::common::VectorIndexType VectorIndex::VectorIndexType() const { return vector_index_type; }

butil::Status VectorIndex::BatchSearch(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                       std::vector<pb::index::VectorWithDistanceResult>& results, bool reconstruct) {
  for (auto& vector_with_id : vector_with_ids) {
    std::vector<float> vector;
    for (auto value : vector_with_id.vector().float_values()) {
      vector.push_back(value);
    }

    std::vector<pb::common::VectorWithDistance> vector_with_distances;
    Search(vector, topk, vector_with_distances, reconstruct);

    // build results
    pb::index::VectorWithDistanceResult vector_with_distance_result;
    for (auto& vector_with_distance : vector_with_distances) {
      auto* new_result = vector_with_distance_result.add_vector_with_distances();
      new_result->Swap(&vector_with_distance);
    }
    results.push_back(std::move(vector_with_distance_result));
  }

  return butil::Status::OK();
}

butil::Status VectorIndex::Search([[maybe_unused]] pb::common::VectorWithId vector_with_id,
                                  [[maybe_unused]] uint32_t topk, std::vector<pb::common::VectorWithDistance>& results,
                                  [[maybe_unused]] bool reconstruct) {
  std::vector<float> vector;
  for (auto value : vector_with_id.vector().float_values()) {
    vector.push_back(value);
  }

  return Search(vector, topk, results, reconstruct);
}

butil::Status VectorIndex::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  for (const auto& vector_with_id : vector_with_ids) {
    auto ret = Upsert(vector_with_id);
    if (!ret.ok()) {
      return ret;
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndex::Upsert(const pb::common::VectorWithId& vector_with_id) {
  std::vector<float> vector;
  for (const auto& value : vector_with_id.vector().float_values()) {
    vector.push_back(value);
  }

  return Upsert(vector_with_id.id(), vector);
}

butil::Status VectorIndex::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  for (const auto& vector_with_id : vector_with_ids) {
    auto ret = Add(vector_with_id);
    if (!ret.ok()) {
      return ret;
    }
  }
  return butil::Status::OK();
}

butil::Status VectorIndex::Add(const pb::common::VectorWithId& vector_with_id) {
  std::vector<float> vector;
  for (const auto& value : vector_with_id.vector().float_values()) {
    vector.push_back(value);
  }

  return Add(vector_with_id.id(), vector);
}

butil::Status VectorIndex::DeleteBatch(const std::vector<uint64_t>& delete_ids) {
  for (auto id : delete_ids) {
    auto ret = Delete(id);
    if (!ret.ok()) {
      return ret;
    }
  }

  return butil::Status::OK();
}

void VectorIndex::SetSnapshotLogIndex(uint64_t snapshot_log_index) {
  this->snapshot_log_index.store(snapshot_log_index, std::memory_order_relaxed);
}

uint64_t VectorIndex::ApplyLogIndex() const { return apply_log_index.load(std::memory_order_relaxed); }

void VectorIndex::SetApplyLogIndex(uint64_t apply_log_index) {
  this->apply_log_index.store(apply_log_index, std::memory_order_relaxed);
}

uint64_t VectorIndex::SnapshotLogIndex() const { return snapshot_log_index.load(std::memory_order_relaxed); }

butil::Status VectorIndex::Save(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement save");
}

butil::Status VectorIndex::Load(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement load");
}

butil::Status VectorIndex::GetCount([[maybe_unused]] uint64_t& count) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement get count");
}

butil::Status VectorIndex::NeedToRebuild([[maybe_unused]] bool& need_to_rebuild,
                                         [[maybe_unused]] uint64_t last_save_log_behind) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement need to rebuild");
}

butil::Status VectorIndex::NeedToSave([[maybe_unused]] bool& need_to_save,
                                      [[maybe_unused]] uint64_t last_save_log_behind) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement need to rebuild");
}

}  // namespace dingodb

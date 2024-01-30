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

#ifndef DINGODB_SDK_VECTOR_INDEX_ITEM_H_
#define DINGODB_SDK_VECTOR_INDEX_ITEM_H_

#include <cstdint>

#include "proto/meta.pb.h"
#include "sdk/vector.h"

namespace dingodb {
namespace sdk {

class VectorIndexCache;
class VectorIndex {
 public:
  VectorIndex(const VectorIndex&) = delete;
  const VectorIndex& operator=(const VectorIndex&) = delete;

  explicit VectorIndex(pb::meta::IndexDefinitionWithId index_def_with_id);

  ~VectorIndex() = default;

  int64_t GetId() const { return id_; }

  int64_t GetSchemaId() const { return schema_id_; }

  std::string GetName() const { return name_; }

  VectorIndexType GetVectorIndexType() const;

  int64_t GetPartitionId(int64_t vector_id) const;

  std::vector<int64_t> GetPartitionIds() const;

  // be sure partition id is valid
  const pb::common::Range& GetPartitionRange(int64_t part_id) const;

  bool IsStale() { return stale_.load(std::memory_order_relaxed); }

  std::string ToString(bool verbose = false) const;

 private:
  friend class VectorIndexCache;

  void MarkStale() { stale_.store(true, std::memory_order_relaxed); }

  void UnMarkStale() { stale_.store(false, std::memory_order_relaxed); }

  const int64_t id_{-1};
  const int64_t schema_id_{-1};
  const std::string name_;
  const pb::meta::IndexDefinitionWithId index_def_with_id_;
  // start_key is 0 or valid vector id
  std::map<int64_t, int64_t> start_key_to_part_id_;
  std::map<int64_t, pb::common::Range> part_id_to_range_;

  std::atomic<bool> stale_{true};
};
}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_VECTOR_INDEX_ITEM_H_
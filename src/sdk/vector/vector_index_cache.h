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

#include <cstdint>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>

#include "proto/meta.pb.h"
#include "sdk/coordinator_proxy.h"
#include "sdk/vector.h"
#include "vector/vector_index.h"

namespace dingodb {
namespace sdk {

class VectorIndex {
 public:
  VectorIndex(const VectorIndex&) = delete;
  const VectorIndex& operator=(const VectorIndex&) = delete;

  explicit VectorIndex(int64_t id, pb::meta::IndexDefinition definition);

  ~VectorIndex() = default;

  int64_t GetId() const { return id_; }

  bool IsStale() { return stale_.load(std::memory_order_relaxed); }

  std::string GetName() const;

  VectorIndexType GetIndexType() const;

  int64_t GetPartitionId(int64_t vector_id) const;

 private:
  int64_t id_{-1};
  std::atomic<bool> stale_{true};
  pb::meta::IndexDefinition index_definition_;
};

class VectorIndexCache {
 public:
  VectorIndexCache(const VectorIndexCache&) = delete;
  const VectorIndexCache& operator=(const VectorIndexCache&) = delete;

  explicit VectorIndexCache(CoordinatorProxy& coordinator_proxy);

  ~VectorIndexCache();

  Status GetIndexIdByName(std::string_view index_name, int64_t& index_id);

  Status GetVectorIndexByName(std::string_view index_name, std::shared_ptr<VectorIndex>& out_vector_index);

  Status GetVectorIndexById(int64_t index_id, std::shared_ptr<VectorIndex>& out_vector_index);

  void RemoveVectorIndexById(int64_t index_id);

  void RemoveVectorIndexByName(std::string_view index_name);

 private:
  mutable std::shared_mutex rw_lck_;
  std::unordered_map<std::string, int64_t, std::less<void>> index_name_to_id_;
  std::unordered_map<int64_t, std::shared_ptr<VectorIndex>, std::less<void>> id_to_index_;
};

}  // namespace sdk
}  // namespace dingodb
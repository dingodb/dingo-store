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

#ifndef DINGODB_SDK_VECTOR_INDEX_CACHE_H_
#define DINGODB_SDK_VECTOR_INDEX_CACHE_H_

#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>

#include "sdk/coordinator_proxy.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {
namespace sdk {

using VectorIndexCacheKey = std::string;

class VectorIndexCache {
 public:
  VectorIndexCache(const VectorIndexCache&) = delete;
  const VectorIndexCache& operator=(const VectorIndexCache&) = delete;

  explicit VectorIndexCache(CoordinatorProxy& coordinator_proxy);

  ~VectorIndexCache() = default;

  Status GetIndexIdByKey(const VectorIndexCacheKey& index_key, int64_t& index_id);

  Status GetVectorIndexByKey(const VectorIndexCacheKey& index_key, std::shared_ptr<VectorIndex>& out_vector_index);

  Status GetVectorIndexById(int64_t index_id, std::shared_ptr<VectorIndex>& out_vector_index);

  void RemoveVectorIndexById(int64_t index_id);

  void RemoveVectorIndexByKey(const VectorIndexCacheKey& index_key);

 private:
  Status SlowGetVectorIndexByKey(const VectorIndexCacheKey& index_key, std::shared_ptr<VectorIndex>& out_vector_index);
  Status SlowGetVectorIndexById(int64_t index_id, std::shared_ptr<VectorIndex>& out_vector_index);
  Status ProcessIndexDefinitionWithId(const pb::meta::IndexDefinitionWithId& index_def_with_id,
                                      std::shared_ptr<VectorIndex>& out_vector_index);

  static bool CheckIndexDefinitionWithId(const pb::meta::IndexDefinitionWithId& index_def_with_id);
  template <class VectorIndexResponse>
  static bool CheckIndexResponse(const VectorIndexResponse& response);

  CoordinatorProxy& coordinator_proxy_;
  mutable std::shared_mutex rw_lock_;
  std::unordered_map<VectorIndexCacheKey, int64_t> index_key_to_id_;
  std::unordered_map<int64_t, std::shared_ptr<VectorIndex>> id_to_index_;
};

template <class VectorIndexResponse>
bool VectorIndexCache::CheckIndexResponse(const VectorIndexResponse& response) {
  bool checked = true;
  if (!response.has_index_definition_with_id()) {
    checked = false;
  } else {
    checked = CheckIndexDefinitionWithId(response.index_definition_with_id());
  }

  if (!checked) {
    DINGO_LOG(WARNING) << "Fail checked, response:" << response.DebugString();
  }

  return checked;
}

static VectorIndexCacheKey EncodeVectorIndexCacheKey(int64_t schema_id, const std::string& index_name) {
  DCHECK_GT(schema_id, 0);
  DCHECK(!index_name.empty());
  auto buf_size = sizeof(schema_id) + index_name.size();
  char buf[buf_size];
  memcpy(buf, &schema_id, sizeof(schema_id));
  memcpy(buf + sizeof(schema_id), index_name.data(), index_name.size());
  std::string tmp(buf, buf_size);
  return std::move(tmp);
}

static void DecodeVectorIndexCacheKey(const VectorIndexCacheKey& key, int64_t& schema_id, std::string& index_name) {
  DCHECK_GE(key.size(), sizeof(schema_id));
  int64_t tmp_schema_id;
  memcpy(&tmp_schema_id, key.data(), sizeof(schema_id));
  schema_id = tmp_schema_id;
  index_name = std::string(key.data() + sizeof(schema_id), key.size() - sizeof(schema_id));
}

static VectorIndexCacheKey GetVectorIndexCacheKey(const VectorIndex& index) {
  return std::move(EncodeVectorIndexCacheKey(index.GetSchemaId(), index.GetName()));
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_INDEX_CACHE_H_
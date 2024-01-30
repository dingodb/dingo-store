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

#include "sdk/vector/vector_index_cache.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "engine/engine.h"
#include "glog/logging.h"
#include "proto/meta.pb.h"
#include "sdk/status.h"
#include "sdk/vector/vector_common.h"

namespace dingodb {
namespace sdk {

VectorIndexCache::VectorIndexCache(CoordinatorProxy& coordinator_proxy) : coordinator_proxy_(coordinator_proxy) {}

Status VectorIndexCache::GetIndexIdByKey(const VectorIndexCacheKey& index_key, int64_t& index_id) {
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    auto iter = index_key_to_id_.find(index_key);
    if (iter != index_key_to_id_.end()) {
      index_id = iter->second;
      return Status::OK();
    }
  }

  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(SlowGetVectorIndexByKey(index_key, tmp));

  index_id = tmp->GetId();
  return Status::OK();
}

Status VectorIndexCache::GetVectorIndexByKey(const VectorIndexCacheKey& index_key,
                                             std::shared_ptr<VectorIndex>& out_vector_index) {
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    auto iter = index_key_to_id_.find(index_key);
    if (iter != index_key_to_id_.end()) {
      auto index_iter = id_to_index_.find(iter->second);
      CHECK(index_iter != id_to_index_.end());
      out_vector_index = index_iter->second;
      return Status::OK();
    }
  }

  return SlowGetVectorIndexByKey(index_key, out_vector_index);
}

Status VectorIndexCache::GetVectorIndexById(int64_t index_id, std::shared_ptr<VectorIndex>& out_vector_index) {
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    auto iter = id_to_index_.find(index_id);
    if (iter != id_to_index_.end()) {
      out_vector_index = iter->second;
      return Status::OK();
    }
  }

  return SlowGetVectorIndexById(index_id, out_vector_index);
}

void VectorIndexCache::RemoveVectorIndexById(int64_t index_id) {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  auto id_iter = id_to_index_.find(index_id);
  if (id_iter != id_to_index_.end()) {
    auto vector_index = id_iter->second;
    auto name_iter = index_key_to_id_.find(GetVectorIndexCacheKey(*vector_index));
    CHECK(name_iter != index_key_to_id_.end());

    id_to_index_.erase(id_iter);
    index_key_to_id_.erase(name_iter);
  }
}

void VectorIndexCache::RemoveVectorIndexByKey(const VectorIndexCacheKey& index_key) {
  std::unique_lock<std::shared_mutex> w(rw_lock_);

  auto name_iter = index_key_to_id_.find(index_key);
  if (name_iter != index_key_to_id_.end()) {
    auto id_iter = id_to_index_.find(name_iter->second);
    CHECK(id_iter != id_to_index_.end());

    id_to_index_.erase(id_iter);
    index_key_to_id_.erase(name_iter);
  }
}

Status VectorIndexCache::SlowGetVectorIndexByKey(const VectorIndexCacheKey& index_key,
                                                 std::shared_ptr<VectorIndex>& out_vector_index) {
  int64_t schema_id{0};
  std::string index_name;
  DecodeVectorIndexCacheKey(index_key, schema_id, index_name);
  CHECK(!index_name.empty()) << "illegal index key: " << index_key;
  CHECK_NE(schema_id, 0) << "illegal index key: " << index_key;

  pb::meta::GetIndexByNameRequest request;
  pb::meta::GetIndexByNameResponse response;
  auto* schema = request.mutable_schema_id();
  schema->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema->set_entity_id(schema_id);
  request.set_index_name(index_name);

  DINGO_RETURN_NOT_OK(coordinator_proxy_.GetIndexByName(request, response));

  if (CheckIndexResponse(response)) {
    return ProcessIndexDefinitionWithId(response.index_definition_with_id(), out_vector_index);
  } else {
    return Status::NotFound("response check invalid");
  }
}

Status VectorIndexCache::SlowGetVectorIndexById(int64_t index_id, std::shared_ptr<VectorIndex>& out_vector_index) {
  pb::meta::GetIndexRequest request;
  pb::meta::GetIndexResponse response;
  auto* index_id_pb = request.mutable_index_id();
  index_id_pb->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id_pb->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  index_id_pb->set_entity_id(index_id);
  DINGO_RETURN_NOT_OK(coordinator_proxy_.GetIndexById(request, response));

  if (CheckIndexResponse(response)) {
    return ProcessIndexDefinitionWithId(response.index_definition_with_id(), out_vector_index);
  } else {
    return Status::NotFound("response check invalid");
  }
}

Status VectorIndexCache::ProcessIndexDefinitionWithId(const pb::meta::IndexDefinitionWithId& index_def_with_id,
                                                      std::shared_ptr<VectorIndex>& out_vector_index) {
  int64_t index_id = index_def_with_id.index_id().entity_id();

  std::unique_lock<std::shared_mutex> w(rw_lock_);
  auto iter = id_to_index_.find(index_id);
  if (iter != id_to_index_.end()) {
    CHECK_EQ(iter->second->GetName(), index_def_with_id.index_definition().name());
    out_vector_index = iter->second;
    return Status::OK();
  } else {
    // becareful change CHECK to DCHECK
    auto vector_index = std::make_shared<VectorIndex>(index_def_with_id);
    CHECK(index_key_to_id_.insert({GetVectorIndexCacheKey(*vector_index), index_id}).second);
    CHECK(id_to_index_.insert({index_id, vector_index}).second);
    vector_index->UnMarkStale();
    out_vector_index = vector_index;
    return Status::OK();
  }
}

bool VectorIndexCache::CheckIndexDefinitionWithId(const pb::meta::IndexDefinitionWithId& index_def_with_id) {
  if (!index_def_with_id.has_index_id()) {
    return false;
  }
  if (!(index_def_with_id.index_id().entity_id() > 0)) {
    return false;
  }
  if (!(index_def_with_id.index_id().parent_entity_id() > 0)) {
    return false;
  }
  if (!index_def_with_id.has_index_definition()) {
    return false;
  }
  if (index_def_with_id.index_definition().name().empty()) {
    return false;
  }
  if (!(index_def_with_id.index_definition().index_partition().partitions_size() > 0)) {
    return false;
  }
  return true;
}
}  // namespace sdk
}  // namespace dingodb
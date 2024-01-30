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

#include "sdk/client_stub.h"
#include "sdk/status.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_add_task.h"
#include "sdk/vector/vector_delete_task.h"
#include "sdk/vector/vector_index_cache.h"
#include "sdk/vector/vector_search_task.h"

namespace dingodb {
namespace sdk {

VectorClient::VectorClient(const ClientStub& stub) : stub_(stub) {}

Status VectorClient::Add(int64_t index_id, const std::vector<VectorWithId>& vectors, bool replace_deleted,
                         bool is_update) {
  VectorAddTask task(stub_, index_id, vectors, replace_deleted, is_update);
  return task.Run();
}

Status VectorClient::Add(int64_t schema_id, const std::string& index_name, const std::vector<VectorWithId>& vectors,
                         bool replace_deleted, bool is_update) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorAddTask task(stub_, index_id, vectors, replace_deleted, is_update);
  return task.Run();
}

Status VectorClient::Search(int64_t index_id, const SearchParameter& search_param,
                            const std::vector<VectorWithId>& target_vectors, std::vector<SearchResult>& out_result) {
  VectorSearchTask task(stub_, index_id, search_param, target_vectors, out_result);
  return task.Run();
}

Status VectorClient::Search(int64_t schema_id, const std::string& index_name, const SearchParameter& search_param,
                            const std::vector<VectorWithId>& target_vectors, std::vector<SearchResult>& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorSearchTask task(stub_, index_id, search_param, target_vectors, out_result);
  return task.Run();
}

Status VectorClient::Delete(int64_t index_id, const std::vector<int64_t>& vector_ids,
                            std::vector<DeleteResult>& out_result) {
  VectorDeleteTask task(stub_, index_id, vector_ids, out_result);
  return task.Run();
}

Status VectorClient::Delete(int64_t schema_id, const std::string& index_name, const std::vector<int64_t>& vector_ids,
                            std::vector<DeleteResult>& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorDeleteTask task(stub_, index_id, vector_ids, out_result);
  return task.Run();
}

}  // namespace sdk

}  // namespace dingodb
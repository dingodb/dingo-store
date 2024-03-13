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
#include "sdk/vector/vector_batch_query_task.h"
#include "sdk/vector/vector_count_task.h"
#include "sdk/vector/vector_delete_task.h"
#include "sdk/vector/vector_get_border_task.h"
#include "sdk/vector/vector_get_index_metrics_task.h"
#include "sdk/vector/vector_index_cache.h"
#include "sdk/vector/vector_scan_query_task.h"
#include "sdk/vector/vector_search_task.h"

namespace dingodb {
namespace sdk {

VectorClient::VectorClient(const ClientStub& stub) : stub_(stub) {}

Status VectorClient::AddByIndexId(int64_t index_id, const std::vector<VectorWithId>& vectors, bool replace_deleted,
                                  bool is_update) {
  VectorAddTask task(stub_, index_id, vectors, replace_deleted, is_update);
  return task.Run();
}

Status VectorClient::AddByIndexName(int64_t schema_id, const std::string& index_name,
                                    const std::vector<VectorWithId>& vectors, bool replace_deleted, bool is_update) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorAddTask task(stub_, index_id, vectors, replace_deleted, is_update);
  return task.Run();
}

Status VectorClient::SearchByIndexId(int64_t index_id, const SearchParam& search_param,
                                     const std::vector<VectorWithId>& target_vectors,
                                     std::vector<SearchResult>& out_result) {
  VectorSearchTask task(stub_, index_id, search_param, target_vectors, out_result);
  return task.Run();
}

Status VectorClient::SearchByIndexName(int64_t schema_id, const std::string& index_name,
                                       const SearchParam& search_param, const std::vector<VectorWithId>& target_vectors,
                                       std::vector<SearchResult>& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorSearchTask task(stub_, index_id, search_param, target_vectors, out_result);
  return task.Run();
}

Status VectorClient::DeleteByIndexId(int64_t index_id, const std::vector<int64_t>& vector_ids,
                                     std::vector<DeleteResult>& out_result) {
  VectorDeleteTask task(stub_, index_id, vector_ids, out_result);
  return task.Run();
}

Status VectorClient::DeleteByIndexName(int64_t schema_id, const std::string& index_name,
                                       const std::vector<int64_t>& vector_ids, std::vector<DeleteResult>& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorDeleteTask task(stub_, index_id, vector_ids, out_result);
  return task.Run();
}

Status VectorClient::BatchQueryByIndexId(int64_t index_id, const QueryParam& query_param, QueryResult& out_result) {
  VectorBatchQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status VectorClient::BatchQueryByIndexName(int64_t schema_id, const std::string& index_name,
                                           const QueryParam& query_param, QueryResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorBatchQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status VectorClient::GetBorderByIndexId(int64_t index_id, bool is_max, int64_t& out_vector_id) {
  VectorGetBorderTask task(stub_, index_id, is_max, out_vector_id);
  return task.Run();
}

Status VectorClient::GetBorderByIndexName(int64_t schema_id, const std::string& index_name, bool is_max,
                                          int64_t& out_vector_id) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorGetBorderTask task(stub_, index_id, is_max, out_vector_id);
  return task.Run();
}

Status VectorClient::ScanQueryByIndexId(int64_t index_id, const ScanQueryParam& query_param,
                                        ScanQueryResult& out_result) {
  VectorScanQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status VectorClient::ScanQueryByIndexName(int64_t schema_id, const std::string& index_name,
                                          const ScanQueryParam& query_param, ScanQueryResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorScanQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status VectorClient::GetIndexMetricsByIndexId(int64_t index_id, IndexMetricsResult& out_result) {
  VectorGetIndexMetricsTask task(stub_, index_id, out_result);
  return task.Run();
}

Status VectorClient::GetIndexMetricsByIndexName(int64_t schema_id, const std::string& index_name,
                                                IndexMetricsResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorGetIndexMetricsTask task(stub_, index_id, out_result);
  return task.Run();
}

Status VectorClient::CountByIndexId(int64_t index_id, int64_t start_vector_id, int64_t end_vector_id,
                                    int64_t& out_count) {
  VectorCountTask task(stub_, index_id, start_vector_id, end_vector_id, out_count);
  return task.Run();
}

Status VectorClient::CountByIndexName(int64_t schema_id, const std::string& index_name, int64_t start_vector_id,
                                      int64_t end_vector_id, int64_t& out_count) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);

  VectorCountTask task(stub_, index_id, start_vector_id, end_vector_id, out_count);
  return task.Run();
}

}  // namespace sdk

}  // namespace dingodb
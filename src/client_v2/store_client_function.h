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

#ifndef DINGODB_CLIENT_STORE_CLIENT_FUNCTION_H_  // NOLINT
#define DINGODB_CLIENT_STORE_CLIENT_FUNCTION_H_

#include <cstdint>
#include <memory>
#include <string>

#include "proto/meta.pb.h"
#include "proto/store.pb.h"

namespace client_v2 {

struct Context {
  std::unique_ptr<Context> Clone() const {
    auto clone_ctx = std::make_unique<Context>();

    clone_ctx->table_name = table_name;
    clone_ctx->partition_num = partition_num;
    clone_ctx->req_num = req_num;
    clone_ctx->table_id = table_id;
    clone_ctx->index_id = index_id;
    clone_ctx->region_id = region_id;

    clone_ctx->thread_num = thread_num;
    clone_ctx->thread_no = thread_no;
    clone_ctx->prefix = prefix;

    clone_ctx->dimension = dimension;
    clone_ctx->start_id = start_id;
    clone_ctx->count = count;
    clone_ctx->step_count = step_count;
    clone_ctx->with_scalar = with_scalar;
    clone_ctx->with_table = with_table;

    clone_ctx->db_path = db_path;
    clone_ctx->offset = offset;
    clone_ctx->limit = limit;

    clone_ctx->key = key;

    clone_ctx->show_vector = show_vector;

    clone_ctx->scalar_filter_key = scalar_filter_key;
    clone_ctx->scalar_filter_value = scalar_filter_value;
    clone_ctx->scalar_filter_key2 = scalar_filter_key2;
    clone_ctx->scalar_filter_value2 = scalar_filter_value2;

    clone_ctx->show_lock = show_lock;
    clone_ctx->show_write = show_write;
    clone_ctx->show_last_data = show_last_data;
    clone_ctx->show_pretty = show_pretty;
    clone_ctx->print_column_width = print_column_width;
    return clone_ctx;
  }

  std::string table_name;
  int partition_num;
  int req_num;

  int64_t table_id;
  int64_t index_id;
  int64_t region_id;

  int32_t thread_num;
  int32_t thread_no;
  std::string prefix;

  uint32_t dimension;
  uint32_t start_id;
  uint32_t count;
  uint32_t step_count;

  bool with_scalar;
  bool with_table;

  std::string db_path;
  int32_t offset;
  int32_t limit;

  std::string key;

  bool show_vector;

  std::string csv_data;
  std::string json_data;

  std::string scalar_filter_key;
  std::string scalar_filter_value;
  std::string scalar_filter_key2;
  std::string scalar_filter_value2;

  bool show_lock;
  bool show_write;
  bool show_last_data;
  bool show_all_data;
  bool show_pretty;
  int32_t print_column_width;
};

// meta
dingodb::pb::meta::TableDefinition SendGetIndex(int64_t index_id);
dingodb::pb::meta::TableDefinition SendGetTable(int64_t table_id);
dingodb::pb::meta::TableRange SendGetTableRange(int64_t table_id);
dingodb::pb::meta::IndexRange SendGetIndexRange(int64_t table_id);

// coordinator
dingodb::pb::common::Region SendQueryRegion(int64_t region_id);
dingodb::pb::store::Context GetRegionContext(int64_t region_id);

// document
void SendDocumentAdd(int64_t region_id, int64_t document_id, std::string document_text1, std::string document_text2,
                     bool is_update);
void SendDocumentDelete(int64_t region_id, uint32_t start_id, uint32_t count);
void SendDocumentSearch(int64_t region_id, std::string query_string, int32_t topn, bool without_scalar);
void SendDocumentBatchQuery(int64_t region_id, std::vector<int64_t> document_ids, bool without_scalar, std::string key);
void SendDocumentGetMaxId(int64_t region_id);
void SendDocumentGetMinId(int64_t region_id);
void SendDocumentScanQuery(int64_t region_id, int64_t start_id, int64_t end_id, int64_t limit, bool is_reverse,
                           bool without_scalar, std::string key);
int64_t SendDocumentCount(int64_t region_id, int64_t start_document_id, int64_t end_document_id);
void SendDocumentGetRegionMetrics(int64_t region_id);

// vector
void SendVectorSearch(int64_t region_id, uint32_t dimension, uint32_t topn, std::string vector_data, std::string key,
                      bool without_vector, bool without_scalar, bool without_table, bool with_vector_ids,
                      bool with_scalar_pre_filter, bool with_table_pre_filter, std::string scalar_filter_key,
                      std::string scalar_filter_value, std::string scalar_filter_key2, std::string scalar_filter_value2,
                      bool with_scalar_post_filter, int64_t ef_search, bool bruteforce, bool print_vector_search_delay,
                      std::string csv_output);

void SendVectorSearchDebug(int64_t region_id, uint32_t dimension, int64_t start_vector_id, uint32_t topn,
                           uint32_t batch_count, const std::string& key, const std::string& value, bool without_vector,
                           bool without_scalar, bool without_table, bool with_vector_ids, int32_t vector_ids_count,
                           bool with_scalar_pre_filter, bool with_scalar_post_filter, bool print_vector_search_delay);

void SendVectorRangeSearch(int64_t region_id, uint32_t dimension, double radius, std::string key, bool without_vector,
                           bool without_scalar, bool without_table, bool with_vector_ids, bool with_scalar_pre_filter,
                           bool with_scalar_post_filter, bool print_vector_search_delay);

void SendVectorRangeSearchDebug(int64_t region_id, uint32_t dimension, int64_t start_vector_id, double radius,
                                uint32_t batch_count, const std::string& key, const std::string& value,
                                bool without_vector, bool without_scalar, bool without_table, bool with_vector_ids,
                                int32_t vector_ids_count, bool with_scalar_pre_filter, bool with_scalar_post_filter,
                                bool print_vector_search_delay);

void SendVectorBatchSearch(int64_t region_id, uint32_t dimension, uint32_t topn, uint32_t batch_count, std::string key,
                           bool without_vector, bool without_scalar, bool without_table, bool with_vector_ids,
                           bool with_scalar_pre_filter, bool with_scalar_post_filter, bool print_vector_search_delay);

void SendVectorBatchQuery(int64_t region_id, std::vector<int64_t> vector_ids, std::string key, bool without_vector,
                          bool without_scalar, bool without_table);
void SendVectorAddRetry(std::shared_ptr<Context> ctx);
void SendVectorAdd(std::shared_ptr<Context> ctx);
void SendVectorDelete(int64_t region_id, uint32_t start_id, uint32_t count);
void SendVectorGetMaxId(int64_t region_id);
void SendVectorGetMinId(int64_t region_id);
void SendVectorAddBatch(int64_t region_id, uint32_t dimension, uint32_t count, uint32_t step_count, int64_t start_id,
                        const std::string& file, bool without_scalar);
void SendVectorScanQuery(int64_t region_id, int64_t start_id, int64_t end_id, int64_t limit, bool is_reverse,
                         bool without_vector, bool without_scalar, bool without_table, std::string key,
                         std::string scalar_filter_key, std::string scalar_filter_value, std::string scalar_filter_key2,
                         std::string scalar_filter_value2);

void SendVectorScanDump(int64_t region_id, int64_t start_id, int64_t end_id, int64_t limit, bool is_reverse,
                        std::string csv_output);
void SendVectorAddBatchDebug(int64_t region_id, uint32_t dimension, uint32_t count, uint32_t step_count,
                             int64_t start_id, const std::string& file, bool without_scalar);
void SendVectorGetRegionMetrics(int64_t region_id);
void SendVectorCalcDistance(uint32_t dimension, const std::string& alg_type, const std::string& metric_type,
                            int32_t left_vector_size, int32_t right_vector_size, bool is_return_normlize);
void SendCalcDistance(std::string vector_data1, std::string vector_data2);

int64_t SendVectorCount(int64_t region_id, int64_t start_vector_id, int64_t end_vector_id);
void CountVectorTable(std::shared_ptr<Context> ctx);

// key/value
void SendKvGet(int64_t region_id, const std::string& key, std::string& value);
void SendKvBatchGet(int64_t region_id, const std::string& prefix, int count);
int SendKvPut(int64_t region_id, const std::string& key, std::string value = "");
void SendKvBatchPut(int64_t region_id, const std::string& prefix, int count);
void SendKvPutIfAbsent(int64_t region_id, const std::string& key);
void SendKvBatchPutIfAbsent(int64_t region_id, const std::string& prefix, int count);
void SendKvBatchDelete(int64_t region_id, const std::string& key);
void SendKvDeleteRange(int64_t region_id, const std::string& prefix);
void SendKvScan(int64_t region_id, const std::string& prefix);
void SendKvCompareAndSet(int64_t region_id, const std::string& key);
void SendKvBatchCompareAndSet(int64_t region_id, const std::string& prefix, int count);
void SendKvScanBeginV2(int64_t region_id, int64_t scan_id);
void SendKvScanContinueV2(int64_t region_id, int64_t scan_id);
void SendKvScanReleaseV2(int64_t region_id, int64_t scan_id);

// Txn
std::string OctalToHex(const std::string& str);
std::string StringToHex(const std::string& key);
std::string HexToString(const std::string& hex);
std::string VectorPrefixToHex(char prefix, int64_t part_id);
std::string VectorPrefixToHex(char prefix, int64_t part_id, int64_t vector_id);
std::string TablePrefixToHex(char prefix, const std::string& user_key);
std::string TablePrefixToHex(char prefix, int64_t part_id);
std::string TablePrefixToHex(char prefix, int64_t part_id, const std::string& user_key);

std::string HexToTablePrefix(const std::string& hex, bool has_part_id = false);
std::string HexToVectorPrefix(const std::string& hex);
bool TxnGetRegion(int64_t region_id, dingodb::pb::common::Region& region);
std::string GetServiceName(const dingodb::pb::common::Region& region);

void SendTxnGet(int64_t region_id, bool rc, std::string key, bool key_is_hex, int64_t start_ts, int64_t resolve_locks);

void SendTxnBatchGet(int64_t region_id, bool rc, std::string key, std::string key2, bool key_is_hex, int64_t start_ts,
                     int64_t resolve_locks);
void SendTxnScan(int64_t region_id, bool rc, std::string start_key, std::string end_key, int64_t limit,
                 int64_t start_ts, bool is_reverse, bool key_only, int64_t resolve_locks, bool key_is_hex,
                 bool with_start, bool with_end);

void SendTxnPessimisticLock(int64_t region_id, bool rc, std::string primary_lock, bool key_is_hex, int64_t start_ts,
                            int64_t lock_ttl, int64_t for_update_ts, std::string mutation_op, std::string key,
                            std::string value, bool value_is_hex);

void SendTxnPessimisticRollback(int64_t region_id, bool rc, int64_t start_ts, int64_t for_update_ts, std::string key,
                                bool key_is_hex);

void SendTxnPrewrite(int64_t region_id, bool rc, std::string primary_lock, bool key_is_hex, int64_t start_ts,
                     int64_t lock_ttl, int64_t txn_size, bool try_one_pc, int64_t max_commit_ts,
                     std::string mutation_op, std::string key, std::string key2, std::string value, std::string value2,
                     bool value_is_hex, std::string extra_data, int64_t for_update_ts, int64_t vector_id,
                     int64_t document_id, std::string document_text1, std::string document_text2);

void SendTxnCommit(int64_t region_id, bool rc, int64_t start_ts, int64_t commit_ts, std::string key, std::string key2,
                   bool key_is_hex);

void SendTxnCheckTxnStatus(int64_t region_id, bool rc, std::string primary_key, bool key_is_hex, int64_t lock_ts,
                           int64_t caller_start_ts, int64_t current_ts);

void SendTxnResolveLock(int64_t region_id, bool rc, int64_t start_ts, int64_t commit_ts, std::string key,
                        bool key_is_hex);

void SendTxnBatchRollback(int64_t region_id, bool rc, std::string key, std::string key2, bool key_is_hex,
                          int64_t start_ts);

void SendTxnScanLock(int64_t region_id, bool rc, int64_t max_ts, std::string start_key, std::string end_key,
                     bool key_is_hex, int64_t limit);

void SendTxnHeartBeat(int64_t region_id, bool rc, std::string primary_lock, int64_t start_ts, int64_t advise_lock_ttl,
                      bool key_is_hex);
void SendTxnGc(int64_t region_id, bool rc, int64_t safe_point_ts);

void SendTxnDeleteRange(int64_t region_id, bool rc, std::string start_key, std::string end_key, bool key_is_hex);

void SendTxnDump(int64_t region_id, bool rc, std::string start_key, std::string end_key, bool key_is_hex,
                 int64_t start_ts, int64_t end_ts);

void StoreSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region, bool rc,
                          std::string primary_lock, bool key_is_hex, int64_t start_ts, int64_t lock_ttl,
                          int64_t txn_size, bool try_one_pc, int64_t max_commit_ts, std::string mutation_op,
                          std::string key, std::string key2, std::string value, std::string value2, bool value_is_hex,
                          std::string extra_data, int64_t for_update_ts);

void IndexSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region, bool rc,
                          std::string primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size,
                          bool try_one_pc, int64_t max_commit_ts, std::string mutation_op, std::string extra_data,
                          int64_t for_update_ts, int64_t vector_id);

void DocumentSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region, bool rc,
                             std::string primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size,
                             bool try_one_pc, int64_t max_commit_ts, std::string mutation_op, std::string extra_data,
                             int64_t for_update_ts, int64_t document_id, std::string document_text1,
                             std::string document_text2);

// region
void SendAddRegion(int64_t region_id, const std::string& raft_group, std::vector<std::string> raft_addrs);
void SendChangeRegion(int64_t region_id, const std::string& raft_group, std::vector<std::string> raft_addrs);
void SendMergeRegion(int64_t source_region_id, int64_t target_region_id);
void SendDestroyRegion(int64_t region_id);
void SendSnapshot(int64_t region_id);
void BatchSendAddRegion(int start_region_id, int region_count, int thread_num, const std::string& raft_group,
                        std::vector<std::string>& raft_addrs);
void SendSnapshotVectorIndex(int64_t vector_index_id);
void SendCompact(const std::string& cf_name);
void GetMemoryStats();
void ReleaseFreeMemory(double rate);

// test
void TestBatchPut(std::shared_ptr<Context> ctx);
void TestBatchPutGet(int64_t region_id, int thread_num, int req_num, const std::string& prefix);
void TestRegionLifecycle(int64_t region_id, const std::string& raft_group, std::vector<std::string>& raft_addrs,
                         int region_count, int thread_num, int req_num, const std::string& prefix);
void TestDeleteRangeWhenTransferLeader(int64_t region_id, int req_num, const std::string& prefix);
void AutoTest(std::shared_ptr<Context> ctx);
void AutoMergeRegion(std::shared_ptr<Context> ctx);

// Table
void AutoDropTable(std::shared_ptr<Context> ctx);

void CheckTableDistribution(std::shared_ptr<Context> ctx);
void CheckIndexDistribution(std::shared_ptr<Context> ctx);

}  // namespace client_v2

#endif  // DINGODB_CLIENT_STORE_CLIENT_FUNCTION_H_

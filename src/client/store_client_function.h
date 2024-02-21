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

namespace client {

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
};

// meta
dingodb::pb::meta::TableDefinition SendGetIndex(int64_t index_id);
dingodb::pb::meta::TableDefinition SendGetTable(int64_t table_id);
dingodb::pb::meta::TableRange SendGetTableRange(int64_t table_id);

// coordinator
dingodb::pb::common::Region SendQueryRegion(int64_t region_id);
dingodb::pb::store::Context GetRegionContext(int64_t region_id);

// vector
void SendVectorSearch(int64_t region_id, uint32_t dimension, uint32_t topn);
void SendVectorSearchDebug(int64_t region_id, uint32_t dimension, int64_t start_vector_id, uint32_t topn,
                           uint32_t batch_count, const std::string& key, const std::string& value);
void SendVectorRangeSearch(int64_t region_id, uint32_t dimension, double radius);
void SendVectorRangeSearchDebug(int64_t region_id, uint32_t dimension, int64_t start_vector_id, double radius,
                                uint32_t batch_count, const std::string& key, const std::string& value);
void SendVectorBatchSearch(int64_t region_id, uint32_t dimension, uint32_t topn, uint32_t batch_count);
void SendVectorBatchQuery(int64_t region_id, std::vector<int64_t> vector_ids);
void SendVectorAddRetry(std::shared_ptr<Context> ctx);
void SendVectorAdd(std::shared_ptr<Context> ctx);
void SendVectorDelete(int64_t region_id, uint32_t start_id, uint32_t count);
void SendVectorGetMaxId(int64_t region_id);
void SendVectorGetMinId(int64_t region_id);
void SendVectorAddBatch(int64_t region_id, uint32_t dimension, uint32_t count, uint32_t step_count, int64_t start_id,
                        const std::string& file);
void SendVectorScanQuery(int64_t region_id, int64_t start_id, int64_t end_id, int64_t limit, bool is_reverse);
void SendVectorScanDump(int64_t region_id, int64_t start_id, int64_t end_id, int64_t limit, bool is_reverse);
void SendVectorAddBatchDebug(int64_t region_id, uint32_t dimension, uint32_t count, uint32_t step_count,
                             int64_t start_id, const std::string& file);
void SendVectorGetRegionMetrics(int64_t region_id);
void SendVectorCalcDistance(uint32_t dimension, const std::string& alg_type, const std::string& metric_type,
                            int32_t left_vector_size, int32_t right_vector_size, bool is_return_normlize);
void SendCalcDistance();

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

void SendTxnGet(int64_t region_id);
void SendTxnBatchGet(int64_t region_id);
void SendTxnScan(int64_t region_id);
void SendTxnPessimisticLock(int64_t region_id);
void SendTxnPessimisticRollback(int64_t region_id);
void SendTxnPrewrite(int64_t region_id);
void SendTxnCommit(int64_t region_id);
void SendTxnCheckTxnStatus(int64_t region_id);
void SendTxnResolveLock(int64_t region_id);
void SendTxnBatchRollback(int64_t region_id);
void SendTxnScanLock(int64_t region_id);
void SendTxnHeartBeat(int64_t region_id);
void SendTxnGc(int64_t region_id);
void SendTxnDeleteRange(int64_t region_id);
void SendTxnDump(int64_t region_id);

void StoreSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region);
void IndexSendTxnPrewrite(int64_t region_id, const dingodb::pb::common::Region& region);

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

}  // namespace client

#endif  // DINGODB_CLIENT_STORE_CLIENT_FUNCTION_H_

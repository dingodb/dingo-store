
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGODB_CLIENT_STORE_H_
#define DINGODB_CLIENT_STORE_H_

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "CLI/CLI.hpp"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"

namespace client_v2 {

void SetUpStoreSubCommands(CLI::App &app);

// store/index/document commands
struct AddRegionOptions {
  std::string coor_url;
  std::string raft_group;
  std::string raft_addrs;
  int64_t region_id;
};
void SetUpAddRegion(CLI::App &app);
void RunAddRegion(AddRegionOptions const &opt);

struct ChangeRegionOptions {
  std::string coor_url;
  int64_t region_id;
  std::string raft_group;
  std::string raft_addrs;
};
void SetUpChangeRegion(CLI::App &app);
void RunChangeRegion(ChangeRegionOptions const &opt);

struct MergeRegionAtStoreOptions {
  std::string coor_url;
  int64_t source_id;
  int64_t target_id;
};
void SetUpMergeRegionAtStore(CLI::App &app);
void RunMergeRegionAtStore(MergeRegionAtStoreOptions const &opt);

struct DestroyRegionOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpDestroyRegion(CLI::App &app);
void RunDestroyRegion(DestroyRegionOptions const &opt);

struct SnapshotOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSnapshot(CLI::App &app);
void RunSnapshot(SnapshotOptions const &opt);

struct BatchAddRegionOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t region_count;
  int32_t thread_num;
  std::string raft_group;
  std::string raft_addrs;
};
void SetUpBatchAddRegion(CLI::App &app);
void RunBatchAddRegion(BatchAddRegionOptions const &opt);

struct SnapshotVectorIndexOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSnapshotVectorIndex(CLI::App &app);
void RunSnapshotVectorIndex(SnapshotVectorIndexOptions const &opt);

struct CompactOptions {
  std::string store_addrs;
};
void SetUpCompact(CLI::App &app);
void RunCompact(CompactOptions const &opt);

struct GetMemoryStatsOptions {
  std::string store_addrs;
};
void SetUpGetMemoryStats(CLI::App &app);
void RunGetMemoryStats(GetMemoryStatsOptions const &opt);

struct ReleaseFreeMemoryOptions {
  std::string store_addrs;
  double rate;
};
void SetUpReleaseFreeMemory(CLI::App &app);
void RunReleaseFreeMemory(ReleaseFreeMemoryOptions const &opt);

struct KvGetOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
};
void SetUpKvGet(CLI::App &app);
void RunKvGet(KvGetOptions const &opt);

struct KvBatchGetOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
  int32_t req_num;
};
void SetUpKvBatchGet(CLI::App &app);
void RunKvBatchGet(KvBatchGetOptions const &opt);

struct KvPutOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
  std::string value;
};
void SetUpKvPut(CLI::App &app);
void RunKvPut(KvPutOptions const &opt);

struct KvBatchPutOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
  int64_t count;
};
void SetUpKvBatchPut(CLI::App &app);
void RunKvBatchPut(KvBatchPutOptions const &opt);

struct KvPutIfAbsentOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
};
void SetUpKvPutIfAbsent(CLI::App &app);
void RunKvPutIfAbsent(KvPutIfAbsentOptions const &opt);

struct KvBatchPutIfAbsentOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
  int64_t count;
};
void SetUpKvBatchPutIfAbsent(CLI::App &app);
void RunKvBatchPutIfAbsent(KvBatchPutIfAbsentOptions const &opt);

struct KvBatchDeleteOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
};
void SetUpKvBatchDelete(CLI::App &app);
void RunKvBatchDelete(KvBatchDeleteOptions const &opt);

struct KvDeleteRangeOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
};
void SetUpKvDeleteRange(CLI::App &app);
void RunKvDeleteRange(KvDeleteRangeOptions const &opt);

struct KvScanOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
};
void SetUpKvScan(CLI::App &app);
void RunKvScan(KvScanOptions const &opt);

struct KvCompareAndSetOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
};
void SetUpKvCompareAndSet(CLI::App &app);
void RunKvCompareAndSet(KvCompareAndSetOptions const &opt);

struct KvBatchCompareAndSetOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
  int64_t count;
};
void SetUpKvBatchCompareAndSet(CLI::App &app);
void RunKvBatchCompareAndSet(KvBatchCompareAndSetOptions const &opt);

struct KvScanBeginV2Options {
  std::string coor_url;
  int64_t region_id;
  int64_t scan_id;
};
void SetUpKvScanBeginV2(CLI::App &app);
void RunKvScanBeginV2(KvScanBeginV2Options const &opt);

struct KvScanContinueV2Options {
  std::string coor_url;
  int64_t region_id;
  int64_t scan_id;
};
void SetUpKvScanContinueV2(CLI::App &app);
void RunKvScanContinueV2(KvScanContinueV2Options const &opt);

struct KvScanReleaseV2Options {
  std::string coor_url;
  int64_t region_id;
  int64_t scan_id;
};
void SetUpKvScanReleaseV2(CLI::App &app);
void RunKvScanReleaseV2(KvScanReleaseV2Options const &opt);

struct TxnGetOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string key;
  bool is_hex;
  int64_t start_ts;
  int64_t resolve_locks;
};
void SetUpTxnGet(CLI::App &app);
void RunTxnGet(TxnGetOptions const &opt);

struct TxnScanOptions {
  std::string coor_url;
  int64_t id;
  bool rc;
  std::string start_key;
  std::string end_key;
  int64_t limit;
  int64_t start_ts;
  bool is_reverse;
  bool key_only;
  int64_t resolve_locks;
  bool is_hex;
};
void SetUpTxnScan(CLI::App &app);
void RunTxnScan(TxnScanOptions const &opt);

struct TxnScanLockOptions {
  std::string coor_url;
  int64_t id;
  bool rc;
  int64_t max_ts;
  std::string start_key;
  std::string end_key;
  bool is_hex;
  int64_t limit;
};
void SetUpTxnScanLock(CLI::App &app);
void RunTxnScanLock(TxnScanLockOptions const &opt);

struct TxnPessimisticLockOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string primary_lock;
  bool is_hex;
  int64_t start_ts;
  int64_t lock_ttl;
  int64_t for_update_ts;
  std::string mutation_op;
  std::string key;
  std::string value;
  bool value_is_hex;
  bool return_values;
};
void SetUpTxnPessimisticLock(CLI::App &app);
void RunTxnPessimisticLock(TxnPessimisticLockOptions const &opt);

struct TxnPessimisticRollbackOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  int64_t start_ts;
  int64_t for_update_ts;
  std::string key;
  bool is_hex;
};
void SetUpTxnPessimisticRollback(CLI::App &app);
void RunTxnPessimisticRollback(TxnPessimisticRollbackOptions const &opt);

struct TxnPrewriteOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string primary_lock;
  bool is_hex;
  int64_t start_ts;
  int64_t lock_ttl;
  int64_t txn_size;
  bool try_one_pc;
  int64_t max_commit_ts;
  std::string mutation_op;
  std::string key;
  std::string key2;
  std::string value;
  std::string value2;
  bool value_is_hex;
  std::string extra_data;
  int64_t for_update_ts;

  int64_t vector_id;
  int64_t document_id;
  std::string document_text1;
  std::string document_text2;
};
void SetUpTxnPrewrite(CLI::App &app);
void RunTxnPrewrite(TxnPrewriteOptions const &opt);

struct TxnCommitOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  int64_t start_ts;
  int64_t commit_ts;
  std::string key;
  std::string key2;
  bool is_hex;
};
void SetUpTxnCommit(CLI::App &app);
void RunTxnCommit(TxnCommitOptions const &opt);

struct TxnCheckTxnStatusOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string primary_key;
  bool is_hex;
  int64_t lock_ts;
  int64_t caller_start_ts;
  int64_t current_ts;
};
void SetUpTxnCheckTxnStatus(CLI::App &app);
void RunTxnCheckTxnStatus(TxnCheckTxnStatusOptions const &opt);

struct TxnResolveLockOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  int64_t start_ts;
  int64_t commit_ts;
  std::string key;
  bool is_hex;
};
void SetUpTxnResolveLock(CLI::App &app);
void RunTxnResolveLock(TxnResolveLockOptions const &opt);

struct TxnBatchGetOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string key;
  std::string key2;
  bool is_hex;
  int64_t start_ts;
  int64_t resolve_locks;
};
void SetUpTxnBatchGet(CLI::App &app);
void RunTxnBatchGet(TxnBatchGetOptions const &opt);

struct TxnBatchRollbackOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string key;
  std::string key2;
  bool is_hex;
  int64_t start_ts;
};
void SetUpTxnBatchRollback(CLI::App &app);
void RunTxnBatchRollback(TxnBatchRollbackOptions const &opt);

struct TxnHeartBeatOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string primary_lock;
  int64_t start_ts;
  int64_t advise_lock_ttl;
  bool is_hex;
};
void SetUpTxnHeartBeat(CLI::App &app);
void RunTxnHeartBeat(TxnHeartBeatOptions const &opt);

struct TxnGCOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  int64_t safe_point_ts;
};
void SetUpTxnGC(CLI::App &app);
void RunTxnGC(TxnGCOptions const &opt);

struct TxnDeleteRangeOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string start_key;
  std::string end_key;
  bool is_hex;
};
void SetUpTxnDeleteRange(CLI::App &app);
void RunTxnDeleteRange(TxnDeleteRangeOptions const &opt);

struct TxnDumpOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string start_key;
  std::string end_key;
  bool is_hex;
  int64_t start_ts;
  int64_t end_ts;
};
void SetUpTxnDump(CLI::App &app);
void RunTxnDump(TxnDumpOptions const &opt);

// test operation
struct TestBatchPutOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t region_id;
  int32_t thread_num;
  int64_t req_num;
  std::string prefix;
};
void SetUpTestBatchPut(CLI::App &app);
void RunTestBatchPut(TestBatchPutOptions const &opt);

struct TestBatchPutGetOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t region_id;
  int32_t thread_num;
  int64_t req_num;
  std::string prefix;
};
void SetUpTestBatchPutGet(CLI::App &app);
void RunTestBatchPutGet(TestBatchPutGetOptions const &opt);

struct TestRegionLifecycleOptions {
  std::string coor_url;
  int64_t region_id;
  std::string raft_group;
  std::string raft_addrs;
  int64_t region_count;
  int32_t thread_num;
  int64_t req_num;
  std::string prefix;
};
void SetUpTestRegionLifecycle(CLI::App &app);
void RunTestRegionLifecycle(TestRegionLifecycleOptions const &opt);

struct TestDeleteRangeWhenTransferLeaderOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t req_num;
  std::string prefix;
};
void SetUpTestDeleteRangeWhenTransferLeader(CLI::App &app);
void RunTestDeleteRangeWhenTransferLeader(TestDeleteRangeWhenTransferLeaderOptions const &opt);

struct AutoMergeRegionOptions {
  std::string coor_url;
  std::string store_addrs;
  int64_t table_id;
  int64_t index_id;
};
void SetUpAutoMergeRegion(CLI::App &app);
void RunAutoMergeRegion(AutoMergeRegionOptions const &opt);

// test operation
struct AutoDropTableOptions {
  std::string coor_url;
  int64_t req_num;
};
void SetUpAutoDropTable(CLI::App &app);
void RunAutoDropTable(AutoDropTableOptions const &opt);

struct CheckTableDistributionOptions {
  std::string coor_url;
  int64_t table_id;
  std::string key;
};
void SetUpCheckTableDistribution(CLI::App &app);
void RunCheckTableDistribution(CheckTableDistributionOptions const &opt);

struct CheckIndexDistributionOptions {
  std::string coor_url;
  int64_t table_id;
};
void SetUpCheckIndexDistribution(CLI::App &app);
void RunCheckIndexDistribution(CheckIndexDistributionOptions const &opt);

struct DumpDbOptions {
  std::string coor_url;
  int64_t id;
  std::string db_path;
  int32_t offset;
  int64_t limit;
  std::string exclude_columns;
};
void SetUpDumpDb(CLI::App &app);
void RunDumpDb(DumpDbOptions const &opt);

struct WhichRegionOptions {
  std::string coor_url;
  int64_t id;
  std::string key;
};
void SetUpWhichRegion(CLI::App &app);
void RunWhichRegion(WhichRegionOptions const &opt);

struct DumpRegionOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t offset;
  int32_t limit;
  std::string exclude_columns;
};
void SetUpDumpRegion(CLI::App &app);
void RunDumpRegion(DumpRegionOptions const &opt);

struct RegionMetricsOptions {
  std::string coor_url;
  std::string store_addrs;
  std::vector<int64_t> region_ids;
  int type;
};
void SetUpRegionMetrics(CLI::App &app);
void RunRegionMetrics(RegionMetricsOptions const &opt);

struct QueryRegionStatusOptions {
  std::string coor_url;
  std::string store_addrs;
  std::vector<int64_t> region_ids;
};
void SetUpQueryRegionStatusMetrics(CLI::App &app);
void RunQueryRegionStatus(QueryRegionStatusOptions const &opt);

struct ModifyRegionMetaOptions {
  std::string coor_url;
  std::string store_addrs;
  int64_t region_id;
  int state;
};
void SetUpModifyRegionMeta(CLI::App &app);
void RunModifyRegionMeta(ModifyRegionMetaOptions const &opt);

// raw kv
void SendKvGet(KvGetOptions const &opt, std::string &value);
void SendKvBatchGet(KvBatchGetOptions const &opt);
int SendKvPut(KvPutOptions const &opt, std::string value = "");
void SendKvBatchPut(KvBatchPutOptions const &opt);
void SendKvPutIfAbsent(KvPutIfAbsentOptions const &opt);
void SendKvBatchPutIfAbsent(KvBatchPutIfAbsentOptions const &opt);
void SendKvBatchDelete(KvBatchDeleteOptions const &opt);
void SendKvDeleteRange(KvDeleteRangeOptions const &opt);
void SendKvScan(KvScanOptions const &opt);
void SendKvCompareAndSet(KvCompareAndSetOptions const &opt);
void SendKvBatchCompareAndSet(KvBatchCompareAndSetOptions const &opt);
void SendKvScanBeginV2(KvScanBeginV2Options const &opt);
void SendKvScanContinueV2(KvScanContinueV2Options const &opt);
void SendKvScanReleaseV2(KvScanReleaseV2Options const &opt);

// Txn
std::string OctalToHex(const std::string &str);
std::string StringToHex(const std::string &key);
std::string HexToString(const std::string &hex);
std::string VectorPrefixToHex(char prefix, int64_t part_id);
std::string VectorPrefixToHex(char prefix, int64_t part_id, int64_t vector_id);
std::string TablePrefixToHex(char prefix, const std::string &user_key);
std::string TablePrefixToHex(char prefix, int64_t part_id);
std::string TablePrefixToHex(char prefix, int64_t part_id, const std::string &user_key);

std::string HexToTablePrefix(const std::string &hex, bool has_part_id = false);
std::string HexToVectorPrefix(const std::string &hex);
bool TxnGetRegion(int64_t region_id, dingodb::pb::common::Region &region);
std::string GetServiceName(const dingodb::pb::common::Region &region);

void SendTxnGet(TxnGetOptions const &opt);
void SendTxnBatchGet(TxnBatchGetOptions const &opt);
void SendTxnScan(TxnScanOptions const &opt);
void SendTxnPessimisticLock(TxnPessimisticLockOptions const &opt);
void SendTxnPessimisticRollback(TxnPessimisticRollbackOptions const &opt);
void SendTxnPrewrite(TxnPrewriteOptions const &opt);
void SendTxnCommit(TxnCommitOptions const &opt);
void SendTxnCheckTxnStatus(TxnCheckTxnStatusOptions const &opt);
void SendTxnResolveLock(TxnResolveLockOptions const &opt);
void SendTxnBatchRollback(TxnBatchRollbackOptions const &opt);
void SendTxnScanLock(TxnScanLockOptions const &opt);
void SendTxnHeartBeat(TxnHeartBeatOptions const &opt);
void SendTxnGc(TxnGCOptions const &opt);
void SendTxnDeleteRange(TxnDeleteRangeOptions const &opt);
void SendTxnDump(TxnDumpOptions const &opt);

void StoreSendTxnPrewrite(TxnPrewriteOptions const &opt, const dingodb::pb::common::Region &region);

void IndexSendTxnPrewrite(TxnPrewriteOptions const &opt, const dingodb::pb::common::Region &region);

void DocumentSendTxnPrewrite(TxnPrewriteOptions const &opt, const dingodb::pb::common::Region &region);

// region
void SendAddRegion(int64_t region_id, const std::string &raft_group, std::vector<std::string> raft_addrs);
void SendChangeRegion(ChangeRegionOptions const &opt);
void SendMergeRegion(MergeRegionAtStoreOptions const &opt);
void SendDestroyRegion(DestroyRegionOptions const &opt);
void SendSnapshot(SnapshotOptions const &opt);
void BatchSendAddRegion(BatchAddRegionOptions const &opt);
void SendSnapshotVectorIndex(SnapshotVectorIndexOptions const &opt);
void SendCompact(const std::string &cf_name);
void GetMemoryStats();
void ReleaseFreeMemory(ReleaseFreeMemoryOptions const &opt);

dingodb::pb::store::TxnScanResponse SendTxnScanByStreamMode(dingodb::pb::common::Region region,
                                                            const dingodb::pb::common::Range &range, size_t limit,
                                                            int64_t start_ts, int64_t resolve_locks = 0,
                                                            bool key_only = false, bool is_reverse = false);

}  // namespace client_v2

#endif  // DINGODB_CLIENT_STORE_H_
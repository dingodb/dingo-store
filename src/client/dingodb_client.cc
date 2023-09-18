
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
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "client/client_helper.h"
#include "client/client_interation.h"
#include "client/coordinator_client_function.h"
#include "client/store_client_function.h"
#include "client/store_tool_dump.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "proto/common.pb.h"

DEFINE_bool(log_each_request, false, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_uint32(thread_num, 1, "Number of threads sending requests");
DEFINE_uint64(timeout_ms, 5000, "Timeout for each request");
DEFINE_uint32(req_num, 1, "Number of requests");
DEFINE_string(method, "", "Request method");
DEFINE_string(id, "", "Request parameter id, for example: table_id for CreateTable/DropTable");
DEFINE_string(host, "127.0.0.1", "Request parameter host");
DEFINE_uint32(port, 18888, "Request parameter port");
DEFINE_string(peer, "", "Request parameter peer, for example: 127.0.0.1:22101");
DEFINE_string(peers, "", "Request parameter peer, for example: 127.0.0.1:22101,127.0.0.1:22002,127.0.0.1:22103");
DEFINE_string(name, "", "Request parameter name, for example: table_id for GetSchemaByName/GetTableByName");
DEFINE_string(user, "", "Request parameter user");
DEFINE_string(level, "", "Request log level [DEBUG, INFO, WARNING, ERROR, FATAL]");
DEFINE_string(keyring, "", "Request parameter keyring");
DEFINE_string(new_keyring, "", "Request parameter new_keyring");
DEFINE_string(coordinator_addr, "", "coordinator servr addr, for example: 127.0.0.1:8001");
DEFINE_string(addr, "", "coordinator servr addr, for example: 127.0.0.1:8001");
DEFINE_string(group, "0", "Id of the replication group, now coordinator use 0 as groupid");
DEFINE_uint64(split_from_id, 0, "split_from_id");
DEFINE_uint64(split_to_id, 0, "split_to_id");
DEFINE_string(split_key, "", "split_water_shed_key");
DEFINE_uint64(merge_from_id, 0, "merge_from_id");
DEFINE_uint64(merge_to_id, 0, "merge_to_id");
DEFINE_uint64(peer_add_store_id, 0, "peer_add_store_id");
DEFINE_uint64(peer_del_store_id, 0, "peer_del_store_id");
DEFINE_uint64(store_id, 0, "store_id");
DEFINE_uint64(start_region_cmd_id, 0, "start_region_cmd_id");
DEFINE_uint64(end_region_cmd_id, 0, "end_region_cmd_id");
DEFINE_uint64(region_id, 0, "region_id");
DEFINE_uint64(region_cmd_id, 0, "region_cmd_id");
DEFINE_string(store_ids, "1001,1002,1003", "store_ids splited by ,");
DEFINE_uint64(index, 0, "index");
DEFINE_uint32(service_type, 0, "service type for getting leader, 0: meta or coordinator, 2: auto increment");
DEFINE_string(start_key, "", "start_key");
DEFINE_string(end_key, "", "end_key");
DEFINE_string(coor_url, "", "coordinator url");
DEFINE_string(url, "", "coordinator url");
DEFINE_uint64(schema_id, 0, "schema_id");
DEFINE_uint64(replica, 0, "replica num");
DEFINE_string(state, "", "state string");
DEFINE_bool(is_force, false, "force");
DEFINE_uint32(max_elements, 0, "max_elements");
DEFINE_uint32(dimension, 0, "dimension");
DEFINE_uint32(efconstruction, 0, "efconstruction");
DEFINE_uint32(nlinks, 0, "nlinks");
DEFINE_uint32(ncentroids, 10, "ncentroids default : 10");
DEFINE_uint32(part_count, 1, "partition count");
DEFINE_bool(with_auto_increment, true, "with_auto_increment");
DEFINE_string(vector_index_type, "", "vector_index_type:flat, hnsw, ivf_flat");
DEFINE_int32(round_num, 1, "Round of requests");
DEFINE_string(store_addrs, "", "server addrs");
DEFINE_string(raft_addrs, "127.0.0.1:10101:0,127.0.0.1:10102:0,127.0.0.1:10103:0", "raft addrs");
DEFINE_string(key, "", "Request key");
DEFINE_bool(key_is_hex, false, "Request key is hex");
DEFINE_string(value, "", "Request values");
DEFINE_string(prefix, "", "key prefix");
DEFINE_uint64(region_count, 1, "region count");
DEFINE_uint64(table_id, 0, "table id");
DEFINE_string(table_name, "", "table name");
DEFINE_uint64(index_id, 0, "index id");
DEFINE_string(raft_group, "store_default_test", "raft group");
DEFINE_uint64(partition_num, 1, "table partition num");
DEFINE_uint64(start_id, 0, "start id");
DEFINE_uint64(end_id, 0, "end id");
DEFINE_uint64(count, 50, "count");
DEFINE_uint64(vector_id, 0, "vector_id");
DEFINE_uint32(topn, 10, "top n");
DEFINE_uint32(batch_count, 5, "batch count");
DEFINE_uint64(part_id, 0, "part_id");
DEFINE_bool(without_vector, false, "Search vector without output vector data");
DEFINE_bool(without_scalar, false, "Search vector without scalar data");
DEFINE_bool(without_table, false, "Search vector without table data");
DEFINE_uint64(vector_index_id, 0, "vector index id unique. default 0");
DEFINE_string(vector_index_add_cost_file, "./cost.txt", "exec batch vector add. cost time");
DEFINE_int32(step_count, 1024, "step_count");
DEFINE_bool(print_vector_search_delay, false, "print vector search delay");
DEFINE_int32(offset, 0, "offset");
DEFINE_uint64(limit, 50, "limit");
DEFINE_bool(is_reverse, false, "is_revers");
DEFINE_string(scalar_filter_key, "", "Request scalar_filter_key");
DEFINE_string(scalar_filter_value, "", "Request scalar_filter_value");
DEFINE_string(scalar_filter_key2, "", "Request scalar_filter_key");
DEFINE_string(scalar_filter_value2, "", "Request scalar_filter_value");
DEFINE_uint64(ttl, 0, "ttl");
DEFINE_bool(auto_split, false, "auto split");

DEFINE_string(alg_type, "faiss", "use alg type. such as faiss or hnsw");
DEFINE_string(metric_type, "L2", "metric type. such as L2 or IP or cosine");
DEFINE_int32(left_vector_size, 2, "left vector size. <= 0 error");
DEFINE_int32(right_vector_size, 3, "right vector size. <= 0 error");
DEFINE_bool(is_return_normlize, true, "is return normlize default true");

DEFINE_uint64(revision, 0, "revision");
DEFINE_uint64(sub_revision, 0, "sub_revision");
DEFINE_string(range_end, "", "range_end for coor kv");
DEFINE_bool(count_only, false, "count_only for coor kv");
DEFINE_bool(keys_only, false, "keys_only for coor kv");
DEFINE_bool(need_prev_kv, false, "need_prev_kv for coor kv");
DEFINE_bool(ignore_value, false, "ignore_value for coor kv");
DEFINE_bool(ignore_lease, false, "ignore_lease for coor kv");
DEFINE_uint64(lease, 0, "lease for coor kv put");
DEFINE_bool(no_put, false, "watch no put");
DEFINE_bool(no_delete, false, "watch no delete");
DEFINE_bool(wait_on_not_exist_key, false, "watch wait for not exist key");
DEFINE_uint32(max_watch_count, 10, "max_watch_count");
DEFINE_bool(with_vector_ids, false, "Search vector with vector ids list default false");
DEFINE_bool(with_scalar_pre_filter, false, "Search vector with scalar data pre filter");
DEFINE_bool(with_scalar_post_filter, false, "Search vector with scalar data post filter");
DEFINE_uint32(vector_ids_count, 100, "vector ids count");

DEFINE_string(lock_name, "", "Request lock_name");
DEFINE_string(client_uuid, "", "Request client_uuid");

DEFINE_bool(store_create_region, false, "store create region");
DEFINE_string(db_path, "", "rocksdb path");

DEFINE_bool(show_vector, false, "show vector data");
DEFINE_string(metrics_type, "L2", "metrics type");

bvar::LatencyRecorder g_latency_recorder("dingo-store");

const std::map<std::string, std::vector<std::string>> kParamConstraint = {
    {"RaftGroup", {"AddRegion", "ChangeRegion", "BatchAddRegion", "TestBatchPutGet"}},
    {"RaftAddrs", {"AddRegion", "ChangeRegion", "BatchAddRegion", "TestBatchPutGet"}},
    {"ThreadNum", {"BatchAddRegion", "TestBatchPutGet", "TestBatchPutGet"}},
    {"RegionCount", {"BatchAddRegion", "TestBatchPutGet"}},
    {"ReqNum", {"KvBatchGet", "TestBatchPutGet", "TestBatchPutGet", "AutoTest"}},
    {"TableName", {"AutoTest"}},
    {"PartitionNum", {"AutoTest"}},
};

int ValidateParam() {
  if (FLAGS_raft_group.empty()) {
    auto methods = kParamConstraint.find("RaftGroup")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(ERROR) << "missing param raft_group error";
        return -1;
      }
    }
  }

  if (FLAGS_raft_addrs.empty()) {
    auto methods = kParamConstraint.find("RaftAddrs")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(ERROR) << "missing param raft_addrs error";
        return -1;
      }
    }
  }

  if (FLAGS_thread_num == 0) {
    auto methods = kParamConstraint.find("ThreadNum")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(ERROR) << "missing param thread_num error";
        return -1;
      }
    }
  }

  if (FLAGS_region_count == 0) {
    auto methods = kParamConstraint.find("RegionCount")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(ERROR) << "missing param region_count error";
        return -1;
      }
    }
  }

  if (FLAGS_req_num == 0) {
    auto methods = kParamConstraint.find("ReqNum")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(ERROR) << "missing param req_num error";
        return -1;
      }
    }
  }

  if (FLAGS_table_name.empty()) {
    auto methods = kParamConstraint.find("TableName")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(ERROR) << "missing param table_name error";
        return -1;
      }
    }
  }

  if (FLAGS_partition_num == 0) {
    auto methods = kParamConstraint.find("PartitionNum")->second;
    for (const auto& method : methods) {
      if (method == FLAGS_method) {
        DINGO_LOG(ERROR) << "missing param partition_num error";
        return -1;
      }
    }
  }

  return 0;
}

void Sender(std::shared_ptr<client::Context> ctx, const std::string& method, int round_num) {
  auto ret = ValidateParam();
  if (ret < 0) {
    DINGO_LOG(ERROR) << "ValidateParam error";
    return;
  }

  std::vector<std::string> raft_addrs;
  butil::SplitString(FLAGS_raft_addrs, ',', &raft_addrs);

  if (!FLAGS_store_addrs.empty()) {
    if (!client::InteractionManager::GetInstance().CreateStoreInteraction({FLAGS_store_addrs})) {
      return;
    }

  } else if (FLAGS_region_id != 0) {
    // Get store addr from coordinator
    auto status = client::InteractionManager::GetInstance().CreateStoreInteraction(FLAGS_region_id);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Create store interaction failed, error: " << status.error_cstr();
      return;
    }
  }

  for (int i = 0; i < round_num; ++i) {
    DINGO_LOG(INFO) << fmt::format("round: {} / {}", i, round_num);
    // Region operation
    if (method == "AddRegion") {
      client::SendAddRegion(FLAGS_region_id, FLAGS_raft_group, raft_addrs);
    } else if (method == "ChangeRegion") {
      client::SendChangeRegion(FLAGS_region_id, FLAGS_raft_group, raft_addrs);
    } else if (method == "DestroyRegion") {
      client::SendDestroyRegion(FLAGS_region_id);
    } else if (method == "Snapshot") {
      client::SendSnapshot(FLAGS_region_id);
    } else if (method == "BatchAddRegion") {
      client::BatchSendAddRegion(FLAGS_region_id, FLAGS_region_count, FLAGS_thread_num, FLAGS_raft_group, raft_addrs);
    } else if (method == "SnapshotVectorIndex") {
      client::SendSnapshotVectorIndex(FLAGS_region_id);
    } else if (method == "Compact") {
      client::SendCompact("");

      // Kev/Value operation
    } else if (method == "KvGet") {
      std::string value;
      client::SendKvGet(FLAGS_region_id, FLAGS_key, value);
    } else if (method == "KvBatchGet") {
      client::SendKvBatchGet(FLAGS_region_id, FLAGS_prefix, FLAGS_req_num);
    } else if (method == "KvPut") {
      client::SendKvPut(FLAGS_region_id, FLAGS_key);
    } else if (method == "KvBatchPut") {
      client::SendKvBatchPut(FLAGS_region_id, FLAGS_prefix, 100);
    } else if (method == "KvPutIfAbsent") {
      client::SendKvPutIfAbsent(FLAGS_region_id, FLAGS_key);
    } else if (method == "KvBatchPutIfAbsent") {
      client::SendKvBatchPutIfAbsent(FLAGS_region_id, FLAGS_prefix, 100);
    } else if (method == "KvBatchDelete") {
      client::SendKvBatchDelete(FLAGS_region_id, FLAGS_key);
    } else if (method == "KvDeleteRange") {
      client::SendKvDeleteRange(FLAGS_region_id, FLAGS_prefix);
    } else if (method == "KvScan") {
      client::SendKvScan(FLAGS_region_id, FLAGS_prefix);
    } else if (method == "KvCompareAndSet") {
      client::SendKvCompareAndSet(FLAGS_region_id, FLAGS_key);
    } else if (method == "KvBatchCompareAndSet") {
      client::SendKvBatchCompareAndSet(FLAGS_region_id, FLAGS_prefix, 100);
    }

    // txn
    else if (method == "TxnGet") {
      client::SendTxnGet(ctx, FLAGS_region_id);
    } else if (method == "TxnScan") {
      client::SendTxnScan(ctx, FLAGS_region_id);
    } else if (method == "TxnPrewrite") {
      client::SendTxnPrewrite(ctx, FLAGS_region_id);
    } else if (method == "TxnCommit") {
      client::SendTxnCommit(ctx, FLAGS_region_id);
    } else if (method == "TxnCheckTxnStatus") {
      client::SendTxnCheckTxnStatus(ctx, FLAGS_region_id);
    } else if (method == "TxnResolveLock") {
      client::SendTxnResolveLock(ctx, FLAGS_region_id);
    } else if (method == "TxnBatchGet") {
      client::SendTxnBatchGet(ctx, FLAGS_region_id);
    } else if (method == "TxnBatchRollback") {
      client::SendTxnBatchRollback(ctx, FLAGS_region_id);
    } else if (method == "TxnScanLock") {
      client::SendTxnScanLock(ctx, FLAGS_region_id);
    } else if (method == "TxnHeartBeat") {
      client::SendTxnHeartBeat(ctx, FLAGS_region_id);
    } else if (method == "TxnGC") {
      client::SendTxnGc(ctx, FLAGS_region_id);
    } else if (method == "TxnDeleteRange") {
      client::SendTxnDeleteRange(ctx, FLAGS_region_id);
    } else if (method == "TxnDump") {
      client::SendTxnDump(ctx, FLAGS_region_id);
    }

    // Vector operation
    else if (method == "VectorSearch") {
      client::SendVectorSearch(FLAGS_region_id, FLAGS_dimension, FLAGS_topn);
    } else if (method == "VectorSearchDebug") {
      client::SendVectorSearchDebug(FLAGS_region_id, FLAGS_dimension, FLAGS_vector_id, FLAGS_topn, FLAGS_batch_count,
                                    FLAGS_key, FLAGS_value);
    } else if (method == "VectorBatchSearch") {
      client::SendVectorBatchSearch(FLAGS_region_id, FLAGS_dimension, FLAGS_topn, FLAGS_batch_count);
    } else if (method == "VectorBatchQuery") {
      client::SendVectorBatchQuery(FLAGS_region_id, {static_cast<int64_t>(FLAGS_vector_id)});
    } else if (method == "VectorScanQuery") {
      client::SendVectorScanQuery(FLAGS_region_id, FLAGS_start_id, FLAGS_end_id, FLAGS_limit, FLAGS_is_reverse);
    } else if (method == "VectorGetRegionMetrics") {
      client::SendVectorGetRegionMetrics(FLAGS_region_id);
    } else if (method == "VectorAdd") {
      ctx->table_id = FLAGS_table_id;
      ctx->region_id = FLAGS_region_id;
      ctx->dimension = FLAGS_dimension;
      ctx->start_id = FLAGS_start_id;
      ctx->count = FLAGS_count;
      ctx->step_count = FLAGS_step_count;
      ctx->with_scalar = !FLAGS_without_scalar;
      ctx->with_table = !FLAGS_without_table;

      if (ctx->table_id > 0) {
        client::SendVectorAddRetry(ctx);
      } else {
        client::SendVectorAdd(ctx);
      }
    } else if (method == "VectorDelete") {
      client::SendVectorDelete(FLAGS_region_id, FLAGS_start_id, FLAGS_count);
    } else if (method == "VectorGetMaxId") {
      client::SendVectorGetMaxId(FLAGS_region_id);
    } else if (method == "VectorGetMinId") {
      client::SendVectorGetMinId(FLAGS_region_id);
    } else if (method == "VectorAddBatch") {
      client::SendVectorAddBatch(FLAGS_region_id, FLAGS_dimension, FLAGS_count, FLAGS_step_count, FLAGS_start_id,
                                 FLAGS_vector_index_add_cost_file);
    } else if (method == "VectorAddBatchDebug") {
      client::SendVectorAddBatchDebug(FLAGS_region_id, FLAGS_dimension, FLAGS_count, FLAGS_step_count, FLAGS_start_id,
                                      FLAGS_vector_index_add_cost_file);
    } else if (method == "VectorCalcDistance") {
      client::SendVectorCalcDistance(FLAGS_dimension, FLAGS_alg_type, FLAGS_metric_type, FLAGS_left_vector_size,
                                     FLAGS_right_vector_size, FLAGS_is_return_normlize);
    } else if (method == "VectorCount") {
      client::SendVectorCount(FLAGS_region_id, FLAGS_start_id, FLAGS_end_id);

    } else if (method == "CountVectorTable") {
      ctx->table_id = FLAGS_table_id;
      client::CountVectorTable(ctx);

      // Test
    } else if (method == "TestBatchPut") {
      ctx->table_id = FLAGS_table_id;
      ctx->region_id = FLAGS_region_id;
      ctx->thread_num = FLAGS_thread_num;
      ctx->req_num = FLAGS_req_num;

      client::TestBatchPut(ctx);
    } else if (method == "TestBatchPutGet") {
      client::TestBatchPutGet(FLAGS_region_id, FLAGS_thread_num, FLAGS_req_num, FLAGS_prefix);
    } else if (method == "TestRegionLifecycle") {
      client::TestRegionLifecycle(FLAGS_region_id, FLAGS_raft_group, raft_addrs, FLAGS_region_count, FLAGS_thread_num,
                                  FLAGS_req_num, FLAGS_prefix);
    } else if (method == "TestDeleteRangeWhenTransferLeader") {
      client::TestDeleteRangeWhenTransferLeader(ctx, FLAGS_region_id, FLAGS_req_num, FLAGS_prefix);
    }

    // Auto test
    else if (method == "AutoTest") {
      ctx->table_name = FLAGS_table_name;
      ctx->partition_num = FLAGS_partition_num;
      ctx->req_num = FLAGS_req_num;

      AutoTest(ctx);
    }

    // Table operation
    else if (method == "AutoDropTable") {
      ctx->req_num = FLAGS_req_num;
      client::AutoDropTable(ctx);
    }
    // Check table range
    else if (method == "CheckTableDistribution") {
      ctx->table_id = FLAGS_table_id;
      ctx->key = FLAGS_key;
      client::CheckTableDistribution(ctx);

    } else if (method == "CheckIndexDistribution") {
      ctx->table_id = FLAGS_table_id;
      client::CheckIndexDistribution(ctx);

    } else if (method == "DumpDb") {
      ctx->table_id = FLAGS_table_id;
      ctx->index_id = FLAGS_index_id;
      ctx->db_path = FLAGS_db_path;
      ctx->offset = FLAGS_offset;
      ctx->limit = FLAGS_limit;
      if (ctx->table_id == 0 && ctx->index_id == 0) {
        DINGO_LOG(ERROR) << "Param table_id|index_id is error.";
        return;
      }
      if (ctx->db_path.empty()) {
        DINGO_LOG(ERROR) << "Param db_path is error.";
        return;
      }
      if (ctx->offset < 0) {
        DINGO_LOG(ERROR) << "Param offset is error.";
        return;
      }
      if (ctx->limit < 0) {
        DINGO_LOG(ERROR) << "Param limit is error.";
        return;
      }
      client::DumpDb(ctx);
    } else if (method == "DumpVectorIndexDb") {
      ctx->table_id = FLAGS_table_id;
      ctx->index_id = FLAGS_index_id;
      ctx->db_path = FLAGS_db_path;
      ctx->offset = FLAGS_offset;
      ctx->limit = FLAGS_limit;
      ctx->show_vector = FLAGS_show_vector;
      if (ctx->table_id == 0 && ctx->index_id == 0) {
        DINGO_LOG(ERROR) << "Param table_id|index_id is error.";
        return;
      }
      if (ctx->db_path.empty()) {
        DINGO_LOG(ERROR) << "Param db_path is error.";
        return;
      }
      if (ctx->offset < 0) {
        DINGO_LOG(ERROR) << "Param offset is error.";
        return;
      }
      if (ctx->limit < 0) {
        DINGO_LOG(ERROR) << "Param limit is error.";
        return;
      }
      client::DumpVectorIndexDb(ctx);
      // illegal method
    } else {
      DINGO_LOG(ERROR) << "Unknown method: " << method;
      return;
    }

    if (i + 1 < round_num) {
      bthread_usleep(1000 * 1000L);
    }
  }
}

std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction;
std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_meta;
std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version;

bool GetBrpcChannel(const std::string& location, brpc::Channel& channel) {
  braft::PeerId node;
  if (node.parse(location) != 0) {
    DINGO_LOG(ERROR) << "Fail to parse node peer_id " << FLAGS_coordinator_addr;
    return false;
  }

  // rpc for leader access
  if (channel.Init(node.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << location;
    bthread_usleep(FLAGS_timeout_ms * 1000L);
    return false;
  }

  return true;
}

std::string EncodeUint64(int64_t value) {
  std::string str(reinterpret_cast<const char*>(&value), sizeof(value));
  std::reverse(str.begin(), str.end());
  return str;
}

int64_t DecodeUint64(const std::string& str) {
  if (str.size() != sizeof(int64_t)) {
    throw std::invalid_argument("Invalid string size for int64_t decoding");
  }

  std::string reversed_str(str.rbegin(), str.rend());
  int64_t value;
  std::memcpy(&value, reversed_str.data(), sizeof(value));
  return value;
}

void CoordinatorSendDebug() {
  int64_t test1 = 1001;
  auto encode_result = EncodeUint64(test1);
  DINGO_LOG(INFO) << encode_result.size();
  DINGO_LOG(INFO) << dingodb::Helper::StringToHex(encode_result);

  DINGO_LOG(INFO) << "==========================";

  if (FLAGS_start_key.empty() || FLAGS_end_key.empty()) {
    DINGO_LOG(ERROR) << "start_key or end_key is empty";
    return;
  }

  std::string start_key = dingodb::Helper::HexToString(FLAGS_start_key);
  std::string end_key = dingodb::Helper::HexToString(FLAGS_end_key);

  auto real_mid = dingodb::Helper::CalculateMiddleKey(start_key, end_key);
  DINGO_LOG(INFO) << " mid real  = " << dingodb::Helper::StringToHex(real_mid);

  DINGO_LOG(INFO) << "==========================";

  if (start_key.size() < end_key.size()) {
    start_key.resize(end_key.size(), 0);
  } else {
    end_key.resize(start_key.size(), 0);
  }

  std::vector<uint8_t> start_vec(start_key.begin(), start_key.end());
  std::vector<uint8_t> end_vec(end_key.begin(), end_key.end());

  // calc the mid value between start_vec and end_vec
  std::vector<uint8_t> diff = dingodb::Helper::SubtractByteArrays(start_vec, end_vec);
  std::vector<uint8_t> half_diff = dingodb::Helper::DivideByteArrayByTwo(diff);
  std::vector<uint8_t> mid = dingodb::Helper::AddByteArrays(start_vec, half_diff);

  std::string mid_key(mid.begin(), mid.end());

  std::vector<uint8_t> half = dingodb::Helper::DivideByteArrayByTwo(start_vec);

  DINGO_LOG(INFO) << "start_key:    " << dingodb::Helper::StringToHex(start_key);
  DINGO_LOG(INFO) << "end_key:      " << dingodb::Helper::StringToHex(end_key);
  DINGO_LOG(INFO) << "diff:         " << dingodb::Helper::StringToHex(std::string(diff.begin(), diff.end()));
  DINGO_LOG(INFO) << "half_diff:    " << dingodb::Helper::StringToHex(std::string(half_diff.begin(), half_diff.end()));
  DINGO_LOG(INFO) << "half:         " << dingodb::Helper::StringToHex(std::string(half.begin(), half.end()));

  DINGO_LOG(INFO) << "mid_key:      " << dingodb::Helper::StringToHex(mid_key.substr(1, mid_key.size() - 1));
}

int CoordinatorSender() {
  std::string method = FLAGS_method;
  if (FLAGS_method == "RaftAddPeer") {  // raft control
    SendRaftAddPeer();
  } else if (FLAGS_method == "RaftRemovePeer") {
    SendRaftRemovePeer();
  } else if (FLAGS_method == "RaftTransferLeader") {
    SendRaftTransferLeader();
  } else if (FLAGS_method == "RaftSnapshot") {
    SendRaftSnapshot();
  } else if (FLAGS_method == "RaftResetPeer") {
    SendRaftResetPeer();
  } else if (FLAGS_method == "GetNodeInfo") {  // node control
    SendGetNodeInfo();
  } else if (FLAGS_method == "GetLogLevel") {
    SendGetLogLevel();
  } else if (FLAGS_method == "ChangeLogLevel") {
    SendChangeLogLevel();
  } else if (FLAGS_method == "Hello") {
    SendHello(coordinator_interaction);
  } else if (FLAGS_method == "StoreHeartbeat") {
    SendStoreHearbeat(coordinator_interaction, 100);
    SendStoreHearbeat(coordinator_interaction, 200);
    SendStoreHearbeat(coordinator_interaction, 300);
  } else if (FLAGS_method == "CreateStore") {
    SendCreateStore(coordinator_interaction);
  } else if (FLAGS_method == "DeleteStore") {
    SendDeleteStore(coordinator_interaction);
  } else if (FLAGS_method == "UpdateStore") {
    SendUpdateStore(coordinator_interaction);
  } else if (FLAGS_method == "CreateExecutor") {
    SendCreateExecutor(coordinator_interaction);
  } else if (FLAGS_method == "DeleteExecutor") {
    SendDeleteExecutor(coordinator_interaction);
  } else if (FLAGS_method == "CreateExecutorUser") {
    SendCreateExecutorUser(coordinator_interaction);
  } else if (FLAGS_method == "UpdateExecutorUser") {
    SendUpdateExecutorUser(coordinator_interaction);
  } else if (FLAGS_method == "DeleteExecutorUser") {
    SendDeleteExecutorUser(coordinator_interaction);
  } else if (FLAGS_method == "GetExecutorUserMap") {
    SendGetExecutorUserMap(coordinator_interaction);
  } else if (FLAGS_method == "ExecutorHeartbeat") {
    SendExecutorHeartbeat(coordinator_interaction);
  } else if (FLAGS_method == "GetStoreMap") {
    SendGetStoreMap(coordinator_interaction);
  } else if (FLAGS_method == "GetExecutorMap") {
    SendGetExecutorMap(coordinator_interaction);
  } else if (FLAGS_method == "GetRegionMap") {
    SendGetRegionMap(coordinator_interaction);
  } else if (FLAGS_method == "GetDeletedRegionMap") {
    SendGetDeletedRegionMap(coordinator_interaction);
  } else if (FLAGS_method == "AddDeletedRegionMap") {
    SendAddDeletedRegionMap(coordinator_interaction);
  } else if (FLAGS_method == "CleanDeletedRegionMap") {
    SendCleanDeletedRegionMap(coordinator_interaction);
  } else if (FLAGS_method == "GetRegionCount") {
    SendGetRegionCount(coordinator_interaction);
  } else if (FLAGS_method == "GetCoordinatorMap") {
    SendGetCoordinatorMap(coordinator_interaction);
  } else if (FLAGS_method == "CreateRegionId") {
    SendCreateRegionId(coordinator_interaction);
  } else if (FLAGS_method == "QueryRegion") {
    SendQueryRegion(coordinator_interaction);
  } else if (FLAGS_method == "CreateRegionForSplit") {
    SendCreateRegionForSplit(coordinator_interaction);
  } else if (FLAGS_method == "DropRegion") {
    SendDropRegion(coordinator_interaction);
  } else if (FLAGS_method == "DropRegionPermanently") {
    SendDropRegionPermanently(coordinator_interaction);
  } else if (FLAGS_method == "SplitRegion") {
    SendSplitRegion(coordinator_interaction);
  } else if (FLAGS_method == "MergeRegion") {
    SendMergeRegion(coordinator_interaction);
  } else if (FLAGS_method == "AddPeerRegion") {
    SendAddPeerRegion(coordinator_interaction);
  } else if (FLAGS_method == "RemovePeerRegion") {
    SendRemovePeerRegion(coordinator_interaction);
  } else if (FLAGS_method == "TransferLeaderRegion") {
    SendTransferLeaderRegion(coordinator_interaction);
  } else if (FLAGS_method == "GetOrphanRegion") {
    SendGetOrphanRegion(coordinator_interaction);
  } else if (FLAGS_method == "ScanRegions") {
    SendScanRegions(coordinator_interaction);
  } else if (FLAGS_method == "GetStoreOperation") {
    SendGetStoreOperation(coordinator_interaction);
  } else if (FLAGS_method == "GetTaskList") {
    SendGetTaskList(coordinator_interaction);
  } else if (FLAGS_method == "CleanTaskList") {
    SendCleanTaskList(coordinator_interaction);
  } else if (FLAGS_method == "CleanStoreOperation") {
    SendCleanStoreOperation(coordinator_interaction);
  } else if (FLAGS_method == "AddStoreOperation") {
    SendAddStoreOperation(coordinator_interaction);
  } else if (FLAGS_method == "RemoveStoreOperation") {
    SendRemoveStoreOperation(coordinator_interaction);
  } else if (FLAGS_method == "GetRegionCmd") {
    SendGetRegionCmd(coordinator_interaction);
  } else if (FLAGS_method == "GetStoreMetrics") {
    SendGetStoreMetrics(coordinator_interaction);
  } else if (FLAGS_method == "DeleteStoreMetrics") {
    SendDeleteStoreMetrics(coordinator_interaction);
  } else if (FLAGS_method == "GetRegionMetrics") {
    SendGetRegionMetrics(coordinator_interaction);
  } else if (FLAGS_method == "DeleteRegionMetrics") {
    SendDeleteRegionMetrics(coordinator_interaction);
  } else if (FLAGS_method == "GetSchemas") {  // meta control
    SendGetSchemas(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetSchema") {
    SendGetSchema(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetSchemaByName") {
    SendGetSchemaByName(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetTablesBySchema") {
    SendGetTablesBySchema(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetTablesCount") {
    SendGetTablesCount(coordinator_interaction_meta);
  } else if (FLAGS_method == "CreateTable") {
    SendCreateTable(coordinator_interaction_meta, false);
  } else if (FLAGS_method == "CreateTableWithIncrement") {
    SendCreateTable(coordinator_interaction_meta, true);
  } else if (FLAGS_method == "CreateTableId") {
    SendCreateTableId(coordinator_interaction_meta);
  } else if (FLAGS_method == "DropTable") {
    SendDropTable(coordinator_interaction_meta);
  } else if (FLAGS_method == "CreateSchema") {
    SendCreateSchema(coordinator_interaction_meta);
  } else if (FLAGS_method == "DropSchema") {
    SendDropSchema(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetTable") {
    SendGetTable(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetTableByName") {
    SendGetTableByName(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetTableRange") {
    SendGetTableRange(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetTableMetrics") {
    SendGetTableMetrics(coordinator_interaction_meta);
  } else if (FLAGS_method == "SwitchAutoSplit") {
    SendSwitchAutoSplit(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetDeletedTable") {
    SendGetDeletedTable(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetDeletedIndex") {
    SendGetDeletedIndex(coordinator_interaction_meta);
  } else if (FLAGS_method == "CleanDeletedTable") {
    SendCleanDeletedTable(coordinator_interaction_meta);
  } else if (FLAGS_method == "CleanDeletedIndex") {
    SendCleanDeletedIndex(coordinator_interaction_meta);
  }

  // indexes
  else if (FLAGS_method == "GetIndexes") {
    SendGetIndexes(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetIndexsCount") {
    SendGetIndexesCount(coordinator_interaction_meta);
  } else if (FLAGS_method == "CreateIndex") {
    SendCreateIndex(coordinator_interaction_meta);
  } else if (FLAGS_method == "CreateIndexId") {
    SendCreateIndexId(coordinator_interaction_meta);
  } else if (FLAGS_method == "UpdateIndex") {
    SendUpdateIndex(coordinator_interaction_meta);
  } else if (FLAGS_method == "DropIndex") {
    SendDropIndex(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetIndex") {
    SendGetIndex(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetIndexByName") {
    SendGetIndexByName(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetIndexRange") {
    SendGetIndexRange(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetIndexMetrics") {
    SendGetIndexMetrics(coordinator_interaction_meta);
  }
  // table index
  else if (FLAGS_method == "GenerateTableIds") {
    SendGenerateTableIds(coordinator_interaction_meta);
  } else if (FLAGS_method == "CreateTables") {
    SendCreateTables(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetTables") {
    SendGetTables(coordinator_interaction_meta);
  } else if (FLAGS_method == "DropTables") {
    SendDropTables(coordinator_interaction_meta);
  }

  // auto increment
  else if (FLAGS_method == "GetAutoIncrements") {  // auto increment
    SendGetAutoIncrements(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetAutoIncrement") {
    SendGetAutoIncrement(coordinator_interaction_meta);
  } else if (FLAGS_method == "CreateAutoIncrement") {
    SendCreateAutoIncrement(coordinator_interaction_meta);
  } else if (FLAGS_method == "UpdateAutoIncrement") {
    SendUpdateAutoIncrement(coordinator_interaction_meta);
  } else if (FLAGS_method == "GenerateAutoIncrement") {
    SendGenerateAutoIncrement(coordinator_interaction_meta);
  } else if (FLAGS_method == "DeleteAutoIncrement") {
    SendDeleteAutoIncrement(coordinator_interaction_meta);
  }

  // version kv
  else if (FLAGS_method == "LeaseGrant") {
    SendLeaseGrant(coordinator_interaction_version);
  } else if (FLAGS_method == "LeaseRevoke") {
    SendLeaseRevoke(coordinator_interaction_version);
  } else if (FLAGS_method == "LeaseRenew") {
    SendLeaseRenew(coordinator_interaction_version);
  } else if (FLAGS_method == "LeaseQuery") {
    SendLeaseQuery(coordinator_interaction_version);
  } else if (FLAGS_method == "ListLeases") {
    SendListLeases(coordinator_interaction_version);
  }

  // coordinator kv
  else if (FLAGS_method == "GetRawKvIndex") {
    SendGetRawKvIndex(coordinator_interaction_version);
  } else if (FLAGS_method == "GetRawKvRev") {
    SendGetRawKvRev(coordinator_interaction_version);
  } else if (FLAGS_method == "CoorKvRange") {
    SendCoorKvRange(coordinator_interaction_version);
  } else if (FLAGS_method == "CoorKvPut") {
    SendCoorKvPut(coordinator_interaction_version);
  } else if (FLAGS_method == "CoorKvDeleteRange") {
    SendCoorKvDeleteRange(coordinator_interaction_version);
  } else if (FLAGS_method == "CoorKvCompaction") {
    SendCoorKvCompaction(coordinator_interaction_version);
  }

  // coordinator watch
  else if (FLAGS_method == "OneTimeWatch") {
    SendOneTimeWatch(coordinator_interaction_version);
  } else if (FLAGS_method == "Lock") {
    SendLock(coordinator_interaction_version);
  }

  // tso
  else if (FLAGS_method == "GenTso") {
    SendGenTso(coordinator_interaction_meta);
  }

  // tools
  else if (method == "KeyToHex") {
    if (FLAGS_key.empty()) {
      DINGO_LOG(ERROR) << "key is empty";
      exit(-1);
    }
    auto str = client::StringToHex(FLAGS_key);
    DINGO_LOG(INFO) << fmt::format("key: {} to hex: {}", FLAGS_key, str);
  } else if (method == "HexToKey") {
    if (FLAGS_key.empty()) {
      DINGO_LOG(ERROR) << "key is empty";
      exit(-1);
    }
    auto str = client::HexToString(FLAGS_key);
    DINGO_LOG(INFO) << fmt::format("hex: {} to key: {}", FLAGS_key, str);
  } else if (method == "VectorPrefixToHex") {
    if (FLAGS_vector_id == 0) {
      DINGO_LOG(ERROR) << "vector_id is empty";
      exit(-1);
    }
    if (FLAGS_part_id == 0) {
      DINGO_LOG(ERROR) << "part_id is empty";
      exit(-1);
    }
    auto str = client::VectorPrefixToHex(FLAGS_part_id, FLAGS_vector_id);
    DINGO_LOG(INFO) << fmt::format("part_id: {}, vector_id {} to key: {}", FLAGS_part_id, FLAGS_vector_id, str);
  } else if (method == "DecodeVectorPrefix") {
    if (FLAGS_key.empty()) {
      DINGO_LOG(ERROR) << "key is empty";
      exit(-1);
    }
    auto str = client::HexToVectorPrefix(FLAGS_key);
    DINGO_LOG(INFO) << fmt::format("hex: {} to key: {}", FLAGS_key, str);
  } else if (method == "OctalToHex") {
    if (FLAGS_key.empty()) {
      DINGO_LOG(ERROR) << "key is empty";
      exit(-1);
    }
    auto str = client::OctalToHex(FLAGS_key);
    DINGO_LOG(INFO) << fmt::format("oct: {} to hex: {}", FLAGS_key, str);
  }

  // debug
  else if (FLAGS_method == "CoordinatorDebug") {
    CoordinatorSendDebug();
  } else {
    DINGO_LOG(INFO) << " not coordinator method, try to send to store";
    return -1;
  }

  return 0;
}

std::shared_ptr<client::Context> global_ctx;

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  google::InitGoogleLogging(argv[0]);

  if (argc > 1) {
    if (dingodb::Helper::IsExistPath(argv[1])) {
      google::SetCommandLineOption("flagfile", argv[1]);
    } else {
      FLAGS_method = argv[1];
    }
  }

  google::ParseCommandLineFlags(&argc, &argv, true);

  if (dingodb::FLAGS_show_version || FLAGS_method.empty()) {
    dingodb::DingoShowVerion();
    printf("Usage: %s [method] [paramters]\n", argv[0]);
    printf("Example: %s CreateTable --name=test_table_name\n", argv[0]);
    exit(-1);
  }

  if (FLAGS_coor_url.empty()) {
    DINGO_LOG(ERROR) << "coordinator url is empty, try to use file://./coor_list";
    FLAGS_coor_url = "file://./coor_list";
  }

  if (!FLAGS_url.empty()) {
    FLAGS_coor_url = FLAGS_url;
  }

  auto ctx = std::make_shared<client::Context>();
  if (!FLAGS_coor_url.empty()) {
    std::string path = FLAGS_coor_url;
    path = path.replace(path.find("file://"), 7, "");
    auto addrs = client::Helper::GetAddrsFromFile(path);
    if (addrs.empty()) {
      DINGO_LOG(ERROR) << "url not find addr, path=" << path;
      return -1;
    }

    auto coordinator_interaction = std::make_shared<client::ServerInteraction>();
    if (!coordinator_interaction->Init(addrs)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << FLAGS_coor_url;
      return -1;
    }

    client::InteractionManager::GetInstance().SetCoorinatorInteraction(coordinator_interaction);
  }

  // this is for legacy coordinator_client use, will be removed in the future
  if (!FLAGS_coor_url.empty()) {
    coordinator_interaction = std::make_shared<dingodb::CoordinatorInteraction>();
    if (!coordinator_interaction->InitByNameService(
            FLAGS_coor_url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeCoordinator)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << FLAGS_coor_url;
      return -1;
    }

    coordinator_interaction_meta = std::make_shared<dingodb::CoordinatorInteraction>();
    if (!coordinator_interaction_meta->InitByNameService(
            FLAGS_coor_url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeMeta)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction_meta, please check parameter --url=" << FLAGS_coor_url;
      return -1;
    }

    coordinator_interaction_version = std::make_shared<dingodb::CoordinatorInteraction>();
    if (!coordinator_interaction_version->InitByNameService(
            FLAGS_coor_url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeVersion)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction_version, please check parameter --url="
                       << FLAGS_coor_url;
      return -1;
    }
  }

  if (!FLAGS_addr.empty()) {
    FLAGS_coordinator_addr = FLAGS_addr;
  }

  global_ctx = ctx;

  auto ret = CoordinatorSender();
  if (ret < 0) {
    Sender(ctx, FLAGS_method, FLAGS_round_num);
  }

  return 0;
}

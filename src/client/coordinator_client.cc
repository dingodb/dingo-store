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

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "client/coordinator_client_function.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_string(method, "", "Request method");
DEFINE_string(id, "", "Request parameter id, for example: table_id for CreateTable/DropTable");
DEFINE_string(host, "127.0.0.1", "Request parameter host");
DEFINE_int32(port, 18888, "Request parameter port");
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
DEFINE_int64(split_from_id, 0, "split_from_id");
DEFINE_int64(split_to_id, 0, "split_to_id");
DEFINE_string(split_key, "", "split_water_shed_key");
DEFINE_int64(merge_from_id, 0, "merge_from_id");
DEFINE_int64(merge_to_id, 0, "merge_to_id");
DEFINE_int64(peer_add_store_id, 0, "peer_add_store_id");
DEFINE_int64(peer_del_store_id, 0, "peer_del_store_id");
DEFINE_int64(store_id, 0, "store_id");
DEFINE_int64(region_id, 0, "region_id");
DEFINE_int64(region_cmd_id, 0, "region_cmd_id");
DEFINE_string(store_ids, "1001,1002,1003", "store_ids splited by ,");
DEFINE_int64(index, 0, "index");
DEFINE_uint32(service_type, 0, "service type for getting leader, 0: meta or coordinator, 2: auto increment");
DEFINE_string(start_key, "", "start_key");
DEFINE_string(end_key, "", "end_key");
DEFINE_string(coor_url, "", "coordinator url");
DEFINE_string(url, "", "coordinator url");
DEFINE_int64(schema_id, 0, "schema_id");
DEFINE_int64(replica, 0, "replica num");
DEFINE_string(state, "", "state string");
DEFINE_bool(is_force, false, "force");
DEFINE_int64(max_elements, 0, "max_elements");
DEFINE_int64(dimension, 0, "dimension");
DEFINE_int64(efconstruction, 0, "efconstruction");
DEFINE_int64(nlinks, 0, "nlinks");
DEFINE_bool(with_auto_increment, true, "with_auto_increment");
DEFINE_string(vector_index_type, "", "vector_index_type");

std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction;
std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_meta;

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

std::string EncodeUint64(uint64_t value) {
  std::string str(reinterpret_cast<const char*>(&value), sizeof(value));
  std::reverse(str.begin(), str.end());
  return str;
}

uint64_t DecodeUint64(const std::string& str) {
  if (str.size() != sizeof(uint64_t)) {
    throw std::invalid_argument("Invalid string size for uint64_t decoding");
  }

  std::string reversed_str(str.rbegin(), str.rend());
  uint64_t value;
  std::memcpy(&value, reversed_str.data(), sizeof(value));
  return value;
}

void SendDebug() {
  uint64_t test1 = 1001;
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

void* Sender(void* /*arg*/) {
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
  } else if (FLAGS_method == "GetStoreMetrics") {
    SendGetStoreMetrics(coordinator_interaction);
  } else if (FLAGS_method == "DeleteStoreMetrics") {
    SendDeleteStoreMetrics(coordinator_interaction);
  } else if (FLAGS_method == "GetSchemas") {  // meta control
    SendGetSchemas(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetSchema") {
    SendGetSchema(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetSchemaByName") {
    SendGetSchemaByName(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetTables") {
    SendGetTables(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetTablesCount") {
    SendGetTablesCount(coordinator_interaction_meta);
  } else if (FLAGS_method == "CreateTable") {
    SendCreateTable(coordinator_interaction_meta, false, false);
  } else if (FLAGS_method == "CreateTableWithId") {
    SendCreateTable(coordinator_interaction_meta, true, false);
  } else if (FLAGS_method == "CreateTableWithIncrement") {
    SendCreateTable(coordinator_interaction_meta, false, true);
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
  } else if (FLAGS_method == "GetIndexes") {
    SendGetIndexes(coordinator_interaction_meta);
  } else if (FLAGS_method == "GetIndexsCount") {
    SendGetIndexesCount(coordinator_interaction_meta);
  } else if (FLAGS_method == "CreateIndex") {
    SendCreateIndex(coordinator_interaction_meta, false);
  } else if (FLAGS_method == "CreateIndexWithId") {
    SendCreateIndex(coordinator_interaction_meta, true);
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
  } else if (FLAGS_method == "GetAutoIncrements") {  // auto increment
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
  } else if (FLAGS_method == "Debug") {
    SendDebug();
  } else {
    DINGO_LOG(INFO) << " method illegal , exit";
    return nullptr;
  }

  return nullptr;
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  google::InitGoogleLogging(argv[0]);

  google::ParseCommandLineFlags(&argc, &argv, true);

  if (dingodb::FLAGS_show_version || FLAGS_method.empty()) {
    dingodb::DingoShowVerion();

    printf("Usage: %s --method=[method]\n", argv[0]);
    printf(
        "Methods: [ RaftAddPeer | RaftRemovePeer | RaftTransferLeader | RaftSnapshot | RaftResetPeer | GetNodeInfo | "
        "GetLogLevel | ChangeLogLevel | Hello | StoreHeartbeat | CreateStore | DeleteStore | UpdateStore | "
        "CreateExecutor | DeleteExecutor | CreateExecutorUser | UpdateExecutorUser | DeleteExecutorUser | "
        "GetExecutorUserMap | ExecutorHeartbeat | GetStoreMap | GetExecutorMap | GetRegionMap | GetDeletedRegionMap | "
        "AddDeletedRegionMap | CleanDeletedRegionMap | GetRegionCount | GetCoordinatorMap | QueryRegion | "
        "CreateRegionForSplit | DropRegion | DropRegionPermanently | SplitRegion | MergeRegion | AddPeerRegion | "
        "RemovePeerRegion | TransferLeaderRegion | GetOrphanRegion | GetStoreOperation | GetTaskList | CleanTaskList | "
        "CleanStoreOperation | AddStoreOperation | RemoveStoreOperation | GetStoreMetrics | DeleteStoreMetrics | "
        "GetSchemas | GetSchema | GetSchemaByName | GetTables | GetTablesCount | CreateTable | CreateTableWithId | "
        "CreateTableWithIncrement | CreateTableId | DropTable | CreateSchema | DropSchema | GetTable | GetTableByName "
        "| GetTableRange | GetTableMetrics | GetIndexes | GetIndexsCount | CreateIndex | CreateIndexWithId | "
        "CreateIndexId | DropIndex | GetIndex | GetIndexByName | GetIndexRange | GetIndexMetrics | GetAutoIncrements | "
        "GetAutoIncrement | CreateAutoIncrement | UpdateAutoIncrement | GenerateAutoIncrement | DeleteAutoIncrement | "
        "Debug ]\n");
    exit(-1);
  }

  if (!FLAGS_url.empty()) {
    FLAGS_coor_url = FLAGS_url;
  }

  if (FLAGS_coor_url.empty()) {
    DINGO_LOG(ERROR) << "coordinator url is empty, try to use file://./coor_list";
    FLAGS_coor_url = "file://./coor_list";
  }

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
  }

  if (!FLAGS_addr.empty()) {
    FLAGS_coordinator_addr = FLAGS_addr;
  }

  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    if (bthread_start_background(&tids[i], nullptr, Sender, nullptr) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      return -1;
    }
  }

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }

  return 0;
}

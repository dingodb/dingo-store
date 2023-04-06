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
#include <string>

#include "bthread/bthread.h"
#include "client/coordinator_client_function.h"
#include "common/logging.h"
#include "gflags/gflags.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_string(method, "Hello", "Request method");
DEFINE_string(id, "", "Request parameter id, for example: table_id for CreateTable/DropTable");
DEFINE_string(keyring, "", "Request parameter keyring");
DEFINE_string(coordinator_addr, "", "coordinator servr addr, for example: 127.0.0.1:8001");
DEFINE_string(group, "0", "Id of the replication group, now coordinator use 0 as groupid");

void* Sender(void* /*arg*/) {
  // get leader location
  std::string leader_location = GetLeaderLocation();
  if (leader_location.empty()) {
    DINGO_LOG(WARNING) << "GetLeaderLocation failed, use coordinator_addr instead";
    leader_location = FLAGS_coordinator_addr;
  }

  braft::PeerId leader;
  if (leader.parse(leader_location) != 0) {
    DINGO_LOG(ERROR) << "Fail to parse leader peer_id " << leader_location;
    return nullptr;
  }

  // get orignial node location
  braft::PeerId node;
  if (node.parse(FLAGS_coordinator_addr) != 0) {
    DINGO_LOG(ERROR) << "Fail to parse node peer_id " << FLAGS_coordinator_addr;
    return nullptr;
  }

  // rpc for leader access
  brpc::Channel channel;
  if (channel.Init(leader.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << leader;
    bthread_usleep(FLAGS_timeout_ms * 1000L);
    return nullptr;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub coordinator_stub(&channel);
  dingodb::pb::meta::MetaService_Stub meta_stub(&channel);

  // rpc for node access
  brpc::Channel channel_node;
  if (channel_node.Init(node.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel_node to " << node;
    bthread_usleep(FLAGS_timeout_ms * 1000L);
    return nullptr;
  }
  dingodb::pb::node::NodeService_Stub node_stub(&channel_node);

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (FLAGS_method == "Hello") {
    SendHello(cntl, coordinator_stub);
  } else if (FLAGS_method == "StoreHeartbeat") {
    SendStoreHearbeat(cntl, coordinator_stub, 100);
    cntl.Reset();
    SendStoreHearbeat(cntl, coordinator_stub, 200);
    cntl.Reset();
    SendStoreHearbeat(cntl, coordinator_stub, 300);
  } else if (FLAGS_method == "CreateStore") {
    SendCreateStore(cntl, coordinator_stub);
  } else if (FLAGS_method == "DeleteStore") {
    SendDeleteStore(cntl, coordinator_stub);
  } else if (FLAGS_method == "CreateExecutor") {
    SendCreateExecutor(cntl, coordinator_stub);
  } else if (FLAGS_method == "DeleteExecutor") {
    SendDeleteExecutor(cntl, coordinator_stub);
  } else if (FLAGS_method == "GetStoreMap") {
    SendGetStoreMap(cntl, coordinator_stub);
  } else if (FLAGS_method == "GetExecutorMap") {
    SendGetExecutorMap(cntl, coordinator_stub);
  } else if (FLAGS_method == "GetRegionMap") {
    SendGetRegionMap(cntl, coordinator_stub);
  } else if (FLAGS_method == "GetCoordinatorMap") {
    SendGetCoordinatorMap(cntl, coordinator_stub);
  } else if (FLAGS_method == "GetStoreMetrics") {
    SendGetStoreMetrics(cntl, coordinator_stub);
  } else if (FLAGS_method == "GetNodeInfo") {
    SendGetNodeInfo(cntl, node_stub);
  } else if (FLAGS_method == "GetLogLevel") {
    SendGetLogLevel(cntl, node_stub);
  } else if (FLAGS_method == "ChangeLogLevel") {
    SendChangeLogLevel(cntl, node_stub);
  } else if (FLAGS_method == "GetSchemas") {
    SendGetSchemas(cntl, meta_stub);
  } else if (FLAGS_method == "GetSchema") {
    SendGetSchema(cntl, meta_stub);
  } else if (FLAGS_method == "GetTables") {
    SendGetTables(cntl, meta_stub);
  } else if (FLAGS_method == "GetTablesCount") {
    SendGetTablesCount(cntl, meta_stub);
  } else if (FLAGS_method == "CreateTable") {
    SendCreateTable(cntl, meta_stub, false);
  } else if (FLAGS_method == "CreateTableWithId") {
    SendCreateTable(cntl, meta_stub, true);
  } else if (FLAGS_method == "CreateTableId") {
    SendCreateTableId(cntl, meta_stub);
  } else if (FLAGS_method == "DropTable") {
    SendDropTable(cntl, meta_stub);
  } else if (FLAGS_method == "CreateSchema") {
    SendCreateSchema(cntl, meta_stub);
  } else if (FLAGS_method == "DropSchema") {
    SendDropSchema(cntl, meta_stub);
  } else if (FLAGS_method == "GetTable") {
    SendGetTable(cntl, meta_stub);
  } else if (FLAGS_method == "GetTableRange") {
    SendGetTableRange(cntl, meta_stub);
  } else if (FLAGS_method == "GetTableMetrics") {
    SendGetTableMetrics(cntl, meta_stub);
  } else {
    DINGO_LOG(INFO) << " method illegal , exit";
    return nullptr;
  }

  return nullptr;
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --conf or --coordinator_addr";
    return -1;
  }

  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    if (bthread_start_background(&tids[i], nullptr, Sender, nullptr) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      return -1;
    }
  }

  // DINGO_LOG(INFO) << "Coordinator client is going to quit";
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }

  return 0;
}

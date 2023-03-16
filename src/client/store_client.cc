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

#include <numeric>

#include "braft/raft.h"
#include "braft/route_table.h"
#include "braft/util.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "gflags/gflags.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_int32(round_num, 1, "Round of requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_string(addr, "127.0.0.1:10001", "server addr");
DEFINE_string(method, "KvGet", "Request method");
DEFINE_string(key, "hello", "Request key");
DEFINE_int32(region_id, 111111, "region id");
DEFINE_int32(table_id, 0, "table id");

bvar::LatencyRecorder g_latency_recorder("dingo-store");

const char alphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                         's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

// rand string
std::string genRandomString(int len) {
  std::string result;
  int alphabet_len = sizeof(alphabet);
  for (int i = 0; i < len; ++i) {
    result.append(1, alphabet[rand() % alphabet_len]);
  }

  return result;
}

std::vector<std::string> genKeys(int nums) {
  std::vector<std::string> vec;
  for (int i = 0; i < nums; ++i) {
    vec.push_back(genRandomString(4));
  }

  return vec;
}

int64_t SendKvGet(dingodb::pb::store::StoreService_Stub& stub, uint64_t region_id, const std::string& key,
                  std::string& value) {
  dingodb::pb::store::KvGetRequest request;
  dingodb::pb::store::KvGetResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(region_id);
  request.set_key(key);
  stub.KvGet(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  value = response.value();
  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }

  return cntl.latency_us();
}

void SendKvBatchGet(dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvBatchGetRequest request;
  dingodb::pb::store::KvBatchGetResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);
  for (int i = 0; i < 10; ++i) {
    std::string key = FLAGS_key + "_" + std::to_string(i);
    request.add_keys(key);
  }

  stub.KvBatchGet(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

int64_t SendKvPut(dingodb::pb::store::StoreService_Stub& stub, uint64_t region_id, const std::string& key,
                  const std::string& value) {
  dingodb::pb::store::KvPutRequest request;
  dingodb::pb::store::KvPutResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(key);
  kv->set_value(value);

  stub.KvPut(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }

  return cntl.latency_us();
}

std::map<std::string, std::string> genDataset(int n) {
  std::map<std::string, std::string> dataset;

  for (int i = 0; i < n; ++i) {
    std::string key = genRandomString(32);
    dataset[key] = genRandomString(256);
  }

  return dataset;
}

void TestBatchPutGet(dingodb::pb::store::StoreService_Stub& stub, uint64_t region_id, int num) {
  auto dataset = genDataset(num);

  std::vector<int64_t> latencys;
  for (auto& it : dataset) {
    latencys.push_back(SendKvPut(stub, region_id, it.first, it.second));
  }

  int sum = std::accumulate(latencys.begin(), latencys.end(), 0);
  LOG(INFO) << "Put average latency: " << sum / latencys.size() << " us";

  latencys.clear();
  for (auto& it : dataset) {
    std::string value;
    latencys.push_back(SendKvGet(stub, region_id, it.first, value));
    if (value != it.second) {
      LOG(INFO) << "Not match...";
    }
  }

  sum = std::accumulate(latencys.begin(), latencys.end(), 0);
  LOG(INFO) << "Get average latency: " << sum / latencys.size() << " us";
}

void SendKvBatchPut(dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvBatchPutRequest request;
  dingodb::pb::store::KvBatchPutResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);
  for (int i = 0; i < 10; ++i) {
    std::string key = FLAGS_key + "_" + std::to_string(i);
    auto kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(genRandomString(64));
  }

  stub.KvBatchPut(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void SendKvPutIfAbsent(dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvPutIfAbsentRequest request;
  dingodb::pb::store::KvPutIfAbsentResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(FLAGS_key);
  kv->set_value(genRandomString(64));

  stub.KvPutIfAbsent(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void SendKvBatchPutIfAbsent(dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvBatchPutIfAbsentRequest request;
  dingodb::pb::store::KvBatchPutIfAbsentResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);
  for (int i = 0; i < 10; ++i) {
    std::string key = FLAGS_key + "_" + std::to_string(i);
    auto kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(genRandomString(64));
  }

  stub.KvBatchPutIfAbsent(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void SendAddRegion(dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::AddRegionRequest request;
  dingodb::pb::store::AddRegionResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  dingodb::pb::common::Region* region = request.mutable_region();
  region->set_id(FLAGS_region_id);
  region->set_epoch(1);
  region->set_table_id(10);
  region->set_name("test-" + std::to_string(FLAGS_region_id));
  region->set_state(dingodb::pb::common::REGION_NEW);
  dingodb::pb::common::Range* range = region->mutable_range();
  range->set_start_key("0000000");
  range->set_end_key("11111111");
  auto peer = region->add_peers();
  peer->set_store_id(1001);
  auto raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(10101);

  peer = region->add_peers();
  peer->set_store_id(1002);
  raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(10102);

  peer = region->add_peers();
  peer->set_store_id(1003);
  raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(10103);

  stub.AddRegion(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void SendChangeRegion(dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::ChangeRegionRequest request;
  dingodb::pb::store::ChangeRegionResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  dingodb::pb::common::Region* region = request.mutable_region();
  region->set_id(FLAGS_region_id);
  region->set_epoch(1);
  region->set_table_id(10);
  region->set_name("test-" + std::to_string(FLAGS_region_id));
  dingodb::pb::common::Range* range = region->mutable_range();
  range->set_start_key("0000000");
  range->set_end_key("11111111");
  auto peer = region->add_peers();
  peer->set_store_id(1001);
  auto raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(10101);

  peer = region->add_peers();
  peer->set_store_id(1002);
  raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(10102);

  peer = region->add_peers();
  peer->set_store_id(1003);
  raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(10103);

  stub.ChangeRegion(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void SendDestroyRegion(dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::DestroyRegionRequest request;
  dingodb::pb::store::DestroyRegionResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);

  stub.DestroyRegion(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void SendCreateTable(dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::CreateTableRequest request;
  dingodb::pb::meta::CreateTableResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  // string name = 1;
  auto table_definition = request.mutable_table_definition();
  table_definition->set_name("zihui_table_01");
  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto column = table_definition->add_columns();
    std::string column_name("column_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type(::dingodb::pb::meta::SqlType::SQL_TYPE_INTEGER);
    column->set_element_type(::dingodb::pb::meta::ElementType::ELEM_TYPE_BYTES);
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");
  }

  table_definition->set_version(1);
  table_definition->set_ttl(0);
  table_definition->set_engine(dingodb::pb::common::Engine::ENG_RAFT_STORE);
  // map<string, string> properties = 8;
  auto prop = table_definition->mutable_properties();
  (*prop)["user"] = "zihuideng";

  // partition
  auto partition_rule = table_definition->mutable_table_partition();
  auto part_column = partition_rule->add_columns();
  part_column->assign("column_0");
  auto range_partition = partition_rule->mutable_range_partition();

  for (int i = 0; i < 1; i++) {
    auto* part_range = range_partition->add_ranges();
    auto* part_range_start = part_range->mutable_start_key();
    part_range_start->assign(std::to_string(i * 100));
    auto* part_range_end = part_range->mutable_start_key();
    part_range_end->assign(std::to_string((i + 1) * 100));
  }

  brpc::Controller cntl;
  stub.CreateTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void SendDropTable(dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  table_id->set_entity_id(FLAGS_table_id);

  brpc::Controller cntl;
  stub.DropTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void* sender(void* arg) {
  for (int i = 0; i < FLAGS_round_num; ++i) {
    braft::PeerId leader(FLAGS_addr);
    // rpc
    brpc::Channel channel;
    if (channel.Init(leader.addr, NULL) != 0) {
      LOG(ERROR) << "Fail to init channel to " << leader;
      bthread_usleep(FLAGS_timeout_ms * 1000L);
      continue;
    }
    dingodb::pb::store::StoreService_Stub stub(&channel);
    dingodb::pb::meta::MetaService_Stub meta_stub(&channel);

    if (FLAGS_method == "AddRegion") {
      SendAddRegion(stub);

    } else if (FLAGS_method == "ChangeRegion") {
      SendChangeRegion(stub);

    } else if (FLAGS_method == "DestroyRegion") {
      SendDestroyRegion(stub);

    } else if (FLAGS_method == "KvPut") {
      SendKvPut(stub, FLAGS_region_id, FLAGS_key, genRandomString(64));

    } else if (FLAGS_method == "KvBatchPut") {
      SendKvBatchPut(stub);

    } else if (FLAGS_method == "KvPutIfAbsent") {
      SendKvPutIfAbsent(stub);

    } else if (FLAGS_method == "KvBatchPutIfAbsent") {
      SendKvBatchPutIfAbsent(stub);

    } else if (FLAGS_method == "KvGet") {
      std::string value;
      SendKvGet(stub, FLAGS_region_id, FLAGS_key, value);

    } else if (FLAGS_method == "KvBatchGet") {
      SendKvBatchGet(stub);

    } else if (FLAGS_method == "TestBatchPutGet") {
      TestBatchPutGet(stub, FLAGS_region_id, FLAGS_req_num);
    } else if (FLAGS_method == "CreateTable") {
      SendCreateTable(meta_stub);
    } else if (FLAGS_method == "DropTable") {
      SendDropTable(meta_stub);
    }

    bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  return nullptr;
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);
  std::srand(std::time(nullptr));

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    if (bthread_start_background(&tids[i], NULL, sender, NULL) != 0) {
      LOG(ERROR) << "Fail to create bthread";
      return -1;
    }
  }

  // while (!brpc::IsAskedToQuit()) {
  //   LOG_IF(INFO, !FLAGS_log_each_request)
  //           << "Sending Request"
  //           << " qps=" << g_latency_recorder.qps(1)
  //           << " latency=" << g_latency_recorder.latency(1);
  // }

  // LOG(INFO) << "Store client is going to quit";
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    bthread_join(tids[i], NULL);
  }

  return 0;
}

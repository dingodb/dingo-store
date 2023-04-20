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
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "braft/raft.h"
#include "braft/route_table.h"
#include "braft/util.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "butil/strings/string_split.h"
#include "butil/strings/stringprintf.h"
#include "common/helper.h"
#include "common/logging.h"
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
DEFINE_string(addrs, "127.0.0.1:10001,127.0.0.1:10002,127.0.0.1:10003", "server addrs");
DEFINE_string(raft_addrs, "127.0.0.1:10001,127.0.0.1:10002,127.0.0.1:10003", "server addrs");
DEFINE_string(method, "KvGet", "Request method");
DEFINE_string(key, "hello", "Request key");
DEFINE_int32(region_id, 111111, "region id");
DEFINE_int32(region_count, 1, "region count");
DEFINE_int32(table_id, 0, "table id");

bvar::LatencyRecorder g_latency_recorder("dingo-store");

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

const char kAlphabetV2[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y'};

int GetRandInt() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
  return distrib(gen);
}

// rand string
std::string GenRandomString(int len) {
  std::string result;
  int alphabet_len = sizeof(kAlphabet);

  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
  for (int i = 0; i < len; ++i) {
    result.append(1, kAlphabet[distrib(rng) % alphabet_len]);
  }

  return result;
}

std::string GenRandomStringV2(int len) {
  std::string result;
  int alphabet_len = sizeof(kAlphabetV2);

  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
  for (int i = 0; i < len; ++i) {
    result.append(1, kAlphabetV2[distrib(rng) % alphabet_len]);
  }

  return result;
}

std::vector<std::string> GenKeys(int nums) {
  std::vector<std::string> vec;
  vec.reserve(nums);
  for (int i = 0; i < nums; ++i) {
    vec.push_back(GenRandomString(4));
  }

  return vec;
}

std::shared_ptr<dingodb::pb::store::StoreService_Stub> GenStoreServiceStub(const std::string& addr) {
  braft::PeerId peer(addr);

  std::unique_ptr<brpc::Channel> channel = std::make_unique<brpc::Channel>();
  if (channel->Init(peer.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << peer;
    return nullptr;
  }

  return std::make_shared<dingodb::pb::store::StoreService_Stub>(channel.release(),
                                                                 ::google::protobuf::Service::STUB_OWNS_CHANNEL);
}

std::string GetRedirectAddr(const dingodb::pb::error::Error& error) {
  if (error.errcode() == dingodb::pb::error::ERAFT_NOTLEADER) {
    const auto& location = error.leader_location();
    if (!location.host().empty()) {
      return butil::StringPrintf("%s:%d", location.host().c_str(), location.port());
    } else {
      return "NO_LEADER";
    }
  }

  return "";
}

std::shared_ptr<dingodb::pb::meta::MetaService_Stub> GenMetaServiceStub(const std::string& addr) {
  braft::PeerId peer(addr);

  std::unique_ptr<brpc::Channel> channel = std::make_unique<brpc::Channel>();
  if (channel->Init(peer.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << peer;
    return nullptr;
  }

  return std::make_shared<dingodb::pb::meta::MetaService_Stub>(channel.release(),
                                                               ::google::protobuf::Service::STUB_OWNS_CHANNEL);
}

int64_t SendKvGet(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub, uint64_t region_id,
                  const std::string& key, std::string& value) {
  dingodb::pb::store::KvGetRequest request;
  dingodb::pb::store::KvGetResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(region_id);
  request.set_key(key);
  stub->KvGet(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  } else {
    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString();
    }
  }

  value = response.value();
  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }

  return cntl.latency_us();
}

void SendKvBatchGet(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub) {
  dingodb::pb::store::KvBatchGetRequest request;
  dingodb::pb::store::KvBatchGetResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);
  for (int i = 0; i < 10; ++i) {
    std::string key = FLAGS_key + "_" + std::to_string(i);
    request.add_keys(key);
  }

  stub->KvBatchGet(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  } else {
    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString();
    }
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }
}

int64_t SendKvPut(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub, uint64_t region_id,
                  const std::string& key, const std::string& value, std::string& redirect_addr) {
  dingodb::pb::store::KvPutRequest request;
  dingodb::pb::store::KvPutResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(key);
  kv->set_value(value);

  stub->KvPut(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  } else {
    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString();
    }
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }

  redirect_addr = GetRedirectAddr(response.error());

  return cntl.latency_us();
}

std::map<std::string, std::string> GenDataset(int n) {
  std::map<std::string, std::string> dataset;

  for (int i = 0; i < n; ++i) {
    std::string key = GenRandomStringV2(32);
    dataset[key] = GenRandomString(256);
  }

  return dataset;
}

void TestBatchPutGet(uint64_t region_id, int num) {
  std::vector<std::string> addrs;
  butil::SplitString(FLAGS_addrs, ',', &addrs);

  std::map<std::string, std::shared_ptr<dingodb::pb::store::StoreService_Stub>> stubs;
  for (auto& addr : addrs) {
    stubs.insert({addr, GenStoreServiceStub(addr)});
  }
  auto stub = stubs[addrs[0]];

  auto dataset = GenDataset(num);

  std::vector<int64_t> latencys;
  latencys.reserve(dataset.size());
  for (auto& [key, value] : dataset) {
    std::string redirect_addr = {};
    latencys.push_back(SendKvPut(stub, region_id, key, value, redirect_addr));
    if (redirect_addr == "NO_LEADER") {
      bthread_usleep(1000 * 1000L);
    } else if (!redirect_addr.empty()) {
      stub = stubs[redirect_addr];
    }
  }

  int64_t sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<int64_t>(0));
  DINGO_LOG(INFO) << "Put average latency: " << sum / latencys.size() << " us";

  latencys.clear();
  for (auto& [key, value] : dataset) {
    std::string result_value;
    latencys.push_back(SendKvGet(stub, region_id, key, result_value));
    if (result_value != value) {
      DINGO_LOG(INFO) << "Not match: " << key << " = " << result_value << " expected=" << value;
    }
  }

  sum = std::accumulate(latencys.begin(), latencys.end(), static_cast<int64_t>(0));
  DINGO_LOG(INFO) << "Get average latency: " << sum / latencys.size() << " us";
}

std::string SendKvBatchPut(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub) {
  dingodb::pb::store::KvBatchPutRequest request;
  dingodb::pb::store::KvBatchPutResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);
  for (int i = 0; i < 10; ++i) {
    std::string key = FLAGS_key + "_" + std::to_string(i);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(GenRandomString(64));
  }

  stub->KvBatchPut(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  } else {
    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString();
    }
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }

  return GetRedirectAddr(response.error());
}

void SendKvPutIfAbsent(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub) {
  dingodb::pb::store::KvPutIfAbsentRequest request;
  dingodb::pb::store::KvPutIfAbsentResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(FLAGS_key);
  kv->set_value(GenRandomString(64));

  stub->KvPutIfAbsent(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  } else {
    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString();
    }
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }
}

void SendKvBatchPutIfAbsent(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub) {
  dingodb::pb::store::KvBatchPutIfAbsentRequest request;
  dingodb::pb::store::KvBatchPutIfAbsentResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);
  for (int i = 0; i < 10; ++i) {
    std::string key = FLAGS_key + "_" + std::to_string(i);
    auto* kv = request.add_kvs();
    kv->set_key(key);
    kv->set_value(GenRandomString(64));
  }

  stub->KvBatchPutIfAbsent(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  } else {
    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString();
    }
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }
}

void AddRegion(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub, uint64_t region_id,
               std::vector<std::string>& raft_addrs) {
  dingodb::pb::store::AddRegionRequest request;
  dingodb::pb::store::AddRegionResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  dingodb::pb::common::RegionDefinition* region = request.mutable_region();
  region->set_id(region_id);
  region->set_epoch(1);
  region->set_name("test-" + std::to_string(region_id));
  dingodb::pb::common::Range* range = region->mutable_range();
  range->set_start_key("a");
  range->set_end_key("z");

  int count = 0;
  for (auto& addr : raft_addrs) {
    std::vector<std::string> host_port;
    butil::SplitString(addr, ':', &host_port);

    auto* peer = region->add_peers();
    peer->set_store_id(1000 + (++count));
    auto* raft_loc = peer->mutable_raft_location();
    raft_loc->set_host(host_port[0]);
    raft_loc->set_port(std::stoi(host_port[1]));
  }

  stub->AddRegion(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  } else {
    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString();
    }
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }
}

void SendAddRegion(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub) {
  std::vector<std::string> addrs;
  butil::SplitString(FLAGS_raft_addrs, ',', &addrs);

  AddRegion(stub, FLAGS_region_id, addrs);
}

struct AddRegionParam {
  uint64_t start_region_id;
  int32_t region_count;

  std::vector<std::string> raft_addrs;
  std::map<std::string, std::shared_ptr<dingodb::pb::store::StoreService_Stub>> stubs;
};

void* AdddRegionRoutine(void* arg) {
  std::unique_ptr<AddRegionParam> param(static_cast<AddRegionParam*>(arg));

  for (int i = 0; i < param->region_count; ++i) {
    for (const auto& [_, stub] : param->stubs) {
      AddRegion(stub, param->start_region_id + i, param->raft_addrs);
    }

    bthread_usleep(3 * 1000 * 1000L);
  }

  return nullptr;
}

void BatchSendAddRegion() {
  int count = 0;
  std::vector<std::string> addrs;
  butil::SplitString(FLAGS_addrs, ',', &addrs);

  std::vector<std::string> raft_addrs;
  butil::SplitString(FLAGS_raft_addrs, ',', &raft_addrs);

  std::map<std::string, std::shared_ptr<dingodb::pb::store::StoreService_Stub>> stubs;
  for (auto& addr : addrs) {
    stubs.insert({addr, GenStoreServiceStub(addr)});
  }

  int32_t step = FLAGS_region_count / FLAGS_thread_num;
  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    AddRegionParam* param = new AddRegionParam;
    param->start_region_id = FLAGS_region_id + i * step;
    param->region_count = (i + 1 == FLAGS_thread_num) ? FLAGS_region_count - i * step : step;
    param->stubs = stubs;
    param->raft_addrs = raft_addrs;

    if (bthread_start_background(&tids[i], nullptr, AdddRegionRoutine, param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void SendChangeRegion(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub) {
  dingodb::pb::store::ChangeRegionRequest request;
  dingodb::pb::store::ChangeRegionResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  dingodb::pb::common::RegionDefinition* region = request.mutable_region();
  region->set_id(FLAGS_region_id);
  region->set_epoch(1);
  region->set_name("test-" + std::to_string(FLAGS_region_id));
  dingodb::pb::common::Range* range = region->mutable_range();
  range->set_start_key("0000000");
  range->set_end_key("11111111");
  auto* peer = region->add_peers();
  peer->set_store_id(1001);
  auto* raft_loc = peer->mutable_raft_location();
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

  stub->ChangeRegion(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }
}

void DestroyRegion(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub, uint64_t region_id) {
  dingodb::pb::store::DestroyRegionRequest request;
  dingodb::pb::store::DestroyRegionResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(region_id);

  stub->DestroyRegion(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  } else {
    if (response.error().errcode() != dingodb::pb::error::OK) {
      DINGO_LOG(ERROR) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString();
    }
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }
}

void SendDestroyRegion(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub) {
  DestroyRegion(stub, FLAGS_region_id);
}

void SendCreateTable(std::shared_ptr<dingodb::pb::meta::MetaService_Stub> stub) {
  dingodb::pb::meta::CreateTableRequest request;
  dingodb::pb::meta::CreateTableResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  // string name = 1;
  auto* table_definition = request.mutable_table_definition();
  table_definition->set_name("zihui_table_" + std::to_string(dingodb::Helper::Timestamp()));
  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
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
  auto* prop = table_definition->mutable_properties();
  (*prop)["user"] = "zihuideng";

  // partition
  auto* partition_rule = table_definition->mutable_table_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("column_0");
  auto* range_partition = partition_rule->mutable_range_partition();

  for (int i = 0; i < 1; i++) {
    auto* part_range = range_partition->add_ranges();
    part_range->set_start_key("a");
    part_range->set_end_key("z");
  }

  brpc::Controller cntl;
  stub->CreateTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }
}

void SendDropTable(std::shared_ptr<dingodb::pb::meta::MetaService_Stub> stub) {
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  table_id->set_entity_id(FLAGS_table_id);

  brpc::Controller cntl;
  stub->DropTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }
}

void SendSnapshot(std::shared_ptr<dingodb::pb::store::StoreService_Stub> stub) {
  dingodb::pb::store::SnapshotRequest request;
  dingodb::pb::store::SnapshotResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  request.set_region_id(FLAGS_region_id);

  stub->Snapshot(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
                    << " latency=" << cntl.latency_us() << "us";
  }
}

void* OperationRegionRoutine(void* arg) {
  std::unique_ptr<AddRegionParam> param(static_cast<AddRegionParam*>(arg));

  bthread_usleep((GetRandInt() % 1000) * 1000L);
  for (int i = 0; i < param->region_count; ++i) {
    uint64_t region_id = param->start_region_id + i;

    DINGO_LOG(INFO) << "======Create region " << region_id;
    // Create region
    for (const auto& [_, stub] : param->stubs) {
      AddRegion(stub, region_id, param->raft_addrs);
    }

    bthread_usleep(3 * 1000 * 1000L);
    DINGO_LOG(INFO) << "======Put region " << region_id;
    // Put/Get
    TestBatchPutGet(region_id, FLAGS_req_num);

    bthread_usleep(3 * 1000 * 1000L);

    DINGO_LOG(INFO) << "======Delete region " << region_id;
    // Destroy region
    for (const auto& [_, stub] : param->stubs) {
      DestroyRegion(stub, region_id);
    }

    bthread_usleep(1 * 1000 * 1000L);
  }

  return nullptr;
}

void TestRegionLifecycle() {
  int count = 0;
  std::vector<std::string> addrs;
  butil::SplitString(FLAGS_addrs, ',', &addrs);

  std::vector<std::string> raft_addrs;
  butil::SplitString(FLAGS_raft_addrs, ',', &raft_addrs);

  std::map<std::string, std::shared_ptr<dingodb::pb::store::StoreService_Stub>> stubs;
  for (auto& addr : addrs) {
    stubs.insert({addr, GenStoreServiceStub(addr)});
  }

  int32_t step = FLAGS_region_count / FLAGS_thread_num;
  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    AddRegionParam* param = new AddRegionParam;
    param->start_region_id = FLAGS_region_id + i * step;
    param->region_count = (i + 1 == FLAGS_thread_num) ? FLAGS_region_count - i * step : step;
    param->stubs = stubs;
    param->raft_addrs = raft_addrs;

    if (bthread_start_background(&tids[i], nullptr, OperationRegionRoutine, param) != 0) {
      DINGO_LOG(ERROR) << "Fail to create bthread";
      continue;
    }
  }

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }
}

void* Sender(void*) {
  for (int i = 0; i < FLAGS_round_num; ++i) {
    auto store_stub = GenStoreServiceStub(FLAGS_addr);

    if (FLAGS_method == "AddRegion") {
      SendAddRegion(store_stub);
    } else if (FLAGS_method == "ChangeRegion") {
      SendChangeRegion(store_stub);
    } else if (FLAGS_method == "DestroyRegion") {
      SendDestroyRegion(store_stub);
    } else if (FLAGS_method == "KvPut") {
      std::string redirect_addr = {};
      SendKvPut(store_stub, FLAGS_region_id, FLAGS_key, GenRandomString(64), redirect_addr);
    } else if (FLAGS_method == "KvBatchPut") {
      SendKvBatchPut(store_stub);
    } else if (FLAGS_method == "KvPutIfAbsent") {
      SendKvPutIfAbsent(store_stub);
    } else if (FLAGS_method == "KvBatchPutIfAbsent") {
      SendKvBatchPutIfAbsent(store_stub);
    } else if (FLAGS_method == "KvGet") {
      std::string value;
      SendKvGet(store_stub, FLAGS_region_id, FLAGS_key, value);
    } else if (FLAGS_method == "KvBatchGet") {
      SendKvBatchGet(store_stub);
    } else if (FLAGS_method == "TestBatchPutGet") {
      TestBatchPutGet(FLAGS_region_id, FLAGS_req_num);
    } else if (FLAGS_method == "CreateTable") {
      SendCreateTable(GenMetaServiceStub(FLAGS_addr));
    } else if (FLAGS_method == "DropTable") {
      SendDropTable(GenMetaServiceStub(FLAGS_addr));
    } else if (FLAGS_method == "Snapshot") {
      SendSnapshot(store_stub);
    } else if (FLAGS_method == "BatchAddRegion") {
      BatchSendAddRegion();
    } else if (FLAGS_method == "TestRegionLifecycle") {
      // Create region
      // Put
      // Destroy region
      TestRegionLifecycle();

    } else {
      DINGO_LOG(ERROR) << "unknown method " << FLAGS_method;
    }

    bthread_usleep(1000 * 1000L);
  }

  return nullptr;
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::srand(std::time(nullptr));

  Sender(nullptr);

  return 0;
}

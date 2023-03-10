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

#include "braft/raft.h"
#include "braft/route_table.h"
#include "braft/util.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "gflags/gflags.h"
#include "proto/store.pb.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_string(store_addr, "127.0.0.1:20001", "store server addr");
DEFINE_string(method, "KvGet", "Request method");
DEFINE_string(key, "hello", "Request key");
DEFINE_int32(region_id, 111111, "region id");

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

void sendKvGet(brpc::Controller& cntl, dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvGetRequest request;
  dingodb::pb::store::KvGetResponse response;

  request.set_region_id(FLAGS_region_id);
  request.set_key(FLAGS_key);
  stub.KvGet(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void sendKvBatchGet(brpc::Controller& cntl, dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvBatchGetRequest request;
  dingodb::pb::store::KvBatchGetResponse response;

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

void sendKvPut(brpc::Controller& cntl, dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvPutRequest request;
  dingodb::pb::store::KvPutResponse response;

  request.set_region_id(FLAGS_region_id);
  dingodb::pb::common::KeyValue* kv = request.mutable_kv();
  kv->set_key(FLAGS_key);
  kv->set_value(genRandomString(64));

  stub.KvPut(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void sendKvBatchPut(brpc::Controller& cntl, dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvBatchPutRequest request;
  dingodb::pb::store::KvBatchPutResponse response;

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

void sendKvPutIfAbsent(brpc::Controller& cntl, dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvPutIfAbsentRequest request;
  dingodb::pb::store::KvPutIfAbsentResponse response;

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

void sendKvBatchPutIfAbsent(brpc::Controller& cntl, dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::KvBatchPutIfAbsentRequest request;
  dingodb::pb::store::KvBatchPutIfAbsentResponse response;

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

void sendAddRegion(brpc::Controller& cntl, dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::AddRegionRequest request;
  dingodb::pb::store::AddRegionResponse response;

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
  raft_loc->set_port(20101);

  peer = region->add_peers();
  peer->set_store_id(1002);
  raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(20102);

  peer = region->add_peers();
  peer->set_store_id(1003);
  raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(20103);

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

void sendChangeRegion(brpc::Controller& cntl, dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::ChangeRegionRequest request;
  dingodb::pb::store::ChangeRegionResponse response;

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
  raft_loc->set_port(20101);

  peer = region->add_peers();
  peer->set_store_id(1002);
  raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(20102);

  peer = region->add_peers();
  peer->set_store_id(1004);
  raft_loc = peer->mutable_raft_location();
  raft_loc->set_host("127.0.0.1");
  raft_loc->set_port(20104);

  stub.ChangeRegion(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << " request=" << request.ShortDebugString() << " response=" << response.ShortDebugString()
              << " latency=" << cntl.latency_us() << "us";
  }
}

void sendDestroyRegion(brpc::Controller& cntl, dingodb::pb::store::StoreService_Stub& stub) {
  dingodb::pb::store::DestroyRegionRequest request;
  dingodb::pb::store::DestroyRegionResponse response;

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

void* sender(void* arg) {
  for (int i = 0; i < FLAGS_req_num; ++i) {
    braft::PeerId leader(FLAGS_store_addr);

    // rpc
    brpc::Channel channel;
    if (channel.Init(leader.addr, NULL) != 0) {
      LOG(ERROR) << "Fail to init channel to " << leader;
      bthread_usleep(FLAGS_timeout_ms * 1000L);
      continue;
    }
    dingodb::pb::store::StoreService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);

    if (FLAGS_method == "AddRegion") {
      sendAddRegion(cntl, stub);

    } else if (FLAGS_method == "ChangeRegion") {
      sendAddRegion(cntl, stub);

    } else if (FLAGS_method == "DestroyRegion") {
      sendAddRegion(cntl, stub);

    } else if (FLAGS_method == "KvPut") {
      sendKvPut(cntl, stub);

    } else if (FLAGS_method == "KvBatchPut") {
      sendKvBatchPut(cntl, stub);

    } else if (FLAGS_method == "KvPutIfAbsent") {
      sendKvPutIfAbsent(cntl, stub);

    } else if (FLAGS_method == "KvBatchPutIfAbsent") {
      sendKvBatchPutIfAbsent(cntl, stub);

    } else if (FLAGS_method == "KvGet") {
      sendKvGet(cntl, stub);

    } else if (FLAGS_method == "KvBatchGet") {
      sendKvBatchGet(cntl, stub);
    }

    g_latency_recorder << cntl.latency_us();

    bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  return nullptr;
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);

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

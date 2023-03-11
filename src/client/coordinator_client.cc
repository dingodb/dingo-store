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

#include "braft/raft.h"
#include "braft/route_table.h"
#include "braft/util.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "gflags/gflags.h"
#include "proto/coordinator.pb.h"
#include "proto/meta.pb.h"

DEFINE_bool(log_each_request, true, "Print log for each request");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 500, "Timeout for each request");
DEFINE_string(coordinator_addr, "127.0.0.1:8201", "coordinator server addr");
DEFINE_int32(req_num, 1, "Number of requests");
DEFINE_string(method, "Hello", "Request method");

bvar::LatencyRecorder g_latency_recorder("dingo-coordinator");

void SendHello(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  std::string key = "Hello";
  // const char* op = nullptr;
  request.set_hello(0);
  stub.Hello(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " hello=" << request.hello() << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    LOG(INFO) << response.DebugString();
  }
}

void SendGetStoreMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(1);

  stub.GetStoreMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " get_store_map=" << request.epoch() << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    LOG(INFO) << response.DebugString();
  }
}

void SendGetRegionMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.set_epoch(1);

  stub.GetRegionMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " get_store_map=" << request.epoch() << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    LOG(INFO) << response.DebugString();
  }
}
void SendCreateStore(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::CreateStoreRequest request;
  dingodb::pb::coordinator::CreateStoreResponse response;

  request.set_cluster_id(1);
  stub.CreateStore(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " create store cluster_id =" << request.cluster_id()
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    LOG(INFO) << response.DebugString();
  }
}

void SendStoreHearbeat(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub,
                       uint64_t store_id) {
  dingodb::pb::coordinator::StoreHeartbeatRequest request;
  dingodb::pb::coordinator::StoreHeartbeatResponse response;

  request.set_self_storemap_epoch(1);
  request.set_self_regionmap_epoch(1);
  // mock store
  auto* store = request.mutable_store();
  store->set_id(store_id);
  store->set_state(::dingodb::pb::common::StoreState::STORE_NORMAL);
  auto* server_location = store->mutable_server_location();
  server_location->set_host("127.0.0.1");
  server_location->set_port(19191);
  auto* raft_location = store->mutable_raft_location();
  raft_location->set_host("127.0.0.1");
  raft_location->set_port(19192);
  store->set_resource_tag("DINGO_DEFAULT");

  // mock regions
  for (int i = 0; i < 3; i++) {
    auto* region = request.add_regions();
    region->set_id(store_id * 100 + i);
    region->set_epoch(1);
    std::string region_name("test_region_");
    region_name.append(std::to_string(i));
    region->set_name(region_name);
    region->set_state(::dingodb::pb::common::RegionState::REGION_NORMAL);
    region->set_leader_store_id(1);

    // mock peers
    for (int j = 0; j < 3; j++) {
      auto* peer = region->add_peers();
      peer->set_store_id(store_id);
      auto* server_location = peer->mutable_server_location();
      server_location->set_host("127.0.0.1");
      server_location->set_port(19191);
      auto* raft_location = peer->mutable_server_location();
      raft_location->set_host("127.0.0.1");
      raft_location->set_port(19192);
    }

    // mock range
    auto* range = region->mutable_range();
    const char start_key[] = {0, 0, 0, 0};
    const char end_key[] = {static_cast<char>(255), static_cast<char>(255), static_cast<char>(255),
                            static_cast<char>(255)};

    range->set_start_key(std::string(start_key));
    range->set_end_key(std::string(end_key));

    // mock meta
    region->set_schema_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
    region->set_table_id(2);

    // mock create ts
    region->set_create_timestamp(1677496540);
  }

  // LOG(INFO) << request.DebugString();

  stub.StoreHeartbeat(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    LOG(INFO) << "Received response"
              << " store_heartbeat "
              << " request_attachment=" << cntl.request_attachment().size()
              << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    LOG(INFO) << response.DebugString();
  }
}

void* Sender(void* /*arg*/) {
  while (!brpc::IsAskedToQuit()) {
    braft::PeerId leader(FLAGS_coordinator_addr);

    // rpc
    brpc::Channel channel;
    if (channel.Init(leader.addr, nullptr) != 0) {
      LOG(ERROR) << "Fail to init channel to " << leader;
      bthread_usleep(FLAGS_timeout_ms * 1000L);
      continue;
    }
    dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);

    if (FLAGS_method == "Hello") {
      SendHello(cntl, stub);
    } else if (FLAGS_method == "StoreHeartbeat") {
      SendStoreHearbeat(cntl, stub, 100);
      cntl.Reset();
      SendStoreHearbeat(cntl, stub, 200);
      cntl.Reset();
      SendStoreHearbeat(cntl, stub, 300);
    } else if (FLAGS_method == "CreateStore") {
      SendCreateStore(cntl, stub);
    } else if (FLAGS_method == "GetStoreMap") {
      SendGetStoreMap(cntl, stub);
    } else if (FLAGS_method == "GetRegionMap") {
      SendGetRegionMap(cntl, stub);
    } else {
      LOG(INFO) << " method illegal , exit";
      return nullptr;
    }

    bthread_usleep(FLAGS_timeout_ms * 10000L);
  }
  return nullptr;
}

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<bthread_t> tids;
  tids.resize(FLAGS_thread_num);

  for (int i = 0; i < FLAGS_thread_num; ++i) {
    if (bthread_start_background(&tids[i], nullptr, Sender, nullptr) != 0) {
      LOG(ERROR) << "Fail to create bthread";
      return -1;
    }
  }

  while (!brpc::IsAskedToQuit()) {
    LOG_IF(INFO, !FLAGS_log_each_request)
        << "Sending Request"
        << " qps=" << g_latency_recorder.qps(1) << " latency=" << g_latency_recorder.latency(1);
  }

  LOG(INFO) << "Coordinator client is going to quit";
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    bthread_join(tids[i], nullptr);
  }

  return 0;
}

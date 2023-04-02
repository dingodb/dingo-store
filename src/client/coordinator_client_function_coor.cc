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

#include <string>

#include "client/coordinator_client_function.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "proto/node.pb.h"

DECLARE_bool(log_each_request);
DECLARE_int32(timeout_ms);
DECLARE_string(id);
DECLARE_string(keyring);
DECLARE_string(coordinator_addr);

std::string MessageToJsonString(const google::protobuf::Message& message) {
  std::string json_string;
  google::protobuf::util::JsonOptions options;
  options.always_print_primitive_fields = true;
  google::protobuf::util::Status status = google::protobuf::util::MessageToJsonString(message, &json_string, options);
  if (!status.ok()) {
    std::cerr << "Failed to convert message to JSON: [" << status.message() << "]" << std::endl;
  }
  return json_string;
}

std::string GetLeaderLocation() {
  braft::PeerId leader;
  if (!FLAGS_coordinator_addr.empty()) {
    if (leader.parse(FLAGS_coordinator_addr) != 0) {
      DINGO_LOG(ERROR) << "Fail to parse peer_id " << FLAGS_coordinator_addr;
      return std::string();
    }
  } else {
    DINGO_LOG(ERROR) << "Please set --coordinator_addr";
    return std::string();
  }

  // rpc
  brpc::Channel channel;
  if (channel.Init(leader.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << leader;
    bthread_usleep(FLAGS_timeout_ms * 1000L);
    return std::string();
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);
  dingodb::pb::coordinator::GetCoordinatorMapRequest request;
  dingodb::pb::coordinator::GetCoordinatorMapResponse response;

  request.set_cluster_id(0);

  brpc::Controller cntl;
  stub.GetCoordinatorMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    return std::string();
  }

  auto leader_location = response.leader_location().host() + ":" + std::to_string(response.leader_location().port());
  DINGO_LOG(INFO) << "leader_location: " << leader_location;
  return leader_location;
}

void SendGetNodeInfo(brpc::Controller& cntl, dingodb::pb::node::NodeService_Stub& stub) {
  dingodb::pb::node::GetNodeInfoRequest request;
  dingodb::pb::node::GetNodeInfoResponse response;

  std::string const key = "Hello";
  // const char* op = nullptr;
  request.set_cluster_id(0);
  stub.GetNodeInfo(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " cluster_id=" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetLogLevel(brpc::Controller& cntl, dingodb::pb::node::NodeService_Stub& stub) {
  dingodb::pb::node::GetLogLevelRequest request;
  dingodb::pb::node::GetLogLevelResponse response;
  stub.GetLogLevel(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  if (FLAGS_log_each_request) {
    std::string format_response = MessageToJsonString(response);
    DINGO_LOG(INFO) << "Received response" << format_response
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendChangeLogLevel(brpc::Controller& cntl, dingodb::pb::node::NodeService_Stub& stub) {
  dingodb::pb::node::ChangeLogLevelRequest request;
  dingodb::pb::node::ChangeLogLevelResponse response;

  request.set_log_level(dingodb::pb::node::WARNING);
  auto* log_detail = request.mutable_log_detail();
  log_detail->set_log_buf_secs(10);
  log_detail->set_max_log_size(100);
  log_detail->set_stop_logging_if_full_disk(false);

  stub.ChangeLogLevel(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  std::string const request_format = MessageToJsonString(request);
  std::string const response_format = MessageToJsonString(response);
  DINGO_LOG(INFO) << request_format;
  DINGO_LOG(INFO) << response_format;
}

void SendHello(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  std::string const key = "Hello";
  // const char* op = nullptr;
  request.set_hello(0);
  request.set_get_memory_info(true);
  stub.Hello(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " hello=" << request.hello() << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetStoreMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(1);

  stub.GetStoreMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " get_store_map=" << request.epoch()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetExecutorMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetExecutorMapRequest request;
  dingodb::pb::coordinator::GetExecutorMapResponse response;

  request.set_epoch(1);

  stub.GetExecutorMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " get_executor_map=" << request.epoch()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetCoordinatorMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetCoordinatorMapRequest request;
  dingodb::pb::coordinator::GetCoordinatorMapResponse response;

  request.set_cluster_id(0);

  stub.GetCoordinatorMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " get_coordinator_map=" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetRegionMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.set_epoch(1);

  stub.GetRegionMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " get_store_map=" << request.epoch()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us()
                    << " response=" << MessageToJsonString(response);
    for (const auto& region : response.regionmap().regions()) {
      DINGO_LOG(INFO) << "Region id=" << region.id() << " name=" << region.name()
                      << " state=" << dingodb::pb::common::RegionState_Name(region.state())
                      << " leader_store_id=" << region.leader_store_id();
    }
    // DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendCreateStore(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::CreateStoreRequest request;
  dingodb::pb::coordinator::CreateStoreResponse response;

  request.set_cluster_id(1);
  stub.CreateStore(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " create store cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendDeleteStore(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::DeleteStoreRequest request;
  dingodb::pb::coordinator::DeleteStoreResponse response;

  request.set_cluster_id(1);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  request.set_store_id(std::stol(FLAGS_id));

  if (FLAGS_keyring.empty()) {
    DINGO_LOG(WARNING) << "keyring is empty";
    return;
  }
  auto* keyring = request.mutable_keyring();
  keyring->assign(FLAGS_keyring);

  stub.DeleteStore(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " create store cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendCreateExecutor(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::CreateExecutorRequest request;
  dingodb::pb::coordinator::CreateExecutorResponse response;

  request.set_cluster_id(1);
  stub.CreateExecutor(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " create executor cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendDeleteExecutor(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::DeleteExecutorRequest request;
  dingodb::pb::coordinator::DeleteExecutorResponse response;

  request.set_cluster_id(1);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  request.set_executor_id(std::stol(FLAGS_id));

  if (FLAGS_keyring.empty()) {
    DINGO_LOG(WARNING) << "keyring is empty";
    return;
  }
  auto* keyring = request.mutable_keyring();
  keyring->assign(FLAGS_keyring);

  stub.DeleteExecutor(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " create executor cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
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

  // DINGO_LOG(INFO) << request.DebugString();

  stub.StoreHeartbeat(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " store_heartbeat "
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetStoreMetrics(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetStoreMetricsRequest request;
  dingodb::pb::coordinator::GetStoreMetricsResponse response;

  stub.GetStoreMetrics(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

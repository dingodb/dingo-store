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

#include <cstdint>
#include <string>
#include <vector>

#include "client/coordinator_client_function.h"
#include "common/logging.h"
#include "gflags/gflags_declare.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/node.pb.h"

DECLARE_bool(log_each_request);
DECLARE_int32(timeout_ms);
DECLARE_string(id);
DECLARE_string(host);
DECLARE_int32(port);
DECLARE_string(level);
DECLARE_string(name);
DECLARE_string(user);
DECLARE_string(keyring);
DECLARE_string(new_keyring);
DECLARE_string(coordinator_addr);
DECLARE_int64(split_from_id);
DECLARE_int64(split_to_id);
DECLARE_string(split_key);
DECLARE_int64(merge_from_id);
DECLARE_int64(merge_to_id);
DECLARE_int64(peer_add_store_id);
DECLARE_int64(peer_del_store_id);
DECLARE_int64(store_id);
DECLARE_int64(region_id);
DECLARE_int64(region_cmd_id);
DECLARE_string(store_ids);
DECLARE_int64(index);

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

  using ::dingodb::pb::node::LogLevel;

  if (FLAGS_level == "DEBUG") {
    request.set_log_level(dingodb::pb::node::DEBUG);
  } else if (FLAGS_level == "INFO") {
    request.set_log_level(dingodb::pb::node::INFO);
  } else if (FLAGS_level == "WARNING") {
    request.set_log_level(dingodb::pb::node::WARNING);
  } else if (FLAGS_level == "ERROR") {
    request.set_log_level(dingodb::pb::node::ERROR);
  } else if (FLAGS_level == "FATAL") {
    request.set_log_level(dingodb::pb::node::FATAL);
  } else {
    DINGO_LOG(WARNING) << "level is not valid";
    request.set_log_level(dingodb::pb::node::WARNING);
  }

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

  // print all store's id, state, in_state, create_timestamp, last_seen_timestamp
  for (auto const& store : response.storemap().stores()) {
    DINGO_LOG(INFO) << "store_id=" << store.id() << " state=" << store.state() << " in_state=" << store.in_state()
                    << " create_timestamp=" << store.create_timestamp()
                    << " last_seen_timestamp=" << store.last_seen_timestamp();
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

  if (FLAGS_user.empty()) {
    DINGO_LOG(WARNING) << "user is empty";
    return;
  }
  request.mutable_executor()->mutable_executor_user()->set_user(FLAGS_user);

  if (FLAGS_keyring.empty()) {
    DINGO_LOG(WARNING) << "keyring is empty";
    return;
  }
  request.mutable_executor()->mutable_executor_user()->set_keyring(FLAGS_keyring);

  if (FLAGS_host.empty()) {
    DINGO_LOG(WARNING) << "host is empty";
    return;
  }
  request.mutable_executor()->mutable_server_location()->set_host(FLAGS_host);

  if (FLAGS_port == 0) {
    DINGO_LOG(WARNING) << "port is empty";
    return;
  }
  request.mutable_executor()->mutable_server_location()->set_port(FLAGS_port);

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
  request.mutable_executor()->set_id(FLAGS_id);

  if (FLAGS_user.empty()) {
    DINGO_LOG(WARNING) << "user is empty";
    return;
  }
  request.mutable_executor()->mutable_executor_user()->set_user(FLAGS_user);

  if (FLAGS_keyring.empty()) {
    DINGO_LOG(WARNING) << "keyring is empty";
    return;
  }
  request.mutable_executor()->mutable_executor_user()->set_keyring(FLAGS_keyring);

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

void SendCreateExecutorUser(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::CreateExecutorUserRequest request;
  dingodb::pb::coordinator::CreateExecutorUserResponse response;

  if (FLAGS_user.empty()) {
    DINGO_LOG(WARNING) << "user is empty";
    return;
  }
  request.mutable_executor_user()->set_user(FLAGS_user);

  if (FLAGS_keyring.empty()) {
    DINGO_LOG(WARNING) << "keyring is empty, coordinator will generate a random keyring";
  } else {
    request.mutable_executor_user()->set_keyring(FLAGS_keyring);
  }

  request.set_cluster_id(1);
  stub.CreateExecutorUser(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " create executor user cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendUpdateExecutorUser(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::UpdateExecutorUserRequest request;
  dingodb::pb::coordinator::UpdateExecutorUserResponse response;

  if (FLAGS_user.empty()) {
    DINGO_LOG(WARNING) << "user is empty";
    return;
  }
  request.mutable_executor_user()->set_user(FLAGS_user);

  if (FLAGS_keyring.empty()) {
    DINGO_LOG(WARNING) << "keyring is empty";
    return;
  }
  request.mutable_executor_user()->set_keyring(FLAGS_keyring);

  if (FLAGS_new_keyring.empty()) {
    DINGO_LOG(WARNING) << "new_keyring is empty";
    return;
  }
  request.mutable_executor_user_update()->set_keyring(FLAGS_new_keyring);

  request.set_cluster_id(1);
  stub.UpdateExecutorUser(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " update executor user cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendDeleteExecutorUser(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::DeleteExecutorUserRequest request;
  dingodb::pb::coordinator::DeleteExecutorUserResponse response;

  if (FLAGS_user.empty()) {
    DINGO_LOG(WARNING) << "user is empty";
    return;
  }
  request.mutable_executor_user()->set_user(FLAGS_user);

  if (FLAGS_keyring.empty()) {
    DINGO_LOG(WARNING) << "keyring is empty";
    return;
  }
  request.mutable_executor_user()->set_keyring(FLAGS_keyring);

  request.set_cluster_id(1);
  stub.DeleteExecutorUser(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " delete executor user cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetExecutorUserMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetExecutorUserMapRequest request;
  dingodb::pb::coordinator::GetExecutorUserMapResponse response;

  request.set_cluster_id(1);
  stub.GetExecutorUserMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " get executor user map cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendExecutorHeartbeat(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::ExecutorHeartbeatRequest request;
  dingodb::pb::coordinator::ExecutorHeartbeatResponse response;

  request.set_self_executormap_epoch(1);
  // mock executor
  auto* executor = request.mutable_executor();
  if (!FLAGS_id.empty()) {
    executor->set_id(FLAGS_id);
  } else if (!FLAGS_host.empty()) {
    executor->mutable_server_location()->set_host(FLAGS_host);
    executor->mutable_server_location()->set_port(FLAGS_port);
  } else {
    DINGO_LOG(WARNING) << "id or host can not empty";
    return;
  }
  executor->set_state(::dingodb::pb::common::ExecutorState::EXECUTOR_NORMAL);

  auto* user = executor->mutable_executor_user();
  if (FLAGS_user.empty()) {
    DINGO_LOG(WARNING) << "user is empty,use default";
    user->set_user("administrator");
  } else {
    user->set_user(FLAGS_user);
  }

  if (FLAGS_keyring.empty()) {
    user->set_keyring("TO_BE_CONTINUED");
  } else {
    user->set_keyring(FLAGS_keyring);
  }

  stub.ExecutorHeartbeat(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " executor heartbeat executor_id=" << request.executor().id()
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
  // request.set_self_regionmap_epoch(1);
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
  // for (int i = 0; i < 3; i++) {
  //   auto* region = request.add_regions();
  //   region->set_id(store_id * 100 + i);
  //   region->set_epoch(1);
  //   std::string region_name("test_region_");
  //   region_name.append(std::to_string(i));
  //   region->set_name(region_name);
  //   region->set_state(::dingodb::pb::common::RegionState::REGION_NORMAL);
  //   region->set_leader_store_id(1);

  //   // mock peers
  //   for (int j = 0; j < 3; j++) {
  //     auto* peer = region->add_peers();
  //     peer->set_store_id(store_id);
  //     auto* server_location = peer->mutable_server_location();
  //     server_location->set_host("127.0.0.1");
  //     server_location->set_port(19191);
  //     auto* raft_location = peer->mutable_server_location();
  //     raft_location->set_host("127.0.0.1");
  //     raft_location->set_port(19192);
  //   }

  //   // mock range
  //   auto* range = region->mutable_range();
  //   const char start_key[] = {0, 0, 0, 0};
  //   const char end_key[] = {static_cast<char>(255), static_cast<char>(255), static_cast<char>(255),
  //                           static_cast<char>(255)};

  //   range->set_start_key(std::string(start_key));
  //   range->set_end_key(std::string(end_key));

  //   // mock meta
  //   region->set_schema_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  //   region->set_table_id(2);

  //   // mock create ts
  //   region->set_create_timestamp(butil::gettimeofday_ms());
  // }

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

// region
void SendQueryRegion(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  if (!FLAGS_id.empty()) {
    request.set_region_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty";
    return;
  }

  stub.QueryRegion(&cntl, &request, &response, nullptr);
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

void SendCreateRegionForSplit(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  if (FLAGS_id.empty()) {
    DINGO_LOG(ERROR) << "id is empty (this is the region id to split)";
    return;
  }

  uint64_t region_id_split = std::stoull(FLAGS_id);
  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(region_id_split);
  stub.QueryRegion(&cntl, &query_request, &query_response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
    return;
  }

  if (query_response.region().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return;
  }

  dingodb::pb::common::RegionDefinition new_region_definition;
  new_region_definition.CopyFrom(query_response.region().definition());
  new_region_definition.mutable_range()->set_start_key(query_response.region().range().end_key());
  new_region_definition.mutable_range()->set_end_key(query_response.region().range().start_key());
  new_region_definition.set_name(query_response.region().name() + "_split");
  new_region_definition.set_id(0);

  std::vector<uint64_t> new_region_store_ids;
  for (const auto& it : query_response.region().peers()) {
    new_region_store_ids.push_back(it.store_id());
  }

  dingodb::pb::coordinator::CreateRegionRequest request;
  dingodb::pb::coordinator::CreateRegionResponse response;

  request.set_region_name(new_region_definition.name());
  request.set_replica_num(new_region_definition.peers_size());
  request.mutable_range()->CopyFrom(new_region_definition.range());
  request.set_schema_id(query_response.region().schema_id());
  request.set_table_id(query_response.region().table_id());
  for (auto it : new_region_store_ids) {
    request.add_store_ids(it);
  }
  request.set_split_from_region_id(region_id_split);  // set split from region id

  cntl.Reset();
  stub.CreateRegion(&cntl, &request, &response, nullptr);
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

void SendDropRegion(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::DropRegionRequest request;
  dingodb::pb::coordinator::DropRegionResponse response;

  if (!FLAGS_id.empty()) {
    request.set_region_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty";
    return;
  }

  stub.DropRegion(&cntl, &request, &response, nullptr);
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

void SendDropRegionPermanently(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::DropRegionPermanentlyRequest request;
  dingodb::pb::coordinator::DropRegionPermanentlyResponse response;

  if (!FLAGS_id.empty()) {
    request.set_region_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty";
    return;
  }

  stub.DropRegionPermanently(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << response.DebugString();
  }
}

void SendSplitRegion(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::SplitRegionRequest request;
  dingodb::pb::coordinator::SplitRegionResponse response;

  if (FLAGS_split_to_id > 0) {
    request.mutable_split_request()->set_split_to_region_id(FLAGS_split_to_id);
  } else {
    DINGO_LOG(ERROR) << "split_to_id is empty";
    return;
  }

  if (FLAGS_split_from_id > 0) {
    request.mutable_split_request()->set_split_from_region_id(FLAGS_split_from_id);
  } else {
    DINGO_LOG(ERROR) << "split_from_id is empty";
    return;
  }

  if (!FLAGS_split_key.empty()) {
    request.mutable_split_request()->set_split_watershed_key(FLAGS_split_key);
  } else {
    DINGO_LOG(ERROR) << "split_key is empty, will auto generate from the mid between start_key and end_key";
    // query the region
    dingodb::pb::coordinator::QueryRegionRequest query_request;
    dingodb::pb::coordinator::QueryRegionResponse query_response;
    query_request.set_region_id(FLAGS_split_from_id);
    cntl.Reset();
    stub.QueryRegion(&cntl, &query_request, &query_response, nullptr);
    if (cntl.Failed()) {
      DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
      // bthread_usleep(FLAGS_timeout_ms * 1000L);
      return;
    }

    if (query_response.region().range().start_key().empty()) {
      DINGO_LOG(ERROR) << "split from region " << FLAGS_split_from_id << " has no start_key";
      return;
    }

    if (query_response.region().range().end_key().empty()) {
      DINGO_LOG(ERROR) << "split from region " << FLAGS_split_from_id << " has no end_key";
      return;
    }

    std::string mid_key = query_response.region().range().start_key();
    mid_key.push_back(0x80);

    request.mutable_split_request()->set_split_watershed_key(mid_key);
  }

  DINGO_LOG(INFO) << "split from region " << FLAGS_split_from_id << " to region " << FLAGS_split_to_id
                  << " with watershed key [" << FLAGS_split_key << "] will be sent";

  cntl.Reset();
  stub.SplitRegion(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << response.DebugString();
  }
}

void SendMergeRegion(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::MergeRegionRequest request;
  dingodb::pb::coordinator::MergeRegionResponse response;

  if (FLAGS_merge_to_id > 0) {
    request.mutable_merge_request()->set_merge_to_region_id(FLAGS_merge_to_id);
  } else {
    DINGO_LOG(ERROR) << "merge_to_id is empty";
    return;
  }

  if (FLAGS_merge_from_id > 0) {
    request.mutable_merge_request()->set_merge_from_region_id(FLAGS_merge_from_id);
  } else {
    DINGO_LOG(ERROR) << "merge_from_id is empty";
    return;
  }

  stub.MergeRegion(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << response.DebugString();
  }
}

void SendAddPeerRegion(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  if (FLAGS_id.empty()) {
    DINGO_LOG(ERROR) << "id is empty (this is store_id)";
    return;
  }

  uint64_t store_id = std::stoull(FLAGS_id);

  // get StoreMap
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(0);
  stub.GetStoreMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
    return;
  }

  dingodb::pb::common::Peer new_peer;
  for (const auto& store : response.storemap().stores()) {
    if (store.id() == store_id) {
      new_peer.set_store_id(store.id());
      new_peer.set_role(dingodb::pb::common::PeerRole::VOTER);
      new_peer.mutable_server_location()->CopyFrom(store.server_location());
      new_peer.mutable_raft_location()->CopyFrom(store.raft_location());
    }
  }

  if (new_peer.store_id() == 0) {
    DINGO_LOG(ERROR) << "store_id not found";
    return;
  }

  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(FLAGS_region_id);
  cntl.Reset();
  stub.QueryRegion(&cntl, &query_request, &query_response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
    return;
  }

  if (query_response.region().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return;
  }

  // validate peer not exists in region peers
  for (const auto& peer : query_response.region().peers()) {
    if (peer.store_id() == store_id) {
      DINGO_LOG(ERROR) << "peer already exists";
      return;
    }
  }

  // generate change peer
  dingodb::pb::coordinator::ChangePeerRegionRequest change_peer_request;
  dingodb::pb::coordinator::ChangePeerRegionResponse change_peer_response;

  auto* new_definition = change_peer_request.mutable_change_peer_request()->mutable_region_definition();
  new_definition->CopyFrom(query_response.region());
  auto* new_peer_to_add = new_definition->add_peers();
  new_peer_to_add->CopyFrom(new_peer);

  DINGO_LOG(INFO) << "new_definition: " << new_definition->DebugString();

  cntl.Reset();
  stub.ChangePeerRegion(&cntl, &change_peer_request, &change_peer_response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << change_peer_response.DebugString();
  }
}

void SendRemovePeerRegion(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  if (FLAGS_id.empty()) {
    DINGO_LOG(ERROR) << "id is empty (this is store_id)";
    return;
  }

  uint64_t store_id = std::stoull(FLAGS_id);

  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(FLAGS_region_id);
  stub.QueryRegion(&cntl, &query_request, &query_response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
    return;
  }

  if (query_response.region().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return;
  }

  // validate peer not exists in region peers
  bool found = false;
  for (const auto& peer : query_response.region().peers()) {
    if (peer.store_id() == store_id) {
      found = true;
      break;
    }
  }

  if (!found) {
    DINGO_LOG(ERROR) << "peer not found";
    return;
  }

  // generate change peer
  dingodb::pb::coordinator::ChangePeerRegionRequest change_peer_request;
  dingodb::pb::coordinator::ChangePeerRegionResponse change_peer_response;

  auto* new_definition = change_peer_request.mutable_change_peer_request()->mutable_region_definition();
  new_definition->CopyFrom(query_response.region());
  for (int i = 0; i < new_definition->peers_size(); i++) {
    if (new_definition->peers(i).store_id() == store_id) {
      new_definition->mutable_peers()->SwapElements(i, new_definition->peers_size() - 1);
      new_definition->mutable_peers()->RemoveLast();
      break;
    }
  }

  DINGO_LOG(INFO) << "new_definition: " << new_definition->DebugString();
  cntl.Reset();
  stub.ChangePeerRegion(&cntl, &change_peer_request, &change_peer_response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << change_peer_response.DebugString();
  }
}

// store operation
void SendGetStoreOperation(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetStoreOperationRequest request;
  dingodb::pb::coordinator::GetStoreOperationResponse response;

  if (!FLAGS_id.empty()) {
    request.set_store_id(std::stoull(FLAGS_id));
  }

  stub.GetStoreOperation(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  for (const auto& it : response.store_operations()) {
    DINGO_LOG(INFO) << "store_id=" << it.id() << " cmd_count=" << it.region_cmds_size();
  }

  for (const auto& it : response.store_operations()) {
    DINGO_LOG(INFO) << "store_id=" << it.id() << " store_operation=" << it.DebugString();
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
  }
}

void SendCleanStoreOperation(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::CleanStoreOperationRequest request;
  dingodb::pb::coordinator::CleanStoreOperationResponse response;

  if (!FLAGS_id.empty()) {
    request.set_store_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty(this is store_id)";
    return;
  }

  stub.CleanStoreOperation(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << response.DebugString();
  }
}

void SendAddStoreOperation(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::AddStoreOperationRequest request;
  dingodb::pb::coordinator::AddStoreOperationResponse response;

  if (!FLAGS_id.empty()) {
    request.mutable_store_operation()->set_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty (this is store_id)";
    return;
  }

  if (FLAGS_region_id != 0) {
    auto* region_cmd = request.mutable_store_operation()->add_region_cmds();
    region_cmd->set_region_id(FLAGS_region_id);
    region_cmd->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_NONE);
    region_cmd->set_create_timestamp(butil::gettimeofday_ms());
  } else {
    DINGO_LOG(ERROR) << "region_id is empty";
    return;
  }

  stub.AddStoreOperation(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << response.DebugString();
  }
}

void SendRemoveStoreOperation(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::RemoveStoreOperationRequest request;
  dingodb::pb::coordinator::RemoveStoreOperationResponse response;

  if (!FLAGS_id.empty()) {
    request.set_store_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty (this is store_id)";
    return;
  }

  if (FLAGS_region_cmd_id != 0) {
    request.set_region_cmd_id(FLAGS_region_cmd_id);
  } else {
    DINGO_LOG(ERROR) << "region_cmd_id is empty";
    return;
  }

  stub.RemoveStoreOperation(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << response.DebugString();
  }
}

void SendRaftAddPeer(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  if (!FLAGS_host.empty() && FLAGS_port != 0) {
    request.set_add_peer(FLAGS_host + ":" + std::to_string(FLAGS_port));
  } else {
    DINGO_LOG(ERROR) << "host or port is empty";
    return;
  }
  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::AddPeer);

  if (FLAGS_index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (FLAGS_index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    DINGO_LOG(ERROR) << "index is error";
    return;
  }

  stub.RaftControl(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << response.DebugString();
  }
}

void SendRaftRemovePeer(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  if (!FLAGS_host.empty() && FLAGS_port != 0) {
    request.set_remove_peer(FLAGS_host + ":" + std::to_string(FLAGS_port));
  } else {
    DINGO_LOG(ERROR) << "host or port is empty";
    return;
  }
  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::RemovePeer);

  if (FLAGS_index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (FLAGS_index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    DINGO_LOG(ERROR) << "index is error";
    return;
  }

  stub.RaftControl(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG_INFO << response.DebugString();
  }
}
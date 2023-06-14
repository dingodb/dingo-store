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
#include <memory>
#include <string>
#include <vector>

#include "butil/time.h"
#include "client/coordinator_client_function.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "gflags/gflags_declare.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"

DECLARE_bool(log_each_request);
DECLARE_int32(timeout_ms);
DECLARE_string(id);
DECLARE_string(host);
DECLARE_int32(port);
DECLARE_string(peer);
DECLARE_string(peers);
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
DECLARE_string(state);
DECLARE_bool(is_force);

// raft control
void SendRaftAddPeer() {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  if (!FLAGS_peer.empty()) {
    request.set_add_peer(FLAGS_peer);
  } else if (!FLAGS_host.empty() && FLAGS_port != 0) {
    request.set_add_peer(FLAGS_host + ":" + std::to_string(FLAGS_port));
  } else {
    DINGO_LOG(ERROR) << "peer, host or port is empty";
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

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (FLAGS_coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(FLAGS_coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

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

void SendRaftRemovePeer() {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  if (!FLAGS_peer.empty()) {
    request.set_remove_peer(FLAGS_peer);
  } else if (!FLAGS_host.empty() && FLAGS_port != 0) {
    request.set_remove_peer(FLAGS_host + ":" + std::to_string(FLAGS_port));
  } else {
    DINGO_LOG(ERROR) << "peer, host or port is empty";
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

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (FLAGS_coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(FLAGS_coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

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

void SendRaftTransferLeader() {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  if (!FLAGS_peer.empty()) {
    request.set_new_leader(FLAGS_peer);
  } else if (!FLAGS_host.empty() && FLAGS_port != 0) {
    request.set_new_leader(FLAGS_host + ":" + std::to_string(FLAGS_port));
  } else {
    DINGO_LOG(ERROR) << "peer, host or port is empty";
    return;
  }
  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::TransferLeader);

  if (FLAGS_index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (FLAGS_index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    DINGO_LOG(ERROR) << "index is error";
    return;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (FLAGS_coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(FLAGS_coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

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

void SendRaftSnapshot() {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::Snapshot);

  if (FLAGS_index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (FLAGS_index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    DINGO_LOG(ERROR) << "index is error";
    return;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (FLAGS_coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(FLAGS_coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

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

void SendRaftResetPeer() {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  if (!FLAGS_peers.empty()) {
    std::vector<std::string> tokens;

    std::stringstream ss(FLAGS_peers);
    std::string token;
    while (std::getline(ss, token, ',')) {
      tokens.push_back(token);
    }

    if (tokens.empty()) {
      DINGO_LOG(ERROR) << "peers is empty, for example: 127.0.0.1:22101,127.0.0.1:22102,127.0.0.1:22103";
      return;
    }

    for (const auto& it : tokens) {
      DINGO_LOG(INFO) << "peer: " << it;
      request.add_new_peers(it);
    }
  } else {
    DINGO_LOG(ERROR) << "peers is empty, for example: 127.0.0.1:22101,127.0.0.1:22102,127.0.0.1:22103";
    return;
  }
  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::ResetPeer);

  if (FLAGS_index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (FLAGS_index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    DINGO_LOG(ERROR) << "index is error";
    return;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (FLAGS_coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(FLAGS_coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

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

// node service
void SendGetNodeInfo() {
  dingodb::pb::node::GetNodeInfoRequest request;
  dingodb::pb::node::GetNodeInfoResponse response;

  std::string const key = "Hello";
  // const char* op = nullptr;
  request.set_cluster_id(0);

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (FLAGS_coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(FLAGS_coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::node::NodeService_Stub stub(&channel);

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

void SendGetLogLevel() {
  dingodb::pb::node::GetLogLevelRequest request;
  dingodb::pb::node::GetLogLevelResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (FLAGS_coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(FLAGS_coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::node::NodeService_Stub stub(&channel);

  stub.GetLogLevel(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  DINGO_LOG(INFO) << response.DebugString();
  DINGO_LOG(INFO) << ::dingodb::pb::node::LogLevel_descriptor()->FindValueByNumber(response.log_level())->name();
}

void SendChangeLogLevel() {
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
    DINGO_LOG(WARNING) << "level is not valid, please set --level=DEBUG|INFO|WARNING|ERROR|FATAL";
    return;
  }

  auto* log_detail = request.mutable_log_detail();
  log_detail->set_log_buf_secs(10);
  log_detail->set_max_log_size(100);
  log_detail->set_stop_logging_if_full_disk(false);

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);

  if (FLAGS_coordinator_addr.empty()) {
    DINGO_LOG(ERROR) << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(FLAGS_coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::node::NodeService_Stub stub(&channel);

  stub.ChangeLogLevel(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }

  DINGO_LOG(INFO) << request.DebugString();
  DINGO_LOG(INFO) << ::dingodb::pb::node::LogLevel_descriptor()->FindValueByNumber(request.log_level())->name();
  DINGO_LOG(INFO) << response.DebugString();
}

// coordinator service
void SendHello(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  std::string const key = "Hello";
  // const char* op = nullptr;
  request.set_hello(0);
  request.set_get_memory_info(true);

  auto status = coordinator_interaction->SendRequest("Hello", request, response);
  DINGO_LOG(INFO) << "SendRequest status: " << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetStoreMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(1);

  auto status = coordinator_interaction->SendRequest("GetStoreMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  DINGO_LOG(INFO) << response.DebugString();

  // print all store's id, state, in_state, create_timestamp, last_seen_timestamp
  int32_t store_count_available = 0;
  for (auto const& store : response.storemap().stores()) {
    if (store.state() == dingodb::pb::common::StoreState::STORE_NORMAL) {
      store_count_available++;
    }
    DINGO_LOG(INFO) << "store_id=" << store.id() << " state=" << dingodb::pb::common::StoreState_Name(store.state())
                    << " in_state=" << dingodb::pb::common::StoreInState_Name(store.in_state())
                    << " create_timestamp=" << store.create_timestamp()
                    << " last_seen_timestamp=" << store.last_seen_timestamp();
  }

  // don't modify this log, it is used by sdk
  if (store_count_available > 0) {
    DINGO_LOG(INFO) << "DINGODB_HAVE_STORE_AVAILABLE, store_count=" << store_count_available;
  }
}

void SendGetExecutorMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetExecutorMapRequest request;
  dingodb::pb::coordinator::GetExecutorMapResponse response;

  request.set_epoch(1);

  auto status = coordinator_interaction->SendRequest("GetExecutorMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetCoordinatorMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetCoordinatorMapRequest request;
  dingodb::pb::coordinator::GetCoordinatorMapResponse response;

  request.set_cluster_id(0);

  auto status = coordinator_interaction->SendRequest("GetCoordinatorMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.set_epoch(1);

  auto status = coordinator_interaction->SendRequest("GetRegionMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  // for (const auto& region : response.regionmap().regions()) {
  //   DINGO_LOG(INFO) << region.DebugString();
  // }

  uint64_t normal_region_count = 0;
  uint64_t online_region_count = 0;
  for (const auto& region : response.regionmap().regions()) {
    DINGO_LOG(INFO) << "Region id=" << region.id() << " name=" << region.definition().name()
                    << " state=" << dingodb::pb::common::RegionState_Name(region.state())
                    << " heartbeat_state=" << dingodb::pb::common::RegionHeartbeatState_Name(region.heartbeat_state())
                    << " replica_state=" << dingodb::pb::common::ReplicaStatus_Name(region.replica_status())
                    << " raft_status=" << dingodb::pb::common::RegionRaftStatus_Name(region.raft_status())
                    << " leader_store_id=" << region.leader_store_id();

    if (region.state() == dingodb::pb::common::RegionState::REGION_NORMAL) {
      normal_region_count++;
    }

    if (region.heartbeat_state() == dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
      online_region_count++;
    }
  }

  DINGO_LOG(INFO) << "region_count=" << response.regionmap().regions_size()
                  << ", normal_region_count=" << normal_region_count << ", online_region_count=" << online_region_count;
}

void SendGetDeletedRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetDeletedRegionMapRequest request;
  dingodb::pb::coordinator::GetDeletedRegionMapResponse response;

  request.set_epoch(1);

  auto status = coordinator_interaction->SendRequest("GetDeletedRegionMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto& region : response.regionmap().regions()) {
    DINGO_LOG(INFO) << "Region id=" << region.id() << " name=" << region.definition().name()
                    << " state=" << dingodb::pb::common::RegionState_Name(region.state())
                    << " deleted_time_stamp=" << region.deleted_timestamp()
                    << " deleted_time_minutes=" << (butil::gettimeofday_ms() - region.deleted_timestamp()) / 1000 / 60
                    << " deleted_time_hours=" << (butil::gettimeofday_ms() - region.deleted_timestamp()) / 1000 / 3600;
  }

  DINGO_LOG(INFO) << "region_count=" << response.regionmap().regions_size();
}

void SendAddDeletedRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::AddDeletedRegionMapRequest request;
  dingodb::pb::coordinator::AddDeletedRegionMapResponse response;

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  request.set_region_id(std::stol(FLAGS_id));

  if (FLAGS_is_force) {
    request.set_force(FLAGS_is_force);
  }

  auto status = coordinator_interaction->SendRequest("AddDeletedRegionMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "response=" << response.DebugString();
}

void SendCleanDeletedRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::CleanDeletedRegionMapRequest request;
  dingodb::pb::coordinator::CleanDeletedRegionMapResponse response;

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  request.set_region_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("CleanDeletedRegionMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "response=" << response.DebugString();
}

void SendGetRegionCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetRegionCountRequest request;
  dingodb::pb::coordinator::GetRegionCountResponse response;

  request.set_epoch(1);

  auto status = coordinator_interaction->SendRequest("GetRegionCount", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  DINGO_LOG(INFO) << " region_count=" << response.region_count();
}

void SendCreateStore(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::CreateStoreRequest request;
  dingodb::pb::coordinator::CreateStoreResponse response;

  request.set_cluster_id(1);

  auto status = coordinator_interaction->SendRequest("CreateStore", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendDeleteStore(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("DeleteStore", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendUpdateStore(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::UpdateStoreRequest request;
  dingodb::pb::coordinator::UpdateStoreResponse response;

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

  if (!FLAGS_state.empty()) {
    if (FLAGS_state == "IN") {
      request.set_store_in_state(::dingodb::pb::common::StoreInState::STORE_IN);
    } else if (FLAGS_state == "OUT") {
      request.set_store_in_state(::dingodb::pb::common::StoreInState::STORE_OUT);
    } else {
      DINGO_LOG(WARNING) << "state is invalid, must be  IN or OUT";
      return;
    };
  }

  auto status = coordinator_interaction->SendRequest("UpdateStore", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendCreateExecutor(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("CreateExecutor", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendDeleteExecutor(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("DeleteExecutor", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendCreateExecutorUser(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("CreateExecutorUser", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendUpdateExecutorUser(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("UpdateExecutorUser", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendDeleteExecutorUser(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("DeleteExecutorUser", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetExecutorUserMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetExecutorUserMapRequest request;
  dingodb::pb::coordinator::GetExecutorUserMapResponse response;

  request.set_cluster_id(1);

  auto status = coordinator_interaction->SendRequest("GetExecutorUserMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendExecutorHeartbeat(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("ExecutorHeartbeat", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendStoreHearbeat(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, uint64_t store_id) {
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

  auto status = coordinator_interaction->SendRequest("StoreHeartbeat", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetStoreMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetStoreMetricsRequest request;
  dingodb::pb::coordinator::GetStoreMetricsResponse response;

  if (!FLAGS_id.empty()) {
    request.set_store_id(std::stoull(FLAGS_id));
  }

  auto status = coordinator_interaction->SendRequest("GetStoreMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  for (const auto& it : response.store_metrics()) {
    DINGO_LOG(INFO) << it.DebugString();
  }

  for (const auto& it : response.store_metrics()) {
    std::string region_ids;
    for (const auto& it2 : it.region_metrics_map()) {
      region_ids += fmt::format("{},", it2.first);
    }
    DINGO_LOG(INFO) << fmt::format("store_id={},region_count={},region_ids={}", it.id(), it.region_metrics_map_size(),
                                   region_ids);
  }
}

void SendDeleteStoreMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::DeleteStoreMetricsRequest request;
  dingodb::pb::coordinator::DeleteStoreMetricsResponse response;

  if (!FLAGS_id.empty()) {
    request.set_store_id(std::stoull(FLAGS_id));
  }

  auto status = coordinator_interaction->SendRequest("DeleteStoreMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

// region
void SendQueryRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  if (!FLAGS_id.empty()) {
    request.set_region_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty";
    return;
  }

  auto status = coordinator_interaction->SendRequest("QueryRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  auto region = response.region();
  DINGO_LOG(INFO) << "Region id=" << region.id() << " name=" << region.definition().name()
                  << " state=" << dingodb::pb::common::RegionState_Name(region.state())
                  << " leader_store_id=" << region.leader_store_id()
                  << " replica_state=" << dingodb::pb::common::ReplicaStatus_Name(region.replica_status())
                  << " raft_status=" << dingodb::pb::common::RegionRaftStatus_Name(region.raft_status());
}

void SendCreateRegionForSplit(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  if (FLAGS_id.empty()) {
    DINGO_LOG(ERROR) << "id is empty (this is the region id to split)";
    return;
  }

  uint64_t region_id_split = std::stoull(FLAGS_id);
  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(region_id_split);

  auto status = coordinator_interaction->SendRequest("QueryRegion", query_request, query_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << query_response.DebugString();

  if (query_response.region().definition().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return;
  }

  dingodb::pb::common::RegionDefinition new_region_definition;
  new_region_definition.CopyFrom(query_response.region().definition());
  new_region_definition.mutable_range()->set_start_key(query_response.region().definition().range().end_key());
  new_region_definition.mutable_range()->set_end_key(query_response.region().definition().range().start_key());
  new_region_definition.set_name(query_response.region().definition().name() + "_split");
  new_region_definition.set_id(0);

  std::vector<uint64_t> new_region_store_ids;
  for (const auto& it : query_response.region().definition().peers()) {
    new_region_store_ids.push_back(it.store_id());
  }

  dingodb::pb::coordinator::CreateRegionRequest request;
  dingodb::pb::coordinator::CreateRegionResponse response;

  request.set_region_name(new_region_definition.name());
  request.set_replica_num(new_region_definition.peers_size());
  request.mutable_range()->CopyFrom(new_region_definition.range());
  request.set_schema_id(query_response.region().definition().schema_id());
  request.set_table_id(query_response.region().definition().table_id());
  for (auto it : new_region_store_ids) {
    request.add_store_ids(it);
  }
  request.set_split_from_region_id(region_id_split);  // set split from region id

  DINGO_LOG(INFO) << "Create region request: " << request.DebugString();

  auto status2 = coordinator_interaction->SendRequest("CreateRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status2;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendDropRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::DropRegionRequest request;
  dingodb::pb::coordinator::DropRegionResponse response;

  if (!FLAGS_id.empty()) {
    request.set_region_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty";
    return;
  }

  auto status = coordinator_interaction->SendRequest("DropRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendDropRegionPermanently(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::DropRegionPermanentlyRequest request;
  dingodb::pb::coordinator::DropRegionPermanentlyResponse response;

  if (!FLAGS_id.empty()) {
    request.set_region_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty";
    return;
  }

  auto status = coordinator_interaction->SendRequest("DropRegionPermanently", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendSplitRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::SplitRegionRequest request;
  dingodb::pb::coordinator::SplitRegionResponse response;

  request.mutable_split_request()->set_split_to_region_id(FLAGS_split_to_id);

  if (FLAGS_split_from_id > 0) {
    request.mutable_split_request()->set_split_from_region_id(FLAGS_split_from_id);
  } else {
    DINGO_LOG(ERROR) << "split_from_id is empty";
    return;
  }

  if (!FLAGS_split_key.empty()) {
    std::string split_key = dingodb::Helper::HexToString(FLAGS_split_key);
    request.mutable_split_request()->set_split_watershed_key(FLAGS_split_key);
  } else {
    DINGO_LOG(ERROR) << "split_key is empty, will auto generate from the mid between start_key and end_key";
    // query the region
    dingodb::pb::coordinator::QueryRegionRequest query_request;
    dingodb::pb::coordinator::QueryRegionResponse query_response;
    query_request.set_region_id(FLAGS_split_from_id);

    auto status = coordinator_interaction->SendRequest("QueryRegion", query_request, query_response);
    DINGO_LOG(INFO) << "SendRequest status=" << status;
    DINGO_LOG(INFO) << query_response.DebugString();

    if (query_response.region().definition().range().start_key().empty()) {
      DINGO_LOG(ERROR) << "split from region " << FLAGS_split_from_id << " has no start_key";
      return;
    }

    if (query_response.region().definition().range().end_key().empty()) {
      DINGO_LOG(ERROR) << "split from region " << FLAGS_split_from_id << " has no end_key";
      return;
    }

    // calc the mid value between start_key and end_key
    const auto& start_key = query_response.region().definition().range().start_key();
    const auto& end_key = query_response.region().definition().range().end_key();

    auto real_mid = dingodb::Helper::CalculateMiddleKey(start_key, end_key);

    request.mutable_split_request()->set_split_watershed_key(real_mid);

    if (query_response.region().definition().range().start_key().compare(real_mid) >= 0 ||
        query_response.region().definition().range().end_key().compare(real_mid) <= 0) {
      DINGO_LOG(ERROR) << "SplitRegion split_watershed_key is illegal, split_watershed_key = "
                       << dingodb::Helper::StringToHex(real_mid)
                       << ", query_response.region()_id = " << query_response.region().id() << " start_key="
                       << dingodb::Helper::StringToHex(query_response.region().definition().range().start_key())
                       << ", end_key="
                       << dingodb::Helper::StringToHex(query_response.region().definition().range().end_key());
      return;
    }
  }

  DINGO_LOG(INFO) << "split from region " << FLAGS_split_from_id << " to region " << FLAGS_split_to_id
                  << " with watershed key ["
                  << dingodb::Helper::StringToHex(request.split_request().split_watershed_key()) << "] will be sent";

  auto status = coordinator_interaction->SendRequest("SplitRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "split from region " << FLAGS_split_from_id << " to region " << FLAGS_split_to_id
                    << " with watershed key ["
                    << dingodb::Helper::StringToHex(request.split_request().split_watershed_key())
                    << "] failed, error: "
                    << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name()
                    << " " << response.error().errmsg();
  } else {
    DINGO_LOG(INFO) << "split from region " << FLAGS_split_from_id << " to region " << FLAGS_split_to_id
                    << " with watershed key ["
                    << dingodb::Helper::StringToHex(request.split_request().split_watershed_key()) << "] success";
  }
}

void SendMergeRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("MergeRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendAddPeerRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  if (FLAGS_id.empty()) {
    DINGO_LOG(ERROR) << "id is empty (this is store_id)";
    return;
  }

  uint64_t store_id = std::stoull(FLAGS_id);

  // get StoreMap
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(0);

  auto status = coordinator_interaction->SendRequest("GetStoreMap", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

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

  auto status2 = coordinator_interaction->SendRequest("QueryRegion", query_request, query_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status2;

  if (query_response.region().definition().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return;
  }

  // validate peer not exists in region peers
  for (const auto& peer : query_response.region().definition().peers()) {
    if (peer.store_id() == store_id) {
      DINGO_LOG(ERROR) << "peer already exists";
      return;
    }
  }

  // generate change peer
  dingodb::pb::coordinator::ChangePeerRegionRequest change_peer_request;
  dingodb::pb::coordinator::ChangePeerRegionResponse change_peer_response;

  auto* new_definition = change_peer_request.mutable_change_peer_request()->mutable_region_definition();
  new_definition->CopyFrom(query_response.region().definition());
  auto* new_peer_to_add = new_definition->add_peers();
  new_peer_to_add->CopyFrom(new_peer);

  DINGO_LOG(INFO) << "new_definition: " << new_definition->DebugString();

  auto status3 = coordinator_interaction->SendRequest("ChangePeerRegion", change_peer_request, change_peer_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status3;
  DINGO_LOG(INFO) << change_peer_response.DebugString();

  if (change_peer_response.has_error() && change_peer_response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "change peer error: " << change_peer_response.error().DebugString();
  } else {
    DINGO_LOG(INFO) << "change peer success";
  }
}

void SendRemovePeerRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  if (FLAGS_id.empty()) {
    DINGO_LOG(ERROR) << "id is empty (this is store_id)";
    return;
  }

  uint64_t store_id = std::stoull(FLAGS_id);

  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(FLAGS_region_id);

  auto status = coordinator_interaction->SendRequest("QueryRegion", query_request, query_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  if (query_response.region().definition().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return;
  }

  // validate peer not exists in region peers
  bool found = false;
  for (const auto& peer : query_response.region().definition().peers()) {
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
  new_definition->CopyFrom(query_response.region().definition());
  for (int i = 0; i < new_definition->peers_size(); i++) {
    if (new_definition->peers(i).store_id() == store_id) {
      new_definition->mutable_peers()->SwapElements(i, new_definition->peers_size() - 1);
      new_definition->mutable_peers()->RemoveLast();
      break;
    }
  }

  DINGO_LOG(INFO) << "new_definition: " << new_definition->DebugString();

  auto status2 = coordinator_interaction->SendRequest("ChangePeerRegion", change_peer_request, change_peer_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status2;
  DINGO_LOG(INFO) << change_peer_response.DebugString();

  if (change_peer_response.has_error() && change_peer_response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "change peer error: " << change_peer_response.error().DebugString();
  } else {
    DINGO_LOG(INFO) << "change peer success";
  }
}

void SendTransferLeaderRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  if (FLAGS_region_id == 0) {
    DINGO_LOG(ERROR) << "region_id is empty";
    return;
  }

  if (FLAGS_store_id == 0) {
    DINGO_LOG(ERROR) << "store_id is empty";
    return;
  }

  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(FLAGS_region_id);

  auto status = coordinator_interaction->SendRequest("QueryRegion", query_request, query_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  if (query_response.region().definition().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return;
  }

  // validate peer not exists in region peers
  bool found = false;
  for (const auto& peer : query_response.region().definition().peers()) {
    if (peer.store_id() == FLAGS_store_id) {
      found = true;
      break;
    }
  }

  if (!found) {
    DINGO_LOG(ERROR) << "store_id not found";
    return;
  }

  // generate tranfer leader
  dingodb::pb::coordinator::TransferLeaderRegionRequest transfer_leader_request;
  dingodb::pb::coordinator::TransferLeaderRegionResponse transfer_leader_response;

  transfer_leader_request.set_region_id(FLAGS_region_id);
  transfer_leader_request.set_leader_store_id(FLAGS_store_id);

  DINGO_LOG(INFO) << "transfer leader: region_id=" << FLAGS_region_id << ", store_id=" << FLAGS_store_id;

  auto status2 =
      coordinator_interaction->SendRequest("TransferLeaderRegion", transfer_leader_request, transfer_leader_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status2;
  DINGO_LOG(INFO) << transfer_leader_response.DebugString();

  if (transfer_leader_response.has_error() &&
      transfer_leader_response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "transfer leader error: " << transfer_leader_response.error().DebugString();
  } else {
    DINGO_LOG(INFO) << "transfer leader success";
  }
}

void SendGetOrphanRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetOrphanRegionRequest request;
  dingodb::pb::coordinator::GetOrphanRegionResponse response;

  if (!FLAGS_id.empty()) {
    request.set_store_id(std::stoull(FLAGS_id));
  }

  auto status = coordinator_interaction->SendRequest("GetOrphanRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  DINGO_LOG(INFO) << "orphan_regions_size=" << response.orphan_regions_size();

  for (const auto& it : response.orphan_regions()) {
    DINGO_LOG(INFO) << "store_id=" << it.first << " region_id=" << it.second.id();
    DINGO_LOG(INFO) << it.second.DebugString();
  }

  for (const auto& it : response.orphan_regions()) {
    DINGO_LOG(INFO) << "store_id=" << it.first << " region_id=" << it.second.id()
                    << " region_name=" << it.second.region_definition().name();
  }
}

// store operation
void SendGetStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetStoreOperationRequest request;
  dingodb::pb::coordinator::GetStoreOperationResponse response;

  if (!FLAGS_id.empty()) {
    request.set_store_id(std::stoull(FLAGS_id));
  }

  auto status = coordinator_interaction->SendRequest("GetStoreOperation", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto& it : response.store_operations()) {
    DINGO_LOG(INFO) << "store_id=" << it.id() << " store_operation=" << it.DebugString();
  }

  for (const auto& it : response.store_operations()) {
    DINGO_LOG(INFO) << "store_id=" << it.id() << " cmd_count=" << it.region_cmds_size();
  }
}

void SendCleanStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::CleanStoreOperationRequest request;
  dingodb::pb::coordinator::CleanStoreOperationResponse response;

  if (!FLAGS_id.empty()) {
    request.set_store_id(std::stoull(FLAGS_id));
  } else {
    DINGO_LOG(ERROR) << "id is empty(this is store_id)";
    return;
  }

  auto status = coordinator_interaction->SendRequest("CleanStoreOperation", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendAddStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("AddStoreOperation", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendRemoveStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
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

  auto status = coordinator_interaction->SendRequest("RemoveStoreOperation", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendGetTaskList(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetTaskListRequest request;
  dingodb::pb::coordinator::GetTaskListResponse response;

  auto status = coordinator_interaction->SendRequest("GetTaskList", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendCleanTaskList(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::CleanTaskListRequest request;
  dingodb::pb::coordinator::CleanTaskListResponse response;

  if (FLAGS_id.empty()) {
    DINGO_LOG(ERROR) << "id is empty, if you want to clean all task_list, set --id=0";
    return;
  }
  request.set_task_list_id(std::stoull(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("CleanTaskList", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}
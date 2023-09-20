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
#include "client/client_helper.h"
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
DECLARE_uint64(timeout_ms);
DECLARE_string(id);
DECLARE_string(host);
DECLARE_uint32(port);
DECLARE_string(peer);
DECLARE_string(peers);
DECLARE_string(level);
DECLARE_string(name);
DECLARE_string(user);
DECLARE_string(keyring);
DECLARE_string(new_keyring);
DECLARE_string(coordinator_addr);
DECLARE_uint64(split_from_id);
DECLARE_uint64(split_to_id);
DECLARE_string(split_key);
DECLARE_uint64(merge_from_id);
DECLARE_uint64(merge_to_id);
DECLARE_uint64(peer_add_store_id);
DECLARE_uint64(peer_del_store_id);
DECLARE_uint64(store_id);
DECLARE_uint64(region_id);
DECLARE_uint64(region_cmd_id);
DECLARE_string(store_ids);
DECLARE_uint64(index);
DECLARE_string(state);
DECLARE_bool(is_force);
DECLARE_uint64(count);
DECLARE_bool(store_create_region);
DECLARE_uint64(start_region_cmd_id);
DECLARE_uint64(end_region_cmd_id);
DECLARE_uint64(vector_id);
DECLARE_string(key);
DECLARE_bool(key_is_hex);
DECLARE_string(range_end);
DECLARE_int32(limit);
DECLARE_uint64(safe_point);

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
  int32_t index_count_available = 0;
  for (auto const& store : response.storemap().stores()) {
    if (store.state() == dingodb::pb::common::StoreState::STORE_NORMAL &&
        store.store_type() == dingodb::pb::common::StoreType::NODE_TYPE_STORE) {
      store_count_available++;
    }
    if (store.state() == dingodb::pb::common::StoreState::STORE_NORMAL &&
        store.store_type() == dingodb::pb::common::StoreType::NODE_TYPE_INDEX) {
      index_count_available++;
    }
    std::cout << "store_id=" << store.id() << " type=" << dingodb::pb::common::StoreType_Name(store.store_type())
              << " addr={" << store.server_location().ShortDebugString()
              << "} state=" << dingodb::pb::common::StoreState_Name(store.state())
              << " in_state=" << dingodb::pb::common::StoreInState_Name(store.in_state())
              << " create_timestamp=" << store.create_timestamp()
              << " last_seen_timestamp=" << store.last_seen_timestamp() << "\n";
  }

  // don't modify this log, it is used by sdk
  if (store_count_available > 0) {
    std::cout << "DINGODB_HAVE_STORE_AVAILABLE, store_count=" << store_count_available << "\n";
  }
  if (index_count_available > 0) {
    std::cout << "DINGODB_HAVE_INDEX_AVAILABLE, index_count=" << index_count_available << "\n";
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

  std::cout << "\n";
  int64_t normal_region_count = 0;
  int64_t online_region_count = 0;
  for (const auto& region : response.regionmap().regions()) {
    std::cout << "id=" << region.id() << " name=" << region.definition().name()
              << " epoch=" << region.definition().epoch().conf_version() << "," << region.definition().epoch().version()
              << " state=" << dingodb::pb::common::RegionState_Name(region.state()) << ","
              << dingodb::pb::common::RegionHeartbeatState_Name(region.status().heartbeat_status()) << ","
              << dingodb::pb::common::ReplicaStatus_Name(region.status().replica_status()) << ","
              << dingodb::pb::common::RegionRaftStatus_Name(region.status().raft_status())
              << " leader=" << region.leader_store_id() << " create=" << region.create_timestamp()
              << " update=" << region.status().last_update_timestamp() << " range=[0x"
              << dingodb::Helper::StringToHex(region.definition().range().start_key()) << ",0x"
              << dingodb::Helper::StringToHex(region.definition().range().end_key()) << "]\n";

    if (region.metrics().has_vector_index_status()) {
      std::cout << "vector_id=" << region.id()
                << " vector_status=" << region.metrics().vector_index_status().ShortDebugString() << "\n";
    }

    if (region.state() == dingodb::pb::common::RegionState::REGION_NORMAL) {
      normal_region_count++;
    }

    if (region.status().heartbeat_status() == dingodb::pb::common::RegionHeartbeatState::REGION_ONLINE) {
      online_region_count++;
    }
  }

  std::cout << '\n'
            << "region_count=[" << response.regionmap().regions_size() << "], normal_region_count=["
            << normal_region_count << "], online_region_count=[" << online_region_count << "]\n\n";
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

  DINGO_LOG(INFO) << "Deleted region_count=" << response.regionmap().regions_size();
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

void SendStoreHearbeat(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, int64_t store_id) {
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
    request.set_store_id(std::stoll(FLAGS_id));
  }

  if (FLAGS_store_id > 0) {
    request.set_store_id(FLAGS_store_id);
  }

  if (FLAGS_region_id > 0) {
    request.set_region_id(FLAGS_region_id);
  }

  auto status = coordinator_interaction->SendRequest("GetStoreMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  for (const auto& it : response.store_metrics()) {
    DINGO_LOG(INFO) << it.store_own_metrics().DebugString();
    for (const auto& it2 : it.region_metrics_map()) {
      DINGO_LOG(INFO) << it2.second.DebugString();
    }
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
    request.set_store_id(std::stoll(FLAGS_id));
  }

  auto status = coordinator_interaction->SendRequest("DeleteStoreMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetRegionMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetRegionMetricsRequest request;
  dingodb::pb::coordinator::GetRegionMetricsResponse response;

  if (!FLAGS_id.empty()) {
    request.set_region_id(std::stoll(FLAGS_id));
  }

  if (FLAGS_region_id > 0) {
    request.set_region_id(FLAGS_region_id);
  }

  auto status = coordinator_interaction->SendRequest("GetRegionMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  for (const auto& it : response.region_metrics()) {
    DINGO_LOG(INFO) << it.DebugString();
  }
  DINGO_LOG(INFO) << "region_count=" << response.region_metrics_size();
}

void SendDeleteRegionMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::DeleteRegionMetricsRequest request;
  dingodb::pb::coordinator::DeleteRegionMetricsResponse response;

  if (!FLAGS_id.empty()) {
    request.set_region_id(std::stoll(FLAGS_id));
  }

  if (FLAGS_region_id > 0) {
    request.set_region_id(FLAGS_region_id);
  }

  if (request.region_id() == 0) {
    DINGO_LOG(WARNING) << "region_id is empty";
    return;
  }

  auto status = coordinator_interaction->SendRequest("DeleteRegionMetrics", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

// region
void SendCreateRegionId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::CreateRegionIdRequest request;
  dingodb::pb::coordinator::CreateRegionIdResponse response;

  if (FLAGS_count <= 0) {
    DINGO_LOG(WARNING) << "count must > 0 and < 2048";
    return;
  } else if (FLAGS_count > 2048) {
    DINGO_LOG(WARNING) << "count must > 0 and < 2048";
    return;
  }

  request.set_count(FLAGS_count);

  auto status = coordinator_interaction->SendRequest("CreateRegionId", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "response=" << response.DebugString();
}

void SendQueryRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  if (!FLAGS_id.empty()) {
    request.set_region_id(std::stoll(FLAGS_id));
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
                  << " replica_state=" << dingodb::pb::common::ReplicaStatus_Name(region.status().replica_status())
                  << " raft_status=" << dingodb::pb::common::RegionRaftStatus_Name(region.status().raft_status());
  DINGO_LOG(INFO) << "start_key=[" << dingodb::Helper::StringToHex(region.definition().range().start_key()) << "]";
  DINGO_LOG(INFO) << "  end_key=[" << dingodb::Helper::StringToHex(region.definition().range().end_key()) << "]";
}

void SendCreateRegionForSplit(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  if (FLAGS_id.empty()) {
    DINGO_LOG(ERROR) << "id is empty (this is the region id to split)";
    return;
  }

  int64_t region_id_split = std::stoll(FLAGS_id);
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
  new_region_definition = query_response.region().definition();
  new_region_definition.mutable_range()->set_start_key(query_response.region().definition().range().end_key());
  new_region_definition.mutable_range()->set_end_key(query_response.region().definition().range().start_key());
  new_region_definition.set_name(query_response.region().definition().name() + "_split");
  new_region_definition.set_id(0);

  std::vector<int64_t> new_region_store_ids;
  for (const auto& it : query_response.region().definition().peers()) {
    new_region_store_ids.push_back(it.store_id());
  }

  dingodb::pb::coordinator::CreateRegionRequest request;
  dingodb::pb::coordinator::CreateRegionResponse response;

  request.set_region_name(new_region_definition.name());
  request.set_replica_num(new_region_definition.peers_size());
  *(request.mutable_range()) = new_region_definition.range();
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
    request.set_region_id(std::stoll(FLAGS_id));
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
    request.set_region_id(std::stoll(FLAGS_id));
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
    request.mutable_split_request()->set_split_watershed_key(split_key);
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

    std::string real_mid;
    if (query_response.region().definition().index_parameter().index_type() == dingodb::pb::common::INDEX_TYPE_VECTOR) {
      if (FLAGS_vector_id > 0) {
        int64_t partition_id = dingodb::VectorCodec::DecodePartitionId(start_key);
        dingodb::VectorCodec::EncodeVectorKey(partition_id, FLAGS_vector_id, real_mid);
      } else {
        real_mid = client::Helper::CalculateVectorMiddleKey(start_key, end_key);
      }
    } else {
      real_mid = dingodb::Helper::CalculateMiddleKey(start_key, end_key);
    }

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

  if (FLAGS_store_create_region) {
    request.mutable_split_request()->set_store_create_region(FLAGS_store_create_region);
  }

  DINGO_LOG(INFO) << "split from region_id=" << FLAGS_split_from_id << ", to region_id=" << FLAGS_split_to_id
                  << ", store_create_region=" << FLAGS_store_create_region << ", with watershed key ["
                  << dingodb::Helper::StringToHex(request.split_request().split_watershed_key()) << "] will be sent";

  auto status = coordinator_interaction->SendRequest("SplitRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "split from region " << FLAGS_split_from_id << " to region " << FLAGS_split_to_id
                    << " store_create_region=" << FLAGS_store_create_region << " with watershed key ["
                    << dingodb::Helper::StringToHex(request.split_request().split_watershed_key())
                    << "] failed, error: "
                    << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name()
                    << " " << response.error().errmsg();
  } else {
    DINGO_LOG(INFO) << "split from region " << FLAGS_split_from_id << " to region " << FLAGS_split_to_id
                    << " store_create_region=" << FLAGS_store_create_region << " with watershed key ["
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

  int64_t store_id = std::stoll(FLAGS_id);

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
      *(new_peer.mutable_server_location()) = store.server_location();
      *(new_peer.mutable_raft_location()) = store.raft_location();
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
  *new_definition = query_response.region().definition();
  auto* new_peer_to_add = new_definition->add_peers();
  *new_peer_to_add = new_peer;

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

  int64_t store_id = std::stoll(FLAGS_id);

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
  *new_definition = query_response.region().definition();
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
    request.set_store_id(std::stoll(FLAGS_id));
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
    request.set_store_id(std::stoll(FLAGS_id));
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
    request.set_store_id(std::stoll(FLAGS_id));
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
    request.mutable_store_operation()->set_id(std::stoll(FLAGS_id));
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
    request.set_store_id(std::stoll(FLAGS_id));
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
  request.set_task_list_id(std::stoll(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("CleanTaskList", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendGetRegionCmd(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetRegionCmdRequest request;
  dingodb::pb::coordinator::GetRegionCmdResponse response;

  if (FLAGS_store_id == 0) {
    DINGO_LOG(ERROR) << "store_id is empty";
    return;
  }

  if (FLAGS_start_region_cmd_id == 0) {
    DINGO_LOG(ERROR) << "start_region_cmd_id is empty";
    return;
  }

  if (FLAGS_end_region_cmd_id == 0) {
    DINGO_LOG(ERROR) << "end_region_cmd_id is empty";
    return;
  }

  request.set_store_id(FLAGS_store_id);
  request.set_start_region_cmd_id(FLAGS_start_region_cmd_id);
  request.set_end_region_cmd_id(FLAGS_end_region_cmd_id);

  auto status = coordinator_interaction->SendRequest("GetRegionCmd", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendScanRegions(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::ScanRegionsRequest request;
  dingodb::pb::coordinator::ScanRegionsResponse response;

  if (FLAGS_key.empty()) {
    DINGO_LOG(WARNING) << "key is empty, please input key";
    return;
  }

  if (FLAGS_key_is_hex) {
    request.set_key(dingodb::Helper::HexToString(FLAGS_key));
  } else {
    request.set_key(FLAGS_key);
  }

  if (FLAGS_key_is_hex) {
    request.set_range_end(dingodb::Helper::HexToString(FLAGS_range_end));
  } else {
    request.set_range_end(FLAGS_range_end);
  }

  request.set_limit(FLAGS_limit);

  DINGO_LOG(INFO) << "ScanRegions request: " << request.DebugString();

  auto status = coordinator_interaction->SendRequest("ScanRegions", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendGetGCSafePoint(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::GetGCSafePointRequest request;
  dingodb::pb::coordinator::GetGCSafePointResponse response;

  auto status = coordinator_interaction->SendRequest("GetGCSafePoint", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
  DINGO_LOG(INFO) << "gc_safe_point=" << response.safe_point();
}

void SendUpdateGCSafePoint(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;

  if (FLAGS_safe_point == 0) {
    DINGO_LOG(ERROR) << "gc_safe_point is empty";
    return;
  }

  request.set_safe_point(FLAGS_safe_point);

  auto status = coordinator_interaction->SendRequest("UpdateGCSafePoint", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

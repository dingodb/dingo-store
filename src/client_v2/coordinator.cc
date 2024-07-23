
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

#include "client_v2/coordinator.h"

#include <iostream>
#include <ostream>

#include "client_v2/helper.h"
#include "client_v2/pretty.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/version.h"
#include "document/codec.h"

namespace client_v2 {

void SetUpCoordinatorSubCommands(CLI::App &app) {
  // coordinator commands
  SetUpGetCoordinatorMap(app);
  SetUpGetStoreMap(app);
  SetUpGetRegionMap(app);
  SetUpGetGCSafePoint(app);
  SetUpGetTaskList(app);
  SetUpCleanTaskList(app);
  // region commands
  SetUpAddPeerRegion(app);
  SetUpRemovePeerRegion(app);
  SetUpSplitRegion(app);
  SetUpMergeRegion(app);
  SetUpQueryRegion(app);

  // executor
  SetUpCreateExecutor(app);
  SetUpExecutorHeartbeat(app);
  SetUpGetExecutorMap(app);
}

bool GetBrpcChannel(const std::string &location, brpc::Channel &channel) {
  braft::PeerId node;
  if (node.parse(location) != 0) {
    DINGO_LOG(ERROR) << "Fail to parse node peer_id " << location;
    return false;
  }

  // rpc for leader access
  if (channel.Init(node.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << location;
    bthread_usleep(kTimeoutMs * 1000L);
    return false;
  }

  return true;
}

void SetUpGetRegionMap(CLI::App &app) {
  auto opt = std::make_shared<GetRegionMapCommandOptions>();
  auto *cmd = app.add_subcommand("GetRegionMap", "Get region map")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--tenant_id", opt->tenant_id, "Get all the region maps belonging to this tenant_id")->default_val(0);
  cmd->callback([opt]() { RunGetRegionMap(*opt); });
}

void RunGetRegionMap(GetRegionMapCommandOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    std::cout << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.set_epoch(1);
  request.set_tenant_id(opt.tenant_id);
  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetRegionMap", request, response);

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Get region map failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg() << std::endl;
    return;
  }

  std::cout << "\n";
  int64_t normal_region_count = 0;
  int64_t online_region_count = 0;
  for (const auto &region : response.regionmap().regions()) {
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

void SetUpLogLevel(CLI::App &app) {
  auto opt = std::make_shared<GetLogLevelCommandOptions>();
  auto *cmd = app.add_subcommand("GetLogLevel", "Get log level")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--timeout_ms", opt->timeout_ms, "Timeout for each request")
      ->default_val(60000)
      ->group("Coordinator Manager Commands");

  cmd->callback([opt]() { RunLogLevel(*opt); });
}
void RunLogLevel(GetLogLevelCommandOptions const &opt) {
  dingodb::pb::node::GetLogLevelRequest request;
  dingodb::pb::node::GetLogLevelResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(opt.timeout_ms);

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::node::NodeService_Stub stub(&channel);

  stub.GetLogLevel(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
  }
  std::cout << "response:" << response.DebugString();
  DINGO_LOG(INFO) << response.DebugString();
  DINGO_LOG(INFO) << ::dingodb::pb::node::LogLevel_descriptor()->FindValueByNumber(response.log_level())->name();
}

void SetUpRaftAddPeer(CLI::App &app) {
  auto opt = std::make_shared<RaftAddPeerCommandOptions>();
  auto *cmd = app.add_subcommand("RaftAddPeer", "coordinator RaftAddPeer")->group("Coordinator Manager Commands");
  cmd->add_option("--peer", opt->peer, "Request parameter peer, for example: 127.0.0.1:22101")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--index", opt->index, "Index")->expected(0, 1)->group("Coordinator Manager Commands");
  cmd->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunRaftAddPeer(*opt); });
}

void RunRaftAddPeer(RaftAddPeerCommandOptions const &opt) {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  request.set_add_peer(opt.peer);
  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::AddPeer);

  if (opt.index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (opt.index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else if (opt.index == 2) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::TsoNodeIndex);
  } else if (opt.index == 3) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    std::cout << "index is error";
    return;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(kTimeoutMs);

  if (opt.coordinator_addr.empty()) {
    std::cout << "Please set --addr or --coordinator_addr";
    return;
  }

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

  stub.RaftControl(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }
  DINGO_LOG(INFO) << "Received response"
                  << " request_attachment=" << cntl.request_attachment().size()
                  << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
  DINGO_LOG(INFO) << response.DebugString();
  std::cout << "response:" << response.DebugString();
}

void SetUpRaftRemovePeer(CLI::App &app) {
  auto opt = std::make_shared<RaftRemovePeerOption>();

  auto *cmd = app.add_subcommand("RaftRemovePeer", "Raft remove peer")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--peer", opt->peer, "Request parameter peer, for example: 127.0.0.1:22101")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--index", opt->index, "Index")->expected(0, 1)->group("Coordinator Manager Commands");

  cmd->callback([opt]() { RunRaftRemovePeer(*opt); });
}
void RunRaftRemovePeer(RaftRemovePeerOption const &opt) {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;
  request.set_add_peer(opt.peer);
  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::RemovePeer);

  if (opt.index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (opt.index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    std::cout << "index is error";
    return;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(kTimeoutMs);

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

  stub.RaftControl(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(timeout_ms * 1000L);
  }
  std::cout << "response:" << response.DebugString();
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpRaftTansferLeader(CLI::App &app) {
  auto opt = std::make_shared<RaftTansferLeaderOption>();

  auto *cmd = app.add_subcommand("RaftTansferLeader", "Raft transfer leader")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--peer", opt->peer, "Request parameter peer, for example: 127.0.0.1:22101")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--index", opt->index, "Index")->expected(0, 3)->group("Coordinator Manager Commands");

  cmd->callback([opt]() { RunRaftTansferLeader(*opt); });
}
void RunRaftTansferLeader(RaftTansferLeaderOption const &opt) {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;
  request.set_add_peer(opt.peer);
  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::TransferLeader);
  if (opt.index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (opt.index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else if (opt.index == 2) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::TsoNodeIndex);
  } else if (opt.index == 3) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    std::cout << "index is error";
    return;
  }

  brpc::Controller cntl;

  cntl.set_timeout_ms(kTimeoutMs);

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

  stub.RaftControl(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(timeout_ms * 1000L);
  }
  std::cout << "response:" << response.DebugString();
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpRaftSnapshot(CLI::App &app) {
  auto opt = std::make_shared<RaftSnapshotOption>();
  auto *cmd = app.add_subcommand("RaftSnapshot", "Raft snapshot")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--peer", opt->peer, "Request parameter peer, for example: 127.0.0.1:22101")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--index", opt->index, "Index")->expected(0, 3)->group("Coordinator Manager Commands");

  cmd->callback([opt]() { RunRaftSnapshot(*opt); });
}
void RunRaftSnapshot(RaftSnapshotOption const &opt) {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;
  request.set_add_peer(opt.peer);
  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::Snapshot);

  if (opt.index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (opt.index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else if (opt.index == 2) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::TsoNodeIndex);
  } else if (opt.index == 3) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    std::cout << "index is error";
    return;
  }

  brpc::Controller cntl;

  cntl.set_timeout_ms(kTimeoutMs);

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

  stub.RaftControl(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(timeout_ms * 1000L);
  }
  std::cout << "response:" << response.DebugString();
  DINGO_LOG(INFO) << "Received response"
                  << " request_attachment=" << cntl.request_attachment().size()
                  << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
}

void SetUpRaftResetPeer(CLI::App &app) {
  auto opt = std::make_shared<RaftResetPeerOption>();
  auto *cmd = app.add_subcommand("RaftResetPeer", "Raft reset peer")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--peer", opt->peer, "Request parameter peer, for example: 127.0.0.1:22101")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--index", opt->index, "Index")->expected(0, 3)->group("Coordinator Manager Commands");

  cmd->callback([opt]() { RunRaftResetPeer(*opt); });
}
void RunRaftResetPeer(RaftResetPeerOption const &opt) {
  dingodb::pb::coordinator::RaftControlRequest request;
  dingodb::pb::coordinator::RaftControlResponse response;

  std::vector<std::string> tokens;
  std::stringstream ss(opt.peer);
  std::string token;
  while (std::getline(ss, token, ',')) {
    tokens.push_back(token);
  }

  if (tokens.empty()) {
    std::cout << "peers is empty, for example: 127.0.0.1:22101,127.0.0.1:22102,127.0.0.1:22103";
    return;
  }

  for (const auto &it : tokens) {
    DINGO_LOG(INFO) << "peer: " << it;
    request.add_new_peers(it);
  }

  request.set_op_type(::dingodb::pb::coordinator::RaftControlOp::Snapshot);
  if (opt.index == 0) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::CoordinatorNodeIndex);
  } else if (opt.index == 1) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else if (opt.index == 2) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::TsoNodeIndex);
  } else if (opt.index == 3) {
    request.set_node_index(dingodb::pb::coordinator::RaftControlNodeIndex::AutoIncrementNodeIndex);
  } else {
    std::cout << "index is error";
    return;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(kTimeoutMs);

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);

  stub.RaftControl(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(timeout_ms * 1000L);
  }
  std::cout << "response:" << response.DebugString();
  DINGO_LOG(INFO) << "Received response"
                  << " request_attachment=" << cntl.request_attachment().size()
                  << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
}

void SetUpGetNodeInfo(CLI::App &app) {
  auto opt = std::make_shared<GetNodeInfoOption>();
  auto *cmd = app.add_subcommand("GetNodeInfo", "Get node info")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--peer", opt->peer, "Request parameter peer, for example: 127.0.0.1:22101")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--index", opt->index, "Index")->expected(0, 3)->group("Coordinator Manager Commands");

  cmd->callback([opt]() { RunGetNodeInfo(*opt); });
}
void RunGetNodeInfo(GetNodeInfoOption const &opt) {
  dingodb::pb::node::GetNodeInfoRequest request;
  dingodb::pb::node::GetNodeInfoResponse response;

  std::string const key = "Hello";
  // const char* op = nullptr;
  request.set_cluster_id(0);

  brpc::Controller cntl;
  cntl.set_timeout_ms(kTimeoutMs);

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
    return;
  }
  dingodb::pb::node::NodeService_Stub stub(&channel);
  stub.GetNodeInfo(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(timeout_ms * 1000L);
  }

  DINGO_LOG(INFO) << "Received response"
                  << " cluster_id=" << request.cluster_id()
                  << " request_attachment=" << cntl.request_attachment().size()
                  << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpChangeLogLevel(CLI::App &app) {
  auto opt = std::make_shared<GetChangeLogLevelOption>();
  auto *cmd = app.add_subcommand("ChangeLogLevel", "Change log level")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_addr", opt->coordinator_addr, "Coordinator servr addr, for example: 127.0.0.1:22001")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--level", opt->level, "Request parameter log level, DEBUG|INFO|WARNING|ERROR|FATAL")
      ->required()
      ->group("Coordinator Manager Commands");

  cmd->callback([opt]() { RunChangeLogLevel(*opt); });
}
void RunChangeLogLevel(GetChangeLogLevelOption const &opt) {
  dingodb::pb::node::ChangeLogLevelRequest request;
  dingodb::pb::node::ChangeLogLevelResponse response;

  using ::dingodb::pb::node::LogLevel;

  if (opt.level == "DEBUG") {
    request.set_log_level(dingodb::pb::node::DEBUG);
  } else if (opt.level == "INFO") {
    request.set_log_level(dingodb::pb::node::INFO);
  } else if (opt.level == "WARNING") {
    request.set_log_level(dingodb::pb::node::WARNING);
  } else if (opt.level == "ERROR") {
    request.set_log_level(dingodb::pb::node::ERROR);
  } else if (opt.level == "FATAL") {
    request.set_log_level(dingodb::pb::node::FATAL);
  } else {
    std::cout << "level is not valid, please set --level=DEBUG|INFO|WARNING|ERROR|FATAL";
    return;
  }

  auto *log_detail = request.mutable_log_detail();
  log_detail->set_log_buf_secs(10);
  log_detail->set_max_log_size(100);
  log_detail->set_stop_logging_if_full_disk(false);

  brpc::Controller cntl;
  cntl.set_timeout_ms(kTimeoutMs);

  brpc::Channel channel;
  if (!GetBrpcChannel(opt.coordinator_addr, channel)) {
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

void SetUpHello(CLI::App &app) {
  auto opt = std::make_shared<HelloOption>();
  auto *cmd = app.add_subcommand("Hello", "Coordinator hello ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunHello(*opt); });
}

void RunHello(HelloOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  std::string const key = "Hello";
  // const char* op = nullptr;
  request.set_hello(0);
  request.set_get_memory_info(true);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("Hello", request, response);
  DINGO_LOG(INFO) << "SendRequest status: " << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpStoreHeartbeat(CLI::App &app) {
  auto opt = std::make_shared<StoreHeartbeatOption>();
  auto *cmd = app.add_subcommand("StoreHeartbeat", "Store heart beat ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunStoreHeartbeat(*opt); });
}

void RunStoreHeartbeat(StoreHeartbeatOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  SendStoreHearbeatV2(CoordinatorInteraction::GetInstance().GetCoorinatorInteraction(), 100);
  SendStoreHearbeatV2(CoordinatorInteraction::GetInstance().GetCoorinatorInteraction(), 200);
  SendStoreHearbeatV2(CoordinatorInteraction::GetInstance().GetCoorinatorInteraction(), 300);
}
void SendStoreHearbeatV2(dingodb::CoordinatorInteractionPtr coordinator_interaction, int64_t store_id) {
  dingodb::pb::coordinator::StoreHeartbeatRequest request;
  dingodb::pb::coordinator::StoreHeartbeatResponse response;

  request.set_self_storemap_epoch(1);
  // request.set_self_regionmap_epoch(1);
  // mock store
  auto *store = request.mutable_store();
  store->set_id(store_id);
  store->set_state(::dingodb::pb::common::StoreState::STORE_NORMAL);
  auto *server_location = store->mutable_server_location();
  server_location->set_host("127.0.0.1");
  server_location->set_port(19191);
  auto *raft_location = store->mutable_raft_location();
  raft_location->set_host("127.0.0.1");
  raft_location->set_port(19192);
  store->set_resource_tag("DINGO_DEFAULT");

  auto status = coordinator_interaction->SendRequest("StoreHeartbeat", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpCreateStore(CLI::App &app) {
  auto opt = std::make_shared<CreateStoreOption>();
  auto *cmd = app.add_subcommand("CreateStore", "Create store ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunCreateStore(*opt); });
}

void RunCreateStore(CreateStoreOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::CreateStoreRequest request;
  dingodb::pb::coordinator::CreateStoreResponse response;

  request.set_cluster_id(1);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CreateStore", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDeleteStore(CLI::App &app) {
  auto opt = std::make_shared<DeleteStoreOption>();
  auto *cmd = app.add_subcommand("DeleteStore", "Delete store ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--keyring", opt->keyring, "Request parameter keyring")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "store id")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunDeleteStore(*opt); });
}

void RunDeleteStore(DeleteStoreOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::DeleteStoreRequest request;
  dingodb::pb::coordinator::DeleteStoreResponse response;

  request.set_cluster_id(1);

  request.set_store_id(std::stol(opt.id));
  auto *keyring = request.mutable_keyring();
  keyring->assign(opt.keyring);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("DeleteStore", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateStore(CLI::App &app) {
  auto opt = std::make_shared<UpdateStoreOption>();
  auto *cmd = app.add_subcommand("UpdateStore", "Update store state")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--keyring", opt->keyring, "Request parameter keyring")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "store id")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--state", opt->state, "set store state IN|OUT")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunUpdateStore(*opt); });
}

void RunUpdateStore(UpdateStoreOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::UpdateStoreRequest request;
  dingodb::pb::coordinator::UpdateStoreResponse response;

  request.set_cluster_id(1);

  request.set_store_id(std::stol(opt.id));
  auto *keyring = request.mutable_keyring();
  keyring->assign(opt.keyring);
  if (opt.state == "IN") {
    request.set_store_in_state(::dingodb::pb::common::StoreInState::STORE_IN);
  } else if (opt.state == "OUT") {
    request.set_store_in_state(::dingodb::pb::common::StoreInState::STORE_OUT);
  } else {
    DINGO_LOG(WARNING) << "state is invalid, must be  IN or OUT";
    return;
  }

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("UpdateStore", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpCreateExecutor(CLI::App &app) {
  auto opt = std::make_shared<CreateExecutorOption>();
  auto *cmd = app.add_subcommand("CreateExecutor", "Create executor")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--keyring", opt->keyring, "Request parameter keyring")->required();
  cmd->add_option("--host", opt->host, "host")->required();
  cmd->add_option("--port", opt->port, "port")->required();
  cmd->add_option("--user", opt->user, "Request parameter user")->required();

  cmd->callback([opt]() { RunCreateExecutor(*opt); });
}

void RunCreateExecutor(CreateExecutorOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::CreateExecutorRequest request;
  dingodb::pb::coordinator::CreateExecutorResponse response;

  request.set_cluster_id(1);
  request.mutable_executor()->mutable_executor_user()->set_user(opt.user);
  request.mutable_executor()->mutable_executor_user()->set_keyring(opt.keyring);
  request.mutable_executor()->mutable_server_location()->set_host(opt.host);
  request.mutable_executor()->mutable_server_location()->set_port(opt.port);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CreateExecutor", request,
                                                                                              response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Create executor  failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Create executor success." << std::endl;
  std::cout << "executors: " << response.executor().DebugString() << std::endl;
}

void SetUpDeleteExecutor(CLI::App &app) {
  auto opt = std::make_shared<DeleteExecutorOption>();
  auto *cmd = app.add_subcommand("CreateStore", "Create store ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--keyring", opt->keyring, "Request parameter keyring")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--user", opt->user, "Request parameter user")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter executor id")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunDeleteExecutor(*opt); });
}

void RunDeleteExecutor(DeleteExecutorOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::DeleteExecutorRequest request;
  dingodb::pb::coordinator::DeleteExecutorResponse response;

  request.set_cluster_id(1);
  request.mutable_executor()->set_id(opt.id);
  request.mutable_executor()->mutable_executor_user()->set_user(opt.user);
  request.mutable_executor()->mutable_executor_user()->set_keyring(opt.keyring);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CreateExecutor", request,
                                                                                              response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpCreateExecutorUser(CLI::App &app) {
  auto opt = std::make_shared<CreateExecutorUserOption>();
  auto *cmd = app.add_subcommand("CreateExecutorUser", "Create executor user ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--keyring", opt->keyring, "Request parameter keyring")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--user", opt->user, "Request parameter user")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunCreateExecutorUser(*opt); });
}

void RunCreateExecutorUser(CreateExecutorUserOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::CreateExecutorUserRequest request;
  dingodb::pb::coordinator::CreateExecutorUserResponse response;

  request.set_cluster_id(1);
  request.mutable_executor_user()->set_user(opt.user);
  request.mutable_executor_user()->set_keyring(opt.keyring);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CreateExecutorUser",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateExecutorUser(CLI::App &app) {
  auto opt = std::make_shared<UpdateExecutorUserOption>();
  auto *cmd = app.add_subcommand("UpdateExecutorUser", "Update executor user ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--keyring", opt->keyring, "Request parameter keyring")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--new_keyring", opt->new_keyring, "Request parameter new keyring")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--user", opt->user, "Request parameter user")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunUpdateExecutorUser(*opt); });
}

void RunUpdateExecutorUser(UpdateExecutorUserOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::UpdateExecutorUserRequest request;
  dingodb::pb::coordinator::UpdateExecutorUserResponse response;

  request.set_cluster_id(1);
  request.mutable_executor_user()->set_user(opt.user);
  request.mutable_executor_user()->set_keyring(opt.keyring);
  request.mutable_executor_user_update()->set_keyring(opt.new_keyring);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("UpdateExecutorUser",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDeleteExecutorUser(CLI::App &app) {
  auto opt = std::make_shared<DeleteExecutorUserOption>();
  auto *cmd = app.add_subcommand("DeleteExecutorUser", "Delete executor user ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--keyring", opt->keyring, "Request parameter keyring")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--user", opt->user, "Request parameter user")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunDeleteExecutorUser(*opt); });
}
void RunDeleteExecutorUser(DeleteExecutorUserOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::DeleteExecutorUserRequest request;
  dingodb::pb::coordinator::DeleteExecutorUserResponse response;

  request.set_cluster_id(1);
  request.mutable_executor_user()->set_user(opt.user);
  request.mutable_executor_user()->set_keyring(opt.keyring);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("DeleteExecutorUser",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetExecutorUserMap(CLI::App &app) {
  auto opt = std::make_shared<GetExecutorUserMapOption>();
  auto *cmd = app.add_subcommand("GetExecutorUserMap", "Get executor user map")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetExecutorUserMap(*opt); });
}

void RunGetExecutorUserMap(GetExecutorUserMapOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetExecutorUserMapRequest request;
  dingodb::pb::coordinator::GetExecutorUserMapResponse response;

  request.set_cluster_id(1);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetExecutorUserMap",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpExecutorHeartbeat(CLI::App &app) {
  auto opt = std::make_shared<ExecutorHeartbeatOption>();
  auto *cmd = app.add_subcommand("ExecutorHeartbeat", "Executor Heart beat ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--keyring", opt->keyring, "Request parameter keyring")->default_val("TO_BE_CONTINUED");
  cmd->add_option("--host", opt->host, "host")->required();
  cmd->add_option("--port", opt->port, "host")->required();
  cmd->add_option("--id", opt->id, "Request parameter executor id");
  cmd->add_option("--user", opt->user, "Request parameter user")->default_val("administrator");
  cmd->add_option("--cluster_name", opt->cluster_name, "Request parameter cluster_name");
  cmd->callback([opt]() { RunExecutorHeartbeat(*opt); });
}

void RunExecutorHeartbeat(ExecutorHeartbeatOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::ExecutorHeartbeatRequest request;
  dingodb::pb::coordinator::ExecutorHeartbeatResponse response;

  request.set_self_executormap_epoch(1);
  // mock executor
  auto *executor = request.mutable_executor();
  executor->set_id(opt.id);
  executor->mutable_server_location()->set_host(opt.host);
  executor->mutable_server_location()->set_port(opt.port);
  executor->set_state(::dingodb::pb::common::ExecutorState::EXECUTOR_NORMAL);
  executor->set_cluster_name(opt.cluster_name);

  auto *user = executor->mutable_executor_user();
  user->set_user(opt.user);
  user->set_keyring(opt.keyring);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("ExecutorHeartbeat",
                                                                                              request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Executor Heartbeat table error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Executor Heartbeat success." << std::endl;
  for (auto const &executor : response.executormap().executors()) {
    std::cout << "executor: " << executor.DebugString() << std::endl;
  }
}

void SetUpGetStoreMap(CLI::App &app) {
  auto opt = std::make_shared<GetStoreMapOption>();
  auto *cmd = app.add_subcommand("GetStoreMap", "Get store map")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_flag("--use_filter_store_type", opt->use_filter_store_type, "use filter_store_type")->default_val(false);
  cmd->add_option("--filter_store_type", opt->filter_store_type, "store type 0:store;1:index;2:document")
      ->check(CLI::Range(0, 2));
  cmd->callback([opt]() { RunGetStoreMap(*opt); });
}

void RunGetStoreMap(GetStoreMapOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(1);
  if (opt.use_filter_store_type) {
    request.add_filter_store_types(static_cast<dingodb::pb::common::StoreType>(opt.filter_store_type));
  }

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetStoreMap", request, response);
  Pretty::Show(response);
}

void SetUpGetExecutorMap(CLI::App &app) {
  auto opt = std::make_shared<GetExecutorMapOption>();
  auto *cmd = app.add_subcommand("GetExecutorMap", "Get executor map")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--cluster_name", opt->cluster_name, "if cluster_name is empty, return all executors")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetExecutorMap(*opt); });
}

void RunGetExecutorMap(GetExecutorMapOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetExecutorMapRequest request;
  dingodb::pb::coordinator::GetExecutorMapResponse response;

  request.set_epoch(1);
  request.set_cluster_name(opt.cluster_name);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetExecutorMap", request,
                                                                                              response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Get executor map  failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Get executor map success." << std::endl;
  for (auto const &executor : response.executormap().executors()) {
    std::cout << "executor: " << executor.DebugString() << std::endl;
  }
}

void SetUpGetDeleteRegionMap(CLI::App &app) {
  auto opt = std::make_shared<GetDeleteRegionMapOption>();
  auto *cmd = app.add_subcommand("DeleteRegionMap", "Delete region map")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetDeleteRegionMap(*opt); });
}

void RunGetDeleteRegionMap(GetDeleteRegionMapOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetDeletedRegionMapRequest request;
  dingodb::pb::coordinator::GetDeletedRegionMapResponse response;

  request.set_epoch(1);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetDeletedRegionMap",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto &region : response.regionmap().regions()) {
    DINGO_LOG(INFO) << "Region id=" << region.id() << " name=" << region.definition().name()
                    << " state=" << dingodb::pb::common::RegionState_Name(region.state())
                    << " deleted_time_stamp=" << region.deleted_timestamp()
                    << " deleted_time_minutes=" << (butil::gettimeofday_ms() - region.deleted_timestamp()) / 1000 / 60
                    << " deleted_time_hours=" << (butil::gettimeofday_ms() - region.deleted_timestamp()) / 1000 / 3600;
  }

  DINGO_LOG(INFO) << "Deleted region_count=" << response.regionmap().regions_size();
}

void SetUpAddDeleteRegionMap(CLI::App &app) {
  auto opt = std::make_shared<AddDeleteRegionMapOption>();
  auto *cmd = app.add_subcommand("AddDeleteRegionMap", "Add delete region map")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--is_force", opt->is_force, "Request parameter force ")->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter region id ")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunAddDeleteRegionMap(*opt); });
}

void RunAddDeleteRegionMap(AddDeleteRegionMapOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::AddDeletedRegionMapRequest request;
  dingodb::pb::coordinator::AddDeletedRegionMapResponse response;

  request.set_region_id(std::stol(opt.id));
  request.set_force(opt.is_force);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("AddDeletedRegionMap",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "response=" << response.DebugString();
}

void SetUpCleanDeleteRegionMap(CLI::App &app) {
  auto opt = std::make_shared<CleanDeleteRegionMapOption>();
  auto *cmd = app.add_subcommand("AddDeleteRegionMap", "Add delete region map")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter region id ")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunCleanDeleteRegionMap(*opt); });
}

void RunCleanDeleteRegionMap(CleanDeleteRegionMapOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::CleanDeletedRegionMapRequest request;
  dingodb::pb::coordinator::CleanDeletedRegionMapResponse response;

  request.set_region_id(std::stol(opt.id));
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CleanDeletedRegionMap",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "response=" << response.DebugString();
}

void SetUpGetRegionCount(CLI::App &app) {
  auto opt = std::make_shared<GetRegionCountOption>();
  auto *cmd = app.add_subcommand("GetRegionCount", "Get region map count")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetRegionCount(*opt); });
}

void RunGetRegionCount(GetRegionCountOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetRegionCountRequest request;
  dingodb::pb::coordinator::GetRegionCountResponse response;

  request.set_epoch(1);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetRegionCount", request,
                                                                                              response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  DINGO_LOG(INFO) << " region_count=" << response.region_count();
}

void SetUpGetCoordinatorMap(CLI::App &app) {
  auto opt = std::make_shared<GetCoordinatorMapOption>();
  auto *cmd = app.add_subcommand("GetCoordinatorMap", "Get coordinator map")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_flag("--get_coordinator_map", opt->get_coordinator_map, "Request patameter get_coordinaotor_map");
  cmd->callback([opt]() { RunGetCoordinatorMap(*opt); });
}

void RunGetCoordinatorMap(GetCoordinatorMapOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetCoordinatorMapRequest request;
  dingodb::pb::coordinator::GetCoordinatorMapResponse response;

  request.set_cluster_id(0);

  request.set_get_coordinator_map(opt.get_coordinator_map);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetCoordinatorMap",
                                                                                              request, response);

  Pretty::Show(response);
}

void SetUpCreateRegionId(CLI::App &app) {
  auto opt = std::make_shared<CreateRegionIdOption>();
  auto *cmd = app.add_subcommand("CreateRegionId", "Create Region ID")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--count", opt->count, "count must > 0 and < 2048")
      ->required()
      ->check(CLI::Range(1, 2048))
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunCreateRegionId(*opt); });
}

void RunCreateRegionId(CreateRegionIdOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::CreateRegionIdRequest request;
  dingodb::pb::coordinator::CreateRegionIdResponse response;

  request.set_count(opt.count);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CreateRegionId", request,
                                                                                              response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << "response=" << response.DebugString();
}

void SetUpQueryRegion(CLI::App &app) {
  auto opt = std::make_shared<QueryRegionOption>();
  auto *cmd = app.add_subcommand("QueryRegion", "Query region ")->group("Region Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter region id ")->required();
  cmd->callback([opt]() { RunQueryRegion(*opt); });
}

void RunQueryRegion(QueryRegionOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  request.set_region_id(opt.id);
  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("QueryRegion", request, response);

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "query region " << opt.id << " failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  auto region = response.region();
  std::cout << "Region id=" << region.id() << " name=" << region.definition().name()
            << " state=" << dingodb::pb::common::RegionState_Name(region.state())
            << " leader_store_id=" << region.leader_store_id()
            << " replica_state=" << dingodb::pb::common::ReplicaStatus_Name(region.status().replica_status())
            << " raft_status=" << dingodb::pb::common::RegionRaftStatus_Name(region.status().raft_status())
            << std::endl;
  std::cout << "start_key=[" << dingodb::Helper::StringToHex(region.definition().range().start_key()) << "]"
            << std::endl;
  std::cout << "  end_key=[" << dingodb::Helper::StringToHex(region.definition().range().end_key()) << "]" << std::endl;
}

void SetUpCreateRegion(CLI::App &app) {
  auto opt = std::make_shared<CreateRegionOption>();
  auto *cmd = app.add_subcommand("CreateRegion", "Create region ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--name", opt->name, "Request parameter region name")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--replica", opt->replica, "Request parameter replica num, must greater than 0")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--raw_engine", opt->raw_engine, "Request parameter raw_engine , rocksdb|bdb|xdp")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--create_document_region", opt->create_document_region, "Request parameter create document region")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--region_prefix", opt->region_prefix, "Request parameter region prefix, size must be 1")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--part_id", opt->part_id, "Request parameter part id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--start_id", opt->start_id, "Request parameter start_id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--end_id", opt->end_id, "Request parameter start_id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--start_key", opt->start_key, "Request parameter start_key")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--end_key", opt->end_key, "Request parameter end_key")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--use_json_parameter", opt->use_json_parameter, "Request parameter use_json_parameter swtich")
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")->group("Coordinator Manager Commands");
  cmd->add_option("--vector_index_type", opt->vector_index_type,
                  "Request parameter vector_index_type, hnsw|flat|ivf_flat|ivf_pq")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--dimension", opt->dimension, "Request parameter dimension")->group("Coordinator Manager Commands");
  cmd->add_option("--metrics_type", opt->metrics_type, "Request parameter metrics_type, L2|IP|COSINE")
      ->ignore_case()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--max_elements", opt->max_elements, "Request parameter max_elements")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--efconstruction", opt->efconstruction, "Request parameter efconstruction")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--nlinks", opt->nlinks, "Request parameter nlinks")->group("Coordinator Manager Commands");
  cmd->add_option("--ncentroids", opt->ncentroids, "Request parameter ncentroids, ncentroids default 10")
      ->default_val(10)
      ->group("Coordinator Manager Commands");
  cmd->add_option("--nsubvector", opt->nsubvector, "Request parameter nsubvector, ivf pq default subvector nums 8")
      ->default_val(8)
      ->group("Coordinator Manager Commands");
  cmd->add_option("--nbits_per_idx", opt->nbits_per_idx,
                  "Request parameter nbits_per_idx, ivf pq default nbits_per_idx 8")
      ->default_val(8)
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunCreateRegion(*opt); });
}

void RunCreateRegion(CreateRegionOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  if (opt.name.empty()) {
    DINGO_LOG(ERROR) << "name is empty";
    return;
  }

  if (opt.replica == 0) {
    DINGO_LOG(ERROR) << "replica is empty";
    return;
  }

  dingodb::pb::coordinator::CreateRegionRequest request;
  dingodb::pb::coordinator::CreateRegionResponse response;

  request.set_region_name(opt.name);
  request.set_replica_num(opt.replica);
  if (opt.raw_engine != "rocksdb" && opt.raw_engine != "bdb" && opt.raw_engine != "xdp") {
    DINGO_LOG(ERROR) << "raw_engine must be rocksdb, bdb or xdp";
    return;
  }

  request.set_raw_engine(client_v2::Helper::GetRawEngine(opt.raw_engine));

  if (opt.create_document_region) {
    DINGO_LOG(INFO) << "create a document region";
    if (opt.region_prefix.size() != 1) {
      DINGO_LOG(ERROR) << "region_prefix must be 1";
      return;
    }

    // document index region must have a part_id
    if (opt.part_id == 0) {
      DINGO_LOG(ERROR) << "part_id is empty";
      return;
    }
    request.set_part_id(opt.part_id);

    std::string start_key;
    std::string end_key;

    if (opt.start_id > 0 && opt.end_id > 0 && opt.start_id < opt.end_id) {
      DINGO_LOG(INFO) << "use start_id " << opt.start_id << ", end_id " << opt.end_id << " to create region range";
      start_key = dingodb::DocumentCodec::PackageDocumentKey(opt.region_prefix.at(0), opt.part_id, opt.start_id);
      end_key = dingodb::DocumentCodec::PackageDocumentKey(opt.region_prefix.at(0), opt.part_id, opt.end_id);
    } else {
      DINGO_LOG(INFO) << "use part_id " << opt.part_id << " to create region range";
      start_key = dingodb::DocumentCodec::PackageDocumentKey(opt.region_prefix.at(0), opt.part_id);
      end_key = dingodb::DocumentCodec::PackageDocumentKey(opt.region_prefix.at(0), opt.part_id + 1);
    }
    DINGO_LOG(INFO) << "region range is : " << dingodb::Helper::StringToHex(start_key) << " to "
                    << dingodb::Helper::StringToHex(end_key);

    request.mutable_range()->set_start_key(start_key);
    request.mutable_range()->set_end_key(end_key);

    // set index_type for vector index
    request.mutable_index_parameter()->set_index_type(dingodb::pb::common::IndexType::INDEX_TYPE_DOCUMENT);

    std::string multi_type_column_json =
        R"({"col1": { "tokenizer": { "type": "chinese"}}, "col2": { "tokenizer": {"type": "i64", "indexed": true }}, "col3": { "tokenizer": {"type": "f64", "indexed": true }}, "col4": { "tokenizer": {"type": "chinese"}} })";

    // document index parameter
    request.mutable_index_parameter()->set_index_type(dingodb::pb::common::IndexType::INDEX_TYPE_DOCUMENT);
    auto *document_index_parameter = request.mutable_index_parameter()->mutable_document_index_parameter();

    if (opt.use_json_parameter) {
      document_index_parameter->set_json_parameter(multi_type_column_json);
    }

    auto *scalar_schema = document_index_parameter->mutable_scalar_schema();
    auto *field_col1 = scalar_schema->add_fields();
    field_col1->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field_col1->set_key("col1");
    auto *field_col2 = scalar_schema->add_fields();
    field_col2->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field_col2->set_key("col2");
    auto *field_col3 = scalar_schema->add_fields();
    field_col3->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field_col3->set_key("col3");
    auto *field_col4 = scalar_schema->add_fields();
    field_col4->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field_col4->set_key("col4");

  } else if (opt.vector_index_type.empty()) {
    DINGO_LOG(WARNING) << "vector_index_type is empty, so create a store region";
    if (opt.start_key.empty()) {
      DINGO_LOG(ERROR) << "start_key is empty";
      return;
    }

    if (opt.end_key.empty()) {
      DINGO_LOG(ERROR) << "end_key is empty";
      return;
    }

    std::string start_key = opt.start_key;
    std::string end_key = opt.end_key;

    if (opt.key_is_hex) {
      DINGO_LOG(INFO) << "key is hex";
      start_key = dingodb::Helper::HexToString(opt.start_key);
      end_key = dingodb::Helper::HexToString(opt.end_key);
    }

    if (start_key > end_key) {
      DINGO_LOG(ERROR) << "start_key must < end_key";
      return;
    }
    request.mutable_range()->set_start_key(start_key);
    request.mutable_range()->set_end_key(end_key);

    if (opt.part_id > 0) {
      request.set_part_id(opt.part_id);
    }
  } else {
    DINGO_LOG(INFO) << "vector_index_type=" << opt.vector_index_type << ", so create a vector index region";

    if (opt.region_prefix.size() != 1) {
      DINGO_LOG(ERROR) << "region_prefix must be 1";
      return;
    }

    // vector index region must have a part_id
    if (opt.part_id == 0) {
      DINGO_LOG(ERROR) << "part_id is empty";
      return;
    }
    request.set_part_id(opt.part_id);

    std::string start_key;
    std::string end_key;

    if (opt.start_id > 0 && opt.end_id > 0 && opt.start_id < opt.end_id) {
      DINGO_LOG(INFO) << "use start_id " << opt.start_id << ", end_id " << opt.end_id << " to create region range";
      start_key = dingodb::VectorCodec::PackageVectorKey(opt.region_prefix.at(0), opt.part_id, opt.start_id);
      end_key = dingodb::VectorCodec::PackageVectorKey(opt.region_prefix.at(0), opt.part_id, opt.end_id);
    } else {
      DINGO_LOG(INFO) << "use part_id " << opt.part_id << " to create region range";
      start_key = dingodb::VectorCodec::PackageVectorKey(opt.region_prefix.at(0), opt.part_id);
      end_key = dingodb::VectorCodec::PackageVectorKey(opt.region_prefix.at(0), opt.part_id + 1);
    }
    DINGO_LOG(INFO) << "region range is : " << dingodb::Helper::StringToHex(start_key) << " to "
                    << dingodb::Helper::StringToHex(end_key);

    request.mutable_range()->set_start_key(start_key);
    request.mutable_range()->set_end_key(end_key);

    auto *vector_index_parameter = request.mutable_index_parameter()->mutable_vector_index_parameter();
    if (opt.vector_index_type == "hnsw") {
      vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW);
    } else if (opt.vector_index_type == "flat") {
      vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    } else if (opt.vector_index_type == "ivf_flat") {
      vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT);
    } else if (opt.vector_index_type == "ivf_pq") {
      vector_index_parameter->set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
    } else {
      DINGO_LOG(WARNING) << "vector_index_type is invalid, now only support hnsw and flat";
      return;
    }

    // set index_type for vector index
    request.mutable_index_parameter()->set_index_type(dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR);

    if (opt.dimension == 0) {
      DINGO_LOG(WARNING) << "dimension is empty";
      return;
    }

    dingodb::pb::common::MetricType metric_type;

    if (opt.metrics_type == "L2" || opt.metrics_type == "l2") {
      metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_L2;
    } else if (opt.metrics_type == "IP" || opt.metrics_type == "ip") {
      metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
    } else if (opt.metrics_type == "COSINE" || opt.metrics_type == "cosine") {
      metric_type = ::dingodb::pb::common::MetricType::METRIC_TYPE_COSINE;
    } else {
      DINGO_LOG(WARNING) << "metrics_type is invalid, now only support L2, IP and COSINE";
      return;
    }

    if (opt.vector_index_type == "hnsw") {
      if (opt.max_elements == 0) {
        DINGO_LOG(WARNING) << "max_elements is empty";
        return;
      }
      if (opt.efconstruction == 0) {
        DINGO_LOG(WARNING) << "efconstruction is empty";
        return;
      }
      if (opt.nlinks == 0) {
        DINGO_LOG(WARNING) << "nlinks is empty";
        return;
      }

      DINGO_LOG(INFO) << "max_elements=" << opt.max_elements << ", dimension=" << opt.dimension;

      auto *hsnw_index_parameter = vector_index_parameter->mutable_hnsw_parameter();

      hsnw_index_parameter->set_dimension(opt.dimension);
      hsnw_index_parameter->set_metric_type(metric_type);
      hsnw_index_parameter->set_efconstruction(opt.efconstruction);
      hsnw_index_parameter->set_nlinks(opt.nlinks);
      hsnw_index_parameter->set_max_elements(opt.max_elements);
    } else if (opt.vector_index_type == "flat") {
      auto *flat_index_parameter = vector_index_parameter->mutable_flat_parameter();
      flat_index_parameter->set_dimension(opt.dimension);
      flat_index_parameter->set_metric_type(metric_type);
    } else if (opt.vector_index_type == "ivf_flat") {
      auto *ivf_flat_index_parameter = vector_index_parameter->mutable_ivf_flat_parameter();
      ivf_flat_index_parameter->set_dimension(opt.dimension);
      ivf_flat_index_parameter->set_metric_type(metric_type);
      ivf_flat_index_parameter->set_ncentroids(opt.ncentroids);
    } else if (opt.vector_index_type == "ivf_pq") {
      auto *ivf_pq_index_parameter = vector_index_parameter->mutable_ivf_pq_parameter();
      ivf_pq_index_parameter->set_dimension(opt.dimension);
      ivf_pq_index_parameter->set_metric_type(metric_type);
      ivf_pq_index_parameter->set_ncentroids(opt.ncentroids);
      ivf_pq_index_parameter->set_nsubvector(opt.nsubvector);
      ivf_pq_index_parameter->set_nbits_per_idx(opt.nbits_per_idx);
    }
  }

  DINGO_LOG(INFO) << "Create region request: " << request.DebugString();

  auto status2 =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CreateRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status2;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpCreateRegionForSplit(CLI::App &app) {
  auto opt = std::make_shared<CreateRegionForSplitOption>();
  auto *cmd =
      app.add_subcommand("CreateRegionForSplit", "Create region for split")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter region id ")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunCreateRegionForSplit(*opt); });
}

void RunCreateRegionForSplit(CreateRegionForSplitOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest(
      "QueryRegion", query_request, query_response);
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
  for (const auto &it : query_response.region().definition().peers()) {
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
  request.set_split_from_region_id(opt.id);  // set split from region id

  DINGO_LOG(INFO) << "Create region request: " << request.DebugString();

  auto status2 =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CreateRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status2;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDropRegion(CLI::App &app) {
  auto opt = std::make_shared<DropRegionOption>();
  auto *cmd = app.add_subcommand("DropRegion", "Drop region ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter region id ")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunDropRegion(*opt); });
}
void RunDropRegion(DropRegionOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  dingodb::pb::coordinator::DropRegionRequest request;
  dingodb::pb::coordinator::DropRegionResponse response;

  request.set_region_id(opt.id);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("DropRegion", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpDropRegionPermanently(CLI::App &app) {
  auto opt = std::make_shared<DropRegionPermanentlyOption>();
  auto *cmd =
      app.add_subcommand("DropRegionPermanently", "Drop region permanently")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter region id ")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunDropRegionPermanently(*opt); });
}

void RunDropRegionPermanently(DropRegionPermanentlyOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }

  dingodb::pb::coordinator::DropRegionPermanentlyRequest request;
  dingodb::pb::coordinator::DropRegionPermanentlyResponse response;

  request.set_region_id(std::stoll(opt.id));
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("DropRegionPermanently",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpSplitRegion(CLI::App &app) {
  auto opt = std::make_shared<SplitRegionOption>();
  auto *cmd = app.add_subcommand("SplitRegion", "Split region ")->group("Region Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--split_to_id", opt->split_to_id, "Request parameter split to region id")
      ->check(CLI::Range(1, std::numeric_limits<int32_t>::max()));
  cmd->add_option("--split_from_id", opt->split_from_id, "Request parameter split from region id")
      ->check(CLI::Range(1, std::numeric_limits<int32_t>::max()))
      ->required();
  cmd->add_option("--split_key", opt->split_key,
                  "Request parameter split_water_shed_key, if key is empty will auto generate from the mid between "
                  "start_key and end_key");
  cmd->add_option("--vector_id", opt->vector_id, "Request parameter vector_id");
  cmd->add_option("--document_id", opt->document_id, "Request parameter document_id ");
  cmd->add_flag("--store_create_region", opt->store_create_region, "Request parameter store_create_region ")
      ->default_val(false);
  cmd->callback([opt]() { RunSplitRegion(*opt); });
}

void RunSplitRegion(SplitRegionOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  dingodb::pb::coordinator::SplitRegionRequest request;
  dingodb::pb::coordinator::SplitRegionResponse response;

  request.mutable_split_request()->set_split_to_region_id(opt.split_to_id);

  if (opt.split_from_id > 0) {
    request.mutable_split_request()->set_split_from_region_id(opt.split_from_id);
  } else {
    std::cout << "split_from_id is empty";
    return;
  }

  if (!opt.split_key.empty()) {
    std::string split_key = dingodb::Helper::HexToString(opt.split_key);
    request.mutable_split_request()->set_split_watershed_key(split_key);
  } else {
    DINGO_LOG(INFO) << "split_key is empty, will auto generate from the mid between start_key and end_key";
    // query the region
    dingodb::pb::coordinator::QueryRegionRequest query_request;
    dingodb::pb::coordinator::QueryRegionResponse query_response;
    query_request.set_region_id(opt.split_from_id);

    auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest(
        "QueryRegion", query_request, query_response);
    if (query_response.has_error() && query_response.error().errcode() != dingodb::pb::error::Errno::OK) {
      std::cout << "query region failed, error: "
                << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(query_response.error().errcode())->name()
                << " " << query_response.error().errmsg();
      return;
    }

    if (query_response.region().definition().range().start_key().empty()) {
      std::cout << "split from region " << opt.split_from_id << " has no start_key";
      return;
    }

    if (query_response.region().definition().range().end_key().empty()) {
      std::cout << "split from region " << opt.split_from_id << " has no end_key";
      return;
    }

    // calc the mid value between start_key and end_key
    const auto &start_key = query_response.region().definition().range().start_key();
    const auto &end_key = query_response.region().definition().range().end_key();

    std::string real_mid;
    if (query_response.region().definition().index_parameter().index_type() ==
        dingodb::pb::common::IndexType::INDEX_TYPE_VECTOR) {
      if (opt.vector_id > 0) {
        int64_t partition_id = dingodb::VectorCodec::UnPackagePartitionId(start_key);
        real_mid = dingodb::VectorCodec::PackageVectorKey(start_key[0], partition_id, opt.vector_id);
      } else {
        real_mid = client_v2::Helper::CalculateVectorMiddleKey(start_key, end_key);
      }
    } else if (query_response.region().definition().index_parameter().index_type() ==
               dingodb::pb::common::IndexType::INDEX_TYPE_DOCUMENT) {
      if (opt.document_id > 0) {
        int64_t partition_id = dingodb::DocumentCodec::UnPackagePartitionId(start_key);
        real_mid = dingodb::DocumentCodec::PackageDocumentKey(start_key[0], partition_id, opt.document_id);
      } else {
        real_mid = client_v2::Helper::CalculateDocumentMiddleKey(start_key, end_key);
      }
    } else {
      real_mid = dingodb::Helper::CalculateMiddleKey(start_key, end_key);
    }

    request.mutable_split_request()->set_split_watershed_key(real_mid);

    if (query_response.region().definition().range().start_key().compare(real_mid) >= 0 ||
        query_response.region().definition().range().end_key().compare(real_mid) <= 0) {
      std::cout << "SplitRegion split_watershed_key is illegal, split_watershed_key = "
                << dingodb::Helper::StringToHex(real_mid)
                << ", query_response.region()_id = " << query_response.region().id() << " start_key="
                << dingodb::Helper::StringToHex(query_response.region().definition().range().start_key())
                << ", end_key=" << dingodb::Helper::StringToHex(query_response.region().definition().range().end_key());
      return;
    }
  }

  if (opt.store_create_region) {
    request.mutable_split_request()->set_store_create_region(opt.store_create_region);
  }

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("SplitRegion", request, response);

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "split from region " << opt.split_from_id << " to region " << opt.split_to_id
              << " store_create_region=" << opt.store_create_region << " with watershed key ["
              << dingodb::Helper::StringToHex(request.split_request().split_watershed_key()) << "] failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "split from region " << opt.split_from_id << " to region " << opt.split_to_id
            << " store_create_region=" << opt.store_create_region << " with watershed key ["
            << dingodb::Helper::StringToHex(request.split_request().split_watershed_key()) << "] success" << std::endl;
}

void SetUpMergeRegion(CLI::App &app) {
  auto opt = std::make_shared<MergeRegionOption>();
  auto *cmd = app.add_subcommand("MergeRegion", "Merge region ")->group("Region Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--target_id", opt->target_id, "Request parameter target id ")
      ->check(CLI::Range(1, std::numeric_limits<int32_t>::max()))
      ->required();
  cmd->add_option("--source_id", opt->source_id, "Request parameter source id ")
      ->check(CLI::Range(1, std::numeric_limits<int32_t>::max()))
      ->required();
  cmd->callback([opt]() { RunMergeRegion(*opt); });
}

void RunMergeRegion(MergeRegionOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  dingodb::pb::coordinator::MergeRegionRequest request;
  dingodb::pb::coordinator::MergeRegionResponse response;

  if (opt.target_id > 0) {
    request.mutable_merge_request()->set_target_region_id(opt.target_id);
  } else {
    std::cout << "target_id is empty";
    return;
  }

  if (opt.source_id > 0) {
    request.mutable_merge_request()->set_source_region_id(opt.source_id);
  } else {
    std::cout << "source_id is empty";
    return;
  }

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("MergeRegion", request, response);

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "merge from source_region " << opt.source_id << " to target_region " << opt.target_id
              << "] failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "merge from source_region " << opt.source_id << " to target_region " << opt.target_id << " success"
            << std::endl;
}

void SetUpAddPeerRegion(CLI::App &app) {
  auto opt = std::make_shared<AddPeerRegionOption>();
  auto *cmd = app.add_subcommand("AddPeerRegion", "Add peer region ")->group("Region Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--store_id", opt->store_id, "Request parameter store id ")->required();
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id ")->required();
  cmd->callback([opt]() { RunAddPeerRegion(*opt); });
}

void RunAddPeerRegion(AddPeerRegionOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  // get StoreMap
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(0);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetStoreMap", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "get store map failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  dingodb::pb::common::Peer new_peer;
  for (const auto &store : response.storemap().stores()) {
    if (store.id() == opt.store_id) {
      new_peer.set_store_id(store.id());
      new_peer.set_role(dingodb::pb::common::PeerRole::VOTER);
      *(new_peer.mutable_server_location()) = store.server_location();
      *(new_peer.mutable_raft_location()) = store.raft_location();
    }
  }

  if (new_peer.store_id() == 0) {
    std::cout << "store_id not found";
    return;
  }

  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(opt.region_id);

  auto status2 = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest(
      "QueryRegion", query_request, query_response);
  if (query_response.has_error() && query_response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "query region failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(query_response.error().errcode())->name()
              << " " << query_response.error().errmsg();
    return;
  }

  if (query_response.region().definition().peers_size() == 0) {
    std::cout << "region not found";
    return;
  }

  // validate peer not exists in region peers
  for (const auto &peer : query_response.region().definition().peers()) {
    if (peer.store_id() == opt.store_id) {
      std::cout << "peer already exists";
      return;
    }
  }

  // generate change peer
  dingodb::pb::coordinator::ChangePeerRegionRequest change_peer_request;
  dingodb::pb::coordinator::ChangePeerRegionResponse change_peer_response;

  auto *new_definition = change_peer_request.mutable_change_peer_request()->mutable_region_definition();
  *new_definition = query_response.region().definition();
  auto *new_peer_to_add = new_definition->add_peers();
  *new_peer_to_add = new_peer;

  auto status3 = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest(
      "ChangePeerRegion", change_peer_request, change_peer_response);
  if (change_peer_response.has_error() && change_peer_response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout
        << "change peer region failed, error: "
        << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(change_peer_response.error().errcode())->name()
        << " " << change_peer_response.error().errmsg();
    return;
  }
  std::cout << "change peer success" << std::endl;
}

void SetUpRemovePeerRegion(CLI::App &app) {
  auto opt = std::make_shared<RemovePeerRegionOption>();
  auto *cmd = app.add_subcommand("RemovePeerRegion", "Remove peer region")->group("Region Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--store_id", opt->store_id, "Request parameter store id ")->required();
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id ")->required();
  cmd->callback([opt]() { RunRemovePeerRegion(*opt); });
}

void RunRemovePeerRegion(RemovePeerRegionOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(opt.region_id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest(
      "QueryRegion", query_request, query_response);
  if (query_response.has_error() && query_response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "query region failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(query_response.error().errcode())->name()
              << " " << query_response.error().errmsg();
    return;
  }

  if (query_response.region().definition().peers_size() == 0) {
    std::cout << "region not found";
    return;
  }

  // validate peer not exists in region peers
  bool found = false;
  for (const auto &peer : query_response.region().definition().peers()) {
    if (peer.store_id() == opt.store_id) {
      found = true;
      break;
    }
  }

  if (!found) {
    std::cout << "peer not found";
    return;
  }

  // generate change peer
  dingodb::pb::coordinator::ChangePeerRegionRequest change_peer_request;
  dingodb::pb::coordinator::ChangePeerRegionResponse change_peer_response;

  auto *new_definition = change_peer_request.mutable_change_peer_request()->mutable_region_definition();
  *new_definition = query_response.region().definition();
  for (int i = 0; i < new_definition->peers_size(); i++) {
    if (new_definition->peers(i).store_id() == opt.store_id) {
      new_definition->mutable_peers()->SwapElements(i, new_definition->peers_size() - 1);
      new_definition->mutable_peers()->RemoveLast();
      break;
    }
  }

  DINGO_LOG(INFO) << "new_definition: " << new_definition->DebugString();

  auto status2 = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest(
      "ChangePeerRegion", change_peer_request, change_peer_response);

  if (change_peer_response.has_error() && change_peer_response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout
        << "change peer region failed, error: "
        << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(change_peer_response.error().errcode())->name()
        << " " << change_peer_response.error().errmsg();
    return;
  }
  std::cout << "change peer success" << std::endl;
}

void SetUpTransferLeaderRegion(CLI::App &app) {
  auto opt = std::make_shared<TransferLeaderRegionOption>();
  auto *cmd =
      app.add_subcommand("TransferLeaderRegion", "Transfer leader region")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--store_id", opt->store_id, "Request parameter store id ")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id ")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunTransferLeaderRegion(*opt); });
}

void RunTransferLeaderRegion(TransferLeaderRegionOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  // query region
  dingodb::pb::coordinator::QueryRegionRequest query_request;
  dingodb::pb::coordinator::QueryRegionResponse query_response;

  query_request.set_region_id(opt.region_id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest(
      "QueryRegion", query_request, query_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  if (query_response.region().definition().peers_size() == 0) {
    DINGO_LOG(ERROR) << "region not found";
    return;
  }

  // validate peer not exists in region peers
  bool found = false;
  for (const auto &peer : query_response.region().definition().peers()) {
    if (peer.store_id() == opt.store_id) {
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

  transfer_leader_request.set_region_id(opt.region_id);
  transfer_leader_request.set_leader_store_id(opt.store_id);

  DINGO_LOG(INFO) << "transfer leader: region_id=" << opt.region_id << ", store_id=" << opt.store_id;

  auto status2 = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest(
      "TransferLeaderRegion", transfer_leader_request, transfer_leader_response);
  DINGO_LOG(INFO) << "SendRequest status=" << status2;
  DINGO_LOG(INFO) << transfer_leader_response.DebugString();

  if (transfer_leader_response.has_error() &&
      transfer_leader_response.error().errcode() != dingodb::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "transfer leader error: " << transfer_leader_response.error().DebugString();
  } else {
    DINGO_LOG(INFO) << "transfer leader success";
  }
}

void SetUpGetOrphanRegion(CLI::App &app) {
  auto opt = std::make_shared<GetOrphanRegionOption>();
  auto *cmd =
      app.add_subcommand("TransferLeaderRegion", "Transfer leader region")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--store_id", opt->store_id, "Request parameter store id ")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetOrphanRegion(*opt); });
}
void RunGetOrphanRegion(GetOrphanRegionOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetOrphanRegionRequest request;
  dingodb::pb::coordinator::GetOrphanRegionResponse response;
  request.set_store_id(opt.store_id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetOrphanRegion",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  DINGO_LOG(INFO) << "orphan_regions_size=" << response.orphan_regions_size();

  for (const auto &it : response.orphan_regions()) {
    DINGO_LOG(INFO) << "store_id=" << it.first << " region_id=" << it.second.id();
    DINGO_LOG(INFO) << it.second.DebugString();
  }

  for (const auto &it : response.orphan_regions()) {
    DINGO_LOG(INFO) << "store_id=" << it.first << " region_id=" << it.second.id()
                    << " region_name=" << it.second.region_definition().name();
  }
}

void SetUpScanRegions(CLI::App &app) {
  auto opt = std::make_shared<ScanRegionsOptions>();
  auto *cmd = app.add_subcommand("ScanRegions", "Scan region")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--key", opt->key, "Request parameter key")->required()->group("Coordinator Manager Commands");
  cmd->add_flag("--key_is_hex", opt->key_is_hex, "Request parameter key_is_hex")->group("Coordinator Manager Commands");
  cmd->add_option("--range_end", opt->range_end, "Request parameter key")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--limit", opt->limit, "Request parameter limit")
      ->default_val(50)
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunScanRegions(*opt); });
}
void RunScanRegions(ScanRegionsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::ScanRegionsRequest request;
  dingodb::pb::coordinator::ScanRegionsResponse response;

  if (opt.key_is_hex) {
    request.set_key(dingodb::Helper::HexToString(opt.key));
  } else {
    request.set_key(opt.key);
  }

  if (opt.key_is_hex) {
    request.set_range_end(dingodb::Helper::HexToString(opt.range_end));
  } else {
    request.set_range_end(opt.range_end);
  }

  request.set_limit(opt.limit);

  DINGO_LOG(INFO) << "ScanRegions request: " << request.DebugString();

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("ScanRegions", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetRangeRegionMap(CLI::App &app) {
  auto opt = std::make_shared<GetRangeRegionMapOption>();
  auto *cmd = app.add_subcommand("GetRangeRegionMap", "Get range region map")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetRangeRegionMap(*opt); });
}
void RunGetRangeRegionMap(GetRangeRegionMapOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetRangeRegionMapRequest request;
  dingodb::pb::coordinator::GetRangeRegionMapResponse response;

  DINGO_LOG(INFO) << "GetRangeRegionMap request: " << request.DebugString();

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetRangeRegionMap",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();

  DINGO_LOG(INFO) << "region count: " << response.range_regions_size();
}

void SetUpGetStoreOperation(CLI::App &app) {
  auto opt = std::make_shared<GetStoreOperationOption>();
  auto *cmd = app.add_subcommand("GetStoreOperation", "Get store operation ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--store_id", opt->store_id, "Request parameter store id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetStoreOperation(*opt); });
}

void RunGetStoreOperation(GetStoreOperationOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetStoreOperationRequest request;
  dingodb::pb::coordinator::GetStoreOperationResponse response;
  request.set_store_id(opt.store_id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetStoreOperation",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;

  for (const auto &it : response.store_operations()) {
    DINGO_LOG(INFO) << "store_id=" << it.id() << " store_operation=" << it.DebugString();
  }

  for (const auto &it : response.store_operations()) {
    DINGO_LOG(INFO) << "store_id=" << it.id() << " cmd_count=" << it.region_cmds_size();
  }
}

void SetUpGetTaskList(CLI::App &app) {
  auto opt = std::make_shared<GetTaskListOptions>();
  auto *cmd = app.add_subcommand("GetTaskList", "Get task list")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter task id");
  cmd->add_option("--start_id", opt->start_id, "Request parameter start id");
  cmd->add_flag("--include_archive", opt->include_archive, "Request parameter include history archive")
      ->default_val(false);
  cmd->add_option("--limit", opt->limit, "Request parameter limit ");
  cmd->callback([opt]() { RunGetTaskList(*opt); });
}

void RunGetTaskList(GetTaskListOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetTaskListRequest request;
  dingodb::pb::coordinator::GetTaskListResponse response;

  request.set_task_list_id(opt.id);
  request.set_include_archive(opt.include_archive);
  request.set_archive_limit(opt.limit);
  request.set_archive_start_id(opt.start_id);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetTaskList", request, response);
  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "get task list failed, error: "
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }

  for (const auto &task_list : response.task_lists()) {
    std::cout << "task_list: " << task_list.DebugString() << std::endl;
  }
}

void SetUpCleanTaskList(CLI::App &app) {
  auto opt = std::make_shared<CleanTaskListOption>();
  auto *cmd = app.add_subcommand("CleanTaskList", "Clean task list")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_option("--id", opt->id, "Request parameter task id, if you want to clean all task_list, set --id=0")
      ->required();
  cmd->callback([opt]() { RunCleanTaskList(*opt); });
}

void RunCleanTaskList(CleanTaskListOption const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::CleanTaskListRequest request;
  dingodb::pb::coordinator::CleanTaskListResponse response;

  request.set_task_list_id(opt.id);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CleanTaskList", request, response);

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "Clean task list failed , error:"
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "Clean task list success." << std::endl;
}

void SetUpUpdateRegionCmdStatus(CLI::App &app) {
  auto opt = std::make_shared<UpdateRegionCmdStatusOptions>();
  auto *cmd =
      app.add_subcommand("UpdateRegionCmdStatus", "Update region cmd status")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--task_list_id", opt->task_list_id, "Request parameter task_list_id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--region_cmd_id", opt->region_cmd_id, "Request parameter region cmd id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--status", opt->status, "Request parameter status, must be 1 [DONE] or 2 [FAIL]")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--errcode", opt->errcode, "Request parameter errcode ")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->errmsg, "Request parameter errmsg")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunUpdateRegionCmdStatus(*opt); });
}

void RunUpdateRegionCmdStatus(UpdateRegionCmdStatusOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::UpdateRegionCmdStatusRequest request;
  dingodb::pb::coordinator::UpdateRegionCmdStatusResponse response;
  request.set_task_list_id(opt.task_list_id);
  request.set_region_cmd_id(opt.region_cmd_id);
  request.set_status(static_cast<dingodb::pb::coordinator::RegionCmdStatus>(opt.status));
  request.mutable_error()->set_errcode(static_cast<dingodb::pb::error::Errno>(opt.errcode));
  request.mutable_error()->set_errmsg(opt.errmsg);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("UpdateRegionCmdStatus",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpCleanStoreOperation(CLI::App &app) {
  auto opt = std::make_shared<CleanStoreOperationOptions>();
  auto *cmd = app.add_subcommand("CleanStoreOperation", "Clean store operation")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter store id")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunCleanStoreOperation(*opt); });
}

void RunCleanStoreOperation(CleanStoreOperationOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::CleanStoreOperationRequest request;
  dingodb::pb::coordinator::CleanStoreOperationResponse response;

  request.set_store_id(opt.id);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("CleanStoreOperation",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpAddStoreOperation(CLI::App &app) {
  auto opt = std::make_shared<AddStoreOperationOptions>();
  auto *cmd = app.add_subcommand("AddStoreOperation", "Clean store operation")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter store id")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunAddStoreOperation(*opt); });
}

void RunAddStoreOperation(AddStoreOperationOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  dingodb::pb::coordinator::AddStoreOperationRequest request;
  dingodb::pb::coordinator::AddStoreOperationResponse response;

  request.mutable_store_operation()->set_id(opt.id);
  auto *region_cmd = request.mutable_store_operation()->add_region_cmds();
  region_cmd->set_region_id(opt.region_id);
  region_cmd->set_region_cmd_type(::dingodb::pb::coordinator::RegionCmdType::CMD_NONE);
  region_cmd->set_create_timestamp(butil::gettimeofday_ms());

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("AddStoreOperation",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpRemoveStoreOperation(CLI::App &app) {
  auto opt = std::make_shared<RemoveStoreOperationOptions>();
  auto *cmd =
      app.add_subcommand("RemoveStoreOperation", "Remove store operation")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter store id")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--region_cmd_id", opt->region_cmd_id, "Request parameter region cmd id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunRemoveStoreOperation(*opt); });
}

void RunRemoveStoreOperation(RemoveStoreOperationOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  dingodb::pb::coordinator::RemoveStoreOperationRequest request;
  dingodb::pb::coordinator::RemoveStoreOperationResponse response;

  request.set_store_id(opt.id);
  request.set_region_cmd_id(opt.region_cmd_id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("RemoveStoreOperation",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetRegionCmd(CLI::App &app) {
  auto opt = std::make_shared<GetRegionCmdOptions>();
  auto *cmd = app.add_subcommand("GetRegionCmd", "Get region cmd")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->store_id, "Request parameter store id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--start_region_cmd_id", opt->start_region_cmd_id, "Request parameter start region cmd id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--end_region_cmd_id", opt->end_region_cmd_id, "Request parameter end region cmd id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetRegionCmd(*opt); });
}

void RunGetRegionCmd(GetRegionCmdOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  dingodb::pb::coordinator::GetRegionCmdRequest request;
  dingodb::pb::coordinator::GetRegionCmdResponse response;

  request.set_store_id(opt.store_id);
  request.set_start_region_cmd_id(opt.start_region_cmd_id);
  request.set_end_region_cmd_id(opt.end_region_cmd_id);

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetRegionCmd", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetStoreMetricsn(CLI::App &app) {
  auto opt = std::make_shared<GetStoreMetricsOptions>();
  auto *cmd = app.add_subcommand("GetStoreMetrics", "Get store metrics")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter store id")->required()->group("Coordinator Manager Commands");
  cmd->add_option("--region_id", opt->region_id, "Request parameter region id")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetStoreMetrics(*opt); });
}

void RunGetStoreMetrics(GetStoreMetricsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  dingodb::pb::coordinator::GetStoreMetricsRequest request;
  dingodb::pb::coordinator::GetStoreMetricsResponse response;

  request.set_store_id(opt.id);
  request.set_region_id(opt.region_id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetStoreMetrics",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  for (const auto &it : response.store_metrics()) {
    DINGO_LOG(INFO) << it.store_own_metrics().DebugString();
    for (const auto &it2 : it.region_metrics_map()) {
      DINGO_LOG(INFO) << it2.second.DebugString();
    }
  }

  for (const auto &it : response.store_metrics()) {
    std::string region_ids;
    for (const auto &it2 : it.region_metrics_map()) {
      region_ids += fmt::format("{},", it2.first);
    }
    DINGO_LOG(INFO) << fmt::format("store_id={},region_count={},region_ids={}", it.id(), it.region_metrics_map_size(),
                                   region_ids);
  }
}

void SetUpDeleteStoreMetrics(CLI::App &app) {
  auto opt = std::make_shared<DeleteStoreMetricsOptions>();
  auto *cmd = app.add_subcommand("DeleteStoreMetrics", "Delete store metrics")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter store id")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunDeleteStoreMetrics(*opt); });
}

void RunDeleteStoreMetrics(DeleteStoreMetricsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  dingodb::pb::coordinator::DeleteStoreMetricsRequest request;
  dingodb::pb::coordinator::DeleteStoreMetricsResponse response;

  request.set_store_id(opt.id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("DeleteStoreMetrics",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetRegionMetrics(CLI::App &app) {
  auto opt = std::make_shared<GetRegionMetricsOptions>();
  auto *cmd = app.add_subcommand("GetRegionMetrics", "Get region metrics")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter region id")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunGetRegionMetrics(*opt); });
}

void RunGetRegionMetrics(GetRegionMetricsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }

  dingodb::pb::coordinator::GetRegionMetricsRequest request;
  dingodb::pb::coordinator::GetRegionMetricsResponse response;

  request.set_region_id(opt.id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetRegionMetrics",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  for (const auto &it : response.region_metrics()) {
    DINGO_LOG(INFO) << it.DebugString();
  }
  DINGO_LOG(INFO) << "region_count=" << response.region_metrics_size();
}

void SetUpDeleteRegionMetrics(CLI::App &app) {
  auto opt = std::make_shared<DeleteRegionMetricsOptions>();
  auto *cmd = app.add_subcommand("DeleteRegionMetrics", "Delete region metrics")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--id", opt->id, "Request parameter region id")->required()->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunDeleteRegionMetrics(*opt); });
}

void RunDeleteRegionMetrics(DeleteRegionMetricsOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::DeleteRegionMetricsRequest request;
  dingodb::pb::coordinator::DeleteRegionMetricsResponse response;

  request.set_region_id(opt.id);
  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("DeleteRegionMetrics",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpUpdateGCSafePoint(CLI::App &app) {
  auto opt = std::make_shared<UpdateGCSafePointOptions>();
  auto *cmd = app.add_subcommand("UpdateGCSafePoint", "Update GC safepoint ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--safe_point", opt->safe_point, "Request parameter safe point")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--gc_flag", opt->gc_flag, "Request parameter gc_flag must be start|stop")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant id")->group("Coordinator Manager Commands");
  cmd->add_option("--safe_point2", opt->safe_point2, "Request parameter safe_point2 ")
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunUpdateGCSafePoint(*opt); });
}

void RunUpdateGCSafePoint(UpdateGCSafePointOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::UpdateGCSafePointRequest request;
  dingodb::pb::coordinator::UpdateGCSafePointResponse response;

  if (opt.gc_flag == "start") {
    request.set_gc_flag(
        ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_START);
  } else if (opt.gc_flag == "stop") {
    request.set_gc_flag(
        ::dingodb::pb::coordinator::UpdateGCSafePointRequest_GcFlagType::UpdateGCSafePointRequest_GcFlagType_GC_STOP);
  } else {
    DINGO_LOG(ERROR) << "gc_flag is invalid, must be start or stop";
    return;
  }

  request.set_safe_point(opt.safe_point);

  if (opt.tenant_id > 0) {
    request.mutable_tenant_safe_points()->insert({opt.tenant_id, opt.safe_point2});
  }

  DINGO_LOG(INFO) << "UpdateGCSafePoint request: " << request.DebugString();

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("UpdateGCSafePoint",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SetUpGetGCSafePoint(CLI::App &app) {
  auto opt = std::make_shared<GetGCSafePointOptions>();
  auto *cmd = app.add_subcommand("GetGCSafePoint", "Get GC safepoint ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list");
  cmd->add_flag("--get_all_tenant", opt->get_all_tenant, "Request parameter get_all_tenant")->default_val(false);
  cmd->add_option("--tenant_id", opt->tenant_id, "Request parameter tenant_id");

  cmd->callback([opt]() { RunGetGCSafePoint(*opt); });
}

void RunGetGCSafePoint(GetGCSafePointOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::GetGCSafePointRequest request;
  dingodb::pb::coordinator::GetGCSafePointResponse response;

  if (opt.tenant_id > 0) {
    request.add_tenant_ids(opt.tenant_id);
  }

  request.set_get_all_tenant(opt.get_all_tenant);

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("GetGCSafePoint", request,
                                                                                              response);

  if (response.has_error() && response.error().errcode() != dingodb::pb::error::Errno::OK) {
    std::cout << "get gc safe point failed , error:"
              << dingodb::pb::error::Errno_descriptor()->FindValueByNumber(response.error().errcode())->name() << " "
              << response.error().errmsg();
    return;
  }
  std::cout << "gc_safe_point: " << response.safe_point() << " , gc_stop: " << response.gc_stop() << std::endl;
}

void SetUpBalanceLeader(CLI::App &app) {
  auto opt = std::make_shared<BalanceLeaderOptions>();
  auto *cmd = app.add_subcommand("BalanceLeader", "Balance leader ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_option("--dryrun", opt->dryrun, "Request parameter safe dryrun")
      ->default_val(true)
      ->group("Coordinator Manager Commands");
  cmd->add_option("--type", opt->store_type, "Request parameter store_type")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunBalanceLeader(*opt); });
}

void RunBalanceLeader(BalanceLeaderOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << opt.coor_url;
    exit(-1);
  }
  dingodb::pb::coordinator::BalanceLeaderRequest request;
  dingodb::pb::coordinator::BalanceLeaderResponse response;

  request.set_dryrun(opt.dryrun);
  request.set_store_type(dingodb::pb::common::StoreType(opt.store_type));

  auto status =
      CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("BalanceLeader", request, response);
  if (!status.ok()) {
    DINGO_LOG(INFO) << "SendRequest status=" << status;
  } else {
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SetUpUpdateForceReadOnly(CLI::App &app) {
  auto opt = std::make_shared<UpdateForceReadOnlyOptions>();
  auto *cmd =
      app.add_subcommand("UpdateForceReadOnly", "Update force read only ")->group("Coordinator Manager Commands");
  cmd->add_option("--coor_url", opt->coor_url, "Coordinator url, default:file://./coor_list")
      ->group("Coordinator Manager Commands");
  cmd->add_flag("--force_read_only", opt->force_read_only, "Request parameter safe dryrun")
      ->default_val(false)
      ->group("Coordinator Manager Commands");
  cmd->add_option("--force_read_only_reason", opt->force_read_only_reason,
                  "Request parameter storforce_read_only_reasone_type")
      ->required()
      ->group("Coordinator Manager Commands");
  cmd->callback([opt]() { RunUpdateForceReadOnly(*opt); });
}

void RunUpdateForceReadOnly(UpdateForceReadOnlyOptions const &opt) {
  if (Helper::SetUp(opt.coor_url) < 0) {
    exit(-1);
  }
  dingodb::pb::coordinator::ConfigCoordinatorRequest request;
  dingodb::pb::coordinator::ConfigCoordinatorResponse response;

  request.set_set_force_read_only(true);
  request.set_is_force_read_only(opt.force_read_only);

  request.set_force_read_only_reason(opt.force_read_only_reason);

  DINGO_LOG(INFO) << "Try to set_force_read_only to " << opt.force_read_only
                  << ", reason: " << opt.force_read_only_reason;

  auto status = CoordinatorInteraction::GetInstance().GetCoorinatorInteraction()->SendRequest("ConfigCoordinator",
                                                                                              request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

// refactor
}  // namespace client_v2

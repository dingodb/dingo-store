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

#include "engine/raft_kv_engine.h"


#include "butil/endpoint.h"

#include "common/helper.h"
#include "config/config_manager.h"
#include "raft/state_machine.h"
#include "server/server.h"


namespace dingodb {


RaftKvEngine::RaftKvEngine(Engine* engine)
  : engine_(engine),
    raft_node_manager_(std::move(std::make_unique<RaftNodeManager>())) {
}

RaftKvEngine::~RaftKvEngine() {
}

bool RaftKvEngine::Init() {
  return true;
}

std::string RaftKvEngine::GetName() {
  return "RAFT_KV_ENGINE";
}

uint32_t RaftKvEngine::GetID() {
  return Engine::Type::RAFT_KV_ENGINE;
}

butil::EndPoint getRaftEndPoint(const std::string host, int port) {
  butil::ip_t ip;
  if (host.empty()) {
    ip = butil::IP_ANY;
  } else {
    if (Helper::IsIp(host)) {
      butil::str2ip(host.c_str(), &ip);
    } else {
      butil::hostname2ip(host.c_str(), &ip);
    }
  }

  return butil::EndPoint(ip, port);
}

// format peers like 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
std::string formatPeers(const google::protobuf::RepeatedPtrField<std::__cxx11::string>& peers) {
  std::string init_conf;
  for (int i = 0; i < peers.size(); ++i) {
    init_conf += peers.Get(i);
    if (i + 1 < peers.size()) {
      init_conf += ",";
    }
  }
  return init_conf;
}

int RaftKvEngine::AddRegion(uint64_t region_id, const dingodb::pb::common::Region& region) {
  std::shared_ptr<RaftNode> node = std::make_shared<RaftNode>(region_id,
                                                              braft::PeerId(Server::GetInstance()->get_raft_endpoint()),
                                                              new StoreStateMachine(engine_));
  if (node->Init(formatPeers(region.peers())) != 0) {
    node->Destroy();
    return -1;
  }

  raft_node_manager_->AddNode(region_id, node);
  return 0;
}

int RaftKvEngine::DestroyRegion(uint64_t region_id) {
  return 0;
}

std::shared_ptr<std::string> RaftKvEngine::KvGet(std::shared_ptr<Context> ctx, const std::string& key) {
  return nullptr;
}

int RaftKvEngine::KvPut(std::shared_ptr<Context> ctx, const std::string& key, const std::string& value) {
  dingodb::pb::raft::RaftCmdRequest raft_cmd;
  auto request = raft_cmd.add_requests();
  request->set_cmd_type(dingodb::pb::raft::CmdType::PUT);
  dingodb::pb::raft::PutRequest put_request;
  auto kv = put_request.add_kvs();
  kv->set_key(key);
  kv->set_value(value);
  request->set_allocated_put(&put_request);

  raft_node_manager_->GetNode(ctx->get_region_id())->Commit(ctx, raft_cmd);

  return 0;
}

} // namespace dingodb
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


namespace dingodb {


RaftKvEngine::RaftKvEngine(Engine* engine)
  : engine_(engine),
    raft_node_manager_(std::make_unique<RaftNodeManager>()) {
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

braft::PeerId HostPortToPeerId(const std::string host, int port) {
  if (port == 0) {
    port = 9800;
  }

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

  return braft::PeerId(butil::EndPoint(ip, port));
}

int RaftKvEngine::AddRegion(uint64_t region_id, const dingodb::pb::common::RegionInfo& region) {
  auto config = ConfigManager::GetInstance()->GetConfig("store");
  braft::PeerId peerId = HostPortToPeerId(config->GetString("raft.host"),
                                          config->GetInt("raft.port"));

  std::shared_ptr<RaftNode> node = std::make_shared<RaftNode>(region_id, peerId, new StoreStateMachine(engine_));
  if (node->Init() != 0) {
    node->Destroy();
    return -1;
  }
  raft_node_manager_->AddNode(region_id, node);
  return 0;
}

int RaftKvEngine::DestroyRegion(uint64_t region_id) {
  return 0;
}

Slice RaftKvEngine::KvGet(const Slice& key) {
  return Slice();
}

int RaftKvEngine::KvPut(const Slice& key, const Slice& value) {
  return 0;
}

} // namespace dingodb
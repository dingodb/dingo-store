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

#include "engine/raft_meta_engine.h"

#include <cstdint>
#include <memory>

#include "braft/raft.h"
#include "butil/endpoint.h"
#include "common/helper.h"
#include "common/synchronization.h"
#include "config/config_manager.h"
#include "coordinator/coordinator_control.h"
#include "engine/raft_kv_engine.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "raft/meta_state_machine.h"
#include "raft/state_machine.h"
#include "server/server.h"

namespace dingodb {

// Recover raft node from coordinator meta data.
// Invoke when server starting.
// Coordinator do this in CoordinatorControl

RaftMetaEngine::RaftMetaEngine(std::shared_ptr<Engine> engine, std::shared_ptr<MetaControl> meta_control)
    : meta_control_(meta_control), RaftKvEngine(engine) {}

RaftMetaEngine::~RaftMetaEngine() = default;

bool RaftMetaEngine::Init(std::shared_ptr<Config> config) {
  LOG(INFO) << "Now=> Int Raft Kv Engine with config[" << config->ToString();
  return true;
}

bool RaftMetaEngine::Recover() { return true; }

pb::error::Errno RaftMetaEngine::InitCoordinatorRegion(std::shared_ptr<Context> ctx,
                                                       const std::shared_ptr<pb::common::Region> region) {
  LOG(INFO) << "RaftkvEngine add region, region_id " << region->id();

  // construct MetaStatMachine here
  braft::StateMachine* state_machine = new MetaStateMachine(engine_, meta_control_);

  std::shared_ptr<RaftNode> node = std::make_shared<RaftNode>(
      ctx->ClusterRole(), region->id(), braft::PeerId(Server::GetInstance()->RaftEndpoint()), state_machine);

  if (node->Init(Helper::FormatPeers(Helper::ExtractLocations(region->peers()))) != 0) {
    node->Destroy();
    return pb::error::ERAFT_INIT;
  }

  raft_node_manager_->AddNode(region->id(), node);

  // set raft_node to coordinator_control
  meta_control_->SetRaftNode(node);

  return pb::error::OK;
}

pb::error::Errno RaftMetaEngine::MetaPut(std::shared_ptr<Context> ctx,
                                         const pb::coordinator_internal::MetaIncrement& meta) {
  // use MetaIncrement to construct RaftCmdRequest
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
  auto* request = raft_cmd->add_requests();
  request->set_cmd_type(pb::raft::CmdType::META_WRITE);

  pb::raft::RaftMetaRequest* meta_request = request->mutable_meta_req();
  auto* meta_increment_request = meta_request->mutable_meta_increment();
  meta_increment_request->CopyFrom(meta);

  // call braft node->apply()
  auto node = raft_node_manager_->GetNode(ctx->region_id());
  if (node == nullptr) {
    LOG(ERROR) << "Not found raft node " << ctx->region_id();
    return pb::error::ERAFT_NOTNODE;
  }
  return node->Commit(ctx, raft_cmd);
}

}  // namespace dingodb

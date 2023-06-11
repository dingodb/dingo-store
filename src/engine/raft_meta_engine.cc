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
#include "common/logging.h"
#include "common/synchronization.h"
#include "config/config_manager.h"
#include "coordinator/coordinator_control.h"
#include "engine/raft_kv_engine.h"
#include "engine/raw_engine.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/raft.pb.h"
#include "raft/meta_state_machine.h"
#include "raft/store_state_machine.h"
#include "server/server.h"

namespace dingodb {

// Recover raft node from coordinator meta data.
// Invoke when server starting.
// Coordinator do this in CoordinatorControl

RaftMetaEngine::RaftMetaEngine(std::shared_ptr<RawEngine> engine) : RaftKvEngine(engine) {}

RaftMetaEngine::~RaftMetaEngine() = default;

bool RaftMetaEngine::Init(std::shared_ptr<Config> config) {
  DINGO_LOG(INFO) << "Now=> Int Raft Kv Engine with config[" << config->ToString() + "]";
  return true;
}

bool RaftMetaEngine::Recover() { return true; }

butil::Status RaftMetaEngine::InitCoordinatorRegion(const std::shared_ptr<pb::common::RegionDefinition> region,
                                                    const std::shared_ptr<MetaControl>& meta_control,
                                                    bool is_volatile) {
  DINGO_LOG(INFO) << "RaftkvEngine add region, region_id " << region->id();

  // construct MetaStatMachine here
  braft::StateMachine* state_machine = new MetaStateMachine(meta_control, is_volatile);

  std::string const meta_raft_name = fmt::format("{}-{}", region->name(), region->id());
  std::shared_ptr<RaftNode> const node = std::make_shared<RaftNode>(
      region->id(), meta_raft_name, braft::PeerId(Server::GetInstance()->RaftEndpoint()), state_machine);

  if (node->Init(Helper::FormatPeers(Helper::ExtractLocations(region->peers())),
                 ConfigManager::GetInstance()->GetConfig(Server::GetInstance()->GetRole())) != 0) {
    node->Destroy();
    return butil::Status(pb::error::ERAFT_INIT, "Raft init failed");
  }

  raft_node_manager_->AddNode(region->id(), node);

  // set raft_node to coordinator_control
  meta_control->SetRaftNode(node);

  return butil::Status();
}

butil::Status RaftMetaEngine::MetaPut(std::shared_ptr<Context> ctx,
                                      const pb::coordinator_internal::MetaIncrement& meta) {
  // use MetaIncrement to construct RaftCmdRequest
  std::shared_ptr<pb::raft::RaftCmdRequest> const raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
  auto* request = raft_cmd->add_requests();
  request->set_cmd_type(pb::raft::CmdType::META_WRITE);

  pb::raft::RaftMetaRequest* meta_request = request->mutable_meta_req();
  auto* meta_increment_request = meta_request->mutable_meta_increment();
  meta_increment_request->CopyFrom(meta);

  // call braft node->apply()
  auto node = raft_node_manager_->GetNode(ctx->RegionId());
  if (node == nullptr) {
    DINGO_LOG(ERROR) << "Not found raft node on region " << ctx->RegionId();
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }
  return node->Commit(ctx, raft_cmd);
}

}  // namespace dingodb

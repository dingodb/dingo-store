
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

#include "server/region_control_service.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/region_control.pb.h"
#include "server/server.h"

using dingodb::pb::error::Errno;

namespace dingodb {

void RegionControlServiceImpl::AddRegion(google::protobuf::RpcController* controller,
                                         const dingodb::pb::region_control::AddRegionRequest* request,
                                         dingodb::pb::region_control::AddRegionResponse* response,
                                         google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "AddRegion request: " << request->ShortDebugString();

  auto region_controller = Server::GetInstance()->GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region().id());
  command->set_region_cmd_type(pb::coordinator::CMD_CREATE);
  command->mutable_create_request()->mutable_region_definition()->CopyFrom(request->region());

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, done), command);
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void RegionControlServiceImpl::ChangeRegion(google::protobuf::RpcController* controller,
                                            const pb::region_control::ChangeRegionRequest* request,
                                            pb::region_control::ChangeRegionResponse* response,
                                            google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "ChangeRegion request: " << request->ShortDebugString();

  auto region_controller = Server::GetInstance()->GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region().id());
  command->set_region_cmd_type(pb::coordinator::CMD_CHANGE_PEER);
  command->mutable_change_peer_request()->mutable_region_definition()->CopyFrom(request->region());

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, done), command);
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void RegionControlServiceImpl::DestroyRegion(google::protobuf::RpcController* controller,
                                             const dingodb::pb::region_control::DestroyRegionRequest* request,
                                             dingodb::pb::region_control::DestroyRegionResponse* response,
                                             google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "DestroyRegion request: " << request->ShortDebugString();

  auto region_controller = Server::GetInstance()->GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region_id());
  command->set_region_cmd_type(pb::coordinator::CMD_DELETE);
  command->mutable_delete_request()->set_region_id(request->region_id());

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, done), command);
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void RegionControlServiceImpl::Snapshot(google::protobuf::RpcController* controller,
                                        const pb::region_control::SnapshotRequest* request,
                                        pb::region_control::SnapshotResponse* response,
                                        google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "Snapshot request: " << request->ShortDebugString();

  auto region_controller = Server::GetInstance()->GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region_id());
  command->set_region_cmd_type(pb::coordinator::CMD_SNAPSHOT);

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, done), command);
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void RegionControlServiceImpl::TransferLeader(google::protobuf::RpcController* controller,
                                              const pb::region_control::TransferLeaderRequest* request,
                                              pb::region_control::TransferLeaderResponse* response,
                                              google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "TransferLeader request: " << request->ShortDebugString();

  auto engine = Server::GetInstance()->GetEngine();
  if (engine->GetID() == pb::common::ENG_RAFT_STORE) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(engine);
    auto status = raft_kv_engine->TransferLeader(request->region_id(), request->peer());
    if (!status.ok()) {
      auto* mut_err = response->mutable_error();
      mut_err->set_errcode(static_cast<Errno>(status.error_code()));
      mut_err->set_errmsg(status.error_str());
    }
  }
}

void RegionControlServiceImpl::Debug(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::region_control::DebugRequest* request,
                                     ::dingodb::pb::region_control::DebugResponse* response,
                                     ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "Debug request: " << request->ShortDebugString();

  if (request->type() == pb::region_control::DebugType::STORE_REGION_META_STAT) {
    auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
    auto regions = store_region_meta->GetAllRegion();

    std::map<std::string, int32_t> state_counts;
    for (auto& region : regions) {
      std::string name = pb::common::StoreRegionState_Name(region->State());
      if (state_counts.find(name) == state_counts.end()) {
        state_counts[name] = 0;
      }
      ++state_counts[name];
    }

    for (auto [name, count] : state_counts) {
      response->mutable_region_meta_stat()->mutable_state_counts()->insert({name, count});
    }

  } else if (request->type() == pb::region_control::DebugType::STORE_REGION_META_DETAILS) {
    auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
    std::vector<store::RegionPtr> regions;
    if (request->region_ids().empty()) {
      regions = store_region_meta->GetAllRegion();
    } else {
      for (auto region_id : request->region_ids()) {
        auto region = store_region_meta->GetRegion(region_id);
        if (region != nullptr) {
          regions.push_back(region);
        }
      }
    }

    for (auto& region : regions) {
      response->mutable_region_meta_details()->add_regions()->CopyFrom(region->InnerRegion());
    }

  } else if (request->type() == pb::region_control::DebugType::STORE_REGION_CONTROL_COMMAND) {
    std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> commands;
    if (request->region_ids().empty()) {
      commands = Server::GetInstance()->GetRegionCommandManager()->GetAllCommand();
    } else {
      for (auto region_id : request->region_ids()) {
        auto region_commands = Server::GetInstance()->GetRegionCommandManager()->GetCommands(region_id);
        if (!region_commands.empty()) {
          commands.insert(commands.end(), region_commands.begin(), region_commands.end());
        }
      }
    }

    for (auto& command : commands) {
      response->mutable_region_control_command()->add_region_cmds()->CopyFrom(*command);
    }

  } else if (request->type() == pb::region_control::DebugType::STORE_RAFT_META) {
    auto store_raft_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta();

    std::vector<StoreRaftMeta::RaftMetaPtr> raft_metas;
    if (request->region_ids().empty()) {
      raft_metas = store_raft_meta->GetAllRaftMeta();
    } else {
      for (auto region_id : request->region_ids()) {
        auto raft_meta = store_raft_meta->GetRaftMeta(region_id);
        if (raft_meta != nullptr) {
          raft_metas.push_back(raft_meta);
        }
      }
    }

    for (auto& raft_meta : raft_metas) {
      response->mutable_raft_meta()->add_raft_metas()->CopyFrom(*raft_meta);
    }

  } else if (request->type() == pb::region_control::DebugType::STORE_REGION_EXECUTOR) {
    auto region_ids = Server::GetInstance()->GetRegionController()->GetAllRegion();

    for (auto region_id : region_ids) {
      response->mutable_region_executor()->add_region_ids(region_id);
    }

  } else if (request->type() == pb::region_control::DebugType::STORE_REGION_METRICS) {
    auto store_region_metrics = Server::GetInstance()->GetStoreMetricsManager()->GetStoreRegionMetrics();

    std::vector<store::RegionMetricsPtr> region_metricses;
    if (request->region_ids().empty()) {
      region_metricses = store_region_metrics->GetAllMetrics();
    } else {
      for (auto region_id : request->region_ids()) {
        auto metrics = store_region_metrics->GetMetrics(region_id);
        if (metrics != nullptr) {
          region_metricses.push_back(metrics);
        }
      }
    }

    for (auto& region_metrics : region_metricses) {
      response->mutable_region_metrics()->add_region_metricses()->CopyFrom(region_metrics->InnerRegionMetrics());
    }
  }
}

}  // namespace dingodb

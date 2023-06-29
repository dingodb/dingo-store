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

#include "server/index_service.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/constant.h"
#include "common/context.h"
#include "common/failpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "server/service_helper.h"

using dingodb::pb::error::Errno;

namespace dingodb {

IndexServiceImpl::IndexServiceImpl() = default;

void IndexServiceImpl::AddRegion(google::protobuf::RpcController* controller,
                                 const dingodb::pb::index::AddRegionRequest* request,
                                 dingodb::pb::index::AddRegionResponse* response, google::protobuf::Closure* done) {
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

void IndexServiceImpl::ChangeRegion(google::protobuf::RpcController* controller,
                                    const pb::index::ChangeRegionRequest* request,
                                    pb::index::ChangeRegionResponse* response, google::protobuf::Closure* done) {
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

void IndexServiceImpl::DestroyRegion(google::protobuf::RpcController* controller,
                                     const dingodb::pb::index::DestroyRegionRequest* request,
                                     dingodb::pb::index::DestroyRegionResponse* response,
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

void IndexServiceImpl::Snapshot(google::protobuf::RpcController* controller, const pb::index::SnapshotRequest* request,
                                pb::index::SnapshotResponse* response, google::protobuf::Closure* done) {
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

void IndexServiceImpl::TransferLeader(google::protobuf::RpcController* controller,
                                      const pb::index::TransferLeaderRequest* request,
                                      pb::index::TransferLeaderResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "TransferLeader request: " << request->ShortDebugString();

  auto engine = Server::GetInstance()->GetEngine();
  if (engine->GetID() == pb::common::ENG_RAFT_STORE) {
    auto raft_kv_engine = std::dynamic_pointer_cast<RaftKvEngine>(engine);
    auto status = raft_kv_engine->TransferLeader(request->region_id(), request->peer());
    if (!status.ok()) {
      auto* mut_err = response->mutable_error();
      mut_err->set_errcode(static_cast<Errno>(status.error_code()));
      mut_err->set_errmsg(status.error_str());
    }
  }
}

void IndexServiceImpl::Debug(google::protobuf::RpcController* controller,
                             const ::dingodb::pb::index::DebugRequest* request,
                             ::dingodb::pb::index::DebugResponse* response, ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "Debug request: " << request->ShortDebugString();

  if (request->type() == pb::index::DebugType::STORE_REGION_META_STAT) {
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

  } else if (request->type() == pb::index::DebugType::STORE_REGION_META_DETAILS) {
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

  } else if (request->type() == pb::index::DebugType::STORE_REGION_CONTROL_COMMAND) {
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

  } else if (request->type() == pb::index::DebugType::STORE_RAFT_META) {
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

  } else if (request->type() == pb::index::DebugType::STORE_REGION_EXECUTOR) {
    auto region_ids = Server::GetInstance()->GetRegionController()->GetAllRegion();

    for (auto region_id : region_ids) {
      response->mutable_region_executor()->add_region_ids(region_id);
    }

  } else if (request->type() == pb::index::DebugType::STORE_REGION_METRICS) {
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

butil::Status ValidateVectorSearchRequest(const dingodb::pb::index::VectorSearchRequest* request) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vector().vector().values_size() == 0) {
    return butil::Status(pb::error::EVECTOR_EMPTY, "Vector is empty");
  }

  return ServiceHelper::ValidateIndexRegion(request->region_id());
}

// vector
void IndexServiceImpl::VectorSearch(google::protobuf::RpcController* controller,
                                    const dingodb::pb::index::VectorSearchRequest* request,
                                    dingodb::pb::index::VectorSearchResponse* response,
                                    google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorSearch request: " << request->ShortDebugString();

  butil::Status status = ValidateVectorSearchRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  auto* mut_request = const_cast<dingodb::pb::index::VectorSearchRequest*>(request);

  std::vector<pb::common::VectorWithDistance> vector_results;
  status = storage_->VectorSearch(ctx, request->vector(), request->parameter(), vector_results);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    return;
  }

  for (auto& vector_result : vector_results) {
    response->add_results()->CopyFrom(vector_result);
  }
}

butil::Status ValidateVectorAddRequest(const dingodb::pb::index::VectorAddRequest* request) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->vectors().empty()) {
    return butil::Status(pb::error::EVECTOR_EMPTY, "Vector quantity is empty");
  }

  for (const auto& vector : request->vectors()) {
    if (vector.vector().values().empty()) {
      return butil::Status(pb::error::EVECTOR_EMPTY, "Vector is empty");
    }
  }

  return ServiceHelper::ValidateIndexRegion(request->region_id());
}

void IndexServiceImpl::VectorAdd(google::protobuf::RpcController* controller,
                                 const dingodb::pb::index::VectorAddRequest* request,
                                 dingodb::pb::index::VectorAddResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorAdd request: " << request->ShortDebugString();

  butil::Status status = ValidateVectorAddRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::VectorWithId> vectors;
  for (const auto& vector : request->vectors()) {
    vectors.push_back(vector);
  }

  status = storage_->VectorAdd(ctx, vectors);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

butil::Status ValidateVectorDeleteRequest(const dingodb::pb::index::VectorDeleteRequest* request) {
  if (request->region_id() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Param region_id is error");
  }

  if (request->ids().empty()) {
    return butil::Status(pb::error::EVECTOR_EMPTY, "Vector id quantity is empty");
  }

  return ServiceHelper::ValidateIndexRegion(request->region_id());
}

void IndexServiceImpl::VectorDelete(google::protobuf::RpcController* controller,
                                    const dingodb::pb::index::VectorDeleteRequest* request,
                                    dingodb::pb::index::VectorDeleteResponse* response,
                                    google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "VectorDelete request: " << request->ShortDebugString();

  butil::Status status = ValidateVectorDeleteRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<uint64_t> ids;
  for (auto id : request->ids()) {
    ids.push_back(id);
  }

  status = storage_->VectorDelete(ctx, ids);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg("Not leader, please redirect leader.");
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

void IndexServiceImpl::SetStorage(std::shared_ptr<Storage> storage) { storage_ = storage; }

}  // namespace dingodb

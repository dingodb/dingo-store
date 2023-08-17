
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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "butil/endpoint.h"
#include "butil/status.h"
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
#include "server/file_service.h"
#include "server/server.h"
#include "vector/vector_index_snapshot.h"

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

void RegionControlServiceImpl::SnapshotVectorIndex(google::protobuf::RpcController* controller,
                                                   const pb::region_control::SnapshotVectorIndexRequest* request,
                                                   pb::region_control::SnapshotVectorIndexResponse* response,
                                                   google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "SnapshotVectorIndex request: " << request->ShortDebugString();

  auto region_controller = Server::GetInstance()->GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->vector_index_id());
  command->set_region_cmd_type(pb::coordinator::CMD_SNAPSHOT_VECTOR_INDEX);
  command->mutable_snapshot_vector_index_request()->set_vector_index_id(request->vector_index_id());

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, done), command);
  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

void RegionControlServiceImpl::TriggerVectorIndexSnapshot(
    google::protobuf::RpcController* controller, const pb::region_control::TriggerVectorIndexSnapshotRequest* request,
    pb::region_control::TriggerVectorIndexSnapshotResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "TriggerVectorIndexSnapshot request: " << request->ShortDebugString();

  butil::EndPoint endpoint;
  butil::str2endpoint(request->location().host().c_str(), request->location().port(), &endpoint);

  butil::Status status;
  if (Helper::ToUpper(request->type()) == "INSTALL") {
    status = VectorIndexSnapshotManager::LaunchInstallSnapshot(endpoint, request->vector_index_id());
  } else if (Helper::ToUpper(request->type()) == "PULL") {
    status = VectorIndexSnapshotManager::LaunchPullSnapshot(endpoint, request->vector_index_id());
  } else {
    auto* error = response->mutable_error();
    error->set_errcode(pb::error::EILLEGAL_PARAMTETERS);
    error->set_errmsg("Param type is error");
  }

  if (!status.ok()) {
    auto* mut_err = response->mutable_error();
    mut_err->set_errcode(static_cast<Errno>(status.error_code()));
    mut_err->set_errmsg(status.error_str());
  }
}

static pb::common::RegionMetrics GetRegionActualMetrics(uint64_t region_id) {
  pb::common::RegionMetrics region_metrics;
  region_metrics.set_id(region_id);
  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(region_id);
  if (region == nullptr) {
    return region_metrics;
  }

  auto raw_engine = Server::GetInstance()->GetRawEngine();

  int32_t size = 0;
  int32_t key_count = 0;
  std::string min_key, max_key;
  auto ranges = region->PhysicsRange();
  for (int i = 0; i < ranges.size(); ++i) {
    auto range = ranges[i];
    IteratorOptions options;
    options.upper_bound = range.end_key();
    auto iter = raw_engine->NewIterator(Constant::kStoreDataCF, options);

    for (iter->Seek(range.start_key()); iter->Valid(); iter->Next()) {
      size += iter->Key().size() + iter->Value().size();
      if (i == 0) {
        ++key_count;
        if (min_key.empty()) {
          min_key = iter->Key();
        }
        max_key = iter->Key();
      }
    }
  }

  region_metrics.set_min_key(min_key);
  region_metrics.set_max_key(max_key);
  region_metrics.set_region_size(size);
  region_metrics.set_row_count(key_count);

  return region_metrics;
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
  } else if (request->type() == pb::region_control::DebugType::STORE_FILE_READER) {
    auto reader_ids = FileServiceReaderManager::GetInstance().GetAllReaderId();

    response->mutable_file_reader()->set_count(reader_ids.size());
    for (auto reader_id : reader_ids) {
      response->mutable_file_reader()->add_reader_ids(reader_id);
    }
  } else if (request->type() == pb::region_control::DebugType::STORE_REGION_ACTUAL_METRICS) {
    for (auto region_id : request->region_ids()) {
      response->mutable_region_actual_metrics()->add_region_metricses()->CopyFrom(GetRegionActualMetrics(region_id));
    }
  } else if (request->type() == pb::region_control::DebugType::INDEX_VECTOR_INDEX_METRICS) {
    auto vector_index_manager = Server::GetInstance()->GetVectorIndexManager();
    if (vector_index_manager == nullptr) {
      return;
    }
    std::vector<std::shared_ptr<VectorIndex>> vector_indexs;
    if (request->region_ids().empty()) {
      vector_indexs = vector_index_manager->GetAllVectorIndex();
    } else {
      for (auto region_id : request->region_ids()) {
        auto vector_index = vector_index_manager->GetVectorIndex(region_id);
        if (vector_index != nullptr) {
          vector_indexs.push_back(vector_index);
        }
      }
    }

    for (auto& vector_index : vector_indexs) {
      auto* entry = response->mutable_vector_index_metrics()->add_entries();

      entry->set_id(vector_index->Id());
      entry->set_version(vector_index->Version());
      entry->set_dimension(vector_index->GetDimension());
      entry->set_apply_log_index(vector_index->ApplyLogIndex());
      entry->set_snapshot_log_index(vector_index->SnapshotLogIndex());
      uint64_t key_count = 0;
      vector_index->GetCount(key_count);
      entry->set_key_count(key_count);
      uint64_t deleted_key_count = 0;
      vector_index->GetDeletedCount(deleted_key_count);
      entry->set_deleted_key_count(deleted_key_count);
      uint64_t memory_size = 0;
      vector_index->GetMemorySize(memory_size);
      entry->set_memory_size(memory_size);
    }
  }
}

}  // namespace dingodb

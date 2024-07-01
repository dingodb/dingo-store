
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

#include "server/debug_service.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "butil/endpoint.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "common/helper.h"
#include "common/logging.h"
#include "document/codec.h"
#include "engine/raw_engine.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/debug.pb.h"
#include "proto/error.pb.h"
#include "server/file_service.h"
#include "server/server.h"
#include "server/service_helper.h"
#include "vector/codec.h"
#include "vector/vector_index_snapshot_manager.h"

#ifdef LINK_TCMALLOC
#include "gperftools/malloc_extension.h"
#endif

using dingodb::pb::error::Errno;

namespace dingodb {

void DebugServiceImpl::AddRegion(google::protobuf::RpcController* controller,
                                 const dingodb::pb::debug::AddRegionRequest* request,
                                 dingodb::pb::debug::AddRegionResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto region_controller = Server::GetInstance().GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region().id());
  command->set_region_cmd_type(pb::coordinator::CMD_CREATE);
  *(command->mutable_create_request()->mutable_region_definition()) = request->region();

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, nullptr), command);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DebugServiceImpl::ChangeRegion(google::protobuf::RpcController* controller,
                                    const pb::debug::ChangeRegionRequest* request,
                                    pb::debug::ChangeRegionResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto region_controller = Server::GetInstance().GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region().id());
  command->set_region_cmd_type(pb::coordinator::CMD_CHANGE_PEER);
  *(command->mutable_change_peer_request()->mutable_region_definition()) = request->region();

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, nullptr), command);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DebugServiceImpl::MergeRegion(google::protobuf::RpcController* controller,
                                   const dingodb::pb::debug::MergeRegionRequest* request,
                                   dingodb::pb::debug::MergeRegionResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  DINGO_LOG(INFO) << "MergeRegion request: " << request->ShortDebugString();

  auto storage = Server::GetInstance().GetStorage();
  auto status = storage->ValidateLeader(request->source_region_id());
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    return;
  }

  auto region_controller = Server::GetInstance().GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->source_region_id());
  command->set_region_cmd_type(pb::coordinator::CMD_MERGE);

  auto* merge_request = command->mutable_merge_request();
  merge_request->set_source_region_id(request->source_region_id());
  merge_request->set_target_region_id(request->target_region_id());

  status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, nullptr), command);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DebugServiceImpl::DestroyRegion(google::protobuf::RpcController* controller,
                                     const dingodb::pb::debug::DestroyRegionRequest* request,
                                     dingodb::pb::debug::DestroyRegionResponse* response,
                                     google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto region_controller = Server::GetInstance().GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region_id());
  command->set_region_cmd_type(pb::coordinator::CMD_DELETE);
  command->mutable_delete_request()->set_region_id(request->region_id());

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, nullptr), command);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DebugServiceImpl::Snapshot(google::protobuf::RpcController* controller, const pb::debug::SnapshotRequest* request,
                                pb::debug::SnapshotResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto region_controller = Server::GetInstance().GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->region_id());
  command->set_region_cmd_type(pb::coordinator::CMD_SNAPSHOT);

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, nullptr), command);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DebugServiceImpl::TransferLeader(google::protobuf::RpcController* controller,
                                      const pb::debug::TransferLeaderRequest* request,
                                      pb::debug::TransferLeaderResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine != nullptr) {
    auto status = raft_store_engine->TransferLeader(request->region_id(), request->peer());
    if (!status.ok()) {
      ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
    }
  }
}

void DebugServiceImpl::ModifyRegionMeta(google::protobuf::RpcController* controller,
                                        const pb::debug::ModifyRegionMetaRequest* request,
                                        pb::debug::ModifyRegionMetaResponse* response,
                                        google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  if (request->fields().empty()) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETERS, "Missing param fields");
    return;
  }

  auto region = Server::GetInstance().GetRegion(request->region_id());
  if (region == nullptr) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREGION_NOT_FOUND, "Not found region");
    return;
  }

  for (const auto& field : request->fields()) {
    if (field == "state") {
      region->SetState(request->state());
    }
  }
}

void DebugServiceImpl::SnapshotVectorIndex(google::protobuf::RpcController* controller,
                                           const pb::debug::SnapshotVectorIndexRequest* request,
                                           pb::debug::SnapshotVectorIndexResponse* response,
                                           google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto region_controller = Server::GetInstance().GetRegionController();

  auto command = std::make_shared<pb::coordinator::RegionCmd>();
  command->set_id(Helper::TimestampNs());
  command->set_region_id(request->vector_index_id());
  command->set_region_cmd_type(pb::coordinator::CMD_SNAPSHOT_VECTOR_INDEX);
  command->mutable_snapshot_vector_index_request()->set_vector_index_id(request->vector_index_id());

  auto status = region_controller->DispatchRegionControlCommand(std::make_shared<Context>(cntl, nullptr), command);
  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DebugServiceImpl::TriggerVectorIndexSnapshot(google::protobuf::RpcController* controller,
                                                  const pb::debug::TriggerVectorIndexSnapshotRequest* request,
                                                  pb::debug::TriggerVectorIndexSnapshotResponse* response,
                                                  google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  butil::EndPoint endpoint;
  butil::str2endpoint(request->location().host().c_str(), request->location().port(), &endpoint);

  auto store_region_meta = GET_STORE_REGION_META;
  auto region = store_region_meta->GetRegion(request->vector_index_id());
  if (region == nullptr) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EREGION_NOT_FOUND);
    error->set_errmsg(fmt::format("Not found region {}.", request->vector_index_id()));
    return;
  }
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper == nullptr) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EVECTOR_INDEX_NOT_FOUND);
    error->set_errmsg(fmt::format("Not found vector index {}.", request->vector_index_id()));
    return;
  }
  auto snapshot_set = vector_index_wrapper->SnapshotSet();

  butil::Status status;
  if (Helper::ToUpper(request->type()) == "INSTALL") {
    auto snapshot = snapshot_set->GetLastSnapshot();
    if (snapshot == nullptr) {
      auto* error = response->mutable_error();
      error->set_errcode(Errno::EVECTOR_SNAPSHOT_NOT_FOUND);
      error->set_errmsg(fmt::format("Not found vector index snapshot {}.", request->vector_index_id()));
      return;
    }

    status = VectorIndexSnapshotManager::LaunchInstallSnapshot(endpoint, snapshot);
  } else if (Helper::ToUpper(request->type()) == "PULL") {
    status = VectorIndexSnapshotManager::LaunchPullSnapshot(endpoint, snapshot_set);
  } else {
    auto* error = response->mutable_error();
    error->set_errcode(pb::error::EILLEGAL_PARAMTETERS);
    error->set_errmsg("Param type is error");
  }

  if (!status.ok()) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void DebugServiceImpl::TriggerRebuildVectorIndex(google::protobuf::RpcController* controller,
                                                 const pb::debug::TriggerRebuildVectorIndexRequest* request,
                                                 pb::debug::TriggerRebuildVectorIndexResponse* response,
                                                 google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto store_region_meta = GET_STORE_REGION_META;
  auto region = store_region_meta->GetRegion(request->vector_index_id());
  if (region == nullptr) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EREGION_NOT_FOUND);
    error->set_errmsg(fmt::format("Not found region {}.", request->vector_index_id()));
    return;
  }
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper == nullptr) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EVECTOR_INDEX_NOT_FOUND);
    error->set_errmsg(fmt::format("Not found vector index {}.", request->vector_index_id()));
    return;
  }

  VectorIndexManager::LaunchRebuildVectorIndex(vector_index_wrapper, Helper::TimestampMs(), false, true, true,
                                               "from debug");
}

void DebugServiceImpl::TriggerSaveVectorIndex(google::protobuf::RpcController* controller,
                                              const pb::debug::TriggerSaveVectorIndexRequest* request,
                                              pb::debug::TriggerSaveVectorIndexResponse* response,
                                              google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto store_region_meta = GET_STORE_REGION_META;
  auto region = store_region_meta->GetRegion(request->vector_index_id());
  if (region == nullptr) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EREGION_NOT_FOUND);
    error->set_errmsg(fmt::format("Not found region {}.", request->vector_index_id()));
    return;
  }
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper == nullptr) {
    auto* error = response->mutable_error();
    error->set_errcode(Errno::EVECTOR_INDEX_NOT_FOUND);
    error->set_errmsg(fmt::format("Not found vector index {}.", request->vector_index_id()));
    return;
  }

  VectorIndexManager::LaunchSaveVectorIndex(vector_index_wrapper, "from debug");
}

void DebugServiceImpl::Compact(google::protobuf::RpcController* controller, const pb::debug::CompactRequest* request,
                               pb::debug::CompactResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto raw_engine = Server::GetInstance().GetRawEngine(pb::common::RawEngine::RAW_ENG_ROCKSDB);
  if (raw_engine == nullptr) {
    response->mutable_error()->set_errcode(pb::error::ERAW_ENGINE_NOT_FOUND);
    response->mutable_error()->set_errmsg("Not found raw engine.");
    return;
  }

  butil::Status status;
  if (!request->cf_name().empty()) {
    status = raw_engine->Compact(request->cf_name());
  } else {
    status = raw_engine->Compact(Constant::kStoreDataCF);
    if (status.ok()) {
      status = raw_engine->Compact(Constant::kStoreMetaCF);
    }
  }

  if (!status.ok()) {
    response->mutable_error()->set_errcode(static_cast<pb::error::Errno>(status.error_code()));
    response->mutable_error()->set_errmsg(status.error_str());
  }
}

static pb::common::RegionMetrics GetRegionActualMetrics(int64_t region_id) {
  pb::common::RegionMetrics region_metrics;
  region_metrics.set_id(region_id);
  auto region = Server::GetInstance().GetRegion(region_id);
  if (region == nullptr) {
    return region_metrics;
  }

  auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
  if (raw_engine == nullptr) {
    DINGO_LOG(ERROR) << "Not found raw engine for region " << region_id;
    return region_metrics;
  }

  int64_t size = 0;
  int32_t key_count = 0;
  std::string min_key, max_key;
  auto encode_range = region->Range(true);

  auto column_family_names = Helper::GetColumnFamilyNames(encode_range.start_key());
  for (const auto& name : column_family_names) {
    IteratorOptions options;
    options.upper_bound = encode_range.end_key();

    auto iter = raw_engine->Reader()->NewIterator(name, options);
    int32_t temp_key_count = 0;
    for (iter->Seek(encode_range.start_key()); iter->Valid(); iter->Next()) {
      size += iter->Key().size() + iter->Value().size();

      ++temp_key_count;
      if (min_key.empty()) {
        min_key = iter->Key();
      }
      if (max_key.compare(iter->Key()) < 0) {
        max_key = iter->Key();
      }
    }
    key_count = std::max(key_count, temp_key_count);
  }

  region_metrics.set_min_key(min_key);
  region_metrics.set_max_key(max_key);
  region_metrics.set_region_size(size);
  region_metrics.set_row_count(key_count);

  return region_metrics;
}

void DebugServiceImpl::Debug(google::protobuf::RpcController* controller,
                             const ::dingodb::pb::debug::DebugRequest* request,
                             ::dingodb::pb::debug::DebugResponse* response, ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  if (request->type() == pb::debug::DebugType::STORE_REGION_META_STAT) {
    auto store_region_meta = GET_STORE_REGION_META;
    auto regions = store_region_meta->GetAllRegion();

    std::vector<int64_t> leader_region_ids;
    std::vector<int64_t> follower_region_ids;
    std::map<std::string, int32_t> state_counts;
    for (auto& region : regions) {
      std::string name = pb::common::StoreRegionState_Name(region->State());
      if (state_counts.find(name) == state_counts.end()) {
        state_counts[name] = 0;
      }
      ++state_counts[name];

      if (Server::GetInstance().IsLeader(region->Id())) {
        leader_region_ids.push_back(region->Id());
      } else {
        follower_region_ids.push_back(region->Id());
      }
    }

    for (auto [name, count] : state_counts) {
      response->mutable_region_meta_stat()->mutable_state_counts()->insert({name, count});
    }
    response->mutable_region_meta_stat()->set_leader_count(leader_region_ids.size());
    response->mutable_region_meta_stat()->set_follower_count(follower_region_ids.size());
    Helper::VectorToPbRepeated(leader_region_ids, response->mutable_region_meta_stat()->mutable_leader_regoin_ids());
    Helper::VectorToPbRepeated(follower_region_ids,
                               response->mutable_region_meta_stat()->mutable_follower_regoin_ids());

  } else if (request->type() == pb::debug::DebugType::STORE_REGION_META_DETAILS) {
    auto store_region_meta = GET_STORE_REGION_META;
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
      *(response->mutable_region_meta_details()->add_regions()) = region->InnerRegion();
    }

  } else if (request->type() == pb::debug::DebugType::STORE_REGION_CONTROL_COMMAND) {
    std::vector<std::shared_ptr<pb::coordinator::RegionCmd>> commands;
    if (request->region_ids().empty()) {
      commands = Server::GetInstance().GetRegionCommandManager()->GetAllCommand();
    } else {
      for (auto region_id : request->region_ids()) {
        auto region_commands = Server::GetInstance().GetRegionCommandManager()->GetCommands(region_id);
        if (!region_commands.empty()) {
          commands.insert(commands.end(), region_commands.begin(), region_commands.end());
        }
      }
    }

    for (auto& command : commands) {
      *(response->mutable_region_control_command()->add_region_cmds()) = (*command);
    }

  } else if (request->type() == pb::debug::DebugType::STORE_RAFT_META) {
    auto store_raft_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta();

    std::vector<store::RaftMetaPtr> raft_metas;
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
      *(response->mutable_raft_meta()->add_raft_metas()) = raft_meta->InnerRaftMeta();
    }

  } else if (request->type() == pb::debug::DebugType::STORE_REGION_EXECUTOR) {
    auto region_ids = Server::GetInstance().GetRegionController()->GetAllRegion();

    for (auto region_id : region_ids) {
      response->mutable_region_executor()->add_region_ids(region_id);
    }

  } else if (request->type() == pb::debug::DebugType::STORE_REGION_METRICS) {
    auto store_region_metrics = Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics();

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
      *(response->mutable_region_metrics()->add_region_metricses()) = region_metrics->InnerRegionMetrics();
    }
  } else if (request->type() == pb::debug::DebugType::STORE_FILE_READER) {
    auto reader_ids = FileServiceReaderManager::GetInstance().GetAllReaderId();

    response->mutable_file_reader()->set_count(reader_ids.size());
    for (auto reader_id : reader_ids) {
      response->mutable_file_reader()->add_reader_ids(reader_id);
    }
  } else if (request->type() == pb::debug::DebugType::STORE_REGION_ACTUAL_METRICS) {
    for (auto region_id : request->region_ids()) {
      *(response->mutable_region_actual_metrics()->add_region_metricses()) = GetRegionActualMetrics(region_id);
    }

  } else if (request->type() == pb::debug::DebugType::STORE_METRICS) {
    auto store_metrics_manager = Server::GetInstance().GetStoreMetricsManager();
    if (store_metrics_manager == nullptr) {
      return;
    }
    auto store_metrics = store_metrics_manager->GetStoreMetrics()->Metrics();
    *(response->mutable_store_metrics()->mutable_metrics()) = store_metrics;

  } else if (request->type() == pb::debug::DebugType::STORE_REGION_CHANGE_RECORD) {
    std::vector<pb::store_internal::RegionChangeRecord> records;
    if (request->region_ids().empty()) {
      records = GET_REGION_CHANGE_RECORDER->GetAllChangeRecord();
    } else {
      for (auto region_id : request->region_ids()) {
        auto temp_records = GET_REGION_CHANGE_RECORDER->GetChangeRecord(region_id);
        if (!temp_records.empty()) {
          records.insert(records.end(), temp_records.begin(), temp_records.end());
        }
      }
    }

    Helper::VectorToPbRepeated(records, response->mutable_region_change_record()->mutable_records());

  } else if (request->type() == pb::debug::DebugType::INDEX_VECTOR_INDEX_METRICS) {
    auto store_region_meta = GET_STORE_REGION_META;
    std::vector<store::RegionPtr> regions;
    if (request->region_ids().empty()) {
      regions = store_region_meta->GetAllAliveRegion();
    } else {
      for (auto region_id : request->region_ids()) {
        auto region = store_region_meta->GetRegion(region_id);
        if (region != nullptr) {
          regions.push_back(region);
        }
      }
    }

    for (auto& region : regions) {
      auto vector_index_wrapper = region->VectorIndexWrapper();
      if (vector_index_wrapper == nullptr || !vector_index_wrapper->IsReady()) {
        continue;
      }
      auto* entry = response->mutable_vector_index_metrics()->add_entries();

      entry->set_id(vector_index_wrapper->Id());
      entry->set_version(vector_index_wrapper->Version());
      entry->set_last_build_epoch_version(vector_index_wrapper->LastBuildEpochVersion());
      entry->set_dimension(vector_index_wrapper->GetDimension());
      entry->set_apply_log_index(vector_index_wrapper->ApplyLogId());
      entry->set_snapshot_log_index(vector_index_wrapper->SnapshotLogId());
      int64_t key_count = 0;
      vector_index_wrapper->GetCount(key_count);
      entry->set_key_count(key_count);
      int64_t deleted_key_count = 0;
      vector_index_wrapper->GetDeletedCount(deleted_key_count);
      entry->set_deleted_key_count(deleted_key_count);
      int64_t memory_size = 0;
      vector_index_wrapper->GetMemorySize(memory_size);
      entry->set_memory_size(memory_size);

      // raw vector index
      {
        auto vector_index = vector_index_wrapper->GetOwnVectorIndex();
        if (vector_index != nullptr) {
          auto* vector_index_state = entry->add_entries();
          vector_index_state->set_id(vector_index->Id());
          vector_index_state->set_type(vector_index->VectorIndexType());
          vector_index_state->set_sub_type(vector_index->VectorIndexSubType());
          vector_index_state->set_apply_log_id(vector_index->ApplyLogId());
          vector_index_state->set_snapshot_log_id(vector_index->SnapshotLogId());
          *vector_index_state->mutable_epoch() = vector_index->Epoch();

          std::string start_key, end_key;
          VectorCodec::DebugRange(false, vector_index->Range(), start_key, end_key);
          vector_index_state->set_start_key(start_key);
          vector_index_state->set_end_key(end_key);

          int64_t key_count = 0;
          vector_index->GetCount(key_count);
          vector_index_state->set_key_count(key_count);
          int64_t deleted_key_count = 0;
          vector_index->GetDeletedCount(deleted_key_count);
          vector_index_state->set_deleted_key_count(deleted_key_count);
          int64_t memory_size = 0;
          vector_index_wrapper->GetMemorySize(memory_size);
          vector_index_state->set_memory_size(memory_size);
          *vector_index_state->mutable_parameter() = vector_index->VectorIndexParameter();
          vector_index_state->set_comment("own index");
        }
      }

      {
        auto vector_index = vector_index_wrapper->ShareVectorIndex();
        if (vector_index != nullptr) {
          auto* vector_index_state = entry->add_entries();
          vector_index_state->set_id(vector_index->Id());
          vector_index_state->set_type(vector_index->VectorIndexType());
          vector_index_state->set_apply_log_id(vector_index->ApplyLogId());
          vector_index_state->set_snapshot_log_id(vector_index->SnapshotLogId());
          *vector_index_state->mutable_epoch() = vector_index->Epoch();

          std::string start_key, end_key;
          VectorCodec::DebugRange(false, vector_index->Range(), start_key, end_key);
          vector_index_state->set_start_key(start_key);
          vector_index_state->set_end_key(end_key);

          int64_t key_count = 0;
          vector_index->GetCount(key_count);
          vector_index_state->set_key_count(key_count);
          int64_t deleted_key_count = 0;
          vector_index->GetDeletedCount(deleted_key_count);
          vector_index_state->set_deleted_key_count(deleted_key_count);
          int64_t memory_size = 0;
          vector_index_wrapper->GetMemorySize(memory_size);
          vector_index_state->set_memory_size(memory_size);
          *vector_index_state->mutable_parameter() = vector_index->VectorIndexParameter();
          vector_index_state->set_comment("share index");
        }
      }

      {
        auto vector_index = vector_index_wrapper->SiblingVectorIndex();
        if (vector_index != nullptr) {
          auto* vector_index_state = entry->add_entries();
          vector_index_state->set_id(vector_index->Id());
          vector_index_state->set_type(vector_index->VectorIndexType());
          vector_index_state->set_apply_log_id(vector_index->ApplyLogId());
          vector_index_state->set_snapshot_log_id(vector_index->SnapshotLogId());
          *vector_index_state->mutable_epoch() = vector_index->Epoch();

          std::string start_key, end_key;
          VectorCodec::DebugRange(false, vector_index->Range(), start_key, end_key);
          vector_index_state->set_start_key(start_key);
          vector_index_state->set_end_key(end_key);

          int64_t key_count = 0;
          vector_index->GetCount(key_count);
          vector_index_state->set_key_count(key_count);
          int64_t deleted_key_count = 0;
          vector_index->GetDeletedCount(deleted_key_count);
          vector_index_state->set_deleted_key_count(deleted_key_count);
          int64_t memory_size = 0;
          vector_index_wrapper->GetMemorySize(memory_size);
          vector_index_state->set_memory_size(memory_size);
          *vector_index_state->mutable_parameter() = vector_index->VectorIndexParameter();
          vector_index_state->set_comment("sibling index");
        }
      }
    }
  }
}

void DebugServiceImpl::GetMemoryStats(google::protobuf::RpcController* controller,
                                      const ::dingodb::pb::debug::GetMemoryStatsRequest* request,
                                      ::dingodb::pb::debug::GetMemoryStatsResponse* response,
                                      ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

#ifdef LINK_TCMALLOC
  std::string stat_buf(4096, '\0');
  MallocExtension::instance()->GetStats(stat_buf.data(), stat_buf.size());

  response->set_memory_stats(stat_buf.c_str());
#else
  response->mutable_error()->set_errcode(pb::error::EINTERNAL);
  response->mutable_error()->set_errmsg("No use tcmalloc");
#endif
}

void DebugServiceImpl::ReleaseFreeMemory(google::protobuf::RpcController* controller,
                                         const ::dingodb::pb::debug::ReleaseFreeMemoryRequest* request,
                                         ::dingodb::pb::debug::ReleaseFreeMemoryResponse* response,
                                         ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

#ifdef LINK_TCMALLOC
  if (request->is_force()) {
    MallocExtension::instance()->ReleaseFreeMemory();
  } else {
    MallocExtension::instance()->SetMemoryReleaseRate(request->rate());
  }
#else
  response->mutable_error()->set_errcode(pb::error::EINTERNAL);
  response->mutable_error()->set_errmsg("No use tcmalloc");
#endif
}

void DebugServiceImpl::TraceWorkQueue(google::protobuf::RpcController* controller,
                                      const ::dingodb::pb::debug::TraceWorkQueueRequest* request,
                                      ::dingodb::pb::debug::TraceWorkQueueResponse* response,
                                      ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  if (request->type() == pb::debug::WORK_QUEUE_STORE_SERVICE_READ) {
    auto worker_set_traces = Server::GetInstance().GetStoreServiceReadWorkerSetTrace();

    auto* mut_worker_set_traces = response->add_worker_set_traces();
    for (auto& worker_trace : worker_set_traces) {
      auto* worker_traces = mut_worker_set_traces->add_worker_traces();
      Helper::VectorToPbRepeated(worker_trace, worker_traces->mutable_traces());
      worker_traces->set_count(worker_trace.size());
    }
    mut_worker_set_traces->set_count(worker_set_traces.size());

  } else if (request->type() == pb::debug::WORK_QUEUE_STORE_SERVICE_WRITE) {
    auto worker_set_traces = Server::GetInstance().GetStoreServiceWriteWorkerSetTrace();

    auto* mut_worker_set_traces = response->add_worker_set_traces();
    for (auto& worker_trace : worker_set_traces) {
      auto* worker_traces = mut_worker_set_traces->add_worker_traces();
      Helper::VectorToPbRepeated(worker_trace, worker_traces->mutable_traces());
      worker_traces->set_count(worker_trace.size());
    }
    mut_worker_set_traces->set_count(worker_set_traces.size());

  } else if (request->type() == pb::debug::WORK_QUEUE_INDEX_SERVICE_READ) {
    auto worker_set_traces = Server::GetInstance().GetIndexServiceReadWorkerSetTrace();

    auto* mut_worker_set_traces = response->add_worker_set_traces();
    for (auto& worker_trace : worker_set_traces) {
      auto* worker_traces = mut_worker_set_traces->add_worker_traces();
      Helper::VectorToPbRepeated(worker_trace, worker_traces->mutable_traces());
      worker_traces->set_count(worker_trace.size());
    }
    mut_worker_set_traces->set_count(worker_set_traces.size());

  } else if (request->type() == pb::debug::WORK_QUEUE_INDEX_SERVICE_WRITE) {
    auto worker_set_traces = Server::GetInstance().GetIndexServiceWriteWorkerSetTrace();

    auto* mut_worker_set_traces = response->add_worker_set_traces();
    for (auto& worker_trace : worker_set_traces) {
      auto* worker_traces = mut_worker_set_traces->add_worker_traces();
      Helper::VectorToPbRepeated(worker_trace, worker_traces->mutable_traces());
      worker_traces->set_count(worker_trace.size());
    }
    mut_worker_set_traces->set_count(worker_set_traces.size());

  } else if (request->type() == pb::debug::WORK_QUEUE_VECTOR_INDEX_BACKGROUND) {
    auto worker_set_traces = Server::GetInstance().GetVectorIndexBackgroundWorkerSetTrace();

    auto* mut_worker_set_traces = response->add_worker_set_traces();
    for (auto& worker_trace : worker_set_traces) {
      auto* worker_traces = mut_worker_set_traces->add_worker_traces();
      Helper::VectorToPbRepeated(worker_trace, worker_traces->mutable_traces());
      worker_traces->set_count(worker_trace.size());
    }
    mut_worker_set_traces->set_count(worker_set_traces.size());
  }
}

void DebugServiceImpl::AdjustThreadPoolSize(google::protobuf::RpcController* controller,
                                            const ::dingodb::pb::debug::AdjustThreadPoolSizeRequest* request,
                                            ::dingodb::pb::debug::AdjustThreadPoolSizeResponse* response,
                                            ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
  if (thread_pool == nullptr) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::ETHREADPOOL_NOTFOUND,
                            fmt::format("Not exist thread pool {}", request->name()));
    return;
  }

  thread_pool->AdjustPoolSize(request->size());
}

void DebugServiceImpl::BindCore(google::protobuf::RpcController* controller,
                                const ::dingodb::pb::debug::BindCoreRequest* request,
                                ::dingodb::pb::debug::BindCoreResponse* response, ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
  if (thread_pool == nullptr) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::ETHREADPOOL_NOTFOUND,
                            fmt::format("Not exist thread pool {}", request->thread_pool_name()));
    return;
  }

  std::vector<uint32_t> threads = Helper::PbRepeatedToVector(request->threads());
  std::vector<uint32_t> cores = Helper::PbRepeatedToVector(request->cores());

  if (!thread_pool->BindCore(threads, cores)) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EBIND_CORE, "Bind thread to core failed");
  }
}

void DebugServiceImpl::UnbindCore(google::protobuf::RpcController* controller,
                                  const ::dingodb::pb::debug::UnbindCoreRequest* request,
                                  ::dingodb::pb::debug::UnbindCoreResponse* response,
                                  ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
  if (thread_pool == nullptr) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::ETHREADPOOL_NOTFOUND,
                            fmt::format("Not exist thread pool {}", request->thread_pool_name()));
    return;
  }

  if (!thread_pool->UnbindCore()) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EUNBIND_CORE, "Unbind thread to core failed");
  }
}

void DebugServiceImpl::ShowAffinity(google::protobuf::RpcController* controller,
                                    const ::dingodb::pb::debug::ShowAffinityRequest* request,
                                    ::dingodb::pb::debug::ShowAffinityResponse* response,
                                    ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();
  if (thread_pool == nullptr) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::ETHREADPOOL_NOTFOUND,
                            fmt::format("Not exist thread pool {}", request->thread_pool_name()));
    return;
  }

  auto pairs = thread_pool->GetAffinity();
  for (auto& pair : pairs) {
    auto* mut_pair = response->add_pairs();
    mut_pair->set_thread_name(pair.first);
    mut_pair->set_core(pair.second);
  }
}

std::vector<pb::debug::DumpRegionResponse::KV> DumpRawKvRegion(RawEnginePtr raw_engine, const pb::common::Range& range,
                                                               int64_t offset, int64_t limit) {
  auto reader = raw_engine->Reader();

  dingodb::IteratorOptions options;
  options.upper_bound = range.end_key();

  std::vector<pb::debug::DumpRegionResponse::KV> kvs;
  int64_t curr_offset = 0;
  auto iter = reader->NewIterator(Constant::kStoreDataCF, options);
  for (iter->Seek(range.start_key()); iter->Valid(); iter->Next(), ++curr_offset) {
    if (curr_offset < offset) {
      continue;
    }
    if (curr_offset >= offset + limit) {
      break;
    }

    std::string decode_key;
    int64_t ts;
    mvcc::Codec::DecodeKey(iter->Key(), decode_key, ts);
    pb::debug::DumpRegionResponse::KV kv;
    kv.set_key(decode_key);
    kv.set_ts(ts);

    mvcc::ValueFlag flag;
    int64_t ttl;
    auto value = mvcc::Codec::UnPackageValue(iter->Value(), flag, ttl);

    kv.set_flag(static_cast<pb::debug::DumpRegionResponse::ValueFlag>(flag));
    kv.set_ttl(ttl);
    kv.set_value(std::string(value));

    kvs.push_back(kv);
  }

  return std::move(kvs);
}

std::vector<pb::debug::DumpRegionResponse::Vector> DumpRawVectorRegion(RawEnginePtr raw_engine,
                                                                       const pb::common::Range& range, int64_t offset,
                                                                       int64_t limit) {
  auto reader = raw_engine->Reader();

  std::vector<pb::debug::DumpRegionResponse::Vector> vectors;

  // vector data
  {
    int64_t curr_offset = 0;
    dingodb::IteratorOptions options;
    options.upper_bound = range.end_key();
    auto iter = reader->NewIterator(Constant::kVectorDataCF, options);
    for (iter->Seek(range.start_key()); iter->Valid(); iter->Next(), ++curr_offset) {
      if (curr_offset < offset) {
        continue;
      }
      if (curr_offset >= offset + limit) {
        break;
      }

      const auto& encode_key = std::string(iter->Key());
      pb::debug::DumpRegionResponse::Vector vector;
      vector.set_key(encode_key);

      int64_t ts = VectorCodec::TruncateKeyForTs(encode_key);
      std::string encode_key_no_ts(VectorCodec::TruncateTsForKey(encode_key));
      vector.set_vector_id(VectorCodec::DecodeVectorIdFromEncodeKey(encode_key_no_ts));
      vector.set_ts(ts);

      mvcc::ValueFlag flag;
      int64_t ttl;
      auto value = mvcc::Codec::UnPackageValue(iter->Value(), flag, ttl);

      vector.set_flag(static_cast<pb::debug::DumpRegionResponse::ValueFlag>(flag));
      vector.set_ttl(ttl);

      if (flag == mvcc::ValueFlag::kPut || flag == mvcc::ValueFlag::kPutTTL) {
        if (!vector.mutable_vector()->ParseFromArray(value.data(), value.size())) {
          DINGO_LOG(FATAL) << fmt::format("Parse vector proto failed, value size: {}.", value.size());
        }
      }

      vectors.push_back(vector);
    }
  }

  // scalar data
  if (!vectors.empty()) {
    auto first_vector = vectors.front();
    auto last_vector = vectors.back();

    dingodb::IteratorOptions options;
    options.upper_bound = Helper::PrefixNext(last_vector.key());
    uint32_t count = 0;
    auto iter = reader->NewIterator(Constant::kVectorScalarCF, options);
    for (iter->Seek(first_vector.key()); iter->Valid(); iter->Next()) {
      auto& vector = vectors.at(count++);
      CHECK(iter->Key() == vector.key()) << "Not match key.";

      mvcc::ValueFlag flag;
      int64_t ttl;
      auto value = mvcc::Codec::UnPackageValue(iter->Value(), flag, ttl);
      if (flag == mvcc::ValueFlag::kPut || flag == mvcc::ValueFlag::kPutTTL) {
        if (!vector.mutable_scalar_data()->ParseFromArray(value.data(), value.size())) {
          DINGO_LOG(FATAL) << fmt::format("Parse vector scalar proto failed, value size: {}.", value.size());
        }
      }
    }
  }

  // table data
  if (!vectors.empty()) {
    auto first_vector = vectors.front();
    auto last_vector = vectors.back();

    dingodb::IteratorOptions options;
    options.upper_bound = Helper::PrefixNext(last_vector.key());
    uint32_t count = 0;
    auto iter = reader->NewIterator(Constant::kVectorTableCF, options);
    for (iter->Seek(first_vector.key()); iter->Valid(); iter->Next()) {
      auto& vector = vectors.at(count++);
      CHECK(iter->Key() == vector.key()) << "Not match key.";

      mvcc::ValueFlag flag;
      int64_t ttl;
      auto value = mvcc::Codec::UnPackageValue(iter->Value(), flag, ttl);
      if (flag == mvcc::ValueFlag::kPut || flag == mvcc::ValueFlag::kPutTTL) {
        if (!vector.mutable_table_data()->ParseFromArray(value.data(), value.size())) {
          DINGO_LOG(FATAL) << fmt::format("Parse vector table proto failed, value size: {}.", value.size());
        }
      }
    }
  }

  return std::move(vectors);
}

std::vector<pb::debug::DumpRegionResponse::Ducument> DumpRawDucmentRegion(RawEnginePtr raw_engine,
                                                                          const pb::common::Range& range,
                                                                          int64_t offset, int64_t limit) {
  auto reader = raw_engine->Reader();

  std::vector<pb::debug::DumpRegionResponse::Ducument> documents;

  // vector data
  {
    int64_t curr_offset = 0;
    dingodb::IteratorOptions options;
    options.upper_bound = range.end_key();
    auto iter = reader->NewIterator(Constant::kVectorDataCF, options);
    for (iter->Seek(range.start_key()); iter->Valid(); iter->Next(), ++curr_offset) {
      if (curr_offset < offset) {
        continue;
      }
      if (curr_offset >= offset + limit) {
        break;
      }

      std::string key(iter->Key());

      std::string plain_key;
      int64_t partition_id;
      int64_t document_id;
      DocumentCodec::DecodeFromEncodeKeyWithTs(key, partition_id, document_id);

      pb::debug::DumpRegionResponse::Ducument ducument;
      ducument.set_document_id(document_id);
      ducument.set_ts(DocumentCodec::TruncateKeyForTs(key));

      mvcc::ValueFlag flag;
      int64_t ttl;
      auto value = mvcc::Codec::UnPackageValue(iter->Value(), flag, ttl);

      ducument.set_flag(static_cast<pb::debug::DumpRegionResponse::ValueFlag>(flag));
      ducument.set_ttl(ttl);

      if (flag == mvcc::ValueFlag::kPut || flag == mvcc::ValueFlag::kPutTTL) {
        if (!ducument.mutable_document()->ParseFromArray(value.data(), value.size())) {
          DINGO_LOG(FATAL) << fmt::format("Parse document proto failed, value size: {}.", value.size());
        }
      }

      documents.push_back(ducument);
    }
  }

  return std::move(documents);
}

void DebugServiceImpl::DumpRegion(google::protobuf::RpcController* controller,
                                  const pb::debug::DumpRegionRequest* request, pb::debug::DumpRegionResponse* response,
                                  ::google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  if (request->offset() < 0 || request->limit() < 0) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETERS, "Param offset/limit is error");
    return;
  }

  auto region = Server::GetInstance().GetRegion(request->region_id());
  if (region == nullptr) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREGION_NOT_FOUND, "Not found region");
    return;
  }

  if (region->IsTxn()) {
    ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_SUPPORT, "Not support dump txn");
    return;
  }

  DINGO_LOG(INFO) << fmt::format("region({}) range{} offset({}) limit({})", request->region_id(),
                                 region->RangeToString(), request->offset(), request->limit());

  auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
  if (region->Type() == pb::common::RegionType::STORE_REGION) {
    auto kvs = DumpRawKvRegion(raw_engine, region->Range(true), request->offset(), request->limit());
    Helper::VectorToPbRepeated(kvs, response->mutable_data()->mutable_kvs());
  } else if (region->Type() == pb::common::RegionType::INDEX_REGION) {
    auto vectors = DumpRawVectorRegion(raw_engine, region->Range(true), request->offset(), request->limit());
    Helper::VectorToPbRepeated(vectors, response->mutable_data()->mutable_vectors());
  } else {
    auto documents = DumpRawDucmentRegion(raw_engine, region->Range(true), request->offset(), request->limit());
    Helper::VectorToPbRepeated(documents, response->mutable_data()->mutable_ducuments());
  }
}

}  // namespace dingodb

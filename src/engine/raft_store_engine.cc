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

#include "engine/raft_store_engine.h"

#include <netinet/in.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/endpoint.h"
#include "butil/status.h"
#include "bvar/latency_recorder.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/role.h"
#include "config/config_manager.h"
#include "document/document_reader.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "engine/txn_engine_helper.h"
#include "engine/write_data.h"
#include "event/store_state_machine_event.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "log/rocks_log_storage.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "mvcc/reader.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/raft.pb.h"
#include "raft/meta_state_machine.h"
#include "raft/raft_node.h"
#include "raft/store_state_machine.h"
#include "server/server.h"
#include "vector/vector_reader.h"

DECLARE_int32(init_election_timeout_ms);

namespace dingodb {

RaftStoreEngine::RaftStoreEngine(RawEnginePtr rocks_raw_engine, RawEnginePtr bdb_raw_engine,
                                 mvcc::TsProviderPtr ts_provider)
    : rocks_raw_engine_(rocks_raw_engine),
      bdb_raw_engine_(bdb_raw_engine),
      raft_node_manager_(std::move(std::make_unique<RaftNodeManager>())),
      ts_provider_(ts_provider) {}

RaftStoreEngine::~RaftStoreEngine() = default;

RaftStoreEnginePtr RaftStoreEngine::GetSelfPtr() {
  return std::dynamic_pointer_cast<RaftStoreEngine>(shared_from_this());
}

bool RaftStoreEngine::Init(std::shared_ptr<Config> /*config*/) { return true; }

// Clean region raft directory
static bool CleanRaftDirectory(int64_t region_id, const std::string& raft_log_path) {
  std::string region_raft_log_path = fmt::format("{}/{}", raft_log_path, region_id);
  return Helper::RemoveAllFileOrDirectory(region_raft_log_path);
}

// Recover raft node from region meta data.
// Invoke when server starting.
bool RaftStoreEngine::Recover() {
  auto store_region_meta = GET_STORE_REGION_META;
  auto store_raft_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta();
  auto store_region_metrics = Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics();
  auto regions = store_region_meta->GetAllRegion();

  // shuffle regions for balance leader on restart
  Helper::ShuffleVector(regions);

  int count = 0;
  auto ctx = std::make_shared<Context>();
  auto listener_factory = std::make_shared<StoreSmEventListenerFactory>();
  for (auto& region : regions) {
    if ((region->State() == pb::common::StoreRegionState::NORMAL ||
         region->State() == pb::common::StoreRegionState::STANDBY ||
         region->State() == pb::common::StoreRegionState::SPLITTING ||
         region->State() == pb::common::StoreRegionState::MERGING ||
         region->State() == pb::common::StoreRegionState::TOMBSTONE) &&
        region->GetStoreEngineType() == pb::common::StorageEngine::STORE_ENG_RAFT_STORE) {
      auto raft_meta = store_raft_meta->GetRaftMeta(region->Id());
      if (raft_meta == nullptr) {
        DINGO_LOG(ERROR) << fmt::format("[raft.engine][region({})] recover raft meta not found.", region->Id());
        continue;
      }
      auto region_metrics = store_region_metrics->GetMetrics(region->Id());
      if (region_metrics == nullptr) {
        DINGO_LOG(WARNING) << fmt::format("[raft.engine][region({})] recover raft metrics not found.", region->Id());
      }

      RaftControlAble::AddNodeParameter parameter;
      parameter.role = GetRole();
      parameter.is_restart = true;
      parameter.raft_endpoint = Server::GetInstance().RaftEndpoint();

      parameter.raft_path = Server::GetInstance().GetRaftPath();
      // random election timeout for balance leader on restart
      parameter.election_timeout_ms = FLAGS_init_election_timeout_ms +
                                      Helper::GenerateRealRandomInteger(Constant::kRandomElectionTimeoutMinDeltaMs,
                                                                        Constant::kRandomElectionTimeoutMaxDeltaMs);

      parameter.raft_meta = raft_meta;
      parameter.region_metrics = region_metrics;
      parameter.listeners = listener_factory->Build();
      parameter.apply_worker_set = Server::GetInstance().GetApplyWorkerSet();

      AddNode(region, parameter);
      if (region->NeedBootstrapDoSnapshot()) {
        DINGO_LOG(INFO) << fmt::format("[raft.engine][region({})] need do snapshot.", region->Id());
        auto node = GetNode(region->Id());
        if (node != nullptr) {
          auto ctx = std::make_shared<Context>();
          ctx->SetRegionId(region->Id());
          node->Snapshot(ctx, true);
        }
      }

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
      if (GetRole() == pb::common::INDEX) {
        const auto& definition = region->Definition();
        if (definition.index_parameter().vector_index_parameter().enable_scalar_speed_up_with_document()) {
          auto document_index_wrapper = region->DocumentIndexWrapper();
          if (document_index_wrapper != nullptr) {
            DocumentIndexManager::LaunchLoadOrBuildDocumentIndex(document_index_wrapper, false, false, 0, "recover");
          }
        }
      }
#endif

      if (GetRole() == pb::common::DOCUMENT) {
        auto document_index_wrapper = region->DocumentIndexWrapper();
        DocumentIndexManager::LaunchLoadOrBuildDocumentIndex(document_index_wrapper, false, false, 0, "recover");
      }

      ++count;
    }
  }

  DINGO_LOG(INFO) << fmt::format("[raft.engine][region(*)] recover Raft node num({}).", count);

  return true;
}

std::string RaftStoreEngine::GetName() { return pb::common::StorageEngine_Name(GetID()); }

pb::common::StorageEngine RaftStoreEngine::GetID() { return pb::common::StorageEngine::STORE_ENG_RAFT_STORE; }

RawEnginePtr RaftStoreEngine::GetRawEngine(pb::common::RawEngine type) {
  if (type == pb::common::RawEngine::RAW_ENG_ROCKSDB) {
    return rocks_raw_engine_;
  } else if (type == pb::common::RawEngine::RAW_ENG_BDB) {
    return bdb_raw_engine_;
  }

  DINGO_LOG(FATAL) << "[raft.engine] unknown raw engine type.";
  return nullptr;
}

butil::Status RaftStoreEngine::AddNode(store::RegionPtr region, const AddNodeParameter& parameter) {
  DINGO_LOG(INFO) << fmt::format("[raft.engine][region({})] add region.", region->Id());

  RawEnginePtr raw_engine = GetRawEngine(region->GetRawEngineType());
  // Build StateMachine
  auto state_machine =
      std::make_shared<StoreStateMachine>(raw_engine, region, parameter.raft_meta, parameter.region_metrics,
                                          parameter.listeners, parameter.apply_worker_set);
  if (!state_machine->Init()) {
    return butil::Status(pb::error::ERAFT_INIT, "State machine init failed");
  }

  // Build log storage
  auto log_storage = Server::GetInstance().GetRaftLogStorage();

  // Build RaftNode
  auto node = std::make_shared<RaftNode>(region->Id(), region->Name(), braft::PeerId(parameter.raft_endpoint),
                                         state_machine, log_storage);

  if (node->Init(region, Helper::LocationsToString(Helper::ExtractRaftLocations(region->Peers())), parameter.raft_path,
                 parameter.election_timeout_ms) != 0) {
    if (parameter.is_restart) {
      DINGO_LOG(FATAL) << fmt::format("[raft.engine][region({})] Raft init failed. Please check raft storage!",
                                      region->Id())
                       << ", raft_path: " << parameter.raft_path
                       << ", election_timeout_ms: " << parameter.election_timeout_ms
                       << ", peers: " << Helper::LocationsToString(Helper::ExtractRaftLocations(region->Peers()));
    } else {
      node->Destroy();
    }
    return butil::Status(pb::error::ERAFT_INIT, "Raft init failed");
  }

  raft_node_manager_->AddNode(region->Id(), node);
  return butil::Status();
}

// This function is only for coordinator to create raft node. Store cannot use this function.
butil::Status RaftStoreEngine::AddNode(std::shared_ptr<pb::common::RegionDefinition> region,
                                       std::shared_ptr<MetaControl> meta_control, bool is_volatile) {
  DINGO_LOG(INFO) << fmt::format("[raft.engine][region({})] add region.", region->id());

  // Build StatMachine
  auto state_machine = std::make_shared<MetaStateMachine>(region->id(), meta_control, is_volatile);

  // Build log storage
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  std::string raft_path = Server::GetInstance().GetRaftPath();
  auto log_storage = Server::GetInstance().GetRaftLogStorage();

  std::string const meta_raft_name = fmt::format("{}-{}", region->name(), region->id());
  auto const node = std::make_shared<RaftNode>(
      region->id(), meta_raft_name, braft::PeerId(Server::GetInstance().RaftEndpoint()), state_machine, log_storage);

  // Build RaftNode
  int election_timeout = config->GetInt("raft.election_timeout_s");
  if (node->Init(nullptr, Helper::LocationsToString(Helper::ExtractRaftLocations(region->peers())), raft_path,
                 election_timeout * 1000) != 0) {
    // node->Destroy();
    // this function is only used by coordinator, and will only be called on starting.
    // so if init failed, we can just exit the process, let user to check if the config is correct.
    DINGO_LOG(FATAL) << fmt::format("[raft.engine][region({})] Raft init failed. Please check raft storage!",
                                    region->id())
                     << ", raft_path: " << raft_path << ", election_timeout_ms: " << election_timeout * 1000
                     << ", peers: " << Helper::LocationsToString(Helper::ExtractRaftLocations(region->peers()))
                     << ", region: " << region->ShortDebugString();

    return butil::Status(pb::error::ERAFT_INIT, "Raft init failed");
  }

  raft_node_manager_->AddNode(region->id(), node);

  // set raft_node to coordinator_control
  meta_control->SetRaftNode(node);

  return butil::Status();
}

butil::Status RaftStoreEngine::ChangeNode(std::shared_ptr<Context> /*ctx*/, int64_t region_id,
                                          std::vector<pb::common::Peer> peers) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }
  if (!node->IsLeader()) {
    return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
  }

  node->ChangePeers(peers, nullptr);

  return butil::Status();
}

butil::Status RaftStoreEngine::StopNode(std::shared_ptr<Context> /*ctx*/, int64_t region_id) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }
  raft_node_manager_->DeleteNode(region_id);

  node->Stop();

  return butil::Status();
}

butil::Status RaftStoreEngine::DestroyNode(std::shared_ptr<Context> /*ctx*/, int64_t region_id) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }
  raft_node_manager_->DeleteNode(region_id);

  node->Destroy();

  return butil::Status();
}

std::shared_ptr<RaftNode> RaftStoreEngine::GetNode(int64_t region_id) { return raft_node_manager_->GetNode(region_id); }

bool RaftStoreEngine::IsLeader(int64_t region_id) {
  auto node = GetNode(region_id);
  if (node == nullptr) {
    return false;
  }

  return node->IsLeader();
}

butil::Status RaftStoreEngine::SaveSnapshot(std::shared_ptr<Context> ctx, int64_t region_id, bool force) {
  ctx->SetRegionId(region_id);
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  auto sync_mode_cond = ctx->CreateSyncModeCond();

  auto status = node->Snapshot(ctx, force);
  if (!status.ok()) {
    return status;
  }

  sync_mode_cond->IncreaseWait();
  if (!ctx->Status().ok()) {
    return ctx->Status();
  }

  return butil::Status();
}

butil::Status RaftStoreEngine::AyncSaveSnapshot(std::shared_ptr<Context> ctx, int64_t region_id, bool force) {
  ctx->SetRegionId(region_id);
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  return node->Snapshot(ctx, force);
}

void RaftStoreEngine::DoSnapshotPeriodicity() {
  auto nodes = raft_node_manager_->GetAllNode();

  for (auto& node : nodes) {
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(node->GetNodeId());
    node->Snapshot(ctx, false);
  }
}

butil::Status RaftStoreEngine::TransferLeader(int64_t region_id, const pb::common::Peer& peer) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  if (!node->IsLeader()) {
    return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
  }

  auto ret = node->TransferLeadershipTo(Helper::LocationToPeerId(peer.raft_location()));
  if (ret != 0) {
    return butil::Status(pb::error::ERAFT_TRANSFER_LEADER, fmt::format("Transfer leader failed, ret_code {}", ret));
  }

  return butil::Status();
}

std::shared_ptr<pb::raft::RaftCmdRequest> GenRaftCmdRequest(const std::shared_ptr<Context> ctx,       // NOLINT
                                                            std::shared_ptr<WriteData> write_data) {  // NOLINT
  std::shared_ptr<pb::raft::RaftCmdRequest> raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();

  pb::raft::RequestHeader* header = raft_cmd->mutable_header();
  header->set_region_id(ctx->RegionId());
  *header->mutable_epoch() = ctx->RegionEpoch();

  auto* requests = raft_cmd->mutable_requests();
  for (auto& datum : write_data->Datums()) {
    requests->AddAllocated(datum->TransformToRaft());
  }

  return raft_cmd;
}

bvar::LatencyRecorder g_raft_write_latency("dingo_raft_store_engine_write_latency");

butil::Status RaftStoreEngine::Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) {
  BvarLatencyGuard bvar_guard(&g_raft_write_latency);

  auto node = raft_node_manager_->GetNode(ctx->RegionId());
  if (BAIDU_UNLIKELY(node == nullptr)) {
    DINGO_LOG(ERROR) << fmt::format("[raft.engine][region({})] not found raft node.", ctx->RegionId());
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  // CAUTION: sync mode cannot pass Done here
  CHECK(ctx->Done() == nullptr) << fmt::format("[raft.engine][region({})] sync mode cannot pass Done here.",
                                               ctx->RegionId());

  auto sync_mode_cond = ctx->CreateSyncModeCond();

  auto status = node->Commit(ctx, GenRaftCmdRequest(ctx, write_data));
  if (BAIDU_UNLIKELY(!status.ok())) {
    return status;
  }

  sync_mode_cond->IncreaseWait();

  if (BAIDU_UNLIKELY(!ctx->Status().ok())) {
    return ctx->Status();
  }

  return butil::Status();
}

butil::Status RaftStoreEngine::AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) {
  return AsyncWrite(ctx, write_data, [](std::shared_ptr<Context> ctx, butil::Status status) {});
}

bvar::LatencyRecorder g_raft_async_write_latency("dingo_raft_store_engine_async_write_latency");

butil::Status RaftStoreEngine::AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
                                          WriteCbFunc cb) {
  BvarLatencyGuard bvar_guard(&g_raft_async_write_latency);

  auto node = raft_node_manager_->GetNode(ctx->RegionId());
  if (node == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[raft.engine][region({})] not found raft node.", ctx->RegionId());
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  ctx->SetWriteCb(cb);
  return node->Commit(ctx, GenRaftCmdRequest(ctx, write_data));
}

butil::Status RaftStoreEngine::Reader::KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) {
  return reader_->KvGet(ctx->CfName(), key, value);
}

butil::Status RaftStoreEngine::Reader::KvScan(std::shared_ptr<Context> ctx, const std::string& start_key,
                                              const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) {
  return reader_->KvScan(ctx->CfName(), start_key, end_key, kvs);
}

butil::Status RaftStoreEngine::Reader::KvCount(std::shared_ptr<Context> ctx, const std::string& start_key,
                                               const std::string& end_key, int64_t& count) {
  return reader_->KvCount(ctx->CfName(), start_key, end_key, count);
}

// vector
butil::Status RaftStoreEngine::VectorReader::VectorBatchSearch(
    std::shared_ptr<VectorReader::Context> ctx, std::vector<pb::index::VectorWithDistanceResult>& results) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchSearch(ctx, results);
}

butil::Status RaftStoreEngine::VectorReader::VectorBatchQuery(std::shared_ptr<VectorReader::Context> ctx,
                                                              std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchQuery(ctx, vector_with_ids);
}

butil::Status RaftStoreEngine::VectorReader::VectorGetBorderId(int64_t ts, const pb::common::Range& region_range,
                                                               bool get_min, int64_t& vector_id) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorGetBorderId(ts, region_range, get_min, vector_id);
}

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
butil::Status RaftStoreEngine::VectorReader::VectorGetBorderIdForDocument(int64_t ts,
                                                                          const pb::common::Range& region_range,
                                                                          bool get_min, int64_t& vector_id) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorGetBorderIdForDocument(ts, region_range, get_min, vector_id);
}
#endif

butil::Status RaftStoreEngine::VectorReader::VectorScanQuery(std::shared_ptr<VectorReader::Context> ctx,
                                                             std::vector<pb::common::VectorWithId>& vector_with_ids) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorScanQuery(ctx, vector_with_ids);
}

butil::Status RaftStoreEngine::VectorReader::VectorGetRegionMetrics(int64_t region_id,
                                                                    const pb::common::Range& region_range,
                                                                    VectorIndexWrapperPtr vector_index,
                                                                    pb::common::VectorIndexMetrics& region_metrics) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorGetRegionMetrics(region_id, region_range, vector_index, region_metrics);
}

butil::Status RaftStoreEngine::VectorReader::VectorCount(int64_t ts, const pb::common::Range& range, int64_t& count) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorCount(ts, range, count);
}

butil::Status RaftStoreEngine::VectorReader::VectorCountMemory(std::shared_ptr<VectorReader::Context> ctx,
                                                               int64_t& count) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorCountMemory(ctx, count);
}

butil::Status RaftStoreEngine::VectorReader::VectorBuild(std::shared_ptr<VectorReader::Context> ctx,
                                                         const pb::common::VectorBuildParameter& parameter, int64_t ts,
                                                         pb::common::VectorStateParameter& vector_state_parameter) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBuild(ctx, parameter, ts, vector_state_parameter);
}

butil::Status RaftStoreEngine::VectorReader::VectorLoad(std::shared_ptr<VectorReader::Context> ctx,
                                                        const pb::common::VectorLoadParameter& parameter,
                                                        pb::common::VectorStateParameter& vector_state_parameter) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorLoad(ctx, parameter, vector_state_parameter);
}

butil::Status RaftStoreEngine::VectorReader::VectorStatus(std::shared_ptr<VectorReader::Context> ctx,
                                                          pb::common::VectorStateParameter& vector_state_parameter,
                                                          pb::error::Error& internal_error) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorStatus(ctx, vector_state_parameter, internal_error);
}

butil::Status RaftStoreEngine::VectorReader::VectorReset(std::shared_ptr<VectorReader::Context> ctx,
                                                         bool delete_data_file,
                                                         pb::common::VectorStateParameter& vector_state_parameter) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorReset(ctx, delete_data_file, vector_state_parameter);
}

butil::Status RaftStoreEngine::VectorReader::VectorDump(std::shared_ptr<VectorReader::Context> ctx, bool dump_all,
                                                        std::vector<std::string>& dump_datas) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorDump(ctx, dump_all, dump_datas);
}

butil::Status RaftStoreEngine::VectorReader::VectorBatchSearchDebug(
    std::shared_ptr<VectorReader::Context> ctx,  // NOLINT
    std::vector<pb::index::VectorWithDistanceResult>& results, int64_t& deserialization_id_time_us,
    int64_t& scan_scalar_time_us, int64_t& search_time_us) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchSearchDebug(ctx, results, deserialization_id_time_us, scan_scalar_time_us,
                                               search_time_us);
}

// document
butil::Status RaftStoreEngine::DocumentReader::DocumentSearch(std::shared_ptr<DocumentReader::Context> ctx,
                                                              std::vector<pb::common::DocumentWithScore>& results) {
  auto document_reader = dingodb::DocumentReader::New(reader_);
  return document_reader->DocumentSearch(ctx, results);
}

butil::Status RaftStoreEngine::DocumentReader::DocumentSearchAll(std::shared_ptr<DocumentReader::Context> ctx,
                                                                 bool& has_more,
                                                                 std::vector<pb::common::DocumentWithScore>& results) {
  auto document_reader = dingodb::DocumentReader::New(reader_);
  return document_reader->DocumentSearchAll(ctx, has_more, results);
}

butil::Status RaftStoreEngine::DocumentReader::DocumentBatchQuery(
    std::shared_ptr<DocumentReader::Context> ctx, std::vector<pb::common::DocumentWithId>& document_with_ids) {
  auto document_reader = dingodb::DocumentReader::New(reader_);
  return document_reader->DocumentBatchQuery(ctx, document_with_ids);
}

butil::Status RaftStoreEngine::DocumentReader::DocumentGetBorderId(int64_t ts, const pb::common::Range& region_range,
                                                                   bool get_min, int64_t& document_id) {
  auto document_reader = dingodb::DocumentReader::New(reader_);
  return document_reader->DocumentGetBorderId(ts, region_range, get_min, document_id);
}

butil::Status RaftStoreEngine::DocumentReader::DocumentScanQuery(
    std::shared_ptr<DocumentReader::Context> ctx, std::vector<pb::common::DocumentWithId>& document_with_ids) {
  auto document_reader = dingodb::DocumentReader::New(reader_);
  return document_reader->DocumentScanQuery(ctx, document_with_ids);
}

butil::Status RaftStoreEngine::DocumentReader::DocumentGetRegionMetrics(
    int64_t region_id, const pb::common::Range& region_range, DocumentIndexWrapperPtr document_index,
    pb::common::DocumentIndexMetrics& region_metrics) {
  auto document_reader = dingodb::DocumentReader::New(reader_);
  return document_reader->DocumentGetRegionMetrics(region_id, region_range, document_index, region_metrics);
}

butil::Status RaftStoreEngine::DocumentReader::DocumentCount(int64_t ts, const pb::common::Range& range,
                                                             int64_t& count) {
  auto document_reader = dingodb::DocumentReader::New(reader_);
  return document_reader->DocumentCount(ts, range, count);
}

butil::Status RaftStoreEngine::TxnReader::TxnBatchGet(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                      const std::vector<std::string>& keys,
                                                      std::vector<pb::common::KeyValue>& kvs,
                                                      const std::set<int64_t>& resolved_locks,
                                                      pb::store::TxnResultInfo& txn_result_info) {
  return TxnEngineHelper::BatchGet(txn_reader_raw_engine_, ctx->IsolationLevel(), start_ts, keys, resolved_locks,
                                   txn_result_info, kvs);
}

butil::Status RaftStoreEngine::TxnReader::TxnScan(
    std::shared_ptr<Context> ctx, int64_t start_ts, const pb::common::Range& range, int64_t limit, bool key_only,
    bool is_reverse, const std::set<int64_t>& resolved_locks, bool disable_coprocessor,
    const pb::common::CoprocessorV2& coprocessor, pb::store::TxnResultInfo& txn_result_info,
    std::vector<pb::common::KeyValue>& kvs, bool& has_more, std::string& end_scan_key) {
  return TxnEngineHelper::Scan(ctx->Stream(), txn_reader_raw_engine_, ctx->IsolationLevel(), start_ts, range, limit,
                               key_only, is_reverse, resolved_locks, disable_coprocessor, coprocessor, txn_result_info,
                               kvs, has_more, end_scan_key);
}

butil::Status RaftStoreEngine::TxnReader::TxnScanLock(std::shared_ptr<Context> ctx, int64_t min_lock_ts,
                                                      int64_t max_lock_ts, const pb::common::Range& range,
                                                      int64_t limit, std::vector<pb::store::LockInfo>& lock_infos,
                                                      bool& has_more, std::string& end_scan_key) {
  return TxnEngineHelper::ScanLockInfo(ctx->Stream(), txn_reader_raw_engine_, min_lock_ts, max_lock_ts, range, limit,
                                       lock_infos, has_more, end_scan_key);
}

butil::Status RaftStoreEngine::TxnWriter::TxnPessimisticLock(std::shared_ptr<Context> ctx,
                                                             const std::vector<pb::store::Mutation>& mutations,
                                                             const std::string& primary_lock, int64_t start_ts,
                                                             int64_t lock_ttl, int64_t for_update_ts,
                                                             bool return_values,
                                                             std::vector<pb::common::KeyValue>& kvs) {
  return TxnEngineHelper::PessimisticLock(txn_writer_raw_engine_, raft_engine_, ctx, mutations, primary_lock, start_ts,
                                          lock_ttl, for_update_ts, return_values, kvs);
}

butil::Status RaftStoreEngine::TxnWriter::TxnPessimisticRollback(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                                 int64_t start_ts, int64_t for_update_ts,
                                                                 const std::vector<std::string>& keys) {
  return TxnEngineHelper::PessimisticRollback(txn_writer_raw_engine_, raft_engine_, ctx, region, start_ts,
                                              for_update_ts, keys);
}

butil::Status RaftStoreEngine::TxnWriter::TxnPrewrite(
    std::shared_ptr<Context> ctx, store::RegionPtr region, const std::vector<pb::store::Mutation>& mutations,
    const std::string& primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size, bool try_one_pc,
    int64_t min_commit_ts, int64_t max_commit_ts, const std::vector<int64_t>& pessimistic_checks,
    const std::map<int64_t, int64_t>& for_update_ts_checks, const std::map<int64_t, std::string>& lock_extra_datas,
    const std::vector<std::string>& secondaries) {
  return TxnEngineHelper::Prewrite(txn_writer_raw_engine_, raft_engine_, ctx, region, mutations, primary_lock, start_ts,
                                   lock_ttl, txn_size, try_one_pc, min_commit_ts, max_commit_ts, pessimistic_checks,
                                   for_update_ts_checks, lock_extra_datas, secondaries);
}

butil::Status RaftStoreEngine::TxnWriter::TxnCommit(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                    int64_t start_ts, int64_t commit_ts,
                                                    const std::vector<std::string>& keys) {
  return TxnEngineHelper::Commit(txn_writer_raw_engine_, raft_engine_, ctx, region, start_ts, commit_ts, keys);
}

butil::Status RaftStoreEngine::TxnWriter::TxnCheckTxnStatus(std::shared_ptr<Context> ctx,
                                                            const std::string& primary_key, int64_t lock_ts,
                                                            int64_t caller_start_ts, int64_t current_ts,
                                                            bool force_sync_commit) {
  return TxnEngineHelper::CheckTxnStatus(txn_writer_raw_engine_, raft_engine_, ctx, primary_key, lock_ts,
                                         caller_start_ts, current_ts, force_sync_commit);
}

butil::Status RaftStoreEngine::TxnWriter::TxnCheckSecondaryLocks(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                                                 int64_t start_ts,
                                                                 const std::vector<std::string>& keys) {
  return TxnEngineHelper::TxnCheckSecondaryLocks(txn_writer_raw_engine_, ctx, region, start_ts, keys);
}

butil::Status RaftStoreEngine::TxnWriter::TxnResolveLock(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                         int64_t commit_ts, const std::vector<std::string>& keys) {
  return TxnEngineHelper::ResolveLock(txn_writer_raw_engine_, raft_engine_, ctx, start_ts, commit_ts, keys);
}

butil::Status RaftStoreEngine::TxnWriter::TxnBatchRollback(std::shared_ptr<Context> ctx, int64_t start_ts,
                                                           const std::vector<std::string>& keys) {
  return TxnEngineHelper::BatchRollback(txn_writer_raw_engine_, raft_engine_, ctx, start_ts, keys);
}

butil::Status RaftStoreEngine::TxnWriter::TxnHeartBeat(std::shared_ptr<Context> ctx, const std::string& primary_lock,
                                                       int64_t start_ts, int64_t advise_lock_ttl) {
  return TxnEngineHelper::HeartBeat(txn_writer_raw_engine_, raft_engine_, ctx, primary_lock, start_ts, advise_lock_ttl);
}

butil::Status RaftStoreEngine::TxnWriter::TxnDeleteRange(std::shared_ptr<Context> ctx, const std::string& start_key,
                                                         const std::string& end_key) {
  return TxnEngineHelper::DeleteRange(txn_writer_raw_engine_, raft_engine_, ctx, start_key, end_key);
}

butil::Status RaftStoreEngine::TxnWriter::TxnGc(std::shared_ptr<Context> ctx, int64_t safe_point_ts) {
  return TxnEngineHelper::Gc(txn_writer_raw_engine_, raft_engine_, ctx, safe_point_ts);
}

butil::Status RaftStoreEngine::Writer::KvPut(std::shared_ptr<Context> ctx,
                                             const std::vector<pb::common::KeyValue>& kvs) {
  int64_t ts = ts_provider_->GetTs();
  if (BAIDU_UNLIKELY(ts == 0)) {
    return butil::Status(pb::error::ETSO_NOT_AVAILABLE, "TSO not available");
  }
  auto encode_kvs = ctx->Ttl() == 0 ? mvcc::Codec::EncodeKeyValuesWithPut(ts, kvs)
                                    : mvcc::Codec::EncodeKeyValuesWithTTL(ts, ctx->Ttl(), kvs);

  auto status = raft_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), encode_kvs, ts));
  if (!status.ok()) {
    return status;
  }

  if (ctx->Response() && kvs.size() == 1) {
    auto* response = dynamic_cast<pb::store::KvPutResponse*>(ctx->Response());
    if (BAIDU_LIKELY(response != nullptr)) {
      response->set_ts(ts);
    } else {
      auto* response = dynamic_cast<pb::store::KvBatchPutResponse*>(ctx->Response());
      CHECK(response != nullptr) << "KvBatchPutResponse is nullptr.";
      response->set_ts(ts);
    }

  } else if (ctx->Response() && kvs.size() > 1) {
    auto* response = dynamic_cast<pb::store::KvBatchPutResponse*>(ctx->Response());
    CHECK(response != nullptr) << "KvBatchPutResponse is nullptr.";
    response->set_ts(ts);
  }

  return butil::Status::OK();
}

butil::Status RaftStoreEngine::Writer::KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                                                std::vector<bool>& key_states) {
  int64_t ts = ts_provider_->GetTs();
  if (BAIDU_UNLIKELY(ts == 0)) {
    return butil::Status(pb::error::ETSO_NOT_AVAILABLE, "TSO not available");
  }
  auto reader = raft_engine_->NewMVCCReader(ctx->RawEngineType());

  key_states.resize(keys.size(), false);
  for (int i = 0; i < keys.size(); ++i) {
    const auto& key = keys[i];
    std::string value;
    auto status = reader->KvGet(ctx->CfName(), ctx->Ts(), key, value);
    if (status.ok()) {
      key_states[i] = true;
    }
  }

  auto encode_keys = mvcc::Codec::EncodeKeys(ts, keys);

  return raft_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), encode_keys, ts));
}

butil::Status RaftStoreEngine::Writer::KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) {
  auto encode_range = mvcc::Codec::EncodeRange(range);

  return raft_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), encode_range));
}

butil::Status RaftStoreEngine::Writer::KvPutIfAbsent(std::shared_ptr<Context> ctx,
                                                     const std::vector<pb::common::KeyValue>& kvs, bool is_atomic,
                                                     std::vector<bool>& key_states) {
  if (BAIDU_UNLIKELY(kvs.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  int64_t ts = ts_provider_->GetTs();
  if (BAIDU_UNLIKELY(ts == 0)) {
    return butil::Status(pb::error::ETSO_NOT_AVAILABLE, "TSO not available");
  }

  key_states.resize(kvs.size(), false);
  std::vector<bool> temp_key_states(kvs.size(), false);

  auto reader = raft_engine_->NewMVCCReader(ctx->RawEngineType());
  std::vector<pb::common::KeyValue> put_kvs;
  for (int i = 0; i < kvs.size(); ++i) {
    const auto& kv = kvs[i];
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    std::string old_value;
    auto status = reader->KvGet(ctx->CfName(), 0, kv.key(), old_value);
    if (!status.ok() && status.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
      return butil::Status(pb::error::EINTERNAL, "Internal error");
    }

    if (is_atomic) {
      if (status.ok()) {
        return butil::Status();
      }
    } else {
      if (status.ok()) {
        continue;
      }
    }

    pb::common::KeyValue encode_kv;
    encode_kv.set_key(mvcc::Codec::EncodeKey(kv.key(), ts));
    if (ctx->Ttl() == 0) {
      mvcc::Codec::PackageValue(mvcc::ValueFlag::kPut, kv.value(), *encode_kv.mutable_value());
    } else {
      mvcc::Codec::PackageValue(mvcc::ValueFlag::kPutTTL, ctx->Ttl(), kv.value(), *encode_kv.mutable_value());
    }

    put_kvs.push_back(std::move(encode_kv));
    temp_key_states[i] = true;
  }

  if (put_kvs.empty()) {
    return butil::Status::OK();
  }

  auto status = raft_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), put_kvs, ts));
  if (!status.ok()) {
    return status;
  }

  key_states = temp_key_states;

  if (ctx->Response() && kvs.size() == 1) {
    auto* response = dynamic_cast<pb::store::KvPutIfAbsentResponse*>(ctx->Response());
    if (BAIDU_LIKELY(response != nullptr)) {
      response->set_ts(ts);
    } else {
      auto* response = dynamic_cast<pb::store::KvBatchPutIfAbsentResponse*>(ctx->Response());
      CHECK(response != nullptr) << "KvBatchPutIfAbsentResponse is nullptr.";
      response->set_ts(ts);
    }
  } else if (ctx->Response() && kvs.size() > 1) {
    auto* response = dynamic_cast<pb::store::KvBatchPutIfAbsentResponse*>(ctx->Response());
    CHECK(response != nullptr) << "KvBatchPutIfAbsentResponse is nullptr.";
    response->set_ts(ts);
  }

  return butil::Status();
}

butil::Status RaftStoreEngine::Writer::KvCompareAndSet(std::shared_ptr<Context> ctx,
                                                       const std::vector<pb::common::KeyValue>& kvs,
                                                       const std::vector<std::string>& expect_values, bool is_atomic,
                                                       std::vector<bool>& key_states) {
  if (BAIDU_UNLIKELY(kvs.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }
  if (BAIDU_UNLIKELY(kvs.size() != expect_values.size())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is mismatch");
  }

  int64_t ts = ts_provider_->GetTs();
  if (BAIDU_UNLIKELY(ts == 0)) {
    return butil::Status(pb::error::ETSO_NOT_AVAILABLE, "TSO not available");
  }

  key_states.resize(kvs.size(), false);
  std::vector<bool> temp_key_states(kvs.size(), false);

  auto reader = raft_engine_->NewMVCCReader(ctx->RawEngineType());
  std::vector<pb::common::KeyValue> put_kvs;
  for (int i = 0; i < kvs.size(); ++i) {
    const auto& kv = kvs[i];
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    std::string old_value;
    auto status = reader->KvGet(ctx->CfName(), 0, kv.key(), old_value);
    if (!status.ok() && status.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
      return butil::Status(pb::error::EINTERNAL, "Internal error");
    }

    if (is_atomic) {
      if (status.ok()) {
        if (old_value != expect_values[i]) {
          return butil::Status();
        }
      } else if (status.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
        if (!expect_values[i].empty()) {
          return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key");
        }
      }
    } else {
      if (status.ok()) {
        if (old_value != expect_values[i]) {
          continue;
        }
      } else if (status.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
        if (!expect_values[i].empty()) {
          continue;
        }
      }
    }

    pb::common::KeyValue encode_kv;
    encode_kv.set_key(mvcc::Codec::EncodeKey(kv.key(), ts));

    // value empty means delete
    if (kv.value().empty()) {
      mvcc::Codec::PackageValue(mvcc::ValueFlag::kDelete, *encode_kv.mutable_value());
    } else if (ctx->Ttl() == 0) {
      mvcc::Codec::PackageValue(mvcc::ValueFlag::kPut, kv.value(), *encode_kv.mutable_value());
    } else {
      mvcc::Codec::PackageValue(mvcc::ValueFlag::kPutTTL, ctx->Ttl(), kv.value(), *encode_kv.mutable_value());
    }

    put_kvs.push_back(std::move(encode_kv));

    temp_key_states[i] = true;
  }

  if (put_kvs.empty()) {
    return butil::Status::OK();
  }

  auto status = raft_engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), put_kvs, ts));
  if (!status.ok()) {
    return status;
  }

  key_states = temp_key_states;

  if (ctx->Response() && kvs.size() == 1) {
    auto* response = dynamic_cast<pb::store::KvCompareAndSetResponse*>(ctx->Response());
    if (BAIDU_LIKELY(response != nullptr)) {
      response->set_ts(ts);
    } else {
      auto* response = dynamic_cast<pb::store::KvBatchCompareAndSetResponse*>(ctx->Response());
      CHECK(response != nullptr) << "KvBatchCompareAndSetResponse is nullptr.";
      response->set_ts(ts);
    }
  } else if (ctx->Response() && kvs.size() > 1) {
    auto* response = dynamic_cast<pb::store::KvBatchCompareAndSetResponse*>(ctx->Response());
    CHECK(response != nullptr) << "KvBatchCompareAndSetResponse is nullptr.";
    response->set_ts(ts);
  }

  return butil::Status();
}

mvcc::ReaderPtr RaftStoreEngine::NewMVCCReader(pb::common::RawEngine type) {
  return std::make_shared<mvcc::KvReader>(GetRawEngine(type)->Reader());
}

Engine::ReaderPtr RaftStoreEngine::NewReader(pb::common::RawEngine type) {
  return std::make_shared<RaftStoreEngine::Reader>(GetRawEngine(type)->Reader());
}

Engine::WriterPtr RaftStoreEngine::NewWriter(pb::common::RawEngine) {
  return std::make_shared<RaftStoreEngine::Writer>(GetSelfPtr(), ts_provider_);
}

Engine::VectorReaderPtr RaftStoreEngine::NewVectorReader(pb::common::RawEngine type) {
  return std::make_shared<RaftStoreEngine::VectorReader>(mvcc::VectorReader::New(GetRawEngine(type)->Reader()));
}

Engine::DocumentReaderPtr RaftStoreEngine::NewDocumentReader(pb::common::RawEngine type) {
  return std::make_shared<RaftStoreEngine::DocumentReader>(mvcc::DocumentReader::New(GetRawEngine(type)->Reader()));
}

// txn
Engine::TxnReaderPtr RaftStoreEngine::NewTxnReader(pb::common::RawEngine type) {
  return std::make_shared<RaftStoreEngine::TxnReader>(GetRawEngine(type));
}

Engine::TxnWriterPtr RaftStoreEngine::NewTxnWriter(pb::common::RawEngine type) {
  return std::make_shared<RaftStoreEngine::TxnWriter>(GetRawEngine(type), GetSelfPtr());
}

}  // namespace dingodb

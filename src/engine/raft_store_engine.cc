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
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "braft/raft.h"
#include "butil/endpoint.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "config/config_manager.h"
#include "engine/engine.h"
#include "engine/snapshot.h"
#include "engine/txn_engine_helper.h"
#include "engine/write_data.h"
#include "event/store_state_machine_event.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/raft.pb.h"
#include "raft/meta_state_machine.h"
#include "raft/store_state_machine.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_reader.h"

namespace dingodb {

RaftStoreEngine::RaftStoreEngine(std::shared_ptr<RawEngine> engine)
    : engine_(engine), raft_node_manager_(std::move(std::make_unique<RaftNodeManager>())) {}

RaftStoreEngine::~RaftStoreEngine() = default;

bool RaftStoreEngine::Init(std::shared_ptr<Config> /*config*/) { return true; }

// Clean region raft directory
static bool CleanRaftDirectory(int64_t region_id, const std::string& raft_path, const std::string& raft_log_path) {
  std::string region_raft_path = fmt::format("{}/{}", raft_path, region_id);
  if (!Helper::RemoveAllFileOrDirectory(region_raft_path)) {
    return false;
  }

  std::string region_raft_log_path = fmt::format("{}/{}", raft_log_path, region_id);
  return Helper::RemoveAllFileOrDirectory(region_raft_log_path);
}

// check region raft complete
static bool IsCompleteRaftNode(int64_t region_id, const std::string& raft_path, const std::string& raft_log_path) {
  std::string raft_meta_path = fmt::format("{}/{}/raft_meta/raft_meta", raft_path, region_id);
  if (!Helper::IsExistPath(raft_meta_path)) {
    DINGO_LOG(WARNING) << fmt::format("[raft.engine][region({})] missing raft_meta file.", region_id);
    return false;
  }

  std::string region_raft_log_path = fmt::format("{}/{}/log_meta", raft_log_path, region_id);
  if (!Helper::IsExistPath(region_raft_log_path)) {
    DINGO_LOG(WARNING) << fmt::format("[raft.engine][region({})] missing raft log file.", region_id);
    return false;
  }

  return true;
}

// Recover raft node from region meta data.
// Invoke when server starting.
bool RaftStoreEngine::Recover() {
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto store_raft_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta();
  auto store_region_metrics = Server::GetInstance()->GetStoreMetricsManager()->GetStoreRegionMetrics();
  auto config = Server::GetInstance()->GetConfig();
  auto regions = store_region_meta->GetAllRegion();

  int count = 0;
  auto ctx = std::make_shared<Context>();
  auto listener_factory = std::make_shared<StoreSmEventListenerFactory>();
  for (auto& region : regions) {
    if (region->State() == pb::common::StoreRegionState::NORMAL ||
        region->State() == pb::common::StoreRegionState::STANDBY ||
        region->State() == pb::common::StoreRegionState::SPLITTING ||
        region->State() == pb::common::StoreRegionState::MERGING) {
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
      parameter.role = Server::GetInstance()->GetRole();
      parameter.is_restart = true;
      parameter.raft_endpoint = Server::GetInstance()->RaftEndpoint();

      parameter.raft_path = config->GetString("raft.path");
      parameter.election_timeout_ms = config->GetInt("raft.election_timeout_s") * 1000;
      parameter.snapshot_interval_s = config->GetInt("raft.snapshot_interval_s");
      parameter.log_max_segment_size = config->GetInt64("raft.segmentlog_max_segment_size");
      parameter.log_path = config->GetString("raft.log_path");

      parameter.raft_meta = raft_meta;
      parameter.region_metrics = region_metrics;
      parameter.listeners = listener_factory->Build();

      auto is_complete = IsCompleteRaftNode(region->Id(), parameter.raft_path, parameter.log_path);
      if (!is_complete) {
        DINGO_LOG(INFO) << fmt::format("[raft.engine][region({})] raft node is not complete.", region->Id());
        if (!CleanRaftDirectory(region->Id(), parameter.raft_path, parameter.log_path)) {
          DINGO_LOG(WARNING) << fmt::format("[raft.engine][region({})] clean region raft directory failed.",
                                            region->Id());
          continue;
        }
        raft_meta = StoreRaftMeta::NewRaftMeta(region->Id());
        store_raft_meta->UpdateRaftMeta(raft_meta);
        parameter.raft_meta = raft_meta;
        parameter.is_restart = false;
      }

      AddNode(region, parameter, true);
      if (region->NeedBootstrapDoSnapshot()) {
        DINGO_LOG(INFO) << fmt::format("[raft.engine][region({})] need do snapshot.", region->Id());
        auto node = GetNode(region->Id());
        if (node != nullptr) {
          node->Snapshot(new SplitHandler::SplitClosure(region));
        }
      }

      ++count;
    }
  }

  DINGO_LOG(INFO) << fmt::format("[raft.engine][region(*)] recover Raft node num({}).", count);

  return true;
}

std::string RaftStoreEngine::GetName() { return pb::common::Engine_Name(pb::common::ENG_RAFT_STORE); }

pb::common::Engine RaftStoreEngine::GetID() { return pb::common::ENG_RAFT_STORE; }

std::shared_ptr<RawEngine> RaftStoreEngine::GetRawEngine() { return engine_; }

butil::Status RaftStoreEngine::AddNode(store::RegionPtr region, const AddNodeParameter& parameter, bool is_recover) {
  DINGO_LOG(INFO) << fmt::format("[raft.engine][region({})] add region.", region->Id());

  // Build StateMachine
  auto state_machine = std::make_shared<StoreStateMachine>(
      engine_, region, parameter.raft_meta, parameter.region_metrics, parameter.listeners, parameter.is_restart);
  if (!state_machine->Init()) {
    return butil::Status(pb::error::ERAFT_INIT, "State machine init failed");
  }

  // Build log storage
  std::string log_path = fmt::format("{}/{}", parameter.log_path, region->Id());
  int64_t max_segment_size =
      parameter.log_max_segment_size > 0 ? parameter.log_max_segment_size : Constant::kSegmentLogDefaultMaxSegmentSize;
  auto log_storage = std::make_shared<SegmentLogStorage>(log_path, region->Id(), max_segment_size);
  Server::GetInstance()->GetLogStorageManager()->AddLogStorage(region->Id(), log_storage);

  // Build RaftNode
  auto node = std::make_shared<RaftNode>(region->Id(), region->Name(), braft::PeerId(parameter.raft_endpoint),
                                         state_machine, log_storage);

  if (node->Init(Helper::FormatPeers(Helper::ExtractLocations(region->Peers())), parameter.raft_path,
                 parameter.election_timeout_ms, parameter.snapshot_interval_s) != 0) {
    if (is_recover) {
      DINGO_LOG(FATAL) << fmt::format("[raft.engine][region({})] Raft init failed. Please check raft storage!",
                                      region->Id())
                       << ", raft_path: " << parameter.raft_path
                       << ", election_timeout_ms: " << parameter.election_timeout_ms
                       << ", snapshot_interval_s: " << parameter.snapshot_interval_s
                       << ", peers: " << Helper::FormatPeers(Helper::ExtractLocations(region->Peers()));
    } else {
      node->Destroy();
    }
    return butil::Status(pb::error::ERAFT_INIT, "Raft init failed");
  }

  raft_node_manager_->AddNode(region->Id(), node);
  return butil::Status();
}

butil::Status RaftStoreEngine::AddNode(std::shared_ptr<pb::common::RegionDefinition> region,
                                       std::shared_ptr<MetaControl> meta_control, bool is_volatile) {
  DINGO_LOG(INFO) << fmt::format("[raft.engine][region({})] add region.", region->id());

  // Build StatMachine
  auto state_machine = std::make_shared<MetaStateMachine>(meta_control, is_volatile);

  // Build log storage
  auto config = Server::GetInstance()->GetConfig();
  std::string log_path = fmt::format("{}/{}", config->GetString("raft.log_path"), region->id());
  int64_t max_segment_size = config->GetInt64("raft.segmentlog_max_segment_size");
  max_segment_size = max_segment_size > 0 ? max_segment_size : Constant::kSegmentLogDefaultMaxSegmentSize;
  auto log_storage = std::make_shared<SegmentLogStorage>(log_path, region->id(), max_segment_size);
  Server::GetInstance()->GetLogStorageManager()->AddLogStorage(region->id(), log_storage);

  std::string const meta_raft_name = fmt::format("{}-{}", region->name(), region->id());
  auto const node = std::make_shared<RaftNode>(
      region->id(), meta_raft_name, braft::PeerId(Server::GetInstance()->RaftEndpoint()), state_machine, log_storage);

  // Build RaftNode
  if (node->Init(Helper::FormatPeers(Helper::ExtractLocations(region->peers())), config->GetString("raft.path"),
                 config->GetInt("raft.election_timeout_s") * 1000, config->GetInt("raft.snapshot_interval_s")) != 0) {
    // node->Destroy();
    // this function is only used by coordinator, and will only be called on starting.
    // so if init failed, we can just exit the process, let user to check if the config is correct.
    DINGO_LOG(FATAL) << fmt::format("[raft.engine][region({})] Raft init failed. Please check raft storage!",
                                    region->id())
                     << ", raft_path: " << config->GetString("raft.path")
                     << ", election_timeout_ms: " << config->GetInt("raft.election_timeout_s") * 1000
                     << ", snapshot_interval_s: " << config->GetInt("raft.snapshot_interval_s")
                     << ", peers: " << Helper::FormatPeers(Helper::ExtractLocations(region->peers()))
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

butil::Status RaftStoreEngine::DoSnapshot(std::shared_ptr<Context> ctx, int64_t region_id) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  node->Snapshot(dynamic_cast<braft::Closure*>(ctx->Done()));
  return butil::Status();
}

butil::Status RaftStoreEngine::TransferLeader(int64_t region_id, const pb::common::Peer& peer) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  if (!node->IsLeader()) {
    return butil::Status(pb::error::ERAFT_NOTLEADER, node->GetLeaderId().to_string());
  }

  auto ret = node->TransferLeadershipTo(Helper::LocationToPeer(peer.raft_location()));
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

  auto* requests = raft_cmd->mutable_requests();
  for (auto& datum : write_data->Datums()) {
    requests->AddAllocated(datum->TransformToRaft());
  }

  return raft_cmd;
}

butil::Status RaftStoreEngine::Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) {
  auto node = raft_node_manager_->GetNode(ctx->RegionId());
  if (node == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[raft.engine][region({})] not found raft node.", ctx->RegionId());
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  auto s = node->Commit(ctx, GenRaftCmdRequest(ctx, write_data));
  if (!s.ok()) {
    return s;
  }

  ctx->EnableSyncMode();
  ctx->Cond()->IncreaseWait();

  if (!ctx->Status().ok()) {
    return ctx->Status();
  }
  return butil::Status();
}

butil::Status RaftStoreEngine::AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) {
  return AsyncWrite(ctx, write_data, [](std::shared_ptr<Context> ctx, butil::Status status) {});
}

butil::Status RaftStoreEngine::AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
                                          WriteCbFunc cb) {
  auto node = raft_node_manager_->GetNode(ctx->RegionId());
  if (node == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[raft.engine][region({})] not found raft node.", ctx->RegionId());
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  ctx->SetWriteCb(cb);
  return node->Commit(ctx, GenRaftCmdRequest(ctx, write_data));
}

butil::Status RaftStoreEngine::Reader::KvGet(std::shared_ptr<Context> /*ctx*/, const std::string& key,
                                             std::string& value) {
  return reader_->KvGet(key, value);
}

butil::Status RaftStoreEngine::Reader::KvScan(std::shared_ptr<Context> /*ctx*/, const std::string& start_key,
                                              const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) {
  return reader_->KvScan(start_key, end_key, kvs);
}

butil::Status RaftStoreEngine::Reader::KvCount(std::shared_ptr<Context> /*ctx*/, const std::string& start_key,
                                               const std::string& end_key, int64_t& count) {
  return reader_->KvCount(start_key, end_key, count);
}

std::shared_ptr<Engine::Reader> RaftStoreEngine::NewReader(const std::string& cf_name) {
  return std::make_shared<RaftStoreEngine::Reader>(engine_->NewReader(cf_name));
}

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

butil::Status RaftStoreEngine::VectorReader::VectorGetBorderId(const pb::common::Range& region_range, bool get_min,
                                                               int64_t& vector_id) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorGetBorderId(region_range, get_min, vector_id);
}

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

butil::Status RaftStoreEngine::VectorReader::VectorCount(const pb::common::Range& range, int64_t& count) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorCount(range, count);
}

butil::Status RaftStoreEngine::VectorReader::VectorBatchSearchDebug(
    std::shared_ptr<VectorReader::Context> ctx,  // NOLINT
    std::vector<pb::index::VectorWithDistanceResult>& results, int64_t& deserialization_id_time_us,
    int64_t& scan_scalar_time_us, int64_t& search_time_us) {
  auto vector_reader = dingodb::VectorReader::New(reader_);
  return vector_reader->VectorBatchSearchDebug(ctx, results, deserialization_id_time_us, scan_scalar_time_us,
                                               search_time_us);
}

std::shared_ptr<Engine::VectorReader> RaftStoreEngine::NewVectorReader(const std::string& cf_name) {
  return std::make_shared<RaftStoreEngine::VectorReader>(engine_->NewReader(cf_name));
}

std::shared_ptr<Engine::TxnReader> RaftStoreEngine::NewTxnReader() {
  return std::make_shared<RaftStoreEngine::TxnReader>(engine_);
}

butil::Status RaftStoreEngine::TxnReader::TxnBatchGet(std::shared_ptr<Context> ctx, uint64_t start_ts,
                                                      const std::vector<std::string>& keys,
                                                      std::vector<pb::common::KeyValue>& kvs,
                                                      pb::store::TxnResultInfo& txn_result_info) {
  return TxnEngineHelper::BatchGet(engine_, ctx->IsolationLevel(), start_ts, keys, kvs, txn_result_info);
}

butil::Status RaftStoreEngine::TxnReader::TxnScan(std::shared_ptr<Context> ctx, uint64_t start_ts,
                                                  const pb::common::Range& range, uint64_t limit, bool key_only,
                                                  bool is_reverse, bool disable_coprocessor,
                                                  const pb::store::Coprocessor& coprocessor,
                                                  pb::store::TxnResultInfo& txn_result_info,
                                                  std::vector<pb::common::KeyValue>& kvs, bool& has_more,
                                                  std::string& end_key) {
  return TxnEngineHelper::Scan(engine_, ctx->IsolationLevel(), start_ts, range, limit, key_only, is_reverse,
                               disable_coprocessor, coprocessor, txn_result_info, kvs, has_more, end_key);
}

butil::Status RaftStoreEngine::TxnReader::TxnScanLock(std::shared_ptr<Context> /*ctx*/, uint64_t min_lock_ts,
                                                      uint64_t max_lock_ts, const pb::common::Range& range,
                                                      uint64_t limit, std::vector<pb::store::LockInfo>& lock_infos) {
  return TxnEngineHelper::ScanLockInfo(engine_, min_lock_ts, max_lock_ts, range.start_key(), range.end_key(), limit,
                                       lock_infos);
}

}  // namespace dingodb

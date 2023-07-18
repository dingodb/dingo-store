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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "braft/raft.h"
#include "butil/endpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "config/config_manager.h"
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

namespace dingodb {

RaftStoreEngine::RaftStoreEngine(std::shared_ptr<RawEngine> engine)
    : engine_(engine), raft_node_manager_(std::move(std::make_unique<RaftNodeManager>())) {}

RaftStoreEngine::~RaftStoreEngine() = default;

bool RaftStoreEngine::Init(std::shared_ptr<Config> /*config*/) { return true; }

// Recover raft node from region meta data.
// Invoke when server starting.
bool RaftStoreEngine::Recover() {
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  auto store_raft_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta();
  auto store_region_metrics = Server::GetInstance()->GetStoreMetricsManager()->GetStoreRegionMetrics();
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
        DINGO_LOG(ERROR) << "Recover raft meta not found: " << region->Id();
        continue;
      }
      auto region_metrics = store_region_metrics->GetMetrics(region->Id());
      if (region_metrics == nullptr) {
        DINGO_LOG(WARNING) << "Recover region metrics not found: " << region->Id();
      }

      AddNode(ctx, region, raft_meta, region_metrics, listener_factory->Build(), true);
      ++count;
    }
  }

  DINGO_LOG(INFO) << "Recover Raft node num: " << count;

  return true;
}

std::string RaftStoreEngine::GetName() { return pb::common::Engine_Name(pb::common::ENG_RAFT_STORE); }

pb::common::Engine RaftStoreEngine::GetID() { return pb::common::ENG_RAFT_STORE; }

std::shared_ptr<RawEngine> RaftStoreEngine::GetRawEngine() { return engine_; }

butil::Status RaftStoreEngine::AddNode(std::shared_ptr<Context> /*ctx*/, store::RegionPtr region,
                                       std::shared_ptr<pb::store_internal::RaftMeta> raft_meta,
                                       store::RegionMetricsPtr region_metrics,
                                       std::shared_ptr<EventListenerCollection> listeners, bool is_restart) {
  DINGO_LOG(INFO) << "RaftkvEngine add region, region_id " << region->Id();

  // Build StateMachine
  auto state_machine =
      std::make_shared<StoreStateMachine>(engine_, region, raft_meta, region_metrics, listeners, is_restart);
  if (!state_machine->Init()) {
    return butil::Status(pb::error::ERAFT_INIT, "State machine init failed");
  }

  // Build log storage
  auto config = ConfigManager::GetInstance()->GetConfig(Server::GetInstance()->GetRole());
  std::string log_path = fmt::format("{}/{}", config->GetString("raft.log_path"), region->Id());
  auto log_storage = std::make_shared<SegmentLogStorage>(log_path, region->Id(),
                                                         Server::GetInstance()->GetRole() == pb::common::INDEX);
  Server::GetInstance()->GetLogStorageManager()->AddLogStorage(region->Id(), log_storage);

  // Build RaftNode
  std::shared_ptr<RaftNode> node = std::make_shared<RaftNode>(
      region->Id(), region->Name(), braft::PeerId(Server::GetInstance()->RaftEndpoint()), state_machine, log_storage);

  if (node->Init(Helper::FormatPeers(Helper::ExtractLocations(region->Peers())), config) != 0) {
    node->Destroy();
    return butil::Status(pb::error::ERAFT_INIT, "Raft init failed");
  }

  raft_node_manager_->AddNode(region->Id(), node);
  return butil::Status();
}

butil::Status RaftStoreEngine::AddNode(std::shared_ptr<pb::common::RegionDefinition> region,
                                       std::shared_ptr<MetaControl> meta_control, bool is_volatile) {
  DINGO_LOG(INFO) << "RaftkvEngine add region, region_id " << region->id();

  // Build StatMachine
  auto state_machine = std::make_shared<MetaStateMachine>(meta_control, is_volatile);

  // Build log storage
  auto config = ConfigManager::GetInstance()->GetConfig(Server::GetInstance()->GetRole());
  std::string log_path = fmt::format("{}/{}", config->GetString("raft.log_path"), region->id());
  auto log_storage = std::make_shared<SegmentLogStorage>(log_path, region->id());
  Server::GetInstance()->GetLogStorageManager()->AddLogStorage(region->id(), log_storage);

  std::string const meta_raft_name = fmt::format("{}-{}", region->name(), region->id());
  std::shared_ptr<RaftNode> const node = std::make_shared<RaftNode>(
      region->id(), meta_raft_name, braft::PeerId(Server::GetInstance()->RaftEndpoint()), state_machine, log_storage);

  // Build RaftNode
  if (node->Init(Helper::FormatPeers(Helper::ExtractLocations(region->peers())), config) != 0) {
    node->Destroy();
    return butil::Status(pb::error::ERAFT_INIT, "Raft init failed");
  }

  raft_node_manager_->AddNode(region->id(), node);

  // set raft_node to coordinator_control
  meta_control->SetRaftNode(node);

  return butil::Status();
}

butil::Status RaftStoreEngine::ChangeNode(std::shared_ptr<Context> /*ctx*/, uint64_t region_id,
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

butil::Status RaftStoreEngine::StopNode(std::shared_ptr<Context> /*ctx*/, uint64_t region_id) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }
  raft_node_manager_->DeleteNode(region_id);

  node->Stop();

  return butil::Status();
}

butil::Status RaftStoreEngine::DestroyNode(std::shared_ptr<Context> /*ctx*/, uint64_t region_id) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }
  raft_node_manager_->DeleteNode(region_id);

  node->Destroy();

  return butil::Status();
}

std::shared_ptr<RaftNode> RaftStoreEngine::GetNode(uint64_t region_id) {
  return raft_node_manager_->GetNode(region_id);
}

butil::Status RaftStoreEngine::DoSnapshot(std::shared_ptr<Context> ctx, uint64_t region_id) {
  auto node = raft_node_manager_->GetNode(region_id);
  if (node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node");
  }

  node->Snapshot(dynamic_cast<braft::Closure*>(ctx->Done()));
  return butil::Status();
}

butil::Status RaftStoreEngine::TransferLeader(uint64_t region_id, const pb::common::Peer& peer) {
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

std::shared_ptr<pb::raft::RaftCmdRequest> GenRaftCmdRequest(const std::shared_ptr<Context> ctx,
                                                            std::shared_ptr<WriteData> write_data) {
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
    DINGO_LOG(ERROR) << "Not found raft node " << ctx->RegionId();
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
    DINGO_LOG(ERROR) << "Not found raft node " << ctx->RegionId();
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
                                               const std::string& end_key, uint64_t& count) {
  return reader_->KvCount(start_key, end_key, count);
}

std::shared_ptr<Engine::Reader> RaftStoreEngine::NewReader(const std::string& cf_name) {
  return std::make_shared<RaftStoreEngine::Reader>(engine_->NewReader(cf_name));
}

butil::Status RaftStoreEngine::VectorReader::QueryVectorWithId(uint64_t region_id, uint64_t vector_id,
                                                               pb::common::VectorWithId& vector_with_id,
                                                               bool with_vector_data) {
  std::string key;
  VectorCodec::EncodeVectorId(region_id, vector_id, key);

  std::string value;
  auto status = reader_->KvGet(key, value);
  if (!status.ok()) {
    return status;
  }

  if (with_vector_data) {
    pb::common::Vector vector;
    if (!vector.ParseFromString(value)) {
      return butil::Status(pb::error::EINTERNAL, "Internal ParseFromString error");
    }
    vector_with_id.mutable_vector()->Swap(&vector);
  }

  vector_with_id.set_id(vector_id);

  return butil::Status();
}

butil::Status RaftStoreEngine::VectorReader::SearchVector(
    uint64_t region_id, const std::vector<pb::common::VectorWithId>& vector_with_ids,
    const pb::common::VectorSearchParameter& parameter,
    std::vector<pb::index::VectorWithDistanceResult>& vector_with_distance_results) {
  auto vector_index = Server::GetInstance()->GetVectorIndexManager()->GetVectorIndex(region_id);
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, fmt::format("Not found vector index {}", region_id));
  }

  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "Empty vector with ids";
    return butil::Status();
  }

  uint32_t top_n = parameter.top_n();
  bool use_scalar_filter = parameter.use_scalar_filter();
  if (use_scalar_filter) {
    if (BAIDU_UNLIKELY(vector_with_ids[0].scalar_data().scalar_data_size() == 0)) {
      return butil::Status(
          pb::error::EVECTOR_SCALAR_DATA_NOT_FOUND,
          fmt::format("Not found vector scalar data, region: {}, vector id: {}", region_id, vector_with_ids[0].id()));
    }
    top_n *= 10;
  }

  bool with_vector_data = !(parameter.without_vector_data());
  std::vector<pb::index::VectorWithDistanceResult> tmp_results;

  vector_index->Search(vector_with_ids, top_n, tmp_results, with_vector_data);

  if (use_scalar_filter) {
    for (auto& vector_with_distance_result : tmp_results) {
      pb::index::VectorWithDistanceResult new_vector_with_distance_result;

      bool is_reserve = false;
      for (auto& temp_vector_with_distance : *vector_with_distance_result.mutable_vector_with_distances()) {
        uint64_t temp_id = temp_vector_with_distance.vector_with_id().id();
        bool compare_result = false;
        butil::Status status =
            CompareVectorScalarData(region_id, temp_id, vector_with_ids[0].scalar_data(), compare_result);
        if (!status.ok()) {
          return status;
        }
        if (!compare_result) {
          continue;
        }

        new_vector_with_distance_result.add_vector_with_distances()->Swap(&temp_vector_with_distance);
      }

      vector_with_distance_results.emplace_back(std::move(new_vector_with_distance_result));
    }
  } else {
    vector_with_distance_results.swap(tmp_results);
  }

  // if vector index does not support restruct vector ,we restruct it using RocksDB
  if (with_vector_data) {
    for (auto& result : vector_with_distance_results) {
      for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
        if (vector_with_distance.vector_with_id().vector().float_values_size() > 0 ||
            vector_with_distance.vector_with_id().vector().binary_values_size() > 0) {
          continue;
        }

        pb::common::VectorWithId vector_with_id;
        auto status = QueryVectorWithId(region_id, vector_with_distance.vector_with_id().id(), vector_with_id);
        if (!status.ok()) {
          return status;
        }
        vector_with_distance.mutable_vector_with_id()->Swap(&vector_with_id);
      }
    }
  }

  return butil::Status();
}

butil::Status RaftStoreEngine::VectorReader::QueryVectorScalarData(uint64_t region_id,
                                                                   std::vector<std::string> selected_scalar_keys,
                                                                   pb::common::VectorWithId& vector_with_id) {
  std::string key, value;

  // get scalardata
  {
    VectorCodec::EncodeVectorScalar(region_id, vector_with_id.id(), key);

    auto status = reader_->KvGet(key, value);
    if (!status.ok()) {
      return status;
    }

    pb::common::VectorScalardata vector_scalar;
    if (!vector_scalar.ParseFromString(value)) {
      return butil::Status(pb::error::EINTERNAL, "Internal error, decode VectorScalar failed");
    }

    auto* scalar = vector_with_id.mutable_scalar_data()->mutable_scalar_data();
    for (const auto& [key, value] : vector_scalar.scalar_data()) {
      if (!selected_scalar_keys.empty() &&
          std::find(selected_scalar_keys.begin(), selected_scalar_keys.end(), key) == selected_scalar_keys.end()) {
        continue;
      }

      scalar->insert({key, value});
    }
  }

  return butil::Status();
}

butil::Status RaftStoreEngine::VectorReader::QueryVectorScalarData(
    uint64_t region_id, std::vector<std::string> selected_scalar_keys,
    std::vector<pb::index::VectorWithDistanceResult>& results) {
  // get metadata by parameter
  for (auto& result : results) {
    for (auto& vector_with_distance : *result.mutable_vector_with_distances()) {
      pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
      QueryVectorScalarData(region_id, selected_scalar_keys, vector_with_id);
    }
  }

  return butil::Status();
}

butil::Status RaftStoreEngine::VectorReader::QueryVectorScalarData(
    uint64_t region_id, std::vector<std::string> selected_scalar_keys,
    std::vector<pb::common::VectorWithDistance>& vector_with_distances) {
  // get metadata by parameter
  for (auto& vector_with_distance : vector_with_distances) {
    pb::common::VectorWithId& vector_with_id = *(vector_with_distance.mutable_vector_with_id());
    QueryVectorScalarData(region_id, selected_scalar_keys, vector_with_id);
  }

  return butil::Status();
}

butil::Status RaftStoreEngine::VectorReader::CompareVectorScalarData(
    uint64_t region_id, uint64_t vector_id, const pb::common::VectorScalardata& source_scalar_data,
    bool& compare_result) {
  compare_result = false;
  std::string key, value;

  VectorCodec::EncodeVectorScalar(region_id, vector_id, key);

  auto status = reader_->KvGet(key, value);
  if (!status.ok()) {
    return status;
  }

  pb::common::VectorScalardata vector_scalar;
  if (!vector_scalar.ParseFromString(value)) {
    return butil::Status(pb::error::EINTERNAL, "Internal error, decode VectorScalar failed");
  }

  for (const auto& [key, value] : source_scalar_data.scalar_data()) {
    auto it = vector_scalar.scalar_data().find(key);
    if (it == vector_scalar.scalar_data().end()) {
      return butil::Status();
    }

    compare_result = Helper::IsEqualVectorScalarValue(value, it->second);
    if (!compare_result) {
      return butil::Status();
    }
  }

  compare_result = true;
  return butil::Status();
}

butil::Status RaftStoreEngine::VectorReader::VectorBatchSearch(
    std::shared_ptr<Context> ctx, const std::vector<pb::common::VectorWithId>& vector_with_ids,  // NOLINT
    pb::common::VectorSearchParameter parameter,                                                 // NOLINT
    std::vector<pb::index::VectorWithDistanceResult>& results) {                                 // NOLINT

  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "VectorBatchSearch: vector_with_ids is empty";
    return butil::Status::OK();
  }

  // Search vectors by vectors
  auto status = SearchVector(ctx->RegionId(), vector_with_ids, parameter, results);
  if (!status.ok()) {
    return status;
  }

  if (parameter.with_scalar_data()) {
    // Get metadata by parameter
    std::vector<std::string> selected_scalar_keys = Helper::PbRepeatedToVector(parameter.selected_keys());
    auto status = QueryVectorScalarData(ctx->RegionId(), selected_scalar_keys, results);
    if (!status.ok()) {
      return status;
    }
  }

  return butil::Status();
}

butil::Status RaftStoreEngine::VectorReader::VectorBatchQuery(std::shared_ptr<Context> ctx,
                                                              std::vector<uint64_t> vector_ids, bool with_vector_data,
                                                              bool with_scalar_data,
                                                              std::vector<std::string> selected_scalar_keys,
                                                              std::vector<pb::common::VectorWithId>& vector_with_ids) {
  for (auto vector_id : vector_ids) {
    pb::common::VectorWithId vector_with_id;
    auto status = QueryVectorWithId(ctx->RegionId(), vector_id, vector_with_id, with_vector_data);
    if (!status.ok()) {
      DINGO_LOG(WARNING) << "Failed to query vector with id: " << vector_id << ", status: " << status.error_str();
    }

    // if the id is not exist, the vector_with_id will be empty, sdk client will handle this
    vector_with_ids.push_back(vector_with_id);
  }

  if (with_scalar_data) {
    for (auto& vector_with_id : vector_with_ids) {
      if (vector_with_id.ByteSizeLong() == 0) {
        continue;
      }

      auto status = QueryVectorScalarData(ctx->RegionId(), selected_scalar_keys, vector_with_id);
      if (!status.ok()) {
        DINGO_LOG(WARNING) << "Failed to query vector scalar data, id: " << vector_with_id.id()
                           << ", status: " << status.error_str();
      }
    }
  }

  return butil::Status::OK();
}

butil::Status RaftStoreEngine::VectorReader::VectorGetBorderId(std::shared_ptr<Context> ctx, uint64_t& id,
                                                               bool get_min) {
  auto vector_index_mgr = Server::GetInstance()->GetVectorIndexManager();

  auto status = vector_index_mgr->GetBorderId(ctx->RegionId(), id, get_min);
  if (!status.ok()) {
    DINGO_LOG(INFO) << "Failed to get border id: " << status.error_str();
    return status;
  }

  return butil::Status();
}

std::shared_ptr<Engine::VectorReader> RaftStoreEngine::NewVectorReader(const std::string& cf_name) {
  return std::make_shared<RaftStoreEngine::VectorReader>(engine_->NewReader(cf_name));
}

}  // namespace dingodb

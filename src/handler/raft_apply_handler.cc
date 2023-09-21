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

#include "handler/raft_apply_handler.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "engine/raw_engine.h"
#include "event/store_state_machine_event.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
#include "vector/codec.h"

namespace dingodb {

void PutHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                        const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/,
                        uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.put();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->RawRange();
    for (const auto &kv : request.kvs()) {
      if (range.end_key().compare(kv.key()) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  auto writer = engine->NewWriter(request.cf_name());
  if (request.kvs().size() == 1) {
    status = writer->KvPut(request.kvs().Get(0));
  } else {
    status = writer->KvBatchPut(Helper::PbRepeatedToVector(request.kvs()));
  }

  if (ctx) {
    ctx->SetStatus(status);
  }

  // Update region metrics min/max key
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKey(request.kvs());
  }
}

void PutIfAbsentHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/, uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.put_if_absent();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->RawRange();
    for (const auto &kv : request.kvs()) {
      if (range.end_key().compare(kv.key()) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  std::vector<bool> key_states;  // NOLINT
  bool key_state;
  auto writer = engine->NewWriter(request.cf_name());
  bool const is_write_batch = (request.kvs().size() != 1);
  if (!is_write_batch) {
    status = writer->KvPutIfAbsent(request.kvs().Get(0), key_state);
  } else {
    status = writer->KvBatchPutIfAbsent(Helper::PbRepeatedToVector(request.kvs()), key_states, request.is_atomic());
  }

  if (ctx) {
    ctx->SetStatus(status);
    if (is_write_batch) {
      auto *response = dynamic_cast<pb::store::KvBatchPutIfAbsentResponse *>(ctx->Response());
      // std::vector<bool> must do not use foreach
      for (auto &&key_state : key_states) {
        response->add_key_states(key_state);
      }
    } else {  // only one key
      pb::store::KvPutIfAbsentResponse *response = dynamic_cast<pb::store::KvPutIfAbsentResponse *>(ctx->Response());
      if (response) {
        response->set_key_state(key_state);
      } else {
        pb::store::KvBatchPutIfAbsentResponse *response =
            dynamic_cast<pb::store::KvBatchPutIfAbsentResponse *>(ctx->Response());
        if (response) {
          response->add_key_states(key_state);
        }
      }
    }
  }

  // Update region metrics min/max key
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKey(request.kvs());
  }
}

void CompareAndSetHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                  std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                  store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/, uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.compare_and_set();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->RawRange();
    for (const auto &kv : request.kvs()) {
      if (range.end_key().compare(kv.key()) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  std::vector<bool> key_states;  // NOLINT

  auto writer = engine->NewWriter(request.cf_name());
  bool const is_write_batch = (request.kvs().size() != 1);
  status = writer->KvBatchCompareAndSet(Helper::PbRepeatedToVector(request.kvs()),
                                        Helper::PbRepeatedToVector(request.expect_values()), key_states,
                                        request.is_atomic());

  if (ctx) {
    ctx->SetStatus(status);
    if (is_write_batch) {
      auto *response = dynamic_cast<pb::store::KvBatchCompareAndSetResponse *>(ctx->Response());
      // std::vector<bool> must do not use foreach
      for (auto &&key_state : key_states) {
        response->add_key_states(key_state);
      }
    } else {  // only one key
      pb::store::KvCompareAndSetResponse *response =
          dynamic_cast<pb::store::KvCompareAndSetResponse *>(ctx->Response());
      if (response) {
        response->set_key_state(key_states[0]);
      } else {
        pb::store::KvBatchCompareAndSetResponse *response =
            dynamic_cast<pb::store::KvBatchCompareAndSetResponse *>(ctx->Response());
        if (response) {
          response->add_key_states(key_states[0]);
        }
      }
    }
  }

  // Update region metrics min/max key
  if (region_metrics != nullptr) {
    size_t i = 0;
    store::RegionMetrics::PbKeyValues new_kvs;
    store::RegionMetrics::PbKeys delete_keys;
    for (const auto &key_state : key_states) {
      const auto &kv = request.kvs().at(i);
      if (key_state) {
        if (!request.expect_values(i).empty() && kv.value().empty()) {
          delete_keys.Add(std::string(kv.key()));
        }

        if (request.expect_values(i).empty() && !kv.value().empty()) {
          new_kvs.Add(pb::common::KeyValue(kv));
        }
      }
    }

    // add
    region_metrics->UpdateMaxAndMinKey(new_kvs);
    // delete key
    region_metrics->UpdateMaxAndMinKeyPolicy(delete_keys);
  }
}

void DeleteRangeHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/, uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.delete_range();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->RawRange();
    for (const auto &delete_range : request.ranges()) {
      if (range.end_key().compare(delete_range.end_key()) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  auto reader = engine->NewReader(request.cf_name());
  auto writer = engine->NewWriter(request.cf_name());
  uint64_t delete_count = 0;
  if (1 == request.ranges().size()) {
    uint64_t internal_delete_count = 0;
    const auto &range = request.ranges()[0];
    status = reader->KvCount(range.start_key(), range.end_key(), internal_delete_count);
    if (status.ok() && 0 != internal_delete_count) {
      status = writer->KvDeleteRange(range);
    }
    delete_count = internal_delete_count;
  } else {
    auto snapshot = engine->GetSnapshot();
    for (const auto &range : request.ranges()) {
      uint64_t internal_delete_count = 0;
      status = reader->KvCount(snapshot, range.start_key(), range.end_key(), internal_delete_count);
      if (!status.ok()) {
        delete_count = 0;
        break;
      }
      delete_count += internal_delete_count;
    }

    if (status.ok() && 0 != delete_count) {
      status = writer->KvBatchDeleteRange(Helper::PbRepeatedToVector(request.ranges()));
    }
  }

  if (ctx && ctx->Response()) {
    auto *response = dynamic_cast<pb::store::KvDeleteRangeResponse *>(ctx->Response());
    if (response) {
      ctx->SetStatus(status);
      response->set_delete_count(delete_count);
    }
  }

  // Update region metrics min/max key policy
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKeyPolicy(request.ranges());
  }
}

void DeleteBatchHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                store::RegionMetricsPtr region_metrics, uint64_t /*term_id*/, uint64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.delete_batch();
  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->RawRange();
    for (const auto &key : request.keys()) {
      if (range.end_key().compare(key) <= 0) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  auto reader = engine->NewReader(request.cf_name());
  std::vector<bool> key_states(request.keys().size(), false);
  auto snapshot = engine->GetSnapshot();
  size_t i = 0;
  for (const auto &key : request.keys()) {
    std::string value;
    status = reader->KvGet(snapshot, key, value);
    if (status.ok()) {
      key_states[i] = true;
    }
    i++;
  }

  auto writer = engine->NewWriter(request.cf_name());
  if (request.keys().size() == 1) {
    status = writer->KvDelete(request.keys().Get(0));
  } else {
    status = writer->KvBatchDelete(Helper::PbRepeatedToVector(request.keys()));
  }

  if (ctx && ctx->Response()) {
    auto *response = dynamic_cast<pb::store::KvBatchDeleteResponse *>(ctx->Response());
    ctx->SetStatus(status);
    for (const auto &state : key_states) {
      response->add_key_states(state);
    }
  }

  // Update region metrics min/max key policy
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKeyPolicy(request.keys());
  }
}

// Launch rebuild vector index through raft state machine
static void LaunchRebuildVectorIndex(uint64_t region_id) {
  auto engine = Server::GetInstance()->GetEngine();
  if (engine != nullptr) {
    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(region_id);
    auto status = engine->AsyncWrite(ctx, WriteDataBuilder::BuildWrite());
    if (!status.ok()) {
      if (status.error_code() != pb::error::ERAFT_NOTLEADER) {
        DINGO_LOG(ERROR) << fmt::format("Launch rebuild vector index failed, error: {}", status.error_str());
      }
    }
  }
}

// Launch do snapshot
static void LaunchDoSnapshot(store::RegionPtr region) {  // NOLINT
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  store_region_meta->UpdateNeedBootstrapDoSnapshot(region, true);

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetDone(new SplitHandler::SplitClosure(region));
  auto engine = Server::GetInstance()->GetEngine();
  bool is_success = false;
  for (int i = 0; i < Constant::kSplitDoSnapshotRetryTimes; ++i) {
    auto ret = engine->DoSnapshot(ctx, region->Id());
    if (ret.ok()) {
      is_success = true;
      break;
    }
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({})] do snapshot failed, retry({}) error: {}",
                                    region->Id(), i, ret.error_str());
    bthread_usleep(1000 * 100);
  }

  if (!is_success) {
    store_region_meta->UpdateNeedBootstrapDoSnapshot(region, false);
  }
}

void SplitHandler::SplitClosure::Run() {
  std::unique_ptr<SplitClosure> self_guard(this);
  if (!status().ok()) {
    DINGO_LOG(WARNING) << fmt::format("[split.spliting][region({})] finish snapshot failed, error: {}", region_->Id(),
                                      status().error_str());
    bthread_usleep(1000 * 200);

    // Retry do snapshot
    LaunchDoSnapshot(region_);
    return;
  } else {
    DINGO_LOG(INFO) << fmt::format("[split.spliting][region({})] finish snapshot success", region_->Id());
  }

  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();

  if (region_->Type() == pb::common::STORE_REGION) {
    store_region_meta->UpdateTemporaryDisableChange(region_, false);
  }

  store_region_meta->UpdateNeedBootstrapDoSnapshot(region_, false);
}

// Pre create region split
bool HandlePreCreateRegionSplit(const pb::raft::SplitRequest &request, store::RegionPtr from_region, uint64_t term_id,
                                uint64_t log_id) {
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();

  if (request.epoch().version() != from_region->Epoch().version()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[split.spliting][region({}->{})] region version changed, split version({}) region version({})",
        request.from_region_id(), request.to_region_id(), request.epoch().version(), from_region->Epoch().version());
    return false;
  }

  auto to_region = store_region_meta->GetRegion(request.to_region_id());
  if (to_region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] child region not found", request.from_region_id(),
                                    request.to_region_id());
    return false;
  }

  if (to_region->State() != pb::common::StoreRegionState::STANDBY) {
    DINGO_LOG(WARNING) << fmt::format("[split.spliting][region({}->{})] child region state is not standby",
                                      from_region->Id(), to_region->Id());
    return false;
  }
  if (from_region->RawRange().start_key() >= from_region->RawRange().end_key()) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] from region invalid range [{}-{})",
                                    from_region->Id(), to_region->Id(),
                                    Helper::StringToHex(from_region->RawRange().start_key()),
                                    Helper::StringToHex(from_region->RawRange().end_key()));
    return false;
  }
  if (request.split_key() < from_region->RawRange().start_key() ||
      request.split_key() > from_region->RawRange().end_key()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[split.spliting][region({}->{})] from region invalid split key {} region range: [{}-{})", from_region->Id(),
        to_region->Id(), Helper::StringToHex(request.split_key()),
        Helper::StringToHex(from_region->RawRange().start_key()),
        Helper::StringToHex(from_region->RawRange().end_key()));
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] begin split, term({}) log_id({})", from_region->Id(),
                                 to_region->Id(), term_id, log_id);

  // temporary disable split, avoid overlap change.
  store_region_meta->UpdateTemporaryDisableChange(from_region, true);
  store_region_meta->UpdateTemporaryDisableChange(to_region, true);

  // Set region state spliting
  store_region_meta->UpdateState(from_region, pb::common::StoreRegionState::SPLITTING);

  DINGO_LOG(INFO) << fmt::format(
      "[split.spliting][region({}->{})] from region range[{}-{}] to region range[{}-{}]", from_region->Id(),
      to_region->Id(), Helper::StringToHex(from_region->RawRange().start_key()),
      Helper::StringToHex(from_region->RawRange().end_key()), Helper::StringToHex(to_region->RawRange().start_key()),
      Helper::StringToHex(to_region->RawRange().end_key()));

  pb::common::Range to_range;
  // Set child range
  to_range.set_start_key(to_region->RawRange().start_key());
  to_range.set_end_key(request.split_key());
  if (to_range.start_key().compare(request.split_key()) > 0) {
    to_range.set_start_key(from_region->RawRange().start_key());
  }
  store_region_meta->UpdateRange(to_region, to_range);

  // Set parent range
  pb::common::Range from_range;
  from_range.set_start_key(request.split_key());
  from_range.set_end_key(from_region->RawRange().end_key());
  Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->UpdateRange(from_region, from_range);
  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] from region range[{}-{}] to region range[{}-{}]",
                                 from_region->Id(), to_region->Id(), Helper::StringToHex(from_range.start_key()),
                                 Helper::StringToHex(from_range.end_key()), Helper::StringToHex(to_range.start_key()),
                                 Helper::StringToHex(to_range.end_key()));

  // Set split record
  to_region->SetParentId(from_region->Id());
  from_region->UpdateLastSplitTimestamp();
  pb::store_internal::RegionSplitRecord record;
  record.set_region_id(to_region->Id());
  record.set_split_time(Helper::NowTime());
  from_region->AddChild(record);

  // Increase region version
  store_region_meta->UpdateEpochVersion(from_region, from_region->Epoch().version() + 1);

  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] parent do snapshot", from_region->Id(),
                                 to_region->Id());

  // Do parent region snapshot
  LaunchDoSnapshot(from_region);

  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] child do snapshot", from_region->Id(),
                                 to_region->Id());
  // Do child region snapshot
  LaunchDoSnapshot(to_region);

  if (to_region->Type() == pb::common::INDEX_REGION) {
    // Set child share vector index
    auto vector_index = from_region->VectorIndexWrapper()->GetOwnVectorIndex();
    if (vector_index != nullptr) {
      to_region->VectorIndexWrapper()->SetShareVectorIndex(vector_index);
    } else {
      DINGO_LOG(WARNING) << fmt::format("[split.spliting][region({}->{})] split region get vector index failed",
                                        from_region->Id(), to_region->Id());
    }

    // build vector index
    VectorIndexManager::LaunchRebuildVectorIndex(to_region->VectorIndexWrapper(), true);
    VectorIndexManager::LaunchRebuildVectorIndex(from_region->VectorIndexWrapper(), true);

    from_region->VectorIndexWrapper()->SetNeedBootstrapBuild(false);
  }

  store_region_meta->UpdateState(from_region, pb::common::StoreRegionState::NORMAL);
  store_region_meta->UpdateState(to_region, pb::common::StoreRegionState::NORMAL);
  Heartbeat::TriggerStoreHeartbeat(from_region->Id());
  Heartbeat::TriggerStoreHeartbeat(to_region->Id());

  return true;
}

store::RegionPtr CreateNewRegion(const pb::common::RegionDefinition &definition, uint64_t parent_region_id) {  // NOLINT
  store::RegionPtr region = store::Region::New(definition);
  region->SetState(pb::common::STANDBY);
  region->SetSplitStrategy(pb::raft::POST_CREATE_REGION);
  region->SetParentId(parent_region_id);

  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());

  auto raft_store_engine = Server::GetInstance()->GetRaftStoreEngine();
  if (raft_store_engine == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] Not found raft store engine.", parent_region_id,
                                    region->Id());
    return nullptr;
  }

  auto parent_node = raft_store_engine->GetNode(parent_region_id);
  if (parent_node == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] Not found raft node.", parent_region_id,
                                    region->Id());
    return nullptr;
  }

  auto raft_meta = StoreRaftMeta::NewRaftMeta(region->Id());
  Server::GetInstance()->GetStoreMetaManager()->GetStoreRaftMeta()->AddRaftMeta(raft_meta);
  auto config = Server::GetInstance()->GetConfig();

  RaftControlAble::AddNodeParameter parameter;
  parameter.role = Server::GetInstance()->GetRole();
  parameter.is_restart = false;
  parameter.raft_endpoint = Server::GetInstance()->RaftEndpoint();

  parameter.raft_path = config->GetString("raft.path");
  parameter.election_timeout_ms = parent_node->IsLeader() ? 200 : 10 * 1000;
  parameter.snapshot_interval_s = config->GetInt("raft.snapshot_interval_s");
  parameter.log_max_segment_size = config->GetInt64("raft.segmentlog_max_segment_size");
  parameter.log_path = config->GetString("raft.log_path");

  parameter.raft_meta = raft_meta;
  parameter.region_metrics = region_metrics;
  auto listener_factory = std::make_shared<StoreSmEventListenerFactory>();
  parameter.listeners = listener_factory->Build();

  auto status = raft_store_engine->AddNode(region, parameter);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] add node failed, error: {}", parent_region_id,
                                    region->Id(), status.error_str());
    return nullptr;
  }

  Server::GetInstance()->GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);
  Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->AddRegion(region);
  Server::GetInstance()->GetRegionController()->RegisterExecutor(region->Id());

  return region;
}

// Post create region split
bool HandlePostCreateRegionSplit(const pb::raft::SplitRequest &request, store::RegionPtr parent_region,
                                 uint64_t term_id, uint64_t log_id) {
  auto store_region_meta = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta();
  uint64_t parent_region_id = request.from_region_id();
  uint64_t child_region_id = request.to_region_id();

  if (request.epoch().version() != parent_region->Epoch().version()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[split.spliting][region({}->{})] region version changed, split version({}) region version({})",
        parent_region_id, child_region_id, request.epoch().version(), parent_region->Epoch().version());
    return false;
  }

  auto old_range = parent_region->RawRange();

  if (old_range.start_key() >= old_range.end_key()) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] from region invalid range [{}-{})",
                                    parent_region_id, child_region_id, Helper::StringToHex(old_range.start_key()),
                                    Helper::StringToHex(old_range.end_key()));
    return false;
  }

  if (request.split_key() < old_range.start_key() || request.split_key() > old_range.end_key()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[split.spliting][region({}->{})] from region invalid split key {} region range: [{}-{})", parent_region_id,
        child_region_id, Helper::StringToHex(request.split_key()), Helper::StringToHex(old_range.start_key()),
        Helper::StringToHex(old_range.end_key()));
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] begin split, term({}) log_id({})", parent_region_id,
                                 child_region_id, term_id, log_id);

  // temporary disable split, avoid overlap change.
  store_region_meta->UpdateTemporaryDisableChange(parent_region, true);

  // Set region state spliting
  store_region_meta->UpdateState(parent_region, pb::common::StoreRegionState::SPLITTING);

  pb::common::Range child_range;
  // Set child range
  child_range.set_start_key(old_range.start_key());
  child_range.set_end_key(request.split_key());

  // Create child region
  pb::common::RegionDefinition definition = parent_region->InnerRegion().definition();
  definition.set_id(child_region_id);
  definition.set_name(fmt::format("{}_{}", definition.name(), child_region_id));
  definition.mutable_epoch()->set_conf_version(1);
  definition.mutable_epoch()->set_version(1);
  *(definition.mutable_range()) = child_range;
  *(definition.mutable_raw_range()) = child_range;

  auto child_region = CreateNewRegion(definition, parent_region->Id());
  if (child_region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] create child region failed.", parent_region_id,
                                    child_region_id);
    return false;
  }

  // Set parent range
  pb::common::Range parent_range;
  parent_range.set_start_key(request.split_key());
  parent_range.set_end_key(old_range.end_key());
  store_region_meta->UpdateRange(parent_region, parent_range);
  DINGO_LOG(INFO) << fmt::format(
      "[split.spliting][region({}->{})] from region range[{}-{}] to region range[{}-{}]", parent_region_id,
      child_region_id, Helper::StringToHex(parent_range.start_key()), Helper::StringToHex(parent_range.end_key()),
      Helper::StringToHex(child_range.start_key()), Helper::StringToHex(child_range.end_key()));

  // Set split record
  parent_region->UpdateLastSplitTimestamp();
  pb::store_internal::RegionSplitRecord record;
  record.set_region_id(child_region_id);
  record.set_split_time(Helper::NowTime());
  parent_region->AddChild(record);

  // Increase region version
  store_region_meta->UpdateEpochVersion(parent_region, parent_region->Epoch().version() + 1);

  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] parent do snapshot", parent_region_id,
                                 child_region_id);
  // Do parent region snapshot
  LaunchDoSnapshot(parent_region);

  // Set do snapshot when bootstrap
  store_region_meta->UpdateNeedBootstrapDoSnapshot(child_region, true);

  store_region_meta->UpdateState(parent_region, pb::common::StoreRegionState::NORMAL);
  Heartbeat::TriggerStoreHeartbeat(parent_region->Id());

  return true;
}

// region-100: [start_key,end_key) ->
// region-101: [start_key, split_key) and region-100: [split_key, end_key)
void SplitHandler::Handle(std::shared_ptr<Context>, store::RegionPtr from_region, std::shared_ptr<RawEngine>,
                          const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, uint64_t term_id,
                          uint64_t log_id) {
  const auto &request = req.split();

  if (request.split_strategy() == pb::raft::PRE_CREATE_REGION) {
    bool ret = HandlePreCreateRegionSplit(request, from_region, term_id, log_id);
    if (!ret) {
      return;
    }
  } else {
    bool ret = HandlePostCreateRegionSplit(request, from_region, term_id, log_id);
    if (!ret) {
      return;
    }
  }

  // Update region metrics min/max key policy
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKeyPolicy();
  }
}

void SaveRaftSnapshotHandler::Handle(std::shared_ptr<Context>, store::RegionPtr region, std::shared_ptr<RawEngine>,
                                     const pb::raft::Request &, store::RegionMetricsPtr, uint64_t term_id,
                                     uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] save snapshot, term({}) log_id({})",
                                 region->ParentId(), region->Id(), term_id, log_id);
  auto engine = Server::GetInstance()->GetEngine();
  std::shared_ptr<Context> to_ctx = std::make_shared<Context>();
  to_ctx->SetDone(new SplitHandler::SplitClosure(region));
  auto status = engine->DoSnapshot(to_ctx, region->Id());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] do snapshot failed, error: {}",
                                    region->ParentId(), region->Id(), status.error_str());
  }
}

void VectorAddHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                              const pb::raft::Request &req, store::RegionMetricsPtr /*region_metrics*/,
                              uint64_t /*term_id*/, uint64_t log_id) {
  auto set_ctx_status = [ctx](butil::Status status) {
    if (ctx) {
      ctx->SetStatus(status);
    }
  };

  butil::Status status;
  const auto &request = req.vector_add();

  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->RawRange();
    uint64_t start_vector_id = VectorCodec::DecodeVectorId(range.start_key());
    uint64_t end_vector_id = VectorCodec::DecodeVectorId(range.end_key());
    for (const auto &vector : request.vectors()) {
      if (vector.id() < start_vector_id || vector.id() >= end_vector_id) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  // Transform vector to kv
  std::vector<pb::common::KeyValue> kvs;
  for (const auto &vector : request.vectors()) {
    // vector data
    {
      pb::common::KeyValue kv;
      std::string key;
      VectorCodec::EncodeVectorData(region->PartitionId(), vector.id(), key);
      kv.mutable_key()->swap(key);
      kv.set_value(vector.vector().SerializeAsString());
      kvs.push_back(kv);
    }
    // vector scalar data
    {
      pb::common::KeyValue kv;
      std::string key;
      VectorCodec::EncodeVectorScalar(region->PartitionId(), vector.id(), key);
      kv.mutable_key()->swap(key);
      kv.set_value(vector.scalar_data().SerializeAsString());
      kvs.push_back(kv);
    }
    // vector table data
    {
      pb::common::KeyValue kv;
      std::string key;
      VectorCodec::EncodeVectorTable(region->PartitionId(), vector.id(), key);
      kv.mutable_key()->swap(key);
      kv.set_value(vector.table_data().SerializeAsString());
      kvs.push_back(kv);
    }
  }

  // build vector_with_ids
  std::vector<pb::common::VectorWithId> vector_with_ids;
  vector_with_ids.reserve(request.vectors_size());

  for (const auto &vector : request.vectors()) {
    pb::common::VectorWithId vector_with_id;
    *(vector_with_id.mutable_vector()) = vector.vector();
    vector_with_id.set_id(vector.id());
    vector_with_ids.push_back(vector_with_id);
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  uint64_t vector_index_id = vector_index_wrapper->Id();
  bool is_ready = vector_index_wrapper->IsReady();
  // if leadder vector_index is nullptr, return internal error
  if (ctx != nullptr && !is_ready) {
    DINGO_LOG(ERROR) << fmt::format("Not found vector index {}", vector_index_id);
    status = butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "Not found vector index %ld", vector_index_id);
    set_ctx_status(status);
    return;
  }

  // Only leader and specific follower write vector index, other follower don't write vector index.
  if (is_ready) {
    // Check if the log_id is greater than the ApplyLogIndex of the vector index
    if (log_id > vector_index_wrapper->ApplyLogId()) {
      try {
        auto start = std::chrono::steady_clock::now();

        auto ret = vector_index_wrapper->Upsert(vector_with_ids);

        auto end = std::chrono::steady_clock::now();

        auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        DINGO_LOG(INFO) << fmt::format("vector index {} upsert {} vectors, cost {}us", vector_index_id,
                                       vector_with_ids.size(), diff);

        if (ret.error_code() == pb::error::Errno::EVECTOR_INDEX_FULL) {
          DINGO_LOG(INFO) << fmt::format("vector index {} is full", vector_index_id);
          status = butil::Status(pb::error::EVECTOR_INDEX_FULL, "Vector index %lu is full", vector_index_id);
        } else if (!ret.ok()) {
          DINGO_LOG(ERROR) << fmt::format("vector index {} upsert failed, vector_count={}, err={}", vector_index_id,
                                          vector_with_ids.size(), ret.error_str());
          status = butil::Status(pb::error::EINTERNAL, "Vector index %lu upsert failed, vector_count=[%ld], err=[%s]",
                                 vector_index_id, vector_with_ids.size(), ret.error_cstr());
          set_ctx_status(status);
        }
      } catch (const std::exception &e) {
        DINGO_LOG(ERROR) << fmt::format("vector_index add failed : {}", e.what());
        status =
            butil::Status(pb::error::EINTERNAL, "Vector index %lu add failed, err=[%s]", vector_index_id, e.what());
      }
    } else {
      DINGO_LOG(WARNING) << fmt::format("Vector index {} already applied log index, log_id({}) / apply_log_index({})",
                                        vector_index_id, log_id, vector_index_wrapper->ApplyLogId());
    }
  }

  // Store vector
  if (!kvs.empty() && status.ok()) {
    auto writer = engine->NewWriter(request.cf_name());
    status = writer->KvBatchPut(kvs);

    if (is_ready) {
      // Update the ApplyLogIndex of the vector index to the current log_id
      vector_index_wrapper->SetApplyLogId(log_id);
    }
  }

  if (ctx) {
    if (ctx->Response()) {
      bool key_state = false;
      if (status.ok()) {
        key_state = true;
      }
      auto *response = dynamic_cast<pb::index::VectorAddResponse *>(ctx->Response());
      for (int i = 0; i < request.vectors_size(); i++) {
        response->add_key_states(key_state);
      }
    }

    ctx->SetStatus(status);
  }
}

void VectorDeleteHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                 std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                 store::RegionMetricsPtr /*region_metrics*/, uint64_t /*term_id*/, uint64_t log_id) {
  auto set_ctx_status = [ctx](butil::Status status) {
    if (ctx) {
      ctx->SetStatus(status);
    }
  };

  butil::Status status;
  const auto &request = req.vector_delete();

  // region is spliting, check key out range
  if (region->State() == pb::common::StoreRegionState::SPLITTING) {
    const auto &range = region->RawRange();
    uint64_t start_vector_id = VectorCodec::DecodeVectorId(range.start_key());
    uint64_t end_vector_id = VectorCodec::DecodeVectorId(range.end_key());
    for (auto vector_id : request.ids()) {
      if (vector_id < start_vector_id || vector_id >= end_vector_id) {
        if (ctx) {
          status.set_error(pb::error::EREGION_REDIRECT, "Region is spliting, please update route");
          ctx->SetStatus(status);
        }
        return;
      }
    }
  }

  auto reader = engine->NewReader(request.cf_name());
  auto snapshot = engine->GetSnapshot();

  if (request.ids_size() == 0) {
    DINGO_LOG(WARNING) << fmt::format("vector_delete ids_size is 0, region_id={}", region->Id());
    status = butil::Status::OK();
    set_ctx_status(status);
    return;
  }

  // Transform vector to kv
  std::vector<bool> key_states(request.ids_size(), false);
  std::vector<std::string> keys;
  std::vector<uint64_t> delete_ids;

  for (int i = 0; i < request.ids_size(); i++) {
    // set key_states
    std::string key;
    VectorCodec::EncodeVectorData(region->PartitionId(), request.ids(i), key);

    std::string value;
    auto ret = reader->KvGet(snapshot, key, value);
    if (ret.ok()) {
      // delete vector data
      {
        std::string key;
        VectorCodec::EncodeVectorData(region->PartitionId(), request.ids(i), key);
        keys.push_back(key);
      }

      // delete scalar data
      {
        std::string key;
        VectorCodec::EncodeVectorScalar(region->PartitionId(), request.ids(i), key);
        keys.push_back(key);
      }

      // delete table data
      {
        std::string key;
        VectorCodec::EncodeVectorTable(region->PartitionId(), request.ids(i), key);
        keys.push_back(key);
      }

      key_states[i] = true;
      delete_ids.push_back(request.ids(i));

      DINGO_LOG(DEBUG) << fmt::format("vector_delete id={}, region_id={}", request.ids(i), region->Id());
    }
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  uint64_t vector_index_id = vector_index_wrapper->Id();
  bool is_ready = vector_index_wrapper->IsReady();
  // if leadder vector_index is nullptr, return internal error
  if (ctx != nullptr && !is_ready) {
    DINGO_LOG(ERROR) << fmt::format("Not found vector index {}", vector_index_id);
    status = butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "Not found vector index %ld", vector_index_id);
    set_ctx_status(status);
    return;
  }

  // Only leader and specific follower write vector index, other follower don't write vector index.
  if (is_ready && !delete_ids.empty()) {
    if (log_id > vector_index_wrapper->ApplyLogId()) {
      // delete vector from index
      try {
        auto ret = vector_index_wrapper->Delete(delete_ids);
        if (ret.error_code() == pb::error::Errno::EVECTOR_NOT_FOUND) {
          DINGO_LOG(ERROR) << fmt::format("vector not found at vector index {}, vector_count={}, err={}",
                                          vector_index_id, delete_ids.size(), ret.error_str());
        } else if (!ret.ok()) {
          DINGO_LOG(ERROR) << fmt::format("vector index {} delete failed, vector_count={}, err={}", vector_index_id,
                                          delete_ids.size(), ret.error_str());
          status = butil::Status(pb::error::EINTERNAL, "Vector index %lu delete failed, vector_count=[%ld], err=[%s]",
                                 vector_index_id, delete_ids.size(), ret.error_cstr());
          set_ctx_status(status);
        }
      } catch (const std::exception &e) {
        DINGO_LOG(ERROR) << fmt::format("vector index {} delete failed : {}", vector_index_id, e.what());
        status =
            butil::Status(pb::error::EINTERNAL, "Vector index %lu delete failed, err=[%s]", vector_index_id, e.what());
      }
    } else {
      DINGO_LOG(WARNING) << fmt::format("Vector index {} already applied log index, log_id({}) / apply_log_index({})",
                                        vector_index_id, log_id, vector_index_wrapper->ApplyLogId());
    }
  }

  // Delete vector and write wal
  if (!keys.empty() && status.ok()) {
    auto writer = engine->NewWriter(request.cf_name());
    status = writer->KvBatchDelete(keys);

    if (is_ready) {
      // Update the ApplyLogIndex of the vector index to the current log_id
      vector_index_wrapper->SetApplyLogId(log_id);
    }
  }

  if (ctx) {
    if (ctx->Response()) {
      auto *response = dynamic_cast<pb::index::VectorDeleteResponse *>(ctx->Response());
      if (status.ok()) {
        for (const auto &state : key_states) {
          response->add_key_states(state);
        }
      } else {
        for (const auto &state : key_states) {
          response->add_key_states(false);
        }
      }
    }

    ctx->SetStatus(status);
  }
}

void RebuildVectorIndexHandler::Handle(std::shared_ptr<Context>, store::RegionPtr region, std::shared_ptr<RawEngine>,
                                       [[maybe_unused]] const pb::raft::Request &req, store::RegionMetricsPtr, uint64_t,
                                       uint64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.rebuild][index_id({})] Handle rebuild vector index, apply_log_id: {}",
                                 region->Id(), log_id);
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper != nullptr) {
    vector_index_wrapper->SaveApplyLogId(log_id);

    VectorIndexManager::LaunchRebuildVectorIndex(vector_index_wrapper, true);
  }
}

std::shared_ptr<HandlerCollection> RaftApplyHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  handler_collection->Register(std::make_shared<PutHandler>());
  handler_collection->Register(std::make_shared<PutIfAbsentHandler>());
  handler_collection->Register(std::make_shared<DeleteRangeHandler>());
  handler_collection->Register(std::make_shared<DeleteBatchHandler>());
  handler_collection->Register(std::make_shared<SplitHandler>());
  handler_collection->Register(std::make_shared<CompareAndSetHandler>());
  handler_collection->Register(std::make_shared<VectorAddHandler>());
  handler_collection->Register(std::make_shared<VectorDeleteHandler>());
  handler_collection->Register(std::make_shared<RebuildVectorIndexHandler>());
  handler_collection->Register(std::make_shared<SaveRaftSnapshotHandler>());

  return handler_collection;
}

}  // namespace dingodb

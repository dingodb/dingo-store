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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/role.h"
#include "common/service_access.h"
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

int PutHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                       const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t /*term_id*/,
                       int64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.put();

  auto writer = engine->Writer();
  if (!writer) {
    DINGO_LOG(FATAL) << "[raft.apply][region(" << region->Id() << ")] NewWriter failed";
  }
  if (request.kvs().size() == 1) {
    status = writer->KvPut(request.cf_name(), request.kvs().Get(0));
  } else {
    status = writer->KvBatchPut(request.cf_name(), Helper::PbRepeatedToVector(request.kvs()));
  }

  if (status.error_code() == pb::error::Errno::EINTERNAL) {
    DINGO_LOG(FATAL) << fmt::format("[raft.apply][region({})] put failed, error: {}", region->Id(), status.error_str());
  }

  if (ctx) {
    ctx->SetStatus(status);
  }

  // Update region metrics min/max key
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKey(request.kvs());
  }

  return 0;
}

int DeleteRangeHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr /*region*/,
                               std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                               store::RegionMetricsPtr region_metrics, int64_t /*term_id*/, int64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.delete_range();

  auto reader = engine->Reader();
  auto writer = engine->Writer();
  int64_t delete_count = 0;
  if (1 == request.ranges().size()) {
    int64_t internal_delete_count = 0;
    const auto &range = request.ranges()[0];
    status = reader->KvCount(request.cf_name(), range.start_key(), range.end_key(), internal_delete_count);
    if (status.ok() && 0 != internal_delete_count) {
      status = writer->KvDeleteRange(request.cf_name(), range);
    }
    delete_count = internal_delete_count;
  } else {
    auto snapshot = engine->GetSnapshot();
    for (const auto &range : request.ranges()) {
      int64_t internal_delete_count = 0;
      status = reader->KvCount(request.cf_name(), snapshot, range.start_key(), range.end_key(), internal_delete_count);
      if (!status.ok()) {
        delete_count = 0;
        break;
      }
      delete_count += internal_delete_count;
    }

    if (status.ok() && 0 != delete_count) {
      status = writer->KvBatchDeleteRange(request.cf_name(), Helper::PbRepeatedToVector(request.ranges()));
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

  return 0;
}

int DeleteBatchHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                               const pb::raft::Request &req, store::RegionMetricsPtr region_metrics,
                               int64_t /*term_id*/, int64_t /*log_id*/) {
  butil::Status status;
  const auto &request = req.delete_batch();

  auto reader = engine->Reader();
  std::vector<bool> key_states(request.keys().size(), false);
  auto snapshot = engine->GetSnapshot();
  size_t i = 0;
  for (const auto &key : request.keys()) {
    std::string value;
    status = reader->KvGet(request.cf_name(), snapshot, key, value);
    if (status.ok()) {
      key_states[i] = true;
    }
    i++;
  }

  auto writer = engine->Writer();
  if (!writer) {
    DINGO_LOG(FATAL) << "[raft.apply][region(" << region->Id() << ")] NewWriter failed";
  }
  if (request.keys().size() == 1) {
    status = writer->KvDelete(request.cf_name(), request.keys().Get(0));
  } else {
    status = writer->KvBatchDelete(request.cf_name(), Helper::PbRepeatedToVector(request.keys()));
  }

  if (status.error_code() == pb::error::Errno::EINTERNAL) {
    DINGO_LOG(FATAL) << fmt::format("[raft.apply][region({})] delete failed, error: {}", region->Id(),
                                    status.error_str());
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

  return 0;
}

// Launch do snapshot
static void LaunchDoSnapshot(store::RegionPtr region) {  // NOLINT
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();
  assert(store_region_meta != nullptr);
  store_region_meta->UpdateNeedBootstrapDoSnapshot(region, true);

  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  ctx->SetDone(new SplitHandler::SplitClosure(region));
  auto engine = Server::GetInstance().GetEngine();
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

  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();

  if (region_->Type() == pb::common::STORE_REGION) {
    store_region_meta->UpdateTemporaryDisableChange(region_, false);
  }

  store_region_meta->UpdateNeedBootstrapDoSnapshot(region_, false);
}

// Pre create region split
bool HandlePreCreateRegionSplit(const pb::raft::SplitRequest &request, store::RegionPtr from_region, int64_t term_id,
                                int64_t log_id) {
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();

  // Update last_change_cmd_id
  store_region_meta->UpdateLastChangeCmdId(from_region, request.split_id());

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
  if (from_region->Range().start_key() >= from_region->Range().end_key()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[split.spliting][region({}->{})] from region invalid range [{}-{})", from_region->Id(), to_region->Id(),
        Helper::StringToHex(from_region->Range().start_key()), Helper::StringToHex(from_region->Range().end_key()));
    return false;
  }
  if (request.split_key() < from_region->Range().start_key() || request.split_key() > from_region->Range().end_key()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[split.spliting][region({}->{})] from region invalid split key {} region range: [{}-{})", from_region->Id(),
        to_region->Id(), Helper::StringToHex(request.split_key()),
        Helper::StringToHex(from_region->Range().start_key()), Helper::StringToHex(from_region->Range().end_key()));
    return false;
  }

  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] begin split, term({}) log_id({})", from_region->Id(),
                                 to_region->Id(), term_id, log_id);

  // temporary disable split/merge/change_peer, avoid overlap change.
  store_region_meta->UpdateTemporaryDisableChange(from_region, true);
  store_region_meta->UpdateTemporaryDisableChange(to_region, true);

  DINGO_LOG(INFO) << fmt::format(
      "[split.spliting][region({}->{})] pre from region range[{}-{}] to region range[{}-{}]", from_region->Id(),
      to_region->Id(), Helper::StringToHex(from_region->Range().start_key()),
      Helper::StringToHex(from_region->Range().end_key()), Helper::StringToHex(to_region->Range().start_key()),
      Helper::StringToHex(to_region->Range().end_key()));

  pb::common::Range to_range;
  // child range
  to_range.set_start_key(from_region->Range().start_key());
  to_range.set_end_key(request.split_key());

  // parent range
  pb::common::Range from_range;
  from_range.set_start_key(request.split_key());
  from_range.set_end_key(from_region->Range().end_key());

  DINGO_LOG(INFO) << fmt::format(
      "[split.spliting][region({}->{})] post from region range[{}-{}] to region range[{}-{}]", from_region->Id(),
      to_region->Id(), Helper::StringToHex(from_range.start_key()), Helper::StringToHex(from_range.end_key()),
      Helper::StringToHex(to_range.start_key()), Helper::StringToHex(to_range.end_key()));

  // Set split record
  to_region->SetParentId(from_region->Id());
  from_region->UpdateLastSplitTimestamp();
  pb::store_internal::RegionSplitRecord record;
  record.set_region_id(to_region->Id());
  record.set_split_time(Helper::NowTime());
  from_region->AddChild(record);

  // set child region version/range/state
  store_region_meta->UpdateEpochVersionAndRange(to_region, to_region->Epoch().version() + 1, to_range);
  store_region_meta->UpdateState(to_region, pb::common::StoreRegionState::SPLITTING);

  // set parent region version/range/state
  store_region_meta->UpdateEpochVersionAndRange(from_region, from_region->Epoch().version() + 1, from_range);
  store_region_meta->UpdateState(from_region, pb::common::StoreRegionState::SPLITTING);

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
      DINGO_LOG(WARNING) << fmt::format("[split.spliting][region({}->{})] split region get vector index failed.",
                                        from_region->Id(), to_region->Id());
    }

    // Rebuild vector index
    VectorIndexManager::LaunchRebuildVectorIndex(to_region->VectorIndexWrapper(), true);
    VectorIndexManager::LaunchRebuildVectorIndex(from_region->VectorIndexWrapper(), true);
  }

  // update StoreRegionState to NORMAL
  store_region_meta->UpdateState(from_region, pb::common::StoreRegionState::NORMAL);
  store_region_meta->UpdateState(to_region, pb::common::StoreRegionState::NORMAL);
  Heartbeat::TriggerStoreHeartbeat({from_region->Id(), to_region->Id()}, true);

  return true;
}

store::RegionPtr CreateNewRegion(const pb::common::RegionDefinition &definition, int64_t parent_region_id) {  // NOLINT
  store::RegionPtr region = store::Region::New(definition);
  region->SetState(pb::common::STANDBY);
  region->SetSplitStrategy(pb::raft::POST_CREATE_REGION);
  region->SetParentId(parent_region_id);

  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
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
  Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta()->AddRaftMeta(raft_meta);
  auto config = ConfigManager::GetInstance().GetRoleConfig();

  RaftControlAble::AddNodeParameter parameter;
  parameter.role = GetRole();
  parameter.is_restart = false;
  parameter.raft_endpoint = Server::GetInstance().RaftEndpoint();

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

  Server::GetInstance().GetStoreMetricsManager()->GetStoreRegionMetrics()->AddMetrics(region_metrics);
  Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta()->AddRegion(region);
  Server::GetInstance().GetRegionController()->RegisterExecutor(region->Id());

  return region;
}

// Post create region split
bool HandlePostCreateRegionSplit(const pb::raft::SplitRequest &request, store::RegionPtr parent_region, int64_t term_id,
                                 int64_t log_id) {
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();
  int64_t parent_region_id = request.from_region_id();
  int64_t child_region_id = request.to_region_id();

  if (request.epoch().version() != parent_region->Epoch().version()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[split.spliting][region({}->{})] region version changed, split version({}) region version({})",
        parent_region_id, child_region_id, request.epoch().version(), parent_region->Epoch().version());
    return false;
  }

  auto old_range = parent_region->Range();

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

  pb::common::Range child_range;
  // Set child range
  child_range.set_start_key(old_range.start_key());
  child_range.set_end_key(request.split_key());

  // Create child region
  pb::common::RegionDefinition definition = parent_region->Definition();
  definition.set_id(child_region_id);
  definition.set_name(fmt::format("{}_{}", definition.name(), child_region_id));
  definition.mutable_epoch()->set_conf_version(1);
  definition.mutable_epoch()->set_version(1);
  *(definition.mutable_range()) = child_range;

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

  DINGO_LOG(INFO) << fmt::format(
      "[split.spliting][region({}->{})] from region range[{}-{}] to region range[{}-{}]", parent_region_id,
      child_region_id, Helper::StringToHex(parent_range.start_key()), Helper::StringToHex(parent_range.end_key()),
      Helper::StringToHex(child_range.start_key()), Helper::StringToHex(child_range.end_key()));
  // Set region state spliting
  store_region_meta->UpdateState(parent_region, pb::common::StoreRegionState::SPLITTING);
  store_region_meta->UpdateState(child_region, pb::common::StoreRegionState::SPLITTING);

  // Set split record
  parent_region->UpdateLastSplitTimestamp();
  pb::store_internal::RegionSplitRecord record;
  record.set_region_id(child_region_id);
  record.set_split_time(Helper::NowTime());
  parent_region->AddChild(record);

  // Increase region version
  store_region_meta->UpdateEpochVersionAndRange(child_region, child_region->Epoch().version() + 1, child_range);
  store_region_meta->UpdateEpochVersionAndRange(parent_region, parent_region->Epoch().version() + 1, parent_range);

  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] parent do snapshot", parent_region_id,
                                 child_region_id);
  // Do parent region snapshot
  LaunchDoSnapshot(parent_region);

  // Set do snapshot when bootstrap
  store_region_meta->UpdateNeedBootstrapDoSnapshot(child_region, true);

  // update to NORMAL after save snapshot in SplitClosure::Run
  store_region_meta->UpdateState(parent_region, pb::common::StoreRegionState::NORMAL);
  Heartbeat::TriggerStoreHeartbeat({parent_region->Id(), child_region->Id()}, true);

  return true;
}

// region-100: [start_key,end_key) ->
// region-101: [start_key, split_key) and region-100: [split_key, end_key)
int SplitHandler::Handle(std::shared_ptr<Context>, store::RegionPtr from_region, std::shared_ptr<RawEngine>,
                         const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t term_id,
                         int64_t log_id) {
  const auto &request = req.split();

  if (request.split_strategy() == pb::raft::PRE_CREATE_REGION) {
    bool ret = HandlePreCreateRegionSplit(request, from_region, term_id, log_id);
    if (!ret) {
      return 0;
    }
  } else {
    bool ret = HandlePostCreateRegionSplit(request, from_region, term_id, log_id);
    if (!ret) {
      return 0;
    }
  }

  auto store_raft_meata = Server::GetInstance().GetStoreMetaManager()->GetStoreRaftMeta();
  if (store_raft_meata != nullptr) {
    store_raft_meata->SaveRaftMeta(from_region->Id());
  }

  // Update region metrics min/max key policy
  if (region_metrics != nullptr) {
    region_metrics->UpdateMaxAndMinKeyPolicy();
  }

  return 0;
}

// Get raft log entries.
static std::vector<pb::raft::LogEntry> GetRaftLogEntries(int64_t region_id, int64_t begin_log_id, int64_t end_log_id) {
  auto log_storage = Server::GetInstance().GetLogStorageManager()->GetLogStorage(region_id);
  auto log_entries = log_storage->GetEntrys(begin_log_id, end_log_id);
  std::vector<pb::raft::LogEntry> pb_log_entries;
  pb_log_entries.resize(log_entries.size());
  for (int i = 0; i < log_entries.size(); ++i) {
    auto &pb_log_entry = pb_log_entries[i];
    pb_log_entry.set_index(log_entries[i]->index);
    pb_log_entry.set_term(log_entries[i]->term);
    std::string data;
    log_entries[i]->data.copy_to(&data);
    pb_log_entry.mutable_data()->swap(data);
  }

  return pb_log_entries;
}

static void LaunchCommitMergeCommand(const pb::raft::PrepareMergeRequest &request,
                                     const pb::common::RegionDefinition &source_region_definition,
                                     store::RegionPtr target_region, int64_t prepare_merge_log_id) {
  auto storage = Server::GetInstance().GetStorage();
  assert(storage != nullptr);
  auto node = storage->GetRaftStoreEngine()->GetNode(source_region_definition.id());
  assert(node != nullptr);

  uint64_t start_time = Helper::TimestampMs();
  // Generate LogEntry.
  auto log_entries =
      GetRaftLogEntries(source_region_definition.id(), request.min_applied_log_id() + 1, prepare_merge_log_id);

  // Timing commit CommitMerge command to target region
  // Just target region leader node will success
  int retry_count = 0;
  for (;;) {
    // CommitMerge command already commit success
    if (target_region->LastChangeCmdId() >= request.merge_id()) {
      break;
    }

    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(request.target_region_id());
    ctx->SetRegionEpoch(request.target_region_epoch());

    // Try to commit local target region raft.
    auto status =
        storage->CommitMerge(ctx, request.merge_id(), source_region_definition, prepare_merge_log_id, log_entries);
    DINGO_LOG(INFO) << fmt::format(
        "[merge.merging][merge_id({}).region({}/{})] Commit CommitMerge failed, times({}) error: {} {}",
        request.merge_id(), source_region_definition.id(), request.target_region_id(), ++retry_count,
        pb::error::Errno_Name(status.error_code()), status.error_str());

    bthread_usleep(500000);  // 500ms
  }

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][merge_id({}).region({}/{})] Commit CommitMerge finish, log_entries({}) elapsed time({}ms)",
      request.merge_id(), source_region_definition.id(), request.target_region_id(), log_entries.size(),
      Helper::TimestampMs() - start_time);
}

int PrepareMergeHandler::Handle(std::shared_ptr<Context>, store::RegionPtr source_region, std::shared_ptr<RawEngine>,
                                const pb::raft::Request &req, store::RegionMetricsPtr /*region_metrics*/,
                                int64_t /*term_id*/, int64_t log_id) {
  const auto &request = req.prepare_merge();
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();
  auto target_region = store_region_meta->GetRegion(request.target_region_id());

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][merge_id({}).region({}/{})] Apply PrepareMerge, source_region({}/{}/log[{},{})) "
      "target_region({}/{})",
      request.merge_id(), source_region->Id(), request.target_region_id(), source_region->EpochToString(),
      source_region->RangeToString(), request.min_applied_log_id(), log_id,
      Helper::RegionEpochToString(request.target_region_epoch()), Helper::RangeToString(request.target_region_range()));

  uint64_t start_time = Helper::TimestampMs();

  FAIL_POINT("apply_prepare_merge");

  // Update last_change_cmd_id
  store_region_meta->UpdateLastChangeCmdId(source_region, request.merge_id());
  // Update disable_change
  store_region_meta->UpdateTemporaryDisableChange(source_region, true);

  // Validate
  if (target_region != nullptr) {
    int retry_count = 0;
    for (;;) {
      int comparison = Helper::CompareRegionEpoch(target_region->Epoch(), request.target_region_epoch());
      if (comparison == 0) {
        break;

      } else if (comparison < 0) {
        DINGO_LOG(WARNING) << fmt::format(
            "[merge.merging][merge_id({}).region({}/{})] waiting({}), target region epoch({}) less the special "
            "epoch({})",
            request.merge_id(), source_region->Id(), target_region->Id(), ++retry_count,
            target_region->Epoch().version(), request.target_region_epoch().version());
        bthread_usleep(100000);  // 100ms
        target_region = store_region_meta->GetRegion(request.target_region_id());

      } else if (comparison > 0) {
        // Todo
        DINGO_LOG(FATAL) << fmt::format(
            "[merge.merging][merge_id({}).region({}/{})] epoch not match, source_region({}/{}) "
            "target_region({}/{}/{}) ",
            request.merge_id(), source_region->Id(), target_region->Id(), source_region->EpochToString(),
            source_region->RangeToString(), Helper::RegionEpochToString(request.target_region_epoch()),
            target_region->EpochToString(), target_region->RangeToString());
        return 0;
      }
    }
  } else {
    DINGO_LOG(INFO) << fmt::format(
        "[merge.merging][merge_id({}).region({}/{})] Apply PrepareMerge, target region is nullptr.", request.merge_id(),
        source_region->Id(), request.target_region_id());
  }

  // Set source region state.
  store_region_meta->UpdateState(source_region, pb::common::StoreRegionState::MERGING);

  FAIL_POINT("before_launch_commit_merge");

  // Get source region definition.
  auto source_region_definition = source_region->Definition();
  // Set source region epoch/range.
  auto new_range = source_region_definition.range();
  new_range.set_start_key(Helper::GenMaxStartKey());
  int64_t new_version = source_region_definition.epoch().version() + 1;
  store_region_meta->UpdateEpochVersionAndRange(source_region, new_version, new_range);

  FAIL_POINT("before_launch_commit_merge");

  if (target_region != nullptr) {
    // Commit raft command CommitMerge.
    LaunchCommitMergeCommand(request, source_region_definition, target_region, log_id);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][merge_id({}).region({}/{})] Apply PrepareMerge finish, source_region({}/{}/log[{},{})) "
      "target_region({}/{}) elapsed_time({})",
      request.merge_id(), source_region->Id(), request.target_region_id(), source_region->EpochToString(),
      source_region->RangeToString(), request.min_applied_log_id(), log_id,
      Helper::RegionEpochToString(request.target_region_epoch()), Helper::RangeToString(request.target_region_range()),
      Helper::TimestampMs() - start_time);

  return 0;
}

int CommitMergeHandler::Handle(std::shared_ptr<Context>, store::RegionPtr target_region, std::shared_ptr<RawEngine>,
                               const pb::raft::Request &req, store::RegionMetricsPtr, int64_t, int64_t /*log_id*/) {
  assert(target_region != nullptr);
  const auto &request = req.commit_merge();
  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();
  assert(store_region_meta != nullptr);
  auto raft_store_engine = Server::GetInstance().GetStorage()->GetRaftStoreEngine();
  assert(raft_store_engine != nullptr);

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][merge_id({}).region({}/{})] Apply CommitMerge, source_region({}/{}/{}) target_region({}/{}).",
      request.merge_id(), request.source_region_id(), target_region->Id(),
      Helper::RegionEpochToString(request.source_region_epoch()), Helper::RangeToString(request.source_region_range()),
      request.entries().size(), target_region->EpochToString(), target_region->RangeToString());

  uint64_t start_time = Helper::TimestampMs();

  FAIL_POINT("apply_commit_merge");

  // Update last_change_cmd_id
  store_region_meta->UpdateLastChangeCmdId(target_region, request.merge_id());
  // Disable temporary change
  store_region_meta->UpdateTemporaryDisableChange(target_region, true);

  auto source_region = store_region_meta->GetRegion(request.source_region_id());
  if (source_region == nullptr) {
    DINGO_LOG(FATAL) << fmt::format(
        "[merge.merging][merge_id({}).region({}/{})] Apply CommitMerge, source region is nullptr.", request.merge_id(),
        request.source_region_id(), target_region->Id());
    return 0;
  }

  // Catch up apply source region raft log.
  auto node = raft_store_engine->GetNode(source_region->Id());
  if (node == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[merge.merging][merge_id({}).region({}/{})] Not found source node.",
                                    request.merge_id(), request.source_region_id(), target_region->Id());
    return 0;
  }
  auto state_machine = std::dynamic_pointer_cast<StoreStateMachine>(node->GetStateMachine());
  if (state_machine == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[merge.merging][merge_id({}).region({}/{})] Not found source state machine.",
                                    request.merge_id(), request.source_region_id(), target_region->Id());
    return 0;
  }

  int32_t actual_apply_log_count = 0;
  if (!request.entries().empty()) {
    actual_apply_log_count = state_machine->CatchUpApplyLog(Helper::PbRepeatedToVector(request.entries()));
  }

  // Set source region TOMBSTONE state
  store_region_meta->UpdateState(source_region, pb::common::StoreRegionState::TOMBSTONE);

  FAIL_POINT("before_commit_merge_modify_epoch");

  // Set target region range and epoch
  // range: source range + target range
  // epoch: max(source_epoch, target_epoch) + 1
  pb::common::Range new_range;
  // left(source region) right(target_region)
  if (request.source_region_range().end_key() == target_region->Range().start_key()) {
    new_range.set_start_key(request.source_region_range().start_key());
    new_range.set_end_key(target_region->Range().end_key());
  } else {
    // left(target region) right(source region)
    new_range.set_start_key(target_region->Range().start_key());
    new_range.set_end_key(request.source_region_range().end_key());
  }
  int64_t new_version = std::max(request.source_region_epoch().version(), target_region->Epoch().version()) + 1;

  store_region_meta->UpdateEpochVersionAndRange(target_region, new_version, new_range);

  // Do snapshot
  LaunchDoSnapshot(target_region);

  if (target_region->Type() == pb::common::INDEX_REGION) {
    // Set child share vector index
    auto vector_index = source_region->VectorIndexWrapper()->GetOwnVectorIndex();
    if (vector_index != nullptr) {
      target_region->VectorIndexWrapper()->SetSiblingVectorIndex(vector_index);
    } else {
      DINGO_LOG(WARNING) << fmt::format(
          "[merge.merging][merge_id({}).region({}/{})] merge region get vector index failed.", source_region->Id(),
          target_region->Id());
    }

    // Rebuild vector index
    VectorIndexManager::LaunchRebuildVectorIndex(target_region->VectorIndexWrapper(), true);
  } else {
    store_region_meta->UpdateTemporaryDisableChange(target_region, false);
  }

  // Notify coordinator
  Heartbeat::TriggerStoreHeartbeat({request.source_region_id(), target_region->Id()}, true);

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][merge_id({}).region({}/{})] Apply CommitMerge finish, source_region({}/{}/{}/{}) "
      "target_region({}/{}) elapsed_time({}).",
      request.merge_id(), request.source_region_id(), target_region->Id(),
      Helper::RegionEpochToString(request.source_region_epoch()), Helper::RangeToString(request.source_region_range()),
      request.entries().size(), actual_apply_log_count, target_region->EpochToString(), target_region->RangeToString(),
      Helper::TimestampMs() - start_time);

  return 0;
}

int RollbackMergeHandler::Handle(std::shared_ptr<Context>, store::RegionPtr /*source_region*/,
                                 std::shared_ptr<RawEngine>, const pb::raft::Request &req,
                                 store::RegionMetricsPtr /*region_metrics*/, int64_t /*term_id*/, int64_t /*log_id*/) {
  const auto &request = req.rollback_merge();

  return 0;
}

int SaveRaftSnapshotHandler::Handle(std::shared_ptr<Context>, store::RegionPtr region, std::shared_ptr<RawEngine>,
                                    const pb::raft::Request &, store::RegionMetricsPtr, int64_t term_id,
                                    int64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[split.spliting][region({}->{})] save snapshot, term({}) log_id({})",
                                 region->ParentId(), region->Id(), term_id, log_id);
  auto engine = Server::GetInstance().GetEngine();
  std::shared_ptr<Context> to_ctx = std::make_shared<Context>();
  to_ctx->SetDone(new SplitHandler::SplitClosure(region));
  auto status = engine->DoSnapshot(to_ctx, region->Id());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] do snapshot failed, error: {}",
                                    region->ParentId(), region->Id(), status.error_str());
  }

  return 0;
}

int VectorAddHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region, std::shared_ptr<RawEngine> engine,
                             const pb::raft::Request &req, store::RegionMetricsPtr /*region_metrics*/,
                             int64_t /*term_id*/, int64_t log_id) {
  auto set_ctx_status = [ctx](butil::Status status) {
    if (ctx) {
      ctx->SetStatus(status);
    }
  };

  butil::Status status;
  const auto &request = req.vector_add();

  // Transform vector to kv
  std::map<std::string, std::vector<pb::common::KeyValue>> kv_puts_with_cf;
  std::map<std::string, std::vector<std::string>> kv_deletes_with_cf;

  std::vector<pb::common::KeyValue> kvs_default;  // for vector data
  std::vector<pb::common::KeyValue> kvs_scalar;   // for vector scalar data
  std::vector<pb::common::KeyValue> kvs_table;    // for vector table data

  auto region_start_key = region->Range().start_key();
  auto region_part_id = region->PartitionId();
  for (const auto &vector : request.vectors()) {
    // vector data
    {
      pb::common::KeyValue kv;
      std::string key;
      // VectorCodec::EncodeVectorData(region->PartitionId(), vector.id(), key);
      VectorCodec::EncodeVectorKey(region_start_key[0], region_part_id, vector.id(), key);

      kv.mutable_key()->swap(key);
      kv.set_value(vector.vector().SerializeAsString());
      kvs_default.push_back(kv);
    }
    // vector scalar data
    {
      pb::common::KeyValue kv;
      std::string key;
      // VectorCodec::EncodeVectorScalar(region->PartitionId(), vector.id(), key);
      VectorCodec::EncodeVectorKey(region_start_key[0], region_part_id, vector.id(), key);
      kv.mutable_key()->swap(key);
      kv.set_value(vector.scalar_data().SerializeAsString());
      kvs_scalar.push_back(kv);
    }
    // vector table data
    {
      pb::common::KeyValue kv;
      std::string key;
      // VectorCodec::EncodeVectorTable(region->PartitionId(), vector.id(), key);
      VectorCodec::EncodeVectorKey(region_start_key[0], region_part_id, vector.id(), key);
      kv.mutable_key()->swap(key);
      kv.set_value(vector.table_data().SerializeAsString());
      kvs_table.push_back(kv);
    }
  }
  kv_puts_with_cf.insert_or_assign(Constant::kStoreDataCF, kvs_default);
  kv_puts_with_cf.insert_or_assign(Constant::kVectorScalarCF, kvs_scalar);
  kv_puts_with_cf.insert_or_assign(Constant::kVectorTableCF, kvs_table);

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
  int64_t vector_index_id = vector_index_wrapper->Id();
  bool is_ready = vector_index_wrapper->IsReady();
  // if leadder vector_index is nullptr, return internal error
  if (ctx != nullptr && !is_ready) {
    DINGO_LOG(ERROR) << fmt::format("Not found vector index {}", vector_index_id);
    status = butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "Not found vector index %ld", vector_index_id);
    set_ctx_status(status);
    return 0;
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
          DINGO_LOG(WARNING) << fmt::format("vector index {} is full", vector_index_id);
          status = butil::Status(pb::error::EVECTOR_INDEX_FULL, "Vector index %lu is full", vector_index_id);
        } else if (ret.error_code() == pb::error::Errno::EVECTOR_INDEX_NOT_FOUND) {
          DINGO_LOG(WARNING) << fmt::format("vector index {} is not found.", vector_index_id);
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
  if (!kv_puts_with_cf.empty() && status.ok()) {
    auto writer = engine->Writer();
    status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
    if (status.error_code() == pb::error::Errno::EINTERNAL) {
      DINGO_LOG(FATAL) << "[raft.apply][region(" << region->Id()
                       << ")] VectorAdd->KvBatchPutAndDelete failed, error: " << status.error_str();
    }

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

  return 0;
}

int VectorDeleteHandler::Handle(std::shared_ptr<Context> ctx, store::RegionPtr region,
                                std::shared_ptr<RawEngine> engine, const pb::raft::Request &req,
                                store::RegionMetricsPtr /*region_metrics*/, int64_t /*term_id*/, int64_t log_id) {
  auto set_ctx_status = [ctx](butil::Status status) {
    if (ctx) {
      ctx->SetStatus(status);
    }
  };

  butil::Status status;
  const auto &request = req.vector_delete();

  auto reader = engine->Reader();
  auto snapshot = engine->GetSnapshot();
  if (!snapshot) {
    DINGO_LOG(FATAL) << "[raft.apply][region(" << region->Id() << ")][cf_name(" << request.cf_name()
                     << ")] GetSnapshot failed";
  }

  if (request.ids_size() == 0) {
    DINGO_LOG(WARNING) << fmt::format("vector_delete ids_size is 0, region_id={}", region->Id());
    status = butil::Status::OK();
    set_ctx_status(status);
    return 0;
  }

  // Transform vector to kv
  std::map<std::string, std::vector<pb::common::KeyValue>> kv_puts_with_cf;
  std::map<std::string, std::vector<std::string>> kv_deletes_with_cf;

  std::vector<std::string> kv_deletes_default;

  std::vector<bool> key_states(request.ids_size(), false);
  // std::vector<std::string> keys;
  std::vector<int64_t> delete_ids;

  auto region_start_key = region->Range().start_key();
  auto region_part_id = region->PartitionId();
  for (int i = 0; i < request.ids_size(); i++) {
    // set key_states
    std::string key;
    VectorCodec::EncodeVectorKey(region_start_key[0], region_part_id, request.ids(i), key);

    std::string value;
    auto ret = reader->KvGet(request.cf_name(), snapshot, key, value);
    if (ret.ok()) {
      kv_deletes_default.push_back(key);

      key_states[i] = true;
      delete_ids.push_back(request.ids(i));

      DINGO_LOG(DEBUG) << fmt::format("vector_delete id={}, region_id={}", request.ids(i), region->Id());
    }
  }

  if (!kv_deletes_default.empty()) {
    kv_deletes_with_cf.insert_or_assign(Constant::kStoreDataCF, kv_deletes_default);
    kv_deletes_with_cf.insert_or_assign(Constant::kVectorScalarCF, kv_deletes_default);
    kv_deletes_with_cf.insert_or_assign(Constant::kVectorTableCF, kv_deletes_default);
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  int64_t vector_index_id = vector_index_wrapper->Id();
  bool is_ready = vector_index_wrapper->IsReady();
  // if leadder vector_index is nullptr, return internal error
  if (ctx != nullptr && !is_ready) {
    DINGO_LOG(ERROR) << fmt::format("Not found vector index {}", vector_index_id);
    status = butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "Not found vector index %ld", vector_index_id);
    set_ctx_status(status);
    return 0;
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
        } else if (ret.error_code() == pb::error::Errno::EVECTOR_INDEX_NOT_FOUND) {
          DINGO_LOG(WARNING) << fmt::format("vector index {} is not found.", vector_index_id);
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
  if (!kv_deletes_with_cf.empty() && status.ok()) {
    auto writer = engine->Writer();
    status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
    if (status.error_code() == pb::error::Errno::EINTERNAL) {
      DINGO_LOG(FATAL) << "[raft.apply][region(" << region->Id()
                       << ")] VectorDelete->KvBatchDelete failed, error: " << status.error_str();
    }

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

  return 0;
}

int RebuildVectorIndexHandler::Handle(std::shared_ptr<Context>, store::RegionPtr region, std::shared_ptr<RawEngine>,
                                      [[maybe_unused]] const pb::raft::Request &req, store::RegionMetricsPtr, int64_t,
                                      int64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.rebuild][index_id({})] Handle rebuild vector index, apply_log_id: {}",
                                 region->Id(), log_id);
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper != nullptr) {
    vector_index_wrapper->SaveApplyLogId(log_id);

    VectorIndexManager::LaunchRebuildVectorIndex(vector_index_wrapper, true);
  }

  return 0;
}

std::shared_ptr<HandlerCollection> RaftApplyHandlerFactory::Build() {
  auto handler_collection = std::make_shared<HandlerCollection>();
  handler_collection->Register(std::make_shared<PutHandler>());
  handler_collection->Register(std::make_shared<DeleteRangeHandler>());
  handler_collection->Register(std::make_shared<DeleteBatchHandler>());
  handler_collection->Register(std::make_shared<SplitHandler>());
  handler_collection->Register(std::make_shared<PrepareMergeHandler>());
  handler_collection->Register(std::make_shared<CommitMergeHandler>());
  handler_collection->Register(std::make_shared<RollbackMergeHandler>());
  handler_collection->Register(std::make_shared<VectorAddHandler>());
  handler_collection->Register(std::make_shared<VectorDeleteHandler>());
  handler_collection->Register(std::make_shared<RebuildVectorIndexHandler>());
  handler_collection->Register(std::make_shared<SaveRaftSnapshotHandler>());
  handler_collection->Register(std::make_shared<TxnHandler>());

  return handler_collection;
}

}  // namespace dingodb

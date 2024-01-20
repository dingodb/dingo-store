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
#include "common/failpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/role.h"
#include "config/config_manager.h"
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

DECLARE_int32(init_election_timeout_ms);

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
    status = writer->KvBatchPutAndDelete(request.cf_name(), Helper::PbRepeatedToVector(request.kvs()), {});
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
      std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
      range_with_cfs[request.cf_name()] = Helper::PbRepeatedToVector(request.ranges());
      status = writer->KvBatchDeleteRange(range_with_cfs);
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
    status = writer->KvBatchPutAndDelete(request.cf_name(), {}, Helper::PbRepeatedToVector(request.keys()));
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

static void LaunchAyncSaveSnapshot(store::RegionPtr region) {  // NOLINT
  auto store_region_meta = GET_STORE_REGION_META;
  store_region_meta->UpdateNeedBootstrapDoSnapshot(region, true);
  auto engine = Server::GetInstance().GetEngine();

  bool is_success = false;
  for (int i = 0; i < Constant::kSplitDoSnapshotRetryTimes; ++i) {
    auto *done = new SplitHandler::SplitClosure(region->Id());
    auto ctx = std::make_shared<Context>();
    ctx->SetDone(done);
    auto status = engine->AyncSaveSnapshot(ctx, region->Id(), true);
    if (status.ok()) {
      is_success = true;
      break;
    }

    delete done;
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({})] do snapshot failed, retry({}) error: {}",
                                    region->Id(), i, status.error_str());
    bthread_usleep(1000 * 100);
  }

  if (!is_success) {
    store_region_meta->UpdateNeedBootstrapDoSnapshot(region, false);
  }
}

void SplitHandler::SplitClosure::Run() {
  std::unique_ptr<SplitClosure> self_guard(this);

  auto region = Server::GetInstance().GetRegion(region_id_);
  if (region == nullptr) {
    DINGO_LOG(INFO) << fmt::format("[split.spliting][region({})] not found region.", region_id_);
    return;
  }

  if (!status().ok()) {
    DINGO_LOG(WARNING) << fmt::format("[split.spliting][region({})] finish snapshot failed, error: {}", region_id_,
                                      status().error_str());
    bthread_usleep(1000 * 100);

    // Retry do snapshot
    LaunchAyncSaveSnapshot(region);
    return;
  } else {
    DINGO_LOG(INFO) << fmt::format("[split.spliting][region({})] finish snapshot success", region_id_);
  }

  auto store_region_meta = Server::GetInstance().GetStoreMetaManager()->GetStoreRegionMeta();

  if (region->Type() == pb::common::STORE_REGION) {
    store_region_meta->UpdateTemporaryDisableChange(region, false);
  }

  store_region_meta->UpdateNeedBootstrapDoSnapshot(region, false);
}

// Pre create region split
bool HandlePreCreateRegionSplit(const pb::raft::SplitRequest &request, store::RegionPtr from_region, int64_t term_id,
                                int64_t log_id) {
  auto store_region_meta = GET_STORE_REGION_META;

  ADD_REGION_CHANGE_RECORD(request);
  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Appling SplitRequest");

  // Update last_change_cmd_id
  store_region_meta->UpdateLastChangeJobId(from_region, request.job_id());

  if (request.epoch().version() != from_region->Epoch().version()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[split.spliting][job_id({}).region({}->{})] region version changed, split version({}) region version({})",
        request.job_id(), request.from_region_id(), request.to_region_id(), request.epoch().version(),
        from_region->Epoch().version());
    return false;
  }

  auto to_region = store_region_meta->GetRegion(request.to_region_id());
  if (to_region == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[split.spliting][job_id({}).region({}->{})] child region not found",
                                    request.job_id(), request.from_region_id(), request.to_region_id());
    return false;
  }

  if (to_region->State() != pb::common::StoreRegionState::STANDBY) {
    DINGO_LOG(FATAL) << fmt::format("[split.spliting][job_id({}).region({}->{})] child region state is not standby",
                                    request.job_id(), from_region->Id(), to_region->Id());
    return false;
  }
  if (from_region->Range().start_key() >= from_region->Range().end_key()) {
    DINGO_LOG(FATAL) << fmt::format("[split.spliting][job_id({}).region({}->{})] from region invalid range [{}-{})",
                                    request.job_id(), from_region->Id(), to_region->Id(),
                                    Helper::StringToHex(from_region->Range().start_key()),
                                    Helper::StringToHex(from_region->Range().end_key()));
    return false;
  }
  if (request.split_key() < from_region->Range().start_key() || request.split_key() > from_region->Range().end_key()) {
    DINGO_LOG(FATAL) << fmt::format(
        "[split.spliting][job_id({}).region({}->{})] from region invalid split key {} region range: [{}-{})",
        request.job_id(), from_region->Id(), to_region->Id(), Helper::StringToHex(request.split_key()),
        Helper::StringToHex(from_region->Range().start_key()), Helper::StringToHex(from_region->Range().end_key()));
    return false;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[split.spliting][job_id({}).region({}->{})] split, term({}) log_id({}) split_key({}) epoch({}) parent "
      "region({}/{})",
      request.job_id(), from_region->Id(), to_region->Id(), term_id, log_id, Helper::StringToHex(request.split_key()),
      Helper::RegionEpochToString(request.epoch()), from_region->EpochToString(), from_region->RangeToString());

  // temporary disable split/merge/change_peer, avoid overlap change.
  store_region_meta->UpdateTemporaryDisableChange(from_region, true);
  store_region_meta->UpdateTemporaryDisableChange(to_region, true);

  pb::common::Range to_range;
  // child range
  to_range.set_start_key(from_region->Range().start_key());
  to_range.set_end_key(request.split_key());

  // parent range
  pb::common::Range from_range;
  from_range.set_start_key(request.split_key());
  from_range.set_end_key(from_region->Range().end_key());

  to_region->SetParentId(from_region->Id());

  // Note: full heartbeat do not report region metrics when the region is in SPLITTING or MERGING
  store_region_meta->UpdateState(to_region, pb::common::StoreRegionState::SPLITTING);
  store_region_meta->UpdateState(from_region, pb::common::StoreRegionState::SPLITTING);

  // set child region version/range/state
  store_region_meta->UpdateEpochVersionAndRange(to_region, to_region->Epoch().version() + 1, to_range, "split child");

  // set parent region version/range/state
  store_region_meta->UpdateEpochVersionAndRange(from_region, from_region->Epoch().version() + 1, from_range,
                                                "split parent");

  DINGO_LOG(INFO) << fmt::format(
      "[split.spliting][job_id({}).region({}->{})] splited, child region({}/{}) parent region({}/{})", request.job_id(),
      from_region->Id(), to_region->Id(), to_region->EpochToString(), to_region->RangeToString(),
      from_region->EpochToString(), from_region->RangeToString());

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Doing raft snapshot");
  DINGO_LOG(INFO) << fmt::format("[split.spliting][job_id({}).region({}->{})] parent do snapshot", request.job_id(),
                                 from_region->Id(), to_region->Id());

  // Do parent region snapshot
  LaunchAyncSaveSnapshot(from_region);

  DINGO_LOG(INFO) << fmt::format("[split.spliting][job_id({}).region({}->{})] child do snapshot", request.job_id(),
                                 from_region->Id(), to_region->Id());
  // Do child region snapshot
  LaunchAyncSaveSnapshot(to_region);

  // update StoreRegionState to NORMAL
  store_region_meta->UpdateState(from_region, pb::common::StoreRegionState::NORMAL);
  store_region_meta->UpdateState(to_region, pb::common::StoreRegionState::NORMAL);

  if (to_region->Type() == pb::common::INDEX_REGION) {
    // Set child share vector index
    auto vector_index = from_region->VectorIndexWrapper()->GetOwnVectorIndex();
    if (vector_index != nullptr) {
      to_region->VectorIndexWrapper()->SetShareVectorIndex(vector_index);
    } else {
      DINGO_LOG(INFO) << fmt::format("[split.spliting][job_id({}).region({}->{})] not found parent vector index.",
                                     request.job_id(), from_region->Id(), to_region->Id());
    }

    ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Launch rebuild vector index");
    // Rebuild vector index
    if (Server::GetInstance().IsLeader(to_region->Id())) {
      VectorIndexManager::LaunchRebuildVectorIndex(to_region->VectorIndexWrapper(), request.job_id(), "child split");
    } else {
      DINGO_LOG(INFO) << fmt::format(
          "[split.spliting][job_id({}).region({}->{})] child follower not need rebuild vector index.", request.job_id(),
          from_region->Id(), to_region->Id());

      auto vector_index_wrapper = to_region->VectorIndexWrapper();
      vector_index_wrapper->SetIsTempHoldVectorIndex(false);
      if (!VectorIndexWrapper::IsPermanentHoldVectorIndex(vector_index_wrapper->Id())) {
        vector_index_wrapper->ClearVectorIndex("child split");
      }

      store_region_meta->UpdateTemporaryDisableChange(to_region, false);
      ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(),
                                         fmt::format("Clear follower vector index {}", to_region->Id()));
    }

    if (Server::GetInstance().IsLeader(from_region->Id())) {
      VectorIndexManager::LaunchRebuildVectorIndex(from_region->VectorIndexWrapper(), request.job_id(), "parent split");
    } else {
      DINGO_LOG(INFO) << fmt::format(
          "[split.spliting][job_id({}).region({}->{})] parent follower not need rebuild vector index.",
          request.job_id(), from_region->Id(), to_region->Id());

      auto vector_index_wrapper = from_region->VectorIndexWrapper();
      vector_index_wrapper->SetIsTempHoldVectorIndex(false);
      if (!VectorIndexWrapper::IsPermanentHoldVectorIndex(vector_index_wrapper->Id())) {
        vector_index_wrapper->ClearVectorIndex("parent split");
      }

      store_region_meta->UpdateTemporaryDisableChange(from_region, false);
      ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(),
                                         fmt::format("Clear follower vector index {}", from_region->Id()));
    }
  }

  Heartbeat::TriggerStoreHeartbeat({from_region->Id(), to_region->Id()}, true);

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Applied SplitRequest");

  return true;
}

store::RegionPtr CreateNewRegion(std::shared_ptr<RaftStoreEngine> raft_store_engine,
                                 std::shared_ptr<RaftNode> parent_node, const pb::common::RegionDefinition &definition,
                                 int64_t parent_region_id) {  // NOLINT
  store::RegionPtr region = store::Region::New(definition);
  region->SetState(pb::common::SPLITTING);
  region->SetSplitStrategy(pb::raft::POST_CREATE_REGION);
  region->SetParentId(parent_region_id);

  auto raft_meta = store::RaftMeta::New(region->Id());
  ADD_RAFT_META(raft_meta);

  auto config = ConfigManager::GetInstance().GetRoleConfig();
  RaftControlAble::AddNodeParameter parameter;
  parameter.role = GetRole();
  parameter.is_restart = false;
  parameter.raft_endpoint = Server::GetInstance().RaftEndpoint();

  parameter.raft_path = config->GetString("raft.path");
  parameter.election_timeout_ms =
      parent_node->IsLeader() ? FLAGS_init_election_timeout_ms : 30 * FLAGS_init_election_timeout_ms;
  parameter.log_max_segment_size = config->GetInt64("raft.segmentlog_max_segment_size");
  parameter.log_path = config->GetString("raft.log_path");

  parameter.raft_meta = raft_meta;
  auto region_metrics = StoreRegionMetrics::NewMetrics(region->Id());
  parameter.region_metrics = region_metrics;
  auto listener_factory = std::make_shared<StoreSmEventListenerFactory>();
  parameter.listeners = listener_factory->Build();

  auto status = raft_store_engine->AddNode(region, parameter);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[split.spliting][region({}->{})] add node failed, error: {}", parent_region_id,
                                    region->Id(), status.error_str());
    return nullptr;
  }

  ADD_REGION_METRICS(region_metrics);
  GET_STORE_REGION_META->AddRegion(region);

  Server::GetInstance().GetRegionController()->RegisterExecutor(region->Id());

  return region;
}

// Post create region split
bool HandlePostCreateRegionSplit(const pb::raft::SplitRequest &request, store::RegionPtr parent_region, int64_t term_id,
                                 int64_t log_id) {
  auto store_region_meta = GET_STORE_REGION_META;
  int64_t parent_region_id = request.from_region_id();
  int64_t child_region_id = request.to_region_id();

  ADD_REGION_CHANGE_RECORD(request);
  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Appling SplitRequest");

  auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
  if (raft_store_engine == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[split.spliting][job_id({}).region({}->{})] Not found raft store engine.",
                                    request.job_id(), parent_region_id, child_region_id);
    return false;
  }
  auto parent_node = raft_store_engine->GetNode(parent_region_id);
  if (parent_node == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[split.spliting][job_id({}).region({}->{})] Not found raft node.",
                                    request.job_id(), parent_region_id, child_region_id);
    return false;
  }

  // Update last_change_cmd_id
  store_region_meta->UpdateLastChangeJobId(parent_region, request.job_id());

  if (request.epoch().version() != parent_region->Epoch().version()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[split.spliting][job_id({}).region({}->{})] region version changed, split version({}) region version({})",
        request.job_id(), parent_region_id, child_region_id, request.epoch().version(),
        parent_region->Epoch().version());
    return false;
  }

  auto old_range = parent_region->Range();

  if (old_range.start_key() >= old_range.end_key()) {
    DINGO_LOG(FATAL) << fmt::format("[split.spliting][job_id({}).region({}->{})] from region invalid range [{}-{})",
                                    request.job_id(), parent_region_id, child_region_id,
                                    Helper::StringToHex(old_range.start_key()),
                                    Helper::StringToHex(old_range.end_key()));
    return false;
  }

  if (request.split_key() < old_range.start_key() || request.split_key() > old_range.end_key()) {
    DINGO_LOG(FATAL) << fmt::format(
        "[split.spliting][job_id({}).region({}->{})] from region invalid split key {} region range: [{}-{})",
        request.job_id(), parent_region_id, child_region_id, Helper::StringToHex(request.split_key()),
        Helper::StringToHex(old_range.start_key()), Helper::StringToHex(old_range.end_key()));
    return false;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[split.spliting][job_id({}).region({}->{})] split, term({}) log_id({}) split_key({}) epoch({}) parent "
      "region({}/{})",
      request.job_id(), parent_region->Id(), child_region_id, term_id, log_id, Helper::StringToHex(request.split_key()),
      Helper::RegionEpochToString(request.epoch()), parent_region->EpochToString(), parent_region->RangeToString());

  // Set region state spliting
  store_region_meta->UpdateState(parent_region, pb::common::StoreRegionState::SPLITTING);

  // temporary disable split, avoid overlap change.
  store_region_meta->UpdateTemporaryDisableChange(parent_region, true);

  // Set child region definition
  pb::common::RegionDefinition definition = parent_region->Definition();
  definition.set_id(child_region_id);
  definition.set_name(fmt::format("{}_{}", definition.name(), child_region_id));
  definition.mutable_epoch()->set_conf_version(1);
  definition.mutable_epoch()->set_version(2);
  pb::common::Range child_range;
  child_range.set_start_key(old_range.start_key());
  child_range.set_end_key(request.split_key());
  *(definition.mutable_range()) = child_range;

  auto child_region = CreateNewRegion(raft_store_engine, parent_node, definition, parent_region->Id());
  if (child_region == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[split.spliting][region({}->{})] create child region failed.", parent_region_id,
                                    child_region_id);
    return false;
  }

  child_region->SetParentId(parent_region->Id());

  // Set parent/child range/epoch
  pb::common::Range parent_range;
  parent_range.set_start_key(request.split_key());
  parent_range.set_end_key(old_range.end_key());
  store_region_meta->UpdateEpochVersionAndRange(parent_region, parent_region->Epoch().version() + 1, parent_range,
                                                "split parent");
  // store_region_meta->UpdateEpochVersionAndRange(child_region, child_region->Epoch().version() + 1, child_range,
  //                                               "child parent");

  DINGO_LOG(INFO) << fmt::format(
      "[split.spliting][job_id({}).region({}->{})] splited, child region({}/{}) parent region({}/{})", request.job_id(),
      parent_region->Id(), child_region->Id(), child_region->EpochToString(), child_region->RangeToString(),
      parent_region->EpochToString(), parent_region->RangeToString());

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Doing raft snapshot");
  // Do region snapshot
  LaunchAyncSaveSnapshot(parent_region);

  // Set do snapshot when bootstrap
  store_region_meta->UpdateNeedBootstrapDoSnapshot(child_region, true);

  // update to NORMAL after save snapshot in SplitClosure::Run
  store_region_meta->UpdateState(parent_region, pb::common::StoreRegionState::NORMAL);
  store_region_meta->UpdateState(child_region, pb::common::StoreRegionState::NORMAL);

  if (parent_region->Type() == pb::common::INDEX_REGION) {
    // Set child share vector index
    auto vector_index = parent_region->VectorIndexWrapper()->GetOwnVectorIndex();
    if (vector_index != nullptr) {
      child_region->VectorIndexWrapper()->SetShareVectorIndex(vector_index);
    } else {
      DINGO_LOG(INFO) << fmt::format("[split.spliting][job_id({}).region({}->{})] not found parent vector index.",
                                     request.job_id(), parent_region->Id(), child_region->Id());
    }

    ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Launch rebuild vector index");
    // Rebuild vector index
    if (Server::GetInstance().IsLeader(child_region->Id())) {
      VectorIndexManager::LaunchRebuildVectorIndex(child_region->VectorIndexWrapper(), request.job_id(), "child split");
    } else {
      DINGO_LOG(INFO) << fmt::format(
          "[split.spliting][job_id({}).region({}->{})] child follower not need rebuild vector index.", request.job_id(),
          parent_region->Id(), child_region->Id());

      auto vector_index_wrapper = child_region->VectorIndexWrapper();
      vector_index_wrapper->SetIsTempHoldVectorIndex(false);
      if (!VectorIndexWrapper::IsPermanentHoldVectorIndex(vector_index_wrapper->Id())) {
        vector_index_wrapper->ClearVectorIndex("child split");
      }

      store_region_meta->UpdateTemporaryDisableChange(child_region, false);
      ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(),
                                         fmt::format("Clear follower vector index {}", child_region->Id()));
    }

    if (Server::GetInstance().IsLeader(parent_region->Id())) {
      VectorIndexManager::LaunchRebuildVectorIndex(parent_region->VectorIndexWrapper(), request.job_id(),
                                                   "parent split");
    } else {
      DINGO_LOG(INFO) << fmt::format(
          "[split.spliting][job_id({}).region({}->{})] parent follower not need rebuild vector index.",
          request.job_id(), parent_region->Id(), child_region->Id());
      auto vector_index_wrapper = parent_region->VectorIndexWrapper();
      vector_index_wrapper->SetIsTempHoldVectorIndex(false);
      if (!VectorIndexWrapper::IsPermanentHoldVectorIndex(vector_index_wrapper->Id())) {
        vector_index_wrapper->ClearVectorIndex("parent split");
      }

      store_region_meta->UpdateTemporaryDisableChange(parent_region, false);
      ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(),
                                         fmt::format("Clear follower vector index {}", parent_region->Id()));
    }
  }

  Heartbeat::TriggerStoreHeartbeat({parent_region->Id(), child_region->Id()}, true);

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Applied SplitRequest");

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
  // Update region_size in next collect region metrics
  if (region_metrics != nullptr) {
    region_metrics->ResetMetricsForRegionVersionUpdate();
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
    if (target_region->LastChangeJobId() >= request.job_id()) {
      break;
    }

    auto ctx = std::make_shared<Context>();
    ctx->SetRegionId(request.target_region_id());
    ctx->SetRegionEpoch(request.target_region_epoch());

    // Try to commit local target region raft.
    auto status =
        storage->CommitMerge(ctx, request.job_id(), source_region_definition, prepare_merge_log_id, log_entries);
    DINGO_LOG(INFO) << fmt::format(
        "[merge.merging][job_id({}).region({}/{})] Commit CommitMerge failed, times({}) error: {}", request.job_id(),
        source_region_definition.id(), request.target_region_id(), ++retry_count, Helper::PrintStatus(status));

    bthread_usleep(500000);  // 500ms
  }

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][job_id({}).region({}/{})] Commit CommitMerge finish, log_entries({}) elapsed time({}ms)",
      request.job_id(), source_region_definition.id(), request.target_region_id(), log_entries.size(),
      Helper::TimestampMs() - start_time);
}

int PrepareMergeHandler::Handle(std::shared_ptr<Context>, store::RegionPtr source_region, std::shared_ptr<RawEngine>,
                                const pb::raft::Request &req, store::RegionMetricsPtr /*region_metrics*/,
                                int64_t /*term_id*/, int64_t log_id) {
  const auto &request = req.prepare_merge();
  auto store_region_meta = GET_STORE_REGION_META;
  auto target_region = store_region_meta->GetRegion(request.target_region_id());

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][job_id({}).region({}/{})] Appling PrepareMerge, source_region({}/{}/log[{},{})) "
      "target_region({}/{})",
      request.job_id(), source_region->Id(), request.target_region_id(), source_region->EpochToString(),
      source_region->RangeToString(), request.min_applied_log_id(), log_id,
      Helper::RegionEpochToString(request.target_region_epoch()), Helper::RangeToString(request.target_region_range()));

  // Set change record.
  ADD_REGION_CHANGE_RECORD(request, source_region->Id());
  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Apply PrepareMerge");

  uint64_t start_time = Helper::TimestampMs();

  FAIL_POINT("apply_prepare_merge");

  // Update last_change_cmd_id
  store_region_meta->UpdateLastChangeJobId(source_region, request.job_id());
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
            "[merge.merging][job_id({}).region({}/{})] waiting({}), target region epoch({}) less the special "
            "epoch({})",
            request.job_id(), source_region->Id(), target_region->Id(), ++retry_count, target_region->Epoch().version(),
            request.target_region_epoch().version());
        bthread_usleep(100000);  // 100ms
        target_region = store_region_meta->GetRegion(request.target_region_id());

      } else if (comparison > 0) {
        // Todo
        DINGO_LOG(FATAL) << fmt::format(
            "[merge.merging][job_id({}).region({}/{})] epoch not match, source_region({}/{}) "
            "target_region({}/{}/{}) ",
            request.job_id(), source_region->Id(), target_region->Id(), source_region->EpochToString(),
            source_region->RangeToString(), Helper::RegionEpochToString(request.target_region_epoch()),
            target_region->EpochToString(), target_region->RangeToString());
        return 0;
      }
    }
  } else {
    DINGO_LOG(INFO) << fmt::format(
        "[merge.merging][job_id({}).region({}/{})] Apply PrepareMerge, target region is nullptr.", request.job_id(),
        source_region->Id(), request.target_region_id());
  }

  // Set source region state.
  store_region_meta->UpdateState(source_region, pb::common::StoreRegionState::MERGING);

  FAIL_POINT("before_launch_commit_merge");

  // Get source region definition.
  auto source_region_definition = source_region->Definition();
  // Set source region epoch/range.
  int64_t new_version = source_region_definition.epoch().version() + 1;
  store_region_meta->UpdateEpochVersionAndRange(source_region, new_version, source_region_definition.range(),
                                                "merge source");

  FAIL_POINT("before_launch_commit_merge");

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Launch CommitMerge");

  if (target_region != nullptr) {
    // Commit raft command CommitMerge.
    LaunchCommitMergeCommand(request, source_region_definition, target_region, log_id);
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Launch CommitMerge finish");

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][job_id({}).region({}/{})] Applied PrepareMerge, source_region({}/{}/log[{},{})) "
      "target_region({}/{}) elapsed_time({})",
      request.job_id(), source_region->Id(), request.target_region_id(), source_region->EpochToString(),
      source_region->RangeToString(), request.min_applied_log_id(), log_id,
      Helper::RegionEpochToString(request.target_region_epoch()), Helper::RangeToString(request.target_region_range()),
      Helper::TimestampMs() - start_time);

  return 0;
}

int CommitMergeHandler::Handle(std::shared_ptr<Context>, store::RegionPtr target_region, std::shared_ptr<RawEngine>,
                               const pb::raft::Request &req, store::RegionMetricsPtr region_metrics, int64_t,
                               int64_t /*log_id*/) {
  assert(target_region != nullptr);
  const auto &request = req.commit_merge();
  auto store_region_meta = GET_STORE_REGION_META;
  assert(store_region_meta != nullptr);
  auto raft_store_engine = Server::GetInstance().GetStorage()->GetRaftStoreEngine();
  assert(raft_store_engine != nullptr);

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][job_id({}).region({}/{})] Appling CommitMerge, source_region({}/{}/{}) target_region({}/{}).",
      request.job_id(), request.source_region_id(), target_region->Id(),
      Helper::RegionEpochToString(request.source_region_epoch()), Helper::RangeToString(request.source_region_range()),
      request.entries().size(), target_region->EpochToString(), target_region->RangeToString());

  // Set change record.
  ADD_REGION_CHANGE_RECORD(request, target_region->Id());
  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Apply CommitMerge");

  uint64_t start_time = Helper::TimestampMs();

  FAIL_POINT("apply_commit_merge");

  // Update last_change_cmd_id
  store_region_meta->UpdateLastChangeJobId(target_region, request.job_id());
  // Disable temporary change
  store_region_meta->UpdateTemporaryDisableChange(target_region, true);

  auto source_region = store_region_meta->GetRegion(request.source_region_id());
  if (source_region == nullptr) {
    DINGO_LOG(FATAL) << fmt::format(
        "[merge.merging][job_id({}).region({}/{})] Apply CommitMerge, source region is nullptr.", request.job_id(),
        request.source_region_id(), target_region->Id());
    return 0;
  }
  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Apply target region CommitMerge");

  // Catch up apply source region raft log.
  auto node = raft_store_engine->GetNode(source_region->Id());
  if (node == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[merge.merging][job_id({}).region({}/{})] Not found source node.",
                                    request.job_id(), request.source_region_id(), target_region->Id());
    return 0;
  }
  auto state_machine = std::dynamic_pointer_cast<StoreStateMachine>(node->GetStateMachine());
  if (state_machine == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[merge.merging][job_id({}).region({}/{})] Not found source state machine.",
                                    request.job_id(), request.source_region_id(), target_region->Id());
    return 0;
  }

  int32_t actual_apply_log_count = 0;
  if (!request.entries().empty()) {
    actual_apply_log_count = state_machine->CatchUpApplyLog(Helper::PbRepeatedToVector(request.entries()));
  }

  FAIL_POINT("before_commit_merge_modify_epoch");

  store_region_meta->UpdateState(source_region, pb::common::StoreRegionState::MERGING);
  store_region_meta->UpdateState(target_region, pb::common::StoreRegionState::MERGING);

  {
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

    store_region_meta->UpdateEpochVersionAndRange(target_region, new_version, new_range, "merge target");
  }

  {
    // Set source region
    // range: [0xFFFFFFF, source_region.end_key)
    // epoch: source_epoch + 1
    int64_t new_version = source_region->Epoch().version() + 1;
    pb::common::Range new_range = source_region->Range();
    new_range.set_start_key(Helper::GenMaxStartKey());
    store_region_meta->UpdateEpochVersionAndRange(source_region, new_version, new_range, "merge source");
  }

  // Set source region TOMBSTONE state
  store_region_meta->UpdateState(source_region, pb::common::StoreRegionState::TOMBSTONE);
  store_region_meta->UpdateState(target_region, pb::common::StoreRegionState::NORMAL);

  // Do snapshot
  LaunchAyncSaveSnapshot(target_region);

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Save target region snapshot finish");
  ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Save snapshot finish");

  if (target_region->Type() == pb::common::INDEX_REGION) {
    // Set child share vector index
    auto vector_index = source_region->VectorIndexWrapper()->GetOwnVectorIndex();
    if (vector_index != nullptr) {
      target_region->VectorIndexWrapper()->SetSiblingVectorIndex(vector_index);
    } else {
      DINGO_LOG(WARNING) << fmt::format(
          "[merge.merging][job_id({}).region({}/{})] merge region get vector index failed.", request.job_id(),
          source_region->Id(), target_region->Id());
    }

    ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Launch target region rebuild vector index");
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Launch rebuild vector index");
    // Rebuild vector index
    if (Server::GetInstance().IsLeader(target_region->Id())) {
      VectorIndexManager::LaunchRebuildVectorIndex(target_region->VectorIndexWrapper(), request.job_id(), "merge");
    } else {
      DINGO_LOG(WARNING) << fmt::format(
          "[merge.merging][job_id({}).region({}/{})] target follower not need rebuild vector index.", request.job_id(),
          source_region->Id(), target_region->Id());

      auto vector_index_wrapper = target_region->VectorIndexWrapper();
      vector_index_wrapper->SetIsTempHoldVectorIndex(false);
      if (!VectorIndexWrapper::IsPermanentHoldVectorIndex(vector_index_wrapper->Id())) {
        vector_index_wrapper->ClearVectorIndex("merge");
      }

      store_region_meta->UpdateTemporaryDisableChange(target_region, false);
      ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(),
                                         fmt::format("Clear follower vector index {}", target_region->Id()));
    }
  } else {
    store_region_meta->UpdateTemporaryDisableChange(target_region, false);
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Apply target region CommitMerge finish");
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(request.job_id(), "Apply CommitMerge finish");
  }

  // Notify coordinator
  Heartbeat::TriggerStoreHeartbeat({request.source_region_id(), target_region->Id()}, true);

  DINGO_LOG(INFO) << fmt::format(
      "[merge.merging][job_id({}).region({}/{})] Applied CommitMerge, source_region({}/{}/{}/{}) "
      "target_region({}/{}) elapsed_time({}).",
      request.job_id(), request.source_region_id(), target_region->Id(),
      Helper::RegionEpochToString(request.source_region_epoch()), Helper::RangeToString(request.source_region_range()),
      request.entries().size(), actual_apply_log_count, target_region->EpochToString(), target_region->RangeToString(),
      Helper::TimestampMs() - start_time);

  // Update region metrics min/max key policy
  // Update region_size in next collect region metrics
  if (region_metrics != nullptr) {
    region_metrics->ResetMetricsForRegionVersionUpdate();
  }

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
  std::shared_ptr<Context> ctx = std::make_shared<Context>();
  auto status = engine->AyncSaveSnapshot(ctx, region->Id(), true);
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

  // Put vector data to rocksdb
  if (!kv_puts_with_cf.empty()) {
    auto writer = engine->Writer();
    status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
    if (status.error_code() == pb::error::Errno::EINTERNAL) {
      DINGO_LOG(FATAL) << fmt::format("[raft.apply][region({})] KvBatchPutAndDelete failed, error: {}", region->Id(),
                                      status.error_str());
      return 0;
    }
  }

  if (ctx) {
    if (ctx->Response()) {
      bool key_state = status.ok();
      auto *response = dynamic_cast<pb::index::VectorAddResponse *>(ctx->Response());
      for (int i = 0; i < request.vectors_size(); i++) {
        response->add_key_states(key_state);
      }
    }

    ctx->SetStatus(status);
  }

  // Handle vector index
  auto vector_index_wrapper = region->VectorIndexWrapper();
  int64_t vector_index_id = vector_index_wrapper->Id();
  bool is_ready = vector_index_wrapper->IsReady();
  if (is_ready) {
    // Check if the log_id is greater than the ApplyLogIndex of the vector index
    if (log_id > vector_index_wrapper->ApplyLogId()) {
      try {
        // Build vector_with_ids
        std::vector<pb::common::VectorWithId> vector_with_ids;
        vector_with_ids.reserve(request.vectors_size());

        for (const auto &vector : request.vectors()) {
          pb::common::VectorWithId vector_with_id;
          *(vector_with_id.mutable_vector()) = vector.vector();
          vector_with_id.set_id(vector.id());
          vector_with_ids.push_back(vector_with_id);
        }

        auto start_time = Helper::TimestampNs();
        auto status = vector_index_wrapper->Upsert(vector_with_ids);
        DINGO_LOG(DEBUG) << fmt::format("[raft.apply][region({})] upsert vector, count: {} cost: {}us", vector_index_id,
                                        vector_with_ids.size(), Helper::TimestampNs() - start_time);
        if (status.ok()) {
          vector_index_wrapper->SetApplyLogId(log_id);
        } else {
          DINGO_LOG(WARNING) << fmt::format("[raft.apply][region({})] upsert vector failed, count: {} err: {}",
                                            vector_index_id, vector_with_ids.size(), Helper::PrintStatus(status));
        }
      } catch (const std::exception &e) {
        DINGO_LOG(FATAL) << fmt::format("[raft.apply][region({})] upsert vector exception, error: {}", vector_index_id,
                                        e.what());
      }
    }
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
    DINGO_LOG(FATAL) << fmt::format("[raft.apply][region({})][cf_name({})] GetSnapshot failed.", region->Id(),
                                    request.cf_name());
  }

  if (request.ids_size() == 0) {
    DINGO_LOG(WARNING) << fmt::format("[raft.apply][region({})] delete vector id is empty.", region->Id());
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

      DINGO_LOG(DEBUG) << fmt::format("[raft.apply][region({})] delete vector id {}", region->Id(), request.ids(i));
    }
  }

  if (!kv_deletes_default.empty()) {
    kv_deletes_with_cf.insert_or_assign(Constant::kStoreDataCF, kv_deletes_default);
    kv_deletes_with_cf.insert_or_assign(Constant::kVectorScalarCF, kv_deletes_default);
    kv_deletes_with_cf.insert_or_assign(Constant::kVectorTableCF, kv_deletes_default);
  }

  // Delete vector and write wal
  if (!kv_deletes_with_cf.empty()) {
    auto writer = engine->Writer();
    status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
    if (status.error_code() == pb::error::Errno::EINTERNAL) {
      DINGO_LOG(FATAL) << fmt::format("[raft.apply][region({})] KvBatchPutAndDelete failed, error: {}", region->Id(),
                                      status.error_str());
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
        response->mutable_key_states()->Resize(key_states.size(), false);
      }
    }

    ctx->SetStatus(status);
  }

  auto vector_index_wrapper = region->VectorIndexWrapper();
  int64_t vector_index_id = vector_index_wrapper->Id();
  bool is_ready = vector_index_wrapper->IsReady();
  if (is_ready && !delete_ids.empty()) {
    if (log_id > vector_index_wrapper->ApplyLogId()) {
      try {
        auto status = vector_index_wrapper->Delete(delete_ids);
        if (status.ok()) {
          vector_index_wrapper->SetApplyLogId(log_id);
        } else {
          DINGO_LOG(WARNING) << fmt::format("[raft.apply][region({})] delete vector failed, count: {}, error: {}",
                                            vector_index_id, delete_ids.size(), Helper::PrintStatus(status));
        }
      } catch (const std::exception &e) {
        DINGO_LOG(FATAL) << fmt::format("[raft.apply][region({})] delete vector exception, error: {}", vector_index_id,
                                        e.what());
      }
    }
  }

  return 0;
}

int RebuildVectorIndexHandler::Handle(std::shared_ptr<Context>, store::RegionPtr region, std::shared_ptr<RawEngine>,
                                      [[maybe_unused]] const pb::raft::Request &req, store::RegionMetricsPtr, int64_t,
                                      int64_t log_id) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.rebuild][index_id({})] Handle rebuild vector index, apply_log_id: {}",
                                 region->Id(), log_id);
  const auto &request = req.rebuild_vector_index();
  auto vector_index_wrapper = region->VectorIndexWrapper();
  if (vector_index_wrapper != nullptr) {
    vector_index_wrapper->SaveApplyLogId(log_id);

    VectorIndexManager::LaunchRebuildVectorIndex(vector_index_wrapper, request.cmd_id(), "from raft");
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

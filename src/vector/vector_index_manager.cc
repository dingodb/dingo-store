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

#include "vector/vector_index_manager.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "butil/status.h"
#include "bvar/latency_recorder.h"
#include "bvar/reducer.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "mvcc/reader.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/file_service.pb.h"
#include "proto/node.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_index.h"
#include "vector/vector_index_diskann.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_index_snapshot.h"
#include "vector/vector_index_snapshot_manager.h"

namespace dingodb {

DEFINE_int64(vector_catchup_log_min_gap, 8, "vector catch up log min gap");
BRPC_VALIDATE_GFLAG(vector_catchup_log_min_gap, brpc::PositiveInteger);

DEFINE_int32(vector_background_worker_num, 16, "vector index background worker num");
DEFINE_int32(vector_fast_background_worker_num, 8, "vector index fast background worker num");
DEFINE_int64(vector_fast_build_log_gap, 50, "vector index fast build log gap");
DEFINE_int64(vector_pull_snapshot_min_log_gap, 66, "vector index pull snapshot min log gap");
DEFINE_int64(vector_max_background_task_count, 32, "vector index max background task count");

std::string RebuildVectorIndexTask::Trace() {
  return fmt::format("[vector_index.rebuild][id({}).start_time({}).job_id({})] {}", vector_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void RebuildVectorIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.rebuild][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) wait_time({}).",
      vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->RebuildingNum(),
      vector_index_wrapper_->PendingTaskNum(), VectorIndexManager::GetVectorIndexRebuildTaskRunningNum(),
      VectorIndexManager::GetVectorIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);

  int64_t start_time = Helper::TimestampMs();
  VectorIndexManager::IncVectorIndexTaskRunningNum();
  VectorIndexManager::IncVectorIndexRebuildTaskRunningNum();
  ON_SCOPE_EXIT([&]() {
    VectorIndexManager::DecVectorIndexTaskRunningNum();
    VectorIndexManager::DecVectorIndexRebuildTaskRunningNum();
    vector_index_wrapper_->DecPendingTaskNum();
    vector_index_wrapper_->DecRebuildingNum();

    LOG(INFO) << fmt::format(
        "[vector_index.rebuild][index_id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}).",
        vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->RebuildingNum(),
        vector_index_wrapper_->PendingTaskNum(), VectorIndexManager::GetVectorIndexRebuildTaskRunningNum(),
        VectorIndexManager::GetVectorIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);
  });

  auto region = Server::GetInstance().GetRegion(vector_index_wrapper_->Id());
  if (region == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.rebuild][index_id({})][trace({})] Not found region.",
                                      vector_index_wrapper_->Id(), trace_);
    return;
  }
  auto state = region->State();
  if (state == pb::common::StoreRegionState::STANDBY || state == pb::common::StoreRegionState::DELETING ||
      state == pb::common::StoreRegionState::DELETED || state == pb::common::StoreRegionState::ORPHAN ||
      state == pb::common::StoreRegionState::TOMBSTONE) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.rebuild][index_id({})][trace({})] region state({}) not match.",
                                      vector_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state));
    return;
  }

  if (Helper::InvalidRange(region->Range(false))) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.rebuild][index_id({})][trace({})] region range invalid.",
                                      vector_index_wrapper_->Id(), trace_);
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuild vector index {}", region->Id()));

  if (vector_index_wrapper_->IsStop()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.rebuild][index_id({})][trace({})] vector index is stop, gave up rebuild.",
        vector_index_wrapper_->Id(), trace_);
    return;
  }

  if (is_double_check_) {
    if (!vector_index_wrapper_->IsOwnReady()) {
      DINGO_LOG(INFO) << fmt::format(
          "[vector_index.rebuild][index_id({})][trace({})] vector index is not ready, gave up rebuild.",
          vector_index_wrapper_->Id(), trace_);
      return;
    }
    if (!vector_index_wrapper_->NeedToRebuild()) {
      DINGO_LOG(INFO) << fmt::format(
          "[vector_index.rebuild][index_id({})][trace({})] vector index not need rebuild, gave up rebuild.",
          vector_index_wrapper_->Id(), trace_);
      return;
    }
  }

  if (!is_force_) {
    // Compare vector index snapshot epoch and region epoch.
    auto snapshot_set = vector_index_wrapper_->SnapshotSet();
    if (snapshot_set != nullptr) {
      auto last_snapshot = snapshot_set->GetLastSnapshot();
      if (last_snapshot != nullptr && region->Epoch().version() <= last_snapshot->Epoch().version()) {
        DINGO_LOG(INFO) << fmt::format(
            "[vector_index.rebuild][index_id({})][trace({})] vector index snapshot epoch({}/{}) is latest, gave up "
            "rebuild.",
            vector_index_wrapper_->Id(), trace_, Helper::RegionEpochToString(region->Epoch()),
            Helper::RegionEpochToString(last_snapshot->Epoch()));
        return;
      }
    }
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuilding vector index {}", region->Id()));

  vector_index_wrapper_->SetIsTempHoldVectorIndex(true);
  auto status = VectorIndexManager::RebuildVectorIndex(vector_index_wrapper_, fmt::format("REBUILD-{}", trace_));
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuilded vector index {}", region->Id()));
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.rebuild][index_id({}_v{})][trace({})] rebuild vector index failed, error: {}.",
        vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), trace_, Helper::PrintStatus(status));
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Saving vector index {}", region->Id()));

  if (region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    // raft store engine use snapshot
    status = VectorIndexManager::SaveVectorIndex(vector_index_wrapper_, trace_);
    if (!status.ok()) {
      ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Saved vector index {} failed", region->Id()));
      DINGO_LOG(ERROR) << fmt::format(
          "[vector_index.save][index_id({}_v{})][trace({})] save vector index failed, error: {}.",
          vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), trace_, Helper::PrintStatus(status));
    }
  }

  vector_index_wrapper_->SetIsTempHoldVectorIndex(false);
  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Saved vector index {}", region->Id()));

  if (is_clear_) {
    if (!VectorIndexWrapper::IsPermanentHoldVectorIndex(vector_index_wrapper_->Id())) {
      vector_index_wrapper_->ClearVectorIndex(trace_);
    }

    auto store_region_meta = GET_STORE_REGION_META;
    store_region_meta->UpdateTemporaryDisableChange(region, false);
  }
}

std::string SaveVectorIndexTask::Trace() {
  return fmt::format("[vector_index.save][id({}).start_time({})] {}", vector_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), trace_);
}

void SaveVectorIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.save][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) wait_time({}).",
      vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->SavingNum(), vector_index_wrapper_->PendingTaskNum(),
      VectorIndexManager::GetVectorIndexSaveTaskRunningNum(), VectorIndexManager::GetVectorIndexTaskRunningNum(),
      Helper::TimestampMs() - start_time_);

  int64_t start_time = Helper::TimestampMs();
  VectorIndexManager::IncVectorIndexTaskRunningNum();
  VectorIndexManager::IncVectorIndexSaveTaskRunningNum();
  ON_SCOPE_EXIT([&]() {
    VectorIndexManager::DecVectorIndexTaskRunningNum();
    VectorIndexManager::DecVectorIndexSaveTaskRunningNum();
    vector_index_wrapper_->DecPendingTaskNum();
    vector_index_wrapper_->DecSavingNum();

    LOG(INFO) << fmt::format(
        "[vector_index.save][index_id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}).",
        vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->SavingNum(),
        vector_index_wrapper_->PendingTaskNum(), VectorIndexManager::GetVectorIndexSaveTaskRunningNum(),
        VectorIndexManager::GetVectorIndexTaskRunningNum(), Helper::TimestampMs() - start_time);
  });

  if (vector_index_wrapper_->IsStop()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.save][index_id({})][trace({})] vector index is stop, gave up save vector index.",
        vector_index_wrapper_->Id(), trace_);
    return;
  }
  if (!vector_index_wrapper_->IsOwnReady()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.save][index_id({})][trace({})] vector index is not ready, gave up save vector index.",
        vector_index_wrapper_->Id(), trace_);
    return;
  }

  auto status = VectorIndexManager::SaveVectorIndex(vector_index_wrapper_, trace_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.save][index_id({}_v{})][trace({})] save vector index failed, error {}",
        vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), trace_, status.error_str());
    return;
  }
}

std::string LoadOrBuildVectorIndexTask::Trace() {
  return fmt::format("[vector_index.loadorbuild][id({}).start_time({}).job_id({})] {}", vector_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void LoadOrBuildVectorIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.loadorbuild][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) "
      "wait_time({}).",
      vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->LoadorbuildingNum(),
      vector_index_wrapper_->PendingTaskNum(), VectorIndexManager::GetVectorIndexLoadorbuildTaskRunningNum(),
      VectorIndexManager::GetVectorIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);

  int64_t start_time = Helper::TimestampMs();
  VectorIndexManager::IncVectorIndexTaskRunningNum();
  VectorIndexManager::IncVectorIndexLoadorbuildTaskRunningNum();
  ON_SCOPE_EXIT([&]() {
    VectorIndexManager::DecVectorIndexTaskRunningNum();
    VectorIndexManager::DecVectorIndexLoadorbuildTaskRunningNum();
    vector_index_wrapper_->DecPendingTaskNum();
    vector_index_wrapper_->DecLoadoruildingNum();

    LOG(INFO) << fmt::format(
        "[vector_index.loadorbuild][index_id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}).",
        vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->LoadorbuildingNum(),
        vector_index_wrapper_->PendingTaskNum(), VectorIndexManager::GetVectorIndexLoadorbuildTaskRunningNum(),
        VectorIndexManager::GetVectorIndexTaskRunningNum(), Helper::TimestampMs() - start_time);
  });

  // Get region meta
  auto region = Server::GetInstance().GetRegion(vector_index_wrapper_->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.loadorbuild][region({})][trace({})] not found region.",
                                    vector_index_wrapper_->Id(), trace_);
    return;
  }
  auto state = region->State();
  if (state == pb::common::StoreRegionState::DELETING || state == pb::common::StoreRegionState::DELETED ||
      state == pb::common::StoreRegionState::ORPHAN || state == pb::common::StoreRegionState::TOMBSTONE) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.loadorbuild][index_id({})][trace({})] region state({}) not match.",
                                      vector_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state));
    return;
  }

  if (is_temp_hold_vector_index_) {
    vector_index_wrapper_->SetIsTempHoldVectorIndex(true);
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuild vector index {}", region->Id()));

  if (vector_index_wrapper_->IsStop()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadorbuild][index_id({})][trace({})] vector index is stop, gave up loadorbuild vector index.",
        vector_index_wrapper_->Id(), trace_);
    return;
  }

  if (vector_index_wrapper_->IsOwnReady() &&
      vector_index_wrapper_->LastBuildEpochVersion() >= region->Epoch().version()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Already own vector index {}", region->Id()));
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadorbuild][index_id({})][trace({})] vector index is ready, gave up loadorbuild vector index.",
        vector_index_wrapper_->Id(), trace_);
    return;
  }

  // Pull snapshot from peers.
  // New region don't pull snapshot, directly build.
  auto raft_meta = Server::GetInstance().GetRaftMeta(vector_index_wrapper_->Id());
  int64_t applied_index = -1;
  if (raft_meta != nullptr) {
    applied_index = raft_meta->AppliedId();
  }

  if (region->Epoch().version() > 1 || applied_index > FLAGS_vector_pull_snapshot_min_log_gap) {
    auto snapshot_set = vector_index_wrapper_->SnapshotSet();
    auto status = VectorIndexSnapshotManager::PullLastSnapshotFromPeers(snapshot_set, region->Epoch());
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadorbuild][region({})][trace({})] pull vector index last snapshot done, error: {}",
        vector_index_wrapper_->Id(), trace_, Helper::PrintStatus(status));
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuilding vector index {}", region->Id()));

  auto status = VectorIndexManager::LoadOrBuildVectorIndex(vector_index_wrapper_, region->Epoch(), trace_);
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuilded vector index {} failed", region->Id()));
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.load][index_id({}_v{})][trace({})] load or build vector index failed, error {}",
        vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), trace_, status.error_str());
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuilded vector index {}", region->Id()));
}

std::string LoadAsyncBuildVectorIndexTask::Trace() {
  return fmt::format("[vector_index.loadasyncbuild][id({}).start_time({}).job_id({})] {}", vector_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void LoadAsyncBuildVectorIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.loadasyncbuild][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) "
      "wait_time({}) is_fast_load ({}).",
      vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->LoadorbuildingNum(),
      vector_index_wrapper_->PendingTaskNum(), VectorIndexManager::GetVectorIndexLoadorbuildTaskRunningNum(),
      VectorIndexManager::GetVectorIndexTaskRunningNum(), Helper::TimestampMs() - start_time_, is_fast_load_);

  int64_t start_time = Helper::TimestampMs();
  VectorIndexManager::IncVectorIndexTaskRunningNum();
  if (is_fast_load_) {
    VectorIndexManager::IncVectorIndexFastLoadTaskRunningNum();
  } else {
    VectorIndexManager::IncVectorIndexSlowLoadTaskRunningNum();
  }
  ON_SCOPE_EXIT([&]() {
    VectorIndexManager::DecVectorIndexTaskRunningNum();
    if (is_fast_load_) {
      VectorIndexManager::DecVectorIndexFastLoadTaskRunningNum();
    } else {
      VectorIndexManager::DecVectorIndexSlowLoadTaskRunningNum();
    }
    vector_index_wrapper_->DecPendingTaskNum();
    vector_index_wrapper_->DecLoadoruildingNum();

    LOG(INFO) << fmt::format(
        "[vector_index.loadasyncbuild][index_id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}) is_fast_load ({}).",
        vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->LoadorbuildingNum(),
        vector_index_wrapper_->PendingTaskNum(), VectorIndexManager::GetVectorIndexFastLoadTaskRunningNum(),
        VectorIndexManager::GetVectorIndexTaskRunningNum(), Helper::TimestampMs() - start_time, is_fast_load_);
  });

  // Get region meta
  auto region = Server::GetInstance().GetRegion(vector_index_wrapper_->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.loadasyncbuild][region({})][trace({})] not found region.",
                                    vector_index_wrapper_->Id(), trace_);
    return;
  }
  auto state = region->State();
  if (state == pb::common::StoreRegionState::DELETING || state == pb::common::StoreRegionState::DELETED ||
      state == pb::common::StoreRegionState::ORPHAN || state == pb::common::StoreRegionState::TOMBSTONE) {
    DINGO_LOG(WARNING) << fmt::format(
        "[vector_index.loadasyncbuild][index_id({})][trace({})] region state({}) not match. is_fast_load ({})",
        vector_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state), is_fast_load_);
    return;
  }

  if (is_temp_hold_vector_index_) {
    vector_index_wrapper_->SetIsTempHoldVectorIndex(true);
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncbuild vector index {}", region->Id()));

  if (vector_index_wrapper_->IsStop()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadasyncbuild][index_id({})][trace({})] vector index is stop, gave up loadasyncbuild vector "
        "index.",
        vector_index_wrapper_->Id(), trace_);
    return;
  }

  if (vector_index_wrapper_->IsOwnReady() &&
      vector_index_wrapper_->LastBuildEpochVersion() >= region->Epoch().version()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Already own vector index {}", region->Id()));
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadasyncbuild][index_id({})][trace({})] vector index is ready, gave up loadasyncbuild vector "
        "index.",
        vector_index_wrapper_->Id(), trace_);
    return;
  }

  // Pull snapshot from peers.
  // New region don't pull snapshot, directly build.
  auto raft_meta = Server::GetInstance().GetRaftMeta(vector_index_wrapper_->Id());
  int64_t applied_index = (raft_meta != nullptr) ? raft_meta->AppliedId() : -1;

  if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
    // start to do async build vector index.
    bool is_fast_build =
        is_fast_load_ && (region->Epoch().version() == 1) && (applied_index <= FLAGS_vector_fast_build_log_gap);

    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadasyncbuild][index_id({}_v{})][trace({})] rocks engine direct do async "
        "build,  is_fast_build {} region_version {}",
        vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), trace_, is_fast_build,
        region->Epoch().version());

    VectorIndexManager::LaunchBuildVectorIndex(vector_index_wrapper_, is_temp_hold_vector_index_, is_fast_build,
                                               job_id_, "load async build");
    return;
  }

  if (region->Epoch().version() > 1 || applied_index > FLAGS_vector_pull_snapshot_min_log_gap) {
    auto snapshot_set = vector_index_wrapper_->SnapshotSet();
    auto status = VectorIndexSnapshotManager::PullLastSnapshotFromPeers(snapshot_set, region->Epoch());
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadasyncbuild][region({})][trace({})] pull vector index last snapshot done, error: {} version: "
        "{} applied_index {}",
        vector_index_wrapper_->Id(), trace_, Helper::PrintStatus(status), region->Epoch().version(), applied_index);
  } else {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadasyncbuild][region({})][trace({})] skip pull vector index last snapshot for new create "
        "region, version: {} applied_index {}",
        vector_index_wrapper_->Id(), trace_, region->Epoch().version(), applied_index);
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncbuilding vector index {}", region->Id()));

  auto status = VectorIndexManager::LoadVectorIndexOnly(vector_index_wrapper_, region->Epoch(), trace_);
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncbuilded vector index {} failed", region->Id()));

    // start to do async build vector index.
    bool is_fast_build =
        is_fast_load_ && (region->Epoch().version() == 1) && (applied_index <= FLAGS_vector_fast_build_log_gap);

    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadasyncbuild][index_id({}_v{})][trace({})] load vector index failed, will try to do async "
        "build, error {} applied_index {} is_fast_build {} region_version {}",
        vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), trace_, status.error_str(), applied_index,
        is_fast_build, region->Epoch().version());

    VectorIndexManager::LaunchBuildVectorIndex(vector_index_wrapper_, is_temp_hold_vector_index_, is_fast_build,
                                               job_id_, "load async build");

    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncrbuilded vector index {}", region->Id()));
}

std::string BuildVectorIndexTask::Trace() {
  return fmt::format("[vector_index.build][id({}).start_time({}).job_id({})] {}", vector_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void BuildVectorIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.build][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) "
      "wait_time({}).",
      vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->LoadorbuildingNum(),
      vector_index_wrapper_->PendingTaskNum(), VectorIndexManager::GetVectorIndexLoadorbuildTaskRunningNum(),
      VectorIndexManager::GetVectorIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);

  int64_t start_time = Helper::TimestampMs();
  VectorIndexManager::IncVectorIndexTaskRunningNum();
  if (is_fast_build_) {
    VectorIndexManager::IncVectorIndexFastBuildTaskRunningNum();
  } else {
    VectorIndexManager::IncVectorIndexSlowBuildTaskRunningNum();
  }
  ON_SCOPE_EXIT([&]() {
    VectorIndexManager::DecVectorIndexTaskRunningNum();
    if (is_fast_build_) {
      VectorIndexManager::DecVectorIndexFastBuildTaskRunningNum();
    } else {
      VectorIndexManager::DecVectorIndexSlowBuildTaskRunningNum();
    }
    vector_index_wrapper_->DecPendingTaskNum();
    vector_index_wrapper_->DecRebuildingNum();

    LOG(INFO) << fmt::format(
        "[vector_index.build][index_id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}).",
        vector_index_wrapper_->Id(), trace_, vector_index_wrapper_->RebuildingNum(),
        vector_index_wrapper_->PendingTaskNum(), VectorIndexManager::GetVectorIndexRebuildTaskRunningNum(),
        VectorIndexManager::GetVectorIndexTaskRunningNum(), Helper::TimestampMs() - start_time);
  });

  // Get region meta
  auto region = Server::GetInstance().GetRegion(vector_index_wrapper_->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.build][region({})][trace({})] not found region.",
                                    vector_index_wrapper_->Id(), trace_);
    return;
  }
  auto state = region->State();
  if (state == pb::common::StoreRegionState::DELETING || state == pb::common::StoreRegionState::DELETED ||
      state == pb::common::StoreRegionState::ORPHAN || state == pb::common::StoreRegionState::TOMBSTONE) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.build][index_id({})][trace({})] region state({}) not match.",
                                      vector_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state));
    return;
  }

  if (is_temp_hold_vector_index_) {
    vector_index_wrapper_->SetIsTempHoldVectorIndex(true);
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("build vector index {}", region->Id()));

  if (vector_index_wrapper_->IsStop()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.build][index_id({})][trace({})] vector index is stop, gave up build vector "
        "index.",
        vector_index_wrapper_->Id(), trace_);
    return;
  }

  if (vector_index_wrapper_->IsOwnReady() &&
      vector_index_wrapper_->LastBuildEpochVersion() >= region->Epoch().version()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Already own vector index {}", region->Id()));
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.build][index_id({})][trace({})] vector index is ready, gave up build vector "
        "index.",
        vector_index_wrapper_->Id(), trace_);
    return;
  }

  auto status = VectorIndexManager::BuildVectorIndexOnly(vector_index_wrapper_, region->Epoch(), trace_);
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("builded vector index {} failed", region->Id()));
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.load][index_id({}_v{})][trace({})] load async build vector index failed, error {}",
        vector_index_wrapper_->Id(), vector_index_wrapper_->Version(), trace_, status.error_str());
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncrbuilded vector index {}", region->Id()));
}

bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_task_running_num("dingo_vector_index_task_running_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_rebuild_task_running_num(
    "dingo_vector_index_rebuild_task_running_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_save_task_running_num(
    "dingo_vector_index_save_task_running_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_loadorbuild_task_running_num(
    "dingo_vector_index_loadorbuild_task_running_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_fast_load_task_running_num(
    "dingo_vector_index_fast_load_task_running_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_slow_load_task_running_num(
    "dingo_vector_index_slow_load_task_running_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_fast_build_task_running_num(
    "dingo_vector_index_fast_build_task_running_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_slow_build_task_running_num(
    "dingo_vector_index_slow_build_task_running_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_load_catchup_running_num(
    "dingo_vector_index_load_catchup_task_running_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_rebuild_catchup_running_num(
    "dingo_vector_index_rebuild_catchup_task_running_num");

bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_task_total_num("dingo_vector_index_task_total_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_rebuild_task_total_num(
    "dingo_vector_index_rebuild_task_total_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_save_task_total_num(
    "dingo_vector_index_save_task_total_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_loadorbuild_task_total_num(
    "dingo_vector_index_loadorbuild_task_total_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_fast_load_task_total_num(
    "dingo_vector_index_fast_load_task_total_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_slow_load_task_total_num(
    "dingo_vector_index_slow_load_task_total_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_fast_build_task_total_num(
    "dingo_vector_index_fast_build_task_total_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_slow_build_task_total_num(
    "dingo_vector_index_slow_build_task_total_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_load_catchup_total_num(
    "dingo_vector_index_load_catchup_task_total_num");
bvar::Adder<uint64_t> VectorIndexManager::bvar_vector_index_rebuild_catchup_total_num(
    "dingo_vector_index_rebuild_catchup_task_total_num");

bvar::LatencyRecorder VectorIndexManager::bvar_vector_index_catchup_latency_first_rounds(
    "dingo_vector_index_catchup_latency_first_rounds");
bvar::LatencyRecorder VectorIndexManager::bvar_vector_index_catchup_latency_last_round(
    "dingo_vector_index_catchup_latency_last_round");

std::atomic<int> VectorIndexManager::vector_index_task_running_num = 0;
std::atomic<int> VectorIndexManager::vector_index_rebuild_task_running_num = 0;
std::atomic<int> VectorIndexManager::vector_index_save_task_running_num = 0;
std::atomic<int> VectorIndexManager::vector_index_loadorbuild_task_running_num = 0;
std::atomic<int> VectorIndexManager::vector_index_fast_load_task_running_num = 0;
std::atomic<int> VectorIndexManager::vector_index_slow_load_task_running_num = 0;
std::atomic<int> VectorIndexManager::vector_index_fast_build_task_running_num = 0;
std::atomic<int> VectorIndexManager::vector_index_slow_build_task_running_num = 0;

int VectorIndexManager::GetVectorIndexTaskRunningNum() { return vector_index_task_running_num.load(); }

void VectorIndexManager::IncVectorIndexTaskRunningNum() {
  vector_index_task_running_num.fetch_add(1);
  bvar_vector_index_task_running_num << 1;
  bvar_vector_index_task_total_num << 1;
}

void VectorIndexManager::DecVectorIndexTaskRunningNum() {
  vector_index_task_running_num.fetch_sub(1);
  bvar_vector_index_task_running_num << -1;
}

int VectorIndexManager::GetVectorIndexRebuildTaskRunningNum() { return vector_index_rebuild_task_running_num.load(); }

void VectorIndexManager::IncVectorIndexRebuildTaskRunningNum() {
  vector_index_rebuild_task_running_num.fetch_add(1);
  bvar_vector_index_rebuild_task_running_num << 1;
  bvar_vector_index_rebuild_task_total_num << 1;
}

void VectorIndexManager::DecVectorIndexRebuildTaskRunningNum() {
  vector_index_rebuild_task_running_num.fetch_sub(1);
  bvar_vector_index_rebuild_task_running_num << -1;
}

int VectorIndexManager::GetVectorIndexSaveTaskRunningNum() { return vector_index_save_task_running_num.load(); }

void VectorIndexManager::IncVectorIndexSaveTaskRunningNum() {
  vector_index_save_task_running_num.fetch_add(1);
  bvar_vector_index_save_task_running_num << 1;
  bvar_vector_index_save_task_total_num << 1;
}

void VectorIndexManager::DecVectorIndexSaveTaskRunningNum() {
  vector_index_save_task_running_num.fetch_sub(1);
  bvar_vector_index_save_task_running_num << -1;
}

int VectorIndexManager::GetVectorIndexLoadorbuildTaskRunningNum() {
  return vector_index_loadorbuild_task_running_num.load();
}

void VectorIndexManager::IncVectorIndexLoadorbuildTaskRunningNum() {
  vector_index_loadorbuild_task_running_num.fetch_add(1);
  bvar_vector_index_loadorbuild_task_running_num << 1;
  bvar_vector_index_loadorbuild_task_total_num << 1;
}

void VectorIndexManager::DecVectorIndexLoadorbuildTaskRunningNum() {
  vector_index_loadorbuild_task_running_num.fetch_sub(1);
  bvar_vector_index_loadorbuild_task_running_num << -1;
}

int VectorIndexManager::GetVectorIndexFastLoadTaskRunningNum() {
  return vector_index_fast_load_task_running_num.load();
}

void VectorIndexManager::IncVectorIndexFastLoadTaskRunningNum() {
  vector_index_fast_load_task_running_num.fetch_add(1);
  bvar_vector_index_fast_load_task_running_num << 1;
  bvar_vector_index_fast_load_task_total_num << 1;
}

void VectorIndexManager::DecVectorIndexFastLoadTaskRunningNum() {
  vector_index_fast_load_task_running_num.fetch_sub(1);
  bvar_vector_index_fast_load_task_running_num << -1;
}

int VectorIndexManager::GetVectorIndexSlowLoadTaskRunningNum() {
  return vector_index_slow_load_task_running_num.load();
}

void VectorIndexManager::IncVectorIndexSlowLoadTaskRunningNum() {
  vector_index_slow_load_task_running_num.fetch_add(1);
  bvar_vector_index_slow_load_task_running_num << 1;
  bvar_vector_index_slow_load_task_total_num << 1;
}

void VectorIndexManager::DecVectorIndexSlowLoadTaskRunningNum() {
  vector_index_slow_load_task_running_num.fetch_sub(1);
  bvar_vector_index_slow_load_task_running_num << -1;
}

int VectorIndexManager::GetVectorIndexFastBuildTaskRunningNum() {
  return vector_index_fast_build_task_running_num.load();
}

void VectorIndexManager::IncVectorIndexFastBuildTaskRunningNum() {
  vector_index_fast_build_task_running_num.fetch_add(1);
  bvar_vector_index_fast_build_task_running_num << 1;
  bvar_vector_index_fast_build_task_total_num << 1;
}

void VectorIndexManager::DecVectorIndexFastBuildTaskRunningNum() {
  vector_index_fast_build_task_running_num.fetch_sub(1);
  bvar_vector_index_fast_build_task_running_num << -1;
}

int VectorIndexManager::GetVectorIndexSlowBuildTaskRunningNum() {
  return vector_index_slow_build_task_running_num.load();
}

void VectorIndexManager::IncVectorIndexSlowBuildTaskRunningNum() {
  vector_index_slow_build_task_running_num.fetch_add(1);
  bvar_vector_index_slow_build_task_running_num << 1;
  bvar_vector_index_slow_build_task_total_num << 1;
}

void VectorIndexManager::DecVectorIndexSlowBuildTaskRunningNum() {
  vector_index_slow_build_task_running_num.fetch_sub(1);
  bvar_vector_index_slow_build_task_running_num << -1;
}

bool VectorIndexManager::Init() {
  background_workers_ = ExecqWorkerSet::New("vector_mgr_background", FLAGS_vector_background_worker_num, 0);
  if (!background_workers_->Init()) {
    DINGO_LOG(ERROR) << "Init vector index manager background worker set failed!";
    return false;
  }

  fast_background_workers_ =
      ExecqWorkerSet::New("vector_mgr_fast_background", FLAGS_vector_fast_background_worker_num, 0);
  if (!fast_background_workers_->Init()) {
    DINGO_LOG(ERROR) << "Init vector index manager fast background worker set failed!";
    return false;
  }

  VectorIndex::SetSimdHook();

  VectorIndexDiskANN::Init();

  return true;
}

void VectorIndexManager::Destroy() {
  if (background_workers_ != nullptr) {
    background_workers_->Destroy();
  }
  if (fast_background_workers_ != nullptr) {
    fast_background_workers_->Destroy();
  }
}

// Load vector index for already exist vector index at bootstrap.
// Priority load from snapshot, if snapshot not exist then load from original data.
butil::Status VectorIndexManager::LoadOrBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                         const pb::common::RegionEpoch& epoch,
                                                         const std::string& trace) {
  assert(vector_index_wrapper != nullptr);

  int64_t start_time = Helper::TimestampMs();
  int64_t vector_index_id = vector_index_wrapper->Id();

  // try to load vector index from snapshot
  auto status = LoadVectorIndex(vector_index_wrapper, epoch, fmt::format("LOAD.SNAPSHOT-{}", trace));
  if (status.ok()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadorbuild][index_id({})][trace({})] Load vector index from snapshot success, elapsed "
        "time({}ms)",
        vector_index_id, trace, Helper::TimestampMs() - start_time);
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.loadorbuild][index_id({})][trace({})] Load vector index from snapshot failed, error: {}.",
      vector_index_id, trace, Helper::PrintStatus(status));

  // Build a new vector index from original data
  status = RebuildVectorIndex(vector_index_wrapper, fmt::format("LOAD.REBUILD-{}", trace));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.loadorbuild][index_id({})][trace({})] Rebuild vector index failed.",
                                    vector_index_id, trace);
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.loadorbuild][index_id({})][trace({})] Rebuild vector index success, elapsed time({}ms).",
      vector_index_id, trace, Helper::TimestampMs() - start_time);

  auto region = Server::GetInstance().GetRegion(vector_index_wrapper->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.loadorbuild][region({})] Not found region.",
                                    vector_index_wrapper->Id());
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", vector_index_wrapper->Id()));
  }
  if (region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    // raft store engine use snapshot
    // Save vector index
    status = VectorIndexManager::SaveVectorIndex(vector_index_wrapper, trace);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format(
          "[vector_index.loadorbuild][index_id({})][trace({})] save vector index failed, error: {}.",
          vector_index_wrapper->Id(), trace, Helper::PrintStatus(status));
    }
  }

  return butil::Status();
}

// Load vector index for already exist vector index at bootstrap.
// Priority load from snapshot, if snapshot not exist then return error.
// This function is for LoadAsyncBuild.
butil::Status VectorIndexManager::LoadVectorIndexOnly(VectorIndexWrapperPtr vector_index_wrapper,
                                                      const pb::common::RegionEpoch& epoch, const std::string& trace) {
  assert(vector_index_wrapper != nullptr);

  int64_t start_time = Helper::TimestampMs();
  int64_t vector_index_id = vector_index_wrapper->Id();

  // try to load vector index from snapshot
  auto status = LoadVectorIndex(vector_index_wrapper, epoch, fmt::format("LOAD.SNAPSHOT-{}", trace));
  if (status.ok()) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.loadorbuild][index_id({})][trace({})] Load vector index from snapshot success, elapsed "
        "time({}ms)",
        vector_index_id, trace, Helper::TimestampMs() - start_time);
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.loadorbuild][index_id({})][trace({})] Load vector index from snapshot failed, will do async "
      "build, error: {}.",
      vector_index_id, trace, Helper::PrintStatus(status));

  return status;
}

// Build vector index for already exist vector index at bootstrap.
butil::Status VectorIndexManager::BuildVectorIndexOnly(VectorIndexWrapperPtr vector_index_wrapper,
                                                       const pb::common::RegionEpoch& /*epoch*/,
                                                       const std::string& trace) {
  assert(vector_index_wrapper != nullptr);

  int64_t start_time = Helper::TimestampMs();
  int64_t vector_index_id = vector_index_wrapper->Id();

  DINGO_LOG(INFO) << fmt::format("[vector_index.buildonly][index_id({})][trace({})] build_vector_index_only start.",
                                 vector_index_id, trace);

  // Build a new vector index from original data
  auto status = RebuildVectorIndex(vector_index_wrapper, fmt::format("LOAD.REBUILD-{}", trace));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.buildonly][index_id({})][trace({})] Rebuild vector index failed.",
                                    vector_index_id, trace);
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.buildonly][index_id({})][trace({})] Rebuild vector index success, elapsed time({}ms).",
      vector_index_id, trace, Helper::TimestampMs() - start_time);

  auto region = Server::GetInstance().GetRegion(vector_index_wrapper->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.buildonly][region({})] Not found region.",
                                    vector_index_wrapper->Id());
    return butil::Status(pb::error::EREGION_NOT_FOUND, fmt::format("Not found region {}", vector_index_wrapper->Id()));
  }

  if (region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    status = VectorIndexManager::SaveVectorIndex(vector_index_wrapper, trace);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format(
          "[vector_index.buildonly][index_id({})][trace({})] save vector index failed, error: {}.",
          vector_index_wrapper->Id(), trace, Helper::PrintStatus(status));
    }
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.buildonly][index_id({})][trace({})] build_vector_index_only done.",
                                 vector_index_id, trace);

  return butil::Status();
}

void VectorIndexManager::LaunchLoadOrBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                      bool is_temp_hold_vector_index, int64_t job_id,
                                                      const std::string& trace) {
  assert(vector_index_wrapper != nullptr);

  if (is_temp_hold_vector_index) {
    vector_index_wrapper->SetIsTempHoldVectorIndex(true);
  }

  if (vector_index_wrapper->LoadorbuildingNum() > 0) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.launch][index_id({})] Already exist loadorbuild on execute queue, job({}) trace({}).",
        vector_index_wrapper->Id(), job_id, trace);
    return;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.launch][index_id({})] Launch loadorbuild vector index, pending tasks({}) total running({}) "
      "job({}) trace({}).",
      vector_index_wrapper->Id(), vector_index_wrapper->PendingTaskNum(), GetVectorIndexTaskRunningNum(), job_id,
      trace);

  auto task = std::make_shared<LoadOrBuildVectorIndexTask>(vector_index_wrapper, is_temp_hold_vector_index, job_id,
                                                           fmt::format("{}-{}", job_id, trace));
  if (!Server::GetInstance().GetVectorIndexManager()->ExecuteTask(vector_index_wrapper->Id(), task)) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.launch][index_id({})] Launch loadorbuild vector index failed, job({}) trace({})",
        vector_index_wrapper->Id(), job_id, trace);
  } else {
    vector_index_wrapper->IncLoadoruildingNum();
    vector_index_wrapper->IncPendingTaskNum();
  }
}

void VectorIndexManager::LaunchLoadAsyncBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                         bool is_temp_hold_vector_index, bool is_fast_load,
                                                         int64_t job_id, const std::string& trace) {
  assert(vector_index_wrapper != nullptr);

  if (is_temp_hold_vector_index) {
    vector_index_wrapper->SetIsTempHoldVectorIndex(true);
  }

  if (vector_index_wrapper->LoadorbuildingNum() > 0) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.launch][index_id({})] Already exist loadasyncbuild on execute queue, job({}) trace({}).",
        vector_index_wrapper->Id(), job_id, trace);
    return;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.launch][index_id({})] Launch loadasyncbuild vector index, pending tasks({}) total running({}) "
      "job({}) trace({}) is_fast_load({}).",
      vector_index_wrapper->Id(), vector_index_wrapper->PendingTaskNum(), GetVectorIndexTaskRunningNum(), job_id, trace,
      is_fast_load);

  auto task = std::make_shared<LoadAsyncBuildVectorIndexTask>(
      vector_index_wrapper, is_temp_hold_vector_index, is_fast_load, job_id, fmt::format("{}-{}", job_id, trace));

  bool ret = false;
  if (is_fast_load) {
    ret = Server::GetInstance().GetVectorIndexManager()->ExecuteTaskFast(vector_index_wrapper->Id(), task);
  } else {
    ret = Server::GetInstance().GetVectorIndexManager()->ExecuteTask(vector_index_wrapper->Id(), task);
  }

  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.launch][index_id({})] Launch loadasyncbuild vector index failed, job({}) trace({}) is_fast_load "
        "({})",
        vector_index_wrapper->Id(), job_id, trace, is_fast_load);
  } else {
    vector_index_wrapper->IncLoadoruildingNum();
    vector_index_wrapper->IncPendingTaskNum();
  }
}

// Parallel load vector index at server bootstrap.
butil::Status VectorIndexManager::ParallelLoadOrBuildVectorIndex(std::vector<store::RegionPtr> regions, int concurrency,
                                                                 const std::string& trace) {
  struct Parameter {
    std::vector<store::RegionPtr> regions;
    std::atomic<int> offset;
    std::vector<int> results;
    std::string trace;
  };

  auto param = std::make_shared<Parameter>();
  param->regions = regions;
  param->offset = 0;
  param->results.resize(regions.size(), 0);
  param->trace = trace;

  auto task = [](void* arg) -> void* {
    if (arg == nullptr) {
      return nullptr;
    }
    auto* param = static_cast<Parameter*>(arg);

    for (;;) {
      int offset = param->offset.fetch_add(1, std::memory_order_relaxed);
      if (offset >= param->regions.size()) {
        break;
      }

      auto region = param->regions[offset];
      auto vector_index_wrapper = region->VectorIndexWrapper();

      int64_t vector_index_id = vector_index_wrapper->Id();
      LOG(INFO) << fmt::format("Init load region {} vector index", vector_index_id);

      auto status = VectorIndexManager::LoadOrBuildVectorIndex(vector_index_wrapper, region->Epoch(), param->trace);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("Load region {} vector index failed, ", vector_index_id);
        param->results[offset] = -1;
        break;
      }
    }

    return nullptr;
  };

  if (!Helper::ParallelRunTask(task, param.get(), concurrency)) {
    return butil::Status(pb::error::EINTERNAL, "Create bthread failed.");
  }

  for (auto result : param->results) {
    if (result == -1) {
      return butil::Status(pb::error::EINTERNAL, "Load or build vector index failed.");
    }
  }

  return butil::Status();
}

// Replay vector index from WAL
butil::Status VectorIndexManager::ReplayWalToVectorIndex(VectorIndexPtr vector_index, int64_t start_log_id,
                                                         int64_t end_log_id) {
  assert(vector_index != nullptr);

  if (start_log_id >= end_log_id) {
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.replaywal][index_id({})] replay wal log({}-{})", vector_index->Id(),
                                 start_log_id, end_log_id);

  int64_t start_time = Helper::TimestampMs();
  auto raft_kv_engine = Server::GetInstance().GetRaftStoreEngine();
  auto node = raft_kv_engine->GetNode(vector_index->Id());
  if (node == nullptr) {
    return butil::Status(pb::error::Errno::ERAFT_NOT_FOUND, fmt::format("Not found node {}", vector_index->Id()));
  }

  auto log_stroage = Server::GetInstance().GetLogStorageManager()->GetLogStorage(vector_index->Id());
  if (log_stroage == nullptr) {
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("Not found log stroage {}", vector_index->Id()));
  }

  int64_t min_vector_id = 0, max_vector_id = 0;
  VectorCodec::DecodeRangeToVectorId(false, vector_index->Range(), min_vector_id, max_vector_id);

  std::vector<pb::common::VectorWithId> vectors;
  vectors.reserve(Constant::kBuildVectorIndexBatchSize);
  std::vector<int64_t> ids;
  ids.reserve(Constant::kBuildVectorIndexBatchSize);

  int64_t last_log_id = vector_index->ApplyLogId();
  auto log_entrys = log_stroage->GetEntrys(start_log_id, end_log_id);
  for (const auto& log_entry : log_entrys) {
    auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
    butil::IOBufAsZeroCopyInputStream wrapper(log_entry->data);
    CHECK(raft_cmd->ParseFromZeroCopyStream(&wrapper));
    for (auto& request : *raft_cmd->mutable_requests()) {
      switch (request.cmd_type()) {
        case pb::raft::VECTOR_ADD: {
          if (!ids.empty()) {
            vector_index->DeleteByParallel(ids, false);
            ids.clear();
          }

          for (auto& vector : *request.mutable_vector_add()->mutable_vectors()) {
            if (vector.id() >= min_vector_id && vector.id() < max_vector_id) {
              vectors.push_back(vector);
            }
          }

          if (vectors.size() >= Constant::kBuildVectorIndexBatchSize) {
            vector_index->UpsertByParallel(vectors, false);
            vectors.clear();
          }
          break;
        }
        case pb::raft::VECTOR_DELETE: {
          if (!vectors.empty()) {
            vector_index->UpsertByParallel(vectors, false);
            vectors.clear();
          }

          for (auto vector_id : request.vector_delete().ids()) {
            if (vector_id >= min_vector_id && vector_id < max_vector_id) {
              ids.push_back(vector_id);
            }
          }
          if (ids.size() >= Constant::kBuildVectorIndexBatchSize) {
            vector_index->DeleteByParallel(ids, false);
            ids.clear();
          }
          break;
        }
        default:
          break;
      }
    }

    last_log_id = log_entry->index;
  }
  if (!vectors.empty()) {
    vector_index->UpsertByParallel(vectors, false);
  } else if (!ids.empty()) {
    vector_index->DeleteByParallel(ids, false);
  }

  if (last_log_id > vector_index->ApplyLogId()) {
    vector_index->SetApplyLogId(last_log_id);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.replaywal][index_id({})] replay wal finish, log({}-{}) last_log_id({}) vector_id({}-{}) elapsed "
      "time({}ms)",
      vector_index->Id(), start_log_id, end_log_id, last_log_id, min_vector_id, max_vector_id,
      Helper::TimestampMs() - start_time);

  return butil::Status();
}

// Build vector index with original all data.
VectorIndexPtr VectorIndexManager::BuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                    const std::string& trace) {
  assert(vector_index_wrapper != nullptr);
  int64_t vector_index_id = vector_index_wrapper->Id();
  auto region = Server::GetInstance().GetRegion(vector_index_id);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.build][index_id({})][trace({})] not found region.", vector_index_id,
                                    trace);
    return nullptr;
  }

  auto range = region->Range(false);
  auto vector_index =
      VectorIndexFactory::New(vector_index_id, vector_index_wrapper->IndexParameter(), region->Epoch(), range);
  if (!vector_index) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.build][index_id({})][trace({})] New vector index failed.",
                                      vector_index_id, trace);
    return nullptr;
  }

  // diskann index only need to build index, no need to build data !!!
  if (pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN == vector_index_wrapper->Type()) {
    return vector_index;
  }

  if (region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    // Get last applied log id
    auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
    if (raft_store_engine == nullptr) {
      DINGO_LOG(FATAL) << fmt::format("[vector_index.build][index_id({})] raft store engine is null.", vector_index_id);
    }

    auto raft_node = raft_store_engine->GetNode(vector_index_id);
    if (raft_node == nullptr) {
      DINGO_LOG(ERROR) << fmt::format(
          "[vector_index.build][index_id({})][trace({})] not found raft node, skip this build.", vector_index_id,
          trace);
      return nullptr;
    }

    auto raft_status = raft_node->GetStatus();
    if (raft_status->known_applied_index() > 0) {
      vector_index->SetApplyLogId(raft_status->known_applied_index());
    }
  }

  auto encode_range = mvcc::Codec::EncodeRange(vector_index->Range());

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.build][index_id({})][trace({})] Build vector index, range: {} parallel: {}", vector_index_id,
      trace, vector_index->RangeString(), vector_index->WriteOpParallelNum());

  int64_t start_time = Helper::TimestampMs();
  // load vector data to vector index
  auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
  auto reader = mvcc::VectorReader::New(raw_engine->Reader());

  // Note: This is iterated 2 times for the following reasons:
  // ivf_flat must train first before adding data
  // train requires full data. If you just traverse it once, it will consume a huge amount of memory.
  // This is done here to cancel the use of slower disk speed in exchange for memory usage.

  // build if need
  if (vector_index->NeedTrain() && !vector_index->IsTrained()) {
    auto status = TrainForBuild(vector_index, reader, encode_range);
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.build][index_id({})][trace({})] Train finish, elapsed_time: {}ms error: {} {}", vector_index_id,
        trace, Helper::TimestampMs() - start_time, status.error_code(), status.error_cstr());
    if (!status.ok()) {
      return {};
    }
    start_time = Helper::TimestampMs();
  } else {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.build][index_id({})][trace({})] Maybe not need train, is_need_train: {} is_trained: {}",
        vector_index_id, trace, vector_index->NeedTrain(), vector_index->IsTrained());
  }

  IteratorOptions options;
  options.upper_bound = encode_range.end_key();
  auto iter = reader->NewIterator(Constant::kVectorDataCF, 0, options);
  CHECK(iter != nullptr) << fmt::format("[vector_index.build][index_id({})] NewIterator failed.", vector_index_id);

  int64_t count = 0;
  int64_t upsert_use_time = 0;
  std::vector<pb::common::VectorWithId> vectors;
  vectors.reserve(Constant::kBuildVectorIndexBatchSize);
  for (iter->Seek(encode_range.start_key()); iter->Valid(); iter->Next()) {
    pb::common::VectorWithId vector;

    std::string key(iter->Key());
    vector.set_id(VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(key));

    std::string value(mvcc::Codec::UnPackageValue(iter->Value()));
    CHECK(vector.mutable_vector()->ParseFromString(value)) << "parse vector pb failed.";

    if (vector.vector().float_values_size() <= 0) {
      DINGO_LOG(WARNING) << fmt::format("[vector_index.build][index_id({})][trace({})] vector values_size error.",
                                        vector_index_id, trace);
      continue;
    }

    vectors.push_back(vector);
    if (++count % Constant::kBuildVectorIndexBatchSize == 0) {
      int64_t upsert_start_time = Helper::TimestampMs();

      vector_index->AddByParallel(vectors, false);

      int32_t this_upsert_time = Helper::TimestampMs() - upsert_start_time;
      upsert_use_time += this_upsert_time;

      DINGO_LOG(INFO) << fmt::format(
          "[vector_index.build][index_id({})][trace({})] Build vector index progress, speed({:.3}) count({}) elapsed "
          "time({}/{}ms)",
          vector_index_id, trace, static_cast<double>(this_upsert_time) / vectors.size(), count, upsert_use_time,
          Helper::TimestampMs() - start_time);

      vectors.clear();
      // yield, for other bthread run.
      bthread_yield();
    }
  }

  if (!vectors.empty()) {
    int64_t upsert_start_time = Helper::TimestampMs();
    vector_index->AddByParallel(vectors, false);
    upsert_use_time += (Helper::TimestampMs() - upsert_start_time);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.build][index_id({})][trace({})] Build vector index finish, parallel({}) count({}) epoch({}) "
      "range({}) elapsed time({}/{}ms)",
      vector_index_id, trace, vector_index->WriteOpParallelNum(), count,
      Helper::RegionEpochToString(vector_index->Epoch()), vector_index->RangeString(), upsert_use_time,
      Helper::TimestampMs() - start_time);

  return vector_index;
}

void VectorIndexManager::LaunchRebuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, int64_t job_id,
                                                  bool is_double_check, bool is_force, bool is_clear,
                                                  const std::string& trace) {
  assert(vector_index_wrapper != nullptr);

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.launch][index_id({})][trace({})] Launch rebuild vector index, rebuild({}) pending tasks({}) total "
      "running({}).",
      vector_index_wrapper->Id(), vector_index_wrapper->RebuildingNum(), vector_index_wrapper->PendingTaskNum(),
      GetVectorIndexTaskRunningNum(), trace);

  auto task = std::make_shared<RebuildVectorIndexTask>(vector_index_wrapper, job_id, is_double_check, is_force,
                                                       is_clear, fmt::format("{}-{}", job_id, trace));
  if (!Server::GetInstance().GetVectorIndexManager()->ExecuteTask(vector_index_wrapper->Id(), task)) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.launch][index_id({})][trace({})] Launch rebuild vector index failed",
                                    vector_index_wrapper->Id(), job_id);
  } else {
    vector_index_wrapper->IncRebuildingNum();
    vector_index_wrapper->IncPendingTaskNum();
  }
}

void VectorIndexManager::LaunchBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                bool is_temp_hold_vector_index, bool is_fast_build, int64_t job_id,
                                                const std::string& trace) {
  assert(vector_index_wrapper != nullptr);

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.launch][index_id({})][trace({})] Launch build vector index, rebuild({}), is_fast_build({}) "
      "pending tasks({}) total running({}).",
      vector_index_wrapper->Id(), trace, vector_index_wrapper->RebuildingNum(), is_fast_build,
      vector_index_wrapper->PendingTaskNum(), GetVectorIndexTaskRunningNum());

  auto task = std::make_shared<BuildVectorIndexTask>(vector_index_wrapper, is_temp_hold_vector_index, is_fast_build,
                                                     job_id, fmt::format("{}-{}", job_id, trace));
  bool ret = false;
  if (is_fast_build) {
    ret = Server::GetInstance().GetVectorIndexManager()->ExecuteTaskFast(vector_index_wrapper->Id(), task);
  } else {
    ret = Server::GetInstance().GetVectorIndexManager()->ExecuteTask(vector_index_wrapper->Id(), task);
  }

  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.launch][index_id({})][trace({})] Launch build vector index failed, is_fast_build({})",
        vector_index_wrapper->Id(), trace, is_fast_build);
  } else {
    vector_index_wrapper->IncRebuildingNum();
    vector_index_wrapper->IncPendingTaskNum();
  }
}

// Rebuild vector index
butil::Status VectorIndexManager::RebuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                     const std::string& trace) {
  assert(vector_index_wrapper != nullptr);

  int64_t vector_index_id = vector_index_wrapper->Id();
  DINGO_LOG(INFO) << fmt::format("[vector_index.rebuild][index_id({}_v{})][trace({})] Start rebuild vector index.",
                                 vector_index_id, vector_index_wrapper->Version(), trace);

  int64_t start_time = Helper::TimestampMs();
  // Build vector index with original data.
  auto vector_index = BuildVectorIndex(vector_index_wrapper, trace);
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.rebuild][index_id({})][trace({})] Build vector index failed.",
                                      vector_index_id, trace);

    vector_index_wrapper->SetRebuildError();

    return butil::Status(pb::error::Errno::EINTERNAL, "Build vector index failed");
  }

  // Check vector index is stop
  if (vector_index_wrapper->IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.rebuild][index_id({})][trace({})] vector index is stop.",
                                      vector_index_id, trace);
    vector_index_wrapper->SetRebuildError();

    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.rebuild][index_id({}_v{})][trace({})] Build vector index success, log_id {} elapsed time: {}ms",
      vector_index_id, vector_index_wrapper->Version(), trace, vector_index->ApplyLogId(),
      Helper::TimestampMs() - start_time);

  bvar_vector_index_rebuild_catchup_total_num << 1;
  bvar_vector_index_rebuild_catchup_running_num << 1;
  DEFER(bvar_vector_index_rebuild_catchup_running_num << -1;);

  auto status = CatchUpLogToVectorIndex(vector_index_wrapper, vector_index, trace);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.rebuild][index_id({})][trace({})] Catch up log failed, error: {}.",
                                      vector_index_id, trace, Helper::PrintStatus(status));
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.rebuild][index_id({}_v{})][trace({})] Rebuild vector index success, elapsed time: {}ms.",
      vector_index_id, vector_index_wrapper->Version(), trace, Helper::TimestampMs() - start_time);

  return butil::Status();
}

butil::Status VectorIndexManager::LoadVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                  const pb::common::RegionEpoch& epoch, const std::string& trace) {
  int64_t vector_index_id = vector_index_wrapper->Id();
  int64_t start_time = Helper::TimestampMs();

  // try to load vector index from snapshot
  auto vector_index = VectorIndexSnapshotManager::LoadVectorIndexSnapshot(vector_index_wrapper, epoch);
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_LOAD_SNAPSHOT, "load vector snapshot failed");
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.load][index_id({})][trace({})] Load vector index snapshot success, epoch: {} elapsed time: "
      "{}ms.",
      vector_index_id, trace, Helper::RegionEpochToString(vector_index->Epoch()), Helper::TimestampMs() - start_time);

  // catch up wal
  bvar_vector_index_load_catchup_total_num << 1;
  bvar_vector_index_load_catchup_running_num << 1;
  DEFER(bvar_vector_index_load_catchup_running_num << -1;);

  auto status = CatchUpLogToVectorIndex(vector_index_wrapper, vector_index, trace);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.load][index_id({})][trace({})] Catch up log failed, error: {}.",
                                      vector_index_id, trace, Helper::PrintStatus(status));
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.load][index_id({})][trace({})] Load vector index success, epoch: {} elapsed time: {}ms.",
      vector_index_id, trace, Helper::RegionEpochToString(vector_index->Epoch()), Helper::TimestampMs() - start_time);

  return butil::Status();
}

butil::Status VectorIndexManager::CatchUpLogToVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                          std::shared_ptr<VectorIndex> vector_index,
                                                          const std::string& trace) {
  assert(vector_index_wrapper != nullptr);
  assert(vector_index != nullptr);

  int64_t vector_index_id = vector_index_wrapper->Id();
  int64_t start_time = Helper::TimestampMs();

  // Get region
  auto region = Server::GetInstance().GetRegion(vector_index_wrapper->Id());
  if (region == nullptr) {
    return butil::Status(pb::error::ERAFT_META_NOT_FOUND, "not found region.");
  }

  if (region->GetStoreEngineType() != pb::common::STORE_ENG_RAFT_STORE ||
      pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN == vector_index_wrapper->Type()) {
    // stop write index
    vector_index_wrapper->SetIsSwitchingVectorIndex(true);
    DEFER(vector_index_wrapper->SetIsSwitchingVectorIndex(false);
          bvar_vector_index_catchup_latency_last_round << (Helper::TimestampMs() - start_time););

    pb::common::RegionEpoch epoch;
    pb::common::Range range;
    region->GetEpochAndRange(epoch, range);
    vector_index->SetEpochAndRange(epoch, range);

    vector_index_wrapper->UpdateVectorIndex(vector_index, trace);
    vector_index_wrapper->SetRebuildSuccess();

    return butil::Status();
  }

  auto raft_meta = Server::GetInstance().GetRaftMeta(vector_index_wrapper->Id());
  for (int i = 0;; ++i) {
    int64_t start_log_id = vector_index->ApplyLogId() + 1;
    int64_t end_log_id = raft_meta->AppliedId();
    if (end_log_id - start_log_id < FLAGS_vector_catchup_log_min_gap) {
      break;
    }

    auto status = ReplayWalToVectorIndex(vector_index, start_log_id, end_log_id);
    if (!status.ok()) {
      vector_index_wrapper->SetRebuildError();
      return butil::Status(pb::error::Errno::EINTERNAL,
                           fmt::format("Catch up {}-round({}-{}) failed", i, start_log_id, end_log_id));
    }

    DINGO_LOG(INFO) << fmt::format("[vector_index.catchup][index_id({})][trace({})] Catch up {}-round({}-{}) success.",
                                   vector_index_id, trace, i, start_log_id, end_log_id);

    // Check vector index is stop
    if (vector_index_wrapper->IsStop()) {
      DINGO_LOG(WARNING) << fmt::format("[vector_index.catchup][index_id({})][trace({})] vector index is stop.",
                                        vector_index_id, trace);
      return butil::Status();
    }
  }
  bvar_vector_index_catchup_latency_first_rounds << (Helper::TimestampMs() - start_time);

  {
    start_time = Helper::TimestampMs();

    // stop write index
    vector_index_wrapper->SetIsSwitchingVectorIndex(true);
    DEFER(vector_index_wrapper->SetIsSwitchingVectorIndex(false);
          bvar_vector_index_catchup_latency_last_round << (Helper::TimestampMs() - start_time););

    int64_t start_log_id = vector_index->ApplyLogId() + 1;
    int64_t end_log_id = raft_meta->AppliedId();
    // second ground replay wal
    auto status = ReplayWalToVectorIndex(vector_index, start_log_id, end_log_id);
    if (!status.ok()) {
      vector_index_wrapper->SetRebuildError();
      return status;
    }

    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.catchup][index_id({})][trace({})] Catch up last-round({}-{}) success, elapsed time({}ms).",
        vector_index_id, trace, start_log_id, end_log_id, Helper::TimestampMs() - start_time);

    pb::common::RegionEpoch epoch;
    pb::common::Range range;
    region->GetEpochAndRange(epoch, range);
    vector_index->SetEpochAndRange(epoch, range);

    vector_index_wrapper->UpdateVectorIndex(vector_index, trace);
    vector_index_wrapper->SetRebuildSuccess();
  }

  return butil::Status();
}

butil::Status VectorIndexManager::SaveVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                  const std::string& trace) {
  assert(vector_index_wrapper != nullptr);
  int64_t start_time = Helper::TimestampMs();

  // Check vector index is stop
  if (vector_index_wrapper->IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.save][index_id({})][trace({})] vector index is stop.",
                                      vector_index_wrapper->Id(), trace);
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.save][index_id({}_v{})][trace({})] Save vector index.",
                                 vector_index_wrapper->Id(), vector_index_wrapper->Version(), trace);

  int64_t snapshot_log_id = 0;
  auto status = VectorIndexSnapshotManager::SaveVectorIndexSnapshot(vector_index_wrapper, snapshot_log_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.save][index_id({})][trace({})] Save vector index snapshot failed, errno: {}, errstr: {}",
        vector_index_wrapper->Id(), trace, status.error_code(), status.error_str());
    return status;
  } else {
    vector_index_wrapper->SetSnapshotLogId(snapshot_log_id);
  }

  // Update vector index status NORMAL
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.save][index_id({}_v{})][trace({})] Save vector index success, elapsed time({}ms)",
      vector_index_wrapper->Id(), vector_index_wrapper->Version(), trace, Helper::TimestampMs() - start_time);

  // Check vector index is stop
  if (vector_index_wrapper->IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.save][index_id({}_v{})][trace({})] vector index is stop.",
                                      vector_index_wrapper->Id(), vector_index_wrapper->Version(), trace);
    return butil::Status();
  }

  return butil::Status();
}

void VectorIndexManager::LaunchSaveVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, const std::string& trace) {
  assert(vector_index_wrapper != nullptr);
  auto region = Server::GetInstance().GetRegion(vector_index_wrapper->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.launch][index_id({})][trace({})] Not found region.",
                                    vector_index_wrapper->Id(), trace);
    return;
  }
  if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.launch][index_id({})][trace({})] rocks engine not need save vector index.",
        vector_index_wrapper->Id(), trace);
    return;
  }
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.launch][index_id({})][trace({})] Launch save vector index, pending tasks({}) total running({}).",
      vector_index_wrapper->Id(), trace, vector_index_wrapper->PendingTaskNum(), GetVectorIndexTaskRunningNum());

  auto task = std::make_shared<SaveVectorIndexTask>(vector_index_wrapper, trace);
  if (!Server::GetInstance().GetVectorIndexManager()->ExecuteTask(vector_index_wrapper->Id(), task)) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.launch][index_id({})][trace({})] Launch save vector index failed",
                                    vector_index_wrapper->Id(), trace);
  } else {
    vector_index_wrapper->IncPendingTaskNum();
    vector_index_wrapper->IncSavingNum();
  }
}

butil::Status VectorIndexManager::ScrubVectorIndex() {
  auto regions = Server::GetInstance().GetAllAliveRegion();
  if (regions.empty()) {
    DINGO_LOG(INFO) << "[vector_index.scrub][index_id()] No alive region, skip scrub vector index";
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << "[vector_index.scrub][index_id()] Scrub vector index start, alive region_count is "
                  << regions.size();

  for (const auto& region : regions) {
    int64_t vector_index_id = region->Id();
    if (region->State() != pb::common::NORMAL) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] region state is not normal, dont't scrub.",
                                     vector_index_id);
      continue;
    }
    auto vector_index_wrapper = region->VectorIndexWrapper();
    if (!vector_index_wrapper->IsReady()) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] vector index is not ready, dont't scrub.",
                                     vector_index_id);
      continue;
    }
    if (vector_index_wrapper->IsStop()) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] vector index is stop, dont't scrub.",
                                     vector_index_id);
      continue;
    }

    bool need_rebuild = vector_index_wrapper->NeedToRebuild();
    if (need_rebuild && vector_index_wrapper->RebuildingNum() == 0) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] need rebuild, do rebuild vector index.",
                                     vector_index_id);
      LaunchRebuildVectorIndex(vector_index_wrapper, 0, true, false, false, "from scrub");
      continue;
    }

    std::string trace;
    bool need_save = vector_index_wrapper->NeedToSave(trace);
    if (need_save && vector_index_wrapper->RebuildingNum() == 0 && vector_index_wrapper->SavingNum() == 0) {
      DINGO_LOG(INFO) << fmt::format("[vector_index.scrub][index_id({})] need save, trace: {}.", vector_index_id,
                                     trace);

      LaunchSaveVectorIndex(vector_index_wrapper, fmt::format("scrub-{}", trace));
    }
  }

  return butil::Status::OK();
}

// range is encode range
butil::Status VectorIndexManager::TrainForBuild(std::shared_ptr<VectorIndex> vector_index, mvcc::ReaderPtr reader,
                                                const pb::common::Range& encode_range) {
  IteratorOptions options;
  options.upper_bound = encode_range.end_key();
  auto iter = reader->NewIterator(Constant::kVectorDataCF, 0, options);
  CHECK(iter != nullptr) << fmt::format("[vector_index.build][index_id({})] NewIterator failed.", vector_index->Id());

  std::vector<float> train_vectors;
  train_vectors.reserve(100000 * vector_index->GetDimension());  // todo opt
  for (iter->Seek(encode_range.start_key()); iter->Valid(); iter->Next()) {
    pb::common::VectorWithId vector;

    std::string value(mvcc::Codec::UnPackageValue(iter->Value()));
    CHECK(vector.mutable_vector()->ParseFromString(value)) << "parse vector pb failed.";

    if (vector.vector().float_values_size() <= 0) {
      DINGO_LOG(WARNING) << fmt::format("[vector_index.build][index_id({})] vector values_size error.",
                                        vector_index->Id());
      continue;
    }

    train_vectors.insert(train_vectors.end(), vector.vector().float_values().begin(),
                         vector.vector().float_values().end());
  }

  if (!train_vectors.empty()) {
    auto status = vector_index->TrainByParallel(train_vectors);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[vector_index.build][index_id({})] train failed, error: {}", vector_index->Id(),
                                      status.error_str());
      return status;
    }
  }

  return butil::Status::OK();
}

bool VectorIndexManager::ExecuteTask(int64_t region_id, TaskRunnablePtr task) {
  if (background_workers_ == nullptr) {
    return false;
  }

  return background_workers_->ExecuteHashByRegionId(region_id, task);
}

bool VectorIndexManager::ExecuteTaskFast(int64_t region_id, TaskRunnablePtr task) {
  if (fast_background_workers_ == nullptr) {
    return false;
  }

  return fast_background_workers_->ExecuteHashByRegionId(region_id, task);
}

std::vector<std::vector<std::string>> VectorIndexManager::GetPendingTaskTrace() {
  if (background_workers_ == nullptr) {
    return {};
  }

  return background_workers_->GetPendingTaskTrace();
}

uint64_t VectorIndexManager::GetBackgroundPendingTaskCount() {
  if (background_workers_ == nullptr) {
    return 0;
  }

  return background_workers_->PendingTaskCount();
}

}  // namespace dingodb

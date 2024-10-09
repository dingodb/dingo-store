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

#include "document/document_index_manager.h"

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
#include "document/codec.h"
#include "document/document_index.h"
#include "document/document_index_factory.h"
#include "document/document_index_snapshot_manager.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/file_service.pb.h"
#include "proto/node.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
namespace dingodb {

DEFINE_int32(document_background_worker_num, 16, "document index background worker num");
DEFINE_int32(document_fast_background_worker_num, 8, "document index fast background worker num");
DEFINE_int64(document_fast_build_log_gap, 50, "document index fast build log gap");
DEFINE_int64(document_pull_snapshot_min_log_gap, 66, "document index pull snapshot min log gap");
DEFINE_int64(document_max_background_task_count, 32, "document index max background task count");

std::string RebuildDocumentIndexTask::Trace() {
  return fmt::format("[document_index.rebuild][id({}).start_time({}).job_id({})] {}", document_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void RebuildDocumentIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.rebuild][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) wait_time({}).",
      document_index_wrapper_->Id(), trace_, document_index_wrapper_->RebuildingNum(),
      document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexRebuildTaskRunningNum(),
      DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);

  int64_t start_time = Helper::TimestampMs();
  DocumentIndexManager::IncDocumentIndexTaskRunningNum();
  DocumentIndexManager::IncDocumentIndexRebuildTaskRunningNum();
  ON_SCOPE_EXIT([&]() {
    DocumentIndexManager::DecDocumentIndexTaskRunningNum();
    DocumentIndexManager::DecDocumentIndexRebuildTaskRunningNum();
    document_index_wrapper_->DecPendingTaskNum();
    document_index_wrapper_->DecRebuildingNum();

    LOG(INFO) << fmt::format(
        "[document_index.rebuild][index_id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}).",
        document_index_wrapper_->Id(), trace_, document_index_wrapper_->RebuildingNum(),
        document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexRebuildTaskRunningNum(),
        DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);
  });

  auto region = Server::GetInstance().GetRegion(document_index_wrapper_->Id());
  if (region == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.rebuild][index_id({})][trace({})] Not found region.",
                                      document_index_wrapper_->Id(), trace_);
    return;
  }
  auto state = region->State();
  if (state == pb::common::StoreRegionState::STANDBY || state == pb::common::StoreRegionState::DELETING ||
      state == pb::common::StoreRegionState::DELETED || state == pb::common::StoreRegionState::ORPHAN ||
      state == pb::common::StoreRegionState::TOMBSTONE) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.rebuild][index_id({})][trace({})] region state({}) not match.",
                                      document_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state));
    return;
  }

  if (Helper::InvalidRange(region->Range(false))) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.rebuild][index_id({})][trace({})] region range invalid.",
                                      document_index_wrapper_->Id(), trace_);
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuild document index {}", region->Id()));

  if (document_index_wrapper_->IsDestoryed()) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.rebuild][index_id({})][trace({})] document index is stop, gave up rebuild.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  if (is_double_check_) {
    if (!document_index_wrapper_->IsOwnReady()) {
      DINGO_LOG(INFO) << fmt::format(
          "[document_index.rebuild][index_id({})][trace({})] document index is not ready, gave up rebuild.",
          document_index_wrapper_->Id(), trace_);
      return;
    }
    if (!document_index_wrapper_->NeedToRebuild()) {
      DINGO_LOG(INFO) << fmt::format(
          "[document_index.rebuild][index_id({})][trace({})] document index not need rebuild, gave up rebuild.",
          document_index_wrapper_->Id(), trace_);
      return;
    }
  }

  if (!is_force_) {
    // Compare document index snapshot epoch and region epoch.
    auto snapshot_set = document_index_wrapper_->SnapshotSet();
    if (snapshot_set != nullptr) {
      auto last_snapshot = snapshot_set->GetLastSnapshot();
      if (last_snapshot != nullptr && region->Epoch().version() <= last_snapshot->Epoch().version()) {
        DINGO_LOG(INFO) << fmt::format(
            "[document_index.rebuild][index_id({})][trace({})] document index snapshot epoch({}/{}) is latest, gave up "
            "rebuild.",
            document_index_wrapper_->Id(), trace_, Helper::RegionEpochToString(region->Epoch()),
            Helper::RegionEpochToString(last_snapshot->Epoch()));
        return;
      }
    }

    pb::store_internal::DocumentIndexSnapshotMeta meta;
    auto ret1 = DocumentIndexSnapshotManager::GetLatestSnapshotMeta(document_index_wrapper_->Id(), meta);
    if (!ret1.ok()) {
      DINGO_LOG(ERROR) << fmt::format(
          "[document_index.rebuild][index_id({})][trace({})] get document index snapshot meta failed, error: {}.",
          document_index_wrapper_->Id(), trace_, Helper::PrintStatus(ret1));
      return;
    }

    if (region->Epoch().version() <= meta.epoch().version()) {
      DINGO_LOG(INFO) << fmt::format(
          "[document_index.rebuild][index_id({})][trace({})] document index snapshot epoch({}/{}) is latest, gave up "
          "rebuild.",
          document_index_wrapper_->Id(), trace_, Helper::RegionEpochToString(region->Epoch()),
          Helper::RegionEpochToString(meta.epoch()));
      return;
    }
  } else {
    DINGO_LOG(WARNING) << fmt::format(
        "[document_index.rebuild][index_id({})][trace({})] force is not support now, gave up rebuild.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuilding document index {}", region->Id()));

  // document_index_wrapper_->SetIsTempHoldDocumentIndex(true);
  auto status = DocumentIndexManager::RebuildDocumentIndex(document_index_wrapper_, fmt::format("REBUILD-{}", trace_));
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuilded document index {}", region->Id()));
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.rebuild][index_id({}_v{})][trace({})] rebuild document index failed, error: {}.",
        document_index_wrapper_->Id(), document_index_wrapper_->Version(), trace_, Helper::PrintStatus(status));
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Saving document index {}", region->Id()));

  if (region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    // raft store engine use snapshot
    status = DocumentIndexManager::SaveDocumentIndex(document_index_wrapper_, trace_);
    if (!status.ok()) {
      ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Saved document index {} failed", region->Id()));
      DINGO_LOG(ERROR) << fmt::format(
          "[document_index.save][index_id({}_v{})][trace({})] save document index failed, error: {}.",
          document_index_wrapper_->Id(), document_index_wrapper_->Version(), trace_, Helper::PrintStatus(status));
    }
  }

  // document_index_wrapper_->SetIsTempHoldDocumentIndex(false);
  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Saved document index {}", region->Id()));

  if (is_clear_) {
    if (!DocumentIndexWrapper::IsPermanentHoldDocumentIndex(document_index_wrapper_->Id())) {
      document_index_wrapper_->ClearDocumentIndex(trace_);
    }

    auto store_region_meta = GET_STORE_REGION_META;
    store_region_meta->UpdateTemporaryDisableChange(region, false);
  }
}

std::string SaveDocumentIndexTask::Trace() {
  return fmt::format("[document_index.save][id({}).start_time({})] {}", document_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), trace_);
}

void SaveDocumentIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.save][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) wait_time({}).",
      document_index_wrapper_->Id(), trace_, document_index_wrapper_->SavingNum(),
      document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexSaveTaskRunningNum(),
      DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);

  int64_t start_time = Helper::TimestampMs();
  DocumentIndexManager::IncDocumentIndexTaskRunningNum();
  DocumentIndexManager::IncDocumentIndexSaveTaskRunningNum();
  ON_SCOPE_EXIT([&]() {
    DocumentIndexManager::DecDocumentIndexTaskRunningNum();
    DocumentIndexManager::DecDocumentIndexSaveTaskRunningNum();
    document_index_wrapper_->DecPendingTaskNum();
    document_index_wrapper_->DecSavingNum();

    LOG(INFO) << fmt::format(
        "[document_index.save][index_id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}).",
        document_index_wrapper_->Id(), trace_, document_index_wrapper_->SavingNum(),
        document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexSaveTaskRunningNum(),
        DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time);
  });

  if (document_index_wrapper_->IsDestoryed()) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.save][index_id({})][trace({})] document index is stop, gave up save document index.",
        document_index_wrapper_->Id(), trace_);
    return;
  }
  if (!document_index_wrapper_->IsOwnReady()) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.save][index_id({})][trace({})] document index is not ready, gave up save document index.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  auto status = DocumentIndexManager::SaveDocumentIndex(document_index_wrapper_, trace_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.save][index_id({}_v{})][trace({})] save document index failed, error {}",
        document_index_wrapper_->Id(), document_index_wrapper_->Version(), trace_, status.error_str());
    return;
  }
}

std::string LoadOrBuildDocumentIndexTask::Trace() {
  return fmt::format("[document_index.loadorbuild][id({}).start_time({}).job_id({})] {}", document_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void LoadOrBuildDocumentIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.loadorbuild][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) "
      "wait_time({}).",
      document_index_wrapper_->Id(), trace_, document_index_wrapper_->LoadorbuildingNum(),
      document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexLoadorbuildTaskRunningNum(),
      DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);

  int64_t start_time = Helper::TimestampMs();
  DocumentIndexManager::IncDocumentIndexTaskRunningNum();
  DocumentIndexManager::IncDocumentIndexLoadorbuildTaskRunningNum();
  ON_SCOPE_EXIT([&]() {
    DocumentIndexManager::DecDocumentIndexTaskRunningNum();
    DocumentIndexManager::DecDocumentIndexLoadorbuildTaskRunningNum();
    document_index_wrapper_->DecPendingTaskNum();
    document_index_wrapper_->DecLoadoruildingNum();

    LOG(INFO) << fmt::format(
        "[document_index.loadorbuild][index_id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}).",
        document_index_wrapper_->Id(), trace_, document_index_wrapper_->LoadorbuildingNum(),
        document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexLoadorbuildTaskRunningNum(),
        DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time);
  });

  // Get region meta
  auto region = Server::GetInstance().GetRegion(document_index_wrapper_->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.loadorbuild][region({})][trace({})] not found region.",
                                    document_index_wrapper_->Id(), trace_);
    return;
  }
  auto state = region->State();
  if (state == pb::common::StoreRegionState::DELETING || state == pb::common::StoreRegionState::DELETED ||
      state == pb::common::StoreRegionState::ORPHAN || state == pb::common::StoreRegionState::TOMBSTONE) {
    DINGO_LOG(WARNING) << fmt::format(
        "[document_index.loadorbuild][index_id({})][trace({})] region state({}) not match.",
        document_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state));
    return;
  }

  // if (is_temp_hold_document_index_) {
  //   document_index_wrapper_->SetIsTempHoldDocumentIndex(true);
  // }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuild document index {}", region->Id()));

  if (document_index_wrapper_->IsDestoryed()) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.loadorbuild][index_id({})][trace({})] document index is stop, gave up loadorbuild document "
        "index.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  if (document_index_wrapper_->IsOwnReady() &&
      document_index_wrapper_->LastBuildEpochVersion() >= region->Epoch().version()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Already own document index {}", region->Id()));
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.loadorbuild][index_id({})][trace({})] document index is ready, gave up loadorbuild document "
        "index.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  // Pull snapshot from peers.
  // New region don't pull snapshot, directly build.
  auto raft_meta = Server::GetInstance().GetRaftMeta(document_index_wrapper_->Id());
  int64_t applied_index = -1;
  if (raft_meta != nullptr) {
    applied_index = raft_meta->AppliedId();
  }

  // if (region->Epoch().version() > 1 || applied_index > FLAGS_document_pull_snapshot_min_log_gap) {
  //   auto snapshot_set = document_index_wrapper_->SnapshotSet();
  //   auto status = DocumentIndexSnapshotManager::PullLastSnapshotFromPeers(snapshot_set, region->Epoch());
  //   DINGO_LOG(INFO) << fmt::format(
  //       "[document_index.loadorbuild][region({})][trace({})] pull document index last snapshot done, error: {}",
  //       document_index_wrapper_->Id(), trace_, Helper::PrintStatus(status));
  // }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuilding document index {}", region->Id()));

  auto status = DocumentIndexManager::LoadOrBuildDocumentIndex(document_index_wrapper_, region->Epoch(), trace_);
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuilded document index {} failed", region->Id()));
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.load][index_id({}_v{})][trace({})] load or build document index failed, error {}",
        document_index_wrapper_->Id(), document_index_wrapper_->Version(), trace_, status.error_str());
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuilded document index {}", region->Id()));
}

std::string LoadAsyncBuildDocumentIndexTask::Trace() {
  return fmt::format("[document_index.loadasyncbuild][id({}).start_time({}).job_id({})] {}",
                     document_index_wrapper_->Id(), Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void LoadAsyncBuildDocumentIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.loadasyncbuild][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) "
      "wait_time({}) is_fast_load ({}).",
      document_index_wrapper_->Id(), trace_, document_index_wrapper_->LoadorbuildingNum(),
      document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexLoadorbuildTaskRunningNum(),
      DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time_, is_fast_load_);

  int64_t start_time = Helper::TimestampMs();
  DocumentIndexManager::IncDocumentIndexTaskRunningNum();
  if (is_fast_load_) {
    DocumentIndexManager::IncDocumentIndexFastLoadTaskRunningNum();
  } else {
    DocumentIndexManager::IncDocumentIndexSlowLoadTaskRunningNum();
  }
  ON_SCOPE_EXIT([&]() {
    DocumentIndexManager::DecDocumentIndexTaskRunningNum();
    if (is_fast_load_) {
      DocumentIndexManager::DecDocumentIndexFastLoadTaskRunningNum();
    } else {
      DocumentIndexManager::DecDocumentIndexSlowLoadTaskRunningNum();
    }
    document_index_wrapper_->DecPendingTaskNum();
    document_index_wrapper_->DecLoadoruildingNum();

    LOG(INFO) << fmt::format(
        "[document_index.loadasyncbuild][index_id({})][trace({})] run finish, pending tasks({}/{}) total "
        "running({}/{}) "
        "run_time({}) is_fast_load ({}).",
        document_index_wrapper_->Id(), trace_, document_index_wrapper_->LoadorbuildingNum(),
        document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexFastLoadTaskRunningNum(),
        DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time, is_fast_load_);
  });

  // Get region meta
  auto region = Server::GetInstance().GetRegion(document_index_wrapper_->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.loadasyncbuild][region({})][trace({})] not found region.",
                                    document_index_wrapper_->Id(), trace_);
    return;
  }
  auto state = region->State();
  if (state == pb::common::StoreRegionState::DELETING || state == pb::common::StoreRegionState::DELETED ||
      state == pb::common::StoreRegionState::ORPHAN || state == pb::common::StoreRegionState::TOMBSTONE) {
    DINGO_LOG(WARNING) << fmt::format(
        "[document_index.loadasyncbuild][index_id({})][trace({})] region state({}) not match. is_fast_load ({})",
        document_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state), is_fast_load_);
    return;
  }

  // if (is_temp_hold_document_index_) {
  //   document_index_wrapper_->SetIsTempHoldDocumentIndex(true);
  // }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncbuild document index {}", region->Id()));

  if (document_index_wrapper_->IsDestoryed()) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.loadasyncbuild][index_id({})][trace({})] document index is stop, gave up loadasyncbuild "
        "document "
        "index.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  if (document_index_wrapper_->IsOwnReady() &&
      document_index_wrapper_->LastBuildEpochVersion() >= region->Epoch().version()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Already own document index {}", region->Id()));
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.loadasyncbuild][index_id({})][trace({})] document index is ready, gave up loadasyncbuild "
        "document "
        "index.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  // Pull snapshot from peers.
  // New region don't pull snapshot, directly build.
  auto raft_meta = Server::GetInstance().GetRaftMeta(document_index_wrapper_->Id());
  int64_t applied_index = (raft_meta != nullptr) ? raft_meta->AppliedId() : -1;

  if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
    // start to do async build document index.
    bool is_fast_build =
        is_fast_load_ && (region->Epoch().version() == 1) && (applied_index <= FLAGS_document_fast_build_log_gap);

    DINGO_LOG(INFO) << fmt::format(
        "[document_index.loadasyncbuild][index_id({}_v{})][trace({})] rocks engine direct do async "
        "build,  is_fast_build {} region_version {}",
        document_index_wrapper_->Id(), document_index_wrapper_->Version(), trace_, is_fast_build,
        region->Epoch().version());

    DocumentIndexManager::LaunchBuildDocumentIndex(document_index_wrapper_, is_temp_hold_document_index_, is_fast_build,
                                                   job_id_, "load async build");
    return;
  }

  // if (region->Epoch().version() > 1 || applied_index > FLAGS_document_pull_snapshot_min_log_gap) {
  //   auto snapshot_set = document_index_wrapper_->SnapshotSet();
  //   auto status = DocumentIndexSnapshotManager::PullLastSnapshotFromPeers(snapshot_set, region->Epoch());
  //   DINGO_LOG(INFO) << fmt::format(
  //       "[document_index.loadasyncbuild][region({})][trace({})] pull document index last snapshot done, error: {} "
  //       "version: "
  //       "{} applied_index {}",
  //       document_index_wrapper_->Id(), trace_, Helper::PrintStatus(status), region->Epoch().version(),
  //       applied_index);
  // } else {
  //   DINGO_LOG(INFO) << fmt::format(
  //       "[document_index.loadasyncbuild][region({})][trace({})] skip pull document index last snapshot for new create
  //       " "region, version: {} applied_index {}", document_index_wrapper_->Id(), trace_, region->Epoch().version(),
  //       applied_index);
  // }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncbuilding document index {}", region->Id()));

  auto status = DocumentIndexManager::LoadDocumentIndexOnly(document_index_wrapper_, region->Epoch(), trace_);
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncbuilded document index {} failed", region->Id()));

    // start to do async build document index.
    bool is_fast_build =
        is_fast_load_ && (region->Epoch().version() == 1) && (applied_index <= FLAGS_document_fast_build_log_gap);

    DINGO_LOG(INFO) << fmt::format(
        "[document_index.loadasyncbuild][index_id({}_v{})][trace({})] load document index failed, will try to do async "
        "build, error {} applied_index {} is_fast_build {} region_version {}",
        document_index_wrapper_->Id(), document_index_wrapper_->Version(), trace_, status.error_str(), applied_index,
        is_fast_build, region->Epoch().version());

    DocumentIndexManager::LaunchBuildDocumentIndex(document_index_wrapper_, is_temp_hold_document_index_, is_fast_build,
                                                   job_id_, "load async build");

    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncrbuilded document index {}", region->Id()));
}

std::string BuildDocumentIndexTask::Trace() {
  return fmt::format("[document_index.build][id({}).start_time({}).job_id({})] {}", document_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void BuildDocumentIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.build][index_id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) "
      "wait_time({}).",
      document_index_wrapper_->Id(), trace_, document_index_wrapper_->LoadorbuildingNum(),
      document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexLoadorbuildTaskRunningNum(),
      DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);

  int64_t start_time = Helper::TimestampMs();
  DocumentIndexManager::IncDocumentIndexTaskRunningNum();
  if (is_fast_build_) {
    DocumentIndexManager::IncDocumentIndexFastBuildTaskRunningNum();
  } else {
    DocumentIndexManager::IncDocumentIndexSlowBuildTaskRunningNum();
  }
  ON_SCOPE_EXIT([&]() {
    DocumentIndexManager::DecDocumentIndexTaskRunningNum();
    if (is_fast_build_) {
      DocumentIndexManager::DecDocumentIndexFastBuildTaskRunningNum();
    } else {
      DocumentIndexManager::DecDocumentIndexSlowBuildTaskRunningNum();
    }
    document_index_wrapper_->DecPendingTaskNum();
    document_index_wrapper_->DecRebuildingNum();

    LOG(INFO) << fmt::format(
        "[document_index.build][index_id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}).",
        document_index_wrapper_->Id(), trace_, document_index_wrapper_->RebuildingNum(),
        document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexRebuildTaskRunningNum(),
        DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time);
  });

  // Get region meta
  auto region = Server::GetInstance().GetRegion(document_index_wrapper_->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.build][region({})][trace({})] not found region.",
                                    document_index_wrapper_->Id(), trace_);
    return;
  }
  auto state = region->State();
  if (state == pb::common::StoreRegionState::DELETING || state == pb::common::StoreRegionState::DELETED ||
      state == pb::common::StoreRegionState::ORPHAN || state == pb::common::StoreRegionState::TOMBSTONE) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.build][index_id({})][trace({})] region state({}) not match.",
                                      document_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state));
    return;
  }

  // if (is_temp_hold_document_index_) {
  //   document_index_wrapper_->SetIsTempHoldDocumentIndex(true);
  // }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("build document index {}", region->Id()));

  if (document_index_wrapper_->IsDestoryed()) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.build][index_id({})][trace({})] document index is stop, gave up build document "
        "index.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  if (document_index_wrapper_->IsOwnReady() &&
      document_index_wrapper_->LastBuildEpochVersion() >= region->Epoch().version()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Already own document index {}", region->Id()));
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.build][index_id({})][trace({})] document index is ready, gave up build document "
        "index.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  auto status = DocumentIndexManager::BuildDocumentIndexOnly(document_index_wrapper_, region->Epoch(), trace_);
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("builded document index {} failed", region->Id()));
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.load][index_id({}_v{})][trace({})] load async build document index failed, error {}",
        document_index_wrapper_->Id(), document_index_wrapper_->Version(), trace_, status.error_str());
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadasyncrbuilded document index {}", region->Id()));
}

bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_task_running_num(
    "dingo_document_index_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_rebuild_task_running_num(
    "dingo_document_index_rebuild_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_save_task_running_num(
    "dingo_document_index_save_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_loadorbuild_task_running_num(
    "dingo_document_index_loadorbuild_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_fast_load_task_running_num(
    "dingo_document_index_fast_load_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_slow_load_task_running_num(
    "dingo_document_index_slow_load_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_fast_build_task_running_num(
    "dingo_document_index_fast_build_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_slow_build_task_running_num(
    "dingo_document_index_slow_build_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_load_catchup_running_num(
    "dingo_document_index_load_catchup_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_rebuild_catchup_running_num(
    "dingo_document_index_rebuild_catchup_task_running_num");

bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_task_total_num("dingo_document_index_task_total_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_rebuild_task_total_num(
    "dingo_document_index_rebuild_task_total_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_save_task_total_num(
    "dingo_document_index_save_task_total_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_loadorbuild_task_total_num(
    "dingo_document_index_loadorbuild_task_total_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_fast_load_task_total_num(
    "dingo_document_index_fast_load_task_total_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_slow_load_task_total_num(
    "dingo_document_index_slow_load_task_total_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_fast_build_task_total_num(
    "dingo_document_index_fast_build_task_total_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_slow_build_task_total_num(
    "dingo_document_index_slow_build_task_total_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_load_catchup_total_num(
    "dingo_document_index_load_catchup_task_total_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_rebuild_catchup_total_num(
    "dingo_document_index_rebuild_catchup_task_total_num");

bvar::LatencyRecorder DocumentIndexManager::bvar_document_index_catchup_latency_first_rounds(
    "dingo_document_index_catchup_latency_first_rounds");
bvar::LatencyRecorder DocumentIndexManager::bvar_document_index_catchup_latency_last_round(
    "dingo_document_index_catchup_latency_last_round");

std::atomic<int> DocumentIndexManager::document_index_task_running_num = 0;
std::atomic<int> DocumentIndexManager::document_index_rebuild_task_running_num = 0;
std::atomic<int> DocumentIndexManager::document_index_save_task_running_num = 0;
std::atomic<int> DocumentIndexManager::document_index_loadorbuild_task_running_num = 0;
std::atomic<int> DocumentIndexManager::document_index_fast_load_task_running_num = 0;
std::atomic<int> DocumentIndexManager::document_index_slow_load_task_running_num = 0;
std::atomic<int> DocumentIndexManager::document_index_fast_build_task_running_num = 0;
std::atomic<int> DocumentIndexManager::document_index_slow_build_task_running_num = 0;

int DocumentIndexManager::GetDocumentIndexTaskRunningNum() { return document_index_task_running_num.load(); }

void DocumentIndexManager::IncDocumentIndexTaskRunningNum() {
  document_index_task_running_num.fetch_add(1);
  bvar_document_index_task_running_num << 1;
  bvar_document_index_task_total_num << 1;
}

void DocumentIndexManager::DecDocumentIndexTaskRunningNum() {
  document_index_task_running_num.fetch_sub(1);
  bvar_document_index_task_running_num << -1;
}

int DocumentIndexManager::GetDocumentIndexRebuildTaskRunningNum() {
  return document_index_rebuild_task_running_num.load();
}

void DocumentIndexManager::IncDocumentIndexRebuildTaskRunningNum() {
  document_index_rebuild_task_running_num.fetch_add(1);
  bvar_document_index_rebuild_task_running_num << 1;
  bvar_document_index_rebuild_task_total_num << 1;
}

void DocumentIndexManager::DecDocumentIndexRebuildTaskRunningNum() {
  document_index_rebuild_task_running_num.fetch_sub(1);
  bvar_document_index_rebuild_task_running_num << -1;
}

int DocumentIndexManager::GetDocumentIndexSaveTaskRunningNum() { return document_index_save_task_running_num.load(); }

void DocumentIndexManager::IncDocumentIndexSaveTaskRunningNum() {
  document_index_save_task_running_num.fetch_add(1);
  bvar_document_index_save_task_running_num << 1;
  bvar_document_index_save_task_total_num << 1;
}

void DocumentIndexManager::DecDocumentIndexSaveTaskRunningNum() {
  document_index_save_task_running_num.fetch_sub(1);
  bvar_document_index_save_task_running_num << -1;
}

int DocumentIndexManager::GetDocumentIndexLoadorbuildTaskRunningNum() {
  return document_index_loadorbuild_task_running_num.load();
}

void DocumentIndexManager::IncDocumentIndexLoadorbuildTaskRunningNum() {
  document_index_loadorbuild_task_running_num.fetch_add(1);
  bvar_document_index_loadorbuild_task_running_num << 1;
  bvar_document_index_loadorbuild_task_total_num << 1;
}

void DocumentIndexManager::DecDocumentIndexLoadorbuildTaskRunningNum() {
  document_index_loadorbuild_task_running_num.fetch_sub(1);
  bvar_document_index_loadorbuild_task_running_num << -1;
}

int DocumentIndexManager::GetDocumentIndexFastLoadTaskRunningNum() {
  return document_index_fast_load_task_running_num.load();
}

void DocumentIndexManager::IncDocumentIndexFastLoadTaskRunningNum() {
  document_index_fast_load_task_running_num.fetch_add(1);
  bvar_document_index_fast_load_task_running_num << 1;
  bvar_document_index_fast_load_task_total_num << 1;
}

void DocumentIndexManager::DecDocumentIndexFastLoadTaskRunningNum() {
  document_index_fast_load_task_running_num.fetch_sub(1);
  bvar_document_index_fast_load_task_running_num << -1;
}

int DocumentIndexManager::GetDocumentIndexSlowLoadTaskRunningNum() {
  return document_index_slow_load_task_running_num.load();
}

void DocumentIndexManager::IncDocumentIndexSlowLoadTaskRunningNum() {
  document_index_slow_load_task_running_num.fetch_add(1);
  bvar_document_index_slow_load_task_running_num << 1;
  bvar_document_index_slow_load_task_total_num << 1;
}

void DocumentIndexManager::DecDocumentIndexSlowLoadTaskRunningNum() {
  document_index_slow_load_task_running_num.fetch_sub(1);
  bvar_document_index_slow_load_task_running_num << -1;
}

int DocumentIndexManager::GetDocumentIndexFastBuildTaskRunningNum() {
  return document_index_fast_build_task_running_num.load();
}

void DocumentIndexManager::IncDocumentIndexFastBuildTaskRunningNum() {
  document_index_fast_build_task_running_num.fetch_add(1);
  bvar_document_index_fast_build_task_running_num << 1;
  bvar_document_index_fast_build_task_total_num << 1;
}

void DocumentIndexManager::DecDocumentIndexFastBuildTaskRunningNum() {
  document_index_fast_build_task_running_num.fetch_sub(1);
  bvar_document_index_fast_build_task_running_num << -1;
}

int DocumentIndexManager::GetDocumentIndexSlowBuildTaskRunningNum() {
  return document_index_slow_build_task_running_num.load();
}

void DocumentIndexManager::IncDocumentIndexSlowBuildTaskRunningNum() {
  document_index_slow_build_task_running_num.fetch_add(1);
  bvar_document_index_slow_build_task_running_num << 1;
  bvar_document_index_slow_build_task_total_num << 1;
}

void DocumentIndexManager::DecDocumentIndexSlowBuildTaskRunningNum() {
  document_index_slow_build_task_running_num.fetch_sub(1);
  bvar_document_index_slow_build_task_running_num << -1;
}

bool DocumentIndexManager::Init() {
  background_workers_ = ExecqWorkerSet::New("document_mgr_background", FLAGS_document_background_worker_num, 0);
  if (!background_workers_->Init()) {
    DINGO_LOG(ERROR) << "Init document index manager background worker set failed!";
    return false;
  }

  fast_background_workers_ =
      ExecqWorkerSet::New("document_mgr_fast_background", FLAGS_document_fast_background_worker_num, 0);
  if (!fast_background_workers_->Init()) {
    DINGO_LOG(ERROR) << "Init document index manager fast background worker set failed!";
    return false;
  }

  return true;
}

void DocumentIndexManager::Destroy() {
  if (background_workers_ != nullptr) {
    background_workers_->Destroy();
  }
  if (fast_background_workers_ != nullptr) {
    fast_background_workers_->Destroy();
  }
}

// Load document index for already exist document index at bootstrap.
// Priority load from snapshot, if snapshot not exist then load from original data.
butil::Status DocumentIndexManager::LoadOrBuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                             const pb::common::RegionEpoch& epoch,
                                                             const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  int64_t start_time = Helper::TimestampMs();
  int64_t document_index_id = document_index_wrapper->Id();

  // try to load document index from snapshot
  auto status = LoadDocumentIndex(document_index_wrapper, epoch, fmt::format("LOAD.SNAPSHOT-{}", trace));
  if (status.ok()) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.loadorbuild][index_id({})][trace({})] Load document index from snapshot success, elapsed "
        "time({}ms)",
        document_index_id, trace, Helper::TimestampMs() - start_time);
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.loadorbuild][index_id({})][trace({})] Load document index from snapshot failed, error: {}.",
      document_index_id, trace, Helper::PrintStatus(status));

  // Build a new document index from original data
  status = RebuildDocumentIndex(document_index_wrapper, fmt::format("LOAD.REBUILD-{}", trace));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.loadorbuild][index_id({})][trace({})] Rebuild document index failed.", document_index_id,
        trace);
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.loadorbuild][index_id({})][trace({})] Rebuild document index success, elapsed time({}ms).",
      document_index_id, trace, Helper::TimestampMs() - start_time);

  auto region = Server::GetInstance().GetRegion(document_index_wrapper->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.loadorbuild][region({})] Not found region.",
                                    document_index_wrapper->Id());
    return butil::Status(pb::error::EREGION_NOT_FOUND,
                         fmt::format("Not found region {}", document_index_wrapper->Id()));
  }
  if (region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    // raft store engine use snapshot
    // Save document index
    status = DocumentIndexManager::SaveDocumentIndex(document_index_wrapper, trace);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format(
          "[document_index.loadorbuild][index_id({})][trace({})] save document index failed, error: {}.",
          document_index_wrapper->Id(), trace, Helper::PrintStatus(status));
    }
  }

  return butil::Status();
}

// Load document index for already exist document index at bootstrap.
// Priority load from snapshot, if snapshot not exist then return error.
// This function is for LoadAsyncBuild.
butil::Status DocumentIndexManager::LoadDocumentIndexOnly(DocumentIndexWrapperPtr document_index_wrapper,
                                                          const pb::common::RegionEpoch& epoch,
                                                          const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  int64_t start_time = Helper::TimestampMs();
  int64_t document_index_id = document_index_wrapper->Id();

  // try to load document index from snapshot
  auto status = LoadDocumentIndex(document_index_wrapper, epoch, fmt::format("LOAD.SNAPSHOT-{}", trace));
  if (status.ok()) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.loadorbuild][index_id({})][trace({})] Load document index from snapshot success, elapsed "
        "time({}ms)",
        document_index_id, trace, Helper::TimestampMs() - start_time);
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.loadorbuild][index_id({})][trace({})] Load document index from snapshot failed, will do async "
      "build, error: {}.",
      document_index_id, trace, Helper::PrintStatus(status));

  return status;
}

// Build document index for already exist document index at bootstrap.
butil::Status DocumentIndexManager::BuildDocumentIndexOnly(DocumentIndexWrapperPtr document_index_wrapper,
                                                           const pb::common::RegionEpoch& /*epoch*/,
                                                           const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  int64_t start_time = Helper::TimestampMs();
  int64_t document_index_id = document_index_wrapper->Id();

  DINGO_LOG(INFO) << fmt::format("[document_index.buildonly][index_id({})][trace({})] build_document_index_only start.",
                                 document_index_id, trace);

  // Build a new document index from original data
  auto status = RebuildDocumentIndex(document_index_wrapper, fmt::format("LOAD.REBUILD-{}", trace));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.buildonly][index_id({})][trace({})] Rebuild document index failed.", document_index_id, trace);
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.buildonly][index_id({})][trace({})] Rebuild document index success, elapsed time({}ms).",
      document_index_id, trace, Helper::TimestampMs() - start_time);

  auto region = Server::GetInstance().GetRegion(document_index_wrapper->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.buildonly][region({})] Not found region.",
                                    document_index_wrapper->Id());
    return butil::Status(pb::error::EREGION_NOT_FOUND,
                         fmt::format("Not found region {}", document_index_wrapper->Id()));
  }

  if (region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    status = DocumentIndexManager::SaveDocumentIndex(document_index_wrapper, trace);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format(
          "[document_index.buildonly][index_id({})][trace({})] save document index failed, error: {}.",
          document_index_wrapper->Id(), trace, Helper::PrintStatus(status));
    }
  }

  DINGO_LOG(INFO) << fmt::format("[document_index.buildonly][index_id({})][trace({})] build_document_index_only done.",
                                 document_index_id, trace);

  return butil::Status();
}

void DocumentIndexManager::LaunchLoadOrBuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                          bool is_temp_hold_document_index, int64_t job_id,
                                                          const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  // if (is_temp_hold_document_index) {
  //   document_index_wrapper->SetIsTempHoldDocumentIndex(true);
  // }

  if (document_index_wrapper->LoadorbuildingNum() > 0) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.launch][index_id({})] Already exist loadorbuild on execute queue, job({}) trace({}).",
        document_index_wrapper->Id(), job_id, trace);
    return;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.launch][index_id({})] Launch loadorbuild document index, pending tasks({}) total running({}) "
      "job({}) trace({}).",
      document_index_wrapper->Id(), document_index_wrapper->PendingTaskNum(), GetDocumentIndexTaskRunningNum(), job_id,
      trace);

  auto task = std::make_shared<LoadOrBuildDocumentIndexTask>(document_index_wrapper, is_temp_hold_document_index,
                                                             job_id, fmt::format("{}-{}", job_id, trace));
  if (!Server::GetInstance().GetDocumentIndexManager()->ExecuteTask(document_index_wrapper->Id(), task)) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.launch][index_id({})] Launch loadorbuild document index failed, job({}) trace({})",
        document_index_wrapper->Id(), job_id, trace);
  } else {
    document_index_wrapper->IncLoadoruildingNum();
    document_index_wrapper->IncPendingTaskNum();
  }
}

void DocumentIndexManager::LaunchLoadAsyncBuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                             bool is_temp_hold_document_index, bool is_fast_load,
                                                             int64_t job_id, const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  // if (is_temp_hold_document_index) {
  //   document_index_wrapper->SetIsTempHoldDocumentIndex(true);
  // }

  if (document_index_wrapper->LoadorbuildingNum() > 0) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.launch][index_id({})] Already exist loadasyncbuild on execute queue, job({}) trace({}).",
        document_index_wrapper->Id(), job_id, trace);
    return;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.launch][index_id({})] Launch loadasyncbuild document index, pending tasks({}) total running({}) "
      "job({}) trace({}) is_fast_load({}).",
      document_index_wrapper->Id(), document_index_wrapper->PendingTaskNum(), GetDocumentIndexTaskRunningNum(), job_id,
      trace, is_fast_load);

  auto task = std::make_shared<LoadAsyncBuildDocumentIndexTask>(
      document_index_wrapper, is_temp_hold_document_index, is_fast_load, job_id, fmt::format("{}-{}", job_id, trace));

  bool ret = false;
  if (is_fast_load) {
    ret = Server::GetInstance().GetDocumentIndexManager()->ExecuteTaskFast(document_index_wrapper->Id(), task);
  } else {
    ret = Server::GetInstance().GetDocumentIndexManager()->ExecuteTask(document_index_wrapper->Id(), task);
  }

  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.launch][index_id({})] Launch loadasyncbuild document index failed, job({}) trace({}) "
        "is_fast_load "
        "({})",
        document_index_wrapper->Id(), job_id, trace, is_fast_load);
  } else {
    document_index_wrapper->IncLoadoruildingNum();
    document_index_wrapper->IncPendingTaskNum();
  }
}

// Parallel load document index at server bootstrap.
butil::Status DocumentIndexManager::ParallelLoadOrBuildDocumentIndex(std::vector<store::RegionPtr> regions,
                                                                     int concurrency, const std::string& trace) {
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
      auto document_index_wrapper = region->DocumentIndexWrapper();

      int64_t document_index_id = document_index_wrapper->Id();
      LOG(INFO) << fmt::format("Init load region {} document index", document_index_id);

      auto status =
          DocumentIndexManager::LoadOrBuildDocumentIndex(document_index_wrapper, region->Epoch(), param->trace);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("Load region {} document index failed, ", document_index_id);
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
      return butil::Status(pb::error::EINTERNAL, "Load or build document index failed.");
    }
  }

  return butil::Status();
}

// Replay document index from WAL
butil::Status DocumentIndexManager::ReplayWalToDocumentIndex(DocumentIndexPtr document_index, int64_t start_log_id,
                                                             int64_t end_log_id) {
  assert(document_index != nullptr);

  if (start_log_id >= end_log_id) {
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format("[document_index.replaywal][index_id({})] replay wal log({}-{})", document_index->Id(),
                                 start_log_id, end_log_id);

  int64_t start_time = Helper::TimestampMs();
  auto raft_kv_engine = Server::GetInstance().GetRaftStoreEngine();
  auto node = raft_kv_engine->GetNode(document_index->Id());
  if (node == nullptr) {
    return butil::Status(pb::error::Errno::ERAFT_NOT_FOUND, fmt::format("Not found node {}", document_index->Id()));
  }

  auto log_storage = Server::GetInstance().GetLogStorageManager()->GetLogStorage(document_index->Id());
  if (log_storage == nullptr) {
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("Not found log stroage {}", document_index->Id()));
  }

  int64_t min_document_id = 0, max_document_id = 0;
  DocumentCodec::DecodeRangeToDocumentId(false, document_index->Range(false), min_document_id, max_document_id);

  std::vector<pb::common::DocumentWithId> documents;
  documents.reserve(Constant::kBuildDocumentIndexBatchSize);
  std::vector<int64_t> ids;
  ids.reserve(Constant::kBuildDocumentIndexBatchSize);

  int64_t last_log_id = document_index->ApplyLogId();
  auto log_entrys = log_storage->GetEntrys(start_log_id, end_log_id);
  for (const auto& log_entry : log_entrys) {
    auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
    butil::IOBufAsZeroCopyInputStream wrapper(log_entry->data);
    CHECK(raft_cmd->ParseFromZeroCopyStream(&wrapper));
    for (auto& request : *raft_cmd->mutable_requests()) {
      switch (request.cmd_type()) {
        case pb::raft::DOCUMENT_ADD: {
          if (!ids.empty()) {
            document_index->Delete(ids);
            ids.clear();
          }

          for (auto& document : *request.mutable_document_add()->mutable_documents()) {
            if (document.id() >= min_document_id && document.id() < max_document_id) {
              documents.push_back(document);
            }
          }

          if (documents.size() >= Constant::kBuildDocumentIndexBatchSize) {
            document_index->Add(documents, false);
            documents.clear();
          }
          break;
        }
        case pb::raft::DOCUMENT_DELETE: {
          if (!documents.empty()) {
            document_index->Add(documents, false);
            documents.clear();
          }

          for (auto document_id : request.document_delete().ids()) {
            if (document_id >= min_document_id && document_id < max_document_id) {
              ids.push_back(document_id);
            }
          }
          if (ids.size() >= Constant::kBuildDocumentIndexBatchSize) {
            document_index->Delete(ids);
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
  if (!documents.empty()) {
    document_index->Add(documents, false);
  } else if (!ids.empty()) {
    document_index->Delete(ids);
  }

  if (last_log_id > document_index->ApplyLogId()) {
    document_index->SetApplyLogId(last_log_id);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.replaywal][index_id({})] replay wal finish, log({}-{}) last_log_id({}) document_id({}-{}) "
      "elapsed "
      "time({}ms)",
      document_index->Id(), start_log_id, end_log_id, last_log_id, min_document_id, max_document_id,
      Helper::TimestampMs() - start_time);

  return butil::Status();
}

// Build document index with original all data.
DocumentIndexPtr DocumentIndexManager::BuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                          const std::string& trace) {
  assert(document_index_wrapper != nullptr);
  int64_t document_index_id = document_index_wrapper->Id();
  auto region = Server::GetInstance().GetRegion(document_index_id);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.build][index_id({})][trace({})] not found region.",
                                    document_index_id, trace);
    return nullptr;
  }

  auto range = region->Range(false);
  butil::Status status;

  auto document_index_path =
      DocumentIndexSnapshotManager::GetSnapshotPath(document_index_id, region->Epoch().version());
  if (document_index_path.empty()) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.build][index_id({})][trace({})] get document index path failed.",
                                    document_index_id, trace);
    return nullptr;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.build][index_id({})][trace({})] Build document index, range: {}, path:({})", document_index_id,
      trace, Helper::RangeToString(range), document_index_path);
  auto document_index = DocumentIndexFactory::LoadOrCreateIndex(
      document_index_id, document_index_path, document_index_wrapper->IndexParameter(), region->Epoch(), range, status);
  if (!document_index) {
    DINGO_LOG(WARNING) << fmt::format(
        "[document_index.build][index_id({})][trace({})] New document index failed, error_code: {}, error_msg: {}.",
        document_index_id, trace, status.error_code(), status.error_str());
    return nullptr;
  }

  if (region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    // Get last applied log id
    auto raft_store_engine = Server::GetInstance().GetRaftStoreEngine();
    if (raft_store_engine == nullptr) {
      DINGO_LOG(FATAL) << fmt::format("[document_index.build][index_id({})] raft store engine is null.",
                                      document_index_id);
    }

    auto raft_node = raft_store_engine->GetNode(document_index_id);
    if (raft_node == nullptr) {
      DINGO_LOG(ERROR) << fmt::format(
          "[document_index.build][index_id({})][trace({})] not found raft node, skip this build.", document_index_id,
          trace);
      return nullptr;
    }

    auto raft_status = raft_node->GetStatus();
    if (raft_status->known_applied_index() > 0) {
      document_index->SetApplyLogId(raft_status->known_applied_index());
    }
  }
  auto encode_range = document_index->Range(true);
  const std::string& start_key = encode_range.start_key();
  const std::string& end_key = encode_range.end_key();

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.build][index_id({})][trace({})] Build document index, range: [{}({})-{}({})) parallel: {} path: "
      "({})",
      document_index_id, trace, Helper::StringToHex(start_key), DocumentCodec::UnPackageDocumentId(start_key),
      Helper::StringToHex(end_key), DocumentCodec::UnPackageDocumentId(end_key), document_index->WriteOpParallelNum(),
      document_index_path);

  int64_t start_time = Helper::TimestampMs();
  // load document data to document index
  IteratorOptions options;
  options.upper_bound = end_key;

  auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
  auto iter = raw_engine->Reader()->NewIterator(Constant::kStoreDataCF, options);
  if (iter == nullptr) {
    DINGO_LOG(FATAL) << fmt::format("[document_index.build][index_id({})] NewIterator failed.", document_index_id);
  }

  // Note: This is iterated 2 times for the following reasons:
  // ivf_flat must train first before adding data
  // train requires full data. If you just traverse it once, it will consume a huge amount of memory.
  // This is done here to cancel the use of slower disk speed in exchange for memory usage.

  // build if need
  if (document_index->NeedTrain() && !document_index->IsTrained()) {
    auto status = TrainForBuild(document_index, iter, start_key, end_key);
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.build][index_id({})][trace({})] Train finish, elapsed_time: {}ms error: {} {}",
        document_index_id, trace, Helper::TimestampMs() - start_time, status.error_code(), status.error_cstr());
    if (!status.ok()) {
      return {};
    }
    start_time = Helper::TimestampMs();
  } else {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.build][index_id({})][trace({})] Maybe not need train, is_need_train: {} is_trained: {}",
        document_index_id, trace, document_index->NeedTrain(), document_index->IsTrained());
  }

  int64_t count = 0;
  int64_t upsert_use_time = 0;
  std::vector<pb::common::DocumentWithId> documents;
  documents.reserve(Constant::kBuildDocumentIndexBatchSize);
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    pb::common::DocumentWithId document;

    std::string key(iter->Key());
    document.set_id(DocumentCodec::DecodeDocumentIdFromEncodeKeyWithTs(key));
    std::string value(mvcc::Codec::UnPackageValue(iter->Value()));
    if (!document.mutable_document()->ParseFromString(value)) {
      DINGO_LOG(ERROR) << fmt::format(
          "[document_index.build][index_id({})][trace({})] document with id ParseFromString failed.", document_index_id,
          trace);
      continue;
    }

    if (document.document().document_data_size() <= 0) {
      DINGO_LOG(WARNING) << fmt::format("[document_index.build][index_id({})][trace({})] document values_size error.",
                                        document_index_id, trace);
      continue;
    }

    documents.push_back(document);
    if (++count % Constant::kBuildDocumentIndexBatchSize == 0) {
      int64_t upsert_start_time = Helper::TimestampMs();

      document_index->Add(documents, false);

      int32_t this_upsert_time = Helper::TimestampMs() - upsert_start_time;
      upsert_use_time += this_upsert_time;

      DINGO_LOG(INFO) << fmt::format(
          "[document_index.build][index_id({})][trace({})] Build document index progress, speed({:.3}) count({}) "
          "elapsed "
          "time({}/{}ms)",
          document_index_id, trace, static_cast<double>(this_upsert_time) / documents.size(), count, upsert_use_time,
          Helper::TimestampMs() - start_time);

      documents.clear();
      // yield, for other bthread run.
      bthread_yield();
    }
  }

  if (!documents.empty()) {
    int64_t upsert_start_time = Helper::TimestampMs();
    document_index->Add(documents, false);
    upsert_use_time += (Helper::TimestampMs() - upsert_start_time);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.build][index_id({})][trace({})] Build document index finish, parallel({}) count({}) epoch({}) "
      "range({}) "
      "elapsed time({}/{}ms)",
      document_index_id, trace, document_index->WriteOpParallelNum(), count,
      Helper::RegionEpochToString(document_index->Epoch()),
      DocumentCodec::DebugRange(false, document_index->Range(false)), upsert_use_time,
      Helper::TimestampMs() - start_time);

  return document_index;
}

void DocumentIndexManager::LaunchRebuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper, int64_t job_id,
                                                      bool is_double_check, bool is_force, bool is_clear,
                                                      const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.launch][index_id({})][trace({})] Launch rebuild document index, rebuild({}) pending tasks({}) "
      "total "
      "running({}).",
      document_index_wrapper->Id(), document_index_wrapper->RebuildingNum(), document_index_wrapper->PendingTaskNum(),
      GetDocumentIndexTaskRunningNum(), trace);

  auto task = std::make_shared<RebuildDocumentIndexTask>(document_index_wrapper, job_id, is_double_check, is_force,
                                                         is_clear, fmt::format("{}-{}", job_id, trace));
  if (!Server::GetInstance().GetDocumentIndexManager()->ExecuteTask(document_index_wrapper->Id(), task)) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.launch][index_id({})][trace({})] Launch rebuild document index failed",
        document_index_wrapper->Id(), job_id);
  } else {
    document_index_wrapper->IncRebuildingNum();
    document_index_wrapper->IncPendingTaskNum();
  }
}

void DocumentIndexManager::LaunchBuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                    bool is_temp_hold_document_index, bool is_fast_build,
                                                    int64_t job_id, const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.launch][index_id({})][trace({})] Launch build document index, rebuild({}), is_fast_build({}) "
      "pending tasks({}) total running({}).",
      document_index_wrapper->Id(), trace, document_index_wrapper->RebuildingNum(), is_fast_build,
      document_index_wrapper->PendingTaskNum(), GetDocumentIndexTaskRunningNum());

  auto task = std::make_shared<BuildDocumentIndexTask>(document_index_wrapper, is_temp_hold_document_index,
                                                       is_fast_build, job_id, fmt::format("{}-{}", job_id, trace));
  bool ret = false;
  if (is_fast_build) {
    ret = Server::GetInstance().GetDocumentIndexManager()->ExecuteTaskFast(document_index_wrapper->Id(), task);
  } else {
    ret = Server::GetInstance().GetDocumentIndexManager()->ExecuteTask(document_index_wrapper->Id(), task);
  }

  if (!ret) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.launch][index_id({})][trace({})] Launch build document index failed, is_fast_build({})",
        document_index_wrapper->Id(), trace, is_fast_build);
  } else {
    document_index_wrapper->IncRebuildingNum();
    document_index_wrapper->IncPendingTaskNum();
  }
}

// Rebuild document index
butil::Status DocumentIndexManager::RebuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                         const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  int64_t document_index_id = document_index_wrapper->Id();
  DINGO_LOG(INFO) << fmt::format("[document_index.rebuild][index_id({}_v{})][trace({})] Start rebuild document index.",
                                 document_index_id, document_index_wrapper->Version(), trace);

  int64_t start_time = Helper::TimestampMs();
  // Build document index with original data.
  auto document_index = BuildDocumentIndex(document_index_wrapper, trace);
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.rebuild][index_id({})][trace({})] Build document index failed.",
                                      document_index_id, trace);

    document_index_wrapper->SetRebuildError();

    return butil::Status(pb::error::Errno::EINTERNAL, "Build document index failed");
  }

  // Check document index is stop
  if (document_index_wrapper->IsDestoryed()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.rebuild][index_id({})][trace({})] document index is stop.",
                                      document_index_id, trace);
    document_index_wrapper->SetRebuildError();

    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.rebuild][index_id({}_v{})][trace({})] Build document index success, log_id {} elapsed time: "
      "{}ms",
      document_index_id, document_index_wrapper->Version(), trace, document_index->ApplyLogId(),
      Helper::TimestampMs() - start_time);

  bvar_document_index_rebuild_catchup_total_num << 1;
  bvar_document_index_rebuild_catchup_running_num << 1;
  DEFER(bvar_document_index_rebuild_catchup_running_num << -1;);

  auto status = CatchUpLogToDocumentIndex(document_index_wrapper, document_index, trace);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[document_index.rebuild][index_id({})][trace({})] Catch up log failed, error: {}.", document_index_id, trace,
        Helper::PrintStatus(status));
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.rebuild][index_id({}_v{})][trace({})] Rebuild document index success, elapsed time: {}ms.",
      document_index_id, document_index_wrapper->Version(), trace, Helper::TimestampMs() - start_time);

  return butil::Status();
}

butil::Status DocumentIndexManager::LoadDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                      const pb::common::RegionEpoch& epoch, const std::string& trace) {
  int64_t document_index_id = document_index_wrapper->Id();
  int64_t start_time = Helper::TimestampMs();

  // try to load document index from snapshot
  auto document_index = DocumentIndexSnapshotManager::LoadDocumentIndexSnapshot(document_index_wrapper, epoch);
  if (document_index == nullptr) {
    return butil::Status(pb::error::EDOCUMENT_INDEX_LOAD_SNAPSHOT, "load document snapshot failed");
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.load][index_id({})][trace({})] Load document index snapshot success, epoch: {} elapsed time: "
      "{}ms.",
      document_index_id, trace, Helper::RegionEpochToString(document_index->Epoch()),
      Helper::TimestampMs() - start_time);

  // catch up wal
  bvar_document_index_load_catchup_total_num << 1;
  bvar_document_index_load_catchup_running_num << 1;
  DEFER(bvar_document_index_load_catchup_running_num << -1;);

  auto status = CatchUpLogToDocumentIndex(document_index_wrapper, document_index, trace);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.load][index_id({})][trace({})] Catch up log failed, error: {}.",
                                      document_index_id, trace, Helper::PrintStatus(status));
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.load][index_id({})][trace({})] Load document index success, epoch: {} elapsed time: {}ms.",
      document_index_id, trace, Helper::RegionEpochToString(document_index->Epoch()),
      Helper::TimestampMs() - start_time);

  return butil::Status::OK();
}

butil::Status DocumentIndexManager::CatchUpLogToDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                              std::shared_ptr<DocumentIndex> document_index,
                                                              const std::string& trace) {
  assert(document_index_wrapper != nullptr);
  assert(document_index != nullptr);

  int64_t document_index_id = document_index_wrapper->Id();
  int64_t start_time = Helper::TimestampMs();

  // Get region
  auto regoin = Server::GetInstance().GetRegion(document_index_wrapper->Id());
  if (regoin == nullptr) {
    return butil::Status(pb::error::ERAFT_META_NOT_FOUND, "not found region.");
  }

  if (regoin->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    auto raft_meta = Server::GetInstance().GetRaftMeta(document_index_wrapper->Id());
    {
      start_time = Helper::TimestampMs();

      DEFER(document_index_wrapper->SetIsSwitchingDocumentIndex(false);
            bvar_document_index_catchup_latency_last_round << (Helper::TimestampMs() - start_time););

      int64_t start_log_id = document_index->ApplyLogId() + 1;
      int64_t end_log_id = raft_meta->AppliedId();
      // second ground replay wal
      auto status = ReplayWalToDocumentIndex(document_index, start_log_id, end_log_id);
      if (!status.ok()) {
        document_index_wrapper->SetRebuildError();
        return status;
      }

      DINGO_LOG(INFO) << fmt::format(
          "[document_index.catchup][index_id({})][trace({})] Catch up last-round({}-{}) success.", document_index_id,
          trace, start_log_id, end_log_id);
    }
  } else if (regoin->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
  } else {
    return butil::Status(pb::error::ERAFT_META_NOT_FOUND, "not found raft meta.");
  }
  pb::common::RegionEpoch epoch;
  pb::common::Range range;
  regoin->GetEpochAndRange(epoch, range);
  document_index->SetEpochAndRange(epoch, range);

  document_index_wrapper->UpdateDocumentIndex(document_index, trace);
  document_index_wrapper->SetRebuildSuccess();
  return butil::Status();
}

butil::Status DocumentIndexManager::SaveDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                      const std::string& trace) {
  assert(document_index_wrapper != nullptr);
  int64_t start_time = Helper::TimestampMs();

  // Check document index is stop
  if (document_index_wrapper->IsDestoryed()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.save][index_id({})][trace({})] document index is stop.",
                                      document_index_wrapper->Id(), trace);
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format("[document_index.save][index_id({}_v{})][trace({})] Save document index.",
                                 document_index_wrapper->Id(), document_index_wrapper->Version(), trace);

  int64_t snapshot_log_id = 0;
  auto status = DocumentIndexSnapshotManager::SaveDocumentIndexSnapshot(document_index_wrapper, snapshot_log_id);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.save][index_id({})][trace({})] Save document index snapshot failed, errno: {}, errstr: {}",
        document_index_wrapper->Id(), trace, status.error_code(), status.error_str());
    return status;
  } else {
    document_index_wrapper->SetSnapshotLogId(snapshot_log_id);
  }

  // Update document index status NORMAL
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.save][index_id({}_v{})][trace({})] Save document index success, elapsed time({}ms)",
      document_index_wrapper->Id(), document_index_wrapper->Version(), trace, Helper::TimestampMs() - start_time);

  // Check document index is stop
  if (document_index_wrapper->IsDestoryed()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.save][index_id({}_v{})][trace({})] document index is stop.",
                                      document_index_wrapper->Id(), document_index_wrapper->Version(), trace);
    return butil::Status();
  }

  return butil::Status::OK();
}

void DocumentIndexManager::LaunchSaveDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                   const std::string& trace) {
  assert(document_index_wrapper != nullptr);
  auto region = Server::GetInstance().GetRegion(document_index_wrapper->Id());
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.launch][index_id({})][trace({})] Not found region.",
                                    document_index_wrapper->Id(), trace);
    return;
  }
  if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.launch][index_id({})][trace({})] rocks engine not need save document index.",
        document_index_wrapper->Id(), trace);
    return;
  }
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.launch][index_id({})][trace({})] Launch save document index, pending tasks({}) total "
      "running({}).",
      document_index_wrapper->Id(), trace, document_index_wrapper->PendingTaskNum(), GetDocumentIndexTaskRunningNum());

  auto task = std::make_shared<SaveDocumentIndexTask>(document_index_wrapper, trace);
  if (!Server::GetInstance().GetDocumentIndexManager()->ExecuteTask(document_index_wrapper->Id(), task)) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.launch][index_id({})][trace({})] Launch save document index failed",
        document_index_wrapper->Id(), trace);
  } else {
    document_index_wrapper->IncPendingTaskNum();
    document_index_wrapper->IncSavingNum();
  }
}

butil::Status DocumentIndexManager::ScrubDocumentIndex() {
  auto regions = Server::GetInstance().GetAllAliveRegion();
  if (regions.empty()) {
    DINGO_LOG(INFO) << "[document_index.scrub][index_id()] No alive region, skip scrub document index";
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << "[document_index.scrub][index_id()] Scrub document index start, alive region_count is "
                  << regions.size();

  for (const auto& region : regions) {
    int64_t document_index_id = region->Id();
    if (region->State() != pb::common::NORMAL) {
      DINGO_LOG(INFO) << fmt::format("[document_index.scrub][index_id({})] region state is not normal, dont't scrub.",
                                     document_index_id);
      continue;
    }
    auto document_index_wrapper = region->DocumentIndexWrapper();
    if (!document_index_wrapper->IsReady()) {
      DINGO_LOG(INFO) << fmt::format("[document_index.scrub][index_id({})] document index is not ready, dont't scrub.",
                                     document_index_id);
      continue;
    }
    if (document_index_wrapper->IsDestoryed()) {
      DINGO_LOG(INFO) << fmt::format("[document_index.scrub][index_id({})] document index is stop, dont't scrub.",
                                     document_index_id);
      continue;
    }

    bool need_rebuild = document_index_wrapper->NeedToRebuild();
    if (need_rebuild && document_index_wrapper->RebuildingNum() == 0) {
      DINGO_LOG(INFO) << fmt::format("[document_index.scrub][index_id({})] need rebuild, do rebuild document index.",
                                     document_index_id);
      LaunchRebuildDocumentIndex(document_index_wrapper, 0, true, false, false, "from scrub");
      continue;
    }

    std::string trace;
    bool need_save = document_index_wrapper->NeedToSave(trace);
    if (need_save && document_index_wrapper->RebuildingNum() == 0 && document_index_wrapper->SavingNum() < 128) {
      DINGO_LOG(INFO) << fmt::format("[document_index.scrub][index_id({})] need save, trace: {}.", document_index_id,
                                     trace);

      LaunchSaveDocumentIndex(document_index_wrapper, fmt::format("scrub-{}", trace));
    }
  }

  return butil::Status::OK();
}

butil::Status DocumentIndexManager::TrainForBuild(std::shared_ptr<DocumentIndex> /*document_index*/,
                                                  std::shared_ptr<Iterator> /*iter*/, const std::string& /*start_key*/,
                                                  [[maybe_unused]] const std::string& end_key) {
  // std::vector<float> train_documents;
  // train_documents.reserve(100000 * document_index->GetDimension());  // todo opt
  // for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
  //   pb::common::DocumentWithId document;

  //   std::string value(iter->Value());
  //   if (!document.mutable_document()->ParseFromString(value)) {
  //     DINGO_LOG(WARNING) << fmt::format("[document_index.build][index_id({})] document with id ParseFromString
  //     failed.",
  //                                       document_index->Id());
  //     continue;
  //   }

  //   if (document.document().float_values_size() <= 0) {
  //     DINGO_LOG(WARNING) << fmt::format("[document_index.build][index_id({})] document values_size error.",
  //                                       document_index->Id());
  //     continue;
  //   }

  //   train_documents.insert(train_documents.end(), document.document().float_values().begin(),
  //                          document.document().float_values().end());
  // }

  // if (!train_documents.empty()) {
  //   auto status = document_index->TrainByParallel(train_documents);
  //   if (!status.ok()) {
  //     DINGO_LOG(ERROR) << fmt::format("[document_index.build][index_id({})] train failed, error: {}",
  //                                     document_index->Id(), status.error_str());
  //     return status;
  //   }
  // }

  return butil::Status::OK();
}

bool DocumentIndexManager::ExecuteTask(int64_t region_id, TaskRunnablePtr task) {
  if (background_workers_ == nullptr) {
    return false;
  }

  return background_workers_->ExecuteHashByRegionId(region_id, task);
}

bool DocumentIndexManager::ExecuteTaskFast(int64_t region_id, TaskRunnablePtr task) {
  if (fast_background_workers_ == nullptr) {
    return false;
  }

  return fast_background_workers_->ExecuteHashByRegionId(region_id, task);
}

std::vector<std::vector<std::string>> DocumentIndexManager::GetPendingTaskTrace() {
  if (background_workers_ == nullptr) {
    return {};
  }

  return background_workers_->GetPendingTaskTrace();
}

uint64_t DocumentIndexManager::GetBackgroundPendingTaskCount() {
  if (background_workers_ == nullptr) {
    return 0;
  }

  return background_workers_->PendingTaskCount();
}

}  // namespace dingodb

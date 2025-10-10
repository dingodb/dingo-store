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
#include "fmt/core.h"
#include "glog/logging.h"
#include "meta/store_meta_manager.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/file_service.pb.h"
#include "proto/node.pb.h"
#include "proto/raft.pb.h"
#include "server/server.h"
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
#include "vector/vector_index_utils.h"
#endif

namespace dingodb {

DEFINE_int64(document_catchup_log_min_gap, 8, "catch up log min gap");
BRPC_VALIDATE_GFLAG(document_catchup_log_min_gap, brpc::PositiveInteger);

DEFINE_int32(document_background_worker_num, 16, "document index background worker num");
BRPC_VALIDATE_GFLAG(document_background_worker_num, brpc::PositiveInteger);

DEFINE_int32(document_fast_background_worker_num, 8, "document index fast background worker num");
BRPC_VALIDATE_GFLAG(document_fast_background_worker_num, brpc::PositiveInteger);

DEFINE_int64(document_max_background_task_count, 32, "document index max background task count");
BRPC_VALIDATE_GFLAG(document_max_background_task_count, brpc::PositiveInteger);

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
DEFINE_int32(document_vector_scalar_search_background_worker_num, 10,
             "document index vector scalar search background worker num");
BRPC_VALIDATE_GFLAG(document_vector_scalar_search_background_worker_num, brpc::PositiveInteger);

DEFINE_int32(document_vector_scalar_search_background_pending_num, 10,
             "document index vector scalar search background pending num");
BRPC_VALIDATE_GFLAG(document_vector_scalar_search_background_pending_num, brpc::PositiveInteger);

DEFINE_bool(use_pthread_document_vector_scalar_search_background_worker_set, true,
            "use pthread document vector scalar search background worker set");
BRPC_VALIDATE_GFLAG(use_pthread_document_vector_scalar_search_background_worker_set, brpc::PassValidate);
#endif

std::string RebuildDocumentIndexTask::Trace() {
  return fmt::format("[document_index.rebuild][id({}).start_time({}).job_id({})] {}", document_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void RebuildDocumentIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.rebuild][id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) wait_time({}).",
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
        "[document_index.rebuild][id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}ms).",
        document_index_wrapper_->Id(), trace_, document_index_wrapper_->RebuildingNum(),
        document_index_wrapper_->PendingTaskNum(), DocumentIndexManager::GetDocumentIndexRebuildTaskRunningNum(),
        DocumentIndexManager::GetDocumentIndexTaskRunningNum(), Helper::TimestampMs() - start_time_);
  });

  auto region = Server::GetInstance().GetRegion(document_index_wrapper_->Id());
  if (region == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.rebuild][id({})][trace({})] Not found region.",
                                      document_index_wrapper_->Id(), trace_);
    return;
  }
  auto state = region->State();
  if (state == pb::common::StoreRegionState::STANDBY || state == pb::common::StoreRegionState::DELETING ||
      state == pb::common::StoreRegionState::DELETED || state == pb::common::StoreRegionState::ORPHAN ||
      state == pb::common::StoreRegionState::TOMBSTONE) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.rebuild][id({})][trace({})] region state({}) not match.",
                                      document_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state));
    return;
  }

  if (Helper::InvalidRange(region->Range(false))) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.rebuild][id({})][trace({})] region range invalid.",
                                      document_index_wrapper_->Id(), trace_);
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuild document index {}", region->Id()));

  if (document_index_wrapper_->IsDestoryed()) {
    DINGO_LOG(INFO) << fmt::format("[document_index.rebuild][id({})][trace({})] document index is stop, gave up.",
                                   document_index_wrapper_->Id(), trace_);
    return;
  }

  if (document_index_wrapper_->IsOwnReady() &&
      document_index_wrapper_->LastBuildEpochVersion() >= region->Epoch().version()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Already own document index {}", region->Id()));
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.rebuild][id({})][trace({})] document index version is lastest, gave up.",
        document_index_wrapper_->Id(), trace_);
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuilding document index {}", region->Id()));

  // document_index_wrapper_->SetIsTempHoldDocumentIndex(true);
  auto status = DocumentIndexManager::RebuildDocumentIndex(document_index_wrapper_, fmt::format("REBUILD-{}", trace_));
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuilded document index {}", region->Id()));
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.rebuild][id({}_v{})][trace({})] rebuild document index fail, error: {}.",
        document_index_wrapper_->Id(), document_index_wrapper_->Version(), trace_, Helper::PrintStatus(status));
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Rebuilded document index {}", region->Id()));

  if (is_clear_) {
    if (!DocumentIndexWrapper::IsPermanentHoldDocumentIndex(document_index_wrapper_->Id())) {
      document_index_wrapper_->ClearDocumentIndex(trace_);
    }

    auto store_region_meta = GET_STORE_REGION_META;
    store_region_meta->UpdateTemporaryDisableChange(region, false);
  }
}

std::string LoadOrBuildDocumentIndexTask::Trace() {
  return fmt::format("[document_index.loadorbuild][id({}).start_time({}).job_id({})] {}", document_index_wrapper_->Id(),
                     Helper::FormatMsTime(start_time_), job_id_, trace_);
}

void LoadOrBuildDocumentIndexTask::Run() {
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.loadorbuild][id({})][trace({})] run, pending tasks({}/{}) total running({}/{}) "
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
        "[document_index.loadorbuild][id({})][trace({})] run finish, pending tasks({}/{}) total running({}/{}) "
        "run_time({}ms).",
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
    DINGO_LOG(WARNING) << fmt::format("[document_index.loadorbuild][id({})][trace({})] region state({}) not match.",
                                      document_index_wrapper_->Id(), trace_, pb::common::StoreRegionState_Name(state));
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuild document index {}", region->Id()));

  if (document_index_wrapper_->IsDestoryed()) {
    DINGO_LOG(INFO) << fmt::format("[document_index.loadorbuild][id({})][trace({})] document index is stop, gave up.",
                                   document_index_wrapper_->Id(), trace_);
    return;
  }

  if (document_index_wrapper_->IsOwnReady() &&
      document_index_wrapper_->LastBuildEpochVersion() >= region->Epoch().version()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Already own document index {}", region->Id()));
    DINGO_LOG(INFO) << fmt::format("[document_index.loadorbuild][id({})][trace({})] document index is ready, gave up.",
                                   document_index_wrapper_->Id(), trace_);
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuilding document index {}", region->Id()));

  auto status = DocumentIndexManager::LoadOrBuildDocumentIndex(document_index_wrapper_, region->Epoch(), trace_);
  if (!status.ok()) {
    ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuilded document index {} fail", region->Id()));
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.load][id({}_v{})][trace({})] load or build document index fail, error {}",
        document_index_wrapper_->Id(), document_index_wrapper_->Version(), trace_, status.error_str());
    return;
  }

  ADD_REGION_CHANGE_RECORD_TIMEPOINT(job_id_, fmt::format("Loadorbuilded document index {}", region->Id()));
}

bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_task_running_num(
    "dingo_document_index_task_running_num");
bvar::Adder<uint64_t> DocumentIndexManager::bvar_document_index_rebuild_task_running_num(
    "dingo_document_index_rebuild_task_running_num");
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
  workers_ = ExecqWorkerSet::New("document_mgr_background", FLAGS_document_background_worker_num, 0);
  if (!workers_->Init()) {
    DINGO_LOG(ERROR) << "Init document index manager background worker set fail!";
    return false;
  }

  fast_workers_ = ExecqWorkerSet::New("document_mgr_fast_background", FLAGS_document_fast_background_worker_num, 0);
  if (!fast_workers_->Init()) {
    DINGO_LOG(ERROR) << "Init document index manager fast background worker set fail!";
    return false;
  }

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  // only for vector index scalar search with document
  if (use_document_purpose_type_ == UseDocumentPurposeType::kVectorIndexModule) {
    vector_scalar_search_workers_ = SimpleWorkerSet::New(
        "document_mgr_vector_scalar_search_background", FLAGS_document_vector_scalar_search_background_worker_num,
        FLAGS_document_vector_scalar_search_background_pending_num,
        FLAGS_use_pthread_document_vector_scalar_search_background_worker_set, false);
    if (!vector_scalar_search_workers_->Init()) {
      DINGO_LOG(ERROR) << "Init document index manager vector scalar search background worker set fail!";
      return false;
    }
  }
#endif

  return true;
}

void DocumentIndexManager::Destroy() {
  if (workers_ != nullptr) {
    workers_->Destroy();
  }
  if (fast_workers_ != nullptr) {
    fast_workers_->Destroy();
  }
}

// Load document index for already exist document index at bootstrap.
butil::Status DocumentIndexManager::LoadOrBuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                             const pb::common::RegionEpoch& epoch,
                                                             const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  int64_t start_time = Helper::TimestampMs();
  int64_t document_index_id = document_index_wrapper->Id();

  // try to load document index
  auto status = LoadDocumentIndex(document_index_wrapper, epoch, fmt::format("LOAD.INDEX-{}", trace));
  if (status.ok()) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.loadorbuild][id({})][trace({})] Load document index from success, elapsed time({}ms)",
        document_index_id, trace, Helper::TimestampMs() - start_time);
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format("[document_index.loadorbuild][id({})][trace({})] Load document index fail, error: {}.",
                                 document_index_id, trace, Helper::PrintStatus(status));

  // Build a new document index from original data
  status = RebuildDocumentIndex(document_index_wrapper, fmt::format("LOAD.REBUILD-{}", trace));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.loadorbuild][id({})][trace({})] Rebuild document index fail.",
                                    document_index_id, trace);
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.loadorbuild][id({})][trace({})] Rebuild document index success, elapsed time({}ms).",
      document_index_id, trace, Helper::TimestampMs() - start_time);

  return butil::Status();
}

void DocumentIndexManager::LaunchLoadOrBuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                          bool is_temp_hold_document_index, bool is_fast,
                                                          int64_t job_id, const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  if (document_index_wrapper->LoadorbuildingNum() > 0) {
    DINGO_LOG(INFO) << fmt::format(
        "[document_index.launch][id({})] Already exist loadorbuild on execute queue, job({}) trace({}).",
        document_index_wrapper->Id(), job_id, trace);
    return;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.launch][id({})] Launch loadorbuild document index, pending tasks({}) total running({}) "
      "job({}) trace({}).",
      document_index_wrapper->Id(), document_index_wrapper->PendingTaskNum(), GetDocumentIndexTaskRunningNum(), job_id,
      trace);

  auto task = std::make_shared<LoadOrBuildDocumentIndexTask>(document_index_wrapper, is_temp_hold_document_index,
                                                             job_id, fmt::format("{}-{}", job_id, trace));
  if (!DocumentIndexManager::ExecuteTask(document_index_wrapper->Id(), task, is_fast)) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.launch][id({})] Launch loadorbuild document index fail, job({}) trace({})",
        document_index_wrapper->Id(), job_id, trace);
  } else {
    document_index_wrapper->IncLoadoruildingNum();
    document_index_wrapper->IncPendingTaskNum();
  }
}

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
// Replay log to document index.
butil::Status DocumentIndexManager::ReplayWalToDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                             DocumentIndexPtr document_index, int64_t start_log_id,
                                                             int64_t end_log_id) {
#else
// Replay document index from WAL
butil::Status DocumentIndexManager::ReplayWalToDocumentIndex(DocumentIndexPtr document_index, int64_t start_log_id,
                                                             int64_t end_log_id) {
#endif
  assert(document_index != nullptr);

  if (start_log_id >= end_log_id) {
    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format("[document_index.replaywal][id({})] replay wal log({}-{})", document_index->Id(),
                                 start_log_id, end_log_id);

  int64_t start_time = Helper::TimestampMs();
  auto raft_engine = Server::GetInstance().GetRaftStoreEngine();
  auto node = raft_engine->GetNode(document_index->Id());
  CHECK(node != nullptr) << fmt::format("[document_index.replaywal][id({})] Not found raft node.",
                                        document_index->Id());

  auto log_storage = Server::GetInstance().GetRaftLogStorage();

  int64_t min_document_id = 0, max_document_id = 0;
  DocumentCodec::DecodeRangeToDocumentId(false, document_index->Range(false), min_document_id, max_document_id);

  std::vector<pb::common::DocumentWithId> documents;
  documents.reserve(Constant::kBuildDocumentIndexBatchSize);
  std::vector<int64_t> ids;
  ids.reserve(Constant::kBuildDocumentIndexBatchSize);

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  store::RegionPtr region = Server::GetInstance().GetRegion(document_index->Id());
  const pb::common::RegionDefinition& definition = region->Definition();
  bool enable_scalar_speed_up_with_document = false;
  if (definition.index_parameter().vector_index_parameter().enable_scalar_speed_up_with_document() &&
      region->DocumentIndexWrapper()) {
    enable_scalar_speed_up_with_document = true;
  }

  UseDocumentPurposeType use_document_purpose_type = document_index_wrapper->GetUseDocumentPurposeType();

  bool is_first_document_add = false;
  bool is_first_document_delete = false;
  bool is_first_vector_add = false;
  bool is_first_vector_delete = false;

#endif

  int64_t last_log_id = document_index->ApplyLogId();
  auto log_entrys = log_storage->GetDataEntries(document_index->Id(), start_log_id, end_log_id);
  for (const auto& log_entry : log_entrys) {
    auto raft_cmd = std::make_shared<pb::raft::RaftCmdRequest>();
    CHECK(raft_cmd->ParseFromString(log_entry->out_data));
    for (auto& request : *raft_cmd->mutable_requests()) {
      switch (request.cmd_type()) {
        case pb::raft::DOCUMENT_ADD: {
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
          if (!is_first_document_add) {
            is_first_document_add = true;
            if (use_document_purpose_type != UseDocumentPurposeType::kDocumentModule) {
              DINGO_LOG(FATAL) << fmt::format(
                  "[document_index.replaywal][id({})] first document add, document index use purpose should be "
                  "UseDocumentPurposeType::kDocumentModule, but is : {}, maybe some thing wrong.",
                  document_index->Id(), static_cast<int>(use_document_purpose_type));
            }
          }
#endif
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
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
          if (!is_first_document_delete) {
            is_first_document_delete = true;
            if (use_document_purpose_type != UseDocumentPurposeType::kDocumentModule) {
              DINGO_LOG(FATAL) << fmt::format(
                  "[document_index.replaywal][id({})] first document delete, document index use purpose should be "
                  "UseDocumentPurposeType::kDocumentModule, but is : {}, maybe some thing wrong.",
                  document_index->Id(), static_cast<int>(use_document_purpose_type));
            }
          }
#endif
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
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
        case pb::raft::VECTOR_ADD: {
          if (!is_first_vector_add) {
            is_first_vector_add = true;
            if (use_document_purpose_type != UseDocumentPurposeType::kVectorIndexModule) {
              DINGO_LOG(FATAL) << fmt::format(
                  "[document_index.replaywal][id({})] first vector add, document index use purpose should be "
                  "UseDocumentPurposeType::kVectorIndexModule, but is : {}, "
                  "maybe "
                  "some thing wrong.",
                  document_index->Id(), static_cast<int>(use_document_purpose_type));
            }
          }
          if (enable_scalar_speed_up_with_document) {
            if (!ids.empty()) {
              document_index->Delete(ids);
              ids.clear();
            }

            for (auto& vector : *request.mutable_vector_add()->mutable_vectors()) {
              if (vector.id() >= min_document_id && vector.id() < max_document_id) {
                std::vector<std::pair<std::string, pb::common::ScalarValue>> scalar_key_value_pairs;
                const pb::common::ScalarSchema& scalar_schema = region->ScalarSchema();
                VectorIndexUtils::SplitVectorScalarData(scalar_schema, vector.scalar_data(), scalar_key_value_pairs);

                // vector scalar data use document
                if (!scalar_key_value_pairs.empty()) {
                  pb::common::DocumentWithId document_with_id;
                  document_with_id.set_id(vector.id());
                  for (const auto& [scalar_key, scalar_value] : scalar_key_value_pairs) {
                    pb::common::DocumentValue document_value;
                    document_value.set_field_type(scalar_value.field_type());
                    document_value.mutable_field_value()->CopyFrom(scalar_value.fields(0));
                    document_with_id.mutable_document()->mutable_document_data()->insert(
                        std::make_pair(scalar_key, document_value));
                  }

                  documents.push_back(std::move(document_with_id));
                }  // if (!scalar_key_value_pairs.empty() && region->DocumentIndexWrapper()) {
              }
            }

            if (documents.size() >= Constant::kBuildDocumentIndexBatchSize) {
              document_index->Add(documents, false);
              documents.clear();
            }
          }  // if (enable_scalar_speed_up_with_document) {

          break;
        }
        case pb::raft::VECTOR_DELETE: {
          if (!is_first_vector_delete) {
            is_first_vector_delete = true;
            if (use_document_purpose_type != UseDocumentPurposeType::kVectorIndexModule) {
              DINGO_LOG(FATAL) << fmt::format(
                  "[document_index.replaywal][id({})] first vector delete, document index use purpose should be "
                  "UseDocumentPurposeType::kVectorIndexModule, but is : {}, "
                  "maybe "
                  "some thing wrong.",
                  document_index->Id(), static_cast<int>(use_document_purpose_type));
            }
          }
          if (enable_scalar_speed_up_with_document) {
            if (!documents.empty()) {
              document_index->Add(documents, false);
              documents.clear();
            }

            for (auto vector_id : request.vector_delete().ids()) {
              if (vector_id >= min_document_id && vector_id < max_document_id) {
                ids.push_back(vector_id);
              }
            }

            if (ids.size() >= Constant::kBuildDocumentIndexBatchSize) {
              document_index->Delete(ids);
              ids.clear();
            }
          }  // if (enable_scalar_speed_up_with_document) {
          break;
        }
#endif
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
      "[document_index.replaywal][id({})] replay wal finish, log({}-{}) last_log_id({}) document_id({}-{}) "
      "elapsed time({}ms)",
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
    DINGO_LOG(ERROR) << fmt::format("[document_index.build][id({})][trace({})] not found region.", document_index_id,
                                    trace);
    return nullptr;
  }

  auto range = region->Range(false);
  auto epoch = region->Epoch();
  auto document_index_path = DocumentIndex::GetIndexPath(document_index_id, epoch);

  DINGO_LOG(INFO) << fmt::format("[document_index.build][id({})][trace({})] Build document index, range: {}, path: {}",
                                 document_index_id, trace, Helper::RangeToString(range), document_index_path);

  auto document_index = DocumentIndexFactory::CreateIndex(document_index_id, document_index_path,
                                                          document_index_wrapper->IndexParameter(), epoch, range, true);
  if (!document_index) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.build][id({})][trace({})] create document index fail.",
                                    document_index_id, trace);
    return nullptr;
  }

  if (region->GetStoreEngineType() == pb::common::STORE_ENG_RAFT_STORE) {
    // Get last applied log id
    auto raft_meta = Server::GetInstance().GetRaftMeta(document_index_wrapper->Id());
    if (raft_meta == nullptr) {
      DINGO_LOG(ERROR) << fmt::format("[document_index.build][id({})][trace({})] get raft meta fail.",
                                      document_index_id, trace);
      return nullptr;
    }

    document_index->SetApplyLogId(raft_meta->AppliedId());
  }

  auto encode_range = document_index->Range(true);
  const std::string& start_key = encode_range.start_key();
  const std::string& end_key = encode_range.end_key();

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.build][id({})][trace({})] Build document index, range: [{}({})-{}({})) parallel: {} path: "
      "({})",
      document_index_id, trace, Helper::StringToHex(start_key), DocumentCodec::UnPackageDocumentId(start_key),
      Helper::StringToHex(end_key), DocumentCodec::UnPackageDocumentId(end_key), document_index->WriteOpParallelNum(),
      document_index_path);

  int64_t start_time = Helper::TimestampMs();
  // load document data to document index
  IteratorOptions options;
  options.upper_bound = end_key;

  auto raw_engine = Server::GetInstance().GetRawEngine(region->GetRawEngineType());
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  std::string which_cf;
  auto use_document_purpose_type = document_index_wrapper->GetUseDocumentPurposeType();
  switch (use_document_purpose_type) {
    case UseDocumentPurposeType::kDocumentModule: {
      which_cf = Constant::kStoreDataCF;
      break;
    }
    case UseDocumentPurposeType::kVectorIndexModule: {
      which_cf = Constant::kVectorScalarUseDocumentCF;
      break;
    }
    case UseDocumentPurposeType::kNone:
      [[fallthrough]];
    default: {
      DINGO_LOG(FATAL) << fmt::format(
          "[document_index.build][id({})][trace({})] use_document_purpose_type not set : {}, please set it first.",
          document_index_id, trace, static_cast<int>(use_document_purpose_type));
      return nullptr;
    }
  }
  auto iter = raw_engine->Reader()->NewIterator(which_cf, options);
#else
  auto iter = raw_engine->Reader()->NewIterator(Constant::kStoreDataCF, options);
#endif

  CHECK(iter != nullptr) << fmt::format("[document_index.build][id({})] NewIterator fail.", document_index_id);

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
          "[document_index.build][id({})][trace({})] document with id ParseFromString fail.", document_index_id, trace);
      continue;
    }

    if (document.document().document_data_size() <= 0) {
      DINGO_LOG(WARNING) << fmt::format("[document_index.build][id({})][trace({})] document values_size error.",
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
          "[document_index.build][id({})][trace({})] Build document index progress, speed({:.3}) count({}) "
          "elapsed time({}/{}ms)",
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
      "[document_index.build][id({})][trace({})] Build document index finish, parallel({}) count({}) epoch({}) "
      "range({}) elapsed time({}/{}ms)",
      document_index_id, trace, document_index->WriteOpParallelNum(), count,
      Helper::RegionEpochToString(document_index->Epoch()),
      DocumentCodec::DebugRange(false, document_index->Range(false)), upsert_use_time,
      Helper::TimestampMs() - start_time);

  return document_index;
}

void DocumentIndexManager::LaunchRebuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper, int64_t job_id,
                                                      bool is_clear, const std::string& trace) {
  assert(document_index_wrapper != nullptr);

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.launch][id({})][trace({})] Launch rebuild document index, rebuild({}) pending tasks({}) "
      "total running({}).",
      document_index_wrapper->Id(), document_index_wrapper->RebuildingNum(), document_index_wrapper->PendingTaskNum(),
      GetDocumentIndexTaskRunningNum(), trace);

  auto task = std::make_shared<RebuildDocumentIndexTask>(document_index_wrapper, job_id, is_clear,
                                                         fmt::format("{}-{}", job_id, trace));
  if (!DocumentIndexManager::ExecuteTask(document_index_wrapper->Id(), task, false)) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.launch][id({})][trace({})] Launch rebuild document index fail",
                                    document_index_wrapper->Id(), job_id);
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
  DINGO_LOG(INFO) << fmt::format("[document_index.rebuild][id({}_v{})][trace({})] Start rebuild document index.",
                                 document_index_id, document_index_wrapper->Version(), trace);

  int64_t start_time = Helper::TimestampMs();
  // Build document index with original data.
  auto document_index = BuildDocumentIndex(document_index_wrapper, trace);
  if (document_index == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.rebuild][id({})][trace({})] Build document index fail.",
                                    document_index_id, trace);

    document_index_wrapper->SetRebuildError();

    return butil::Status(pb::error::Errno::EINTERNAL, "Build document index fail");
  }

  // Check document index is stop
  if (document_index_wrapper->IsDestoryed()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.rebuild][id({})][trace({})] document index is stop.",
                                      document_index_id, trace);
    document_index_wrapper->SetRebuildError();

    return butil::Status();
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.rebuild][id({}_v{})][trace({})] Build document index success, log_id({}) elapsed "
      "time({}ms).",
      document_index_id, document_index_wrapper->Version(), trace, document_index->ApplyLogId(),
      Helper::TimestampMs() - start_time);

  bvar_document_index_rebuild_catchup_total_num << 1;
  bvar_document_index_rebuild_catchup_running_num << 1;
  DEFER(bvar_document_index_rebuild_catchup_running_num << -1;);

  auto status = CatchUpLogToDocumentIndex(document_index_wrapper, document_index, trace);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.rebuild][id({})][trace({})] Catch up log fail, error: {}.",
                                    document_index_id, trace, Helper::PrintStatus(status));
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.rebuild][id({}_v{})][trace({})] Rebuild document index success, elapsed time({}ms).",
      document_index_id, document_index_wrapper->Version(), trace, Helper::TimestampMs() - start_time);

  return butil::Status();
}

butil::Status DocumentIndexManager::LoadDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                      const pb::common::RegionEpoch& epoch, const std::string& trace) {
  int64_t document_index_id = document_index_wrapper->Id();
  int64_t start_time = Helper::TimestampMs();

  // try to load document index
  auto document_index = DocumentIndex::LoadIndex(document_index_id, epoch, document_index_wrapper->IndexParameter());
  if (document_index == nullptr) {
    return butil::Status(pb::error::EDOCUMENT_INDEX_LOAD_SNAPSHOT, "load document index fail");
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.load][id({})][trace({})] Load document index success, epoch({}) elapsed tim({}ms).",
      document_index_id, trace, Helper::RegionEpochToString(document_index->Epoch()),
      Helper::TimestampMs() - start_time);

  // catch up wal
  bvar_document_index_load_catchup_total_num << 1;
  bvar_document_index_load_catchup_running_num << 1;
  DEFER(bvar_document_index_load_catchup_running_num << -1;);

  auto status = CatchUpLogToDocumentIndex(document_index_wrapper, document_index, trace);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.load][id({})][trace({})] Catch up log fail, error: {}.",
                                      document_index_id, trace, Helper::PrintStatus(status));
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.load][id({})][trace({})] Load document index success, epoch({}) elapsed time({}ms).",
      document_index_id, trace, Helper::RegionEpochToString(document_index->Epoch()),
      Helper::TimestampMs() - start_time);

  return butil::Status::OK();
}

butil::Status DocumentIndexManager::CatchUpLogToDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                              DocumentIndexPtr document_index,
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

  if (regoin->GetStoreEngineType() != pb::common::STORE_ENG_RAFT_STORE) {
    // stop write index
    document_index_wrapper->SetIsSwitchingDocumentIndex(true);
    DEFER(document_index_wrapper->SetIsSwitchingDocumentIndex(false);
          bvar_document_index_catchup_latency_last_round << (Helper::TimestampMs() - start_time););

    document_index->SaveMeta(0);

    document_index_wrapper->UpdateDocumentIndex(document_index, trace);
    document_index_wrapper->SetRebuildSuccess();

    return butil::Status();
  }

  auto raft_meta = Server::GetInstance().GetRaftMeta(document_index_wrapper->Id());
  for (int i = 0;; ++i) {
    int64_t start_log_id = document_index->ApplyLogId() + 1;
    int64_t end_log_id = raft_meta->AppliedId();
    if (end_log_id - start_log_id < FLAGS_document_catchup_log_min_gap) {
      break;
    }
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
    auto status = ReplayWalToDocumentIndex(document_index_wrapper, document_index, start_log_id, end_log_id);
#else

    auto status = ReplayWalToDocumentIndex(document_index, start_log_id, end_log_id);
#endif
    if (!status.ok()) {
      document_index_wrapper->SetRebuildError();
      return butil::Status(pb::error::Errno::EINTERNAL,
                           fmt::format("Catch up {}-round({}-{}) fail", i, start_log_id, end_log_id));
    }

    document_index->SaveMeta(end_log_id);

    DINGO_LOG(INFO) << fmt::format("[document_index.catchup][id({})][trace({})] Catch up {}-round({}-{}) success.",
                                   document_index_id, trace, i, start_log_id, end_log_id);

    // Check vector index is stop
    if (document_index_wrapper->IsDestoryed()) {
      DINGO_LOG(WARNING) << fmt::format("[document_index.catchup]id({})][trace({})] document index is destoried.",
                                        document_index_id, trace);
      return butil::Status();
    }
  }

  bvar_document_index_catchup_latency_first_rounds << (Helper::TimestampMs() - start_time);

  {
    start_time = Helper::TimestampMs();

    // stop write index
    document_index_wrapper->SetIsSwitchingDocumentIndex(true);
    DEFER(document_index_wrapper->SetIsSwitchingDocumentIndex(false);
          bvar_document_index_catchup_latency_last_round << (Helper::TimestampMs() - start_time););

    int64_t start_log_id = document_index->ApplyLogId() + 1;
    int64_t end_log_id = raft_meta->AppliedId();
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
    // second ground replay wal
    auto status = ReplayWalToDocumentIndex(document_index_wrapper, document_index, start_log_id, end_log_id);
#else
    // second ground replay wal
    auto status = ReplayWalToDocumentIndex(document_index, start_log_id, end_log_id);
#endif
    if (!status.ok()) {
      document_index_wrapper->SetRebuildError();
      return status;
    }

    if (end_log_id > start_log_id) {
      document_index->SaveMeta(end_log_id);
    }

    DINGO_LOG(INFO) << fmt::format(
        "[document_index.catchup][id({})][trace({})] Catch up last-round({}-{}) success, elapsed time({}ms).",
        document_index_id, trace, start_log_id, end_log_id, Helper::TimestampMs() - start_time);

    document_index_wrapper->UpdateDocumentIndex(document_index, trace);
    document_index_wrapper->SetRebuildSuccess();
  }

  return butil::Status();
}

bool DocumentIndexManager::ExecuteTask(int64_t region_id, TaskRunnablePtr task) {
  if (workers_ == nullptr) {
    return false;
  }

  return workers_->ExecuteHashByRegionId(region_id, task);
}

bool DocumentIndexManager::ExecuteTaskFast(int64_t region_id, TaskRunnablePtr task) {
  if (fast_workers_ == nullptr) {
    return false;
  }

  return fast_workers_->ExecuteHashByRegionId(region_id, task);
}

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
bool DocumentIndexManager::ExecuteTaskVectorScalarSearch(int64_t /*region_id*/, TaskRunnablePtr task) {
  return vector_scalar_search_workers_->Execute(task);
}

bool DocumentIndexManager::ExecuteTaskForVector(int64_t region_id, TaskRunnablePtr task) {
  return Server::GetInstance().GetDocumentIndexManager()->ExecuteTaskVectorScalarSearch(region_id, task);
}
#endif

bool DocumentIndexManager::ExecuteTask(int64_t region_id, TaskRunnablePtr task, bool is_fast_task) {
  if (is_fast_task) {
    return Server::GetInstance().GetDocumentIndexManager()->ExecuteTaskFast(region_id, task);
  }

  return Server::GetInstance().GetDocumentIndexManager()->ExecuteTask(region_id, task);
}

std::vector<std::vector<std::string>> DocumentIndexManager::GetPendingTaskTrace() {
  if (workers_ == nullptr || fast_workers_ == nullptr) {
    return {};
  }

  std::vector<std::vector<std::string>> result;

  {
    auto traces = workers_->GetPendingTaskTrace();
    result.insert(result.end(), traces.begin(), traces.end());
  }

  {
    auto traces = fast_workers_->GetPendingTaskTrace();
    result.insert(result.end(), traces.begin(), traces.end());
  }

  return result;
}

uint64_t DocumentIndexManager::GetBackgroundPendingTaskCount() {
  if (workers_ == nullptr || fast_workers_ == nullptr) {
    return 0;
  }

  return workers_->PendingTaskCount() + fast_workers_->PendingTaskCount();
}

}  // namespace dingodb

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

#ifndef DINGODB_DOCUMENT_INDEX_MANAGER_H_
#define DINGODB_DOCUMENT_INDEX_MANAGER_H_

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
#include <sys/types.h>
#endif

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "bvar/latency_recorder.h"
#include "common/helper.h"
#include "document/document_index.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"

namespace dingodb {

// Rebuild document index task
class RebuildDocumentIndexTask : public TaskRunnable {
 public:
  RebuildDocumentIndexTask(DocumentIndexWrapperPtr document_index_wrapper, int64_t job_id, bool is_clear,
                           const std::string& trace)
      : document_index_wrapper_(document_index_wrapper), is_clear_(is_clear), job_id_(job_id), trace_(trace) {
    start_time_ = Helper::TimestampMs();
  }
  ~RebuildDocumentIndexTask() override = default;

  std::string Type() override { return "REBUILD_DOCUMENT_INDEX"; }

  void Run() override;

  std::string Trace() override;

 private:
  DocumentIndexWrapperPtr document_index_wrapper_;

  bool is_clear_;

  int64_t job_id_{0};
  std::string trace_;
  int64_t start_time_;
};

// Load or build document index task
class LoadOrBuildDocumentIndexTask : public TaskRunnable {
 public:
  LoadOrBuildDocumentIndexTask(DocumentIndexWrapperPtr document_index_wrapper, bool is_temp_hold_document_index,
                               int64_t job_id, const std::string& trace)
      : document_index_wrapper_(document_index_wrapper),
        is_temp_hold_document_index_(is_temp_hold_document_index),
        job_id_(job_id),
        trace_(trace) {
    start_time_ = Helper::TimestampMs();
  }
  ~LoadOrBuildDocumentIndexTask() override = default;

  std::string Type() override { return "LOAD_OR_BUILD_DOCUMENT_INDEX"; }

  void Run() override;

  std::string Trace() override;

 private:
  DocumentIndexWrapperPtr document_index_wrapper_;
  bool is_temp_hold_document_index_;
  int64_t job_id_;
  std::string trace_;
  int64_t start_time_;
};

// Manage document index, e.g. build/rebuild/save/load document index.
class DocumentIndexManager {
 public:
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  explicit DocumentIndexManager(const UseDocumentPurposeType& use_document_purpose_type)
      : use_document_purpose_type_(use_document_purpose_type) {}
#else
  DocumentIndexManager() = default;
#endif
  ~DocumentIndexManager() = default;

  bool Init();
  void Destroy();

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  // default use for document module
  static std::shared_ptr<DocumentIndexManager> New(const UseDocumentPurposeType& use_document_purpose_type) {
    return std::make_shared<DocumentIndexManager>(use_document_purpose_type);
  }
#else
  static std::shared_ptr<DocumentIndexManager> New() { return std::make_shared<DocumentIndexManager>(); }
#endif

  // Load document index for already exist document index at bootstrap.
  // Priority load from snapshot, if snapshot not exist then load from rocksdb.
  static butil::Status LoadOrBuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                const pb::common::RegionEpoch& epoch, const std::string& trace);

  // LaunchLoadOrBuildDocumentIndex is unused now.
  static void LaunchLoadOrBuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                             bool is_temp_hold_document_index, bool is_fast, int64_t job_id,
                                             const std::string& trace);
  // Invoke when server running.
  static butil::Status RebuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper, const std::string& trace);
  // Launch rebuild document index at execute queue.
  static void LaunchRebuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper, int64_t job_id, bool is_clear,
                                         const std::string& trace);

  static bvar::Adder<uint64_t> bvar_document_index_task_running_num;
  static bvar::Adder<uint64_t> bvar_document_index_rebuild_task_running_num;
  static bvar::Adder<uint64_t> bvar_document_index_loadorbuild_task_running_num;
  static bvar::Adder<uint64_t> bvar_document_index_fast_load_task_running_num;
  static bvar::Adder<uint64_t> bvar_document_index_slow_load_task_running_num;
  static bvar::Adder<uint64_t> bvar_document_index_fast_build_task_running_num;
  static bvar::Adder<uint64_t> bvar_document_index_slow_build_task_running_num;
  static bvar::Adder<uint64_t> bvar_document_index_load_catchup_running_num;
  static bvar::Adder<uint64_t> bvar_document_index_rebuild_catchup_running_num;

  static bvar::Adder<uint64_t> bvar_document_index_task_total_num;
  static bvar::Adder<uint64_t> bvar_document_index_rebuild_task_total_num;
  static bvar::Adder<uint64_t> bvar_document_index_loadorbuild_task_total_num;
  static bvar::Adder<uint64_t> bvar_document_index_fast_load_task_total_num;
  static bvar::Adder<uint64_t> bvar_document_index_slow_load_task_total_num;
  static bvar::Adder<uint64_t> bvar_document_index_fast_build_task_total_num;
  static bvar::Adder<uint64_t> bvar_document_index_slow_build_task_total_num;
  static bvar::Adder<uint64_t> bvar_document_index_load_catchup_total_num;
  static bvar::Adder<uint64_t> bvar_document_index_rebuild_catchup_total_num;
  static bvar::LatencyRecorder bvar_document_index_catchup_latency_first_rounds;
  static bvar::LatencyRecorder bvar_document_index_catchup_latency_last_round;

  static std::atomic<int> document_index_task_running_num;
  static std::atomic<int> document_index_rebuild_task_running_num;
  static std::atomic<int> document_index_loadorbuild_task_running_num;
  static std::atomic<int> document_index_fast_load_task_running_num;
  static std::atomic<int> document_index_slow_load_task_running_num;
  static std::atomic<int> document_index_fast_build_task_running_num;
  static std::atomic<int> document_index_slow_build_task_running_num;

  static int GetDocumentIndexTaskRunningNum();
  static void IncDocumentIndexTaskRunningNum();
  static void DecDocumentIndexTaskRunningNum();

  static int GetDocumentIndexRebuildTaskRunningNum();
  static void IncDocumentIndexRebuildTaskRunningNum();
  static void DecDocumentIndexRebuildTaskRunningNum();

  static int GetDocumentIndexLoadorbuildTaskRunningNum();
  static void IncDocumentIndexLoadorbuildTaskRunningNum();
  static void DecDocumentIndexLoadorbuildTaskRunningNum();

  static int GetDocumentIndexFastLoadTaskRunningNum();
  static void IncDocumentIndexFastLoadTaskRunningNum();
  static void DecDocumentIndexFastLoadTaskRunningNum();

  static int GetDocumentIndexSlowLoadTaskRunningNum();
  static void IncDocumentIndexSlowLoadTaskRunningNum();
  static void DecDocumentIndexSlowLoadTaskRunningNum();

  static int GetDocumentIndexFastBuildTaskRunningNum();
  static void IncDocumentIndexFastBuildTaskRunningNum();
  static void DecDocumentIndexFastBuildTaskRunningNum();

  static int GetDocumentIndexSlowBuildTaskRunningNum();
  static void IncDocumentIndexSlowBuildTaskRunningNum();
  static void DecDocumentIndexSlowBuildTaskRunningNum();

  bool ExecuteTask(int64_t region_id, TaskRunnablePtr task);
  bool ExecuteTaskFast(int64_t region_id, TaskRunnablePtr task);
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  bool ExecuteTaskVectorScalarSearch(int64_t region_id, TaskRunnablePtr task);
  static bool ExecuteTaskForVector(int64_t region_id, TaskRunnablePtr task);
#endif

  static bool ExecuteTask(int64_t region_id, TaskRunnablePtr task, bool is_fast_task);

  std::vector<std::vector<std::string>> GetPendingTaskTrace();

  uint64_t GetBackgroundPendingTaskCount();

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  UseDocumentPurposeType GetUseDocumentPurposeType() const { return use_document_purpose_type_; }
#endif

 private:
  static butil::Status LoadDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                         const pb::common::RegionEpoch& epoch, const std::string& trace);
  // Build document index with original data(rocksdb).
  // Invoke when server starting.
  static DocumentIndexPtr BuildDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper, const std::string& trace);
  // Catch up document index.
  static butil::Status CatchUpLogToDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                 DocumentIndexPtr document_index, const std::string& trace);
#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  // Replay log to document index.
  static butil::Status ReplayWalToDocumentIndex(DocumentIndexWrapperPtr document_index_wrapper,
                                                DocumentIndexPtr document_index, int64_t start_log_id,
                                                int64_t end_log_id);
#else
  // Replay log to document index.
  static butil::Status ReplayWalToDocumentIndex(DocumentIndexPtr document_index, int64_t start_log_id,
                                                int64_t end_log_id);
#endif
  // Execute all document index load/build/rebuild/save task.
  WorkerSetPtr workers_;
  WorkerSetPtr fast_workers_;

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
  // only for vector index scalar search with document
  WorkerSetPtr vector_scalar_search_workers_;
  UseDocumentPurposeType use_document_purpose_type_;
#endif
};

using DocumentIndexManagerPtr = std::shared_ptr<DocumentIndexManager>;

}  // namespace dingodb

#endif  // DINGODB_DOCUMENT_INDEX_MANAGER_H_

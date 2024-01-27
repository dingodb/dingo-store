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

#ifndef DINGODB_VECTOR_INDEX_MANAGER_H_
#define DINGODB_VECTOR_INDEX_MANAGER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "bvar/latency_recorder.h"
#include "common/helper.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

// Rebuild vector index task
class RebuildVectorIndexTask : public TaskRunnable {
 public:
  RebuildVectorIndexTask(VectorIndexWrapperPtr vector_index_wrapper, int64_t job_id, const std::string& trace)
      : vector_index_wrapper_(vector_index_wrapper), force_(job_id > 0), job_id_(job_id), trace_(trace) {
    start_time_ = Helper::TimestampMs();
  }
  ~RebuildVectorIndexTask() override = default;

  std::string Type() override { return "REBUILD_VECTOR_INDEX"; }

  void Run() override;

  std::string Trace() override;

 private:
  VectorIndexWrapperPtr vector_index_wrapper_;
  bool force_;
  int64_t job_id_{0};
  std::string trace_;
  int64_t start_time_;
};

// Save vector index task
class SaveVectorIndexTask : public TaskRunnable {
 public:
  SaveVectorIndexTask(VectorIndexWrapperPtr vector_index_wrapper, const std::string& trace)
      : vector_index_wrapper_(vector_index_wrapper), trace_(trace) {
    start_time_ = Helper::TimestampMs();
  }
  ~SaveVectorIndexTask() override = default;

  std::string Type() override { return "SAVE_VECTOR_INDEX"; }

  void Run() override;

  std::string Trace() override;

 private:
  VectorIndexWrapperPtr vector_index_wrapper_;
  std::string trace_;
  int64_t start_time_;
};

// Load or build vector index task
class LoadOrBuildVectorIndexTask : public TaskRunnable {
 public:
  LoadOrBuildVectorIndexTask(VectorIndexWrapperPtr vector_index_wrapper, bool is_temp_hold_vector_index, int64_t job_id,
                             const std::string& trace)
      : vector_index_wrapper_(vector_index_wrapper),
        is_temp_hold_vector_index_(is_temp_hold_vector_index),
        job_id_(job_id),
        trace_(trace) {
    start_time_ = Helper::TimestampMs();
  }
  ~LoadOrBuildVectorIndexTask() override = default;

  std::string Type() override { return "LOAD_OR_BUILD_VECTOR_INDEX"; }

  void Run() override;

  std::string Trace() override;

 private:
  VectorIndexWrapperPtr vector_index_wrapper_;
  bool is_temp_hold_vector_index_;
  int64_t job_id_;
  std::string trace_;
  int64_t start_time_;
};

class LoadAsyncBuildVectorIndexTask : public TaskRunnable {
 public:
  LoadAsyncBuildVectorIndexTask(VectorIndexWrapperPtr vector_index_wrapper, bool is_temp_hold_vector_index,
                                bool is_fast_load, int64_t job_id, const std::string& trace)
      : vector_index_wrapper_(vector_index_wrapper),
        is_temp_hold_vector_index_(is_temp_hold_vector_index),
        is_fast_load_(is_fast_load),
        job_id_(job_id),
        trace_(trace) {
    start_time_ = Helper::TimestampMs();
  }
  ~LoadAsyncBuildVectorIndexTask() override = default;

  std::string Type() override { return "LOAD_ASYNC_BUILD_VECTOR_INDEX"; }

  void Run() override;

  std::string Trace() override;

 private:
  VectorIndexWrapperPtr vector_index_wrapper_;
  bool is_temp_hold_vector_index_;
  bool is_fast_load_;
  int64_t job_id_;
  std::string trace_;
  int64_t start_time_;
};

class BuildVectorIndexTask : public TaskRunnable {
 public:
  BuildVectorIndexTask(VectorIndexWrapperPtr vector_index_wrapper, bool is_temp_hold_vector_index, bool is_fast_build,
                       int64_t job_id, const std::string& trace)
      : vector_index_wrapper_(vector_index_wrapper),
        is_temp_hold_vector_index_(is_temp_hold_vector_index),
        is_fast_build_(is_fast_build),
        job_id_(job_id),
        trace_(trace) {
    start_time_ = Helper::TimestampMs();
  }
  ~BuildVectorIndexTask() override = default;

  std::string Type() override { return "LOAD_ASYNC_BUILD_VECTOR_INDEX"; }

  void Run() override;

  std::string Trace() override;

 private:
  VectorIndexWrapperPtr vector_index_wrapper_;
  bool is_temp_hold_vector_index_;
  bool is_fast_build_;
  int64_t job_id_;
  std::string trace_;
  int64_t start_time_;
};

// Manage vector index, e.g. build/rebuild/save/load vector index.
class VectorIndexManager {
 public:
  VectorIndexManager() = default;
  ~VectorIndexManager() = default;

  bool Init();
  void Destroy();

  static std::shared_ptr<VectorIndexManager> New() { return std::make_shared<VectorIndexManager>(); }

  // Load vector index for already exist vector index at bootstrap.
  // Priority load from snapshot, if snapshot not exist then load from rocksdb.
  static butil::Status LoadOrBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                              const pb::common::RegionEpoch& epoch, const std::string& trace);
  static butil::Status LoadVectorIndexOnly(VectorIndexWrapperPtr vector_index_wrapper,
                                           const pb::common::RegionEpoch& epoch, const std::string& trace);
  static butil::Status BuildVectorIndexOnly(VectorIndexWrapperPtr vector_index_wrapper,
                                            const pb::common::RegionEpoch& epoch, const std::string& trace);

  // LaunchLoadAsyncBuildVectorIndex is unused now.
  static void LaunchLoadOrBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, bool is_temp_hold_vector_index,
                                           int64_t job_id, const std::string& trace);
  static void LaunchLoadAsyncBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                              bool is_temp_hold_vector_index, bool is_fast_load, int64_t job_id,
                                              const std::string& trace);

  // Parallel load or build vector index at server bootstrap.
  static butil::Status ParallelLoadOrBuildVectorIndex(std::vector<store::RegionPtr> regions, int concurrency,
                                                      const std::string& trace);

  // Save vector index snapshot.
  static butil::Status SaveVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, const std::string& trace);
  // Launch save vector index at execute queue.
  static void LaunchSaveVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, const std::string& trace);

  // Invoke when server running.
  static butil::Status RebuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, const std::string& trace);
  // Launch rebuild vector index at execute queue.
  static void LaunchRebuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, int64_t job_id,
                                       const std::string& trace);
  static void LaunchBuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, bool is_temp_hold_vector_index,
                                     bool is_fast_build, int64_t job_id, const std::string& trace);

  static butil::Status ScrubVectorIndex();

  static bvar::Adder<uint64_t> bvar_vector_index_task_running_num;
  static bvar::Adder<uint64_t> bvar_vector_index_rebuild_task_running_num;
  static bvar::Adder<uint64_t> bvar_vector_index_save_task_running_num;
  static bvar::Adder<uint64_t> bvar_vector_index_loadorbuild_task_running_num;
  static bvar::Adder<uint64_t> bvar_vector_index_fast_load_task_running_num;
  static bvar::Adder<uint64_t> bvar_vector_index_slow_load_task_running_num;
  static bvar::Adder<uint64_t> bvar_vector_index_fast_build_task_running_num;
  static bvar::Adder<uint64_t> bvar_vector_index_slow_build_task_running_num;
  static bvar::Adder<uint64_t> bvar_vector_index_load_catchup_running_num;
  static bvar::Adder<uint64_t> bvar_vector_index_rebuild_catchup_running_num;

  static bvar::Adder<uint64_t> bvar_vector_index_task_total_num;
  static bvar::Adder<uint64_t> bvar_vector_index_rebuild_task_total_num;
  static bvar::Adder<uint64_t> bvar_vector_index_save_task_total_num;
  static bvar::Adder<uint64_t> bvar_vector_index_loadorbuild_task_total_num;
  static bvar::Adder<uint64_t> bvar_vector_index_fast_load_task_total_num;
  static bvar::Adder<uint64_t> bvar_vector_index_slow_load_task_total_num;
  static bvar::Adder<uint64_t> bvar_vector_index_fast_build_task_total_num;
  static bvar::Adder<uint64_t> bvar_vector_index_slow_build_task_total_num;
  static bvar::Adder<uint64_t> bvar_vector_index_load_catchup_total_num;
  static bvar::Adder<uint64_t> bvar_vector_index_rebuild_catchup_total_num;
  static bvar::LatencyRecorder bvar_vector_index_catchup_latency_first_rounds;
  static bvar::LatencyRecorder bvar_vector_index_catchup_latency_last_round;

  static std::atomic<int> vector_index_task_running_num;
  static std::atomic<int> vector_index_rebuild_task_running_num;
  static std::atomic<int> vector_index_save_task_running_num;
  static std::atomic<int> vector_index_loadorbuild_task_running_num;
  static std::atomic<int> vector_index_fast_load_task_running_num;
  static std::atomic<int> vector_index_slow_load_task_running_num;
  static std::atomic<int> vector_index_fast_build_task_running_num;
  static std::atomic<int> vector_index_slow_build_task_running_num;

  static int GetVectorIndexTaskRunningNum();
  static void IncVectorIndexTaskRunningNum();
  static void DecVectorIndexTaskRunningNum();

  static int GetVectorIndexRebuildTaskRunningNum();
  static void IncVectorIndexRebuildTaskRunningNum();
  static void DecVectorIndexRebuildTaskRunningNum();

  static int GetVectorIndexSaveTaskRunningNum();
  static void IncVectorIndexSaveTaskRunningNum();
  static void DecVectorIndexSaveTaskRunningNum();

  static int GetVectorIndexLoadorbuildTaskRunningNum();
  static void IncVectorIndexLoadorbuildTaskRunningNum();
  static void DecVectorIndexLoadorbuildTaskRunningNum();

  static int GetVectorIndexFastLoadTaskRunningNum();
  static void IncVectorIndexFastLoadTaskRunningNum();
  static void DecVectorIndexFastLoadTaskRunningNum();

  static int GetVectorIndexSlowLoadTaskRunningNum();
  static void IncVectorIndexSlowLoadTaskRunningNum();
  static void DecVectorIndexSlowLoadTaskRunningNum();

  static int GetVectorIndexFastBuildTaskRunningNum();
  static void IncVectorIndexFastBuildTaskRunningNum();
  static void DecVectorIndexFastBuildTaskRunningNum();

  static int GetVectorIndexSlowBuildTaskRunningNum();
  static void IncVectorIndexSlowBuildTaskRunningNum();
  static void DecVectorIndexSlowBuildTaskRunningNum();

  bool ExecuteTask(int64_t region_id, TaskRunnablePtr task);
  bool ExecuteTaskFast(int64_t region_id, TaskRunnablePtr task);

  std::vector<std::vector<std::string>> GetPendingTaskTrace();

  uint64_t GetBackgroundPendingTaskCount();

 private:
  static butil::Status LoadVectorIndex(VectorIndexWrapperPtr vector_index_wrapper, const pb::common::RegionEpoch& epoch,
                                       const std::string& trace);
  // Build vector index with original data(rocksdb).
  // Invoke when server starting.
  static std::shared_ptr<VectorIndex> BuildVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                                       const std::string& trace);
  // Catch up vector index.
  static butil::Status CatchUpLogToVectorIndex(VectorIndexWrapperPtr vector_index_wrapper,
                                               std::shared_ptr<VectorIndex> vector_index, const std::string& trace);
  // Replay log to vector index.
  static butil::Status ReplayWalToVectorIndex(std::shared_ptr<VectorIndex> vector_index, int64_t start_log_id,
                                              int64_t end_log_id);

  static butil::Status TrainForBuild(std::shared_ptr<VectorIndex> vector_index, std::shared_ptr<Iterator> iter,
                                     const std::string& start_key, [[maybe_unused]] const std::string& end_key);

  // Execute all vector index load/build/rebuild/save task.
  WorkerSetPtr background_workers_;
  WorkerSetPtr fast_background_workers_;
};

using VectorIndexManagerPtr = std::shared_ptr<VectorIndexManager>;

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_MANAGER_H_

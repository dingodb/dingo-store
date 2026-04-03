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

#ifndef DINGODB_GC_TASK_MONITOR_H_
#define DINGODB_GC_TASK_MONITOR_H_

#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <unordered_set>
#include <vector>

#include "bthread/mutex.h"
#include "bvar/status.h"
#include "proto/debug.pb.h"

namespace dingodb {

class GcTaskMonitor {
 public:
  static GcTaskMonitor& GetInstance();

  void StartRun(int64_t start_time_ms);

  void AddRegionStats(int64_t region_id, int64_t start_time_ms, int64_t end_time_ms, int64_t iter_count,
                      int64_t delete_count, int64_t safe_point_ts);

  void RemoveMissingRegionStats(const std::unordered_set<int64_t>& alive_region_ids);

  void FinishRun(int64_t end_time_ms);

  void FillDebugInfo(pb::debug::DebugResponse::GCMetrics* gc_metrics, const std::vector<int64_t>& region_ids);

 private:
  struct GcRunRecord {
    int64_t start_time_ms{0};
    int64_t end_time_ms{0};
    int64_t iter_count{0};
    int64_t delete_count{0};
    int64_t safe_point_ts{0};
  };

  struct GcRunAccumulator {
    int64_t start_time_ms{0};
    int64_t iter_count{0};
    int64_t delete_count{0};
  };

  struct GcRegionStat {
    std::optional<GcRunRecord> last;
    std::optional<GcRunRecord> last_with_delete;
  };

  GcTaskMonitor();

  bthread::Mutex mutex_;
  int64_t first_start_time_ms_{0};
  int64_t run_count_{0};
  int64_t last_end_time_ms_{0};
  int64_t total_iter_count_{0};
  int64_t total_delete_count_{0};
  std::optional<GcRunAccumulator> current_run_;
  std::optional<GcRunRecord> last_with_delete_;
  std::deque<GcRunRecord> history_;
  std::map<int64_t, GcRegionStat> region_stats_;

  bvar::Status<int64_t> first_start_time_ms_bvar_;
  bvar::Status<int64_t> run_count_bvar_;
  bvar::Status<int64_t> last_end_time_ms_bvar_;
  bvar::Status<int64_t> total_iter_count_bvar_;
  bvar::Status<int64_t> total_delete_count_bvar_;
};

class ScopedGcTaskRun {
 public:
  ScopedGcTaskRun();
  ~ScopedGcTaskRun();

 private:
  int64_t start_time_ms_;
};

void GetGcMetricsDebugInfo(pb::debug::DebugResponse::GCMetrics* gc_metrics, const std::vector<int64_t>& region_ids);

}  // namespace dingodb

#endif  // DINGODB_GC_TASK_MONITOR_H_
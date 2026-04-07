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

#ifndef DINGODB_GC_TASK_TRACKER_H_
#define DINGODB_GC_TASK_TRACKER_H_

#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <unordered_set>
#include <vector>

#include "bthread/mutex.h"
#include "proto/debug.pb.h"

namespace dingodb {

class GcTaskTracker {
 public:
  static GcTaskTracker& GetInstance();

  void StartRun(int64_t start_time_ms);

  void AddRegionStats(int64_t region_id, int64_t start_time_ms, int64_t end_time_ms, int64_t iter_count,
                      int64_t delete_count, int64_t safe_point_ts);

  void RemoveMissingRegionStats(const std::unordered_set<int64_t>& alive_region_ids);

  // Clear all region stats (e.g. when no leader region exists on this node).
  void ClearRegionStats();

  void FinishRun(int64_t end_time_ms);

  void FillDebugInfo(pb::debug::DebugResponse::GCMetrics* gc_metrics, const std::vector<int64_t>& region_ids);

 private:
  struct GcRunRecord {
    int64_t start_time_ms{0};
    int64_t end_time_ms{0};
    int64_t iter_count{0};
    int64_t delete_count{0};
    // Only useful in history records at the regional level
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

  GcTaskTracker();
  GcTaskTracker(const GcTaskTracker&) = delete;
  GcTaskTracker& operator=(const GcTaskTracker&) = delete;

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
};

class ScopedGcTaskTracker {
 public:
  ScopedGcTaskTracker();
  ~ScopedGcTaskTracker();

  ScopedGcTaskTracker(const ScopedGcTaskTracker&) = delete;
  ScopedGcTaskTracker& operator=(const ScopedGcTaskTracker&) = delete;
};

// Retrieves GC metrics for debugging purposes.
// If region_ids is empty, fill both the GC metrics for all regions and the history of GC task metrics.
// Otherwise, fill metrics only for the specified regions.
void GetGcMetricsDebugInfo(pb::debug::DebugResponse::GCMetrics* gc_metrics, const std::vector<int64_t>& region_ids);

}  // namespace dingodb

#endif  // DINGODB_GC_TASK_TRACKER_H_

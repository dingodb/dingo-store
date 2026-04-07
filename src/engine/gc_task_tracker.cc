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

#include "engine/gc_task_tracker.h"

#include <cstddef>

#include "butil/scoped_lock.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/format.h"

namespace dingodb {

namespace {

constexpr size_t kGcHistorySize = 20;

}  // namespace

GcTaskTracker& GcTaskTracker::GetInstance() {
  static GcTaskTracker instance;
  return instance;
}

void GcTaskTracker::StartRun(int64_t start_time_ms) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (current_run_.has_value()) {
    DINGO_LOG(WARNING) << fmt::format("[txn_gc] GcTaskTracker StartRun, already has value, reset it");
  }

  current_run_ = GcRunAccumulator{start_time_ms, 0, 0};

  if (first_start_time_ms_ == 0) {
    first_start_time_ms_ = start_time_ms;
  }
}

void GcTaskTracker::AddRegionStats(int64_t region_id, int64_t start_time_ms, int64_t end_time_ms, int64_t iter_count,
                                   int64_t delete_count, int64_t safe_point_ts) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (!current_run_.has_value()) {
    DINGO_LOG(ERROR) << fmt::format("[txn_gc] GcTaskTracker AddRegionStats, has no value");
    return;
  }

  current_run_->iter_count += iter_count;
  current_run_->delete_count += delete_count;

  GcRunRecord region_record{start_time_ms, end_time_ms, iter_count, delete_count, safe_point_ts};

  auto& region_stat = region_stats_[region_id];
  region_stat.last = region_record;
  if (delete_count > 0) {
    region_stat.last_with_delete = region_record;
  }
}

void GcTaskTracker::RemoveMissingRegionStats(const std::unordered_set<int64_t>& alive_region_ids) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (auto iter = region_stats_.begin(); iter != region_stats_.end();) {
    if (alive_region_ids.find(iter->first) == alive_region_ids.end()) {
      iter = region_stats_.erase(iter);
    } else {
      ++iter;
    }
  }
}

void GcTaskTracker::ClearRegionStats() {
  BAIDU_SCOPED_LOCK(mutex_);
  region_stats_.clear();
}

void GcTaskTracker::FinishRun(int64_t end_time_ms) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (!current_run_.has_value()) {
    return;
  }

  ++run_count_;
  total_iter_count_ += current_run_->iter_count;
  total_delete_count_ += current_run_->delete_count;

  last_end_time_ms_ = end_time_ms;

  if (current_run_->delete_count > 0) {
    // safe_point_ts is intentionally left as 0 for global-level last_with_delete_ record,
    // because each run aggregates multiple regions with different safe points.
    last_with_delete_ =
        GcRunRecord{current_run_->start_time_ms, end_time_ms, current_run_->iter_count, current_run_->delete_count};
  }

  // safe_point_ts is intentionally left as 0 for global-level history records,
  // because each run aggregates multiple regions with different safe points.
  history_.push_back(
      GcRunRecord{current_run_->start_time_ms, end_time_ms, current_run_->iter_count, current_run_->delete_count});
  if (history_.size() > kGcHistorySize) {
    history_.pop_front();
  }

  current_run_.reset();
}

void GcTaskTracker::FillDebugInfo(pb::debug::DebugResponse::GCMetrics* gc_metrics,
                                  const std::vector<int64_t>& region_ids) {
  if (gc_metrics == nullptr) {
    return;
  }

  BAIDU_SCOPED_LOCK(mutex_);
  gc_metrics->Clear();
  gc_metrics->set_first_start_time_ms(first_start_time_ms_);
  gc_metrics->set_run_count(run_count_);
  gc_metrics->set_last_end_time_ms(last_end_time_ms_);
  gc_metrics->set_total_iter_count(total_iter_count_);
  gc_metrics->set_total_delete_count(total_delete_count_);

  if (region_ids.empty()) {
    for (auto iter = history_.rbegin(); iter != history_.rend(); ++iter) {
      auto* item = gc_metrics->add_history();
      item->set_start_time_ms(iter->start_time_ms);
      item->set_end_time_ms(iter->end_time_ms);
      item->set_iter_count(iter->iter_count);
      item->set_delete_count(iter->delete_count);
      // safe_point_ts is only meaningful at the per-region level;
      // global runs aggregate multiple regions with different safe points.
    }
  }

  auto append_region_stat = [&](int64_t region_id, const GcRegionStat& region_stat) {
    auto* region_item = gc_metrics->add_region_histories();
    region_item->set_region_id(region_id);

    if (region_stat.last.has_value()) {
      auto* last = region_item->mutable_last_history();
      last->set_start_time_ms(region_stat.last->start_time_ms);
      last->set_end_time_ms(region_stat.last->end_time_ms);
      last->set_iter_count(region_stat.last->iter_count);
      last->set_delete_count(region_stat.last->delete_count);
      last->set_safe_point_ts(region_stat.last->safe_point_ts);
    }

    if (region_stat.last_with_delete.has_value()) {
      auto* last_delete = region_item->mutable_last_delete_history();
      last_delete->set_start_time_ms(region_stat.last_with_delete->start_time_ms);
      last_delete->set_end_time_ms(region_stat.last_with_delete->end_time_ms);
      last_delete->set_iter_count(region_stat.last_with_delete->iter_count);
      last_delete->set_delete_count(region_stat.last_with_delete->delete_count);
      last_delete->set_safe_point_ts(region_stat.last_with_delete->safe_point_ts);
    }
  };

  if (region_ids.empty()) {
    for (const auto& [region_id, region_stat] : region_stats_) {
      append_region_stat(region_id, region_stat);
    }
  } else {
    for (const auto region_id : region_ids) {
      auto iter = region_stats_.find(region_id);
      if (iter == region_stats_.end()) {
        continue;
      }
      append_region_stat(region_id, iter->second);
    }
  }

  if (region_ids.empty() && last_with_delete_.has_value()) {
    auto* item = gc_metrics->mutable_last_with_delete_history();
    item->set_start_time_ms(last_with_delete_->start_time_ms);
    item->set_end_time_ms(last_with_delete_->end_time_ms);
    item->set_iter_count(last_with_delete_->iter_count);
    item->set_delete_count(last_with_delete_->delete_count);
    // safe_point_ts is only meaningful at the per-region level;
    // global runs aggregate multiple regions with different safe points.
  }
}

GcTaskTracker::GcTaskTracker() = default;

ScopedGcTaskTracker::ScopedGcTaskTracker() { GcTaskTracker::GetInstance().StartRun(Helper::TimestampMs()); }

ScopedGcTaskTracker::~ScopedGcTaskTracker() { GcTaskTracker::GetInstance().FinishRun(Helper::TimestampMs()); }

void GetGcMetricsDebugInfo(pb::debug::DebugResponse::GCMetrics* gc_metrics, const std::vector<int64_t>& region_ids) {
  GcTaskTracker::GetInstance().FillDebugInfo(gc_metrics, region_ids);
}

}  // namespace dingodb

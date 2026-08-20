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

#include "engine/txn_auto_compaction_checker.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "bvar/latency_recorder.h"
#include "bvar/reducer.h"
#include "common/constant.h"
#include "common/gflag_validator.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "engine/rocks_raw_engine.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "mvcc/codec.h"
#include "server/server.h"

namespace dingodb {

DECLARE_bool(gc_enable_compaction_filter);
DECLARE_bool(gc_enable_safe_point_read_check);
DECLARE_int32(gc_auto_compaction_check_interval_s);

DEFINE_bool(gc_enable_auto_compaction, true,
            "score regions by write-cf MVCC garbage every check interval and manually compact the dirtiest ones so "
            "the gc compaction filter reclaims them promptly; no-op while the gc safe point is 0");
DEFINE_validator(gc_enable_auto_compaction, &PassBool);

DEFINE_int64(gc_auto_compaction_min_tombstones, 10000,
             "tombstone-rule absolute trigger: rocksdb point tombstones (plus raft range deletions) in a region");
DEFINE_validator(gc_auto_compaction_min_tombstones, &PassInt64);
DEFINE_int32(gc_auto_compaction_tombstones_percent, 30,
             "tombstone-rule ratio trigger, percent of the region's total write-cf entries");
DEFINE_validator(gc_auto_compaction_tombstones_percent, &PassInt32);

DEFINE_int64(gc_auto_compaction_min_redundant_versions, 50000,
             "redundancy-rule absolute trigger: estimated MVCC versions at or below the safe point that a "
             "filter-enabled compaction could physically drop");
DEFINE_validator(gc_auto_compaction_min_redundant_versions, &PassInt64);
DEFINE_int32(gc_auto_compaction_redundant_versions_percent, 20,
             "redundancy-rule ratio trigger, percent of the region's total write-cf entries");
DEFINE_validator(gc_auto_compaction_redundant_versions_percent, &PassInt32);

DEFINE_bool(gc_auto_compaction_force_bottommost, false,
            "force bottommost-level recompaction for triggered ranges; mainly affects the data CF (the write CF "
            "always carries a compaction filter, which already implies bottommost rewrite)");
DEFINE_validator(gc_auto_compaction_force_bottommost, &PassBool);

DEFINE_int32(gc_auto_compaction_region_cooldown_s, 1800,
             "seconds a region stays exempt after an auto compaction; 0 disables. Guards against re-compacting "
             "regions whose delete-mark garbage waits on scan GC and cannot shrink yet");
DEFINE_validator(gc_auto_compaction_region_cooldown_s, &PassInt32);

namespace {

bvar::Adder<int64_t> g_auto_compaction_rounds("dingo_gc_auto_compaction_rounds");
bvar::Adder<int64_t> g_auto_compaction_skipped_no_safepoint("dingo_gc_auto_compaction_skipped_no_safepoint");
bvar::Adder<int64_t> g_auto_compaction_regions_evaluated("dingo_gc_auto_compaction_regions_evaluated");
bvar::Adder<int64_t> g_auto_compaction_regions_eligible("dingo_gc_auto_compaction_regions_eligible");
bvar::Adder<int64_t> g_auto_compaction_compactions("dingo_gc_auto_compaction_compactions");
bvar::Adder<int64_t> g_auto_compaction_fail("dingo_gc_auto_compaction_fail");
bvar::Adder<int64_t> g_auto_compaction_reeval_dropped("dingo_gc_auto_compaction_reeval_dropped");
bvar::Adder<int64_t> g_auto_compaction_budget_stops("dingo_gc_auto_compaction_budget_stops");
bvar::Adder<int64_t> g_auto_compaction_cooldown_skips("dingo_gc_auto_compaction_cooldown_skips");
bvar::LatencyRecorder g_auto_compaction_compact_latency("dingo_gc_auto_compaction_compact");
bvar::LatencyRecorder g_auto_compaction_round_latency("dingo_gc_auto_compaction_round");

}  // namespace

TxnAutoCompactionChecker& TxnAutoCompactionChecker::GetInstance() {
  static TxnAutoCompactionChecker instance;
  return instance;
}

void TxnAutoCompactionChecker::Wire(std::shared_ptr<RocksRawEngine> engine, SafePointProvider safe_point_provider) {
  std::lock_guard<std::mutex> lk(mutex_);
  engine_ = engine;
  safe_point_provider_ = std::move(safe_point_provider);
}

void TxnAutoCompactionChecker::RegularCheckHandler(void* /*arg*/) {
  static std::atomic<bool> running(false);
  if (running.load(std::memory_order_relaxed)) {
    return;
  }
  AtomicGuard guard(running);

  if (!FLAGS_gc_enable_auto_compaction) {
    return;
  }
  GetInstance().RunOnce();
}

int64_t TxnAutoCompactionChecker::InterpolateCount(int64_t count, int64_t oldest_ts, int64_t newest_ts,
                                                   int64_t safe_point_ts) {
  if (count <= 0 || oldest_ts <= 0 || newest_ts <= 0 || oldest_ts > newest_ts) {
    return 0;
  }
  if (safe_point_ts >= newest_ts) {
    return count;
  }
  if (safe_point_ts < oldest_ts) {
    return 0;
  }
  // oldest_ts <= safe_point_ts < newest_ts, so newest_ts > oldest_ts here.
  double portion = static_cast<double>(safe_point_ts - oldest_ts) / static_cast<double>(newest_ts - oldest_ts);
  return static_cast<int64_t>(std::llround(static_cast<double>(count) * portion));
}

int64_t TxnAutoCompactionChecker::EstimateDiscardable(const TxnMvccProperties& mvcc, int64_t safe_point_ts) {
  int64_t discardable =
      InterpolateCount(mvcc.num_deletes, mvcc.oldest_delete_ts, mvcc.newest_delete_ts, safe_point_ts);
  int64_t stale_versions = std::max<int64_t>(mvcc.num_versions - mvcc.num_rows, 0);
  discardable +=
      InterpolateCount(stale_versions, mvcc.oldest_stale_version_ts, mvcc.newest_stale_version_ts, safe_point_ts);
  return discardable;
}

TxnAutoCompactionChecker::ScoreDetail TxnAutoCompactionChecker::ComputeScore(const TxnMvccRangeStats& stats,
                                                                             int64_t safe_point_ts, bool filter_on) {
  ScoreDetail detail;
  detail.total_entries = stats.total_entries;
  detail.tombstones = stats.tombstones + stats.range_deletions;
  if (stats.total_entries <= 0) {
    return detail;
  }

  // Tombstone rule: physical rocksdb garbage a compaction reclaims with or
  // without the gc filter.
  double tombstone_score = 0.0;
  {
    double ratio = static_cast<double>(detail.tombstones) / static_cast<double>(stats.total_entries);
    if (detail.tombstones >= FLAGS_gc_auto_compaction_min_tombstones ||
        ratio * 100.0 >= static_cast<double>(FLAGS_gc_auto_compaction_tombstones_percent)) {
      tombstone_score = static_cast<double>(detail.tombstones) * ratio;
    }
  }

  if (!filter_on) {
    detail.score = tombstone_score;
    return detail;
  }

  // Redundancy rule (filter mode): MVCC versions the filter could drop.
  // Files without dingo.mvcc.* properties contribute nothing here — an old
  // file must not look like garbage.
  double redundancy_score = 0.0;
  if (stats.num_files_with_props > 0) {
    detail.discardable = std::min(EstimateDiscardable(stats.mvcc, safe_point_ts), stats.total_entries);
    double ratio =
        static_cast<double>(detail.tombstones + detail.discardable) / static_cast<double>(stats.total_entries);
    if (detail.discardable >= FLAGS_gc_auto_compaction_min_redundant_versions ||
        ratio * 100.0 >= static_cast<double>(FLAGS_gc_auto_compaction_redundant_versions_percent)) {
      redundancy_score = static_cast<double>(detail.discardable) * ratio;
    }
  }

  // TiKV's filter-on formula ignores pure-tombstone regions (score =
  // discardable * ratio = 0). This tree cannot: head-rollback point deletes
  // keep flowing in filter mode and pre-filter tombstone mountains predate
  // the properties. Take the better of both rules instead.
  detail.score = std::max(redundancy_score, tombstone_score);
  return detail;
}

std::vector<int64_t> TxnAutoCompactionChecker::RunForRanges(const std::shared_ptr<RocksRawEngine>& engine,
                                                            const std::vector<RangeEntry>& entries,
                                                            const SafePointProvider& safe_point_provider,
                                                            int64_t budget_ms) {
  std::vector<int64_t> attempted;
  if (engine == nullptr || safe_point_provider == nullptr) {
    return attempted;
  }
  int64_t start_ms = Helper::TimestampMs();
  int64_t safe_point_ts = safe_point_provider();
  if (safe_point_ts <= 0) {
    g_auto_compaction_skipped_no_safepoint << 1;
    return attempted;
  }
  const bool filter_on = FLAGS_gc_enable_compaction_filter && FLAGS_gc_enable_safe_point_read_check;

  struct Candidate {
    RangeEntry entry;
    ScoreDetail detail;
  };
  std::vector<Candidate> candidates;
  candidates.reserve(entries.size());
  for (const auto& entry : entries) {
    TxnMvccRangeStats stats;
    auto status = engine->GetTxnMvccPropertiesInRange(entry.encoded_range, stats);
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format("[auto_compaction][region({})] get range properties failed, error: {}",
                                        entry.region_id, status.error_str());
      continue;
    }
    ScoreDetail detail = ComputeScore(stats, safe_point_ts, filter_on);
    if (detail.score > 0.0) {
      candidates.push_back({entry, detail});
    }
  }
  g_auto_compaction_regions_eligible << static_cast<int64_t>(candidates.size());

  std::sort(candidates.begin(), candidates.end(),
            [](const Candidate& lhs, const Candidate& rhs) { return lhs.detail.score > rhs.detail.score; });
  // TiKV sizes the batch by the round budget, assuming >= 1s per compaction.
  size_t top_n = std::max<size_t>(10, static_cast<size_t>(budget_ms / 1000));
  if (candidates.size() > top_n) {
    candidates.resize(top_n);
  }

  for (const auto& candidate : candidates) {
    if (Helper::TimestampMs() - start_ms >= budget_ms) {
      g_auto_compaction_budget_stops << 1;
      break;
    }
    // Re-evaluate with a fresh safe point right before burning IO: the
    // candidate's SSTs may already have been rewritten by write-driven
    // compaction or by a neighbor's boundary file, and gc_stop (br backup)
    // may have arrived mid-round.
    int64_t current_safe_point_ts = safe_point_provider();
    if (current_safe_point_ts <= 0) {
      break;
    }
    TxnMvccRangeStats stats;
    if (!engine->GetTxnMvccPropertiesInRange(candidate.entry.encoded_range, stats).ok()) {
      continue;
    }
    ScoreDetail fresh = ComputeScore(stats, current_safe_point_ts, filter_on);
    if (fresh.score <= 0.0) {
      g_auto_compaction_reeval_dropped << 1;
      continue;
    }

    int64_t compact_start_us = Helper::TimestampMs() * 1000;
    // Write CF first (the gc filter's battlefield); the data CF follows so
    // the orphan rows the filter just deleted get their tombstones ground
    // down in the same visit.
    auto write_status = engine->CompactRangeForAutoGc(Constant::kTxnWriteCF, candidate.entry.encoded_range,
                                                      FLAGS_gc_auto_compaction_force_bottommost);
    auto data_status = engine->CompactRangeForAutoGc(Constant::kTxnDataCF, candidate.entry.encoded_range,
                                                     FLAGS_gc_auto_compaction_force_bottommost);
    g_auto_compaction_compact_latency << (Helper::TimestampMs() * 1000 - compact_start_us);

    // Attempted either way: a failing region must not be retried every round.
    attempted.push_back(candidate.entry.region_id);
    if (!write_status.ok() || !data_status.ok()) {
      g_auto_compaction_fail << 1;
      DINGO_LOG(WARNING) << fmt::format(
          "[auto_compaction][region({})] compact range failed, write: {} data: {}", candidate.entry.region_id,
          write_status.error_str(), data_status.error_str());
      continue;
    }
    g_auto_compaction_compactions << 1;
    DINGO_LOG(INFO) << fmt::format(
        "[auto_compaction][region({})] compacted, score: {:.1f} total_entries: {} tombstones: {} discardable: {}",
        candidate.entry.region_id, fresh.score, fresh.total_entries, fresh.tombstones, fresh.discardable);
  }
  return attempted;
}

bool TxnAutoCompactionChecker::InCooldown(int64_t region_id, int64_t now_ms) {
  if (FLAGS_gc_auto_compaction_region_cooldown_s <= 0) {
    return false;
  }
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = last_compact_ms_.find(region_id);
  if (it == last_compact_ms_.end()) {
    return false;
  }
  return now_ms - it->second < static_cast<int64_t>(FLAGS_gc_auto_compaction_region_cooldown_s) * 1000;
}

void TxnAutoCompactionChecker::PruneCooldown(const std::vector<int64_t>& alive_region_ids) {
  std::lock_guard<std::mutex> lk(mutex_);
  std::map<int64_t, int64_t> pruned;
  for (int64_t region_id : alive_region_ids) {
    auto it = last_compact_ms_.find(region_id);
    if (it != last_compact_ms_.end()) {
      pruned.emplace(it->first, it->second);
    }
  }
  last_compact_ms_ = std::move(pruned);
}

void TxnAutoCompactionChecker::RunOnce() {
  g_auto_compaction_rounds << 1;
  int64_t round_start_ms = Helper::TimestampMs();

  std::shared_ptr<RocksRawEngine> engine;
  SafePointProvider safe_point_provider;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    engine = engine_.lock();
    safe_point_provider = safe_point_provider_;
  }
  if (engine == nullptr || safe_point_provider == nullptr) {
    // Unwired: XDPROCKS build, coordinator role, or shutdown already reset
    // the engine.
    return;
  }
  if (safe_point_provider() <= 0) {
    g_auto_compaction_skipped_no_safepoint << 1;
    return;
  }

  std::vector<RangeEntry> entries;
  std::vector<int64_t> alive_region_ids;
  for (const auto& region : Server::GetInstance().GetAllAliveRegion()) {
    alive_region_ids.push_back(region->Id());
    if (region->State() != pb::common::StoreRegionState::NORMAL) {
      continue;
    }
    if (region->GetRawEngineType() != pb::common::RawEngine::RAW_ENG_ROCKSDB) {
      continue;
    }
    auto range = region->Range(false);
    if (range.start_key().empty() || range.start_key() >= range.end_key()) {
      continue;
    }
    if (InCooldown(region->Id(), round_start_ms)) {
      g_auto_compaction_cooldown_skips << 1;
      continue;
    }
    RangeEntry entry;
    entry.region_id = region->Id();
    // Same bounds formula as the scan-GC iterator (txn_engine_helper.cc):
    // one encoded interval serves both the write CF (~commit_ts suffix) and
    // the data CF (~start_ts suffix).
    entry.encoded_range.set_start_key(mvcc::Codec::EncodeKey(range.start_key(), Constant::kMaxVer));
    entry.encoded_range.set_end_key(mvcc::Codec::EncodeKey(range.end_key(), Constant::kMaxVer));
    entries.push_back(std::move(entry));
  }
  PruneCooldown(alive_region_ids);
  g_auto_compaction_regions_evaluated << static_cast<int64_t>(entries.size());

  int64_t budget_ms = static_cast<int64_t>(std::max(FLAGS_gc_auto_compaction_check_interval_s, 1)) * 1000;
  auto attempted = RunForRanges(engine, entries, safe_point_provider, budget_ms);

  if (!attempted.empty()) {
    int64_t now_ms = Helper::TimestampMs();
    std::lock_guard<std::mutex> lk(mutex_);
    for (int64_t region_id : attempted) {
      last_compact_ms_[region_id] = now_ms;
    }
  }

  int64_t elapsed_ms = Helper::TimestampMs() - round_start_ms;
  g_auto_compaction_round_latency << elapsed_ms * 1000;
  DINGO_LOG(INFO) << fmt::format("[auto_compaction] round done, evaluated: {} compacted: {} elapsed_ms: {}",
                                 entries.size(), attempted.size(), elapsed_ms);
}

}  // namespace dingodb

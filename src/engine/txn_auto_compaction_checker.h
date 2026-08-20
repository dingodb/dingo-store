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

#ifndef DINGODB_ENGINE_TXN_AUTO_COMPACTION_CHECKER_H_  // NOLINT
#define DINGODB_ENGINE_TXN_AUTO_COMPACTION_CHECKER_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "engine/txn_mvcc_properties_collector.h"
#include "proto/common.pb.h"

namespace dingodb {

class RocksRawEngine;

// TiKV-style MVCC-aware auto compaction (gc_worker/compaction_runner.rs):
// every check interval, score each local region (leader or follower —
// compaction is a per-replica affair) from the write CF's aggregated table
// properties, and manually CompactRange the dirtiest ones so the GC
// compaction filter reclaims their garbage NOW instead of whenever
// write-driven compaction happens to visit. Scoring is estimation over
// SST-overlap-granular statistics; a wasted compaction costs IO, never
// correctness.
class TxnAutoCompactionChecker {
 public:
  using SafePointProvider = std::function<int64_t()>;

  static TxnAutoCompactionChecker& GetInstance();

  TxnAutoCompactionChecker(const TxnAutoCompactionChecker&) = delete;
  TxnAutoCompactionChecker& operator=(const TxnAutoCompactionChecker&) = delete;

  // Called once from Server::InitRocksRawEngine (non-XDPROCKS branch), same
  // provider semantics as the gc filter: min over tenants, 0 while any
  // tenant is gc-stopped or uninitialized. weak_ptr never extends the
  // engine's lifetime past shutdown.
  void Wire(std::shared_ptr<RocksRawEngine> engine, SafePointProvider safe_point_provider);

  // Crontab entry (GC_AUTO_COMPACTION): re-entrancy guarded; cheap no-op
  // while the enable flag is off, unwired, or the safe point is 0.
  static void RegularCheckHandler(void* arg);

  struct RangeEntry {
    int64_t region_id{0};
    pb::common::Range encoded_range;
  };
  struct ScoreDetail {
    double score{0.0};
    int64_t tombstones{0};   // point tombstones + raft range deletions
    int64_t discardable{0};  // estimated MVCC versions the filter could drop
    int64_t total_entries{0};
  };

  // ---- pure logic, no Server dependency: unit-tested directly ----

  // How many of `count` entries, spread over [oldest_ts, newest_ts], sit at
  // or below the safe point — linear interpolation under a uniform-
  // distribution assumption. 0 ts bounds mean "no data". Unlike TiKV, a
  // single-ts segment (oldest == newest) fully below the safe point counts.
  static int64_t InterpolateCount(int64_t count, int64_t oldest_ts, int64_t newest_ts, int64_t safe_point_ts);

  // Estimated write-CF entries a filter-enabled compaction could physically
  // drop: the delete-mark segment plus the shadowed-version segment.
  static int64_t EstimateDiscardable(const TxnMvccProperties& mvcc, int64_t safe_point_ts);

  // filter_on mirrors the factory's flag gate (both gc flags). Thresholds
  // come from the gc_auto_compaction_* flags. Deviation from TiKV, on
  // purpose: in filter mode the tombstone rule still applies (max of both
  // scores) — this tree keeps writing point tombstones (head rollbacks) and
  // carries pre-filter tombstone mountains that TiKV's formula would ignore.
  static ScoreDetail ComputeScore(const TxnMvccRangeStats& stats, int64_t safe_point_ts, bool filter_on);

  // Evaluate, rank (top max(10, budget_s)), and compact `entries` within
  // `budget_ms`; each candidate is re-evaluated with a fresh safe point
  // right before compacting (its SSTs may already have been rewritten).
  // Compacts the write CF first, then the data CF, over the same encoded
  // range. Returns the region ids attempted — success or failure alike, so
  // the caller's cooldown also covers persistently failing regions.
  // Server-free: production RunOnce and tests share this path.
  static std::vector<int64_t> RunForRanges(const std::shared_ptr<RocksRawEngine>& engine,
                                           const std::vector<RangeEntry>& entries,
                                           const SafePointProvider& safe_point_provider, int64_t budget_ms);

 private:
  TxnAutoCompactionChecker() = default;

  void RunOnce();
  bool InCooldown(int64_t region_id, int64_t now_ms);
  void PruneCooldown(const std::vector<int64_t>& alive_region_ids);

  std::mutex mutex_;
  std::weak_ptr<RocksRawEngine> engine_;
  SafePointProvider safe_point_provider_;
  // Regions whose garbage the filter cannot reclaim yet (delete-mark groups
  // wait for scan GC's raft range) would otherwise be re-selected every
  // round with unchanged properties — the cooldown breaks that IO spin.
  std::map<int64_t, int64_t> last_compact_ms_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_TXN_AUTO_COMPACTION_CHECKER_H_  // NOLINT

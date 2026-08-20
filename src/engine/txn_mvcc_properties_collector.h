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

#ifndef DINGODB_ENGINE_TXN_MVCC_PROPERTIES_COLLECTOR_H_  // NOLINT
#define DINGODB_ENGINE_TXN_MVCC_PROPERTIES_COLLECTOR_H_

#include <cstdint>
#include <string>

#include "rocksdb/table_properties.h"

namespace dingodb {

// User-collected property keys written per write-CF SST. Decimal-string
// encoded so `sst_dump --show_properties` stays human-readable; encoding runs
// once per table file, cost is irrelevant.
inline constexpr char kMvccPropMinTs[] = "dingo.mvcc.min_ts";
inline constexpr char kMvccPropMaxTs[] = "dingo.mvcc.max_ts";
inline constexpr char kMvccPropNumRows[] = "dingo.mvcc.num_rows";
inline constexpr char kMvccPropNumPuts[] = "dingo.mvcc.num_puts";
inline constexpr char kMvccPropNumDeletes[] = "dingo.mvcc.num_deletes";
inline constexpr char kMvccPropNumRollbacks[] = "dingo.mvcc.num_rollbacks";
inline constexpr char kMvccPropNumVersions[] = "dingo.mvcc.num_versions";
inline constexpr char kMvccPropMaxRowVersions[] = "dingo.mvcc.max_row_versions";
inline constexpr char kMvccPropOldestStaleTs[] = "dingo.mvcc.oldest_stale_version_ts";
inline constexpr char kMvccPropNewestStaleTs[] = "dingo.mvcc.newest_stale_version_ts";
inline constexpr char kMvccPropOldestDeleteTs[] = "dingo.mvcc.oldest_delete_ts";
inline constexpr char kMvccPropNewestDeleteTs[] = "dingo.mvcc.newest_delete_ts";
inline constexpr char kMvccPropNumErrors[] = "dingo.mvcc.num_errors";

// Per-SST MVCC statistics of the txn write CF, collected at table-file
// creation (flush/compaction output) and persisted as user collected
// properties (TiKV's mvcc-properties-collector scheme). The auto-compaction
// checker aggregates them per region to estimate how much garbage the GC
// compaction filter could reclaim from a manual compaction.
//
// All ts fields use 0 as the "no data" sentinel (a real commit_ts is >= 1;
// the collector treats ts <= 0 as a parse error).
struct TxnMvccProperties {
  int64_t min_ts{0};  // over ALL entries, including rocksdb tombstones
  int64_t max_ts{0};
  int64_t num_rows{0};      // distinct user keys (per SST; a key spanning SSTs counts once per file)
  int64_t num_puts{0};      // WriteInfo.op == Put
  int64_t num_deletes{0};   // WriteInfo.op == Delete (MVCC delete marks, not rocksdb tombstones)
  int64_t num_rollbacks{0};
  int64_t num_versions{0};  // value entries only (rocksdb Put type); tombstones excluded
  int64_t max_row_versions{0};
  // ts range of shadowed (non-newest-in-file) versions: what a compaction
  // could physically drop once the safe point passes them.
  int64_t oldest_stale_version_ts{0};
  int64_t newest_stale_version_ts{0};
  // ts range of MVCC delete marks.
  int64_t oldest_delete_ts{0};
  int64_t newest_delete_ts{0};
  int64_t num_errors{0};  // unparseable keys/values/ops seen while building this SST

  void Add(const TxnMvccProperties& other);
  void EncodeTo(rocksdb::UserCollectedProperties* out) const;
  // False when the SST predates the collector or the properties are damaged:
  // the caller then counts that file into denominators (built-in counters)
  // only, never into MVCC sums — a no-props file must not look like garbage.
  static bool DecodeFrom(const rocksdb::UserCollectedProperties& props, TxnMvccProperties* out);
};

// Aggregation result over every write-CF SST overlapping one queried range.
// Built-in rocksdb counters cover EVERY overlapping file (valid for
// pre-collector SSTs too); `mvcc` sums only files carrying dingo.mvcc.*.
// Granularity is SST-overlap: a boundary file shared with the neighbor
// region is counted in full for both.
struct TxnMvccRangeStats {
  int64_t num_files{0};
  int64_t num_files_with_props{0};
  int64_t total_entries{0};    // Σ built-in num_entries
  int64_t tombstones{0};       // Σ built-in num_deletions (point tombstones)
  int64_t range_deletions{0};  // Σ built-in num_range_deletions (raft group deletes of scan GC)
  TxnMvccProperties mvcc;
};

// One instance per table-file creation (created by the factory), called
// sequentially — no locking needed. Never lets an exception escape and never
// returns a non-OK status: rocksdb is not exception safe, and a non-OK
// status would drop the whole property block of the file.
class TxnMvccPropertiesCollector : public rocksdb::TablePropertiesCollector {
 public:
  rocksdb::Status AddUserKey(const rocksdb::Slice& key, const rocksdb::Slice& value, rocksdb::EntryType type,
                             rocksdb::SequenceNumber seq, uint64_t file_size) override;
  rocksdb::Status Finish(rocksdb::UserCollectedProperties* properties) override;
  rocksdb::UserCollectedProperties GetReadableProperties() const override;
  const char* Name() const override { return "TxnMvccPropertiesCollector"; }

 private:
  TxnMvccProperties props_;
  std::string cur_user_key_;  // encoded user key of the current version group (no ts suffix)
  int64_t cur_row_versions_{0};
};

// Registered once on the txn write CF at DB open, unconditionally: the
// statistics are a per-entry constant cost inside SST builds that are IO
// bound anyway, and properties must accumulate before any consumer flips on.
class TxnMvccPropertiesCollectorFactory : public rocksdb::TablePropertiesCollectorFactory {
 public:
  rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector(
      rocksdb::TablePropertiesCollectorFactory::Context context) override;
  const char* Name() const override { return "dingo.mvcc-properties-collector"; }
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_TXN_MVCC_PROPERTIES_COLLECTOR_H_  // NOLINT

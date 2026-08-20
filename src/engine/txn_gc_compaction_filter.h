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

#ifndef DINGODB_ENGINE_TXN_GC_COMPACTION_FILTER_H_  // NOLINT
#define DINGODB_ENGINE_TXN_GC_COMPACTION_FILTER_H_

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"

namespace dingodb {

class TxnGcCompactionFilterFactory;

// Retry lane for txn data CF orphan deletions the compaction thread could not
// write inline (no_slowdown write rejected/failed). Bounded queue; a dropped
// batch only leaks space, never exposes data: the write CF versions those
// data keys belonged to are already gone.
class TxnGcOrphanCleaner {
 public:
  TxnGcOrphanCleaner() = default;
  ~TxnGcOrphanCleaner();

  TxnGcOrphanCleaner(const TxnGcOrphanCleaner&) = delete;
  TxnGcOrphanCleaner& operator=(const TxnGcOrphanCleaner&) = delete;

  void Start(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* data_cf_handle);
  // Blocks until the worker exits; remaining batches get one final blocking
  // write attempt while the db handle is still valid.
  void Stop();

  // May drop the batch when the queue is full.
  void Submit(std::vector<std::string> keys);

 private:
  void Run();
  bool WriteBatchToDb(const std::vector<std::string>& keys);

  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::vector<std::string>> queue_;
  bool running_{false};
  bool stop_{false};
  rocksdb::DB* db_{nullptr};
  rocksdb::ColumnFamilyHandle* data_cf_handle_{nullptr};
  std::thread worker_;
};

// TiKV-style GC compaction filter for the txn write CF.
//
// One instance serves a single table-file-creation thread (created per
// compaction by the factory), so per-key-group state needs no locking. The
// state machine walks versions of one user key newest-first (key encoding is
// EncodeBytes(user_key) + ~commit_ts big-endian):
//
//   ts > safe_point            -> Keep, group state untouched
//   head Put  (<= safe_point)  -> Keep unconditionally, older versions removed
//   head Delete                -> whole group kept (scan GC owns delete-mark
//                                 groups: it removes them atomically via raft,
//                                 which a local-view filter cannot do safely)
//   head Rollback              -> KEPT: 1PC/async-commit commit_ts is computed
//                                 (not TSO-issued), so another txn's commit
//                                 record can share this exact encoded key;
//                                 kRemoveAndSkipUntil drops every physical
//                                 entry of the key and would destroy that live
//                                 commit. Shadowed rollbacks (below a Put
//                                 head) are removed — any colliding twin there
//                                 is shadowed garbage too.
//   anything unparseable/alien -> Keep (never FATAL inside compaction)
//
// Removal uses kRemoveAndSkipUntil with the key's byte successor so no
// tombstone is written — writing tombstones is the disease being treated.
//
// ENABLEMENT INVARIANTS: the filter's head decision is compaction-local.
// When a Delete mark sits outside the input (memtable/young level) the
// filter removes versions the full view would classify kSkipGroup, and
// replicas diverge physically. That is safe ONLY while scan GC removes
// delete-mark groups as a raft-replicated RANGE per user key (covering
// versions it cannot see) — satisfied in this tree: rocksdb regions use
// ranges in EVERY gc mode (TxnEngineHelper::IsRocksdbRegionForGc), so the
// safety survives flag toggles. Remaining operational preconditions:
// (a) gc_enable_safe_point_read_check on — enforced both by
//     CreateCompactionFilter and by the scan-GC narrowing gate;
// (b) all replicas on a binary understanding raft delete_ranges_with_cf
//     before the flag goes on (older followers ignore the field).
class TxnGcCompactionFilter : public rocksdb::CompactionFilter {
 public:
  TxnGcCompactionFilter(int64_t safe_point_ts, TxnGcCompactionFilterFactory* factory);
  ~TxnGcCompactionFilter() override;

  rocksdb::CompactionFilter::Decision FilterV2(int level, const rocksdb::Slice& key, ValueType value_type,
                                               const rocksdb::Slice& existing_value, std::string* new_value,
                                               std::string* skip_until) const override;

  const char* Name() const override { return "TxnGcCompactionFilter"; }

 private:
  enum class GroupMode : uint8_t {
    kUndecided = 0,   // no version <= safe_point seen yet for this user key
    kRemoveOlder = 1, // head Put kept; every older version is garbage
    kSkipGroup = 2,   // head Delete (or unparseable head): keep whole group
  };

  rocksdb::CompactionFilter::Decision DoFilterV2(const rocksdb::Slice& key, ValueType value_type,
                                                 const rocksdb::Slice& existing_value, std::string* skip_until) const;
  static rocksdb::CompactionFilter::Decision RemoveOne(const rocksdb::Slice& key, std::string* skip_until);
  void EnqueueOrphan(const rocksdb::Slice& encoded_user_key, int64_t start_ts) const;
  void FlushOrphans() const;

  const int64_t safe_point_ts_;
  TxnGcCompactionFilterFactory* const factory_;

  // Single-threaded per-compaction state (FilterV2 is const in the API).
  mutable std::string cur_key_prefix_;
  mutable GroupMode mode_{GroupMode::kUndecided};
  mutable std::vector<std::string> orphan_keys_;
  mutable size_t orphan_bytes_{0};
};

// Registered once on the write CF at DB open; decides per table-file-creation
// whether filtering is on (flag + safe point provider + safe point > 0), so
// runtime toggling needs no DB reopen. Thread-safe: multiple compaction
// threads call CreateCompactionFilter concurrently.
class TxnGcCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  // Returns the filter-effective safe point: min over tenants, or 0 to
  // disable filtering entirely (unknown/stopped/absent safe point).
  using SafePointProvider = std::function<int64_t()>;

  TxnGcCompactionFilterFactory() = default;
  ~TxnGcCompactionFilterFactory() override;

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override;

  const char* Name() const override { return "TxnGcCompactionFilterFactory"; }

  void SetSafePointProvider(SafePointProvider provider);

  // Called after DB::Open with the txn data CF handle; enables orphan
  // deletion and starts the cleaner.
  void SetDBContext(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* data_cf_handle);
  // Called on engine Close AFTER CancelAllBackgroundWork (no live filters)
  // and BEFORE the db handles are destroyed.
  void ClearDBContext();

  // Inline orphan deletion from the compaction thread: no_slowdown write, on
  // failure hand the batch to the cleaner. Never blocks on a write stall.
  void WriteOrphanBatch(std::vector<std::string> keys);

 private:
  std::mutex mutex_;
  SafePointProvider provider_;
  rocksdb::DB* db_{nullptr};
  rocksdb::ColumnFamilyHandle* data_cf_handle_{nullptr};
  std::unique_ptr<TxnGcOrphanCleaner> cleaner_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_TXN_GC_COMPACTION_FILTER_H_  // NOLINT

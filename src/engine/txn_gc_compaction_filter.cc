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

#include "engine/txn_gc_compaction_filter.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "fmt/core.h"

#include "brpc/reloadable_flags.h"
#include "bvar/latency_recorder.h"
#include "bvar/passive_status.h"
#include "bvar/reducer.h"
#include "butil/time.h"
#include "common/gflag_validator.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/serial_helper.h"
#include "gflags/gflags.h"
#include "proto/store.pb.h"
#include "rocksdb/write_batch.h"

namespace dingodb {

DECLARE_bool(gc_enable_safe_point_read_check);

DEFINE_bool(gc_enable_compaction_filter, true,
            "gc the txn write cf inside rocksdb compaction (TiKV-style): shadowed versions are dropped without "
            "writing tombstones; scan GC narrows to delete-mark ranges + rollbacks. Enable ONLY when (a) "
            "gc_enable_safe_point_read_check is on (enforced per compaction and by the scan-GC narrowing) and (b) "
            "EVERY replica runs a binary understanding raft delete_ranges_with_cf — older followers silently ignore "
            "the field and would diverge.");
DEFINE_validator(gc_enable_compaction_filter, &PassBool);

// The four orphan cleaner knobs are read on every enqueue/flush, so a runtime
// change through brpc /flags takes effect immediately.
DEFINE_int32(gc_compaction_filter_orphan_batch_keys, 128,
             "flush the pending txn data cf orphan deletions once this many keys accumulate");
BRPC_VALIDATE_GFLAG(gc_compaction_filter_orphan_batch_keys, brpc::PositiveInteger);
DEFINE_int64(gc_compaction_filter_orphan_batch_bytes, 256 * 1024,
             "flush the pending txn data cf orphan deletions once their keys reach this many bytes");
BRPC_VALIDATE_GFLAG(gc_compaction_filter_orphan_batch_bytes, brpc::PositiveInteger);
DEFINE_int32(gc_compaction_filter_orphan_queue_size, 256,
             "max batches queued for the orphan cleaner retry thread; overflow batches are dropped (space-only leak)");
BRPC_VALIDATE_GFLAG(gc_compaction_filter_orphan_queue_size, brpc::PositiveInteger);
DEFINE_int32(gc_compaction_filter_orphan_retry_num, 3,
             "write attempts per orphan batch on the cleaner thread before dropping it; 0 behaves like 1");
BRPC_VALIDATE_GFLAG(gc_compaction_filter_orphan_retry_num, brpc::NonNegativeInteger);

namespace {

// Mirrors kValidEncodeKeyMinLength in mvcc/codec.cc: EncodeBytes("") is 9
// bytes plus the 8-byte ~ts suffix. Redefined here because the codec helpers
// CHECK-crash on short keys and a compaction thread must never FATAL.
constexpr size_t kEncodedKeyMinLength = 17;

bvar::Adder<int64_t> g_filter_created("dingo_gc_filter_created");
bvar::Adder<int64_t> g_filter_skipped_no_safepoint("dingo_gc_filter_skipped_no_safepoint");
bvar::Adder<int64_t> g_filter_versions_removed("dingo_gc_filter_versions_removed");
bvar::Adder<int64_t> g_filter_rollbacks_removed("dingo_gc_filter_rollbacks_removed");
bvar::Adder<int64_t> g_filter_delete_marks_kept("dingo_gc_filter_delete_marks_kept");
bvar::Adder<int64_t> g_filter_unexpected_op("dingo_gc_filter_unexpected_op");
bvar::Adder<int64_t> g_filter_parse_error("dingo_gc_filter_parse_error");
bvar::Adder<int64_t> g_filter_orphan_deletes("dingo_gc_filter_orphan_deletes");
bvar::Adder<int64_t> g_filter_orphan_fallback_batches("dingo_gc_filter_orphan_fallback_batches");
bvar::Adder<int64_t> g_filter_orphan_dropped_batches("dingo_gc_filter_orphan_dropped_batches");
bvar::Adder<int64_t> g_filter_head_rollbacks_kept("dingo_gc_filter_head_rollbacks_kept");
bvar::Adder<int64_t> g_filter_skipped_guard_off("dingo_gc_filter_skipped_guard_off");
bvar::Adder<int64_t> g_filter_swallowed_exception("dingo_gc_filter_swallowed_exception");
bvar::LatencyRecorder g_filter_orphan_write_latency("dingo_gc_filter_orphan_write");

std::atomic<int64_t> g_effective_safe_point{0};
int64_t GetEffectiveSafePoint(void*) { return g_effective_safe_point.load(std::memory_order_relaxed); }
bvar::PassiveStatus<int64_t> g_filter_safe_point("dingo_gc_filter_safe_point", GetEffectiveSafePoint, nullptr);

// Anomalies come from data corruption and should be ~nonexistent; if one SST
// is bad they arrive by the million, so gate the log, not the counter.
bool ShouldLogAnomaly() {
  static std::atomic<uint64_t> count{0};
  uint64_t n = count.fetch_add(1, std::memory_order_relaxed);
  return n < 16 || (n & 0x3FF) == 0;
}

}  // namespace

// ---------------- TxnGcOrphanCleaner ----------------

TxnGcOrphanCleaner::~TxnGcOrphanCleaner() { Stop(); }

void TxnGcOrphanCleaner::Start(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* data_cf_handle) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (running_) {
    return;
  }
  db_ = db;
  data_cf_handle_ = data_cf_handle;
  stop_ = false;
  running_ = true;
  worker_ = std::thread(&TxnGcOrphanCleaner::Run, this);
}

void TxnGcOrphanCleaner::Stop() {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!running_) {
      return;
    }
    stop_ = true;
  }
  cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
  std::lock_guard<std::mutex> lk(mutex_);
  running_ = false;
}

void TxnGcOrphanCleaner::Submit(std::vector<std::string> keys) {
  if (keys.empty()) {
    return;
  }
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!running_ || stop_ || queue_.size() >= static_cast<size_t>(FLAGS_gc_compaction_filter_orphan_queue_size)) {
      g_filter_orphan_dropped_batches << 1;
      return;
    }
    queue_.emplace_back(std::move(keys));
  }
  cv_.notify_one();
}

bool TxnGcOrphanCleaner::WriteBatchToDb(const std::vector<std::string>& keys) {
  rocksdb::WriteBatch batch;
  for (const auto& key : keys) {
    batch.Delete(data_cf_handle_, key);
  }
  // no_slowdown even here: a stall-blocking write could wedge forever after
  // CancelAllBackgroundWork (nothing left to clear the stall), deadlocking
  // Stop()/Close(). Transient stalls are absorbed by the retry loop instead.
  rocksdb::WriteOptions write_options;
  write_options.no_slowdown = true;
  auto status = db_->Write(write_options, &batch);
  if (status.ok()) {
    g_filter_orphan_deletes << static_cast<int64_t>(keys.size());
    return true;
  }
  DINGO_LOG(WARNING) << fmt::format("[txn_gc_filter] orphan cleaner write failed, keys({}) error({})", keys.size(),
                                    status.ToString());
  return false;
}

void TxnGcOrphanCleaner::Run() {
  while (true) {
    std::vector<std::string> keys;
    bool stopping = false;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
      if (queue_.empty()) {
        break;  // stop_ with a drained queue
      }
      keys = std::move(queue_.front());
      queue_.pop_front();
      stopping = stop_;
    }

    // While stopping, one attempt per batch keeps shutdown bounded.
    int attempts = stopping ? 1 : std::max(1, FLAGS_gc_compaction_filter_orphan_retry_num);
    bool ok = false;
    try {
      for (int i = 0; i < attempts && !ok; ++i) {
        if (i > 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        ok = WriteBatchToDb(keys);
      }
    } catch (...) {
      // An escaped exception would std::terminate the process from a thread.
      g_filter_swallowed_exception << 1;
    }
    if (!ok) {
      g_filter_orphan_dropped_batches << 1;
    }
  }
}

// ---------------- TxnGcCompactionFilter ----------------

TxnGcCompactionFilter::TxnGcCompactionFilter(int64_t safe_point_ts, TxnGcCompactionFilterFactory* factory)
    : safe_point_ts_(safe_point_ts), factory_(factory) {}

TxnGcCompactionFilter::~TxnGcCompactionFilter() {
  try {
    FlushOrphans();
  } catch (...) {
    // Destructor runs inside rocksdb's table-creation thread; an escaped
    // exception is UB there. The batch is only garbage — drop it.
    g_filter_swallowed_exception << 1;
  }
}

rocksdb::CompactionFilter::Decision TxnGcCompactionFilter::RemoveOne(const rocksdb::Slice& key,
                                                                     std::string* skip_until) {
  // Byte successor via +1 on the trailing big-endian ~ts, which is exactly
  // EncodeKey(user_key, ts-1); callers guarantee ts >= 1 so the carry never
  // reaches the user key. Covers [key, successor) == this single version, and
  // physically drops it instead of converting it to a tombstone.
  skip_until->assign(key.data(), key.size());
  for (size_t i = skip_until->size(); i-- > skip_until->size() - 8;) {
    auto& byte = reinterpret_cast<unsigned char&>((*skip_until)[i]);
    if (++byte != 0) {
      break;
    }
  }
  return rocksdb::CompactionFilter::Decision::kRemoveAndSkipUntil;
}

void TxnGcCompactionFilter::EnqueueOrphan(const rocksdb::Slice& encoded_user_key, int64_t start_ts) const {
  std::string data_key;
  data_key.reserve(encoded_user_key.size() + 8);
  data_key.assign(encoded_user_key.data(), encoded_user_key.size());
  SerialHelper::WriteLongWithNegation(start_ts, data_key);

  orphan_bytes_ += data_key.size();
  orphan_keys_.emplace_back(std::move(data_key));

  if (orphan_keys_.size() >= static_cast<size_t>(FLAGS_gc_compaction_filter_orphan_batch_keys) ||
      orphan_bytes_ >= static_cast<size_t>(FLAGS_gc_compaction_filter_orphan_batch_bytes)) {
    FlushOrphans();
  }
}

void TxnGcCompactionFilter::FlushOrphans() const {
  if (orphan_keys_.empty()) {
    return;
  }
  factory_->WriteOrphanBatch(std::move(orphan_keys_));
  orphan_keys_.clear();
  orphan_bytes_ = 0;
}

rocksdb::CompactionFilter::Decision TxnGcCompactionFilter::FilterV2(int /*level*/, const rocksdb::Slice& key,
                                                                    ValueType value_type,
                                                                    const rocksdb::Slice& existing_value,
                                                                    std::string* /*new_value*/,
                                                                    std::string* skip_until) const {
  // rocksdb is not exception safe: anything escaping here is UB (data loss,
  // deadlock). Keeping the entry is always a safe answer in this design.
  try {
    return DoFilterV2(key, value_type, existing_value, skip_until);
  } catch (...) {
    g_filter_swallowed_exception << 1;
    return Decision::kKeep;
  }
}

rocksdb::CompactionFilter::Decision TxnGcCompactionFilter::DoFilterV2(const rocksdb::Slice& key, ValueType value_type,
                                                                      const rocksdb::Slice& existing_value,
                                                                      std::string* skip_until) const {
  if (value_type != ValueType::kValue) {
    return Decision::kKeep;
  }

  if (key.size() < kEncodedKeyMinLength) {
    g_filter_parse_error << 1;
    DINGO_LOG_IF(WARNING, ShouldLogAnomaly()) << fmt::format(
        "[txn_gc_filter] key too short for encode key, keep. key: {}", Helper::StringToHex(key.ToStringView()));
    return Decision::kKeep;
  }

  std::string_view prefix(key.data(), key.size() - 8);
  if (prefix != std::string_view(cur_key_prefix_)) {
    cur_key_prefix_.assign(prefix.data(), prefix.size());
    mode_ = GroupMode::kUndecided;
  }

  int64_t ts = SerialHelper::ReadLongWithNegation(std::string_view(key.data() + key.size() - 8, 8));
  if (ts > safe_point_ts_) {
    // Live version: keep without parsing. Deliberately NOT the scan-GC "A
    // rule" (which may drop the newest Put <= safe point when newer versions
    // exist): in a compaction's partial view that would expose an even older
    // version from an unseen level to reads in (safe_point, newer_commit_ts).
    return Decision::kKeep;
  }
  if (ts <= 0) {
    g_filter_parse_error << 1;
    DINGO_LOG_IF(WARNING, ShouldLogAnomaly()) << fmt::format("[txn_gc_filter] non-positive ts({}) in key, keep. key: {}",
                                                             ts, Helper::StringToHex(key.ToStringView()));
    return Decision::kKeep;
  }

  if (mode_ == GroupMode::kSkipGroup) {
    return Decision::kKeep;
  }

  pb::store::WriteInfo write_info;
  if (!write_info.ParseFromArray(existing_value.data(), existing_value.size())) {
    g_filter_parse_error << 1;
    DINGO_LOG_IF(WARNING, ShouldLogAnomaly()) << fmt::format(
        "[txn_gc_filter] parse WriteInfo failed, keep. key: {} value: {}", Helper::StringToHex(key.ToStringView()),
        Helper::StringToHex(existing_value.ToStringView()));
    if (mode_ == GroupMode::kUndecided) {
      // Unknown head could be a delete mark; keep the whole group so no
      // shadowed version is dropped on evidence we could not read.
      mode_ = GroupMode::kSkipGroup;
    }
    return Decision::kKeep;
  }

  const auto op = write_info.op();

  if (mode_ == GroupMode::kUndecided) {
    switch (op) {
      case pb::store::Op::Put:
        // Newest Put at or below the safe point: kept unconditionally, all
        // older versions of this user key are shadowed garbage.
        mode_ = GroupMode::kRemoveOlder;
        return Decision::kKeep;
      case pb::store::Op::Delete:
        // Delete-mark groups belong to scan GC: it enumerates the whole group
        // from a full snapshot and removes it atomically via raft, which a
        // local-view filter cannot do without risking follower resurrection.
        mode_ = GroupMode::kSkipGroup;
        g_filter_delete_marks_kept << 1;
        return Decision::kKeep;
      case pb::store::Op::Rollback:
        // Head-position rollbacks are KEPT: commit_ts of 1PC/async commit is
        // computed rather than TSO-issued, so another txn's live commit
        // record can share this exact encoded key (commit_ts == this
        // rollback's start_ts). kRemoveAndSkipUntil drops every physical
        // entry of the key and would destroy that commit. Scan GC owns
        // rollback cleanup. The head slot stays undecided — the next version
        // below determines the group's fate.
        g_filter_head_rollbacks_kept << 1;
        return Decision::kKeep;
      default:
        g_filter_unexpected_op << 1;
        DINGO_LOG_IF(WARNING, ShouldLogAnomaly())
            << fmt::format("[txn_gc_filter] unexpected op({}) in write cf, keep. key: {}",
                           pb::store::Op_Name(op), Helper::StringToHex(key.ToStringView()));
        return Decision::kKeep;
    }
  }

  // GroupMode::kRemoveOlder
  switch (op) {
    case pb::store::Op::Put:
      if (write_info.short_value().empty()) {
        if (write_info.start_ts() > 0) {
          EnqueueOrphan(rocksdb::Slice(prefix.data(), prefix.size()), write_info.start_ts());
        } else {
          g_filter_parse_error << 1;
          DINGO_LOG_IF(WARNING, ShouldLogAnomaly())
              << fmt::format("[txn_gc_filter] shadowed Put with non-positive start_ts({}), data cf orphan skipped. "
                             "key: {}",
                             write_info.start_ts(), Helper::StringToHex(key.ToStringView()));
        }
      }
      g_filter_versions_removed << 1;
      return RemoveOne(key, skip_until);
    case pb::store::Op::Delete:
      g_filter_versions_removed << 1;
      return RemoveOne(key, skip_until);
    case pb::store::Op::Rollback:
      // Below a kept Put head everything at this encoded key is shadowed —
      // including a computed-commit_ts twin, if any — so removal is safe
      // here, unlike at head position.
      g_filter_rollbacks_removed << 1;
      return RemoveOne(key, skip_until);
    default:
      g_filter_unexpected_op << 1;
      DINGO_LOG_IF(WARNING, ShouldLogAnomaly())
          << fmt::format("[txn_gc_filter] unexpected op({}) below head, keep. key: {}", pb::store::Op_Name(op),
                         Helper::StringToHex(key.ToStringView()));
      return Decision::kKeep;
  }
}

// ---------------- TxnGcCompactionFilterFactory ----------------

TxnGcCompactionFilterFactory::~TxnGcCompactionFilterFactory() { ClearDBContext(); }

void TxnGcCompactionFilterFactory::SetSafePointProvider(SafePointProvider provider) {
  std::lock_guard<std::mutex> lk(mutex_);
  provider_ = std::move(provider);
}

void TxnGcCompactionFilterFactory::SetDBContext(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* data_cf_handle) {
  ClearDBContext();
  std::lock_guard<std::mutex> lk(mutex_);
  db_ = db;
  data_cf_handle_ = data_cf_handle;
  cleaner_ = std::make_unique<TxnGcOrphanCleaner>();
  cleaner_->Start(db, data_cf_handle);
}

void TxnGcCompactionFilterFactory::ClearDBContext() {
  std::unique_ptr<TxnGcOrphanCleaner> cleaner;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    cleaner = std::move(cleaner_);
    db_ = nullptr;
    data_cf_handle_ = nullptr;
  }
  if (cleaner != nullptr) {
    cleaner->Stop();
  }
}

std::unique_ptr<rocksdb::CompactionFilter> TxnGcCompactionFilterFactory::CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& /*context*/) {
  if (!FLAGS_gc_enable_compaction_filter) {
    g_effective_safe_point.store(0, std::memory_order_relaxed);
    return nullptr;
  }

  if (!FLAGS_gc_enable_safe_point_read_check) {
    // The S0 read guard is the only defense left for a read whose snapshot is
    // taken below the safe point (filters ignore rocksdb snapshots); without
    // it such reads would silently return wrong data. Refuse to filter.
    g_effective_safe_point.store(0, std::memory_order_relaxed);
    g_filter_skipped_guard_off << 1;
    DINGO_LOG_IF(ERROR, ShouldLogAnomaly())
        << "[txn_gc_filter] gc_enable_compaction_filter is on but gc_enable_safe_point_read_check is off; "
           "refusing to filter until the read guard is enabled.";
    return nullptr;
  }

  SafePointProvider provider;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (provider_ == nullptr || db_ == nullptr || data_cf_handle_ == nullptr) {
      // Not fully wired (startup window, shutdown, or misconfigured role):
      // keep everything.
      g_effective_safe_point.store(0, std::memory_order_relaxed);
      return nullptr;
    }
    provider = provider_;
  }

  // Invoked outside mutex_: the provider takes its own locks.
  int64_t safe_point_ts = provider();
  g_effective_safe_point.store(safe_point_ts, std::memory_order_relaxed);
  if (safe_point_ts <= 0) {
    g_filter_skipped_no_safepoint << 1;
    return nullptr;
  }

  g_filter_created << 1;
  return std::make_unique<TxnGcCompactionFilter>(safe_point_ts, this);
}

void TxnGcCompactionFilterFactory::WriteOrphanBatch(std::vector<std::string> keys) try {
  if (keys.empty()) {
    return;
  }

  rocksdb::DB* db = nullptr;
  rocksdb::ColumnFamilyHandle* data_cf_handle = nullptr;
  TxnGcOrphanCleaner* cleaner = nullptr;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    db = db_;
    data_cf_handle = data_cf_handle_;
    cleaner = cleaner_.get();
  }
  if (db == nullptr || data_cf_handle == nullptr) {
    g_filter_orphan_dropped_batches << 1;
    return;
  }

  rocksdb::WriteBatch batch;
  for (const auto& key : keys) {
    batch.Delete(data_cf_handle, key);
  }

  // The compaction thread must never block on a write stall (the stall may be
  // waiting on this very compaction — deadlock), hence no_slowdown.
  rocksdb::WriteOptions write_options;
  write_options.no_slowdown = true;

  int64_t start_us = butil::gettimeofday_us();
  auto status = db->Write(write_options, &batch);
  g_filter_orphan_write_latency << (butil::gettimeofday_us() - start_us);

  if (status.ok()) {
    g_filter_orphan_deletes << static_cast<int64_t>(keys.size());
    return;
  }

  g_filter_orphan_fallback_batches << 1;
  if (cleaner != nullptr) {
    cleaner->Submit(std::move(keys));
  } else {
    g_filter_orphan_dropped_batches << 1;
  }
} catch (...) {
  // Called from compaction threads (inline or via the filter destructor);
  // an escaped exception is UB inside rocksdb. Dropping the batch only
  // leaks already-invisible space.
  g_filter_swallowed_exception << 1;
}

}  // namespace dingodb

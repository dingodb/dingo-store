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

#include "engine/txn_mvcc_properties_collector.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>

#include "bvar/reducer.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/serial_helper.h"
#include "fmt/core.h"
#include "proto/store.pb.h"

namespace dingodb {

namespace {

// Mirrors kValidEncodeKeyMinLength in mvcc/codec.cc: EncodeBytes("") is 9
// bytes plus the 8-byte ~ts suffix. Redefined here (same as the gc filter)
// because the codec helpers CHECK-crash on short keys and a table-building
// thread must never FATAL.
constexpr size_t kEncodedKeyMinLength = 17;

bvar::Adder<int64_t> g_mvcc_collector_finished("dingo_mvcc_collector_finished");
bvar::Adder<int64_t> g_mvcc_collector_parse_error("dingo_mvcc_collector_parse_error");
bvar::Adder<int64_t> g_mvcc_collector_swallowed_exception("dingo_mvcc_collector_swallowed_exception");

// Anomalies come from data corruption and should be ~nonexistent; if one SST
// is bad they arrive by the million, so gate the log, not the counter.
bool ShouldLogAnomaly() {
  static std::atomic<uint64_t> count{0};
  uint64_t n = count.fetch_add(1, std::memory_order_relaxed);
  return n < 16 || (n & 0x3FF) == 0;
}

// 0 means "no data yet" for every ts bound in TxnMvccProperties.
int64_t MergeOldestTs(int64_t a, int64_t b) {
  if (a == 0) return b;
  if (b == 0) return a;
  return std::min(a, b);
}

int64_t MergeNewestTs(int64_t a, int64_t b) { return std::max(a, b); }

void PutI64(rocksdb::UserCollectedProperties* out, const char* key, int64_t value) {
  (*out)[key] = std::to_string(value);
}

bool GetI64(const rocksdb::UserCollectedProperties& props, const char* key, int64_t* value) {
  auto it = props.find(key);
  if (it == props.end() || it->second.empty()) {
    return false;
  }
  errno = 0;
  char* end = nullptr;
  int64_t parsed = std::strtoll(it->second.c_str(), &end, 10);
  if (errno != 0 || end == nullptr || *end != '\0') {
    return false;
  }
  *value = parsed;
  return true;
}

}  // namespace

void TxnMvccProperties::Add(const TxnMvccProperties& other) {
  min_ts = MergeOldestTs(min_ts, other.min_ts);
  max_ts = MergeNewestTs(max_ts, other.max_ts);
  num_rows += other.num_rows;
  num_puts += other.num_puts;
  num_deletes += other.num_deletes;
  num_rollbacks += other.num_rollbacks;
  num_versions += other.num_versions;
  max_row_versions = std::max(max_row_versions, other.max_row_versions);
  oldest_stale_version_ts = MergeOldestTs(oldest_stale_version_ts, other.oldest_stale_version_ts);
  newest_stale_version_ts = MergeNewestTs(newest_stale_version_ts, other.newest_stale_version_ts);
  oldest_delete_ts = MergeOldestTs(oldest_delete_ts, other.oldest_delete_ts);
  newest_delete_ts = MergeNewestTs(newest_delete_ts, other.newest_delete_ts);
  num_errors += other.num_errors;
}

void TxnMvccProperties::EncodeTo(rocksdb::UserCollectedProperties* out) const {
  PutI64(out, kMvccPropMinTs, min_ts);
  PutI64(out, kMvccPropMaxTs, max_ts);
  PutI64(out, kMvccPropNumRows, num_rows);
  PutI64(out, kMvccPropNumPuts, num_puts);
  PutI64(out, kMvccPropNumDeletes, num_deletes);
  PutI64(out, kMvccPropNumRollbacks, num_rollbacks);
  PutI64(out, kMvccPropNumVersions, num_versions);
  PutI64(out, kMvccPropMaxRowVersions, max_row_versions);
  PutI64(out, kMvccPropOldestStaleTs, oldest_stale_version_ts);
  PutI64(out, kMvccPropNewestStaleTs, newest_stale_version_ts);
  PutI64(out, kMvccPropOldestDeleteTs, oldest_delete_ts);
  PutI64(out, kMvccPropNewestDeleteTs, newest_delete_ts);
  PutI64(out, kMvccPropNumErrors, num_errors);
}

bool TxnMvccProperties::DecodeFrom(const rocksdb::UserCollectedProperties& props, TxnMvccProperties* out) {
  // Every key is always written together by EncodeTo, so all-or-nothing:
  // a partially damaged block is treated the same as an absent one.
  TxnMvccProperties parsed;
  bool ok = GetI64(props, kMvccPropMinTs, &parsed.min_ts) && GetI64(props, kMvccPropMaxTs, &parsed.max_ts) &&
            GetI64(props, kMvccPropNumRows, &parsed.num_rows) && GetI64(props, kMvccPropNumPuts, &parsed.num_puts) &&
            GetI64(props, kMvccPropNumDeletes, &parsed.num_deletes) &&
            GetI64(props, kMvccPropNumRollbacks, &parsed.num_rollbacks) &&
            GetI64(props, kMvccPropNumVersions, &parsed.num_versions) &&
            GetI64(props, kMvccPropMaxRowVersions, &parsed.max_row_versions) &&
            GetI64(props, kMvccPropOldestStaleTs, &parsed.oldest_stale_version_ts) &&
            GetI64(props, kMvccPropNewestStaleTs, &parsed.newest_stale_version_ts) &&
            GetI64(props, kMvccPropOldestDeleteTs, &parsed.oldest_delete_ts) &&
            GetI64(props, kMvccPropNewestDeleteTs, &parsed.newest_delete_ts) &&
            GetI64(props, kMvccPropNumErrors, &parsed.num_errors);
  if (!ok) {
    return false;
  }
  *out = parsed;
  return true;
}

rocksdb::Status TxnMvccPropertiesCollector::AddUserKey(const rocksdb::Slice& key, const rocksdb::Slice& value,
                                                       rocksdb::EntryType type, rocksdb::SequenceNumber /*seq*/,
                                                       uint64_t /*file_size*/) {
  // rocksdb is not exception safe: anything escaping a collector callback is
  // UB (the header says so explicitly). Miscounting one entry is always the
  // better failure.
  try {
    if (type != rocksdb::kEntryPut && type != rocksdb::kEntryDelete) {
      // No Merge/Titan in dingo; range tombstones (scan GC's raft group
      // deletes) do not arrive per-key here — they are read back from the
      // built-in num_range_deletions instead.
      return rocksdb::Status::OK();
    }
    if (key.size() < kEncodedKeyMinLength) {
      props_.num_errors++;
      g_mvcc_collector_parse_error << 1;
      DINGO_LOG_IF(WARNING, ShouldLogAnomaly()) << fmt::format(
          "[mvcc_collector] key too short for encode key, skip. key: {}", Helper::StringToHex(key.ToStringView()));
      return rocksdb::Status::OK();
    }
    int64_t ts = SerialHelper::ReadLongWithNegation(std::string_view(key.data() + key.size() - 8, 8));
    if (ts <= 0) {
      props_.num_errors++;
      g_mvcc_collector_parse_error << 1;
      DINGO_LOG_IF(WARNING, ShouldLogAnomaly()) << fmt::format(
          "[mvcc_collector] non-positive ts({}) in key, skip. key: {}", ts, Helper::StringToHex(key.ToStringView()));
      return rocksdb::Status::OK();
    }

    // Tombstones update the ts bounds too, then stop: keeping them out of
    // num_versions is what lets a reader derive them from built-in counters.
    props_.min_ts = MergeOldestTs(props_.min_ts, ts);
    props_.max_ts = MergeNewestTs(props_.max_ts, ts);
    if (type == rocksdb::kEntryDelete) {
      return rocksdb::Status::OK();
    }

    props_.num_versions++;
    std::string_view user_key(key.data(), key.size() - 8);
    if (user_key != std::string_view(cur_user_key_)) {
      props_.num_rows++;
      cur_row_versions_ = 1;
      cur_user_key_.assign(user_key.data(), user_key.size());
    } else {
      cur_row_versions_++;
      // Keys sort newest-first within a user key (~ts suffix), so every
      // non-first version in this file is shadowed by a newer one.
      props_.oldest_stale_version_ts = MergeOldestTs(props_.oldest_stale_version_ts, ts);
      props_.newest_stale_version_ts = MergeNewestTs(props_.newest_stale_version_ts, ts);
    }
    props_.max_row_versions = std::max(props_.max_row_versions, cur_row_versions_);

    pb::store::WriteInfo write_info;
    if (!write_info.ParseFromArray(value.data(), value.size())) {
      props_.num_errors++;
      g_mvcc_collector_parse_error << 1;
      DINGO_LOG_IF(WARNING, ShouldLogAnomaly()) << fmt::format(
          "[mvcc_collector] parse WriteInfo failed, skip. key: {} value: {}", Helper::StringToHex(key.ToStringView()),
          Helper::StringToHex(value.ToStringView()));
      return rocksdb::Status::OK();
    }
    switch (write_info.op()) {
      case pb::store::Op::Put:
        props_.num_puts++;
        break;
      case pb::store::Op::Delete:
        props_.num_deletes++;
        props_.oldest_delete_ts = MergeOldestTs(props_.oldest_delete_ts, ts);
        props_.newest_delete_ts = MergeNewestTs(props_.newest_delete_ts, ts);
        break;
      case pb::store::Op::Rollback:
        props_.num_rollbacks++;
        break;
      default:
        props_.num_errors++;
        DINGO_LOG_IF(WARNING, ShouldLogAnomaly())
            << fmt::format("[mvcc_collector] unexpected op({}) in write cf, skip. key: {}",
                           pb::store::Op_Name(write_info.op()), Helper::StringToHex(key.ToStringView()));
        break;
    }
  } catch (...) {
    g_mvcc_collector_swallowed_exception << 1;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TxnMvccPropertiesCollector::Finish(rocksdb::UserCollectedProperties* properties) {
  // A non-OK status would drop the file's whole property block; whatever was
  // counted is always worth keeping.
  try {
    props_.EncodeTo(properties);
    g_mvcc_collector_finished << 1;
  } catch (...) {
    g_mvcc_collector_swallowed_exception << 1;
  }
  return rocksdb::Status::OK();
}

rocksdb::UserCollectedProperties TxnMvccPropertiesCollector::GetReadableProperties() const {
  rocksdb::UserCollectedProperties props;
  try {
    props_.EncodeTo(&props);
  } catch (...) {
    g_mvcc_collector_swallowed_exception << 1;
  }
  return props;
}

rocksdb::TablePropertiesCollector* TxnMvccPropertiesCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context /*context*/) {
  return new TxnMvccPropertiesCollector();
}

}  // namespace dingodb

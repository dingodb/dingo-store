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

#ifndef DINGODB_ROCKS_LOG_STORAGE_H_
#define DINGODB_ROCKS_LOG_STORAGE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "braft/log_entry.h"
#include "braft/storage.h"
#include "bthread/execution_queue.h"
#include "butil/iobuf.h"
#include "common/synchronization.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/checkpoint.h"

namespace dingodb {

namespace wal {

enum class ClientType {
  kRaft,
  kVectorIndex,
};

std::string ClientTypeName(ClientType client_type);

enum class LogEntryType {
  kEntryTypeUnknown = 0,
  kEntryTypeNoOp = 1,
  kEntryTypeData = 2,
  kEntryTypeConfiguration = 3,
  kEntryTypeTombstone = 4,
};

std::string LogEntryTypeName(LogEntryType type);

struct LogIndexMeta {
  std::atomic<int64_t> first_index{1};
  std::atomic<int64_t> last_index{0};

  std::vector<std::pair<ClientType, int64_t>> truncate_prefixs;

  LogIndexMeta() = default;

  LogIndexMeta(const LogIndexMeta& log_index_meta) {
    this->first_index.store(log_index_meta.first_index, std::memory_order_release);
    this->last_index.store(log_index_meta.last_index, std::memory_order_release);
    this->truncate_prefixs = log_index_meta.truncate_prefixs;
  }

  LogIndexMeta(LogIndexMeta&& log_index_meta) noexcept {
    this->first_index.store(log_index_meta.first_index, std::memory_order_release);
    this->last_index.store(log_index_meta.last_index, std::memory_order_release);
    this->truncate_prefixs.swap(log_index_meta.truncate_prefixs);
  }

  LogIndexMeta& operator=(const LogIndexMeta& log_index_meta) {
    this->first_index.store(log_index_meta.first_index, std::memory_order_release);
    this->last_index.store(log_index_meta.last_index, std::memory_order_release);
    this->truncate_prefixs = log_index_meta.truncate_prefixs;

    return *this;
  }
};

struct LogEntry;
using LogEntryPtr = std::shared_ptr<LogEntry>;

struct LogEntry {
  LogEntryType type{LogEntryType::kEntryTypeUnknown};
  int64_t region_id{0};
  int64_t term{0};
  int64_t index{0};
  std::string out_data;
  const butil::IOBuf* in_data{nullptr};

  static LogEntryPtr New() { return std::make_shared<LogEntry>(); }

  LogEntry() = default;

  LogEntry(LogEntry&& entry) noexcept {
    this->type = entry.type;
    this->region_id = entry.region_id;
    this->term = entry.term;
    this->index = entry.index;
    this->out_data.swap(entry.out_data);
    this->in_data = entry.in_data;
    entry.in_data = nullptr;
  }

  void Print() const;
};

struct Mutation {
  enum class Type {
    kAppendLogEntry = 0,
    kTruncatePrefix = 1,
    kTruncateSuffix = 2,
    kReset = 3,
    kDestroy = 4,
  };

  Type type{Type::kAppendLogEntry};

  int64_t region_id{0};

  int64_t start_time{0};

  // append log entries
  std::vector<LogEntry> log_entries;

  // truncate prefix or suffix [start_index, end_index)
  ClientType client_type{ClientType::kRaft};
  int64_t start_index{0};
  int64_t end_index{0};

  // reset
  int64_t reset_index{0};

  BthreadCond cond;
  bool ret{true};

  void Print() const;
};

struct WriteOp {
  enum class Type {
    kPut = 0,
    kDeleteRange = 1,
  };

  Type type;
  std::string key_or_start_key;
  std::string value_or_end_key;

  WriteOp() = default;

  WriteOp(const WriteOp& write_op) noexcept {
    this->type = write_op.type;
    this->key_or_start_key = write_op.key_or_start_key;
    this->value_or_end_key = write_op.value_or_end_key;
  }

  WriteOp(WriteOp&& write_op) noexcept {
    this->type = write_op.type;
    this->key_or_start_key.swap(write_op.key_or_start_key);
    this->value_or_end_key.swap(write_op.value_or_end_key);
  }

  int64_t Size() const { return key_or_start_key.size() + value_or_end_key.size(); }
};

class RocksLogStorage;
using RocksLogStoragePtr = std::shared_ptr<RocksLogStorage>;
using LogStoragePtr = std::shared_ptr<RocksLogStorage>;

class RocksLogStorage {
 public:
  RocksLogStorage(const std::string& path) : path_(path) {}
  ~RocksLogStorage() = default;

  static LogStoragePtr New(const std::string& path) { return std::make_shared<RocksLogStorage>(path); }

  bool Init();
  bool Close();

  void RegisterClientType(ClientType client_type);
  bool RegisterRegion(int64_t region_id);

  bool CommitMutation(Mutation* mutation);

  static size_t AppendToWriteBatch(const Mutation* mutation, std::vector<WriteOp>& write_ops);
  bool ApplyWriteOp(const std::vector<WriteOp>& write_ops);
  void AdjustIndexMeta(const std::vector<Mutation*>& mutations);

  bool SyncWal();

  int64_t FirstLogIndex(int64_t region_id);
  int64_t LastLogIndex(int64_t region_id);

  int64_t GetTerm(int64_t region_id, int64_t index);

  LogEntryPtr GetEntry(int64_t region_id, int64_t index);
  std::vector<LogEntryPtr> GetEntries(int64_t region_id, int64_t start_index, int64_t end_index);
  std::vector<LogEntryPtr> GetDataEntries(int64_t region_id, int64_t start_index, int64_t end_index);

  std::vector<LogEntry> GetConfigurations(int64_t region_id);

  // delete logs from storage's head, [1, first_index_kept) will be discarded
  int TruncatePrefix(ClientType client_type, int64_t region_id, int64_t keep_first_index);

  // delete uncommitted logs from storage's tail, (keep_last_index, infinity) will be discarded
  int TruncateSuffix(ClientType client_type, int64_t region_id, int64_t keep_last_index);

  bool DestroyRegionLog(int64_t region_id);

  using RockMatchFuncer = std::function<bool(const LogEntry&)>;
  bool HasSpecificLog(int64_t region_id, int64_t begin_index, int64_t end_index, RockMatchFuncer matcher);

  bool GetLogIndexMeta(int64_t region_id, LogIndexMeta& log_index_meta);
  std::map<int64_t, LogIndexMeta> GetLogIndexMeta(const std::vector<int64_t>& region_ids, bool is_actual);
  void PrintLogIndexMeta();

 private:
  bool InitExecutionQueue();
  bool CloseExecutionQueue();

  bool InitRocksDB();
  bool CloseRocksDB();

  bool InitIndexMeta();
  bool SaveIndexMeta(int64_t region_id, const LogIndexMeta& log_index_meta);
  void DeleteIndexMeta(int64_t region_id);
  bool IsExistIndexMeta(int64_t region_id);

  void UpdateFirstIndex(int64_t region_id, int64_t first_index);
  void UpdateLastIndex(int64_t region_id, int64_t last_index);
  void UpdateLastIndex(const std::vector<LogEntry>& log_entries);
  void ResetFirstAndLastIndex(int64_t region_id, int64_t first_index, int64_t last_index);

  int64_t GetFirstLogIndex(int64_t region_id);
  int64_t GetLastLogIndex(int64_t region_id);

  bool DeleteRange(const std::string& start_key, const std::string& end_key);

  std::string path_;

  std::vector<ClientType> client_types_;

  RWLock rw_lock_;
  // regoin index range cache, if not exist then take from rocksdb
  // region_id: [first_index, last_index]
  std::map<int64_t, LogIndexMeta> log_index_metas_;

  std::shared_ptr<rocksdb::DB> db_;
  std::shared_ptr<rocksdb::RateLimiter> rate_limiter_;
  std::vector<rocksdb::ColumnFamilyHandle*> family_handles_;

  bthread::ExecutionQueueId<Mutation*> queue_id_;
};

class RocksLogStorageWrapper : public braft::LogStorage {
 public:
  explicit RocksLogStorageWrapper(int64_t region_id, RocksLogStoragePtr log_storage)
      : region_id_(region_id), log_storage_(log_storage) {}
  ~RocksLogStorageWrapper() override = default;

  // init logstorage, check consistency and integrity
  int init(braft::ConfigurationManager* configuration_manager) override;

  // first log index in log
  int64_t first_log_index() override;

  // last log index in log
  int64_t last_log_index() override;

  // get logentry by index
  braft::LogEntry* get_entry(const int64_t index) override;  // NOLINT

  // get logentry's term by index
  int64_t get_term(const int64_t index) override;  // NOLINT

  // append entry to log
  int append_entry(const braft::LogEntry* entry) override;

  // append entries to log and update IOMetric, return success append number
  int append_entries(const std::vector<braft::LogEntry*>& entries, braft::IOMetric* metric) override;

  // delete logs from storage's head, [1, first_index_kept) will be discarded
  int truncate_prefix(const int64_t keep_first_index) override;  // NOLINT

  // delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
  int truncate_suffix(const int64_t keep_last_index) override;  // NOLINT

  int reset(const int64_t next_log_index) override;  // NOLINT

  braft::LogStorage* new_instance(const std::string& uri) const override;

  butil::Status gc_instance(const std::string& uri) const override;

 private:
  static LogEntry ToLogEntry(int64_t region_id, const braft::LogEntry* entry);
  static braft::LogEntry* ToBraftLogEntry(LogEntryPtr log_entry);
  static braft::LogEntry* ToBraftLogEntry(const LogEntry& log_entry);

  int64_t region_id_;
  RocksLogStoragePtr log_storage_;
};

}  // namespace wal

}  // namespace dingodb

#endif  // DINGODB_ROCKS_LOG_STORAGE_H_

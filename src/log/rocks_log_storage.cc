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

#include "log/rocks_log_storage.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "brpc/reloadable_flags.h"
#include "butil/compiler_specific.h"
#include "butil/iobuf.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/store_internal.pb.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"

namespace dingodb {

namespace wal {

const char kLogDataPrefix = 'D';
const char kLogIndexMetaPrefix = 'M';

DEFINE_int32(rocks_log_max_write_batch_size, 512, "rocks log storage max write batch size");
BRPC_VALIDATE_GFLAG(rocks_log_max_write_batch_size, brpc::PositiveInteger);

DEFINE_int32(rocks_log_max_mutation_batch_size, 256, "rocks log storage max mutation batch size");
BRPC_VALIDATE_GFLAG(rocks_log_max_mutation_batch_size, brpc::PositiveInteger);

DEFINE_int32(rocks_log_write_wait_time_ms, 2, "rocks log storage write wait time(ms)");
BRPC_VALIDATE_GFLAG(rocks_log_write_wait_time_ms, brpc::PositiveInteger);

DEFINE_int32(rocks_log_recycle_file_num, 8, "rocks log storage recycle log file num");
BRPC_VALIDATE_GFLAG(rocks_log_recycle_file_num, brpc::PositiveInteger);

static bvar::LatencyRecorder g_append_entry_total_latency("dingo_rocks_raft_log_total");
static bvar::LatencyRecorder g_append_entry_wait_latency("dingo_rocks_raft_log_wait");
static bvar::LatencyRecorder g_write_latency("dingo_rocks_raft_log_write");
static bvar::LatencyRecorder g_write_size("dingo_rocks_raft_log_write_size");
static bvar::LatencyRecorder g_sync_wal_latency("dingo_rocks_raft_log_sync");

static bool IsLE() {
  uint32_t i = 1;
  char* c = (char*)&i;
  return *c == 1;
}

class Codec {
 public:
  static void WriteLong(int64_t l, char* buf) {
    static const bool kIsLe = IsLE();

    uint64_t* ll = (uint64_t*)&l;
    unsigned char* data = (unsigned char*)buf;

    if (kIsLe) {
      data[0] = *ll >> 56;
      data[1] = *ll >> 48;
      data[2] = *ll >> 40;
      data[3] = *ll >> 32;
      data[4] = *ll >> 24;
      data[5] = *ll >> 16;
      data[6] = *ll >> 8;
      data[7] = *ll;
    } else {
      memcpy(data, ll, 8);
    }
  }

  static int64_t ReadLong(const char* buf) {
    static const bool kIsLe = IsLE();

    uint64_t l;
    const unsigned char* data = (const unsigned char*)buf;

    if (kIsLe) {
      l = (data[0] & 0xFF);
      for (int i = 1; i < 8; ++i) {
        l <<= 8;
        l |= (data[i] & 0xFF);
      }

    } else {
      memcpy(&l, data, 8);
    }

    return l;
  }

  // key: 16byte
  // | region_id(8byte) | index(8byte) |
  static std::string EncodeKey(int64_t region_id, int64_t index) {
    std::string key(17, 0);

    key[0] = kLogDataPrefix;

    WriteLong(region_id, key.data() + 1);
    WriteLong(index, key.data() + 9);

    return key;
  }

  static void DecodeKey(const std::string_view& key, int64_t& region_id, int64_t& index) {
    CHECK(key.size() == 17) << fmt::format("key length({}) is invalid.", key.size());

    region_id = ReadLong(key.data() + 1);
    index = ReadLong(key.data() + 9);
  }

  // value
  // | data(nbyte) | type(4byte) |  term(8byte)  |
  static std::string EncodeValue(int type, int64_t term, const butil::IOBuf* data) {
    std::string value(12 + data->size(), 0);

    size_t offset = 0;
    const auto* inner_data = data->fetch(value.data(), data->size());
    CHECK(inner_data != nullptr) << "IOBfu fetch data fail.";
    if (inner_data != value.data()) {
      memcpy(value.data() + offset, inner_data, data->size());
    }

    offset += data->size();
    memcpy(value.data() + offset, &type, 4);

    offset += 4;
    memcpy(value.data() + offset, &term, 8);

    return std::move(value);
  }

  static void DecodeValue(const std::string& value, int& type, int64_t& term) {
    CHECK(value.size() > 12) << fmt::format("value length({}) is invalid.", value.size());

    memcpy(&type, value.data() + value.size() - 12, 4);
    memcpy(&term, value.data() + value.size() - 8, 8);
  }

  static void DecodeValue(const std::string_view& value, int& type, int64_t& term) {
    CHECK(value.size() > 12) << fmt::format("value length({}) is invalid.", value.size());

    memcpy(&type, value.data() + value.size() - 12, 4);
    memcpy(&term, value.data() + value.size() - 8, 8);
  }

  static pb::common::KeyValue EncodeKv(const LogEntry& log_entry) {
    pb::common::KeyValue kv;
    kv.set_key(EncodeKey(log_entry.region_id, log_entry.index));
    kv.set_value(EncodeValue(static_cast<int>(log_entry.type), log_entry.term, log_entry.in_data));

    return std::move(kv);
  }
};

std::string LogEntryTypeName(LogEntryType type) {
  switch (type) {
    case LogEntryType::kEntryTypeNoOp:
      return "EntryTypeNoOp";
    case LogEntryType::kEntryTypeData:
      return "EntryTypeData";
    case LogEntryType::kEntryTypeConfiguration:
      return "EntryTypeConfiguration";
    case LogEntryType::kEntryTypeTombstone:
      return "EntryTypeTombstone";
    default:
      break;
  }

  return "";
}

static std::string MutationTypeName(Mutation::Type type) {
  switch (type) {
    case Mutation::Type::kAppendLogEntry:
      return "AppendLogEntry";
    case Mutation::Type::kTruncatePrefix:
      return "TruncatePrefix";
    case Mutation::Type::kTruncateSuffix:
      return "TruncateSuffix";
    case Mutation::Type::kReset:
      return "Reset";
    case Mutation::Type::kDestroy:
      return "Destroy";
    default:
      break;
  }

  return "";
}

std::string ClientTypeName(ClientType client_type) {
  switch (client_type) {
    case ClientType::kRaft:
      return "Raft";
    case ClientType::kVectorIndex:
      return "VectorIndex";
    default:
      break;
  }

  return "";
}

static std::string ClientTypeName(int client_type) { return ClientTypeName(static_cast<ClientType>(client_type)); }

// log index meta helper function.
static LogIndexMeta TransformLogIndexMeta(const pb::store_internal::LogIndexMeta& log_index_meta_pb) {
  LogIndexMeta log_index_meta;

  log_index_meta.first_index.store(log_index_meta_pb.first_log_index(), std::memory_order_release);
  log_index_meta.last_index.store(-1, std::memory_order_release);

  for (const auto& truncate_index : log_index_meta_pb.truncate_indexes()) {
    log_index_meta.truncate_prefixs.push_back(
        std::make_pair(static_cast<ClientType>(truncate_index.client_type()), truncate_index.truncate_index()));
  }

  return log_index_meta;
}

static std::string GenIndexMetaValue(const LogIndexMeta& log_index_meta) {
  pb::store_internal::LogIndexMeta log_index_meta_pb;
  log_index_meta_pb.set_first_log_index(log_index_meta.first_index.load(std::memory_order_acquire));
  log_index_meta_pb.set_last_log_index(log_index_meta.last_index.load(std::memory_order_acquire));

  for (const auto& truncate_prefix : log_index_meta.truncate_prefixs) {
    auto* mut_trucate_prefix = log_index_meta_pb.add_truncate_indexes();
    mut_trucate_prefix->set_client_type(static_cast<int32_t>(truncate_prefix.first));
    mut_trucate_prefix->set_truncate_index(truncate_prefix.second);
  }

  return log_index_meta_pb.SerializePartialAsString();
}

static std::string GenIndexMetaKey(int64_t region_id) {
  std::string key(9, 0);
  key[0] = kLogIndexMetaPrefix;

  memcpy(key.data() + 1, &region_id, 8);

  return key;
}

static int64_t ParseRegionIdFromIndexMetaKey(const std::string& key) {
  int64_t region_id = 0;

  memcpy(&region_id, key.data() + 1, 8);
  return region_id;
}

static std::string GenIndexMetaMinKey() { return GenIndexMetaKey(0); }

static std::string GenIndexMetaMaxKey() { return GenIndexMetaKey(INT64_MAX); }

static int ExecuteRoutine(void* meta, bthread::TaskIterator<Mutation*>& iter) {  // NOLINT
  RocksLogStorage* log_storage = static_cast<RocksLogStorage*>(meta);

  std::vector<Mutation*> mutations;
  mutations.reserve(FLAGS_rocks_log_max_mutation_batch_size);

  std::vector<WriteOp> write_ops;
  write_ops.reserve(FLAGS_rocks_log_max_write_batch_size);

  auto sync_log_func = [&]() {
    if (BAIDU_UNLIKELY(mutations.empty())) return;
    if (BAIDU_UNLIKELY(write_ops.empty())) return;

    int64_t start_time = Helper::TimestampUs();

    bool ret = log_storage->ApplyWriteOp(write_ops);
    CHECK(ret) << "apply write op fail.";

    log_storage->AdjustIndexMeta(mutations);

    g_write_latency << Helper::TimestampUs() - start_time;

    start_time = Helper::TimestampUs();

    ret = log_storage->SyncWal();

    g_sync_wal_latency << Helper::TimestampUs() - start_time;

    for (auto* mutation : mutations) {
      mutation->ret = ret;
      mutation->cond.DecreaseSignal();
    }

    mutations.clear();
    write_ops.clear();
  };

  for (;;) {
    if (BAIDU_LIKELY(FLAGS_rocks_log_write_wait_time_ms > 0)) {
      std::this_thread::sleep_for(std::chrono::microseconds(FLAGS_rocks_log_write_wait_time_ms));
    }

    size_t size = 0;
    int64_t now_time = Helper::TimestampUs();
    while (iter) {
      Mutation* mutation = *iter;
      ++iter;
      if (BAIDU_UNLIKELY(mutation == nullptr)) {
        DINGO_LOG(WARNING) << fmt::format("[raft.log] mutation is nullptr.");
        continue;
      }

      g_append_entry_wait_latency << now_time - mutation->start_time;
      size += log_storage->AppendToWriteBatch(mutation, write_ops);

      mutations.push_back(mutation);

      if (BAIDU_UNLIKELY(mutations.size() >= FLAGS_rocks_log_max_mutation_batch_size ||
                         write_ops.size() >= FLAGS_rocks_log_max_write_batch_size)) {
        break;
      }
    }

    if (mutations.empty()) {
      break;
    }

    g_write_size << size;

    sync_log_func();
  }

  if (BAIDU_UNLIKELY(iter.is_queue_stopped())) {
    DINGO_LOG(INFO) << fmt::format("[raft.log] execute queue is stop.");
  }

  return 0;
}

void LogEntry::Print() const {
  DINGO_LOG(INFO) << fmt::format("[raft.log][{}] log entry, type({}) term({}) index({}) in_data({}) out_data({})",
                                 region_id, LogEntryTypeName(type), term, index, in_data->size(), out_data.size());
}

void Mutation::Print() const {
  DINGO_LOG(INFO) << fmt::format(
      "[raft.log][{}] mutation type({}) client_type({}) start_index({}) end_index({}) reset_index({})", region_id,
      MutationTypeName(type), ClientTypeName(client_type), start_index, end_index, reset_index);
}

static rocksdb::ColumnFamilyOptions GenColumnFamilyOptions() {
  rocksdb::ColumnFamilyOptions family_options;

  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size = Helper::StringToInt64(Constant::kBlockSizeDefaultValue);
  table_options.block_cache = rocksdb::NewLRUCache(Helper::StringToInt64(Constant::kBlockCacheDefaultValue));

  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10.0, false));
  table_options.whole_key_filtering = true;

  rocksdb::TableFactory* table_factory = NewBlockBasedTableFactory(table_options);
  family_options.table_factory.reset(table_factory);

  family_options.write_buffer_size = Helper::StringToInt64(Constant::kWriteBufferSizeDefaultValue);
  family_options.max_write_buffer_number = Helper::StringToInt64(Constant::kMaxWriteBufferNumberDefaultValue);

  family_options.arena_block_size = Helper::StringToInt64(Constant::kArenaBlockSizeDefaultValue);

  family_options.min_write_buffer_number_to_merge =
      Helper::StringToInt64(Constant::kMinWriteBufferNumberToMergeDefaultValue);
  family_options.max_compaction_bytes = Helper::StringToInt64(Constant::kMaxCompactionBytesDefaultValue);
  family_options.max_bytes_for_level_multiplier =
      Helper::StringToInt64(Constant::kMaxBytesForLevelMultiplierDefaultValue);

  family_options.prefix_extractor.reset(
      rocksdb::NewCappedPrefixTransform(Helper::StringToInt32(Constant::kPrefixExtractorDefaultValue)));

  family_options.max_bytes_for_level_base = Helper::StringToInt64(Constant::kMaxBytesForLevelBaseDefaultValue);

  family_options.target_file_size_base = Helper::StringToInt64(Constant::kTargetFileSizeBaseDefaultValue);

  family_options.compression_per_level = {
      rocksdb::CompressionType::kNoCompression,  rocksdb::CompressionType::kNoCompression,
      rocksdb::CompressionType::kLZ4Compression, rocksdb::CompressionType::kLZ4Compression,
      rocksdb::CompressionType::kLZ4Compression, rocksdb::CompressionType::kZSTD,
      rocksdb::CompressionType::kZSTD,
  };

  return family_options;
}

bool RocksLogStorage::InitExecutionQueue() {
  bthread::ExecutionQueueOptions options;
  options.bthread_attr = BTHREAD_ATTR_NORMAL;
  options.use_pthread = true;

  if (bthread::execution_queue_start(&queue_id_, &options, ExecuteRoutine, this) != 0) {
    DINGO_LOG(ERROR) << "[raft.log] start execution queue failed.";
    return false;
  }

  return true;
}

bool RocksLogStorage::CloseExecutionQueue() {
  if (bthread::execution_queue_stop(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "[raft.log] stop simple log storage execution queue failed.";
    return false;
  }

  if (bthread::execution_queue_join(queue_id_) != 0) {
    DINGO_LOG(ERROR) << "[raft.log] join simple log storage execution queue failed.";
    return false;
  }

  return true;
}

bool RocksLogStorage::InitRocksDB() {
  DINGO_LOG(INFO) << "[raft.log] init rocksdb.";

  rocksdb::Options db_options;
  db_options.create_if_missing = true;
  db_options.create_missing_column_families = true;
  db_options.max_background_jobs = 8;
  db_options.max_subcompactions = 6;
  db_options.stats_dump_period_sec = 60;
  db_options.recycle_log_file_num = FLAGS_rocks_log_recycle_file_num;
  db_options.use_direct_io_for_flush_and_compaction = true;
  db_options.manual_wal_flush = false;

  // rate_limiter_.reset(rocksdb::NewGenericRateLimiter(1024 * 1024 * FLAGS_rate_bytes_per_sec, 500 * 1000, 10,
  //                                                    rocksdb::RateLimiter::Mode::kWritesOnly, false));
  // db_options.rate_limiter = rate_limiter_;

  std::vector<rocksdb::ColumnFamilyDescriptor> column_family_descs;
  column_family_descs.push_back(rocksdb::ColumnFamilyDescriptor("default", GenColumnFamilyOptions()));

  rocksdb::DB* db = nullptr;
  rocksdb::Status status = rocksdb::DB::Open(db_options, path_, column_family_descs, &family_handles_, &db);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log] open rocksdb fail, error: {}", status.ToString());
    return false;
  }

  db_.reset(db);

  return true;
}

bool RocksLogStorage::CloseRocksDB() {
  if (db_ != nullptr) {
    CancelAllBackgroundWork(db_.get(), true);
    db_->Close();
  }

  return true;
}

bool RocksLogStorage::SaveIndexMeta(int64_t region_id, const LogIndexMeta& log_index_meta) {
  rocksdb::WriteBatch batch;

  std::string key = GenIndexMetaKey(region_id);
  std::string value = GenIndexMetaValue(log_index_meta);

  rocksdb::Status status = batch.Put(key, value);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log] batch put failed, error: {}.", status.ToString());
    return false;
  }

  rocksdb::WriteOptions options;
  options.sync = true;
  status = db_->Write(options, &batch);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log] write failed, error: {}", status.ToString());
    return false;
  }

  return true;
}

void RocksLogStorage::DeleteIndexMeta(int64_t region_id) {
  RWLockWriteGuard guard(&rw_lock_);

  log_index_metas_.erase(region_id);
}

bool RocksLogStorage::IsExistIndexMeta(int64_t region_id) {
  RWLockReadGuard guard(&rw_lock_);

  return log_index_metas_.find(region_id) != log_index_metas_.end();
}

void RocksLogStorage::UpdateFirstIndex(int64_t region_id, int64_t first_index) {
  RWLockReadGuard guard(&rw_lock_);

  auto it = log_index_metas_.find(region_id);
  CHECK(it != log_index_metas_.end()) << fmt::format("[raft.log][{}] not found log index meta.", region_id);

  it->second.first_index.store(first_index, std::memory_order_release);
}

void RocksLogStorage::UpdateLastIndex(int64_t region_id, int64_t last_index) {
  RWLockReadGuard guard(&rw_lock_);

  auto it = log_index_metas_.find(region_id);
  CHECK(it != log_index_metas_.end()) << fmt::format("[raft.log][{}] not found log index meta.", region_id);

  it->second.last_index.store(last_index, std::memory_order_release);
}

void RocksLogStorage::UpdateLastIndex(const std::vector<LogEntry>& log_entries) {
  // region_id: max_last_index
  std::map<int64_t, int64_t> last_indexes;
  for (const auto& log_entry : log_entries) {
    auto it = last_indexes.find(log_entry.region_id);
    if (it == last_indexes.end()) {
      last_indexes.insert(std::make_pair(log_entry.region_id, log_entry.index));
    } else {
      if (it->second < log_entry.index) {
        it->second = log_entry.index;
      }
    }
  }

  {
    RWLockReadGuard guard(&rw_lock_);
    for (auto& [region_id, last_index] : last_indexes) {
      auto it = log_index_metas_.find(region_id);
      CHECK(it != log_index_metas_.end()) << fmt::format("[raft.log][{}] not found log index meta.", region_id);
      it->second.last_index.store(last_index, std::memory_order_release);
    }
  }
}

void RocksLogStorage::ResetFirstAndLastIndex(int64_t region_id, int64_t first_index, int64_t last_index) {
  RWLockReadGuard guard(&rw_lock_);

  auto it = log_index_metas_.find(region_id);
  CHECK(it != log_index_metas_.end()) << fmt::format("[raft.log][{}] not found log index meta.", region_id);

  it->second.first_index.store(first_index, std::memory_order_release);
  it->second.last_index.store(last_index, std::memory_order_release);
  for (auto& trucate_prefix : it->second.truncate_prefixs) {
    trucate_prefix.second = 0;
  }
}

bool RocksLogStorage::InitIndexMeta() {
  {
    std::string start_key = GenIndexMetaMinKey();
    std::string end_key = GenIndexMetaMaxKey();

    rocksdb::ReadOptions read_option;
    read_option.auto_prefix_mode = true;
    read_option.async_io = true;
    read_option.adaptive_readahead = true;
    rocksdb::Slice upper_bound(end_key);
    read_option.iterate_upper_bound = &upper_bound;

    rocksdb::Iterator* it = db_->NewIterator(read_option);
    CHECK(it != nullptr) << "[raft.log] new iterator fail.";

    RWLockWriteGuard guard(&rw_lock_);

    std::string_view end_key_view(end_key);
    for (it->Seek(start_key); it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
      // key it->key();
      int64_t region_id = ParseRegionIdFromIndexMetaKey(it->key().ToString());

      // value
      pb::store_internal::LogIndexMeta log_index_meta_pb;
      CHECK(log_index_meta_pb.ParseFromString(it->value().ToString())) << "[raft.log] parse log index meta pb fail.";

      auto log_index_meta = TransformLogIndexMeta(log_index_meta_pb);
      log_index_metas_.insert(std::make_pair(region_id, log_index_meta));
    }

    delete it;
  }

  {
    RWLockReadGuard guard(&rw_lock_);

    // update first and last log index
    for (auto& [region_id, log_index_meta] : log_index_metas_) {
      log_index_meta.first_index.store(GetFirstLogIndex(region_id), std::memory_order_release);
      log_index_meta.last_index.store(GetLastLogIndex(region_id), std::memory_order_release);
    }
  }

  return true;
}

bool RocksLogStorage::Init() {
  CHECK(!client_types_.empty()) << "[raft.log] please firstly register client type.";

  DINGO_LOG(INFO) << fmt::format("[raft.log] init rocks log storage, path({}).", path_);

  // init execution queue
  if (!InitExecutionQueue()) {
    DINGO_LOG(ERROR) << "[raft.log] init execution queue failed.";
    return false;
  }

  if (!InitRocksDB()) {
    DINGO_LOG(ERROR) << "[raft.log] init rocksdb failed.";
    return false;
  }

  if (!InitIndexMeta()) {
    DINGO_LOG(ERROR) << "[raft.log] init index meta failed.";
    return false;
  }

  return true;
}

bool RocksLogStorage::Close() {
  if (!CloseExecutionQueue()) {
    DINGO_LOG(ERROR) << "[raft.log] close execution queue failed.";
  }

  if (!CloseRocksDB()) {
    DINGO_LOG(ERROR) << "[raft.log] close rocksdb failed.";
  }

  return true;
}

void RocksLogStorage::RegisterClientType(ClientType client_type) { client_types_.push_back(client_type); }

bool RocksLogStorage::RegisterRegion(int64_t region_id) {
  DINGO_LOG(INFO) << fmt::format("[raft.log][{}] register region.", region_id);

  bool is_save = false;
  LogIndexMeta log_index_meta;

  {
    RWLockWriteGuard guard(&rw_lock_);

    auto it = log_index_metas_.find(region_id);
    if (it == log_index_metas_.end()) {
      is_save = true;
      for (auto& client_type : client_types_) {
        log_index_meta.truncate_prefixs.push_back(std::make_pair(client_type, 0));
      }
      log_index_metas_.insert(std::make_pair(region_id, log_index_meta));
    }
  }

  if (is_save) {
    SaveIndexMeta(region_id, log_index_meta);
  }

  return true;
}

bool RocksLogStorage::CommitMutation(Mutation* mutation) {
  mutation->start_time = Helper::TimestampUs();

  if (BAIDU_UNLIKELY(bthread::execution_queue_execute(queue_id_, mutation) != 0)) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][{}] execution queue execute fail, type({}).", mutation->region_id,
                                    MutationTypeName(mutation->type));
    return false;
  }

  mutation->cond.IncreaseWait();
  if (BAIDU_UNLIKELY(!mutation->ret)) {
    DINGO_LOG(INFO) << fmt::format("[raft.log][{}] commit mutation fail, type({}).", mutation->region_id,
                                   MutationTypeName(mutation->type));
  }

  return mutation->ret;
}

size_t RocksLogStorage::AppendToWriteBatch(const Mutation* mutation, std::vector<WriteOp>& write_ops) {
  int64_t size = 0;
  switch (mutation->type) {
    case Mutation::Type::kAppendLogEntry: {
      for (const auto& log_entry : mutation->log_entries) {
        WriteOp write_op;
        write_op.type = WriteOp::Type::kPut;
        write_op.key_or_start_key = Codec::EncodeKey(log_entry.region_id, log_entry.index);
        write_op.value_or_end_key =
            Codec::EncodeValue(static_cast<int>(log_entry.type), log_entry.term, log_entry.in_data);

        size += write_op.key_or_start_key.size() + write_op.value_or_end_key.size();
        write_ops.push_back(std::move(write_op));
      }
    } break;

    case Mutation::Type::kTruncatePrefix:
    case Mutation::Type::kTruncateSuffix: {
      WriteOp write_op;
      write_op.type = WriteOp::Type::kDeleteRange;

      write_op.key_or_start_key = Codec::EncodeKey(mutation->region_id, mutation->start_index);
      write_op.value_or_end_key = Codec::EncodeKey(mutation->region_id, mutation->end_index);

      size += write_op.key_or_start_key.size() + write_op.value_or_end_key.size();
      write_ops.push_back(std::move(write_op));
    } break;

    case Mutation::Type::kReset:
    case Mutation::Type::kDestroy: {
      WriteOp write_op;
      write_op.type = WriteOp::Type::kDeleteRange;

      write_op.key_or_start_key = Codec::EncodeKey(mutation->region_id, 0);
      write_op.value_or_end_key = Codec::EncodeKey(mutation->region_id, INT64_MAX);

      size += write_op.key_or_start_key.size() + write_op.value_or_end_key.size();
      write_ops.push_back(std::move(write_op));
    } break;

    default:
      DINGO_LOG(FATAL) << fmt::format("[raft.log][{}] unknown mutation type({}) ptr({}).", mutation->region_id,
                                      static_cast<int>(mutation->type), fmt::ptr(mutation));
      break;
  }

  return size;
}

bool RocksLogStorage::ApplyWriteOp(const std::vector<WriteOp>& write_ops) {
  rocksdb::WriteBatch batch;

  size_t size = 0;
  for (const auto& write_op : write_ops) {
    rocksdb::Status status;
    if (BAIDU_LIKELY(write_op.type == WriteOp::Type::kPut)) {
      status = batch.Put(write_op.key_or_start_key, write_op.value_or_end_key);

    } else if (write_op.type == WriteOp::Type::kDeleteRange) {
      status = batch.DeleteRange(write_op.key_or_start_key, write_op.value_or_end_key);
    }

    size += write_op.key_or_start_key.size() + write_op.value_or_end_key.size();

    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[raft.log] batch put failed, error: {}.", status.ToString());
      return false;
    }
  }

  rocksdb::WriteOptions options;
  options.sync = false;
  rocksdb::Status status = db_->Write(options, &batch);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log] write failed, error: {}", status.ToString());
    return false;
  }

  return true;
}

void RocksLogStorage::AdjustIndexMeta(const std::vector<Mutation*>& mutations) {
  for (auto* mutation : mutations) {
    switch (mutation->type) {
      case Mutation::Type::kAppendLogEntry: {
        int64_t last_index = mutation->log_entries.back().index;
        UpdateLastIndex(mutation->region_id, last_index);
      } break;

      case Mutation::Type::kTruncatePrefix: {
        int64_t region_id = mutation->region_id;

        int64_t first_index = FirstLogIndex(region_id);
        int64_t last_index = LastLogIndex(region_id);

        DINGO_LOG(INFO) << fmt::format("[raft.log][{}.{}.{}] truncate prefix, range[{}-{}).", region_id, first_index,
                                       last_index, mutation->start_index, mutation->end_index);

        if (mutation->end_index > first_index) {
          UpdateFirstIndex(region_id, mutation->end_index);
          if (last_index < mutation->end_index) {
            UpdateLastIndex(region_id, mutation->end_index - 1);
          }
        }
      } break;

      case Mutation::Type::kTruncateSuffix: {
        int64_t region_id = mutation->region_id;

        int64_t first_index = FirstLogIndex(region_id);
        int64_t last_index = LastLogIndex(region_id);

        DINGO_LOG(INFO) << fmt::format("[raft.log][{}.{}.{}] truncate suffix, range[{}-{}).", region_id, first_index,
                                       last_index, mutation->start_index, mutation->end_index);

        if (mutation->start_index <= last_index) {
          UpdateLastIndex(region_id, mutation->start_index - 1);
          if (mutation->start_index <= first_index) {
            UpdateFirstIndex(region_id, mutation->start_index);
          }
        }

      } break;

      case Mutation::Type::kReset: {
        int64_t region_id = mutation->region_id;

        DINGO_LOG(INFO) << fmt::format("[raft.log][{}] apply reset log.", region_id);
        ResetFirstAndLastIndex(region_id, mutation->reset_index, mutation->reset_index - 1);
      } break;

      case Mutation::Type::kDestroy: {
        int64_t region_id = mutation->region_id;

        DINGO_LOG(INFO) << fmt::format("[raft.log][{}] apply destroy log.", region_id);
        DeleteIndexMeta(region_id);

      } break;

      default:
        DINGO_LOG(FATAL) << fmt::format("[raft.log][{}] unknown mutation type({}) ptr({}).", mutation->region_id,
                                        static_cast<int>(mutation->type), fmt::ptr(mutation));
        break;
    }
  }
}

bool RocksLogStorage::SyncWal() {
  // auto status = db_->FlushWAL(true);
  auto status = db_->SyncWAL();
  if (!status.ok()) {
    DINGO_LOG(INFO) << fmt::format("[raft.log] sync wal fail, error: {}.", status.ToString());
    return false;
  }

  return true;
}

bool RocksLogStorage::GetLogIndexMeta(int64_t region_id, LogIndexMeta& log_index_meta) {
  RWLockReadGuard guard(&rw_lock_);
  auto it = log_index_metas_.find(region_id);
  if (it == log_index_metas_.end()) {
    return false;
  }

  log_index_meta = it->second;
  return true;
}

std::map<int64_t, LogIndexMeta> RocksLogStorage::GetLogIndexMeta(const std::vector<int64_t>& region_ids,
                                                                 bool is_actual) {
  std::map<int64_t, LogIndexMeta> result;
  {
    RWLockReadGuard guard(&rw_lock_);

    if (region_ids.empty()) {
      result = log_index_metas_;
    } else {
      for (const auto& region_id : region_ids) {
        auto it = log_index_metas_.find(region_id);
        if (it != log_index_metas_.end()) {
          result.insert(std::make_pair(region_id, it->second));
        }
      }
    }
  }

  if (is_actual) {
    for (auto& [region_id, log_index_meta] : result) {
      log_index_meta.first_index.store(GetFirstLogIndex(region_id));
      log_index_meta.last_index.store(GetLastLogIndex(region_id));
    }
  }

  return result;
}

void RocksLogStorage::PrintLogIndexMeta() {
  RWLockReadGuard guard(&rw_lock_);

  for (auto& [region_id, log_index_meta] : log_index_metas_) {
    std::string str;
    for (auto& [client_type, truncate_index] : log_index_meta.truncate_prefixs) {
      str += fmt::format("{}/{};", static_cast<int>(client_type), truncate_index);
    }

    DINGO_LOG(INFO) << fmt::format("[raft.log][{}] log index meta, first_index({}) last_index({}) truncate({})",
                                   region_id, log_index_meta.first_index.load(std::memory_order_acquire),
                                   log_index_meta.last_index.load(std::memory_order_acquire), str);
  }
}

int64_t RocksLogStorage::FirstLogIndex(int64_t region_id) {
  RWLockReadGuard guard(&rw_lock_);

  auto it = log_index_metas_.find(region_id);
  CHECK(it != log_index_metas_.end()) << "[raft.log] not found region.";

  int64_t first_index = it->second.first_index.load(std::memory_order_acquire);

  DINGO_LOG(DEBUG) << fmt::format("[raft.log][{}] first log index({}).", region_id, first_index);

  return first_index;
}

int64_t RocksLogStorage::LastLogIndex(int64_t region_id) {
  RWLockReadGuard guard(&rw_lock_);

  auto it = log_index_metas_.find(region_id);
  CHECK(it != log_index_metas_.end()) << "[raft.log] not found region.";

  int64_t last_index = it->second.last_index.load(std::memory_order_acquire);

  DINGO_LOG(DEBUG) << fmt::format("[raft.log][{}] last log index({}).", region_id, last_index);

  return last_index;
}

int64_t RocksLogStorage::GetFirstLogIndex(int64_t region_id) {
  std::string start_key = Codec::EncodeKey(region_id, 0);
  std::string end_key = Codec::EncodeKey(region_id, INT64_MAX);

  rocksdb::ReadOptions read_option;
  read_option.auto_prefix_mode = true;
  read_option.async_io = true;
  read_option.adaptive_readahead = true;
  rocksdb::Slice upper_bound(end_key);
  read_option.iterate_upper_bound = &upper_bound;

  rocksdb::Iterator* it = db_->NewIterator(read_option);
  CHECK(it != nullptr) << fmt::format("[raft.log][{}] new iterator fail.", region_id);

  int64_t temp_region_id = 0;
  int64_t index = -1;
  it->Seek(start_key);
  if (it->Valid() && it->key().ToStringView() < std::string_view(end_key)) {
    Codec::DecodeKey(it->key().ToStringView(), temp_region_id, index);
    CHECK(temp_region_id == region_id) << fmt::format("[raft.log][{}] not match region id({}).", region_id,
                                                      temp_region_id);
  }

  delete it;

  return index;
}

int64_t RocksLogStorage::GetLastLogIndex(int64_t region_id) {
  std::string start_key = Codec::EncodeKey(region_id, 0);
  std::string end_key = Codec::EncodeKey(region_id, INT64_MAX);

  rocksdb::ReadOptions read_option;
  read_option.auto_prefix_mode = true;
  read_option.async_io = true;
  read_option.adaptive_readahead = true;
  rocksdb::Slice upper_bound(end_key);
  read_option.iterate_upper_bound = &upper_bound;

  rocksdb::Iterator* it = db_->NewIterator(read_option);
  CHECK(it != nullptr) << fmt::format("[raft.log][{}] new iterator fail.", region_id);

  int64_t temp_region_id = 0;
  int64_t index = -1;
  it->SeekForPrev(end_key);
  if (it->Valid() && it->key().ToStringView() >= std::string_view(start_key)) {
    Codec::DecodeKey(it->key().ToStringView(), temp_region_id, index);
    CHECK(temp_region_id == region_id) << fmt::format("[raft.log][{}] not match region id({}).", region_id,
                                                      temp_region_id);
  }

  delete it;

  return index;
}

bool RocksLogStorage::DeleteRange(const std::string& start_key, const std::string& end_key) {
  DINGO_LOG(INFO) << fmt::format("[raft.log] delete range, range[{}-{})", Helper::StringToHex(start_key),
                                 Helper::StringToHex(end_key));

  rocksdb::WriteOptions options;
  options.sync = false;
  rocksdb::Status status = db_->DeleteRange(options, start_key, end_key);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log] delete range fail, error: {}.", status.ToString());
    return false;
  }

  return true;
}

int64_t RocksLogStorage::GetTerm(int64_t region_id, int64_t index) {
  std::string key = Codec::EncodeKey(region_id, index);

  int64_t first_index = FirstLogIndex(region_id);
  int64_t last_index = LastLogIndex(region_id);

  std::string value;
  rocksdb::ReadOptions read_option;
  rocksdb::Status status = db_->Get(read_option, key, &value);
  if (!status.ok()) {
    if (!status.IsNotFound()) {
      DINGO_LOG(ERROR) << fmt::format("[raft.log][{}.{}.{}] get index({}) term fail, error: {}.", region_id,
                                      first_index, last_index, index, status.ToString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("[raft.log][{}.{}.{}] get index({}) term fail, key({}) error: {}.", region_id,
                                        first_index, last_index, index, Helper::StringToHex(key), status.ToString());
    }

    return 0;
  }

  int type = 0;
  int64_t term = 0;
  Codec::DecodeValue(value, type, term);
  CHECK(term > 0) << fmt::format("[raft.log][{}.{}.{}] term of index({}) is invalid.", region_id, first_index,
                                 last_index, index);

  DINGO_LOG(DEBUG) << fmt::format("[raft.log][{}] get term({}) by index({}).", region_id, term, index);

  return term;
}

LogEntryPtr RocksLogStorage::GetEntry(int64_t region_id, int64_t index) {
  std::string key = Codec::EncodeKey(region_id, index);

  std::string value;
  rocksdb::ReadOptions read_option;
  rocksdb::Status status = db_->Get(read_option, key, &value);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][{}] get index({}) term fail, error: {}", region_id, index,
                                    status.ToString());
    return nullptr;
  }

  auto log_entry = LogEntry::New();

  int type = 0;
  int64_t term = 0;
  Codec::DecodeValue(value, type, term);

  log_entry->region_id = region_id;
  log_entry->index = index;
  log_entry->term = term;
  log_entry->type = static_cast<LogEntryType>(type);
  value.resize(value.size() - 12);
  log_entry->out_data.swap(value);

  return log_entry;
}

std::vector<LogEntryPtr> RocksLogStorage::GetEntries(int64_t region_id, int64_t start_index, int64_t end_index) {
  std::string start_key = Codec::EncodeKey(region_id, start_index);
  std::string end_key = Codec::EncodeKey(region_id, end_index);

  rocksdb::ReadOptions read_option;
  read_option.auto_prefix_mode = true;
  read_option.async_io = true;
  read_option.adaptive_readahead = true;
  rocksdb::Slice upper_bound(end_key);
  read_option.iterate_upper_bound = &upper_bound;

  rocksdb::Iterator* it = db_->NewIterator(read_option);
  CHECK(it != nullptr) << fmt::format("[raft.log][{}] new iterator fail.", region_id);

  std::vector<LogEntryPtr> log_entries;
  std::string_view end_key_view(end_key);
  for (it->Seek(start_key); it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    // key
    int64_t temp_region_id;
    int64_t index;
    Codec::DecodeKey(it->key().ToStringView(), temp_region_id, index);
    CHECK(temp_region_id == region_id) << fmt::format("[raft.log][{}] not match region id({}).", region_id,
                                                      temp_region_id);

    // value
    int type = 0;
    int64_t term = 0;
    std::string value = it->value().ToString(false);
    Codec::DecodeValue(value, type, term);

    auto log_entry = LogEntry::New();
    log_entry->region_id = region_id;
    log_entry->index = index;
    log_entry->term = term;
    log_entry->type = static_cast<LogEntryType>(type);
    value.resize(value.size() - 12);
    log_entry->out_data.swap(value);

    log_entries.push_back(log_entry);
  }

  delete it;

  return log_entries;
}

std::vector<LogEntry> RocksLogStorage::GetConfigurations(int64_t region_id) {
  std::string start_key = Codec::EncodeKey(region_id, 0);
  std::string end_key = Codec::EncodeKey(region_id, INT64_MAX);

  rocksdb::ReadOptions read_option;
  read_option.auto_prefix_mode = true;
  read_option.async_io = true;
  read_option.adaptive_readahead = true;
  rocksdb::Slice upper_bound(end_key);
  read_option.iterate_upper_bound = &upper_bound;

  rocksdb::Iterator* it = db_->NewIterator(read_option);
  CHECK(it != nullptr) << fmt::format("[raft.log][{}] new iterator fail.", region_id);

  std::vector<LogEntry> log_entries;
  std::string_view end_key_view(end_key);
  for (it->Seek(start_key); it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    // key
    int64_t temp_region_id;
    int64_t index;

    Codec::DecodeKey(it->key().ToStringView(), temp_region_id, index);
    CHECK(temp_region_id == region_id) << fmt::format("[raft.log][{}] not match region id({}).", region_id,
                                                      temp_region_id);

    // value
    int type = 0;
    int64_t term = 0;
    Codec::DecodeValue(it->value().ToStringView(), type, term);

    if (static_cast<LogEntryType>(type) == LogEntryType::kEntryTypeConfiguration) {
      LogEntry log_entry;
      log_entry.region_id = region_id;
      log_entry.index = index;
      log_entry.term = term;
      log_entry.type = static_cast<LogEntryType>(type);
      std::string value = it->value().ToString(false);
      value.resize(value.size() - 12);
      log_entry.out_data.swap(value);

      log_entries.push_back(std::move(log_entry));
    }
  }

  delete it;

  return log_entries;
}

// delete logs from storage's head, [1, first_index_kept) will be discarded
int RocksLogStorage::TruncatePrefix(ClientType client_type, int64_t region_id, int64_t keep_first_index) {
  int64_t first_index = 0;
  int64_t last_index = 0;
  int64_t min_keep_first_index = INT64_MAX;
  {
    RWLockReadGuard guard(&rw_lock_);

    auto it = log_index_metas_.find(region_id);
    CHECK(it != log_index_metas_.end()) << fmt::format("[raft.log][{}] not found region.", region_id);

    first_index = it->second.first_index.load(std::memory_order_acquire);
    last_index = it->second.last_index.load(std::memory_order_acquire);
    if (keep_first_index <= first_index) {
      DINGO_LOG(INFO) << fmt::format("[raft.log][{}.{}.{}] truncate prefix ignore, keep_first_index({}).", region_id,
                                     first_index, last_index, keep_first_index);
      return 0;
    }

    for (auto& truncate_prefix : it->second.truncate_prefixs) {
      if (truncate_prefix.first == client_type) {
        if (keep_first_index <= truncate_prefix.second) {
          DINGO_LOG(INFO) << fmt::format(
              "[raft.log][{}.{}.{}] truncate prefix ignore, client_type({}) keep_first_index({}/{}).", region_id,
              first_index, last_index, ClientTypeName(client_type), truncate_prefix.second, keep_first_index);
          return 0;
        } else {
          truncate_prefix.second = keep_first_index;
        }
      }

      min_keep_first_index = std::min(min_keep_first_index, truncate_prefix.second);
    }
  }

  if (min_keep_first_index == INT64_MAX || min_keep_first_index <= first_index) {
    DINGO_LOG(INFO) << fmt::format("[raft.log][{}.{}.{}] truncate prefix ignore, min_keep_first_index({}).", region_id,
                                   first_index, last_index, min_keep_first_index);
    return 0;
  }

  Mutation mutation;
  mutation.type = Mutation::Type::kTruncatePrefix;
  mutation.client_type = client_type;
  mutation.region_id = region_id;
  mutation.start_index = 0;
  mutation.end_index = min_keep_first_index;

  return CommitMutation(&mutation) ? 0 : -1;
}

// delete uncommitted logs from storage's tail, (keep_last_index, infinity) will be discarded
int RocksLogStorage::TruncateSuffix(ClientType client_type, int64_t region_id, int64_t keep_last_index) {
  {
    RWLockReadGuard guard(&rw_lock_);

    auto it = log_index_metas_.find(region_id);
    CHECK(it != log_index_metas_.end()) << fmt::format("[raft.log] not found region({}).", region_id);

    int64_t first_index = it->second.first_index.load(std::memory_order_acquire);
    int64_t last_index = it->second.last_index.load(std::memory_order_acquire);
    if (keep_last_index > last_index) {
      DINGO_LOG(INFO) << fmt::format("[raft.log][{}.{}.{}] truncate suffix fail, keep_last_index({}).", region_id,
                                     first_index, last_index, keep_last_index);
      return -1;
    } else if (keep_last_index == last_index) {
      DINGO_LOG(INFO) << fmt::format("[raft.log][{}.{}.{}] truncate suffix ignore, keep_last_index({}).", region_id,
                                     first_index, last_index, keep_last_index);
      return 0;
    }
  }

  Mutation mutation;
  mutation.type = Mutation::Type::kTruncateSuffix;
  mutation.client_type = client_type;
  mutation.region_id = region_id;
  mutation.start_index = keep_last_index + 1;
  mutation.end_index = INT64_MAX;

  return CommitMutation(&mutation) ? 0 : -1;
}

bool RocksLogStorage::DestroyRegionLog(int64_t region_id) {
  DINGO_LOG(INFO) << fmt::format("[raft.log][{}] destroy region.", region_id);

  Mutation mutation;
  mutation.type = Mutation::Type::kDestroy;
  mutation.region_id = region_id;

  if (!CommitMutation(&mutation)) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][{}] destroy region log fail.", region_id);
    return false;
  }

  return true;
}

bool RocksLogStorage::HasSpecificLog(int64_t region_id, int64_t begin_index, int64_t end_index,
                                     RockMatchFuncer matcher) {
  std::string start_key = Codec::EncodeKey(region_id, begin_index);
  std::string end_key = Codec::EncodeKey(region_id, end_index);

  rocksdb::ReadOptions read_option;
  read_option.auto_prefix_mode = true;
  read_option.async_io = true;
  read_option.adaptive_readahead = true;
  rocksdb::Slice upper_bound(end_key);
  read_option.iterate_upper_bound = &upper_bound;

  rocksdb::Iterator* it = db_->NewIterator(read_option);
  CHECK(it != nullptr) << fmt::format("[raft.log][{}] new iterator fail.", region_id);

  std::string_view end_key_view(end_key);
  for (it->Seek(start_key); it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    // key
    int64_t temp_region_id;
    int64_t index;

    Codec::DecodeKey(it->key().ToStringView(), temp_region_id, index);
    CHECK(temp_region_id == region_id) << fmt::format("[raft.log][{}] not match region id({}).", region_id,
                                                      temp_region_id);

    // value
    int type = 0;
    int64_t term = 0;
    Codec::DecodeValue(it->value().ToStringView(), type, term);

    LogEntryType log_entry_type = static_cast<LogEntryType>(type);

    if (log_entry_type != LogEntryType::kEntryTypeData && log_entry_type != LogEntryType::kEntryTypeConfiguration) {
      continue;
    }

    LogEntry log_entry;
    log_entry.region_id = region_id;
    log_entry.index = index;
    log_entry.term = term;
    log_entry.type = log_entry_type;

    std::string value = it->value().ToString(false);
    value.resize(value.size() - 12);
    log_entry.out_data.swap(value);

    if (matcher(log_entry)) {
      return true;
    }
  }

  delete it;

  return true;
}

LogEntry RocksLogStorageWrapper::ToLogEntry(int64_t region_id, const braft::LogEntry* entry) {
  CHECK(entry != nullptr) << fmt::format("[raft.log][{}] entry is nullptr.", region_id);

  LogEntry log_entry;

  log_entry.type = static_cast<LogEntryType>(entry->type);
  log_entry.region_id = region_id;
  log_entry.term = entry->id.term;
  log_entry.index = entry->id.index;

  if (log_entry.type == LogEntryType::kEntryTypeData || log_entry.type == LogEntryType::kEntryTypeNoOp) {
    log_entry.in_data = &entry->data;

  } else if (log_entry.type == LogEntryType::kEntryTypeConfiguration) {
    butil::IOBuf* data = new butil::IOBuf();
    auto status = braft::serialize_configuration_meta(entry, *data);
    log_entry.in_data = data;  // need free memory
  }

  return std::move(log_entry);
}

braft::LogEntry* RocksLogStorageWrapper::ToBraftLogEntry(LogEntryPtr log_entry) {
  CHECK(log_entry != nullptr) << "[raft.log] entry is nullptr.";

  return ToBraftLogEntry(*log_entry);
}

braft::LogEntry* RocksLogStorageWrapper::ToBraftLogEntry(const LogEntry& log_entry) {
  braft::LogEntry* entry = new braft::LogEntry();
  entry->AddRef();

  entry->type = static_cast<braft::EntryType>(log_entry.type);
  entry->id.term = log_entry.term;
  entry->id.index = log_entry.index;

  if (log_entry.type == LogEntryType::kEntryTypeData || log_entry.type == LogEntryType::kEntryTypeNoOp) {
    entry->data = log_entry.out_data;

  } else if (log_entry.type == LogEntryType::kEntryTypeConfiguration) {
    butil::IOBuf data;
    data = log_entry.out_data;
    auto status = braft::parse_configuration_meta(data, entry);
    if (!status.ok()) {
      entry->Release();
      return nullptr;
    }
  }

  return entry;
}

int RocksLogStorageWrapper::init(braft::ConfigurationManager* configuration_manager) {
  DINGO_LOG(INFO) << fmt::format("[raft.log][{}] init RocksLogStorageWrapper.", region_id_);

  if (!log_storage_->RegisterRegion(region_id_)) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][{}] register region fail.", region_id_);
    return -1;
  }

  auto log_entries = log_storage_->GetConfigurations(region_id_);
  for (auto& log_entry : log_entries) {
    auto* braft_entry = RocksLogStorageWrapper::ToBraftLogEntry(log_entry);
    braft::ConfigurationEntry conf_entry(*braft_entry);
    configuration_manager->add(conf_entry);

    braft_entry->Release();
  }

  return 0;
}

// first log index in log
int64_t RocksLogStorageWrapper::first_log_index() { return log_storage_->FirstLogIndex(region_id_); }

// last log index in log
int64_t RocksLogStorageWrapper::last_log_index() { return log_storage_->LastLogIndex(region_id_); }

// get logentry by index
braft::LogEntry* RocksLogStorageWrapper::get_entry(const int64_t index) {
  DINGO_LOG(DEBUG) << fmt::format("[raft.log][{}] get entry, index({}).", region_id_, index);

  auto log_entry = log_storage_->GetEntry(region_id_, index);
  if (log_entry == nullptr) {
    return nullptr;
  }

  return ToBraftLogEntry(log_entry);
}

// get logentry's term by index
int64_t RocksLogStorageWrapper::get_term(const int64_t index) { return log_storage_->GetTerm(region_id_, index); }

// append entry to log
int RocksLogStorageWrapper::append_entry(const braft::LogEntry* entry) {
  Mutation mutation;
  mutation.type = Mutation::Type::kAppendLogEntry;
  mutation.region_id = region_id_;
  mutation.log_entries.push_back(ToLogEntry(region_id_, entry));

  return log_storage_->CommitMutation(&mutation) ? 0 : -1;
}

// append entries to log and update IOMetric, return success append number
int RocksLogStorageWrapper::append_entries(const std::vector<braft::LogEntry*>& entries, braft::IOMetric* metric) {
  Mutation mutation;
  mutation.type = Mutation::Type::kAppendLogEntry;
  mutation.region_id = region_id_;
  mutation.log_entries.reserve(entries.size());
  for (auto* entry : entries) {
    mutation.log_entries.push_back(ToLogEntry(region_id_, entry));
  }
  int64_t start_time = Helper::TimestampUs();

  bool ret = log_storage_->CommitMutation(&mutation);

  int64_t elapsed_time = Helper::TimestampUs() - start_time;

  if (metric != nullptr) {
    metric->open_segment_time_us += 0;
    metric->append_entry_time_us += 1;
    metric->sync_segment_time_us += elapsed_time;
  }

  g_append_entry_total_latency << elapsed_time;

  return ret ? entries.size() : -1;
}

// delete logs from storage's head, [1, first_index_kept) will be discarded
int RocksLogStorageWrapper::truncate_prefix(const int64_t keep_first_index) {
  return log_storage_->TruncatePrefix(ClientType::kRaft, region_id_, keep_first_index);
}

// delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
int RocksLogStorageWrapper::truncate_suffix(const int64_t keep_last_index) {
  return log_storage_->TruncateSuffix(ClientType::kRaft, region_id_, keep_last_index);
}

int RocksLogStorageWrapper::reset(const int64_t next_log_index) {
  DINGO_LOG(INFO) << fmt::format("[raft.log][{}] reset, next_log_index({}).", region_id_, next_log_index);

  Mutation mutation;
  mutation.type = Mutation::Type::kReset;
  mutation.region_id = region_id_;
  mutation.reset_index = next_log_index;

  return log_storage_->CommitMutation(&mutation) ? 0 : -1;
}

braft::LogStorage* RocksLogStorageWrapper::new_instance(const std::string& uri) const {
  DINGO_LOG(INFO) << fmt::format("[raft.log][{}] new rock log storage wrapper instance, uri({}).", region_id_, uri);
  return new RocksLogStorageWrapper(region_id_, log_storage_);
}

butil::Status RocksLogStorageWrapper::gc_instance(const std::string& uri) const {
  DINGO_LOG(INFO) << fmt::format("[raft.log][{}] gc instance, uri({}).", region_id_, uri);
  return butil::Status::OK();
}

}  // namespace wal

}  // namespace dingodb

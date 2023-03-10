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
#include "engine/rocks_engine.h"

#include <climits>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "butil/strings/stringprintf.h"
#include "config/config_manager.h"
#include "engine/engine.h"
#include "glog/logging.h"
#include "proto/error.pb.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/write_batch.h"

namespace dingodb {

class RocksIterator : public EngineIterator {
 public:
  explicit RocksIterator(rocksdb::Iterator* iter, const std::string& begin_key, const std::string& end_key)
      : iter_(iter), begin_key_(begin_key), end_key_(end_key) {
    Start(begin_key);
  }
  void Start(const std::string& key) {
    if (!key.empty()) {
      iter_->Seek(key);
    }
  }

  bool HasNext() override {
    bool ret = iter_->Valid();
    if (!ret) {
      return ret;
    }

    if (!end_key_.empty()) {
      return end_key_ > std::string(iter_->key().data(), iter_->key().size());
    }

    return true;
  }

  void Next() override { iter_->Next(); }

  void GetKV(std::string& key, std::string& value) {  // NOLINT
    key.assign(iter_->key().data(), iter_->key().size());
    value.assign(iter_->value().data(), iter_->value().size());
  }

  const std::string& GetName() const override { return name_; }
  uint32_t GetID() override { return id_; }

  ~RocksIterator() override {
    if (iter_) {
      delete iter_;
      iter_ = nullptr;
    }
  }

 protected:
 private:  // NOLINT
  rocksdb::Iterator* iter_;
  const std::string name_ = "RocksIterator";
  uint32_t id_ = static_cast<uint32_t>(EnumEngineIterator::kRocksIterator);
  std::string begin_key_;
  std::string end_key_;
};

class RocksReader : public EngineReader {
 public:
  explicit RocksReader(rocksdb::TransactionDB* txn_db, rocksdb::ColumnFamilyHandle* handle,
                       const rocksdb::WriteOptions& write_options)
      : txn_db_(txn_db), handle_(handle), write_options_(write_options) {
    snapshot_ = nullptr;
    txn_ = nullptr;
  }

  bool BeginTransaction() {
    rocksdb::TransactionOptions txn_options;
    txn_options.set_snapshot = true;

    txn_ = txn_db_->BeginTransaction(write_options_, txn_options);
    if (!txn_) {
      LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::BeginTransaction failed");
      return false;
    }

    snapshot_ = txn_db_->GetSnapshot();

    return true;
  }

  std::shared_ptr<EngineIterator> Scan(const std::string& begin_key, const std::string& end_key) override {
    rocksdb::ReadOptions read_options;
    read_options.snapshot = snapshot_;

    rocksdb::Iterator* iter = txn_db_->NewIterator(read_options, handle_);

    return std::make_shared<RocksIterator>(iter, begin_key, end_key);
  }

  std::shared_ptr<std::string> KvGet(const std::string& key) override {
    rocksdb::ReadOptions read_options;
    read_options.snapshot = snapshot_;
    std::string value;
    rocksdb::Status s = txn_db_->Get(read_options, handle_, key, &value);
    if (!s.ok()) {
      LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Get faild : %s", s.ToString().c_str());
      return {};
    }

    return std::make_shared<std::string>(value);
  }
  // cppcheck-suppress returnTempReference
  const std::string& GetName() const override { return name_; }
  uint32_t GetID() override { return id_; }

  ~RocksReader() override {
    if (txn_) {
      delete txn_;
      txn_ = nullptr;
    }
  }

 protected:
 private:  // NOLINT
  rocksdb::TransactionDB* txn_db_;
  rocksdb::ColumnFamilyHandle* handle_;
  rocksdb::WriteOptions write_options_;
  rocksdb::Transaction* txn_;
  const rocksdb::Snapshot* snapshot_;
  const std::string name_ = "RocksReader";
  uint32_t id_ = static_cast<uint32_t>(EnumEngineReader::kRocksReader);
};

const std::string& RocksEngine::kDbPath = "store.dbPath";
const std::string& RocksEngine::kColumnFamilies = "store.columnFamilies";
const std::string& RocksEngine::kBaseColumnFamily = "store.base";

static const char* kBlockSize = "block_size";
static const char* kBlockCache = "block_cache";
static const char* kArenaBlockSize = "arena_block_size";
static const char* kMinWriteBufferNumberToMerge = "min_write_buffer_number_to_merge";
static const char* kMaxWriteBufferNumber = "max_write_buffer_number";
static const char* kMaxCompactionBytes = "max_compaction_bytes";
static const char* kWriteBufferSize = "write_buffer_size";
static const char* kPrefixExtractor = "prefix_extractor";
static const char* kMaxBytesForLevelBase = "max_bytes_for_level_base";
static const char* kTargetFileSizeBase = "target_file_size_base";

RocksEngine::RocksEngine()
    : type_(pb::common::Engine::ENG_ROCKSDB), name_("ROCKS_ENGINE"), txn_db_(nullptr), column_familys_({}) {}

RocksEngine::~RocksEngine() { Close(); }

// load rocksdb config from config file
bool RocksEngine::Init(std::shared_ptr<Config> config) {
  if (!config) {
    LOG(ERROR) << butil::StringPrintf("config empty not support!");
    return false;
  }

  std::string store_db_path_value = config->GetString(kDbPath);
  if (store_db_path_value.empty()) {
    LOG(ERROR) << butil::StringPrintf("can not find : %s", kDbPath.c_str());
    return false;
  }

  DLOG(INFO) << butil::StringPrintf("rocksdb path : %s", store_db_path_value.c_str());

  std::vector<std::string> column_family = config->GetStringList(kColumnFamilies);
  if (column_family.empty()) {
    LOG(ERROR) << butil::StringPrintf("%s : empty. not found any column family", kColumnFamilies.c_str());
    return false;
  }

  SetDefaultIfNotExist(column_family);

  InitCfConfig(column_family);

  SetColumnFamilyFromConfig(config, column_family);

  std::vector<rocksdb::ColumnFamilyHandle*> family_handles;
  bool ret = RocksdbInit(store_db_path_value, column_family, family_handles);
  if (!ret) {
    DLOG(ERROR) << butil::StringPrintf("rocksdb::DB::Open : %s failed", store_db_path_value.c_str());
    return false;
  }

  SetColumnFamilyHandle(column_family, family_handles);

  DLOG(INFO) << butil::StringPrintf("rocksdb::DB::Open : %s success!", store_db_path_value.c_str());

  return true;
}

std::string RocksEngine::GetName() { return name_; }

pb::common::Engine RocksEngine::GetID() { return type_; }

// not implement
Snapshot* RocksEngine::GetSnapshot() { return nullptr; }
// not implement
void RocksEngine::ReleaseSnapshot() {}

pb::error::Errno RocksEngine::KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr not support");
    return pb::error::ECONTEXT;
  }

  if (key.empty()) {
    LOG(ERROR) << butil::StringPrintf("key empty not support");
    return pb::error::EKEY_EMPTY;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = column_familys_.find(cf);
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", cf.c_str());
    return pb::error::ESTORE_INVALID_CF;
  }

  rocksdb::PinnableSlice pinnable_slice;
  rocksdb::Status s =
      txn_db_->Get(iter->second.GetReadOptions(), iter->second.GetHandle(), rocksdb::Slice(key), &pinnable_slice);
  if (!s.ok()) {
    if (s.IsNotFound()) return pb::error::EKEY_NOTFOUND;
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Get failed : %s", s.ToString().c_str());
    return pb::error::EINTERNAL;
  }

  value.assign(pinnable_slice.data(), pinnable_slice.size());

  return pb::error::OK;
}

pb::error::Errno RocksEngine::KvBatchGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                                         std::vector<pb::common::KeyValue>& kvs) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr not support");
    return pb::error::ECONTEXT;
  }

  if (keys.empty()) {
    LOG(ERROR) << butil::StringPrintf("keys empty not support");
    return pb::error::EKEY_EMPTY;
  }

  for (const auto& key : keys) {
    if (key.empty()) {
      LOG(ERROR) << butil::StringPrintf("key empty not support");
      return pb::error::EKEY_EMPTY;
    }
  }

  const std::string& cf = ctx->cf_name();
  auto iter = column_familys_.find(cf);
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", cf.c_str());
    return pb::error::ESTORE_INVALID_CF;
  }

  for (const auto& key : keys) {
    rocksdb::PinnableSlice pinnable_slice;
    rocksdb::Status s =
        txn_db_->Get(iter->second.GetReadOptions(), iter->second.GetHandle(), rocksdb::Slice(key), &pinnable_slice);
    if (!s.ok()) {
      LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Get failed : %s", s.ToString().c_str());
      return pb::error::EINTERNAL;
    }

    pb::common::KeyValue kv;
    kv.set_key(key);
    kv.set_value(pinnable_slice.ToString());
    kvs.emplace_back(std::move(kv));
  }

  return pb::error::OK;
}

pb::error::Errno RocksEngine::KvPut(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not support");
    return pb::error::ECONTEXT;
  }

  if (kv.key().empty()) {
    LOG(ERROR) << butil::StringPrintf("key empty  not support");
    return pb::error::Errno::EKEY_EMPTY;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = column_familys_.find(cf);
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", cf.c_str());
    return pb::error::Errno::ESTORE_INVALID_CF;
  }

  rocksdb::Status s = txn_db_->Put(iter->second.GetWriteOptions(), iter->second.GetHandle(), rocksdb::Slice(kv.key()),
                                   rocksdb::Slice(kv.value()));
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Put failed : %s", s.ToString().c_str());
    return pb::error::Errno::EINTERNAL;
  }

  if (ctx->flush()) {
    rocksdb::FlushOptions option;
    txn_db_->Flush(option, iter->second.GetHandle());
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvBatchPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not support");
    return pb::error::ECONTEXT;
  }

  if (kvs.empty()) {
    LOG(ERROR) << butil::StringPrintf("keys empty not support");
    return pb::error::EKEY_EMPTY;
  }

  for (const auto& kv : kvs) {
    if (kv.key().empty()) {
      LOG(ERROR) << butil::StringPrintf("key empty not support");
      return pb::error::EKEY_EMPTY;
    }
  }

  const std::string& cf = ctx->cf_name();
  auto iter = column_familys_.find(cf);
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", cf.c_str());
    return pb::error::Errno::ESTORE_INVALID_CF;
  }

  rocksdb::WriteBatch batch;
  for (const auto& kv : kvs) {
    rocksdb::Status s = batch.Put(iter->second.GetHandle(), kv.key(), kv.value());
    if (!s.ok()) {
      LOG(ERROR) << butil::StringPrintf("rocksdb::WriteBatch::Put failed : %s", s.ToString().c_str());
      return pb::error::Errno::EINTERNAL;
    }
  }

  rocksdb::Status s = txn_db_->Write(iter->second.GetWriteOptions(), &batch);
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Write failed : %s", s.ToString().c_str());
    return pb::error::Errno::EINTERNAL;
  }

  if (ctx->flush()) {
    rocksdb::FlushOptions option;
    txn_db_->Flush(option, iter->second.GetHandle());
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvPutIfAbsent(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) {
  pb::common::KeyValue internal_kv;
  internal_kv.set_key(kv.key());
  internal_kv.set_value("");

  const std::string& value = kv.value();

  // compare and replace. support does not exist
  return KvCompareAndSet(ctx, internal_kv, value);
}

pb::error::Errno RocksEngine::KvBatchPutIfAbsentAtomic(std::shared_ptr<Context> ctx,
                                                       const std::vector<pb::common::KeyValue>& kvs,
                                                       std::vector<std::string>& put_keys) {  // NOLINT
  return KvBatchPutIfAbsent(ctx, kvs, put_keys, true);
}
pb::error::Errno RocksEngine::KvBatchPutIfAbsentNonAtomic(std::shared_ptr<Context> ctx,
                                                          const std::vector<pb::common::KeyValue>& kvs,
                                                          std::vector<std::string>& put_keys) {  // NOLINT
  return KvBatchPutIfAbsent(ctx, kvs, put_keys, false);
}

pb::error::Errno RocksEngine::KvBatchPutIfAbsent(std::shared_ptr<Context> ctx,
                                                 const std::vector<pb::common::KeyValue>& kvs,
                                                 std::vector<std::string>& put_keys, bool is_atomic) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not support");
    return pb::error::ECONTEXT;
  }

  if (kvs.empty()) {
    LOG(ERROR) << butil::StringPrintf("empty keys not support");
    return pb::error::Errno::EKEY_EMPTY;
  }

  for (const auto& kv : kvs) {
    if (kv.key().empty()) {
      LOG(ERROR) << butil::StringPrintf("empty key not support");
      return pb::error::Errno::EKEY_EMPTY;
    }
  }

  const std::string& cf = ctx->cf_name();
  auto iter = column_familys_.find(cf);
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", cf.c_str());
    return pb::error::Errno::ESTORE_INVALID_CF;
  }

  rocksdb::Status s;
  rocksdb::TransactionOptions txn_options;
  txn_options.set_snapshot = true;
  std::unique_ptr<rocksdb::Transaction> utxn(txn_db_->BeginTransaction(iter->second.GetWriteOptions()));

  if (!utxn) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::BeginTransaction failed");
    return pb::error::Errno::EINTERNAL;
  }

  const rocksdb::Snapshot* snapshot = utxn->GetSnapshot();

  rocksdb::ReadOptions read_options;
  read_options.snapshot = snapshot;

  for (size_t i = 0; i < kvs.size(); ++i) {
    std::string value_old;

    // other read will failed
    s = utxn->GetForUpdate(read_options, iter->second.GetHandle(),
                           rocksdb::Slice(kvs[i].key().data(), kvs[i].key().size()), &value_old);
    if (is_atomic) {
      if (!s.IsNotFound()) {
        put_keys.clear();
        utxn->Rollback();
        LOG(INFO) << butil::StringPrintf("rocksdb::TransactionDB::GetForUpdate failed : %s", s.ToString().c_str());
        return pb::error::Errno::EINTERNAL;
      }

    } else {
      if (!s.IsNotFound()) {
        continue;
      }
    }

    // write a key in this transaction
    s = utxn->Put(iter->second.GetHandle(), rocksdb::Slice(kvs[i].key().data(), kvs[i].key().size()),
                  rocksdb::Slice(kvs[i].value().data(), kvs[i].value().size()));
    if (!s.ok()) {
      if (is_atomic) put_keys.clear();
      utxn->Rollback();
      LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Put failed : %s", s.ToString().c_str());
      return pb::error::Errno::EINTERNAL;
    }
    put_keys.push_back(kvs[i].key());
  }

  s = utxn->Commit();
  if (!s.ok()) {
    put_keys.clear();
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Commit failed : %s", s.ToString().c_str());
    return pb::error::Errno::EINTERNAL;
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvCompareAndSet(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv,
                                              const std::string& value) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not support");
    return pb::error::ECONTEXT;
  }

  if (kv.key().empty()) {
    LOG(ERROR) << butil::StringPrintf("key empty  not support");
    return pb::error::Errno::EKEY_EMPTY;
  }

  auto iter = column_familys_.find(ctx->cf_name());
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", ctx->cf_name().c_str());
    return pb::error::Errno::ESTORE_INVALID_CF;
  }

  std::unique_ptr<rocksdb::Transaction> txn(txn_db_->BeginTransaction(iter->second.GetWriteOptions()));
  if (!txn) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::BeginTransaction failed");
    return pb::error::Errno::EINTERNAL;
  }

  // other read will failed
  std::string old_value;
  rocksdb::Status s = txn->GetForUpdate(iter->second.GetReadOptions(), iter->second.GetHandle(),
                                        rocksdb::Slice(kv.key().data(), kv.key().size()), &old_value);
  if (!s.ok()) {
    if (!s.IsNotFound() || !kv.value().empty()) {
      txn->Rollback();
      LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::GetForUpdate failed : %s", s.ToString().c_str());
      return pb::error::Errno::EINTERNAL;
    }
  }

  if (kv.value() != old_value) {
    txn->Rollback();
    LOG(WARNING) << butil::StringPrintf("rocksdb::TransactionDB::GetForUpdate value is not equal");
    return pb::error::Errno::EINTERNAL;
  }

  // write a key in this transaction
  s = txn->Put(iter->second.GetHandle(), rocksdb::Slice(kv.key().data(), kv.key().size()),
               rocksdb::Slice(value.data(), value.size()));
  if (!s.ok()) {
    txn->Rollback();
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Put failed : %s", s.ToString().c_str());
    return pb::error::Errno::EINTERNAL;
  }

  s = txn->Commit();
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Commit failed : %s", s.ToString().c_str());
    return pb::error::Errno::EINTERNAL;
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvDelete(std::shared_ptr<Context> ctx, const std::string& key) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not support");
    return pb::error::ECONTEXT;
  }

  if (key.empty()) {
    LOG(ERROR) << butil::StringPrintf("key empty  not support");
    return pb::error::Errno::EKEY_EMPTY;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = column_familys_.find(cf);
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", cf.c_str());
    return pb::error::Errno::ESTORE_INVALID_CF;
  }

  rocksdb::Status s =
      txn_db_->Delete(iter->second.GetWriteOptions(), iter->second.GetHandle(), rocksdb::Slice(key.data(), key.size()));
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Delete failed : %s", s.ToString().c_str());
    return pb::error::Errno::EINTERNAL;
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not support");
    return pb::error::ECONTEXT;
  }

  if (range.start_key().empty()) {
    LOG(ERROR) << butil::StringPrintf("begin_key empty  not support");
    return pb::error::Errno::EKEY_EMPTY;
  }
  if (range.end_key().empty()) {
    LOG(ERROR) << butil::StringPrintf("end_key empty  not support");
    return pb::error::Errno::EKEY_EMPTY;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = column_familys_.find(cf);
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", cf.c_str());
    return pb::error::Errno::ESTORE_INVALID_CF;
  }

  rocksdb::Slice slice_begin{range.start_key()};
  rocksdb::Slice slice_end{range.end_key()};

  rocksdb::Status s;

  if (ctx->delete_files_in_range()) {
    s = rocksdb::DeleteFilesInRange(txn_db_, iter->second.GetHandle(), &slice_begin, &slice_end);
    if (!s.ok()) {
      LOG(ERROR) << butil::StringPrintf("rocksdb::DeleteFilesInRange failed : %s", s.ToString().c_str());
      return pb::error::Errno::EINTERNAL;
    }
  }

  rocksdb::TransactionDBWriteOptimizations opt;
  opt.skip_concurrency_control = true;
  opt.skip_duplicate_key_check = true;

  rocksdb::WriteBatch batch;
  s = batch.DeleteRange(iter->second.GetHandle(), slice_begin, slice_end);
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::WriteBatch::DeleteRange failed : %s", s.ToString().c_str());
    return pb::error::Errno::EINTERNAL;
  }

  s = txn_db_->Write(iter->second.GetWriteOptions(), opt, &batch);
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Write failed : %s", s.ToString().c_str());
    return pb::error::Errno::EINTERNAL;
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvScan(std::shared_ptr<Context> ctx, const std::string& start_key,
                                     const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not support");
    return pb::error::ECONTEXT;
  }

  if (start_key.empty()) {
    LOG(ERROR) << butil::StringPrintf("begin_key empty  not support");
    return pb::error::Errno::EKEY_EMPTY;
  }

  if (end_key.empty()) {
    LOG(ERROR) << butil::StringPrintf("end_key empty  not support");
    return pb::error::Errno::EKEY_EMPTY;
  }

  auto iter = column_familys_.find(ctx->cf_name());
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", ctx->cf_name().c_str());
    return pb::error::Errno::ESTORE_INVALID_CF;
  }

  rocksdb::ReadOptions read_options;
  read_options.snapshot = txn_db_->GetSnapshot();

  std::string_view end_key_view(end_key);
  rocksdb::Iterator* it = txn_db_->NewIterator(read_options, iter->second.GetHandle());
  for (it->Seek(start_key); it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    pb::common::KeyValue kv;
    kv.set_key(it->key().data(), it->key().size());
    kv.set_value(it->value().data(), it->value().size());

    kvs.emplace_back(std::move(kv));
  }

  delete it;
  txn_db_->ReleaseSnapshot(read_options.snapshot);

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvCount(std::shared_ptr<Context> ctx, const std::string& start_key,
                                      const std::string& end_key, int64_t& count) {
  count = -1;
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not support");
    return pb::error::Errno::ECONTEXT;
  }

  if (start_key.empty()) {
    LOG(ERROR) << butil::StringPrintf("start_key empty  not support");
    return pb::error::Errno::EKEY_EMPTY;
  }

  if (end_key.empty()) {
    LOG(ERROR) << butil::StringPrintf("end_key empty  not support");
    return pb::error::Errno::EKEY_EMPTY;
  }

  auto iter = column_familys_.find(ctx->cf_name());
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", ctx->cf_name().c_str());
    return pb::error::Errno::ESTORE_INVALID_CF;
  }

  rocksdb::ReadOptions read_options;
  read_options.snapshot = txn_db_->GetSnapshot();

  count = 0;
  std::string_view end_key_view(end_key);
  rocksdb::Iterator* it = txn_db_->NewIterator(read_options, iter->second.GetHandle());
  for (it->Seek(start_key); it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    count++;
  }

  delete it;
  txn_db_->ReleaseSnapshot(read_options.snapshot);

  return pb::error::Errno::OK;
}

std::shared_ptr<EngineReader> RocksEngine::CreateReader(std::shared_ptr<Context> ctx) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not support");
    return {};
  }

  const std::string& cf = ctx->cf_name();
  auto iter = column_familys_.find(cf);
  if (iter == column_familys_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found", cf.c_str());
    return {};
  }

  std::shared_ptr<RocksReader> reader =
      std::make_shared<RocksReader>(txn_db_, iter->second.GetHandle(), iter->second.GetWriteOptions());

  if (!reader->BeginTransaction()) {
    LOG(ERROR) << butil::StringPrintf("RocksReader::BeginTransaction cf ; %s", cf.c_str());
    return {};
  }

  return reader;
}

void RocksEngine::Close() {
  type_ = pb::common::Engine::ENG_ROCKSDB;

  if (txn_db_) {
    for (const auto& [_, cf] : column_familys_) {
      txn_db_->DestroyColumnFamilyHandle(cf.GetHandle());
    }
    column_familys_.clear();
    delete txn_db_;
    txn_db_ = nullptr;
  }

  rocksdb::DestroyDB(kDbPath, db_options_);

  DLOG(INFO) << butil::StringPrintf("rocksdb::DB::Close");
}

template <typename T>
void SetCfConfigurationElement(const std::map<std::string, std::string>& cf_configuration, const char* name,
                               const T& default_value, T& value) {  // NOLINT
  auto iter = cf_configuration.find(name);

  if (iter == cf_configuration.end()) {
    value = default_value;
  } else {
    const std::string& value_string = iter->second;
    try {
      if (std::is_same_v<size_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
        value = std::stoul(value_string);
      } else if (std::is_same_v<uint64_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
        if (std::is_same_v<uint64_t, unsigned long long>) {  // NOLINT
          value = std::stoull(value_string);
        } else {
          value = std::stoul(value_string);
        }
      } else if (std::is_same_v<int, std::remove_reference_t<std::remove_cv_t<T>>>) {
        value = std::stoi(value_string);
      } else {
        LOG(WARNING) << butil::StringPrintf("only support int size_t uint64_t");
        value = default_value;
      }
    } catch (const std::invalid_argument& e) {
      LOG(ERROR) << butil::StringPrintf("%s trans string to  (int size_t uint64_t) failed : %s", value_string.c_str(),
                                        e.what());
      value = default_value;
    } catch (const std::out_of_range& e) {
      LOG(ERROR) << butil::StringPrintf("%s trans string to  (int size_t uint64_t) failed : %s", value_string.c_str(),
                                        e.what());
      value = default_value;
    }
  }
}

template <typename T>
void SetCfConfigurationElementWrapper(const RocksEngine::CfDefaultConf& default_conf,
                                      const std::map<std::string, std::string>& cf_configuration, const char* name,
                                      T& value) {  // NOLINT
  if (auto iter = default_conf.find(name); iter != default_conf.end()) {
    if (iter->second.has_value()) {
      T default_value = static_cast<T>(std::get<int64_t>(iter->second.value()));

      SetCfConfigurationElement(cf_configuration, name, static_cast<T>(default_value), value);
    }
  }
}

bool RocksEngine::InitCfConfig(const std::vector<std::string>& column_family) {
  CfDefaultConf dcf_default_conf;
  dcf_default_conf.emplace(kBlockSize, std::make_optional(static_cast<int64_t>(131072)));

  dcf_default_conf.emplace(kBlockCache, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf.emplace(kArenaBlockSize, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf.emplace(kMinWriteBufferNumberToMerge, std::make_optional(static_cast<int64_t>(4)));

  dcf_default_conf.emplace(kMaxWriteBufferNumber, std::make_optional(static_cast<int64_t>(2)));

  dcf_default_conf.emplace(kMaxCompactionBytes, std::make_optional(static_cast<int64_t>(134217728)));

  dcf_default_conf.emplace(kWriteBufferSize, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf.emplace(kPrefixExtractor, std::make_optional(static_cast<int64_t>(8)));

  dcf_default_conf.emplace(kMaxBytesForLevelBase, std::make_optional(static_cast<int64_t>(134217728)));

  dcf_default_conf.emplace(kTargetFileSizeBase, std::make_optional(static_cast<int64_t>(67108864)));

  for (const auto& cf : column_family) {
    column_familys_.emplace(cf, ColumnFamily(cf, cf, dcf_default_conf, {}));
  }

  return true;
}

// set cf config
bool RocksEngine::SetCfConfiguration(const CfDefaultConf& default_conf,
                                     const std::map<std::string, std::string>& cf_configuration,
                                     rocksdb::ColumnFamilyOptions* family_options) {
  rocksdb::ColumnFamilyOptions& cf_options = *family_options;

  rocksdb::BlockBasedTableOptions table_options;

  // block_size
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, kBlockSize, table_options.block_size);

  // block_cache
  {
    size_t value = 0;

    SetCfConfigurationElementWrapper(default_conf, cf_configuration, kBlockCache, value);

    auto cache = rocksdb::NewLRUCache(value);  // LRUcache
    table_options.block_cache = cache;
  }

  // arena_block_size

  SetCfConfigurationElementWrapper(default_conf, cf_configuration, kArenaBlockSize, cf_options.arena_block_size);

  // min_write_buffer_number_to_merge
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, kMinWriteBufferNumberToMerge,
                                   cf_options.min_write_buffer_number_to_merge);

  // max_write_buffer_number
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, kMaxWriteBufferNumber,
                                   cf_options.max_write_buffer_number);

  // max_compaction_bytes
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, kMaxCompactionBytes,
                                   cf_options.max_compaction_bytes);

  // write_buffer_size
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, kWriteBufferSize, cf_options.write_buffer_size);

  // prefix_extractor
  {
    size_t value = 0;
    SetCfConfigurationElementWrapper(default_conf, cf_configuration, kPrefixExtractor, value);

    cf_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(value));
  }

  // max_bytes_for_level_base
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, kMaxBytesForLevelBase,
                                   cf_options.max_bytes_for_level_base);

  // target_file_size_base
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, kTargetFileSizeBase,
                                   cf_options.target_file_size_base);

  cf_options.compression_per_level = {
      rocksdb::CompressionType::kNoCompression,  rocksdb::CompressionType::kNoCompression,
      rocksdb::CompressionType::kLZ4Compression, rocksdb::CompressionType::kLZ4Compression,
      rocksdb::CompressionType::kLZ4Compression, rocksdb::CompressionType::kZSTD,
      rocksdb::CompressionType::kZSTD,
  };

  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10.0, false));
  table_options.whole_key_filtering = true;

  cf_options.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(8));

  auto compressed_block_cache = rocksdb::NewLRUCache(1024 * 1024 * 1024);  // LRUcache

  // table_options.block_cache_compressed.reset(compressed_block_cache);

  rocksdb::TableFactory* table_factory = NewBlockBasedTableFactory(table_options);
  cf_options.table_factory.reset(table_factory);

  return true;
}

void RocksEngine::SetDefaultIfNotExist(std::vector<std::string>& column_family) {
  // First find the default configuration, if there is, then exchange the
  // position, if not, add
  bool found_default = false;
  size_t i = 0;
  for (; i < column_family.size(); i++) {
    if (column_family[i] == ROCKSDB_NAMESPACE::kDefaultColumnFamilyName) {
      found_default = true;
      break;
    }
  }

  if (found_default) {
    if (0 != i) {
      std::swap(column_family[i], column_family[0]);
    }
  } else {
    column_family.insert(column_family.begin(), ROCKSDB_NAMESPACE::kDefaultColumnFamilyName);
  }
}

void RocksEngine::CreateNewMap(const std::map<std::string, std::string>& base,
                               const std::map<std::string, std::string>& cf,
                               std::map<std::string, std::string>& new_cf) {
  new_cf = base;

  for (const auto& [key, value] : cf) {
    new_cf[key] = value;
  }
}

bool RocksEngine::RocksdbInit(const std::string& db_path, const std::vector<std::string>& column_family,
                              std::vector<rocksdb::ColumnFamilyHandle*>& family_handles) {
  // cppcheck-suppress variableScope
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  for (const auto& column_family : column_family) {
    rocksdb::ColumnFamilyOptions family_options;
    SetCfConfiguration(column_familys_[column_family].GetDefaultConf(), column_familys_[column_family].GetConf(),
                       &family_options);

    column_families.push_back(rocksdb::ColumnFamilyDescriptor(column_family, family_options));
  }

  rocksdb::DBOptions db_options;
  rocksdb::TransactionDBOptions txn_db_options;

  db_options.create_if_missing = true;
  db_options.create_missing_column_families = true;

  rocksdb::Status s =
      rocksdb::TransactionDB::Open(db_options, txn_db_options, db_path, column_families, &family_handles, &txn_db_);
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Open faild : %s", s.ToString().c_str());
    return false;
  }

  return true;
}

void RocksEngine::SetColumnFamilyHandle(const std::vector<std::string>& column_family,
                                        const std::vector<rocksdb::ColumnFamilyHandle*>& family_handles) {
  size_t i = 0;
  for (const auto& column_family : column_family) {
    column_familys_[column_family].SetHandle(family_handles[i++]);
  }
}

void RocksEngine::SetColumnFamilyFromConfig(const std::shared_ptr<Config>& config,
                                            const std::vector<std::string>& column_family) {
  // get base column family configure. allow empty
  const std::map<std::string, std::string>& base_cf_configuration = config->GetStringMap(kBaseColumnFamily);

  // assign values ​​to each column family
  for (const auto& column_family : column_family) {
    std::string column_family_key("store." + column_family);
    // get column family configure
    const std::map<std::string, std::string>& cf_configuration = config->GetStringMap(column_family_key);

    std::map<std::string, std::string> new_cf_configuration;

    CreateNewMap(base_cf_configuration, cf_configuration, new_cf_configuration);

    column_familys_[column_family].SetConf(new_cf_configuration);
  }
}

RocksEngine::ColumnFamily::ColumnFamily() : ColumnFamily("", "", {}, {}, {}, {}, nullptr){};  // NOLINT

RocksEngine::ColumnFamily::ColumnFamily(const std::string& cf_index, const std::string& cf_desc_name,
                                        const rocksdb::WriteOptions& wo, const rocksdb::ReadOptions& ro,
                                        const CfDefaultConf& default_conf,
                                        const std::map<std::string, std::string>& conf,
                                        rocksdb::ColumnFamilyHandle* handle)
    : cf_index_(cf_index),
      cf_desc_name_(cf_desc_name),
      wo_(wo),
      ro_(ro),
      default_conf_(default_conf),
      conf_(conf),
      handle_(handle) {}

RocksEngine::ColumnFamily::ColumnFamily(const std::string& cf_index, const std::string& cf_desc_name,
                                        const rocksdb::WriteOptions& wo, const rocksdb::ReadOptions& ro,
                                        const CfDefaultConf& default_conf,
                                        const std::map<std::string, std::string>& conf)
    : ColumnFamily(cf_index, cf_desc_name, wo, ro, default_conf, conf, nullptr) {}

RocksEngine::ColumnFamily::ColumnFamily(const std::string& cf_index, const std::string& cf_desc_name,
                                        const CfDefaultConf& default_conf,
                                        const std::map<std::string, std::string>& conf)
    : ColumnFamily(cf_index, cf_desc_name, {}, {}, default_conf, conf, nullptr) {}

RocksEngine::ColumnFamily::~ColumnFamily() {
  cf_index_ = "";
  cf_desc_name_ = "";
  default_conf_.clear();
  conf_.clear();
  handle_ = nullptr;
}

RocksEngine::ColumnFamily::ColumnFamily(const RocksEngine::ColumnFamily& rhs) {
  cf_index_ = rhs.cf_index_;
  cf_desc_name_ = rhs.cf_desc_name_;
  wo_ = rhs.wo_;
  ro_ = rhs.ro_;
  default_conf_ = rhs.default_conf_;
  conf_ = rhs.conf_;
  handle_ = rhs.handle_;
}

RocksEngine::ColumnFamily& RocksEngine::ColumnFamily::operator=(const RocksEngine::ColumnFamily& rhs) {
  if (this == &rhs) {
    return *this;
  }

  cf_index_ = rhs.cf_index_;
  cf_desc_name_ = rhs.cf_desc_name_;
  wo_ = rhs.wo_;
  ro_ = rhs.ro_;
  default_conf_ = rhs.default_conf_;
  conf_ = rhs.conf_;
  handle_ = rhs.handle_;

  return *this;
}

RocksEngine::ColumnFamily::ColumnFamily(RocksEngine::ColumnFamily&& rhs) noexcept {
  cf_index_ = std::move(rhs.cf_index_);
  cf_desc_name_ = std::move(rhs.cf_desc_name_);
  wo_ = rhs.wo_;
  ro_ = std::move(rhs.ro_);
  default_conf_ = std::move(rhs.default_conf_);
  conf_ = std::move(rhs.conf_);
  handle_ = rhs.handle_;
}

RocksEngine::ColumnFamily& RocksEngine::ColumnFamily::operator=(RocksEngine::ColumnFamily&& rhs) noexcept {
  if (this == &rhs) {
    return *this;
  }

  cf_index_ = std::move(rhs.cf_index_);
  cf_desc_name_ = std::move(rhs.cf_desc_name_);
  wo_ = rhs.wo_;
  ro_ = std::move(rhs.ro_);
  default_conf_ = std::move(rhs.default_conf_);
  conf_ = std::move(rhs.conf_);
  handle_ = rhs.handle_;

  return *this;
}

}  // namespace dingodb

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
  explicit RocksIterator(rocksdb::Iterator* iter, const std::string& key_begin,
                         const std::string& key_end)
      : iter_(iter), key_begin_(key_begin), key_end_(key_end) {
    Start(key_begin);
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

    if (!key_end_.empty()) {
      return key_end_ > std::string(iter_->key().data(), iter_->key().size());
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
  std::string key_begin_;
  std::string key_end_;
};

class RocksReader : public EngineReader {
 public:
  explicit RocksReader(rocksdb::TransactionDB* txn_db,
                       rocksdb::ColumnFamilyHandle* handle,
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
      LOG(ERROR) << butil::StringPrintf(
          "rocksdb::TransactionDB::BeginTransaction failed");
      return false;
    }

    snapshot_ = txn_db_->GetSnapshot();

    return true;
  }

  std::shared_ptr<EngineIterator> CreateIterator(
      const std::string& key_begin, const std::string& key_end) override {
    rocksdb::ReadOptions read_options;
    read_options.snapshot = snapshot_;

    rocksdb::Iterator* iter = txn_db_->NewIterator(read_options, handle_);

    return std::make_shared<RocksIterator>(iter, key_begin, key_end);
  }

  std::shared_ptr<std::string> KvGet(const std::string& key) override {
    rocksdb::ReadOptions read_options;
    read_options.snapshot = snapshot_;
    std::string value;
    rocksdb::Status s = txn_db_->Get(read_options, handle_, key, &value);
    if (!s.ok()) {
      LOG(ERROR) << butil::StringPrintf(
          "rocksdb::TransactionDB::Get faild : %s", s.ToString().c_str());
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

const std::string& RocksEngine::k_db_path = "store.dbPath";
const std::string& RocksEngine::k_column_families = "store.columnFamilies";
const std::string& RocksEngine::k_base_column_family = "store.base";

static const char* k_block_size = "block_size";
static const char* k_block_cache = "block_cache";
static const char* k_arena_block_size = "arena_block_size";
static const char* k_min_write_buffer_number_to_merge =
    "min_write_buffer_number_to_merge";
static const char* k_max_write_buffer_number = "max_write_buffer_number";
static const char* k_max_compaction_bytes = "max_compaction_bytes";
static const char* k_write_buffer_size = "write_buffer_size";
static const char* k_prefix_extractor = "prefix_extractor";
static const char* k_max_bytes_for_level_base = "max_bytes_for_level_base";
static const char* k_target_file_size_base = "target_file_size_base";

RocksEngine::RocksEngine()
    : type_(pb::common::Engine::ENG_ROCKSDB),
      name_("ROCKS_ENGINE"),
      txn_db_(nullptr),
      map_index_cf_item_({}) {}

RocksEngine::~RocksEngine() { Close(); }

// load rocksdb config from config file
bool RocksEngine::Init(const std::shared_ptr<Config>& config) {
  bool ret = false;

  if (!config) {
    LOG(ERROR) << butil::StringPrintf("config empty not support!");
    return ret;
  }

  std::string store_db_path_value = config->GetString(k_db_path);
  if (store_db_path_value.empty()) {
    LOG(WARNING) << butil::StringPrintf("can not find : %s", k_db_path.c_str());
  }

  DLOG(INFO) << butil::StringPrintf("rocksdb path : %s",
                                    store_db_path_value.c_str());

  std::vector<std::string> vt_column_family =
      config->GetStringList(k_column_families);
  if (vt_column_family.empty()) {
    LOG(ERROR) << butil::StringPrintf("%s : empty. not found any column family",
                                      k_column_families.c_str());
    return ret;
  }

  SetDefaultIfNotExist(vt_column_family);

  InitCfConfig(vt_column_family);

  SetColumnFamilyFromConfig(config, vt_column_family);

  std::vector<rocksdb::ColumnFamilyHandle*> vt_family_handles;
  ret = RocksdbInit(store_db_path_value, vt_column_family, vt_family_handles);
  if (!ret) {
    DLOG(ERROR) << butil::StringPrintf("rocksdb::DB::Open : %s failed",
                                       store_db_path_value.c_str());
    return ret;
  }

  SetColumnFamilyHandle(vt_column_family, vt_family_handles);

  DLOG(INFO) << butil::StringPrintf("rocksdb::DB::Open : %s success!",
                                    store_db_path_value.c_str());

  ret = true;

  return ret;
}

std::string RocksEngine::GetName() { return name_; }

uint32_t RocksEngine::GetID() { return type_; }

// not implement
int RocksEngine::AddRegion([[maybe_unused]] uint64_t region_id,
                           [[maybe_unused]] const pb::common::Region& region) {
  return 0;
}

// not implement
int RocksEngine::DestroyRegion([[maybe_unused]] uint64_t region_id) {
  return 0;
}

// not implement
Snapshot* RocksEngine::GetSnapshot() { return nullptr; }
// not implement
void RocksEngine::ReleaseSnapshot() {}

std::shared_ptr<std::string> RocksEngine::KvGet(
    [[maybe_unused]] std::shared_ptr<Context> ctx, const std::string& key) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not supprt");
    return {};
  }

  if (nullptr == key.data() || key.empty()) {
    LOG(ERROR) << butil::StringPrintf("key empty or nullptr. not supprt");
    return {};
  }

  const std::string& cf = ctx->cf_name();
  auto iter = map_index_cf_item_.find(cf);
  if (iter == map_index_cf_item_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found",
                                      cf.c_str());
    return {};
  }

  rocksdb::PinnableSlice pinnable_slice;
  rocksdb::Status s = txn_db_->Get(map_index_cf_item_[cf].GetReadOptions(),
                                   map_index_cf_item_[cf].GetHandle(),
                                   rocksdb::Slice(key), &pinnable_slice);

  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Get failed : %s",
                                      s.ToString().c_str());
    return {};
  }

  std::shared_ptr<std::string> value = std::make_shared<std::string>(
      pinnable_slice.data(), pinnable_slice.size());

  return value;
}

pb::error::Errno RocksEngine::KvPut(
    [[maybe_unused]] std::shared_ptr<Context> ctx,
    const pb::common::KeyValue& kv) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not supprt");
    return {};
  }
  const std::string& key = kv.key();
  const std::string& value = kv.value();

  if (key.empty()) {
    LOG(ERROR) << butil::StringPrintf("key empty  not supprt");
    return pb::error::Errno::EKEY_FORMAT;
  }

  if (value.empty()) {
    LOG(ERROR) << butil::StringPrintf("value empty not supprt");
    return pb::error::Errno::EKEY_FORMAT;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = map_index_cf_item_.find(cf);
  if (iter == map_index_cf_item_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found",
                                      cf.c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  rocksdb::Status s = txn_db_->Put(map_index_cf_item_[cf].GetWriteOptions(),
                                   map_index_cf_item_[cf].GetHandle(),
                                   rocksdb::Slice(key), rocksdb::Slice(value));

  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Put failed : %s",
                                      s.ToString().c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvBcompareAndSet(std::shared_ptr<Context> ctx,
                                               const pb::common::KeyValue& kv,
                                               const std::string& value) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not supprt");
    return {};
  }
  const std::string& key_internal = kv.key();
  const std::string& value_internal = kv.value();

  if (key_internal.empty()) {
    LOG(ERROR) << butil::StringPrintf("key empty  not supprt");
    return pb::error::Errno::EKEY_FORMAT;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = map_index_cf_item_.find(cf);
  if (iter == map_index_cf_item_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found",
                                      cf.c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  rocksdb::Transaction* txn =
      txn_db_->BeginTransaction(map_index_cf_item_[cf].GetWriteOptions());
  if (!txn) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::TransactionDB::BeginTransaction failed");
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  std::unique_ptr<rocksdb::Transaction> utxn(txn);

  std::string value_old;

  // other read will failed
  rocksdb::Status s = utxn->GetForUpdate(
      map_index_cf_item_[cf].GetReadOptions(),
      map_index_cf_item_[cf].GetHandle(),
      rocksdb::Slice(key_internal.data(), key_internal.size()), &value_old);
  if (!s.ok()) {
    if (!(s.IsNotFound() && value_internal.empty())) {
      LOG(ERROR) << butil::StringPrintf(
          "rocksdb::TransactionDB::GetForUpdate failed : %s",
          s.ToString().c_str());
      return pb::error::Errno::EKEY_NOTFOUND;
    }
  }

  // write a key in this transaction
  s = utxn->Put(map_index_cf_item_[cf].GetHandle(),
                rocksdb::Slice(key_internal.data(), key_internal.size()),
                rocksdb::Slice(value.data(), value.size()));
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Put failed : %s",
                                      s.ToString().c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  s = txn->Commit();
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::TransactionDB::Commit failed : %s", s.ToString().c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvDelete(std::shared_ptr<Context> ctx,
                                       const std::string& key) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not supprt");
    return {};
  }

  if (key.empty()) {
    LOG(ERROR) << butil::StringPrintf("key empty  not supprt");
    return pb::error::Errno::EKEY_FORMAT;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = map_index_cf_item_.find(cf);
  if (iter == map_index_cf_item_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found",
                                      cf.c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  rocksdb::Status s = txn_db_->Delete(map_index_cf_item_[cf].GetWriteOptions(),
                                      map_index_cf_item_[cf].GetHandle(),
                                      rocksdb::Slice(key.data(), key.size()));
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::TransactionDB::Delete failed : %s", s.ToString().c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvDeleteRange(std::shared_ptr<Context> ctx,
                                            const std::string& key_begin,
                                            const std::string& key_end,
                                            bool delete_files_in_range) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not supprt");
    return {};
  }

  if (key_begin.empty()) {
    LOG(ERROR) << butil::StringPrintf("key_begin empty  not supprt");
    return pb::error::Errno::EKEY_FORMAT;
  }
  if (key_end.empty()) {
    LOG(ERROR) << butil::StringPrintf("key_end empty  not supprt");
    return pb::error::Errno::EKEY_FORMAT;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = map_index_cf_item_.find(cf);
  if (iter == map_index_cf_item_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found",
                                      cf.c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  rocksdb::Slice slice_begin{key_begin};
  rocksdb::Slice slice_end{key_end};

  rocksdb::Status s;

  if (delete_files_in_range) {
    s = rocksdb::DeleteFilesInRange(txn_db_, map_index_cf_item_[cf].GetHandle(),
                                    &slice_begin, &slice_end);
    if (!s.ok()) {
      LOG(ERROR) << butil::StringPrintf(
          "rocksdb::DeleteFilesInRange failed : %s", s.ToString().c_str());
      return pb::error::Errno::EKEY_NOTFOUND;
    }
  }

  rocksdb::TransactionDBWriteOptimizations opt;
  opt.skip_concurrency_control = true;
  opt.skip_duplicate_key_check = true;

  rocksdb::WriteBatch batch;
  s = batch.DeleteRange(map_index_cf_item_[cf].GetHandle(), slice_begin,
                        slice_end);
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::WriteBatch::DeleteRange failed : %s", s.ToString().c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  s = txn_db_->Write(map_index_cf_item_[cf].GetWriteOptions(), opt, &batch);
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::TransactionDB::Write failed : %s", s.ToString().c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  return pb::error::Errno::OK;
}

pb::error::Errno RocksEngine::KvWriteBatch(
    std::shared_ptr<Context> ctx,
    const std::vector<pb::common::KeyValue>& vt_put) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not supprt");
    return {};
  }

  const std::string& cf = ctx->cf_name();
  auto iter = map_index_cf_item_.find(cf);
  if (iter == map_index_cf_item_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found",
                                      cf.c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  rocksdb::WriteBatch batch;

  rocksdb::Status s;

  for (const auto& kv : vt_put) {
    s = batch.Put(map_index_cf_item_[cf].GetHandle(), kv.key(), kv.value());
    if (!s.ok()) {
      LOG(ERROR) << butil::StringPrintf("rocksdb::WriteBatch::Put failed : %s",
                                        s.ToString().c_str());
      return pb::error::Errno::EKEY_NOTFOUND;
    }
  }

  s = txn_db_->Write(map_index_cf_item_[cf].GetWriteOptions(), &batch);
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::TransactionDB::Write failed : %s", s.ToString().c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  return pb::error::Errno::OK;
}


pb::error::Errno RocksEngine::KvScan(std::shared_ptr<Context> ctx,
                                     const std::string& key_begin,
                                     const std::string& key_end,
                                     std::vector<pb::common::KeyValue>& vt_kv) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not supprt");
    return {};
  }

  if (key_begin.empty()) {
    LOG(ERROR) << butil::StringPrintf("key_begin empty  not supprt");
    return pb::error::Errno::EKEY_FORMAT;
  }

  if (key_end.empty()) {
    LOG(ERROR) << butil::StringPrintf("key_end empty  not supprt");
    return pb::error::Errno::EKEY_FORMAT;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = map_index_cf_item_.find(cf);
  if (iter == map_index_cf_item_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found",
                                      cf.c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  rocksdb::TransactionOptions txn_options;
  txn_options.set_snapshot = true;

  rocksdb::Transaction* txn = txn_db_->BeginTransaction(
      map_index_cf_item_[cf].GetWriteOptions(), txn_options);
  if (!txn) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::TransactionDB::BeginTransaction failed");
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  const rocksdb::Snapshot* snapshot = txn_db_->GetSnapshot();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = snapshot;

  std::unique_ptr<rocksdb::Transaction> utxn(txn);

  rocksdb::Iterator* itr =
      txn_db_->NewIterator(read_options, map_index_cf_item_[cf].GetHandle());

  itr->Seek(key_begin);

  while (itr->Valid()) {
    const auto& key = itr->key();
    const auto& value = itr->value();

    if (key_end >= key.data()) {
      break;
    }

    pb::common::KeyValue kv;
    kv.set_key(key.data(), key.size());
    kv.set_value(value.data(), value.size());

    vt_kv.emplace_back(std::move(kv));

    itr->Next();
  }

  delete itr;

#if 0
  txn_db_->ReleaseSnapshot(snapshot);
  s = txn->Commit();
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::TransactionDB::Commit failed : %s", s.ToString().c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }
#endif

  return pb::error::Errno::OK;
}

int64_t RocksEngine::KvCount(std::shared_ptr<Context> ctx,
                             const std::string& key_begin,
                             const std::string& key_end) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not supprt");
    return {};
  }

  if (key_begin.empty()) {
    LOG(ERROR) << butil::StringPrintf("key_begin empty  not supprt");
    return -1;
  }

  if (key_end.empty()) {
    LOG(ERROR) << butil::StringPrintf("key_end empty  not supprt");
    return -1;
  }

  const std::string& cf = ctx->cf_name();
  auto iter = map_index_cf_item_.find(cf);
  if (iter == map_index_cf_item_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found",
                                      cf.c_str());
    return -1;
  }

  int64_t count = 0;

  rocksdb::TransactionOptions txn_options;
  txn_options.set_snapshot = true;
  rocksdb::Transaction* txn = txn_db_->BeginTransaction(
      map_index_cf_item_[cf].GetWriteOptions(), txn_options);
  if (!txn) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::TransactionDB::BeginTransaction failed");
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  const rocksdb::Snapshot* snapshot = txn_db_->GetSnapshot();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = snapshot;

  std::unique_ptr<rocksdb::Transaction> utxn(txn);

  rocksdb::Iterator* itr =
      txn_db_->NewIterator(read_options, map_index_cf_item_[cf].GetHandle());

  if (!key_begin.empty()) {
    itr->Seek(key_begin);
  }

  while (itr->Valid()) {
    const auto& key = itr->key();

    if (key_end >= key.data()) {
      break;
    }

    count++;
  }

  delete itr;

  rocksdb::Status s;

#if 0
  txn_db_->ReleaseSnapshot(snapshot);
  s = txn->Commit();
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf(
        "rocksdb::TransactionDB::Commit failed : %s", s.ToString().c_str());
    return pb::error::Errno::EKEY_NOTFOUND;
  }
#endif

  return count;
}

std::shared_ptr<EngineReader> RocksEngine::CreateReader(
    std::shared_ptr<Context> ctx) {
  if (!ctx) {
    LOG(ERROR) << butil::StringPrintf("ctx nullptr  not supprt");
    return {};
  }

  const std::string& cf = ctx->cf_name();
  auto iter = map_index_cf_item_.find(cf);
  if (iter == map_index_cf_item_.end()) {
    LOG(ERROR) << butil::StringPrintf("column family : %s not found",
                                      cf.c_str());
    return {};
  }

  std::shared_ptr<RocksReader> reader =
      std::make_shared<RocksReader>(txn_db_, map_index_cf_item_[cf].GetHandle(),
                                    map_index_cf_item_[cf].GetWriteOptions());

  if (!reader->BeginTransaction()) {
    LOG(ERROR) << butil::StringPrintf("RocksReader::BeginTransaction cf ; %s",
                                      cf.c_str());
    return {};
  }

  return reader;
}

void RocksEngine::Close() {
  type_ = pb::common::Engine::ENG_ROCKSDB;

  if (txn_db_) {
    for (const auto& [_, cf] : map_index_cf_item_) {
      txn_db_->DestroyColumnFamilyHandle(cf.GetHandle());
    }
    map_index_cf_item_.clear();
    delete txn_db_;
    txn_db_ = nullptr;
  }

  rocksdb::DestroyDB(k_db_path, db_options_);

  DLOG(INFO) << butil::StringPrintf("rocksdb::DB::Close");
}

template <typename T>
void SetCfConfigurationElement(
    const std::map<std::string, std::string>& cf_configuration_map,
    const char* name, const T& default_value, T& value) {  // NOLINT
  auto iter = cf_configuration_map.find(name);

  if (iter == cf_configuration_map.end()) {
    value = default_value;
  } else {
    const std::string& value_string = iter->second;
    try {
      if (std::is_same_v<size_t,
                         std::remove_reference_t<std::remove_cv_t<T>>>) {
        value = std::stoul(value_string);
      } else if (std::is_same_v<uint64_t,
                                std::remove_reference_t<std::remove_cv_t<T>>>) {
        if (std::is_same_v<uint64_t, unsigned long long>) {  // NOLINT
          value = std::stoull(value_string);
        } else {
          value = std::stoul(value_string);
        }
      } else if (std::is_same_v<int,
                                std::remove_reference_t<std::remove_cv_t<T>>>) {
        value = std::stoi(value_string);
      } else {
        LOG(WARNING) << butil::StringPrintf("only support int size_t uint64_t");
        value = default_value;
      }
    } catch (const std::invalid_argument& e) {
      LOG(ERROR) << butil::StringPrintf(
          "%s trans string to  (int size_t uint64_t) failed : %s",
          value_string.c_str(), e.what());
      value = default_value;
    } catch (const std::out_of_range& e) {
      LOG(ERROR) << butil::StringPrintf(
          "%s trans string to  (int size_t uint64_t) failed : %s",
          value_string.c_str(), e.what());
      value = default_value;
    }
  }
}

template <typename T>
void SetCfConfigurationElementWrapper(
    const RocksEngine::CfDefaultConfMap& map_default_conf,
    const std::map<std::string, std::string>& cf_configuration_map,
    const char* name, T& value) {  // NOLINT
  if (auto iter = map_default_conf.find(name); iter != map_default_conf.end()) {
    if (iter->second.has_value()) {
      T default_value = static_cast<T>(std::get<int64_t>(iter->second.value()));

      SetCfConfigurationElement(cf_configuration_map, name,
                                static_cast<T>(default_value), value);
    }
  }
}

bool RocksEngine::InitCfConfig(
    const std::vector<std::string>& vt_column_family) {
  CfDefaultConfMap dcf_map_default_conf;
  dcf_map_default_conf.emplace(
      k_block_size, std::make_optional(static_cast<int64_t>(131072)));

  dcf_map_default_conf.emplace(
      k_block_cache, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_map_default_conf.emplace(
      k_arena_block_size, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_map_default_conf.emplace(k_min_write_buffer_number_to_merge,
                               std::make_optional(static_cast<int64_t>(4)));

  dcf_map_default_conf.emplace(k_max_write_buffer_number,
                               std::make_optional(static_cast<int64_t>(2)));

  dcf_map_default_conf.emplace(
      k_max_compaction_bytes,
      std::make_optional(static_cast<int64_t>(134217728)));

  dcf_map_default_conf.emplace(
      k_write_buffer_size, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_map_default_conf.emplace(k_prefix_extractor,
                               std::make_optional(static_cast<int64_t>(8)));

  dcf_map_default_conf.emplace(
      k_max_bytes_for_level_base,
      std::make_optional(static_cast<int64_t>(134217728)));

  dcf_map_default_conf.emplace(
      k_target_file_size_base,
      std::make_optional(static_cast<int64_t>(67108864)));

  for (const auto& cf : vt_column_family) {
    map_index_cf_item_.emplace(cf,
                               ColumnFamily(cf, cf, dcf_map_default_conf, {}));
  }

  return true;
}

// set cf config
bool RocksEngine::SetCfConfiguration(
    const CfDefaultConfMap& map_default_conf,
    const std::map<std::string, std::string>& cf_configuration_map,
    rocksdb::ColumnFamilyOptions* family_options) {
  rocksdb::ColumnFamilyOptions& cf_options = *family_options;

  rocksdb::BlockBasedTableOptions table_options;

  // block_size
  SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                   k_block_size, table_options.block_size);

  // block_cache
  {
    size_t value = 0;

    SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                     k_block_cache, value);

    auto cache = rocksdb::NewLRUCache(value);  // LRUcache
    table_options.block_cache = cache;
  }

  // arena_block_size

  SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                   k_arena_block_size,
                                   cf_options.arena_block_size);

  // min_write_buffer_number_to_merge
  SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                   k_min_write_buffer_number_to_merge,
                                   cf_options.min_write_buffer_number_to_merge);

  // max_write_buffer_number
  SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                   k_max_write_buffer_number,
                                   cf_options.max_write_buffer_number);

  // max_compaction_bytes
  SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                   k_max_compaction_bytes,
                                   cf_options.max_compaction_bytes);

  // write_buffer_size
  SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                   k_write_buffer_size,
                                   cf_options.write_buffer_size);

  // prefix_extractor
  {
    size_t value = 0;
    SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                     k_prefix_extractor, value);

    cf_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(value));
  }

  // max_bytes_for_level_base
  SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                   k_max_bytes_for_level_base,
                                   cf_options.max_bytes_for_level_base);

  // target_file_size_base
  SetCfConfigurationElementWrapper(map_default_conf, cf_configuration_map,
                                   k_target_file_size_base,
                                   cf_options.target_file_size_base);

  cf_options.compression_per_level = {
      rocksdb::CompressionType::kNoCompression,
      rocksdb::CompressionType::kNoCompression,
      rocksdb::CompressionType::kLZ4Compression,
      rocksdb::CompressionType::kLZ4Compression,
      rocksdb::CompressionType::kLZ4Compression,
      rocksdb::CompressionType::kZSTD,
      rocksdb::CompressionType::kZSTD,
  };

  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10.0, false));
  table_options.whole_key_filtering = true;

  cf_options.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(8));

  auto compressed_block_cache =
      rocksdb::NewLRUCache(1024 * 1024 * 1024);  // LRUcache

  // table_options.block_cache_compressed.reset(compressed_block_cache);

  rocksdb::TableFactory* table_factory =
      NewBlockBasedTableFactory(table_options);
  cf_options.table_factory.reset(table_factory);

  return true;
}

void RocksEngine::SetDefaultIfNotExist(
    std::vector<std::string>& vt_column_family) {
  // First find the default configuration, if there is, then exchange the
  // position, if not, add
  bool found_default = false;
  size_t i = 0;
  for (; i < vt_column_family.size(); i++) {
    if (vt_column_family[i] == ROCKSDB_NAMESPACE::kDefaultColumnFamilyName) {
      found_default = true;
      break;
    }
  }

  if (found_default) {
    if (0 != i) {
      std::swap(vt_column_family[i], vt_column_family[0]);
    }
  } else {
    vt_column_family.insert(vt_column_family.begin(),
                            ROCKSDB_NAMESPACE::kDefaultColumnFamilyName);
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

bool RocksEngine::RocksdbInit(
    const std::string& db_path,
    const std::vector<std::string>& vt_column_family,
    std::vector<rocksdb::ColumnFamilyHandle*>& vt_family_handles) {
  // cppcheck-suppress variableScope
  bool ret = false;
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  for (const auto& column_family : vt_column_family) {
    rocksdb::ColumnFamilyOptions family_options;
    SetCfConfiguration(map_index_cf_item_[column_family].GetDefaultConfMap(),
                       map_index_cf_item_[column_family].GetConfMap(),
                       &family_options);

    column_families.push_back(
        rocksdb::ColumnFamilyDescriptor(column_family, family_options));
  }

  rocksdb::DBOptions db_options;
  rocksdb::TransactionDBOptions txn_db_options;

  db_options.create_if_missing = true;
  db_options.create_missing_column_families = true;

  rocksdb::Status s = rocksdb::TransactionDB::Open(
      db_options, txn_db_options, db_path, column_families, &vt_family_handles,
      &txn_db_);
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::TransactionDB::Open faild : %s",
                                      s.ToString().c_str());
    return ret;
  }

  ret = true;

  return ret;
}

void RocksEngine::SetColumnFamilyHandle(
    const std::vector<std::string>& vt_column_family,
    const std::vector<rocksdb::ColumnFamilyHandle*>& vt_family_handles) {
  size_t i = 0;
  for (const auto& column_family : vt_column_family) {
    map_index_cf_item_[column_family].SetHandle(vt_family_handles[i++]);
  }
}

void RocksEngine::SetColumnFamilyFromConfig(
    const std::shared_ptr<Config>& config,
    const std::vector<std::string>& vt_column_family) {
  // get base column family configure. allow empty
  const std::map<std::string, std::string>& map_base_cf_configuration =
      config->GetStringMap(k_base_column_family);

  // assign values ​​to each column family
  for (const auto& column_family : vt_column_family) {
    std::string column_family_key("store." + column_family);
    // get column family configure
    const std::map<std::string, std::string>& map_cf_configuration =
        config->GetStringMap(column_family_key);

    std::map<std::string, std::string> map_new_cf_configuration;

    CreateNewMap(map_base_cf_configuration, map_cf_configuration,
                 map_new_cf_configuration);

    map_index_cf_item_[column_family].SetConfMap(map_new_cf_configuration);
  }
}

RocksEngine::ColumnFamily::ColumnFamily()
    : ColumnFamily("", "", {}, {}, {}, {}, nullptr){};  // NOLINT

RocksEngine::ColumnFamily::ColumnFamily(
    const std::string& cf_index, const std::string& cf_desc_name,
    const rocksdb::WriteOptions& wo, const rocksdb::ReadOptions& ro,
    const CfDefaultConfMap& map_default_conf,
    const std::map<std::string, std::string>& map_conf,
    rocksdb::ColumnFamilyHandle* handle)
    : cf_index_(cf_index),
      cf_desc_name_(cf_desc_name),
      wo_(wo),
      ro_(ro),
      map_default_conf_(map_default_conf),
      map_conf_(map_conf),
      handle_(handle) {}

RocksEngine::ColumnFamily::ColumnFamily(
    const std::string& cf_index, const std::string& cf_desc_name,
    const rocksdb::WriteOptions& wo, const rocksdb::ReadOptions& ro,
    const CfDefaultConfMap& map_default_conf,
    const std::map<std::string, std::string>& map_conf)
    : ColumnFamily(cf_index, cf_desc_name, wo, ro, map_default_conf, map_conf,
                   nullptr) {}

RocksEngine::ColumnFamily::ColumnFamily(
    const std::string& cf_index, const std::string& cf_desc_name,
    const CfDefaultConfMap& map_default_conf,
    const std::map<std::string, std::string>& map_conf)
    : ColumnFamily(cf_index, cf_desc_name, {}, {}, map_default_conf, map_conf,
                   nullptr) {}

RocksEngine::ColumnFamily::~ColumnFamily() {
  cf_index_ = "";
  cf_desc_name_ = "";
  map_default_conf_.clear();
  map_conf_.clear();
  handle_ = nullptr;
}

RocksEngine::ColumnFamily::ColumnFamily(const RocksEngine::ColumnFamily& rhs) {
  cf_index_ = rhs.cf_index_;
  cf_desc_name_ = rhs.cf_desc_name_;
  wo_ = rhs.wo_;
  ro_ = rhs.ro_;
  map_default_conf_ = rhs.map_default_conf_;
  map_conf_ = rhs.map_conf_;
  handle_ = rhs.handle_;
}

RocksEngine::ColumnFamily& RocksEngine::ColumnFamily::operator=(
    const RocksEngine::ColumnFamily& rhs) {
  if (this == &rhs) {
    return *this;
  }

  cf_index_ = rhs.cf_index_;
  cf_desc_name_ = rhs.cf_desc_name_;
  wo_ = rhs.wo_;
  ro_ = rhs.ro_;
  map_default_conf_ = rhs.map_default_conf_;
  map_conf_ = rhs.map_conf_;
  handle_ = rhs.handle_;

  return *this;
}

RocksEngine::ColumnFamily::ColumnFamily(
    RocksEngine::ColumnFamily&& rhs) noexcept {
  cf_index_ = std::move(rhs.cf_index_);
  cf_desc_name_ = std::move(rhs.cf_desc_name_);
  wo_ = rhs.wo_;
  ro_ = std::move(rhs.ro_);
  map_default_conf_ = std::move(rhs.map_default_conf_);
  map_conf_ = std::move(rhs.map_conf_);
  handle_ = rhs.handle_;
}

RocksEngine::ColumnFamily& RocksEngine::ColumnFamily::operator=(
    RocksEngine::ColumnFamily&& rhs) noexcept {
  if (this == &rhs) {
    return *this;
  }

  cf_index_ = std::move(rhs.cf_index_);
  cf_desc_name_ = std::move(rhs.cf_desc_name_);
  wo_ = rhs.wo_;
  ro_ = std::move(rhs.ro_);
  map_default_conf_ = std::move(rhs.map_default_conf_);
  map_conf_ = std::move(rhs.map_conf_);
  handle_ = rhs.handle_;

  return *this;
}

}  // namespace dingodb

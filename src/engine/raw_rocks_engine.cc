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

#include "engine/raw_rocks_engine.h"

#include <elf.h>

#include <algorithm>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/macros.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/safe_map.h"
#include "config/config_manager.h"
#include "engine/engine.h"
#include "engine/raft_kv_engine.h"
#include "engine/raw_engine.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"

namespace dingodb {

class RocksIterator : public EngineIterator {
 public:
  explicit RocksIterator(std::shared_ptr<dingodb::Snapshot> snapshot, std::shared_ptr<rocksdb::DB> db,
                         std::shared_ptr<RawRocksEngine::ColumnFamily> column_family, const std::string& start_key,
                         const std::string& end_key)
      : snapshot_(snapshot),
        db_(db),
        column_family_(column_family),
        iter_(nullptr),
        start_key_(start_key),
        end_key_(end_key),
        with_start_(true),
        with_end_(false),
        has_valid_kv_(false) {
    rocksdb::ReadOptions read_option;
    read_option.auto_prefix_mode = true;
    read_option.snapshot = static_cast<const rocksdb::Snapshot*>(
        std::dynamic_pointer_cast<RawRocksEngine::RocksSnapshot>(snapshot)->Inner());

    iter_ = db_->NewIterator(read_option, column_family_->GetHandle());
  }
  void Start() override {
    iter_->Seek(start_key_);

    while (iter_->Valid()) {
      rocksdb::Slice key = iter_->key();
      if (FilterStartKey(key)) {
        has_valid_kv_ = true;
        return;
      }
      iter_->Next();
    }

    has_valid_kv_ = false;
  }

  bool HasNext() override {
    if (iter_->Valid()) {
      rocksdb::Slice key = iter_->key();
      if (FilterEndKey(key)) {
        has_valid_kv_ = true;
        return true;
      }
    }
    has_valid_kv_ = false;
    return false;
  }

  void Next() override { iter_->Next(); }

  bool GetKV(std::string& key, std::string& value) {  // NOLINT
    if (has_valid_kv_) {
      key.assign(iter_->key().data(), iter_->key().size());
      value.assign(iter_->value().data(), iter_->value().size());
    }

    return has_valid_kv_;
  }

  bool GetKey(std::string& key) override {
    if (has_valid_kv_) {
      key.assign(iter_->key().data(), iter_->key().size());
    }
    return has_valid_kv_;
  }
  bool GetValue(std::string& value) override {
    if (has_valid_kv_) {
      value.assign(iter_->value().data(), iter_->value().size());
    }
    return has_valid_kv_;
  }

  const std::string& GetName() const override { return name_; }
  uint32_t GetID() override { return id_; }

  ~RocksIterator() override {
    if (iter_) {
      delete iter_;
      iter_ = nullptr;
    }
    column_family_ = nullptr;
    snapshot_ = nullptr;
    db_ = nullptr;
    has_valid_kv_ = false;
  }

 protected:
  //
 private:
  [[maybe_unused]] bool FilterKey(const rocksdb::Slice& key) {
    // such as key >= start_key_ && key <= end_key_
    return FilterStartKey(key) && FilterEndKey(key);
  }

  // key >= start_key_ || key > start_key_
  bool FilterStartKey(const rocksdb::Slice& key) {
    size_t min = std ::min(key.size(), start_key_.size());
    int cmp = memcmp(key.data(), start_key_.data(), min);

    if (cmp > 0) {
      return true;
    } else if (cmp < 0) {
      return false;
    } else {  // 0 ==  cmp
      if (key.size() > start_key_.size()) {
        return with_start_;
      } else if (key.size() < start_key_.size()) {
        return false;
      } else {  // key and start_key_ same [data and size]
        return with_start_;
      }
    }
  }

  // key <= end_key_ ||  key < end_key_
  bool FilterEndKey(const rocksdb::Slice& key) {
    size_t min = std::min(key.size(), end_key_.size());
    int cmp = memcmp(key.data(), end_key_.data(), min);

    if (cmp > 0) {
      return false;
    } else if (cmp < 0) {
      return true;
    } else {  // 0 ==  cmp
      if (key.size() > end_key_.size()) {
        return with_end_;
      } else if (key.size() < end_key_.size()) {
        return true;
      } else {  // key and end_key_ same [data and size]
        return with_end_;
      }
    }
  }

  std::shared_ptr<dingodb::Snapshot> snapshot_;
  std::shared_ptr<rocksdb::DB> db_;
  std::shared_ptr<RawRocksEngine::ColumnFamily> column_family_;
  rocksdb::Iterator* iter_;
  const std::string name_ = "Rocks";
  uint32_t id_ = static_cast<uint32_t>(EnumEngineIterator::kRocks);
  std::string start_key_;
  std::string end_key_;
  bool with_start_;
  bool with_end_;
  bool has_valid_kv_;
};

RawRocksEngine::RawRocksEngine() : db_(nullptr), column_families_({}) {}

RawRocksEngine::~RawRocksEngine() = default;

// load rocksdb config from config file
bool RawRocksEngine::Init(std::shared_ptr<Config> config) {
  if (BAIDU_UNLIKELY(!config)) {
    DINGO_LOG(ERROR) << fmt::format("config empty not support!");
    return false;
  }

  std::string store_db_path_value = config->GetString(Constant::kDbPath);
  if (BAIDU_UNLIKELY(store_db_path_value.empty())) {
    DINGO_LOG(ERROR) << fmt::format("can not find : {}", Constant::kDbPath);
    return false;
  }

  db_path_ = store_db_path_value;

  DINGO_LOG(INFO) << fmt::format("rocksdb path : {}", db_path_);

  std::vector<std::string> column_families = config->GetStringList(Constant::kColumnFamilies);
  if (BAIDU_UNLIKELY(column_families.empty())) {
    DINGO_LOG(ERROR) << fmt::format("{} : empty. not found any column family", Constant::kColumnFamilies);
    return false;
  }

  SetDefaultIfNotExist(column_families);

  InitCfConfig(column_families);

  SetColumnFamilyFromConfig(config, column_families);

  std::vector<rocksdb::ColumnFamilyHandle*> family_handles;
  bool ret = RocksdbInit(config, db_path_, column_families, family_handles);
  if (BAIDU_UNLIKELY(!ret)) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Open : {} failed", db_path_);
    return false;
  }

  SetColumnFamilyHandle(column_families, family_handles);

  DINGO_LOG(INFO) << fmt::format("rocksdb::DB::Open : {} success!", db_path_);

  return true;
}

std::string RawRocksEngine::GetName() { return pb::common::RawEngine_Name(pb::common::RAW_ENG_ROCKSDB); }

pb::common::RawEngine RawRocksEngine::GetID() { return pb::common::RAW_ENG_ROCKSDB; }

std::shared_ptr<Snapshot> RawRocksEngine::GetSnapshot() {
  return std::make_shared<RocksSnapshot>(db_->GetSnapshot(), db_);
}

butil::Status RawRocksEngine::MergeCheckpointFile(const std::string& path, const pb::common::Range& range,
                                                  std::string& merge_sst_path) {
  rocksdb::Options options;
  options.create_if_missing = false;

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families = {
      rocksdb::ColumnFamilyDescriptor(Constant::kStoreDataCF, rocksdb::ColumnFamilyOptions())};

  // Due to delete other region sst file, so need repair db, or rocksdb::DB::Open will fail.
  auto status = rocksdb::RepairDB(path, options, column_families);
  if (!status.ok()) {
    return butil::Status(pb::error::EINTERNAL, fmt::format("Rocksdb Repair db failed, {}", status.ToString()));
  }

  // Open snapshot db.
  rocksdb::DB* snapshot_db = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  status = rocksdb::DB::OpenForReadOnly(options, path, column_families, &handles, &snapshot_db);
  if (!status.ok()) {
    return butil::Status(pb::error::EINTERNAL, fmt::format("Rocksdb open checkpoint failed, {}", status.ToString()));
  }

  // Create iterator
  IteratorOptions iter_options;
  iter_options.upper_bound = range.end_key();

  rocksdb::ReadOptions read_options;
  read_options.auto_prefix_mode = true;

  auto iter =
      std::make_shared<RawRocksEngine::Iterator>(iter_options, snapshot_db->NewIterator(read_options, handles[0]));
  iter->Seek(range.start_key());

  // Create sst writer
  return NewSstFileWriter()->SaveFile(iter, merge_sst_path);
}

butil::Status RawRocksEngine::IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files) {
  rocksdb::IngestExternalFileOptions options;
  options.write_global_seqno = false;
  auto status = db_->IngestExternalFile(GetColumnFamily(cf_name)->GetHandle(), files, options);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "IngestExternalFile failed " << status.ToString();
    return butil::Status(status.code(), status.ToString());
  }

  return butil::Status();
}

void RawRocksEngine::Flush(const std::string& cf_name) {
  if (db_) {
    rocksdb::FlushOptions flush_options;
    db_->Flush(flush_options, GetColumnFamily(cf_name)->GetHandle());
  }
}

void RawRocksEngine::Destroy() { rocksdb::DestroyDB(db_path_, db_options_); }

std::shared_ptr<dingodb::Snapshot> RawRocksEngine::NewSnapshot() {
  return std::make_shared<RocksSnapshot>(db_->GetSnapshot(), db_);
}

std::shared_ptr<RawEngine::Reader> RawRocksEngine::NewReader(const std::string& cf_name) {
  auto column_family = GetColumnFamily(cf_name);
  if (column_family == nullptr) {
    return nullptr;
  }
  return std::make_shared<Reader>(db_, column_family);
}

std::shared_ptr<RawEngine::Writer> RawRocksEngine::NewWriter(const std::string& cf_name) {
  auto column_family = GetColumnFamily(cf_name);
  if (column_family == nullptr) {
    return nullptr;
  }
  return std::make_shared<Writer>(db_, column_family);
}

std::shared_ptr<dingodb::Iterator> RawRocksEngine::NewIterator(const std::string& cf_name, IteratorOptions options) {
  return NewIterator(cf_name, NewSnapshot(), options);
}

std::shared_ptr<dingodb::Iterator> RawRocksEngine::NewIterator(const std::string& cf_name,
                                                               std::shared_ptr<Snapshot> snapshot,
                                                               IteratorOptions options) {
  auto column_family = GetColumnFamily(cf_name);
  if (column_family == nullptr) {
    return nullptr;
  }

  // Correct free iterate_upper_bound
  // auto slice = std::make_unique<rocksdb::Slice>(options.upper_bound);
  rocksdb::ReadOptions read_options;
  if (snapshot != nullptr) {
    read_options.snapshot = static_cast<const rocksdb::Snapshot*>(snapshot->Inner());
  }
  read_options.auto_prefix_mode = true;
  // if (!options.upper_bound.empty()) {
  //   read_options.iterate_upper_bound = slice.get();
  // }
  return std::make_shared<RawRocksEngine::Iterator>(options, db_->NewIterator(read_options, column_family->GetHandle()),
                                                    snapshot);
}

std::shared_ptr<RawRocksEngine::SstFileWriter> RawRocksEngine::NewSstFileWriter() {
  return std::make_shared<RawRocksEngine::SstFileWriter>(rocksdb::Options());
}

std::shared_ptr<RawRocksEngine::Checkpoint> RawRocksEngine::NewCheckpoint() {
  return std::make_shared<RawRocksEngine::Checkpoint>(db_);
}

void RawRocksEngine::Close() {
  if (db_) {
    column_families_.clear();
    db_->Close();
    db_ = nullptr;
  }

  DINGO_LOG(INFO) << fmt::format("rocksdb::DB::Close");
}

std::shared_ptr<RawRocksEngine::ColumnFamily> RawRocksEngine::GetColumnFamily(const std::string& cf_name) {
  auto iter = column_families_.find(cf_name);
  if (iter == column_families_.end()) {
    DINGO_LOG(ERROR) << fmt::format("column family {} not found", cf_name);
    return nullptr;
  }

  return iter->second;
}

std::vector<uint64_t> RawRocksEngine::GetApproximateSizes(const std::string& cf_name,
                                                          std::vector<pb::common::Range>& ranges) {
  rocksdb::SizeApproximationOptions options;

  rocksdb::Range inner_ranges[ranges.size()];
  for (int i = 0; i < ranges.size(); ++i) {
    inner_ranges[i].start = ranges[i].start_key();
    inner_ranges[i].limit = ranges[i].end_key();
  }

  uint64_t sizes[ranges.size()];
  db_->GetApproximateSizes(options, GetColumnFamily(cf_name)->GetHandle(), inner_ranges, ranges.size(), sizes);

  std::vector<uint64_t> result;
  result.reserve(ranges.size());
  for (int i = 0; i < ranges.size(); ++i) {
    result.push_back(sizes[i]);
  }

  return result;
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
      } else if (std::is_same_v<double, std::remove_reference_t<std::remove_cv_t<T>>>) {
        value = std::stod(value_string);
      } else {
        DINGO_LOG(WARNING) << fmt::format("only support int size_t uint64_t");
        value = default_value;
      }
    } catch (const std::invalid_argument& e) {
      DINGO_LOG(ERROR) << fmt::format("{} trans string to  (int size_t uint64_t) failed : {}", value_string, e.what());
      value = default_value;
    } catch (const std::out_of_range& e) {
      DINGO_LOG(ERROR) << fmt::format("{} trans string to  (int size_t uint64_t) failed : {}", value_string, e.what());
      value = default_value;
    }
  }
}

template <typename T>
void SetCfConfigurationElementWrapper(const RawRocksEngine::CfDefaultConf& default_conf,
                                      const std::map<std::string, std::string>& cf_configuration, const char* name,
                                      T& value) {  // NOLINT
  if (auto iter = default_conf.find(name); iter != default_conf.end()) {
    if (iter->second.has_value()) {
      T default_value = static_cast<T>(std::get<int64_t>(iter->second.value()));

      SetCfConfigurationElement(cf_configuration, name, static_cast<T>(default_value), value);
    }
  }
}

bool RawRocksEngine::InitCfConfig(const std::vector<std::string>& column_families) {
  CfDefaultConf dcf_default_conf;
  dcf_default_conf.emplace(Constant::kBlockSize, std::make_optional(static_cast<int64_t>(131072)));

  dcf_default_conf.emplace(Constant::kBlockCache, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf.emplace(Constant::kArenaBlockSize, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf.emplace(Constant::kMinWriteBufferNumberToMerge, std::make_optional(static_cast<int64_t>(4)));

  dcf_default_conf.emplace(Constant::kMaxWriteBufferNumber, std::make_optional(static_cast<int64_t>(2)));

  dcf_default_conf.emplace(Constant::kMaxCompactionBytes, std::make_optional(static_cast<int64_t>(134217728)));

  dcf_default_conf.emplace(Constant::kWriteBufferSize, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf.emplace(Constant::kPrefixExtractor, std::make_optional(static_cast<int64_t>(8)));

  dcf_default_conf.emplace(Constant::kMaxBytesForLevelBase, std::make_optional(static_cast<int64_t>(134217728)));

  dcf_default_conf.emplace(Constant::kTargetFileSizeBase, std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf.emplace(Constant::kMaxBytesForLevelMultiplier, std::make_optional(static_cast<int64_t>(10)));

  for (const auto& cf_name : column_families) {
    std::map<std::string, std::string> conf;
    column_families_.emplace(cf_name, std::make_shared<ColumnFamily>(cf_name, dcf_default_conf, conf));
  }

  return true;
}

// set cf config
bool RawRocksEngine::SetCfConfiguration(const CfDefaultConf& default_conf,
                                        const std::map<std::string, std::string>& cf_configuration,
                                        rocksdb::ColumnFamilyOptions* family_options) {
  rocksdb::ColumnFamilyOptions& cf_options = *family_options;

  rocksdb::BlockBasedTableOptions table_options;

  // block_size
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kBlockSize.c_str(),
                                   table_options.block_size);

  // block_cache
  {
    size_t value = 0;

    SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kBlockCache.c_str(), value);

    auto cache = rocksdb::NewLRUCache(value);  // LRUcache
    table_options.block_cache = cache;
  }

  // arena_block_size

  SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kArenaBlockSize.c_str(),
                                   cf_options.arena_block_size);

  // min_write_buffer_number_to_merge
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kMinWriteBufferNumberToMerge.c_str(),
                                   cf_options.min_write_buffer_number_to_merge);

  // max_write_buffer_number
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kMaxWriteBufferNumber.c_str(),
                                   cf_options.max_write_buffer_number);

  // max_compaction_bytes
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kMaxCompactionBytes.c_str(),
                                   cf_options.max_compaction_bytes);

  // write_buffer_size
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kWriteBufferSize.c_str(),
                                   cf_options.write_buffer_size);

  // max_bytes_for_level_multiplier
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kMaxBytesForLevelMultiplier.c_str(),
                                   cf_options.max_bytes_for_level_multiplier);

  // prefix_extractor
  {
    size_t value = 0;
    SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kPrefixExtractor.c_str(), value);

    cf_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(value));
  }

  // max_bytes_for_level_base
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kMaxBytesForLevelBase.c_str(),
                                   cf_options.max_bytes_for_level_base);

  // target_file_size_base
  SetCfConfigurationElementWrapper(default_conf, cf_configuration, Constant::kTargetFileSizeBase.c_str(),
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

void RawRocksEngine::SetDefaultIfNotExist(std::vector<std::string>& column_families) {
  // First find the default configuration, if there is, then exchange the
  // position, if not, add
  bool found_default = false;
  size_t i = 0;
  for (; i < column_families.size(); i++) {
    if (column_families[i] == ROCKSDB_NAMESPACE::kDefaultColumnFamilyName) {
      found_default = true;
      break;
    }
  }

  if (found_default) {
    if (0 != i) {
      std::swap(column_families[i], column_families[0]);
    }
  } else {
    column_families.insert(column_families.begin(), ROCKSDB_NAMESPACE::kDefaultColumnFamilyName);
  }
}

void RawRocksEngine::CreateNewMap(const std::map<std::string, std::string>& base,
                                  const std::map<std::string, std::string>& cf,
                                  std::map<std::string, std::string>& new_cf) {
  new_cf = base;

  for (const auto& [key, value] : cf) {
    new_cf[key] = value;
  }
}

int GetBackgroundThreadNum(std::shared_ptr<dingodb::Config> config) {
  int num = config->GetInt("store.background_thread_num");
  if (num <= 0) {
    double ratio = config->GetDouble("store.background_thread_ratio");
    if (ratio > 0) {
      num = std::round(ratio * static_cast<double>(dingodb::Helper::GetCoreNum()));
    }
  }

  return num > 0 ? num : Constant::kRocksdbBackgroundThreadNumDefault;
}

int GetStatsDumpPeriodSec(std::shared_ptr<dingodb::Config> config) {
  int num = config->GetInt("store.stats_dump_period_sec");
  return (num <= 0) ? Constant::kStatsDumpPeriodSecDefault : num;
}

bool RawRocksEngine::RocksdbInit(std::shared_ptr<Config> config, const std::string& db_path,
                                 const std::vector<std::string>& column_family,
                                 std::vector<rocksdb::ColumnFamilyHandle*>& family_handles) {
  // cppcheck-suppress variableScope
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  for (const auto& column_family : column_family) {
    rocksdb::ColumnFamilyOptions family_options;
    SetCfConfiguration(column_families_[column_family]->GetDefaultConf(), column_families_[column_family]->GetConf(),
                       &family_options);

    column_families.push_back(rocksdb::ColumnFamilyDescriptor(column_family, family_options));
  }

  rocksdb::DBOptions db_options;
  db_options.create_if_missing = true;
  db_options.create_missing_column_families = true;
  db_options.max_background_jobs = GetBackgroundThreadNum(config);
  db_options.max_subcompactions = db_options.max_background_jobs / 4 * 3;
  db_options.stats_dump_period_sec = GetStatsDumpPeriodSec(config);

  rocksdb::DB* db;
  rocksdb::Status s = rocksdb::DB::Open(db_options, db_path, column_families, &family_handles, &db);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Open failed : {}", s.ToString());
    return false;
  }

  db_.reset(db);

  return true;
}

void RawRocksEngine::SetColumnFamilyHandle(const std::vector<std::string>& column_families,
                                           const std::vector<rocksdb::ColumnFamilyHandle*>& family_handles) {
  size_t i = 0;
  for (const auto& column_family : column_families) {
    column_families_[column_family]->SetHandle(family_handles[i++]);
  }
}

void RawRocksEngine::SetColumnFamilyFromConfig(const std::shared_ptr<Config>& config,
                                               const std::vector<std::string>& column_families) {
  // get base column family configure. allow empty
  const std::map<std::string, std::string>& base_cf_configuration = config->GetStringMap(Constant::kBaseColumnFamily);

  // assign values ​​to each column family
  for (const auto& column_family : column_families) {
    std::string column_family_key("store." + column_family);
    // get column family configure
    const std::map<std::string, std::string>& cf_configuration = config->GetStringMap(column_family_key);

    std::map<std::string, std::string> new_cf_configuration;

    CreateNewMap(base_cf_configuration, cf_configuration, new_cf_configuration);

    column_families_[column_family]->SetConf(new_cf_configuration);
  }
}

RawRocksEngine::ColumnFamily::ColumnFamily() : ColumnFamily("", {}, {}, nullptr){};  // NOLINT

RawRocksEngine::ColumnFamily::ColumnFamily(const std::string& cf_name, const CfDefaultConf& default_conf,
                                           const std::map<std::string, std::string>& conf,
                                           rocksdb::ColumnFamilyHandle* handle)
    : name_(cf_name), default_conf_(default_conf), conf_(conf), handle_(handle) {}

RawRocksEngine::ColumnFamily::ColumnFamily(const std::string& cf_name, const CfDefaultConf& default_conf,
                                           const std::map<std::string, std::string>& conf)
    : ColumnFamily(cf_name, default_conf, conf, nullptr) {}

RawRocksEngine::ColumnFamily::~ColumnFamily() {
  name_ = "";
  default_conf_.clear();
  conf_.clear();
  delete handle_;
  handle_ = nullptr;
}

RawRocksEngine::ColumnFamily::ColumnFamily(const RawRocksEngine::ColumnFamily& rhs) {
  name_ = rhs.name_;
  default_conf_ = rhs.default_conf_;
  conf_ = rhs.conf_;
  handle_ = rhs.handle_;
}

RawRocksEngine::ColumnFamily& RawRocksEngine::ColumnFamily::operator=(const RawRocksEngine::ColumnFamily& rhs) {
  if (this == &rhs) {
    return *this;
  }

  name_ = rhs.name_;
  default_conf_ = rhs.default_conf_;
  conf_ = rhs.conf_;
  handle_ = rhs.handle_;

  return *this;
}

RawRocksEngine::ColumnFamily::ColumnFamily(RawRocksEngine::ColumnFamily&& rhs) noexcept {
  name_ = std::move(rhs.name_);
  default_conf_ = std::move(rhs.default_conf_);
  conf_ = std::move(rhs.conf_);
  handle_ = rhs.handle_;
}

RawRocksEngine::ColumnFamily& RawRocksEngine::ColumnFamily::operator=(RawRocksEngine::ColumnFamily&& rhs) noexcept {
  if (this == &rhs) {
    return *this;
  }

  name_ = std::move(rhs.name_);
  default_conf_ = std::move(rhs.default_conf_);
  conf_ = std::move(rhs.conf_);
  handle_ = rhs.handle_;

  return *this;
}

butil::Status RawRocksEngine::Reader::KvGet(const std::string& key, std::string& value) {
  auto snapshot = std::make_shared<RocksSnapshot>(db_->GetSnapshot(), db_);
  return KvGet(snapshot, key, value);
}

butil::Status RawRocksEngine::Reader::KvGet(std::shared_ptr<dingodb::Snapshot> snapshot, const std::string& key,
                                            std::string& value) {
  if (BAIDU_UNLIKELY(key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("key empty not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::ReadOptions read_option;
  read_option.snapshot = static_cast<const rocksdb::Snapshot*>(snapshot->Inner());
  rocksdb::Status s = db_->Get(read_option, column_family_->GetHandle(), rocksdb::Slice(key), &value);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found");
    }
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Get failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal get error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Reader::VectorSearch(uint64_t region_id, const pb::common::VectorWithId& vector,
                                                   const pb::common::VectorSearchParameter& parameter,
                                                   std::vector<pb::common::VectorWithDistance>& vectors) {
  if (BAIDU_UNLIKELY(vector.vector().values_size() == 0)) {
    DINGO_LOG(ERROR) << fmt::format("vector empty not support");
    return butil::Status(pb::error::EVECTOR_EMPTY, "vector is empty");
  }

  if (parameter.top_n() == 0) {
    DINGO_LOG(ERROR) << fmt::format("top_n is 0 not support");
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "top_n is 0");
  }

  std::string key_header = std::to_string(region_id);

  if (vector.id() > 0) {
    auto snapshot = std::make_shared<RocksSnapshot>(db_->GetSnapshot(), db_);
    rocksdb::ReadOptions read_option;
    read_option.snapshot = static_cast<const rocksdb::Snapshot*>(snapshot->Inner());
    std::string value;
    rocksdb::Status s =
        db_->Get(read_option, column_family_->GetHandle(), key_header + std::to_string(vector.id()), &value);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found");
      }
      DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Get failed : {}", s.ToString());
      return butil::Status(pb::error::EINTERNAL, "Internal error");
    }

    pb::common::Vector result;
    if (!result.ParseFromString(value)) {
      DINGO_LOG(ERROR) << fmt::format("ParseFromString failed");
      return butil::Status(pb::error::EINTERNAL, "Internal error");
    }

    pb::common::VectorWithDistance vector_with_distance;
    vector_with_distance.set_id(vector.id());
    vector_with_distance.set_distance(0);
    vector_with_distance.mutable_vector()->CopyFrom(result);

    vectors.push_back(vector_with_distance);

    return butil::Status();
  }

  // for (int i = 0; i < 10; i++) {
  //   pb::common::VectorWithDistance vector_with_distance;
  //   vector_with_distance.set_id(i);
  //   vector_with_distance.set_distance(i);

  //   for (int j = 0; j < 16; j++) {
  //     vector_with_distance.mutable_vector()->add_values(i * 16 + j);
  //   }

  //   vectors.push_back(vector_with_distance);
  // }

  std::shared_ptr<VectorIndex> vector_index;
  auto ret = Server::GetInstance()->GetRegionController()->vector_index_map.Get(region_id, vector_index);
  if (ret < 0) {
    DINGO_LOG(ERROR) << fmt::format("Get vector_index failed");
    return butil::Status(pb::error::EINTERNAL, "Internal error, Get vector_index failed");
  }

  vector_index->Search(vector, parameter.top_n(), vectors);

  return butil::Status();
}

butil::Status RawRocksEngine::Reader::KvScan(const std::string& start_key, const std::string& end_key,
                                             std::vector<pb::common::KeyValue>& kvs) {
  auto snapshot = std::make_shared<RocksSnapshot>(db_->GetSnapshot(), db_);
  return KvScan(snapshot, start_key, end_key, kvs);
}

butil::Status RawRocksEngine::Reader::KvScan(std::shared_ptr<dingodb::Snapshot> snapshot, const std::string& start_key,
                                             const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) {
  if (BAIDU_UNLIKELY(start_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("start_key empty  not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("end_key empty  not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::ReadOptions read_option;
  read_option.auto_prefix_mode = true;
  read_option.snapshot = static_cast<const rocksdb::Snapshot*>(snapshot->Inner());

  std::string_view end_key_view(end_key);
  rocksdb::Iterator* it = db_->NewIterator(read_option, column_family_->GetHandle());
  for (it->Seek(start_key); it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    pb::common::KeyValue kv;
    kv.set_key(it->key().data(), it->key().size());
    kv.set_value(it->value().data(), it->value().size());

    kvs.emplace_back(std::move(kv));
  }
  delete it;

  return butil::Status();
}

butil::Status RawRocksEngine::Reader::KvCount(const std::string& start_key, const std::string& end_key,
                                              uint64_t& count) {
  auto snapshot = std::make_shared<RocksSnapshot>(db_->GetSnapshot(), db_);
  return KvCount(snapshot, start_key, end_key, count);
}

butil::Status RawRocksEngine::Reader::KvCount(std::shared_ptr<dingodb::Snapshot> snapshot, const std::string& start_key,
                                              const std::string& end_key, uint64_t& count) {
  if (BAIDU_UNLIKELY(start_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("start_key empty not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("end_key empty not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::ReadOptions read_options;
  read_options.auto_prefix_mode = true;
  read_options.snapshot = static_cast<const rocksdb::Snapshot*>(snapshot->Inner());

  std::string_view end_key_view(end_key.data(), end_key.size());
  rocksdb::Iterator* it = db_->NewIterator(read_options, column_family_->GetHandle());
  for (it->Seek(start_key), count = 0; it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    ++count;
  }
  delete it;

  return butil::Status();
}

std::shared_ptr<EngineIterator> RawRocksEngine::Reader::NewIterator(const std::string& start_key,
                                                                    const std::string& end_key) {
  auto snapshot = std::make_shared<RocksSnapshot>(db_->GetSnapshot(), db_);
  return NewIterator(snapshot, start_key, end_key);
}

std::shared_ptr<EngineIterator> RawRocksEngine::Reader::NewIterator(std::shared_ptr<dingodb::Snapshot> snapshot,
                                                                    const std::string& start_key,
                                                                    const std::string& end_key) {
  return std::make_shared<RocksIterator>(snapshot, db_, column_family_, start_key, end_key);
}

butil::Status RawRocksEngine::Writer::KvPut(const pb::common::KeyValue& kv) {
  if (BAIDU_UNLIKELY(kv.key().empty())) {
    DINGO_LOG(ERROR) << fmt::format("key empty  not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::WriteOptions write_options;
  rocksdb::Status s =
      db_->Put(write_options, column_family_->GetHandle(), rocksdb::Slice(kv.key()), rocksdb::Slice(kv.value()));
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Put failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal put error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::VectorAdd(uint64_t region_id, uint64_t log_id,
                                                const std::vector<pb::common::VectorWithId>& vectors) {
  if (BAIDU_UNLIKELY(vectors.empty())) {
    DINGO_LOG(ERROR) << fmt::format("vectors empty  not support");
    return butil::Status(pb::error::EVECTOR_EMPTY, "vectors is empty");
  }

  std::string key_header = std::to_string(region_id);

  // write vector, WAL, WAL_LOG_ID
  rocksdb::WriteBatch batch;
  for (const auto& vector : vectors) {
    if (BAIDU_UNLIKELY(vector.id() == 0 || vector.vector().values_size() == 0)) {
      DINGO_LOG(ERROR) << fmt::format("vector empty  not support");
      return butil::Status(pb::error::EVECTOR_EMPTY, "vector is empty");
    } else {
      rocksdb::Status s = batch.Put(column_family_->GetHandle(), key_header + std::to_string(vector.id()),
                                    vector.vector().SerializeAsString());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("rocksdb::WriteBatch::Put failed : {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal error");
      }

      s = batch.Put(column_family_->GetHandle(), key_header + std::string("WAL") + std::to_string(vector.id()),
                    vector.vector().SerializeAsString());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("rocksdb::WriteBatch::Put failed : {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal error");
      }

      s = batch.Put(column_family_->GetHandle(), key_header + std::string("WAL_LOG_ID"), std::to_string(log_id));
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("rocksdb::WriteBatch::Put failed : {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal error");
      }
    }
  }

  // write vector index
  std::shared_ptr<VectorIndex> vector_index;
  auto ret = Server::GetInstance()->GetRegionController()->vector_index_map.Get(region_id, vector_index);
  if (ret < 0) {
    DINGO_LOG(ERROR) << fmt::format("Get vector_index failed");
    return butil::Status(pb::error::EINTERNAL, "Internal error, Get vector_index failed");
  }

  vector_index->Add(vectors);

  // commit rocksdb
  rocksdb::WriteOptions write_options;
  rocksdb::Status s = db_->Write(write_options, &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Write failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::VectorDelete(uint64_t region_id, uint64_t log_id,
                                                   const std::vector<uint64_t>& ids) {
  if (BAIDU_UNLIKELY(ids.empty())) {
    DINGO_LOG(ERROR) << fmt::format("vectors empty  not support");
    return butil::Status(pb::error::EVECTOR_EMPTY, "vectors is empty");
  }

  std::string key_header = std::to_string(region_id);

  // write vector, WAL, WAL_LOG_ID
  rocksdb::WriteBatch batch;
  for (const auto& id : ids) {
    if (BAIDU_UNLIKELY(id == 0)) {
      DINGO_LOG(ERROR) << fmt::format("vector empty  not support");
      return butil::Status(pb::error::EVECTOR_EMPTY, "vector is empty");
    } else {
      rocksdb::Status s = batch.Delete(column_family_->GetHandle(), key_header + std::to_string(id));
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("rocksdb::WriteBatch::Put failed : {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal error");
      }

      s = batch.Put(column_family_->GetHandle(), key_header + std::string("WAL") + std::to_string(id), std::string());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("rocksdb::WriteBatch::Put failed : {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal error");
      }

      s = batch.Put(column_family_->GetHandle(), key_header + std::string("WAL_LOG_ID"), std::to_string(log_id));
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("rocksdb::WriteBatch::Put failed : {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal error");
      }
    }
  }

  // write vector index
  // write vector index
  std::shared_ptr<VectorIndex> vector_index;
  auto ret = Server::GetInstance()->GetRegionController()->vector_index_map.Get(region_id, vector_index);
  if (ret < 0) {
    DINGO_LOG(ERROR) << fmt::format("Get vector_index failed");
    return butil::Status(pb::error::EINTERNAL, "Internal error, Get vector_index failed");
  }

  for (auto id : ids) {
    vector_index->Delete(id);
  }

  // commit rocksdb
  rocksdb::WriteOptions write_options;
  rocksdb::Status s = db_->Write(write_options, &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Write failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::KvBatchPut(const std::vector<pb::common::KeyValue>& kvs) {
  return KvBatchPutAndDelete(kvs, {});
}

butil::Status RawRocksEngine::Writer::KvBatchPutAndDelete(const std::vector<pb::common::KeyValue>& kv_puts,
                                                          const std::vector<pb::common::KeyValue>& kv_deletes) {
  if (BAIDU_UNLIKELY(kv_puts.empty() && kv_deletes.empty())) {
    DINGO_LOG(ERROR) << fmt::format("keys empty not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::WriteBatch batch;
  for (const auto& kv : kv_puts) {
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      DINGO_LOG(ERROR) << fmt::format("key empty not support");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    } else {
      rocksdb::Status s = batch.Put(column_family_->GetHandle(), kv.key(), kv.value());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("rocksdb::WriteBatch::Put failed : {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal put error");
      }
    }
  }

  for (const auto& kv : kv_deletes) {
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      DINGO_LOG(ERROR) << fmt::format("key empty not support");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    } else {
      rocksdb::Status s = batch.Delete(column_family_->GetHandle(), kv.key());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("rocksdb::WriteBatch::Put failed : {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal delete error");
      }
    }
  }
  rocksdb::WriteOptions write_options;
  rocksdb::Status s = db_->Write(write_options, &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Write failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal write error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::KvPutIfAbsent(const pb::common::KeyValue& kv, bool& key_state) {
  pb::common::KeyValue internal_kv;
  internal_kv.set_key(kv.key());
  internal_kv.set_value("");

  const std::string& value = kv.value();

  // compare and replace. support does not exist
  return KvCompareAndSetInternal(internal_kv, value, false, key_state);
}

butil::Status RawRocksEngine::Writer::KvBatchPutIfAbsent(const std::vector<pb::common::KeyValue>& kvs,
                                                         std::vector<bool>& key_states, bool is_atomic) {
  if (BAIDU_UNLIKELY(kvs.empty())) {
    DINGO_LOG(ERROR) << fmt::format("empty keys not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  // Warning : be careful with vector<bool>
  key_states.clear();
  key_states.resize(kvs.size(), false);

  size_t key_index = 0;
  rocksdb::WriteBatch batch;
  for (const auto& kv : kvs) {
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      DINGO_LOG(ERROR) << fmt::format("empty key not support");
      key_states.clear();
      key_states.resize(kvs.size(), false);
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    std::string value_old;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), column_family_->GetHandle(),
                                 rocksdb::Slice(kv.key().data(), kv.key().size()), &value_old);
    if (is_atomic) {
      if (!s.IsNotFound()) {
        key_states.clear();
        key_states.resize(kvs.size(), false);
        DINGO_LOG(INFO) << fmt::format("rocksdb::DB::Get failed or found: {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal get error");
      }
    } else {
      if (!s.IsNotFound()) {
        key_index++;
        continue;
      }
    }

    // write a key in this batch
    s = batch.Put(column_family_->GetHandle(), rocksdb::Slice(kv.key().data(), kv.key().size()),
                  rocksdb::Slice(kv.value().data(), kv.value().size()));
    if (BAIDU_UNLIKELY(!s.ok())) {
      if (is_atomic) {
        key_states.clear();
        key_states.resize(kvs.size(), false);
      }
      DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Put failed : {}", s.ToString());
      return butil::Status(pb::error::EINTERNAL, "Internal put error");
    }
    key_states[key_index] = true;
    key_index++;
  }

  rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
  if (!s.ok()) {
    key_states.clear();
    key_states.resize(kvs.size(), false);
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Write failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal write error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::KvCompareAndSet(const pb::common::KeyValue& kv, const std::string& value,
                                                      bool& key_state) {
  return KvCompareAndSetInternal(kv, value, true, key_state);
}

butil::Status RawRocksEngine::Writer::KvDelete(const std::string& key) {
  if (BAIDU_UNLIKELY(key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("key empty  not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  rocksdb::WriteOptions const write_options;
  rocksdb::Status const s =
      db_->Delete(write_options, column_family_->GetHandle(), rocksdb::Slice(key.data(), key.size()));
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Delete failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal delete error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::KvBatchCompareAndSet(const std::vector<pb::common::KeyValue>& kvs,
                                                           const std::vector<std::string>& expect_values,
                                                           std::vector<bool>& key_states, bool is_atomic) {
  if (BAIDU_UNLIKELY(kvs.empty())) {
    DINGO_LOG(ERROR) << fmt::format("empty keys not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  if (BAIDU_UNLIKELY(kvs.size() != expect_values.size())) {
    DINGO_LOG(ERROR) << fmt::format("kvs {} != expect_values {} size", kvs.size(), expect_values.size());
    return butil::Status(pb::error::EKEY_EMPTY, "Key is mismatch");
  }

  // Warning : be careful with vector<bool>
  key_states.clear();
  key_states.resize(kvs.size(), false);

  size_t key_index = 0;
  rocksdb::WriteBatch batch;
  for (const auto& kv : kvs) {
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      DINGO_LOG(ERROR) << fmt::format("empty key not support");
      key_states.clear();
      key_states.resize(kvs.size(), false);
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    std::string value_old;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), column_family_->GetHandle(),
                                 rocksdb::Slice(kv.key().data(), kv.key().size()), &value_old);
    if (is_atomic) {
      if (s.ok()) {
        if (value_old != expect_values[key_index]) {
          key_states.clear();
          key_states.resize(kvs.size(), false);
          DINGO_LOG(DEBUG) << fmt::format("compare and set old_value: {} expect_value: {}", value_old,
                                          expect_values[key_index]);
          return butil::Status();
        }
      } else if (s.IsNotFound()) {
        if (!expect_values[key_index].empty()) {
          key_states.clear();
          key_states.resize(kvs.size(), false);
          DINGO_LOG(ERROR) << fmt::format("NotFound : expect_values[{}] not empty", key_index);
          return butil::Status(pb::error::EINTERNAL, "Internal not found error");
        }
      } else {
        key_states.clear();
        key_states.resize(kvs.size(), false);
        DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Get failed key_index :{} {}", key_index, s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal get error");
      }
    } else {
      if (s.ok()) {
        if (value_old != expect_values[key_index]) {
          key_index++;
          continue;
        }
      } else if (s.IsNotFound()) {
        if (!expect_values[key_index].empty()) {
          key_index++;
          continue;
        }
      } else {
        key_index++;
        continue;
      }
    }

    // value empty means delete
    if (kv.value().empty()) {
      // delete a key in this batch
      s = batch.Delete(column_family_->GetHandle(), rocksdb::Slice(kv.key().data(), kv.key().size()));
      if (BAIDU_UNLIKELY(!s.ok())) {
        if (is_atomic) {
          key_states.clear();
          key_states.resize(kvs.size(), false);
        }
        DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Delete failed key_index:{} : {}", key_index, s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal delete error");
      }
    } else {
      // write a key in this batch
      s = batch.Put(column_family_->GetHandle(), rocksdb::Slice(kv.key().data(), kv.key().size()),
                    rocksdb::Slice(kv.value().data(), kv.value().size()));
      if (BAIDU_UNLIKELY(!s.ok())) {
        if (is_atomic) {
          key_states.clear();
          key_states.resize(kvs.size(), false);
        }
        DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Put failed key_index :{} : {}", key_index, s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal put error");
      }
    }

    key_states[key_index] = true;
    key_index++;
  }

  rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
  if (!s.ok()) {
    key_states.clear();
    key_states.resize(kvs.size(), false);
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Write failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal write error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::KvBatchDelete(const std::vector<std::string>& keys) {
  std::vector<pb::common::KeyValue> kvs;
  for (const auto& key : keys) {
    pb::common::KeyValue kv;
    kv.set_key(key);

    kvs.emplace_back(std::move(kv));
  }

  return KvBatchPutAndDelete({}, kvs);
}

butil::Status RawRocksEngine::Writer::KvDeleteRange(const pb::common::Range& range) {
  if (range.start_key().empty() || range.end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
  }
  if (range.start_key() >= range.end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
  }

  auto status =
      db_->DeleteRange(rocksdb::WriteOptions(), column_family_->GetHandle(), range.start_key(), range.end_key());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Write failed : {}", status.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal delete range error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::KvBatchDeleteRange(const std::vector<pb::common::Range>& ranges) {
  for (const auto& range : ranges) {
    if (range.start_key().empty() || range.end_key().empty()) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
    }
    if (range.start_key() >= range.end_key()) {
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
    }
  }

  rocksdb::WriteBatch batch;
  for (const auto& range : ranges) {
    rocksdb::Status s = batch.DeleteRange(column_family_->GetHandle(), range.start_key(), range.start_key());
    if (!s.ok()) {
      DINGO_LOG(ERROR) << fmt::format("rocksdb::WriteBatch::DeleteRange failed : {}", s.ToString());
      return butil::Status(pb::error::EINTERNAL, "Internal delete range error");
    }
  }

  rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Write failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal write error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::KvDeleteIfEqual(const pb::common::KeyValue& kv) {
  if (BAIDU_UNLIKELY(kv.key().empty())) {
    DINGO_LOG(ERROR) << fmt::format("key empty  not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  // other read will failed
  std::string old_value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), column_family_->GetHandle(),
                               rocksdb::Slice(kv.key().data(), kv.key().size()), &old_value);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::GetForUpdate not found key");
      return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found");
    }
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::GetForUpdate failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal get error");
  }

  if (kv.value() != old_value) {
    DINGO_LOG(WARNING) << fmt::format("rocksdb::DB::Get value is not equal, {} | {}.", kv.value(), old_value);
    return butil::Status(pb::error::EINTERNAL, "Internal compare value error");
  }

  // delete a key
  s = db_->Delete(rocksdb::WriteOptions(), column_family_->GetHandle(),
                  rocksdb::Slice(kv.key().data(), kv.key().size()));
  if (BAIDU_UNLIKELY(!s.ok())) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Delete failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal delete error");
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Writer::KvCompareAndSetInternal(const pb::common::KeyValue& kv, const std::string& value,
                                                              bool is_key_exist, bool& key_state) {
  if (BAIDU_UNLIKELY(kv.key().empty())) {
    DINGO_LOG(ERROR) << fmt::format("key empty  not support");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  key_state = false;

  std::string old_value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), column_family_->GetHandle(),
                               rocksdb::Slice(kv.key().data(), kv.key().size()), &old_value);
  if (s.ok()) {
    if (!is_key_exist) {
      // The key already exists, the client requests not to return an error code and key_state set false
      key_state = false;
      return butil::Status();
    }
  } else if (s.IsNotFound()) {
    if (is_key_exist || (!is_key_exist && !kv.value().empty())) {
      DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Get not found key");
      return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found");
    }
  } else {  // error
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Get failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal get error");
  }

  if (kv.value() != old_value) {
    DINGO_LOG(DEBUG) << fmt::format("compare and set old_value: {} expect_value: {}", old_value, kv.value());
    return butil::Status();
  }

  // write a key
  s = db_->Put(rocksdb::WriteOptions(), column_family_->GetHandle(), rocksdb::Slice(kv.key().data(), kv.key().size()),
               rocksdb::Slice(value.data(), value.size()));
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("rocksdb::DB::Put failed : {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal put error");
  }

  key_state = true;

  return butil::Status();
}

butil::Status RawRocksEngine::SstFileWriter::SaveFile(const std::vector<pb::common::KeyValue>& kvs,
                                                      const std::string& filename) {
  auto status = sst_writer_->Open(filename);
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  for (const auto& kv : kvs) {
    status = sst_writer_->Put(kv.key(), kv.value());
    if (!status.ok()) {
      return butil::Status(status.code(), status.ToString());
    }
  }

  status = sst_writer_->Finish();
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  return butil::Status();
}

butil::Status RawRocksEngine::SstFileWriter::SaveFile(std::shared_ptr<dingodb::Iterator> iter,
                                                      const std::string& filename) {
  auto status = sst_writer_->Open(filename);
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  for (; iter->Valid(); iter->Next()) {
    status = sst_writer_->Put(iter->Key(), iter->Value());
    if (!status.ok()) {
      sst_writer_->Finish();
      return butil::Status(status.code(), status.ToString());
    }
  }

  status = sst_writer_->Finish();
  if (!status.ok()) {
    return butil::Status((status.code() == rocksdb::Status::Code::kInvalidArgument &&
                          status.ToString().find("no entries") != std::string::npos)
                             ? pb::error::ENO_ENTRIES
                             : static_cast<int>(status.code()),
                         status.ToString());
  }

  return butil::Status();
}

butil::Status RawRocksEngine::Checkpoint::Create(const std::string& dirpath) {
  // std::unique_ptr<rocksdb::Checkpoint> checkpoint = std::make_unique<rocksdb::Checkpoint>();
  rocksdb::Checkpoint* checkpoint = nullptr;
  auto status = rocksdb::Checkpoint::Create(db_.get(), &checkpoint);
  if (!status.ok()) {
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }

  status = checkpoint->CreateCheckpoint(dirpath);
  if (!status.ok()) {
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }

  delete checkpoint;
  return butil::Status();
}

butil::Status RawRocksEngine::Checkpoint::Create(const std::string& dirpath,
                                                 std::shared_ptr<ColumnFamily> column_family,
                                                 std::vector<pb::store_internal::SstFileInfo>& sst_files) {
  rocksdb::Checkpoint* checkpoint = nullptr;
  auto status = rocksdb::Checkpoint::Create(db_.get(), &checkpoint);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Checkpoint create failed " << status.ToString();
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }

  status = db_->DisableFileDeletions();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Disable file deletion failed " << status.ToString();
    return butil::Status(status.code(), status.ToString());
  }

  status = checkpoint->CreateCheckpoint(dirpath);
  if (!status.ok()) {
    db_->EnableFileDeletions(false);
    DINGO_LOG(ERROR) << "Export column family checkpoint failed " << status.ToString();
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }
  rocksdb::ColumnFamilyMetaData meta_data;
  db_->GetColumnFamilyMetaData(column_family->GetHandle(), &meta_data);

  status = db_->EnableFileDeletions(false);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "Enable file deletion failed " << status.ToString();
    return butil::Status(status.code(), status.ToString());
  }

  for (auto& level : meta_data.levels) {
    for (const auto& file : level.files) {
      pb::store_internal::SstFileInfo sst_file;
      sst_file.set_level(level.level);
      sst_file.set_name(file.name);
      sst_file.set_path(dirpath + file.name);
      sst_file.set_start_key(file.smallestkey);
      sst_file.set_end_key(file.largestkey);
      sst_files.emplace_back(std::move(sst_file));
    }
  }

  pb::store_internal::SstFileInfo sst_file;
  sst_file.set_level(-1);
  sst_file.set_name("CURRENT");
  sst_file.set_path(dirpath + "/CURRENT");
  sst_files.push_back(sst_file);

  std::string manifest_name = Helper::FindFileInDirectory(dirpath, "MANIFEST");
  sst_file.set_level(-1);
  sst_file.set_name(manifest_name);
  sst_file.set_path(dirpath + "/" + manifest_name);
  sst_files.push_back(sst_file);

  std::string options_name = Helper::FindFileInDirectory(dirpath, "OPTIONS");
  sst_file.set_level(-1);
  sst_file.set_name(options_name);
  sst_file.set_path(dirpath + "/" + options_name);
  sst_files.push_back(sst_file);

  delete checkpoint;
  return butil::Status();
}

}  // namespace dingodb

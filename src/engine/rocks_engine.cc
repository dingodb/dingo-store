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

#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "butil/strings/stringprintf.h"
#include "common/slice.h"
#include "config/config_manager.h"
#include "glog/logging.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

namespace dingodb {

RocksEngine::RocksEngine()
    : type_(pb::common::Engine::ENG_ROCKSDB),
      engine_name_("ROCKS_ENGINE"),
      conf_path_("./conf/store.yaml"),
      store_db_path_key_("store.dbPath"),
      store_db_path_string_("/opt/dingo/data/executor/db"),
      vt_family_handles_({}),
      rocksdb_db_(nullptr),
      already_call_init_(false),
      init_return_value_(false),
      index_cf_item_map_({}) {}

RocksEngine::~RocksEngine() { Close(); }

// load rocksdb config from config file
bool RocksEngine::Init(const std::string& conf_path) {
  if (already_call_init_) {
    DLOG(INFO) << butil::StringPrintf("RocksEngine::Init Only Call Once!");
    return init_return_value_;
  }

  init_return_value_ = false;

  conf_path_ = conf_path;

  bool is_exist = ConfigManager::GetInstance()->IsExist(conf_path_);
  if (!is_exist) {
    LOG(ERROR) << butil::StringPrintf("config %s not exist!",
                                      conf_path_.c_str());
    return init_return_value_;
  }

  std::shared_ptr<Config> rocksdb_config =  // NOLINT
      ConfigManager::GetInstance()->GetConfig(conf_path_);
  if (!rocksdb_config) {
    LOG(ERROR) << butil::StringPrintf("config %s load empty!",
                                      conf_path_.c_str());
    return init_return_value_;
  }

  std::string store_db_path = rocksdb_config->GetString(store_db_path_key_);
  if (store_db_path.empty()) {
    LOG(WARNING) << butil::StringPrintf("can not find : %s use default:%s",
                                        store_db_path_key_.c_str(),
                                        store_db_path_string_.c_str());
  } else {
    store_db_path_string_ = store_db_path;
  }

  DLOG(INFO) << butil::StringPrintf("rocksdb path : %s",
                                    store_db_path_string_.c_str());

  const auto* store_dcf_configuration_key("store.dcfConfiguration");

  // get deafult column family
  const std::map<std::string, std::string>& store_dcf_configuration_map =
      rocksdb_config->GetStringMap(store_dcf_configuration_key);

  const auto* store_icf_configuration_key("store.icfConfiguration");
  // get i column family
  const std::map<std::string, std::string>& store_icf_configuration_map =
      rocksdb_config->GetStringMap(store_icf_configuration_key);

  InitCfConfig();

  index_cf_item_map_["dcf"].SetConfMap(store_dcf_configuration_map);
  index_cf_item_map_["icf"].SetConfMap(store_icf_configuration_map);

  // open DB with two column families
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  rocksdb::ColumnFamilyOptions dcf_options;

  SetCfConfiguration(index_cf_item_map_["dcf"].GetDefaultConfMap(),
                     store_dcf_configuration_map, &dcf_options);
  // have to open default column family
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, dcf_options));

  rocksdb::ColumnFamilyOptions icf_options;

  SetCfConfiguration(index_cf_item_map_["icf"].GetDefaultConfMap(),
                     store_icf_configuration_map, &icf_options);
  // open the new one, too
  column_families.push_back(
      rocksdb::ColumnFamilyDescriptor("INSTRUCTION", icf_options));

  rocksdb::DBOptions db_option;
  db_option.create_if_missing = true;
  db_option.create_missing_column_families = true;

  rocksdb::Status s =
      rocksdb::DB::Open(db_option, store_db_path_string_, column_families,
                        &vt_family_handles_, &rocksdb_db_);
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("rocksdb::DB::Open faild : %s",
                                      s.ToString().c_str());
    return init_return_value_;
  }

  // we just hold the pointer temporarily
  index_cf_item_map_["dcf"].SetHandle(vt_family_handles_[0]);
  index_cf_item_map_["dcf"].SetHandle(vt_family_handles_[1]);

  DLOG(INFO) << butil::StringPrintf("rocksdb::DB::Open : %s success!",
                                    store_db_path_string_.c_str());

  init_return_value_ = true;

  return init_return_value_;
}

std::string RocksEngine::GetName() { return engine_name_; }

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
  if (nullptr == key.data() || key.empty()) {
    LOG(ERROR) << butil::StringPrintf("key empty or nullptr. not supprt");
    return {};
  }

  std::shared_ptr<std::string> value = std::make_shared<std::string>();

  auto s =
      rocksdb_db_->Get(index_cf_item_map_["dcf"].GetReadOptions(),
                       index_cf_item_map_["dcf"].GetHandle(),
                       rocksdb::Slice(key.data(), key.size()), value.get());
  return value;
}

pb::error::Errno RocksEngine::KvPut(
    [[maybe_unused]] std::shared_ptr<Context> ctx,
    const pb::common::KeyValue& kv) {
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

  // put and get from default column family
  rocksdb::Status s =
      rocksdb_db_->Put(index_cf_item_map_["dcf"].GetWriteOptions(),
                       index_cf_item_map_["dcf"].GetHandle(),
                       rocksdb::Slice(key.data(), key.size()),
                       rocksdb::Slice(value.data(), value.size()));
  if (!s.ok()) {
    LOG(ERROR) << butil::StringPrintf("RocksEngine::KvPut failed");
    return pb::error::Errno::EKEY_NOTFOUND;
  }

  return pb::error::Errno::OK;
}

void RocksEngine::Close() {
  type_ = pb::common::Engine::ENG_ROCKSDB;
  engine_name_ = "ROCKS_ENGINE";
  conf_path_ = "./conf/store.yaml";
  store_db_path_key_ = "store.dbPath";
  store_db_path_string_ = "/opt/dingo/data/executor/db";
  if (rocksdb_db_) {
    for (auto* handle : vt_family_handles_) {
      rocksdb_db_->DestroyColumnFamilyHandle(handle);
    }
    vt_family_handles_.clear();
    rocksdb_db_ = nullptr;
  }
  already_call_init_ = false;
  init_return_value_ = false;
  index_cf_item_map_.clear();
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
    } catch (...) {
      value = default_value;
    }
  }
}

template <typename T>
void SetCfConfigurationElementWrapper(
    const RocksEngine::CfDefaultConfMap& default_conf_map,
    const std::map<std::string, std::string>& cf_configuration_map,
    const char* name, T& value) {  // NOLINT
  if (auto iter = default_conf_map.find(name); iter != default_conf_map.end()) {
    if (iter->second.has_value()) {
      T default_value = static_cast<T>(std::get<int64_t>(iter->second.value()));

      SetCfConfigurationElement(cf_configuration_map, "tcBlockSize",
                                static_cast<T>(default_value), value);
    }
  }
}

bool RocksEngine::InitCfConfig() {
  index_cf_item_map_.clear();

  CfDefaultConfMap dcf_default_conf_map;
  dcf_default_conf_map.emplace(
      "tcBlockSize", std::make_optional(static_cast<int64_t>(131072)));

  dcf_default_conf_map.emplace(
      "tcBlockCacheSize", std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf_map.emplace(
      "cfArenaBlockSize", std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf_map.emplace("cfMinWriteBufferNumberToMerge",
                               std::make_optional(static_cast<int64_t>(4)));

  dcf_default_conf_map.emplace("cfMaxWriteBufferNumber",
                               std::make_optional(static_cast<int64_t>(2)));

  dcf_default_conf_map.emplace(
      "cfMaxCompactionBytes",
      std::make_optional(static_cast<int64_t>(134217728)));

  dcf_default_conf_map.emplace(
      "cfWriteBufferSize", std::make_optional(static_cast<int64_t>(67108864)));

  dcf_default_conf_map.emplace("cfFixedLengthPrefixExtractor",
                               std::make_optional(static_cast<int64_t>(8)));

  dcf_default_conf_map.emplace(
      "cfMaxBytesForLevelBase",
      std::make_optional(static_cast<int64_t>(134217728)));

  dcf_default_conf_map.emplace(
      "cfTargetFileSizeBase",
      std::make_optional(static_cast<int64_t>(67108864)));

  CfDefaultConfMap icf_default_conf_map = dcf_default_conf_map;

  icf_default_conf_map["cfMaxWriteBufferNumber"] =
      std::make_optional(static_cast<int64_t>(3));

  index_cf_item_map_.emplace(
      "dcf", CfItem("dcf", ROCKSDB_NAMESPACE::kDefaultColumnFamilyName,
                    dcf_default_conf_map, {}));

  index_cf_item_map_.emplace(
      "icf", CfItem("icf", "INSTRUCTION", icf_default_conf_map, {}));

  return true;
}

// set cf config
bool RocksEngine::SetCfConfiguration(
    const CfDefaultConfMap& default_conf_map,
    const std::map<std::string, std::string>& cf_configuration_map,
    rocksdb::ColumnFamilyOptions* family_options) {
  rocksdb::ColumnFamilyOptions& cf_options = *family_options;

  rocksdb::BlockBasedTableOptions table_options;

  // tcBlockSize
  SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                   "tcBlockSize", table_options.block_size);

  // tcBlockCacheSize
  {
    size_t value = 0;

    SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                     "tcBlockCacheSize", value);

    auto cache = rocksdb::NewLRUCache(value);  // LRUcache
    table_options.block_cache = cache;
  }

  // cfArenaBlockSize

  SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                   "cfArenaBlockSize",
                                   cf_options.arena_block_size);

  // cfMinWriteBufferNumberToMerge
  SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                   "cfMinWriteBufferNumberToMerge",
                                   cf_options.min_write_buffer_number_to_merge);

  // cfMaxWriteBufferNumber
  SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                   "cfMaxWriteBufferNumber",
                                   cf_options.max_write_buffer_number);

  // cfMaxCompactionBytes
  SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                   "cfMaxCompactionBytes",
                                   cf_options.max_compaction_bytes);

  // cfWriteBufferSize
  SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                   "cfWriteBufferSize",
                                   cf_options.write_buffer_size);

  // cfFixedLengthPrefixExtractor
  {
    size_t value = 0;
    SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                     "cfFixedLengthPrefixExtractor", value);

    cf_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(value));
  }

  // cfMaxBytesForLevelBase
  SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                   "cfMaxBytesForLevelBase",
                                   cf_options.max_bytes_for_level_base);

  // cfTargetFileSizeBase
  SetCfConfigurationElementWrapper(default_conf_map, cf_configuration_map,
                                   "cfTargetFileSizeBase",
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

RocksEngine::CfItem::CfItem()
    : CfItem("", "", {}, {}, {}, {}, nullptr){};  // NOLINT

RocksEngine::CfItem::CfItem(const std::string& cf_index,
                            const std::string& cf_desc_name,
                            const rocksdb::WriteOptions& wo,
                            const rocksdb::ReadOptions& ro,
                            const CfDefaultConfMap& default_conf_map,
                            const std::map<std::string, std::string>& conf_map,
                            rocksdb::ColumnFamilyHandle* handle)
    : cf_index_(cf_index),
      cf_desc_name_(cf_desc_name),
      wo_(wo),
      ro_(ro),
      default_conf_map_(default_conf_map),
      conf_map_(conf_map),
      handle_(handle) {}

RocksEngine::CfItem::CfItem(const std::string& cf_index,
                            const std::string& cf_desc_name,
                            const rocksdb::WriteOptions& wo,
                            const rocksdb::ReadOptions& ro,
                            const CfDefaultConfMap& default_conf_map,
                            const std::map<std::string, std::string>& conf_map)
    : CfItem(cf_index, cf_desc_name, wo, ro, default_conf_map, conf_map,
             nullptr) {}

RocksEngine::CfItem::CfItem(const std::string& cf_index,
                            const std::string& cf_desc_name,
                            const CfDefaultConfMap& default_conf_map,
                            const std::map<std::string, std::string>& conf_map)
    : CfItem(cf_index, cf_desc_name, {}, {}, default_conf_map, conf_map,
             nullptr) {}

RocksEngine::CfItem::~CfItem() {
  cf_index_ = "";
  cf_desc_name_ = "";
  default_conf_map_.clear();
  conf_map_.clear();
  handle_ = nullptr;
}

RocksEngine::CfItem::CfItem(const RocksEngine::CfItem& rhs) {
  cf_index_ = rhs.cf_index_;
  cf_desc_name_ = rhs.cf_desc_name_;
  wo_ = rhs.wo_;
  ro_ = rhs.ro_;
  default_conf_map_ = rhs.default_conf_map_;
  conf_map_ = rhs.conf_map_;
  handle_ = rhs.handle_;
}

RocksEngine::CfItem& RocksEngine::CfItem::operator=(
    const RocksEngine::CfItem& rhs) {
  if (this == &rhs) {
    return *this;
  }

  cf_index_ = rhs.cf_index_;
  cf_desc_name_ = rhs.cf_desc_name_;
  wo_ = rhs.wo_;
  ro_ = rhs.ro_;
  default_conf_map_ = rhs.default_conf_map_;
  conf_map_ = rhs.conf_map_;
  handle_ = rhs.handle_;

  return *this;
}

RocksEngine::CfItem::CfItem(RocksEngine::CfItem&& rhs) noexcept {
  cf_index_ = std::move(rhs.cf_index_);
  cf_desc_name_ = std::move(rhs.cf_desc_name_);
  wo_ = rhs.wo_;
  ro_ = std::move(rhs.ro_);
  default_conf_map_ = std::move(rhs.default_conf_map_);
  conf_map_ = std::move(rhs.conf_map_);
  handle_ = rhs.handle_;
}

RocksEngine::CfItem& RocksEngine::CfItem::operator=(
    RocksEngine::CfItem&& rhs) noexcept {
  if (this == &rhs) {
    return *this;
  }

  cf_index_ = std::move(rhs.cf_index_);
  cf_desc_name_ = std::move(rhs.cf_desc_name_);
  wo_ = rhs.wo_;
  ro_ = std::move(rhs.ro_);
  default_conf_map_ = std::move(rhs.default_conf_map_);
  conf_map_ = std::move(rhs.conf_map_);
  handle_ = rhs.handle_;

  return *this;
}

}  // namespace dingodb

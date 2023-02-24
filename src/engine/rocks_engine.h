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

#ifndef DINGODB_ENGINE_ROCKS_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_ROCKS_ENGINE_H_

#include <map>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "engine/engine.h"
#include "openssl/core_dispatch.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

namespace dingodb {

class RocksEngine : public Engine {
 public:
  RocksEngine();
  ~RocksEngine() override;

  RocksEngine(const RocksEngine& rhs) = delete;
  RocksEngine& operator=(const RocksEngine& rhs) = delete;
  RocksEngine(RocksEngine&& rhs) = delete;
  RocksEngine& operator=(RocksEngine&& rhs) = delete;

  bool Init(const std::string& conf_path) override;
  std::string GetName() override;
  uint32_t GetID() override;

  // not
  int AddRegion(uint64_t region_id, const pb::common::Region& region) override;

  int DestroyRegion(uint64_t region_id) override;

  Snapshot* GetSnapshot() override;
  void ReleaseSnapshot() override;

  std::shared_ptr<std::string> KvGet(std::shared_ptr<Context> ctx,
                                     const std::string& key) override;
  pb::error::Errno KvPut(std::shared_ptr<Context> ctx,
                         const pb::common::KeyValue& kv) override;

 private:
  void Close();

  bool InitCfConfig();

  pb::common::Engine type_;
  std::string engine_name_;

  std::string conf_path_;

  std::string store_db_path_key_;
  std::string store_db_path_string_;

  std::vector<rocksdb::ColumnFamilyHandle*> vt_family_handles_;
  rocksdb::DB* rocksdb_db_;

  // call init once
  bool already_call_init_ = false;
  bool init_return_value_ = false;

  using CfDefaultConfValueBase = std::variant<int64_t, double, std::string>;
  using CfDefaultConfValue = std::optional<CfDefaultConfValueBase>;
  using CfDefaultConfMap = std::map<std::string, CfDefaultConfValue>;
  CfDefaultConfMap cf_configuration_default_map_;

  // set cf config
  static bool SetCfConfiguration(
      const CfDefaultConfMap& default_conf_map,
      const std::map<std::string, std::string>& cf_configuration_map,
      rocksdb::ColumnFamilyOptions* family_options);

  class CfItem {
   public:
    CfItem();
    CfItem(const std::string& cf_index, const std::string& cf_desc_name,
           const rocksdb::WriteOptions& wo, const rocksdb::ReadOptions& ro,
           const CfDefaultConfMap& default_conf_map,
           const std::map<std::string, std::string>& conf_map,
           rocksdb::ColumnFamilyHandle* handle);
    CfItem(const std::string& cf_index, const std::string& cf_desc_name,
           const rocksdb::WriteOptions& wo, const rocksdb::ReadOptions& ro,
           const CfDefaultConfMap& default_conf_map,
           const std::map<std::string, std::string>& conf_map);

    CfItem(const std::string& cf_index, const std::string& cf_desc_name,
           const CfDefaultConfMap& default_conf_map,
           const std::map<std::string, std::string>& conf_map);

    ~CfItem();

    CfItem(const CfItem& rhs);
    CfItem& operator=(const CfItem& rhs);
    CfItem(CfItem&& rhs) noexcept;
    CfItem& operator=(CfItem&& rhs) noexcept;

    void SetCfIndex(const std::string& cf_index) { cf_index_ = cf_index; }
    const std::string& GetCfIndex() const { return cf_index_; }
    void SetCfDescName(const std::string& cf_desc_name) {
      cf_desc_name_ = cf_desc_name;
    }
    const std::string& GetCfDescName() const { return cf_desc_name_; }

    void SetWriteOptions(const rocksdb::WriteOptions& wo) { wo_ = wo; }
    const rocksdb::WriteOptions& GetWriteOptions() const { return wo_; }

    void SetReadOptions(const rocksdb::ReadOptions& ro) { ro_ = ro; }
    const rocksdb::ReadOptions& GetReadOptions() const { return ro_; }

    void SetDefaultConfMap(const CfDefaultConfMap& default_conf_map) {
      default_conf_map_ = default_conf_map;
    }
    const CfDefaultConfMap& GetDefaultConfMap() const {
      return default_conf_map_;
    }

    void SetConfMap(const std::map<std::string, std::string>& conf_map) {
      conf_map_ = conf_map;
    }
    const std::map<std::string, std::string>& GetConfMap() const {
      return conf_map_;
    }

    void SetHandle(rocksdb::ColumnFamilyHandle* handle) { handle_ = handle; }
    rocksdb::ColumnFamilyHandle* GetHandle() { return handle_; }

   protected:
    // NOLINT
   private:
    // such as "icf" "dcf"
    std::string cf_index_;
    // such add "default" ...
    std::string cf_desc_name_;
    rocksdb::WriteOptions wo_;
    rocksdb::ReadOptions ro_;
    CfDefaultConfMap default_conf_map_;
    std::map<std::string, std::string> conf_map_;
    // reference vt_family_handles_  do not release this handle
    rocksdb::ColumnFamilyHandle* handle_;
  };

  std::map<std::string, CfItem> index_cf_item_map_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ROCKS_ENGINE_H_  // NOLINT

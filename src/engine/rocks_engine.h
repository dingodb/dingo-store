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

#include "config/config.h"
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

  bool Init(const std::shared_ptr<Config>& config) override;
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

  // compare and replace. support does not exist
  pb::error::Errno KvBcompareAndSet(std::shared_ptr<Context> ctx,
                                    const pb::common::KeyValue& kv,
                                    const std::string& value) override;

  pb::error::Errno KvDelete(std::shared_ptr<Context> ctx,
                            const std::string& key) override;

  pb::error::Errno KvDeleteRange(std::shared_ptr<Context> ctx,
                                 const std::string& key_begin,
                                 const std::string& key_endbool,
                                 bool delete_files_in_range) override;

  pb::error::Errno KvWriteBatch(
      std::shared_ptr<Context> ctx,
      const std::vector<pb::common::KeyValue>& vt_put) override;

  // read range When the amount of data is relatively small.
  // CreateReader may be better
  // [key_begin, key_end)
  pb::error::Errno KvScan(
      std::shared_ptr<Context> ctx, const std::string& key_begin,
      const std::string& key_end,
      std::vector<pb::common::KeyValue>& vt_kv) override;  // NOLINT

  // [key_begin, key_end)
  int64_t KvCount(std::shared_ptr<Context> ctx, const std::string& key_begin,
                  const std::string& key_end) override;

  std::shared_ptr<EngineReader> CreateReader(
      std::shared_ptr<Context> ctx) override;

 private:
  void Close();

  bool InitCfConfig(const std::vector<std::string>& vt_column_family);

  pb::common::Engine type_;
  std::string name_;

  // destroy rocksdb need
  rocksdb::Options db_options_;
  rocksdb::TransactionDB* txn_db_;

  using CfDefaultConfValueBase = std::variant<int64_t, double, std::string>;
  using CfDefaultConfValue = std::optional<CfDefaultConfValueBase>;
  using CfDefaultConfMap = std::map<std::string, CfDefaultConfValue>;
  CfDefaultConfMap cf_configuration_default_map_;

  // set cf config
  static bool SetCfConfiguration(
      const CfDefaultConfMap& map_default_conf,
      const std::map<std::string, std::string>& cf_configuration_map,
      rocksdb::ColumnFamilyOptions* family_options);

  // set default column family if not exist. rockdb not allow no default column
  // family we will move default to first
  static void SetDefaultIfNotExist(
      std::vector<std::string>& vt_column_family);  // NOLINT

  // new_cf = base + cf . cf will overwite base value if exists.
  static void CreateNewMap(
      const std::map<std::string, std::string>& base,
      const std::map<std::string, std::string>& cf,
      std::map<std::string, std::string>& new_cf);  // NOLINT

  bool RocksdbInit(
      const std::string& db_path,
      const std::vector<std::string>& vt_column_family,
      std::vector<rocksdb::ColumnFamilyHandle*>& vt_family_handles);  // NOLINT

  void SetColumnFamilyHandle(
      const std::vector<std::string>& vt_column_family,
      const std::vector<rocksdb::ColumnFamilyHandle*>& vt_family_handles);

  void SetColumnFamilyFromConfig(
      const std::shared_ptr<Config>& config,
      const std::vector<std::string>& vt_column_family);

  // class ColumnFamily;
  class ColumnFamily {
   public:
    ColumnFamily();
    explicit ColumnFamily(const std::string& cf_index,
                          const std::string& cf_desc_name,
                          const rocksdb::WriteOptions& wo,
                          const rocksdb::ReadOptions& ro,
                          const CfDefaultConfMap& map_default_conf,
                          const std::map<std::string, std::string>& map_conf,
                          rocksdb::ColumnFamilyHandle* handle);
    explicit ColumnFamily(const std::string& cf_index,
                          const std::string& cf_desc_name,
                          const rocksdb::WriteOptions& wo,
                          const rocksdb::ReadOptions& ro,
                          const CfDefaultConfMap& map_default_conf,
                          const std::map<std::string, std::string>& map_conf);

    explicit ColumnFamily(const std::string& cf_index,
                          const std::string& cf_desc_name,
                          const CfDefaultConfMap& map_default_conf,
                          const std::map<std::string, std::string>& map_conf);

    ~ColumnFamily();

    ColumnFamily(const ColumnFamily& rhs);
    ColumnFamily& operator=(const ColumnFamily& rhs);
    ColumnFamily(ColumnFamily&& rhs) noexcept;
    ColumnFamily& operator=(ColumnFamily&& rhs) noexcept;

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

    void SetDefaultConfMap(const CfDefaultConfMap& map_default_conf) {
      map_default_conf_ = map_default_conf;
    }
    const CfDefaultConfMap& GetDefaultConfMap() const {
      return map_default_conf_;
    }

    void SetConfMap(const std::map<std::string, std::string>& map_conf) {
      map_conf_ = map_conf;
    }
    const std::map<std::string, std::string>& GetConfMap() const {
      return map_conf_;
    }

    void SetHandle(rocksdb::ColumnFamilyHandle* handle) { handle_ = handle; }
    rocksdb::ColumnFamilyHandle* GetHandle() const { return handle_; }

   protected:
    // NOLINT
   private:
    // such as "icf" "dcf"
    std::string cf_index_;
    // such add "default" ...
    std::string cf_desc_name_;
    rocksdb::WriteOptions wo_;
    rocksdb::ReadOptions ro_;
    CfDefaultConfMap map_default_conf_;
    std::map<std::string, std::string> map_conf_;
    // reference vt_family_handles_  do not release this handle
    rocksdb::ColumnFamilyHandle* handle_;
  };

  std::map<std::string, ColumnFamily> map_index_cf_item_;

  static const std::string& k_db_path;
  static const std::string& k_column_families;
  static const std::string& k_base_column_family;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ROCKS_ENGINE_H_  // NOLINT

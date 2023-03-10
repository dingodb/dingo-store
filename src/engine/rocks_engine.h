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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
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

  bool Init(std::shared_ptr<Config> config) override;
  std::string GetName() override;
  pb::common::Engine GetID() override;

  Snapshot* GetSnapshot() override;
  void ReleaseSnapshot() override;

  pb::error::Errno KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) override;
  pb::error::Errno KvBatchGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                              std::vector<pb::common::KeyValue>& kvs) override;

  pb::error::Errno KvPut(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) override;
  pb::error::Errno KvBatchPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs) override;

  pb::error::Errno KvPutIfAbsent(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) override;

  pb::error::Errno KvBatchPutIfAbsentAtomic(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                            std::vector<std::string>& put_keys) override;

  pb::error::Errno KvBatchPutIfAbsentNonAtomic(std::shared_ptr<Context> ctx,
                                               const std::vector<pb::common::KeyValue>& kvs,
                                               std::vector<std::string>& put_keys) override;

  // compare and replace. support does not exist
  pb::error::Errno KvCompareAndSet(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv,
                                   const std::string& value) override;

  pb::error::Errno KvDelete(std::shared_ptr<Context> ctx, const std::string& key) override;
  // [begin_key, end_key)
  pb::error::Errno KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) override;

  // read range When the amount of data is relatively small.
  // CreateReader may be better
  // [begin_key, end_key)
  pb::error::Errno KvScan(std::shared_ptr<Context> ctx, const std::string& begin_key, const std::string& end_key,
                          std::vector<pb::common::KeyValue>& kvs) override;  // NOLINT

  // [begin_key, end_key)
  pb::error::Errno KvCount(std::shared_ptr<Context> ctx, const std::string& begin_key, const std::string& end_key,
                           int64_t& count) override;

  std::shared_ptr<EngineReader> CreateReader(std::shared_ptr<Context> ctx) override;

  using CfDefaultConfValueBase = std::variant<int64_t, double, std::string>;
  using CfDefaultConfValue = std::optional<CfDefaultConfValueBase>;
  using CfDefaultConf = std::map<std::string, CfDefaultConfValue>;

 private:
  pb::error::Errno KvBatchPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                      std::vector<std::string>& put_keys, bool is_atomic);  // NOLINT

  void Close();

  bool InitCfConfig(const std::vector<std::string>& column_family);

  pb::common::Engine type_;
  std::string name_;

  // destroy rocksdb need
  rocksdb::Options db_options_;
  rocksdb::TransactionDB* txn_db_;

  // set cf config
  static bool SetCfConfiguration(const CfDefaultConf& default_conf,
                                 const std::map<std::string, std::string>& cf_configuration,
                                 rocksdb::ColumnFamilyOptions* family_options);

  // set default column family if not exist. rockdb not allow no default
  // column family we will move default to first
  static void SetDefaultIfNotExist(std::vector<std::string>& column_family);  // NOLINT

  // new_cf = base + cf . cf will overwite base value if exists.
  static void CreateNewMap(const std::map<std::string, std::string>& base, const std::map<std::string, std::string>& cf,
                           std::map<std::string, std::string>& new_cf);  // NOLINT

  bool RocksdbInit(const std::string& db_path, const std::vector<std::string>& column_family,
                   std::vector<rocksdb::ColumnFamilyHandle*>& family_handles);  // NOLINT

  void SetColumnFamilyHandle(const std::vector<std::string>& column_family,
                             const std::vector<rocksdb::ColumnFamilyHandle*>& family_handles);

  void SetColumnFamilyFromConfig(const std::shared_ptr<Config>& config, const std::vector<std::string>& column_family);

  // class ColumnFamily;
  class ColumnFamily {
   public:
    ColumnFamily();
    explicit ColumnFamily(const std::string& cf_index, const std::string& cf_desc_name, const rocksdb::WriteOptions& wo,
                          const rocksdb::ReadOptions& ro, const CfDefaultConf& default_conf,
                          const std::map<std::string, std::string>& conf, rocksdb::ColumnFamilyHandle* handle);
    explicit ColumnFamily(const std::string& cf_index, const std::string& cf_desc_name, const rocksdb::WriteOptions& wo,
                          const rocksdb::ReadOptions& ro, const CfDefaultConf& default_conf,
                          const std::map<std::string, std::string>& conf);

    explicit ColumnFamily(const std::string& cf_index, const std::string& cf_desc_name,
                          const CfDefaultConf& default_conf, const std::map<std::string, std::string>& conf);

    ~ColumnFamily();

    ColumnFamily(const ColumnFamily& rhs);
    ColumnFamily& operator=(const ColumnFamily& rhs);
    ColumnFamily(ColumnFamily&& rhs) noexcept;
    ColumnFamily& operator=(ColumnFamily&& rhs) noexcept;

    void SetCfIndex(const std::string& cf_index) { cf_index_ = cf_index; }
    const std::string& GetCfIndex() const { return cf_index_; }
    void SetCfDescName(const std::string& cf_desc_name) { cf_desc_name_ = cf_desc_name; }
    const std::string& GetCfDescName() const { return cf_desc_name_; }

    void SetWriteOptions(const rocksdb::WriteOptions& wo) { wo_ = wo; }
    const rocksdb::WriteOptions& GetWriteOptions() const { return wo_; }

    void SetReadOptions(const rocksdb::ReadOptions& ro) { ro_ = ro; }
    const rocksdb::ReadOptions& GetReadOptions() const { return ro_; }

    void SetDefaultConf(const CfDefaultConf& default_conf) { default_conf_ = default_conf; }
    const CfDefaultConf& GetDefaultConf() const { return default_conf_; }

    void SetConf(const std::map<std::string, std::string>& conf) { conf_ = conf; }
    const std::map<std::string, std::string>& GetConf() const { return conf_; }

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
    CfDefaultConf default_conf_;
    std::map<std::string, std::string> conf_;
    // reference family_handles_  do not release this handle
    rocksdb::ColumnFamilyHandle* handle_;
  };

  std::map<std::string, ColumnFamily> column_familys_;

  static const std::string& kDbPath;
  static const std::string& kColumnFamilies;
  static const std::string& kBaseColumnFamily;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ROCKS_ENGINE_H_  // NOLINT

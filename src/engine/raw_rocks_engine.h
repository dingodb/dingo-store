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

#ifndef DINGODB_ENGINE_ROCKS_KV_ENGINE_H_
#define DINGODB_ENGINE_ROCKS_KV_ENGINE_H_

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "config/config.h"
#include "engine/raw_engine.h"
#include "engine/snapshot.h"
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

class RawRocksEngine : public RawEngine {
 public:
  RawRocksEngine();
  ~RawRocksEngine() override;

  RawRocksEngine(const RawRocksEngine& rhs) = delete;
  RawRocksEngine& operator=(const RawRocksEngine& rhs) = delete;
  RawRocksEngine(RawRocksEngine&& rhs) = delete;
  RawRocksEngine& operator=(RawRocksEngine&& rhs) = delete;

  using CfDefaultConfValueBase = std::variant<int64_t, double, std::string>;
  using CfDefaultConfValue = std::optional<CfDefaultConfValueBase>;
  using CfDefaultConf = std::map<std::string, CfDefaultConfValue>;

  // class ColumnFamily;
  class ColumnFamily {
   public:
    ColumnFamily();
    explicit ColumnFamily(const std::string& cf_name, const CfDefaultConf& default_conf,
                          const std::map<std::string, std::string>& conf, rocksdb::ColumnFamilyHandle* handle);
    explicit ColumnFamily(const std::string& cf_name, const CfDefaultConf& default_conf,
                          const std::map<std::string, std::string>& conf);

    ~ColumnFamily();

    ColumnFamily(const ColumnFamily& rhs);
    ColumnFamily& operator=(const ColumnFamily& rhs);
    ColumnFamily(ColumnFamily&& rhs) noexcept;
    ColumnFamily& operator=(ColumnFamily&& rhs) noexcept;

    void SetName(const std::string& name) { name_ = name; }
    const std::string& Name() const { return name_; }

    void SetDefaultConf(const CfDefaultConf& default_conf) { default_conf_ = default_conf; }
    const CfDefaultConf& GetDefaultConf() const { return default_conf_; }

    void SetConf(const std::map<std::string, std::string>& conf) { conf_ = conf; }
    const std::map<std::string, std::string>& GetConf() const { return conf_; }

    void SetHandle(rocksdb::ColumnFamilyHandle* handle) { handle_ = handle; }
    rocksdb::ColumnFamilyHandle* GetHandle() const { return handle_; }

   protected:
    // NOLINT
   private:
    std::string name_;
    CfDefaultConf default_conf_;
    std::map<std::string, std::string> conf_;
    // reference family_handles_  do not release this handle
    rocksdb::ColumnFamilyHandle* handle_;
  };

  class RocksSnapshot : public dingodb::Snapshot {
   public:
    RocksSnapshot(const rocksdb::Snapshot* snapshot) : snapshot_(snapshot) {}
    ~RocksSnapshot() override{};

    const rocksdb::Snapshot* InnerSnapshot() { return snapshot_; }

   private:
    const rocksdb::Snapshot* snapshot_;
  };

  class Reader : public RawEngine::Reader {
   public:
    Reader(std::shared_ptr<rocksdb::TransactionDB> txn_db, std::shared_ptr<ColumnFamily> column_family)
        : txn_db_(txn_db), column_family_(column_family) {}
    ~Reader() override{};
    butil::Status KvGet(const std::string& key, std::string& value) override;
    butil::Status KvGet(std::shared_ptr<dingodb::Snapshot> snapshot, const std::string& key,
                        std::string& value) override;

    butil::Status KvScan(const std::string& start_key, const std::string& end_key,
                         std::vector<pb::common::KeyValue>& kvs) override;
    butil::Status KvScan(std::shared_ptr<dingodb::Snapshot> snapshot, const std::string& start_key,
                         const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) override;

    butil::Status KvCount(const std::string& start_key, const std::string& end_key, int64_t& count) override;
    butil::Status KvCount(std::shared_ptr<dingodb::Snapshot> snapshot, const std::string& start_key,
                          const std::string& end_key, int64_t& count) override;

   private:
    std::shared_ptr<rocksdb::TransactionDB> txn_db_;
    std::shared_ptr<ColumnFamily> column_family_;
  };

  class Writer : public RawEngine::Writer {
   public:
    Writer(std::shared_ptr<rocksdb::TransactionDB> txn_db, std::shared_ptr<ColumnFamily> column_family)
        : txn_db_(txn_db), column_family_(column_family) {}
    ~Writer() override{};
    butil::Status KvPut(const pb::common::KeyValue& kv) override;
    butil::Status KvBatchPut(const std::vector<pb::common::KeyValue>& kvs) override;
    butil::Status KvBatchPutAndDelete(const std::vector<pb::common::KeyValue>& kv_puts,
                                      const std::vector<pb::common::KeyValue>& kv_deletes) override;

    butil::Status KvPutIfAbsent(const pb::common::KeyValue& kv) override;

    butil::Status KvBatchPutIfAbsent(const std::vector<pb::common::KeyValue>& kvs, std::vector<std::string>& put_keys,
                                     bool is_atomic) override;
    // key must be exist
    butil::Status KvCompareAndSet(const pb::common::KeyValue& kv, const std::string& value) override;

    butil::Status KvDelete(const std::string& key) override;
    butil::Status KvDeleteBatch(const std::vector<std::string>& keys) override;

    butil::Status KvDeleteRange(const pb::common::Range& range) override;

   private:
    butil::Status KvCompareAndSetInternal(const pb::common::KeyValue& kv, const std::string& value, bool is_key_exist);
    std::shared_ptr<ColumnFamily> column_family_;
    std::shared_ptr<rocksdb::TransactionDB> txn_db_;
  };

  bool Init(std::shared_ptr<Config> config) override;
  std::string GetName() override;
  pb::common::RawEngine GetID() override;

  std::shared_ptr<Snapshot> GetSnapshot() override;
  void ReleaseSnapshot(std::shared_ptr<Snapshot>) override;

  void Flush(const std::string& cf_name) override;

  std::shared_ptr<RawEngine::Reader> NewReader(const std::string& cf_name) override;
  std::shared_ptr<RawEngine::Writer> NewWriter(const std::string& cf_name) override;

 private:
  void Close();

  std::shared_ptr<ColumnFamily> GetColumnFamily(const std::string& cf_name);

  bool InitCfConfig(const std::vector<std::string>& column_family);

  // destroy rocksdb need
  rocksdb::Options db_options_;
  std::shared_ptr<rocksdb::TransactionDB> txn_db_;

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

  std::map<std::string, std::shared_ptr<ColumnFamily> > column_familys_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ROCKS_KV_ENGINE_H_
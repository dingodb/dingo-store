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

#ifndef DINGODB_ENGINE_ROCKS_KV_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_ROCKS_KV_ENGINE_H_

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "config/config.h"
#include "engine/iterator.h"
#include "engine/raw_engine.h"
#include "engine/snapshot.h"
#include "openssl/core_dispatch.h"
#include "proto/store_internal.pb.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/checkpoint.h"
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
    explicit RocksSnapshot(const rocksdb::Snapshot* snapshot, std::shared_ptr<rocksdb::TransactionDB> txn_db)
        : snapshot_(snapshot), txn_db_(txn_db) {}
    ~RocksSnapshot() override {
      if (txn_db_) {
        txn_db_->ReleaseSnapshot(snapshot_);
      }
    };

    const void* Inner() override { return snapshot_; }

   private:
    const rocksdb::Snapshot* snapshot_;
    std::shared_ptr<rocksdb::TransactionDB> txn_db_;
  };

  class Iterator : public dingodb::Iterator {
   public:
    explicit Iterator(IteratorOptions options, std::shared_ptr<Snapshot> snapshot, rocksdb::Iterator* iter)
        : options_(options), snapshot_(snapshot), iter_(iter) {}
    ~Iterator() override = default;

    std::string GetName() override { return "RawRocks"; }
    IteratorType GetID() override { return IteratorType::kRawRocksEngine; }

    bool Valid() const override {
      if (!iter_->Valid()) {
        return false;
      }

      if (!options_.upper_bound.empty()) {
        auto upper_bound = rocksdb::Slice(options_.upper_bound);
        if (upper_bound.compare(iter_->key()) <= 0) {
          return false;
        }
      }

      return true;
    }

    void SeekToFirst() override { iter_->SeekToFirst(); }
    void SeekToLast() override { iter_->SeekToLast(); }

    void Seek(const std::string& target) override { return iter_->Seek(target); }

    void SeekForPrev(const std::string& target) override { return iter_->SeekForPrev(target); }

    void Next() override { iter_->Next(); }

    void Prev() override { iter_->Prev(); }

    std::string_view Key() const override { return std::string_view(iter_->key().data(), iter_->key().size()); }
    std::string_view Value() const override { return std::string_view(iter_->value().data(), iter_->value().size()); }

   private:
    IteratorOptions options_;
    std::shared_ptr<Snapshot> snapshot_;
    std::unique_ptr<rocksdb::Iterator> iter_;
  };

  class Reader : public RawEngine::Reader {
   public:
    Reader(std::shared_ptr<rocksdb::TransactionDB> txn_db, std::shared_ptr<ColumnFamily> column_family)
        : txn_db_(txn_db), column_family_(column_family) {}
    ~Reader() override = default;
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

    butil::Status KvCount(const pb::common::RangeWithOptions& range, uint64_t* count) override;
    butil::Status KvCount(std::shared_ptr<dingodb::Snapshot> snapshot, const pb::common::RangeWithOptions& range,
                          uint64_t* count) override;

    std::shared_ptr<EngineIterator> NewIterator(const std::string& start_key, const std::string& end_key,
                                                bool with_start, bool with_end) override;

   private:
    std::shared_ptr<EngineIterator> NewIterator(std::shared_ptr<dingodb::Snapshot> snapshot,
                                                const std::string& start_key, const std::string& end_key,
                                                bool with_start, bool with_end);
    std::shared_ptr<rocksdb::TransactionDB> txn_db_;
    std::shared_ptr<ColumnFamily> column_family_;
  };

  class Writer : public RawEngine::Writer {
   public:
    Writer(std::shared_ptr<rocksdb::TransactionDB> txn_db, std::shared_ptr<ColumnFamily> column_family)
        : txn_db_(txn_db), column_family_(column_family) {}
    ~Writer() override = default;
    butil::Status KvPut(const pb::common::KeyValue& kv) override;
    butil::Status KvBatchPut(const std::vector<pb::common::KeyValue>& kvs) override;
    butil::Status KvBatchPutAndDelete(const std::vector<pb::common::KeyValue>& kv_puts,
                                      const std::vector<pb::common::KeyValue>& kv_deletes) override;

    butil::Status KvPutIfAbsent(const pb::common::KeyValue& kv, bool& key_state) override;

    butil::Status KvBatchPutIfAbsent(const std::vector<pb::common::KeyValue>& kvs, std::vector<bool>& key_states,
                                     bool is_atomic) override;
    // key must be exist
    butil::Status KvCompareAndSet(const pb::common::KeyValue& kv, const std::string& value, bool& key_state) override;

    butil::Status KvDelete(const std::string& key) override;
    butil::Status KvBatchDelete(const std::vector<std::string>& keys) override;

    butil::Status KvDeleteRange(const pb::common::Range& range) override;
    butil::Status KvDeleteRange(const pb::common::RangeWithOptions& range) override;
    butil::Status KvBatchDeleteRange(const std::vector<pb::common::RangeWithOptions>& ranges) override;

    // key must be exist
    butil::Status KvDeleteIfEqual(const pb::common::KeyValue& kv) override;

   private:
    butil::Status KvCompareAndSetInternal(const pb::common::KeyValue& kv, const std::string& value, bool is_key_exist,
                                          bool& key_state);

    std::shared_ptr<EngineIterator> NewIterator(const rocksdb::Snapshot* snapshot, const std::string& start_key,
                                                const std::string& end_key, bool with_start, bool with_end);

    butil::Status KvBatchDeleteRangeCore(const std::vector<std::pair<std::string, std::string>>& key_pairs);

    static butil::Status KvDeleteRangeParamCheck(const pb::common::RangeWithOptions& range, std::string* real_start_key,
                                                 std::string* real_end_key);
    // // original_key + 1. note overflow
    // static bool Increment(const std::string& original_key, std::string* key);

    std::shared_ptr<ColumnFamily> column_family_;
    std::shared_ptr<rocksdb::TransactionDB> txn_db_;
  };

  class SstFileWriter {
   public:
    SstFileWriter(const rocksdb::Options& options)
        : options_(options),
          sst_writer_(std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), options_, nullptr, true)) {}
    ~SstFileWriter() = default;

    SstFileWriter(SstFileWriter&& rhs) = delete;
    SstFileWriter& operator=(SstFileWriter&& rhs) = delete;

    butil::Status SaveFile(const std::vector<pb::common::KeyValue>& kvs, const std::string& filename);
    butil::Status SaveFile(std::shared_ptr<Iterator> iter, const std::string& filename);

    uint64_t GetSize() { return sst_writer_->FileSize(); }

   private:
    rocksdb::Options options_;
    std::unique_ptr<rocksdb::SstFileWriter> sst_writer_;
  };

  class Checkpoint {
   public:
    explicit Checkpoint(std::shared_ptr<rocksdb::TransactionDB> txn_db) : txn_db_(txn_db) {}
    ~Checkpoint() = default;

    Checkpoint(Checkpoint&& rhs) = delete;
    Checkpoint& operator=(Checkpoint&& rhs) = delete;

    butil::Status Create(const std::string& dirpath);
    butil::Status Create(const std::string& dirpath, std::shared_ptr<ColumnFamily> column_family,
                         std::vector<pb::store_internal::SstFileInfo>& sst_files);

   private:
    std::shared_ptr<rocksdb::TransactionDB> txn_db_;
  };

  friend class Checkpoint;
  std::string GetName() override;
  pb::common::RawEngine GetID() override;

  bool Init(std::shared_ptr<Config> config) override;

  std::string DbPath() { return db_path_; }

  std::shared_ptr<Snapshot> GetSnapshot() override;

  butil::Status IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files);

  void Flush(const std::string& cf_name) override;
  void Close();
  void Destroy();

  std::shared_ptr<dingodb::Snapshot> NewSnapshot() override;
  std::shared_ptr<RawEngine::Reader> NewReader(const std::string& cf_name) override;
  std::shared_ptr<RawEngine::Writer> NewWriter(const std::string& cf_name) override;
  std::shared_ptr<Iterator> NewIterator(const std::string& cf_name, IteratorOptions options);
  std::shared_ptr<Iterator> NewIterator(const std::string& cf_name, std::shared_ptr<Snapshot> snapshot,
                                        IteratorOptions options);
  std::shared_ptr<SstFileWriter> NewSstFileWriter();
  std::shared_ptr<Checkpoint> NewCheckpoint();

  std::shared_ptr<ColumnFamily> GetColumnFamily(const std::string& cf_name);

 private:
  bool InitCfConfig(const std::vector<std::string>& column_family);

  // set cf config
  static bool SetCfConfiguration(const CfDefaultConf& default_conf,
                                 const std::map<std::string, std::string>& cf_configuration,
                                 rocksdb::ColumnFamilyOptions* family_options);

  // set default column family if not exist. rocksdb not allow no default
  // column family we will move default to first
  static void SetDefaultIfNotExist(std::vector<std::string>& column_family);  // NOLINT

  // new_cf = base + cf . cf will overwrite base value if exists.
  static void CreateNewMap(const std::map<std::string, std::string>& base, const std::map<std::string, std::string>& cf,
                           std::map<std::string, std::string>& new_cf);  // NOLINT

  bool RocksdbInit(const std::string& db_path, const std::vector<std::string>& column_family,
                   std::vector<rocksdb::ColumnFamilyHandle*>& family_handles);  // NOLINT

  void SetColumnFamilyHandle(const std::vector<std::string>& column_family,
                             const std::vector<rocksdb::ColumnFamilyHandle*>& family_handles);

  void SetColumnFamilyFromConfig(const std::shared_ptr<Config>& config, const std::vector<std::string>& column_family);

  // destroy rocksdb need
  std::string db_path_;
  rocksdb::Options db_options_;
  std::shared_ptr<rocksdb::TransactionDB> txn_db_;
  std::map<std::string, std::shared_ptr<ColumnFamily>> column_families_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ROCKS_KV_ENGINE_H_  // NOLINT

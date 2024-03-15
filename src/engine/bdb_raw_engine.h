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

#ifndef DINGODB_ENGINE_RAW_BDB_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_RAW_BDB_ENGINE_H_

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/synchronization.h"
#include "config/config.h"
#include "db_cxx.h"
#include "engine/iterator.h"
#include "engine/raw_engine.h"

namespace dingodb {

class BdbRawEngine;

namespace bdb {

// CF_ID_LEN is the length of cf id in bdb key.
// the cf_id is the first byte of bdb key, now it is a char.
#define CF_ID_LEN 1

class BdbHelper {
 public:
  static std::string EncodeKey(const std::string& cf_name, const std::string& key);
  static std::string EncodeKey(char cf_id, const std::string& key);
  static std::string EncodeCf(const std::string& cf_name);
  static std::string EncodeCf(char cf_id);

  static int DecodeKey(const std::string& cf_name, const Dbt& bdb_key, std::string& key);

  static void DbtToString(const Dbt& dbt, std::string& str);
  static void DbtToUserKey(const Dbt& dbt, std::string& user_data);
  static std::string DbtToString(const Dbt& dbt);

  static uint32_t GetKeysSize(const std::vector<std::string>& keys);

  // static std::string DbtToBinary(const Dbt& dbt);
  static void StringToDbt(const std::string& str, Dbt& dbt);

  static int CompareDbt(const Dbt& dbt1, const Dbt& dbt2);

  static std::string EncodedCfUpperBound(const std::string& cf_name);
  static std::string EncodedCfUpperBound(char cf_id);

  static const int kCommitException = -60000;

  // we use a char to represent cf_id, so we can support 256 cf at most
  // in dingo-store v0.8.0, we add a prefix to the user_key to represent the column family in bdb.
  static char GetCfId(const std::string& cf_name);
  static std::string GetCfName(char cf_id);

  static std::unordered_map<std::string, char> cf_name_to_id;
  static std::unordered_map<char, std::string> cf_id_to_name;

  static int TxnCommit(DbTxn** txn_ptr);
  static int TxnAbort(DbTxn** txn_ptr);

  static void CheckpointThread(DbEnv* env, Db* db, std::atomic<bool>& is_close);
  static void GetLogFileNames(DbEnv* env, std::set<std::string>& file_names);
  static void PrintEnvStat(DbEnv* env);
};

// Snapshot
class BdbSnapshot : public dingodb::Snapshot {
 public:
  explicit BdbSnapshot(Db* db, DbTxn* txn, Dbc* cursorp, ResourcePool<Db*>* db_pool);
  ~BdbSnapshot() override;
  const void* Inner() override { return nullptr; }

  DbTxn* GetDbTxn() { return txn_; };
  Db* GetDb() { return db_; };
  DbEnv* GetEnv() { return db_->get_env(); };

 private:
  Db* db_;

  // Note: use DbEnv::txn_begin() to get pointers to a DbTxn,
  // and call DbTxn::abort() or DbTxn::commit rather than
  // delete to release them.
  DbTxn* txn_;
  Dbc* cursorp_;
  ResourcePool<Db*>* db_pool_;
};

class Iterator : public dingodb::Iterator {
 public:
  explicit Iterator(const std::string& cf_name, const IteratorOptions& options, Dbc* cursorp,
                    std::shared_ptr<bdb::BdbSnapshot> bdb_snapshot);

  ~Iterator() override;

  std::string GetName() override { return "RawBdb"; }
  IteratorType GetID() override { return IteratorType::kBdbEngine; }

  bool Valid() const override;

  void SeekToFirst() override;
  void SeekToLast() override;

  void Seek(const std::string& target) override;
  void SeekForPrev(const std::string& target) override;

  void Next() override;
  void Prev() override;

  DbEnv* GetEnv() { return snapshot_->GetEnv(); }

  std::string_view Key() const override {
    // size check is in Valid() function
    return std::string_view((const char*)bdb_key_.get_data() + CF_ID_LEN, bdb_key_.get_size() - CF_ID_LEN);
  }

  std::string_view Value() const override {
    return std::string_view((const char*)bdb_value_.get_data(), bdb_value_.get_size());
  }

  butil::Status Status() const override { return status_; };

 private:
  IteratorOptions options_;

  // raw_start_key and raw_end_key is init in contructor
  // because Dbt does not have memory management, so we need to use std::string to store the raw_start_key and
  // raw_end_key
  std::string raw_start_key_;
  std::string raw_end_key_;

  char cf_id_;
  std::string cf_upper_bound_;

  Dbc* cursorp_;
  Dbt bdb_key_;
  Dbt bdb_value_;
  bool valid_;

  butil::Status status_;
  std::shared_ptr<bdb::BdbSnapshot> snapshot_;
};

using IteratorPtr = std::shared_ptr<Iterator>;

class Reader : public RawEngine::Reader {
 public:
  Reader(std::shared_ptr<BdbRawEngine> raw_engine) : raw_engine_(raw_engine) {}
  ~Reader() override = default;

  butil::Status KvGet(const std::string& cf_name, const std::string& key, std::string& value) override;
  butil::Status KvGet(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& key,
                      std::string& value) override;

  butil::Status KvScan(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                       std::vector<pb::common::KeyValue>& kvs) override;
  butil::Status KvScan(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& start_key,
                       const std::string& end_key, std::vector<pb::common::KeyValue>& kvs) override;

  butil::Status KvCount(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                        int64_t& count) override;
  butil::Status KvCount(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& start_key,
                        const std::string& end_key, int64_t& count) override;

  dingodb::IteratorPtr NewIterator(const std::string& cf_name, IteratorOptions options) override;
  dingodb::IteratorPtr NewIterator(const std::string& cf_name, dingodb::SnapshotPtr snapshot,
                                   IteratorOptions options) override;

  butil::Status GetRangeKeys(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                             std::vector<std::string>& keys);

  // Calculate the size of the key and value in the range within the limit count records
  butil::Status GetRangeKeyValueSize(const std::string& cf_name, const std::string& start_key,
                                     const std::string& end_key, int64_t limit, int64_t& key_size, int64_t& value_size);

 private:
  std::shared_ptr<BdbRawEngine> GetRawEngine();
  Db* GetDb();
  void PutDb(Db* db);
  dingodb::SnapshotPtr GetSnapshot();
  butil::Status RetrieveByCursor(const std::string& cf_name, DbTxn* txn, const std::string& key, std::string& value);

  std::weak_ptr<BdbRawEngine> raw_engine_;
};

class Writer : public RawEngine::Writer {
 public:
  Writer(std::shared_ptr<BdbRawEngine> raw_engine) : raw_engine_(raw_engine) {}
  ~Writer() override = default;

  butil::Status KvPut(const std::string& cf_name, const pb::common::KeyValue& kv) override;
  butil::Status KvDelete(const std::string& cf_name, const std::string& key) override;

  butil::Status KvBatchPutAndDelete(const std::string& cf_name, const std::vector<pb::common::KeyValue>& kvs_to_put,
                                    const std::vector<std::string>& keys_to_delete) override;
  butil::Status KvBatchPutAndDelete(const std::map<std::string, std::vector<pb::common::KeyValue>>& kv_puts_with_cf,
                                    const std::map<std::string, std::vector<std::string>>& kv_deletes_with_cf) override;

  butil::Status KvDeleteRange(const std::string& cf_name, const pb::common::Range& range) override;
  butil::Status KvBatchDeleteRange(
      const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs) override;
  butil::Status KvBatchDeleteRangeNormal(const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs);
  butil::Status KvBatchDeleteRangeBulk(const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs);

 private:
  butil::Status KvBatchDelete(const std::string& cf_name, const std::vector<std::string>& keys);
  butil::Status DeleteRangeByCursor(const std::string& cf_name, const pb::common::Range& range, DbTxn* txn);

  std::shared_ptr<BdbRawEngine> GetRawEngine();
  Db* GetDb();
  void PutDb(Db* db);

  std::weak_ptr<BdbRawEngine> raw_engine_;
};

}  // namespace bdb

class BdbRawEngine : public RawEngine {
 public:
  BdbRawEngine() = default;
  ~BdbRawEngine() override = default;

  BdbRawEngine(const BdbRawEngine& rhs) = delete;
  BdbRawEngine& operator=(const BdbRawEngine& rhs) = delete;
  BdbRawEngine(BdbRawEngine&& rhs) = delete;
  BdbRawEngine& operator=(BdbRawEngine&& rhs) = delete;

  // Open a DB database
  static int32_t OpenDb(Db** dbpp, const char* file_name, DbEnv* envp, u_int32_t extra_flags);
  Db* GetDb();
  void PutDb(Db* db);
  DbEnv* GetEnv() { return envp_; }
  std::shared_ptr<BdbRawEngine> GetSelfPtr() { return std::dynamic_pointer_cast<BdbRawEngine>(shared_from_this()); }

  // override functions
  bool Init(std::shared_ptr<Config> config, const std::vector<std::string>& cf_names) override;
  void Close() override;
  void Destroy() override{};
  // bool Recover() override;

  std::string GetName() override;
  pb::common::RawEngine GetRawEngineType() override;
  dingodb::SnapshotPtr GetSnapshot() override;

  RawEngine::ReaderPtr Reader() override { return reader_; }
  RawEngine::WriterPtr Writer() override { return writer_; }
  RawEngine::CheckpointPtr NewCheckpoint() override;

  butil::Status MergeCheckpointFiles(const std::string& path, const pb::common::Range& range,
                                     const std::vector<std::string>& cf_names,
                                     std::vector<std::string>& merge_sst_paths) override;
  butil::Status IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files) override;

  void Flush(const std::string& cf_name) override;
  butil::Status Compact(const std::string& cf_name) override;
  std::vector<int64_t> GetApproximateSizes(const std::string& cf_name, std::vector<pb::common::Range>& ranges) override;

 private:
  DbEnv* envp_{nullptr};
  std::string db_path_;
  Db* db_{nullptr};

  ResourcePool<Db*> db_pool_{"bdb_db_handles"};
  std::vector<Db*> db_handles_;

  RawEngine::ReaderPtr reader_;
  RawEngine::WriterPtr writer_;

  std::atomic<bool> is_close_{false};
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_RAW_BDB_ENGINE_H_  // NOLINT

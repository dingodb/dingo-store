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

#include "config/config.h"
#include "db_cxx.h"
#include "engine/iterator.h"
#include "engine/raw_engine.h"
#include "serial/buf.h"

namespace dingodb {

class RawBdbEngine;

namespace bdb {

class BdbHelper {
 public:
  static std::string EncodeKey(const std::string& cf_name, const std::string& key);
  static std::string EncodeCfName(const std::string& cf_name);

  static int DecodeKey(const std::string& cf_name, const Dbt& bdb_key, std::string& key);

  static void DbtToBinary(const Dbt& dbt, std::string& binary);

  // static std::string DbtToBinary(const Dbt& dbt);
  static void BinaryToDbt(const std::string& binary, Dbt& dbt);

  static int DbtPairToKv(const std::string& cf_name, const Dbt& bdb_key, const Dbt& value, pb::common::KeyValue& kv);

  static int DbtCompare(const Dbt& dbt1, const Dbt& dbt2);

  static bool IsBdbKeyPrefixWith(const Dbt& bdb_key, const std::string& binary);

  static std::string GetEncodedCfNameUpperBound(const std::string& cf_name);

  inline static int kCommitException = -60000;
};

class Iterator : public dingodb::Iterator {
 public:
  explicit Iterator(const std::string& cf_name, /*bool snapshot_mode,*/ IteratorOptions options, Dbc* cursorp);

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

  std::string_view Key() const override {
    // size check is in Valid() function
    return std::string_view((const char*)bdb_key_.get_data() + encoded_cf_name_.size(),
                            bdb_key_.get_size() - encoded_cf_name_.size());
  }
  std::string_view Value() const override {
    return std::string_view((const char*)bdb_value_.get_data(), bdb_value_.get_size());
  }

  butil::Status Status() const override { return status_; };

 private:
  IteratorOptions options_;
  // const std::string& cf_name_;
  std::string encoded_cf_name_;
  std::string encoded_cf_name_upper_bound_;

  Dbc* cursorp_;
  Dbt bdb_key_;
  Dbt bdb_value_;
  bool valid_;
  // bool snapshot_mode_;
  butil::Status status_;
};
using IteratorPtr = std::shared_ptr<Iterator>;

// Snapshot
class Snapshot : public dingodb::Snapshot {
 public:
  explicit Snapshot(std::shared_ptr<Db> db, DbTxn* txn) : db_(db), txn_(txn) {}
  ~Snapshot() override;
  const void* Inner() override { return nullptr; }

  DbTxn* GetDbTxn() { return txn_; };

 private:
  std::shared_ptr<Db> db_;

  // Note: use DbEnv::txn_begin() to get pointers to a DbTxn,
  // and call DbTxn::abort() or DbTxn::commit rather than
  // delete to release them.
  DbTxn* txn_;
};

class Reader : public RawEngine::Reader {
 public:
  Reader(std::shared_ptr<RawBdbEngine> raw_engine) : raw_engine_(raw_engine) {}
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

  butil::Status GetRangeCountByCursor(const std::string& cf_name, DbTxn* txn, const std::string& start_key,
                                      const std::string& end_key, const int32_t isolation_flag, int64_t& count);

 private:
  std::shared_ptr<RawBdbEngine> GetRawEngine();
  std::shared_ptr<Db> GetDb();
  dingodb::SnapshotPtr GetSnapshot();
  butil::Status RetrieveByCursor(const std::string& cf_name, DbTxn* txn, const std::string& key, std::string& value);

  std::weak_ptr<RawBdbEngine> raw_engine_;
};

class Writer : public RawEngine::Writer {
 public:
  Writer(std::shared_ptr<RawBdbEngine> raw_engine) : raw_engine_(raw_engine) {}
  ~Writer() override = default;

  butil::Status KvPut(const std::string& cf_name, const pb::common::KeyValue& kv) override;
  butil::Status KvBatchPut(const std::string& cf_name, const std::vector<pb::common::KeyValue>& kvs) override;
  butil::Status KvBatchPutAndDelete(const std::string& cf_name, const std::vector<pb::common::KeyValue>& kv_puts,
                                    const std::vector<pb::common::KeyValue>& kv_deletes) override;
  butil::Status KvBatchPutAndDelete(const std::map<std::string, std::vector<pb::common::KeyValue>>& kv_puts_with_cf,
                                    const std::map<std::string, std::vector<std::string>>& kv_deletes_with_cf) override;

  butil::Status KvPutIfAbsent(const std::string& cf_name, const pb::common::KeyValue& kv, bool& key_state) override;
  butil::Status KvBatchPutIfAbsent(const std::string& cf_name, const std::vector<pb::common::KeyValue>& kvs,
                                   std::vector<bool>& key_states, bool is_atomic) override;

  butil::Status KvCompareAndSet(const std::string& cf_name, const pb::common::KeyValue& kv, const std::string& value,
                                bool& key_state) override;
  butil::Status KvBatchCompareAndSet(const std::string& cf_name, const std::vector<pb::common::KeyValue>& kvs,
                                     const std::vector<std::string>& expect_values, std::vector<bool>& key_states,
                                     bool is_atomic) override;

  butil::Status KvDelete(const std::string& cf_name, const std::string& key) override;
  butil::Status KvBatchDelete(const std::string& cf_name, const std::vector<std::string>& keys) override;

  butil::Status KvDeleteRange(const std::string& cf_name, const pb::common::Range& range) override;
  butil::Status KvDeleteRange(const std::vector<std::string>& cf_names, const pb::common::Range& range) override;
  butil::Status KvBatchDeleteRange(const std::string& cf_name, const std::vector<pb::common::Range>& ranges) override;
  butil::Status KvBatchDeleteRange(
      const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs) override;

  butil::Status KvDeleteIfEqual(const std::string& cf_name, const pb::common::KeyValue& kv) override;

 private:
  butil::Status KvCompareAndSet(const std::string& cf_name, const pb::common::KeyValue& kv, const std::string& value,
                                bool is_key_exist, bool& key_state);

  butil::Status KvBatchDeleteRange(const std::vector<std::string>& cf_names,
                                   const std::vector<pb::common::Range>& ranges);

  butil::Status DeleteRangeByCursor(const std::string& cf_name, const pb::common::Range& range, DbTxn* txn);

  std::shared_ptr<RawBdbEngine> GetRawEngine();
  std::shared_ptr<Db> GetDb();

  std::weak_ptr<RawBdbEngine> raw_engine_;
  std::shared_ptr<Db> db_;
};
}  // namespace bdb

class RawBdbEngine : public RawEngine {
 public:
  RawBdbEngine() = default;
  ~RawBdbEngine() override = default;

  RawBdbEngine(const RawBdbEngine& rhs) = delete;
  RawBdbEngine& operator=(const RawBdbEngine& rhs) = delete;
  RawBdbEngine(RawBdbEngine&& rhs) = delete;
  RawBdbEngine& operator=(RawBdbEngine&& rhs) = delete;

  // Open a DB database
  int32_t OpenDb(Db** dbpp, const char* file_name, DbEnv* envp, u_int32_t extra_flags);
  std::shared_ptr<Db> GetDb() { return db_; }
  std::shared_ptr<RawBdbEngine> GetSelfPtr() { return std::dynamic_pointer_cast<RawBdbEngine>(shared_from_this()); }

  // override functions
  bool Init(std::shared_ptr<Config> config, const std::vector<std::string>& cf_names) override;
  void Close() override;
  void Destroy() override{};
  // bool Recover() override;

  std::string GetName() override;
  pb::common::RawEngine GetID() override;
  dingodb::SnapshotPtr GetSnapshot() override;

  RawEngine::ReaderPtr Reader() override { return reader_; }
  RawEngine::WriterPtr Writer() override { return writer_; }

  butil::Status IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files) override;
  void Flush(const std::string& cf_name) override;
  butil::Status Compact(const std::string& cf_name) override;
  std::vector<int64_t> GetApproximateSizes(const std::string& cf_name, std::vector<pb::common::Range>& ranges) override;

 private:
  std::string db_path_;
  std::shared_ptr<Db> db_;

  RawEngine::ReaderPtr reader_;
  RawEngine::WriterPtr writer_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_RAW_BDB_ENGINE_H_  // NOLINT

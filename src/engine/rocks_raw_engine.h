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

#ifndef DINGODB_ENGINE_ROCKS_RAW_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_ROCKS_RAW_ENGINE_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "config/config.h"
#include "engine/iterator.h"
#include "engine/raw_engine.h"
#include "engine/snapshot.h"
#include "proto/common.pb.h"
#include "proto/store_internal.pb.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/checkpoint.h"

namespace dingodb {

class RocksRawEngine;

namespace rocks {

class ColumnFamily {
 public:
  using ColumnFamilyConfig = std::map<std::string, std::string>;

  explicit ColumnFamily(const std::string& cf_name, const ColumnFamilyConfig& config,
                        rocksdb::ColumnFamilyHandle* handle);
  explicit ColumnFamily(const std::string& cf_name, const ColumnFamilyConfig& config);

  ~ColumnFamily();

  static std::shared_ptr<ColumnFamily> New(const std::string& cf_name, const ColumnFamilyConfig& config) {
    return std::make_shared<ColumnFamily>(cf_name, config);
  }

  void SetName(const std::string& name) { name_ = name; }
  const std::string& Name() const { return name_; }

  void SetConfItem(const std::string& name, const std::string& value);
  std::string GetConfItem(const std::string& name);

  void SetHandle(rocksdb::ColumnFamilyHandle* handle) { handle_ = handle; }
  rocksdb::ColumnFamilyHandle* GetHandle() const { return handle_; }

  void Dump();

 private:
  std::string name_;
  ColumnFamilyConfig config_;
  // reference family_handles_  do not release this handle
  rocksdb::ColumnFamilyHandle* handle_;
};

using ColumnFamilyPtr = std::shared_ptr<ColumnFamily>;
using ColumnFamilyMap = std::map<std::string, ColumnFamilyPtr>;

class Iterator : public dingodb::Iterator {
 public:
  explicit Iterator(IteratorOptions options, rocksdb::Iterator* iter)
      : options_(options), iter_(iter), snapshot_(nullptr) {}
  explicit Iterator(IteratorOptions options, rocksdb::Iterator* iter, std::shared_ptr<Snapshot> snapshot)
      : options_(options), iter_(iter), snapshot_(snapshot) {}
  ~Iterator() override = default;

  std::string GetName() override { return "RawRocks"; }
  IteratorType GetID() override { return IteratorType::kRawRocksEngine; }

  bool Valid() const override;

  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }

  void Seek(const std::string& target) override { return iter_->Seek(target); }

  void SeekForPrev(const std::string& target) override { return iter_->SeekForPrev(target); }

  void Next() override { iter_->Next(); }

  void Prev() override { iter_->Prev(); }

  std::string_view Key() const override { return std::string_view(iter_->key().data(), iter_->key().size()); }
  std::string_view Value() const override { return std::string_view(iter_->value().data(), iter_->value().size()); }

  butil::Status Status() const override;

 private:
  IteratorOptions options_;
  std::unique_ptr<rocksdb::Iterator> iter_;
  std::shared_ptr<Snapshot> snapshot_;
};
using IteratorPtr = std::shared_ptr<Iterator>;

class Snapshot : public dingodb::Snapshot {
 public:
  explicit Snapshot(const rocksdb::Snapshot* snapshot, std::shared_ptr<rocksdb::DB> db)
      : snapshot_(snapshot), db_(db) {}
  ~Snapshot() override {
    if (db_ != nullptr && snapshot_ != nullptr) {
      db_->ReleaseSnapshot(snapshot_);
      snapshot_ = nullptr;
    }
  };

  const void* Inner() override { return snapshot_; }

 private:
  const rocksdb::Snapshot* snapshot_;
  std::shared_ptr<rocksdb::DB> db_;
};
using SnapshotPtr = std::shared_ptr<Snapshot>;

class SstFileWriter {
 public:
  SstFileWriter(const rocksdb::Options& options)
      : options_(options),
        sst_writer_(std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), options_, nullptr, true)) {}
  ~SstFileWriter() = default;

  SstFileWriter(SstFileWriter&& rhs) = delete;
  SstFileWriter& operator=(SstFileWriter&& rhs) = delete;

  butil::Status SaveFile(const std::vector<pb::common::KeyValue>& kvs, const std::string& filename);
  butil::Status SaveFile(std::shared_ptr<dingodb::Iterator> iter, const std::string& filename);

  int64_t GetSize() { return sst_writer_->FileSize(); }

 private:
  rocksdb::Options options_;
  std::unique_ptr<rocksdb::SstFileWriter> sst_writer_;
};
using SstFileWriterPtr = std::shared_ptr<SstFileWriter>;

class Checkpoint : public RawEngine::Checkpoint {
 public:
  Checkpoint(std::shared_ptr<RocksRawEngine> raw_engine) : raw_engine_(raw_engine) {}
  ~Checkpoint() override = default;

  Checkpoint(Checkpoint&& rhs) = delete;
  Checkpoint& operator=(Checkpoint&& rhs) = delete;

  butil::Status Create(const std::string& dirpath) override;
  butil::Status Create(const std::string& dirpath, const std::vector<std::string>& cf_names,
                       std::vector<pb::store_internal::SstFileInfo>& sst_files) override;

 private:
  std::shared_ptr<RocksRawEngine> GetRawEngine();
  std::shared_ptr<rocksdb::DB> GetDB();
  std::vector<rocks::ColumnFamilyPtr> GetColumnFamilies(const std::vector<std::string>& cf_names);

  std::weak_ptr<RocksRawEngine> raw_engine_;
};
using CheckpointPtr = std::shared_ptr<Checkpoint>;

class Reader : public RawEngine::Reader {
 public:
  Reader(std::shared_ptr<RocksRawEngine> raw_engine) : raw_engine_(raw_engine){};
  ~Reader() override = default;

  butil::Status KvGet(const std::string& cf_name, const std::string& key, std::string& value) override;
  butil::Status KvGet(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& key,
                      std::string& value) override;

  butil::Status KvScan(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                       std::vector<pb::common::KeyValue>& kvs) override;
  butil::Status KvScan(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                       const std::string& start_key, const std::string& end_key,
                       std::vector<pb::common::KeyValue>& kvs) override;

  butil::Status KvCount(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                        int64_t& count) override;
  butil::Status KvCount(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& start_key,
                        const std::string& end_key, int64_t& count) override;

  dingodb::IteratorPtr NewIterator(const std::string& cf_name, IteratorOptions options) override;
  dingodb::IteratorPtr NewIterator(const std::string& cf_name, dingodb::SnapshotPtr snapshot,
                                   IteratorOptions options) override;

 private:
  std::shared_ptr<RocksRawEngine> GetRawEngine();
  dingodb::SnapshotPtr GetSnapshot();
  std::shared_ptr<rocksdb::DB> GetDB();
  ColumnFamilyPtr GetColumnFamily(const std::string& cf_name);
  ColumnFamilyPtr GetDefaultColumnFamily();

  butil::Status KvGet(ColumnFamilyPtr column_family, dingodb::SnapshotPtr snapshot, const std::string& key,
                      std::string& value);
  butil::Status KvScan(ColumnFamilyPtr column_family, std::shared_ptr<dingodb::Snapshot> snapshot,
                       const std::string& start_key, const std::string& end_key,
                       std::vector<pb::common::KeyValue>& kvs);
  butil::Status KvCount(ColumnFamilyPtr column_family, std::shared_ptr<dingodb::Snapshot> snapshot,
                        const std::string& start_key, const std::string& end_key, int64_t& count);
  dingodb::IteratorPtr NewIterator(ColumnFamilyPtr column_family, dingodb::SnapshotPtr snapshot,
                                   IteratorOptions options);

  std::weak_ptr<RocksRawEngine> raw_engine_;
};

class Writer : public RawEngine::Writer {
 public:
  Writer(std::shared_ptr<RocksRawEngine> raw_engine) : raw_engine_(raw_engine) {}
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

 private:
  std::shared_ptr<RocksRawEngine> GetRawEngine();
  std::shared_ptr<rocksdb::DB> GetDB();
  ColumnFamilyPtr GetColumnFamily(const std::string& cf_name);
  ColumnFamilyPtr GetDefaultColumnFamily();

  std::weak_ptr<RocksRawEngine> raw_engine_;
};

}  // namespace rocks

class RocksRawEngine : public RawEngine {
 public:
  RocksRawEngine();
  ~RocksRawEngine() override;

  RocksRawEngine(const RocksRawEngine& rhs) = delete;
  RocksRawEngine& operator=(const RocksRawEngine& rhs) = delete;
  RocksRawEngine(RocksRawEngine&& rhs) = delete;
  RocksRawEngine& operator=(RocksRawEngine&& rhs) = delete;

  std::shared_ptr<RocksRawEngine> GetSelfPtr();

  std::string GetName() override;
  pb::common::RawEngine GetRawEngineType() override;
  std::string DbPath();

  bool Init(std::shared_ptr<Config> config, const std::vector<std::string>& cf_names) override;
  void Close() override;
  void Destroy() override;

  dingodb::SnapshotPtr GetSnapshot() override;

  RawEngine::ReaderPtr Reader() override;
  RawEngine::WriterPtr Writer() override;

  static rocks::SstFileWriterPtr NewSstFileWriter();
  RawEngine::CheckpointPtr NewCheckpoint() override;

  butil::Status MergeCheckpointFiles(const std::string& path, const pb::common::Range& range,
                                     const std::vector<std::string>& cf_names,
                                     std::vector<std::string>& merge_sst_paths) override;

  butil::Status IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files) override;

  void Flush(const std::string& cf_name) override;
  butil::Status Compact(const std::string& cf_name) override;

  std::vector<int64_t> GetApproximateSizes(const std::string& cf_name, std::vector<pb::common::Range>& ranges) override;

 private:
  friend rocks::Reader;
  friend rocks::Writer;
  friend rocks::Checkpoint;

  std::shared_ptr<rocksdb::DB> GetDB();

  rocks::ColumnFamilyPtr GetDefaultColumnFamily();
  rocks::ColumnFamilyPtr GetColumnFamily(const std::string& cf_name);
  std::vector<rocks::ColumnFamilyPtr> GetColumnFamilies(const std::vector<std::string>& cf_names);

  std::string db_path_;
  std::shared_ptr<rocksdb::DB> db_;
  rocks::ColumnFamilyMap column_families_;

  RawEngine::ReaderPtr reader_;
  RawEngine::WriterPtr writer_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ROCKS_RAW_ENGINE_H_  // NOLINT

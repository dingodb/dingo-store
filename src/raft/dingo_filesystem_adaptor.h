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

#ifndef DINGODB_DINGO_FILESYSTEM_ADAPTOR_H_
#define DINGODB_DINGO_FILESYSTEM_ADAPTOR_H_

#include <braft/file_system_adaptor.h>
#include <braft/raft.h>
#include <braft/util.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/constant.h"
#include "coordinator/tso_control.h"
#include "engine/raw_engine.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_writer.h"

namespace dingodb {

class SstFileWriter {
 public:
  SstFileWriter(const rocksdb::Options& options) : options_(options) {
    options_.bottommost_compression = rocksdb::kNoCompression;
    options_.bottommost_compression_opts = rocksdb::CompressionOptions();
    sst_writer_ = std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), options_, nullptr, true);
  }

  rocksdb::Status Open(const std::string& sst_file) { return sst_writer_->Open(sst_file); }
  rocksdb::Status Put(const rocksdb::Slice& key, const rocksdb::Slice& value) { return sst_writer_->Put(key, value); }
  rocksdb::Status Finish(rocksdb::ExternalSstFileInfo* file_info = nullptr) { return sst_writer_->Finish(file_info); }
  uint64_t FileSize() { return sst_writer_->FileSize(); }
  virtual ~SstFileWriter() = default;

 private:
  rocksdb::Options options_;
  std::unique_ptr<rocksdb::SstFileWriter> sst_writer_ = nullptr;
};

const std::string kSnapshotDataFile = Constant::kRaftSnapshotRegionDateFileNameSuffix;
const std::string kSnapshotMetaFile = Constant::kRaftSnapshotRegionMetaFileName;

class DingoFileSystemAdaptor;
struct SnapshotContext;
struct IteratorContext {
  int64_t region_id = 0;
  std::string cf_name{};
  bool reading = false;
  std::string lower_bound;
  std::string upper_bound;
  int64_t applied_index = 0;
  SnapshotContext* snapshot_context = nullptr;
  std::shared_ptr<dingodb::Iterator> iter;
  int64_t offset = 0;
  TimeCost offset_update_time;  // update this time when updating offset, if long time not access, the peer may be down
  bool done = false;
};

struct SnapshotContext {
  SnapshotContext(std::shared_ptr<RawEngine> raw_engine_of_region) {
    raw_engine = raw_engine_of_region;
    snapshot = raw_engine->GetSnapshot();
  }
  ~SnapshotContext() {
    if (snapshot != nullptr) {
      snapshot = nullptr;
    }
    data_iterators.clear();
  }
  std::shared_ptr<dingodb::Snapshot> snapshot;
  std::map<std::string, std::shared_ptr<IteratorContext>> data_iterators;
  int64_t applied_term = 0;
  int64_t applied_index = 0;
  pb::common::RegionEpoch region_epoch;
  pb::common::Range range;
  std::shared_ptr<RawEngine> raw_engine;
};

class PosixDirReader : public braft::DirReader {
  friend class DingoFileSystemAdaptor;

 public:
  ~PosixDirReader() override = default;

  bool is_valid() const override;

  bool next() override;

  const char* name() const override;

 protected:
  PosixDirReader(const std::string& path) : dir_reader_(path.c_str()) {}

 private:
  butil::DirReaderPosix dir_reader_;
};

// DingoReaderAdaptor is used to read data from raw engine when leader doing snapshot install to follower
// the data read from raw engine will be written to sst file by SstWriterAdaptor on follower side
class DingoDataReaderAdaptor : public braft::FileAdaptor {
  friend class DingoFileSystemAdaptor;

 public:
  ~DingoDataReaderAdaptor() override;

  ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;

  ssize_t size() override;

  bool close() override;
  void Open() { closed_ = false; }
  ssize_t write(const butil::IOBuf& data, off_t offset) override;

  bool sync() override;

 protected:
  DingoDataReaderAdaptor(int64_t region_id, const std::string& path, DingoFileSystemAdaptor* dingo_fs_adaptor,
                         std::shared_ptr<IteratorContext> context);

 private:
  static int64_t SerializeToIobuf(butil::IOPortal* portal, const rocksdb::Slice& key) {
    if (portal != nullptr) {
      portal->append((void*)&key.size_, sizeof(size_t));
      portal->append((void*)key.data_, key.size_);
    }
    return sizeof(size_t) + key.size_;
  }

  bool RegionShutdown();

  void ContextReset();

  int64_t region_id_ = 0;
  int64_t cf_id_ = 0;
  store::RegionPtr region_ptr_;
  std::string path_;
  DingoFileSystemAdaptor* dingo_fs_adatpor_ = nullptr;
  std::shared_ptr<IteratorContext> iter_ctx_in_adaptor_ = nullptr;
  bool closed_ = true;
  size_t num_lines_ = 0;
  butil::IOPortal last_package_;
  off_t last_offset_ = 0;
};

class DingoMetaReaderAdaptor : public braft::FileAdaptor {
  friend class DingoFileSystemAdaptor;

 public:
  ~DingoMetaReaderAdaptor() override;

  ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;

  ssize_t size() override;

  bool close() override;
  void Open() { closed_ = false; }
  ssize_t write(const butil::IOBuf& data, off_t offset) override;

  bool sync() override;

 protected:
  DingoMetaReaderAdaptor(int64_t region_id, const std::string& path, int64_t applied_term, int64_t applied_index,
                         pb::common::RegionEpoch& region_epoch, pb::common::Range& range);

 private:
  static int64_t SerializeToIobuf(butil::IOPortal* portal, const rocksdb::Slice& key) {
    if (portal != nullptr) {
      // portal->append((void*)&key.size_, sizeof(size_t));
      portal->append((void*)key.data_, key.size_);
    }
    // return sizeof(size_t) + key.size_;
    return key.size_;
  }

  bool RegionShutdown();

  int64_t region_id_ = 0;
  int64_t cf_id_ = 0;
  store::RegionPtr region_ptr_;
  std::string path_;
  bool closed_ = true;

  int64_t applied_term_;
  int64_t applied_index_;
  pb::common::RegionEpoch region_epoch_;
  pb::common::Range range_;
  std::string meta_data_;
};

// SstWriterAdaptor is used to write sst file on follower when doing snapshot install to follower
// the data to write is from raft snapshot executor's remote file copier
class SstWriterAdaptor : public braft::FileAdaptor {
  friend class DingoFileSystemAdaptor;

 public:
  ~SstWriterAdaptor() override;

  int Open();

  ssize_t write(const butil::IOBuf& data, off_t offset) override;
  bool close() override;

  ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;

  ssize_t size() override;

  bool sync() override;

 protected:
  SstWriterAdaptor(int64_t region_id, const std::string& path, const rocksdb::Options& option);

  bool RegionShutdown();

 private:
  bool FinishSst();
  int IobufToSst(butil::IOBuf data);
  int64_t region_id_;
  store::RegionPtr region_ptr_;
  std::string path_;
  size_t count_ = 0;
  size_t data_size_ = 0;
  bool closed_ = true;
  std::unique_ptr<SstFileWriter> writer_;
};

// PosixFileAdaptor is used to write meta file on leader when doing snapshot install to follower
// and to read all file on follower when doing snapshot install
class PosixFileAdaptor : public braft::FileAdaptor {
  friend class DingoFileSystemAdaptor;

 public:
  ~PosixFileAdaptor() override;
  int Open(int oflag);
  ssize_t write(const butil::IOBuf& data, off_t offset) override;
  ssize_t read(butil::IOPortal* portal, off_t offset, size_t size) override;
  ssize_t size() override;
  bool sync() override;
  bool close() override;

 protected:
  PosixFileAdaptor(const std::string& p) : path_(p) {}

 private:
  std::string path_;
  int fd_{-1};
};

// DingoFileSystemAdaptor is passed to raft to handle snapshot file read and write
class DingoFileSystemAdaptor : public braft::FileSystemAdaptor {
 public:
  DingoFileSystemAdaptor(int64_t region_id);
  ~DingoFileSystemAdaptor() override;

  bool delete_file(const std::string& path, bool recursive) override;
  bool rename(const std::string& old_path, const std::string& new_path) override;
  bool link(const std::string& old_path, const std::string& new_path) override;
  bool create_directory(const std::string& path, butil::File::Error* error, bool create_parent_directories) override;
  bool path_exists(const std::string& path) override;
  bool directory_exists(const std::string& path) override;
  braft::DirReader* directory_reader(const std::string& path) override;

  braft::FileAdaptor* open(const std::string& path, int oflag, const ::google::protobuf::Message* file_meta,
                           butil::File::Error* e) override;
  bool open_snapshot(const std::string& snapshot_path) override;
  void close_snapshot(const std::string& snapshot_path) override;

  void Close(const std::string& path);

 private:
  braft::FileAdaptor* OpenReaderAdaptor(const std::string& path, int oflag,
                                        const ::google::protobuf::Message* file_meta, butil::File::Error* e);
  braft::FileAdaptor* OpenWriterAdaptor(const std::string& path, int oflag,
                                        const ::google::protobuf::Message* file_meta, butil::File::Error* e);

  std::shared_ptr<SnapshotContext> GetSnapshot(const std::string& path);

  struct SnapshotContextEnv {
    std::shared_ptr<SnapshotContext> snapshot_context;
    int64_t count = 0;
    TimeCost cost;
  };
  int64_t region_id_;
  bthread::Mutex snapshot_mutex_;
  bthread::Mutex open_reader_adaptor_mutex_;
  BthreadCond mutil_snapshot_cond_;
  std::map<std::string, SnapshotContextEnv> snapshot_context_env_map_;
};

}  // namespace dingodb

#endif  // DINGODB_DINGO_FILESYSTEM_ADAPTOR_H_

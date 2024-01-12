// Copyright(c) 2023 dingodb.com, Inc.All Rights Reserved
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

#include "raft/dingo_filesystem_adaptor.h"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>

#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "engine/iterator.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

DEFINE_int64(snapshot_timeout_min, 10, "snapshot_timeout_min : 10min");
DECLARE_string(raft_snapshot_policy);

bool inline IsSnapshotMetaFile(const std::string& path) {
  butil::StringPiece sp(path);
  return sp.ends_with(kSnapshotMetaFile);
}

bool inline IsSnapshotDataFile(const std::string& path) {
  butil::StringPiece sp(path);
  return sp.ends_with(kSnapshotDataFile);
}

std::string GetSnapshotPath(const std::string& path) {
  // the path is like /data/snapshot_00001/default.dingo_sst
  // the snapshot_path is like /data/snapshot_00001
  std::filesystem::path p(path);
  return p.parent_path().string();
}

std::string GetSnapshotFileName(const std::string& path) {
  // the path is like /data/snapshot_00001/default.dingo_sst
  // the snapshot_file_name is like snapshot_00001.dingo_sst
  std::filesystem::path p(path);
  return p.filename().string();
}

std::string GetSnapshotCfName(const std::string& path) {
  // the path is like /data/snapshot_00001/default.dingo_sst
  // the snapshot_file_name is like snapshot_00001.dingo_sst
  std::filesystem::path p(path);
  auto snapshot_file_name = p.filename().string();
  // trim ".sst" of snapshot_file_name
  auto cf_name =
      snapshot_file_name.substr(0, snapshot_file_name.size() - Constant::kRaftSnapshotRegionDateFileNameSuffix.size());

  auto cf_names = Helper::GetColumnFamilyNamesByRole();
  for (auto& cf : cf_names) {
    if (cf == cf_name) {
      return cf_name;
    }
  }

  DINGO_LOG(ERROR) << "snapshot file name: " << snapshot_file_name << " not match any cf name";

  return std::string();
}

bool PosixDirReader::is_valid() const { return dir_reader_.IsValid(); }

bool PosixDirReader::next() {
  bool rc = dir_reader_.Next();
  while (rc && (strcmp(name(), ".") == 0 || strcmp(name(), "..") == 0)) {
    rc = dir_reader_.Next();
  }
  return rc;
}

const char* PosixDirReader::name() const { return dir_reader_.name(); }

// DingoDataReaderAdaptor
DingoDataReaderAdaptor::DingoDataReaderAdaptor(int64_t region_id, const std::string& path,
                                               DingoFileSystemAdaptor* dingo_fs_adaptor,
                                               std::shared_ptr<IteratorContext> context)
    : region_id_(region_id), path_(path), dingo_fs_adatpor_(dingo_fs_adaptor), iter_ctx_in_adaptor_(context) {
  region_ptr_ = Server::GetInstance().GetRegion(region_id_);
}

DingoDataReaderAdaptor::~DingoDataReaderAdaptor() { close(); }

bool DingoDataReaderAdaptor::RegionShutdown() {
  return region_ptr_ == nullptr || region_ptr_->State() == pb::common::StoreRegionState::DELETING ||
         region_ptr_->State() == pb::common::StoreRegionState::DELETED ||
         region_ptr_->State() == pb::common::StoreRegionState::STANDBY;
}

void DingoDataReaderAdaptor::ContextReset() {
  std::shared_ptr<IteratorContext> temp_iter_context = nullptr;
  temp_iter_context.reset(new IteratorContext);
  if (temp_iter_context == nullptr) {
    return;
  }
  temp_iter_context->region_id = iter_ctx_in_adaptor_->region_id;
  temp_iter_context->cf_name = iter_ctx_in_adaptor_->cf_name;
  temp_iter_context->reading = iter_ctx_in_adaptor_->reading;
  temp_iter_context->lower_bound = iter_ctx_in_adaptor_->lower_bound;
  temp_iter_context->upper_bound = iter_ctx_in_adaptor_->upper_bound;
  temp_iter_context->applied_index = iter_ctx_in_adaptor_->applied_index;
  temp_iter_context->snapshot_context = iter_ctx_in_adaptor_->snapshot_context;
  IteratorOptions iter_options;
  iter_options.lower_bound = temp_iter_context->lower_bound;
  iter_options.upper_bound = temp_iter_context->upper_bound;
  auto new_iter = iter_ctx_in_adaptor_->snapshot_context->raw_engine->Reader()->NewIterator(
      iter_ctx_in_adaptor_->cf_name, iter_ctx_in_adaptor_->snapshot_context->snapshot, IteratorOptions());
  temp_iter_context->iter = new_iter;
  temp_iter_context->iter->Seek(temp_iter_context->lower_bound);
  temp_iter_context->snapshot_context->data_iterators[iter_ctx_in_adaptor_->cf_name] = temp_iter_context;
  iter_ctx_in_adaptor_ = temp_iter_context;
}

ssize_t DingoDataReaderAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
  if (closed_) {
    DINGO_LOG(ERROR) << "data reader has been closed, region_id: " << region_id_ << ", offset: " << offset;
    return -1;
  }
  if (offset < 0) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " read error. offset: " << offset;
    return -1;
  }

  if (RegionShutdown()) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " shutdown, last_off: " << last_offset_ << ", off: " << offset
                     << ", ctx->off: " << iter_ctx_in_adaptor_->offset << ", size: " << size;
    return -1;
  }

  TimeCost time_cost;
  if (offset > iter_ctx_in_adaptor_->offset) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_
                     << " retry last_offset, offset biger fail, time_cost: " << time_cost.GetTime()
                     << ", last_off: " << last_offset_ << ", off: " << offset
                     << ", ctx->off: " << iter_ctx_in_adaptor_->offset << ", size: " << size;
    return -1;
  }
  if (offset < iter_ctx_in_adaptor_->offset) {
    // cache the last package, when retry first time, can recover
    if (last_offset_ == offset) {
      *portal = last_package_;
      DINGO_LOG(ERROR) << "region_id: " << region_id_ << " retry last_offset, time_cost: " << time_cost.GetTime()
                       << ", last_off: " << last_offset_ << ", off: " << offset
                       << ", ctx->off: " << iter_ctx_in_adaptor_->offset << ", size: " << size
                       << ", ret_size: " << last_package_.size();
      return last_package_.size();
    }

    // reset context_
    if (offset == 0 &&
        iter_ctx_in_adaptor_->offset_update_time.GetTime() > FLAGS_snapshot_timeout_min * 60 * 1000 * 1000ULL) {
      last_offset_ = 0;
      num_lines_ = 0;
      ContextReset();
      DINGO_LOG(ERROR) << "region_id: " << region_id_ << " context_reset, time_cost: " << time_cost.GetTime()
                       << ", last_off: " << last_offset_ << ", off: " << offset
                       << ", ctx->off: " << iter_ctx_in_adaptor_->offset << ", size: " << size;
    } else {
      DINGO_LOG(ERROR) << "region_id: " << region_id_ << " retry last_offset fail, time_cost: " << time_cost.GetTime()
                       << ", last_off: " << last_offset_ << ", off: " << offset
                       << ", ctx->off: " << iter_ctx_in_adaptor_->offset << ", size: " << size;
      return -1;
    }
  }

  size_t count = 0;
  int64_t key_num = 0;

  while (count < size) {
    if (!iter_ctx_in_adaptor_->iter->Valid() ||
        !(iter_ctx_in_adaptor_->iter->Key() >= iter_ctx_in_adaptor_->lower_bound)) {
      iter_ctx_in_adaptor_->done = true;
      // portal->append((void*)iter_context->offset, sizeof(size_t));
      DINGO_LOG(WARNING) << "region_id: " << region_id_
                         << " snapshot read over, total size: " << iter_ctx_in_adaptor_->offset;
      auto region = region_ptr_;
      if (region == nullptr) {
        DINGO_LOG(ERROR) << "region_id: " << region_id_ << " is null region";
        return -1;
      }
      break;
    }
    int64_t read_size = 0;
    key_num++;
    read_size += SerializeToIobuf(portal, iter_ctx_in_adaptor_->iter->Key());
    read_size += SerializeToIobuf(portal, iter_ctx_in_adaptor_->iter->Value());
    count += read_size;
    ++num_lines_;
    iter_ctx_in_adaptor_->offset += read_size;
    iter_ctx_in_adaptor_->offset_update_time.Reset();
    iter_ctx_in_adaptor_->iter->Next();
  }
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " read done. count: " << count << ", key_num: " << key_num
                     << ", time_cost: " << time_cost.GetTime() << ", off: " << offset << ", size: " << size
                     << ", last_off: " << last_offset_ << ", last_count: " << last_package_.size();
  last_offset_ = offset;
  last_package_ = *portal;
  return count;
}

bool DingoDataReaderAdaptor::close() {
  if (closed_) {
    DINGO_LOG(WARNING) << "file has been closed, region_id: " << region_id_ << ", num_lines: " << num_lines_
                       << ", path: " << path_;
    return true;
  }
  dingo_fs_adatpor_->Close(path_);
  closed_ = true;
  return true;
}

ssize_t DingoDataReaderAdaptor::size() {
  if (iter_ctx_in_adaptor_->done) {
    return iter_ctx_in_adaptor_->offset;
  }
  return std::numeric_limits<ssize_t>::max();
}

ssize_t DingoDataReaderAdaptor::write(const butil::IOBuf& data, off_t offset) {
  (void)data;
  (void)offset;
  DINGO_LOG(ERROR) << "DingoReaderAdaptor::write not implemented";
  return -1;
}

bool DingoDataReaderAdaptor::sync() { return true; }

// DingoMetaReaderAdaptor
DingoMetaReaderAdaptor::DingoMetaReaderAdaptor(int64_t region_id, const std::string& path, int64_t applied_term,
                                               int64_t applied_index, pb::common::RegionEpoch& region_epoch,
                                               pb::common::Range& range)
    : region_id_(region_id),
      path_(path),
      applied_term_(applied_term),
      applied_index_(applied_index),
      region_epoch_(region_epoch),
      range_(range) {
  region_ptr_ = Server::GetInstance().GetRegion(region_id_);

  pb::store_internal::RaftSnapshotRegionMeta meta;
  *(meta.mutable_epoch()) = region_epoch;
  *(meta.mutable_range()) = range;
  meta.set_term(applied_term);
  meta.set_log_index(applied_index);

  this->meta_data_ = meta.SerializeAsString();
}

DingoMetaReaderAdaptor::~DingoMetaReaderAdaptor() { close(); }

bool DingoMetaReaderAdaptor::RegionShutdown() {
  return region_ptr_ == nullptr || region_ptr_->State() == pb::common::StoreRegionState::DELETING ||
         region_ptr_->State() == pb::common::StoreRegionState::DELETED ||
         region_ptr_->State() == pb::common::StoreRegionState::STANDBY;
}

ssize_t DingoMetaReaderAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
  if (closed_) {
    DINGO_LOG(ERROR) << "meta reader has been closed, region_id: " << region_id_ << ", offset: " << offset;
    return -1;
  }

  if (offset < 0) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " read error. offset: " << offset;
    return -1;
  }

  if (RegionShutdown()) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " shutdown, off: " << offset << ", size: " << size;
    return -1;
  }

  TimeCost time_cost;

  if (offset > meta_data_.size() - 1) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_
                     << " retry last_offset, offset biger fail, time_cost: " << time_cost.GetTime()
                     << ", off: " << offset << ", size: " << size;
    return -1;
  }

  DINGO_LOG(INFO) << "region_id: " << region_id_ << " read meta, time_cost: " << time_cost.GetTime()
                  << ", off: " << offset << ", size: " << size << ", meta_size: " << meta_data_.size();

  size_t read_size = meta_data_.size() - offset > size ? size : meta_data_.size() - offset;
  size_t count = SerializeToIobuf(portal, meta_data_.substr(offset, read_size));

  DINGO_LOG(INFO) << "region_id: " << region_id_ << " read meta, time_cost: " << time_cost.GetTime()
                  << ", off: " << offset << ", size: " << size << ", ret_size: " << count;

  return count;
}

bool DingoMetaReaderAdaptor::close() {
  if (closed_) {
    DINGO_LOG(WARNING) << "file has been closed, region_id: " << region_id_ << ", path: " << path_;
    return true;
  }
  closed_ = true;
  return true;
}

ssize_t DingoMetaReaderAdaptor::size() { return meta_data_.size(); }

ssize_t DingoMetaReaderAdaptor::write(const butil::IOBuf& data, off_t offset) {
  (void)data;
  (void)offset;
  DINGO_LOG(ERROR) << "DingoReaderAdaptor::write not implemented";
  return -1;
}

bool DingoMetaReaderAdaptor::sync() { return true; }

// SstWriterAdaptor
bool SstWriterAdaptor::RegionShutdown() {
  return region_ptr_ == nullptr || region_ptr_->State() == pb::common::StoreRegionState::DELETING ||
         region_ptr_->State() == pb::common::StoreRegionState::DELETED ||
         region_ptr_->State() == pb::common::StoreRegionState::STANDBY;
}

SstWriterAdaptor::SstWriterAdaptor(int64_t region_id, const std::string& path, const rocksdb::Options& option)
    : region_id_(region_id), path_(path), writer_(new SstFileWriter(option)) {}

int SstWriterAdaptor::Open() {
  region_ptr_ = Server::GetInstance().GetRegion(region_id_);
  if (region_ptr_ == nullptr) {
    DINGO_LOG(ERROR) << "open sst file path: " << path_ << " failed, region_id: " << region_id_ << " not exist";
    return -1;
  }
  std::string path = path_;
  auto s = writer_->Open(path);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << "open sst file path: " << path << " failed, err: " << s.ToString()
                     << ", region_id: " << region_id_;
    return -1;
  }
  closed_ = false;
  DINGO_LOG(WARNING) << "sst writer open, path: " << path << ", region_id: " << region_id_;
  return 0;
}

ssize_t SstWriterAdaptor::write(const butil::IOBuf& data, off_t offset) {
  (void)offset;
  std::string path = path_;
  if (RegionShutdown()) {
    DINGO_LOG(ERROR) << "write sst file path: " << path << " failed, region shutdown, data len: " << data.size()
                     << ", region_id: " << region_id_;
    return -1;
  }
  if (closed_) {
    DINGO_LOG(ERROR) << "write sst file path: " << path << " failed, file closed: " << closed_
                     << ", data len: " << data.size() << ", region_id: " << region_id_;
    return -1;
  }
  if (data.empty()) {
    DINGO_LOG(WARNING) << "write sst file path: " << path << " failed, data len = 0, region_id: " << region_id_;
  }
  auto ret = IobufToSst(data);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "write sst file path: " << path << " failed, received invalid data, data len: " << data.size()
                     << ", region_id: " << region_id_;
    return -1;
  }
  data_size_ += data.size();

  DINGO_LOG(WARNING) << "sst write, region_id: " << region_id_ << ", path: " << path << ", offset: " << offset
                     << ", data.size: " << data.size() << ", file_size: " << writer_->FileSize()
                     << ", total_count: " << count_ << ", all_size: " << data_size_;

  return data.size();
}

bool SstWriterAdaptor::close() {
  if (closed_) {
    DINGO_LOG(WARNING) << "file has been closed, path: " << path_;
    return true;
  }
  closed_ = true;
  return FinishSst();
}

bool SstWriterAdaptor::FinishSst() {
  std::string path = path_;
  if (count_ > 0) {
    DINGO_LOG(WARNING) << "writer_ finished, path: " << path << ", region_id: " << region_id_
                       << ", file_size: " << writer_->FileSize() << ", total_count: " << count_
                       << ", all_size: " << data_size_;
    auto s = writer_->Finish();
    if (!s.ok()) {
      DINGO_LOG(ERROR) << "finish sst file path: " << path << " failed, err: " << s.ToString()
                       << ", region_id: " << region_id_;
      return false;
    }
  } else {
    bool ret = butil::DeleteFile(butil::FilePath(path), false);
    DINGO_LOG(WARNING) << "count is 0, delete path: " << path << ", region_id: " << region_id_;
    if (!ret) {
      DINGO_LOG(ERROR) << "delete sst file path: " << path << " failed, region_id: " << region_id_;
    }
  }
  return true;
}

int SstWriterAdaptor::IobufToSst(butil::IOBuf data) {
  char key_buf[4 * 1024];
  // 32KB stack should be enough for most cases
  char value_buf[32 * 1024];
  while (!data.empty()) {
    size_t key_size = 0;
    size_t nbytes = data.cutn((void*)&key_size, sizeof(size_t));
    if (nbytes < sizeof(size_t)) {
      DINGO_LOG(ERROR) << "read key size from iobuf fail, region_id: " << region_id_;
      return -1;
    }
    rocksdb::Slice key;
    std::unique_ptr<char[]> big_key_buf;
    // sst_file_writer does not support SliceParts, using fetch can try to 0 copy
    if (key_size <= sizeof(key_buf)) {
      key.data_ = static_cast<const char*>(data.fetch(key_buf, key_size));
    } else {
      big_key_buf.reset(new char[key_size]);
      DINGO_LOG(WARNING) << "region_id: " << region_id_ << ", key_size: " << key_size << " too big";
      key.data_ = static_cast<const char*>(data.fetch(big_key_buf.get(), key_size));
    }
    key.size_ = key_size;
    if (key.data_ == nullptr) {
      DINGO_LOG(ERROR) << "read key from iobuf fail, region_id: " << region_id_ << ", key_size: " << key_size;
      return -1;
    }
    data.pop_front(key_size);

    size_t value_size = 0;
    nbytes = data.cutn((void*)&value_size, sizeof(size_t));
    if (nbytes < sizeof(size_t)) {
      DINGO_LOG(ERROR) << "read value size from iobuf fail, region_id: " << region_id_ << ", value_size: " << value_size
                       << ", key_size: " << key_size << ", key: " << key.ToString(true);
      return -1;
    }
    rocksdb::Slice value;
    std::unique_ptr<char[]> big_value_buf;
    if (value_size <= sizeof(value_buf)) {
      if (value_size > 0) {
        value.data_ = static_cast<const char*>(data.fetch(value_buf, value_size));
      }
    } else {
      big_value_buf.reset(new char[value_size]);
      DINGO_LOG(WARNING) << "region_id: " << region_id_ << ", value_size: " << value_size << " too big";
      value.data_ = static_cast<const char*>(data.fetch(big_value_buf.get(), value_size));
    }
    value.size_ = value_size;
    if (value.data_ == nullptr) {
      DINGO_LOG(ERROR) << "read value from iobuf, region_id: " << region_id_ << ", value_size: " << value_size
                       << ", key_size: " << key_size << ", key: " << key.ToString(true);
      return -1;
    }
    data.pop_front(value_size);
    count_++;

    auto s = writer_->Put(key, value);
    if (!s.ok()) {
      DINGO_LOG(ERROR) << "write sst file failed, err: " << s.ToString() << ", region_id: " << region_id_;
      return -1;
    }
  }
  return 0;
}

SstWriterAdaptor::~SstWriterAdaptor() { close(); }

ssize_t SstWriterAdaptor::size() {
  DINGO_LOG(ERROR) << "SstWriterAdaptor::size not implemented, region_id: " << region_id_;
  return -1;
}

bool SstWriterAdaptor::sync() {
  // already sync in SstFileWriter::Finish
  return true;
}

ssize_t SstWriterAdaptor::read(butil::IOPortal* /*portal*/, off_t /*offset*/, size_t /*size*/) {
  DINGO_LOG(ERROR) << "SstWriterAdaptor::read not implemented, region_id: " << region_id_;
  return -1;
}

// PosixFileAdaptor
PosixFileAdaptor::~PosixFileAdaptor() { close(); }

int PosixFileAdaptor::Open(int oflag) {
  oflag &= (~O_CLOEXEC);
  fd_ = ::open(path_.c_str(), oflag, 0644);
  if (fd_ <= 0) {
    return -1;
  }
  return 0;
}

bool PosixFileAdaptor::close() {
  if (fd_ > 0) {
    bool res = ::close(fd_) == 0;
    fd_ = -1;
    return res;
  }
  return true;
}

ssize_t PosixFileAdaptor::write(const butil::IOBuf& data, off_t offset) {
  ssize_t ret = braft::file_pwrite(data, fd_, offset);
  return ret;
}

ssize_t PosixFileAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
  return braft::file_pread(portal, fd_, offset, size);
}

ssize_t PosixFileAdaptor::size() {
  off_t sz = lseek(fd_, 0, SEEK_END);
  return ssize_t(sz);
}

bool PosixFileAdaptor::sync() { return braft::raft_fsync(fd_) == 0; }

// DingoFileSystemAdaptor
DingoFileSystemAdaptor::DingoFileSystemAdaptor(int64_t region_id) : region_id_(region_id) {}

DingoFileSystemAdaptor::~DingoFileSystemAdaptor() {
  mutil_snapshot_cond_.Wait();
  DINGO_LOG(INFO) << "region_id: " << region_id_ << " DingoFileSystemAdaptor released";
}

braft::FileAdaptor* DingoFileSystemAdaptor::open(const std::string& path, int oflag,
                                                 const ::google::protobuf::Message* file_meta, butil::File::Error* e) {
  if (!IsSnapshotDataFile(path) && !IsSnapshotMetaFile(path)) {
    PosixFileAdaptor* adaptor = new PosixFileAdaptor(path);
    int ret = adaptor->Open(oflag);
    if (ret != 0) {
      if (e) {
        *e = butil::File::OSErrorToFileError(errno);
      }
      delete adaptor;
      return nullptr;
    }
    DINGO_LOG(WARNING) << "open file: " << path << ", region_id: " << region_id_ << ", " << (oflag & O_WRONLY);
    return adaptor;
  }

  bool for_write = (O_WRONLY & oflag);
  if (for_write) {
    if (IsSnapshotMetaFile(path)) {
      PosixFileAdaptor* adaptor = new PosixFileAdaptor(path);
      int ret = adaptor->Open(oflag);
      if (ret != 0) {
        if (e) {
          *e = butil::File::OSErrorToFileError(errno);
        }
        delete adaptor;
        return nullptr;
      }
      DINGO_LOG(WARNING) << "open file: " << path << ", region_id: " << region_id_ << ", " << (oflag & O_WRONLY);
      return adaptor;
    }
    return OpenWriterAdaptor(path, oflag, file_meta, e);
  }
  return OpenReaderAdaptor(path, oflag, file_meta, e);
}

braft::FileAdaptor* DingoFileSystemAdaptor::OpenWriterAdaptor(const std::string& path, int oflag,  // NOLINT
                                                              const ::google::protobuf::Message* file_meta,
                                                              butil::File::Error* e) {
  (void)file_meta;

  if (FLAGS_raft_snapshot_policy != "dingo") {
    PosixFileAdaptor* adaptor = new PosixFileAdaptor(path);
    int ret = adaptor->Open(oflag | O_WRONLY | O_CREAT | O_TRUNC);
    if (ret != 0) {
      if (e) {
        *e = butil::File::OSErrorToFileError(errno);
      }
      delete adaptor;
      return nullptr;
    }
    DINGO_LOG(WARNING) << "open file: " << path << ", region_id: " << region_id_ << ", "
                       << (O_WRONLY | O_CREAT | O_TRUNC);
    return adaptor;
  }

  rocksdb::Options options;
  options.bottommost_compression = rocksdb::kNoCompression;
  options.bottommost_compression_opts = rocksdb::CompressionOptions();

  SstWriterAdaptor* writer = new SstWriterAdaptor(region_id_, path, options);
  int ret = writer->Open();
  if (ret != 0) {
    if (e) {
      *e = butil::File::FILE_ERROR_FAILED;
    }
    delete writer;
    return nullptr;
  }
  DINGO_LOG(WARNING) << "open for write file, path: " << path << ", region_id: " << region_id_;
  return writer;
}

braft::FileAdaptor* DingoFileSystemAdaptor::OpenReaderAdaptor(const std::string& path, int oflag,
                                                              const ::google::protobuf::Message* file_meta,
                                                              butil::File::Error* e) {
  TimeCost time_cost;
  (void)file_meta;

  if (FLAGS_raft_snapshot_policy != "dingo") {
    PosixFileAdaptor* adaptor = new PosixFileAdaptor(path);
    int ret = adaptor->Open(oflag);
    if (ret != 0) {
      if (e) {
        *e = butil::File::OSErrorToFileError(errno);
      }
      delete adaptor;
      return nullptr;
    }
    DINGO_LOG(WARNING) << "open file: " << path << ", region_id: " << region_id_ << ", " << (O_RDONLY);
    return adaptor;
  }

  auto region = Server::GetInstance().GetRegion(region_id_);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " is null region";
    return nullptr;
  }

  const std::string snapshot_path = GetSnapshotPath(path);
  // raft_copy_remote_file_timeout_ms is set to 300s
  // use another mutex here to avoid blocking the main thread and promise serialize
  BAIDU_SCOPED_LOCK(open_reader_adaptor_mutex_);
  auto snapshot_context = GetSnapshot(snapshot_path);
  if (snapshot_context == nullptr) {
    DINGO_LOG(ERROR) << "snapshot no found, path: " << snapshot_path << ", region_id: " << region_id_;
    if (e != nullptr) {
      *e = butil::File::FILE_ERROR_NOT_FOUND;
    }
    return nullptr;
  }

  if (IsSnapshotMetaFile(path)) {
    auto* reader =
        new DingoMetaReaderAdaptor(region_id_, path, snapshot_context->applied_term, snapshot_context->applied_index,
                                   snapshot_context->region_epoch, snapshot_context->range);
    reader->Open();
    return reader;
  }

  if (!IsSnapshotDataFile(path)) {
    DINGO_LOG(ERROR) << "path: " << path << " is not snapshot file, region_id: " << region_id_;
    if (e != nullptr) {
      *e = butil::File::FILE_ERROR_NOT_FOUND;
    }
    return nullptr;
  }

  auto cf_name = GetSnapshotCfName(path);
  if (cf_name.empty()) {
    DINGO_LOG(ERROR) << "snapshot file path: " << path << " is invalid, region_id: " << region_id_;
    if (e != nullptr) {
      *e = butil::File::FILE_ERROR_NOT_FOUND;
    }
    return nullptr;
  }

  DINGO_LOG(INFO) << "open snapshot file, build iter_context, path: " << path << ", region_id: " << region_id_
                  << ", cf_name: " << cf_name;

  // we build a iterator context for every cf, so the code next line is ok.
  std::shared_ptr<IteratorContext> iter_context = nullptr;
  if (snapshot_context->data_iterators.find(cf_name) != snapshot_context->data_iterators.end()) {
    iter_context = snapshot_context->data_iterators[cf_name];
  }

  // first open snapshot file
  if (iter_context == nullptr) {
    iter_context = std::make_shared<IteratorContext>();
    iter_context->region_id = region_id_;
    iter_context->cf_name = cf_name;
    iter_context->reading = false;

    if (Helper::IsTxnColumnFamilyName(cf_name)) {
      pb::common::Range txn_range = Helper::GetMemComparableRange(snapshot_context->range);
      iter_context->lower_bound = txn_range.start_key();
      iter_context->upper_bound = txn_range.end_key();
    } else {
      iter_context->lower_bound = snapshot_context->range.start_key();
      iter_context->upper_bound = snapshot_context->range.end_key();
    }

    iter_context->applied_index = snapshot_context->applied_index;
    iter_context->snapshot_context = snapshot_context.get();

    IteratorOptions iter_options;
    iter_options.lower_bound = iter_context->lower_bound;
    iter_options.upper_bound = iter_context->upper_bound;
    auto new_iter = iter_context->snapshot_context->raw_engine->Reader()->NewIterator(
        iter_context->cf_name, iter_context->snapshot_context->snapshot, IteratorOptions());
    iter_context->iter = new_iter;
    iter_context->iter->Seek(iter_context->lower_bound);

    snapshot_context->data_iterators[cf_name] = iter_context;

    DINGO_LOG(WARNING) << "region_id: " << region_id_ << " open reader, path: " << path
                       << ", time_cost: " << time_cost.GetTime();
  }

  if (iter_context->reading) {
    DINGO_LOG(WARNING) << "snapshot reader is busy, path: " << path << ", region_id: " << region_id_;
    if (e != nullptr) {
      *e = butil::File::FILE_ERROR_IN_USE;
    }
    return nullptr;
  }
  iter_context->reading = true;
  auto* reader = new DingoDataReaderAdaptor(region_id_, path, this, iter_context);
  reader->Open();
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " open reader: path: " << path
                     << " applied_term: " << snapshot_context->applied_term
                     << " applied_index: " << snapshot_context->applied_index << " time_cost: " << time_cost.GetTime();
  return reader;
}

bool DingoFileSystemAdaptor::delete_file(const std::string& path, bool recursive) {
  butil::FilePath file_path(path);
  return butil::DeleteFile(file_path, recursive);
}

bool DingoFileSystemAdaptor::rename(const std::string& old_path, const std::string& new_path) {
  return ::rename(old_path.c_str(), new_path.c_str()) == 0;
}

bool DingoFileSystemAdaptor::link(const std::string& old_path, const std::string& new_path) {
  return ::link(old_path.c_str(), new_path.c_str()) == 0;
}

bool DingoFileSystemAdaptor::create_directory(const std::string& path, butil::File::Error* error,
                                              bool create_parent_directories) {
  butil::FilePath dir(path);
  return butil::CreateDirectoryAndGetError(dir, error, create_parent_directories);
}

bool DingoFileSystemAdaptor::path_exists(const std::string& path) {
  butil::FilePath file_path(path);
  return butil::PathExists(file_path);
}

bool DingoFileSystemAdaptor::directory_exists(const std::string& path) {
  butil::FilePath file_path(path);
  return butil::DirectoryExists(file_path);
}

braft::DirReader* DingoFileSystemAdaptor::directory_reader(const std::string& path) { return new PosixDirReader(path); }

bool DingoFileSystemAdaptor::open_snapshot(const std::string& path) {
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " start open snapshot path: " << path;
  BAIDU_SCOPED_LOCK(snapshot_mutex_);
  auto iter = snapshot_context_env_map_.find(path);
  if (iter != snapshot_context_env_map_.end()) {
    // raft InstallSnapshot timeout -1
    // If long time no read, means the follower machine is hang
    // TODO: if long time no read, close snapshot
    if (iter->second.cost.GetTime() > 3600 * 1000 * 1000LL && iter->second.count == 1) {
      bool need_erase = true;
      for (const auto& data_iter : iter->second.snapshot_context->data_iterators) {
        if (data_iter.second->offset > 0) {
          need_erase = false;
          break;
        }
      }

      if (need_erase) {
        snapshot_context_env_map_.erase(iter);
        DINGO_LOG(WARNING) << "region_id: " << region_id_ << " snapshot path: " << path
                           << " is hang over 1 hour, erase";
      } else {
        DINGO_LOG(WARNING) << "region_id: " << region_id_ << " snapshot path: " << path
                           << " is hang over 1 hour, but data_iterators is not empty, not erase";
      }
    } else {
      // learner pull snapshot will keep raft_snapshot_reader_expire_time_s
      DINGO_LOG(WARNING) << "region_id: " << region_id_ << " snapshot path: " << path << " is busy";
      snapshot_context_env_map_[path].count++;
      return false;
    }
  }

  mutil_snapshot_cond_.Increase();
  // create new raw engine snapshot
  auto raft_node = Server::GetInstance().GetStorage()->GetRaftStoreEngine()->GetNode(region_id_);
  if (raft_node == nullptr) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " is not found raft node";
    return false;
  }

  auto snapshot_context = raft_node->MakeSnapshotContext();
  if (snapshot_context == nullptr) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " make snapshot context failed.";
    return false;
  }
  snapshot_context_env_map_[path].snapshot_context = snapshot_context;

  DINGO_LOG(INFO) << "region_id: " << region_id_ << " open snapshot path: " << path
                  << ", applied_term: " << snapshot_context_env_map_[path].snapshot_context->applied_term
                  << ", applied_index: " << snapshot_context_env_map_[path].snapshot_context->applied_index
                  << ", range: ["
                  << Helper::StringToHex(snapshot_context_env_map_[path].snapshot_context->range.start_key()) << ", "
                  << Helper::StringToHex(snapshot_context_env_map_[path].snapshot_context->range.end_key()) << "]"
                  << ", epoch: " << snapshot_context_env_map_[path].snapshot_context->region_epoch.ShortDebugString();

  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " open snapshot path: " << path << ", UnockRegionRaft";

  snapshot_context_env_map_[path].count++;
  snapshot_context_env_map_[path].cost.Reset();
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " open snapshot path: " << path
                     << ", applied_index: " << snapshot_context_env_map_[path].snapshot_context->applied_index;
  return true;
}

void DingoFileSystemAdaptor::close_snapshot(const std::string& path) {
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " close snapshot path: " << path;
  BAIDU_SCOPED_LOCK(snapshot_mutex_);
  auto iter = snapshot_context_env_map_.find(path);
  if (iter != snapshot_context_env_map_.end()) {
    snapshot_context_env_map_[path].count--;
    if (snapshot_context_env_map_[path].count == 0) {
      snapshot_context_env_map_.erase(iter);
      mutil_snapshot_cond_.DecreaseBroadcast();
      DINGO_LOG(WARNING) << "region_id: " << region_id_ << " close snapshot path: " << path << " relase";
    }
  }
}

std::shared_ptr<SnapshotContext> DingoFileSystemAdaptor::GetSnapshot(const std::string& path) {
  BAIDU_SCOPED_LOCK(snapshot_mutex_);
  auto iter = snapshot_context_env_map_.find(path);
  if (iter != snapshot_context_env_map_.end()) {
    return iter->second.snapshot_context;
  }
  return nullptr;
}

void DingoFileSystemAdaptor::Close(const std::string& path) {
  const std::string snapshot_path = GetSnapshotPath(path);

  BAIDU_SCOPED_LOCK(snapshot_mutex_);
  auto iter = snapshot_context_env_map_.find(snapshot_path);
  if (iter == snapshot_context_env_map_.end()) {
    DINGO_LOG(ERROR) << "no snapshot found when close reader, path: " << path << ", region_id: " << region_id_;
    return;
  }

  auto& snapshot_ctx = iter->second;

  auto cf_name = GetSnapshotCfName(path);
  if (cf_name.empty()) {
    DINGO_LOG(ERROR) << "snapshot file path: " << path << " is invalid, cannot get cf_name, region_id: " << region_id_;
    return;
  }

  snapshot_ctx.snapshot_context->data_iterators.erase(cf_name);

  DINGO_LOG(WARNING) << "close snapshot data file, path: " << path << ", region_id: " << region_id_;
}

}  // namespace dingodb
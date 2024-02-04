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

#include "log/segment_log_storage.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "braft/enum.pb.h"
#include "braft/fsync.h"
#include "braft/local_storage.pb.h"
#include "braft/log_entry.h"
#include "braft/protobuf_file.h"
#include "braft/util.h"
#include "butil/atomicops.h"
#include "butil/errno.h"
#include "butil/fd_utility.h"              // butil::make_close_on_exec
#include "butil/file_util.h"               // butil::CreateDirectory
#include "butil/files/dir_reader_posix.h"  // butil::DirReaderPosix
#include "butil/raw_pack.h"                // butil::RawPacker
#include "butil/string_printf.h"           // butil::string_appendf
#include "butil/time.h"
#include "bvar/latency_recorder.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/store_internal.pb.h"

#define SEGMENT_OPEN_PATTERN "log_inprogress_%020" PRId64
#define SEGMENT_CLOSED_PATTERN "log_%020" PRId64 "_%020" PRId64
#define SEGMENT_META_FILE "log_meta"

namespace dingodb {

DEFINE_bool(dingo_raft_sync_log, true, "Sync log to disk or not");
DEFINE_bool(dingo_trace_append_entry_latency, false, "Trace append entry latency");

using ::butil::RawPacker;
using ::butil::RawUnpacker;

static bvar::LatencyRecorder g_segment_log_open_segment_latency("dingo_segment_log_open_segment");
static bvar::LatencyRecorder g_segment_log_append_entry_latency("dingo_segment_log_append_entry");
static bvar::LatencyRecorder g_segment_log_sync_segment_latency("dingo_segment_log_sync_segment");

int FtruncateUninterrupted(int fd, off_t length) {
  int rc = 0;
  do {
    rc = ftruncate(fd, length);
  } while (rc == -1 && errno == EINTR);
  return rc;
}

enum class CheckSumType {
  kMurmurhash32 = 0,
  kCrc32 = 1,
};

enum class SyncPolicy {
  kImmediately = 0,
  kByBytes = 1,
};

static const SyncPolicy kSegmentLogSyncPolicy = SyncPolicy::kImmediately;

// Format of Header, all fields are in network order
// | -------------------- term (64bits) -------------------------  |
// | entry-type (8bits) | checksum_type (8bits) | reserved(16bits) |
// | ------------------ data len (32bits) -----------------------  |
// | data_checksum (32bits) | header checksum (32bits)             |

const static size_t kEntryHeaderSize = 24;

struct Segment::EntryHeader {
  int64_t term;
  int type;
  int checksum_type;
  uint32_t data_len;
  uint32_t data_checksum;
};

std::string ToString(const Segment::EntryHeader& h) {
  return fmt::format("(term={}, type={}, data_len={}, checksum_type={}, data_checksum={})", h.term, h.type, h.data_len,
                     h.checksum_type, h.data_checksum);
}

std::ostream& operator<<(std::ostream& os, const Segment::EntryHeader& h) {
  os << "{term=" << h.term << ", type=" << h.type << ", data_len=" << h.data_len
     << ", checksum_type=" << h.checksum_type << ", data_checksum=" << h.data_checksum << '}';
  return os;
}

inline bool VerifyChecksum(int checksum_type, const char* data, size_t len, uint32_t value) {
  switch (static_cast<CheckSumType>(checksum_type)) {
    case CheckSumType::kMurmurhash32:
      return (value == braft::murmurhash32(data, len));
    case CheckSumType::kCrc32:
      return (value == braft::crc32(data, len));
    default:
      DINGO_LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
      return false;
  }
}

inline bool VerifyChecksum(int checksum_type, const butil::IOBuf& data, uint32_t value) {
  switch (static_cast<CheckSumType>(checksum_type)) {
    case CheckSumType::kMurmurhash32:
      return (value == braft::murmurhash32(data));
    case CheckSumType::kCrc32:
      return (value == braft::crc32(data));
    default:
      DINGO_LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
      return false;
  }
}

inline uint32_t GetChecksum(int checksum_type, const char* data, size_t len) {
  switch (static_cast<CheckSumType>(checksum_type)) {
    case CheckSumType::kMurmurhash32:
      return braft::murmurhash32(data, len);
    case CheckSumType::kCrc32:
      return braft::crc32(data, len);
    default:
      CHECK(false) << "Unknown checksum_type=" << checksum_type;
      abort();
      return 0;
  }
}

inline uint32_t GetChecksum(int checksum_type, const butil::IOBuf& data) {
  switch (static_cast<CheckSumType>(checksum_type)) {
    case CheckSumType::kMurmurhash32:
      return braft::murmurhash32(data);
    case CheckSumType::kCrc32:
      return braft::crc32(data);
    default:
      CHECK(false) << "Unknown checksum_type=" << checksum_type;
      abort();
      return 0;
  }
}

int Segment::Create() {
  if (!is_open_) {
    CHECK(false) << fmt::format("[raft.log][region({}).index({}_{})] create on a closed segment, path: {}", region_id_,
                                FirstIndex(), LastIndex(), path_);
    return -1;
  }

  std::string path(path_);
  butil::string_appendf(&path, "/" SEGMENT_OPEN_PATTERN, first_index_);
  fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (fd_ >= 0) {
    butil::make_close_on_exec(fd_);
  }
  DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] created new segment, fd:{} path: {}", region_id_,
                                 FirstIndex(), LastIndex(), fd_, path_);
  return fd_ >= 0 ? 0 : -1;
}

int Segment::LoadEntry(off_t offset, EntryHeader* head, butil::IOBuf* data, size_t size_hint) const {
  butil::IOPortal buf;
  size_t to_read = std::max(size_hint, kEntryHeaderSize);
  const ssize_t n = braft::file_pread(&buf, fd_, offset, to_read);
  if (n != (ssize_t)to_read) {
    return n < 0 ? -1 : 1;
  }
  char header_buf[kEntryHeaderSize];
  const char* p = (const char*)buf.fetch(header_buf, kEntryHeaderSize);
  int64_t term = 0;
  uint32_t meta_field;
  uint32_t data_len = 0;
  uint32_t data_checksum = 0;
  uint32_t header_checksum = 0;
  RawUnpacker(p)
      .unpack64((uint64_t&)term)
      .unpack32(meta_field)
      .unpack32(data_len)
      .unpack32(data_checksum)
      .unpack32(header_checksum);
  EntryHeader tmp;
  tmp.term = term;
  tmp.type = meta_field >> 24;
  tmp.checksum_type = (meta_field << 8) >> 24;
  tmp.data_len = data_len;
  tmp.data_checksum = data_checksum;
  if (!VerifyChecksum(tmp.checksum_type, p, kEntryHeaderSize - 4, header_checksum)) {
    DINGO_LOG(ERROR) << fmt::format(
        "[raft.log][region({}).index({}_{})] found corrupted header at offset: {}, header: {} path_: {}", region_id_,
        FirstIndex(), LastIndex(), ToString(tmp), offset, path_);
    return -1;
  }
  if (head != nullptr) {
    *head = tmp;
  }
  if (data != nullptr) {
    if (buf.length() < kEntryHeaderSize + data_len) {
      const size_t to_read = kEntryHeaderSize + data_len - buf.length();
      const ssize_t n = braft::file_pread(&buf, fd_, offset + buf.length(), to_read);
      if (n != (ssize_t)to_read) {
        return n < 0 ? -1 : 1;
      }
    } else if (buf.length() > kEntryHeaderSize + data_len) {
      buf.pop_back(buf.length() - kEntryHeaderSize - data_len);
    }
    CHECK_EQ(buf.length(), kEntryHeaderSize + data_len);
    buf.pop_front(kEntryHeaderSize);
    if (!VerifyChecksum(tmp.checksum_type, buf, tmp.data_checksum)) {
      DINGO_LOG(ERROR) << fmt::format(
          "[raft.log][region({}).index({}_{})] found corrupted data at offset: {} header: {} path:{}", region_id_,
          FirstIndex(), LastIndex(), offset + kEntryHeaderSize, ToString(tmp), path_);
      // TODO: abort()?
      return -1;
    }
    data->swap(buf);
  }
  return 0;
}

int Segment::GetMeta(int64_t index, LogMeta* meta) const {
  BAIDU_SCOPED_LOCK(mutex_);
  if (index > last_index_.load(butil::memory_order_relaxed) || index < first_index_) {
    // out of range
    DINGO_LOG(DEBUG) << fmt::format("[raft.log][region({}).index({}_{})] index: {}.", region_id_, FirstIndex(),
                                    LastIndex(), index);
    return -1;
  } else if (last_index_ == first_index_ - 1) {
    DINGO_LOG(DEBUG) << fmt::format("[raft.log][region({}).index({}_{})] get meta.", region_id_, FirstIndex(),
                                    LastIndex());
    // empty
    return -1;
  }
  int64_t meta_index = index - first_index_;
  int64_t entry_cursor = offset_and_term_[meta_index].first;
  int64_t next_cursor =
      (index < last_index_.load(butil::memory_order_relaxed)) ? offset_and_term_[meta_index + 1].first : bytes_;
  DCHECK_LT(entry_cursor, next_cursor);
  meta->offset = entry_cursor;
  meta->term = offset_and_term_[meta_index].second;
  meta->length = next_cursor - entry_cursor;
  return 0;
}

int Segment::Load(braft::ConfigurationManager* configuration_manager) {
  int ret = 0;

  std::string path(path_);
  // create fd
  if (is_open_) {
    butil::string_appendf(&path, "/" SEGMENT_OPEN_PATTERN, first_index_);
  } else {
    butil::string_appendf(&path, "/" SEGMENT_CLOSED_PATTERN, first_index_, last_index_.load());
  }
  fd_ = ::open(path.c_str(), O_RDWR);
  if (fd_ < 0) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] open failed, path: {} error: {}", region_id_,
                                    FirstIndex(), LastIndex(), path, berror());
    return -1;
  }
  butil::make_close_on_exec(fd_);

  // get file size
  struct stat st_buf;
  if (fstat(fd_, &st_buf) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] get file stat failed, path: {} error: {}",
                                    region_id_, FirstIndex(), LastIndex(), path, berror());
    ::close(fd_);
    fd_ = -1;
    return -1;
  }

  // load entry index
  int64_t file_size = st_buf.st_size;
  int64_t entry_off = 0;
  int64_t actual_last_index = first_index_ - 1;
  for (int64_t i = first_index_; entry_off < file_size; i++) {
    EntryHeader header;
    const int rc = LoadEntry(entry_off, &header, nullptr, kEntryHeaderSize);
    if (rc > 0) {
      // The last log was not completely written, which should be truncated
      break;
    }
    if (rc < 0) {
      ret = rc;
      break;
    }
    // rc == 0
    const int64_t skip_len = kEntryHeaderSize + header.data_len;
    if (entry_off + skip_len > file_size) {
      // The last log was not completely written and it should be
      // truncated
      break;
    }
    if (header.type == braft::ENTRY_TYPE_CONFIGURATION) {
      butil::IOBuf data;
      // Header will be parsed again but it's fine as configuration
      // changing is rare
      if (LoadEntry(entry_off, nullptr, &data, skip_len) != 0) {
        break;
      }
      scoped_refptr<braft::LogEntry> entry = new braft::LogEntry();
      entry->id.index = i;
      entry->id.term = header.term;
      butil::Status status = parse_configuration_meta(data, entry);
      if (status.ok()) {
        braft::ConfigurationEntry conf_entry(*entry);
        configuration_manager->add(conf_entry);
      } else {
        DINGO_LOG(ERROR) << fmt::format(
            "[raft.log][region({}).index({}_{})] parse configuration meta failed, path: {} entry_off:{}", region_id_,
            FirstIndex(), LastIndex(), path_, entry_off);
        ret = -1;
        break;
      }
    }
    offset_and_term_.push_back(std::make_pair(entry_off, header.term));
    ++actual_last_index;
    entry_off += skip_len;
  }

  const int64_t last_index = last_index_.load(butil::memory_order_relaxed);
  if (ret == 0 && !is_open_) {
    if (actual_last_index < last_index) {
      DINGO_LOG(ERROR) << fmt::format(
          "[raft.log][region({}).index({}_{})] data lost in a full segment, actual_last_index: {} path: {}", region_id_,
          FirstIndex(), LastIndex(), actual_last_index, path_);
      ret = -1;
    } else if (actual_last_index > last_index) {
      // FIXME(zhengpengfei): should we ignore garbage entries silently
      DINGO_LOG(ERROR) << fmt::format(
          "[raft.log][region({}).index({}_{})] found garbage in a full segment, actual_last_index: {} path: {}",
          region_id_, FirstIndex(), LastIndex(), actual_last_index, path_);
      ret = -1;
    }
  }

  if (ret != 0) {
    return ret;
  }

  if (is_open_) {
    last_index_ = actual_last_index;
  }

  // truncate last uncompleted entry
  if (entry_off != file_size) {
    DINGO_LOG(INFO) << fmt::format(
        "[raft.log][region({}).index({}_{})] truncate last uncompleted write entry, old_size: {} new_size:{} path: {}",
        region_id_, FirstIndex(), LastIndex(), file_size, entry_off, path_);
    ret = FtruncateUninterrupted(fd_, entry_off);
  }

  // seek to end, for opening segment
  ::lseek(fd_, entry_off, SEEK_SET);

  bytes_ = entry_off;
  return ret;
}

int Segment::Append(const braft::LogEntry* entry) {
  if (BAIDU_UNLIKELY(!entry || !is_open_)) {
    return EINVAL;
  } else if (entry->id.index != last_index_.load(butil::memory_order_consume) + 1) {
    CHECK(false) << fmt::format("[raft.log][region({}).index({}_{})] append entry failed, index: {}, ", region_id_,
                                FirstIndex(), LastIndex(), entry->id.index);
    return ERANGE;
  }

  butil::IOBuf data;
  switch (entry->type) {
    case braft::ENTRY_TYPE_DATA:
      data.append(entry->data);
      break;
    case braft::ENTRY_TYPE_NO_OP:
      break;
    case braft::ENTRY_TYPE_CONFIGURATION: {
      butil::Status status = serialize_configuration_meta(entry, data);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format(
            "[raft.log][region({}).index({}_{})] serialize ConfigurationPBMeta failed, path: {}", region_id_,
            FirstIndex(), LastIndex(), path_);
        return -1;
      }
    } break;
    default:
      DINGO_LOG(FATAL) << fmt::format("[raft.log][region({}).index({}_{})] unknown entry type: {} path: {}", region_id_,
                                      FirstIndex(), LastIndex(), static_cast<int>(entry->type), path_);
      return -1;
  }
  CHECK_LE(data.length(), 1ul << 56ul);
  char header_buf[kEntryHeaderSize];
  const uint32_t meta_field = (entry->type << 24) | (checksum_type_ << 16);
  RawPacker packer(header_buf);
  packer.pack64(entry->id.term)
      .pack32(meta_field)
      .pack32((uint32_t)data.length())
      .pack32(GetChecksum(checksum_type_, data));
  packer.pack32(GetChecksum(checksum_type_, header_buf, kEntryHeaderSize - 4));
  butil::IOBuf header;
  header.append(header_buf, kEntryHeaderSize);
  const size_t to_write = header.length() + data.length();
  butil::IOBuf* pieces[2] = {&header, &data};
  size_t start = 0;
  ssize_t written = 0;
  while (written < (ssize_t)to_write) {
    const ssize_t n = butil::IOBuf::cut_multiple_into_file_descriptor(fd_, pieces + start, ARRAY_SIZE(pieces) - start);
    if (n < 0) {
      DINGO_LOG(ERROR) << fmt::format(
          "[raft.log][region({}).index({}_{})] write file failed, fd: {}, path: {} first_index: {} error: {}",
          region_id_, FirstIndex(), LastIndex(), fd_, path_, first_index_, berror());
      return -1;
    }
    written += n;
    for (; start < ARRAY_SIZE(pieces) && pieces[start]->empty(); ++start) {
    }
  }
  BAIDU_SCOPED_LOCK(mutex_);
  offset_and_term_.push_back(std::make_pair(bytes_, entry->id.term));
  last_index_.fetch_add(1, butil::memory_order_relaxed);
  bytes_ += to_write;
  unsynced_bytes_ += to_write;

  return 0;
}

int Segment::Sync(bool will_sync) {
  if (last_index_ < first_index_) {
    return 0;
  }
  // CHECK(is_open_);
  if (will_sync) {
    if (kSegmentLogSyncPolicy == SyncPolicy::kByBytes && Constant::kSegmentLogSyncPerBytes > unsynced_bytes_) {
      return 0;
    }
    unsynced_bytes_ = 0;
    return braft::raft_fsync(fd_);
  }
  return 0;
}

braft::LogEntry* Segment::Get(int64_t index) const {
  LogMeta meta;
  if (GetMeta(index, &meta) != 0) {
    return nullptr;
  }

  bool ok = true;
  braft::LogEntry* entry = nullptr;
  do {
    braft::ConfigurationPBMeta configuration_meta;
    EntryHeader header;
    butil::IOBuf data;
    if (LoadEntry(meta.offset, &header, &data, meta.length) != 0) {
      ok = false;
      break;
    }
    CHECK_EQ(meta.term, header.term);
    entry = new braft::LogEntry();
    entry->AddRef();
    switch (header.type) {
      case braft::ENTRY_TYPE_DATA:
        entry->data.swap(data);
        break;
      case braft::ENTRY_TYPE_NO_OP:
        CHECK(data.empty()) << fmt::format("[raft.log][region({}).index({}_{})] data of NO_OP must be empty",
                                           region_id_, FirstIndex(), LastIndex());
        break;
      case braft::ENTRY_TYPE_CONFIGURATION: {
        butil::Status status = parse_configuration_meta(data, entry);
        if (!status.ok()) {
          DINGO_LOG(WARNING) << fmt::format(
              "[raft.log][region({}).index({}_{})] parse ConfigurationPBMeta failed, path: {}", region_id_,
              FirstIndex(), LastIndex(), path_);
          ok = false;
          break;
        }
      } break;
      default:
        CHECK(false) << fmt::format("[raft.log][region({}).index({}_{})] unknown entry type, path: {}", region_id_,
                                    FirstIndex(), LastIndex(), path_);
        break;
    }

    if (!ok) {
      break;
    }
    entry->id.index = index;
    entry->id.term = header.term;
    entry->type = (braft::EntryType)header.type;
  } while (false);

  if (!ok && entry != nullptr) {
    entry->Release();
    entry = nullptr;
  }
  return entry;
}

int64_t Segment::GetTerm(int64_t index) const {
  LogMeta meta;
  if (GetMeta(index, &meta) != 0) {
    return 0;
  }
  return meta.term;
}

int Segment::Close(bool will_sync) {
  CHECK(is_open_);

  std::string old_path(path_);
  butil::string_appendf(&old_path, "/" SEGMENT_OPEN_PATTERN, first_index_);
  std::string new_path(path_);
  butil::string_appendf(&new_path, "/" SEGMENT_CLOSED_PATTERN, first_index_, last_index_.load());

  // TODO: optimize index memory usage by reconstruct vector
  DINGO_LOG(INFO) << fmt::format(
      "[raft.log][region({}).index({}_{})] close a full segment, raft_sync_segments: {} will_sync: {} path: {}",
      region_id_, FirstIndex(), LastIndex(), Constant::kSegmentLogSync, will_sync, new_path);
  int ret = 0;
  if (last_index_ > first_index_) {
    if (Constant::kSegmentLogSync && will_sync) {
      ret = braft::raft_fsync(fd_);
    }
  }
  if (ret == 0) {
    is_open_ = false;
    const int rc = ::rename(old_path.c_str(), new_path.c_str());
    if (rc != 0) {
      DINGO_LOG(ERROR) << fmt::format(
          "[raft.log][region({}).index({}_{})] rename failed, old_path: {} new_path: {} error: {}", region_id_,
          FirstIndex(), LastIndex(), old_path, new_path, berror());
    }

    return rc;
  }
  return ret;
}

std::string Segment::FileName() {
  if (!is_open_) {
    return butil::string_printf(SEGMENT_CLOSED_PATTERN, first_index_, last_index_.load());
  } else {
    return butil::string_printf(SEGMENT_OPEN_PATTERN, first_index_);
  }
}

static void* RunUnlink(void* arg) {
  std::string* file_path = (std::string*)arg;
  butil::Timer timer;
  timer.start();
  int ret = ::unlink(file_path->c_str());
  timer.stop();
  DINGO_LOG(DEBUG) << fmt::format("[raft.log][region(*))] unlink file, path: {} ret: {} time: {}us", *file_path, ret,
                                  timer.u_elapsed());
  delete file_path;

  return nullptr;
}

int Segment::Unlink() {
  int ret = 0;
  do {
    std::string path(path_);
    if (is_open_) {
      butil::string_appendf(&path, "/" SEGMENT_OPEN_PATTERN, first_index_);
    } else {
      butil::string_appendf(&path, "/" SEGMENT_CLOSED_PATTERN, first_index_, last_index_.load());
    }

    std::string tmp_path(path);
    tmp_path.append(".tmp");
    ret = ::rename(path.c_str(), tmp_path.c_str());
    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] rename failed, path: {} tmp_path: {}",
                                      region_id_, FirstIndex(), LastIndex(), path, tmp_path);
      break;
    }

    // start bthread to unlink
    // TODO unlink follow control
    std::string* file_path = new std::string(tmp_path);
    bthread_t tid;
    if (bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, RunUnlink, file_path) != 0) {
      RunUnlink(file_path);
    }

    DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] unlinked segment, path: {}", region_id_,
                                   FirstIndex(), LastIndex(), path);
  } while (false);

  return ret;
}

int Segment::Truncate(int64_t last_index_kept) {
  int64_t truncate_size = 0;
  int64_t first_truncate_in_offset = 0;
  std::unique_lock<bthread::Mutex> lck(mutex_);
  if (last_index_kept >= last_index_) {
    return 0;
  }
  first_truncate_in_offset = last_index_kept + 1 - first_index_;
  truncate_size = offset_and_term_[first_truncate_in_offset].first;
  DINGO_LOG(DEBUG) << fmt::format(
      "[raft.log][region({}).index({}_{})] truncating, last_index_kept:{} truncate_size: {} path: {} ", region_id_,
      FirstIndex(), LastIndex(), last_index_kept, truncate_size, path_);
  lck.unlock();

  // Truncate on a full segment need to rename back to inprogess segment again,
  // because the node may crash before truncate.
  if (!is_open_) {
    std::string old_path(path_);
    butil::string_appendf(&old_path, "/" SEGMENT_CLOSED_PATTERN, first_index_, last_index_.load());

    std::string new_path(path_);
    butil::string_appendf(&new_path, "/" SEGMENT_OPEN_PATTERN, first_index_);
    int ret = ::rename(old_path.c_str(), new_path.c_str());
    if (ret != 0) {
      DINGO_LOG(ERROR) << fmt::format(
          "[raft.log][region({}).index({}_{})] rename failed, old_path: {} new_path: {}, error: {}", region_id_,
          FirstIndex(), LastIndex(), old_path, new_path, berror());
      return ret;
    }

    is_open_ = true;
  }

  // truncate fd
  int ret = FtruncateUninterrupted(fd_, truncate_size);
  if (ret < 0) {
    return ret;
  }

  // seek fd
  off_t ret_off = ::lseek(fd_, truncate_size, SEEK_SET);
  if (ret_off < 0) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] lseek failed, fd: {} size: {} path: {}",
                                    region_id_, FirstIndex(), LastIndex(), fd_, truncate_size, path_);
    return -1;
  }

  lck.lock();
  // update memory var
  offset_and_term_.resize(first_truncate_in_offset);
  last_index_.store(last_index_kept, butil::memory_order_relaxed);
  bytes_ = truncate_size;
  return ret;
}

SegmentLogStorage::SegmentLogStorage(const std::string& path, int64_t region_id, uint64_t max_segment_size,
                                     int64_t init_vector_index_first_log_index, bool enable_sync)
    : path_(path),
      region_id_(region_id),
      max_segment_size_(max_segment_size),
      first_log_index_(1),
      last_log_index_(0),
      init_vector_index_first_log_index_(init_vector_index_first_log_index),
      vector_index_first_log_index_(init_vector_index_first_log_index),
      checksum_type_(0),
      enable_sync_(enable_sync) {
  DINGO_LOG(DEBUG) << fmt::format("[new.SegmentLogStorage][id({})]", region_id_);
}

SegmentLogStorage::SegmentLogStorage(const std::string& path, int64_t region_id, uint64_t max_segment_size,
                                     int64_t init_vector_index_first_log_index)
    : path_(path),
      region_id_(region_id),
      max_segment_size_(max_segment_size),
      first_log_index_(1),
      last_log_index_(0),
      init_vector_index_first_log_index_(init_vector_index_first_log_index),
      vector_index_first_log_index_(init_vector_index_first_log_index),
      checksum_type_(0),
      enable_sync_(FLAGS_dingo_raft_sync_log) {
  DINGO_LOG(DEBUG) << fmt::format("[new.SegmentLogStorage][id({})]", region_id_);
}

SegmentLogStorage::SegmentLogStorage()
    : first_log_index_(1), last_log_index_(0), checksum_type_(0), enable_sync_(true) {
  DINGO_LOG(DEBUG) << fmt::format("[new.SegmentLogStorage][id({})]", region_id_);
}

SegmentLogStorage::~SegmentLogStorage() {
  Helper::RemoveAllFileOrDirectory(path_);
  DINGO_LOG(DEBUG) << fmt::format("[delete.SegmentLogStorage][id({})]", region_id_);
}

int SegmentLogStorage::Init(braft::ConfigurationManager* configuration_manager) {
  butil::FilePath dir_path(path_);
  butil::File::Error e;
  if (!butil::CreateDirectoryAndGetError(dir_path, &e, true)) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][region({})] create directory failed, path: {} error: {}", region_id_,
                                    dir_path.value(), static_cast<int>(e));
    return -1;
  }

  if (butil::crc32c::IsFastCrc32Supported()) {
    checksum_type_ = static_cast<int>(CheckSumType::kCrc32);
    DINGO_LOG(INFO) << fmt::format("[raft.log][region({})] use crc32c as the checksum type of appending entries",
                                   region_id_);
  } else {
    checksum_type_ = static_cast<int>(CheckSumType::kMurmurhash32);
    DINGO_LOG(INFO) << fmt::format("[raft.log][region({})] use murmurhash32 as the checksum type of appending entries",
                                   region_id_);
  }

  int ret = 0;
  bool is_empty = false;
  do {
    ret = LoadMeta();
    if (ret != 0 && errno == ENOENT) {
      DINGO_LOG(INFO) << fmt::format("[raft.log][region({})] file not exists (ENOENT), is_empty=true, path: {}",
                                     region_id_, path_);
      is_empty = true;
    } else if (ret != 0) {
      break;
    }

    ret = ListSegments(is_empty);
    if (ret != 0) {
      break;
    }

    ret = LoadSegments(configuration_manager);
    if (ret != 0) {
      break;
    }
  } while (false);

  if (is_empty) {
    first_log_index_.store(1);
    last_log_index_.store(0);
    ret = SaveMeta(1);
  }
  return ret;
}

int64_t SegmentLogStorage::InitVectorIndexFirstLogIndex() const { return init_vector_index_first_log_index_; }

int64_t SegmentLogStorage::FirstLogIndex() { return first_log_index_.load(butil::memory_order_acquire); };

int64_t SegmentLogStorage::VectorIndexFirstLogIndex() {
  return vector_index_first_log_index_.load(std::memory_order_relaxed);
}

int64_t SegmentLogStorage::LastLogIndex() { return last_log_index_.load(butil::memory_order_acquire); }

int SegmentLogStorage::AppendEntries(const std::vector<braft::LogEntry*>& entries, braft::IOMetric* metric) {
  if (entries.empty()) {
    return 0;
  }

  DINGO_LOG(DEBUG) << fmt::format(
      "[raft.log][region({}).index({}_{})] append entries, log type: {} entry index: {}_{} entry count: {}", region_id_,
      FirstLogIndex(), LastLogIndex(), static_cast<int>(entries.front()->type), entries.front()->id.term,
      entries.front()->id.index, entries.size());

  if (last_log_index_.load(butil::memory_order_relaxed) + 1 != entries.front()->id.index) {
    DINGO_LOG(FATAL) << fmt::format(
        "[raft.log][region({}).index({}_{})] there's gap between appending entries and last_log_index_ path: {} "
        "log type: {} "
        "entry_index: {}_{}",
        region_id_, FirstLogIndex(), LastLogIndex(), path_, static_cast<int>(entries.front()->type),
        entries.front()->id.term, entries.front()->id.index);
    return -1;
  }
  std::shared_ptr<Segment> last_segment;
  int64_t now = 0;
  int64_t delta_time_us = 0;
  for (size_t i = 0; i < entries.size(); i++) {
    now = butil::cpuwide_time_us();
    braft::LogEntry* entry = entries[i];

    auto segment = OpenSegment();
    if (FLAGS_dingo_trace_append_entry_latency && metric) {
      delta_time_us = butil::cpuwide_time_us() - now;
      metric->open_segment_time_us += delta_time_us;
      g_segment_log_open_segment_latency << delta_time_us;
    }
    if (nullptr == segment) {
      return i;
    }
    int ret = segment->Append(entry);
    if (0 != ret) {
      return i;
    }
    if (FLAGS_dingo_trace_append_entry_latency && metric) {
      delta_time_us = butil::cpuwide_time_us() - now;
      metric->append_entry_time_us += delta_time_us;
      g_segment_log_append_entry_latency << delta_time_us;
    }
    last_log_index_.fetch_add(1, butil::memory_order_release);
    last_segment = segment;
  }
  now = butil::cpuwide_time_us();
  last_segment->Sync(enable_sync_);
  if (FLAGS_dingo_trace_append_entry_latency && metric) {
    delta_time_us = butil::cpuwide_time_us() - now;
    metric->sync_segment_time_us += delta_time_us;
    g_segment_log_sync_segment_latency << delta_time_us;
  }
  return entries.size();
}

int SegmentLogStorage::AppendEntry(const braft::LogEntry* entry) {
  DINGO_LOG(DEBUG) << fmt::format("[raft.log][region({}).index({}_{})] append entry, entry index: {}", region_id_,
                                  FirstLogIndex(), LastLogIndex(), entry->id.index);

  auto segment = OpenSegment();
  if (nullptr == segment) {
    return EIO;
  }
  int ret = segment->Append(entry);
  if (ret != 0 && ret != EEXIST) {
    return ret;
  }
  if (EEXIST == ret && entry->id.term != GetTerm(entry->id.index)) {
    return EINVAL;
  }
  last_log_index_.fetch_add(1, butil::memory_order_release);

  return segment->Sync(enable_sync_);
}

braft::LogEntry* SegmentLogStorage::GetEntry(const int64_t index) {
  std::shared_ptr<Segment> segment = GetSegment(index);
  if (segment == nullptr) {
    return nullptr;
  }
  return segment->Get(index);
}

std::vector<std::shared_ptr<LogEntry>> SegmentLogStorage::GetEntrys(uint64_t begin_index, uint64_t end_index) {
  auto segments = GetSegments(begin_index, end_index);
  if (segments.empty()) {
    return {};
  }

  std::vector<std::shared_ptr<LogEntry>> log_entrys;
  for (auto& segment : segments) {
    for (int64_t i = segment->FirstIndex(); i <= segment->LastIndex(); ++i) {
      if (i < begin_index || i > end_index) {
        continue;
      }
      auto* log_entry = segment->Get(i);
      if (log_entry != nullptr) {
        if (log_entry->type == braft::ENTRY_TYPE_DATA) {
          auto tmp_log_entry = std::make_shared<LogEntry>();
          tmp_log_entry->term = log_entry->id.term;
          tmp_log_entry->index = log_entry->id.index;
          tmp_log_entry->data.swap(log_entry->data);
          log_entrys.push_back(tmp_log_entry);
        }
      }
    }
  }

  return log_entrys;
}

bool SegmentLogStorage::HasSpecificLog(uint64_t begin_index, uint64_t end_index, MatchFuncer matcher) {
  auto segments = GetSegments(begin_index, end_index);
  if (segments.empty()) {
    return false;
  }

  for (auto& segment : segments) {
    for (int64_t i = segment->FirstIndex(); i <= segment->LastIndex(); ++i) {
      if (i < begin_index || i > end_index) {
        continue;
      }
      auto* log_entry = segment->Get(i);
      if (log_entry != nullptr) {
        if (log_entry->type == braft::ENTRY_TYPE_DATA) {
          LogEntry tmp_log_entry;
          tmp_log_entry.type = LogEntryType::kEntryTypeData;
          tmp_log_entry.term = log_entry->id.term;
          tmp_log_entry.index = log_entry->id.index;
          tmp_log_entry.data.swap(log_entry->data);
          if (matcher(tmp_log_entry)) {
            return true;
          }
        } else if (log_entry->type == braft::ENTRY_TYPE_CONFIGURATION) {
          LogEntry tmp_log_entry;
          tmp_log_entry.type = LogEntryType::kEntryTypeConfiguration;
          tmp_log_entry.term = log_entry->id.term;
          tmp_log_entry.index = log_entry->id.index;
          if (matcher(tmp_log_entry)) {
            return true;
          }
        }
      }
    }
  }

  return false;
}

int64_t SegmentLogStorage::GetTerm(const int64_t index) {
  std::shared_ptr<Segment> segment = GetSegment(index);
  return (segment == nullptr) ? 0 : segment->GetTerm(index);
}

void SegmentLogStorage::PopSegments(int64_t first_index_kept, std::vector<std::shared_ptr<Segment>>& poppeds) {
  poppeds.reserve(32);
  BAIDU_SCOPED_LOCK(mutex_);

  for (SegmentMap::iterator it = segments_.begin(); it != segments_.end();) {
    std::shared_ptr<Segment>& segment = it->second;
    if (segment->LastIndex() < first_index_kept) {
      poppeds.push_back(segment);
      segments_.erase(it++);
    } else {
      return;
    }
  }

  if (open_segment_) {
    if (open_segment_->LastIndex() < first_index_kept) {
      poppeds.push_back(open_segment_);
      open_segment_ = nullptr;
      last_log_index_.store(FirstLogIndex() - 1);
    }
  }
}

void SegmentLogStorage::SetFirstAndLastLogIndex(int64_t first_index_kept) {
  BAIDU_SCOPED_LOCK(mutex_);

  first_log_index_.store(first_index_kept, butil::memory_order_release);
  if (!open_segment_) {
    last_log_index_.store(FirstLogIndex() - 1);
  }
}

int SegmentLogStorage::TruncatePrefix(int64_t first_index_kept) {
  // segment files
  if (first_log_index_.load(butil::memory_order_acquire) >= first_index_kept) {
    DINGO_LOG(INFO) << fmt::format(
        "[raft.log][region({}).index({}_{})] truncate prefix, nothing happen since first_index_kept: {}", region_id_,
        FirstLogIndex(), LastLogIndex(), first_index_kept);
    return 0;
  }

  // NOTE: truncate_prefix is not important, as it has nothing to do with
  // consensus. We try to save meta on the disk first to make sure even if
  // the deleting fails or the process crashes (which is unlikely to happen).
  // The new process would see the latest `first_log_index'
  if (SaveMeta(first_index_kept) != 0) {  // NOTE
    DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] save meta failed, path: {}", region_id_,
                                    FirstLogIndex(), LastLogIndex(), path_);
    return -1;
  }
  SetFirstAndLastLogIndex(first_index_kept);

  DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] truncate prefix, first_index_kept: {}",
                                 region_id_, FirstLogIndex(), LastLogIndex(), first_index_kept);

  TruncateActualPrefixLog();

  return 0;
}

int SegmentLogStorage::TruncateVectorIndexPrefix(int64_t first_index_kept) {
  if (first_index_kept <= vector_index_first_log_index_.load(std::memory_order_relaxed)) {
    DINGO_LOG(WARNING) << fmt::format(
        "[raft.log][region({}).index({}_{})] truncate vector index prefix, must greater vector_index_first_log_index: "
        "{} first_index_kept: {}",
        region_id_, FirstLogIndex(), LastLogIndex(), vector_index_first_log_index_.load(std::memory_order_relaxed),
        first_index_kept);

    return 0;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[raft.log][region({}).index({}_{})] truncate vector index prefix, first_index_kept: {}", region_id_,
      FirstLogIndex(), LastLogIndex(), first_index_kept);

  vector_index_first_log_index_.store(first_index_kept, std::memory_order_relaxed);

  if (SaveMeta(first_log_index_.load(std::memory_order_relaxed)) != 0) {
    return -1;
  }

  return 0;
}

int64_t SegmentLogStorage::GetMinFirstLogIndex() {
  return std::min(first_log_index_.load(butil::memory_order_relaxed),
                  vector_index_first_log_index_.load(butil::memory_order_relaxed));
}

void SegmentLogStorage::TruncateActualPrefixLog() {
  int64_t truncate_log_index = GetMinFirstLogIndex();

  std::vector<std::shared_ptr<Segment>> poppeds;
  PopSegments(truncate_log_index, poppeds);
  DINGO_LOG(INFO) << fmt::format(
      "[raft.log][region({}).index({}_{})] truncate prefix log, min_log_index: {} delete file num: {}", region_id_,
      FirstLogIndex(), LastLogIndex(), truncate_log_index, poppeds.size());

  for (auto& popped : poppeds) {
    popped->Unlink();
    popped = nullptr;
  }
}

std::shared_ptr<Segment> SegmentLogStorage::PopSegmentsFromBack(int64_t last_index_kept,
                                                                std::vector<std::shared_ptr<Segment>>& poppeds) {
  poppeds.clear();
  poppeds.reserve(32);

  BAIDU_SCOPED_LOCK(mutex_);
  last_log_index_.store(last_index_kept, butil::memory_order_release);
  if (open_segment_ != nullptr) {
    if (open_segment_->FirstIndex() <= last_index_kept) {
      return open_segment_;
    }
    poppeds.push_back(open_segment_);
    open_segment_ = nullptr;
  }

  for (SegmentMap::reverse_iterator it = segments_.rbegin(); it != segments_.rend(); ++it) {
    if (it->second->FirstIndex() <= last_index_kept) {
      // Not return as we need to maintain segments_ at the end of this
      // routine
      break;
    }
    poppeds.push_back(it->second);
    // XXX: C++03 not support erase reverse_iterator
  }

  for (auto& popped : poppeds) {
    segments_.erase(popped->FirstIndex());
  }
  if (segments_.rbegin() != segments_.rend()) {
    return segments_.rbegin()->second;
  } else {
    // all the logs have been cleared, the we move first_log_index_ to the
    // next index
    first_log_index_.store(last_index_kept + 1, butil::memory_order_release);
  }

  return nullptr;
}

int SegmentLogStorage::TruncateSuffix(int64_t last_index_kept) {
  DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] truncate suffix last_index_kept: {}", region_id_,
                                 FirstLogIndex(), LastLogIndex(), last_index_kept);
  // segment files
  std::vector<std::shared_ptr<Segment>> poppeds;
  std::shared_ptr<Segment> last_segment = PopSegmentsFromBack(last_index_kept, poppeds);
  bool truncate_last_segment = false;
  int ret = -1;

  if (last_segment != nullptr) {
    if (first_log_index_.load(butil::memory_order_relaxed) <= last_log_index_.load(butil::memory_order_relaxed)) {
      truncate_last_segment = true;
    } else {
      // trucate_prefix() and truncate_suffix() to discard entire logs
      BAIDU_SCOPED_LOCK(mutex_);
      poppeds.push_back(last_segment);
      segments_.erase(last_segment->FirstIndex());
      if (open_segment_) {
        CHECK(open_segment_.get() == last_segment.get());
        open_segment_ = nullptr;
      }
    }
  }

  // The truncate suffix order is crucial to satisfy log matching property of raft
  // log must be truncated from back to front.
  for (auto& popped : poppeds) {
    ret = popped->Unlink();
    if (ret != 0) {
      return ret;
    }
    popped = nullptr;
  }
  if (truncate_last_segment) {
    bool closed = !last_segment->IsOpen();
    ret = last_segment->Truncate(last_index_kept);
    if (ret == 0 && closed && last_segment->IsOpen()) {
      BAIDU_SCOPED_LOCK(mutex_);
      CHECK(!open_segment_);
      segments_.erase(last_segment->FirstIndex());
      open_segment_.swap(last_segment);
    }
  }

  return ret;
}

int SegmentLogStorage::Reset(int64_t next_log_index) {
  DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] reset log, next_log_index: {}", region_id_,
                                 FirstLogIndex(), LastLogIndex(), next_log_index);
  if (next_log_index <= 0) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] invalid next_log_index: {} path: {}",
                                    region_id_, FirstLogIndex(), LastLogIndex(), next_log_index, path_);
    return EINVAL;
  }
  std::vector<std::shared_ptr<Segment>> poppeds;
  std::unique_lock<bthread::Mutex> lck(mutex_);
  poppeds.reserve(segments_.size());
  for (SegmentMap::const_iterator it = segments_.begin(); it != segments_.end(); ++it) {
    poppeds.push_back(it->second);
  }
  segments_.clear();
  if (open_segment_) {
    poppeds.push_back(open_segment_);
    open_segment_ = nullptr;
  }
  first_log_index_.store(next_log_index, butil::memory_order_relaxed);
  vector_index_first_log_index_.store(next_log_index, butil::memory_order_relaxed);
  last_log_index_.store(next_log_index - 1, butil::memory_order_relaxed);
  lck.unlock();
  // NOTE: see the comments in truncate_prefix
  if (SaveMeta(next_log_index) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] save meta failed, path: {}", region_id_,
                                    FirstLogIndex(), LastLogIndex(), path_);
    return -1;
  }
  for (auto& popped : poppeds) {
    popped->Unlink();
    popped = nullptr;
  }
  return 0;
}

int SegmentLogStorage::ListSegments(bool is_empty) {
  butil::DirReaderPosix dir_reader(path_.c_str());
  if (!dir_reader.IsValid()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[raft.log][region({}).index({}_{})] directory reader failed, maybe NOEXIST or PERMISSION, path: {}.",
        region_id_, FirstLogIndex(), LastLogIndex(), path_);
    return -1;
  }

  // restore segment meta
  while (dir_reader.Next()) {
    // unlink unneed segments and unfinished unlinked segments
    if ((is_empty && 0 == strncmp(dir_reader.name(), "log_", strlen("log_"))) ||
        (0 == strncmp(dir_reader.name() + (strlen(dir_reader.name()) - strlen(".tmp")), ".tmp", strlen(".tmp")))) {
      std::string segment_path(path_);
      segment_path.append("/");
      segment_path.append(dir_reader.name());
      ::unlink(segment_path.c_str());

      DINGO_LOG(WARNING) << fmt::format("[raft.log][region({}).index({}_{})] unlink unused segment, path: {}",
                                        region_id_, FirstLogIndex(), LastLogIndex(), segment_path);

      continue;
    }

    int match = 0;
    int64_t first_index = 0;
    int64_t last_index = 0;
    match = sscanf(dir_reader.name(), SEGMENT_CLOSED_PATTERN, &first_index, &last_index);  // NOLINT
    if (match == 2) {
      DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] restore closed segment, path: {}", region_id_,
                                     first_index, last_index, path_);
      segments_[first_index] = std::make_shared<Segment>(region_id_, path_, first_index, last_index, checksum_type_);
      continue;
    }

    match = sscanf(dir_reader.name(), SEGMENT_OPEN_PATTERN, &first_index);  // NOLINT
    if (match == 1) {
      DINGO_LOG(DEBUG) << fmt::format("[raft.log][region({})] restore open segment, first_index: {} path: {}",
                                      region_id_, first_index, path_);
      if (!open_segment_) {
        open_segment_ = std::make_shared<Segment>(region_id_, path_, first_index, checksum_type_);
        continue;
      } else {
        DINGO_LOG(WARNING) << fmt::format("[raft.log][region({})] open segment conflict, first_index: {} path: {}",
                                          region_id_, first_index, path_);
        return -1;
      }
    }
  }

  // check segment
  int64_t last_log_index = -1;
  int64_t min_first_log_index = GetMinFirstLogIndex();
  SegmentMap::iterator it;
  for (it = segments_.begin(); it != segments_.end();) {
    Segment* segment = it->second.get();
    if (segment->FirstIndex() > segment->LastIndex()) {
      DINGO_LOG(WARNING) << fmt::format("[raft.log][region({}).index({}_{})] closed segment is bad, path: {}",
                                        region_id_, segment->FirstIndex(), segment->LastIndex(), path_);
      return -1;
    } else if (last_log_index != -1 && segment->FirstIndex() != last_log_index + 1) {
      DINGO_LOG(WARNING) << fmt::format("[raft.log][region({}).index({}_{})] closed segment not in order, path: ",
                                        region_id_, segment->FirstIndex(), last_log_index, path_);
      return -1;
    } else if (last_log_index == -1 && first_log_index_.load(butil::memory_order_acquire) < segment->FirstIndex()) {
      DINGO_LOG(WARNING) << fmt::format(
          "[raft.log][region({}).index({}_{})] closed segment has hole, first_log_index: {} path: {}", region_id_,
          segment->FirstIndex(), segment->LastIndex(), first_log_index_.load(butil::memory_order_relaxed), path_);
      return -1;
    } else if (last_log_index == -1 && min_first_log_index > segment->LastIndex()) {
      DINGO_LOG(WARNING) << fmt::format(
          "[raft.log][region({}).index({}_{})] closed segment need discard, first_log_index: {} path: {}", region_id_,
          segment->FirstIndex(), segment->LastIndex(), first_log_index_.load(butil::memory_order_relaxed), path_);
      segment->Unlink();
      segments_.erase(it++);
      continue;
    }

    last_log_index = segment->LastIndex();
    ++it;
  }
  if (open_segment_) {
    if (last_log_index == -1 && first_log_index_.load(butil::memory_order_relaxed) < open_segment_->FirstIndex()) {
      DINGO_LOG(WARNING) << fmt::format(
          "[raft.log][region({}).index({}_{})] open segment has hole, first_log_index: {} path: {}", region_id_,
          open_segment_->FirstIndex(), 0, FirstLogIndex(), path_);
    } else if (last_log_index != -1 && open_segment_->FirstIndex() != last_log_index + 1) {
      DINGO_LOG(WARNING) << fmt::format(
          "[raft.log][region({}).index({}_{})] open segment has hole, first_log_index: {} path: {}", region_id_,
          open_segment_->FirstIndex(), 0, FirstLogIndex(), path_);
    }
    CHECK_LE(last_log_index, open_segment_->LastIndex());
  }

  return 0;
}

int SegmentLogStorage::LoadSegments(braft::ConfigurationManager* configuration_manager) {
  int ret = 0;

  // closed segments
  SegmentMap::iterator it;
  for (it = segments_.begin(); it != segments_.end(); ++it) {
    Segment* segment = it->second.get();
    if (segment->LastIndex() < first_log_index_.load(std::memory_order_relaxed)) {
      continue;
    }
    DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] load closed segment, path: {}", region_id_,
                                   segment->FirstIndex(), segment->LastIndex(), path_);
    ret = segment->Load(configuration_manager);
    if (ret != 0) {
      return ret;
    }
    last_log_index_.store(segment->LastIndex(), butil::memory_order_release);
  }

  // open segment
  if (open_segment_) {
    DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] load open segment, path: {}", region_id_,
                                   open_segment_->FirstIndex(), open_segment_->LastIndex(), path_);
    ret = open_segment_->Load(configuration_manager);
    if (ret != 0) {
      return ret;
    }
    if (first_log_index_.load() > open_segment_->LastIndex()) {
      DINGO_LOG(WARNING) << fmt::format(
          "[raft.log][region({}).index({}_{})] open segment need discard, first_log_index: {} path: {}", region_id_,
          open_segment_->FirstIndex(), open_segment_->LastIndex(), first_log_index_.load(), path_);
      open_segment_->Unlink();
      open_segment_ = nullptr;
    } else {
      last_log_index_.store(open_segment_->LastIndex(), butil::memory_order_release);
    }
  }
  if (last_log_index_ == 0) {
    last_log_index_ = first_log_index_ - 1;
  }
  return 0;
}

int SegmentLogStorage::SaveMeta(int64_t log_index) {
  butil::Timer timer;
  timer.start();

  std::string meta_path(path_);
  meta_path.append("/" SEGMENT_META_FILE);

  pb::store_internal::LogMeta meta;
  meta.set_first_log_index(log_index);
  meta.set_vector_index_first_log_index(vector_index_first_log_index_.load(std::memory_order_relaxed));
  braft::ProtoBufFile pb_file(meta_path);
  int ret = pb_file.save(&meta, braft::raft_sync_meta());
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] save meta failed.", region_id_,
                                    FirstLogIndex(), LastLogIndex());
  }

  timer.stop();

  DINGO_LOG(INFO) << fmt::format(
      "[raft.log][region({}).index({}_{})] save meta finish, log_index: {} elapsed time: {}us", region_id_,
      FirstLogIndex(), LastLogIndex(), log_index, timer.u_elapsed());

  return ret;
}

int SegmentLogStorage::LoadMeta() {
  butil::Timer timer;
  timer.start();

  std::string meta_path(path_);
  meta_path.append("/" SEGMENT_META_FILE);

  // check if meta_path file is exists
  if (!std::filesystem::exists(meta_path)) {
    DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] meta file is not exists, path: {}", region_id_,
                                   FirstLogIndex(), LastLogIndex(), meta_path);
    return -1;
  }

  braft::ProtoBufFile pb_file(meta_path);
  pb::store_internal::LogMeta meta;
  if (0 != pb_file.load(&meta)) {
    if (errno != ENOENT) {
      DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] load meta failed", region_id_,
                                      FirstLogIndex(), LastLogIndex());
    }
    return -1;
  }

  first_log_index_.store(meta.first_log_index());
  vector_index_first_log_index_.store(meta.vector_index_first_log_index());

  timer.stop();

  DINGO_LOG(INFO) << fmt::format(
      "[raft.log][region({}).index({}_{})] load meta finish, vector_index_first_log_index: {} elapsed time: {}us",
      region_id_, FirstLogIndex(), LastLogIndex(), meta.vector_index_first_log_index(), timer.u_elapsed());

  return 0;
}

std::shared_ptr<Segment> SegmentLogStorage::OpenSegment() {
  std::shared_ptr<Segment> prev_open_segment;
  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (!open_segment_) {
      open_segment_ = std::make_shared<Segment>(region_id_, path_, LastLogIndex() + 1, checksum_type_);
      if (open_segment_->Create() != 0) {
        open_segment_ = nullptr;
        return nullptr;
      }
    }
    if (open_segment_->Bytes() > max_segment_size_) {
      segments_[open_segment_->FirstIndex()] = open_segment_;
      prev_open_segment.swap(open_segment_);
    }
  }

  do {
    if (prev_open_segment) {
      if (prev_open_segment->Close(enable_sync_) == 0) {
        BAIDU_SCOPED_LOCK(mutex_);
        open_segment_ = std::make_shared<Segment>(region_id_, path_, LastLogIndex() + 1, checksum_type_);
        if (open_segment_->Create() == 0) {
          // success
          break;
        }
      }

      DINGO_LOG(ERROR) << fmt::format("[raft.log][region({}).index({}_{})] open segment failed, path: {}", region_id_,
                                      FirstLogIndex(), LastLogIndex(), path_);
      // Failed, revert former changes
      BAIDU_SCOPED_LOCK(mutex_);
      segments_.erase(prev_open_segment->FirstIndex());
      open_segment_.swap(prev_open_segment);
      return nullptr;
    }
  } while (false);

  return open_segment_;
}

std::shared_ptr<Segment> SegmentLogStorage::GetSegment(int64_t index) {
  BAIDU_SCOPED_LOCK(mutex_);
  int64_t first_index = FirstLogIndex();
  int64_t last_index = LastLogIndex();
  if (first_index == last_index + 1) {
    return nullptr;
  }
  if (index < first_index || index > last_index + 1) {
    DINGO_LOG(WARNING) << fmt::format(
        "[raft.log][region({}).index({}_{})] attempted to access entry {} outside of log.", region_id_, FirstLogIndex(),
        LastLogIndex(), index);
    return nullptr;
  } else if (index == last_index + 1) {
    return nullptr;
  }

  if (open_segment_ != nullptr && index >= open_segment_->FirstIndex()) {
    return open_segment_;
  } else {
    CHECK(!segments_.empty());
    SegmentMap::iterator it = segments_.upper_bound(index);
    SegmentMap::iterator saved_it = it;
    --it;
    CHECK(it != saved_it);
    return it->second;
  }

  return nullptr;
}

std::vector<std::shared_ptr<Segment>> SegmentLogStorage::GetSegments(uint64_t begin_index, uint64_t end_index) {
  BAIDU_SCOPED_LOCK(mutex_);
  uint64_t first_index = FirstLogIndex();
  uint64_t last_index = LastLogIndex();
  if (first_index == last_index + 1) {
    return {};
  }

  if (end_index < first_index || begin_index > last_index) {
    DINGO_LOG(WARNING) << fmt::format(
        "[raft.log][region({}).index({}_{})] attempted to access entry {}-{} outside of log", region_id_, first_index,
        last_index, begin_index, end_index);
    return {};
  }

  std::vector<std::shared_ptr<Segment>> segments;
  for (auto& [_, segment] : segments_) {
    if (begin_index <= segment->LastIndex() || segment->FirstIndex() <= end_index) {
      segments.push_back(segment);
    }
  }

  if (begin_index <= open_segment_->LastIndex() || open_segment_->FirstIndex() <= end_index) {
    segments.push_back(open_segment_);
  }

  return segments;
}

void SegmentLogStorage::ListFiles(std::vector<std::string>* seg_files) {
  BAIDU_SCOPED_LOCK(mutex_);
  seg_files->push_back(SEGMENT_META_FILE);
  for (auto& [_, segment] : segments_) {
    seg_files->push_back(segment->FileName());
  }
  if (open_segment_) {
    seg_files->push_back(open_segment_->FileName());
  }
}

void SegmentLogStorage::Sync() {
  std::vector<std::shared_ptr<Segment>> segments;
  {
    BAIDU_SCOPED_LOCK(mutex_);
    for (auto& [_, segment] : segments_) {
      segments.push_back(segment);
    }
  }

  for (auto& segment : segments) {
    segment->Sync(true);
  }
}

butil::Status SegmentLogStorage::GcInstance(const std::string& uri) {
  butil::Status status;
  if (braft::gc_dir(uri) != 0) {
    DINGO_LOG(WARNING) << fmt::format("[raft.log][region({}).index({}_{})] gc log storage failed, path: {}", region_id_,
                                      FirstLogIndex(), LastLogIndex(), uri);
    status.set_error(EINVAL, "gc log storage failed path %s", uri.c_str());
    return status;
  }
  DINGO_LOG(INFO) << fmt::format("[raft.log][region({}).index({}_{})] gc log storage success, path: {}", region_id_,
                                 FirstLogIndex(), LastLogIndex(), uri);
  return status;
}

}  // namespace dingodb

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

#ifndef DINGODB_SEGMENT_LOG_STORAGE_H_
#define DINGODB_SEGMENT_LOG_STORAGE_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "braft/log_entry.h"
#include "braft/storage.h"
#include "braft/util.h"
#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "common/helper.h"
#include "common/logging.h"

namespace dingodb {

struct LogEntry {
  int64_t index;
  int64_t term;

  butil::IOBuf data;
};

class BAIDU_CACHELINE_ALIGNMENT Segment {
 public:
  Segment(const std::string& path, const int64_t first_index, int checksum_type)
      : path_(path),
        bytes_(0),
        unsynced_bytes_(0),
        fd_(-1),
        is_open_(true),
        first_index_(first_index),
        last_index_(first_index - 1),
        checksum_type_(checksum_type) {}
  Segment(const std::string& path, const int64_t first_index, const int64_t last_index, int checksum_type)
      : path_(path),
        bytes_(0),
        unsynced_bytes_(0),
        fd_(-1),
        is_open_(false),
        first_index_(first_index),
        last_index_(last_index),
        checksum_type_(checksum_type) {}
  ~Segment() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  struct EntryHeader;

  // Create open segment
  int Create();

  // load open or closed segment
  // open fd, load index, truncate uncompleted entry
  int Load(braft::ConfigurationManager* configuration_manager);

  // serialize entry, and append to open segment
  int Append(const braft::LogEntry* entry);

  // get entry by index
  braft::LogEntry* Get(int64_t index) const;

  // get entry's term by index
  int64_t GetTerm(int64_t index) const;

  // close open segment
  int Close(bool will_sync = true);

  // sync open segment
  int Sync(bool will_sync);

  // unlink segment
  int Unlink();

  // truncate segment to last_index_kept
  int Truncate(int64_t last_index_kept);

  bool IsOpen() const { return is_open_; }

  int64_t Bytes() const { return bytes_; }

  int64_t FirstIndex() const { return first_index_; }

  int64_t LastIndex() const { return last_index_.load(butil::memory_order_consume); }

  std::string FileName();

 private:
  struct LogMeta {
    off_t offset;
    size_t length;
    int64_t term;
  };

  int LoadEntry(off_t offset, EntryHeader* head, butil::IOBuf* body, size_t size_hint) const;
  int GetMeta(int64_t index, LogMeta* meta) const;
  int TruncateMetaAndGetLast(int64_t last);

  std::string path_;
  int64_t bytes_;
  int64_t unsynced_bytes_;
  mutable bthread::Mutex mutex_;

  int fd_;
  bool is_open_;
  const int64_t first_index_;
  butil::atomic<int64_t> last_index_;
  int checksum_type_;
  std::vector<std::pair<int64_t /*offset*/, int64_t /*term*/>> offset_and_term_;
};

// LogStorage use segmented append-only file, all data in disk, all index in memory.
// append one log entry, only cause one disk write, every disk write will call fsync().
//
// SegmentLog layout:
//      log_meta: record start_log
//      log_000001-0001000: closed segment
//      log_inprogress_0001001: open segment
class SegmentLogStorage {
 public:
  using SegmentMap = std::map<int64_t, std::shared_ptr<Segment>>;

  explicit SegmentLogStorage(const std::string& path, uint64_t region_id, bool enable_truncate_control = false,
                             bool enable_sync = true)
      : path_(path),
        region_id_(region_id),
        first_log_index_(1),
        last_log_index_(0),
        checksum_type_(0),
        enable_truncate_control_(enable_truncate_control),
        enable_sync_(enable_sync) {}

  SegmentLogStorage() : first_log_index_(1), last_log_index_(0), checksum_type_(0), enable_sync_(true) {}

  ~SegmentLogStorage();

  // init logstorage, check consistency and integrity
  int Init(braft::ConfigurationManager* configuration_manager);

  uint64_t RegionId() const { return region_id_; }

  // first log index in log
  int64_t FirstLogIndex() { return first_log_index_.load(butil::memory_order_acquire); }

  // last log index in log
  int64_t LastLogIndex();

  // get logentry by index
  braft::LogEntry* GetEntry(int64_t index);

  // [begin_index, end_index]
  std::vector<std::shared_ptr<LogEntry>> GetEntrys(int64_t begin_index, int64_t end_index);

  // get logentry's term by index
  int64_t GetTerm(int64_t index);

  // append entry to log
  int AppendEntry(const braft::LogEntry* entry);

  // append entries to log and update IOMetric, return success append number
  int AppendEntries(const std::vector<braft::LogEntry*>& entries, braft::IOMetric* metric);

  // delete logs from storage's head, [1, first_index_kept) will be discarded
  int TruncatePrefix(int64_t first_index_kept);

  // delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
  int TruncateSuffix(int64_t last_index_kept);

  int Reset(int64_t next_log_index);

  butil::Status GcInstance(const std::string& uri) const;

  SegmentMap Segments() {
    BAIDU_SCOPED_LOCK(mutex_);
    return segments_;
  }

  void ListFiles(std::vector<std::string>* seg_files);

  void Sync();

  void SetVectorIndexTruncateLogIndex(int64_t truncate_log_index) {
    auto it = truncate_log_indexs_.find("VectorIndex");
    if (it != truncate_log_indexs_.end()) {
      if (truncate_log_index > it->second.load(std::memory_order_relaxed)) {
        it->second.store(truncate_log_index, std::memory_order_relaxed);
      }
    }
  }

 private:
  std::shared_ptr<Segment> OpenSegment();
  int SaveMeta(int64_t log_index);
  int LoadMeta();
  int ListSegments(bool is_empty);
  int LoadSegments(braft::ConfigurationManager* configuration_manager);
  std::shared_ptr<Segment> GetSegment(int64_t log_index);
  std::vector<std::shared_ptr<Segment>> GetSegments(int64_t begin_index, int64_t end_index);
  void PopSegments(int64_t first_index_kept, std::vector<std::shared_ptr<Segment>>& poppeds);
  std::shared_ptr<Segment> PopSegmentsFromBack(int64_t last_index_kept, std::vector<std::shared_ptr<Segment>>& poppeds);

  std::string path_;
  uint64_t region_id_;

  butil::atomic<int64_t> first_log_index_;
  butil::atomic<int64_t> last_log_index_;

  // Control truncate log.
  bool enable_truncate_control_;
  std::map<std::string, std::atomic<int64_t>> truncate_log_indexs_;

  bthread::Mutex mutex_;
  SegmentMap segments_;

  std::shared_ptr<Segment> open_segment_;

  int checksum_type_;
  bool enable_sync_;
};

// Wrap SegmentLogStorage for inject braft
class SegmentLogStorageWrapper : public braft::LogStorage {
 public:
  explicit SegmentLogStorageWrapper(std::shared_ptr<SegmentLogStorage> log_storage)
      : log_storage_(log_storage), region_id_(log_storage->RegionId()) {}
  ~SegmentLogStorageWrapper() override = default;

  // init logstorage, check consistency and integrity
  virtual int init(braft::ConfigurationManager* configuration_manager) {
    return log_storage_->Init(configuration_manager);
  }

  // first log index in log
  virtual int64_t first_log_index() { return log_storage_->FirstLogIndex(); }

  // last log index in log
  virtual int64_t last_log_index() { return log_storage_->LastLogIndex(); }

  // get logentry by index
  virtual braft::LogEntry* get_entry(const int64_t index) { return log_storage_->GetEntry(index); }

  // get logentry's term by index
  virtual int64_t get_term(const int64_t index) { return log_storage_->GetTerm(index); }

  // append entry to log
  int append_entry(const braft::LogEntry* entry) { return log_storage_->AppendEntry(entry); }

  // append entries to log and update IOMetric, return success append number
  virtual int append_entries(const std::vector<braft::LogEntry*>& entries, braft::IOMetric* metric) {
    return log_storage_->AppendEntries(entries, metric);
  }

  // delete logs from storage's head, [1, first_index_kept) will be discarded
  virtual int truncate_prefix(const int64_t first_index_kept) { return log_storage_->TruncatePrefix(first_index_kept); }

  // delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
  virtual int truncate_suffix(const int64_t last_index_kept) { return log_storage_->TruncateSuffix(last_index_kept); }

  virtual int reset(const int64_t next_log_index) { return log_storage_->Reset(next_log_index); }

  LogStorage* new_instance(const std::string& uri) const {
    DINGO_LOG(INFO) << "New segment log storage instance " << region_id_;
    auto log_storage = std::make_shared<SegmentLogStorage>(uri, region_id_);
    return new SegmentLogStorageWrapper(log_storage);
  }

  butil::Status gc_instance(const std::string& uri) const { return log_storage_->GcInstance(uri); }

  void list_files(std::vector<std::string>* seg_files) { log_storage_->ListFiles(seg_files); }

  void sync() { log_storage_->Sync(); }

 private:
  uint64_t region_id_;
  std::shared_ptr<SegmentLogStorage> log_storage_;
};

}  //  namespace dingodb

#endif  // DINGODB_SEGMENT_LOG_STORAGE_H_

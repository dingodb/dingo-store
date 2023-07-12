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

#include "common/file_reader.h"

#include "common/logging.h"
#include "fmt/core.h"

namespace dingodb {

LocalDirReader::~LocalDirReader() {
  if (current_file_) {
    current_file_->close();
    delete current_file_;
    current_file_ = nullptr;
  }
  fs_->close_snapshot(path_);
}

bool LocalDirReader::Open() { return fs_->open_snapshot(path_); }

int LocalDirReader::ReadFile(butil::IOBuf* out, const std::string& filename, off_t offset, size_t max_count,
                             size_t* read_count, bool* is_eof) const {
  return ReadFileWithMeta(out, filename, nullptr, offset, max_count, read_count, is_eof);
}

int LocalDirReader::ReadFileWithMeta(butil::IOBuf* out, const std::string& filename,
                                     google::protobuf::Message* file_meta, off_t offset, size_t max_count,
                                     size_t* read_count, bool* is_eof) const {
  std::unique_lock<braft::raft_mutex_t> lck(mutex_);
  if (is_reading_) {
    // Just let follower retry, if there already a reading request in process.
    lck.unlock();
    DINGO_LOG(INFO) << "A courrent read file is in process, path: " << path_;
    return EAGAIN;
  }
  int ret = EINVAL;
  if (filename != current_filename_) {
    if (!eof_reached_ || offset != 0) {
      lck.unlock();
      DINGO_LOG(INFO) << fmt::format("Out of order read request, path: {} filename: {} offset: {} max_count: {}", path_,
                                     filename, offset, max_count);
      return EINVAL;
    }
    if (current_file_) {
      current_file_->close();
      delete current_file_;
      current_file_ = nullptr;
      current_filename_.clear();
    }
    std::string file_path(path_ + "/" + filename);
    butil::File::Error e;
    braft::FileAdaptor* file = fs_->open(file_path, O_RDONLY | O_CLOEXEC, file_meta, &e);
    if (!file) {
      return braft::file_error_to_os_error(e);
    }
    current_filename_ = filename;
    current_file_ = file;
    eof_reached_ = false;
  }
  is_reading_ = true;
  lck.unlock();

  do {
    butil::IOPortal buf;
    ssize_t nread = current_file_->read(&buf, offset, max_count);
    if (nread < 0) {
      ret = EIO;
      break;
    }
    ret = 0;
    *read_count = nread;
    *is_eof = false;
    if ((size_t)nread < max_count) {
      *is_eof = true;
    } else {
      ssize_t size = current_file_->size();
      if (size < 0) {
        return EIO;
      }
      if (size == ssize_t(offset + max_count)) {
        *is_eof = true;
      }
    }
    out->swap(buf);
  } while (false);

  lck.lock();
  is_reading_ = false;
  if (!ret) {
    eof_reached_ = *is_eof;
  }
  return ret;
}

}  // namespace dingodb
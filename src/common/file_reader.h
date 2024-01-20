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

#ifndef DINGODB_COMMON_FILE_READER_H_
#define DINGODB_COMMON_FILE_READER_H_

#include <string>

#include "braft/file_system_adaptor.h"
#include "butil/iobuf.h"

namespace dingodb {

class FileReader {
 protected:
  FileReader() = default;

 public:
  virtual ~FileReader() = default;
  virtual int ReadFile(butil::IOBuf* out, const std::string& filename, off_t offset, size_t max_count,
                       size_t* read_count, bool* is_eof) const = 0;
  // Get the path of this reader
  virtual const std::string& Path() const = 0;
};

// Read files within a local directory
class LocalDirReader : public FileReader {
 public:
  LocalDirReader(braft::FileSystemAdaptor* fs, const std::string& path)
      : path_(path), fs_(fs) /*, current_file_(nullptr), is_reading_(false), eof_reached_(true)*/ {}
  ~LocalDirReader() override;

  // Open a snapshot for read
  virtual bool Open();

  // Read data from filename at |offset| (from the start of the file) for at
  // most |max_count| bytes to |out|. Reading part of a file is allowed if
  // |read_partly| is TRUE. If successfully read the file, |read_count|
  // is the actual read count, it's maybe smaller than |max_count| if the
  // request is throttled or reach the end of the file. In the case of
  // reaching the end of the file, |is_eof| is also set to true.
  // Returns 0 on success, the error otherwise
  int ReadFile(butil::IOBuf* out, const std::string& filename, off_t offset, size_t max_count, size_t* read_count,
               bool* is_eof) const override;
  const std::string& Path() const override { return path_; }

 protected:
  int ReadFileWithMeta(butil::IOBuf* out, const std::string& filename, google::protobuf::Message* file_meta,
                       off_t offset, size_t max_count, size_t* read_count, bool* is_eof) const;
  const scoped_refptr<braft::FileSystemAdaptor>& FileSystem() const { return fs_; }

 private:
  mutable braft::raft_mutex_t mutex_;
  std::string path_;
  scoped_refptr<braft::FileSystemAdaptor> fs_;
  mutable braft::FileAdaptor* current_file_{nullptr};
  mutable std::string current_filename_;
  mutable bool is_reading_{false};
  mutable bool eof_reached_{true};
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_FILE_READER_H_
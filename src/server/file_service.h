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

#ifndef DINGODB_SERVER_FILE_SERVICE_H_
#define DINGODB_SERVER_FILE_SERVICE_H_

#include <cstdint>
#include <memory>
#include <string>

#include "bthread/types.h"
#include "common/file_reader.h"
#include "proto/file_service.pb.h"
#include "vector/vector_index_snapshot.h"

namespace dingodb {

class FileServiceImpl : public pb::fileservice::FileService {
 public:
  FileServiceImpl() = default;
  ~FileServiceImpl() override = default;
  static FileServiceImpl& GetInstance();

  void GetFile(google::protobuf::RpcController* controller, const pb::fileservice::GetFileRequest* request,
               pb::fileservice::GetFileResponse* response, google::protobuf::Closure* done) override;

  void CleanFileReader(google::protobuf::RpcController* controller,
                       const pb::fileservice::CleanFileReaderRequest* request,
                       pb::fileservice::CleanFileReaderResponse* response, google::protobuf::Closure* done) override;
};

class FileReaderWrapper {
 public:
  FileReaderWrapper(vector_index::SnapshotMetaPtr snapshot)
      : snapshot_(snapshot),
        file_reader_(std::make_shared<LocalDirReader>(new braft::PosixFileSystemAdaptor(), snapshot->Path())) {}
  ~FileReaderWrapper() = default;

  int ReadFile(butil::IOBuf* out, const std::string& filename, off_t offset, size_t max_count, size_t* read_count,
               bool* is_eof) {
    return file_reader_->ReadFile(out, filename, offset, max_count, read_count, is_eof);
  }

  std::string Path() { return snapshot_->Path(); }

 private:
  vector_index::SnapshotMetaPtr snapshot_;
  std::shared_ptr<FileReader> file_reader_;
};

class FileServiceReaderManager {
 public:
  static FileServiceReaderManager& GetInstance();

  int64_t AddReader(std::shared_ptr<FileReaderWrapper> reader);
  int DeleteReader(int64_t reader_id);
  std::shared_ptr<FileReaderWrapper> GetReader(int64_t reader_id);
  std::vector<int64_t> GetAllReaderId();

 private:
  FileServiceReaderManager();
  ~FileServiceReaderManager();

  int64_t next_id_;

  bthread_mutex_t mutex_;
  std::map<int64_t, std::shared_ptr<FileReaderWrapper>> readers_;
};

}  // namespace dingodb

#endif  // DINGODB_SERVER_FILE_SERVICE_H_
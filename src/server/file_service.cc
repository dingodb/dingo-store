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

#include "server/file_service.h"

#include <cstdint>
#include <vector>

#include "fmt/core.h"
#include "server/service_helper.h"

namespace dingodb {

void FileServiceImpl::GetFile(google::protobuf::RpcController* controller,
                              const pb::fileservice::GetFileRequest* request,
                              pb::fileservice::GetFileResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  DINGO_LOG(DEBUG) << fmt::format("Send file to {} request {}", butil::endpoint2str(cntl->remote_side()).c_str(),
                                  request->ShortDebugString());

  auto reader = FileServiceReaderManager::GetInstance().GetReader(request->reader_id());
  if (reader == nullptr) {
    cntl->SetFailed(pb::error::EFILE_NOT_FOUND_READER, "Not found reader %lu", request->reader_id());
    return;
  }

  if (request->size() <= 0 || request->offset() < 0) {
    cntl->SetFailed(pb::error::EILLEGAL_PARAMTETERS, "Invalid request %s", request->ShortDebugString().c_str());
    return;
  }

  butil::IOBuf buf;
  bool is_eof = false;
  size_t read_size = 0;

  const int rc = reader->ReadFile(&buf, request->filename(), request->offset(), request->size(), &read_size, &is_eof);
  if (rc != 0) {
    cntl->SetFailed(pb::error::EFILE_READ, "Read file failed, path:%s/%s error: %s", reader->Path().c_str(),
                    request->filename().c_str(), berror(rc));
    return;
  }

  response->set_eof(is_eof);
  response->set_read_size(read_size);

  if (buf.empty()) {
    return;
  }

  cntl->response_attachment().swap(buf);
}

void FileServiceImpl::CleanFileReader(google::protobuf::RpcController* controller,
                                      const pb::fileservice::CleanFileReaderRequest* request,
                                      pb::fileservice::CleanFileReaderResponse* response,
                                      google::protobuf::Closure* done) {
  auto* svr_done = new NoContextServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);

  if (request->reader_id() > 0) {
    FileServiceReaderManager::GetInstance().DeleteReader(request->reader_id());
  }
}

FileServiceReaderManager::FileServiceReaderManager() {
  bthread_mutex_init(&mutex_, nullptr);
  next_id_ = ((int64_t)getpid() << 45) | (butil::gettimeofday_us() << 17 >> 17);
}

FileServiceReaderManager::~FileServiceReaderManager() { bthread_mutex_destroy(&mutex_); }

FileServiceReaderManager& FileServiceReaderManager::GetInstance() {
  static FileServiceReaderManager instance;

  return instance;
}

int64_t FileServiceReaderManager::AddReader(std::shared_ptr<FileReaderWrapper> reader) {
  BAIDU_SCOPED_LOCK(mutex_);
  int64_t reader_id = ++next_id_;
  readers_[reader_id] = reader;

  return reader_id;
}

int FileServiceReaderManager::DeleteReader(int64_t reader_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return readers_.erase(reader_id) == 1 ? 0 : -1;
}

std::shared_ptr<FileReaderWrapper> FileServiceReaderManager::GetReader(int64_t reader_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = readers_.find(reader_id);
  return (it == readers_.end()) ? nullptr : it->second;
}

std::vector<int64_t> FileServiceReaderManager::GetAllReaderId() {
  BAIDU_SCOPED_LOCK(mutex_);

  std::vector<int64_t> reader_ids;
  reader_ids.reserve(readers_.size());
  for (auto& [reader_id, _] : readers_) {
    reader_ids.push_back(reader_id);
  }

  return reader_ids;
}

}  // namespace dingodb
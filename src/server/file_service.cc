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

namespace dingodb {

FileServiceImpl& FileServiceImpl::GetInstance() {
  static FileServiceImpl instance;

  return instance;
}

FileServiceImpl::FileServiceImpl() { next_id_ = ((int64_t)getpid() << 45) | (butil::gettimeofday_us() << 17 >> 17); }

void FileServiceImpl::GetFile(google::protobuf::RpcController* controller,
                              const pb::fileservice::GetFileRequest* request,
                              pb::fileservice::GetFileResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto reader = GetReader(request->reader_id());
  if (reader == nullptr) {
    cntl->SetFailed(pb::error::EFILE_NOT_FOUND_READER, "Fail to find reader=%" PRId64, request->reader_id());
    return;
  }

  DINGO_LOG(INFO) << "Get file for " << cntl->remote_side() << " path=" << reader->Path()
                  << " filename=" << request->filename() << " offset=" << request->offset()
                  << " size=" << request->size();

  if (request->size() <= 0 || request->offset() < 0) {
    cntl->SetFailed(pb::error::EILLEGAL_PARAMTETERS, "Invalid request=%s", request->ShortDebugString().c_str());
    return;
  }

  butil::IOBuf buf;
  bool is_eof = false;
  size_t read_size = 0;

  const int rc = reader->ReadFile(&buf, request->filename(), request->offset(), request->size(), &read_size, &is_eof);
  if (rc != 0) {
    cntl->SetFailed(pb::error::EFILE_READ, "Fail to read from path=%s filename=%s : %s", reader->Path().c_str(),
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

uint64_t FileServiceImpl::AddReader(std::shared_ptr<FileReader> reader) {
  BAIDU_SCOPED_LOCK(mutex_);
  uint64_t reader_id = ++next_id_;
  readers_[reader_id] = reader;

  return reader_id;
}

int FileServiceImpl::RemoveReader(uint64_t reader_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return readers_.erase(reader_id) == 1 ? 0 : -1;
}

std::shared_ptr<FileReader> FileServiceImpl::GetReader(uint64_t reader_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = readers_.find(reader_id);
  return (it == readers_.end()) ? nullptr : it->second;
}

}  // namespace dingodb
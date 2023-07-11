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

#include "brpc/controller.h"
#include "brpc/server.h"
#include "bthread/types.h"
#include "common/file_reader.h"
#include "common/synchronization.h"
#include "proto/file_service.pb.h"

namespace dingodb {

class FileServiceImpl : public pb::fileservice::FileService {
 public:
  static FileServiceImpl& GetInstance();

  uint64_t AddReader(std::shared_ptr<FileReader> reader);
  int RemoveReader(uint64_t reader_id);

  void GetFile(google::protobuf::RpcController* controller, const pb::fileservice::GetFileRequest* request,
               pb::fileservice::GetFileResponse* response, google::protobuf::Closure* done) override;

 private:
  FileServiceImpl();
  ~FileServiceImpl() override = default;

  std::shared_ptr<FileReader> GetReader(uint64_t reader_id);

  uint64_t next_id_;

  bthread_mutex_t mutex_;
  std::map<uint64_t, std::shared_ptr<FileReader>> readers_;
};

}  // namespace dingodb

#endif  // DINGODB_SERVER_FILE_SERVICE_H_
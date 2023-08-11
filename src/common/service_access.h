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

#ifndef DINGODB_COMMON_SERVICE_STUB_H_
#define DINGODB_COMMON_SERVICE_STUB_H_

#include <sys/stat.h>

#include <memory>

#include "braft/remote_file_copier.h"
#include "butil/endpoint.h"
#include "butil/iobuf.h"
#include "butil/status.h"
#include "proto/file_service.pb.h"
#include "proto/node.pb.h"

namespace dingodb {

class ServiceAccess {
 public:
  // NodeService
  static pb::node::NodeInfo GetNodeInfo(const butil::EndPoint& endpoint);
  static pb::node::NodeInfo GetNodeInfo(const std::string& host, int port);

  static butil::Status InstallVectorIndexSnapshot(const pb::node::InstallVectorIndexSnapshotRequest& request,
                                                  const butil::EndPoint& endpoint,
                                                  pb::node::InstallVectorIndexSnapshotResponse& response);
  static butil::Status GetVectorIndexSnapshot(const pb::node::GetVectorIndexSnapshotRequest& request,
                                              const butil::EndPoint& endpoint,
                                              pb::node::GetVectorIndexSnapshotResponse& response);

  static butil::Status CheckVectorIndex(const pb::node::CheckVectorIndexRequest& request,
                                        const butil::EndPoint& endpoint, pb::node::CheckVectorIndexResponse& response);

  // FileService
  static std::shared_ptr<pb::fileservice::CleanFileReaderResponse> CleanFileReader(
      const pb::fileservice::CleanFileReaderRequest& request, const butil::EndPoint& endpoint);

 private:
  ServiceAccess() = default;
};

class RemoteFileCopier {
 public:
  RemoteFileCopier(const butil::EndPoint& endpoint) : endpoint_(endpoint) {}
  ~RemoteFileCopier() = default;

  static std::shared_ptr<RemoteFileCopier> New(const butil::EndPoint& endpoint) {
    return std::make_shared<RemoteFileCopier>(endpoint);
  }

  bool Init();

  std::shared_ptr<pb::fileservice::GetFileResponse> GetFile(const pb::fileservice::GetFileRequest& request,
                                                            butil::IOBuf* buf);

 private:
  butil::EndPoint endpoint_;
  brpc::Channel channel_;
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_SERVICE_STUB_H_
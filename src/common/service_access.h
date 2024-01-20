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
#include <vector>

#include "brpc/channel.h"
#include "bthread/types.h"
#include "butil/endpoint.h"
#include "butil/iobuf.h"
#include "butil/status.h"
#include "proto/file_service.pb.h"
#include "proto/node.pb.h"

namespace dingodb {

// brpc::Channel pool for rpc request.
class ChannelPool {
 public:
  static ChannelPool& GetInstance();

  std::shared_ptr<brpc::Channel> GetChannel(const butil::EndPoint& endpoint);

 private:
  ChannelPool();
  ~ChannelPool();

  bthread_mutex_t mutex_;
  std::map<butil::EndPoint, std::shared_ptr<brpc::Channel>> channels_;
};

class ServiceAccess {
 public:
  // NodeService
  static pb::node::NodeInfo GetNodeInfo(const butil::EndPoint& endpoint);
  static pb::node::NodeInfo GetNodeInfo(const std::string& host, int port);

  static std::vector<pb::store_internal::Region> GetRegionInfo(std::vector<int64_t> region_ids,
                                                               const butil::EndPoint& endpoint);

  static std::vector<pb::node::RaftStatusEntry> GetRaftStatus(std::vector<int64_t> region_ids,
                                                              const butil::EndPoint& endpoint);

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

  static std::shared_ptr<pb::fileservice::GetFileResponse> GetFile(const pb::fileservice::GetFileRequest& request,
                                                                   const butil::EndPoint& endpoint, butil::IOBuf* buf);

  static butil::Status CommitMerge(const pb::node::CommitMergeRequest& request, const butil::EndPoint& endpoint);

 private:
  ServiceAccess() = default;
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_SERVICE_STUB_H_
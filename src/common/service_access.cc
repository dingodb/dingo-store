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

#include "common/service_access.h"

#include <memory>
#include <utility>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/mutex.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"

namespace dingodb {

ChannelPool::ChannelPool() { bthread_mutex_init(&mutex_, nullptr); }
ChannelPool::~ChannelPool() { bthread_mutex_destroy(&mutex_); }

ChannelPool& ChannelPool::GetInstance() {
  static ChannelPool instance;
  return instance;
}

std::shared_ptr<brpc::Channel> ChannelPool::GetChannel(const butil::EndPoint& endpoint) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = channels_.find(endpoint);
  if (it != channels_.end()) {
    return it->second;
  }

  // Create new channel
  auto channel = std::make_shared<brpc::Channel>();
  brpc::ChannelOptions options;
  options.connect_timeout_ms = 4000;
  options.timeout_ms = 6000;
  options.backup_request_ms = 5000;
  options.connection_type = brpc::ConnectionType::CONNECTION_TYPE_SINGLE;
  if (channel->Init(endpoint, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Init channel failed, endpoint: " << Helper::EndPointToStr(endpoint);
    return nullptr;
  }

  channels_.insert(std::make_pair(endpoint, channel));
  return channel;
}

pb::node::NodeInfo ServiceAccess::GetNodeInfo(const butil::EndPoint& endpoint) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return {};
  }

  pb::node::NodeService_Stub stub(channel.get());

  brpc::Controller cntl;
  cntl.set_timeout_ms(6000);

  pb::node::GetNodeInfoRequest request;
  pb::node::GetNodeInfoResponse response;

  request.set_cluster_id(0);
  stub.GetNodeInfo(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << "Fail to send request to : " << cntl.ErrorText();
    return {};
  }

  DINGO_LOG(INFO) << "Received response"
                  << " cluster_id=" << request.cluster_id() << " latency=" << cntl.latency_us()
                  << " server_location=" << response.node_info().server_location().host() << ":"
                  << response.node_info().server_location().port();

  return response.node_info();
}

pb::node::NodeInfo ServiceAccess::GetNodeInfo(const std::string& host, int port) {
  butil::EndPoint endpoint;
  butil::str2endpoint(host.c_str(), port, &endpoint);
  return GetNodeInfo(endpoint);
}

std::vector<pb::store_internal::Region> ServiceAccess::GetRegionInfo(std::vector<int64_t> region_ids,
                                                                     const butil::EndPoint& endpoint) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return {};
  }

  pb::node::NodeService_Stub stub(channel.get());

  brpc::Controller cntl;
  cntl.set_timeout_ms(6000);

  pb::node::GetRegionInfoRequest request;
  for (auto region_id : region_ids) {
    request.add_region_ids(region_id);
  }
  pb::node::GetRegionInfoResponse response;
  stub.GetRegionInfo(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << "Fail to send request to : " << cntl.ErrorText();
    return {};
  }
  if (response.error().errcode() != pb::error::OK) {
    DINGO_LOG(ERROR) << fmt::format("GetRegionInfo failed, error: {} {}", static_cast<int>(response.error().errcode()),
                                    response.error().errmsg());
    return {};
  }

  return Helper::PbRepeatedToVector(response.regions());
}

std::vector<pb::node::RaftStatusEntry> ServiceAccess::GetRaftStatus(std::vector<int64_t> region_ids,
                                                                    const butil::EndPoint& endpoint) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return {};
  }

  pb::node::NodeService_Stub stub(channel.get());

  brpc::Controller cntl;
  cntl.set_timeout_ms(6000);

  pb::node::GetRaftStatusRequest request;
  for (auto region_id : region_ids) {
    request.add_region_ids(region_id);
  }
  pb::node::GetRaftStatusResponse response;
  stub.GetRaftStatus(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << "Fail to send request to : " << cntl.ErrorText();
    return {};
  }
  if (response.error().errcode() != pb::error::OK) {
    DINGO_LOG(ERROR) << fmt::format("GetRaftStatus failed, error: {} {}", static_cast<int>(response.error().errcode()),
                                    response.error().errmsg());
    return {};
  }

  return Helper::PbRepeatedToVector(response.entries());
}

butil::Status ServiceAccess::InstallVectorIndexSnapshot(const pb::node::InstallVectorIndexSnapshotRequest& request,
                                                        const butil::EndPoint& endpoint,
                                                        pb::node::InstallVectorIndexSnapshotResponse& response) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Get channel failed, endpoint: %s",
                         Helper::EndPointToStr(endpoint).c_str());
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(600 * 1000);
  pb::node::NodeService_Stub stub(channel.get());

  stub.InstallVectorIndexSnapshot(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("Send InstallVectorIndexSnapshot request failed, error {}", cntl.ErrorText());
    return butil::Status(pb::error::EINTERNAL, cntl.ErrorText());
  }

  if (response.error().errcode() != pb::error::OK) {
    if (response.error().errcode() != pb::error::EVECTOR_NOT_NEED_SNAPSHOT &&
        response.error().errcode() != pb::error::EVECTOR_SNAPSHOT_EXIST) {
      DINGO_LOG(ERROR) << fmt::format("InstallVectorIndexSnapshot response failed, error {} {}",
                                      static_cast<int>(response.error().errcode()), response.error().errmsg());
    }
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  return butil::Status();
}

butil::Status ServiceAccess::GetVectorIndexSnapshot(const pb::node::GetVectorIndexSnapshotRequest& request,
                                                    const butil::EndPoint& endpoint,
                                                    pb::node::GetVectorIndexSnapshotResponse& response) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Get channel failed, endpoint: %s",
                         Helper::EndPointToStr(endpoint).c_str());
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(6000);
  pb::node::NodeService_Stub stub(channel.get());

  stub.GetVectorIndexSnapshot(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("Send GetVectorIndexSnapshot request failed, error {}", cntl.ErrorText());
    return butil::Status(pb::error::EINTERNAL, cntl.ErrorText());
  }

  if (response.error().errcode() != pb::error::OK) {
    if (response.error().errcode() != pb::error::EREGION_NOT_FOUND &&
        response.error().errcode() != pb::error::EVECTOR_INDEX_NOT_FOUND &&
        response.error().errcode() != pb::error::EVECTOR_SNAPSHOT_NOT_FOUND) {
      DINGO_LOG(ERROR) << fmt::format("GetVectorIndexSnapshot response failed, error {} {}",
                                      static_cast<int>(response.error().errcode()), response.error().errmsg());
    }
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  return butil::Status();
}

butil::Status ServiceAccess::CheckVectorIndex(const pb::node::CheckVectorIndexRequest& request,
                                              const butil::EndPoint& endpoint,
                                              pb::node::CheckVectorIndexResponse& response) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Get channel failed, endpoint: %s",
                         Helper::EndPointToStr(endpoint).c_str());
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(6000);
  pb::node::NodeService_Stub stub(channel.get());

  stub.CheckVectorIndex(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("Send CheckVectorIndex request failed, error {}", cntl.ErrorText());
    return butil::Status(pb::error::EINTERNAL, cntl.ErrorText());
  }

  if (response.error().errcode() != pb::error::OK) {
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  return butil::Status();
}

std::shared_ptr<pb::fileservice::CleanFileReaderResponse> ServiceAccess::CleanFileReader(
    const pb::fileservice::CleanFileReaderRequest& request, const butil::EndPoint& endpoint) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return nullptr;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(6000);
  pb::fileservice::FileService_Stub stub(channel.get());

  auto response = std::make_shared<pb::fileservice::CleanFileReaderResponse>();
  stub.CleanFileReader(&cntl, &request, response.get(), nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("Send CleanFileReader request failed, error {}", cntl.ErrorText());
    return nullptr;
  }

  return response;
}

std::shared_ptr<pb::fileservice::GetFileResponse> ServiceAccess::GetFile(const pb::fileservice::GetFileRequest& request,
                                                                         const butil::EndPoint& endpoint,
                                                                         butil::IOBuf* buf) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return nullptr;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(6000);
  pb::fileservice::FileService_Stub stub(channel.get());

  auto response = std::make_shared<pb::fileservice::GetFileResponse>();
  stub.GetFile(&cntl, &request, response.get(), nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("Send GetFileRequest failed, channel use count {} endpoint {} error {}",
                                    channel.use_count(), Helper::EndPointToStr(endpoint), cntl.ErrorText());
    return nullptr;
  }

  buf->swap(cntl.response_attachment());

  return response;
}

butil::Status ServiceAccess::CommitMerge(const pb::node::CommitMergeRequest& request, const butil::EndPoint& endpoint) {
  auto channel = ChannelPool::GetInstance().GetChannel(endpoint);
  if (channel == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Get channel failed, endpoint: %s",
                         Helper::EndPointToStr(endpoint).c_str());
  }

  pb::node::NodeService_Stub stub(channel.get());

  brpc::Controller cntl;
  cntl.set_timeout_ms(6000);

  pb::node::CommitMergeResponse response;
  stub.CommitMerge(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << "Fail to send request to : " << cntl.ErrorText();
    return {};
  }
  if (response.error().errcode() != pb::error::OK) {
    DINGO_LOG(ERROR) << fmt::format("CommitMerge failed, error: {} {}", static_cast<int>(response.error().errcode()),
                                    response.error().errmsg());
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  return butil::Status();
}

}  // namespace dingodb
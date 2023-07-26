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

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "butil/status.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"

namespace dingodb {

pb::node::NodeInfo ServiceAccess::GetNodeInfo(const butil::EndPoint& endpoint) {
  brpc::Channel channel;
  if (channel.Init(endpoint, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << butil::endpoint2str(endpoint).c_str();
    return {};
  }

  pb::node::NodeService_Stub stub(&channel);

  brpc::Controller cntl;
  cntl.set_timeout_ms(1000L);

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

butil::Status ServiceAccess::InstallVectorIndexSnapshot(const pb::node::InstallVectorIndexSnapshotRequest& request,
                                                        const butil::EndPoint& endpoint,
                                                        pb::node::InstallVectorIndexSnapshotResponse& response) {
  brpc::Channel channel;
  if (channel.Init(endpoint, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << butil::endpoint2str(endpoint).c_str();
    return {};
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(600 * 1000);
  pb::node::NodeService_Stub stub(&channel);

  stub.InstallVectorIndexSnapshot(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("Send InstallVectorIndexSnapshot request failed, error {}", cntl.ErrorText());
    return butil::Status(pb::error::EINTERNAL, cntl.ErrorText());
  }

  if (response.error().errcode() != pb::error::OK) {
    DINGO_LOG(ERROR) << fmt::format("InstallVectorIndexSnapshot response failed, error {} {}",
                                    static_cast<int>(response.error().errcode()), response.error().errmsg());
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  return butil::Status();
}

butil::Status ServiceAccess::GetVectorIndexSnapshot(const pb::node::GetVectorIndexSnapshotRequest& request,
                                                    const butil::EndPoint& endpoint,
                                                    pb::node::GetVectorIndexSnapshotResponse& response) {
  brpc::Channel channel;
  if (channel.Init(endpoint, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << butil::endpoint2str(endpoint).c_str();
    return {};
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(3000);
  pb::node::NodeService_Stub stub(&channel);

  stub.GetVectorIndexSnapshot(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("Send GetVectorIndexSnapshot request failed, error {}", cntl.ErrorText());
    return butil::Status(pb::error::EINTERNAL, cntl.ErrorText());
  }

  if (response.error().errcode() != pb::error::OK) {
    DINGO_LOG(ERROR) << fmt::format("GetVectorIndexSnapshot response failed, error {} {}",
                                    static_cast<int>(response.error().errcode()), response.error().errmsg());
    return butil::Status(response.error().errcode(), response.error().errmsg());
  }

  return butil::Status();
}

std::shared_ptr<pb::fileservice::CleanFileReaderResponse> ServiceAccess::CleanFileReader(
    const pb::fileservice::CleanFileReaderRequest& request, const butil::EndPoint& endpoint) {
  brpc::Channel channel;
  if (channel.Init(endpoint, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << butil::endpoint2str(endpoint).c_str();
    return {};
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(1000L);
  pb::fileservice::FileService_Stub stub(&channel);

  auto response = std::make_shared<pb::fileservice::CleanFileReaderResponse>();
  stub.CleanFileReader(&cntl, &request, response.get(), nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("Send CleanFileReader request failed, error {}", cntl.ErrorText());
    return nullptr;
  }

  return response;
}

bool RemoteFileCopier::Init() {
  if (channel_.Init(endpoint_, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << butil::endpoint2str(endpoint_).c_str();
    return false;
  }
  return true;
}

std::shared_ptr<pb::fileservice::GetFileResponse> RemoteFileCopier::GetFile(
    const pb::fileservice::GetFileRequest& request, butil::IOBuf* buf) {
  brpc::Controller cntl;
  cntl.set_timeout_ms(1000L);
  pb::fileservice::FileService_Stub stub(&channel_);

  auto response = std::make_shared<pb::fileservice::GetFileResponse>();
  stub.GetFile(&cntl, &request, response.get(), nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("Send GetFileRequest failed, error {}", cntl.ErrorText());
    return nullptr;
  }

  buf->swap(cntl.response_attachment());

  return response;
}

}  // namespace dingodb
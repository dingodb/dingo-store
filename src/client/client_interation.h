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

#ifndef DINGODB_CLIENT_INTERATION_H_
#define DINGODB_CLIENT_INTERATION_H_

#include <cstdint>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "braft/util.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "butil/strings/string_split.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"

DECLARE_bool(log_each_request);
DECLARE_int32(timeout_ms);

namespace client {

const int kMaxRetry = 5;

class ServerInteraction {
 public:
  ServerInteraction() : leader_index_(0){};
  ~ServerInteraction() = default;

  ServerInteraction(const ServerInteraction&) = delete;
  const ServerInteraction& operator=(const ServerInteraction&) = delete;

  bool Init(const std::string& addrs);
  bool Init(std::vector<std::string> addrs);

  bool AddAddr(const std::string& addr);

  int GetLeader();
  void NextLeader(int leader_index);
  void NextLeader(const dingodb::pb::common::Location& location);

  template <typename Request, typename Response>
  butil::Status SendRequest(const std::string& service_name, const std::string& api_name, const Request& request,
                            Response& response);

  template <typename Request, typename Response>
  butil::Status AllSendRequest(const std::string& service_name, const std::string& api_name, const Request& request,
                               Response& response);

  uint64_t GetLatency() const { return latency_; }

 private:
  std::atomic<int> leader_index_;
  std::vector<butil::EndPoint> endpoints_;
  std::vector<std::unique_ptr<brpc::Channel> > channels_;
  uint64_t latency_;
};

using ServerInteractionPtr = std::shared_ptr<ServerInteraction>;

template <typename Request, typename Response>
butil::Status ServerInteraction::SendRequest(const std::string& service_name, const std::string& api_name,
                                             const Request& request, Response& response) {
  google::protobuf::ServiceDescriptor* service_desc = nullptr;
  if (service_name == "CoordinatorService") {
    service_desc =
        const_cast<google::protobuf::ServiceDescriptor*>(dingodb::pb::coordinator::CoordinatorService::descriptor());
  } else if (service_name == "MetaService") {
    service_desc = const_cast<google::protobuf::ServiceDescriptor*>(dingodb::pb::meta::MetaService::descriptor());
  } else if (service_name == "StoreService") {
    service_desc = const_cast<google::protobuf::ServiceDescriptor*>(dingodb::pb::store::StoreService::descriptor());
  } else if (service_name == "IndexService") {
    service_desc = const_cast<google::protobuf::ServiceDescriptor*>(dingodb::pb::index::IndexService::descriptor());
  } else {
    DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
  }

  const ::google::protobuf::MethodDescriptor* method = service_desc->FindMethodByName(api_name);

  int retry_count = 0;
  do {
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);
    cntl.set_log_id(butil::fast_rand());
    const int leader_index = GetLeader();
    channels_[leader_index]->CallMethod(method, &cntl, &request, &response, nullptr);
    if (FLAGS_log_each_request) {
      DINGO_LOG(INFO) << "send request api " << api_name << " request: " << request.ShortDebugString()
                      << " response: " << response.ShortDebugString();
    }
    if (cntl.Failed()) {
      DINGO_LOG(ERROR) << fmt::format("{} response failed, {} {} {}", api_name, cntl.log_id(), cntl.ErrorCode(),
                                      cntl.ErrorText());
      latency_ = cntl.latency_us();
      return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      if (response.error().errcode() == dingodb::pb::error::ERAFT_NOTLEADER) {
        ++retry_count;
        NextLeader(response.error().leader_location());

      } else {
        if (!FLAGS_log_each_request) {
          DINGO_LOG(ERROR) << fmt::format("{} response failed, error {} {}", api_name,
                                          dingodb::pb::error::Errno_Name(response.error().errcode()),
                                          response.error().errmsg());
        }
        latency_ = cntl.latency_us();
        return butil::Status(response.error().errcode(), response.error().errmsg());
      }
    } else {
      latency_ = cntl.latency_us();
      return butil::Status();
    }

  } while (retry_count < kMaxRetry);

  DINGO_LOG(ERROR) << fmt::format("{} response failed, error ERAFT_NOTLEADER Not raft leader", api_name);

  return butil::Status(dingodb::pb::error::ERAFT_NOTLEADER, "Not raft leader");
}

template <typename Request, typename Response>
butil::Status ServerInteraction::AllSendRequest(const std::string& service_name, const std::string& api_name,
                                                const Request& request, Response& response) {
  for (int i = 0; i < channels_.size(); ++i) {
    auto status = SendRequest(service_name, api_name, request, response);
    if (!status.ok()) {
      return status;
    }

    NextLeader(GetLeader() + 1);
  }

  return butil::Status();
}

}  // namespace client

#endif  // DINGODB_CLIENT_INTERATION_H_
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

#ifndef DINGODB_BR_INTERATION_H_
#define DINGODB_BR_INTERATION_H_

#include <butil/fast_rand.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/debug.pb.h"
#include "proto/document.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "proto/util.pb.h"

namespace br {

const int kMaxRetry = 5;
const int64_t kTimeoutMs = 60000;
const bool kLogEachRequest = true;

class ServerInteraction {
 public:
  ServerInteraction() : leader_index_(0){};
  ~ServerInteraction() = default;

  ServerInteraction(const ServerInteraction&) = delete;
  const ServerInteraction& operator=(const ServerInteraction&) = delete;
  ServerInteraction(ServerInteraction&&) = delete;
  ServerInteraction& operator=(ServerInteraction&&) = delete;

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

  int64_t GetLatency() const { return latency_; }

 private:
  std::atomic<int> leader_index_;
  std::vector<butil::EndPoint> endpoints_;
  std::vector<std::unique_ptr<brpc::Channel> > channels_;
  int64_t latency_;
};

using ServerInteractionPtr = std::shared_ptr<ServerInteraction>;

template <typename Request, typename Response>
butil::Status ServerInteraction::SendRequest(const std::string& service_name, const std::string& api_name,
                                             const Request& request, Response& response) {
  const google::protobuf::MethodDescriptor* method = nullptr;

  if (service_name == "CoordinatorService") {
    method = dingodb::pb::coordinator::CoordinatorService::descriptor()->FindMethodByName(api_name);
  } else if (service_name == "MetaService") {
    method = dingodb::pb::meta::MetaService::descriptor()->FindMethodByName(api_name);
  } else if (service_name == "StoreService") {
    method = dingodb::pb::store::StoreService::descriptor()->FindMethodByName(api_name);
  } else if (service_name == "IndexService") {
    method = dingodb::pb::index::IndexService::descriptor()->FindMethodByName(api_name);
  } else if (service_name == "DocumentService") {
    method = dingodb::pb::document::DocumentService::descriptor()->FindMethodByName(api_name);
  } else if (service_name == "UtilService") {
    method = dingodb::pb::util::UtilService::descriptor()->FindMethodByName(api_name);
  } else if (service_name == "DebugService") {
    method = dingodb::pb::debug::DebugService::descriptor()->FindMethodByName(api_name);
  } else {
    DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
  }

  if (method == nullptr) {
    DINGO_LOG(FATAL) << "Unknown api name: " << api_name;
  }

  int retry_count = 0;
  do {
    brpc::Controller cntl;
    cntl.set_timeout_ms(kTimeoutMs);
    cntl.set_log_id(butil::fast_rand());
    const int leader_index = GetLeader();
    channels_[leader_index]->CallMethod(method, &cntl, &request, &response, nullptr);
    if (kLogEachRequest) {
      DINGO_LOG(INFO) << fmt::format("send request api [{}] {} response: {} request: {}", leader_index, api_name,
                                     response.ShortDebugString().substr(0, 256),
                                     request.ShortDebugString().substr(0, 256));
    }
    if (cntl.Failed()) {
      DINGO_LOG(ERROR) << fmt::format("{} response failed, {} {} {}", api_name, cntl.log_id(), cntl.ErrorCode(),
                                      cntl.ErrorText());
      if (cntl.ErrorCode() == 112) {
        ++retry_count;
        NextLeader(leader_index);
        continue;
      }
      latency_ = cntl.latency_us();
      return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    if (response.error().errcode() != dingodb::pb::error::OK) {
      if (response.error().errcode() == dingodb::pb::error::ERAFT_NOTLEADER ||
          response.error().errcode() == dingodb::pb::error::EREGION_NOT_FOUND) {
        ++retry_count;
        NextLeader(response.error().leader_location());

      } else {
        DINGO_LOG(ERROR) << fmt::format("{} response failed, error {} {}", api_name,
                                        dingodb::pb::error::Errno_Name(response.error().errcode()),
                                        response.error().errmsg());

        latency_ = cntl.latency_us();
        return butil::Status(response.error().errcode(), response.error().errmsg());
      }
    } else {
      latency_ = cntl.latency_us();
      return butil::Status();
    }

  } while (retry_count < kMaxRetry);

  DINGO_LOG(ERROR) << fmt::format("{} response failed, error: {} {}", api_name,
                                  dingodb::pb::error::Errno_Name(response.error().errcode()),
                                  response.error().errmsg());

  return butil::Status(response.error().errcode(), response.error().errmsg());
}

template <typename Request, typename Response>
butil::Status ServerInteraction::AllSendRequest(const std::string& service_name, const std::string& api_name,
                                                const Request& request, Response& response) {
  for (int i = 0; i < channels_.size(); ++i) {
    auto status = SendRequest(service_name, api_name, request, response);
    if (!status.ok()) {
      return status;
    }

    NextLeader(GetLeader());
  }

  return butil::Status();
}

}  // namespace br

#endif  // DINGODB_BR_INTERATION_H_
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

#include "br/parameter.h"
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

class ServerInteraction {
 public:
  ServerInteraction() : leader_index_(0){};
  ~ServerInteraction() = default;

  ServerInteraction(const ServerInteraction&) = delete;
  const ServerInteraction& operator=(const ServerInteraction&) = delete;
  ServerInteraction(ServerInteraction&&) = delete;
  ServerInteraction& operator=(ServerInteraction&&) = delete;

  static butil::Status CreateInteraction(const std::vector<std::string>& addrs,
                                         std::shared_ptr<ServerInteraction>& interaction);

  static butil::Status CreateInteraction(const std::string& addrs, std::shared_ptr<ServerInteraction>& interaction);

  bool Init(const std::string& addrs);
  bool Init(std::vector<std::string> addrs);

  bool AddAddr(const std::string& addr);

  int GetLeader();
  void NextLeader(int leader_index);
  void NextLeader(const dingodb::pb::common::Location& location);

  template <typename Request, typename Response>
  butil::Status SendRequest(const std::string& service_name, const std::string& api_name, const Request& request,
                            Response& response, int64_t time_out_ms = FLAGS_br_server_interaction_timeout_ms);

  template <typename Request, typename Response>
  butil::Status AllSendRequest(const std::string& service_name, const std::string& api_name, const Request& request,
                               Response& response, int64_t time_out_ms = FLAGS_br_server_interaction_timeout_ms);

  int64_t GetLatency() const { return latency_; }

  std::vector<std::string> GetAddrs();
  std::string GetAddrsAsString();

 private:
  std::atomic<int> leader_index_;
  std::vector<butil::EndPoint> endpoints_;
  std::vector<std::unique_ptr<brpc::Channel> > channels_;
  int64_t latency_;
  std::vector<std::string> addrs_;
};

using ServerInteractionPtr = std::shared_ptr<ServerInteraction>;

template <typename Request, typename Response>
butil::Status ServerInteraction::SendRequest(const std::string& service_name, const std::string& api_name,
                                             const Request& request, Response& response, int64_t time_out_ms) {
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

  butil::Status status;
  int retry_count = 0;
  do {
    brpc::Controller cntl;
    cntl.set_timeout_ms(time_out_ms);
    cntl.set_log_id(butil::fast_rand());
    const int leader_index = GetLeader();
    channels_[leader_index]->CallMethod(method, &cntl, &request, &response, nullptr);
    if (FLAGS_br_server_interaction_print_each_rpc_request) {
      DINGO_LOG(INFO) << fmt::format("send request api [{}] {} response: {} request: {}", leader_index, api_name,
                                     response.ShortDebugString().substr(0, 256),
                                     request.ShortDebugString().substr(0, 256));
    }
    if (cntl.Failed()) {
      DINGO_LOG(ERROR) << fmt::format("{} response failed, {} {} {}", api_name, cntl.log_id(), cntl.ErrorCode(),
                                      cntl.ErrorText());
      if (cntl.ErrorCode() == 112) {
        ++retry_count;
        // NextLeader(leader_index);
        status.set_error(cntl.ErrorCode(), cntl.ErrorText());
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

  } while (retry_count < FLAGS_br_server_interaction_max_retry);

  DINGO_LOG(ERROR) << fmt::format(
      "{} response failed, retry_count:{} status.error_code:{}({}) status.error_cstr:{} response.error.errcode:{}({}) "
      "response.error.errmsg:{}",
      api_name, retry_count, dingodb::pb::error::Errno_Name(status.error_code()), status.error_code(),
      status.error_cstr(), dingodb::pb::error::Errno_Name(response.error().errcode()),
      static_cast<int64_t>(response.error().errcode()), response.error().errmsg());

  return status;
}

template <typename Request, typename Response>
butil::Status ServerInteraction::AllSendRequest(const std::string& service_name, const std::string& api_name,
                                                const Request& request, Response& response, int64_t time_out_ms) {
  for (int i = 0; i < channels_.size(); ++i) {
    auto status = SendRequest(service_name, api_name, request, response, time_out_ms);
    if (!status.ok()) {
      return status;
    }

    NextLeader(GetLeader());
  }

  return butil::Status();
}

}  // namespace br

#endif  // DINGODB_BR_INTERATION_H_
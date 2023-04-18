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

#ifndef DINGODB_COMMON_COORDINATOR_INTERACTION_H_
#define DINGODB_COMMON_COORDINATOR_INTERACTION_H_

#include <atomic>
#include <memory>

#include "brpc/channel.h"
#include "butil/endpoint.h"
#include "butil/fast_rand.h"
#include "butil/macros.h"
#include "butil/strings/stringprintf.h"
#include "common/logging.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

const int kMaxRetry = 3;

// For store interact with coordinator.
class CoordinatorInteraction {
 public:
  CoordinatorInteraction() : leader_index_(0){};
  ~CoordinatorInteraction() = default;

  CoordinatorInteraction(const CoordinatorInteraction&) = delete;
  const CoordinatorInteraction& operator=(const CoordinatorInteraction&) = delete;

  bool Init(const std::string& addr);

  int GetLeader();
  void NextLeader(int leader_index);

  template <typename Request, typename Response>
  butil::Status SendRequest(const std::string& api_name, const Request& request, Response& response);

 private:
  std::atomic<int> leader_index_;
  std::vector<butil::EndPoint> endpoints_;
  std::vector<std::unique_ptr<brpc::Channel> > channels_;
};

template <typename Request, typename Response>
butil::Status CoordinatorInteraction::SendRequest(const std::string& api_name, const Request& request,
                                                  Response& response) {
  const ::google::protobuf::ServiceDescriptor* service_desc = pb::coordinator::CoordinatorService::descriptor();
  const ::google::protobuf::MethodDescriptor* method = service_desc->FindMethodByName(api_name);

  // DINGO_LOG(DEBUG) << "send request to coordinator api " << api_name << " request: " << request.ShortDebugString();
  int retry_count = 0;
  do {
    brpc::Controller cntl;
    cntl.set_log_id(butil::fast_rand());
    const int leader_index = GetLeader();
    channels_[leader_index]->CallMethod(method, &cntl, &request, &response, nullptr);
    // DINGO_LOG(DEBUG) << "send request to coordinator api " << api_name << " response: " <<
    // response.ShortDebugString();
    if (cntl.Failed()) {
      DINGO_LOG(ERROR) << butil::StringPrintf("%s response failed, %lu %d %s", api_name.c_str(), cntl.log_id(),
                                              cntl.ErrorCode(), cntl.ErrorText().c_str());
      return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }

    if (response.error().errcode() != pb::error::OK) {
      if (response.error().errcode() == pb::error::ERAFT_NOTLEADER) {
        ++retry_count;
        NextLeader(leader_index);
      } else {
        return butil::Status(response.error().errcode(), response.error().errmsg());
      }
    } else {
      return butil::Status();
    }

  } while (retry_count < kMaxRetry);

  return butil::Status(pb::error::ERAFT_NOTLEADER, "Not raft leader");
}

}  // namespace dingodb

#endif  // DINGODB_COMMON_COORDINATOR_INTERACTION_H_

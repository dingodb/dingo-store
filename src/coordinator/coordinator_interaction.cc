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

#include "coordinator/coordinator_interaction.h"

#include "common/helper.h"

namespace dingodb {

bool CoordinatorInteraction::Init(const std::string& addr) {
  endpoints_ = Helper::StrToEndpoint(addr);
  if (endpoints_.empty()) {
    LOG(ERROR) << "Parse addr failed " << addr;
    return false;
  }

  for (auto& endpoint : endpoints_) {
    LOG(INFO) << butil::StringPrintf("Init channel %s:%d", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
    std::unique_ptr<brpc::Channel> channel = std::make_unique<brpc::Channel>();
    if (channel->Init(endpoint, nullptr) != 0) {
      LOG(ERROR) << butil::StringPrintf("Init channel failed, %s:%d", butil::ip2str(endpoint.ip).c_str(),
                                        endpoint.port);
      return false;
    }
    channels_.push_back(std::move(channel));
  }

  return true;
}

int CoordinatorInteraction::GetLeader() { return leader_index_.load(); }

void CoordinatorInteraction::NextLeader(int leader_index) {
  int next_leader_index = (leader_index + 1) % endpoints_.size();
  leader_index_.compare_exchange_weak(leader_index, next_leader_index);
}

}  // namespace dingodb
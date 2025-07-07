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

#include "br/interation.h"

#include <memory>
#include <string>

#include "br/helper.h"
#include "bthread/bthread.h"
#include "butil/strings/string_split.h"
#include "common/helper.h"
#include "fmt/core.h"

namespace br {

bool ServerInteraction::Init(const std::string& addrs) {
  std::vector<std::string> vec_addrs;
  butil::SplitString(addrs, ',', &vec_addrs);

  return Init(vec_addrs);
}

bool ServerInteraction::Init(std::vector<std::string> addrs) {
  endpoints_ = br::Helper::VectorToEndpoints(addrs);
  if (endpoints_.empty()) {
    DINGO_LOG(ERROR) << "Parse addr failed";
    return false;
  }

  for (auto& endpoint : endpoints_) {
    if (FLAGS_br_log_switch_restore_detail || FLAGS_br_log_switch_restore_detail_detail ||
        FLAGS_br_log_switch_backup_detail || FLAGS_br_log_switch_backup_detail_detail) {
      DINGO_LOG(INFO) << fmt::format("Init channel {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
    }

    std::unique_ptr<brpc::Channel> channel = std::make_unique<brpc::Channel>();
    if (channel->Init(endpoint, nullptr) != 0) {
      DINGO_LOG(ERROR) << fmt::format("Init channel failed, {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
      return false;
    }
    channels_.push_back(std::move(channel));
  }

  addrs_ = addrs;

  return true;
}

bool ServerInteraction::AddAddr(const std::string& addr) {
  butil::EndPoint endpoint = dingodb::Helper::StringToEndPoint(addr);

  std::unique_ptr<brpc::Channel> channel = std::make_unique<brpc::Channel>();
  if (channel->Init(endpoint, nullptr) != 0) {
    DINGO_LOG(ERROR) << fmt::format("Init channel failed, {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
    return false;
  }

  channels_.push_back(std::move(channel));
  endpoints_.push_back(endpoint);
  addrs_.push_back(addr);

  return true;
}

int ServerInteraction::GetLeader() { return leader_index_.load(); }

void ServerInteraction::NextLeader(int leader_index) {
  int const next_leader_index = (leader_index + 1) % endpoints_.size();
  leader_index_.compare_exchange_weak(leader_index, next_leader_index);
}

void ServerInteraction::NextLeader(const dingodb::pb::common::Location& location) {
  // DINGO_LOG(INFO) << fmt::format("redirect leader {}:{}", location.host(), location.port());
  if (location.port() == 0) {
    bthread_usleep(500 * 1000L);
    return;
  }

  for (int i = 0; i < endpoints_.size(); ++i) {
    auto endpoints = Helper::StringToEndpoints(location.host() + ":" + std::to_string(location.port()));
    if (endpoints.empty()) {
      bthread_usleep(500 * 1000L);
      return;
    }

    if (endpoints[0].ip == endpoints_[i].ip && endpoints[0].port == endpoints_[i].port) {
      leader_index_.store(i);
      return;
    }
  }

  if (AddAddr(fmt::format("{}:{}", location.host(), location.port()))) {
    leader_index_.store(endpoints_.size() - 1);
  }
}

std::vector<std::string> ServerInteraction::GetAddrs() { return addrs_; }

std::string ServerInteraction::GetAddrsAsString() {
  std::string internal_addrs;
  for (size_t i = 0; i < addrs_.size(); i++) {
    if (0 != i) {
      internal_addrs += ",";
    }
    internal_addrs += addrs_[i];
  }
  return internal_addrs;
}

bool ServerInteraction::IsEmpty() const { return (endpoints_.empty() || addrs_.empty()); }

butil::Status ServerInteraction::CreateInteraction(const std::vector<std::string>& addrs,
                                                   ServerInteractionPtr& interaction) {
  butil::Status status;
  interaction = std::make_shared<br::ServerInteraction>();
  if (!interaction->Init(addrs)) {
    std::string s = fmt::format("Fail to init interaction, addrs");
    for (const auto& addr : addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    status = butil::Status(dingodb::pb::error::EINTERNAL, s);
    return status;
  }

  return butil::Status::OK();
}

butil::Status ServerInteraction::CreateInteraction(const std::string& addrs,
                                                   std::shared_ptr<ServerInteraction>& interaction) {
  butil::Status status;
  interaction = std::make_shared<br::ServerInteraction>();
  if (!interaction->Init(addrs)) {
    std::string s = fmt::format("Fail to init interaction, addrs");
    for (const auto& addr : addrs) {
      s += fmt::format(" {}", addr);
    }
    DINGO_LOG(ERROR) << s;
    status = butil::Status(dingodb::pb::error::EINTERNAL, s);
    return status;
  }

  return butil::Status::OK();
}

}  // namespace br
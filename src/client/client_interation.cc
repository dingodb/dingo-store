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

#include "client/client_interation.h"

#include <memory>
#include <string>

#include "bthread/bthread.h"
#include "client/client_helper.h"
#include "common/helper.h"
#include "fmt/core.h"

namespace client {

bool ServerInteraction::Init(const std::string& addrs) {
  std::vector<std::string> vec_addrs;
  butil::SplitString(addrs, ',', &vec_addrs);

  return Init(vec_addrs);
}

bool ServerInteraction::Init(std::vector<std::string> addrs) {
  endpoints_ = client::Helper::VectorToEndpoints(addrs);
  if (endpoints_.empty()) {
    DINGO_LOG(ERROR) << "Parse addr failed";
    return false;
  }

  for (auto& endpoint : endpoints_) {
    DINGO_LOG(INFO) << fmt::format("Init channel {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
    std::unique_ptr<brpc::Channel> channel = std::make_unique<brpc::Channel>();
    if (channel->Init(endpoint, nullptr) != 0) {
      DINGO_LOG(ERROR) << fmt::format("Init channel failed, {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
      return false;
    }
    channels_.push_back(std::move(channel));
  }

  return true;
}

bool ServerInteraction::AddAddr(const std::string& addr) {
  butil::EndPoint endpoint = dingodb::Helper::GetEndPoint(addr);

  std::unique_ptr<brpc::Channel> channel = std::make_unique<brpc::Channel>();
  if (channel->Init(endpoint, nullptr) != 0) {
    DINGO_LOG(ERROR) << fmt::format("Init channel failed, {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
    return false;
  }

  channels_.push_back(std::move(channel));
  endpoints_.push_back(endpoint);

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
    auto endpoints = Helper::StrToEndpoints(location.host() + ":" + std::to_string(location.port()));
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

InteractionManager::InteractionManager() { bthread_mutex_init(&mutex_, nullptr); }

InteractionManager::~InteractionManager() { bthread_mutex_destroy(&mutex_); }

InteractionManager& InteractionManager::GetInstance() {
  static InteractionManager instance;
  return instance;
}

void InteractionManager::SetCoorinatorInteraction(ServerInteractionPtr interaction) {
  coordinator_interaction_ = interaction;
}

void InteractionManager::SetStoreInteraction(ServerInteractionPtr interaction) { store_interaction_ = interaction; }

bool InteractionManager::CreateStoreInteraction(std::vector<std::string> addrs) {
  auto interaction = std::make_shared<ServerInteraction>();
  if (!interaction->Init(addrs)) {
    DINGO_LOG(ERROR) << "Fail to init store_interaction";
    return false;
  }

  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (store_interaction_ == nullptr) {
      store_interaction_ = interaction;
    }
  }

  return true;
}

butil::Status InteractionManager::CreateStoreInteraction(int64_t region_id) {
  auto region_entry = RegionRouter::GetInstance().QueryRegionEntry(region_id);
  if (region_entry == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("not found region entry {}", region_id);
    return butil::Status(dingodb::pb::error::EREGION_NOT_FOUND, "Not found region entry %lu", region_id);
  }
  if (!CreateStoreInteraction(region_entry->GetAddrs())) {
    DINGO_LOG(ERROR) << fmt::format("init store interaction failed, region {}", region_id);
    return butil::Status(dingodb::pb::error::EINTERNAL, "Init store interaction failed, region %lu", region_id);
  }

  return butil::Status();
}

int64_t InteractionManager::GetLatency() const {
  if (store_interaction_ == nullptr) {
    return 0;
  }

  return store_interaction_->GetLatency();
}
}  // namespace client
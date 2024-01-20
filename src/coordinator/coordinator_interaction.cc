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

#include <memory>

#include "butil/scoped_lock.h"
#include "common/helper.h"
#include "common/logging.h"
#include "proto/version.pb.h"

namespace dingodb {

bool CoordinatorInteraction::Init(const std::string& addr, uint32_t service_type) {
  service_type_ = service_type;
  endpoints_ = Helper::StrToEndpoints(addr);
  if (endpoints_.empty()) {
    DINGO_LOG(ERROR) << "Parse addr failed " << addr;
    return false;
  }

  for (auto& endpoint : endpoints_) {
    DINGO_LOG(INFO) << fmt::format("Init channel {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
    std::shared_ptr<brpc::Channel> channel = std::make_shared<brpc::Channel>();
    if (channel->Init(endpoint, nullptr) != 0) {
      DINGO_LOG(ERROR) << fmt::format("Init channel failed, {}:{}", butil::ip2str(endpoint.ip).c_str(), endpoint.port);
      return false;
    }
    channels_.push_back(std::move(channel));
  }

  DINGO_LOG(INFO) << "Init channel " << addr;

  return true;
}

bool CoordinatorInteraction::InitByNameService(const std::string& service_name, uint32_t service_type) {
  service_type_ = service_type;

  brpc::ChannelOptions channel_opt;
  // ChannelOptions should set "timeout_ms > connect_timeout_ms" for circuit breaker
  channel_opt.timeout_ms = 1000;
  channel_opt.connect_timeout_ms = 500;

  if (name_service_channel_.Init(service_name.c_str(), "rr", &channel_opt) != 0) {
    DINGO_LOG(ERROR) << fmt::format("Init channel failed by service_name, {}", service_name);
    return false;
  }

  use_service_name_ = true;

  DINGO_LOG(INFO) << "Init channel by service_name " << service_name << " service_type=" << service_type;

  return true;
}

int CoordinatorInteraction::GetLeader() { return leader_index_.load(); }

void CoordinatorInteraction::NextLeader(int leader_index) {
  int const next_leader_index = (leader_index + 1) % endpoints_.size();
  leader_index_.compare_exchange_weak(leader_index, next_leader_index);
}

const ::google::protobuf::ServiceDescriptor* CoordinatorInteraction::GetServiceDescriptor() const {
  switch (service_type_) {
    case pb::common::CoordinatorServiceType::ServiceTypeCoordinator: {
      return pb::coordinator::CoordinatorService::descriptor();
    }
    case pb::common::CoordinatorServiceType::ServiceTypeMeta:
    case pb::common::CoordinatorServiceType::ServiceTypeAutoIncrement: {
      return pb::meta::MetaService::descriptor();
    }
    case pb::common::CoordinatorServiceType::ServiceTypeVersion: {
      return pb::version::VersionService::descriptor();
    }
    default:
      return nullptr;
  }
  return nullptr;
}

void CoordinatorInteraction::SetLeaderAddress(const butil::EndPoint& addr) {
  BAIDU_SCOPED_LOCK(leader_mutex_);
  leader_addr_ = addr;
}

}  // namespace dingodb

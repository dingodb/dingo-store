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

#include "sdk/client_stub.h"

#include "common/logging.h"
#include "fmt/core.h"
#include "sdk/meta_cache.h"
#include "sdk/rpc_interaction.h"
#include "sdk/status.h"

namespace dingodb {

namespace sdk {

ClientStub::ClientStub() = default;

ClientStub::~ClientStub() = default;

Status ClientStub::Open(std::string naming_service_url) {
  coordinator_interaction_ = std::make_shared<dingodb::CoordinatorInteraction>();
  if (!coordinator_interaction_->InitByNameService(
          naming_service_url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeCoordinator)) {
    std::string msg =
        fmt::format("Fail to init coordinator_interaction, please check parameter --url={}", naming_service_url);
    DINGO_LOG(ERROR) << msg;
    return Status::Uninitialized(msg);
  }

  coordinator_interaction_meta_ = std::make_shared<dingodb::CoordinatorInteraction>();
  if (!coordinator_interaction_meta_->InitByNameService(naming_service_url,
                                                        dingodb::pb::common::CoordinatorServiceType::ServiceTypeMeta)) {
    std::string msg =
        fmt::format("Fail to init coordinator_interaction_meta, please check parameter --url={}", naming_service_url);
    DINGO_LOG(ERROR) << msg;
    return Status::Uninitialized(msg);
  }

  coordinator_interaction_version_ = std::make_shared<dingodb::CoordinatorInteraction>();
  if (!coordinator_interaction_version_->InitByNameService(
          naming_service_url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeVersion)) {
    std::string msg = fmt::format("Fail to init coordinator_interaction_version, please check parameter --url={}",
                                  naming_service_url);
    DINGO_LOG(ERROR) << msg;
    return Status::Uninitialized(msg);
  }

  // TODO: pass use gflag or add options
  brpc::ChannelOptions options;
  // ChannelOptions should set "timeout_ms > connect_timeout_ms" for circuit breaker
  options.timeout_ms = 5000;
  options.connect_timeout_ms = 3000;
  store_rpc_interaction_.reset(new RpcInteraction(options));

  meta_cache_.reset(new MetaCache(coordinator_interaction_));

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb
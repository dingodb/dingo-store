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

#include "sdk/coordinator_proxy.h"

#include "common/logging.h"
#include "fmt/core.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"

namespace dingodb {
namespace sdk {

#define COORDINATOR_RPC_MSG(brpc_status, request, response)                                         \
  fmt::format("error_code: {}, error_cstr:{}, request:{}, response:{}", (brpc_status).error_code(), \
              (brpc_status).error_cstr(), (request).DebugString(), (response).DebugString())

CoordinatorProxy::CoordinatorProxy() = default;

CoordinatorProxy::~CoordinatorProxy() = default;

Status CoordinatorProxy::Open(std::string naming_service_url) {
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

  return Status::OK();
}

Status CoordinatorProxy::Hello(const pb::coordinator::HelloRequest& request, pb::coordinator::HelloResponse& response) {
  butil::Status rpc_status = coordinator_interaction_->SendRequest("Hello", request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail hello {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

Status CoordinatorProxy::QueryRegion(const pb::coordinator::QueryRegionRequest& request,
                                     pb::coordinator::QueryRegionResponse& response) {
  butil::Status rpc_status = coordinator_interaction_->SendRequest("QueryRegion", request, response);
  if (!rpc_status.ok()) {
    if (rpc_status.error_code() == pb::error::Errno::EREGION_NOT_FOUND) {
      return Status::NotFound(rpc_status.error_code(), rpc_status.error_cstr());
    } else {
      DINGO_LOG(INFO) << fmt::format("Fail query region {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
      return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
    }
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

Status CoordinatorProxy::CreateRegion(const pb::coordinator::CreateRegionRequest& request,
                                      pb::coordinator::CreateRegionResponse& response) {
  butil::Status rpc_status = coordinator_interaction_->SendRequest("CreateRegion", request, response);
  DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail create region {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
  }
  return Status::OK();
}

Status CoordinatorProxy::DropRegion(const pb::coordinator::DropRegionRequest& request,
                                    pb::coordinator::DropRegionResponse& response) {
  butil::Status rpc_status = coordinator_interaction_->SendRequest("DropRegion", request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail drop region {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    if (rpc_status.error_code() == pb::error::Errno::EREGION_NOT_FOUND) {
      return Status::NotFound(rpc_status.error_code(), rpc_status.error_cstr());
    } else {
      return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
    }
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

Status CoordinatorProxy::ScanRegions(const pb::coordinator::ScanRegionsRequest& request,
                                     pb::coordinator::ScanRegionsResponse& response) {
  butil::Status rpc_status = coordinator_interaction_->SendRequest("ScanRegions", request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail scan regions {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

Status CoordinatorProxy::TsoService(const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response) {
  butil::Status rpc_status = coordinator_interaction_meta_->SendRequest("TsoService", request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail tso service {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

Status CoordinatorProxy::CreateIndex(const pb::meta::CreateIndexRequest& request,
                                     pb::meta::CreateIndexResponse& response) {
  butil::Status rpc_status = coordinator_interaction_meta_->SendRequest("CreateIndex", request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail create index {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

Status CoordinatorProxy::GetIndexByName(const pb::meta::GetIndexByNameRequest& request,
                                        pb::meta::GetIndexByNameResponse& response) {
  butil::Status rpc_status = coordinator_interaction_meta_->SendRequest("GetIndexByName", request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail get index by name {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

Status CoordinatorProxy::GetIndexById(const pb::meta::GetIndexRequest& request, pb::meta::GetIndexResponse& response) {
  butil::Status rpc_status = coordinator_interaction_meta_->SendRequest("GetIndex", request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail get index by id {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

Status CoordinatorProxy::DropIndex(const pb::meta::DropIndexRequest& request, pb::meta::DropIndexResponse& response) {
  butil::Status rpc_status = coordinator_interaction_meta_->SendRequest("DropIndex", request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail drop index {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

Status CoordinatorProxy::CreateTableIds(const pb::meta::CreateTableIdsRequest& request,
                                        pb::meta::CreateTableIdsResponse& response) {
  butil::Status rpc_status = coordinator_interaction_meta_->SendRequest("CreateTableIds", request, response);
  if (!rpc_status.ok()) {
    DINGO_LOG(INFO) << fmt::format("Fail create table ids {}", COORDINATOR_RPC_MSG(rpc_status, request, response));
    return Status::RemoteError(rpc_status.error_code(), rpc_status.error_cstr());
  } else {
    DINGO_LOG(DEBUG) << COORDINATOR_RPC_MSG(rpc_status, request, response);
    return Status::OK();
  }
}

}  // namespace sdk

}  // namespace dingodb
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

#include "proto/coordinator.pb.h"

namespace dingodb {
namespace sdk {

CoordiantorProxy::CoordiantorProxy() = default;

CoordiantorProxy::~CoordiantorProxy() = default;

Status CoordiantorProxy::Open(std::string naming_service_url) {
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

Status CoordiantorProxy::CreateRegion(const pb::coordinator::CreateRegionRequest& request,
                                      pb::coordinator::CreateRegionResponse& response) {
  butil::Status rpc_status = coordinator_interaction_->SendRequest("CreateRegion", request, response);
  if (!rpc_status.ok()) {
    std::string msg =
        fmt::format("CreateRegion request fail: code: {}, msg:{}", rpc_status.error_code(), rpc_status.error_cstr());
    return Status::NetworkError(msg);
  }
  return Status::OK();
}

Status CoordiantorProxy::ScanRegions(const pb::coordinator::ScanRegionsRequest& request,
                                     pb::coordinator::ScanRegionsResponse& response) {
  butil::Status rpc_status = coordinator_interaction_->SendRequest("ScanRegions", request, response);
  if (!rpc_status.ok()) {
    std::string msg =
        fmt::format("ScanRegions request fail: code: {}, msg:{}", rpc_status.error_code(), rpc_status.error_cstr());
    return Status::NetworkError(msg);
  }
  return Status::OK();
}

Status CoordiantorProxy::TsoService(const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response) {
  butil::Status rpc_status = coordinator_interaction_meta_->SendRequest("TsoService", request, response);
  if (!rpc_status.ok()) {
    std::string msg =
        fmt::format("TsoService request fail: code: {}, msg:{}", rpc_status.error_code(), rpc_status.error_cstr());
    return Status::NetworkError(msg);
  }
  return Status::OK();
}

}  // namespace sdk

}  // namespace dingodb
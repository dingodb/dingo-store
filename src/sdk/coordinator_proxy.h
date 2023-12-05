
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

#ifndef DINGODB_SDK_COORDINATOR_PROXY_H_
#define DINGODB_SDK_COORDINATOR_PROXY_H_

#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"
#include "proto/meta.pb.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {
class CoordinatorProxy {
 public:
  CoordinatorProxy(const CoordinatorProxy&) = delete;
  const CoordinatorProxy& operator=(const CoordinatorProxy&) = delete;

  CoordinatorProxy();

  virtual ~CoordinatorProxy();

  virtual Status Open(std::string naming_service_url);

  // Region
  virtual Status CreateRegion(const pb::coordinator::CreateRegionRequest& request,
                              pb::coordinator::CreateRegionResponse& response);

  virtual Status ScanRegions(const pb::coordinator::ScanRegionsRequest& request,
                             pb::coordinator::ScanRegionsResponse& response);

  // Meta Service
  virtual Status TsoService(const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response);

 private:
  std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_;
  std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_meta_;
  std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version_;
};
}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_COORDINATOR_PROXY_H_
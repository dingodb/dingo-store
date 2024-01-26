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

#ifndef DINGODB_SDK_TEST_MOCK_COORDINATOR_PROXY_H_
#define DINGODB_SDK_TEST_MOCK_COORDINATOR_PROXY_H_

#include "sdk/coordinator_proxy.h"
#include "gmock/gmock.h"

namespace dingodb {
namespace sdk {

class MockCoordinatorProxy final : public CoordinatorProxy {
 public:
  explicit MockCoordinatorProxy() = default;

  ~MockCoordinatorProxy() override = default;

  MOCK_METHOD(Status, Open, (std::string naming_service_url), (override));

  MOCK_METHOD(Status, QueryRegion,
              (const pb::coordinator::QueryRegionRequest& request, pb::coordinator::QueryRegionResponse& response),
              (override));

  MOCK_METHOD(Status, CreateRegion,
              (const pb::coordinator::CreateRegionRequest& request, pb::coordinator::CreateRegionResponse& response),
              (override));

  MOCK_METHOD(Status, DropRegion,
              (const pb::coordinator::DropRegionRequest& request, pb::coordinator::DropRegionResponse& response),
              (override));

  MOCK_METHOD(Status, ScanRegions,
              (const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response),
              (override));

  MOCK_METHOD(Status, TsoService, (const pb::meta::TsoRequest& request, pb::meta::TsoResponse& response), (override));

  MOCK_METHOD(Status, CreateIndex,
              (const pb::meta::CreateIndexRequest& request, pb::meta::CreateIndexResponse& response), (override));

  MOCK_METHOD(Status, GetIndexByName,
              (const pb::meta::GetIndexByNameRequest& request, pb::meta::GetIndexByNameResponse& response), (override));

  MOCK_METHOD(Status, GetIndexById, (const pb::meta::GetIndexRequest& request, pb::meta::GetIndexResponse& response),
              (override));

  MOCK_METHOD(Status, CreateTableIds,
              (const pb::meta::CreateTableIdsRequest& request, pb::meta::CreateTableIdsResponse& response), (override));
};

}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_TEST_MOCK_COORDINATOR_PROXY_H_
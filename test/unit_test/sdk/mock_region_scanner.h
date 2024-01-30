
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

#ifndef DINGODB_SDK_TEST_MOCK_REGION_SCANNER_H_
#define DINGODB_SDK_TEST_MOCK_REGION_SCANNER_H_

#include <memory>

#include "gmock/gmock.h"
#include "sdk/region.h"
#include "sdk/region_scanner.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

class MockRegionScanner final : public RegionScanner {
 public:
  explicit MockRegionScanner(const ClientStub& stub, std::shared_ptr<Region> region, std::string p_start_key,
                             std::string p_end_key)
      : RegionScanner(stub, std::move(region)), start_key(std::move(p_start_key)), end_key(std::move(p_end_key)) {}

  ~MockRegionScanner() override = default;

  MOCK_METHOD(Status, Open, (), (override));

  MOCK_METHOD(void, AsyncOpen, (StatusCallback cb), (override));

  MOCK_METHOD(void, Close, (), (override));

  MOCK_METHOD(void, AsyncClose, (StatusCallback cb), (override));

  MOCK_METHOD(Status, NextBatch, (std::vector<KVPair> & kvs), (override));

  MOCK_METHOD(void, AsyncNextBatch, (std::vector<KVPair> & kvs, StatusCallback cb), (override));

  MOCK_METHOD(bool, HasMore, (), (const, override));

  MOCK_METHOD(Status, SetBatchSize, (int64_t size), (override));

  MOCK_METHOD(int64_t, GetBatchSize, (), (const, override));

  const std::string start_key;
  const std::string end_key;
};

class MockRegionScannerFactory : public RegionScannerFactory {
 public:
  MockRegionScannerFactory() = default;

  ~MockRegionScannerFactory() override = default;

  MOCK_METHOD(Status, NewRegionScanner, (const ScannerOptions& options, std::shared_ptr<RegionScanner>& scanner),
              (override));
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TEST_MOCK_REGION_SCANNER_H_
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

#ifndef DINGODB_INTEGRATION_TEST_HELPER_
#define DINGODB_INTEGRATION_TEST_HELPER_

#include <cstdint>
#include <fstream>
#include <iostream>
#include <ostream>
#include <string>
#include <utility>

#include "environment.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "sdk/client.h"

namespace dingodb {

namespace integration_test {

static const std::string kClientRaw = "w";

class Helper {
 public:
  static std::string EncodeRawKey(const std::string& str) { return kClientRaw + str; }

  static std::string PrefixNext(const std::string& input) {
    std::string ret(input.size(), 0);
    int carry = 1;
    for (int i = input.size() - 1; i >= 0; --i) {
      if (static_cast<uint8_t>(input[i]) == (uint8_t)0xFF && carry == 1) {
        ret[i] = 0;
      } else {
        ret[i] = (input[i] + carry);
        carry = 0;
      }
    }

    return (carry == 0) ? ret : input;
  }

  static int64_t CreateRawRegion(const std::string& name, const std::string& start_key, const std::string& end_key,
                                 sdk::EngineType engine_type = sdk::kLSM, int replicas = 3) {
    CHECK(!name.empty()) << "name should not empty";
    CHECK(!start_key.empty()) << "start_key should not empty";
    CHECK(!end_key.empty()) << "end_key should not empty";
    CHECK(start_key < end_key) << "start_key must < end_key";
    CHECK(replicas > 0) << "replicas must > 0";

    auto client = Environment::GetInstance().GetClient();

    sdk::RegionCreator* tmp;
    auto status = client->NewRegionCreator(&tmp);
    CHECK(status.ok()) << fmt::format("new region creator failed, {}", status.ToString());
    std::shared_ptr<sdk::RegionCreator> creator(tmp);
    int64_t region_id;
    status = creator->SetRegionName(name)
                 .SetEngineType(engine_type)
                 .SetReplicaNum(replicas)
                 .SetRange(EncodeRawKey(start_key), EncodeRawKey(end_key))
                 .Create(region_id);

    CHECK(status.IsOK()) << fmt::format("Create region failed, {}", status.ToString());
    CHECK(region_id != 0) << "region_id is invalid";
    return region_id;
  }

  static void DropRawRegion(int64_t region_id) {
    CHECK(region_id != 0) << "region_id is invalid";
    auto client = Environment::GetInstance().GetClient();
    auto status = client->DropRegion(region_id);
    CHECK(status.IsOK()) << fmt::format("Drop region failed, {}", status.ToString());
  }

  static bool IsContain(const std::vector<sdk::KVPair>& kvs, const std::string& key) {
    for (const auto& kv : kvs) {
      if (kv.key == key) {
        return true;
      }
    }

    return false;
  }
};

}  // namespace integration_test

}  // namespace dingodb

#endif  // DINGODB_INTEGRATION_TEST_HELPER_
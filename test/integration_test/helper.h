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
#include <iostream>
#include <string>

#include "environment.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "proto/coordinator.pb.h"

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
                                 int replicas = 3) {
    CHECK(!name.empty()) << "name should not empty";
    CHECK(!start_key.empty()) << "start_key should not empty";
    CHECK(!end_key.empty()) << "end_key should not empty";
    CHECK(start_key < end_key) << "start_key must < end_key";
    CHECK(replicas > 0) << "replicas must > 0";

    pb::coordinator::CreateRegionRequest request;
    pb::coordinator::CreateRegionResponse response;

    request.set_region_name(name);
    request.set_replica_num(replicas);
    request.mutable_range()->set_start_key(EncodeRawKey(start_key));
    request.mutable_range()->set_end_key(EncodeRawKey(end_key));

    LOG(INFO) << "Create region request: " << request.ShortDebugString();

    auto status = Environment::GetInstance().GetCoordinatorProxy()->CreateRegion(request, response);
    CHECK(status.IsOK()) << fmt::format("Create region failed, {}", status.ToString());
    CHECK(response.region_id() != 0) << "region_id is invalid";
    return response.region_id();
  }

  static void DropRawRegion(int64_t region_id) {
    CHECK(region_id != 0) << "region_id is invalid";

    pb::coordinator::DropRegionRequest request;
    pb::coordinator::DropRegionResponse response;

    request.set_region_id(region_id);

    LOG(INFO) << "Drop region request: " << request.ShortDebugString();

    auto status = Environment::GetInstance().GetCoordinatorProxy()->DropRegion(request, response);
    CHECK(status.IsOK()) << fmt::format("Drop region failed, {}", status.ToString());
  }

  static bool IsContain(const std::vector<sdk::KVPair>& kvs, const std::string& key) {
    for (auto& kv : kvs) {
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
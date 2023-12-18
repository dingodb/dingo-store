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

    auto client = Environment::GetInstance().GetClient();

    std::shared_ptr<sdk::RegionCreator> creator;
    auto status = client->NewRegionCreator(creator);
    CHECK(status.ok()) << fmt::format("new region creator failed, {}", status.ToString());
    int64_t region_id;
    status = creator->SetRegionName(name)
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

  static std::vector<std::pair<std::string, std::string>> TransformVersionInfo(
      const pb::common::VersionInfo& version_info) {
    std::vector<std::pair<std::string, std::string>> result = {
        std::make_pair("git_commit_hash", version_info.git_commit_hash()),
        std::make_pair("git_tag_name", version_info.git_tag_name()),
        std::make_pair("git_commit_user", version_info.git_commit_user()),
        std::make_pair("git_commit_mail", version_info.git_commit_mail()),
        std::make_pair("git_commit_time", version_info.git_commit_time()),
        std::make_pair("major_version", version_info.major_version()),
        std::make_pair("minor_version", version_info.minor_version()),
        std::make_pair("dingo_build_type", version_info.dingo_build_type()),
        std::make_pair("dingo_contrib_build_type", version_info.dingo_contrib_build_type()),
        std::make_pair("use_mkl", version_info.use_mkl() ? "true" : "false"),
        std::make_pair("use_openblas", version_info.use_openblas() ? "true" : "false"),
        std::make_pair("use_openblas", version_info.use_openblas() ? "true" : "false"),
        std::make_pair("use_tcmalloc", version_info.use_tcmalloc() ? "true" : "false"),
        std::make_pair("use_profiler", version_info.use_profiler() ? "true" : "false"),
        std::make_pair("use_sanitizer", version_info.use_sanitizer() ? "true" : "false"),
    };

    return result;
  }

  static bool IsContain(const std::vector<sdk::KVPair>& kvs, const std::string& key) {
    for (const auto& kv : kvs) {
      if (kv.key == key) {
        return true;
      }
    }

    return false;
  }

  static bool SaveFile(const std::string& filepath, const std::string& data) {
    std::ofstream file(filepath);
    if (!file.is_open()) {
      return false;
    }

    file << data;
    file.close();

    return true;
  }
};

}  // namespace integration_test

}  // namespace dingodb

#endif  // DINGODB_INTEGRATION_TEST_HELPER_
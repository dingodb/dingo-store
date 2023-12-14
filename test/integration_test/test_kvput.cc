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

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>

#include "common/helper.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "helper.h"
#include "sdk/client.h"
#include "sdk/status.h"

DECLARE_string(coordinator_url);

namespace dingodb {

namespace integration_test {

const std::string kRegionName = "Region_for_KvPut";
const std::string kKeyPrefix = "KVPUT000";

class KvPutTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static void SetUpTestSuite() {
    region_id = Helper::CreateRawRegion(kRegionName, kKeyPrefix, Helper::PrefixNext(kKeyPrefix));
  }

  static void TearDownTestSuite() { Helper::DropRawRegion(region_id); }

 public:
  static int64_t region_id;
};

int64_t KvPutTest::region_id = 0;

TEST_F(KvPutTest, Normal) {
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv;
  auto status = Environment::GetInstance().GetClient()->NewRawKV(raw_kv);
  if (!status.IsOK()) {
    LOG(FATAL) << fmt::format("New RawKv failed, error: {}", status.ToString());
  }

  {
    const std::string key = Helper::EncodeRawKey(kKeyPrefix + "hello");
    std::string expect_value = "world";

    auto status = raw_kv->Put(key, expect_value);
    EXPECT_EQ(true, status.IsOK()) << status.ToString();

    std::string actual_value;
    status = raw_kv->Get(key, actual_value);
    EXPECT_EQ(true, status.IsOK()) << status.ToString();
    EXPECT_EQ(expect_value, actual_value) << "Not match value";
  }
}

TEST_F(KvPutTest, Batch) {
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv;
  auto status = Environment::GetInstance().GetClient()->NewRawKV(raw_kv);
  if (!status.IsOK()) {
    LOG(FATAL) << fmt::format("New RawKv failed, error: {}", status.ToString());
  }

  {
    // Test: Ready data
    int key_nums = 10;
    std::vector<std::string> keys;
    std::vector<sdk::KVPair> expect_kvs;
    for (int i = 0; i < key_nums; ++i) {
      sdk::KVPair kv;
      kv.key = Helper::EncodeRawKey(kKeyPrefix + "hello" + std::to_string(i));
      kv.value = "world" + std::to_string(i);
      expect_kvs.push_back(kv);
      keys.push_back(kv.key);
    }
    auto status = raw_kv->BatchPut(expect_kvs);
    EXPECT_EQ(true, status.IsOK()) << status.ToString();

    // Test: run
    std::vector<sdk::KVPair> actual_kvs;
    status = raw_kv->BatchGet(keys, actual_kvs);

    // Test: assert result
    EXPECT_EQ(true, status.IsOK()) << status.ToString();
    EXPECT_EQ(expect_kvs.size(), actual_kvs.size());
    for (int i = 0; i < key_nums; ++i) {
      EXPECT_EQ(expect_kvs[i].key, actual_kvs[i].key) << "Not match key";
      EXPECT_EQ(expect_kvs[i].value, actual_kvs[i].value) << "Not match value";
    }
  }
}

}  // namespace integration_test

}  // namespace dingodb
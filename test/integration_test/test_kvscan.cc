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
#include <vector>

#include "common/helper.h"
#include "engine_type.h"
#include "environment.h"
#include "fmt/core.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "helper.h"
#include "sdk/client.h"
#include "sdk/status.h"

DECLARE_string(coordinator_url);

namespace dingodb {

namespace integration_test {

const std::string kRegionName = "Region_for_KvScan";
const std::string kKeyPrefix = "KVSCAN000";

template <class T>
class KvScanTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    region_id = Helper::CreateRawRegion(kRegionName, kKeyPrefix, Helper::PrefixNext(kKeyPrefix), GetEngineType<T>());
  }
  static void TearDownTestSuite() { Helper::DropRawRegion(region_id); }

  static int64_t region_id;
};

template <class T>
int64_t KvScanTest<T>::region_id = 0;

using Implementations = testing::Types<LsmEngine, BtreeEngine>;
TYPED_TEST_SUITE(KvScanTest, Implementations);

TYPED_TEST(KvScanTest, KvScanSingle) {
  testing::Test::RecordProperty("description", "Test kv scan single case");

  dingodb::sdk::RawKV* tmp;
  auto status = Environment::GetInstance().GetClient()->NewRawKV(&tmp);
  if (!status.IsOK()) {
    LOG(FATAL) << fmt::format("New RawKv failed, error: {}", status.ToString());
  }
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv(tmp);

  {
    // Test: Ready data
    const std::string key = Helper::EncodeRawKey(kKeyPrefix + "hello");
    std::string expect_value = "world";

    // Test: run
    auto status = raw_kv->Put(key, expect_value);
    EXPECT_EQ(true, status.IsOK()) << status.ToString();

    // Test: assert result
    std::string actual_value;
    status = raw_kv->Get(key, actual_value);
    EXPECT_EQ(true, status.IsOK()) << status.ToString();
    EXPECT_EQ(expect_value, actual_value) << "Not match value";

    std::vector<sdk::KVPair> out_kvs;
    status = raw_kv->Scan(key, key + "0", 10, out_kvs);
    EXPECT_EQ(true, status.IsOK()) << status.ToString();
    EXPECT_EQ(out_kvs.size(), 1) << "Not match value";
  }
}

TYPED_TEST(KvScanTest, KvScanMulti) {
  testing::Test::RecordProperty("description", "Test kv scan multi case");

  dingodb::sdk::RawKV* tmp;
  auto status = Environment::GetInstance().GetClient()->NewRawKV(&tmp);
  if (!status.IsOK()) {
    LOG(FATAL) << fmt::format("New RawKv failed, error: {}", status.ToString());
  }
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv(tmp);

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
    status = raw_kv->Scan(keys[0], keys[key_nums - 1], key_nums, actual_kvs);
    DINGO_LOG(INFO) << "Scan start_key: " << keys[0] << ", end_key: " << keys[key_nums - 1] << ", limit: " << key_nums
                    << ", actual_kvs.size(): " << actual_kvs.size();

    // Test: assert result
    EXPECT_EQ(true, status.IsOK()) << status.ToString();
    EXPECT_EQ(expect_kvs.size() - 1, actual_kvs.size());
    for (int i = 0; i < key_nums - 1; ++i) {
      EXPECT_EQ(expect_kvs[i].key, actual_kvs[i].key) << "Not match key";
      EXPECT_EQ(expect_kvs[i].value, actual_kvs[i].value) << "Not match value";
    }
  }
}

TYPED_TEST(KvScanTest, KvScanMultiWithStartWithEnd) {
  testing::Test::RecordProperty("description", "Test kv scan multi case");

  dingodb::sdk::RawKV* tmp;
  auto status = Environment::GetInstance().GetClient()->NewRawKV(&tmp);
  if (!status.IsOK()) {
    LOG(FATAL) << fmt::format("New RawKv failed, error: {}", status.ToString());
  }
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv(tmp);

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

    int start_index = 1;
    int end_index = 8;
    // Test: run
    std::vector<sdk::KVPair> actual_kvs;
    status = raw_kv->Scan(keys[start_index], keys[end_index], key_nums, actual_kvs);
    DINGO_LOG(INFO) << "Scan start_key: " << keys[0] << ", end_key: " << keys[key_nums - 1] << ", limit: " << key_nums
                    << ", actual_kvs.size(): " << actual_kvs.size();

    // Test: assert result
    EXPECT_EQ(true, status.IsOK()) << status.ToString();
    int exptect_num = end_index - start_index;
    EXPECT_EQ(actual_kvs.size(), exptect_num);
    for (int i = 0; i < exptect_num; ++i) {
      int expect_index = i + start_index;
      EXPECT_EQ(expect_kvs[expect_index].key, actual_kvs[i].key) << "Not match key";
      EXPECT_EQ(expect_kvs[expect_index].value, actual_kvs[i].value) << "Not match value";
    }
  }
}

TYPED_TEST(KvScanTest, KvScanMultiThread) {
  testing::Test::RecordProperty("description", "Test kv scan multi case");

  dingodb::sdk::RawKV* tmp;
  auto status = Environment::GetInstance().GetClient()->NewRawKV(&tmp);
  if (!status.IsOK()) {
    LOG(FATAL) << fmt::format("New RawKv failed, error: {}", status.ToString());
  }
  std::shared_ptr<dingodb::sdk::RawKV> raw_kv(tmp);

  {
    // Test: Ready data
    int key_nums = 10;
    std::vector<std::string> keys;
    std::vector<sdk::KVPair> expect_kvs;
    for (int i = 0; i < key_nums; ++i) {
      sdk::KVPair kv;
      kv.key = Helper::EncodeRawKey(kKeyPrefix + "multithread" + std::to_string(i));
      kv.value = "world" + std::to_string(i);
      expect_kvs.push_back(kv);
      keys.push_back(kv.key);
    }
    auto status = raw_kv->BatchPut(expect_kvs);
    EXPECT_EQ(true, status.IsOK()) << status.ToString();

    // use 1000 std::thread to do scan
    int thread_nums = 1000;
    std::vector<std::thread> threads;

    for (int i = 0; i < thread_nums; ++i) {
      threads.emplace_back([this, &keys, &expect_kvs, key_nums, i]() {
        LOG(INFO) << "Thread " << i << " start";

        dingodb::sdk::RawKV* tmp;
        auto status = Environment::GetInstance().GetClient()->NewRawKV(&tmp);
        if (!status.IsOK()) {
          LOG(FATAL) << fmt::format("New RawKv failed, error: {}", status.ToString());
        }
        std::shared_ptr<dingodb::sdk::RawKV> raw_kv(tmp);

        LOG(INFO) << "Thread " << i << " new raw_kv";

        // Test: run
        std::vector<sdk::KVPair> actual_kvs;
        status = raw_kv->Scan(keys[0], keys[key_nums - 1], key_nums, actual_kvs);
        LOG(INFO) << "Scan start_key: " << keys[0] << ", end_key: " << keys[key_nums - 1] << ", limit: " << key_nums
                  << ", actual_kvs.size(): " << actual_kvs.size();

        // Test: assert result
        EXPECT_EQ(true, status.IsOK()) << status.ToString();
        EXPECT_EQ(expect_kvs.size() - 1, actual_kvs.size());
        for (int i = 0; i < key_nums - 1; ++i) {
          EXPECT_EQ(expect_kvs[i].key, actual_kvs[i].key) << "Not match key";
          EXPECT_EQ(expect_kvs[i].value, actual_kvs[i].value) << "Not match value";
        }

        LOG(INFO) << "Thread " << i << " end";
      });
    }

    // join all thread
    for (auto& t : threads) {
      t.join();
    }
  }
}

}  // namespace integration_test

}  // namespace dingodb

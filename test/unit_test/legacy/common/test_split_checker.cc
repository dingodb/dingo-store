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

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "common/helper.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "proto/common.pb.h"
#include "split/split_checker.h"

namespace dingodb {  // NOLINT

const std::string kRootPath = "./unit_test";
const std::string kLogPath = kRootPath + "/log";
const std::string kStorePath = kRootPath + "/db";
const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: 666\n"
    "server:\n"
    "  host: 127.0.0.1\n"
    "  port: 23000\n"
    "log:\n"
    "  path: " +
    kLogPath +
    "\n"
    "store:\n"
    "  path: " +
    kStorePath + "\n";

static const std::string kDefaultCf = "default";
static const std::string kDataCf = "data";
static const std::string kMetaCf = "meta";

static const std::vector<std::string> kAllCFs = {kDefaultCf, kDataCf, kMetaCf};

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

// ================== Helper function ==================

// rand string
static std::string GenRandomString(int len) {
  std::string result;
  int alphabet_len = sizeof(kAlphabet);

  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
  for (int i = 0; i < len; ++i) {
    result.append(1, kAlphabet[distrib(rng) % alphabet_len]);
  }

  return result;
}

static dingodb::store::RegionPtr BuildRegion(int64_t region_id, const std::string& raft_group_name,
                                             std::vector<std::string>& raft_addrs, const std::string& start_key,
                                             const std::string& end_key) {  // NOLINT
  dingodb::pb::common::RegionDefinition region_definition;
  region_definition.set_id(region_id);
  region_definition.set_name(raft_group_name);
  auto* range = region_definition.mutable_range();
  range->set_start_key(start_key);
  range->set_end_key(end_key);

  for (const auto& inner_addr : raft_addrs) {
    std::vector<std::string> host_port_index;
    butil::SplitString(inner_addr, ':', &host_port_index);

    auto* peer = region_definition.add_peers();
    auto* raft_loc = peer->mutable_raft_location();
    raft_loc->set_host(host_port_index[0]);
    raft_loc->set_port(std::stoi(host_port_index[1]));
    raft_loc->set_index(std::stoi(host_port_index[2]));
  }

  return dingodb::store::Region::New(region_definition);
}

// ================== Helper function end ==================

class SplitCheckerTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    Helper::CreateDirectories(kLogPath);
    Helper::CreateDirectories(kStorePath);

    std::srand(std::time(nullptr));

    std::shared_ptr<Config> config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  void SetUp() override {}

  void TearDown() override {}

  static std::shared_ptr<RocksRawEngine> engine;
};

std::shared_ptr<RocksRawEngine> SplitCheckerTest::engine = nullptr;

TEST_F(SplitCheckerTest, MergedIterator) {  // NOLINT
  std::vector<std::string> raft_addrs;
  dingodb::pb::common::Range range;
  range.set_start_key("a");
  range.set_end_key("a");
  auto region = BuildRegion(1000, "unit_test", raft_addrs, range.start_key(), range.end_key());

  // Ready data
  int data_num = 10;
  auto writer = SplitCheckerTest::engine->Writer();
  for (int i = 0; i < data_num; ++i) {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("a" + std::to_string(i));
    kv.set_value(GenRandomString(256));
    for (const auto& cf_name : kAllCFs) {
      writer->KvPut(cf_name, kv);
    }
  }

  // Test
  MergedIterator iter(SplitCheckerTest::engine, {kDefaultCf, kDataCf, kMetaCf}, "b");
  iter.Seek("a");

  for (int i = 0; i < data_num; ++i) {
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("a" + std::to_string(i), iter.Key());
    iter.Next();
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("a" + std::to_string(i), iter.Key());
    iter.Next();
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("a" + std::to_string(i), iter.Key());
    iter.Next();
  }

  // Clean
  writer->KvDeleteRange(kAllCFs, range);
}

TEST_F(SplitCheckerTest, HalfSplitKeys) {  // NOLINT
  // Ready data
  auto writer = SplitCheckerTest::engine->Writer();
  const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
  dingodb::pb::common::KeyValue kv;
  for (int i = 0; i < 10000; ++i) {
    int pos = i % prefixs.size();

    kv.set_key(prefixs[pos] + GenRandomString(30));
    kv.set_value(GenRandomString(512));
    for (const auto& cf_name : kAllCFs) {
      writer->KvPut(cf_name, kv);
    }
  }

  // Get split key
  uint32_t split_threshold_size = 64 * 1024;
  uint32_t split_chunk_size = 1 * 1024;
  auto split_checker =
      std::make_shared<HalfSplitChecker>(SplitCheckerTest::engine, split_threshold_size, split_chunk_size);

  uint32_t count = 0;
  std::vector<std::string> raft_addrs;
  dingodb::pb::common::Range range;
  range.set_start_key("aa");
  range.set_end_key("zz");
  auto region = BuildRegion(1000, "unit_test", raft_addrs, range.start_key(), range.end_key());
  auto split_key = split_checker->SplitKey(region, region->Range(), kAllCFs, count);

  auto reader = SplitCheckerTest::engine->Reader();

  int64_t left_count = 0;
  reader->KvCount(kDefaultCf, range.start_key(), split_key, left_count);

  int64_t right_count = 0;
  reader->KvCount(kDefaultCf, split_key, range.end_key(), right_count);

  EXPECT_EQ(true, abs(static_cast<int>(left_count - right_count)) < 1000);

  // Clean
  writer->KvDeleteRange(kAllCFs, range);
}

TEST_F(SplitCheckerTest, SizeSplitKeys) {  // NOLINT
  uint32_t split_threshold_size = 64 * 1024;
  float split_ratio = 0.5;
  auto split_checker = std::make_shared<SizeSplitChecker>(SplitCheckerTest::engine, split_threshold_size, split_ratio);

  auto writer = SplitCheckerTest::engine->Writer();
  const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
  dingodb::pb::common::KeyValue kv;
  for (int i = 0; i < 10000; ++i) {
    int pos = i % prefixs.size();

    kv.set_key(prefixs[pos] + GenRandomString(30));
    kv.set_value(GenRandomString(256));
    for (const auto& cf_name : kAllCFs) {
      writer->KvPut(cf_name, kv);
    }
  }

  uint32_t count = 0;
  std::vector<std::string> raft_addrs;
  dingodb::pb::common::Range range;
  range.set_start_key("aa");
  range.set_end_key("zz");
  auto region = BuildRegion(1000, "unit_test", raft_addrs, range.start_key(), range.end_key());
  auto split_key = split_checker->SplitKey(region, region->Range(), kAllCFs, count);

  EXPECT_EQ(false, split_key.empty());

  auto reader = SplitCheckerTest::engine->Reader();
  int64_t single_key_size = 288 * kAllCFs.size();
  int64_t left_count = 0;
  reader->KvCount(kDefaultCf, range.start_key(), split_key, left_count);

  EXPECT_EQ(true, abs(split_threshold_size * split_ratio - left_count * single_key_size) < 1000);

  // Clean
  writer->KvDeleteRange(kAllCFs, range);
}

TEST_F(SplitCheckerTest, KeysSplitKeys) {  // NOLINT
  int total_key_num = 10000;
  uint32_t split_key_number = total_key_num;
  float split_key_ratio = 0.5;
  auto split_checker = std::make_shared<KeysSplitChecker>(SplitCheckerTest::engine, split_key_number, split_key_ratio);

  auto writer = SplitCheckerTest::engine->Writer();
  const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
  dingodb::pb::common::KeyValue kv;
  for (int i = 0; i < total_key_num; ++i) {
    int pos = i % prefixs.size();

    kv.set_key(prefixs[pos] + GenRandomString(30));
    kv.set_value(GenRandomString(256));
    for (const auto& cf_name : kAllCFs) {
      writer->KvPut(cf_name, kv);
    }
  }

  uint32_t count = 0;
  std::vector<std::string> raft_addrs;
  dingodb::pb::common::Range range;
  range.set_start_key("aa");
  range.set_end_key("zz");
  auto region = BuildRegion(1000, "unit_test", raft_addrs, range.start_key(), range.end_key());
  auto split_key = split_checker->SplitKey(region, region->Range(), kAllCFs, count);
  EXPECT_EQ(true, !split_key.empty());

  auto reader = SplitCheckerTest::engine->Reader();

  int64_t left_count = 0;
  reader->KvCount(kDefaultCf, range.start_key(), split_key, left_count);

  EXPECT_EQ(true, abs(left_count - split_key_number * split_key_ratio) < 10);

  // Clean
  writer->KvDeleteRange(kAllCFs, range);
}

}  // namespace dingodb
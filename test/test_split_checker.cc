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

#include "butil/status.h"
#include "config/yaml_config.h"
#include "engine/raw_rocks_engine.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "split/split_checker.h"

namespace dingodb {  // NOLINT

const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: 12345\n"
    "  coordinators: 127.0.0.1:19190,127.0.0.1:19191,127.0.0.1:19192\n"
    "  keyring: TO_BE_CONTINUED\n"
    "server:\n"
    "  host: 127.0.0.1\n"
    "  port: 23000\n"
    "  heartbeat_interval: 10000 # ms\n"
    "raft:\n"
    "  host: 127.0.0.1\n"
    "  port: 23100\n"
    "  path: /tmp/dingo-store/data/store/raft\n"
    "  election_timeout: 1000 # ms\n"
    "  snapshot_interval: 3600 # s\n"
    "log:\n"
    "  path: /tmp/dingo-store/log\n"
    "store:\n"
    "  path: ./rocks_example\n"
    "  base:\n"
    "    block_size: 131072\n"
    "    block_cache: 67108864\n"
    "    arena_block_size: 67108864\n"
    "    min_write_buffer_number_to_merge: 4\n"
    "    max_write_buffer_number: 4\n"
    "    max_compaction_bytes: 134217728\n"
    "    write_buffer_size: 67108864\n"
    "    prefix_extractor: 8\n"
    "    max_bytes_for_level_base: 41943040\n"
    "    target_file_size_base: 4194304\n"
    "  default:\n"
    "  instruction:\n"
    "    max_write_buffer_number: 3\n"
    "  column_families:\n"
    "    - default\n"
    "    - meta\n"
    "    - instruction\n";

static const std::string kDefaultCf = "default";

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

// rand string
std::string GenRandomString(int len) {
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

class SplitCheckerTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::srand(std::time(nullptr));

    std::shared_ptr<Config> config = std::make_shared<YamlConfig>();
    if (config->Load(kYamlConfigContent) != 0) {
      std::cout << "Load config failed" << std::endl;
      return;
    }

    engine = std::make_shared<RawRocksEngine>();
    if (!engine->Init(config)) {
      std::cout << "RawRocksEngine init failed" << std::endl;
    }
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
  }

  void SetUp() override {}

  void TearDown() override {}

  static std::shared_ptr<RawRocksEngine> engine;
};

std::shared_ptr<RawRocksEngine> SplitCheckerTest::engine = nullptr;

static dingodb::store::RegionPtr BuildRegion(int64_t region_id, const std::string& raft_group_name,
                                             std::vector<std::string>& raft_addrs) {  // NOLINT
  dingodb::pb::common::RegionDefinition region_definition;
  region_definition.set_id(region_id);
  region_definition.set_name(raft_group_name);
  auto* range = region_definition.mutable_range();
  range->set_start_key("a");
  range->set_end_key("z");

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

TEST_F(SplitCheckerTest, HalfSplitKeys) {  // NOLINT
  // Ready data
  auto writer = SplitCheckerTest::engine->NewWriter(kDefaultCf);
  const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
  dingodb::pb::common::KeyValue kv;
  for (int i = 0; i < (1 * 1000 * 1000); ++i) {
    int pos = i % prefixs.size();

    kv.set_key(prefixs[pos] + GenRandomString(30));
    kv.set_value(GenRandomString(256));
    writer->KvPut(kv);
  }

  // Get split key
  uint32_t split_threshold_size = 128 * 1024 * 1024;
  uint32_t split_chunk_size = 1 * 1024 * 1024;
  auto split_checker =
      std::make_shared<HalfSplitChecker>(SplitCheckerTest::engine, split_threshold_size, split_chunk_size);

  uint32_t count = 0;
  std::vector<std::string> raft_addrs;
  auto region = BuildRegion(1000, "unit_test", raft_addrs);
  auto split_key = split_checker->SplitKey(region, count);
  std::cout << "split_key: " << split_key << std::endl;

  auto reader = SplitCheckerTest::engine->NewReader(kDefaultCf);

  int64_t left_count = 0;
  reader->KvCount(region->InnerRegion().definition().range().start_key(), split_key, left_count);

  int64_t right_count = 0;
  reader->KvCount(split_key, region->InnerRegion().definition().range().end_key(), right_count);

  std::cout << fmt::format("region range [{}-{}] split_key: {} count: {} left_count: {} right_count: {}",
                           region->InnerRegion().definition().range().start_key(),
                           region->InnerRegion().definition().range().end_key(), split_key, count, left_count,
                           right_count);
  EXPECT_EQ(true, abs(static_cast<int>(left_count - right_count)) < 10000);
}

TEST_F(SplitCheckerTest, SizeSplitKeys) {  // NOLINT
  uint32_t split_threshold_size = 128 * 1024 * 1024;
  float split_ratio = 0.5;
  auto split_checker = std::make_shared<SizeSplitChecker>(SplitCheckerTest::engine, split_threshold_size, split_ratio);

  auto writer = SplitCheckerTest::engine->NewWriter(kDefaultCf);
  const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
  dingodb::pb::common::KeyValue kv;
  for (int i = 0; i < (1 * 1000); ++i) {
    int pos = i % prefixs.size();

    kv.set_key(prefixs[pos] + GenRandomString(30));
    kv.set_value(GenRandomString(256));
    writer->KvPut(kv);
  }

  uint32_t count = 0;
  std::vector<std::string> raft_addrs;
  auto region = BuildRegion(1000, "unit_test", raft_addrs);
  auto split_key = split_checker->SplitKey(region, count);
  std::cout << "split_key: " << split_key << std::endl;

  EXPECT_EQ(true, !split_key.empty());

  auto reader = SplitCheckerTest::engine->NewReader(kDefaultCf);

  int64_t left_count = 0;
  reader->KvCount(region->InnerRegion().definition().range().start_key(), split_key, left_count);

  std::cout << fmt::format("region range [{}-{}] split_key: {} count: {} left_count: {} left_size: {}",
                           region->InnerRegion().definition().range().start_key(),
                           region->InnerRegion().definition().range().end_key(), split_key, count, left_count,
                           left_count * 288);
  EXPECT_EQ(true, abs(split_threshold_size * split_ratio - left_count * 288) < 1000);
}

TEST_F(SplitCheckerTest, KeysSplitKeys) {  // NOLINT
  uint32_t split_key_number = 100000;
  float split_key_ratio = 0.5;
  auto split_checker = std::make_shared<KeysSplitChecker>(SplitCheckerTest::engine, split_key_number, split_key_ratio);

  auto writer = SplitCheckerTest::engine->NewWriter(kDefaultCf);
  const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
  dingodb::pb::common::KeyValue kv;
  for (int i = 0; i < (1 * 1000); ++i) {
    int pos = i % prefixs.size();

    kv.set_key(prefixs[pos] + GenRandomString(30));
    kv.set_value(GenRandomString(256));
    writer->KvPut(kv);
  }

  uint32_t count = 0;
  std::vector<std::string> raft_addrs;
  auto region = BuildRegion(1000, "unit_test", raft_addrs);
  auto split_key = split_checker->SplitKey(region, count);
  std::cout << "split_key: " << split_key << std::endl;
  EXPECT_EQ(true, !split_key.empty());

  auto reader = SplitCheckerTest::engine->NewReader(kDefaultCf);

  int64_t left_count = 0;
  reader->KvCount(region->InnerRegion().definition().range().start_key(), split_key, left_count);
  std::cout << fmt::format("region range [{}-{}] split_key: {} count: {} left_count: {}",
                           region->InnerRegion().definition().range().start_key(),
                           region->InnerRegion().definition().range().end_key(), split_key, count, left_count);
  EXPECT_EQ(true, abs(left_count - split_key_number * split_key_ratio) < 10);
}

}  // namespace dingodb
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
#include <iostream>
#include <memory>
#include <random>
#include <string>

#include "config/config.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "metrics/store_metrics_manager.h"

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
    "  path: /tmp/rocks_example\n";

static const std::string kDefaultCf = "default";

static const std::vector<std::string> kAllCFs = {kDefaultCf};

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

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

class StoreRegionMetricsTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::srand(std::time(nullptr));

    std::shared_ptr<dingodb::Config> config = std::make_shared<dingodb::YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    engine = std::make_shared<dingodb::RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
  }

  void SetUp() override {}
  void TearDown() override {}

  static std::shared_ptr<dingodb::RocksRawEngine> engine;
};

std::shared_ptr<dingodb::RocksRawEngine> StoreRegionMetricsTest::engine = nullptr;

static dingodb::store::RegionPtr BuildRegion(int64_t region_id, const std::string& raft_group_name,
                                             std::vector<std::string>& raft_addrs) {
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

TEST_F(StoreRegionMetricsTest, GetRegionMinKey) {
  // Ready data
  auto writer = StoreRegionMetricsTest::engine->Writer();
  const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
  dingodb::pb::common::KeyValue kv;
  for (int i = 0; i < (1 * 1000 * 1000); ++i) {
    int pos = i % prefixs.size();

    kv.set_key(prefixs[pos] + GenRandomString(30));
    kv.set_value(GenRandomString(256));
    writer->KvPut(kDefaultCf, kv);
  }

  dingodb::pb::common::Range range;
  range.set_start_key("cc");
  range.set_end_key("ee");
  writer->KvDeleteRange(kDefaultCf, range);

  std::shared_ptr<dingodb::Engine> engine;

  auto store_region_metrics = std::make_shared<dingodb::StoreRegionMetrics>(
      std::make_shared<dingodb::MetaReader>(StoreRegionMetricsTest::engine),
      std::make_shared<dingodb::MetaWriter>(StoreRegionMetricsTest::engine), engine);

  std::vector<std::string> raft_addrs;
  dingodb::store::RegionPtr region = BuildRegion(11111, "unit-test-01", raft_addrs);
  EXPECT_EQ("", store_region_metrics->GetRegionMinKey(region));
}
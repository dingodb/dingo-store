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
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "braft/raft.pb.h"
#include "braft/snapshot.h"
#include "common/helper.h"
#include "config/config.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "fmt/core.h"
#include "handler/raft_snapshot_handler.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"

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
    "  path: /data/dingo-store/data/store/raft\n"
    "  log_path: /data/dingo-store/data/store/log\n"
    "  election_timeout: 1000 # ms\n"
    "  snapshot_interval: 3600 # s\n"
    "log:\n"
    "  path: /data/dingo-store/log\n"
    "store:\n"
    "  path: /data/dingo-store/data/store/db\n";

static const std::string kDefaultCf = "default";

static const std::vector<std::string> kAllCFs = {kDefaultCf};

const std::string kRaftSnapshotPath = "/data/dingo-store/data/store/raft/snapshot";

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

class RaftSnapshotTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::srand(std::time(nullptr));

    std::shared_ptr<dingodb::Config> config = std::make_shared<dingodb::YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    engine = std::make_shared<dingodb::RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));

    std::filesystem::create_directories(kRaftSnapshotPath);
  }

  static void TearDownTestSuite() {
    engine->Close();
    // engine->Destroy();

    // std::filesystem::remove_all(kRaftSnapshotPath);
  }

  void SetUp() override {}

  void TearDown() override {}

  static std::shared_ptr<dingodb::RocksRawEngine> engine;
};

std::shared_ptr<dingodb::RocksRawEngine> RaftSnapshotTest::engine = nullptr;

TEST_F(RaftSnapshotTest, RaftSnapshotByCheckoutpoint) {
  int64_t start_time = dingodb::Helper::TimestampMs();

  // Ready data
  auto writer = RaftSnapshotTest::engine->Writer();
  const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
  dingodb::pb::common::KeyValue kv;
  for (int i = 0; i < (1 * 1000 * 1000); ++i) {
    int pos = i % prefixs.size();

    kv.set_key(prefixs[pos] + GenRandomString(30));
    kv.set_value(GenRandomString(256));
    writer->KvPut(kDefaultCf, kv);
  }

  dingodb::pb::common::Range delete_range;
  delete_range.set_start_key("ddaf");
  delete_range.set_end_key("eeaf");
  writer->KvDeleteRange(kDefaultCf, delete_range);

  LOG(INFO) << fmt::format("Rut data used time: {} ms", dingodb::Helper::TimestampMs() - start_time);
  start_time = dingodb::Helper::TimestampMs();

  // Save snapshot
  std::unique_ptr<dingodb::RaftSnapshot> raft_snapshot =
      std::make_unique<dingodb::RaftSnapshot>(RaftSnapshotTest::engine);

  auto snapshot_storage = std::make_unique<braft::LocalSnapshotStorage>(kRaftSnapshotPath);
  if (snapshot_storage->init() != 0) {
    LOG(ERROR) << "LocalSnapshotStorage init failed";
  }

  dingodb::pb::common::RegionDefinition definition;
  definition.set_id(111);
  definition.set_name("test-snapshot");
  auto* range = definition.mutable_range();
  range->set_start_key("bb");
  range->set_end_key("gg");
  auto region = dingodb::store::Region::New(definition);

  auto* snapshot_writer = snapshot_storage->create();
  auto gen_snapshot_file_func =
      std::bind(&dingodb::RaftSnapshot::GenSnapshotFileByCheckpoint, raft_snapshot.get(),  // NOLINT
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
  EXPECT_EQ(true, raft_snapshot->SaveSnapshot(snapshot_writer, region, gen_snapshot_file_func, 0, 0, 0));
  braft::SnapshotMeta meta;
  meta.set_last_included_index(dingodb::Helper::TimestampMs());
  meta.set_last_included_term(1);
  snapshot_writer->save_meta(meta);
  snapshot_storage->close(snapshot_writer);

  LOG(INFO) << fmt::format("Save snapshot used time: {} ms", dingodb::Helper::TimestampMs() - start_time);
  start_time = dingodb::Helper::TimestampMs();

  // Count key before load snapshot
  auto reader = RaftSnapshotTest::engine->Reader();
  int64_t expect_count = 0;
  reader->KvCount(kDefaultCf, range->start_key(), range->end_key(), expect_count);

  LOG(INFO) << fmt::format("Count used time: {} ms", dingodb::Helper::TimestampMs() - start_time);
  start_time = dingodb::Helper::TimestampMs();

  // Load snapshot
  auto* snapshot_reader = snapshot_storage->open();
  EXPECT_EQ(true, raft_snapshot->LoadSnapshot(snapshot_reader, region));
  snapshot_storage->close(snapshot_reader);

  LOG(INFO) << fmt::format("Load snapshot used time: {} ms", dingodb::Helper::TimestampMs() - start_time);
  start_time = dingodb::Helper::TimestampMs();

  // Count key after load snapshot
  int64_t actual_count = 0;
  reader->KvCount(kDefaultCf, range->start_key(), range->end_key(), actual_count);
  LOG(INFO) << fmt::format("Count expect {} actual {}", expect_count, actual_count);
  EXPECT_EQ(expect_count, actual_count);

  LOG(INFO) << fmt::format("Count used time: {} ms", dingodb::Helper::TimestampMs() - start_time);
  start_time = dingodb::Helper::TimestampMs();
}

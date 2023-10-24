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

#include <dirent.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/context.h"
#include "common/helper.h"
#include "config/config.h"
#include "config/yaml_config.h"
#include "engine/raw_rocks_engine.h"
#include "proto/common.pb.h"
#include "proto/store_internal.pb.h"
#include "server/server.h"

namespace dingodb {  // NOLINT

static const std::string kDefaultCf = "default";
// static const std::string &kDefaultCf = "meta";

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
    "  meta:\n"
    "    max_write_buffer_number: 3\n"
    "  column_families:\n"
    "    - default\n"
    "    - meta\n";

class RawRocksEngineTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::srand(std::time(nullptr));

    Helper::CreateDirectories("./rocks_example");

    config = std::make_shared<YamlConfig>();
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
    Helper::RemoveAllFileOrDirectory("./rocks_example");
  }

  void SetUp() override {}

  void TearDown() override {}

  static std::shared_ptr<RawRocksEngine> engine;
  static std::shared_ptr<Config> config;
};

std::shared_ptr<RawRocksEngine> RawRocksEngineTest::engine = nullptr;
std::shared_ptr<Config> RawRocksEngineTest::config = nullptr;

template <typename T>
bool CastValueTest(std::string value, T &dst_value) {
  if (value.empty()) {
    return false;
  }
  try {
    if (std::is_same_v<size_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoul(value);
    } else if (std::is_same_v<int32_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoi(value);
    } else if (std::is_same_v<uint32_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoi(value);
    } else if (std::is_same_v<int64_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoll(value);
    } else if (std::is_same_v<int64_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoul(value);
    } else if (std::is_same_v<int, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoi(value);
    } else if (std::is_same_v<float, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stof(value);
    } else if (std::is_same_v<double, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stod(value);
    } else {
      return false;
    }
  } catch (const std::invalid_argument &e) {
    return false;
  } catch (const std::out_of_range &e) {
    return false;
  }

  return true;
}

template <>
bool CastValueTest(std::string value, std::string &dst_value) {
  dst_value = value;
  return true;
}

TEST_F(RawRocksEngineTest, CastValue) {
  {
    size_t value = 0;
    EXPECT_EQ(true, CastValueTest("100", value));
    EXPECT_EQ(100, value);
  }

  {
    uint32_t value = 0;
    EXPECT_EQ(true, CastValueTest("100", value));
    EXPECT_EQ(100, value);
  }

  {
    int32_t value = 0;
    EXPECT_EQ(true, CastValueTest("100", value));
    EXPECT_EQ(100, value);
  }

  {
    uint64_t value = 0;
    EXPECT_EQ(true, CastValueTest("100", value));
    EXPECT_EQ(100, value);
  }

  {
    int64_t value = 0;
    EXPECT_EQ(true, CastValueTest("100", value));
    EXPECT_EQ(100, value);
  }

  {
    float value = 0.0;
    EXPECT_EQ(true, CastValueTest("100.1", value));
    EXPECT_FLOAT_EQ(100.1, value);
  }

  {
    double value = 0.0;
    EXPECT_EQ(true, CastValueTest("100.1", value));
    EXPECT_DOUBLE_EQ(100.1, value);
  }

  {
    std::string value = "0";
    EXPECT_EQ(true, CastValueTest("10", value));
    EXPECT_EQ("10", value);
  }
}

TEST_F(RawRocksEngineTest, GenColumnFamilyByDefaultConfig) {
  std::vector<std::string> column_family_names = {"default", "meta"};
  auto column_family_map = RawRocksEngine::GenColumnFamilyByDefaultConfig(column_family_names);
  EXPECT_EQ(2, column_family_map.size());
  for (const auto &cf_name : column_family_names) {
    EXPECT_EQ(cf_name, column_family_map[cf_name]->Name());
  }

  {
    auto column_family = column_family_map["default"];
    EXPECT_EQ("131072", column_family->GetConfItem("block_size"));
    EXPECT_EQ("67108864", column_family->GetConfItem("target_file_size_base"));
  }
  {
    auto column_family = column_family_map["meta"];
    EXPECT_EQ("131072", column_family->GetConfItem("block_size"));
    EXPECT_EQ("67108864", column_family->GetConfItem("target_file_size_base"));
  }
}

TEST_F(RawRocksEngineTest, SetColumnFamilyCustomConfig) {
  std::vector<std::string> column_family_names = {"default", "meta"};
  auto column_family_map = RawRocksEngine::GenColumnFamilyByDefaultConfig(column_family_names);
  RawRocksEngine::SetColumnFamilyCustomConfig(RawRocksEngineTest::config, column_family_map);

  {
    auto column_family = column_family_map["default"];
    EXPECT_EQ("131072", column_family->GetConfItem("block_size"));
    EXPECT_EQ("4194304", column_family->GetConfItem("target_file_size_base"));
    EXPECT_EQ("4", column_family->GetConfItem("max_write_buffer_number"));
  }
  {
    auto column_family = column_family_map["meta"];
    EXPECT_EQ("131072", column_family->GetConfItem("block_size"));
    EXPECT_EQ("4194304", column_family->GetConfItem("target_file_size_base"));
    EXPECT_EQ("3", column_family->GetConfItem("max_write_buffer_number"));
  }
}

TEST_F(RawRocksEngineTest, GenRcoksDBColumnFamilyOptions) {
  std::vector<std::string> column_family_names = {"default", "meta"};
  auto column_family_map = RawRocksEngine::GenColumnFamilyByDefaultConfig(column_family_names);
  RawRocksEngine::SetColumnFamilyCustomConfig(RawRocksEngineTest::config, column_family_map);

  auto column_family = column_family_map["default"];
  auto column_family_options = RawRocksEngine::GenRcoksDBColumnFamilyOptions(column_family);
  EXPECT_EQ(67108864, column_family_options.arena_block_size);
  EXPECT_EQ(4, column_family_options.min_write_buffer_number_to_merge);
  EXPECT_EQ(4, column_family_options.max_write_buffer_number);
  EXPECT_EQ(134217728, column_family_options.max_compaction_bytes);
  EXPECT_EQ(67108864, column_family_options.write_buffer_size);
}

TEST_F(RawRocksEngineTest, GetName) {
  std::string name = RawRocksEngineTest::engine->GetName();
  EXPECT_EQ(name, "RAW_ENG_ROCKSDB");
}

TEST_F(RawRocksEngineTest, GetID) {
  pb::common::RawEngine id = RawRocksEngineTest::engine->GetID();
  EXPECT_EQ(id, pb::common::RawEngine::RAW_ENG_ROCKSDB);
}

TEST_F(RawRocksEngineTest, GetSnapshotReleaseSnapshot) {
  std::shared_ptr<Snapshot> snapshot = RawRocksEngineTest::engine->GetSnapshot();
  EXPECT_NE(snapshot.get(), nullptr);
}

TEST_F(RawRocksEngineTest, Flush) {
  const std::string &cf_name = kDefaultCf;

  // bugs if cf_name empty or not exists. crash
  RawRocksEngineTest::engine->Flush(cf_name);
}

TEST_F(RawRocksEngineTest, NewReader) {
  // cf empty
  {
    const std::string &cf_name = "";

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    EXPECT_EQ(reader.get(), nullptr);
  }

  // cf not exist
  {
    const std::string &cf_name = "12345";

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    EXPECT_EQ(reader.get(), nullptr);
  }

  // ok
  {
    const std::string &cf_name = kDefaultCf;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    EXPECT_NE(reader.get(), nullptr);
  }
}

TEST_F(RawRocksEngineTest, NewWriter) {
  // cf empty
  {
    const std::string &cf_name = "";

    std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

    EXPECT_EQ(writer.get(), nullptr);
  }

  // cf not exist
  {
    const std::string &cf_name = "12345";

    std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

    EXPECT_EQ(writer.get(), nullptr);
  }

  // ok
  {
    const std::string &cf_name = kDefaultCf;
    std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

    EXPECT_NE(writer.get(), nullptr);
  }
}

TEST_F(RawRocksEngineTest, KvPut) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    pb::common::KeyValue kv;

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // value empty allow
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("key2");
    kv.set_value("value2");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("key3");
    kv.set_value("value3");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvBatchPut) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key empty
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kvs;
    kv.set_key("");
    kv.set_value("value1");
    kvs.emplace_back(kv);

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kvs;

    kv.set_key("key1");
    kv.set_value("value1");
    kvs.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kvs.emplace_back(kv);

    kv.set_key("");
    kv.set_value("value3");
    kvs.emplace_back(kv);

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kvs;
    kv.set_key("key1");
    kv.set_value("value1");
    kvs.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kvs.emplace_back(kv);

    kv.set_key("key3");
    kv.set_value("value3");
    kvs.emplace_back(kv);

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string value1;
    std::string value2;
    std::string value3;
    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    ok = reader->KvGet("key1", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value1", value1);

    ok = reader->KvGet("key2", value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value2", value2);

    ok = reader->KvGet("key3", value3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value3", value3);
  }
}

TEST_F(RawRocksEngineTest, KvBatchPutAndDelete) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty failed
  {
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<pb::common::KeyValue> kv_deletes;

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key empty failed
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<pb::common::KeyValue> kv_deletes;
    kv.set_key("");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key empty failed
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<pb::common::KeyValue> kv_deletes;
    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    kv.set_key("");
    kv.set_value("value1");
    kv_deletes.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<pb::common::KeyValue> kv_deletes;

    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kv_puts.emplace_back(kv);

    kv.set_key("");
    kv.set_value("value3");
    kv_puts.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<pb::common::KeyValue> kv_deletes;

    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kv_puts.emplace_back(kv);

    kv.set_key("");
    kv.set_value("value3");
    kv_deletes.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // ok only deletes
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<pb::common::KeyValue> kv_deletes;

    kv.set_key("not_found_key");
    kv.set_value("value_not_found_key");
    kv_deletes.emplace_back(kv);

    kv.set_key("key1");
    kv.set_value("value1");
    kv_deletes.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kv_deletes.emplace_back(kv);

    kv.set_key("key3");
    kv.set_value("value3");
    kv_deletes.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string value0;
    std::string value1;
    std::string value2;
    std::string value3;
    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvGet("not_found_key", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet("key1", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet("key2", value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet("key3", value3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // ok puts and delete
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<pb::common::KeyValue> kv_deletes;
    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kv_puts.emplace_back(kv);

    kv.set_key("key3");
    kv.set_value("value3");
    kv_puts.emplace_back(kv);

    kv.set_key("key99");
    kv.set_value("value99");
    kv_puts.emplace_back(kv);

    ///////////////////////////////////////
    kv.set_key("key1");
    kv_deletes.emplace_back(kv);

    kv.set_key("key2");
    kv_deletes.emplace_back(kv);

    kv.set_key("key3");
    kv_deletes.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string value0;
    std::string value1;
    std::string value2;
    std::string value3;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvGet("key1", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet("key2", value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet("key3", value3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet("key99", value0);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value99", value0);
  }

  // ok only puts
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<pb::common::KeyValue> kv_deletes;
    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kv_puts.emplace_back(kv);

    kv.set_key("key3");
    kv.set_value("value3");
    kv_puts.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string value1;
    std::string value2;
    std::string value3;
    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    ok = reader->KvGet("key1", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value1", value1);

    ok = reader->KvGet("key2", value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value2", value2);

    ok = reader->KvGet("key3", value3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value3", value3);
  }
}

TEST_F(RawRocksEngineTest, KvGet) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

  // key empty
  {
    std::string key;
    std::string value;

    butil::Status ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  {
    const std::string &key = "key1";
    std::string value;

    butil::Status ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvCompareAndSet) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    pb::common::KeyValue kv;
    std::string value = "value123456";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // value empty . key not exist failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key");
    std::string value = "value";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // value empty . key exist current value not empty. failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");
    std::string value = "value123456";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1");
    const std::string &value = "value1_modify";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    std::string key = kv.key();
    std::string value_another;
    ok = reader->KvGet(key, value_another);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(value, value_another);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1_modify");
    const std::string &value = "";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    std::string key = kv.key();
    std::string value_another;
    ok = reader->KvGet(key, value_another);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(value, value_another);
  }
  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("");
    const std::string &value = "value1";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    std::string key = kv.key();
    std::string value_another;
    ok = reader->KvGet(key, value_another);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(value, value_another);
  }
}

// Batch implementation comparisons and settings.
// There are three layers of semantics:
// 1. If the key does not exist, set kv
// 2. The key exists and can be deleted
// 3. The key exists to update the value
// Not available internally, only for RPC use
TEST_F(RawRocksEngineTest, KvBatchCompareAndSet) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // keys empty expect_values emtpy
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    butil::Status ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // keys and expect_values size not equal
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;
    kvs.emplace_back(kv);

    butil::Status ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // keys key emtpy
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;
    kvs.emplace_back(kv);

    expect_values.resize(1);

    butil::Status ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key not exist in rocksdb
  // key not empty . expect_value empty. value empty
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;
    kv.set_key("KeyBatchCompareAndSet");
    kvs.emplace_back(kv);

    expect_values.resize(1);

    butil::Status ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string key = "KeyBatchCompareAndSet";
    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // key exist in rocksdb
  // key not empty . expect_value empty. value empty
  {
    butil::Status ok;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;
    kv.set_key("KeyBatchCompareAndSet");
    kvs.emplace_back(kv);

    expect_values.resize(1);

    ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string key = "KeyBatchCompareAndSet";
    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // key exist value not empty in rocksdb
  // key not empty . expect_value empty. value empty
  {
    butil::Status ok;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;
    kv.set_key("KeyBatchCompareAndSet");
    kvs.emplace_back(kv);

    expect_values.resize(1);

    kv.set_value("KeyBatchCompareAndSetValue");
    ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string key = "KeyBatchCompareAndSet";
    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = writer->KvDelete(kv.key());
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // keys not exist atomic ok
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    for (size_t i = 0; i < 3; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KeyBatchCompareAndSet" + std::to_string(i));
      kv.set_value("ValueBatchCompareAndSet" + std::to_string(i));
      kvs.emplace_back(kv);
    }

    expect_values.resize(3);

    butil::Status ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    for (size_t i = 0; i < 3; i++) {
      std::string key = "KeyBatchCompareAndSet" + std::to_string(i);
      std::string value;
      ok = reader->KvGet(key, value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ("ValueBatchCompareAndSet" + std::to_string(i), value);
      EXPECT_EQ(key_states[i], true);
    }
  }

  // keys exist delete . add keys not exist. update keys  atomic ok
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    // delete key
    for (size_t i = 0; i < 1; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KeyBatchCompareAndSet" + std::to_string(i));
      kv.set_value("");
      kvs.emplace_back(kv);
      expect_values.emplace_back("ValueBatchCompareAndSet" + std::to_string(i));
    }

    // update key
    for (size_t i = 1; i < 2; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KeyBatchCompareAndSet" + std::to_string(i));
      kv.set_value("ValueBatchCompareAndSet------" + std::to_string(i));
      kvs.emplace_back(kv);
      expect_values.emplace_back("ValueBatchCompareAndSet" + std::to_string(i));
    }

    // add key
    for (size_t i = 3; i < 4; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KeyBatchCompareAndSet" + std::to_string(i));
      kv.set_value("ValueBatchCompareAndSet" + std::to_string(i));
      kvs.emplace_back(kv);
      expect_values.emplace_back("");
    }

    butil::Status ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string key = "KeyBatchCompareAndSet" + std::to_string(0);
    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "KeyBatchCompareAndSet" + std::to_string(1);
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet------" + std::to_string(1), value);

    key = "KeyBatchCompareAndSet" + std::to_string(2);
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet" + std::to_string(2), value);

    key = "KeyBatchCompareAndSet" + std::to_string(3);
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet" + std::to_string(3), value);

    for (size_t i = 0; i < 3; i++) {
      EXPECT_EQ(key_states[i], true);
    }
  }

  // add keys again  atomic failed
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;
    key_states.clear();

    for (size_t i = 0; i < 4; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KeyBatchCompareAndSet" + std::to_string(i));
      kv.set_value("ValueBatchCompareAndSet" + std::to_string(i));
      kvs.emplace_back(kv);
      expect_values.emplace_back("");
    }

    butil::Status ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 4; i++) {
      EXPECT_EQ(key_states[i], false);
    }

    pb::common::Range range;
    range.set_start_key("KeyBatchCompareAndSet");
    range.set_end_key("KeyBatchCompareAndSeu");

    ok = writer->KvDeleteRange(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // keys not exist not atomic ok
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    for (size_t i = 0; i < 4; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KeyBatchCompareAndSet" + std::to_string(i));
      kv.set_value("ValueBatchCompareAndSet" + std::to_string(i));
      kvs.emplace_back(kv);
    }

    expect_values.resize(4);

    butil::Status ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    for (size_t i = 0; i < 4; i++) {
      std::string key = "KeyBatchCompareAndSet" + std::to_string(i);
      std::string value;
      ok = reader->KvGet(key, value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ("ValueBatchCompareAndSet" + std::to_string(i), value);
    }
  }

  // keys exist delete . add keys not exist. update keys  not atomic ok
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    // delete key
    for (size_t i = 0; i < 1; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KeyBatchCompareAndSet" + std::to_string(i));
      kv.set_value("");
      kvs.emplace_back(kv);
      expect_values.emplace_back("ValueBatchCompareAndSet" + std::to_string(i));
    }

    // update key
    for (size_t i = 1; i < 2; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KeyBatchCompareAndSet" + std::to_string(i));
      kv.set_value("ValueBatchCompareAndSet------" + std::to_string(i));
      kvs.emplace_back(kv);
      expect_values.emplace_back("ValueBatchCompareAndSet" + std::to_string(i));
    }

    // add key
    for (size_t i = 3; i < 4; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KeyBatchCompareAndSet" + std::to_string(i));
      kv.set_value("ValueBatchCompareAndSet" + std::to_string(i));
      kvs.emplace_back(kv);
      expect_values.emplace_back("");
    }

    butil::Status ok = writer->KvBatchCompareAndSet(kvs, expect_values, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string key = "KeyBatchCompareAndSet" + std::to_string(0);
    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "KeyBatchCompareAndSet" + std::to_string(1);
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet------" + std::to_string(1), value);

    key = "KeyBatchCompareAndSet" + std::to_string(2);
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet" + std::to_string(2), value);

    key = "KeyBatchCompareAndSet" + std::to_string(3);
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet" + std::to_string(3), value);

    for (size_t i = 0; i < 2; i++) {
      EXPECT_EQ(key_states[i], true);
    }

    EXPECT_EQ(key_states[2], false);

    pb::common::Range range;
    range.set_start_key("KeyBatchCompareAndSet");
    range.set_end_key("KeyBatchCompareAndSeu");

    ok = writer->KvDeleteRange(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

#if 0
TEST_F(RawRocksEngineTest, KvBatchGet) {

  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

  // key all empty
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(keys, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key some empty
  {
    std::vector<std::string> keys{"key1", "", "key"};
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(keys, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key not exist
  {
    std::vector<std::string> keys{"key1", "key2", "key", "key4"};
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(keys, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);
  }

  // normal
  {
    std::vector<std::string> keys{"key1", "key"};
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(keys, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}
#endif

TEST_F(RawRocksEngineTest, KvPutIfAbsent) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    pb::common::KeyValue kv;

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key exist value empty failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // value empty . key exist failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key10");
    kv.set_value("value10");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // key value already exist failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key10");
    kv.set_value("value10");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // key value already exist failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key10");
    kv.set_value("");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key11");
    kv.set_value("");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key11");
    kv.set_value("value11");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key11");
    kv.set_value("");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvBatchPutIfAbsentAtomic) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key all empty
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key not exist
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;
    kv.set_key("key10086");
    kv.set_value("value10086");
    // kv.set_key("key1");
    // kv.set_value("value1");
    kvs.push_back(kv);

    kv.set_key("");
    kv.set_value("value2");
    kvs.push_back(kv);

    butil::Status status = writer->KvBatchPutIfAbsent(kvs, key_states, true);
    EXPECT_TRUE((status.error_code() == pb::error::Errno::EKEY_EMPTY) ||
                (status.error_code() == pb::error::Errno::EINTERNAL));
  }

  // some key exist failed
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;

    kv.set_key("key111");
    kv.set_value("value111");
    kvs.push_back(kv);

    kv.set_key("key1");
    kv.set_value("value1");
    kvs.push_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kvs.push_back(kv);

    kv.set_key("key");
    kv.set_value("value");
    kvs.push_back(kv);

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::string value;
    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    ok = reader->KvGet("key111", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // normal key all not exist
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;

    kv.set_key("key101");
    kv.set_value("value101");
    kvs.push_back(kv);

    kv.set_key("key102");
    kv.set_value("value102");
    kvs.push_back(kv);

    kv.set_key("key103");
    kv.set_value("value103");
    kvs.push_back(kv);

    kv.set_key("key104");
    kv.set_value("value104");
    kvs.push_back(kv);

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    ok = reader->KvGet("key101", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key102", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key103", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key104", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvBatchPutIfAbsentNonAtomic) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key all empty
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key not exist
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1");
    kvs.push_back(kv);

    kv.set_key("");
    kv.set_value("value2");
    kvs.push_back(kv);

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key exist ok
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;

    kv.set_key("key1111");
    kv.set_value("value111");
    kvs.push_back(kv);

    kv.set_key("key1");
    kv.set_value("value1");
    kvs.push_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kvs.push_back(kv);

    kv.set_key("key");
    kv.set_value("value");
    kvs.push_back(kv);

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    ok = reader->KvGet("key1111", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key1", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key2", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal key all not exist
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;

    kv.set_key("key201");
    kv.set_value("value201");
    kvs.push_back(kv);

    kv.set_key("key202");
    kv.set_value("value202");
    kvs.push_back(kv);

    kv.set_key("key203");
    kv.set_value("value203");
    kvs.push_back(kv);

    kv.set_key("key204");
    kv.set_value("value204");
    kvs.push_back(kv);

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    ok = reader->KvGet("key201", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key202", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key203", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key204", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal key all  exist
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;

    kv.set_key("key201");
    kv.set_value("value201");
    kvs.push_back(kv);

    kv.set_key("key202");
    kv.set_value("value202");
    kvs.push_back(kv);

    kv.set_key("key203");
    kv.set_value("value203");
    kvs.push_back(kv);

    kv.set_key("key204");
    kv.set_value("value204");
    kvs.push_back(kv);

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());
  }

  // KvPutIfAbsent one data and KvBatchPutIfAbsent put two data
  {
    pb::common::KeyValue kv;
    kv.set_key("key205");
    kv.set_value("value205");
    bool key_state = false;

    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    kv.set_key("key205");
    kv.set_value("value205");
    kvs.push_back(kv);

    kv.set_key("key206");
    kv.set_value("value206");
    kvs.push_back(kv);

    kv.set_key("key207");
    kv.set_value("value207");
    kvs.push_back(kv);

    kv.set_key("key208");
    kv.set_value("value208");
    kvs.push_back(kv);

    ok = writer->KvBatchPutIfAbsent(kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    ok = reader->KvGet("key205", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key206", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key207", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet("key208", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvScan) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

  // start_key empty error
  {
    std::string start_key;
    std::string end_key;
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // start_key valid and end_key empty error
  {
    std::string start_key = "key";
    std::string end_key;
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    std::string start_key = "key101";
    std::string end_key = "key199";
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(start_key, end_key, kvs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }
  }

  // ok
  {
    std::string start_key = "key201";
    std::string end_key = "key204";
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }
  }
}

TEST_F(RawRocksEngineTest, KvCount) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

  // start_key empty error
  {
    std::string start_key;
    std::string end_key;
    int64_t count = 0;

    butil::Status ok = reader->KvCount(start_key, end_key, count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // start_key valid and end_key empty error
  {
    std::string start_key = "key101";
    std::string end_key;
    int64_t count = 0;

    butil::Status ok = reader->KvCount(start_key, end_key, count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    std::string start_key = "key201";
    std::string end_key = "key204";
    int64_t count = 0;

    butil::Status ok = reader->KvCount(start_key, end_key, count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << " count : " << count << std::endl;

    std::vector<pb::common::KeyValue> kvs;

    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, kvs.size());
  }
}

TEST_F(RawRocksEngineTest, KvDelete) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    std::string key;

    butil::Status ok = writer->KvDelete(key);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key not exist in rockdb
  {
    const std::string &key = "not_exist_key";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key1";

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    butil::Status ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // double delete ok
  {
    const std::string &key = "key1";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // ok
  {
    const std::string &key = "key";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key1";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key2";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key3";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key10";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key1111";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key101";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key102";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key103";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key104";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key201";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key202";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key203";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key204";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvBatchDelete) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // keys empty failed
  {
    std::vector<std::string> keys;

    butil::Status ok = writer->KvBatchDelete(keys);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    std::vector<std::string> keys;
    keys.emplace_back("key");
    keys.emplace_back("");

    butil::Status ok = writer->KvBatchDelete(keys);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    std::vector<pb::common::KeyValue> kvs;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("mykey" + std::to_string(i));
      kv.set_value("myvalue" + std::to_string(i));
      kvs.emplace_back(std::move(kv));
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(value, kvs[i].value());
    }

    std::vector<std::string> keys;
    keys.reserve(kvs.size());
    for (const auto &kv : kvs) {
      keys.emplace_back(kv.key());
    }

    ok = writer->KvBatchDelete(keys);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
    }
  }
}

TEST_F(RawRocksEngineTest, KvDeleteIfEqual) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key  empty failed
  {
    pb::common::KeyValue kv;

    butil::Status ok = writer->KvDeleteIfEqual(kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key not exist
  {
    pb::common::KeyValue kv;
    kv.set_key("key598");
    kv.set_value("value598");

    butil::Status ok = writer->KvDeleteIfEqual(kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // key exist value exist but value unequal
  {
    std::vector<pb::common::KeyValue> kvs;

    for (int i = 0; i < 1; i++) {
      pb::common::KeyValue kv;
      kv.set_key("myequalkey" + std::to_string(i));
      kv.set_value("myequalvalue" + std::to_string(i));
      kvs.emplace_back(std::move(kv));
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    for (int i = 0; i < 1; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(value, kvs[i].value());
    }

    for (auto &kv : kvs) {
      kv.set_value("243fgdfgd");
      ok = writer->KvDeleteIfEqual(kv);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);
    }
  }

  // ok
  {
    std::vector<pb::common::KeyValue> kvs;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("myequalkey" + std::to_string(i));
      kv.set_value("myequalvalue" + std::to_string(i));
      kvs.emplace_back(std::move(kv));
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(value, kvs[i].value());
    }

    for (const auto &kv : kvs) {
      ok = writer->KvDeleteIfEqual(kv);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    }

    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
    }
  }
}

TEST_F(RawRocksEngineTest, KvDeleteRange) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // write key -> key999
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 1000; i++) {
      pb::common::KeyValue kv;
      kv.set_key("key" + std::to_string(i));
      kv.set_value("value" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // key empty
  {
    pb::common::Range range;

    butil::Status ok = writer->KvDeleteRange(range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key not empty but end key empty
  {
    pb::common::Range range;
    range.set_start_key("key");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // ok
  {
    pb::common::Range range;
    range.set_start_key("key");
    range.set_end_key("key100");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string start_key = "key";
    std::string end_key = "key100";
    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    std::string key = "key";
    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "key100";
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::Range range;
    range.set_start_key("key100");
    range.set_end_key("key200");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string start_key = "key100";
    std::string end_key = "key200";
    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    std::string key = "key100";
    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "key200";
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::Range range;
    range.set_start_key("key");
    range.set_end_key("key99999");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string start_key = "key";
    std::string end_key = "key99999";
    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }
  }
}

TEST_F(RawRocksEngineTest, KvDeleteRangeWithRangeWithOptions) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // write KEY -> KEY10
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KEY" + std::to_string(i));
      kv.set_value("VALUE" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok delete KEY0 KEY1
  {
    pb::common::Range range;
    range.set_start_key("KEX");
    range.set_end_key("KEY10");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string key1 = "KEY0";
    std::string value1;
    ok = reader->KvGet(key1, value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    std::string key2 = "KEY1";
    std::string value2;
    ok = reader->KvGet(key2, value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    std::string start_key = "KEY";
    std::string end_key = "KEZ";
    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }
  }

  // ok delete KEY
  {
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEY");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string start_key = "KEY";
    std::string end_key = "KEZ";
    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(8, kvs.size());
  }

  // write KEY -> KEY10
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KEY" + std::to_string(i));
      kv.set_value("VALUE" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // write KEZ -> KEZ10
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KEZ" + std::to_string(i));
      kv.set_value("VALUE_Z" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string start_key = "KEY";
    std::string end_key = "KF0";
    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(10, kvs.size());
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string start_key = "KEY";
    std::string end_key = "KF0";
    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(10, kvs.size());
  }

  // write KEY -> KEY10
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KEY" + std::to_string(i));
      kv.set_value("VALUE" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // write KEZ -> KEZ10
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KEZ" + std::to_string(i));
      kv.set_value("VALUE_Z" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEX");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string start_key = "KEY";
    std::string end_key = "KF0";
    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(10, kvs.size());
  }
}

TEST_F(RawRocksEngineTest, KvBatchDeleteRange) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // write KEY -> KEY10
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KEY" + std::to_string(i));
      kv.set_value("VALUE" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // key empty failed
  {
    pb::common::Range range;
    butil::Status ok = writer->KvBatchDeleteRange({range});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key not empty but end key empty failed
  {
    pb::common::Range range;

    range.set_start_key("key");

    butil::Status ok = writer->KvBatchDeleteRange({range});

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // end key not empty but start key empty failed
  {
    pb::common::Range range;

    range.set_end_key("key");

    butil::Status ok = writer->KvBatchDeleteRange({range});

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key >  end key equal failed
  {
    pb::common::Range range;

    range.set_start_key("key");
    range.set_end_key("Key");

    butil::Status ok = writer->KvBatchDeleteRange({range});

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key ==  end key equal failed
  {
    pb::common::Range range;

    range.set_start_key("key");
    range.set_end_key("key");

    butil::Status ok = writer->KvBatchDeleteRange({range});

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key ==  end key equal failed
  {
    pb::common::Range range;

    range.set_start_key("key");
    range.set_end_key("key");

    butil::Status ok = writer->KvBatchDeleteRange({range});

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  {
    std::vector<pb::common::Range> ranges;

    // ok delete KEY
    {
      pb::common::Range range;
      range.set_start_key("KEY");
      range.set_end_key("KEY");
      ranges.emplace_back(range);
    }

    {
      // ok delete KEY
      pb::common::Range range;
      range.set_start_key("KEZ");
      range.set_end_key("KEZ");
      ranges.emplace_back(range);
    }

    butil::Status ok = writer->KvBatchDeleteRange(ranges);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string start_key = "KEY";
    std::string end_key = "KEZ";
    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(10, kvs.size());

    start_key = "KEZ";
    end_key = "KE[";
    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(20, kvs.size());
  }

  // write KEY -> KEY10
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KEY" + std::to_string(i));
      kv.set_value("VALUE" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // write KEZ -> KEZ10
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 10; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KEZ" + std::to_string(i));
      kv.set_value("VALUE_Z" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvBatchDeleteRange({range});

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEX");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvBatchDeleteRange({range});

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    std::shared_ptr<RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string start_key = "KEY";
    std::string end_key = "KF0";
    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(20, kvs.size());
  }

  {
    // ok delete KEY
    pb::common::Range range;
    char array[] = {static_cast<char>(0xFF), static_cast<char>(0xFF), static_cast<char>(0xFF), static_cast<char>(0xFF)};

    range.set_start_key(std::string(array, std::size(array)));
    range.set_end_key(std::string(array, std::size(array)));

    butil::Status ok = writer->KvBatchDeleteRange({range});

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }
}

TEST_F(RawRocksEngineTest, Iterator) {
  auto writer = RawRocksEngineTest::engine->NewWriter(kDefaultCf);
  pb::common::KeyValue kv;
  kv.set_key("bbbbbbbbbbbbb");
  kv.set_value(GenRandomString(256));
  writer->KvPut(kv);

  int count = 0;
  IteratorOptions options;
  options.upper_bound = "cccc";
  auto iter = RawRocksEngineTest::engine->NewIterator(kDefaultCf, options);
  for (iter->Seek("aaaaaaaaaa"); iter->Valid(); iter->Next()) {
    ++count;
  }

  EXPECT_GE(count, 1);
}

// TEST_F(RawRocksEngineTest, Checkpoint) {
//   auto writer = RawRocksEngineTest::engine->NewWriter(kDefaultCf);

//   pb::common::KeyValue kv;
//   for (int i = 0; i < 10000; ++i) {
//     kv.set_key(GenRandomString(32));
//     kv.set_value(GenRandomString(256));
//     writer->KvPut(kv);
//   }

//   auto checkpoint = RawRocksEngineTest::engine->NewCheckpoint();

//   const std::string store_path = std::filesystem::path(RawRocksEngineTest::engine->DbPath()).parent_path().string();
//   std::filesystem::create_directories(store_path);
//   const std::string checkpoint_path = store_path + "/checkpoint_" + std::to_string(Helper::Timestamp());
//   std::cout << "checkpoint_path: " << checkpoint_path << std::endl;
//   //  checkpoint->Create("/tmp/dingo-store/data/store/checkpoint");
//   std::vector<pb::store_internal::SstFileInfo> sst_files;
//   auto status = checkpoint->Create(checkpoint_path, RawRocksEngineTest::engine->GetColumnFamily(kDefaultCf),
//   sst_files); EXPECT_EQ(true, status.ok());

//   std::filesystem::remove_all(checkpoint_path);
// }

// TEST_F(RawRocksEngineTest, Ingest) {
//   auto reader = RawRocksEngineTest::engine->NewReader(kDefaultCf);
//   auto writer = RawRocksEngineTest::engine->NewWriter(kDefaultCf);

//   const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
//   pb::common::KeyValue kv;
//   for (int i = 0; i < 10000; ++i) {
//     int pos = i % prefixs.size();

//     kv.set_key(prefixs[pos] + GenRandomString(30));
//     kv.set_value(GenRandomString(256));
//     writer->KvPut(kv);
//   }

//   auto checkpoint = RawRocksEngineTest::engine->NewCheckpoint();

//   const std::string store_path = std::filesystem::path(RawRocksEngineTest::engine->DbPath()).parent_path().string();
//   std::filesystem::create_directories(store_path);
//   const std::string checkpoint_path = store_path + "/checkpoint_" + std::to_string(Helper::Timestamp());
//   std::cout << "checkpoint_path: " << checkpoint_path << std::endl;

//   std::vector<pb::store_internal::SstFileInfo> sst_files;
//   auto status = checkpoint->Create(checkpoint_path, RawRocksEngineTest::engine->GetColumnFamily(kDefaultCf),
//   sst_files); EXPECT_EQ(true, status.ok());

//   pb::common::Range range;
//   range.set_start_key("bb");
//   range.set_end_key("cc");

//   std::vector<std::string> files;
//   for (auto& sst_file : sst_files) {
//     std::cout << "sst file path: " << sst_file.path() << " " << sst_file.start_key() << "-" << sst_file.end_key()
//               << std::endl;
//     if (sst_file.start_key() < range.end_key() && range.start_key() < sst_file.end_key()) {
//       std::cout << "pick up: " << sst_file.path() << std::endl;
//       files.push_back(sst_file.path());
//     }
//   }

//   int64_t count = 0;
//   reader->KvCount(range.start_key(), range.end_key(), count);
//   std::cout << "count before delete: " << count << std::endl;

//   writer->KvDeleteRange(range);

//   reader->KvCount(range.start_key(), range.end_key(), count);
//   std::cout << "count after delete: " << count << std::endl;

//   RawRocksEngineTest::engine->IngestExternalFile(kDefaultCf, files);

//   reader->KvCount(range.start_key(), range.end_key(), count);
//   std::cout << "count after ingest: " << count << std::endl;
// }

}  // namespace dingodb

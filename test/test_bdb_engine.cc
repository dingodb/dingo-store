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

// #include <dirent.h>
#include <gtest/gtest.h>
// #include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
// #include <filesystem>
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
#include "engine/raw_bdb_engine.h"
#include "proto/common.pb.h"
#include "proto/store_internal.pb.h"
#include "server/server.h"

namespace dingodb {

static const std::string kDefaultCf = "default";
// static const std::string kDefaultCf = "d";

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
    "  path: ./bdb_unit_test\n"
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

class RawBdbEngineTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    std::srand(std::time(nullptr));

    Helper::CreateDirectories("./bdb_unit_test/bdb");

    config = std::make_shared<YamlConfig>();
    if (config->Load(kYamlConfigContent) != 0) {
      std::cout << "Load config failed" << '\n';
      return;
    }

    engine = std::make_shared<RawBdbEngine>();
    if (!engine->Init(config, {})) {
      std::cout << "RawBdbEngine init failed" << '\n';
    }
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory("./bdb_unit_test");
  }

  void SetUp() override {}

  void TearDown() override {}

  static std::shared_ptr<RawBdbEngine> engine;
  static std::shared_ptr<Config> config;
};

std::shared_ptr<RawBdbEngine> RawBdbEngineTest::engine = nullptr;
std::shared_ptr<Config> RawBdbEngineTest::config = nullptr;

TEST_F(RawBdbEngineTest, GetName) {
  std::string name = RawBdbEngineTest::engine->GetName();
  EXPECT_EQ(name, "RAW_ENG_BDB");
}

TEST_F(RawBdbEngineTest, GetID) {
  pb::common::RawEngine id = RawBdbEngineTest::engine->GetID();
  EXPECT_EQ(id, pb::common::RawEngine::RAW_ENG_BDB);
}

TEST_F(RawBdbEngineTest, GetSnapshotReleaseSnapshot) {
  std::shared_ptr<Snapshot> snapshot = RawBdbEngineTest::engine->GetSnapshot();
  EXPECT_NE(snapshot.get(), nullptr);
}

TEST_F(RawBdbEngineTest, NewReader) {
  // ok
  {
    auto reader = RawBdbEngineTest::engine->Reader();

    EXPECT_NE(reader.get(), nullptr);
  }
}

TEST_F(RawBdbEngineTest, NewWriter) {
  // ok
  {
    auto writer = RawBdbEngineTest::engine->Writer();

    EXPECT_NE(writer.get(), nullptr);
  }
}

TEST_F(RawBdbEngineTest, KvPut) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  if (writer == nullptr) {
    std::cout << "writer is null ptr." << std::endl;
    EXPECT_EQ(111, pb::error::Errno::EKEY_EMPTY);
    return;
  }

  // key empty
  {
    pb::common::KeyValue kv;

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // value empty allow
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("key2");
    kv.set_value("value2");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
  {
    pb::common::KeyValue kv;
    kv.set_key("key3");
    kv.set_value("value3");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawBdbEngineTest, KvBatchPut) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // key empty
  {
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = writer->KvBatchPut(cf_name, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key empty
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kvs;
    kv.set_key("");
    kv.set_value("value1");
    kvs.emplace_back(kv);

    butil::Status ok = writer->KvBatchPut(cf_name, kvs);
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

    butil::Status ok = writer->KvBatchPut(cf_name, kvs);
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

    butil::Status ok = writer->KvBatchPut(cf_name, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string value1;
    std::string value2;
    std::string value3;
    auto reader = RawBdbEngineTest::engine->Reader();
    ok = reader->KvGet(cf_name, "key1", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value1", value1);

    ok = reader->KvGet(cf_name, "key2", value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value2", value2);

    ok = reader->KvGet(cf_name, "key3", value3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value3", value3);
  }
}

TEST_F(RawBdbEngineTest, KvBatchPutAndDelete) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // key empty failed
  {
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<pb::common::KeyValue> kv_deletes;

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, kv_deletes);
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, kv_deletes);
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, kv_deletes);
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, kv_deletes);
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, kv_deletes);
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string value0;
    std::string value1;
    std::string value2;
    std::string value3;
    auto reader = RawBdbEngineTest::engine->Reader();

    ok = reader->KvGet(cf_name, "not_found_key", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet(cf_name, "key1", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet(cf_name, "key2", value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet(cf_name, "key3", value3);
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string value0;
    std::string value1;
    std::string value2;
    std::string value3;

    auto reader = RawBdbEngineTest::engine->Reader();

    ok = reader->KvGet(cf_name, "key1", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet(cf_name, "key2", value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet(cf_name, "key3", value3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    ok = reader->KvGet(cf_name, "key99", value0);
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string value1;
    std::string value2;
    std::string value3;
    auto reader = RawBdbEngineTest::engine->Reader();
    ok = reader->KvGet(cf_name, "key1", value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value1", value1);

    ok = reader->KvGet(cf_name, "key2", value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value2", value2);

    ok = reader->KvGet(cf_name, "key3", value3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value3", value3);
  }
}

TEST_F(RawBdbEngineTest, KvGet) {
  const std::string &cf_name = kDefaultCf;
  auto reader = RawBdbEngineTest::engine->Reader();

  // key empty
  {
    std::string key;
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  {
    const std::string &key = "key3";
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(value, "value3");
    std::cout << key << " | " << value << std::endl;
  }

  {
    const std::string &key = "key2";
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(value, "value2");
    std::cout << key << " | " << value << std::endl;
  }

  {
    const std::string &key = "key10010";
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
    std::cout << key << " | " << value << std::endl;
  }
}

TEST_F(RawBdbEngineTest, KvCompareAndSet) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // key empty
  {
    pb::common::KeyValue kv;
    std::string value = "value123456";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(cf_name, kv, value, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // value empty . key not exist failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key");
    std::string value = "value";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(cf_name, kv, value, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // value empty . key exist current value not empty. failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");
    std::string value = "value123456";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(cf_name, kv, value, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1");
    const std::string &value = "value1_modify";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(cf_name, kv, value, key_state);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();
    std::string key = kv.key();
    std::string value_another;
    ok = reader->KvGet(cf_name, key, value_another);
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

    butil::Status ok = writer->KvCompareAndSet(cf_name, kv, value, key_state);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();
    std::string key = kv.key();
    std::string value_another;
    ok = reader->KvGet(cf_name, key, value_another);
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

    butil::Status ok = writer->KvCompareAndSet(cf_name, kv, value, key_state);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();
    std::string key = kv.key();
    std::string value_another;
    ok = reader->KvGet(cf_name, key, value_another);
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
TEST_F(RawBdbEngineTest, KvBatchCompareAndSet) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // keys empty expect_values emtpy
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    butil::Status ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // keys and expect_values size not equal
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;
    kvs.emplace_back(kv);

    butil::Status ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, true);
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

    butil::Status ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key not exist in bdb
  // key not empty . expect_value empty. value empty
  {
    std::vector<pb::common::KeyValue> kvs;
    std::vector<std::string> expect_values;
    std::vector<bool> key_states;

    pb::common::KeyValue kv;
    kv.set_key("KeyBatchCompareAndSet");
    kvs.emplace_back(kv);

    expect_values.resize(1);

    butil::Status ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string key = "KeyBatchCompareAndSet";
    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // key exist in bdb
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

    ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string key = "KeyBatchCompareAndSet";
    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // key exist value not empty in bdb
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
    ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string key = "KeyBatchCompareAndSet";
    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = writer->KvDelete(cf_name, kv.key());
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

    butil::Status ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    for (size_t i = 0; i < 3; i++) {
      std::string key = "KeyBatchCompareAndSet" + std::to_string(i);
      std::string value;
      ok = reader->KvGet(cf_name, key, value);
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

    butil::Status ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string key = "KeyBatchCompareAndSet" + std::to_string(0);
    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "KeyBatchCompareAndSet" + std::to_string(1);
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet------" + std::to_string(1), value);

    key = "KeyBatchCompareAndSet" + std::to_string(2);
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet" + std::to_string(2), value);

    key = "KeyBatchCompareAndSet" + std::to_string(3);
    ok = reader->KvGet(cf_name, key, value);
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

    butil::Status ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 4; i++) {
      EXPECT_EQ(key_states[i], false);
    }

    pb::common::Range range;
    range.set_start_key("KeyBatchCompareAndSet");
    range.set_end_key("KeyBatchCompareAndSeu");

    ok = writer->KvDeleteRange(cf_name, range);
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

    butil::Status ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    for (size_t i = 0; i < 4; i++) {
      std::string key = "KeyBatchCompareAndSet" + std::to_string(i);
      std::string value;
      ok = reader->KvGet(cf_name, key, value);
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

    butil::Status ok = writer->KvBatchCompareAndSet(cf_name, kvs, expect_values, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string key = "KeyBatchCompareAndSet" + std::to_string(0);
    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "KeyBatchCompareAndSet" + std::to_string(1);
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet------" + std::to_string(1), value);

    key = "KeyBatchCompareAndSet" + std::to_string(2);
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet" + std::to_string(2), value);

    key = "KeyBatchCompareAndSet" + std::to_string(3);
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("ValueBatchCompareAndSet" + std::to_string(3), value);

    for (size_t i = 0; i < 2; i++) {
      EXPECT_EQ(key_states[i], true);
    }

    EXPECT_EQ(key_states[2], false);

    pb::common::Range range;
    range.set_start_key("KeyBatchCompareAndSet");
    range.set_end_key("KeyBatchCompareAndSeu");

    ok = writer->KvDeleteRange(cf_name, range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawBdbEngineTest, KvPutIfAbsent) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // key empty
  {
    pb::common::KeyValue kv;

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key exist value empty failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // value empty . key exist failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key1");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key10");
    kv.set_value("value10");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // key value already exist failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key10");
    kv.set_value("value10");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // key value already exist failed
  {
    pb::common::KeyValue kv;
    kv.set_key("key10");
    kv.set_value("");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key11");
    kv.set_value("");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key11");
    kv.set_value("value11");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // normal
  {
    pb::common::KeyValue kv;
    kv.set_key("key11");
    kv.set_value("");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawBdbEngineTest, KvBatchPutIfAbsentAtomic) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // key all empty
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    butil::Status ok = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, true);
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

    butil::Status status = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, true);
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

    butil::Status ok = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::string value;
    auto reader = RawBdbEngineTest::engine->Reader();
    ok = reader->KvGet(cf_name, "key111", value);
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

    butil::Status ok = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, true);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string value;
    ok = reader->KvGet(cf_name, "key101", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key102", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key103", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key104", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawBdbEngineTest, KvBatchPutIfAbsentNonAtomic) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // key all empty
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    butil::Status ok = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, false);
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

    butil::Status ok = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, false);
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

    butil::Status ok = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string value;
    ok = reader->KvGet(cf_name, "key1111", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key1", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key2", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key", value);
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

    butil::Status ok = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string value;
    ok = reader->KvGet(cf_name, "key201", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key202", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key203", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key204", value);
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

    butil::Status ok = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());
  }

  // KvPutIfAbsent one data and KvBatchPutIfAbsent put two data
  {
    pb::common::KeyValue kv;
    kv.set_key("key205");
    kv.set_value("value205");
    bool key_state = false;

    butil::Status ok = writer->KvPutIfAbsent(cf_name, kv, key_state);
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

    ok = writer->KvBatchPutIfAbsent(cf_name, kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string value;
    ok = reader->KvGet(cf_name, "key205", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key206", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key207", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = reader->KvGet(cf_name, "key208", value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawBdbEngineTest, KvScan) {
  const std::string &cf_name = kDefaultCf;
  auto reader = RawBdbEngineTest::engine->Reader();

  // start_key empty error
  {
    std::string start_key;
    std::string end_key;
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // start_key valid and end_key empty error
  {
    std::string start_key = "key";
    std::string end_key;
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    std::string start_key = "key101";
    std::string end_key = "key199";
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(cf_name, start_key, end_key, kvs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << '\n';
    for (const auto &kv : kvs) {
      std::cout << "KvScan ret: " << kv.key() << ":" << kv.value() << '\n';
    }
  }

  // ok
  {
    std::string start_key = "key201";
    std::string end_key = "key204";
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << '\n';
    for (const auto &kv : kvs) {
      std::cout << "KvScan ret: " << kv.key() << ":" << kv.value() << '\n';
    }
  }
}

TEST_F(RawBdbEngineTest, KvCount) {
  const std::string &cf_name = kDefaultCf;
  auto reader = RawBdbEngineTest::engine->Reader();

  // start_key empty error
  {
    std::string start_key;
    std::string end_key;
    int64_t count = 0;

    butil::Status ok = reader->KvCount(cf_name, start_key, end_key, count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // start_key valid and end_key empty error
  {
    std::string start_key = "key101";
    std::string end_key;
    int64_t count = 0;

    butil::Status ok = reader->KvCount(cf_name, start_key, end_key, count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    std::string start_key = "key201";
    std::string end_key = "key204";
    int64_t count = 0;

    butil::Status ok = reader->KvCount(cf_name, start_key, end_key, count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << " count : " << count << '\n';

    std::vector<pb::common::KeyValue> kvs;

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, kvs.size());
  }
}

TEST_F(RawBdbEngineTest, KvDelete) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // key empty
  {
    std::string key;

    butil::Status ok = writer->KvDelete(cf_name, key);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key not exist in rockdb
  {
    const std::string &key = "not_exist_key";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key1";

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string value;
    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // double delete ok
  {
    const std::string &key = "key1";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
  }

  // ok
  {
    const std::string &key = "key";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key1";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key2";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key3";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key10";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key1111";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key101";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key102";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key103";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key104";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key201";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key202";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key203";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key204";

    butil::Status ok = writer->KvDelete(cf_name, key);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawBdbEngineTest, KvBatchDelete) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // keys empty failed
  {
    std::vector<std::string> keys;

    butil::Status ok = writer->KvBatchDelete(cf_name, keys);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    std::vector<std::string> keys;
    keys.emplace_back("key");
    keys.emplace_back("");

    butil::Status ok = writer->KvBatchDelete(cf_name, keys);
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

    butil::Status ok = writer->KvBatchPut(cf_name, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string value;
    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(cf_name, kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(value, kvs[i].value());
    }

    std::vector<std::string> keys;
    keys.reserve(kvs.size());
    for (const auto &kv : kvs) {
      keys.emplace_back(kv.key());
    }

    ok = writer->KvBatchDelete(cf_name, keys);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(cf_name, kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
    }
  }
}

TEST_F(RawBdbEngineTest, KvDeleteIfEqual) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // key  empty failed
  {
    pb::common::KeyValue kv;

    butil::Status ok = writer->KvDeleteIfEqual(cf_name, kv);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key not exist
  {
    pb::common::KeyValue kv;
    kv.set_key("key598");
    kv.set_value("value598");

    butil::Status ok = writer->KvDeleteIfEqual(cf_name, kv);
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

    butil::Status ok = writer->KvBatchPut(cf_name, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();
    std::string value;
    for (int i = 0; i < 1; i++) {
      ok = reader->KvGet(cf_name, kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(value, kvs[i].value());
    }

    for (auto &kv : kvs) {
      kv.set_value("243fgdfgd");
      ok = writer->KvDeleteIfEqual(cf_name, kv);
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

    butil::Status ok = writer->KvBatchPut(cf_name, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    auto reader = RawBdbEngineTest::engine->Reader();

    std::string value;
    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(cf_name, kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(value, kvs[i].value());
    }

    for (const auto &kv : kvs) {
      ok = writer->KvDeleteIfEqual(cf_name, kv);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    }

    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(cf_name, kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
    }
  }
}  // KvDeleteIfEqual

TEST_F(RawBdbEngineTest, KvDeleteRange) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

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

    butil::Status ok = writer->KvBatchPut(cf_name, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // key empty
  {
    pb::common::Range range;

    butil::Status ok = writer->KvDeleteRange(cf_name, range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key not empty but end key empty
  {
    pb::common::Range range;
    range.set_start_key("key");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // ok
  {
    pb::common::Range range;
    range.set_start_key("key");
    range.set_end_key("key100");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string start_key = "key";
    std::string end_key = "key100";
    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawBdbEngineTest::engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << '\n';
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << '\n';
    }

    std::string key = "key";
    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "key100";
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::Range range;
    range.set_start_key("key100");
    range.set_end_key("key200");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string start_key = "key100";
    std::string end_key = "key200";
    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawBdbEngineTest::engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << '\n';
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << '\n';
    }

    std::string key = "key100";
    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "key200";
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok
  {
    pb::common::Range range;
    range.set_start_key("key");
    range.set_end_key("key99999");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string start_key = "key";
    std::string end_key = "key99999";
    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawBdbEngineTest::engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << '\n';
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << '\n';
    }
  }
}

TEST_F(RawBdbEngineTest, Iterator) {
  auto writer = RawBdbEngineTest::engine->Writer();
  {
    pb::common::KeyValue kv;
    kv.set_key("bbbbbbbbbbbbb");
    kv.set_value("123456789123456789");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("bbbbdddddd");
    kv.set_value("22222222");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    const std::string &cf_name = kDefaultCf;
    auto reader = RawBdbEngineTest::engine->Reader();

    std::string start_key = "bbb";
    std::string end_key = "ddd";
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(cf_name, start_key, end_key, kvs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << '\n';
    for (const auto &kv : kvs) {
      std::cout << "KvScan ret: " << kv.key() << ":" << kv.value() << '\n';
    }
  }

  int count = 0;
  IteratorOptions options;
  options.upper_bound = "cccc";
  {
    count = 0;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->Seek("aaaaaaaaaa"); iter->Valid(); iter->Next()) {
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << std::endl;
      ++count;
    }

    EXPECT_EQ(count, 2);

    std::cout << "--------- iter reuse ---------" << std::endl;

    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << std::endl;
      ++count;
    }
    EXPECT_EQ(count, 2);
  }

  std::cout << "------------------" << std::endl;

  {
    pb::common::KeyValue kv;
    kv.set_key("bbbbaaaaaaaa");
    kv.set_value("1111111111111");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("bbbbcccc");
    kv.set_value("2222222222222");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    count = 0;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ++count;
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count << std::endl;
    }

    EXPECT_EQ(count, 4);
  }

  {
    count = 0;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ++count;
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count << std::endl;
    }

    EXPECT_EQ(count, 4);
  }
}

TEST_F(RawBdbEngineTest, GetApproximateSizes) {
  auto writer = RawBdbEngineTest::engine->Writer();
  {
    pb::common::KeyValue kv;
    kv.set_key("111aaa");
    kv.set_value("123456789123456789");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("222bbb");
    kv.set_value("22222222");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("333ccc");
    kv.set_value("1111111111111");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("444ddd");
    kv.set_value("2222222222222");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    int count = 0;

    IteratorOptions options;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ++count;
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count << std::endl;
    }
  }

  std::cout << "-----------------------------------" << std::endl;
  {
    int count = 0;

    IteratorOptions options;
    options.upper_bound = "55";
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->Seek("00"); iter->Valid(); iter->Next()) {
      ++count;
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count << std::endl;
    }
  }

  {
    pb::common::Range range;
    range.set_start_key("00");
    range.set_end_key("55");

    std::vector<pb::common::Range> ranges;
    ranges.push_back(range);

    std::vector<int64_t> count_vec = RawBdbEngineTest::engine->GetApproximateSizes(kDefaultCf, ranges);

    EXPECT_EQ(count_vec.size(), 1);
    if (count_vec.size() >= 1) {
      // EXPECT_EQ(count_vec[0], 4);
      // it is an estimated count
      std::cout << "GetApproximateSizes, cout: " << count_vec[0] << std::endl;
    }
  }
}

TEST_F(RawBdbEngineTest, Compact) {
  const std::string &cf_name = kDefaultCf;

  butil::Status status = RawBdbEngineTest::engine->Compact(cf_name);
  EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
}

TEST_F(RawBdbEngineTest, Flush) {
  const std::string &cf_name = kDefaultCf;

  // bugs if cf_name empty or not exists. crash
  RawBdbEngineTest::engine->Flush(cf_name);
}

TEST_F(RawBdbEngineTest, IngestExternalFile) {
  const std::string &cf_name = kDefaultCf;

  std::string sst_file_name = "/tmp/sst_file.sst";
  if (Helper::IsExistPath(sst_file_name)) {
    std::cout << "find sst file: " << sst_file_name << std::endl;
    std::vector<std::string> sst_files;
    sst_files.push_back(sst_file_name);
    butil::Status status = RawBdbEngineTest::engine->IngestExternalFile(cf_name, sst_files);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);

    std::string value;
    auto reader = RawBdbEngineTest::engine->Reader();

    // "key_099", "value_099" record must be in sst files
    status = reader->KvGet(cf_name, "key_099", value);
    EXPECT_EQ(status.error_code(), pb::error::Errno::OK);
    EXPECT_EQ("value_099", value);
  } else {
    std::cout << "Warning: cannot find sst file: " << sst_file_name << ", skip IngestExternalFile test!" << std::endl;
  }
}

}  // namespace dingodb


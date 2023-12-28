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
#include "engine/bdb_raw_engine.h"
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

    engine = std::make_shared<BdbRawEngine>();
    if (!engine->Init(config, {})) {
      std::cout << "BdbRawEngine init failed" << '\n';
    }
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory("./bdb_unit_test");
  }

  void SetUp() override {}

  void TearDown() override {}

  static std::shared_ptr<BdbRawEngine> engine;
  static std::shared_ptr<Config> config;
};

std::shared_ptr<BdbRawEngine> RawBdbEngineTest::engine = nullptr;
std::shared_ptr<Config> RawBdbEngineTest::config = nullptr;

TEST_F(RawBdbEngineTest, GetName) {
  std::string name = RawBdbEngineTest::engine->GetName();
  EXPECT_EQ(name, "RAW_ENG_BDB");
}

TEST_F(RawBdbEngineTest, GetID) {
  pb::common::RawEngine id = RawBdbEngineTest::engine->GetRawEngineType();
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
    std::cout << "writer is null ptr." << '\n';
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key empty
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kvs;
    kv.set_key("");
    kv.set_value("value1");
    kvs.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
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
    std::vector<std::string> key_deletes;

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key empty failed
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> key_deletes;
    kv.set_key("");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key empty failed
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> key_deletes;
    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    key_deletes.emplace_back("");

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> key_deletes;

    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kv_puts.emplace_back(kv);

    kv.set_key("");
    kv.set_value("value3");
    kv_puts.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> key_deletes;

    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kv_puts.emplace_back(kv);

    key_deletes.emplace_back("");

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // ok only deletes
  {
    pb::common::KeyValue kv;
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> keys_deletes;

    keys_deletes.emplace_back("not_found_key");
    keys_deletes.emplace_back("key1");
    keys_deletes.emplace_back("key2");
    keys_deletes.emplace_back("key3");

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, keys_deletes);
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
    std::vector<std::string> key_deletes;
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
    key_deletes.emplace_back("key1");

    key_deletes.emplace_back("key2");

    key_deletes.emplace_back("key3");

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
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
    std::vector<std::string> key_deletes;
    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    kv.set_key("key2");
    kv.set_value("value2");
    kv_puts.emplace_back(kv);

    kv.set_key("key3");
    kv.set_value("value3");
    kv_puts.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
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
    std::cout << key << " | " << value << '\n';
  }

  {
    const std::string &key = "key2";
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(value, "value2");
    std::cout << key << " | " << value << '\n';
  }

  {
    const std::string &key = "key10010";
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
    std::cout << key << " | " << value << '\n';
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, {}, keys);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    std::vector<std::string> keys;
    keys.emplace_back("key");
    keys.emplace_back("");

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, {}, keys);
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
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

    ok = writer->KvBatchPutAndDelete(cf_name, {}, keys);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(cf_name, kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
    }
  }
}

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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
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
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << '\n';
      ++count;
    }

    EXPECT_EQ(count, 2);

    std::cout << "--------- iter reuse ---------" << '\n';

    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << '\n';
      ++count;
    }
    EXPECT_EQ(count, 2);
  }

  std::cout << "------------------" << '\n';

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
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count << '\n';
    }

    EXPECT_EQ(count, 4);
  }

  {
    count = 0;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ++count;
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count << '\n';
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
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count << '\n';
    }
  }

  std::cout << "-----------------------------------" << '\n';
  {
    int count = 0;

    IteratorOptions options;
    options.upper_bound = "55";
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->Seek("00"); iter->Valid(); iter->Next()) {
      ++count;
      std::cout << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count << '\n';
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
    if (!count_vec.empty()) {
      // EXPECT_EQ(count_vec[0], 4);
      // it is an estimated count
      std::cout << "GetApproximateSizes, cout: " << count_vec[0] << '\n';
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
    std::cout << "find sst file: " << sst_file_name << '\n';
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
    std::cout << "Warning: cannot find sst file: " << sst_file_name << ", skip IngestExternalFile test!" << '\n';
  }
}

}  // namespace dingodb

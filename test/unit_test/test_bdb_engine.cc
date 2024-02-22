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

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config.h"
#include "config/yaml_config.h"
#include "engine/bdb_raw_engine.h"
#include "engine/snapshot.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"

namespace dingodb {

DEFINE_uint32(bdb_test_max_count, 30000, "bdb_test_max_count");

static const std::string kDefaultCf = "default";

static const std::string kTempDataDirectory = "./unit_test/bdb_unit_test";
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
    "store:\n"
    "  path: ./unit_test/bdb_unit_test\n"
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

    Helper::CreateDirectories(kTempDataDirectory);

    config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    engine = std::make_shared<BdbRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, {}));
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory(kTempDataDirectory);
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

TEST_F(RawBdbEngineTest, BdbHelper) {
  char cf_id = 0;

  cf_id = bdb::BdbHelper::GetCfId("default");
  EXPECT_EQ(cf_id, '0');

  cf_id = bdb::BdbHelper::GetCfId("meta");
  EXPECT_EQ(cf_id, '1');

  cf_id = bdb::BdbHelper::GetCfId("vector_scalar");
  EXPECT_EQ(cf_id, '2');

  cf_id = bdb::BdbHelper::GetCfId("vector_table");
  EXPECT_EQ(cf_id, '3');

  cf_id = bdb::BdbHelper::GetCfId("data");
  EXPECT_EQ(cf_id, '4');

  cf_id = bdb::BdbHelper::GetCfId("lock");
  EXPECT_EQ(cf_id, '5');

  cf_id = bdb::BdbHelper::GetCfId("write");
  EXPECT_EQ(cf_id, '6');

  std::string cf_name;

  cf_name = bdb::BdbHelper::GetCfName('0');
  EXPECT_EQ(cf_name, "default");

  cf_name = bdb::BdbHelper::GetCfName('1');
  EXPECT_EQ(cf_name, "meta");

  cf_name = bdb::BdbHelper::GetCfName('2');
  EXPECT_EQ(cf_name, "vector_scalar");

  cf_name = bdb::BdbHelper::GetCfName('3');
  EXPECT_EQ(cf_name, "vector_table");

  cf_name = bdb::BdbHelper::GetCfName('4');
  EXPECT_EQ(cf_name, "data");

  cf_name = bdb::BdbHelper::GetCfName('5');
  EXPECT_EQ(cf_name, "lock");

  cf_name = bdb::BdbHelper::GetCfName('6');
  EXPECT_EQ(cf_name, "write");
}

TEST_F(RawBdbEngineTest, DbtCompare) {
  Dbt dbt1, dbt2;

  // Test case 1: dbt1 and dbt2 are equal
  std::string data = "test_data";
  dbt1.set_data((void *)data.data());
  dbt1.set_size(data.size());
  dbt2.set_data((void *)data.data());
  dbt2.set_size(data.size());
  EXPECT_EQ(bdb::BdbHelper::CompareDbt(dbt1, dbt2), 0);

  // Test case 2: dbt1 is less than dbt2
  std::string data1 = "test_data1";
  std::string data2 = "test_data2";
  dbt1.set_data((void *)data1.data());
  dbt1.set_size(data1.size());
  dbt2.set_data((void *)data2.data());
  dbt2.set_size(data2.size());
  EXPECT_LT(bdb::BdbHelper::CompareDbt(dbt1, dbt2), 0);

  // Test case 3: dbt1 is greater than dbt2
  dbt1.set_data((void *)data2.data());
  dbt1.set_size(data2.size());
  dbt2.set_data((void *)data1.data());
  dbt2.set_size(data1.size());
  EXPECT_GT(bdb::BdbHelper::CompareDbt(dbt1, dbt2), 0);
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
    LOG(INFO) << "writer is null ptr.";
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
    LOG(INFO) << key << " | " << value;
  }

  {
    const std::string &key = "key2";
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(value, "value2");
    LOG(INFO) << key << " | " << value;
  }

  {
    const std::string &key = "key10010";
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);
    LOG(INFO) << key << " | " << value;
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
    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << "KvScan ret: " << kv.key() << ":" << kv.value();
    }
  }

  // ok
  {
    std::string start_key = "key201";
    std::string end_key = "key204";
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << "KvScan ret: " << kv.key() << ":" << kv.value();
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

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key << " count : " << count;

    std::vector<pb::common::KeyValue> kvs;

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    for (const auto &kv : kvs) {
      LOG(INFO) << "KvScan ret: " << kv.key() << ":" << kv.value();
    }
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

TEST_F(RawBdbEngineTest, KvDeleteRange1) {
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
}

TEST_F(RawBdbEngineTest, KvDeleteRange2) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // key empty
  {
    pb::common::Range range;

    butil::Status ok = writer->KvDeleteRange(cf_name, range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }
}

TEST_F(RawBdbEngineTest, KvDeleteRange3) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

  // start key not empty but end key empty
  {
    pb::common::Range range;
    range.set_start_key("key");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }
}

TEST_F(RawBdbEngineTest, KvDeleteRange4) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

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

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }

    std::string key = "key";
    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "key100";
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawBdbEngineTest, KvDeleteRange5) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

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

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }

    std::string key = "key100";
    std::string value;
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    key = "key200";
    ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(RawBdbEngineTest, KvDeleteRange6) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();

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

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }
}

TEST_F(RawBdbEngineTest, Iterator1) {
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
    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << "KvScan ret: " << kv.key() << ":" << kv.value();
    }
  }

  int count = 0;
  IteratorOptions options;
  options.upper_bound = "cccc";
  {
    count = 0;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->Seek("aaaaaaaaaa"); iter->Valid(); iter->Next()) {
      LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value();
      ++count;
    }

    EXPECT_EQ(count, 2);

    LOG(INFO) << "--------- iter reuse ---------";

    count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value();
      ++count;
    }
    EXPECT_EQ(count, 2);
  }

  LOG(INFO) << "------------------";

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
      LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
    }

    EXPECT_EQ(count, 4);
  }

  {
    count = 0;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ++count;
      LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
    }

    EXPECT_EQ(count, 4);
  }
}

TEST_F(RawBdbEngineTest, Iterator2) {
  auto writer = RawBdbEngineTest::engine->Writer();
  {
    pb::common::KeyValue kv;
    kv.set_key("tttt_01");
    kv.set_value("123456789123456789");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("tttt_02");
    kv.set_value("22222222");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("tttt_03");
    kv.set_value("3333333");
    writer->KvPut(kDefaultCf, kv);
  }

  {
    const std::string &cf_name = kDefaultCf;
    auto reader = RawBdbEngineTest::engine->Reader();

    std::string start_key = "ttt";
    std::string end_key = "uuu";
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(cf_name, start_key, end_key, kvs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;

    int32_t count = 0;
    for (const auto &kv : kvs) {
      LOG(INFO) << "KvScan ret: " << kv.key() << ":" << kv.value();
      count++;
    }

    EXPECT_EQ(count, 3);
  }

  LOG(INFO) << "------------------scan1";

  {
    {
      pb::common::KeyValue kv;
      kv.set_key("tttt_03");
      kv.set_value("555555");
      writer->KvPut(kDefaultCf, kv);
    }
    LOG(INFO) << "write tttt_03 to 555555";

    {
      auto snapshot = engine->GetSnapshot();
      LOG(INFO) << "create snapshot";
    }

    const std::string &cf_name = kDefaultCf;
    auto reader = RawBdbEngineTest::engine->Reader();

    std::string start_key = "ttt";
    std::string end_key = "uuu";
    std::vector<pb::common::KeyValue> kvs;

    // butil::Status ok = reader->KvScan(cf_name, snapshot, start_key, end_key, kvs);
    butil::Status ok = reader->KvScan(cf_name, start_key, end_key, kvs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;

    int32_t count = 0;
    for (const auto &kv : kvs) {
      LOG(INFO) << "KvScan ret: " << kv.key() << ":" << kv.value();
      count++;
    }
  }

  LOG(INFO) << "------------------scan2";

  int count = 0;
  IteratorOptions options;
  options.upper_bound = "uuu";
  {
    count = 0;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);

    {
      pb::common::KeyValue kv;
      kv.set_key("tttt_03");
      kv.set_value("4444444");
      writer->KvPut(kDefaultCf, kv);
    }

    LOG(INFO) << "count=" << count;
    for (iter->Seek("tttt_01"); iter->Valid(); iter->Next()) {
      count = count + 1;
      LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << ", count=" << count;

      {
        pb::common::KeyValue kv;
        kv.set_key("tttt_03");
        kv.set_value("test+" + std::to_string(count));
        writer->KvPut(kDefaultCf, kv);
      }
    }

    EXPECT_EQ(count, 3);
  }

  LOG(INFO) << "------------------1";

  {
    count = 0;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->Seek("tttt_01"); iter->Valid(); iter->Next()) {
      LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value();
      ++count;
    }

    EXPECT_EQ(count, 3);
  }

  LOG(INFO) << "------------------2";

  {
    count = 0;
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->Seek("tttt_02"); iter->Valid(); iter->Next()) {
      LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value();
      ++count;
    }

    EXPECT_EQ(count, 2);
  }

  LOG(INFO) << "------------------3";
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
      LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
    }
  }

  LOG(INFO) << "-----------------------------------";
  {
    int count = 0;

    IteratorOptions options;
    options.upper_bound = "55";
    auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
    for (iter->Seek("00"); iter->Valid(); iter->Next()) {
      ++count;
      LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
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
      LOG(INFO) << "GetApproximateSizes, cout: " << count_vec[0];
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
    LOG(INFO) << "find sst file: " << sst_file_name;
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
    LOG(INFO) << "Warning: cannot find sst file: " << sst_file_name << ", skip IngestExternalFile test!";
  }
}

TEST_F(RawBdbEngineTest, KvDeleteRange7) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();
  auto reader = RawBdbEngineTest::engine->Reader();

  // write data
  {
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> key_deletes;

    for (int i = 0; i < 100; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KvDeleteRange7" + std::to_string(1000 + i));
      kv.set_value(kv.key());
      kv_puts.emplace_back(kv);
    }

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, "KvDeleteRange7", "KvDeleteRange8", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 100);
  }

  // test snapshot isolation
  // we do a special snapshot operation in our bdb engine implementation, which use a cursor do a pre-seek
  // to make sure the snapshot isolation is activated.
  // this is a limitation of bdb, if we do not do this, the snapshot isolation will not be activated, and will perform
  // as read committed isolation.
  {
    pb::common::Range range;
    range.set_start_key("KvDeleteRange71010");
    range.set_end_key("KvDeleteRange71030");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, "KvDeleteRange7", "KvDeleteRange8", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 80);

    range.set_start_key("KvDeleteRange71090");
    range.set_end_key("KvDeleteRange8");

    ok = writer->KvDeleteRange(cf_name, range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = reader->KvCount(kDefaultCf, "KvDeleteRange7", "KvDeleteRange8", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 70);

    range.set_start_key("KvDeleteRange71080");
    range.set_end_key("KvDeleteRange71081");

    ok = writer->KvDeleteRange(cf_name, range);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    ok = reader->KvCount(kDefaultCf, "KvDeleteRange7", "KvDeleteRange8", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 69);

    // test if iterator can see the deleted data
    {
      int count = 0;

      IteratorOptions options;
      options.upper_bound = "KvDeleteRange8";
      auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
      // iter->SeekToFirst();

      range.set_start_key("KvDeleteRange71070");
      range.set_end_key("KvDeleteRange71080");
      ok = writer->KvDeleteRange(cf_name, range);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

      for (iter->Seek("KvDeleteRange7"); iter->Valid(); iter->Next()) {
        ++count;
        // LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
      }

      EXPECT_EQ(count, 69);
    }

    // test if iterator with a pre-created snapshot can see the deleted data
    {
      int count = 0;

      auto snapshot = engine->GetSnapshot();
      // {
      //   IteratorOptions options;
      //   options.upper_bound = "KvDeleteRange8";
      //   auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, snapshot, options);
      //   iter->SeekToFirst();
      // }

      range.set_start_key("KvDeleteRange71060");
      range.set_end_key("KvDeleteRange71070");
      ok = writer->KvDeleteRange(cf_name, range);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

      IteratorOptions options;
      options.upper_bound = "KvDeleteRange8";
      auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, snapshot, options);

      for (iter->Seek("KvDeleteRange7"); iter->Valid(); iter->Next()) {
        ++count;
        // LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
      }

      EXPECT_EQ(count, 59);
    }

    /// test if iterator with a pre-created snapshot can see the committed data
    {
      int count = 0;

      auto snapshot = engine->GetSnapshot();

      IteratorOptions options;
      options.upper_bound = "KvDeleteRange8";
      auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, snapshot, options);

      for (iter->Seek("KvDeleteRange7"); iter->Valid(); iter->Next()) {
        ++count;
        // LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
      }

      EXPECT_EQ(count, 49);
    }

    // test the delete range with no keys
    {
      int count = 0;

      auto snapshot = engine->GetSnapshot();

      range.set_start_key("KvDeleteRange8");
      range.set_end_key("KvDeleteRange9");
      ok = writer->KvDeleteRange(cf_name, range);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

      IteratorOptions options;
      options.upper_bound = "KvDeleteRange8";
      auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, snapshot, options);

      for (iter->Seek("KvDeleteRange7"); iter->Valid(); iter->Next()) {
        ++count;
        // LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
      }

      EXPECT_EQ(count, 49);

      count = 0;
      auto iter2 = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, snapshot, options);
      for (iter->Seek("KvDeleteRange7"); iter->Valid(); iter->Next()) {
        ++count;
        // LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
      }

      EXPECT_EQ(count, 49);
    }

    // test iterator value is correct
    {
      int count = 0;

      auto snapshot = engine->GetSnapshot();

      std::string start_key = "KvDeleteRange71038";
      std::string end_key = "KvDeleteRange71040";

      IteratorOptions options;
      options.upper_bound = "KvDeleteRange71040";
      auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, snapshot, options);

      std::vector<std::string> keys;
      std::vector<std::string> values;
      for (iter->Seek("KvDeleteRange71038"); iter->Valid(); iter->Next()) {
        ++count;
        LOG(INFO) << "kv: " << iter->Key() << " | " << iter->Value() << " | " << count;
        keys.emplace_back(iter->Key());
        values.emplace_back(iter->Value());
      }

      EXPECT_EQ(keys.size(), 2);
      EXPECT_EQ(values.size(), 2);

      EXPECT_EQ(keys.at(0), "KvDeleteRange71038");
      EXPECT_EQ(values.at(0), "KvDeleteRange71038");

      EXPECT_EQ(keys.at(1), "KvDeleteRange71039");
      EXPECT_EQ(values.at(1), "KvDeleteRange71039");

      std::vector<pb::common::KeyValue> kvs;
      ok = reader->KvScan(cf_name, start_key, end_key, kvs);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(kvs.size(), 2);
      EXPECT_EQ(kvs.at(0).key(), "KvDeleteRange71038");
      EXPECT_EQ(kvs.at(0).value(), "KvDeleteRange71038");
      EXPECT_EQ(kvs.at(1).key(), "KvDeleteRange71039");
      EXPECT_EQ(kvs.at(1).value(), "KvDeleteRange71039");
    }

    // std::string start_key = "key";
    // std::string end_key = "key99999";
    // std::vector<pb::common::KeyValue> kvs;

    // auto reader = RawBdbEngineTest::engine->Reader();

    // ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    // EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // LOG(INFO) << "start_key : " << start_key << " "
    //           << "end_key : " << end_key;
    // for (const auto &kv : kvs) {
    //   LOG(INFO) << kv.key() << ":" << kv.value();
    // }
  }
}

TEST_F(RawBdbEngineTest, KvDeleteRange8) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();
  auto reader = RawBdbEngineTest::engine->Reader();

  // write data
  {
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> key_deletes;

    for (int i = 0; i < 100; i++) {
      pb::common::KeyValue kv;

      if (i < 50) {
        kv.set_key("KvDeleteRange8AA" + std::to_string(1000 + i));
      } else {
        kv.set_key("KvDeleteRange8BB" + std::to_string(1000 + i));
      }
      kv.set_value(kv.key());
      kv_puts.emplace_back(kv);
    }

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, "KvDeleteRange8", "KvDeleteRange9", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 100);

    ok = reader->KvCount(kDefaultCf, "KvDeleteRange8AA1048", "KvDeleteRange8BB", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 2);
  }

  // test snapshot isolation
  // we do a special snapshot operation in our bdb engine implementation, which use a cursor do a pre-seek
  // to make sure the snapshot isolation is activated.
  // this is a limitation of bdb, if we do not do this, the snapshot isolation will not be activated, and will perform
  // as read committed isolation.
  {
    pb::common::Range range;
    range.set_start_key("KvDeleteRange8AA1048");
    range.set_end_key("KvDeleteRange8BB");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, "KvDeleteRange8AA1048", "KvDeleteRange8BB", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 0);

    ok = reader->KvCount(kDefaultCf, "KvDeleteRange8AA1047", "KvDeleteRange8BB", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 1);
  }
}

// Test KvDeleteRange bigger than 64KB
TEST_F(RawBdbEngineTest, KvDeleteRangeA) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();
  auto reader = RawBdbEngineTest::engine->Reader();

  // write data
  uint64_t keys_count = 64 * 1024;
  {
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> key_deletes;

    for (int i = 0; i < keys_count; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KvDeleteRangeA" + std::to_string(1000 + i));
      kv.set_value(kv.key());
      kv_puts.emplace_back(kv);
    }

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, "KvDeleteRangeA", "KvDeleteRangeB", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, keys_count);
  }

  {
    pb::common::Range range;
    range.set_start_key("KvDeleteRangeA");
    range.set_end_key("KvDeleteRangeB");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, "KvDeleteRangeA", "KvDeleteRangeB", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 0);
  }

  // write data
  keys_count = 128 * 1024;
  {
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> key_deletes;

    for (int i = 0; i < keys_count; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KvDeleteRangeA" + std::to_string(1000 + i));
      kv.set_value(kv.key());
      kv_puts.emplace_back(kv);
    }

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, "KvDeleteRangeA", "KvDeleteRangeB", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, keys_count);
  }

  {
    pb::common::Range range;
    range.set_start_key("KvDeleteRangeA");
    range.set_end_key("KvDeleteRangeB");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, "KvDeleteRangeA", "KvDeleteRangeB", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, 0);
  }

  // write data
  keys_count = 128 * 1024;
  {
    std::vector<pb::common::KeyValue> kv_puts;
    std::vector<std::string> key_deletes;

    for (int i = 0; i < keys_count; i++) {
      pb::common::KeyValue kv;
      kv.set_key("KvDeleteRangeA" + std::to_string(1000 + i));
      kv.set_value(kv.key());
      kv_puts.emplace_back(kv);
    }

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, "KvDeleteRangeA", "KvDeleteRangeB", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, keys_count);
  }

  {
    pb::common::Range range;
    range.set_start_key("KvDeleteRangeA");
    range.set_end_key("KvDeleteRangeB");

    dingodb::SnapshotPtr snapshot = nullptr;

    std::thread([&]() { snapshot = engine->GetSnapshot(); }).join();

    std::thread([&]() {
      int64_t count = 0;
      auto ok = reader->KvCount(kDefaultCf, snapshot, "KvDeleteRangeA", "KvDeleteRangeB", count);
      EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
      EXPECT_EQ(count, keys_count);
    }).join();

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    int64_t count = 0;
    ok = reader->KvCount(kDefaultCf, snapshot, "KvDeleteRangeA", "KvDeleteRangeB", count);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, keys_count);
  }
}

TEST_F(RawBdbEngineTest, MaxTxnNums) {
  DINGO_LOG(ERROR) << "MaxTxnNums start0";

  const std::string &cf_name = kDefaultCf;
  auto writer = RawBdbEngineTest::engine->Writer();
  auto reader = RawBdbEngineTest::engine->Reader();

  // when a iterator is alive, the snapshot transaction will keep increasing until the last page in the cache created by
  // the transaction is evicted from the cache.
  {
    auto iter1 = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, IteratorOptions());

    DINGO_LOG(ERROR) << "MaxTxnNums start1";

    for (int x = 0; x < FLAGS_bdb_test_max_count; x++) {
      // write data
      uint64_t keys_count = 2;
      {
        std::vector<pb::common::KeyValue> kv_puts;
        std::vector<std::string> key_deletes;

        for (int i = 0; i < keys_count; i++) {
          pb::common::KeyValue kv;
          kv.set_key("MaxTxnNums0" + std::to_string(1000 + i));
          kv.set_value(kv.key());
          kv_puts.emplace_back(kv);
        }

        butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
        ASSERT_EQ(ok.error_code(), pb::error::Errno::OK);

        int64_t count = 0;
        ok = reader->KvCount(kDefaultCf, "MaxTxnNums0", "MaxTxnNums1", count);
        ASSERT_EQ(ok.error_code(), pb::error::Errno::OK);
        ASSERT_EQ(count, keys_count);
      }
    }
  }

  DINGO_LOG(ERROR) << "11111==========================================iter is "
                      "destroyed=================================================";
  // bthread_usleep(10000000);
  // DINGO_LOG(ERROR) << "22222==========================================iter is "
  //                     "destroyed=================================================";

  // bthread_usleep(10000000);

  // DINGO_LOG(ERROR) << "33333==========================================iter is "
  //                     "destroyed=================================================";

  // need to observe the snapshot transaction count in the LogBDBMsg log print.
  {
    DINGO_LOG(ERROR) << "MaxTxnNums start2";

    for (int x = 0; x < FLAGS_bdb_test_max_count; x++) {
      // write data
      uint64_t keys_count = 2;
      {
        std::vector<pb::common::KeyValue> kv_puts;
        std::vector<std::string> key_deletes;

        for (int i = 0; i < keys_count; i++) {
          pb::common::KeyValue kv;
          kv.set_key("MaxTxnNums0" + std::to_string(1000 + i));
          kv.set_value(kv.key());
          kv_puts.emplace_back(kv);
        }

        butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
        ASSERT_EQ(ok.error_code(), pb::error::Errno::OK);

        int64_t count = 0;
        ok = reader->KvCount(kDefaultCf, "MaxTxnNums0", "MaxTxnNums1", count);
        ASSERT_EQ(ok.error_code(), pb::error::Errno::OK);
        ASSERT_EQ(count, keys_count);
      }
    }
  }

  DINGO_LOG(ERROR) << "MaxTxnNums start iter";

  {
    IteratorOptions options;
    for (int j = 0; j < 1000; j++) {
      std::vector<std::shared_ptr<dingodb::Iterator>> iters;
      for (int i = 0; i < 10; i++) {
        auto iter = RawBdbEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
        if (iter == nullptr) {
          DINGO_LOG(ERROR) << "iter is nullptr";
        }
        iters.emplace_back(iter);
        DINGO_LOG(INFO) << "i: " << i << " j: " << j;
      }

      DINGO_LOG(INFO) << "j: " << j << " iters.size(): " << iters.size();

      // bthread_usleep(2000000);
    }
  }

  DINGO_LOG(ERROR) << "MaxTxnNums end";
}

}  // namespace dingodb

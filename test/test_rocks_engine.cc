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

#if 0
void Getfilepath(const char *path, const char *filename, char *filepath) {
  strcpy(filepath, path);  // NOLINT

  if (filepath[strlen(path) - 1] != '/') strcat(filepath, "/");  // NOLINT

  strcat(filepath, filename);  // NOLINT
}

bool DeleteFile(const char *path) {
  DIR *dir;
  struct dirent *dirinfo;
  struct stat statbuf;
  char filepath[256] = {0};
  lstat(path, &statbuf);

  if (S_ISREG(statbuf.st_mode)) {
    remove(path);
  } else if (S_ISDIR(statbuf.st_mode)) {
    if ((dir = opendir(path)) == nullptr) return true;
    while ((dirinfo = readdir(dir)) != nullptr) {
      Getfilepath(path, dirinfo->d_name, filepath);
      if (strcmp(dirinfo->d_name, ".") == 0 ||
          strcmp(dirinfo->d_name, "..") == 0)
        continue;
      DeleteFile(filepath);
      rmdir(filepath);
    }
    closedir(dir);
  }
  rmdir(path);
  return true;
}
#endif

static const std::string kDefaultCf = "default";
// static const std::string &kDefaultCf = "meta";

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

// rand string
std::string GenRandomString(int len) {
  std::string result;
  int alphabet_len = sizeof(kAlphabet);

  std::default_random_engine def_engine;
  std::uniform_int_distribution<> distrib(1, 1000000000);
  for (int i = 0; i < len; ++i) {
    result.append(1, kAlphabet[distrib(def_engine) % alphabet_len]);
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
    "  heartbeatInterval: 10000 # ms\n"
    "raft:\n"
    "  host: 127.0.0.1\n"
    "  port: 23100\n"
    "  path: /tmp/dingo-store/data/store/raft\n"
    "  electionTimeout: 1000 # ms\n"
    "  snapshotInterval: 3600 # s\n"
    "log:\n"
    "  logPath: /tmp/dingo-store/log\n"
    "store:\n"
    "  dbPath: /tmp/dingo-store/data/store/db\n"
    "  base:\n"
    "    block_size: 131072\n"
    "    block_cache: 67108864\n"
    "    arena_block_size: 67108864\n"
    "    min_write_buffer_number_to_merge: 4\n"
    "    max_write_buffer_number: 4\n"
    "    max_compaction_bytes: 134217728\n"
    "    write_buffer_size: 4194304\n"
    "    prefix_extractor: 8\n"
    "    max_bytes_for_level_base: 41943040\n"
    "    target_file_size_base: 4194304\n"
    "  default:\n"
    "  instruction:\n"
    "    max_write_buffer_number: 3\n"
    "  columnFamilies:\n"
    "    - default\n"
    "    - meta\n"
    "    - instruction\n";

class RawRocksEngineTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::srand(std::time(nullptr));

    std::shared_ptr<dingodb::Config> config = std::make_shared<dingodb::YamlConfig>();
    if (config->Load(kYamlConfigContent) != 0) {
      std::cout << "Load config failed" << std::endl;
      return;
    }

    engine = std::make_shared<dingodb::RawRocksEngine>();
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

  static std::shared_ptr<dingodb::RawRocksEngine> engine;
};

std::shared_ptr<dingodb::RawRocksEngine> RawRocksEngineTest::engine = nullptr;

TEST_F(RawRocksEngineTest, GetName) {
  std::string name = RawRocksEngineTest::engine->GetName();
  EXPECT_EQ(name, "RAW_ENG_ROCKSDB");
}

TEST_F(RawRocksEngineTest, GetID) {
  dingodb::pb::common::RawEngine id = RawRocksEngineTest::engine->GetID();
  EXPECT_EQ(id, dingodb::pb::common::RawEngine::RAW_ENG_ROCKSDB);
}

TEST_F(RawRocksEngineTest, GetSnapshot$ReleaseSnapshot) {
  std::shared_ptr<dingodb::Snapshot> snapshot = RawRocksEngineTest::engine->GetSnapshot();
  EXPECT_NE(snapshot.get(), nullptr);

  // RawRocksEngineTest::engine->ReleaseSnapshot(snapshot);

  // bugs crash
  // RawRocksEngineTest::engine->ReleaseSnapshot({});
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

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    EXPECT_EQ(reader.get(), nullptr);
  }

  // cf not exist
  {
    const std::string &cf_name = "12345";

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    EXPECT_EQ(reader.get(), nullptr);
  }

  // ok
  {
    const std::string &cf_name = kDefaultCf;

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    EXPECT_NE(reader.get(), nullptr);
  }
}

TEST_F(RawRocksEngineTest, NewWriter) {
  // cf empty
  {
    const std::string &cf_name = "";

    std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

    EXPECT_EQ(writer.get(), nullptr);
  }

  // cf not exist
  {
    const std::string &cf_name = "12345";

    std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

    EXPECT_EQ(writer.get(), nullptr);
  }

  // ok
  {
    const std::string &cf_name = kDefaultCf;
    std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

    EXPECT_NE(writer.get(), nullptr);
  }
}

TEST_F(RawRocksEngineTest, KvPut) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    dingodb::pb::common::KeyValue kv;

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // value empty allow
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key1");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key2");
    kv.set_value("value2");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key3");
    kv.set_value("value3");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvBatchPut) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    std::vector<dingodb::pb::common::KeyValue> kvs;

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // key empty
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    kv.set_key("");
    kv.set_value("value1");
    kvs.emplace_back(kv);

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kvs;

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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kvs;
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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string value1;
    std::string value2;
    std::string value3;
    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    ok = reader->KvGet("key1", value1);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ("value1", value1);

    ok = reader->KvGet("key2", value2);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ("value2", value2);

    ok = reader->KvGet("key3", value3);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ("value3", value3);
  }
}

TEST_F(RawRocksEngineTest, KvBatchPutAndDelete) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty failed
  {
    std::vector<dingodb::pb::common::KeyValue> kv_puts;
    std::vector<dingodb::pb::common::KeyValue> kv_deletes;

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // key empty failed
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kv_puts;
    std::vector<dingodb::pb::common::KeyValue> kv_deletes;
    kv.set_key("");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // key empty failed
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kv_puts;
    std::vector<dingodb::pb::common::KeyValue> kv_deletes;
    kv.set_key("key1");
    kv.set_value("value1");
    kv_puts.emplace_back(kv);

    kv.set_key("");
    kv.set_value("value1");
    kv_deletes.emplace_back(kv);

    butil::Status ok = writer->KvBatchPutAndDelete(kv_puts, kv_deletes);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kv_puts;
    std::vector<dingodb::pb::common::KeyValue> kv_deletes;

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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kv_puts;
    std::vector<dingodb::pb::common::KeyValue> kv_deletes;

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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // ok only deletes
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kv_puts;
    std::vector<dingodb::pb::common::KeyValue> kv_deletes;

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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string value0;
    std::string value1;
    std::string value2;
    std::string value3;
    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvGet("not_found_key", value1);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);

    ok = reader->KvGet("key1", value1);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);

    ok = reader->KvGet("key2", value2);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);

    ok = reader->KvGet("key3", value3);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);
  }

  // ok puts and delete
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kv_puts;
    std::vector<dingodb::pb::common::KeyValue> kv_deletes;
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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string value0;
    std::string value1;
    std::string value2;
    std::string value3;

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvGet("key1", value1);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);

    ok = reader->KvGet("key2", value2);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);

    ok = reader->KvGet("key3", value3);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);

    ok = reader->KvGet("key99", value0);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ("value99", value0);
  }

  // ok only puts
  {
    dingodb::pb::common::KeyValue kv;
    std::vector<dingodb::pb::common::KeyValue> kv_puts;
    std::vector<dingodb::pb::common::KeyValue> kv_deletes;
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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string value1;
    std::string value2;
    std::string value3;
    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    ok = reader->KvGet("key1", value1);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ("value1", value1);

    ok = reader->KvGet("key2", value2);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ("value2", value2);

    ok = reader->KvGet("key3", value3);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ("value3", value3);
  }
}

TEST_F(RawRocksEngineTest, KvGet) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

  // key empty
  {
    std::string key;
    std::string value;

    butil::Status ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  {
    const std::string &key = "key1";
    std::string value;

    butil::Status ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvCompareAndSet) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    dingodb::pb::common::KeyValue kv;
    std::string value = "value123456";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // value empty . key not exist failed
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key");
    std::string value = "value";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);
  }

  // value empty . key exist current value not empty. failed
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key1");
    std::string value = "value123456";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EINTERNAL);
  }

  // normal
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1");
    const std::string &value = "value1_modify";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    std::string key = kv.key();
    std::string value_another;
    ok = reader->KvGet(key, value_another);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ(value, value_another);
  }

  // normal
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1_modify");
    const std::string &value = "";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    std::string key = kv.key();
    std::string value_another;
    ok = reader->KvGet(key, value_another);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ(value, value_another);
  }
  // normal
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("");
    const std::string &value = "value1";
    bool key_state;

    butil::Status ok = writer->KvCompareAndSet(kv, value, key_state);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    std::string key = kv.key();
    std::string value_another;
    ok = reader->KvGet(key, value_another);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ(value, value_another);
  }
}

#if 0
TEST_F(RawRocksEngineTest, KvBatchGet) {

  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

  // key all empty
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(keys, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // key some empty
  {
    std::vector<std::string> keys{"key1", "", "key"};
    std::vector<dingodb::pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(keys, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // some key not exist
  {
    std::vector<std::string> keys{"key1", "key2", "key", "key4"};
    std::vector<dingodb::pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(keys, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EINTERNAL);
  }

  // normal
  {
    std::vector<std::string> keys{"key1", "key"};
    std::vector<dingodb::pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(keys, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }
}
#endif

TEST_F(RawRocksEngineTest, KvPutIfAbsent) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    dingodb::pb::common::KeyValue kv;

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // key exist value empty failed
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // value empty . key exist failed
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key1");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // normal
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key10");
    kv.set_value("value10");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // key value already exist failed
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key10");
    kv.set_value("value10");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // key value already exist failed
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key10");
    kv.set_value("");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // normal
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key11");
    kv.set_value("");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // normal
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key11");
    kv.set_value("value11");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // normal
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key11");
    kv.set_value("");

    bool key_state;
    butil::Status ok = writer->KvPutIfAbsent(kv, key_state);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvBatchPutIfAbsentAtomic) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key all empty
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, true);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // some key not exist
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    dingodb::pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1");
    kvs.push_back(kv);

    kv.set_key("");
    kv.set_value("value2");
    kvs.push_back(kv);

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, true);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // some key exist failed
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    dingodb::pb::common::KeyValue kv;

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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EINTERNAL);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::string value;
    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);
    ok = reader->KvGet("key111", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);
  }

  // normal key all not exist
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    dingodb::pb::common::KeyValue kv;

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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    ok = reader->KvGet("key101", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    ok = reader->KvGet("key102", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    ok = reader->KvGet("key103", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    ok = reader->KvGet("key104", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvBatchPutIfAbsentNonAtomic) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key all empty
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // some key not exist
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    dingodb::pb::common::KeyValue kv;
    kv.set_key("key1");
    kv.set_value("value1");
    kvs.push_back(kv);

    kv.set_key("");
    kv.set_value("value2");
    kvs.push_back(kv);

    butil::Status ok = writer->KvBatchPutIfAbsent(kvs, key_states, false);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // some key exist ok
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    dingodb::pb::common::KeyValue kv;

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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    ok = reader->KvGet("key1111", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // normal key all not exist
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    dingodb::pb::common::KeyValue kv;

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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    ok = reader->KvGet("key201", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    ok = reader->KvGet("key202", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    ok = reader->KvGet("key203", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    ok = reader->KvGet("key204", value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // normal key all  exist
  {
    std::vector<std::string> keys;
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    dingodb::pb::common::KeyValue kv;

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
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ(kvs.size(), key_states.size());
  }
}

TEST_F(RawRocksEngineTest, KvScan) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

  // start_key empty error
  {
    std::string start_key;
    std::string end_key;
    std::vector<dingodb::pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // start_key valid and end_key empty error
  {
    std::string start_key = "key";
    std::string end_key;
    std::vector<dingodb::pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    std::string start_key = "key101";
    std::string end_key = "key199";
    std::vector<dingodb::pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(start_key, end_key, kvs);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
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
    std::vector<dingodb::pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }
  }
}

TEST_F(RawRocksEngineTest, KvCount) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

  // start_key empty error
  {
    std::string start_key;
    std::string end_key;
    int64_t count = 0;

    butil::Status ok = reader->KvCount(start_key, end_key, count);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // start_key valid and end_key empty error
  {
    std::string start_key = "key101";
    std::string end_key;
    int64_t count = 0;

    butil::Status ok = reader->KvCount(start_key, end_key, count);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    std::string start_key = "key201";
    std::string end_key = "key204";
    int64_t count = 0;

    butil::Status ok = reader->KvCount(start_key, end_key, count);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << " count : " << count << std::endl;

    std::vector<dingodb::pb::common::KeyValue> kvs;

    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    EXPECT_EQ(count, kvs.size());
  }
}

// TEST_F(RawRocksEngineTest, CreateReader) {
//   dingodb::RocksEngine &engine = rocks_engine_test->GetRawRocksEngine();

//   // Context empty
//   {
//     std::shared_ptr<dingodb::Context> ctx;

//     std::shared_ptr<dingodb::EngineReader> reader = RawRocksEngineTest::engine->CreateReader({});
//     EXPECT_EQ(reader.get(), nullptr);
//   }

//   std::shared_ptr<dingodb::Context> ctx = std::make_shared<dingodb::Context>();

//   // Context not empty, but Context name empty
//   {
//     std::shared_ptr<dingodb::EngineReader> reader = RawRocksEngineTest::engine->CreateReader(ctx);
//     EXPECT_EQ(reader.get(), nullptr);
//   }

//   // Context not empty, but Context name not exist
//   {
//     ctx->set_cf_name("dummy");

//     std::shared_ptr<dingodb::EngineReader> reader = RawRocksEngineTest::engine->CreateReader(ctx);
//     EXPECT_EQ(reader.get(), nullptr);
//   }

//   const std::string &cf_name = kDefaultCf;
//   ctx->set_cf_name(cf_name);

//   // ok
//   {
//     std::shared_ptr<dingodb::EngineReader> reader = RawRocksEngineTest::engine->CreateReader(ctx);
//     EXPECT_NE(reader.get(), nullptr);
//   }
// }

// TEST_F(RawRocksEngineTest, EngineReader) {
//   dingodb::RocksEngine &engine = rocks_engine_test->GetRawRocksEngine();
//   const std::string &cf_name = kDefaultCf;
//   std::shared_ptr<dingodb::Context> ctx = std::make_shared<dingodb::Context>();
//   ctx->set_cf_name(cf_name);

//   std::shared_ptr<dingodb::EngineReader> reader = RawRocksEngineTest::engine->CreateReader(ctx);

//   // GetSelf
//   {
//     std::shared_ptr<dingodb::EngineReader> reader_another = reader->GetSelf();
//     EXPECT_EQ(2, reader_another.use_count());
//   }

//   // test in another unit. ingore
//   { std::shared_ptr<dingodb::EngineIterator> iter = reader->Scan("", ""); }

//   {
//     const std::string &key = "key";
//     auto value = reader->KvGet(key);
//     EXPECT_EQ("value", *value);
//   }

//   {
//     const std::string &key = "keykey";
//     auto value = reader->KvGet(key);
//     EXPECT_EQ(nullptr, value.get());
//   }

//   {
//     auto name = reader->GetName();
//     EXPECT_EQ("RocksReader", name);
//   }

//   {
//     uint32_t id = reader->GetID();
//     EXPECT_EQ(dingodb::EnumEngineReader::kRocksReader, static_cast<dingodb::EnumEngineReader>(id));
//   }
// }

// TEST_F(RawRocksEngineTest, EngineIterator) {
//   dingodb::RocksEngine &engine = rocks_engine_test->GetRawRocksEngine();
//   const std::string &cf_name = kDefaultCf;
//   std::shared_ptr<dingodb::Context> ctx = std::make_shared<dingodb::Context>();
//   ctx->set_cf_name(cf_name);

//   std::shared_ptr<dingodb::EngineReader> reader = RawRocksEngineTest::engine->CreateReader(ctx);
//   std::shared_ptr<dingodb::EngineIterator> engine_iterator = reader->Scan("", "");

//   // GetSelf
//   {
//     auto engine_iterator_another = engine_iterator->GetSelf();
//     EXPECT_EQ(2, engine_iterator_another.use_count());
//   }

//   {
//     auto name = engine_iterator->GetName();
//     EXPECT_EQ("RocksIterator", name);
//   }

//   {
//     uint32_t id = engine_iterator->GetID();
//     EXPECT_EQ(dingodb::EnumEngineIterator::kRocksIterator, static_cast<dingodb::EnumEngineIterator>(id));
//   }

//   {
//     std::string start_key;
//     std::string end_key;

//     std::shared_ptr<dingodb::EngineIterator> engine_iterator = reader->Scan(start_key, end_key);

//     std::cout << "start_key : " << start_key << " "
//               << "end_key : " << end_key << std::endl;
//     while (engine_iterator->HasNext()) {
//       std::string key;
//       std::string value;
//       engine_iterator->GetKV(key, value);
//       std::cout << key << ":" << value << std::endl;
//       engine_iterator->Next();
//     }
//   }

//   {
//     std::string start_key = "key101";
//     std::string end_key;

//     std::shared_ptr<dingodb::EngineIterator> engine_iterator = reader->Scan(start_key, end_key);

//     std::cout << "start_key : " << start_key << " "
//               << "end_key : " << end_key << std::endl;
//     while (engine_iterator->HasNext()) {
//       std::string key;
//       std::string value;
//       engine_iterator->GetKV(key, value);
//       std::cout << key << ":" << value << std::endl;
//       engine_iterator->Next();
//     }
//   }

//   {
//     std::string start_key = "key201";
//     std::string end_key = "key204";

//     std::shared_ptr<dingodb::EngineIterator> engine_iterator = reader->Scan(start_key, end_key);

//     std::cout << "start_key : " << start_key << " "
//               << "end_key : " << end_key << std::endl;
//     while (engine_iterator->HasNext()) {
//       std::string key;
//       std::string value;
//       engine_iterator->GetKV(key, value);
//       std::cout << key << ":" << value << std::endl;
//       engine_iterator->Next();
//     }
//   }

//   {
//     std::string start_key;
//     std::string end_key;

//     std::vector<dingodb::pb::common::KeyValue> kvs;

//     kvs.reserve(300);

//     for (size_t i = 0; i < 300; i++) {
//       dingodb::pb::common::KeyValue kv;
//       kv.set_key("key" + std::to_string(i));
//       kv.set_value("value" + std::to_string(i));
//       kvs.push_back(kv);
//     }

//     dingodb::pb::error::Errno ok = RawRocksEngineTest::engine->KvBatchPut(ctx, kvs);
//     EXPECT_EQ(ok, dingodb::pb::error::Errno::OK);

//     std::shared_ptr<dingodb::EngineIterator> engine_iterator = reader->Scan(start_key, end_key);

//     std::cout << "start_key : " << start_key << " "
//               << "end_key : " << end_key << std::endl;
//     while (engine_iterator->HasNext()) {
//       std::string key;
//       std::string value;
//       engine_iterator->GetKV(key, value);
//       std::cout << key << ":" << value << std::endl;
//       engine_iterator->Next();
//     }
//   }

//   {
//     std::string start_key;
//     std::string end_key;

//     std::vector<dingodb::pb::common::KeyValue> kvs;

//     kvs.reserve(300);

//     for (size_t i = 0; i < 300; i++) {
//       dingodb::pb::common::KeyValue kv;
//       kv.set_key("key" + std::to_string(i));
//       kv.set_value("value" + std::to_string(i));
//       kvs.push_back(kv);
//     }

//     bool run_once = false;
//     std::shared_ptr<dingodb::EngineIterator> engine_iterator = reader->Scan(start_key, end_key);

//     std::cout << "start_key : " << start_key << " "
//               << "end_key : " << end_key << std::endl;
//     while (engine_iterator->HasNext()) {
//       std::string key;
//       std::string value;
//       engine_iterator->GetKV(key, value);
//       std::cout << key << ":" << value << std::endl;
//       engine_iterator->Next();

//       if (!run_once) {
//         dingodb::pb::error::Errno ok = RawRocksEngineTest::engine->KvBatchPut(ctx, kvs);
//         EXPECT_EQ(ok, dingodb::pb::error::Errno::OK);
//         run_once = true;
//       }
//     }
//   }
// }

TEST_F(RawRocksEngineTest, KvDelete) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key empty
  {
    std::string key;

    butil::Status ok = writer->KvDelete(key);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // key not exist in rockdb
  {
    const std::string &key = "not_exist_key";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key1";

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    butil::Status ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // double delete ok
  {
    const std::string &key = "key1";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);
  }

  // ok
  {
    const std::string &key = "key";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key1";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key2";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key3";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key10";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key1111";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key101";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key102";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key103";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key104";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key201";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key202";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key203";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    const std::string &key = "key204";

    butil::Status ok = writer->KvDelete(key);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }
}

TEST_F(RawRocksEngineTest, KvDeleteBatch) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // keys empty failed
  {
    std::vector<std::string> keys;

    butil::Status ok = writer->KvDeleteBatch(keys);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // some key empty failed
  {
    std::vector<std::string> keys;
    keys.emplace_back("key");
    keys.emplace_back("");

    butil::Status ok = writer->KvDeleteBatch(keys);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    std::vector<dingodb::pb::common::KeyValue> kvs;

    for (int i = 0; i < 10; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("mykey" + std::to_string(i));
      kv.set_value("myvalue" + std::to_string(i));
      kvs.emplace_back(std::move(kv));
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
      EXPECT_EQ(value, kvs[i].value());
    }

    std::vector<std::string> keys;
    keys.reserve(kvs.size());
    for (const auto &kv : kvs) {
      keys.emplace_back(kv.key());
    }

    ok = writer->KvDeleteBatch(keys);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);
    }
  }
}

TEST_F(RawRocksEngineTest, KvDeleteIfEqual) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // key  empty failed
  {
    dingodb::pb::common::KeyValue kv;

    butil::Status ok = writer->KvDeleteIfEqual(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // key not exist
  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("key598");
    kv.set_value("value598");

    butil::Status ok = writer->KvDeleteIfEqual(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);
  }

  // key exist value exist but value unequal
  {
    std::vector<dingodb::pb::common::KeyValue> kvs;

    for (int i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("myequalkey" + std::to_string(i));
      kv.set_value("myequalvalue" + std::to_string(i));
      kvs.emplace_back(std::move(kv));
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    for (int i = 0; i < 1; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
      EXPECT_EQ(value, kvs[i].value());
    }

    for (auto &kv : kvs) {
      kv.set_value("243fgdfgd");
      ok = writer->KvDeleteIfEqual(kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EINTERNAL);
    }
  }

  // ok
  {
    std::vector<dingodb::pb::common::KeyValue> kvs;

    for (int i = 0; i < 10; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("myequalkey" + std::to_string(i));
      kv.set_value("myequalvalue" + std::to_string(i));
      kvs.emplace_back(std::move(kv));
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    std::string value;
    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
      EXPECT_EQ(value, kvs[i].value());
    }

    for (const auto &kv : kvs) {
      ok = writer->KvDeleteIfEqual(kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }

    for (int i = 0; i < 10; i++) {
      ok = reader->KvGet(kvs[i].key(), value);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);
    }
  }
}

TEST_F(RawRocksEngineTest, KvDeleteRange) {
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = RawRocksEngineTest::engine->NewWriter(cf_name);

  // wirite key -> key999
  {
    std::vector<dingodb::pb::common::KeyValue> kvs;
    std::vector<bool> key_states;

    for (int i = 0; i < 1000; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("key" + std::to_string(i));
      kv.set_value("value" + std::to_string(i));
      kvs.push_back(kv);
    }

    butil::Status ok = writer->KvBatchPut(kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // key empty
  {
    dingodb::pb::common::Range range;

    butil::Status ok = writer->KvDeleteRange(range);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // start key not empty but end key empty
  {
    dingodb::pb::common::Range range;
    range.set_start_key("key");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_EMPTY);
  }

  // ok
  {
    dingodb::pb::common::Range range;
    range.set_start_key("key");
    range.set_end_key("key100");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string start_key = "key";
    std::string end_key = "key100";
    std::vector<dingodb::pb::common::KeyValue> kvs;

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    std::string key = "key";
    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);

    key = "key100";
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    dingodb::pb::common::Range range;
    range.set_start_key("key100");
    range.set_end_key("key200");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string start_key = "key100";
    std::string end_key = "key200";
    std::vector<dingodb::pb::common::KeyValue> kvs;

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    std::string key = "key100";
    std::string value;
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EKEY_NOTFOUND);

    key = "key200";
    ok = reader->KvGet(key, value);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  // ok
  {
    dingodb::pb::common::Range range;
    range.set_start_key("key");
    range.set_end_key("key99999");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string start_key = "key";
    std::string end_key = "key99999";
    std::vector<dingodb::pb::common::KeyValue> kvs;

    std::shared_ptr<dingodb::RawEngine::Reader> reader = RawRocksEngineTest::engine->NewReader(cf_name);

    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }
  }
}

TEST_F(RawRocksEngineTest, Iterator) {
  auto writer = RawRocksEngineTest::engine->NewWriter(kDefaultCf);
  dingodb::pb::common::KeyValue kv;
  kv.set_key("bbbbbbbbbbbbb");
  kv.set_value(GenRandomString(256));
  writer->KvPut(kv);

  int count = 0;
  dingodb::IteratorOptions options;
  options.upper_bound = "cccc";
  auto iter = RawRocksEngineTest::engine->NewIterator(kDefaultCf, options);
  for (iter->Seek("aaaaaaaaaa"); iter->Valid(); iter->Next()) {
    ++count;
  }

  EXPECT_GE(count, 1);
}

// TEST_F(RawRocksEngineTest, Checkpoint) {
//   auto writer = RawRocksEngineTest::engine->NewWriter(kDefaultCf);

//   dingodb::pb::common::KeyValue kv;
//   for (int i = 0; i < 10000; ++i) {
//     kv.set_key(GenRandomString(32));
//     kv.set_value(GenRandomString(256));
//     writer->KvPut(kv);
//   }

//   auto checkpoint = RawRocksEngineTest::engine->NewCheckpoint();

//   const std::string store_path = std::filesystem::path(RawRocksEngineTest::engine->DbPath()).parent_path().string();
//   std::filesystem::create_directories(store_path);
//   const std::string checkpoint_path = store_path + "/checkpoint_" + std::to_string(dingodb::Helper::Timestamp());
//   std::cout << "checkpoint_path: " << checkpoint_path << std::endl;
//   //  checkpoint->Create("/tmp/dingo-store/data/store/checkpoint");
//   std::vector<dingodb::pb::store_internal::SstFileInfo> sst_files;
//   auto status = checkpoint->Create(checkpoint_path, RawRocksEngineTest::engine->GetColumnFamily(kDefaultCf),
//   sst_files); EXPECT_EQ(true, status.ok());

//   std::filesystem::remove_all(checkpoint_path);
// }

// TEST_F(RawRocksEngineTest, Ingest) {
//   auto reader = RawRocksEngineTest::engine->NewReader(kDefaultCf);
//   auto writer = RawRocksEngineTest::engine->NewWriter(kDefaultCf);

//   const std::vector<std::string> prefixs = {"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "mm"};
//   dingodb::pb::common::KeyValue kv;
//   for (int i = 0; i < 10000; ++i) {
//     int pos = i % prefixs.size();

//     kv.set_key(prefixs[pos] + GenRandomString(30));
//     kv.set_value(GenRandomString(256));
//     writer->KvPut(kv);
//   }

//   auto checkpoint = RawRocksEngineTest::engine->NewCheckpoint();

//   const std::string store_path = std::filesystem::path(RawRocksEngineTest::engine->DbPath()).parent_path().string();
//   std::filesystem::create_directories(store_path);
//   const std::string checkpoint_path = store_path + "/checkpoint_" + std::to_string(dingodb::Helper::Timestamp());
//   std::cout << "checkpoint_path: " << checkpoint_path << std::endl;

//   std::vector<dingodb::pb::store_internal::SstFileInfo> sst_files;
//   auto status = checkpoint->Create(checkpoint_path, RawRocksEngineTest::engine->GetColumnFamily(kDefaultCf),
//   sst_files); EXPECT_EQ(true, status.ok());

//   dingodb::pb::common::Range range;
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
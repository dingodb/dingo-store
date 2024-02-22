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
#include <iostream>
#include <iterator>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/helper.h"
#include "config/config.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "proto/common.pb.h"

namespace dingodb {  // NOLINT

static const std::string kDefaultCf = "default";
// static const std::string &kDefaultCf = "meta";

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

const std::string kRootPath = "./unit_test";
const std::string kLogPath = kRootPath + "/log";
const std::string kStorePath = kRootPath + "/db";

const std::string kYamlConfigContent =
    "cluster:\n"
    "  name: dingodb\n"
    "  instance_id: 12345\n"
    "  coordinators: 127.0.0.1:19190,127.0.0.1:19191,127.0.0.1:19192\n"
    "  keyring: TO_BE_CONTINUED\n"
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

class RawRocksEngineTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::srand(std::time(nullptr));

    Helper::CreateDirectories(kStorePath);

    config = std::make_shared<YamlConfig>();
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
  static std::shared_ptr<Config> config;
};

std::shared_ptr<RocksRawEngine> RawRocksEngineTest::engine = nullptr;
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

TEST_F(RawRocksEngineTest, GetName) {
  std::string name = RawRocksEngineTest::engine->GetName();
  EXPECT_EQ(name, "RAW_ENG_ROCKSDB");
}

TEST_F(RawRocksEngineTest, GetID) {
  pb::common::RawEngine id = RawRocksEngineTest::engine->GetRawEngineType();
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

TEST_F(RawRocksEngineTest, KvPut) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawRocksEngineTest::engine->Writer();

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

TEST_F(RawRocksEngineTest, KvBatchPut) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawRocksEngineTest::engine->Writer();

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
    auto reader = RawRocksEngineTest::engine->Reader();
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

TEST_F(RawRocksEngineTest, KvBatchPutAndDelete) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawRocksEngineTest::engine->Writer();

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
    std::vector<std::string> key_deletes;

    key_deletes.emplace_back("not_found_key");
    key_deletes.emplace_back("key1");
    key_deletes.emplace_back("key2");
    key_deletes.emplace_back("key3");

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kv_puts, key_deletes);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string value0;
    std::string value1;
    std::string value2;
    std::string value3;
    auto reader = RawRocksEngineTest::engine->Reader();

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

    auto reader = RawRocksEngineTest::engine->Reader();

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
    auto reader = RawRocksEngineTest::engine->Reader();
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

TEST_F(RawRocksEngineTest, KvGet) {
  const std::string &cf_name = kDefaultCf;
  auto reader = RawRocksEngineTest::engine->Reader();

  // key empty
  {
    std::string key;
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  {
    const std::string &key = "key1";
    std::string value;

    butil::Status ok = reader->KvGet(cf_name, key, value);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

#ifdef TEST_KV_BATCH_GET_SWITCH
TEST_F(RawRocksEngineTest, KvBatchGet) {
  const std::string &cf_name = kDefaultCf;
  auto reader = RawRocksEngineTest::engine->Reader();

  // key all empty
  {
    std::vector<std::string> keys;
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(cf_name, keys, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // key some empty
  {
    std::vector<std::string> keys{"key1", "", "key"};
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(cf_name, keys, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_EMPTY);
  }

  // some key not exist
  {
    std::vector<std::string> keys{"key1", "key2", "key", "key4"};
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(cf_name, keys, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EINTERNAL);
  }

  // normal
  {
    std::vector<std::string> keys{"key1", "key"};
    std::vector<pb::common::KeyValue> kvs;

    butil::Status ok = reader->KvBatchGet(cf_name, keys, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}
#endif

TEST_F(RawRocksEngineTest, KvScan) {
  const std::string &cf_name = kDefaultCf;
  auto reader = RawRocksEngineTest::engine->Reader();

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
      LOG(INFO) << kv.key() << ":" << kv.value();
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
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }
}

TEST_F(RawRocksEngineTest, KvCount) {
  const std::string &cf_name = kDefaultCf;
  auto reader = RawRocksEngineTest::engine->Reader();

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
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
    EXPECT_EQ(count, kvs.size());
  }
}

TEST_F(RawRocksEngineTest, KvDelete) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawRocksEngineTest::engine->Writer();

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

    auto reader = RawRocksEngineTest::engine->Reader();

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

    auto reader = RawRocksEngineTest::engine->Reader();

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

TEST_F(RawRocksEngineTest, KvBatchDelete) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawRocksEngineTest::engine->Writer();

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

    auto reader = RawRocksEngineTest::engine->Reader();

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

TEST_F(RawRocksEngineTest, KvDeleteRange) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawRocksEngineTest::engine->Writer();

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

    auto reader = RawRocksEngineTest::engine->Reader();

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

    auto reader = RawRocksEngineTest::engine->Reader();

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

    auto reader = RawRocksEngineTest::engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }
}

TEST_F(RawRocksEngineTest, KvDeleteRangeWithRangeWithOptions) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawRocksEngineTest::engine->Writer();

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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok delete KEY0 KEY1
  {
    pb::common::Range range;
    range.set_start_key("KEX");
    range.set_end_key("KEY10");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawRocksEngineTest::engine->Reader();

    std::string key1 = "KEY0";
    std::string value1;
    ok = reader->KvGet(cf_name, key1, value1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    std::string key2 = "KEY1";
    std::string value2;
    ok = reader->KvGet(cf_name, key2, value2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EKEY_NOT_FOUND);

    std::string start_key = "KEY";
    std::string end_key = "KEZ";
    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }

  // ok delete KEY
  {
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEY");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawRocksEngineTest::engine->Reader();

    std::string start_key = "KEY";
    std::string end_key = "KEZ";
    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawRocksEngineTest::engine->Reader();

    std::string start_key = "KEY";
    std::string end_key = "KF0";
    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }

    EXPECT_EQ(10, kvs.size());
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawRocksEngineTest::engine->Reader();

    std::string start_key = "KEY";
    std::string end_key = "KF0";
    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEX");
    range.set_end_key("KEZ");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawRocksEngineTest::engine->Reader();

    std::string start_key = "KEY";
    std::string end_key = "KF0";
    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }

    EXPECT_EQ(10, kvs.size());
  }
}

TEST_F(RawRocksEngineTest, KvBatchDeleteRange) {
  const std::string &cf_name = kDefaultCf;
  auto writer = RawRocksEngineTest::engine->Writer();

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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // key empty failed
  {
    pb::common::Range range;
    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = {range};
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key not empty but end key empty failed
  {
    pb::common::Range range;

    range.set_start_key("key");

    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = {range};
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // end key not empty but start key empty failed
  {
    pb::common::Range range;

    range.set_end_key("key");

    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = {range};
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key >  end key equal failed
  {
    pb::common::Range range;

    range.set_start_key("key");
    range.set_end_key("Key");

    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = {range};
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key ==  end key equal failed
  {
    pb::common::Range range;

    range.set_start_key("key");
    range.set_end_key("key");

    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = {range};
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // start key ==  end key equal failed
  {
    pb::common::Range range;

    range.set_start_key("key");
    range.set_end_key("key");

    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = {range};
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);

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

    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = ranges;
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);

    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawRocksEngineTest::engine->Reader();

    std::string start_key = "KEY";
    std::string end_key = "KEZ";
    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }

    EXPECT_EQ(10, kvs.size());

    start_key = "KEZ";
    end_key = "KE[";
    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
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

    butil::Status ok = writer->KvBatchPutAndDelete(cf_name, kvs, {});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEY");
    range.set_end_key("KEZ");

    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = {range};
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  {
    // ok delete KEY
    pb::common::Range range;
    range.set_start_key("KEX");
    range.set_end_key("KEZ");

    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = {range};
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::vector<pb::common::KeyValue> kvs;

    auto reader = RawRocksEngineTest::engine->Reader();

    std::string start_key = "KEY";
    std::string end_key = "KF0";
    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }

    EXPECT_EQ(10, kvs.size());
  }

  {
    // ok delete KEY
    pb::common::Range range;
    char array[] = {static_cast<char>(0xFF), static_cast<char>(0xFF), static_cast<char>(0xFF), static_cast<char>(0xFF)};

    range.set_start_key(std::string(array, std::size(array)));
    range.set_end_key(std::string(array, std::size(array)));

    std::map<std::string, std::vector<pb::common::Range>> range_with_cfs;
    range_with_cfs[cf_name] = {range};
    butil::Status ok = writer->KvBatchDeleteRange(range_with_cfs);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }
}

TEST_F(RawRocksEngineTest, Iterator) {
  auto writer = RawRocksEngineTest::engine->Writer();
  pb::common::KeyValue kv;
  kv.set_key("bbbbbbbbbbbbb");
  kv.set_value(GenRandomString(256));
  writer->KvPut(kDefaultCf, kv);

  int count = 0;
  IteratorOptions options;
  options.upper_bound = "cccc";
  auto iter = RawRocksEngineTest::engine->Reader()->NewIterator(kDefaultCf, options);
  for (iter->Seek("aaaaaaaaaa"); iter->Valid(); iter->Next()) {
    ++count;
  }

  EXPECT_GE(count, 1);
}

// TEST_F(RawRocksEngineTest, Checkpoint) {
//   auto writer = RawRocksEngineTest::engine->Writer();

//   pb::common::KeyValue kv;
//   for (int i = 0; i < 10000; ++i) {
//     kv.set_key(GenRandomString(32));
//     kv.set_value(GenRandomString(256));
//     writer->KvPut(kDefaultCf, kv);
//   }

//   auto checkpoint = RawRocksEngineTest::engine->NewCheckpoint();

//   const std::string store_path = std::filesystem::path(RawRocksEngineTest::engine->DbPath()).parent_path().string();
//   std::filesystem::create_directories(store_path);
//   const std::string checkpoint_path = store_path + "/checkpoint_" + std::to_string(Helper::Timestamp());
//   LOG(INFO) << "checkpoint_path: " << checkpoint_path;
//   //  checkpoint->Create("/tmp/dingo-store/data/store/checkpoint");
//   std::vector<pb::store_internal::SstFileInfo> sst_files;
//   auto status = checkpoint->Create(checkpoint_path, RawRocksEngineTest::engine->GetColumnFamily(kDefaultCf),
//   sst_files); EXPECT_EQ(true, status.ok());

//   std::filesystem::remove_all(checkpoint_path);
// }

// TEST_F(RawRocksEngineTest, Ingest) {
//   auto reader = RawRocksEngineTest::engine->Reader();
//   auto writer = RawRocksEngineTest::engine->Writer();

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
//   LOG(INFO) << "checkpoint_path: " << checkpoint_path;

//   std::vector<pb::store_internal::SstFileInfo> sst_files;
//   auto status = checkpoint->Create(checkpoint_path, RawRocksEngineTest::engine->GetColumnFamily(kDefaultCf),
//   sst_files); EXPECT_EQ(true, status.ok());

//   pb::common::Range range;
//   range.set_start_key("bb");
//   range.set_end_key("cc");

//   std::vector<std::string> files;
//   for (auto& sst_file : sst_files) {
//     LOG(INFO) << "sst file path: " << sst_file.path() << " " << sst_file.start_key() << "-" << sst_file.end_key()
//              ;
//     if (sst_file.start_key() < range.end_key() && range.start_key() < sst_file.end_key()) {
//       LOG(INFO) << "pick up: " << sst_file.path();
//       files.push_back(sst_file.path());
//     }
//   }

//   int64_t count = 0;
//   reader->KvCount(kDefaultCf, range.start_key(), range.end_key(), count);
//   LOG(INFO) << "count before delete: " << count;

//   writer->KvDeleteRange(kDefaultCf, range);

//   reader->KvCount(kDefaultCf, range.start_key(), range.end_key(), count);
//   LOG(INFO) << "count after delete: " << count;

//   RawRocksEngineTest::engine->IngestExternalFile(kDefaultCf, files);

//   reader->KvCount(kDefaultCf, range.start_key(), range.end_key(), count);
//   LOG(INFO) << "count after ingest: " << count;
// }

}  // namespace dingodb

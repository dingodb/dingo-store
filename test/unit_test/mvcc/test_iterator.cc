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
#include <string>

#include "common/helper.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "mvcc/codec.h"
#include "mvcc/iterator.h"

namespace dingodb {

static const std::string kDefaultCf = "default";
static const std::vector<std::string> kAllCFs = {kDefaultCf};

const std::string kRootPath = "./unit_test";
const std::string kLogPath = kRootPath + "/log";
const std::string kStorePath = kRootPath + "/mvcc_db";

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

class MvccIteratorTest : public testing::Test {
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

std::shared_ptr<RocksRawEngine> MvccIteratorTest::engine = nullptr;
std::shared_ptr<Config> MvccIteratorTest::config = nullptr;

TEST_F(MvccIteratorTest, BackwardIterator) {
  // arrange data
  auto writer = engine->Writer();

  std::vector<pb::common::KeyValue> kvs;

  {
    pb::common::KeyValue kv;
    kv.set_key("hello1");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100000, kv));  // offset 0
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100001, kv));  // offset 1
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello2");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100001, kv));  // offset 2
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100002, kv));  // offset 3
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello3");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100002, kv));  // offset 4
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100003, kv));  // offset 5
  }

  writer->KvBatchPutAndDelete(kDefaultCf, kvs, {});

  // use iterator read data

  {
    int64_t ts = 99999;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->Seek(start_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100000;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[0].key(), iter->Key());  // hello1+100000
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100001;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[2].key(), iter->Key());  // hello2+100001
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100002;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100002
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[4].key(), iter->Key());  // hello3+100002
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100003;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100002
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[5].key(), iter->Key());  // hello3+100003
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  // clear data
  pb::common::Range range;
  range.set_start_key("hello");
  range.set_end_key("hellz");
  writer->KvDeleteRange(kDefaultCf, range);
}

TEST_F(MvccIteratorTest, BackwardIteratorForDeleteKey) {
  // arrange data
  auto writer = engine->Writer();

  std::vector<pb::common::KeyValue> kvs;

  {
    pb::common::KeyValue kv;
    kv.set_key("hello1");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100000, kv));     // offset 0
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100001, kv));     // offset 1
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithDelete(100002, kv));  // offset 2
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello2");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100001, kv));     // offset 3
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100002, kv));     // offset 4
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithDelete(100003, kv));  // offset 5
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello3");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100002, kv));  // offset 6
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100003, kv));  // offset 7
  }

  writer->KvBatchPutAndDelete(kDefaultCf, kvs, {});

  // use iterator read data

  {
    int64_t ts = 99999;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->Seek(start_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100000;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[0].key(), iter->Key());  // hello1+100000
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100001;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100001
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100002;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[4].key(), iter->Key());  // hello2+100002
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[6].key(), iter->Key());  // hello3+100002
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100003;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[7].key(), iter->Key());  // hello3+100003
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  // clear data
  pb::common::Range range;
  range.set_start_key("hello");
  range.set_end_key("hellz");
  writer->KvDeleteRange(kDefaultCf, range);
}

TEST_F(MvccIteratorTest, BackwardIteratorForTTL) {
  // arrange data
  auto writer = engine->Writer();

  std::vector<pb::common::KeyValue> kvs;

  int64_t ttl = Helper::TimestampMs() + 100000;

  {
    pb::common::KeyValue kv;
    kv.set_key("hello1");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100000, ttl, kv));  // offset 0
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100001, ttl, kv));  // offset 1
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello2");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100001, ttl, kv));  // offset 2
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100002, ttl, kv));  // offset 3
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello3");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100002, ttl, kv));  // offset 4
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100003, ttl, kv));  // offset 5
  }

  writer->KvBatchPutAndDelete(kDefaultCf, kvs, {});

  // use iterator read data

  {
    int64_t ts = 99999;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->Seek(start_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100000;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[0].key(), iter->Key());  // hello1+100000
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100001;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[2].key(), iter->Key());  // hello2+100001
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100002;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100002
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[4].key(), iter->Key());  // hello3+100002
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100003;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100002
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[5].key(), iter->Key());  // hello3+100003
    iter->Next();
    ASSERT_FALSE(iter->Valid());
  }

  // clear data
  pb::common::Range range;
  range.set_start_key("hello");
  range.set_end_key("hellz");
  writer->KvDeleteRange(kDefaultCf, range);
}

TEST_F(MvccIteratorTest, BackwardIteratorForTTLExpire) {
  // arrange data
  auto writer = engine->Writer();

  std::vector<pb::common::KeyValue> kvs;

  int64_t ttl = Helper::TimestampMs() - 100000;

  {
    pb::common::KeyValue kv;
    kv.set_key("hello1");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100000, ttl, kv));  // offset 0
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100001, ttl, kv));  // offset 1
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello2");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100001, ttl, kv));  // offset 2
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100002, ttl, kv));  // offset 3
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello3");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100002, ttl, kv));  // offset 4
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100003, ttl, kv));  // offset 5
  }

  writer->KvBatchPutAndDelete(kDefaultCf, kvs, {});

  // use iterator read data

  {
    int64_t ts = 99999;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->Seek(start_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100000;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->Seek(start_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100001;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100002;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100003;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.upper_bound = end_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->Seek(start_key);
    ASSERT_FALSE(iter->Valid());
  }

  // clear data
  pb::common::Range range;
  range.set_start_key("hello");
  range.set_end_key("hellz");
  writer->KvDeleteRange(kDefaultCf, range);
}

TEST_F(MvccIteratorTest, ForwardIterator) {
  // arrange data
  auto writer = engine->Writer();

  std::vector<pb::common::KeyValue> kvs;

  {
    pb::common::KeyValue kv;
    kv.set_key("hello1");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100000, kv));  // offset 0
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100001, kv));  // offset 1
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello2");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100001, kv));  // offset 2
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100002, kv));  // offset 3
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello3");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100002, kv));  // offset 4
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100003, kv));  // offset 5
  }

  writer->KvBatchPutAndDelete(kDefaultCf, kvs, {});

  // use iterator read data

  {
    int64_t ts = 99999;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->SeekForPrev(end_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100000;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[0].key(), iter->Key());  // hello1+100000
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100001;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[2].key(), iter->Key());  // hello2+100001
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100002;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[4].key(), iter->Key());  // hello3+100002
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100002
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100003;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[5].key(), iter->Key());  // hello3+100003
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100002
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  // clear data
  pb::common::Range range;
  range.set_start_key("hello");
  range.set_end_key("hellz");
  writer->KvDeleteRange(kDefaultCf, range);
}

TEST_F(MvccIteratorTest, ForwardIteratorForDeleteKey) {
  // arrange data
  auto writer = engine->Writer();

  std::vector<pb::common::KeyValue> kvs;

  {
    pb::common::KeyValue kv;
    kv.set_key("hello1");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100000, kv));     // offset 0
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100001, kv));     // offset 1
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithDelete(100002, kv));  // offset 2
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello2");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100001, kv));     // offset 3
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100002, kv));     // offset 4
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithDelete(100003, kv));  // offset 5
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello3");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100002, kv));  // offset 6
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPut(100003, kv));  // offset 7
  }

  writer->KvBatchPutAndDelete(kDefaultCf, kvs, {});

  // use iterator read data

  {
    int64_t ts = 99999;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->SeekForPrev(end_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100000;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[0].key(), iter->Key());  // hello1+100000
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100001;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100001
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100002;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[6].key(), iter->Key());  // hello3+100002
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[4].key(), iter->Key());  // hello2+100002
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100003;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[7].key(), iter->Key());  // hello3+100003
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  // clear data
  pb::common::Range range;
  range.set_start_key("hello");
  range.set_end_key("hellz");
  writer->KvDeleteRange(kDefaultCf, range);
}

TEST_F(MvccIteratorTest, ForwardIteratorForTTL) {
  // arrange data
  auto writer = engine->Writer();

  std::vector<pb::common::KeyValue> kvs;

  int64_t ttl = Helper::TimestampMs() + 100000;

  {
    pb::common::KeyValue kv;
    kv.set_key("hello1");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100000, ttl, kv));  // offset 0
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100001, ttl, kv));  // offset 1
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello2");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100001, ttl, kv));  // offset 2
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100002, ttl, kv));  // offset 3
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello3");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100002, ttl, kv));  // offset 4
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100003, ttl, kv));  // offset 5
  }

  writer->KvBatchPutAndDelete(kDefaultCf, kvs, {});

  // use iterator read data

  {
    int64_t ts = 99999;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->SeekForPrev(end_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100000;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[0].key(), iter->Key());  // hello1+100000
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100001;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[2].key(), iter->Key());  // hello2+100001
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100002;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[4].key(), iter->Key());  // hello3+100002
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100002
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100003;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[5].key(), iter->Key());  // hello3+100003
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[3].key(), iter->Key());  // hello2+100002
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(kvs[1].key(), iter->Key());  // hello1+100001
    iter->Prev();
    ASSERT_FALSE(iter->Valid());
  }

  // clear data
  pb::common::Range range;
  range.set_start_key("hello");
  range.set_end_key("hellz");
  writer->KvDeleteRange(kDefaultCf, range);
}

TEST_F(MvccIteratorTest, ForwardIteratorForTTLExpire) {
  // arrange data
  auto writer = engine->Writer();

  std::vector<pb::common::KeyValue> kvs;

  int64_t ttl = Helper::TimestampMs() - 100000;

  {
    pb::common::KeyValue kv;
    kv.set_key("hello1");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100000, ttl, kv));  // offset 0
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100001, ttl, kv));  // offset 1
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello2");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100001, ttl, kv));  // offset 2
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100002, ttl, kv));  // offset 3
  }

  {
    pb::common::KeyValue kv;
    kv.set_key("hello3");
    kv.set_value(GenRandomString(256));
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100002, ttl, kv));  // offset 4
    kvs.push_back(mvcc::Codec::EncodeKeyValueWithPutTTL(100003, ttl, kv));  // offset 5
  }

  writer->KvBatchPutAndDelete(kDefaultCf, kvs, {});

  // use iterator read data

  {
    int64_t ts = 99999;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->SeekForPrev(end_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100000;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));
    iter->SeekForPrev(end_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100001;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100002;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_FALSE(iter->Valid());
  }

  {
    int64_t ts = 100003;
    std::string start_key = mvcc::Codec::EncodeBytes("hello1");
    std::string end_key = mvcc::Codec::EncodeBytes("hello4");
    dingodb::IteratorOptions options;
    options.lower_bound = start_key;

    auto reader = engine->Reader();
    auto iter = std::make_shared<mvcc::Iterator>(ts, reader->NewIterator(kDefaultCf, options));

    iter->SeekForPrev(end_key);
    ASSERT_FALSE(iter->Valid());
  }

  // clear data
  pb::common::Range range;
  range.set_start_key("hello");
  range.set_end_key("hellz");
  writer->KvDeleteRange(kDefaultCf, range);
}

}  // namespace dingodb
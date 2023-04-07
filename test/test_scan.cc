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
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/context.h"
#include "config/config_manager.h"
#include "engine/engine.h"
#include "engine/raw_rocks_engine.h"
#include "engine/rocks_engine.h"
#include "engine/scan.h"
#include "engine/scan_factory.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

static const std::string &kDefaultCf = Constant::kStoreDataCF;  // NOLINT

class ScanTest {
 public:
  std::shared_ptr<Config> GetConfig() { return config_; }

  std::shared_ptr<RawRocksEngine> GetRawRocksEngine() { return raw_raw_rocks_engine_; }
  ScanContextFactory *&GetFactory() { return factory_; }

  std::shared_ptr<ScanContext> GetScan(std::string *scan_id) {
    if (!scan_) {
      scan_ = factory_->CreateScan(scan_id);
      scan_id_ = *scan_id;
    } else {
      *scan_id = scan_id_;
    }
    return scan_;
  }

  void DeleteScan() {
    if (!scan_id_.empty() || scan_) {
      factory_->DeleteScan(scan_id_);
    }
    scan_.reset();
    scan_id_ = "";
  }

  void MySetUp() {
    std::cout << "ScanTest::SetUp()" << std::endl;
    server_ = Server::GetInstance();
    filename_ = "../../conf/store.yaml";
    server_->SetRole(pb::common::ClusterRole::STORE);
    server_->InitConfig(filename_);
    config_manager_ = ConfigManager::GetInstance();
    config_ = config_manager_->GetConfig(pb::common::ClusterRole::STORE);
    factory_ = ScanContextFactory::GetInstance();
    raw_raw_rocks_engine_ = std::make_shared<RawRocksEngine>();
  }
  void MyTearDown() {}

 private:
  Server *server_;
  std::string filename_ = "../../conf/store.yaml";
  ConfigManager *config_manager_;
  std::shared_ptr<Config> config_;
  std::shared_ptr<RawRocksEngine> raw_raw_rocks_engine_;
  ScanContextFactory *factory_;
  std::shared_ptr<ScanContext> scan_;
  std::string scan_id_;
};

static ScanTest *scan_test = nullptr;

// cppcheck-suppress syntaxError
TEST(ScanTest, BeforeInit) {
  scan_test = new ScanTest();
  scan_test->MySetUp();
}

TEST(ScanTest, InitRocksdb) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();

  bool ret = raw_rocks_engine->Init({});
  EXPECT_FALSE(ret);

  std::shared_ptr<Config> config = scan_test->GetConfig();

  // Test for various configuration file exceptions
  ret = raw_rocks_engine->Init(config);
  EXPECT_TRUE(ret);

  auto *factory = scan_test->GetFactory();
  factory->Init(config);
}

TEST(ScanTest, Open) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;
  auto scan = scan_test->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);

  auto *factory = scan_test->GetFactory();

  butil::Status ok;

  // scan id empty failed
  ok = scan->Open("", factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                  raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // timeout == 0 failed
  ok = scan->Open(scan_id, 0, factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(), raw_rocks_engine,
                  kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // max bytes rpc = 0 failed
  ok = scan->Open(scan_id, factory->GetTimeoutMs(), 0, factory->GetMaxFetchCntByServer(), raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // max_fetch_cnt_by_server == 0 failed
  ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), 0, raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // engin empty {} failed
  ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(), {},
                  kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // kDefaultCf empty  failed
  ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                  raw_rocks_engine, "");
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                  raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
}

// empty data
TEST(ScanTest, ScanBegin) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;
  auto scan = scan_test->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);

  butil::Status ok;

  uint64_t region_id = 1;
  pb::common::PrefixScanRange range;

  uint64_t max_fetch_cnt = 10;
  bool key_only = false;
  bool disable_auto_release = true;
  std::vector<pb::common::KeyValue> kvs;

  // range empty
  ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  range.mutable_range()->set_start_key("keyAAA");
  range.mutable_range()->set_end_key("keyAAA");
  range.set_with_start(true);
  range.set_with_end(false);

  // range value failed
  ok = scan->ScanBegin(0, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  range.mutable_range()->set_start_key("keyAAA");
  range.mutable_range()->set_end_key("keyAA");
  range.set_with_start(true);
  range.set_with_end(false);

  // range value failed
  ok = scan->ScanBegin(0, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  range.mutable_range()->set_start_key("keyAAA");
  range.mutable_range()->set_end_key("keyAAC");
  range.set_with_start(true);
  range.set_with_end(false);

  // ok
  ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  scan_test->DeleteScan();
}

TEST(ScanTest, InsertData) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = raw_rocks_engine->NewWriter(cf_name);

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyAA");
    kv.set_value("valueAA");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyAA" + std::to_string(i));
      kv.set_value("valueAA" + std::to_string(i));

      butil::Status ok = writer->KvPut(kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyAAA");
    kv.set_value("valueAAA");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyAAA" + std::to_string(i));
      kv.set_value("valueAAA" + std::to_string(i));

      butil::Status ok = writer->KvPut(kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyABB");
    kv.set_value("valueABB");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyABB" + std::to_string(i));
      kv.set_value("valueABB" + std::to_string(i));

      butil::Status ok = writer->KvPut(kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyABC");
    kv.set_value("valueABC");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyABC" + std::to_string(i));
      kv.set_value("valueABC" + std::to_string(i));

      butil::Status ok = writer->KvPut(kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyABD");
    kv.set_value("valueABD");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyABD" + std::to_string(i));
      kv.set_value("valueABD" + std::to_string(i));

      butil::Status ok = writer->KvPut(kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyAB");
    kv.set_value("valueAB");

    butil::Status ok = writer->KvPut(kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyAB" + std::to_string(i));
      kv.set_value("valueAB" + std::to_string(i));

      butil::Status ok = writer->KvPut(kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }
}

TEST(ScanTest, ScanBeginEqual) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;
  auto scan = scan_test->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);

  auto *factory = scan_test->GetFactory();

  butil::Status ok;

  ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                  raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  EXPECT_NE(scan.get(), nullptr);

  uint64_t region_id = 1;
  pb::common::PrefixScanRange range;

  uint64_t max_fetch_cnt = 10;
  bool key_only = false;
  bool disable_auto_release = true;
  std::vector<pb::common::KeyValue> kvs;

  range.mutable_range()->set_start_key("keyAAA");
  range.mutable_range()->set_end_key("keyAAA");
  range.set_with_start(true);
  range.set_with_end(true);

  // ok
  ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  scan_test->DeleteScan();
}

TEST(ScanTest, ScanBeginOthers) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto *factory = scan_test->GetFactory();
  butil::Status ok;

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key end_key equal start_key >= keyAA and end_key <=keyAA
  {
    auto scan = scan_test->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                    raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::PrefixScanRange range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.mutable_range()->set_start_key("keyAA");
    range.mutable_range()->set_end_key("keyAA");
    range.set_with_start(true);
    range.set_with_end(true);

    // ok
    ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(kvs.size(), 4);
    EXPECT_EQ(kvs[0].key(), "keyAA");
    EXPECT_EQ(kvs[1].key(), "keyAA0");
    EXPECT_EQ(kvs[2].key(), "keyAAA");
    EXPECT_EQ(kvs[3].key(), "keyAAA0");

    scan_test->DeleteScan();
  }

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key >= keyAA and end_key < keyABB
  {
    auto scan = scan_test->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                    raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::PrefixScanRange range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.mutable_range()->set_start_key("keyAA");
    range.mutable_range()->set_end_key("keyABB");
    range.set_with_start(true);
    range.set_with_end(false);

    // ok
    ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(kvs.size(), 6);
    EXPECT_EQ(kvs[0].key(), "keyAA");
    EXPECT_EQ(kvs[1].key(), "keyAA0");
    EXPECT_EQ(kvs[2].key(), "keyAAA");
    EXPECT_EQ(kvs[3].key(), "keyAAA0");
    EXPECT_EQ(kvs[4].key(), "keyAB");
    EXPECT_EQ(kvs[5].key(), "keyAB0");

    scan_test->DeleteScan();
  }

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key > keyAA and end_key < keyABB
  {
    auto scan = scan_test->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                    raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::PrefixScanRange range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.mutable_range()->set_start_key("keyAA");
    range.mutable_range()->set_end_key("keyABB");
    range.set_with_start(false);
    range.set_with_end(false);

    // ok
    ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(kvs.size(), 2);
    EXPECT_EQ(kvs[0].key(), "keyAB");
    EXPECT_EQ(kvs[1].key(), "keyAB0");

    scan_test->DeleteScan();
  }

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key > keyAA and end_key <= keyABB
  {
    auto scan = scan_test->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                    raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::PrefixScanRange range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.mutable_range()->set_start_key("keyAA");
    range.mutable_range()->set_end_key("keyABB");
    range.set_with_start(false);
    range.set_with_end(true);

    // ok
    ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(kvs.size(), 4);
    EXPECT_EQ(kvs[0].key(), "keyAB");
    EXPECT_EQ(kvs[1].key(), "keyAB0");
    EXPECT_EQ(kvs[2].key(), "keyABB");
    EXPECT_EQ(kvs[3].key(), "keyABB0");

    scan_test->DeleteScan();
  }

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key >= keyAA and end_key <= keyABB
  {
    auto scan = scan_test->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                    raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::PrefixScanRange range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.mutable_range()->set_start_key("keyAA");
    range.mutable_range()->set_end_key("keyABB");
    range.set_with_start(true);
    range.set_with_end(true);

    // ok
    ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(kvs.size(), 8);
    EXPECT_EQ(kvs[0].key(), "keyAA");
    EXPECT_EQ(kvs[1].key(), "keyAA0");
    EXPECT_EQ(kvs[2].key(), "keyAAA");
    EXPECT_EQ(kvs[3].key(), "keyAAA0");
    EXPECT_EQ(kvs[4].key(), "keyAB");
    EXPECT_EQ(kvs[5].key(), "keyAB0");
    EXPECT_EQ(kvs[6].key(), "keyABB");
    EXPECT_EQ(kvs[7].key(), "keyABB0");

    scan_test->DeleteScan();
  }
}

TEST(ScanTest, ScanBeginNormal) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto *factory = scan_test->GetFactory();
  butil::Status ok;

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]

  auto scan = scan_test->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);
  ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                  raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  EXPECT_NE(scan.get(), nullptr);

  uint64_t region_id = 1;
  pb::common::PrefixScanRange range;

  uint64_t max_fetch_cnt = 0;
  bool key_only = false;
  bool disable_auto_release = true;
  std::vector<pb::common::KeyValue> kvs;

  range.mutable_range()->set_start_key("keyAA");
  range.mutable_range()->set_end_key("keyZZ");
  range.set_with_start(true);
  range.set_with_end(true);

  // ok
  ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  EXPECT_EQ(kvs.size(), 0);
}

TEST(ScanTest, ScanContinue) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto scan = scan_test->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  auto *factory = scan_test->GetFactory();
  butil::Status ok;

  uint64_t max_fetch_cnt = 2;
  std::vector<pb::common::KeyValue> kvs;

  // scan_id empty failed
  ok = scan->ScanContinue("", max_fetch_cnt, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // max_fetch_cnt == 0 failed
  ok = scan->ScanContinue(scan_id, 0, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = scan->ScanContinue(scan_id, max_fetch_cnt, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  ok = scan->ScanContinue(scan_id, max_fetch_cnt, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  ok = scan->ScanContinue(scan_id, max_fetch_cnt, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }
}

TEST(ScanTest, ScanRelease) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto scan = scan_test->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  auto *factory = scan_test->GetFactory();
  butil::Status ok;

  // scan_id empty failed
  ok = scan->ScanRelease("");
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = scan->ScanRelease(scan_id);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
}

TEST(ScanTest, IsRecyclable) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto scan = scan_test->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  auto *factory = scan_test->GetFactory();

  bool ret = scan->IsRecyclable();

  EXPECT_EQ(ret, false);

  ret = scan->IsRecyclable();

  EXPECT_EQ(ret, false);

  scan_test->DeleteScan();
}

TEST(ScanTest, scan) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto *factory = scan_test->GetFactory();
  butil::Status ok;

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]

  auto scan = scan_test->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);
  ok = scan->Open(scan_id, factory->GetTimeoutMs(), factory->GetMaxBytesRpc(), factory->GetMaxFetchCntByServer(),
                  raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  EXPECT_NE(scan.get(), nullptr);

  uint64_t region_id = 1;
  pb::common::PrefixScanRange range;

  uint64_t max_fetch_cnt = 0;
  bool key_only = true;
  bool disable_auto_release = false;
  std::vector<pb::common::KeyValue> kvs;

  range.mutable_range()->set_start_key("keyAA");
  range.mutable_range()->set_end_key("keyZZ");
  range.set_with_start(true);
  range.set_with_end(true);

  // ok
  ok = scan->ScanBegin(region_id, range, max_fetch_cnt, key_only, disable_auto_release, kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  EXPECT_EQ(kvs.size(), 0);

  max_fetch_cnt = 1;

  while (true) {
    ok = scan->ScanContinue(scan_id, max_fetch_cnt, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    if (kvs.empty()) {
      break;
    }
  }

  ok = scan->ScanRelease(scan_id);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  scan_test->DeleteScan();
}

TEST(ScanFactoryTest, Init2) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto *factory = scan_test->GetFactory();
  butil::Status ok;
  std::shared_ptr<Config> config = scan_test->GetConfig();
  bool ret = factory->Init(config);

  EXPECT_EQ(ret, true);
}

TEST(ScanFactoryTest, CreateScan) {
  std::string scan_id;

  auto *factory = scan_test->GetFactory();

  std::shared_ptr<ScanContext> scan = factory->CreateScan(&scan_id);

  EXPECT_NE(scan.get(), nullptr);
}

TEST(ScanFactoryTest, FindScan) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto *factory = scan_test->GetFactory();

  scan_test->GetScan(&scan_id);

  auto scan = factory->FindScan(scan_id);

  EXPECT_NE(scan.get(), nullptr);

  scan = factory->FindScan("");

  EXPECT_EQ(scan.get(), nullptr);
}

TEST(ScanFactoryTest, TryDeleteScan) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto *factory = scan_test->GetFactory();

  scan_test->GetScan(&scan_id);

  factory->TryDeleteScan(scan_id);
}

TEST(ScanFactoryTest, DeleteScan) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;

  auto *factory = scan_test->GetFactory();

  scan_test->GetScan(&scan_id);

  factory->DeleteScan(scan_id);
}

TEST(ScanFactoryTest, GetTimeoutMs) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;
  auto *factory = scan_test->GetFactory();

  auto timeout_ms = factory->GetTimeoutMs();
  EXPECT_NE(timeout_ms, 0);
}

TEST(ScanFactoryTest, GetMaxBytesRpc) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;
  auto *factory = scan_test->GetFactory();

  auto max_bytes_rpc = factory->GetMaxBytesRpc();
  EXPECT_NE(max_bytes_rpc, 0);
}

TEST(ScanFactoryTest, GetMaxFetchCntByServer) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;
  auto *factory = scan_test->GetFactory();

  auto max_fetch_cnt_by_server = factory->GetMaxFetchCntByServer();
  EXPECT_NE(max_fetch_cnt_by_server, 0);
}

TEST(ScanFactoryTest, RegularCleaningHandler) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;
  auto *factory = scan_test->GetFactory();

  factory->RegularCleaningHandler(factory);
}

TEST(ScanFactoryTest, max_times) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  std::string scan_id;
  auto *factory = scan_test->GetFactory();

  dingodb::CrontabManager crontab_manager;

  std::shared_ptr<dingodb::Crontab> crontab = std::make_shared<dingodb::Crontab>();
  crontab->name = "SCAN";
  crontab->max_times = 0;
  crontab->interval = 100;
  // crontab->interval_ = factory->GetScanIntervalMs();
  crontab->func = factory->RegularCleaningHandler;
  crontab->arg = factory;

  auto config = scan_test->GetConfig();
  auto name = Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs;
  auto interval = config->GetInt(Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs);

  std::cout << "name : " << name << std::endl;
  std::cout << "interval : " << interval << std::endl;

  crontab_manager.AddAndRunCrontab(crontab);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  crontab_manager.Destroy();
}

TEST(ScanFactoryTest, KvDeleteRange) {
  auto raw_rocks_engine = scan_test->GetRawRocksEngine();
  const std::string &cf_name = kDefaultCf;
  std::shared_ptr<dingodb::RawEngine::Writer> writer = raw_rocks_engine->NewWriter(cf_name);

  // ok
  {
    dingodb::pb::common::Range range;
    range.set_start_key("key");
    range.set_end_key("keyZZZ");

    butil::Status ok = writer->KvDeleteRange(range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string start_key = "key";
    std::string end_key = "keyZZZ";
    std::vector<dingodb::pb::common::KeyValue> kvs;

    std::shared_ptr<dingodb::RawEngine::Reader> reader = raw_rocks_engine->NewReader(cf_name);

    ok = reader->KvScan(start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::cout << "start_key : " << start_key << " "
              << "end_key : " << end_key << std::endl;
    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }
  }
}

TEST(ScanFactoryTest, Destroy) {
  delete scan_test;
  scan_test = nullptr;
}

}  // namespace dingodb

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
#include <exception>
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
#include "proto/common.pb.h"
#include "scan/scan.h"
#include "scan/scan_manager.h"
#include "server/server.h"

namespace dingodb {

static const std::string &kDefaultCf = Constant::kStoreDataCF;  // NOLINT

class ScanTest : public testing::Test {
 public:
  static std::shared_ptr<Config> GetConfig() { return config_; }

  static std::shared_ptr<RawRocksEngine> GetRawRocksEngine() { return raw_raw_rocks_engine_; }
  static ScanManager *&GetManager() { return manager_; }

  static std::shared_ptr<ScanContext> GetScan(std::string *scan_id) {
    if (!scan_) {
      scan_ = manager_->CreateScan(scan_id);
      scan_id_ = *scan_id;
    } else {
      *scan_id = scan_id_;
    }
    return scan_;
  }

  static void DeleteScan() {
    if (!scan_id_.empty() || scan_) {
      manager_->DeleteScan(scan_id_);
    }
    scan_.reset();
    scan_id_ = "";
  }

 protected:
  static void SetUpTestSuite() {
    std::cout << "ScanTest::SetUp()" << std::endl;
    server_ = Server::GetInstance();
    server_->SetRole(pb::common::ClusterRole::STORE);
    server_->InitConfig(kFileName);
    config_manager_ = ConfigManager::GetInstance();
    config_ = config_manager_->GetConfig(pb::common::ClusterRole::STORE);
    manager_ = ScanManager::GetInstance();
    raw_raw_rocks_engine_ = std::make_shared<RawRocksEngine>();
  }

  static void TearDownTestSuite() {
    // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    raw_raw_rocks_engine_->Close();
  }

  void SetUp() override {}
  void TearDown() override {}

 private:
  inline static Server *server_ = nullptr;
  inline static const std::string kFileName = "../../conf/store.yaml";
  inline static ConfigManager *config_manager_ = nullptr;
  inline static std::shared_ptr<Config> config_;
  inline static std::shared_ptr<RawRocksEngine> raw_raw_rocks_engine_;
  inline static ScanManager *manager_ = nullptr;
  inline static std::shared_ptr<ScanContext> scan_;
  inline static std::string scan_id_;
};

TEST_F(ScanTest, InitRocksdb) {
  auto raw_rocks_engine = this->GetRawRocksEngine();

  bool ret = raw_rocks_engine->Init({});
  EXPECT_FALSE(ret);

  std::shared_ptr<Config> config = this->GetConfig();

  // Test for various configuration file exceptions
  ret = raw_rocks_engine->Init(config);
  EXPECT_TRUE(ret);

  auto *manager = this->GetManager();
  manager->Init(config);
}

TEST_F(ScanTest, Open) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;
  auto scan = this->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);

  auto *manager = this->GetManager();

  butil::Status ok;

  // scan id empty failed
  ok = scan->Open("", raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // // timeout == 0 failed
  // ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
  // EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // // max bytes rpc = 0 failed
  // ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
  // EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // // max_fetch_cnt_by_server == 0 failed
  // ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
  // EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // engin empty {} failed
  ok = scan->Open(scan_id, {}, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // kDefaultCf empty  failed
  ok = scan->Open(scan_id, raw_rocks_engine, "");
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
}

// empty data
TEST_F(ScanTest, ScanBegin) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;
  auto scan = this->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);

  butil::Status ok;

  uint64_t region_id = 1;
  pb::common::Range range;

  uint64_t max_fetch_cnt = 10;
  bool key_only = false;
  bool disable_auto_release = true;
  std::vector<pb::common::KeyValue> kvs;

  // range empty
  ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  range.set_start_key("keyAAA");
  range.set_end_key("keyAAA");

  // range value failed
  ok = ScanHandler::ScanBegin(scan, 0, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  range.set_start_key("keyAAA");
  range.set_end_key("keyAAA");

  // range value failed
  ok = ScanHandler::ScanBegin(scan, 0, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  range.set_start_key("keyAAA");
  range.set_end_key("keyAAA");

  // range value failed
  ok = ScanHandler::ScanBegin(scan, 0, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  range.set_start_key("keyAAA");
  range.set_end_key("keyAA");

  // range value failed
  ok = ScanHandler::ScanBegin(scan, 0, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  range.set_start_key("keyAAA");
  range.set_end_key("keyAAC");
  // ok
  ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, false, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  this->DeleteScan();
}

TEST_F(ScanTest, InsertData) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
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

TEST_F(ScanTest, ScanBeginEqual) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;
  auto scan = this->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);

  auto *manager = this->GetManager();

  butil::Status ok;

  ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  EXPECT_NE(scan.get(), nullptr);

  uint64_t region_id = 1;
  pb::common::Range range;

  uint64_t max_fetch_cnt = 10;
  bool key_only = false;
  bool disable_auto_release = true;
  std::vector<pb::common::KeyValue> kvs;

  range.set_start_key("keyAAA");
  range.set_end_key("keyAAB");

  // ok
  ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  this->DeleteScan();
}

TEST_F(ScanTest, ScanBeginOthers) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto *manager = this->GetManager();
  butil::Status ok;

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key end_key equal start_key >= keyAA and end_key <=keyAA
  {
    auto scan = this->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::Range range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyAB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(kvs.size(), 4);
    EXPECT_EQ(kvs[0].key(), "keyAA");
    EXPECT_EQ(kvs[1].key(), "keyAA0");
    EXPECT_EQ(kvs[2].key(), "keyAAA");
    EXPECT_EQ(kvs[3].key(), "keyAAA0");

    this->DeleteScan();
  }

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key >= keyAA and end_key < keyABB
  {
    auto scan = this->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::Range range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyABB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
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

    this->DeleteScan();
  }

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key > keyAA and end_key < keyABB
  {
    auto scan = this->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::Range range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyABB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(kvs.size(), 6);
    EXPECT_EQ(kvs[0].key(), "keyAA");
    EXPECT_EQ(kvs[1].key(), "keyAA0");

    this->DeleteScan();
  }

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key > keyAA and end_key <= keyABB
  {
    auto scan = this->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::Range range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyABB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    EXPECT_EQ(kvs.size(), 6);
    EXPECT_EQ(kvs[0].key(), "keyAA");
    EXPECT_EQ(kvs[1].key(), "keyAA0");
    EXPECT_EQ(kvs[2].key(), "keyAAA");
    EXPECT_EQ(kvs[3].key(), "keyAAA0");

    this->DeleteScan();
  }

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key >= keyAA and end_key <= keyABB
  {
    auto scan = this->GetScan(&scan_id);
    std::cout << "scan_id : " << scan_id << std::endl;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    uint64_t region_id = 1;
    pb::common::Range range;

    uint64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyABB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
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

    this->DeleteScan();
  }
}

TEST_F(ScanTest, ScanBeginNormal) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto *manager = this->GetManager();
  butil::Status ok;

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]

  auto scan = this->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);
  ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  EXPECT_NE(scan.get(), nullptr);

  uint64_t region_id = 1;
  pb::common::Range range;

  uint64_t max_fetch_cnt = 0;
  bool key_only = false;
  bool disable_auto_release = true;
  std::vector<pb::common::KeyValue> kvs;

  range.set_start_key("keyAA");
  range.set_end_key("keyZZ");

  // ok
  ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  EXPECT_EQ(kvs.size(), 0);
}

TEST_F(ScanTest, ScanContinue) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto scan = this->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  auto *manager = this->GetManager();
  butil::Status ok;

  uint64_t max_fetch_cnt = 2;
  std::vector<pb::common::KeyValue> kvs;

  // scan_id empty failed
  ok = ScanHandler::ScanContinue(scan, "", max_fetch_cnt, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // max_fetch_cnt == 0 failed
  ok = ScanHandler::ScanContinue(scan, scan_id, 0, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = ScanHandler::ScanContinue(scan, scan_id, max_fetch_cnt, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  ok = ScanHandler::ScanContinue(scan, scan_id, max_fetch_cnt, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  ok = ScanHandler::ScanContinue(scan, scan_id, max_fetch_cnt, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }
}

TEST_F(ScanTest, ScanRelease) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto scan = this->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  auto *manager = this->GetManager();
  butil::Status ok;

  // scan_id empty failed
  ok = ScanHandler::ScanRelease(scan, "");
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = ScanHandler::ScanRelease(scan, scan_id);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
}

TEST_F(ScanTest, IsRecyclable) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto scan = this->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  auto *manager = this->GetManager();

  bool ret = scan->IsRecyclable();

  EXPECT_EQ(ret, false);

  ret = scan->IsRecyclable();

  EXPECT_EQ(ret, false);

  this->DeleteScan();
}

TEST_F(ScanTest, scan) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto *manager = this->GetManager();
  butil::Status ok;

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]

  auto scan = this->GetScan(&scan_id);
  std::cout << "scan_id : " << scan_id << std::endl;

  EXPECT_NE(scan.get(), nullptr);
  ok = scan->Open(scan_id, raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  EXPECT_NE(scan.get(), nullptr);

  uint64_t region_id = 1;
  pb::common::Range range;

  uint64_t max_fetch_cnt = 0;
  bool key_only = true;
  bool disable_auto_release = false;
  std::vector<pb::common::KeyValue> kvs;

  range.set_start_key("keyAA");
  range.set_end_key("keyZZ");

  // ok
  ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    std::cout << kv.key() << ":" << kv.value() << std::endl;
  }

  EXPECT_EQ(kvs.size(), 0);

  max_fetch_cnt = 1;

  while (true) {
    ok = ScanHandler::ScanContinue(scan, scan_id, max_fetch_cnt, &kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      std::cout << kv.key() << ":" << kv.value() << std::endl;
    }

    if (kvs.empty()) {
      break;
    }
    kvs.clear();
  }

  ok = ScanHandler::ScanRelease(scan, scan_id);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  this->DeleteScan();
}

TEST_F(ScanTest, Init2) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto *manager = this->GetManager();
  butil::Status ok;
  std::shared_ptr<Config> config = this->GetConfig();
  bool ret = manager->Init(config);

  EXPECT_EQ(ret, true);
}

TEST_F(ScanTest, CreateScan) {
  std::string scan_id;

  auto *manager = this->GetManager();

  std::shared_ptr<ScanContext> scan = manager->CreateScan(&scan_id);

  EXPECT_NE(scan.get(), nullptr);
}

TEST_F(ScanTest, FindScan) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto *manager = this->GetManager();

  this->GetScan(&scan_id);

  auto scan = manager->FindScan(scan_id);

  EXPECT_NE(scan.get(), nullptr);

  scan = manager->FindScan("");

  EXPECT_EQ(scan.get(), nullptr);
}

TEST_F(ScanTest, TryDeleteScan) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto *manager = this->GetManager();

  this->GetScan(&scan_id);

  manager->TryDeleteScan(scan_id);
}

TEST_F(ScanTest, DeleteScan) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;

  auto *manager = this->GetManager();

  this->GetScan(&scan_id);

  manager->DeleteScan(scan_id);
}

TEST_F(ScanTest, GetTimeoutMs) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;
  auto *manager = this->GetManager();

  auto timeout_ms = manager->GetTimeoutMs();
  EXPECT_NE(timeout_ms, 0);
}

TEST_F(ScanTest, GetMaxBytesRpc) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;
  auto *manager = this->GetManager();

  auto max_bytes_rpc = manager->GetMaxBytesRpc();
  EXPECT_NE(max_bytes_rpc, 0);
}

TEST_F(ScanTest, GetMaxFetchCntByServer) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;
  auto *manager = this->GetManager();

  auto max_fetch_cnt_by_server = manager->GetMaxFetchCntByServer();
  EXPECT_NE(max_fetch_cnt_by_server, 0);
}

TEST_F(ScanTest, RegularCleaningHandler) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;
  auto *manager = this->GetManager();

  manager->RegularCleaningHandler(manager);
}

TEST_F(ScanTest, max_times) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  std::string scan_id;
  auto *manager = this->GetManager();

  dingodb::CrontabManager crontab_manager;

  std::shared_ptr<dingodb::Crontab> crontab = std::make_shared<dingodb::Crontab>();
  crontab->name = "SCAN";
  crontab->max_times = 0;
  crontab->interval = 100;
  // crontab->interval_ = manager->GetScanIntervalMs();
  crontab->func = manager->RegularCleaningHandler;
  crontab->arg = manager;

  auto config = this->GetConfig();
  auto name = Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs;
  int interval = -1;
  try {
    interval = config->GetInt(Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs);
  } catch (const std::exception &e) {
    std::cout << "exception GetInt " << Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs
              << " failed. use default" << std::endl;
    interval = 60000;
  }

  if (interval <= 0) {
    std::cout << "GetInt " << Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalMs << " failed. use default"
              << std::endl;
    interval = 60000;
  }

  std::cout << "name : " << name << std::endl;
  std::cout << "interval : " << interval << std::endl;

  crontab_manager.AddAndRunCrontab(crontab);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  crontab_manager.Destroy();
}

TEST_F(ScanTest, KvDeleteRange) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
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

}  // namespace dingodb

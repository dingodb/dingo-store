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
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "config/yaml_config.h"
#include "crontab/crontab.h"
#include "engine/rocks_raw_engine.h"
#include "proto/common.pb.h"
#include "scan/scan.h"
#include "scan/scan_manager.h"

namespace dingodb {

static const std::string &kDefaultCf = "default";  // NOLINT

static const std::vector<std::string> kAllCFs = {kDefaultCf};

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
    kStorePath +
    "\n"
    "  scan_v2:\n"
    "    scan_interval_s: 30\n"
    "    timeout_s: 1800\n"
    "    max_bytes_rpc: 4194304\n"
    "    max_fetch_cnt_by_server: 1000\n";

class ScanV2Test : public testing::Test {
 public:
  static std::shared_ptr<Config> GetConfig() { return config_; }
  static std::shared_ptr<RocksRawEngine> GetRawRocksEngine() { return engine_; }
  static ScanManagerV2 &GetManager() { return ScanManagerV2::GetInstance(); }

  static std::shared_ptr<ScanContext> GetScan(int64_t *scan_id) {
    if (!scan_) {
      scan_ = ScanManagerV2::GetInstance().CreateScan(*scan_id);
      scan_id_ = *scan_id;
    } else {
      *scan_id = scan_id_;
    }
    return scan_;
  }

  static void DeleteScan() {
    if (scan_) {
      ScanManagerV2::GetInstance().DeleteScan(scan_id_);
    }
    scan_.reset();
    scan_id_ = std::numeric_limits<int64_t>::max();
  }

 protected:
  static void SetUpTestSuite() {
    Helper::CreateDirectories(kStorePath);

    config_ = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config_->Load(kYamlConfigContent));

    engine_ = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine_ != nullptr);
    ASSERT_TRUE(engine_->Init(config_, kAllCFs));

    ScanManagerV2::GetInstance().Init(config_);
  }

  static void TearDownTestSuite() {
    engine_->Close();
    engine_->Destroy();
    Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  void SetUp() override {}
  void TearDown() override {}

 private:
  inline static std::shared_ptr<Config> config_;          // NOLINT
  inline static std::shared_ptr<RocksRawEngine> engine_;  // NOLINT
  inline static std::shared_ptr<ScanContext> scan_;       // NOLINT
  inline static int64_t scan_id_;                         // NOLINT
};

static std::chrono::milliseconds GetCurrentTime() {
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::chrono::nanoseconds nanosec = now.time_since_epoch();
  std::chrono::milliseconds millisec = std::chrono::duration_cast<std::chrono::milliseconds>(nanosec);

  return millisec;
}

TEST_F(ScanV2Test, Time) {
  std::string t = Helper::NowTime();
  LOG(INFO) << "now : " << t;

  auto ms = GetCurrentTime();
  std::string formate_str;
  formate_str = Helper::FormatMsTime(ms.count(), "%Y-%m-%d %H:%M:%S");
  LOG(INFO) << "formate_str : " << formate_str;
}

TEST_F(ScanV2Test, Open) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  int64_t scan_id = 1;
  auto scan = this->GetScan(&scan_id);
  LOG(INFO) << "scan_id : " << scan_id;

  EXPECT_NE(scan.get(), nullptr);

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
  ok = scan->Open(std::to_string(scan_id), {}, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // kDefaultCf empty  failed
  ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, "");
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
}

// empty data
TEST_F(ScanV2Test, ScanBegin) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  int64_t scan_id = 1;
  auto scan = this->GetScan(&scan_id);
  LOG(INFO) << "scan_id : " << scan_id;

  EXPECT_NE(scan.get(), nullptr);

  butil::Status ok;

  int64_t region_id = 1;
  pb::common::Range range;

  int64_t max_fetch_cnt = 10;
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

TEST_F(ScanV2Test, InsertData) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  const std::string &cf_name = kDefaultCf;
  auto writer = raw_rocks_engine->Writer();

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyAA");
    kv.set_value("valueAA");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyAA" + std::to_string(i));
      kv.set_value("valueAA" + std::to_string(i));

      butil::Status ok = writer->KvPut(cf_name, kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyAAA");
    kv.set_value("valueAAA");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyAAA" + std::to_string(i));
      kv.set_value("valueAAA" + std::to_string(i));

      butil::Status ok = writer->KvPut(cf_name, kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyABB");
    kv.set_value("valueABB");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyABB" + std::to_string(i));
      kv.set_value("valueABB" + std::to_string(i));

      butil::Status ok = writer->KvPut(cf_name, kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyABC");
    kv.set_value("valueABC");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyABC" + std::to_string(i));
      kv.set_value("valueABC" + std::to_string(i));

      butil::Status ok = writer->KvPut(cf_name, kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyABD");
    kv.set_value("valueABD");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyABD" + std::to_string(i));
      kv.set_value("valueABD" + std::to_string(i));

      butil::Status ok = writer->KvPut(cf_name, kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }

  {
    dingodb::pb::common::KeyValue kv;
    kv.set_key("keyAB");
    kv.set_value("valueAB");

    butil::Status ok = writer->KvPut(cf_name, kv);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
  }

  {
    for (size_t i = 0; i < 1; i++) {
      dingodb::pb::common::KeyValue kv;
      kv.set_key("keyAB" + std::to_string(i));
      kv.set_value("valueAB" + std::to_string(i));

      butil::Status ok = writer->KvPut(cf_name, kv);
      EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
    }
  }
}

TEST_F(ScanV2Test, ScanBeginEqual) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  int64_t scan_id = 1;
  auto scan = this->GetScan(&scan_id);
  LOG(INFO) << "scan_id : " << scan_id;

  EXPECT_NE(scan.get(), nullptr);

  butil::Status ok;

  ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  EXPECT_NE(scan.get(), nullptr);

  int64_t region_id = 1;
  pb::common::Range range;

  int64_t max_fetch_cnt = 10;
  bool key_only = false;
  bool disable_auto_release = true;
  std::vector<pb::common::KeyValue> kvs;

  range.set_start_key("keyAAA");
  range.set_end_key("keyAAB");

  // ok
  ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    LOG(INFO) << kv.key() << ":" << kv.value();
  }

  this->DeleteScan();
}

TEST_F(ScanV2Test, ScanBeginOthers) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  int64_t scan_id = 1;

  butil::Status ok;

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]
  // test start_key end_key equal start_key >= keyAA and end_key <=keyAA
  {
    auto scan = this->GetScan(&scan_id);
    LOG(INFO) << "scan_id : " << scan_id;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    int64_t region_id = 1;
    pb::common::Range range;

    int64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyAB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
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
    LOG(INFO) << "scan_id : " << scan_id;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    int64_t region_id = 1;
    pb::common::Range range;

    int64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyABB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
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
    LOG(INFO) << "scan_id : " << scan_id;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    int64_t region_id = 1;
    pb::common::Range range;

    int64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyABB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
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
    LOG(INFO) << "scan_id : " << scan_id;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    int64_t region_id = 1;
    pb::common::Range range;

    int64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyABB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
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
    LOG(INFO) << "scan_id : " << scan_id;

    EXPECT_NE(scan.get(), nullptr);
    ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, kDefaultCf);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    EXPECT_NE(scan.get(), nullptr);

    int64_t region_id = 1;
    pb::common::Range range;

    int64_t max_fetch_cnt = 100;
    bool key_only = false;
    bool disable_auto_release = true;
    std::vector<pb::common::KeyValue> kvs;

    range.set_start_key("keyAA");
    range.set_end_key("keyABB");

    // ok
    ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
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

TEST_F(ScanV2Test, ScanBeginNormal) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  int64_t scan_id = 1;

  butil::Status ok;

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]

  auto scan = this->GetScan(&scan_id);
  LOG(INFO) << "scan_id : " << scan_id;

  EXPECT_NE(scan.get(), nullptr);
  ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  EXPECT_NE(scan.get(), nullptr);

  int64_t region_id = 1;
  pb::common::Range range;

  int64_t max_fetch_cnt = 0;
  bool key_only = false;
  bool disable_auto_release = true;
  std::vector<pb::common::KeyValue> kvs;

  range.set_start_key("keyAA");
  range.set_end_key("keyZZ");

  // ok
  ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    LOG(INFO) << kv.key() << ":" << kv.value();
  }

  EXPECT_EQ(kvs.size(), 0);
}

TEST_F(ScanV2Test, ScanContinue) {
  int64_t scan_id = 1;

  auto scan = this->GetScan(&scan_id);
  LOG(INFO) << "scan_id : " << scan_id;

  butil::Status ok;

  int64_t max_fetch_cnt = 2;
  std::vector<pb::common::KeyValue> kvs;
  bool has_more = false;

  // scan_id empty failed
  ok = ScanHandler::ScanContinue(scan, "", max_fetch_cnt, &kvs, has_more);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  // max_fetch_cnt == 0 failed
  ok = ScanHandler::ScanContinue(scan, std::to_string(scan_id), 0, &kvs, has_more);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = ScanHandler::ScanContinue(scan, std::to_string(scan_id), max_fetch_cnt, &kvs, has_more);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    LOG(INFO) << kv.key() << ":" << kv.value();
  }

  ok = ScanHandler::ScanContinue(scan, std::to_string(scan_id), max_fetch_cnt, &kvs, has_more);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    LOG(INFO) << kv.key() << ":" << kv.value();
  }

  ok = ScanHandler::ScanContinue(scan, std::to_string(scan_id), max_fetch_cnt, &kvs, has_more);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    LOG(INFO) << kv.key() << ":" << kv.value();
  }
}

TEST_F(ScanV2Test, ScanRelease) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  int64_t scan_id = 1;

  auto scan = this->GetScan(&scan_id);
  LOG(INFO) << "scan_id : " << scan_id;

  butil::Status ok;

  // scan_id empty failed
  ok = ScanHandler::ScanRelease(scan, "");
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::EILLEGAL_PARAMTETERS);

  ok = ScanHandler::ScanRelease(scan, std::to_string(scan_id));
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);
}

TEST_F(ScanV2Test, IsRecyclable) {
  int64_t scan_id = 1;

  auto scan = this->GetScan(&scan_id);
  LOG(INFO) << "scan_id : " << scan_id;

  bool ret = scan->IsRecyclable();

  EXPECT_EQ(ret, false);

  ret = scan->IsRecyclable();

  EXPECT_EQ(ret, false);

  this->DeleteScan();
}

TEST_F(ScanV2Test, scan) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  int64_t scan_id = 1;

  butil::Status ok;

  // [keyAA, keyAA0, keyAAA, keyAAA0, keyABB, keyABB0, keyABC, keyABC0, keyABD, keyABD0, keyAB, keyAB0 ]

  auto scan = this->GetScan(&scan_id);
  LOG(INFO) << "scan_id : " << scan_id;

  EXPECT_NE(scan.get(), nullptr);
  ok = scan->Open(std::to_string(scan_id), raw_rocks_engine, kDefaultCf);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  EXPECT_NE(scan.get(), nullptr);

  int64_t region_id = 1;
  pb::common::Range range;

  int64_t max_fetch_cnt = 0;
  bool key_only = true;
  bool disable_auto_release = false;
  std::vector<pb::common::KeyValue> kvs;

  range.set_start_key("keyAA");
  range.set_end_key("keyZZ");

  // ok
  ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, true, {}, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    LOG(INFO) << kv.key() << ":" << kv.value();
  }

  EXPECT_EQ(kvs.size(), 0);

  max_fetch_cnt = 1;

  while (true) {
    bool has_more = false;
    ok = ScanHandler::ScanContinue(scan, std::to_string(scan_id), max_fetch_cnt, &kvs, has_more);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }

    if (kvs.empty()) {
      break;
    }
    kvs.clear();
  }

  ok = ScanHandler::ScanRelease(scan, std::to_string(scan_id));
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  this->DeleteScan();
}

TEST_F(ScanV2Test, Init2) {
  int64_t scan_id = 1;

  auto &manager = this->GetManager();
  butil::Status ok;
  std::shared_ptr<Config> config = this->GetConfig();
  bool ret = manager.Init(config);

  EXPECT_EQ(ret, true);
}

TEST_F(ScanV2Test, CreateScan) {
  int64_t scan_id = 1;

  auto &manager = this->GetManager();

  std::shared_ptr<ScanContext> scan = manager.CreateScan(scan_id);

  EXPECT_NE(scan.get(), nullptr);
}

TEST_F(ScanV2Test, FindScan) {
  int64_t scan_id = 1;

  auto &manager = this->GetManager();

  this->GetScan(&scan_id);

  auto scan = manager.FindScan(scan_id);

  EXPECT_NE(scan.get(), nullptr);

  scan = manager.FindScan(scan_id);

  EXPECT_NE(scan.get(), nullptr);
}

TEST_F(ScanV2Test, TryDeleteScan) {
  int64_t scan_id = 1;

  auto &manager = this->GetManager();

  this->GetScan(&scan_id);

  manager.TryDeleteScan(scan_id);
}

TEST_F(ScanV2Test, DeleteScan) {
  int64_t scan_id = 1;

  auto &manager = this->GetManager();

  this->GetScan(&scan_id);

  manager.DeleteScan(scan_id);
}

TEST_F(ScanV2Test, GetTimeoutMs) {
  int64_t scan_id = 1;
  auto &manager = this->GetManager();

  auto timeout_ms = manager.GetTimeoutMs();
  EXPECT_NE(timeout_ms, 0);
}

TEST_F(ScanV2Test, GetMaxBytesRpc) {
  int64_t scan_id = 1;
  auto &manager = this->GetManager();

  auto max_bytes_rpc = manager.GetMaxBytesRpc();
  EXPECT_NE(max_bytes_rpc, 0);
}

TEST_F(ScanV2Test, GetMaxFetchCntByServer) {
  int64_t scan_id = 1;
  auto &manager = this->GetManager();

  auto max_fetch_cnt_by_server = manager.GetMaxFetchCntByServer();
  EXPECT_NE(max_fetch_cnt_by_server, 0);
}

TEST_F(ScanV2Test, RegularCleaningHandler) {
  int64_t scan_id = 1;
  auto &manager = this->GetManager();

  manager.RegularCleaningHandler(nullptr);
}

TEST_F(ScanV2Test, max_times) {
  int64_t scan_id = 1;
  auto &manager = this->GetManager();

  dingodb::CrontabManager crontab_manager;

  std::shared_ptr<dingodb::Crontab> crontab = std::make_shared<dingodb::Crontab>();
  crontab->name = "SCAN_V2";
  crontab->max_times = 0;
  crontab->interval = 100;
  // crontab->interval_ = manager->GetScanIntervalMs();
  crontab->func = manager.RegularCleaningHandler;
  crontab->arg = nullptr;

  auto config = this->GetConfig();
  auto name = Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalS;
  int interval = -1;
  try {
    interval = config->GetInt(Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalS) * 1000;
  } catch (const std::exception &e) {
    LOG(INFO) << "exception GetInt " << Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalS
              << " failed. use default";
    interval = 60000;
  }

  if (interval <= 0) {
    LOG(INFO) << "GetInt " << Constant::kStoreScan + "." + Constant::kStoreScanScanIntervalS << " failed. use default";
    interval = 60000;
  }

  LOG(INFO) << "name : " << name;
  LOG(INFO) << "interval : " << interval;

  crontab_manager.AddAndRunCrontab(crontab);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  crontab_manager.Destroy();
}

TEST_F(ScanV2Test, KvDeleteRange) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  const std::string &cf_name = kDefaultCf;
  auto writer = raw_rocks_engine->Writer();

  // ok
  {
    dingodb::pb::common::Range range;
    range.set_start_key("key");
    range.set_end_key("keyZZZ");

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string start_key = "key";
    std::string end_key = "keyZZZ";
    std::vector<dingodb::pb::common::KeyValue> kvs;

    auto reader = raw_rocks_engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << start_key << " "
              << "end_key : " << end_key;
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }
}

}  // namespace dingodb

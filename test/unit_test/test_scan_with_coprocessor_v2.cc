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

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "config/yaml_config.h"
#include "coprocessor/coprocessor_v2.h"
#include "engine/rocks_raw_engine.h"
#include "proto/common.pb.h"
#include "scan/scan.h"
#include "scan/scan_manager.h"
#include "serial/record_encoder.h"
#include "serial/schema/base_schema.h"
#include "serial/schema/boolean_schema.h"
#include "serial/schema/double_schema.h"
#include "serial/schema/float_schema.h"
#include "serial/schema/integer_schema.h"
#include "serial/schema/long_schema.h"
#include "serial/schema/string_schema.h"

namespace dingodb {

static const std::string &kDefaultCf = "default";  // NOLINT

static const std::vector<std::string> kAllCFs = {Constant::kStoreDataCF};

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

static std::string StrToHex(std::string str, std::string separator = "") {
  const std::string hex = "0123456789ABCDEF";
  std::stringstream ss;

  for (char i : str) ss << hex[(unsigned char)i >> 4] << hex[(unsigned char)i & 0xf] << separator;

  return ss.str();
}

class ScanWithCoprocessorV2 : public testing::Test {
 public:
  static std::shared_ptr<Config> GetConfig() { return config_; }

  static std::shared_ptr<RocksRawEngine> GetRawRocksEngine() { return engine; }
  static ScanManager &GetManager() { return ScanManager::GetInstance(); }

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

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config_, kAllCFs));

    ScanManagerV2::GetInstance().Init(config_);
  }

  static void TearDownTestSuite() {
    coprocessor.reset();
    engine->Close();
    engine->Destroy();
    Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  void SetUp() override {}
  void TearDown() override {}

 private:
  inline static std::shared_ptr<Config> config_;     // NOLINT
  inline static std::shared_ptr<ScanContext> scan_;  // NOLINT
  inline static std::int64_t scan_id_;               // NOLINT

 public:
  inline static std::shared_ptr<RocksRawEngine> engine;
  inline static std::shared_ptr<CoprocessorV2> coprocessor;
  static inline std::vector<std::string> keys;
};

TEST_F(ScanWithCoprocessorV2, Prepare) {
  int schema_version = 1;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas;
  long common_id = 1;  // NOLINT

  schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

  schemas->reserve(6);

  std::shared_ptr<DingoSchema<std::optional<bool>>> bool_schema = std::make_shared<DingoSchema<std::optional<bool>>>();
  bool_schema->SetIsKey(true);
  bool_schema->SetAllowNull(true);
  bool_schema->SetIndex(0);

  schemas->emplace_back(std::move(bool_schema));

  std::shared_ptr<DingoSchema<std::optional<int32_t>>> int_schema =
      std::make_shared<DingoSchema<std::optional<int32_t>>>();
  int_schema->SetIsKey(false);
  int_schema->SetAllowNull(true);
  int_schema->SetIndex(1);
  schemas->emplace_back(std::move(int_schema));

  std::shared_ptr<DingoSchema<std::optional<float>>> float_schema =
      std::make_shared<DingoSchema<std::optional<float>>>();
  float_schema->SetIsKey(false);
  float_schema->SetAllowNull(true);
  float_schema->SetIndex(2);
  schemas->emplace_back(std::move(float_schema));

  std::shared_ptr<DingoSchema<std::optional<int64_t>>> long_schema =
      std::make_shared<DingoSchema<std::optional<int64_t>>>();
  long_schema->SetIsKey(false);
  long_schema->SetAllowNull(true);
  long_schema->SetIndex(3);
  schemas->emplace_back(std::move(long_schema));

  std::shared_ptr<DingoSchema<std::optional<double>>> double_schema =
      std::make_shared<DingoSchema<std::optional<double>>>();
  double_schema->SetIsKey(true);
  double_schema->SetAllowNull(true);
  double_schema->SetIndex(4);
  schemas->emplace_back(std::move(double_schema));

  std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> string_schema =
      std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
  string_schema->SetIsKey(true);
  string_schema->SetAllowNull(true);
  string_schema->SetIndex(5);
  schemas->emplace_back(std::move(string_schema));

  RecordEncoder record_encoder(schema_version, schemas, common_id);

  // 1
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(std::nullopt);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(std::nullopt);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(std::nullopt);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(std::nullopt);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(std::nullopt);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    record.emplace_back(std::move(any_string));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();

    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }

  // 2
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(false);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(2);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(2.23);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(200);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(2.4545);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_22222"));
    record.emplace_back(std::move(any_string));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();
    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }

  // 3
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(true);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(3);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(3.23);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(300);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(3.4545);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_33333"));
    record.emplace_back(std::move(any_string));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();
    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }

  // 4
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(4);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(std::nullopt);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(4.23);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(400);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(4.4545);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_44444"));
    record.emplace_back(std::move(any_string));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();
    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }

  // 5
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(true);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(5);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(5.23);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(std::nullopt);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(std::nullopt);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_55555"));
    record.emplace_back(std::move(any_string));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();
    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }

  // 6
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(6);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(std::nullopt);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(6.23);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(600);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(6.4545);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    record.emplace_back(std::move(any_string));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();
    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }

  // 7
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(false);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(7);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(7.23);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(700);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(7.4545);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_77777"));
    record.emplace_back(std::move(any_string));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();
    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }

  // 8
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);
    std::any any_bool = std::optional<bool>(true);
    record.emplace_back(std::move(any_bool));

    std::any any_int = std::optional<int32_t>(8);
    record.emplace_back(std::move(any_int));

    std::any any_float = std::optional<float>(8.23);
    record.emplace_back(std::move(any_float));

    std::any any_long = std::optional<int64_t>(800);
    record.emplace_back(std::move(any_long));

    std::any any_double = std::optional<double>(8.4545);
    record.emplace_back(std::move(any_double));

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    record.emplace_back(std::move(any_string));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();
    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }
}

TEST_F(ScanWithCoprocessorV2, scan) {
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
  bool disable_auto_release = false;
  std::vector<pb::common::KeyValue> kvs;

  std::sort(keys.begin(), keys.end());

  std::string my_min_key(keys.front());
  std::string my_max_key(keys.back());

  std::string my_min_key_s = StrToHex(my_min_key, " ");
  LOG(INFO) << "my_min_key_s : " << my_min_key_s;

  std::string my_max_key_s = StrToHex(my_max_key, " ");
  LOG(INFO) << "my_max_key_s : " << my_max_key_s;

  range.set_start_key(my_min_key);
  range.set_end_key(Helper::PrefixNext(my_max_key));

  pb::common::CoprocessorV2 pb_coprocessor;
  {
    pb_coprocessor.set_schema_version(1);

    auto *original_schema = pb_coprocessor.mutable_original_schema();
    original_schema->set_common_id(1);

    auto *schema1 = original_schema->add_schema();
    {
      schema1->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1->set_is_key(true);
      schema1->set_is_nullable(true);
      schema1->set_index(0);
      schema1->set_name("name_bool");
    }

    auto *schema2 = original_schema->add_schema();
    {
      schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2->set_is_key(false);
      schema2->set_is_nullable(true);
      schema2->set_index(1);
      schema2->set_name("name_int");
    }

    auto *schema3 = original_schema->add_schema();
    {
      schema3->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema3->set_is_key(false);
      schema3->set_is_nullable(true);
      schema3->set_index(2);
      schema3->set_name("name_float");
    }

    auto *schema4 = original_schema->add_schema();
    {
      schema4->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema4->set_is_key(false);
      schema4->set_is_nullable(true);
      schema4->set_index(3);
      schema4->set_name("name_int64");
    }

    auto *schema5 = original_schema->add_schema();
    {
      schema5->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5->set_is_key(true);
      schema5->set_is_nullable(true);
      schema5->set_index(4);
      schema5->set_name("name_double");
    }

    auto *schema6 = original_schema->add_schema();
    {
      schema6->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema6->set_is_key(true);
      schema6->set_is_nullable(true);
      schema6->set_index(5);
      schema6->set_name("name_string");
    }

    auto *selection_columns = pb_coprocessor.mutable_selection_columns();
    selection_columns->Add(0);
    selection_columns->Add(1);
    selection_columns->Add(2);
    selection_columns->Add(3);
    selection_columns->Add(4);
    selection_columns->Add(5);

    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));

    auto *result_schema = pb_coprocessor.mutable_result_schema();
    result_schema->set_common_id(1);

    {
      auto *schema1 = result_schema->add_schema();
      {
        schema1->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
        schema1->set_is_key(true);
        schema1->set_is_nullable(true);
        schema1->set_index(0);
        schema1->set_name("name_bool");
      }

      auto *schema2 = result_schema->add_schema();
      {
        schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
        schema2->set_is_key(false);
        schema2->set_is_nullable(true);
        schema2->set_index(1);
        schema2->set_name("name_int");
      }

      auto *schema3 = result_schema->add_schema();
      {
        schema3->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
        schema3->set_is_key(false);
        schema3->set_is_nullable(true);
        schema3->set_index(2);
        schema3->set_name("name_float");
      }

      auto *schema4 = result_schema->add_schema();
      {
        schema4->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
        schema4->set_is_key(false);
        schema4->set_is_nullable(true);
        schema4->set_index(3);
        schema4->set_name("name_int64");
      }

      auto *schema5 = result_schema->add_schema();
      {
        schema5->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
        schema5->set_is_key(true);
        schema5->set_is_nullable(true);
        schema5->set_index(4);
        schema5->set_name("name_double");
      }

      auto *schema6 = result_schema->add_schema();
      {
        schema6->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
        schema6->set_is_key(true);
        schema6->set_is_nullable(true);
        schema6->set_index(5);
        schema6->set_name("name_string");
      }
    }
  }

  // ok
  ok = ScanHandler::ScanBegin(scan, region_id, range, max_fetch_cnt, key_only, disable_auto_release, false,
                              pb_coprocessor, &kvs);
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  for (const auto &kv : kvs) {
    LOG(INFO) << kv.key() << ":" << kv.value();
  }

  EXPECT_EQ(kvs.size(), 0);

  max_fetch_cnt = 1;

  int cnt = 0;
  while (true) {
    bool has_more = false;
    ok = ScanHandler::ScanContinue(scan, std::to_string(scan_id), max_fetch_cnt, &kvs, has_more);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    for (const auto &kv : kvs) {
      LOG(INFO) << StrToHex(kv.key(), " ") << ":" << StrToHex(kv.value(), " ");
    }

    cnt += kvs.size();

    if (!has_more) {
      break;
    }
    kvs.clear();
  }

  LOG(INFO) << "cnt : " << cnt;

  EXPECT_EQ(cnt, keys.size());

  ok = ScanHandler::ScanRelease(scan, std::to_string(scan_id));
  EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

  this->DeleteScan();
}

TEST_F(ScanWithCoprocessorV2, KvDeleteRange) {
  auto raw_rocks_engine = this->GetRawRocksEngine();
  const std::string &cf_name = kDefaultCf;
  auto writer = raw_rocks_engine->Writer();

  // ok
  {
    pb::common::Range range;

    std::sort(keys.begin(), keys.end());

    std::string my_min_key(keys.front());
    std::string my_max_key(keys.back());

    std::string my_min_key_s = StrToHex(my_min_key, " ");
    LOG(INFO) << "my_min_key_s : " << my_min_key_s;

    std::string my_max_key_s = StrToHex(my_max_key, " ");
    LOG(INFO) << "my_max_key_s : " << my_max_key_s;

    range.set_start_key(my_min_key);
    range.set_end_key(Helper::PrefixNext(my_max_key));

    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    std::string start_key = my_min_key;
    std::string end_key = Helper::PrefixNext(my_max_key);
    std::vector<dingodb::pb::common::KeyValue> kvs;

    auto reader = raw_rocks_engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << StrToHex(start_key, " ") << "\n"
              << "end_key : " << StrToHex(end_key, " ");
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }
}

}  // namespace dingodb

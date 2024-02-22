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
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "config/config.h"
#include "config/yaml_config.h"
#include "coordinator/tso_control.h"
#include "coprocessor/coprocessor_scalar.h"
#include "coprocessor/coprocessor_v2.h"
#include "engine/rocks_raw_engine.h"
#include "engine/txn_engine_helper.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/store_internal.pb.h"
#include "serial/record_encoder.h"
#include "serial/schema/boolean_schema.h"
#include "serial/schema/float_schema.h"
#include "serial/schema/integer_schema.h"
#include "serial/schema/long_schema.h"
#include "serial/schema/string_schema.h"

namespace dingodb {

static const std::string kDefaultCf = "default";

static const std::vector<std::string> kAllCFs = {Constant::kTxnWriteCF, Constant::kTxnDataCF, Constant::kTxnLockCF,
                                                 Constant::kStoreDataCF};

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

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

static std::string StrToHex(std::string str, std::string separator = "") {
  const std::string hex = "0123456789ABCDEF";
  std::stringstream ss;

  for (char i : str) ss << hex[(unsigned char)i >> 4] << hex[(unsigned char)i & 0xf] << separator;

  return ss.str();
}

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

class CoprocessorTestV2 : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    dingodb::Helper::CreateDirectories(kStorePath);
    std::srand(std::time(nullptr));

    std::shared_ptr<Config> config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));

    coprocessor = std::make_shared<CoprocessorV2>();
    ASSERT_TRUE(coprocessor != nullptr);
    coprocessor_scalar = std::make_shared<CoprocessorScalar>();
    ASSERT_TRUE(coprocessor_scalar != nullptr);
  }

  static void TearDownTestSuite() {
    engine->Close();
    engine->Destroy();
    dingodb::Helper::RemoveAllFileOrDirectory(kRootPath);
  }

  void SetUp() override {}

  void TearDown() override {}

  static void DeleteRange();

  static inline std::shared_ptr<RocksRawEngine> engine;
  static inline std::shared_ptr<CoprocessorV2> coprocessor;
  static inline std::shared_ptr<CoprocessorScalar> coprocessor_scalar;
  static inline int64_t start_ts = 0;
  static inline int64_t end_ts = 0;

  static inline std::vector<std::string> keys;
};

static int64_t Tso2Timestamp(pb::meta::TsoTimestamp tso) {
  return (tso.physical() << ::dingodb::kLogicalBits) + tso.logical();
}

void CoprocessorTestV2::DeleteRange() {
  const std::string &cf_name = kDefaultCf;
  auto writer = engine->Writer();

  pb::common::Range range;

  // std::sort(keys.begin(), keys.end());

  // std::string my_min_key(keys.front());
  // std::string my_max_key(Helper::PrefixNext(keys.back()));

  std::string my_min_key(Helper::GenMinStartKey());
  std::string my_max_key(Helper::GenMaxStartKey());

  std::string my_min_key_s = StrToHex(my_min_key, " ");
  LOG(INFO) << "my_min_key_s : " << my_min_key_s;

  std::string my_max_key_s = StrToHex(my_max_key, " ");
  LOG(INFO) << "my_max_key_s : " << my_max_key_s;

  range.set_start_key(my_min_key);
  range.set_end_key(Helper::PrefixNext(my_max_key));

  // ok
  {
    butil::Status ok = writer->KvDeleteRange(cf_name, range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    const std::string &start_key = my_min_key;
    std::string end_key = Helper::PrefixNext(my_max_key);
    std::vector<dingodb::pb::common::KeyValue> kvs;

    auto reader = engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << StrToHex(start_key, " ") << "\n"
              << "end_key : " << StrToHex(end_key, " ");
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }

  // ok
  {
    butil::Status ok = writer->KvDeleteRange(Constant::kTxnWriteCF, range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    const std::string &start_key = my_min_key;
    std::string end_key = Helper::PrefixNext(my_max_key);
    std::vector<dingodb::pb::common::KeyValue> kvs;

    auto reader = engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << StrToHex(start_key, " ") << "\n"
              << "end_key : " << StrToHex(end_key, " ");
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }

  // ok
  {
    butil::Status ok = writer->KvDeleteRange(Constant::kTxnDataCF, range);

    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    const std::string &start_key = my_min_key;
    std::string end_key = Helper::PrefixNext(my_max_key);
    std::vector<dingodb::pb::common::KeyValue> kvs;

    auto reader = engine->Reader();

    ok = reader->KvScan(cf_name, start_key, end_key, kvs);
    EXPECT_EQ(ok.error_code(), dingodb::pb::error::Errno::OK);

    LOG(INFO) << "start_key : " << StrToHex(start_key, " ") << "\n"
              << "end_key : " << StrToHex(end_key, " ");
    for (const auto &kv : kvs) {
      LOG(INFO) << kv.key() << ":" << kv.value();
    }
  }
}

TEST_F(CoprocessorTestV2, KvDeleteRangeBefore) { DeleteRange(); }

TEST_F(CoprocessorTestV2, Open) {
  butil::Status ok;

  // wrong  Coprocessor
  {
    pb::store::Coprocessor pb_coprocessor;

    pb_coprocessor.set_schema_version(1);

    ok = coprocessor->Open(CoprocessorPbWrapper{pb_coprocessor});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // expr empty
  {
    pb::common::CoprocessorV2 pb_coprocessor;

    pb_coprocessor.set_schema_version(1);

    ok = coprocessor->Open(CoprocessorPbWrapper{pb_coprocessor});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // selection
  {
    pb::common::CoprocessorV2 pb_coprocessor;

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

    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));
    ok = coprocessor->Open(CoprocessorPbWrapper{pb_coprocessor});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = coprocessor_scalar->Open(CoprocessorPbWrapper{pb_coprocessor});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // result
  {
    pb::common::CoprocessorV2 pb_coprocessor;

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

    ok = coprocessor->Open(CoprocessorPbWrapper{pb_coprocessor});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = coprocessor_scalar->Open(CoprocessorPbWrapper{pb_coprocessor});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorTestV2, Prepare) {
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

  int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
  int64_t logical = 0;

  auto lambda_txn_set_kv_function = [&](const pb::common::KeyValue &key_value) {
    pb::common::KeyValue kv_write;
    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    int64_t start_ts = Tso2Timestamp(tso);
    tso.set_logical(++logical);
    int64_t commit_ts = Tso2Timestamp(tso);

    if (0 == this->start_ts) {
      this->start_ts = start_ts;
    }

    end_ts = commit_ts;

    std::string write_key = Helper::EncodeTxnKey(std::string(key_value.key()), commit_ts);

    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(::dingodb::pb::store::Op::Put);
    // write_info.set_short_value(key_value.value());

    kv_write.set_key(write_key);
    kv_write.set_value(write_info.SerializeAsString());

    engine->Writer()->KvPut(Constant::kTxnWriteCF, kv_write);

    pb::common::KeyValue kv_data;
    std::string data_key = Helper::EncodeTxnKey(std::string(key_value.key()), start_ts);
    kv_data.set_key(data_key);
    kv_data.set_value(key_value.value());

    engine->Writer()->KvPut(Constant::kTxnDataCF, kv_data);
  };

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

    lambda_txn_set_kv_function(key_value);
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
    lambda_txn_set_kv_function(key_value);
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

    lambda_txn_set_kv_function(key_value);
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

    lambda_txn_set_kv_function(key_value);
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

    lambda_txn_set_kv_function(key_value);
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

    lambda_txn_set_kv_function(key_value);
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

    lambda_txn_set_kv_function(key_value);
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

    lambda_txn_set_kv_function(key_value);
    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();
    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }
}

TEST_F(CoprocessorTestV2, Execute) {
  butil::Status ok;

  std::sort(keys.begin(), keys.end());

  std::string my_min_key(keys.front());
  std::string my_max_key(keys.back());

  std::string my_min_key_s = StrToHex(my_min_key, " ");
  LOG(INFO) << "my_min_key_s : " << my_min_key_s;

  std::string my_max_key_s = StrToHex(my_max_key, " ");
  LOG(INFO) << "my_max_key_s : " << my_max_key_s;

  IteratorOptions options;
  options.upper_bound = Helper::PrefixNext(my_max_key);
  auto iter = engine->Reader()->NewIterator(kDefaultCf, options);
  bool key_only = false;
  size_t max_fetch_cnt = 5;
  int64_t max_bytes_rpc = 1000000000000000;
  std::vector<pb::common::KeyValue> kvs;

  iter->Seek(my_min_key);

  size_t cnt = 0;

  while (true) {
    bool has_more = false;
    ok = coprocessor->Execute(iter, key_only, max_fetch_cnt, max_bytes_rpc, &kvs, has_more);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    cnt += kvs.size();
    if (!has_more) {
      break;
    }
    kvs.clear();
  }

  LOG(INFO) << "Execute key_values cnt : " << cnt;

  EXPECT_EQ(cnt, keys.size());
}

TEST_F(CoprocessorTestV2, ExecuteTxn) {
  butil::Status ok;

  std::sort(keys.begin(), keys.end());

  std::string my_min_key(keys.front());
  std::string my_max_key(Helper::PrefixNext(keys.back()));

  std::string my_min_key_s = StrToHex(my_min_key, " ");
  LOG(INFO) << "my_min_key_s : " << my_min_key_s;

  std::string my_max_key_s = StrToHex(my_max_key, " ");
  LOG(INFO) << "my_max_key_s : " << my_max_key_s;

  pb::common::Range range;
  // range.set_start_key(my_min_key);
  range.set_start_key(my_min_key);
  range.set_end_key(my_max_key);

  std::set<int64_t> resolved_locks = {};

  std::shared_ptr<TxnIterator> txn_iter = std::make_shared<TxnIterator>(
      engine, range, ++end_ts, pb::store::IsolationLevel::SnapshotIsolation, resolved_locks);

  bool key_only = false;
  size_t limit = 5;
  std::vector<pb::common::KeyValue> kvs;
  bool is_reverse = false;
  pb::store::TxnResultInfo txn_result_info;
  std::string end_key;

  auto ret = txn_iter->Init();
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "[txn]Scan init txn_iter failed, start_ts: " << start_ts
                     << ", range: " << range.ShortDebugString() << ", status: " << ret.error_str();
    return;
  }

  int64_t response_memory_size = 0;
  txn_iter->Seek(range.start_key());

  size_t cnt = 0;

  while (true) {
    bool has_more = false;
    ok = coprocessor->Execute(txn_iter, limit, key_only, is_reverse, txn_result_info, kvs, has_more, end_key);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    cnt += kvs.size();
    if (!has_more) {
      break;
    }
    kvs.clear();
  }

  LOG(INFO) << "ExecuteTxn key_values cnt : " << cnt;
  EXPECT_EQ(cnt, keys.size());
}

TEST_F(CoprocessorTestV2, FilterKV) {
  butil::Status ok;
  // std::string my_min_key;
  // char my_max_key_char[] = {static_cast<char>(0xEF), static_cast<char>(0xEF), static_cast<char>(0xEF)};
  // std::string my_max_key(my_max_key_char, sizeof(my_max_key_char));

  std::sort(keys.begin(), keys.end());

  std::string my_min_key(keys.front());
  std::string my_max_key(Helper::PrefixNext(keys.back()));

  std::string my_min_key_s = StrToHex(my_min_key, " ");
  LOG(INFO) << "my_min_key_s : " << my_min_key_s;

  std::string my_max_key_s = StrToHex(my_max_key, " ");
  LOG(INFO) << "my_max_key_s : " << my_max_key_s;

  IteratorOptions options;
  options.upper_bound = Helper::PrefixNext(my_max_key);
  auto iter = engine->Reader()->NewIterator(kDefaultCf, options);

  iter->Seek(my_min_key);

  bool is_reserved = false;

  std::string key = std::string(iter->Key());
  std::string value = std::string(iter->Value());

  ok = coprocessor->Filter(key, value, is_reserved);
  EXPECT_EQ(ok.error_code(), pb::error::OK);

  key = iter->Key();
  value = iter->Value();

  ok = coprocessor->Filter(key, value, is_reserved);
  EXPECT_EQ(ok.error_code(), pb::error::OK);
}

TEST_F(CoprocessorTestV2, FilterScalar) {
  butil::Status ok;

  bool is_reserved = false;

  // field empty
  {
    pb::common::VectorScalardata scalar_data;

    pb::common::ScalarValue scalar_value1;
    scalar_value1.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    pb::common::ScalarField sf;

    scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value1});

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // field type not match
  {
    pb::common::VectorScalardata scalar_data;

    pb::common::ScalarValue scalar_value1;
    scalar_value1.set_field_type(::dingodb::pb::common::ScalarFieldType::INT16);
    pb::common::ScalarField sf;
    sf.set_int_data(true);
    auto *field = scalar_value1.add_fields();
    field->set_int_data(10);

    scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value1});

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // field type not match
  {
    pb::common::VectorScalardata scalar_data;

    pb::common::ScalarValue scalar_value1;
    scalar_value1.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    pb::common::ScalarField sf;
    sf.set_int_data(true);
    auto *field = scalar_value1.add_fields();
    field->set_int_data(10);

    scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value1});

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // only one
  {
    pb::common::VectorScalardata scalar_data;

    pb::common::ScalarValue scalar_value1;
    scalar_value1.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    pb::common::ScalarField sf;
    sf.set_bool_data(true);
    auto *field = scalar_value1.add_fields();
    field->set_bool_data(true);

    scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value1});

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // ok
  {
    pb::common::VectorScalardata scalar_data;

    // bool
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
      pb::common::ScalarField sf;
      // sf.set_bool_data(true);
      auto *field = scalar_value.add_fields();
      field->set_bool_data(true);

      scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value});
    }

    // int
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      pb::common::ScalarField sf;
      // sf.set_int_data(true);
      auto *field = scalar_value.add_fields();
      field->set_int_data(10);

      scalar_data.mutable_scalar_data()->insert({std::string("name_int"), scalar_value});
    }

    // float
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
      pb::common::ScalarField sf;
      // sf.set_float_data(true);
      auto *field = scalar_value.add_fields();
      field->set_float_data(10.23f);

      scalar_data.mutable_scalar_data()->insert({std::string("name_float"), scalar_value});
    }

    // long
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
      pb::common::ScalarField sf;
      // sf.set_long_data(true);
      auto *field = scalar_value.add_fields();
      field->set_long_data(10233555666);

      scalar_data.mutable_scalar_data()->insert({std::string("name_int64"), scalar_value});
    }

    // double
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
      pb::common::ScalarField sf;
      // sf.set_double_data(true);
      auto *field = scalar_value.add_fields();
      field->set_double_data(10.23232332);

      scalar_data.mutable_scalar_data()->insert({std::string("name_double"), scalar_value});
    }

    // string
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      pb::common::ScalarField sf;
      // sf.set_string_data(true);
      // sf.set_string_data(true);
      auto *field = scalar_value.add_fields();
      field->set_string_data("vvvvvvvvvvvvvv");

      scalar_data.mutable_scalar_data()->insert({std::string("name_string"), scalar_value});
    }

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

TEST_F(CoprocessorTestV2, KvDeleteRange) { DeleteRange(); }

///////////////////////////////Disorder////////////////////////////////////////

// disorder
TEST_F(CoprocessorTestV2, OpenDisorder) {
  butil::Status ok;

  // ok
  {
    pb::common::CoprocessorV2 pb_coprocessor;

    pb_coprocessor.set_schema_version(1);

    auto *original_schema = pb_coprocessor.mutable_original_schema();
    original_schema->set_common_id(1);

    auto *schema6 = original_schema->add_schema();
    {
      schema6->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema6->set_is_key(true);
      schema6->set_is_nullable(true);
      schema6->set_index(0);
      schema6->set_name("name_string");
    }

    auto *schema5 = original_schema->add_schema();
    {
      schema5->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5->set_is_key(true);
      schema5->set_is_nullable(true);
      schema5->set_index(1);
      schema5->set_name("name_double");
    }

    auto *schema1 = original_schema->add_schema();
    {
      schema1->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1->set_is_key(false);
      schema1->set_is_nullable(true);
      schema1->set_index(5);
      schema1->set_name("name_bool");
    }

    auto *schema2 = original_schema->add_schema();
    {
      schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2->set_is_key(false);
      schema2->set_is_nullable(true);
      schema2->set_index(4);
      schema2->set_name("name_int");
    }

    auto *schema3 = original_schema->add_schema();
    {
      schema3->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema3->set_is_key(false);
      schema3->set_is_nullable(true);
      schema3->set_index(3);
      schema3->set_name("name_float");
    }

    auto *schema4 = original_schema->add_schema();
    {
      schema4->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema4->set_is_key(false);
      schema4->set_is_nullable(true);
      schema4->set_index(2);
      schema4->set_name("name_int64");
    }

    auto *selection_columns = pb_coprocessor.mutable_selection_columns();

    selection_columns->Add(5);
    selection_columns->Add(4);
    selection_columns->Add(3);
    selection_columns->Add(2);
    selection_columns->Add(1);
    selection_columns->Add(0);

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

    start_ts = 0;
    end_ts = 0;
    keys.clear();

    coprocessor->Close();
    ok = coprocessor->Open(CoprocessorPbWrapper{pb_coprocessor});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    coprocessor_scalar->Close();
    ok = coprocessor_scalar->Open(CoprocessorPbWrapper{pb_coprocessor});
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorTestV2, PrepareDisorder) {
  int schema_version = 1;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas;
  long common_id = 1;  // NOLINT

  schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

  schemas->reserve(6);

  std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> string_schema =
      std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
  string_schema->SetIsKey(true);
  string_schema->SetAllowNull(true);
  string_schema->SetIndex(0);
  string_schema->SetName("name_string");
  schemas->emplace_back(std::move(string_schema));

  std::shared_ptr<DingoSchema<std::optional<double>>> double_schema =
      std::make_shared<DingoSchema<std::optional<double>>>();
  double_schema->SetIsKey(true);
  double_schema->SetAllowNull(true);
  double_schema->SetIndex(1);
  double_schema->SetName("name_double");
  schemas->emplace_back(std::move(double_schema));

  std::shared_ptr<DingoSchema<std::optional<bool>>> bool_schema = std::make_shared<DingoSchema<std::optional<bool>>>();
  bool_schema->SetIsKey(false);
  bool_schema->SetAllowNull(true);
  bool_schema->SetIndex(5);
  bool_schema->SetName("name_bool");
  schemas->emplace_back(std::move(bool_schema));

  std::shared_ptr<DingoSchema<std::optional<int32_t>>> int_schema =
      std::make_shared<DingoSchema<std::optional<int32_t>>>();
  int_schema->SetIsKey(false);
  int_schema->SetAllowNull(true);
  int_schema->SetIndex(4);
  int_schema->SetName("name_int");
  schemas->emplace_back(std::move(int_schema));

  std::shared_ptr<DingoSchema<std::optional<float>>> float_schema =
      std::make_shared<DingoSchema<std::optional<float>>>();
  float_schema->SetIsKey(false);
  float_schema->SetAllowNull(true);
  float_schema->SetIndex(3);
  float_schema->SetName("name_float");
  schemas->emplace_back(std::move(float_schema));

  std::shared_ptr<DingoSchema<std::optional<int64_t>>> long_schema =
      std::make_shared<DingoSchema<std::optional<int64_t>>>();
  long_schema->SetIsKey(false);
  long_schema->SetAllowNull(true);
  long_schema->SetIndex(2);
  long_schema->SetName("name_int64");
  schemas->emplace_back(std::move(long_schema));

  RecordEncoder record_encoder(schema_version, schemas, common_id);

  int64_t physical = 1702966356362 /*Helper::TimestampMs()*/;
  int64_t logical = 0;

  auto lambda_txn_set_kv_function = [&](const pb::common::KeyValue &key_value) {
    pb::common::KeyValue kv_write;
    pb::meta::TsoTimestamp tso;
    tso.set_physical(physical);
    tso.set_logical(logical);
    int64_t start_ts = Tso2Timestamp(tso);
    tso.set_logical(++logical);
    int64_t commit_ts = Tso2Timestamp(tso);

    if (0 == this->start_ts) {
      this->start_ts = start_ts;
    }

    end_ts = commit_ts;

    std::string write_key = Helper::EncodeTxnKey(std::string(key_value.key()), commit_ts);

    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(::dingodb::pb::store::Op::Put);
    // write_info.set_short_value(key_value.value());

    kv_write.set_key(write_key);
    kv_write.set_value(write_info.SerializeAsString());

    engine->Writer()->KvPut(Constant::kTxnWriteCF, kv_write);

    pb::common::KeyValue kv_data;
    std::string data_key = Helper::EncodeTxnKey(std::string(key_value.key()), start_ts);
    kv_data.set_key(data_key);
    kv_data.set_value(key_value.value());

    engine->Writer()->KvPut(Constant::kTxnDataCF, kv_data);
  };

  // 1
  {
    pb::common::KeyValue key_value;
    std::vector<std::any> record;
    record.reserve(6);

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    record.emplace_back(std::move(any_string));

    std::any any_double = std::optional<double>(std::nullopt);
    record.emplace_back(std::move(any_double));

    std::any any_long = std::optional<int64_t>(std::nullopt);
    record.emplace_back(std::move(any_long));

    std::any any_float = std::optional<float>(std::nullopt);
    record.emplace_back(std::move(any_float));

    std::any any_int = std::optional<int32_t>(std::nullopt);
    record.emplace_back(std::move(any_int));

    std::any any_bool = std::optional<bool>(std::nullopt);
    record.emplace_back(std::move(any_bool));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    lambda_txn_set_kv_function(key_value);
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

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_22222"));
    record.emplace_back(std::move(any_string));

    std::any any_double = std::optional<double>(2.4545);
    record.emplace_back(std::move(any_double));

    std::any any_long = std::optional<int64_t>(200);
    record.emplace_back(std::move(any_long));

    std::any any_float = std::optional<float>(2.23);
    record.emplace_back(std::move(any_float));

    std::any any_int = std::optional<int32_t>(2);
    record.emplace_back(std::move(any_int));

    std::any any_bool = std::optional<bool>(false);
    record.emplace_back(std::move(any_bool));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);
    lambda_txn_set_kv_function(key_value);
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

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_33333"));
    record.emplace_back(std::move(any_string));

    std::any any_double = std::optional<double>(3.4545);
    record.emplace_back(std::move(any_double));

    std::any any_long = std::optional<int64_t>(300);
    record.emplace_back(std::move(any_long));

    std::any any_float = std::optional<float>(3.23);
    record.emplace_back(std::move(any_float));

    std::any any_int = std::optional<int32_t>(3);
    record.emplace_back(std::move(any_int));

    std::any any_bool = std::optional<bool>(true);
    record.emplace_back(std::move(any_bool));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    lambda_txn_set_kv_function(key_value);
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

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_44444"));
    record.emplace_back(std::move(any_string));

    std::any any_double = std::optional<double>(4.4545);
    record.emplace_back(std::move(any_double));

    std::any any_long = std::optional<int64_t>(400);
    record.emplace_back(std::move(any_long));

    std::any any_float = std::optional<float>(4.23);
    record.emplace_back(std::move(any_float));

    std::any any_int = std::optional<int32_t>(std::nullopt);
    record.emplace_back(std::move(any_int));

    std::any any_bool = std::optional<bool>(4);
    record.emplace_back(std::move(any_bool));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    lambda_txn_set_kv_function(key_value);
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

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_55555"));
    record.emplace_back(std::move(any_string));

    std::any any_double = std::optional<double>(std::nullopt);
    record.emplace_back(std::move(any_double));

    std::any any_long = std::optional<int64_t>(std::nullopt);
    record.emplace_back(std::move(any_long));

    std::any any_float = std::optional<float>(5.23);
    record.emplace_back(std::move(any_float));

    std::any any_int = std::optional<int32_t>(5);
    record.emplace_back(std::move(any_int));

    std::any any_bool = std::optional<bool>(true);
    record.emplace_back(std::move(any_bool));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    lambda_txn_set_kv_function(key_value);
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

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    record.emplace_back(std::move(any_string));

    std::any any_double = std::optional<double>(6.4545);
    record.emplace_back(std::move(any_double));

    std::any any_long = std::optional<int64_t>(600);
    record.emplace_back(std::move(any_long));

    std::any any_float = std::optional<float>(6.23);
    record.emplace_back(std::move(any_float));

    std::any any_int = std::optional<int32_t>(std::nullopt);
    record.emplace_back(std::move(any_int));

    std::any any_bool = std::optional<bool>(6);
    record.emplace_back(std::move(any_bool));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    lambda_txn_set_kv_function(key_value);
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

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("string_77777"));
    record.emplace_back(std::move(any_string));

    std::any any_double = std::optional<double>(7.4545);
    record.emplace_back(std::move(any_double));

    std::any any_long = std::optional<int64_t>(700);
    record.emplace_back(std::move(any_long));

    std::any any_float = std::optional<float>(7.23);
    record.emplace_back(std::move(any_float));

    std::any any_int = std::optional<int32_t>(7);
    record.emplace_back(std::move(any_int));

    std::any any_bool = std::optional<bool>(false);
    record.emplace_back(std::move(any_bool));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    lambda_txn_set_kv_function(key_value);
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

    std::any any_string = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    record.emplace_back(std::move(any_string));

    std::any any_double = std::optional<double>(8.4545);
    record.emplace_back(std::move(any_double));

    std::any any_long = std::optional<int64_t>(800);
    record.emplace_back(std::move(any_long));

    std::any any_float = std::optional<float>(8.23);
    record.emplace_back(std::move(any_float));

    std::any any_int = std::optional<int32_t>(8);
    record.emplace_back(std::move(any_int));

    std::any any_bool = std::optional<bool>(true);
    record.emplace_back(std::move(any_bool));

    int ret = record_encoder.Encode(record, key_value);

    EXPECT_EQ(ret, 0);

    lambda_txn_set_kv_function(key_value);
    butil::Status ok = engine->Writer()->KvPut(kDefaultCf, key_value);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::string key = key_value.key();
    std::string value = key_value.value();
    keys.push_back(key);

    std::string s = StrToHex(key, " ");
    LOG(INFO) << "s : " << s;
  }
}

TEST_F(CoprocessorTestV2, ExecuteDisorder) {
  butil::Status ok;

  std::sort(keys.begin(), keys.end());

  std::string my_min_key(keys.front());
  std::string my_max_key(keys.back());

  std::string my_min_key_s = StrToHex(my_min_key, " ");
  LOG(INFO) << "my_min_key_s : " << my_min_key_s;

  std::string my_max_key_s = StrToHex(my_max_key, " ");
  LOG(INFO) << "my_max_key_s : " << my_max_key_s;

  IteratorOptions options;
  options.upper_bound = Helper::PrefixNext(my_max_key);
  auto iter = engine->Reader()->NewIterator(kDefaultCf, options);
  bool key_only = false;
  size_t max_fetch_cnt = 5;
  int64_t max_bytes_rpc = 1000000000000000;
  std::vector<pb::common::KeyValue> kvs;

  iter->Seek(my_min_key);

  size_t cnt = 0;

  while (true) {
    bool has_more = false;
    ok = coprocessor->Execute(iter, key_only, max_fetch_cnt, max_bytes_rpc, &kvs, has_more);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    cnt += kvs.size();
    if (!has_more) {
      break;
    }
    kvs.clear();
  }

  LOG(INFO) << "Execute key_values cnt : " << cnt;
  EXPECT_EQ(cnt, keys.size());
}

TEST_F(CoprocessorTestV2, ExecuteTxnDisorder) {
  butil::Status ok;

  std::sort(keys.begin(), keys.end());

  std::string my_min_key(keys.front());
  std::string my_max_key(Helper::PrefixNext(keys.back()));

  std::string my_min_key_s = StrToHex(my_min_key, " ");
  LOG(INFO) << "my_min_key_s : " << my_min_key_s;

  std::string my_max_key_s = StrToHex(my_max_key, " ");
  LOG(INFO) << "my_max_key_s : " << my_max_key_s;

  pb::common::Range range;
  // range.set_start_key(my_min_key);
  range.set_start_key(my_min_key);
  range.set_end_key(my_max_key);

  std::set<int64_t> resolved_locks = {};

  std::shared_ptr<TxnIterator> txn_iter = std::make_shared<TxnIterator>(
      engine, range, ++end_ts, pb::store::IsolationLevel::SnapshotIsolation, resolved_locks);

  bool key_only = false;
  size_t limit = 5;
  std::vector<pb::common::KeyValue> kvs;
  bool is_reverse = false;
  pb::store::TxnResultInfo txn_result_info;
  std::string end_key;

  auto ret = txn_iter->Init();
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "[txn]Scan init txn_iter failed, start_ts: " << start_ts
                     << ", range: " << range.ShortDebugString() << ", status: " << ret.error_str();
    return;
  }

  int64_t response_memory_size = 0;
  txn_iter->Seek(range.start_key());

  size_t cnt = 0;

  while (true) {
    bool has_more = false;
    ok = coprocessor->Execute(txn_iter, limit, key_only, is_reverse, txn_result_info, kvs, has_more, end_key);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    cnt += kvs.size();
    if (!has_more) {
      break;
    }
    kvs.clear();
  }

  LOG(INFO) << "ExecuteTxn key_values cnt : " << cnt;
  EXPECT_EQ(cnt, keys.size());
}

TEST_F(CoprocessorTestV2, FilterKVDisorder) {
  butil::Status ok;

  std::sort(keys.begin(), keys.end());

  std::string my_min_key(keys.front());
  std::string my_max_key(Helper::PrefixNext(keys.back()));

  std::string my_min_key_s = StrToHex(my_min_key, " ");
  LOG(INFO) << "my_min_key_s : " << my_min_key_s;

  std::string my_max_key_s = StrToHex(my_max_key, " ");
  LOG(INFO) << "my_max_key_s : " << my_max_key_s;

  IteratorOptions options;
  options.upper_bound = Helper::PrefixNext(my_max_key);
  auto iter = engine->Reader()->NewIterator(kDefaultCf, options);

  iter->Seek(my_min_key);

  bool is_reserved = false;

  std::string key = std::string(iter->Key());
  std::string value = std::string(iter->Value());

  ok = coprocessor->Filter(key, value, is_reserved);
  EXPECT_EQ(ok.error_code(), pb::error::OK);

  key = iter->Key();
  value = iter->Value();

  ok = coprocessor->Filter(key, value, is_reserved);
  EXPECT_EQ(ok.error_code(), pb::error::OK);
}

TEST_F(CoprocessorTestV2, FilterScalarDisorder) {
  butil::Status ok;

  bool is_reserved = false;

  // field empty
  {
    pb::common::VectorScalardata scalar_data;

    pb::common::ScalarValue scalar_value1;
    scalar_value1.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    pb::common::ScalarField sf;

    scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value1});

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // field type not match
  {
    pb::common::VectorScalardata scalar_data;

    pb::common::ScalarValue scalar_value1;
    scalar_value1.set_field_type(::dingodb::pb::common::ScalarFieldType::INT16);
    pb::common::ScalarField sf;
    sf.set_int_data(true);
    auto *field = scalar_value1.add_fields();
    field->set_int_data(10);

    scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value1});

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // field type not match
  {
    pb::common::VectorScalardata scalar_data;

    pb::common::ScalarValue scalar_value1;
    scalar_value1.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    pb::common::ScalarField sf;
    sf.set_int_data(true);
    auto *field = scalar_value1.add_fields();
    field->set_int_data(10);

    scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value1});

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // only one
  {
    pb::common::VectorScalardata scalar_data;

    pb::common::ScalarValue scalar_value1;
    scalar_value1.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    pb::common::ScalarField sf;
    sf.set_bool_data(true);
    auto *field = scalar_value1.add_fields();
    field->set_bool_data(true);

    scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value1});

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // ok
  {
    pb::common::VectorScalardata scalar_data;

    // bool
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
      pb::common::ScalarField sf;
      // sf.set_bool_data(true);
      auto *field = scalar_value.add_fields();
      field->set_bool_data(true);

      scalar_data.mutable_scalar_data()->insert({std::string("name_bool"), scalar_value});
    }

    // int
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      pb::common::ScalarField sf;
      // sf.set_int_data(true);
      auto *field = scalar_value.add_fields();
      field->set_int_data(10);

      scalar_data.mutable_scalar_data()->insert({std::string("name_int"), scalar_value});
    }

    // float
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
      pb::common::ScalarField sf;
      // sf.set_float_data(true);
      auto *field = scalar_value.add_fields();
      field->set_float_data(10.23f);

      scalar_data.mutable_scalar_data()->insert({std::string("name_float"), scalar_value});
    }

    // long
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
      pb::common::ScalarField sf;
      // sf.set_long_data(true);
      auto *field = scalar_value.add_fields();
      field->set_long_data(10233555666);

      scalar_data.mutable_scalar_data()->insert({std::string("name_int64"), scalar_value});
    }

    // double
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
      pb::common::ScalarField sf;
      // sf.set_double_data(true);
      auto *field = scalar_value.add_fields();
      field->set_double_data(10.23232332);

      scalar_data.mutable_scalar_data()->insert({std::string("name_double"), scalar_value});
    }

    // string
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      pb::common::ScalarField sf;
      // sf.set_string_data(true);
      // sf.set_string_data(true);
      auto *field = scalar_value.add_fields();
      field->set_string_data("vvvvvvvvvvvvvv");

      scalar_data.mutable_scalar_data()->insert({std::string("name_string"), scalar_value});
    }

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    ok = coprocessor_scalar->Filter(scalar_data, is_reserved);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

TEST_F(CoprocessorTestV2, KvDeleteRangeDisorder) {
  DeleteRange();
  coprocessor.reset();
  coprocessor_scalar.reset();
}

}  // namespace dingodb

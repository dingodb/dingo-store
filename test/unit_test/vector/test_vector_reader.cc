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

#include <cstdlib>
#include <memory>
#include <random>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "config/yaml_config.h"
#include "engine/rocks_raw_engine.h"
#include "mvcc/reader.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/codec.h"
#include "vector/vector_index_factory.h"
#include "vector/vector_reader.h"

namespace dingodb {

static const std::string kDefaultCf = "default";

static const std::vector<std::string> kAllCFs = {Constant::kVectorDataCF, Constant::kVectorScalarCF,
                                                 Constant::kVectorScalarKeySpeedUpCF};

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

class VectorIndexReaderTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    std::srand(std::time(nullptr));

    Helper::CreateDirectories(kStorePath);

    config = std::make_shared<YamlConfig>();
    ASSERT_EQ(0, config->Load(kYamlConfigContent));

    engine = std::make_shared<RocksRawEngine>();
    ASSERT_TRUE(engine != nullptr);
    ASSERT_TRUE(engine->Init(config, kAllCFs));
    vector_index_thread_pool = std::make_shared<ThreadPool>("vector_index", 4);
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
  static inline char prefix = 'c';
  static inline int64_t partition_id = 0x1122334455667788;
  inline static std::shared_ptr<VectorIndex> vector_index_flat;
  static inline ThreadPoolPtr vector_index_thread_pool = nullptr;
  inline static faiss::idx_t dimension = 8;
  inline static VectorIndexWrapperPtr vector_index;
};

std::shared_ptr<RocksRawEngine> VectorIndexReaderTest::engine = nullptr;
std::shared_ptr<Config> VectorIndexReaderTest::config = nullptr;

TEST_F(VectorIndexReaderTest, PrepareVectorData) {
  static pb::common::Range k_range;
  std::string end_key;

  std::string start_key = VectorCodec::EncodeVectorKey(prefix, partition_id);
  k_range.set_start_key(start_key);
  end_key = Helper::PrefixNext(start_key);
  k_range.set_end_key(end_key);

  static pb::common::RegionEpoch k_epoch;
  k_epoch.set_conf_version(1);
  k_epoch.set_version(10);

  int64_t id = 1;
  pb::common::VectorIndexParameter index_parameter;
  index_parameter.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
  index_parameter.mutable_flat_parameter()->set_dimension(dimension);
  index_parameter.mutable_flat_parameter()->set_metric_type(::dingodb::pb::common::MetricType::METRIC_TYPE_L2);
  vector_index_flat = VectorIndexFactory::NewFlat(id, index_parameter, k_epoch, k_range, vector_index_thread_pool);
  EXPECT_NE(vector_index_flat.get(), nullptr);

  vector_index = std::make_shared<VectorIndexWrapper>(id, index_parameter, 100);
  vector_index->SetShareVectorIndex(vector_index_flat);
}

TEST_F(VectorIndexReaderTest, PrepareRocksdbData) {
  RawEngine::WriterPtr writer = engine->Writer();
  int64_t vector_id = 1;

  for (int i = 0; i < 10; i++, vector_id++) {
    std::map<std::string, std::vector<pb::common::KeyValue>> kv_put_with_cfs;
    std::vector<pb::common::KeyValue> &vector_scalar_cf = kv_put_with_cfs[Constant::kVectorScalarCF];
    std::vector<pb::common::KeyValue> &vector_scalar_key_speed_up =
        kv_put_with_cfs[Constant::kVectorScalarKeySpeedUpCF];

    pb::common::VectorScalardata all_vector_scalar_data;
    std::string key_prefix = VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id);

    // bool speedup key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "speedup_key_bool";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
      pb::common::ScalarField *field = scalar_value.add_fields();
      bool value = i % 2;
      field->set_bool_data(value);

      vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
      std::string result = VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, key);
      pb::common::KeyValue kv;
      kv.set_key(result);
      kv.set_value(scalar_value.SerializeAsString());
      vector_scalar_key_speed_up.emplace_back(std::move(kv));
      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // int speedup key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "speedup_key_int";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      pb::common::ScalarField *field = scalar_value.add_fields();
      int value = i;
      field->set_int_data(value);

      vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
      std::string result = VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, key);
      pb::common::KeyValue kv;
      kv.set_key(result);
      kv.set_value(scalar_value.SerializeAsString());
      vector_scalar_key_speed_up.emplace_back(std::move(kv));
      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // long speedup key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "speedup_key_long";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
      pb::common::ScalarField *field = scalar_value.add_fields();
      int64_t value = i + 1000;
      field->set_long_data(value);

      vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
      std::string result = VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, key);
      pb::common::KeyValue kv;
      kv.set_key(result);
      kv.set_value(scalar_value.SerializeAsString());
      vector_scalar_key_speed_up.emplace_back(std::move(kv));
      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // float speedup key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "speedup_key_float";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
      pb::common::ScalarField *field = scalar_value.add_fields();
      float value = 0.23 + i;
      field->set_float_data(value);

      vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
      std::string result = VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, key);
      pb::common::KeyValue kv;
      kv.set_key(result);
      kv.set_value(scalar_value.SerializeAsString());
      vector_scalar_key_speed_up.emplace_back(std::move(kv));
      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // double speedup key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "speedup_key_double";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
      pb::common::ScalarField *field = scalar_value.add_fields();
      double value = 0.23 + i + 1000;
      field->set_double_data(value);

      vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
      std::string result = VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, key);
      pb::common::KeyValue kv;
      kv.set_key(result);
      kv.set_value(scalar_value.SerializeAsString());
      vector_scalar_key_speed_up.emplace_back(std::move(kv));
      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // string speedup key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "speedup_key_string";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      pb::common::ScalarField *field = scalar_value.add_fields();
      std::string value(1, 's');
      value += std::to_string(i);
      field->set_string_data(value);

      vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
      std::string result = VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, key);
      pb::common::KeyValue kv;
      kv.set_key(result);
      kv.set_value(scalar_value.SerializeAsString());
      vector_scalar_key_speed_up.emplace_back(std::move(kv));
      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // bytes speedup key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "speedup_key_bytes";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
      pb::common::ScalarField *field = scalar_value.add_fields();
      std::string value(1, 'b');
      value += std::to_string(i);
      field->set_bytes_data(value);

      vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
      std::string result = VectorCodec::EncodeVectorKey(prefix, partition_id, vector_id, key);
      pb::common::KeyValue kv;
      kv.set_key(result);
      kv.set_value(scalar_value.SerializeAsString());
      vector_scalar_key_speed_up.emplace_back(std::move(kv));
      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    //////////////////////////////////no speedup
    /// key/////////////////////////////////////////////////////////////////////////
    // bool key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "key_bool";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
      pb::common::ScalarField *field = scalar_value.add_fields();
      bool value = i % 2;
      field->set_bool_data(value);

      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // int key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "key_int";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      pb::common::ScalarField *field = scalar_value.add_fields();
      int value = i;
      field->set_int_data(value);

      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // long key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "key_long";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
      pb::common::ScalarField *field = scalar_value.add_fields();
      int64_t value = i + 1000;
      field->set_long_data(value);

      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // float key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "key_float";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
      pb::common::ScalarField *field = scalar_value.add_fields();
      float value = 0.23 + i;
      field->set_float_data(value);

      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // double key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "key_double";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
      pb::common::ScalarField *field = scalar_value.add_fields();
      double value = 0.23 + i + 1000;
      field->set_double_data(value);

      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // string  key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "key_string";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      pb::common::ScalarField *field = scalar_value.add_fields();
      std::string value(1, 's');
      value += std::to_string(i);
      field->set_string_data(value);

      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    // bytes key
    {
      pb::common::VectorScalardata vector_scalar_data;
      std::string key = "key_bytes";
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
      pb::common::ScalarField *field = scalar_value.add_fields();
      std::string value(1, 'b');
      value += std::to_string(i);
      field->set_bytes_data(value);

      all_vector_scalar_data.mutable_scalar_data()->insert({key, scalar_value});
    }

    pb::common::KeyValue kv;
    kv.set_key(key_prefix);
    kv.set_value(all_vector_scalar_data.SerializeAsString());
    vector_scalar_cf.emplace_back(std::move(kv));

    writer->KvBatchPutAndDelete(kv_put_with_cfs, {});
  }
}

TEST_F(VectorIndexReaderTest, Coprocessor) {
#if !defined(TEST_COPROCESSOR_V2_MOCK)
  GTEST_SKIP() << "TEST_COPROCESSOR_V2_MOCK not defined";
#endif
  butil::Status ok;
  VectorReader vector_reader(mvcc::VectorReader::New(engine->Reader()));
  pb::common::Range region_range;

  std::vector<pb::index::VectorWithDistanceResult> vector_with_distance_results;

  std::string start_key = VectorCodec::EncodeVectorKey(prefix, partition_id);

  region_range.set_start_key(start_key);
  std::string end_key = Helper::PrefixNext(start_key);
  region_range.set_end_key(end_key);

  // scalar_schema empty only bool. not speed up key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
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
      schema1->set_name("key_bool");
    }
    pb_coprocessor.add_selection_columns(0);
    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));
    parameter.mutable_vector_coprocessor()->Swap(&pb_coprocessor);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema empty only int and string.  not speed up key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
    pb::common::CoprocessorV2 pb_coprocessor;
    pb_coprocessor.set_schema_version(1);

    auto *original_schema = pb_coprocessor.mutable_original_schema();
    original_schema->set_common_id(1);

    auto *schema1 = original_schema->add_schema();
    {
      schema1->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema1->set_is_key(true);
      schema1->set_is_nullable(true);
      schema1->set_index(0);
      schema1->set_name("key_int");
    }

    auto *schema2 = original_schema->add_schema();
    {
      schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema2->set_is_key(true);
      schema2->set_is_nullable(true);
      schema2->set_index(1);
      schema2->set_name("key_string");
    }

    pb_coprocessor.add_selection_columns(0);
    pb_coprocessor.add_selection_columns(1);
    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));
    parameter.mutable_vector_coprocessor()->Swap(&pb_coprocessor);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty only bool speed up
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
    pb::common::CoprocessorV2 pb_coprocessor;
    pb_coprocessor.set_schema_version(1);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // scalar_schema not empty only int and string
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
    pb::common::CoprocessorV2 pb_coprocessor;
    pb_coprocessor.set_schema_version(1);

    auto *original_schema = pb_coprocessor.mutable_original_schema();
    original_schema->set_common_id(1);

    auto *schema1 = original_schema->add_schema();
    {
      schema1->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema1->set_is_key(true);
      schema1->set_is_nullable(true);
      schema1->set_index(0);
      schema1->set_name("speedup_key_int");
    }

    auto *schema2 = original_schema->add_schema();
    {
      schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema2->set_is_key(true);
      schema2->set_is_nullable(true);
      schema2->set_index(1);
      schema2->set_name("speedup_key_string");
    }

    pb_coprocessor.add_selection_columns(0);
    pb_coprocessor.add_selection_columns(1);
    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));
    parameter.mutable_vector_coprocessor()->Swap(&pb_coprocessor);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty only int and string
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
    pb::common::CoprocessorV2 pb_coprocessor;
    pb_coprocessor.set_schema_version(1);

    auto *original_schema = pb_coprocessor.mutable_original_schema();
    original_schema->set_common_id(1);

    auto *schema1 = original_schema->add_schema();
    {
      schema1->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema1->set_is_key(true);
      schema1->set_is_nullable(true);
      schema1->set_index(0);
      schema1->set_name("speedup_key_int");
    }

    auto *schema2 = original_schema->add_schema();
    {
      schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema2->set_is_key(true);
      schema2->set_is_nullable(true);
      schema2->set_index(1);
      schema2->set_name("speedup_key_string");
    }

    auto *schema3 = original_schema->add_schema();
    {
      schema3->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema3->set_is_key(true);
      schema3->set_is_nullable(true);
      schema3->set_index(2);
      schema3->set_name("key_int");
    }

    auto *schema4 = original_schema->add_schema();
    {
      schema4->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema4->set_is_key(true);
      schema4->set_is_nullable(true);
      schema4->set_index(3);
      schema4->set_name("key_string");
    }

    pb_coprocessor.add_selection_columns(0);
    pb_coprocessor.add_selection_columns(1);
    pb_coprocessor.add_selection_columns(2);
    pb_coprocessor.add_selection_columns(3);
    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));
    parameter.mutable_vector_coprocessor()->Swap(&pb_coprocessor);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty all speed up key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;

    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(true);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
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
      schema1->set_name("speedup_key_bool");
    }

    auto *schema2 = original_schema->add_schema();
    {
      schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2->set_is_key(true);
      schema2->set_is_nullable(true);
      schema2->set_index(1);
      schema2->set_name("speedup_key_int");
    }

    auto *schema3 = original_schema->add_schema();
    {
      schema3->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema3->set_is_key(true);
      schema3->set_is_nullable(true);
      schema3->set_index(2);
      schema3->set_name("speedup_key_long");
    }

    auto *schema4 = original_schema->add_schema();
    {
      schema4->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema4->set_is_key(true);
      schema4->set_is_nullable(true);
      schema4->set_index(3);
      schema4->set_name("speedup_key_float");
    }

    auto *schema5 = original_schema->add_schema();
    {
      schema5->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5->set_is_key(true);
      schema5->set_is_nullable(true);
      schema5->set_index(4);
      schema5->set_name("speedup_key_double");
    }

    auto *schema6 = original_schema->add_schema();
    {
      schema6->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema6->set_is_key(true);
      schema6->set_is_nullable(true);
      schema6->set_index(5);
      schema6->set_name("speedup_key_string");
    }

    pb_coprocessor.add_selection_columns(0);
    pb_coprocessor.add_selection_columns(1);
    pb_coprocessor.add_selection_columns(2);
    pb_coprocessor.add_selection_columns(3);
    pb_coprocessor.add_selection_columns(4);
    pb_coprocessor.add_selection_columns(5);
    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));
    parameter.mutable_vector_coprocessor()->Swap(&pb_coprocessor);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty all speed up key and normal key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;

    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(false);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
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
      schema1->set_name("speedup_key_bool");
    }

    auto *schema2 = original_schema->add_schema();
    {
      schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2->set_is_key(true);
      schema2->set_is_nullable(true);
      schema2->set_index(1);
      schema2->set_name("speedup_key_int");
    }

    auto *schema3 = original_schema->add_schema();
    {
      schema3->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema3->set_is_key(true);
      schema3->set_is_nullable(true);
      schema3->set_index(2);
      schema3->set_name("speedup_key_long");
    }

    auto *schema4 = original_schema->add_schema();
    {
      schema4->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema4->set_is_key(true);
      schema4->set_is_nullable(true);
      schema4->set_index(3);
      schema4->set_name("speedup_key_float");
    }

    auto *schema5 = original_schema->add_schema();
    {
      schema5->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5->set_is_key(true);
      schema5->set_is_nullable(true);
      schema5->set_index(4);
      schema5->set_name("speedup_key_double");
    }

    auto *schema6 = original_schema->add_schema();
    {
      schema6->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema6->set_is_key(true);
      schema6->set_is_nullable(true);
      schema6->set_index(5);
      schema6->set_name("speedup_key_string");
    }

    auto *schema7 = original_schema->add_schema();
    {
      schema7->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema7->set_is_key(true);
      schema7->set_is_nullable(true);
      schema7->set_index(6);
      schema7->set_name("key_bool");
    }

    auto *schema8 = original_schema->add_schema();
    {
      schema8->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema8->set_is_key(true);
      schema8->set_is_nullable(true);
      schema8->set_index(7);
      schema8->set_name("key_int");
    }

    auto *schema9 = original_schema->add_schema();
    {
      schema9->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema9->set_is_key(true);
      schema9->set_is_nullable(true);
      schema9->set_index(8);
      schema9->set_name("key_long");
    }

    auto *schema10 = original_schema->add_schema();
    {
      schema10->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema10->set_is_key(true);
      schema10->set_is_nullable(true);
      schema10->set_index(9);
      schema10->set_name("key_float");
    }

    auto *schema11 = original_schema->add_schema();
    {
      schema11->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema11->set_is_key(true);
      schema11->set_is_nullable(true);
      schema11->set_index(10);
      schema11->set_name("key_double");
    }

    auto *schema12 = original_schema->add_schema();
    {
      schema12->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema12->set_is_key(true);
      schema12->set_is_nullable(true);
      schema12->set_index(11);
      schema12->set_name("key_string");
    }

    pb_coprocessor.add_selection_columns(0);
    pb_coprocessor.add_selection_columns(1);
    pb_coprocessor.add_selection_columns(2);
    pb_coprocessor.add_selection_columns(3);
    pb_coprocessor.add_selection_columns(4);
    pb_coprocessor.add_selection_columns(5);
    pb_coprocessor.add_selection_columns(6);
    pb_coprocessor.add_selection_columns(7);
    pb_coprocessor.add_selection_columns(8);
    pb_coprocessor.add_selection_columns(9);
    pb_coprocessor.add_selection_columns(10);
    pb_coprocessor.add_selection_columns(11);
    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));
    parameter.mutable_vector_coprocessor()->Swap(&pb_coprocessor);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // normal
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;

    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(false);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
    pb::common::CoprocessorV2 pb_coprocessor;
    pb_coprocessor.set_schema_version(1);

    auto *original_schema = pb_coprocessor.mutable_original_schema();
    original_schema->set_common_id(1);

    auto *schema1 = original_schema->add_schema();
    {
      schema1->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1->set_is_key(true);
      schema1->set_is_nullable(true);
      schema1->set_index(1);
      schema1->set_name("speedup_key_bool");
    }

    auto *schema2 = original_schema->add_schema();
    {
      schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2->set_is_key(true);
      schema2->set_is_nullable(true);
      schema2->set_index(3);
      schema2->set_name("speedup_key_int");
    }

    auto *schema3 = original_schema->add_schema();
    {
      schema3->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema3->set_is_key(true);
      schema3->set_is_nullable(true);
      schema3->set_index(0);
      schema3->set_name("speedup_key_long");
    }

    auto *schema4 = original_schema->add_schema();
    {
      schema4->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema4->set_is_key(true);
      schema4->set_is_nullable(true);
      schema4->set_index(2);
      schema4->set_name("speedup_key_float");
    }

    pb_coprocessor.add_selection_columns(1);
    pb_coprocessor.add_selection_columns(3);
    pb_coprocessor.add_selection_columns(2);
    pb_coprocessor.add_selection_columns(0);

    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));
    parameter.mutable_vector_coprocessor()->Swap(&pb_coprocessor);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // exist empty key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;

    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(false);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
    pb::common::CoprocessorV2 pb_coprocessor;
    pb_coprocessor.set_schema_version(1);

    auto *original_schema = pb_coprocessor.mutable_original_schema();
    original_schema->set_common_id(1);

    auto *schema1 = original_schema->add_schema();
    {
      schema1->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1->set_is_key(true);
      schema1->set_is_nullable(true);
      schema1->set_index(1);
      schema1->set_name("speedup_key_bool");
    }

    auto *schema2 = original_schema->add_schema();
    {
      schema2->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2->set_is_key(true);
      schema2->set_is_nullable(true);
      schema2->set_index(3);
      schema2->set_name("speedup_key_int");
    }

    auto *schema3 = original_schema->add_schema();
    {
      schema3->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema3->set_is_key(true);
      schema3->set_is_nullable(true);
      schema3->set_index(0);
      schema3->set_name("speedup_key_long");
    }

    auto *schema4 = original_schema->add_schema();
    {
      schema4->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema4->set_is_key(true);
      schema4->set_is_nullable(true);
      schema4->set_index(2);
      schema4->set_name("speedup_key_float");
    }

    auto *schema5 = original_schema->add_schema();
    {
      schema5->set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema5->set_is_key(true);
      schema5->set_is_nullable(true);
      schema5->set_index(2);
      schema5->set_name("");
    }

    pb_coprocessor.add_selection_columns(1);
    pb_coprocessor.add_selection_columns(3);
    pb_coprocessor.add_selection_columns(2);
    pb_coprocessor.add_selection_columns(0);

    pb_coprocessor.set_rel_expr(Helper::StringToHex(std::string_view("7134021442480000930400")));
    parameter.mutable_vector_coprocessor()->Swap(&pb_coprocessor);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // exist empty key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;

    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(false);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    pb::common::VectorSearchParameter parameter;
    pb::common::CoprocessorV2 pb_coprocessor;
    pb_coprocessor.set_rel_expr("7134021442480000930400");
    parameter.mutable_vector_coprocessor()->Swap(&pb_coprocessor);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }
}

TEST_F(VectorIndexReaderTest, IdPreFilter) {
  butil::Status ok;
  VectorReader vector_reader(mvcc::VectorReader::New(engine->Reader()));
  pb::common::Range region_range;

  pb::common::VectorSearchParameter parameter;

  std::vector<pb::index::VectorWithDistanceResult> vector_with_distance_results;

  std::string start_key = VectorCodec::EncodeVectorKey(prefix, partition_id);

  region_range.set_start_key(start_key);
  std::string end_key = Helper::PrefixNext(start_key);
  region_range.set_end_key(end_key);

  // scalar_schema empty only bool
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    scalar_value.add_fields()->set_bool_data(true);
    scalar_data.mutable_scalar_data()->insert({"key_bool", scalar_value});
    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema empty only int and string
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      scalar_value.add_fields()->set_int_data(3);
      scalar_data.mutable_scalar_data()->insert({"key_int", scalar_value});

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      scalar_value.add_fields()->set_string_data("s3");
      scalar_data.mutable_scalar_data()->insert({"key_string", scalar_value});
    }

    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty only int and string
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);
    vector_with_ids.push_back(vector_with_id);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // scalar_schema not empty only bool speed up
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    scalar_value.add_fields()->set_bool_data(true);
    scalar_data.mutable_scalar_data()->insert({"speedup_key_bool", scalar_value});
    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty only int and string
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      scalar_value.add_fields()->set_int_data(3);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_int", scalar_value});

      scalar_value.Clear();
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      scalar_value.add_fields()->set_string_data("s3");
      scalar_data.mutable_scalar_data()->insert({"speedup_key_string", scalar_value});
    }

    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty only int and string
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      scalar_value.add_fields()->set_int_data(3);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_int", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      scalar_value.add_fields()->set_string_data("s3");
      scalar_data.mutable_scalar_data()->insert({"speedup_key_string", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      scalar_value.add_fields()->set_int_data(3);
      scalar_data.mutable_scalar_data()->insert({"key_int", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      scalar_value.add_fields()->set_string_data("s3");
      scalar_data.mutable_scalar_data()->insert({"key_string", scalar_value});
      scalar_value.Clear();
    }

    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty all speed up key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;

    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(true);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
      scalar_value.add_fields()->set_bool_data(3 % 2);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_bool", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      scalar_value.add_fields()->set_int_data(3);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_int", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
      scalar_value.add_fields()->set_long_data(3 + 1000);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_long", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
      scalar_value.add_fields()->set_float_data(3 + 0.23);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_float", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
      scalar_value.add_fields()->set_double_data(3 + 0.23 + 1000);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_double", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      scalar_value.add_fields()->set_string_data("s3");
      scalar_data.mutable_scalar_data()->insert({"speedup_key_string", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
      scalar_value.add_fields()->set_bytes_data("b3");
      scalar_data.mutable_scalar_data()->insert({"speedup_key_bytes", scalar_value});
      scalar_value.Clear();
    }

    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty all speed up key and normal key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;

    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("speedup_key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);

    field = scalar_schema.add_fields();
    field->set_key("key_int");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_long");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_float");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_double");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_string");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
    field->set_enable_speed_up(false);

    field = scalar_schema.add_fields();
    field->set_key("key_bytes");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
    field->set_enable_speed_up(false);

    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    {
      pb::common::ScalarValue scalar_value;
      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
      scalar_value.add_fields()->set_bool_data(3 % 2);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_bool", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      scalar_value.add_fields()->set_int_data(3);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_int", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
      scalar_value.add_fields()->set_long_data(3 + 1000);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_long", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
      scalar_value.add_fields()->set_float_data(3 + 0.23);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_float", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
      scalar_value.add_fields()->set_double_data(3 + 0.23 + 1000);
      scalar_data.mutable_scalar_data()->insert({"speedup_key_double", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      scalar_value.add_fields()->set_string_data("s3");
      scalar_data.mutable_scalar_data()->insert({"speedup_key_string", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
      scalar_value.add_fields()->set_bytes_data("b3");
      scalar_data.mutable_scalar_data()->insert({"speedup_key_bytes", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
      scalar_value.add_fields()->set_bool_data(3 % 2);
      scalar_data.mutable_scalar_data()->insert({"key_bool", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT32);
      scalar_value.add_fields()->set_int_data(3);
      scalar_data.mutable_scalar_data()->insert({"key_int", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::INT64);
      scalar_value.add_fields()->set_long_data(3 + 1000);
      scalar_data.mutable_scalar_data()->insert({"key_long", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::FLOAT32);
      scalar_value.add_fields()->set_float_data(3 + 0.23);
      scalar_data.mutable_scalar_data()->insert({"key_float", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::DOUBLE);
      scalar_value.add_fields()->set_double_data(3 + 0.23 + 1000);
      scalar_data.mutable_scalar_data()->insert({"key_double", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::STRING);
      scalar_value.add_fields()->set_string_data("s3");
      scalar_data.mutable_scalar_data()->insert({"key_string", scalar_value});
      scalar_value.Clear();

      scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BYTES);
      scalar_value.add_fields()->set_bytes_data("b3");
      scalar_data.mutable_scalar_data()->insert({"key_bytes", scalar_value});
      scalar_value.Clear();
    }

    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema empty only bool one key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    scalar_value.add_fields()->set_bool_data(true);
    scalar_data.mutable_scalar_data()->insert({"key_bool", scalar_value});
    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    pb::common::Range internal_region_range;

    std::string internal_start_key = VectorCodec::EncodeVectorKey(prefix, partition_id, 1);

    internal_region_range.set_start_key(internal_start_key);
    std::string internal_end_key = Helper::PrefixNext(internal_start_key);
    internal_region_range.set_end_key(internal_end_key);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, internal_region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty only bool speed up one key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    scalar_value.add_fields()->set_bool_data(true);
    scalar_data.mutable_scalar_data()->insert({"speedup_key_bool", scalar_value});
    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    pb::common::Range internal_region_range;

    std::string internal_start_key = VectorCodec::EncodeVectorKey(prefix, partition_id, 1);

    internal_region_range.set_start_key(internal_start_key);
    std::string internal_end_key = Helper::PrefixNext(internal_start_key);
    internal_region_range.set_end_key(internal_end_key);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, internal_region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema empty only bool no key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    scalar_value.add_fields()->set_bool_data(true);
    scalar_data.mutable_scalar_data()->insert({"key_bool", scalar_value});
    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    pb::common::Range internal_region_range;

    std::string internal_start_key = VectorCodec::EncodeVectorKey(prefix, partition_id, 100);

    internal_region_range.set_start_key(internal_start_key);
    std::string internal_end_key = Helper::PrefixNext(internal_start_key);
    internal_region_range.set_end_key(internal_end_key);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, internal_region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // scalar_schema not empty only bool speed up no key
  {
    std::vector<pb::common::VectorWithId> vector_with_ids;
    pb::common::ScalarSchema scalar_schema;
    auto *field = scalar_schema.add_fields();
    field->set_key("speedup_key_bool");
    field->set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    field->set_enable_speed_up(true);
    pb::common::VectorWithId vector_with_id;
    vector_with_id.set_id(1);

    pb::common::VectorScalardata scalar_data;
    pb::common::ScalarValue scalar_value;
    scalar_value.set_field_type(::dingodb::pb::common::ScalarFieldType::BOOL);
    scalar_value.add_fields()->set_bool_data(true);
    scalar_data.mutable_scalar_data()->insert({"speedup_key_bool", scalar_value});
    vector_with_id.mutable_scalar_data()->Swap(&scalar_data);
    vector_with_ids.push_back(vector_with_id);

    pb::common::Range internal_region_range;

    std::string internal_start_key = VectorCodec::EncodeVectorKey(prefix, partition_id, 100);

    internal_region_range.set_start_key(internal_start_key);
    std::string internal_end_key = Helper::PrefixNext(internal_start_key);
    internal_region_range.set_end_key(internal_end_key);

    ok = vector_reader.DoVectorSearchForScalarPreFilter(vector_index, internal_region_range, vector_with_ids, parameter,
                                                        scalar_schema, vector_with_distance_results);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }
}

}  // namespace dingodb

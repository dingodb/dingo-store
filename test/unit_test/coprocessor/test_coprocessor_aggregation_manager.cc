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
#include <memory>
#include <optional>
#include <vector>

#include "butil/status.h"
#include "coprocessor/aggregation_manager.h"
#include "coprocessor/utils.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"

namespace dingodb {  // NOLINT

class CoprocessorAggregationManagerTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}

  static std::shared_ptr<AggregationManager> aggregation_manager;
};

std::shared_ptr<AggregationManager> CoprocessorAggregationManagerTest::aggregation_manager = nullptr;

// test all case
TEST_F(CoprocessorAggregationManagerTest, ExecuteALL) {
  // SUM bool
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::SUM);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(true);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(false);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<bool> v = std::any_cast<std::optional<bool>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_TRUE(v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // SUM int
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::SUM);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<int32_t>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int32_t>(1);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int32_t>(2);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int32_t>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<int32_t> v = std::any_cast<std::optional<int32_t>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(3, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // SUM float
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::SUM);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<float>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<float>(1.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<float>(2.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<float>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<float> v = std::any_cast<std::optional<float>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_FLOAT_EQ(3.46, v.value());
      }

      iter->Next();
    }

    aggregation_manager->Close();
  }

  // SUM long
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::SUM);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<int64_t>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int64_t>(1000000000);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int64_t>(2000000000);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int64_t>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<int64_t> v = std::any_cast<std::optional<int64_t>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(3000000000, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // SUM double
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::SUM);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<double>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<double>(123456.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<double>(1.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<double>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<double> v = std::any_cast<std::optional<double>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_DOUBLE_EQ(123457.46, v.value());
      }

      iter->Next();
    }

    aggregation_manager->Close();
  }

  ////////////////////////////////////////////////// count ////////////////////////////////////////////////////////////

  // COUNT bool
  {
    butil::Status ok;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    google::protobuf::RepeatedPtrField<pb::common::Schema> result_pb_schemas;
    for (size_t i = 0; i < 1; i++) {
      pb::common::Schema schema;
      schema.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema.set_is_key(true);
      schema.set_is_nullable(true);
      schema.set_index(4);
      result_pb_schemas.Add(std::move(schema));
    }
    ok = Utils::TransToSerialSchema(result_pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    aggregation_manager = std::make_shared<AggregationManager>();

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::COUNT);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(true);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(false);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<int64_t> v = std::any_cast<std::optional<int64_t>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(2, v.value());
      }

      iter->Next();
    }

    aggregation_manager->Close();
  }

  ////////////////////////////////////////////////// countwithnull
  ///////////////////////////////////////////////////////////////

  // COUNTWITHNULL bool
  {
    butil::Status ok;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    google::protobuf::RepeatedPtrField<pb::common::Schema> result_pb_schemas;
    for (size_t i = 0; i < 1; i++) {
      pb::common::Schema schema;
      schema.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema.set_is_key(true);
      schema.set_is_nullable(true);
      schema.set_index(4);
      result_pb_schemas.Add(std::move(schema));
    }
    ok = Utils::TransToSerialSchema(result_pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    aggregation_manager = std::make_shared<AggregationManager>();

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::COUNTWITHNULL);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(true);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(false);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<int64_t> v = std::any_cast<std::optional<int64_t>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(4, v.value());
      }

      iter->Next();
    }

    aggregation_manager->Close();
  }

  ////////////////////////////////////////////////// max ////////////////////////////////////////////////////////////
  // MAX bool
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MAX);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(true);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(false);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<bool> v = std::any_cast<std::optional<bool>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(true, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MAX int
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MAX);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<int32_t>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int32_t>(1);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int32_t>(2);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int32_t>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<int32_t> v = std::any_cast<std::optional<int32_t>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(2, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MAX float
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MAX);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<float>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<float>(1.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<float>(2.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<float>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<float> v = std::any_cast<std::optional<float>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_FLOAT_EQ(2.23, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MAX long
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MAX);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<int64_t>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int64_t>(1000000000);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int64_t>(2000000000);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int64_t>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<int64_t> v = std::any_cast<std::optional<int64_t>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(2000000000, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MAX double
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MAX);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<double>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<double>(123456.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<double>(1.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<double>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<double> v = std::any_cast<std::optional<double>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_DOUBLE_EQ(123456.23, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MAX string
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MAX);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<std ::shared_ptr<std::string>>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<std ::shared_ptr<std::string>>(std::make_shared<std::string>("aaaaaaaaaaaaaaaaaaaaa"));
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<std ::shared_ptr<std::string>>(std::make_shared<std::string>("bbbbbbbbbbbbbbbbbbbbbbbb"));
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<std ::shared_ptr<std::string>>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<std::shared_ptr<std::string>> v =
          std::any_cast<std::optional<std::shared_ptr<std::string>>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ("bbbbbbbbbbbbbbbbbbbbbbbb", *(v.value()));
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  ////////////////////////////////////////////////// min ////////////////////////////////////////////////////////////
  // MIN bool
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MIN);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(true);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(false);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<bool>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<bool> v = std::any_cast<std::optional<bool>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(false, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MIN int
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MIN);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<int32_t>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int32_t>(1);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int32_t>(2);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int32_t>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<int32_t> v = std::any_cast<std::optional<int32_t>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(1, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MIN float
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MIN);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<float>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<float>(1.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<float>(2.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<float>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<float> v = std::any_cast<std::optional<float>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_FLOAT_EQ(1.23, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MIN long
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MIN);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<int64_t>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int64_t>(1000000000);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int64_t>(2000000000);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<int64_t>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<int64_t> v = std::any_cast<std::optional<int64_t>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ(1000000000, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MIN double
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MIN);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<double>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<double>(123456.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<double>(1.23);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<double>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<double> v = std::any_cast<std::optional<double>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_DOUBLE_EQ(1.23, v.value());
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }

  // MIN string
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 1; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MIN);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    std::any a1 = std::optional<std ::shared_ptr<std::string>>(std::nullopt);
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<std ::shared_ptr<std::string>>(std::make_shared<std::string>("aaaaaaaaaaaaaaaaaaaaa"));
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<std ::shared_ptr<std::string>>(std::make_shared<std::string>("bbbbbbbbbbbbbbbbbbbbbbbb"));
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    a1 = std::optional<std ::shared_ptr<std::string>>(std::nullopt);
    group_by_operator_record.clear();
    group_by_operator_record.emplace_back(a1);

    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);

    std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
    while (iter->HasNext()) {
      const auto &key = iter->GetKey();
      std::shared_ptr<std::vector<std::any>> value = iter->GetValue();
      std::optional<std::shared_ptr<std::string>> v =
          std::any_cast<std::optional<std::shared_ptr<std::string>>>((*value)[0]);
      EXPECT_TRUE(v.has_value());
      if (v.has_value()) {
        EXPECT_EQ("aaaaaaaaaaaaaaaaaaaaa", *(v.value()));
      }
      iter->Next();
    }

    aggregation_manager->Close();
  }
}

TEST_F(CoprocessorAggregationManagerTest, Open) {
  // SUM
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    pb::common::Schema schema2;
    schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema2.set_is_key(true);
    schema2.set_is_nullable(true);
    schema2.set_index(1);
    pb_schemas.Add(std::move(schema2));

    pb::common::Schema schema3;
    schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema3.set_is_key(true);
    schema3.set_is_nullable(true);
    schema3.set_index(2);
    pb_schemas.Add(std::move(schema3));

    pb::common::Schema schema4;
    schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema4.set_is_key(true);
    schema4.set_is_nullable(true);
    schema4.set_index(3);
    pb_schemas.Add(std::move(schema4));

    pb::common::Schema schema5;
    schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema5.set_is_key(true);
    schema5.set_is_nullable(true);
    schema5.set_index(4);
    pb_schemas.Add(std::move(schema5));

    pb::common::Schema schema6;
    schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
    schema6.set_is_key(true);
    schema6.set_is_nullable(true);
    schema6.set_index(5);
    pb_schemas.Add(std::move(schema6));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 6; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::SUM);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::ENOT_SUPPORT);
  }

  // COUNT
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    pb::common::Schema schema2;
    schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema2.set_is_key(true);
    schema2.set_is_nullable(true);
    schema2.set_index(1);
    pb_schemas.Add(std::move(schema2));

    pb::common::Schema schema3;
    schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema3.set_is_key(true);
    schema3.set_is_nullable(true);
    schema3.set_index(2);
    pb_schemas.Add(std::move(schema3));

    pb::common::Schema schema4;
    schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema4.set_is_key(true);
    schema4.set_is_nullable(true);
    schema4.set_index(3);
    pb_schemas.Add(std::move(schema4));

    pb::common::Schema schema5;
    schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema5.set_is_key(true);
    schema5.set_is_nullable(true);
    schema5.set_index(4);
    pb_schemas.Add(std::move(schema5));

    pb::common::Schema schema6;
    schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
    schema6.set_is_key(true);
    schema6.set_is_nullable(true);
    schema6.set_index(5);
    pb_schemas.Add(std::move(schema6));

    butil::Status ok;

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    google::protobuf::RepeatedPtrField<pb::common::Schema> result_pb_schemas;
    for (size_t i = 0; i < 6; i++) {
      pb::common::Schema schema;
      schema.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema.set_is_key(true);
      schema.set_is_nullable(true);
      schema.set_index(4);
      result_pb_schemas.Add(std::move(schema));
    }
    ok = Utils::TransToSerialSchema(result_pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 6; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(-1);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::COUNT);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // COUNT -1
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    pb::common::Schema schema2;
    schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema2.set_is_key(true);
    schema2.set_is_nullable(true);
    schema2.set_index(1);
    pb_schemas.Add(std::move(schema2));

    pb::common::Schema schema3;
    schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema3.set_is_key(true);
    schema3.set_is_nullable(true);
    schema3.set_index(2);
    pb_schemas.Add(std::move(schema3));

    pb::common::Schema schema4;
    schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema4.set_is_key(true);
    schema4.set_is_nullable(true);
    schema4.set_index(3);
    pb_schemas.Add(std::move(schema4));

    pb::common::Schema schema5;
    schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema5.set_is_key(true);
    schema5.set_is_nullable(true);
    schema5.set_index(4);
    pb_schemas.Add(std::move(schema5));

    pb::common::Schema schema6;
    schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
    schema6.set_is_key(true);
    schema6.set_is_nullable(true);
    schema6.set_index(5);
    pb_schemas.Add(std::move(schema6));

    butil::Status ok;

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    google::protobuf::RepeatedPtrField<pb::common::Schema> result_pb_schemas;
    for (size_t i = 0; i < 6; i++) {
      pb::common::Schema schema;
      schema.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema.set_is_key(true);
      schema.set_is_nullable(true);
      schema.set_index(4);
      result_pb_schemas.Add(std::move(schema));
    }
    ok = Utils::TransToSerialSchema(result_pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 6; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::COUNT);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // COUNTWITHNULL
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    pb::common::Schema schema2;
    schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema2.set_is_key(true);
    schema2.set_is_nullable(true);
    schema2.set_index(1);
    pb_schemas.Add(std::move(schema2));

    pb::common::Schema schema3;
    schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema3.set_is_key(true);
    schema3.set_is_nullable(true);
    schema3.set_index(2);
    pb_schemas.Add(std::move(schema3));

    pb::common::Schema schema4;
    schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema4.set_is_key(true);
    schema4.set_is_nullable(true);
    schema4.set_index(3);
    pb_schemas.Add(std::move(schema4));

    pb::common::Schema schema5;
    schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema5.set_is_key(true);
    schema5.set_is_nullable(true);
    schema5.set_index(4);
    pb_schemas.Add(std::move(schema5));

    pb::common::Schema schema6;
    schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
    schema6.set_is_key(true);
    schema6.set_is_nullable(true);
    schema6.set_index(5);
    pb_schemas.Add(std::move(schema6));

    butil::Status ok;

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    google::protobuf::RepeatedPtrField<pb::common::Schema> result_pb_schemas;
    for (size_t i = 0; i < 6; i++) {
      pb::common::Schema schema;
      schema.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema.set_is_key(true);
      schema.set_is_nullable(true);
      schema.set_index(4);
      result_pb_schemas.Add(std::move(schema));
    }
    ok = Utils::TransToSerialSchema(result_pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 6; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::COUNTWITHNULL);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // MAX
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    pb::common::Schema schema2;
    schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema2.set_is_key(true);
    schema2.set_is_nullable(true);
    schema2.set_index(1);
    pb_schemas.Add(std::move(schema2));

    pb::common::Schema schema3;
    schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema3.set_is_key(true);
    schema3.set_is_nullable(true);
    schema3.set_index(2);
    pb_schemas.Add(std::move(schema3));

    pb::common::Schema schema4;
    schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema4.set_is_key(true);
    schema4.set_is_nullable(true);
    schema4.set_index(3);
    pb_schemas.Add(std::move(schema4));

    pb::common::Schema schema5;
    schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema5.set_is_key(true);
    schema5.set_is_nullable(true);
    schema5.set_index(4);
    pb_schemas.Add(std::move(schema5));

    pb::common::Schema schema6;
    schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
    schema6.set_is_key(true);
    schema6.set_is_nullable(true);
    schema6.set_index(5);
    pb_schemas.Add(std::move(schema6));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 6; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MAX);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // MIN
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;

    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));

    pb::common::Schema schema2;
    schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema2.set_is_key(true);
    schema2.set_is_nullable(true);
    schema2.set_index(1);
    pb_schemas.Add(std::move(schema2));

    pb::common::Schema schema3;
    schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema3.set_is_key(true);
    schema3.set_is_nullable(true);
    schema3.set_index(2);
    pb_schemas.Add(std::move(schema3));

    pb::common::Schema schema4;
    schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema4.set_is_key(true);
    schema4.set_is_nullable(true);
    schema4.set_index(3);
    pb_schemas.Add(std::move(schema4));

    pb::common::Schema schema5;
    schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema5.set_is_key(true);
    schema5.set_is_nullable(true);
    schema5.set_index(4);
    pb_schemas.Add(std::move(schema5));

    pb::common::Schema schema6;
    schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
    schema6.set_is_key(true);
    schema6.set_is_nullable(true);
    schema6.set_index(5);
    pb_schemas.Add(std::move(schema6));

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < 6; i++) {
      pb::store::AggregationOperator aggregation_operator;
      aggregation_operator.set_index_of_column(i);
      aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MIN);
      aggregation_operators.Add(std::move(aggregation_operator));
    }

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ALL
  {
    butil::Status ok;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;

    google::protobuf::RepeatedPtrField<pb::common::Schema> group_by_pb_schemas;
    google::protobuf::RepeatedPtrField<pb::common::Schema> result_pb_schemas;

    // result_pb_schemas has 6 [bool, int, float. long, double. string]
    {
      pb::common::Schema schema1;
      schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1.set_is_key(true);
      schema1.set_is_nullable(true);
      schema1.set_index(0);
      result_pb_schemas.Add(std::move(schema1));

      pb::common::Schema schema2;
      schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2.set_is_key(true);
      schema2.set_is_nullable(true);
      schema2.set_index(1);
      result_pb_schemas.Add(std::move(schema2));

      pb::common::Schema schema3;
      schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema3.set_is_key(true);
      schema3.set_is_nullable(true);
      schema3.set_index(2);
      result_pb_schemas.Add(std::move(schema3));

      pb::common::Schema schema4;
      schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema4.set_is_key(true);
      schema4.set_is_nullable(true);
      schema4.set_index(3);
      result_pb_schemas.Add(std::move(schema4));

      pb::common::Schema schema5;
      schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5.set_is_key(true);
      schema5.set_is_nullable(true);
      schema5.set_index(4);
      result_pb_schemas.Add(std::move(schema5));

      pb::common::Schema schema6;
      schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema5.set_is_key(true);
      schema5.set_is_nullable(true);
      schema5.set_index(5);
      result_pb_schemas.Add(std::move(schema6));
    }

    // SUM
    {
      pb::common::Schema schema1;
      schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1.set_is_key(true);
      schema1.set_is_nullable(true);
      schema1.set_index(0);
      group_by_pb_schemas.Add(std::move(schema1));

      pb::common::Schema schema2;
      schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2.set_is_key(true);
      schema2.set_is_nullable(true);
      schema2.set_index(1);
      group_by_pb_schemas.Add(std::move(schema2));

      pb::common::Schema schema3;
      schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema3.set_is_key(true);
      schema3.set_is_nullable(true);
      schema3.set_index(2);
      group_by_pb_schemas.Add(std::move(schema3));

      pb::common::Schema schema4;
      schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema4.set_is_key(true);
      schema4.set_is_nullable(true);
      schema4.set_index(3);
      group_by_pb_schemas.Add(std::move(schema4));

      pb::common::Schema schema5;
      schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5.set_is_key(true);
      schema5.set_is_nullable(true);
      schema5.set_index(4);
      group_by_pb_schemas.Add(std::move(schema5));

      for (size_t i = 0; i < 5; i++) {
        pb::store::AggregationOperator aggregation_operator;
        aggregation_operator.set_index_of_column(i);
        aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::SUM);
        aggregation_operators.Add(std::move(aggregation_operator));
      }

      pb::common::Schema schema11;
      schema11.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema11.set_is_key(true);
      schema11.set_is_nullable(true);
      schema11.set_index(0);
      result_pb_schemas.Add(std::move(schema11));

      pb::common::Schema schema22;
      schema22.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema22.set_is_key(true);
      schema22.set_is_nullable(true);
      schema22.set_index(1);
      result_pb_schemas.Add(std::move(schema22));

      pb::common::Schema schema33;
      schema33.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema33.set_is_key(true);
      schema33.set_is_nullable(true);
      schema33.set_index(2);
      result_pb_schemas.Add(std::move(schema33));

      pb::common::Schema schema44;
      schema44.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema44.set_is_key(true);
      schema44.set_is_nullable(true);
      schema44.set_index(3);
      result_pb_schemas.Add(std::move(schema44));

      pb::common::Schema schema55;
      schema55.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema55.set_is_key(true);
      schema55.set_is_nullable(true);
      schema55.set_index(4);
      result_pb_schemas.Add(std::move(schema55));
    }

    // COUNT
    {
      pb::common::Schema schema1;
      schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1.set_is_key(true);
      schema1.set_is_nullable(true);
      schema1.set_index(0);
      group_by_pb_schemas.Add(std::move(schema1));

      pb::common::Schema schema2;
      schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2.set_is_key(true);
      schema2.set_is_nullable(true);
      schema2.set_index(1);
      group_by_pb_schemas.Add(std::move(schema2));

      pb::common::Schema schema3;
      schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema3.set_is_key(true);
      schema3.set_is_nullable(true);
      schema3.set_index(2);
      group_by_pb_schemas.Add(std::move(schema3));

      pb::common::Schema schema4;
      schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema4.set_is_key(true);
      schema4.set_is_nullable(true);
      schema4.set_index(3);
      group_by_pb_schemas.Add(std::move(schema4));

      pb::common::Schema schema5;
      schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5.set_is_key(true);
      schema5.set_is_nullable(true);
      schema5.set_index(4);
      group_by_pb_schemas.Add(std::move(schema5));

      pb::common::Schema schema6;
      schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema6.set_is_key(true);
      schema6.set_is_nullable(true);
      schema6.set_index(5);
      group_by_pb_schemas.Add(std::move(schema6));

      for (size_t i = 0; i < 6; i++) {
        pb::store::AggregationOperator aggregation_operator;
        aggregation_operator.set_index_of_column(i);
        aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::COUNT);
        aggregation_operators.Add(std::move(aggregation_operator));
      }

      for (size_t i = 0; i < 6; i++) {
        pb::common::Schema schema;
        schema.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
        schema.set_is_key(true);
        schema.set_is_nullable(true);
        schema.set_index(4);
        result_pb_schemas.Add(std::move(schema));
      }
    }

    // COUNTWITHNULL
    {
      pb::common::Schema schema1;
      schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1.set_is_key(true);
      schema1.set_is_nullable(true);
      schema1.set_index(0);
      group_by_pb_schemas.Add(std::move(schema1));

      pb::common::Schema schema2;
      schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2.set_is_key(true);
      schema2.set_is_nullable(true);
      schema2.set_index(1);
      group_by_pb_schemas.Add(std::move(schema2));

      pb::common::Schema schema3;
      schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema3.set_is_key(true);
      schema3.set_is_nullable(true);
      schema3.set_index(2);
      group_by_pb_schemas.Add(std::move(schema3));

      pb::common::Schema schema4;
      schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema4.set_is_key(true);
      schema4.set_is_nullable(true);
      schema4.set_index(3);
      group_by_pb_schemas.Add(std::move(schema4));

      pb::common::Schema schema5;
      schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5.set_is_key(true);
      schema5.set_is_nullable(true);
      schema5.set_index(4);
      group_by_pb_schemas.Add(std::move(schema5));

      pb::common::Schema schema6;
      schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema6.set_is_key(true);
      schema6.set_is_nullable(true);
      schema6.set_index(5);
      group_by_pb_schemas.Add(std::move(schema6));

      for (size_t i = 0; i < 6; i++) {
        pb::store::AggregationOperator aggregation_operator;
        aggregation_operator.set_index_of_column(i);
        aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::COUNTWITHNULL);
        aggregation_operators.Add(std::move(aggregation_operator));
      }

      for (size_t i = 0; i < 6; i++) {
        pb::common::Schema schema;
        schema.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
        schema.set_is_key(true);
        schema.set_is_nullable(true);
        schema.set_index(4);
        result_pb_schemas.Add(std::move(schema));
      }
    }

    // MAX
    {
      pb::common::Schema schema1;
      schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1.set_is_key(true);
      schema1.set_is_nullable(true);
      schema1.set_index(0);
      group_by_pb_schemas.Add(std::move(schema1));

      pb::common::Schema schema2;
      schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2.set_is_key(true);
      schema2.set_is_nullable(true);
      schema2.set_index(1);
      group_by_pb_schemas.Add(std::move(schema2));

      pb::common::Schema schema3;
      schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema3.set_is_key(true);
      schema3.set_is_nullable(true);
      schema3.set_index(2);
      group_by_pb_schemas.Add(std::move(schema3));

      pb::common::Schema schema4;
      schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema4.set_is_key(true);
      schema4.set_is_nullable(true);
      schema4.set_index(3);
      group_by_pb_schemas.Add(std::move(schema4));

      pb::common::Schema schema5;
      schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5.set_is_key(true);
      schema5.set_is_nullable(true);
      schema5.set_index(4);
      group_by_pb_schemas.Add(std::move(schema5));

      pb::common::Schema schema6;
      schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema6.set_is_key(true);
      schema6.set_is_nullable(true);
      schema6.set_index(5);
      group_by_pb_schemas.Add(std::move(schema6));

      for (size_t i = 0; i < 6; i++) {
        pb::store::AggregationOperator aggregation_operator;
        aggregation_operator.set_index_of_column(i);
        aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MAX);
        aggregation_operators.Add(std::move(aggregation_operator));
      }

      pb::common::Schema schema11;
      schema11.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema11.set_is_key(true);
      schema11.set_is_nullable(true);
      schema11.set_index(0);
      result_pb_schemas.Add(std::move(schema11));

      pb::common::Schema schema22;
      schema22.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema22.set_is_key(true);
      schema22.set_is_nullable(true);
      schema22.set_index(1);
      result_pb_schemas.Add(std::move(schema22));

      pb::common::Schema schema33;
      schema33.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema33.set_is_key(true);
      schema33.set_is_nullable(true);
      schema33.set_index(2);
      result_pb_schemas.Add(std::move(schema33));

      pb::common::Schema schema44;
      schema44.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema44.set_is_key(true);
      schema44.set_is_nullable(true);
      schema44.set_index(3);
      result_pb_schemas.Add(std::move(schema44));

      pb::common::Schema schema55;
      schema55.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema55.set_is_key(true);
      schema55.set_is_nullable(true);
      schema55.set_index(4);
      result_pb_schemas.Add(std::move(schema55));

      pb::common::Schema schema66;
      schema66.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema66.set_is_key(true);
      schema66.set_is_nullable(true);
      schema66.set_index(5);
      result_pb_schemas.Add(std::move(schema66));
    }

    // MIN
    {
      pb::common::Schema schema1;
      schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema1.set_is_key(true);
      schema1.set_is_nullable(true);
      schema1.set_index(0);
      group_by_pb_schemas.Add(std::move(schema1));

      pb::common::Schema schema2;
      schema2.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema2.set_is_key(true);
      schema2.set_is_nullable(true);
      schema2.set_index(1);
      group_by_pb_schemas.Add(std::move(schema2));

      pb::common::Schema schema3;
      schema3.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema3.set_is_key(true);
      schema3.set_is_nullable(true);
      schema3.set_index(2);
      group_by_pb_schemas.Add(std::move(schema3));

      pb::common::Schema schema4;
      schema4.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema4.set_is_key(true);
      schema4.set_is_nullable(true);
      schema4.set_index(3);
      group_by_pb_schemas.Add(std::move(schema4));

      pb::common::Schema schema5;
      schema5.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema5.set_is_key(true);
      schema5.set_is_nullable(true);
      schema5.set_index(4);
      group_by_pb_schemas.Add(std::move(schema5));

      pb::common::Schema schema6;
      schema6.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema6.set_is_key(true);
      schema6.set_is_nullable(true);
      schema6.set_index(5);
      group_by_pb_schemas.Add(std::move(schema6));

      for (size_t i = 0; i < 6; i++) {
        pb::store::AggregationOperator aggregation_operator;
        aggregation_operator.set_index_of_column(i);
        aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::MIN);
        aggregation_operators.Add(std::move(aggregation_operator));
      }

      pb::common::Schema schema11;
      schema11.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
      schema11.set_is_key(true);
      schema11.set_is_nullable(true);
      schema11.set_index(0);
      result_pb_schemas.Add(std::move(schema11));

      pb::common::Schema schema22;
      schema22.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
      schema22.set_is_key(true);
      schema22.set_is_nullable(true);
      schema22.set_index(1);
      result_pb_schemas.Add(std::move(schema22));

      pb::common::Schema schema33;
      schema33.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
      schema33.set_is_key(true);
      schema33.set_is_nullable(true);
      schema33.set_index(2);
      result_pb_schemas.Add(std::move(schema33));

      pb::common::Schema schema44;
      schema44.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
      schema44.set_is_key(true);
      schema44.set_is_nullable(true);
      schema44.set_index(3);
      result_pb_schemas.Add(std::move(schema44));

      pb::common::Schema schema55;
      schema55.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
      schema55.set_is_key(true);
      schema55.set_is_nullable(true);
      schema55.set_index(4);
      result_pb_schemas.Add(std::move(schema55));

      pb::common::Schema schema66;
      schema66.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
      schema66.set_is_key(true);
      schema66.set_is_nullable(true);
      schema66.set_index(5);
      result_pb_schemas.Add(std::move(schema66));
    }

    group_by_operator_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(group_by_pb_schemas, &group_by_operator_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    result_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ok = Utils::TransToSerialSchema(result_pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    aggregation_manager = std::make_shared<AggregationManager>();

    ok = aggregation_manager->Open(group_by_operator_serial_schemas, aggregation_operators, result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorAggregationManagerTest, Execute) {
  // empty key
  {
    std::string group_by_key;
    std::vector<std::any> group_by_operator_record;

    // SUM
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);
    }

    // COUNT
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("mystring"));
      group_by_operator_record.emplace_back(a6);
    }

    // COUNTWITHNULL
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("mystring"));
      group_by_operator_record.emplace_back(a6);
    }

    // MAX
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("mystring"));
      group_by_operator_record.emplace_back(a6);
    }

    // MIN
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("mystring"));
      group_by_operator_record.emplace_back(a6);
    }

    butil::Status ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);
    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);
    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);
  }

  //  key = "123";
  {
    std::string group_by_key = "123";
    std::vector<std::any> group_by_operator_record;

    // SUM
    {
      std::any a1 = std::optional<bool>(std::nullopt);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(std::nullopt);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(std::nullopt);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(std::nullopt);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(std::nullopt);
      group_by_operator_record.emplace_back(a5);
    }

    // COUNT
    {
      std::any a1 = std::optional<bool>(std::nullopt);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(std::nullopt);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(std::nullopt);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(std::nullopt);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(std::nullopt);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::nullopt);
      group_by_operator_record.emplace_back(a6);
    }

    // COUNTWITHNULL
    {
      std::any a1 = std::optional<bool>(std::nullopt);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(std::nullopt);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(std::nullopt);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(std::nullopt);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(std::nullopt);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::nullopt);
      group_by_operator_record.emplace_back(a6);
    }

    // MAX
    {
      std::any a1 = std::optional<bool>(std::nullopt);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(std::nullopt);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(std::nullopt);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(std::nullopt);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(std::nullopt);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::nullopt);
      group_by_operator_record.emplace_back(a6);
    }

    // MIN
    {
      std::any a1 = std::optional<bool>(std::nullopt);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(std::nullopt);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(std::nullopt);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(std::nullopt);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(std::nullopt);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::nullopt);
      group_by_operator_record.emplace_back(a6);
    }

    butil::Status ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);
    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);
    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);
  }

  // key="123"
  {
    std::string group_by_key = "123";
    std::vector<std::any> group_by_operator_record;

    // SUM
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);
    }

    // COUNT
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("mystring"));
      group_by_operator_record.emplace_back(a6);
    }

    // COUNTWITHNULL
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("mystring"));
      group_by_operator_record.emplace_back(a6);
    }

    // MAX
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("mystring"));
      group_by_operator_record.emplace_back(a6);
    }

    // MIN
    {
      std::any a1 = std::optional<bool>(true);
      group_by_operator_record.emplace_back(a1);

      std::any a2 = std::optional<int32_t>(1);
      group_by_operator_record.emplace_back(a2);

      std::any a3 = std::optional<float>(1.23);
      group_by_operator_record.emplace_back(a3);

      std::any a4 = std::optional<int64_t>(100000000000000000);
      group_by_operator_record.emplace_back(a4);

      std::any a5 = std::optional<double>(123456789.23);
      group_by_operator_record.emplace_back(a5);

      std::any a6 = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("mystring"));
      group_by_operator_record.emplace_back(a6);
    }

    butil::Status ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);
    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);
    ok = aggregation_manager->Execute(group_by_key, group_by_operator_record);
  }
}

TEST_F(CoprocessorAggregationManagerTest, CreateIterator) {
  std::shared_ptr<AggregationIterator> iter = aggregation_manager->CreateIterator();
  while (iter->HasNext()) {
    const auto &key = iter->GetKey();
    const auto &value = iter->GetValue();
    iter->Next();
  }
}

TEST_F(CoprocessorAggregationManagerTest, Close) { aggregation_manager->Close(); }

}  // namespace dingodb

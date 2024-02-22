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

#include <memory>
#include <optional>

#include "butil/status.h"
#include "common/logging.h"
#include "coprocessor/utils.h"
#include "proto/common.pb.h"
#include "proto/store.pb.h"
#include "serial/schema/base_schema.h"
#include "serial/schema/boolean_schema.h"
#include "serial/schema/double_schema.h"
#include "serial/schema/float_schema.h"
#include "serial/schema/integer_schema.h"
#include "serial/schema/long_schema.h"
#include "serial/schema/string_schema.h"

namespace dingodb {  // NOLINT

class CoprocessorUtilsTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(CoprocessorUtilsTest, CheckPbSchema) {
  // empty ok
  {
    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;
    butil::Status ok = Utils::CheckPbSchema(pb_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // type invalid
  {
    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;
    pb::common::Schema schema1;
    schema1.set_type(static_cast<::dingodb::pb::common::Schema_Type>(6));
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(0);
    pb_schemas.Add(std::move(schema1));
    butil::Status ok = Utils::CheckPbSchema(pb_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // index invalid
  {
    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;
    pb::common::Schema schema1;
    schema1.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema1.set_is_key(true);
    schema1.set_is_nullable(true);
    schema1.set_index(-1);
    pb_schemas.Add(std::move(schema1));
    butil::Status ok = Utils::CheckPbSchema(pb_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // index invalid
  {
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
    schema2.set_index(2);
    pb_schemas.Add(std::move(schema2));

    butil::Status ok = Utils::CheckPbSchema(pb_schemas);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // OK
  {
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

    butil::Status ok = Utils::CheckPbSchema(pb_schemas);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorUtilsTest, CheckSelection) {
  // empty ok
  {
    ::google::protobuf::RepeatedField<int32_t> selection_columns;
    size_t original_schema_size = 10;
    butil::Status ok = Utils::CheckSelection(selection_columns, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // index = -1 invalid
  {
    ::google::protobuf::RepeatedField<int32_t> selection_columns;
    size_t original_schema_size = 10;
    selection_columns.Add(-1);

    butil::Status ok = Utils::CheckSelection(selection_columns, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // index >= original_schema_size invalid
  {
    ::google::protobuf::RepeatedField<int32_t> selection_columns;
    size_t original_schema_size = 10;
    selection_columns.Add(10);

    butil::Status ok = Utils::CheckSelection(selection_columns, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // OK
  {
    ::google::protobuf::RepeatedField<int32_t> selection_columns;
    size_t original_schema_size = 10;

    for (int i = 0; i < original_schema_size; i++) {
      selection_columns.Add(i);
    }

    butil::Status ok = Utils::CheckSelection(selection_columns, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorUtilsTest, CheckGroupByOperators) {
  // empty ok
  {
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    size_t original_schema_size = 10;
    butil::Status ok = Utils::CheckGroupByOperators(aggregation_operators, 10);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // invalid oper type
  {
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    size_t original_schema_size = 10;

    pb::store::AggregationOperator aggregation_operator1;
    aggregation_operator1.set_oper(::dingodb::pb::store::AggregationType::AGGREGATION_NONE);
    aggregation_operator1.set_index_of_column(0);
    aggregation_operators.Add(std::move(aggregation_operator1));

    butil::Status ok = Utils::CheckGroupByOperators(aggregation_operators, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // invalid index
  {
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    size_t original_schema_size = 10;

    pb::store::AggregationOperator aggregation_operator1;
    aggregation_operator1.set_oper(::dingodb::pb::store::AggregationType::SUM);
    aggregation_operator1.set_index_of_column(-1);
    aggregation_operators.Add(std::move(aggregation_operator1));

    butil::Status ok = Utils::CheckGroupByOperators(aggregation_operators, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // index >= original_schema_size invalid
  {
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    size_t original_schema_size = 10;

    pb::store::AggregationOperator aggregation_operator1;
    aggregation_operator1.set_oper(::dingodb::pb::store::AggregationType::SUM);
    aggregation_operator1.set_index_of_column(10);
    aggregation_operators.Add(std::move(aggregation_operator1));

    butil::Status ok = Utils::CheckGroupByOperators(aggregation_operators, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // count 10 failed
  {
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    size_t original_schema_size = 10;

    pb::store::AggregationOperator aggregation_operator1;
    aggregation_operator1.set_oper(::dingodb::pb::store::AggregationType::COUNT);
    aggregation_operator1.set_index_of_column(10);
    aggregation_operators.Add(std::move(aggregation_operator1));

    butil::Status ok = Utils::CheckGroupByOperators(aggregation_operators, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::EILLEGAL_PARAMTETERS);
  }

  // ok count -1
  {
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    size_t original_schema_size = 10;

    pb::store::AggregationOperator aggregation_operator1;
    aggregation_operator1.set_oper(::dingodb::pb::store::AggregationType::COUNT);
    aggregation_operator1.set_index_of_column(-1);
    aggregation_operators.Add(std::move(aggregation_operator1));

    butil::Status ok = Utils::CheckGroupByOperators(aggregation_operators, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok countwithnull -1
  {
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    size_t original_schema_size = 10;

    pb::store::AggregationOperator aggregation_operator1;
    aggregation_operator1.set_oper(::dingodb::pb::store::AggregationType::COUNTWITHNULL);
    aggregation_operator1.set_index_of_column(-1);
    aggregation_operators.Add(std::move(aggregation_operator1));

    butil::Status ok = Utils::CheckGroupByOperators(aggregation_operators, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // ok countwithnull 10
  {
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    size_t original_schema_size = 10;

    pb::store::AggregationOperator aggregation_operator1;
    aggregation_operator1.set_oper(::dingodb::pb::store::AggregationType::COUNTWITHNULL);
    aggregation_operator1.set_index_of_column(10);
    aggregation_operators.Add(std::move(aggregation_operator1));

    butil::Status ok = Utils::CheckGroupByOperators(aggregation_operators, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // OK
  {
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators;
    size_t original_schema_size = 10;

    pb::store::AggregationOperator aggregation_operator1;
    aggregation_operator1.set_oper(::dingodb::pb::store::AggregationType::SUM);
    aggregation_operator1.set_index_of_column(0);
    aggregation_operators.Add(std::move(aggregation_operator1));

    pb::store::AggregationOperator aggregation_operator2;
    aggregation_operator2.set_oper(::dingodb::pb::store::AggregationType::COUNT);
    aggregation_operator2.set_index_of_column(1);
    aggregation_operators.Add(std::move(aggregation_operator2));

    pb::store::AggregationOperator aggregation_operator3;
    aggregation_operator3.set_oper(::dingodb::pb::store::AggregationType::COUNTWITHNULL);
    aggregation_operator3.set_index_of_column(-1);
    aggregation_operators.Add(std::move(aggregation_operator3));

    pb::store::AggregationOperator aggregation_operator4;
    aggregation_operator4.set_oper(::dingodb::pb::store::AggregationType::MAX);
    aggregation_operator4.set_index_of_column(3);
    aggregation_operators.Add(std::move(aggregation_operator4));

    pb::store::AggregationOperator aggregation_operator5;
    aggregation_operator5.set_oper(::dingodb::pb::store::AggregationType::MIN);
    aggregation_operator5.set_index_of_column(4);
    aggregation_operators.Add(std::move(aggregation_operator5));

    butil::Status ok = Utils::CheckGroupByOperators(aggregation_operators, 10);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorUtilsTest, TransToSerialSchema) {
  // empty
  {
    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // OK
  {
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

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    butil::Status ok = Utils::TransToSerialSchema(pb_schemas, &serial_schemas);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorUtilsTest, CloneSerialSchema) {
  // empty
  {
    std::shared_ptr<BaseSchema> serial_schema;
    std::shared_ptr<BaseSchema> serial_schema_clone;

    serial_schema_clone = Utils::CloneSerialSchema(serial_schema);
    EXPECT_EQ(serial_schema_clone.get(), nullptr);
  }

  // bool OK
  {
    std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema =
        std::make_shared<DingoSchema<std::optional<bool>>>();

    (*serial_schema).SetIndex(0);
    (*serial_schema).SetIsKey(false);
    (*serial_schema).SetAllowNull(false);

    auto serial_schema_clone = Utils::CloneSerialSchema(serial_schema);

    EXPECT_NE(serial_schema_clone.get(), nullptr);

    EXPECT_EQ(serial_schema_clone->GetType(), BaseSchema::Type::kBool);
    EXPECT_EQ(serial_schema_clone->IsKey(), serial_schema->IsKey());
    EXPECT_EQ(serial_schema_clone->AllowNull(), serial_schema->AllowNull());
    EXPECT_EQ(serial_schema_clone->GetIndex(), serial_schema->GetIndex());
  }

  // int OK
  {
    std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema =
        std::make_shared<DingoSchema<std::optional<int32_t>>>();

    (*serial_schema).SetIndex(0);
    (*serial_schema).SetIsKey(false);
    (*serial_schema).SetAllowNull(false);

    auto serial_schema_clone = Utils::CloneSerialSchema(serial_schema);

    EXPECT_NE(serial_schema_clone.get(), nullptr);

    EXPECT_EQ(serial_schema_clone->GetType(), BaseSchema::Type::kInteger);
    EXPECT_EQ(serial_schema_clone->IsKey(), serial_schema->IsKey());
    EXPECT_EQ(serial_schema_clone->AllowNull(), serial_schema->AllowNull());
    EXPECT_EQ(serial_schema_clone->GetIndex(), serial_schema->GetIndex());
  }

  // float OK
  {
    std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema =
        std::make_shared<DingoSchema<std::optional<float>>>();

    (*serial_schema).SetIndex(0);
    (*serial_schema).SetIsKey(false);
    (*serial_schema).SetAllowNull(false);

    auto serial_schema_clone = Utils::CloneSerialSchema(serial_schema);

    EXPECT_NE(serial_schema_clone.get(), nullptr);

    EXPECT_EQ(serial_schema_clone->GetType(), BaseSchema::Type::kFloat);
    EXPECT_EQ(serial_schema_clone->IsKey(), serial_schema->IsKey());
    EXPECT_EQ(serial_schema_clone->AllowNull(), serial_schema->AllowNull());
    EXPECT_EQ(serial_schema_clone->GetIndex(), serial_schema->GetIndex());
  }

  // long  OK
  {
    std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema =
        std::make_shared<DingoSchema<std::optional<int64_t>>>();

    (*serial_schema).SetIndex(0);
    (*serial_schema).SetIsKey(false);
    (*serial_schema).SetAllowNull(false);

    auto serial_schema_clone = Utils::CloneSerialSchema(serial_schema);

    EXPECT_NE(serial_schema_clone.get(), nullptr);

    EXPECT_EQ(serial_schema_clone->GetType(), BaseSchema::Type::kLong);
    EXPECT_EQ(serial_schema_clone->IsKey(), serial_schema->IsKey());
    EXPECT_EQ(serial_schema_clone->AllowNull(), serial_schema->AllowNull());
    EXPECT_EQ(serial_schema_clone->GetIndex(), serial_schema->GetIndex());
  }

  // double  OK
  {
    std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema =
        std::make_shared<DingoSchema<std::optional<double>>>();

    (*serial_schema).SetIndex(0);
    (*serial_schema).SetIsKey(false);
    (*serial_schema).SetAllowNull(false);

    auto serial_schema_clone = Utils::CloneSerialSchema(serial_schema);

    EXPECT_NE(serial_schema_clone.get(), nullptr);

    EXPECT_EQ(serial_schema_clone->GetType(), BaseSchema::Type::kDouble);
    EXPECT_EQ(serial_schema_clone->IsKey(), serial_schema->IsKey());
    EXPECT_EQ(serial_schema_clone->AllowNull(), serial_schema->AllowNull());
    EXPECT_EQ(serial_schema_clone->GetIndex(), serial_schema->GetIndex());
  }

  // string OK
  {
    std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema =
        std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

    (*serial_schema).SetIndex(0);
    (*serial_schema).SetIsKey(false);
    (*serial_schema).SetAllowNull(false);

    auto serial_schema_clone = Utils::CloneSerialSchema(serial_schema);

    EXPECT_NE(serial_schema_clone.get(), nullptr);

    EXPECT_EQ(serial_schema_clone->GetType(), BaseSchema::Type::kString);
    EXPECT_EQ(serial_schema_clone->IsKey(), serial_schema->IsKey());
    EXPECT_EQ(serial_schema_clone->AllowNull(), serial_schema->AllowNull());
    EXPECT_EQ(serial_schema_clone->GetIndex(), serial_schema->GetIndex());
  }
}

TEST_F(CoprocessorUtilsTest, CreateSerialSchema) {
  // empty
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas;
    ::google::protobuf::RepeatedField<int32_t> new_columns;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas;

    butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas, new_columns, &new_serial_schemas);
    EXPECT_EQ(new_serial_schemas.get(), nullptr);
  }

  // normal
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ::google::protobuf::RepeatedField<int32_t> new_columns;
    std::array<int, 12> arr{0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5};

    for (auto value : arr) {
      new_columns.Add(value);
    }

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    old_serial_schemas->reserve(7);

    // bool
    std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema_bool =
        std::make_shared<DingoSchema<std::optional<bool>>>();

    (*serial_schema_bool).SetIndex(0);
    (*serial_schema_bool).SetIsKey(false);
    (*serial_schema_bool).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_bool);

    // int
    std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema_int =
        std::make_shared<DingoSchema<std::optional<int32_t>>>();

    (*serial_schema_int).SetIndex(1);
    (*serial_schema_int).SetIsKey(false);
    (*serial_schema_int).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_int);

    // float
    std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema_float =
        std::make_shared<DingoSchema<std::optional<float>>>();

    (*serial_schema_float).SetIndex(2);
    (*serial_schema_float).SetIsKey(false);
    (*serial_schema_float).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_float);

    // long
    std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema_long =
        std::make_shared<DingoSchema<std::optional<int64_t>>>();

    (*serial_schema_long).SetIndex(3);
    (*serial_schema_long).SetIsKey(false);
    (*serial_schema_long).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_long);

    // double
    std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema_double =
        std::make_shared<DingoSchema<std::optional<double>>>();

    (*serial_schema_double).SetIndex(4);
    (*serial_schema_double).SetIsKey(false);
    (*serial_schema_double).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_double);

    // string
    std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema_string =
        std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

    (*serial_schema_string).SetIndex(5);
    (*serial_schema_string).SetIsKey(false);
    (*serial_schema_string).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_string);

    butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas, new_columns, &new_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    EXPECT_EQ((*new_serial_schemas)[0]->GetType(), BaseSchema::Type::kBool);
    EXPECT_EQ((*new_serial_schemas)[6]->GetType(), BaseSchema::Type::kBool);

    EXPECT_EQ((*new_serial_schemas)[1]->GetType(), BaseSchema::Type::kInteger);
    EXPECT_EQ((*new_serial_schemas)[7]->GetType(), BaseSchema::Type::kInteger);

    EXPECT_EQ((*new_serial_schemas)[2]->GetType(), BaseSchema::Type::kFloat);
    EXPECT_EQ((*new_serial_schemas)[8]->GetType(), BaseSchema::Type::kFloat);

    EXPECT_EQ((*new_serial_schemas)[3]->GetType(), BaseSchema::Type::kLong);
    EXPECT_EQ((*new_serial_schemas)[9]->GetType(), BaseSchema::Type::kLong);

    EXPECT_EQ((*new_serial_schemas)[4]->GetType(), BaseSchema::Type::kDouble);
    EXPECT_EQ((*new_serial_schemas)[10]->GetType(), BaseSchema::Type::kDouble);

    EXPECT_EQ((*new_serial_schemas)[5]->GetType(), BaseSchema::Type::kString);
    EXPECT_EQ((*new_serial_schemas)[11]->GetType(), BaseSchema::Type::kString);
  }
}

TEST_F(CoprocessorUtilsTest, UpdateSerialSchemaIndex) {
  // empty
  {
    butil::Status ok = Utils::UpdateSerialSchemaIndex(nullptr);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // empty
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas;

    butil::Status ok = Utils::UpdateSerialSchemaIndex(&new_serial_schemas);
    EXPECT_EQ(new_serial_schemas.get(), nullptr);
  }

  // normal
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ::google::protobuf::RepeatedField<int32_t> new_columns;
    std::array<int, 12> arr{0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5};

    for (auto value : arr) {
      new_columns.Add(value);
    }

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    old_serial_schemas->reserve(7);

    // bool
    std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema_bool =
        std::make_shared<DingoSchema<std::optional<bool>>>();

    (*serial_schema_bool).SetIndex(0);
    (*serial_schema_bool).SetIsKey(false);
    (*serial_schema_bool).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_bool);

    // int
    std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema_int =
        std::make_shared<DingoSchema<std::optional<int32_t>>>();

    (*serial_schema_int).SetIndex(1);
    (*serial_schema_int).SetIsKey(false);
    (*serial_schema_int).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_int);

    // float
    std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema_float =
        std::make_shared<DingoSchema<std::optional<float>>>();

    (*serial_schema_float).SetIndex(2);
    (*serial_schema_float).SetIsKey(false);
    (*serial_schema_float).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_float);

    // long
    std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema_long =
        std::make_shared<DingoSchema<std::optional<int64_t>>>();

    (*serial_schema_long).SetIndex(3);
    (*serial_schema_long).SetIsKey(false);
    (*serial_schema_long).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_long);

    // double
    std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema_double =
        std::make_shared<DingoSchema<std::optional<double>>>();

    (*serial_schema_double).SetIndex(4);
    (*serial_schema_double).SetIsKey(false);
    (*serial_schema_double).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_double);

    // string
    std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema_string =
        std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

    (*serial_schema_string).SetIndex(5);
    (*serial_schema_string).SetIsKey(false);
    (*serial_schema_string).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_string);

    butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas, new_columns, &new_serial_schemas);

    ok = Utils::UpdateSerialSchemaIndex(&new_serial_schemas);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < new_serial_schemas->size(); i++) {
      EXPECT_EQ(static_cast<int>(i), (*new_serial_schemas)[i]->GetIndex());
    }
  }
}

TEST_F(CoprocessorUtilsTest, UpdateSerialSchemaKey) {
  // empty
  {
    butil::Status ok = Utils::UpdateSerialSchemaIndex(nullptr);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }

  // empty
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas;

    butil::Status ok = Utils::UpdateSerialSchemaIndex(&new_serial_schemas);
    EXPECT_EQ(new_serial_schemas.get(), nullptr);
  }

  // normal
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ::google::protobuf::RepeatedField<int32_t> new_columns;
    std::array<int, 12> arr{0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5};

    for (auto value : arr) {
      new_columns.Add(value);
    }

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    old_serial_schemas->reserve(7);

    // bool
    std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema_bool =
        std::make_shared<DingoSchema<std::optional<bool>>>();

    (*serial_schema_bool).SetIndex(0);
    (*serial_schema_bool).SetIsKey(false);
    (*serial_schema_bool).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_bool);

    // int
    std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema_int =
        std::make_shared<DingoSchema<std::optional<int32_t>>>();

    (*serial_schema_int).SetIndex(1);
    (*serial_schema_int).SetIsKey(false);
    (*serial_schema_int).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_int);

    // float
    std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema_float =
        std::make_shared<DingoSchema<std::optional<float>>>();

    (*serial_schema_float).SetIndex(2);
    (*serial_schema_float).SetIsKey(false);
    (*serial_schema_float).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_float);

    // long
    std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema_long =
        std::make_shared<DingoSchema<std::optional<int64_t>>>();

    (*serial_schema_long).SetIndex(3);
    (*serial_schema_long).SetIsKey(false);
    (*serial_schema_long).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_long);

    // double
    std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema_double =
        std::make_shared<DingoSchema<std::optional<double>>>();

    (*serial_schema_double).SetIndex(4);
    (*serial_schema_double).SetIsKey(false);
    (*serial_schema_double).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_double);

    // string
    std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema_string =
        std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

    (*serial_schema_string).SetIndex(5);
    (*serial_schema_string).SetIsKey(false);
    (*serial_schema_string).SetAllowNull(false);

    old_serial_schemas->emplace_back(serial_schema_string);

    butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas, new_columns, &new_serial_schemas);

    std::vector<bool> keys;
    keys.resize(new_serial_schemas->size(), true);
    ok = Utils::UpdateSerialSchemaKey(keys, &new_serial_schemas);

    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    for (size_t i = 0; i < new_serial_schemas->size(); i++) {
      EXPECT_EQ(true, (*new_serial_schemas)[i]->IsKey());
    }
  }
}

TEST_F(CoprocessorUtilsTest, JoinSerialSchema) {
  // empty
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas1;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas2;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas;

    butil::Status ok = Utils::JoinSerialSchema(old_serial_schemas1, old_serial_schemas2, &new_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // normal
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas1 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    {
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas1 =
          std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

      ::google::protobuf::RepeatedField<int32_t> new_columns1;
      std::array<int, 6> arr1{0, 1, 2, 3, 4, 5};

      for (auto value : arr1) {
        new_columns1.Add(value);
      }

      old_serial_schemas1->reserve(6);

      // bool
      std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema_bool =
          std::make_shared<DingoSchema<std::optional<bool>>>();

      (*serial_schema_bool).SetIndex(0);
      (*serial_schema_bool).SetIsKey(false);
      (*serial_schema_bool).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_bool);

      // int
      std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema_int =
          std::make_shared<DingoSchema<std::optional<int32_t>>>();

      (*serial_schema_int).SetIndex(1);
      (*serial_schema_int).SetIsKey(false);
      (*serial_schema_int).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_int);

      // float
      std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema_float =
          std::make_shared<DingoSchema<std::optional<float>>>();

      (*serial_schema_float).SetIndex(2);
      (*serial_schema_float).SetIsKey(false);
      (*serial_schema_float).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_float);

      // long
      std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema_long =
          std::make_shared<DingoSchema<std::optional<int64_t>>>();

      (*serial_schema_long).SetIndex(3);
      (*serial_schema_long).SetIsKey(false);
      (*serial_schema_long).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_long);

      // double
      std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema_double =
          std::make_shared<DingoSchema<std::optional<double>>>();

      (*serial_schema_double).SetIndex(4);
      (*serial_schema_double).SetIsKey(false);
      (*serial_schema_double).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_double);

      // string
      std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema_string =
          std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

      (*serial_schema_string).SetIndex(5);
      (*serial_schema_string).SetIsKey(false);
      (*serial_schema_string).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_string);

      butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas1, new_columns1, &new_serial_schemas1);
    }

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas2 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    {
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas2 =
          std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

      ::google::protobuf::RepeatedField<int32_t> new_columns2;
      std::array<int, 6> arr2{0, 1, 2, 3, 4, 5};

      for (auto value : arr2) {
        new_columns2.Add(value);
      }

      old_serial_schemas2->reserve(6);

      // bool
      std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema_bool =
          std::make_shared<DingoSchema<std::optional<bool>>>();

      (*serial_schema_bool).SetIndex(0);
      (*serial_schema_bool).SetIsKey(false);
      (*serial_schema_bool).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_bool);

      // int
      std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema_int =
          std::make_shared<DingoSchema<std::optional<int32_t>>>();

      (*serial_schema_int).SetIndex(1);
      (*serial_schema_int).SetIsKey(false);
      (*serial_schema_int).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_int);

      // float
      std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema_float =
          std::make_shared<DingoSchema<std::optional<float>>>();

      (*serial_schema_float).SetIndex(2);
      (*serial_schema_float).SetIsKey(false);
      (*serial_schema_float).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_float);

      // long
      std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema_long =
          std::make_shared<DingoSchema<std::optional<int64_t>>>();

      (*serial_schema_long).SetIndex(3);
      (*serial_schema_long).SetIsKey(false);
      (*serial_schema_long).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_long);

      // double
      std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema_double =
          std::make_shared<DingoSchema<std::optional<double>>>();

      (*serial_schema_double).SetIndex(4);
      (*serial_schema_double).SetIsKey(false);
      (*serial_schema_double).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_double);

      // string
      std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema_string =
          std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

      (*serial_schema_string).SetIndex(5);
      (*serial_schema_string).SetIsKey(false);
      (*serial_schema_string).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_string);

      butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas2, new_columns2, &new_serial_schemas2);
    }

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> join_serial_schemas_1;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> serial_schemas_tmp_1;
    butil::Status ok = Utils::JoinSerialSchema(serial_schemas_tmp_1, new_serial_schemas2, &join_serial_schemas_1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> join_serial_schemas_2;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> serial_schemas_tmp_2;
    ok = Utils::JoinSerialSchema(new_serial_schemas1, serial_schemas_tmp_2, &join_serial_schemas_2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> join_serial_schemas_3;
    ok = Utils::JoinSerialSchema(new_serial_schemas1, new_serial_schemas2, &join_serial_schemas_3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorUtilsTest, CompareSerialSchemaStrict) {
  // empty
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas1;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas2;

    butil::Status ok = Utils::CompareSerialSchemaStrict(old_serial_schemas1, old_serial_schemas2);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // not empty failed
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas1 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas2;

    butil::Status ok = Utils::CompareSerialSchemaStrict(old_serial_schemas1, old_serial_schemas2);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // not empty failed
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas1;

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas2 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    butil::Status ok = Utils::CompareSerialSchemaStrict(old_serial_schemas1, old_serial_schemas2);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // empty ok
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas1 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas2 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    butil::Status ok = Utils::CompareSerialSchemaStrict(old_serial_schemas1, old_serial_schemas2);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // normal
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas1 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    {
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas1 =
          std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

      ::google::protobuf::RepeatedField<int32_t> new_columns1;
      std::array<int, 6> arr1{0, 1, 2, 3, 4, 5};

      for (auto value : arr1) {
        new_columns1.Add(value);
      }

      old_serial_schemas1->reserve(6);

      // bool
      std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema_bool =
          std::make_shared<DingoSchema<std::optional<bool>>>();

      (*serial_schema_bool).SetIndex(0);
      (*serial_schema_bool).SetIsKey(false);
      (*serial_schema_bool).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_bool);

      // int
      std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema_int =
          std::make_shared<DingoSchema<std::optional<int32_t>>>();

      (*serial_schema_int).SetIndex(1);
      (*serial_schema_int).SetIsKey(false);
      (*serial_schema_int).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_int);

      // float
      std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema_float =
          std::make_shared<DingoSchema<std::optional<float>>>();

      (*serial_schema_float).SetIndex(2);
      (*serial_schema_float).SetIsKey(false);
      (*serial_schema_float).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_float);

      // long
      std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema_long =
          std::make_shared<DingoSchema<std::optional<int64_t>>>();

      (*serial_schema_long).SetIndex(3);
      (*serial_schema_long).SetIsKey(false);
      (*serial_schema_long).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_long);

      // double
      std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema_double =
          std::make_shared<DingoSchema<std::optional<double>>>();

      (*serial_schema_double).SetIndex(4);
      (*serial_schema_double).SetIsKey(false);
      (*serial_schema_double).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_double);

      // string
      std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema_string =
          std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

      (*serial_schema_string).SetIndex(5);
      (*serial_schema_string).SetIsKey(false);
      (*serial_schema_string).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_string);

      butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas1, new_columns1, &new_serial_schemas1);
    }

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas2 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    {
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas2 =
          std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

      ::google::protobuf::RepeatedField<int32_t> new_columns2;
      std::array<int, 6> arr2{0, 1, 2, 3, 4, 5};

      for (auto value : arr2) {
        new_columns2.Add(value);
      }

      old_serial_schemas2->reserve(6);

      // bool
      std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema_bool =
          std::make_shared<DingoSchema<std::optional<bool>>>();

      (*serial_schema_bool).SetIndex(0);
      (*serial_schema_bool).SetIsKey(false);
      (*serial_schema_bool).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_bool);

      // int
      std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema_int =
          std::make_shared<DingoSchema<std::optional<int32_t>>>();

      (*serial_schema_int).SetIndex(1);
      (*serial_schema_int).SetIsKey(false);
      (*serial_schema_int).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_int);

      // float
      std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema_float =
          std::make_shared<DingoSchema<std::optional<float>>>();

      (*serial_schema_float).SetIndex(2);
      (*serial_schema_float).SetIsKey(false);
      (*serial_schema_float).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_float);

      // long
      std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema_long =
          std::make_shared<DingoSchema<std::optional<int64_t>>>();

      (*serial_schema_long).SetIndex(3);
      (*serial_schema_long).SetIsKey(false);
      (*serial_schema_long).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_long);

      // double
      std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema_double =
          std::make_shared<DingoSchema<std::optional<double>>>();

      (*serial_schema_double).SetIndex(4);
      (*serial_schema_double).SetIsKey(false);
      (*serial_schema_double).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_double);

      // string
      std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema_string =
          std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

      (*serial_schema_string).SetIndex(5);
      (*serial_schema_string).SetIsKey(false);
      (*serial_schema_string).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_string);

      butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas2, new_columns2, &new_serial_schemas2);
    }

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> join_serial_schemas_1;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> serial_schemas_tmp_1;
    butil::Status ok = Utils::JoinSerialSchema(serial_schemas_tmp_1, new_serial_schemas2, &join_serial_schemas_1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = Utils::CompareSerialSchemaStrict(new_serial_schemas2, join_serial_schemas_1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> join_serial_schemas_2;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> serial_schemas_tmp_2;
    ok = Utils::JoinSerialSchema(new_serial_schemas1, serial_schemas_tmp_2, &join_serial_schemas_2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = Utils::CompareSerialSchemaStrict(new_serial_schemas1, join_serial_schemas_2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> join_serial_schemas_3;
    ok = Utils::JoinSerialSchema(new_serial_schemas1, new_serial_schemas2, &join_serial_schemas_3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    // compare by hand
    size_t j = 0;
    for (size_t i = 0; i < new_serial_schemas1->size(); i++, j++) {
      EXPECT_EQ((*new_serial_schemas1)[i]->GetType(), (*join_serial_schemas_3)[j]->GetType());
    }

    for (size_t i = 0; i < new_serial_schemas1->size(); i++, j++) {
      EXPECT_EQ((*new_serial_schemas2)[i]->GetType(), (*join_serial_schemas_3)[j]->GetType());
    }

    EXPECT_EQ(new_serial_schemas2->size() + new_serial_schemas1->size(), join_serial_schemas_3->size());

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

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

    ///////////////////////////////////////////////////////////////////////
    pb::common::Schema schema7;
    schema7.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema7.set_is_key(true);
    schema7.set_is_nullable(true);
    schema7.set_index(0);
    pb_schemas.Add(std::move(schema7));

    pb::common::Schema schema8;
    schema8.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_INTEGER);
    schema8.set_is_key(true);
    schema8.set_is_nullable(true);
    schema8.set_index(1);
    pb_schemas.Add(std::move(schema8));

    pb::common::Schema schema9;
    schema9.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema9.set_is_key(true);
    schema9.set_is_nullable(true);
    schema9.set_index(2);
    pb_schemas.Add(std::move(schema9));

    pb::common::Schema schema10;
    schema10.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema10.set_is_key(true);
    schema10.set_is_nullable(true);
    schema10.set_index(3);
    pb_schemas.Add(std::move(schema10));

    pb::common::Schema schema11;
    schema11.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema11.set_is_key(true);
    schema11.set_is_nullable(true);
    schema11.set_index(4);
    pb_schemas.Add(std::move(schema11));

    pb::common::Schema schema12;
    schema12.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_STRING);
    schema12.set_is_key(true);
    schema12.set_is_nullable(true);
    schema12.set_index(5);
    pb_schemas.Add(std::move(schema12));

    ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ok = Utils::CompareSerialSchemaStrict(result_serial_schemas, join_serial_schemas_3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorUtilsTest, CompareSerialSchemaNonStrict) {
  // normal
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas1 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    {
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas1 =
          std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

      ::google::protobuf::RepeatedField<int32_t> new_columns1;
      std::array<int, 6> arr1{0, 1, 2, 3, 4, 5};

      for (auto value : arr1) {
        new_columns1.Add(value);
      }

      old_serial_schemas1->reserve(6);

      // bool
      std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema_bool =
          std::make_shared<DingoSchema<std::optional<bool>>>();

      (*serial_schema_bool).SetIndex(0);
      (*serial_schema_bool).SetIsKey(false);
      (*serial_schema_bool).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_bool);

      // int
      std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema_int =
          std::make_shared<DingoSchema<std::optional<int32_t>>>();

      (*serial_schema_int).SetIndex(1);
      (*serial_schema_int).SetIsKey(false);
      (*serial_schema_int).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_int);

      // float
      std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema_float =
          std::make_shared<DingoSchema<std::optional<float>>>();

      (*serial_schema_float).SetIndex(2);
      (*serial_schema_float).SetIsKey(false);
      (*serial_schema_float).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_float);

      // long
      std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema_long =
          std::make_shared<DingoSchema<std::optional<int64_t>>>();

      (*serial_schema_long).SetIndex(3);
      (*serial_schema_long).SetIsKey(false);
      (*serial_schema_long).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_long);

      // double
      std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema_double =
          std::make_shared<DingoSchema<std::optional<double>>>();

      (*serial_schema_double).SetIndex(4);
      (*serial_schema_double).SetIsKey(false);
      (*serial_schema_double).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_double);

      // string
      std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema_string =
          std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

      (*serial_schema_string).SetIndex(5);
      (*serial_schema_string).SetIsKey(false);
      (*serial_schema_string).SetAllowNull(false);

      old_serial_schemas1->emplace_back(serial_schema_string);

      butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas1, new_columns1, &new_serial_schemas1);
    }

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> new_serial_schemas2 =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    {
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> old_serial_schemas2 =
          std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

      ::google::protobuf::RepeatedField<int32_t> new_columns2;
      std::array<int, 6> arr2{0, 1, 2, 3, 4, 5};

      for (auto value : arr2) {
        new_columns2.Add(value);
      }

      old_serial_schemas2->reserve(6);

      // bool
      std::shared_ptr<DingoSchema<std::optional<bool>>> serial_schema_bool =
          std::make_shared<DingoSchema<std::optional<bool>>>();

      (*serial_schema_bool).SetIndex(0);
      (*serial_schema_bool).SetIsKey(false);
      (*serial_schema_bool).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_bool);

      // int
      std::shared_ptr<DingoSchema<std::optional<int32_t>>> serial_schema_int =
          std::make_shared<DingoSchema<std::optional<int32_t>>>();

      (*serial_schema_int).SetIndex(1);
      (*serial_schema_int).SetIsKey(false);
      (*serial_schema_int).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_int);

      // float
      std::shared_ptr<DingoSchema<std::optional<float>>> serial_schema_float =
          std::make_shared<DingoSchema<std::optional<float>>>();

      (*serial_schema_float).SetIndex(2);
      (*serial_schema_float).SetIsKey(false);
      (*serial_schema_float).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_float);

      // long
      std::shared_ptr<DingoSchema<std::optional<int64_t>>> serial_schema_long =
          std::make_shared<DingoSchema<std::optional<int64_t>>>();

      (*serial_schema_long).SetIndex(3);
      (*serial_schema_long).SetIsKey(false);
      (*serial_schema_long).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_long);

      // double
      std::shared_ptr<DingoSchema<std::optional<double>>> serial_schema_double =
          std::make_shared<DingoSchema<std::optional<double>>>();

      (*serial_schema_double).SetIndex(4);
      (*serial_schema_double).SetIsKey(false);
      (*serial_schema_double).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_double);

      // string
      std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> serial_schema_string =
          std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();

      (*serial_schema_string).SetIndex(5);
      (*serial_schema_string).SetIsKey(false);
      (*serial_schema_string).SetAllowNull(false);

      old_serial_schemas2->emplace_back(serial_schema_string);

      butil::Status ok = Utils::CreateSerialSchema(old_serial_schemas2, new_columns2, &new_serial_schemas2);
    }

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> join_serial_schemas_1;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> serial_schemas_tmp_1;
    butil::Status ok = Utils::JoinSerialSchema(serial_schemas_tmp_1, new_serial_schemas2, &join_serial_schemas_1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ::google::protobuf::RepeatedField<int32_t> group_by_columns1;
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators1;
    {
      for (size_t i = 0; i < new_serial_schemas2->size(); i++) {
        pb::store::AggregationOperator aggregation_operator;
        aggregation_operator.set_oper(::dingodb::pb::store::AggregationType::SUM);
        aggregation_operator.set_index_of_column(0);
        aggregation_operators1.Add(std::move(aggregation_operator));
      }
    }

    ok = Utils::CompareSerialSchemaNonStrict(join_serial_schemas_1, new_serial_schemas2, group_by_columns1,
                                             aggregation_operators1);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> join_serial_schemas_2;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> serial_schemas_tmp_2;
    ok = Utils::JoinSerialSchema(new_serial_schemas1, serial_schemas_tmp_2, &join_serial_schemas_2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ::google::protobuf::RepeatedField<int32_t> group_by_columns2;
    group_by_columns2.Resize(new_serial_schemas1->size(), 0);
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators2;

    ok = Utils::CompareSerialSchemaNonStrict(join_serial_schemas_2, new_serial_schemas1, group_by_columns2,
                                             aggregation_operators2);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> join_serial_schemas_3;
    ok = Utils::JoinSerialSchema(new_serial_schemas1, new_serial_schemas2, &join_serial_schemas_3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

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

    ///////////////////////////////////////////////////////////////////////
    pb::common::Schema schema7;
    schema7.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_BOOL);
    schema7.set_is_key(true);
    schema7.set_is_nullable(true);
    schema7.set_index(0);
    pb_schemas.Add(std::move(schema7));

    pb::common::Schema schema8;
    schema8.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema8.set_is_key(true);
    schema8.set_is_nullable(true);
    schema8.set_index(1);
    pb_schemas.Add(std::move(schema8));

    pb::common::Schema schema9;
    schema9.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_FLOAT);
    schema9.set_is_key(true);
    schema9.set_is_nullable(true);
    schema9.set_index(2);
    pb_schemas.Add(std::move(schema9));

    pb::common::Schema schema10;
    schema10.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema10.set_is_key(true);
    schema10.set_is_nullable(true);
    schema10.set_index(3);
    pb_schemas.Add(std::move(schema10));

    pb::common::Schema schema11;
    schema11.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_DOUBLE);
    schema11.set_is_key(true);
    schema11.set_is_nullable(true);
    schema11.set_index(4);
    pb_schemas.Add(std::move(schema11));

    pb::common::Schema schema12;
    schema12.set_type(::dingodb::pb::common::Schema_Type::Schema_Type_LONG);
    schema12.set_is_key(true);
    schema12.set_is_nullable(true);
    schema12.set_index(5);
    pb_schemas.Add(std::move(schema12));

    ok = Utils::TransToSerialSchema(pb_schemas, &result_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);

    ::google::protobuf::RepeatedField<int32_t> group_by_columns3;
    group_by_columns3.Resize(result_serial_schemas->size() / 2, 0);
    ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators3;
    {
      pb::store::AggregationOperator aggregation_operator1;
      aggregation_operator1.set_oper(::dingodb::pb::store::AggregationType::COUNT);
      aggregation_operator1.set_index_of_column(0);
      aggregation_operators3.Add(std::move(aggregation_operator1));

      pb::store::AggregationOperator aggregation_operator2;
      aggregation_operator2.set_oper(::dingodb::pb::store::AggregationType::COUNT);
      aggregation_operator2.set_index_of_column(-1);
      aggregation_operators3.Add(std::move(aggregation_operator2));

      pb::store::AggregationOperator aggregation_operator3;
      aggregation_operator3.set_oper(::dingodb::pb::store::AggregationType::COUNT);
      aggregation_operator3.set_index_of_column(1);
      aggregation_operators3.Add(std::move(aggregation_operator3));

      pb::store::AggregationOperator aggregation_operator4;
      aggregation_operator4.set_oper(::dingodb::pb::store::AggregationType::COUNTWITHNULL);
      aggregation_operator4.set_index_of_column(0);
      aggregation_operators3.Add(std::move(aggregation_operator4));

      pb::store::AggregationOperator aggregation_operator5;
      aggregation_operator5.set_oper(::dingodb::pb::store::AggregationType::COUNTWITHNULL);
      aggregation_operator5.set_index_of_column(-1);
      aggregation_operators3.Add(std::move(aggregation_operator5));

      pb::store::AggregationOperator aggregation_operator6;
      aggregation_operator6.set_oper(::dingodb::pb::store::AggregationType::COUNTWITHNULL);
      aggregation_operator6.set_index_of_column(1);
      aggregation_operators3.Add(std::move(aggregation_operator6));
    }

    ok = Utils::CompareSerialSchemaNonStrict(result_serial_schemas, join_serial_schemas_3, group_by_columns3,
                                             aggregation_operators3);
    EXPECT_EQ(ok.error_code(), pb::error::Errno::OK);
  }
}

TEST_F(CoprocessorUtilsTest, CloneColumn) {
  // bool true
  {
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any original_column = std::optional<bool>(true);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<bool> &new_column_cast = std::any_cast<std::optional<bool>>(new_column);
    if (new_column_cast.has_value()) {
      EXPECT_EQ(true, new_column_cast.value());
    }
  }

  // bool false
  {
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any original_column = std::optional<bool>(false);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<bool> &new_column_cast = std::any_cast<std::optional<bool>>(new_column);
    if (new_column_cast.has_value()) {
      EXPECT_EQ(false, new_column_cast.value());
    }
  }

  // bool nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any original_column = std::optional<bool>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<bool> &new_column_cast = std::any_cast<std::optional<bool>>(new_column);
    EXPECT_EQ(false, new_column_cast.has_value());
  }

  // int 1
  {
    BaseSchema::Type type = BaseSchema::Type::kInteger;
    std::any original_column = std::optional<int32_t>(1);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<int32_t> &new_column_cast = std::any_cast<std::optional<int32_t>>(new_column);
    if (new_column_cast.has_value()) {
      EXPECT_EQ(1, new_column_cast.value());
    }
  }

  // int nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kInteger;
    std::any original_column = std::optional<int32_t>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<int32_t> &new_column_cast = std::any_cast<std::optional<int32_t>>(new_column);
    EXPECT_EQ(false, new_column_cast.has_value());
  }

  // float 1.23
  {
    BaseSchema::Type type = BaseSchema::Type::kFloat;
    std::any original_column = std::optional<float>(1.23);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<float> &new_column_cast = std::any_cast<std::optional<float>>(new_column);
    if (new_column_cast.has_value()) {
      EXPECT_FLOAT_EQ(1.23, new_column_cast.value());
    }
  }

  // float nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kFloat;
    std::any original_column = std::optional<float>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<float> &new_column_cast = std::any_cast<std::optional<float>>(new_column);
    EXPECT_EQ(false, new_column_cast.has_value());
  }

  // long 1000000000000000
  {
    BaseSchema::Type type = BaseSchema::Type::kLong;
    std::any original_column = std::optional<int64_t>(1000000000000000);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<int64_t> &new_column_cast = std::any_cast<std::optional<int64_t>>(new_column);
    if (new_column_cast.has_value()) {
      EXPECT_EQ(1000000000000000, new_column_cast.value());
    }
  }

  // long nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kLong;
    std::any original_column = std::optional<int64_t>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<int64_t> &new_column_cast = std::any_cast<std::optional<int64_t>>(new_column);
    EXPECT_EQ(false, new_column_cast.has_value());
  }

  // double 123456789.23
  {
    BaseSchema::Type type = BaseSchema::Type::kDouble;
    std::any original_column = std::optional<double>(123456789.23);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<double> &new_column_cast = std::any_cast<std::optional<double>>(new_column);
    if (new_column_cast.has_value()) {
      EXPECT_DOUBLE_EQ(123456789.23, new_column_cast.value());
    }
  }

  // double nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kDouble;
    std::any original_column = std::optional<double>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<double> &new_column_cast = std::any_cast<std::optional<double>>(new_column);
    EXPECT_EQ(false, new_column_cast.has_value());
  }

  // string
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("1234567890"));
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<std::shared_ptr<std::string>> &new_column_cast =
        std::any_cast<std::optional<std::shared_ptr<std::string>>>(new_column);
    if (new_column_cast.has_value()) {
      EXPECT_EQ("1234567890", *new_column_cast.value());
    }
  }

  // string nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    const std::optional<std::shared_ptr<std::string>> &new_column_cast =
        std::any_cast<std::optional<std::shared_ptr<std::string>>>(new_column);
    EXPECT_EQ(false, new_column_cast.has_value());
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // bool true
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<bool>(true);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // bool false
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<bool>(false);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // bool nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<bool>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // int 1
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<int32_t>(1);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // int nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<int32_t>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // float 1.23
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<float>(1.23);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // float nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<float>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // long 1000000000000000
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<int64_t>(1000000000000000);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // long nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<int64_t>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // double 123456789.23
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<double>(123456789.23);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // double nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any original_column = std::optional<double>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // string
  {
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any original_column = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("1234567890"));
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }

  // string nullptr
  {
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any original_column = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    std::any new_column = Utils::CloneColumn(original_column, type);
    EXPECT_EQ(true, !new_column.has_value());
  }
}

TEST_F(CoprocessorUtilsTest, CoprocessorParamEmpty) {
  // empty
  {
    pb::store::Coprocessor coprocessor;

    bool ret = Utils::CoprocessorParamEmpty(coprocessor);
    EXPECT_TRUE(ret);
  }
}

TEST_F(CoprocessorUtilsTest, DebugPrintAny) {
  size_t index = 0;

  // empty
  {
    std::any record;
    Utils::DebugPrintAny(record, index);
  }

  // bool
  {
    std::any record = std::make_any<std::optional<bool>>();
    Utils::DebugPrintAny(record, index);

    record = std::make_any<std::optional<bool>>(true);
    Utils::DebugPrintAny(record, index);
  }

  // int
  {
    std::any record = std::make_any<std::optional<int>>();
    Utils::DebugPrintAny(record, index);

    record = std::make_any<std::optional<int>>(12345);
    Utils::DebugPrintAny(record, index);
  }

  // float
  {
    std::any record = std::make_any<std::optional<float>>();
    Utils::DebugPrintAny(record, index);

    record = std::make_any<std::optional<float>>(12345.23);
    Utils::DebugPrintAny(record, index);
  }

  // double
  {
    std::any record = std::make_any<std::optional<double>>();
    Utils::DebugPrintAny(record, index);

    record = std::make_any<std::optional<double>>(12345.256);
    Utils::DebugPrintAny(record, index);
  }

  // int64
  {
    std::any record = std::make_any<std::optional<int64_t>>();
    Utils::DebugPrintAny(record, index);

    record = std::make_any<std::optional<int64_t>>(12345);
    Utils::DebugPrintAny(record, index);
  }

  // string
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::string>>>();
    Utils::DebugPrintAny(record, index);

    record = std::make_any<std::optional<std::shared_ptr<std::string>>>(std::make_shared<std::string>("vvvvv"));
    Utils::DebugPrintAny(record, index);
  }

  // bool list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<bool>>>>();
    Utils::DebugPrintAny(record, index);

    std::shared_ptr<std::vector<bool>> value(new std::vector<bool>{true, false, true});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<bool>>>>(value);
    Utils::DebugPrintAny(record, index);

    value->clear();
    record = std::make_any<std::optional<std::shared_ptr<std::vector<bool>>>>(value);
    Utils::DebugPrintAny(record, index);
  }

  // int list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<int>>>>();
    Utils::DebugPrintAny(record, index);

    std::shared_ptr<std::vector<int>> value(new std::vector<int>{1, 2, 3});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<int>>>>(value);
    Utils::DebugPrintAny(record, index);

    value->clear();
    record = std::make_any<std::optional<std::shared_ptr<std::vector<int>>>>(value);
    Utils::DebugPrintAny(record, index);
  }

  // float list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<float>>>>();
    Utils::DebugPrintAny(record, index);

    std::shared_ptr<std::vector<float>> value(new std::vector<float>{1.23, 2.23, 3.23});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<float>>>>(value);
    Utils::DebugPrintAny(record, index);

    value->clear();
    record = std::make_any<std::optional<std::shared_ptr<std::vector<float>>>>(value);
    Utils::DebugPrintAny(record, index);
  }

  // int64 list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<int64_t>>>>();
    Utils::DebugPrintAny(record, index);

    std::shared_ptr<std::vector<int64_t>> value(new std::vector<int64_t>{1000000, 200000000, 3000000});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<int64_t>>>>(value);
    Utils::DebugPrintAny(record, index);

    value->clear();
    record = std::make_any<std::optional<std::shared_ptr<std::vector<int64_t>>>>(value);
    Utils::DebugPrintAny(record, index);
  }

  // double list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<double>>>>();
    Utils::DebugPrintAny(record, index);

    std::shared_ptr<std::vector<double>> value(new std::vector<double>{1.23, 2.23, 3.23});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<double>>>>(value);
    Utils::DebugPrintAny(record, index);

    value->clear();
    record = std::make_any<std::optional<std::shared_ptr<std::vector<double>>>>(value);
    Utils::DebugPrintAny(record, index);
  }

  // string list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<std::string>>>>();
    Utils::DebugPrintAny(record, index);

    std::shared_ptr<std::vector<std::string>> value(new std::vector<std::string>{"vvvv", "ccccc", "gggggg"});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<std::string>>>>(value);
    Utils::DebugPrintAny(record, index);

    value->clear();
    record = std::make_any<std::optional<std::shared_ptr<std::vector<std::string>>>>(value);
    Utils::DebugPrintAny(record, index);
  }

  // not found
  {
    std::any record = std::make_any<bool>(false);
    Utils::DebugPrintAny(record, index);

    record = std::make_any<bool>(true);
    Utils::DebugPrintAny(record, index);
  }
}

TEST_F(CoprocessorUtilsTest, DebugPrintAnyArray) {
  std::vector<std::any> records;

  // bool
  {
    std::any record = std::make_any<std::optional<bool>>();
    records.push_back(record);

    record = std::make_any<std::optional<bool>>(true);
    records.push_back(record);
  }

  // int
  {
    std::any record = std::make_any<std::optional<int>>();
    records.push_back(record);

    record = std::make_any<std::optional<int>>(12345);
    records.push_back(record);
  }

  // float
  {
    std::any record = std::make_any<std::optional<float>>();
    records.push_back(record);

    record = std::make_any<std::optional<float>>(12345.23);
    records.push_back(record);
  }

  // double
  {
    std::any record = std::make_any<std::optional<double>>();
    records.push_back(record);

    record = std::make_any<std::optional<double>>(12345.256);
    records.push_back(record);
  }

  // int64
  {
    std::any record = std::make_any<std::optional<int64_t>>();
    records.push_back(record);

    record = std::make_any<std::optional<int64_t>>(12345);
    records.push_back(record);
  }

  // string
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::string>>>();
    records.push_back(record);

    record = std::make_any<std::optional<std::shared_ptr<std::string>>>(std::make_shared<std::string>("vvvvv"));
    records.push_back(record);
  }

  // bool list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<bool>>>>();
    records.push_back(record);

    std::shared_ptr<std::vector<bool>> value(new std::vector<bool>{true, false, true});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<bool>>>>(value);
    records.push_back(record);
  }

  // int list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<int>>>>();
    records.push_back(record);

    std::shared_ptr<std::vector<int>> value(new std::vector<int>{1, 2, 3});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<int>>>>(value);
    records.push_back(record);
  }

  // float list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<float>>>>();
    records.push_back(record);

    std::shared_ptr<std::vector<float>> value(new std::vector<float>{1.23, 2.23, 3.23});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<float>>>>(value);
    records.push_back(record);
  }

  // int64 list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<int64_t>>>>();
    records.push_back(record);

    std::shared_ptr<std::vector<int64_t>> value(new std::vector<int64_t>{1000000, 200000000, 3000000});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<int64_t>>>>(value);
    records.push_back(record);
  }

  // double list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<double>>>>();
    records.push_back(record);

    std::shared_ptr<std::vector<double>> value(new std::vector<double>{1.23, 2.23, 3.23});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<double>>>>(value);
    records.push_back(record);
  }

  // string list
  {
    std::any record = std::make_any<std::optional<std::shared_ptr<std::vector<std::string>>>>();
    records.push_back(record);

    std::shared_ptr<std::vector<std::string>> value(new std::vector<std::string>{"vvvv", "ccccc", "gggggg"});
    record = std::make_any<std::optional<std::shared_ptr<std::vector<std::string>>>>(value);
    records.push_back(record);
  }

  Utils::DebugPrintAnyArray(records, "Dummy");
}

}  // namespace dingodb

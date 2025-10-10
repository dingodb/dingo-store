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
#include <sys/types.h>

#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>

#include "butil/status.h"
#include "document/codec.h"
#include "document/document_index_factory.h"

const std::string kDocumentIndexTestIndexPath = "./vector_index_use_document_test_index";

class DingoVectorUseDocumentIndexTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {  // print test start info and current path
    std::cout << "vector_index_use_document test start, current_path: " << std::filesystem::current_path() << '\n';
    std::filesystem::remove_all(kDocumentIndexTestIndexPath);
  }

  static void TearDownTestSuite() {
    std::filesystem::remove_all(kDocumentIndexTestIndexPath);

    // print test end and current path
    std::cout << "vector_index_use_document test end, current_path: " << std::filesystem::current_path() << '\n';
  }

  static inline int64_t add_nums = 200;
  static inline std::string text_search_string = "abc";
};

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
static std::string ToRFC3339(const std::chrono::system_clock::time_point& time_point) {
  std::time_t time_t = std::chrono::system_clock::to_time_t(time_point);
  std::tm* tm_ptr = std::gmtime(&time_t);

  std::ostringstream oss;
  oss << std::put_time(tm_ptr, "%Y-%m-%dT%H:%M:%SZ");
  return oss.str();
}

TEST_F(DingoVectorUseDocumentIndexTest, test_default_create_no_bytes) {
  std::filesystem::remove_all(kDocumentIndexTestIndexPath);
  std::string index_path{kDocumentIndexTestIndexPath};

  std::string error_message;
  std::string json_parameter;
  std::map<std::string, dingodb::TokenizerType> column_tokenizer_parameter;

  dingodb::pb::common::DocumentIndexParameter document_index_parameter;
  auto* scalar_schema = document_index_parameter.mutable_scalar_schema();
  auto* text_field = scalar_schema->add_fields();
  text_field->set_key("text_name");
  text_field->set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
  column_tokenizer_parameter["text_name"] = dingodb::TokenizerType::kTokenizerTypeText;

  auto* i64_field = scalar_schema->add_fields();
  i64_field->set_key("i64_name");
  i64_field->set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
  column_tokenizer_parameter["i64_name"] = dingodb::TokenizerType::kTokenizerTypeI64;

  auto* f64_field = scalar_schema->add_fields();
  f64_field->set_key("f64_name");
  f64_field->set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
  column_tokenizer_parameter["f64_name"] = dingodb::TokenizerType::kTokenizerTypeF64;

  auto* bytes_field = scalar_schema->add_fields();
  bytes_field->set_key("bytes_name");
  bytes_field->set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
  column_tokenizer_parameter["bytes_name"] = dingodb::TokenizerType::kTokenizerTypeBytes;

  auto* datetime_field = scalar_schema->add_fields();
  datetime_field->set_key("datetime_name");
  datetime_field->set_field_type(dingodb::pb::common::ScalarFieldType::DATETIME);
  column_tokenizer_parameter["datetime_name"] = dingodb::TokenizerType::kTokenizerTypeDateTime;

  auto* bool_field = scalar_schema->add_fields();
  bool_field->set_key("bool_name");
  bool_field->set_field_type(dingodb::pb::common::ScalarFieldType::BOOL);
  column_tokenizer_parameter["bool_name"] = dingodb::TokenizerType::kTokenizerTypeBool;

  auto ret1 = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      column_tokenizer_parameter, json_parameter, error_message);
  if (!ret1) {
    std::cout << "error_message: " << error_message << '\n';
  }
  EXPECT_TRUE(ret1);
}

TEST_F(DingoVectorUseDocumentIndexTest, test_default_create) {
  std::filesystem::remove_all(kDocumentIndexTestIndexPath);
  std::string index_path{kDocumentIndexTestIndexPath};

  std::string error_message;
  std::string json_parameter;
  std::map<std::string, dingodb::TokenizerType> column_tokenizer_parameter;

  dingodb::pb::common::DocumentIndexParameter document_index_parameter;
  auto* scalar_schema = document_index_parameter.mutable_scalar_schema();
  auto* text_field = scalar_schema->add_fields();
  text_field->set_key("text_name");
  text_field->set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
  column_tokenizer_parameter["text_name"] = dingodb::TokenizerType::kTokenizerTypeText;

  auto* i64_field = scalar_schema->add_fields();
  i64_field->set_key("i64_name");
  i64_field->set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
  column_tokenizer_parameter["i64_name"] = dingodb::TokenizerType::kTokenizerTypeI64;

  auto* f64_field = scalar_schema->add_fields();
  f64_field->set_key("f64_name");
  f64_field->set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
  column_tokenizer_parameter["f64_name"] = dingodb::TokenizerType::kTokenizerTypeF64;

  auto* datetime_field = scalar_schema->add_fields();
  datetime_field->set_key("datetime_name");
  datetime_field->set_field_type(dingodb::pb::common::ScalarFieldType::DATETIME);
  column_tokenizer_parameter["datetime_name"] = dingodb::TokenizerType::kTokenizerTypeDateTime;

  auto* bool_field = scalar_schema->add_fields();
  bool_field->set_key("bool_name");
  bool_field->set_field_type(dingodb::pb::common::ScalarFieldType::BOOL);
  column_tokenizer_parameter["bool_name"] = dingodb::TokenizerType::kTokenizerTypeBool;

  auto* bytes_field = scalar_schema->add_fields();
  bytes_field->set_key("bytes_name");
  bytes_field->set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
  column_tokenizer_parameter["bytes_name"] = dingodb::TokenizerType::kTokenizerTypeBytes;

  auto ret1 = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      column_tokenizer_parameter, json_parameter, error_message);
  if (!ret1) {
    std::cout << "error_message: " << error_message << '\n';
  }
  EXPECT_TRUE(ret1);

  document_index_parameter.set_json_parameter(json_parameter);

  dingodb::pb::common::RegionEpoch region_epoch;
  dingodb::pb::common::Range range;

  {
    auto document_index =
        dingodb::DocumentIndexFactory::CreateIndex(1, index_path, document_index_parameter, region_epoch, range, true);
    ASSERT_TRUE(document_index != nullptr);
  }

  auto document_index =
      dingodb::DocumentIndexFactory::LoadIndex(1, index_path, document_index_parameter, region_epoch, range);
  ASSERT_TRUE(document_index != nullptr);

  std::vector<dingodb::pb::common::DocumentWithId> document_with_ids;
  std::vector<std::string> texts_to_insert;
  texts_to_insert.reserve(add_nums);
  for (int i = 0; i < add_nums; i++) {
    texts_to_insert.push_back(text_search_string);
  }

  for (int i = 0; i < texts_to_insert.size(); i++) {
    dingodb::pb::common::DocumentWithId document_with_id1;
    document_with_id1.set_id(i + 1);
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data(texts_to_insert.at(i));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"text_name", document_value1});

    dingodb::pb::common::DocumentValue document_value2;
    document_value2.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
    document_value2.mutable_field_value()->set_long_data(1000 + i);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"i64_name", document_value2});

    dingodb::pb::common::DocumentValue document_value3;
    document_value3.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
    document_value3.mutable_field_value()->set_double_data(1000.0 + i);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"f64_name", document_value3});

    dingodb::pb::common::DocumentValue document_value4;
    document_value4.set_field_type(dingodb::pb::common::ScalarFieldType::DATETIME);
    auto data_time = ToRFC3339(std::chrono::system_clock::now());
    document_value4.mutable_field_value()->set_datetime_data(data_time);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"datetime_name", document_value4});

    dingodb::pb::common::DocumentValue document_value5;
    document_value5.set_field_type(dingodb::pb::common::ScalarFieldType::BOOL);
    document_value5.mutable_field_value()->set_bool_data(i % 2 == 0);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"bool_name", document_value5});

    dingodb::pb::common::DocumentValue document_value6;
    document_value6.set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
    document_value6.mutable_field_value()->set_bytes_data(texts_to_insert.at(i));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"bytes_name", document_value6});

    document_with_ids.push_back(document_with_id1);
  }

  auto ret = document_index->Add(document_with_ids, true);
  std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
  EXPECT_EQ(ret.ok(), true);

  // search test_name
  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::string query_string = "text_name:" + text_search_string;
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), add_nums);
    for (const auto& document_with_score : results) {
      std::cout << "bool_name:true document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }

  // search  bool_name = true
  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::string query_string = "bool_name:true";
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), (add_nums / 2 + add_nums % 2));
    for (const auto& document_with_score : results) {
      std::cout << "bool_name:true document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }

  // search  bool_name = false
  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::string query_string = "bool_name:false";
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), (add_nums / 2 + add_nums % 2));
    for (const auto& document_with_score : results) {
      std::cout << "bool_name:false document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }

  // search i64_name >= 1000 and i64_name <= 2000
  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::string query_string = "i64_name:>=1000 AND i64_name:<=2000";
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), add_nums);
    for (const auto& document_with_score : results) {
      std::cout << "i64_name between 1000 and 1100 document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }

  // search i64_name >= 1000
  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::string query_string = "i64_name:[1000 TO *]";
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), add_nums);
    for (const auto& document_with_score : results) {
      std::cout << "i64_name between 1000 and 1100 document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }

  // search i64_name >= 1000 and -i64_name = 1000
  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    // std::string query_string = "i64_name:>=1000 AND -i64_name:1000"; // ok
    std::string query_string = "i64_name:>=1000 NOT i64_name:1000";  // ok
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), add_nums - 1);
    for (const auto& document_with_score : results) {
      std::cout << "i64_name between 1000 and 1100 document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }

  // search i64_name  in  1000 1001
  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::string query_string = "i64_name: IN [1000 1001]";
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 2);
    for (const auto& document_with_score : results) {
      std::cout << "i64_name in [1000, 1001] document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }

  // search f64_name >= 1000.0 and f64_name <= 2000.0
  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::string query_string = "f64_name:>=1000.0 AND f64_name:<=2000.0";
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), add_nums);
    for (const auto& document_with_score : results) {
      std::cout << "f64_name between 1000.0 and 1100.0 document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }

  // search datetime_name >= now - 1 day
  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    auto now = std::chrono::system_clock::now();
    auto one_day_ago = now - std::chrono::hours(24);
    std::string one_day_ago_str = ToRFC3339(one_day_ago);
    std::string query_string = "datetime_name:>=" + one_day_ago_str;
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), add_nums);
    for (const auto& document_with_score : results) {
      std::cout << "datetime_name >= now - 1 day document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }
}

static std::string GenerateRandomString(size_t length) {
  if (length == 0) return "";

  const std::string charset =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<size_t> distribution(0, charset.size() - 1);

  std::string result;
  result.reserve(length);

  for (size_t i = 0; i < length; ++i) {
    result += charset[distribution(generator)];
  }

  return result;
}

TEST_F(DingoVectorUseDocumentIndexTest, test_text_length) {
  std::filesystem::remove_all(kDocumentIndexTestIndexPath);
  std::string index_path{kDocumentIndexTestIndexPath};

  std::string error_message;
  std::string json_parameter;
  std::map<std::string, dingodb::TokenizerType> column_tokenizer_parameter;

  dingodb::pb::common::DocumentIndexParameter document_index_parameter;
  auto* scalar_schema = document_index_parameter.mutable_scalar_schema();
  auto* text_field = scalar_schema->add_fields();
  text_field->set_key("text");
  text_field->set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
  column_tokenizer_parameter["text"] = dingodb::TokenizerType::kTokenizerTypeText;

  auto ret1 = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameter(column_tokenizer_parameter, json_parameter,
                                                                       error_message);
  if (!ret1) {
    std::cout << "error_message: " << error_message << '\n';
  }
  ASSERT_TRUE(ret1);

  document_index_parameter.set_json_parameter(json_parameter);

  dingodb::pb::common::RegionEpoch region_epoch;
  dingodb::pb::common::Range range;

  auto document_index =
      dingodb::DocumentIndexFactory::CreateIndex(1, index_path, document_index_parameter, region_epoch, range, true);
  ASSERT_TRUE(document_index != nullptr);

  std::vector<dingodb::pb::common::DocumentWithId> document_with_ids;
  std::vector<std::string> texts_to_insert;
  std::string text = GenerateRandomString(63 * 1024 + 512 + 256 + 128 + 64 + 32 + 16 + 8 + 2);  // 65530  = 64 * 1024 -6
  // std::string text = GenerateRandomString(64 * 1024);

  int num = 10;
  texts_to_insert.reserve(num);
  for (int i = 0; i < num; i++) {
    texts_to_insert.push_back(text);
  }

  for (int i = 0; i < texts_to_insert.size(); i++) {
    dingodb::pb::common::DocumentWithId document_with_id1;
    document_with_id1.set_id(i + 1);
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data(texts_to_insert.at(i));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"text", document_value1});

    document_with_ids.push_back(document_with_id1);
  }

  auto ret = document_index->Add(document_with_ids, true);
  std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
  EXPECT_EQ(ret.ok(), true);

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::string query_string = "text:" + text;
    ret = document_index->Search(0, query_string, false, 0, INT64_MAX, false, true, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), num);
    for (const auto& document_with_score : results) {
      std::cout << "text document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }
}

#endif  // #if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
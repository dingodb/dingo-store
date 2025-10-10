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
#include "document/document_index.h"
#include "document/document_index_factory.h"

const std::string kDocumentIndexTestIndexPath1 = "./vector_index_use_document_index_test1/";
const std::string kDocumentIndexTestIndexPath2 = "./vector_index_use_document_index_test2/";

class DingoVectorUseDocumentIndexWrapperTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {  // print test start info and current path
    std::cout << "vector_index_use_document_index_wrapper test start, current_path: " << std::filesystem::current_path()
              << '\n';
    std::filesystem::remove_all(kDocumentIndexTestIndexPath1);
    std::filesystem::remove_all(kDocumentIndexTestIndexPath2);
  }

  static void TearDownTestSuite() {
    std::filesystem::remove_all(kDocumentIndexTestIndexPath1);
    std::filesystem::remove_all(kDocumentIndexTestIndexPath2);
    // print test end and current path
    std::cout << "vector_index_use_document_index_wrapper test end, current_path: " << std::filesystem::current_path()
              << '\n';
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

TEST_F(DingoVectorUseDocumentIndexWrapperTest, document_index_search_merge) {
  std::filesystem::remove_all(kDocumentIndexTestIndexPath1);
  std::string index_path1{kDocumentIndexTestIndexPath1};
  std::filesystem::remove_all(kDocumentIndexTestIndexPath2);
  std::string index_path2{kDocumentIndexTestIndexPath2};

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

  region_epoch.set_conf_version(1);
  region_epoch.set_version(1);
  range.set_start_key("wabcdedfg");
  range.set_end_key("wabcdedfh");

  {
    auto document_index1 =
        dingodb::DocumentIndexFactory::CreateIndex(1, index_path1, document_index_parameter, region_epoch, range, true);
    ASSERT_TRUE(document_index1 != nullptr);
  }

  auto document_index1 =
      dingodb::DocumentIndexFactory::LoadIndex(1, index_path1, document_index_parameter, region_epoch, range);
  ASSERT_TRUE(document_index1 != nullptr);

  {
    auto document_index2 =
        dingodb::DocumentIndexFactory::CreateIndex(2, index_path2, document_index_parameter, region_epoch, range, true);
    ASSERT_TRUE(document_index2 != nullptr);
  }

  auto document_index2 =
      dingodb::DocumentIndexFactory::LoadIndex(2, index_path2, document_index_parameter, region_epoch, range);
  ASSERT_TRUE(document_index2 != nullptr);

  std::vector<dingodb::pb::common::DocumentWithId> document_with_ids;
  std::vector<std::string> texts_to_insert;
  texts_to_insert.reserve(add_nums);
  for (int i = 0; i < add_nums; i++) {
    texts_to_insert.push_back(text_search_string);
  }

  for (int i = 0; i < texts_to_insert.size() / 3 * 2; i++) {
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

  auto ret = document_index1->Add(document_with_ids, true);
  std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
  EXPECT_EQ(ret.ok(), true);

  document_with_ids.clear();

  for (int i = texts_to_insert.size() / 3; i < texts_to_insert.size(); i++) {
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

  auto ret2 = document_index2->Add(document_with_ids, true);
  std::cout << "status: " << ret2.error_code() << ", " << ret2.error_str() << '\n';
  EXPECT_EQ(ret2.ok(), true);

  auto document_index_wrapper = dingodb::DocumentIndexWrapper::New(1, document_index_parameter,
                                                                   dingodb::UseDocumentPurposeType::kVectorIndexModule);

  document_index_wrapper->UpdateDocumentIndex(document_index1, "trace_string");
  document_index_wrapper->SetSiblingDocumentIndex(document_index2);

  // search  bool_name = true
  {
    dingodb::pb::common::Range region_range;
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::string query_string = "i64_name:>=1000 AND i64_name:<=2000";
    dingodb::pb::common::DocumentSearchParameter document_search_parameter;
    document_search_parameter.set_top_n(0);  // for all
    document_search_parameter.set_query_string(query_string);
    // document_ids ignore
    // column_names ignore
    document_search_parameter.set_without_scalar_data(true);
    // selected_keys ignore
    document_search_parameter.set_without_table_data(true);  // must be true. else crash.
    document_search_parameter.set_query_unlimited(true);     // for all
    ret = document_index_wrapper->Search(region_range, document_search_parameter, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), add_nums);
    for (const auto& document_with_score : results) {
      std::cout << "document_id: " << document_with_score.document_with_id().id()
                << ", score: " << document_with_score.score() << '\n';
    }
    std::cout << '\n';
  }
}

#endif  // #if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
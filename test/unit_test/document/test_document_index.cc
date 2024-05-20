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
#include <iostream>

#include "butil/status.h"
#include "document/codec.h"
#include "document/document_index_factory.h"

static size_t log_level = 1;

const std::string kDocumentIndexTestIndexPath = "./document_test_index";
const std::string kDocumentIndexTestLogPath = "./document_test_log";

class DingoDocumentIndexTest : public testing::Test {
 protected:
  void SetUp() override {
    // print test start info and current path
    std::cout << "document_index test start, current_path: " << std::filesystem::current_path() << '\n';
    std::filesystem::remove_all(kDocumentIndexTestIndexPath);
    std::filesystem::remove_all(kDocumentIndexTestLogPath);
  }
  void TearDown() override {
    std::filesystem::remove_all(kDocumentIndexTestIndexPath);
    std::filesystem::remove_all(kDocumentIndexTestLogPath);

    // print test end and current path
    std::cout << "document_index test end, current_path: " << std::filesystem::current_path() << '\n';
  }
};

TEST(DingoDocumentIndexTest, test_default_create) {
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

  auto* i64_field = scalar_schema->add_fields();
  i64_field->set_key("i64");
  i64_field->set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
  column_tokenizer_parameter["i64"] = dingodb::TokenizerType::kTokenizerTypeI64;

  auto* f64_field = scalar_schema->add_fields();
  f64_field->set_key("f64");
  f64_field->set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
  column_tokenizer_parameter["f64"] = dingodb::TokenizerType::kTokenizerTypeF64;

  auto* bytes_field = scalar_schema->add_fields();
  bytes_field->set_key("bytes");
  bytes_field->set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
  column_tokenizer_parameter["bytes"] = dingodb::TokenizerType::kTokenizerTypeBytes;

  auto ret1 = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameter(column_tokenizer_parameter, json_parameter,
                                                                       error_message);
  if (!ret1) {
    std::cout << "error_message: " << error_message << '\n';
  }
  ASSERT_TRUE(ret1);

  document_index_parameter.set_json_parameter(json_parameter);

  dingodb::pb::common::RegionEpoch region_epoch;
  dingodb::pb::common::Range range;

  butil::Status tmp_status;
  {
    auto document_index = dingodb::DocumentIndexFactory::CreateIndex(1, index_path, document_index_parameter,
                                                                     region_epoch, range, true, tmp_status);
    std::cout << "status: " << tmp_status.error_code() << ", " << tmp_status.error_str() << '\n';
    ASSERT_TRUE(document_index != nullptr);
  }

  auto document_index = dingodb::DocumentIndexFactory::LoadIndex(1, index_path, document_index_parameter, region_epoch,
                                                                 range, tmp_status);
  std::cout << "status: " << tmp_status.error_code() << ", " << tmp_status.error_str() << '\n';
  ASSERT_TRUE(document_index != nullptr);

  std::vector<dingodb::pb::common::DocumentWithId> document_with_ids;
  std::vector<std::string> texts_to_insert;
  texts_to_insert.push_back("Ancient empires rise and fall, shaping history's course.");
  texts_to_insert.push_back("Artistic expressions reflect diverse cultural heritages.");
  texts_to_insert.push_back("Social movements transform societies, forging new paths.");
  texts_to_insert.push_back("Economies fluctuate, reflecting the complex interplay of global forces.");
  texts_to_insert.push_back("Strategic military campaigns alter the balance of power.");
  texts_to_insert.push_back("Quantum leaps redefine understanding of physical laws.");
  texts_to_insert.push_back("Chemical reactions unlock mysteries of nature.");
  texts_to_insert.push_back("Philosophical debates ponder the essence of existence.");
  texts_to_insert.push_back("Marriages blend traditions, celebrating love's union.");
  texts_to_insert.push_back("Explorers discover uncharted territories, expanding world maps.");

  for (int i = 0; i < texts_to_insert.size(); i++) {
    dingodb::pb::common::DocumentWithId document_with_id1;
    document_with_id1.set_id(i + 1);
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data(texts_to_insert.at(i));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"text", document_value1});

    dingodb::pb::common::DocumentValue document_value2;
    document_value2.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
    document_value2.mutable_field_value()->set_long_data(1000 + i);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"i64", document_value2});

    dingodb::pb::common::DocumentValue document_value3;
    document_value3.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
    document_value3.mutable_field_value()->set_double_data(1000.0 + i);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"f64", document_value3});

    dingodb::pb::common::DocumentValue document_value4;
    document_value4.set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
    document_value4.mutable_field_value()->set_bytes_data("bytes_data_" + std::to_string(i));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"bytes", document_value4});

    document_with_ids.push_back(document_with_id1);
  }

  auto ret = document_index->Add(document_with_ids, true);
  std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
  EXPECT_EQ(ret.ok(), true);

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    ret = document_index->Search(5, "discover", false, 0, INT64_MAX, false, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 1);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    ret = document_index->Search(10, "of", false, 0, INT64_MAX, false, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 5);
  }
}

TEST(DingoDocumentIndexTest, test_load_or_create) {
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

  auto* i64_field = scalar_schema->add_fields();
  i64_field->set_key("i64");
  i64_field->set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
  column_tokenizer_parameter["i64"] = dingodb::TokenizerType::kTokenizerTypeI64;

  auto* f64_field = scalar_schema->add_fields();
  f64_field->set_key("f64");
  f64_field->set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
  column_tokenizer_parameter["f64"] = dingodb::TokenizerType::kTokenizerTypeF64;

  auto* bytes_field = scalar_schema->add_fields();
  bytes_field->set_key("bytes");
  bytes_field->set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
  column_tokenizer_parameter["bytes"] = dingodb::TokenizerType::kTokenizerTypeBytes;

  auto ret1 = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameter(column_tokenizer_parameter, json_parameter,
                                                                       error_message);
  if (!ret1) {
    std::cout << "error_message: " << error_message << '\n';
  }
  ASSERT_TRUE(ret1);

  document_index_parameter.set_json_parameter(json_parameter);

  dingodb::pb::common::RegionEpoch region_epoch;
  dingodb::pb::common::Range range;

  butil::Status tmp_status;
  auto document_index = dingodb::DocumentIndexFactory::LoadOrCreateIndex(1, index_path, document_index_parameter,
                                                                         region_epoch, range, tmp_status);
  std::cout << "status: " << tmp_status.error_code() << ", " << tmp_status.error_str() << '\n';
  ASSERT_TRUE(document_index != nullptr);

  std::vector<dingodb::pb::common::DocumentWithId> document_with_ids;
  std::vector<std::string> texts_to_insert;
  texts_to_insert.push_back("Ancient empires rise and fall, shaping history's course.");                 // 1
  texts_to_insert.push_back("Artistic expressions reflect diverse cultural heritages.");                 // 2
  texts_to_insert.push_back("Social movements transform societies, forging new paths.");                 // 3
  texts_to_insert.push_back("Economies fluctuate, reflecting the complex interplay of global forces.");  // 4 of
  texts_to_insert.push_back("Strategic military campaigns alter the balance of power.");                 // 5 of
  texts_to_insert.push_back("Quantum leaps redefine understanding of physical laws.");                   // 6 of
  texts_to_insert.push_back("Chemical reactions unlock mysteries of nature.");                           // 7 of
  texts_to_insert.push_back("Philosophical debates ponder the essence of existence.");                   // 8 of
  texts_to_insert.push_back("Marriages blend traditions, celebrating love's union.");                    // 9
  texts_to_insert.push_back("Explorers discover uncharted territories, expanding world maps.");          // 10

  for (int i = 0; i < texts_to_insert.size(); i++) {
    dingodb::pb::common::DocumentWithId document_with_id1;
    document_with_id1.set_id(i + 1);
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data(texts_to_insert.at(i));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"text", document_value1});

    dingodb::pb::common::DocumentValue document_value2;
    document_value2.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
    document_value2.mutable_field_value()->set_long_data(1000 + i);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"i64", document_value2});

    dingodb::pb::common::DocumentValue document_value3;
    document_value3.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
    document_value3.mutable_field_value()->set_double_data(1000.0 + i);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"f64", document_value3});

    dingodb::pb::common::DocumentValue document_value4;
    document_value4.set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
    document_value4.mutable_field_value()->set_bytes_data("bytes_data_" + std::to_string(i));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"bytes", document_value4});

    document_with_ids.push_back(document_with_id1);
  }

  auto ret = document_index->Add(document_with_ids, true);
  std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
  EXPECT_EQ(ret.ok(), true);

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    ret = document_index->Search(5, "discover", false, 0, INT64_MAX, false, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 1);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    ret = document_index->Search(10, "of", false, 0, INT64_MAX, false, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 5);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    ret = document_index->Search(10, R"(text:"of")", true, 5, 8, false, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 3);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::vector<uint64_t> alive_ids;
    alive_ids.push_back(6);
    alive_ids.push_back(7);
    alive_ids.push_back(8);
    ret = document_index->Search(10, R"(text:"of")", true, 5, 8, true, alive_ids, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 2);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::vector<uint64_t> alive_ids;
    alive_ids.push_back(6);
    alive_ids.push_back(7);
    alive_ids.push_back(8);
    ret = document_index->Search(10, R"(text:"of")", false, 5, 8, true, alive_ids, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 3);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::vector<uint64_t> alive_ids;
    alive_ids.push_back(6);
    alive_ids.push_back(7);
    alive_ids.push_back(8);
    ret = document_index->Search(10, R"(text:"of" AND row_id:IN [6])", true, 5, 8, true, alive_ids, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 1);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::vector<uint64_t> alive_ids;
    alive_ids.push_back(6);
    alive_ids.push_back(7);
    alive_ids.push_back(8);
    ret = document_index->Search(10, R"(text:"of" AND row_id:IN [6 7] AND i64: >= 1006)", true, 5, 8, true, alive_ids,
                                 {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 1);
  }
}

TEST(DingoDocumentIndexTest, test_upsert) {
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

  auto* i64_field = scalar_schema->add_fields();
  i64_field->set_key("i64");
  i64_field->set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
  column_tokenizer_parameter["i64"] = dingodb::TokenizerType::kTokenizerTypeI64;

  auto* f64_field = scalar_schema->add_fields();
  f64_field->set_key("f64");
  f64_field->set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
  column_tokenizer_parameter["f64"] = dingodb::TokenizerType::kTokenizerTypeF64;

  auto* bytes_field = scalar_schema->add_fields();
  bytes_field->set_key("bytes");
  bytes_field->set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
  column_tokenizer_parameter["bytes"] = dingodb::TokenizerType::kTokenizerTypeBytes;

  auto ret1 = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameter(column_tokenizer_parameter, json_parameter,
                                                                       error_message);
  if (!ret1) {
    std::cout << "error_message: " << error_message << '\n';
  }
  ASSERT_TRUE(ret1);

  document_index_parameter.set_json_parameter(json_parameter);

  dingodb::pb::common::RegionEpoch region_epoch;
  dingodb::pb::common::Range range;

  butil::Status tmp_status;
  auto document_index = dingodb::DocumentIndexFactory::LoadOrCreateIndex(1, index_path, document_index_parameter,
                                                                         region_epoch, range, tmp_status);
  std::cout << "status: " << tmp_status.error_code() << ", " << tmp_status.error_str() << '\n';
  ASSERT_TRUE(document_index != nullptr);

  std::vector<dingodb::pb::common::DocumentWithId> document_with_ids;
  std::vector<std::string> texts_to_insert;
  texts_to_insert.push_back("Ancient empires rise and fall, shaping history's course.");                 // 1
  texts_to_insert.push_back("Artistic expressions reflect diverse cultural heritages.");                 // 2
  texts_to_insert.push_back("Social movements transform societies, forging new paths.");                 // 3
  texts_to_insert.push_back("Economies fluctuate, reflecting the complex interplay of global forces.");  // 4 of
  texts_to_insert.push_back("Strategic military campaigns alter the balance of power.");                 // 5 of
  texts_to_insert.push_back("Quantum leaps redefine understanding of physical laws.");                   // 6 of
  texts_to_insert.push_back("Chemical reactions unlock mysteries of nature.");                           // 7 of
  texts_to_insert.push_back("Philosophical debates ponder the essence of existence.");                   // 8 of
  texts_to_insert.push_back("Marriages blend traditions, celebrating love's union.");                    // 9
  texts_to_insert.push_back("Explorers discover uncharted territories, expanding world maps.");          // 10

  for (int i = 0; i < texts_to_insert.size(); i++) {
    dingodb::pb::common::DocumentWithId document_with_id1;
    document_with_id1.set_id(i + 1);
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data(texts_to_insert.at(i));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"text", document_value1});

    dingodb::pb::common::DocumentValue document_value2;
    document_value2.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
    document_value2.mutable_field_value()->set_long_data(1000 + i);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"i64", document_value2});

    dingodb::pb::common::DocumentValue document_value3;
    document_value3.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
    document_value3.mutable_field_value()->set_double_data(1000.0 + i);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"f64", document_value3});

    dingodb::pb::common::DocumentValue document_value4;
    document_value4.set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
    document_value4.mutable_field_value()->set_bytes_data("bytes_data_" + std::to_string(i));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"bytes", document_value4});

    document_with_ids.push_back(document_with_id1);
  }

  auto ret = document_index->Add(document_with_ids, true);
  std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
  EXPECT_EQ(ret.ok(), true);

  // do upsert
  {
    document_with_ids.clear();

    dingodb::pb::common::DocumentWithId document_with_id1;
    document_with_id1.set_id(7);
    dingodb::pb::common::DocumentValue document_value1;
    document_value1.set_field_type(dingodb::pb::common::ScalarFieldType::STRING);
    document_value1.mutable_field_value()->set_string_data("Hello World");
    document_with_id1.mutable_document()->mutable_document_data()->insert({"text", document_value1});

    dingodb::pb::common::DocumentValue document_value2;
    document_value2.set_field_type(dingodb::pb::common::ScalarFieldType::INT64);
    document_value2.mutable_field_value()->set_long_data(1000 + 6);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"i64", document_value2});

    dingodb::pb::common::DocumentValue document_value3;
    document_value3.set_field_type(dingodb::pb::common::ScalarFieldType::DOUBLE);
    document_value3.mutable_field_value()->set_double_data(1000.0 + 6);
    document_with_id1.mutable_document()->mutable_document_data()->insert({"f64", document_value3});

    dingodb::pb::common::DocumentValue document_value4;
    document_value4.set_field_type(dingodb::pb::common::ScalarFieldType::BYTES);
    document_value4.mutable_field_value()->set_bytes_data("bytes_data_" + std::to_string(7));
    document_with_id1.mutable_document()->mutable_document_data()->insert({"bytes", document_value4});

    document_with_ids.push_back(document_with_id1);

    auto ret = document_index->Upsert(document_with_ids, true);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    ret = document_index->Search(5, "discover", false, 0, INT64_MAX, false, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 1);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    ret = document_index->Search(10, "of", false, 0, INT64_MAX, false, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 4);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    ret = document_index->Search(10, R"(text:"of")", true, 5, 8, false, {}, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 2);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::vector<uint64_t> alive_ids;
    alive_ids.push_back(6);
    alive_ids.push_back(7);
    alive_ids.push_back(8);
    ret = document_index->Search(10, R"(text:"of")", true, 5, 8, true, alive_ids, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);

    for (const auto& result : results) {
      std::cout << "row_id: " << result.document_with_id().id() << ", score: " << result.score() << '\n';
    }

    EXPECT_EQ(results.size(), 1);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::vector<uint64_t> alive_ids;
    alive_ids.push_back(6);
    alive_ids.push_back(7);
    alive_ids.push_back(8);
    ret = document_index->Search(10, R"(text:"of")", false, 5, 8, true, alive_ids, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 2);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::vector<uint64_t> alive_ids;
    alive_ids.push_back(6);
    alive_ids.push_back(7);
    alive_ids.push_back(8);
    ret = document_index->Search(10, R"(text:"of" AND row_id:IN [6])", true, 5, 8, true, alive_ids, {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 1);
  }

  {
    std::vector<dingodb::pb::common::DocumentWithScore> results;
    std::vector<uint64_t> alive_ids;
    alive_ids.push_back(6);
    alive_ids.push_back(7);
    alive_ids.push_back(8);
    ret = document_index->Search(10, R"(text:"of" AND row_id:IN [6 7] AND i64: >= 1006)", true, 5, 8, true, alive_ids,
                                 {}, results);
    std::cout << "status: " << ret.error_code() << ", " << ret.error_str() << '\n';
    EXPECT_EQ(ret.ok(), true);
    EXPECT_EQ(results.size(), 0);
  }
}
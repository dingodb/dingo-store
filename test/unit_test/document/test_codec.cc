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

class DingoDocumentCodecTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}
};

#if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP

TEST_F(DingoDocumentCodecTest, GenDefaultTokenizerJsonParameterForVectorIndexWithDocumentSpeedup) {
  std::map<std::string, dingodb::TokenizerType> column_tokenizer_parameter;

  std::string json_parameter;
  std::string error_message;
  bool ret = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      column_tokenizer_parameter, json_parameter, error_message);

  EXPECT_FALSE(ret);

  column_tokenizer_parameter["bool_name"] = dingodb::TokenizerType::kTokenizerTypeBool;
  column_tokenizer_parameter["text_name"] = dingodb::TokenizerType::kTokenizerTypeText;
  column_tokenizer_parameter["i64_name"] = dingodb::TokenizerType::kTokenizerTypeI64;
  column_tokenizer_parameter["f64_name"] = dingodb::TokenizerType::kTokenizerTypeF64;
  column_tokenizer_parameter["bytes_name"] = dingodb::TokenizerType::kTokenizerTypeBytes;
  column_tokenizer_parameter["datetime_name"] = dingodb::TokenizerType::kTokenizerTypeDateTime;

  ret = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      column_tokenizer_parameter, json_parameter, error_message);
  if (!ret) {
    std::cout << "error_message: " << error_message << '\n';
  }
  EXPECT_TRUE(ret);
}

TEST_F(DingoDocumentCodecTest, IsValidTokenizerJsonParameterForVectorIndexWithDocumentSpeedup) {
  std::map<std::string, dingodb::TokenizerType> column_tokenizer_parameter;
  std::string json_parameter;
  std::string error_message;

  // invalid json
  auto ret = dingodb::DocumentCodec::IsValidTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      json_parameter, column_tokenizer_parameter, error_message);
  EXPECT_FALSE(ret);

  // invalid json
  column_tokenizer_parameter["bool_name"] = dingodb::TokenizerType::kTokenizerTypeBool;
  column_tokenizer_parameter["text_name"] = dingodb::TokenizerType::kTokenizerTypeText;
  column_tokenizer_parameter["i64_name"] = dingodb::TokenizerType::kTokenizerTypeI64;
  column_tokenizer_parameter["f64_name"] = dingodb::TokenizerType::kTokenizerTypeF64;
  column_tokenizer_parameter["bytes_name"] = dingodb::TokenizerType::kTokenizerTypeBytes;
  column_tokenizer_parameter["datetime_name"] = dingodb::TokenizerType::kTokenizerTypeDateTime;

  ret = dingodb::DocumentCodec::IsValidTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      json_parameter, column_tokenizer_parameter, error_message);
  EXPECT_FALSE(ret);

  // invalid column

  ret = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      column_tokenizer_parameter, json_parameter, error_message);
  EXPECT_TRUE(ret);

  column_tokenizer_parameter.clear();

  ret = dingodb::DocumentCodec::IsValidTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      json_parameter, column_tokenizer_parameter, error_message);
  EXPECT_TRUE(ret);

  // ok
  json_parameter.clear();
  ret = dingodb::DocumentCodec::GenDefaultTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      column_tokenizer_parameter, json_parameter, error_message);
  EXPECT_TRUE(ret);

  ret = dingodb::DocumentCodec::IsValidTokenizerJsonParameterForVectorIndexWithDocumentSpeedup(
      json_parameter, column_tokenizer_parameter, error_message);
  EXPECT_TRUE(ret);
}

#endif  // #if WITH_VECTOR_INDEX_USE_DOCUMENT_SPEEDUP
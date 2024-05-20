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

#include "document/codec.h"

#include <cstdint>
#include <nlohmann/json.hpp>

#include "butil/compiler_specific.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "nlohmann/json_fwd.hpp"
#include "serial/buf.h"
#include "serial/schema/long_schema.h"
#include "tantivy_search.h"

namespace dingodb {

// TODO: refact
void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, std::string& result) {
  if (BAIDU_UNLIKELY(prefix == 0)) {
    DINGO_LOG(FATAL) << "Encode document key failed, prefix is 0, partition_id:[" << partition_id << "]";
  }

  // Buf buf(17);
  Buf buf(Constant::kDocumentKeyMinLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  buf.GetBytes(result);
}

void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id, std::string& result) {
  if (BAIDU_UNLIKELY(prefix == 0)) {
    // Buf buf(16);
    // Buf buf(Constant::kDocumentKeyMaxLen);
    // buf.WriteLong(partition_id);
    // DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, document_id);
    // buf.GetBytes(result);

    // prefix == 0 is not allowed
    DINGO_LOG(FATAL) << "Encode document key failed, prefix is 0, partition_id:[" << partition_id << "], document_id:["
                     << document_id << "]";
  }

  // Buf buf(17);
  Buf buf(Constant::kDocumentKeyMaxLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, document_id);
  buf.GetBytes(result);
}

void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id,
                                      const std::string& scalar_key, std::string& result) {
  if (BAIDU_UNLIKELY(prefix == 0)) {  // prefix == 0 is not allowed
    DINGO_LOG(FATAL) << "Encode document key failed, prefix is 0, partition_id:[" << partition_id << "], document_id:["
                     << document_id << "]";
  }

  if (BAIDU_UNLIKELY(scalar_key.empty())) {
    DINGO_LOG(FATAL) << "Encode document key failed, scalar_key is empty, prefix:[" << prefix << "], partition_id:["
                     << partition_id << "], document_id:[" << document_id << "]";
  }

  // Buf buf(17 +  scalar_key.size());
  Buf buf(Constant::kDocumentKeyMaxLenWithPrefix + scalar_key.size());
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, document_id);
  buf.Write(scalar_key);
  buf.GetBytes(result);
}

int64_t DocumentCodec::DecodeDocumentId(const std::string& value) {
  Buf buf(value);
  if (value.size() >= Constant::kDocumentKeyMaxLenWithPrefix) {
    buf.Skip(9);
  } else if (value.size() == Constant::kDocumentKeyMinLenWithPrefix) {
    return 0;
  } else {
    DINGO_LOG(FATAL) << "Decode document id failed, value size is not 9 or >=17, value:[" << Helper::StringToHex(value)
                     << "]";
    return 0;
  }

  // return buf.ReadLong();
  return DingoSchema<std::optional<int64_t>>::InternalDecodeKey(&buf);
}

int64_t DocumentCodec::DecodePartitionId(const std::string& value) {
  Buf buf(value);

  // if (value.size() >= 17 || value.size() == 9) {
  if (value.size() >= Constant::kDocumentKeyMaxLenWithPrefix ||
      value.size() == Constant::kDocumentKeyMinLenWithPrefix) {
    buf.Skip(1);
  }

  return buf.ReadLong();
}

std::string DocumentCodec::DecodeScalarKey(const std::string& value) {
  Buf buf(value);
  if (value.size() <= Constant::kDocumentKeyMaxLenWithPrefix) {
    DINGO_LOG(FATAL) << "Decode scalar key failed, value size <=17, value:[" << Helper::StringToHex(value) << "]";
    return "";
  }

  buf.Skip(Constant::kDocumentKeyMaxLenWithPrefix);

  return buf.ReadString();
}

std::string DocumentCodec::DecodeKeyToString(const std::string& key) {
  return fmt::format("{}_{}", DecodePartitionId(key), DecodeDocumentId(key));
}

std::string DocumentCodec::DecodeRangeToString(const pb::common::Range& range) {
  return fmt::format("[{}, {})", DecodeKeyToString(range.start_key()), DecodeKeyToString(range.end_key()));
}

void DocumentCodec::DecodeRangeToDocumentId(const pb::common::Range& range, int64_t& begin_document_id,
                                            int64_t& end_document_id) {
  begin_document_id = DocumentCodec::DecodeDocumentId(range.start_key());
  int64_t temp_end_document_id = DocumentCodec::DecodeDocumentId(range.end_key());
  if (temp_end_document_id > 0) {
    end_document_id = temp_end_document_id;
  } else {
    if (DecodePartitionId(range.end_key()) > DecodePartitionId(range.start_key())) {
      end_document_id = INT64_MAX;
    }
  }
}

bool DocumentCodec::IsValidKey(const std::string& key) {
  // return (key.size() == 8 || key.size() == 9 || key.size() == 16 || key.size() == 17);
  return (key.size() == Constant::kDocumentKeyMinLenWithPrefix || key.size() >= Constant::kDocumentKeyMaxLenWithPrefix);
}

bool DocumentCodec::IsLegalDocumentId(int64_t document_id) { return document_id > 0 && document_id != INT64_MAX; }

bool DocumentCodec::IsValidTokenizerJsonParameter(const std::string& json_parameter,
                                                  std::map<std::string, TokenizerType>& column_tokenizer_parameter,
                                                  std::string& error_message) {
  if (!nlohmann::json::accept(json_parameter)) {
    error_message = "json_parameter is illegal json";
    return false;
  }

  nlohmann::json json = nlohmann::json::parse(json_parameter);
  for (const auto& item : json.items()) {
    auto tokenizer = item.value();

    if (tokenizer.find("tokenizer") == tokenizer.end()) {
      error_message = "not found tokenizer";
      return false;
    }

    const auto& tokenizer_item = tokenizer.at("tokenizer");

    if (tokenizer_item.find("type") == tokenizer_item.end()) {
      error_message = "not found tokenizer type";
      return false;
    }

    const auto& tokenizer_type = tokenizer_item.at("type");

    if (tokenizer_type == "default" || tokenizer_type == "raw" || tokenizer_type == "simple" ||
        tokenizer_type == "stem" || tokenizer_type == "whitespace" || tokenizer_type == "ngram" ||
        tokenizer_type == "chinese") {
      column_tokenizer_parameter[item.key()] = TokenizerType::kTokenizerTypeText;
    } else if (tokenizer_type == "i64") {
      column_tokenizer_parameter[item.key()] = TokenizerType::kTokenizerTypeI64;
    } else if (tokenizer_type == "f64") {
      column_tokenizer_parameter[item.key()] = TokenizerType::kTokenizerTypeF64;
    } else if (tokenizer_type == "bytes") {
      column_tokenizer_parameter[item.key()] = TokenizerType::kTokenizerTypeBytes;
    } else {
      error_message = "unknown column";
      return false;
    }
  }

  // use ffi to validate json_parameter again for more tokenizer parameter
  auto bool_result = ffi_varify_index_parameter(json_parameter);
  if (!bool_result.result) {
    DINGO_LOG(ERROR) << "ffi_varify_index_parameter failed, error_message:[" << bool_result.error_msg.c_str() << "]";
    error_message = bool_result.error_msg.c_str();
    column_tokenizer_parameter.clear();
    return false;
  }

  return true;
}

bool DocumentCodec::GenDefaultTokenizerJsonParameter(
    const std::map<std::string, TokenizerType>& column_tokenizer_parameter, std::string& json_parameter,
    std::string& error_message) {
  if (column_tokenizer_parameter.empty()) {
    error_message = "column_tokenizer_parameter is empty";
    return false;
  }

  nlohmann::json json;

  for (const auto& [field_name, field_type] : column_tokenizer_parameter) {
    nlohmann::json tokenizer;
    switch (field_type) {
      case TokenizerType::kTokenizerTypeText:
        tokenizer["type"] = "chinese";
        break;
      case TokenizerType::kTokenizerTypeI64:
        tokenizer["type"] = "i64";
        break;
      case TokenizerType::kTokenizerTypeF64:
        tokenizer["type"] = "f64";
        break;
      case TokenizerType::kTokenizerTypeBytes:
        tokenizer["type"] = "bytes";
        break;
      default:
        error_message = "unknown column";
        return false;
    }

    json[field_name] = {{"tokenizer", tokenizer}};
  }

  json_parameter = json.dump();

  return true;
}

std::string DocumentCodec::GetTokenizerTypeString(TokenizerType type) {
  switch (type) {
    case TokenizerType::kTokenizerTypeText:
      return "text";
    case TokenizerType::kTokenizerTypeI64:
      return "i64";
    case TokenizerType::kTokenizerTypeF64:
      return "f64";
    case TokenizerType::kTokenizerTypeBytes:
      return "bytes";
    default:
      return "unknown";
  }
}

}  // namespace dingodb
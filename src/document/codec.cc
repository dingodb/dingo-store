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

#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/serial_helper.h"
#include "fmt/core.h"
#include "nlohmann/json_fwd.hpp"
#include "tantivy_search.h"

namespace dingodb {

// TODO: refact
void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, std::string& result) {
  result.resize(Constant::kDocumentKeyMinLenWithPrefix);
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
}

void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id, std::string& result) {
  result.resize(Constant::kDocumentKeyMaxLenWithPrefix);
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
  SerialHelper::WriteLongComparable(document_id, result);
}

void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id, int64_t ts,
                                      std::string& result) {
  result.resize(Constant::kDocumentKeyMaxLenWithPrefix);
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
  SerialHelper::WriteLongComparable(document_id, result);
  SerialHelper::WriteLongWithNegation(ts, result);
}

void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id,
                                      const std::string& scalar_key, std::string& result) {
  if (BAIDU_UNLIKELY(scalar_key.empty())) {
    DINGO_LOG(FATAL) << fmt::format("scalar key is empty, {}/{}/{}", prefix, partition_id, document_id);
  }

  result.resize(Constant::kDocumentKeyMaxLenWithPrefix + scalar_key.size());
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
  SerialHelper::WriteLongComparable(document_id, result);
  result.append(scalar_key);
}

void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id,
                                      const std::string& scalar_key, int64_t ts, std::string& result) {
  if (BAIDU_UNLIKELY(scalar_key.empty())) {
    DINGO_LOG(FATAL) << fmt::format("scalar key is empty, {}/{}/{}", prefix, partition_id, document_id);
  }

  result.resize(Constant::kVectorKeyMaxLenWithPrefix + scalar_key.size());
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
  SerialHelper::WriteLongComparable(document_id, result);
  result.append(scalar_key);
  SerialHelper::WriteLongWithNegation(ts, result);
}

int64_t DocumentCodec::DecodePartitionId(const std::string& key) {
  if (key.size() < Constant::kDocumentKeyMinLenWithPrefix) {
    DINGO_LOG(FATAL) << fmt::format("Decode partition id failed, value({}) size too small", Helper::StringToHex(key));
  }

  return SerialHelper::ReadLong(key.substr(1, 9));
}

int64_t DocumentCodec::DecodeDocumentId(const std::string& key) {
  if (key.size() >= Constant::kDocumentKeyMaxLenWithPrefix) {
    return SerialHelper::ReadLongComparable(
        key.substr(Constant::kDocumentKeyMinLenWithPrefix, Constant::kDocumentKeyMinLenWithPrefix + 8));

  } else if (key.size() == Constant::kDocumentKeyMinLenWithPrefix) {
    return 0;

  } else {
    DINGO_LOG(FATAL) << fmt::format("Decode vector id failed, value({}) size too small", Helper::StringToHex(key));
    return 0;
  }
}

std::string DocumentCodec::DecodeScalarKey(const std::string& key) {
  if (key.size() <= Constant::kDocumentKeyMaxLenWithPrefix) {
    DINGO_LOG(FATAL) << fmt::format("Decode scalar key failed, value({}) size too small.", Helper::StringToHex(key));
    return "";
  }

  return key.substr(Constant::kVectorKeyMaxLenWithPrefix);
}

// key: prefix(1byes)|partition_id(8bytes)|vector_id(8bytes)|ts(8bytes)
std::string_view DocumentCodec::TruncateTsForKey(const std::string& key) {
  CHECK(key.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(key));

  return std::string_view(key).substr(0, key.size() - 8);
}

// key: prefix(1byes)|partition_id(8bytes)|vector_id(8bytes)|ts(8bytes)
std::string_view DocumentCodec::TruncateTsForKey(const std::string_view& key) {
  CHECK(key.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(key));

  return std::string_view(key).substr(0, key.size() - 8);
}

// key: prefix(1byes)|partition_id(8bytes)|vector_id(8bytes)|ts(8bytes)
int64_t DocumentCodec::TruncateKeyForTs(const std::string& key) {
  CHECK(key.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(key));

  auto ts_str = key.substr(key.size() - 8, key.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
}

// key: prefix(1byes)|partition_id(8bytes)|vector_id(8bytes)|ts(8bytes)
int64_t DocumentCodec::TruncateKeyForTs(const std::string_view& key) {
  CHECK(key.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(key));

  auto ts_str = key.substr(key.size() - 8, key.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
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
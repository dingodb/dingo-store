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
#include <string>
#include <utility>

#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/serial_helper.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "mvcc/codec.h"
#include "nlohmann/json_fwd.hpp"
#include "tantivy_search.h"

namespace dingodb {

std::string DocumentCodec::PackageDocumentKey(char prefix, int64_t partition_id) {
  CHECK(prefix != 0) << fmt::format("Invalid prefix {}.", prefix);
  CHECK(partition_id > 0) << fmt::format("Invalid partition_id {}.", partition_id);

  std::string plain_key;
  plain_key.reserve(Constant::kDocumentKeyMinLenWithPrefix);

  plain_key.push_back(prefix);
  SerialHelper::WriteLong(partition_id, plain_key);

  return std::move(plain_key);
}

std::string DocumentCodec::PackageDocumentKey(char prefix, int64_t partition_id, int64_t document_id) {
  CHECK(prefix != 0) << fmt::format("Invalid prefix {}.", prefix);
  CHECK(partition_id > 0) << fmt::format("Invalid partition_id {}.", partition_id);
  CHECK(document_id >= 0) << fmt::format("Invalid document_id {}.", document_id);

  std::string plain_key;
  plain_key.reserve(Constant::kDocumentKeyMaxLenWithPrefix);

  plain_key.push_back(prefix);
  SerialHelper::WriteLong(partition_id, plain_key);
  SerialHelper::WriteLongComparable(document_id, plain_key);

  return std::move(plain_key);
}

std::string DocumentCodec::PackageDocumentKey(char prefix, int64_t partition_id, int64_t document_id,
                                              const std::string& scalar_key) {
  CHECK(prefix != 0) << fmt::format("Invalid prefix {}.", prefix);
  CHECK(partition_id > 0) << fmt::format("Invalid partition_id {}.", partition_id);
  CHECK(document_id >= 0) << fmt::format("Invalid document_id {}.", document_id);
  CHECK(!scalar_key.empty()) << fmt::format("Scalar key is empty, {}/{}/{}.", prefix, partition_id, document_id);

  std::string plain_key;
  plain_key.reserve(Constant::kDocumentKeyMaxLenWithPrefix + scalar_key.size());

  plain_key.push_back(prefix);
  SerialHelper::WriteLong(partition_id, plain_key);
  SerialHelper::WriteLongComparable(document_id, plain_key);
  plain_key.append(scalar_key);

  return std::move(plain_key);
}

int64_t DocumentCodec::UnPackagePartitionId(const std::string& plain_key) {
  CHECK(plain_key.size() >= Constant::kDocumentKeyMinLenWithPrefix)
      << fmt::format("Decode partition id failed, value({}) size too small", Helper::StringToHex(plain_key));

  return SerialHelper::ReadLong(plain_key.substr(1, 9));
}

int64_t DocumentCodec::UnPackageDocumentId(const std::string& plain_key) {
  if (plain_key.size() >= Constant::kDocumentKeyMaxLenWithPrefix) {
    return SerialHelper::ReadLongComparable(plain_key.substr(9, 17));

  } else if (plain_key.size() == Constant::kDocumentKeyMinLenWithPrefix) {
    return 0;

  } else {
    DINGO_LOG(FATAL) << fmt::format("Decode document id failed, value({}) size too small",
                                    Helper::StringToHex(plain_key));
    return 0;
  }
}

std::string DocumentCodec::UnPackageScalarKey(const std::string& plain_key) {
  CHECK(plain_key.size() > Constant::kDocumentKeyMaxLenWithPrefix)
      << fmt::format("Decode scalar key failed, value({}) size too small.", Helper::StringToHex(plain_key));

  return plain_key.substr(Constant::kVectorKeyMaxLenWithPrefix);
}

std::string DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id) {
  std::string plain_key = PackageDocumentKey(prefix, partition_id);

  return mvcc::Codec::EncodeBytes(plain_key);
}

std::string DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id) {
  std::string plain_key = PackageDocumentKey(prefix, partition_id, document_id);

  return mvcc::Codec::EncodeBytes(plain_key);
}

std::string DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id, int64_t ts) {
  std::string plain_key = PackageDocumentKey(prefix, partition_id, document_id);

  return mvcc::Codec::EncodeKey(plain_key, ts);
}

std::string DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id,
                                             const std::string& scalar_key) {
  std::string plain_key = PackageDocumentKey(prefix, partition_id, document_id, scalar_key);

  return mvcc::Codec::EncodeBytes(plain_key);
}

std::string DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id,
                                             const std::string& scalar_key, int64_t ts) {
  std::string plain_key = PackageDocumentKey(prefix, partition_id, document_id, scalar_key);

  return mvcc::Codec::EncodeKey(plain_key, ts);
}

int64_t DocumentCodec::DecodePartitionIdFromEncodeKey(const std::string& encode_key) {
  CHECK((encode_key.size() == 18 || encode_key.size() >= 27)) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode document key{} fail.", Helper::StringToHex(encode_key));

  return UnPackagePartitionId(plain_key);
}

int64_t DocumentCodec::DecodePartitionIdFromEncodeKeyWithTs(const std::string& encode_key_with_ts) {
  CHECK((encode_key_with_ts.size() == 26 || encode_key_with_ts.size() >= 35))
      << "encode_key_with_ts length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode document key{} fail.", Helper::StringToHex(encode_key_with_ts));

  return UnPackagePartitionId(plain_key);
}

int64_t DocumentCodec::DecodeDocumentIdFromEncodeKey(const std::string& encode_key) {
  CHECK((encode_key.size() == 18 || encode_key.size() >= 27)) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode document key{} fail.", Helper::StringToHex(encode_key));

  return UnPackageDocumentId(plain_key);
}

int64_t DocumentCodec::DecodeDocumentIdFromEncodeKeyWithTs(const std::string& encode_key_with_ts) {
  CHECK((encode_key_with_ts.size() == 26 || encode_key_with_ts.size() >= 35))
      << "encode_key_with_ts length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode document key{} fail.", Helper::StringToHex(encode_key_with_ts));

  return UnPackageDocumentId(plain_key);
}

std::string DocumentCodec::DecodeScalarKeyFromEncodeKey(const std::string& encode_key) {
  CHECK((encode_key.size() == 18 || encode_key.size() >= 27)) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode document key{} fail.", Helper::StringToHex(encode_key));

  return UnPackageScalarKey(plain_key);
}

std::string DocumentCodec::DecodeScalarKeyFromEncodeKeyWithTs(const std::string& encode_key_with_ts) {
  CHECK((encode_key_with_ts.size() == 26 || encode_key_with_ts.size() >= 35))
      << "encode_key_with_ts length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode document key{} fail.", Helper::StringToHex(encode_key_with_ts));

  return UnPackageScalarKey(plain_key);
}

void DocumentCodec::DecodeFromEncodeKey(const std::string& encode_key, int64_t& partition_id, int64_t& document_id) {
  CHECK((encode_key.size() == 18 || encode_key.size() >= 27)) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode document key({}) fail.", Helper::StringToHex(encode_key));

  partition_id = UnPackagePartitionId(plain_key);
  document_id = UnPackageDocumentId(plain_key);
}

void DocumentCodec::DecodeFromEncodeKey(const std::string& encode_key, int64_t& partition_id, int64_t& document_id,
                                        std::string& scalar_key) {
  CHECK((encode_key.size() == 18 || encode_key.size() >= 27)) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode document key({}) fail.", Helper::StringToHex(encode_key));

  partition_id = UnPackagePartitionId(plain_key);
  document_id = UnPackageDocumentId(plain_key);
  scalar_key = UnPackageScalarKey(plain_key);
}

void DocumentCodec::DecodeFromEncodeKeyWithTs(const std::string& encode_key_with_ts, int64_t& partition_id,
                                              int64_t& document_id) {
  CHECK((encode_key_with_ts.size() == 26 || encode_key_with_ts.size() >= 35))
      << "encode_key_with_ts length is invalid.";
  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode document key({}) fail.", Helper::StringToHex(encode_key_with_ts));

  partition_id = UnPackagePartitionId(plain_key);
  document_id = UnPackageDocumentId(plain_key);
}

void DocumentCodec::DecodeFromEncodeKeyWithTs(const std::string& encode_key_with_ts, int64_t& partition_id,
                                              int64_t& document_id, std::string& scalar_key) {
  CHECK((encode_key_with_ts.size() == 26 || encode_key_with_ts.size() >= 35))
      << "encode_key_with_ts length is invalid.";
  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode document key({}) fail.", Helper::StringToHex(encode_key_with_ts));

  partition_id = UnPackagePartitionId(plain_key);
  document_id = UnPackageDocumentId(plain_key);
  scalar_key = UnPackageScalarKey(plain_key);
}

std::string_view DocumentCodec::TruncateTsForKey(const std::string& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  return std::string_view(encode_key_with_ts).substr(0, encode_key_with_ts.size() - 8);
}

std::string_view DocumentCodec::TruncateTsForKey(const std::string_view& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  return std::string_view(encode_key_with_ts).substr(0, encode_key_with_ts.size() - 8);
}

int64_t DocumentCodec::TruncateKeyForTs(const std::string& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= 8) << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  auto ts_str = encode_key_with_ts.substr(encode_key_with_ts.size() - 8, encode_key_with_ts.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
}

int64_t DocumentCodec::TruncateKeyForTs(const std::string_view& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= 88) << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  auto ts_str = encode_key_with_ts.substr(encode_key_with_ts.size() - 8, encode_key_with_ts.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
}

std::string DocumentCodec::DebugKey(bool is_encode, const std::string& key) {
  if (is_encode) {
    return fmt::format("{}_{}", DecodePartitionIdFromEncodeKey(key), DecodeDocumentIdFromEncodeKey(key));
  } else {
    return fmt::format("{}_{}", UnPackagePartitionId(key), UnPackageDocumentId(key));
  }
}

std::string DocumentCodec::DebugRange(bool is_encode, const pb::common::Range& range) {
  return fmt::format("[{}, {})", DebugKey(is_encode, range.start_key()), DebugKey(is_encode, range.end_key()));
}

void DocumentCodec::DebugRange(bool is_encode, const pb::common::Range& range, std::string& start_key,
                               std::string& end_key) {
  start_key = DebugKey(is_encode, range.start_key());
  end_key = DebugKey(is_encode, range.start_key());
}

void DocumentCodec::DecodeRangeToDocumentId(bool is_encode, const pb::common::Range& range, int64_t& begin_document_id,
                                            int64_t& end_document_id) {
  if (is_encode) {
    begin_document_id = DocumentCodec::DecodeDocumentIdFromEncodeKey(range.start_key());
    end_document_id = DocumentCodec::DecodeDocumentIdFromEncodeKey(range.end_key());
    if (end_document_id == 0 &&
        (DecodePartitionIdFromEncodeKey(range.end_key()) > DecodePartitionIdFromEncodeKey(range.start_key()))) {
      end_document_id = INT64_MAX;
    }

  } else {
    begin_document_id = DocumentCodec::UnPackageDocumentId(range.start_key());
    end_document_id = DocumentCodec::UnPackageDocumentId(range.end_key());
    if (end_document_id == 0 && (UnPackagePartitionId(range.end_key()) > UnPackagePartitionId(range.start_key()))) {
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
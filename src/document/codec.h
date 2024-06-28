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

#ifndef DINGODB_DOCUMENT_CODEC_H_
#define DINGODB_DOCUMENT_CODEC_H_

#include <cstdint>
#include <string>

#include "proto/common.pb.h"

namespace dingodb {

enum TokenizerType {
  kTokenizerTypeUnknown = 0,
  kTokenizerTypeText = 1,
  kTokenizerTypeI64 = 2,
  kTokenizerTypeF64 = 3,
  kTokenizerTypeBytes = 4,
};

class DocumentCodec {
 public:
  // package document plain key
  static void PackageDocumentKey(char prefix, int64_t partition_id, std::string& plain_key);
  static void PackageDocumentKey(char prefix, int64_t partition_id, int64_t document_id, std::string& plain_key);
  static void PackageDocumentKey(char prefix, int64_t partition_id, int64_t document_id, const std::string& scalar_key,
                                 std::string& plain_key);

  // encode document key
  static void EncodeDocumentKey(char prefix, int64_t partition_id, std::string& encode_key);
  static void EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id, std::string& encode_key);
  static void EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id, int64_t ts,
                                std::string& encode_key);
  static void EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id, const std::string& scalar_key,
                                std::string& encode_key);
  static void EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id, const std::string& scalar_key,
                                int64_t ts, std::string& encode_key);

  static int64_t UnPackagePartitionId(const std::string& plain_key);
  static int64_t DecodePartitionIdFromEncodeKey(const std::string& encode_key);
  static int64_t DecodePartitionIdFromEncodeKeyWithTs(const std::string& encode_key_with_ts);
  static int64_t UnPackageDocumentId(const std::string& plain_key);
  static int64_t DecodeDocumentIdFromEncodeKey(const std::string& encode_key);
  static int64_t DecodeDocumentIdFromEncodeKeyWithTs(const std::string& encode_key_with_ts);
  static std::string UnPackageScalarKey(const std::string& plain_key);
  static std::string DecodeScalarKeyFromEncodeKey(const std::string& encode_key);
  static std::string DecodeScalarKeyFromEncodeKeyWithTs(const std::string& encode_key_with_ts);

  static void DecodeFromEncodeKey(const std::string& encode_key, int64_t& partition_id, int64_t& document_id);
  static void DecodeFromEncodeKey(const std::string& encode_key, int64_t& partition_id, int64_t& document_id,
                                  std::string& scalar_key);
  static void DecodeFromEncodeKeyWithTs(const std::string& encode_key_with_ts, int64_t& partition_id,
                                        int64_t& document_id);
  static void DecodeFromEncodeKeyWithTs(const std::string& encode_key_with_ts, int64_t& partition_id,
                                        int64_t& document_id, std::string& scalar_key);

  static std::string_view TruncateTsForKey(const std::string& encode_key_with_ts);
  static std::string_view TruncateTsForKey(const std::string_view& encode_key_with_ts);

  static int64_t TruncateKeyForTs(const std::string& encode_key_with_ts);
  static int64_t TruncateKeyForTs(const std::string_view& encode_key_with_ts);

  static std::string DebugKey(bool is_encode, const std::string& key);
  static std::string DebugRange(bool is_encode, const pb::common::Range& range);
  static void DebugRange(bool is_encode, const pb::common::Range& range, std::string& start_key, std::string& end_key);

  static void DecodeRangeToDocumentId(bool is_encode, const pb::common::Range& range, int64_t& begin_document_id,
                                      int64_t& end_document_id);

  static bool IsValidKey(const std::string& key);

  static bool IsLegalDocumentId(int64_t document_id);

  static bool IsValidTokenizerJsonParameter(const std::string& json_parameter,
                                            std::map<std::string, TokenizerType>& column_tokenizer_parameter,
                                            std::string& error_message);
  static bool GenDefaultTokenizerJsonParameter(const std::map<std::string, TokenizerType>& column_tokenizer_parameter,
                                               std::string& json_parameter, std::string& error_message);

  static std::string GetTokenizerTypeString(TokenizerType type);
};

}  // namespace dingodb

#endif  // DINGODB_DOCUMENT_CODEC_H_
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

#ifndef DINGODB_VECTOR_CODEC_H_
#define DINGODB_VECTOR_CODEC_H_

#include <cstdint>
#include <string>

#include "proto/common.pb.h"

namespace dingodb {

class VectorCodec {
 public:
  // package vecotr plain key
  static std::string PackageVectorKey(char prefix, int64_t partition_id);
  static std::string PackageVectorKey(char prefix, int64_t partition_id, int64_t vector_id);
  static std::string PackageVectorKey(char prefix, int64_t partition_id, int64_t vector_id,
                                      const std::string& scalar_key);

  // unpackage vector plain key
  static int64_t UnPackagePartitionId(const std::string& plain_key);
  static int64_t UnPackageVectorId(const std::string& plain_key);
  static std::string UnPackageScalarKey(const std::string& plain_key);

  // result is encode key(padding key)
  static std::string EncodeVectorKey(char prefix, int64_t partition_id);
  static std::string EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id);
  static std::string EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, int64_t ts);

  static std::string EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id,
                                     const std::string& scalar_key);
  static std::string EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id,
                                     const std::string& scalar_key, int64_t ts);

  // key is encode key or plain key
  static int64_t DecodePartitionIdFromEncodeKey(const std::string& encode_key);
  static int64_t DecodePartitionIdFromEncodeKeyWithTs(const std::string& encode_key_with_ts);
  static int64_t DecodeVectorIdFromEncodeKey(const std::string& encode_key);
  static int64_t DecodeVectorIdFromEncodeKeyWithTs(const std::string& encode_key_with_ts);
  static std::string DecodeScalarKeyFromEncodeKey(const std::string& encode_key);
  static std::string DecodeScalarKeyFromEncodeKeyWithTs(const std::string& encode_key_with_ts);

  static void DecodeFromEncodeKey(const std::string& encode_key, int64_t& partition_id, int64_t& vector_id);
  static void DecodeFromEncodeKey(const std::string& encode_key, int64_t& partition_id, int64_t& vector_id,
                                  std::string& scalar_key);
  static void DecodeFromEncodeKeyWithTs(const std::string& encode_key_with_ts, int64_t& partition_id,
                                        int64_t& vector_id);
  static void DecodeFromEncodeKeyWithTs(const std::string& encode_key_with_ts, int64_t& partition_id,
                                        int64_t& vector_id, std::string& scalar_key);

  static std::string_view TruncateTsForKey(const std::string& encode_key_with_ts);
  static std::string_view TruncateTsForKey(const std::string_view& encode_key_with_ts);

  static int64_t TruncateKeyForTs(const std::string& encode_key_with_ts);
  static int64_t TruncateKeyForTs(const std::string_view& encode_key_with_ts);

  static std::string DebugKey(bool is_encode, const std::string& key);
  static std::string DebugRange(bool is_encode, const pb::common::Range& range);
  static void DebugRange(bool is_encode, const pb::common::Range& range, std::string& start_key, std::string& end_key);

  static void DecodeRangeToVectorId(bool is_encode, const pb::common::Range& range, int64_t& begin_vector_id,
                                    int64_t& end_vector_id);

  // key is plain key
  static bool IsValidKey(const std::string& key);

  static bool IsLegalVectorId(int64_t vector_id);
};
}  // namespace dingodb

#endif  // DINGODB_VECTOR_CODEC_H_
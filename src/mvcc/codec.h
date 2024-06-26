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

#ifndef DINGODB_MVCC_CODEC_H_
#define DINGODB_MVCC_CODEC_H_

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "proto/common.pb.h"
#include "vector/codec.h"

namespace dingodb {

namespace mvcc {

enum class ValueFlag : uint8_t { kPut = 0, kPutTTL = 1, kDelete = 2 };

class Codec {
 public:
  static std::string ValueFlagDelete() { return std::string(1, static_cast<char>(ValueFlag::kDelete)); }

  // encode user key to comparable bytes
  // e.g.
  // user key: helloworld
  // encode key: hellowor0xFFld0000000xF8
  static std::string EncodeBytes(const std::string& user_key);
  static void EncodeBytes(const std::string& user_key, std::string& output);
  static void EncodeBytes(const std::string_view& user_key, std::string& output);
  // decode encode key to user key
  static bool DecodeBytes(const std::string& encode_key, std::string& output);
  static bool DecodeBytes(const std::string_view& encode_key, std::string& output);

  // encode user key and ts
  static std::string EncodeKey(const std::string& key, int64_t ts);
  static std::string EncodeKey(const std::string_view& key, int64_t ts);
  // decode encode key to user key and ts
  static bool DecodeKey(const std::string& key, std::string& decode_key, int64_t& ts);
  static bool DecodeKey(const std::string_view& key, std::string& decode_key, int64_t& ts);
  // decode encode key to user key
  static bool DecodeKey(const std::string& key, std::string& decode_key);
  static bool DecodeKey(const std::string_view& key, std::string& decode_key);

  // truncate ts from encode key
  // encode key: user_key|ts: 8bytes
  static std::string_view TruncateTsForKey(const std::string& key);
  static std::string_view TruncateTsForKey(const std::string_view& key);

  // truncate key from ts
  // encode key: user_key|ts: 8bytes
  static int64_t TruncateKeyForTs(const std::string& key);
  static int64_t TruncateKeyForTs(const std::string_view& key);

  // package value, append ttl and flag
  // value is input and outpt
  static void PackageValue(ValueFlag flag, std::string& value);
  static void PackageValue(ValueFlag flag, const std::string& value, std::string& output);
  static void PackageValue(ValueFlag flag, int64_t ttl, std::string& value);
  static void PackageValue(ValueFlag flag, int64_t ttl, const std::string& value, std::string& output);

  static void UnPackageValueInPlace(std::string& value);
  static std::string_view UnPackageValue(const std::string& value);
  static std::string_view UnPackageValue(const std::string_view& value);
  static std::string_view UnPackageValue(const std::string_view& value, ValueFlag& flag, int64_t& ttl);

  // Get value flag
  // value: user value|flag
  static ValueFlag GetValueFlag(const std::string& value);
  static ValueFlag GetValueFlag(const std::string_view& value);

  // Get ttl from value
  // value: user value|ttl|flag=kPutTTL
  static int64_t GetValueTTL(const std::string& value);
  static int64_t GetValueTTL(const std::string_view& value);

  // Helper function
  static std::vector<std::string> EncodeKeys(int64_t ts, const std::vector<std::string>& keys);

  static pb::common::KeyValue EncodeKeyValueWithPut(int64_t ts, const pb::common::KeyValue& kv);
  static pb::common::KeyValue EncodeKeyValueWithPutTTL(int64_t ts, int64_t ttl, const pb::common::KeyValue& kv);
  static pb::common::KeyValue EncodeKeyValueWithDelete(int64_t ts, const pb::common::KeyValue& kv);

  static std::vector<pb::common::KeyValue> EncodeKeyValuesWithPut(int64_t ts,
                                                                  const std::vector<pb::common::KeyValue>& kvs);
  static void EncodeKeyValuesWithPut(int64_t ts, std::vector<pb::common::KeyValue>& kvs);

  static std::vector<pb::common::KeyValue> EncodeKeyValuesWithTTL(int64_t ts, int64_t ttl,
                                                                  const std::vector<pb::common::KeyValue>& kvs);
  static void EncodeKeyValuesWithTTL(int64_t ts, int64_t ttl, std::vector<pb::common::KeyValue>& kvs);

  static std::vector<pb::common::KeyValue> EncodeKeyValuesWithDelete(int64_t ts,
                                                                     const std::vector<pb::common::KeyValue>& kvs);
  static void EncodeKeyValuesWithDelete(int64_t ts, std::vector<pb::common::KeyValue>& kvs);

  static pb::common::Range EncodeRange(const pb::common::Range& range);
};

}  // namespace mvcc

}  // namespace dingodb

#endif
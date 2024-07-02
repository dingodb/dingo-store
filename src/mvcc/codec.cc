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

#include "mvcc/codec.h"

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "common/constant.h"
#include "common/helper.h"
#include "common/serial_helper.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "server/service_helper.h"

namespace dingodb {

namespace mvcc {

const int kGroupSize = 8;
const int kPadGroupSize = 9;
const uint8_t kMarker = 255;

const uint32_t kTsLength = 8;
const uint32_t kValidEncodeKeyMinLength = 17;

std::string Codec::EncodeBytes(const std::string& plain_key) {
  std::string output;
  EncodeBytes(std::string_view(plain_key), output);

  return std::move(output);
}

void Codec::EncodeBytes(const std::string& plain_key, std::string& output) {
  EncodeBytes(std::string_view(plain_key), output);
}

void Codec::EncodeBytes(const std::string_view& plain_key, std::string& output) {
  uint32_t new_size = (plain_key.length() / kGroupSize + 1) * kPadGroupSize;
  output.resize(new_size);

  int index = 0;
  const auto* data = plain_key.data();
  char* buf = output.data();
  for (int i = 0; i < plain_key.length(); ++i) {
    if ((i + 1) % kGroupSize != 0) {
      buf[index++] = data[i];
    } else {
      buf[index++] = data[i];
      buf[index++] = '\xff';
    }
  }

  int padding_num = kGroupSize - (plain_key.length() % kGroupSize);
  for (int i = 0; i < padding_num; ++i) {
    buf[index++] = '\x00';
  }
  buf[index] = '\xff' - padding_num;
}

bool Codec::DecodeBytes(const std::string& encode_key, std::string& output) {
  return DecodeBytes(std::string_view(encode_key), output);
}

bool Codec::DecodeBytes(const std::string_view& encode_key, std::string& output) {
  if (encode_key.length() % kPadGroupSize != 0 || encode_key.back() == '\xff') {
    return false;
  }

  uint32_t new_size = (encode_key.length() / kPadGroupSize) * kGroupSize;
  output.clear();
  output.reserve(new_size);

  const auto* data = encode_key.data();
  for (int i = 0; i < encode_key.size(); i++) {
    uint8_t marker = encode_key.at(i + 8);

    int pad_count = kMarker - marker;
    for (int j = 0; j < kGroupSize - pad_count; ++j) {
      output.push_back(encode_key.at(i++));
    }

    if (pad_count != 0) {
      for (int j = 0; j < pad_count; ++j) {
        if (encode_key.at(i++) != 0) {
          return false;
        }
      }

      break;
    }
  }

  return true;
}

std::string Codec::EncodeKey(const std::string& key, int64_t ts) {
  std::string encode_key;
  encode_key.reserve(key.size() + 256);

  EncodeBytes(key, encode_key);
  SerialHelper::WriteLongWithNegation(ts, encode_key);

  return std::move(encode_key);
}

std::string Codec::EncodeKey(const std::string_view& key, int64_t ts) {
  std::string encode_key;
  encode_key.reserve(key.size() + 256);

  EncodeBytes(key, encode_key);
  SerialHelper::WriteLongWithNegation(ts, encode_key);

  return std::move(encode_key);
}

bool Codec::DecodeKey(const std::string& encode_key_with_ts, std::string& plain_key, int64_t& ts) {
  return DecodeKey(std::string_view(encode_key_with_ts), plain_key, ts);
}

bool Codec::DecodeKey(const std::string_view& encode_key_with_ts, std::string& plain_key, int64_t& ts) {
  if (encode_key_with_ts.length() < kValidEncodeKeyMinLength) {
    return false;
  }

  // decode user key
  {
    auto sub_str = encode_key_with_ts.substr(0, encode_key_with_ts.length() - 8);
    if (!DecodeBytes(sub_str, plain_key)) {
      return false;
    }
  }

  // decode ts
  {
    auto sub_str = encode_key_with_ts.substr(encode_key_with_ts.length() - 8);
    ts = SerialHelper::ReadLongWithNegation(sub_str);
  }

  return true;
}

bool Codec::DecodeKey(const std::string& encode_key_with_ts, std::string& plain_key) {
  int64_t ts{0};
  return DecodeKey(encode_key_with_ts, plain_key, ts);
}

bool Codec::DecodeKey(const std::string_view& encode_key_with_ts, std::string& plain_key) {
  int64_t ts{0};
  return DecodeKey(encode_key_with_ts, plain_key, ts);
}

std::string_view Codec::TruncateTsForKey(const std::string& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= kValidEncodeKeyMinLength)
      << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  return std::string_view(encode_key_with_ts).substr(0, encode_key_with_ts.size() - 8);
}

std::string_view Codec::TruncateTsForKey(const std::string_view& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= kValidEncodeKeyMinLength)
      << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  return encode_key_with_ts.substr(0, encode_key_with_ts.size() - 8);
}

int64_t Codec::TruncateKeyForTs(const std::string& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= kValidEncodeKeyMinLength)
      << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  auto ts_str = encode_key_with_ts.substr(encode_key_with_ts.size() - 8, encode_key_with_ts.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
}

int64_t Codec::TruncateKeyForTs(const std::string_view& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= kValidEncodeKeyMinLength)
      << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  auto ts_str = encode_key_with_ts.substr(encode_key_with_ts.size() - 8, encode_key_with_ts.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
}

void Codec::PackageValue(ValueFlag flag, std::string& value) { value.push_back(static_cast<char>(flag)); }

void Codec::PackageValue(ValueFlag flag, const std::string& value, std::string& output) {
  switch (flag) {
    case ValueFlag::kPut:
      output.reserve(value.size() + 8);

      output.resize(value.size());
      memcpy(output.data(), value.data(), value.size());

      output.push_back(static_cast<char>(flag));
      break;

    case ValueFlag::kPutTTL:
      output.reserve(value.size() + 8);

      output.resize(value.size());
      memcpy(output.data(), value.data(), value.size());

      SerialHelper::WriteLong(0, output);

      output.push_back(static_cast<char>(flag));

      break;
    case ValueFlag::kDelete:
      output.push_back(static_cast<char>(flag));
      break;
    default:
      CHECK(false) << "value flag is not support.";
      break;
  }
}

void Codec::PackageValue(ValueFlag flag, int64_t ttl, std::string& value) {
  switch (flag) {
    case ValueFlag::kPut:
      value.push_back(static_cast<char>(flag));
      break;

    case ValueFlag::kPutTTL:
      SerialHelper::WriteLong(ttl, value);

      value.push_back(static_cast<char>(flag));
      break;

    case ValueFlag::kDelete:
      value.clear();
      value.push_back(static_cast<char>(flag));
      break;
    default:
      CHECK(false) << "value flag is not support.";
      break;
  }
}

void Codec::PackageValue(ValueFlag flag, int64_t ttl, const std::string& value, std::string& output) {
  switch (flag) {
    case ValueFlag::kPut:
      output.reserve(value.size() + 12);

      output.resize(value.size());
      memcpy(output.data(), value.data(), value.size());

      output.push_back(static_cast<char>(flag));
      break;
    case ValueFlag::kPutTTL:
      output.reserve(value.size() + 12);

      output.resize(value.size());
      memcpy(output.data(), value.data(), value.size());

      SerialHelper::WriteLong(ttl, output);

      output.push_back(static_cast<char>(flag));
      break;

    case ValueFlag::kDelete:
      output.push_back(static_cast<char>(flag));
      break;
    default:
      CHECK(false) << "value flag is not support.";
      break;
  }
}

void Codec::UnPackageValueInPlace(std::string& pkg_value) {
  switch (pkg_value.back()) {
    case static_cast<char>(ValueFlag::kPut):
      pkg_value.resize(pkg_value.size() - 1);
      break;

    case static_cast<char>(ValueFlag::kPutTTL):
      pkg_value.resize(pkg_value.size() - 9);
      break;

    case static_cast<char>(ValueFlag::kDelete):
      pkg_value.resize(0);
      break;
    default:
      CHECK(false) << "value flag is not support.";
      break;
  }
}

std::string_view Codec::UnPackageValue(const std::string& pkg_value) {
  return UnPackageValue(std::string_view(pkg_value));
}

std::string_view Codec::UnPackageValue(const std::string_view& pkg_value) {
  switch (pkg_value.back()) {
    case static_cast<char>(ValueFlag::kPut):
      return std::string_view(pkg_value.data(), pkg_value.size() - 1);

    case static_cast<char>(ValueFlag::kPutTTL):
      return std::string_view(pkg_value.data(), pkg_value.size() - 9);

    case static_cast<char>(ValueFlag::kDelete):
      return "";

    default:
      CHECK(false) << "value flag is not support.";
      break;
  }
}

std::string_view Codec::UnPackageValue(const std::string_view& pkg_value, ValueFlag& flag, int64_t& ttl) {
  char c = pkg_value.back();
  switch (c) {
    case static_cast<char>(ValueFlag::kPut):
      flag = ValueFlag::kPut;
      ttl = 0;
      return std::string_view(pkg_value.data(), pkg_value.size() - 1);

    case static_cast<char>(ValueFlag::kPutTTL):
      flag = ValueFlag::kPutTTL;

      ttl = SerialHelper::ReadLong(pkg_value.substr(pkg_value.size() - 9, pkg_value.size() - 1));

      return std::string_view(pkg_value.data(), pkg_value.size() - 9);

    case static_cast<char>(ValueFlag::kDelete):
      flag = ValueFlag::kDelete;
      ttl = 0;
      return "";

    default:
      CHECK(false) << "value flag is not support.";
      break;
  }
}

ValueFlag Codec::GetValueFlag(const std::string& pkg_value) {
  CHECK(!pkg_value.empty()) << "Value is empty.";

  uint8_t flag = static_cast<uint8_t>(pkg_value.back());
  CHECK(flag <= static_cast<uint8_t>(ValueFlag::kDelete)) << fmt::format("Value flag({}) is invalid.", flag);

  return static_cast<ValueFlag>(flag);
}

ValueFlag Codec::GetValueFlag(const std::string_view& pkg_value) {
  CHECK(!pkg_value.empty()) << "Value is empty.";

  uint8_t flag = static_cast<uint8_t>(pkg_value.back());
  CHECK(flag <= static_cast<uint8_t>(ValueFlag::kDelete)) << fmt::format("Value flag({}) is invalid.", flag);

  return static_cast<ValueFlag>(flag);
}

int64_t Codec::GetValueTTL(const std::string& pkg_value) {
  CHECK(pkg_value.size() > 9) << "Value length is invalid.";

  uint8_t flag = static_cast<uint8_t>(pkg_value.back());
  CHECK(flag == static_cast<uint8_t>(ValueFlag::kPutTTL)) << fmt::format("Value flag({}) is not kPutTTL.", flag);

  auto ttl_str = pkg_value.substr(pkg_value.size() - 9, pkg_value.size() - 1);
  return Helper::StringToInt64(ttl_str);
}

int64_t Codec::GetValueTTL(const std::string_view& pkg_value) {
  CHECK(pkg_value.size() > 9) << "Value length is invalid.";

  uint8_t flag = static_cast<uint8_t>(pkg_value.back());
  CHECK(flag == static_cast<uint8_t>(ValueFlag::kPutTTL)) << fmt::format("Value flag({}) is not kPutTTL.", flag);

  auto ttl_str = pkg_value.substr(pkg_value.size() - 9, pkg_value.size() - 1);
  return SerialHelper::ReadLong(ttl_str);
}

std::vector<std::string> Codec::EncodeKeys(int64_t ts, const std::vector<std::string>& plain_keys) {
  std::vector<std::string> encode_keys;
  encode_keys.reserve(plain_keys.size());

  for (const auto& key : plain_keys) {
    encode_keys.push_back(EncodeKey(key, ts));
  }

  return std::move(encode_keys);
}

pb::common::KeyValue Codec::EncodeKeyValueWithPut(int64_t ts, const pb::common::KeyValue& plain_kv) {
  pb::common::KeyValue encode_kv;
  *encode_kv.mutable_key() = EncodeKey(plain_kv.key(), ts);
  PackageValue(ValueFlag::kPut, plain_kv.value(), *encode_kv.mutable_value());
  return encode_kv;
}

pb::common::KeyValue Codec::EncodeKeyValueWithPutTTL(int64_t ts, int64_t ttl, const pb::common::KeyValue& plain_kv) {
  pb::common::KeyValue encode_kv;
  *encode_kv.mutable_key() = EncodeKey(plain_kv.key(), ts);
  PackageValue(ValueFlag::kPutTTL, ttl, plain_kv.value(), *encode_kv.mutable_value());
  return encode_kv;
}

pb::common::KeyValue Codec::EncodeKeyValueWithDelete(int64_t ts, const pb::common::KeyValue& plain_kv) {
  pb::common::KeyValue encode_kv;
  *encode_kv.mutable_key() = EncodeKey(plain_kv.key(), ts);
  encode_kv.mutable_value()->push_back(static_cast<char>(ValueFlag::kDelete));
  return encode_kv;
}

std::vector<pb::common::KeyValue> Codec::EncodeKeyValuesWithPut(int64_t ts,
                                                                const std::vector<pb::common::KeyValue>& plain_kvs) {
  std::vector<pb::common::KeyValue> encode_kvs;

  for (const auto& kv : plain_kvs) {
    pb::common::KeyValue encode_kv;
    *encode_kv.mutable_key() = EncodeKey(kv.key(), ts);
    PackageValue(ValueFlag::kPut, kv.value(), *encode_kv.mutable_value());

    encode_kvs.push_back(std::move(encode_kv));
  }

  return std::move(encode_kvs);
}

void Codec::EncodeKeyValuesWithPut(int64_t ts, std::vector<pb::common::KeyValue>& kvs) {
  for (auto& kv : kvs) {
    kv.set_key(EncodeKey(kv.key(), ts));
    PackageValue(ValueFlag::kPut, *kv.mutable_value());
  }
}

std::vector<pb::common::KeyValue> Codec::EncodeKeyValuesWithTTL(int64_t ts, int64_t ttl,
                                                                const std::vector<pb::common::KeyValue>& plain_kvs) {
  std::vector<pb::common::KeyValue> encode_kvs;

  for (const auto& kv : plain_kvs) {
    pb::common::KeyValue encode_kv;
    *encode_kv.mutable_key() = EncodeKey(kv.key(), ts);
    PackageValue(ValueFlag::kPutTTL, ttl, kv.value(), *encode_kv.mutable_value());

    encode_kvs.push_back(std::move(encode_kv));
  }

  return std::move(encode_kvs);
}

void Codec::EncodeKeyValuesWithTTL(int64_t ts, int64_t ttl, std::vector<pb::common::KeyValue>& kvs) {
  for (auto& kv : kvs) {
    kv.set_key(EncodeKey(kv.key(), ts));
    PackageValue(ValueFlag::kPutTTL, ttl, *kv.mutable_value());
  }
}

std::vector<pb::common::KeyValue> Codec::EncodeKeyValuesWithDelete(int64_t ts,
                                                                   const std::vector<pb::common::KeyValue>& plain_kvs) {
  std::vector<pb::common::KeyValue> encode_kvs;

  for (const auto& kv : plain_kvs) {
    pb::common::KeyValue encode_kv;
    *encode_kv.mutable_key() = EncodeKey(kv.key(), ts);
    encode_kv.mutable_value()->push_back(static_cast<char>(ValueFlag::kDelete));

    encode_kvs.push_back(std::move(encode_kv));
  }

  return std::move(encode_kvs);
}

void Codec::EncodeKeyValuesWithDelete(int64_t ts, std::vector<pb::common::KeyValue>& kvs) {
  for (auto& kv : kvs) {
    kv.set_key(EncodeKey(kv.key(), ts));
    kv.mutable_value()->clear();
    kv.mutable_value()->push_back(static_cast<char>(ValueFlag::kDelete));
  }
}

pb::common::Range Codec::EncodeRange(const pb::common::Range& plain_range) {
  pb::common::Range encode_range;
  encode_range.set_start_key(EncodeBytes(plain_range.start_key()));
  encode_range.set_end_key(EncodeBytes(plain_range.end_key()));
  return std::move(encode_range);
}

pb::common::Range Codec::EncodeRange(const std::string& plain_start_key, const std::string& plain_end_key) {
  pb::common::Range encode_range;
  encode_range.set_start_key(EncodeBytes(plain_start_key));
  encode_range.set_end_key(EncodeBytes(plain_end_key));
  return std::move(encode_range);
}

pb::common::Range Codec::DecodeRange(const pb::common::Range& encode_range) {
  pb::common::Range plain_range;

  std::string start_key;
  bool ret = DecodeBytes(encode_range.start_key(), start_key);
  CHECK(ret) << fmt::format("decode range error, range {}", Helper::RangeToString(encode_range));
  plain_range.set_start_key(start_key);

  std::string end_key;
  ret = DecodeBytes(encode_range.end_key(), end_key);
  CHECK(ret) << fmt::format("decode range error, range {}", Helper::RangeToString(encode_range));
  plain_range.set_end_key(end_key);

  return plain_range;
}

}  // namespace mvcc

}  // namespace dingodb

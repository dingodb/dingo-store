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

#include "common/serial_helper.h"

#include "butil/compiler_specific.h"

namespace dingodb {

// memory comparable is from low addr to high addr, so must use big endian
// little endian int32/int64 must transform to big endian(high num locate low addr)
// e.g. number: 1234567(Ox12d687)     <      2234500(0x221884)
// addr:          0     1     2             0     1     2
// little endian: 0x87  0xd6  0x12    >     0x84  0x18  0x22      compare wrong

// big endian:    0x12  0xd6  0x87    <     0x22  0x18  0x84      compare right

void SerialHelper::WriteLong(int64_t value, std::string& output) {
  if (BAIDU_LIKELY(IsLE())) {
    // value is little endian
    output.push_back(static_cast<char>(value >> 56));
    output.push_back(static_cast<char>(value >> 48));
    output.push_back(static_cast<char>(value >> 40));
    output.push_back(static_cast<char>(value >> 32));
    output.push_back(static_cast<char>(value >> 24));
    output.push_back(static_cast<char>(value >> 16));
    output.push_back(static_cast<char>(value >> 8));
    output.push_back(static_cast<char>(value));
  } else {
    // value is big endian
    output.push_back(static_cast<char>(value));
    output.push_back(static_cast<char>(value >> 8));
    output.push_back(static_cast<char>(value >> 16));
    output.push_back(static_cast<char>(value >> 24));
    output.push_back(static_cast<char>(value >> 32));
    output.push_back(static_cast<char>(value >> 40));
    output.push_back(static_cast<char>(value >> 48));
    output.push_back(static_cast<char>(value >> 56));
  }
}

int64_t SerialHelper::ReadLong(const std::string_view& output) {
  uint64_t l = 0;
  l |= (output.at(0) & 0xFF);
  l <<= 8;
  l |= (output.at(1) & 0xFF);
  l <<= 8;
  l |= (output.at(2) & 0xFF);
  l <<= 8;
  l |= (output.at(3) & 0xFF);
  l <<= 8;
  l |= (output.at(4) & 0xFF);
  l <<= 8;
  l |= (output.at(5) & 0xFF);
  l <<= 8;
  l |= (output.at(6) & 0xFF);
  l <<= 8;
  l |= (output.at(7) & 0xFF);

  return static_cast<int64_t>(l);
}

void SerialHelper::WriteLongWithNegation(int64_t value, std::string& output) {
  int64_t nvalue = ~value;
  if (BAIDU_LIKELY(IsLE())) {
    // value is little endian
    output.push_back(static_cast<char>(nvalue >> 56));
    output.push_back(static_cast<char>(nvalue >> 48));
    output.push_back(static_cast<char>(nvalue >> 40));
    output.push_back(static_cast<char>(nvalue >> 32));
    output.push_back(static_cast<char>(nvalue >> 24));
    output.push_back(static_cast<char>(nvalue >> 16));
    output.push_back(static_cast<char>(nvalue >> 8));
    output.push_back(static_cast<char>(nvalue));
  } else {
    // value is big endian
    output.push_back(static_cast<char>(nvalue));
    output.push_back(static_cast<char>(nvalue >> 8));
    output.push_back(static_cast<char>(nvalue >> 16));
    output.push_back(static_cast<char>(nvalue >> 24));
    output.push_back(static_cast<char>(nvalue >> 32));
    output.push_back(static_cast<char>(nvalue >> 40));
    output.push_back(static_cast<char>(nvalue >> 48));
    output.push_back(static_cast<char>(nvalue >> 56));
  }
}

int64_t SerialHelper::ReadLongWithNegation(const std::string_view& output) { return ~ReadLong(output); }

void SerialHelper::WriteLongComparable(int64_t data, std::string& output) {
  uint64_t* l = (uint64_t*)&data;
  if (BAIDU_LIKELY(IsLE())) {
    // value is little endian
    output.push_back(static_cast<char>(*l >> 56 ^ 0x80));
    output.push_back(static_cast<char>(*l >> 48));
    output.push_back(static_cast<char>(*l >> 40));
    output.push_back(static_cast<char>(*l >> 32));
    output.push_back(static_cast<char>(*l >> 24));
    output.push_back(static_cast<char>(*l >> 16));
    output.push_back(static_cast<char>(*l >> 8));
    output.push_back(static_cast<char>(*l));

  } else {
    // value is big endian
    output.push_back(static_cast<char>(*l ^ 0x80));
    output.push_back(static_cast<char>(*l >> 8));
    output.push_back(static_cast<char>(*l >> 16));
    output.push_back(static_cast<char>(*l >> 24));
    output.push_back(static_cast<char>(*l >> 32));
    output.push_back(static_cast<char>(*l >> 40));
    output.push_back(static_cast<char>(*l >> 48));
    output.push_back(static_cast<char>(*l >> 56));
  }
}

int64_t SerialHelper::ReadLongComparable(const std::string& output) {
  uint64_t l = output.at(0) & 0xFF ^ 0x80;
  if (IsLE()) {
    for (int i = 1; i < 8; ++i) {
      l <<= 8;
      l |= output.at(i) & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; ++i) {
      l |= (((uint64_t)output.at(i) & 0xFF) << (8 * i));
    }
  }

  return static_cast<int64_t>(l);
}

int64_t SerialHelper::ReadLongComparable(const std::string_view& output) {
  uint64_t l = output.at(0) & 0xFF ^ 0x80;
  if (IsLE()) {
    for (int i = 1; i < 8; ++i) {
      l <<= 8;
      l |= output.at(i) & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; ++i) {
      l |= (((uint64_t)output.at(i) & 0xFF) << (8 * i));
    }
  }

  return static_cast<int64_t>(l);
}

}  // namespace dingodb

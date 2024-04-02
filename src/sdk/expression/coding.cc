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

#include "sdk/expression/coding.h"

#include <cstddef>
#include <cstring>
#include <vector>

#include "sdk/expression/encodes.h"

namespace dingodb {
namespace sdk {
namespace expression {

void EncodeFloat(float value, std::string* dst) {
  std::uint32_t bits;
  std::memcpy(&bits, &value, sizeof(float));
  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 24));
  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 16));
  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 8));
  dst->append(sizeof(Byte), static_cast<Byte>(bits));
}

void EncodeDouble(double value, std::string* dst) {
  std::uint64_t bits;
  std::memcpy(&bits, &value, sizeof(double));
  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 56));
  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 48));

  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 40));
  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 32));
  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 24));
  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 16));
  dst->append(sizeof(Byte), static_cast<Byte>(bits >> 8));
  dst->append(sizeof(Byte), static_cast<Byte>(bits));
}

void EncodeString(const std::string& value, std::string* dst) {
  uint32_t len = value.size();
  EncodeVarint(len, dst);
  dst->append(value.data(), len);
}

std::string BytesToHexString(const std::string& bytes) {
  const char* hex_code = "0123456789ABCDEF";
  std::string r;
  r.reserve(bytes.length() * 2);
  for (Byte b : bytes) {
    r.push_back(hex_code[(b >> 4) & 0xF]);
    r.push_back(hex_code[b & 0xF]);
  }
  return r;
}

std::string HexStringToBytes(const std::string& hex) {
  std::string bytes;

  for (unsigned int i = 0; i < hex.length(); i += 2) {
    std::string byte_string = hex.substr(i, 2);
    Byte byte = static_cast<Byte>(std::stoi(byte_string, nullptr, 16));
    bytes.push_back(byte);
  }
  return bytes;
}

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb

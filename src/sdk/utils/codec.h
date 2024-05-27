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

#ifndef DINGODB_SDK_CODEC_H_
#define DINGODB_SDK_CODEC_H_

#include <string>

namespace dingodb {
namespace sdk {

namespace codec {

static std::string BytesToHexString(const std::string& bytes) {
  using Byte = unsigned char;
  const char* hex_code = "0123456789ABCDEF";
  std::string r;
  r.reserve(bytes.length() * 2);
  for (Byte b : bytes) {
    r.push_back(hex_code[(b >> 4) & 0xF]);
    r.push_back(hex_code[b & 0xF]);
  }
  return r;
}

static std::string HexStringToBytes(const std::string& hex) {
  using Byte = unsigned char;
  std::string bytes;

  for (unsigned int i = 0; i < hex.length(); i += 2) {
    std::string byte_string = hex.substr(i, 2);
    Byte byte = static_cast<Byte>(std::stoi(byte_string, nullptr, 16));
    bytes.push_back(byte);
  }
  return bytes;
}
};  // namespace codec
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_CODEC_H_
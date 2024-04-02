
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

#ifndef DINGODB_SDK_EXPRESSION_CODING_H_
#define DINGODB_SDK_EXPRESSION_CODING_H_

#include <cstdint>
#include <string>
#include <vector>

#include "sdk/expression/encodes.h"

namespace dingodb {
namespace sdk {
namespace expression {

template <typename T>
void EncodeVarint(T value, std::string* dst) {
  while (value >= 0x80) {
    dst->append(sizeof(Byte), static_cast<Byte>((value & 0x7F) | 0x80));
    value >>= 7;
  }
  dst->append(sizeof(Byte), static_cast<Byte>(value));
}

void EncodeFloat(float value, std::string* dst);

void EncodeDouble(double value, std::string* dst);

void EncodeString(const std::string& value, std::string* dst);

std::string BytesToHexString(const std::string& bytes);

std::string HexStringToBytes(const std::string& hex);

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_EXPRESSION_CODING_H_
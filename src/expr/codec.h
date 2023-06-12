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

#ifndef DINGODB_EXPR_CODEC_H_
#define DINGODB_EXPR_CODEC_H_

#include "types.h"

namespace dingodb::expr {

// T can be int32_t or int64_t
template <typename T>
const byte *DecodeVarint(T &value, const byte *data) {
  value = 0;
  const byte *p;
  int shift = 0;
  for (p = data; ((*p) & 0x80) != 0; ++p) {
    value |= ((T)(*p & 0x7F) << shift);
    shift += 7;
  }
  value |= ((T)(*p) << shift);
  return p;
}

float DecodeFloat(const byte *data);

double DecodeDouble(const byte *data);

int HexToInt(const char hex);

void HexToBytes(byte *buf, const char *hex, size_t len);

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_CODEC_H_

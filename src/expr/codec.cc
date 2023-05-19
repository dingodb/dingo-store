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

#ifdef __APPLE__
#include <netinet/in.h>
#define be32toh(x) ntohl(x)
#define be64toh(x) ntohll(x)
#else
#include <endian.h>
#endif

#include "codec.h"

namespace dingodb::expr {

float DecodeFloat(const byte *data) {
  uint32_t l = be32toh(*(uint32_t *)data);
  return *(float *)&l;
}

double DecodeDouble(const byte *data) {
  uint64_t l = be64toh(*(uint64_t *)data);
  return *(double *)&l;
}

int HexToInt(const char hex) {
  if ('0' <= hex && hex <= '9') {
    return hex - '0';
  } else if ('a' <= hex && hex <= 'f') {
    return hex - 'a' + 10;
  } else if ('A' <= hex && hex <= 'F') {
    return hex - 'A' + 10;
  }
  return -1;
}

void HexToBytes(byte *buf, const char *hex, size_t len) {
  for (int i = 0; i < len / 2; ++i) {
    buf[i] = (byte)(HexToInt(hex[i + i]) << 4) | HexToInt(hex[i + i + 1]);
  }
}

}  // namespace dingodb::expr

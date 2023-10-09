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

const byte *DecodeFloat(float &value, const byte *data) {
  uint32_t l = be32toh(*(uint32_t *)data);
  value = *(float *)&l;
  return data + 4;
}

const byte *DecodeDouble(double &value, const byte *data) {
  int64_t l = be64toh(*(int64_t *)data);
  value = *(double *)&l;
  return data + 8;
}

const byte *DecodeString(std::shared_ptr<std::string> &value, const byte *data) {
  uint32_t len;
  const byte *p = DecodeVarint(len, data);
  value = std::make_shared<std::string>(reinterpret_cast<const char *>(p), len);
  return p + len;
}

int HexToNibble(const char hex) {
  if ('0' <= hex && hex <= '9') {
    return hex - '0';
  } else if ('a' <= hex && hex <= 'f') {
    return hex - 'a' + 10;
  } else if ('A' <= hex && hex <= 'F') {
    return hex - 'A' + 10;
  }
  return -1;
}

char NibbleToHex(int nibble) {
  if (0 <= nibble && nibble <= 9) {
    return nibble + '0';
  } else if (10 <= nibble && nibble <= 15) {
    return nibble - 10 + 'A';
  }
  return (char)-1;
}

void HexToBytes(byte *buf, const char *hex, size_t len) {
  for (size_t i = 0; i < len / 2; ++i) {
    buf[i] = (byte)(HexToNibble(hex[i + i]) << 4) | HexToNibble(hex[i + i + 1]);
  }
}

void BytesToHex(char *hex, const byte *buf, size_t len) {
  char *p = hex;
  for (size_t i = 0; i < len; ++i) {
    *p = NibbleToHex(buf[i] >> 4);
    ++p;
    *p = NibbleToHex(buf[i] & 0x0F);
    ++p;
  }
}

}  // namespace dingodb::expr

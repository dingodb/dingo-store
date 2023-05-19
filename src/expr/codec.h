#ifndef DINGODB_EXPR_CODEC_H_
#define DINGODB_EXPR_CODEC_H_

#ifdef __APPLE__
#include <netinet/in.h>
#define be64toh(x) ntohll(x)
#define be32toh(x) ntohl(x)
#else
#include <endian.h>
#endif

#include <cctype>

#include "types.h"

namespace dingodb::expr {

template <typename T>
const byte *DecodeVarint(T &value, const byte *data) {
  value = 0;
  const byte *p;
  int shift = 0;
  for (p = data; ((*p) & 0x80) != 0; ++p) {
    value |= ((*p & 0x7F) << shift);
    shift += 7;
  }
  value |= (*p << shift);
  return p;
}

const float DecodeFloat(const byte *data) {
  uint32_t l = be32toh(*(uint32_t *)data);
  return *(float *)&l;
}

const double DecodeDouble(const byte *data) {
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

#endif  // DINGODB_EXPR_CODEC_H_

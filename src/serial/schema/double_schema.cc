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

#include "double_schema.h"

namespace dingodb {

int DingoSchema<std::optional<double>>::GetDataLength() { return 8; }

int DingoSchema<std::optional<double>>::GetWithNullTagLength() { return 9; }

void DingoSchema<std::optional<double>>::InternalEncodeNull(Buf* buf) {
  buf->Write(0);
  buf->Write(0);
  buf->Write(0);
  buf->Write(0);
  buf->Write(0);
  buf->Write(0);
  buf->Write(0);
  buf->Write(0);
}

void DingoSchema<std::optional<double>>::LeInternalEncodeKey(Buf* buf, double data) {
  uint64_t bits;
  memcpy(&bits, &data, 8);
  if (data >= 0) {
    buf->Write(bits >> 56 ^ 0x80);
    buf->Write(bits >> 48);
    buf->Write(bits >> 40);
    buf->Write(bits >> 32);
    buf->Write(bits >> 24);
    buf->Write(bits >> 16);
    buf->Write(bits >> 8);
    buf->Write(bits);
  } else {
    buf->Write(~bits >> 56);
    buf->Write(~bits >> 48);
    buf->Write(~bits >> 40);
    buf->Write(~bits >> 32);
    buf->Write(~bits >> 24);
    buf->Write(~bits >> 16);
    buf->Write(~bits >> 8);
    buf->Write(~bits);
  }
}

void DingoSchema<std::optional<double>>::BeInternalEncodeKey(Buf* buf, double data) {
  uint64_t bits;
  memcpy(&bits, &data, 8);
  if (data >= 0) {
    buf->Write(bits ^ 0x80);
    buf->Write(bits >> 8);
    buf->Write(bits >> 16);
    buf->Write(bits >> 24);
    buf->Write(bits >> 32);
    buf->Write(bits >> 40);
    buf->Write(bits >> 48);
    buf->Write(bits >> 56);
  } else {
    buf->Write(~bits);
    buf->Write(~bits >> 8);
    buf->Write(~bits >> 16);
    buf->Write(~bits >> 24);
    buf->Write(~bits >> 32);
    buf->Write(~bits >> 40);
    buf->Write(~bits >> 48);
    buf->Write(~bits >> 56);
  }
}

void DingoSchema<std::optional<double>>::LeInternalEncodeValue(Buf* buf, double data) {
  uint64_t bits;
  memcpy(&bits, &data, 8);
  buf->Write(bits >> 56);
  buf->Write(bits >> 48);
  buf->Write(bits >> 40);
  buf->Write(bits >> 32);
  buf->Write(bits >> 24);
  buf->Write(bits >> 16);
  buf->Write(bits >> 8);
  buf->Write(bits);
}

void DingoSchema<std::optional<double>>::BeInternalEncodeValue(Buf* buf, double data) {
  uint64_t bits;
  memcpy(&bits, &data, 8);
  buf->Write(bits);
  buf->Write(bits >> 8);
  buf->Write(bits >> 16);
  buf->Write(bits >> 24);
  buf->Write(bits >> 32);
  buf->Write(bits >> 40);
  buf->Write(bits >> 48);
  buf->Write(bits >> 56);
}

BaseSchema::Type DingoSchema<std::optional<double>>::GetType() { return kDouble; }

void DingoSchema<std::optional<double>>::SetIndex(int index) { this->index_ = index; }

int DingoSchema<std::optional<double>>::GetIndex() { return this->index_; }

void DingoSchema<std::optional<double>>::SetIsKey(bool key) { this->key_ = key; }

bool DingoSchema<std::optional<double>>::IsKey() { return this->key_; }

int DingoSchema<std::optional<double>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<double>>::SetAllowNull(bool allow_null) { this->allow_null_ = allow_null; }

bool DingoSchema<std::optional<double>>::AllowNull() { return allow_null_; }

void DingoSchema<std::optional<double>>::SetIsLe(bool le) { this->le_ = le; }

void DingoSchema<std::optional<double>>::EncodeKey(Buf* buf, std::optional<double> data) {
  if (this->allow_null_) {
    buf->EnsureRemainder(GetWithNullTagLength());
    if (data.has_value()) {
      buf->Write(k_not_null);
      if (this->le_) {
        LeInternalEncodeKey(buf, data.value());
      } else {
        BeInternalEncodeKey(buf, data.value());
      }
    } else {
      buf->Write(k_null);
      InternalEncodeNull(buf);
    }
  } else {
    if (data.has_value()) {
      buf->EnsureRemainder(GetDataLength());
      if (this->le_) {
        LeInternalEncodeKey(buf, data.value());
      } else {
        BeInternalEncodeKey(buf, data.value());
      }
    } else {
      // WRONG EMPTY DATA
    }
  }
}

void DingoSchema<std::optional<double>>::EncodeKeyPrefix(Buf* buf, std::optional<double> data) { EncodeKey(buf, data); }
std::optional<double> DingoSchema<std::optional<double>>::DecodeKey(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      buf->Skip(GetDataLength());
      return std::nullopt;
    }
  }
  uint64_t l = buf->Read() & 0xFF;
  if (this->le_) {
    if (l >= 0x80) {
      l = l ^ 0x80;
      for (int i = 0; i < 7; i++) {
        l <<= 8;
        l |= buf->Read() & 0xFF;
      }
    } else {
      l = ~l;
      for (int i = 0; i < 7; i++) {
        l <<= 8;
        l |= ~buf->Read() & 0xFF;
      }
    }
  } else {
    if (l >= 0x80) {
      l = l ^ 0x80;
      for (int i = 1; i < 8; i++) {
        l |= (((uint64_t)buf->Read() & 0xFF) << (8 * i));
      }
    } else {
      for (int i = 1; i < 8; i++) {
        l |= (((uint64_t)buf->Read() & 0xFF) << (8 * i));
      }
      l = ~l;
    }
  }
  double d;
  memcpy(&d, &l, 8);
  return d;
}

void DingoSchema<std::optional<double>>::SkipKey(Buf* buf) { buf->Skip(GetLength()); }

void DingoSchema<std::optional<double>>::EncodeValue(Buf* buf, std::optional<double> data) {
  if (this->allow_null_) {
    buf->EnsureRemainder(GetWithNullTagLength());
    if (data.has_value()) {
      buf->Write(k_not_null);
      if (this->le_) {
        LeInternalEncodeValue(buf, data.value());
      } else {
        BeInternalEncodeValue(buf, data.value());
      }
    } else {
      buf->Write(k_null);
      InternalEncodeNull(buf);
    }
  } else {
    if (data.has_value()) {
      buf->EnsureRemainder(GetDataLength());
      if (this->le_) {
        LeInternalEncodeValue(buf, data.value());
      } else {
        BeInternalEncodeValue(buf, data.value());
      }
    } else {
      // WRONG EMPTY DATA
    }
  }
}

std::optional<double> DingoSchema<std::optional<double>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      buf->Skip(GetDataLength());
      return std::nullopt;
    }
  }
  uint64_t l = buf->Read() & 0xFF;
  ;
  if (this->le_) {
    for (int i = 0; i < 7; i++) {
      l <<= 8;
      l |= buf->Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; i++) {
      l |= (((uint64_t)buf->Read() & 0xFF) << (8 * i));
    }
  }
  double d;
  memcpy(&d, &l, 8);
  return d;
}

void DingoSchema<std::optional<double>>::SkipValue(Buf* buf) { buf->Skip(GetLength()); }

}  // namespace dingodb
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

#include "float_schema.h"

namespace dingodb {

int DingoSchema<std::optional<float>>::GetDataLength() { return 4; }

int DingoSchema<std::optional<float>>::GetWithNullTagLength() { return 5; }

void DingoSchema<std::optional<float>>::InternalEncodeNull(Buf* buf) {
  buf->Write(0);
  buf->Write(0);
  buf->Write(0);
  buf->Write(0);
}

void DingoSchema<std::optional<float>>::LeInternalEncodeKey(Buf* buf, float data) {
  uint32_t bits;
  memcpy(&bits, &data, 4);
  if (data >= 0) {
    buf->Write(bits >> 24 ^ 0x80);
    buf->Write(bits >> 16);
    buf->Write(bits >> 8);
    buf->Write(bits);
  } else {
    buf->Write(~bits >> 24);
    buf->Write(~bits >> 16);
    buf->Write(~bits >> 8);
    buf->Write(~bits);
  }
}

void DingoSchema<std::optional<float>>::BeInternalEncodeKey(Buf* buf, float data) {
  uint32_t bits;
  memcpy(&bits, &data, 4);
  if (data >= 0) {
    buf->Write(bits ^ 0x80);
    buf->Write(bits >> 8);
    buf->Write(bits >> 16);
    buf->Write(bits >> 24);
  } else {
    buf->Write(~bits);
    buf->Write(~bits >> 8);
    buf->Write(~bits >> 16);
    buf->Write(~bits >> 24);
  }
}

void DingoSchema<std::optional<float>>::LeInternalEncodeValue(Buf* buf, float data) {
  uint32_t bits;
  memcpy(&bits, &data, 4);
  buf->Write(bits >> 24);
  buf->Write(bits >> 16);
  buf->Write(bits >> 8);
  buf->Write(bits);
}

void DingoSchema<std::optional<float>>::BeInternalEncodeValue(Buf* buf, float data) {
  uint32_t bits;
  memcpy(&bits, &data, 4);
  buf->Write(bits);
  buf->Write(bits >> 8);
  buf->Write(bits >> 16);
  buf->Write(bits >> 24);
}

BaseSchema::Type DingoSchema<std::optional<float>>::GetType() { return kFloat; }

void DingoSchema<std::optional<float>>::SetIndex(int index) { this->index_ = index; }

int DingoSchema<std::optional<float>>::GetIndex() { return this->index_; }

void DingoSchema<std::optional<float>>::SetIsKey(bool key) { this->key_ = key; }

bool DingoSchema<std::optional<float>>::IsKey() { return this->key_; }

int DingoSchema<std::optional<float>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<float>>::SetAllowNull(bool allow_null) { this->allow_null_ = allow_null; }

bool DingoSchema<std::optional<float>>::AllowNull() { return allow_null_; }

void DingoSchema<std::optional<float>>::SetIsLe(bool le) { this->le_ = le; }

void DingoSchema<std::optional<float>>::EncodeKey(Buf* buf, std::optional<float> data) {
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

void DingoSchema<std::optional<float>>::EncodeKeyPrefix(Buf* buf, std::optional<float> data) { EncodeKey(buf, data); }
std::optional<float> DingoSchema<std::optional<float>>::DecodeKey(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      buf->Skip(GetDataLength());
      return std::nullopt;
    }
  }
  uint32_t in = buf->Read() & 0xFF;
  if (this->le_) {
    if (in >= 0x80) {
      in = in ^ 0x80;
      for (int i = 0; i < 3; i++) {
        in <<= 8;
        in |= buf->Read() & 0xFF;
      }
    } else {
      in = ~in;
      for (int i = 0; i < 3; i++) {
        in <<= 8;
        in |= ~buf->Read() & 0xFF;
      }
    }
  } else {
    if (in >= 0x80) {
      in = in ^ 0x80;
      for (int i = 1; i < 4; i++) {
        in |= (((uint32_t)buf->Read() & 0xFF) << (8 * i));
      }
    } else {
      for (int i = 1; i < 4; i++) {
        in |= (((uint32_t)buf->Read() & 0xFF) << (8 * i));
      }
      in= ~in;
    }
  }
  float d;
  memcpy(&d, &in, 4);
  return d;
}

void DingoSchema<std::optional<float>>::SkipKey(Buf* buf) { buf->Skip(GetLength()); }

void DingoSchema<std::optional<float>>::EncodeValue(Buf* buf, std::optional<float> data) {
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

std::optional<float> DingoSchema<std::optional<float>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      buf->Skip(GetDataLength());
      return std::nullopt;
    }
  }
  uint32_t in = buf->Read() & 0xFF;
  ;
  if (this->le_) {
    for (int i = 0; i < 3; i++) {
      in <<= 8;
      in |= buf->Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 4; i++) {
      in |= (((uint32_t)buf->Read() & 0xFF) << (8 * i));
    }
  }
  float d;
  memcpy(&d, &in, 4);
  return d;
}

void DingoSchema<std::optional<float>>::SkipValue(Buf* buf) { buf->Skip(GetLength()); }

}  // namespace dingodb
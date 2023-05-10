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

#include "boolean_schema.h"

namespace dingodb {

int DingoSchema<std::optional<bool>>::GetDataLength() { return 1; }

int DingoSchema<std::optional<bool>>::GetWithNullTagLength() { return 2; }

void DingoSchema<std::optional<bool>>::InternalEncodeValue(Buf* buf, bool data) { buf->Write(data); }

void DingoSchema<std::optional<bool>>::InternalEncodeNull(Buf* buf) { buf->Write(0); }

BaseSchema::Type DingoSchema<std::optional<bool>>::GetType() { return kBool; }

void DingoSchema<std::optional<bool>>::SetIndex(int index) { this->index_ = index; }

int DingoSchema<std::optional<bool>>::GetIndex() { return this->index_; }

void DingoSchema<std::optional<bool>>::SetIsKey(bool key) { this->key_ = key; }

bool DingoSchema<std::optional<bool>>::IsKey() { return this->key_; }

int DingoSchema<std::optional<bool>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<bool>>::SetAllowNull(bool allow_null) { this->allow_null_ = allow_null; }

bool DingoSchema<std::optional<bool>>::AllowNull() { return this->allow_null_; }

void DingoSchema<std::optional<bool>>::EncodeKey(Buf* buf, std::optional<bool> data) {
  if (this->allow_null_) {
    buf->EnsureRemainder(GetWithNullTagLength());
    if (data.has_value()) {
      buf->Write(k_not_null);
      InternalEncodeValue(buf, data.value());
    } else {
      buf->Write(k_null);
    }
  } else {
    if (data.has_value()) {
      buf->EnsureRemainder(GetDataLength());
      InternalEncodeValue(buf, data.value());
    } else {
      // WRONG EMPTY DATA
    }
  }
}

void DingoSchema<std::optional<bool>>::EncodeKeyPrefix(Buf* buf, std::optional<bool> data) { EncodeKey(buf, data); }

std::optional<bool> DingoSchema<std::optional<bool>>::DecodeKey(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      buf->Skip(GetDataLength());
      return std::nullopt;
    }
  }
  bool b = buf->Read();
  return b;
}

void DingoSchema<std::optional<bool>>::SkipKey(Buf* buf) { buf->Skip(GetLength()); }

void DingoSchema<std::optional<bool>>::EncodeValue(Buf* buf, std::optional<bool> data) {
  if (this->allow_null_) {
    buf->EnsureRemainder(GetWithNullTagLength());
    if (data.has_value()) {
      buf->Write(k_not_null);
      InternalEncodeValue(buf, data.value());
    } else {
      buf->Write(k_null);
      InternalEncodeNull(buf);
    }
  } else {
    if (data.has_value()) {
      buf->EnsureRemainder(GetDataLength());
      InternalEncodeValue(buf, data.value());
    } else {
      // WRONG EMPTY DATA
    }
  }
}

std::optional<bool> DingoSchema<std::optional<bool>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      buf->Skip(GetDataLength());
      return std::nullopt;
    }
  }
  bool b = buf->Read();
  return b;
}

void DingoSchema<std::optional<bool>>::SkipValue(Buf* buf) { buf->Skip(GetLength()); }

}  // namespace dingodb
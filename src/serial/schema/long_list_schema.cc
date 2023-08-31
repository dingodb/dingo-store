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

#include "long_list_schema.h"

namespace dingodb {

int DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::GetDataLength() { return 8; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::GetWithNullTagLength() { return 9; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::LeInternalEncodeValue(Buf* buf, int64_t data) {
  uint64_t* l = (uint64_t*)&data;
  buf->Write(*l >> 56);
  buf->Write(*l >> 48);
  buf->Write(*l >> 40);
  buf->Write(*l >> 32);
  buf->Write(*l >> 24);
  buf->Write(*l >> 16);
  buf->Write(*l >> 8);
  buf->Write(*l);
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::BeInternalEncodeValue(Buf* buf, int64_t data) {
  uint64_t* l = (uint64_t*)&data;
  buf->Write(*l);
  buf->Write(*l >> 8);
  buf->Write(*l >> 16);
  buf->Write(*l >> 24);
  buf->Write(*l >> 32);
  buf->Write(*l >> 40);
  buf->Write(*l >> 48);
  buf->Write(*l >> 56);
}

BaseSchema::Type DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::GetType() { return kLongList; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::SetIndex(int index) { this->index_ = index; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::GetIndex() { return this->index_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::SetIsKey(bool key) { this->key_ = key; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::IsKey() { return this->key_; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::SetAllowNull(bool allow_null) { this->allow_null_ = allow_null; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::AllowNull() { return this->allow_null_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::SetIsLe(bool le) { this->le_ = le; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::EncodeKey(Buf* buf, std::optional<std::shared_ptr<std::vector<int64_t>>> data) {
  throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::EncodeKeyPrefix(Buf* buf, std::optional<std::shared_ptr<std::vector<int64_t>>> data) {
  throw std::runtime_error("Unsupported EncodeKey List Type");
}

std::optional<std::shared_ptr<std::vector<int64_t>>> DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::DecodeKey(Buf* buf) {
  throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::SkipKey(Buf* buf) { throw std::runtime_error("Unsupported EncodeKey List Type"); }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::EncodeValue(Buf* buf, std::optional<std::shared_ptr<std::vector<int64_t>>> data) {
if (this->allow_null_) {
    if (data.has_value()) {
      int data_size = data.value()->size();
      buf->EnsureRemainder(5 + data_size * 8);
      buf->Write(k_not_null);
      buf->WriteInt(data_size);
      for (const int64_t& value : *data.value()) {
        if (this->le_) {
          LeInternalEncodeValue(buf, value);
        } else {
          BeInternalEncodeValue(buf, value);
        }
      }
    } else {
      buf->EnsureRemainder(1);
      buf->Write(k_null);
    }
  } else {
    if (data.has_value()) {
      int data_size = data.value()->size();
      buf->EnsureRemainder(4 + data_size * 8);
      buf->WriteInt(data_size);
      for (const int64_t& value : *data.value()) {
        if (this->le_) {
          LeInternalEncodeValue(buf, value);
        } else {
          BeInternalEncodeValue(buf, value);
        }
      }
    } else {
      // WRONG EMPTY DATA
    }
  }
}

uint64_t DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::InternalDecodeData(Buf* buf) {
 uint64_t l = buf->Read() & 0xFF;
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
  return l;
}

std::optional<std::shared_ptr<std::vector<int64_t>>> DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return std::nullopt;
    }
  }
  int length = buf->ReadInt();
  std::shared_ptr<std::vector<int64_t>> data = std::make_shared<std::vector<int64_t>>();
  data->reserve(length);
  for (int i = 0; i < length; i++) {
    data->emplace_back(InternalDecodeData(buf));
  }
  return data;
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>::SkipValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return;
    }
  }
  int length = buf->ReadInt();
  buf->Skip(length * 8);
}
}  // namespace dingodb
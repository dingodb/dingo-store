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

#include "integer_list_schema.h"

namespace dingodb {

int DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::GetDataLength() { return 4; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::GetWithNullTagLength() { return 5; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::LeInternalEncodeValue(Buf* buf, int32_t data) {
  uint32_t* i = (uint32_t*)&data;
  buf->Write(*i >> 24);
  buf->Write(*i >> 16);
  buf->Write(*i >> 8);
  buf->Write(*i);
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::BeInternalEncodeValue(Buf* buf, int32_t data) {
  uint32_t* i = (uint32_t*)&data;
  buf->Write(*i);
  buf->Write(*i >> 8);
  buf->Write(*i >> 16);
  buf->Write(*i >> 24);
}

BaseSchema::Type DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::GetType() { return kIntegerList; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::SetIndex(int index) { this->index_ = index; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::GetIndex() { return this->index_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::SetIsKey(bool key) { this->key_ = key; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::IsKey() { return this->key_; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::SetAllowNull(bool allow_null) { this->allow_null_ = allow_null; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::AllowNull() { return this->allow_null_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::SetIsLe(bool le) { this->le_ = le; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::EncodeKey(Buf* buf, std::optional<std::shared_ptr<std::vector<int32_t>>> data) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::EncodeKeyPrefix(Buf* buf, std::optional<std::shared_ptr<std::vector<int32_t>>> data) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

std::optional<std::shared_ptr<std::vector<int32_t>>> DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::DecodeKey(Buf* buf) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::SkipKey(Buf* buf) { throw std::runtime_error("Unsupported EncodeKey List Type");}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::EncodeValue(Buf* buf, std::optional<std::shared_ptr<std::vector<int32_t>>> data) {
  if (this->allow_null_) {
    if (data.has_value()) {
      int data_size = data.value()->size();
      buf->EnsureRemainder(5 + data_size * 4);
      buf->Write(k_not_null);
      buf->WriteInt(data_size);
      for (const int32_t& value : *data.value()) {
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
      buf->EnsureRemainder(4 + data_size * 4);
      buf->WriteInt(data_size);
      for (const int32_t& value : *data.value()) {
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

uint32_t DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::InternalDecodeData(Buf* buf) {
  if (this->le_) {
    uint32_t r = ((buf->Read() & 0xFF) << 24) | ((buf->Read() & 0xFF) << 16) | ((buf->Read() & 0xFF) << 8) |
                 (buf->Read() & 0xFF);
    return r;
  } else {
    uint32_t r = (buf->Read() & 0xFF) | ((buf->Read() & 0xFF) << 8) | ((buf->Read() & 0xFF) << 16) |
                 ((buf->Read() & 0xFF) << 24);
    return r;
  }
}

std::optional<std::shared_ptr<std::vector<int32_t>>> DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return std::nullopt;
    }
  }
  int length = buf->ReadInt();
  std::shared_ptr<std::vector<int32_t>> data = std::make_shared<std::vector<int32_t>>();
  data->reserve(length);
  for (int i = 0; i < length; i++) {
    data->emplace_back(InternalDecodeData(buf));
  }
  return data;
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>::SkipValue(Buf* buf) { 
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return;
    }
  }
  int length = buf->ReadInt();
  buf->Skip(length * 4);
}
}  // namespace dingodb
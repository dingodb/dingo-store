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

#include "float_list_schema.h"

namespace dingodb {

int DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::GetDataLength() { return 4; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::GetWithNullTagLength() { return 5; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::LeInternalEncodeValue(Buf* buf, float data) {
  uint32_t bits;
  memcpy(&bits, &data, 4);
  buf->Write(bits >> 24);
  buf->Write(bits >> 16);
  buf->Write(bits >> 8);
  buf->Write(bits);
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::BeInternalEncodeValue(Buf* buf, float data) {
  uint32_t bits;
  memcpy(&bits, &data, 4);
  buf->Write(bits);
  buf->Write(bits >> 8);
  buf->Write(bits >> 16);
  buf->Write(bits >> 24);
}

BaseSchema::Type DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::GetType() { return kFloatList; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::SetIndex(int index) { this->index_ = index; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::GetIndex() { return this->index_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::SetIsKey(bool key) { this->key_ = key; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::IsKey() { return this->key_; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::SetAllowNull(bool allow_null) { this->allow_null_ = allow_null; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::AllowNull() { return allow_null_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::SetIsLe(bool le) { this->le_ = le; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::EncodeKey(Buf* buf, std::optional<std::shared_ptr<std::vector<float>>> data) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::EncodeKeyPrefix(Buf* buf, std::optional<std::shared_ptr<std::vector<float>>> data) { throw std::runtime_error("Unsupported EncodeKey List Type"); }
std::optional<std::shared_ptr<std::vector<float>>> DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::DecodeKey(Buf* buf) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::SkipKey(Buf* buf) { throw std::runtime_error("Unsupported EncodeKey List Type"); }

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::EncodeValue(Buf* buf, std::optional<std::shared_ptr<std::vector<float>>> data) {
  if (this->allow_null_) {
    if (data.has_value()) {
      int data_size = data.value()->size();
      buf->EnsureRemainder(5 + data_size * 4);
      buf->Write(k_not_null);
      buf->WriteInt(data_size);
      for (const float& value : *data.value()) {
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
      for (const float& value : *data.value()) {
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

float DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::InternalDecodeData(Buf* buf) {
  uint32_t in = buf->Read() & 0xFF;
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
//   float d;
//   memcpy(&d, &in, 4);
//   return d;
  return *reinterpret_cast<float*>(&in);
}

std::optional<std::shared_ptr<std::vector<float>>> DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return std::nullopt;
    }
  }
  int length = buf->ReadInt();
  std::shared_ptr<std::vector<float>> data = std::make_shared<std::vector<float>>();
  data->reserve(length);
  for (int i = 0; i < length; i++) {
    data->emplace_back(InternalDecodeData(buf));
  }
  return data;
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>::SkipValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return;
    }
  }
  int length = buf->ReadInt();
  buf->Skip(length * 4);
}

}  // namespace dingodb
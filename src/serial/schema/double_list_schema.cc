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

#include "double_list_schema.h"

namespace dingodb {

int DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::GetDataLength() { return 8; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::GetWithNullTagLength() { return 9; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::LeInternalEncodeValue(Buf* buf, double data) {
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

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::BeInternalEncodeValue(Buf* buf, double data) {
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

BaseSchema::Type DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::GetType() { return kDoubleList; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::SetIndex(int index) { this->index_ = index; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::GetIndex() { return this->index_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::SetIsKey(bool key) { this->key_ = key; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::IsKey() { return this->key_; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::SetAllowNull(bool allow_null) { this->allow_null_ = allow_null; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::AllowNull() { return allow_null_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::SetIsLe(bool le) { this->le_ = le; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::EncodeKey(Buf* buf, std::optional<std::shared_ptr<std::vector<double>>> data) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::EncodeKeyPrefix(Buf* buf, std::optional<std::shared_ptr<std::vector<double>>> data) { throw std::runtime_error("Unsupported EncodeKey List Type");}
std::optional<std::shared_ptr<std::vector<double>>> DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::DecodeKey(Buf* buf) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::SkipKey(Buf* buf) { throw std::runtime_error("Unsupported EncodeKey List Type"); }

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::EncodeValue(Buf* buf, std::optional<std::shared_ptr<std::vector<double>>> data) {
  if (this->allow_null_) {
    if (data.has_value()) {
      int data_size = data.value()->size();
      buf->EnsureRemainder(5 + data_size * 8);
      buf->Write(k_not_null);
      buf->WriteInt(data_size);
      for (const double& value : *data.value()) {
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
      for (const double& value : *data.value()) {
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

double DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::InternalDecodeData(Buf* buf) {
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
  // double d;
  // memcpy(&d, &l, 8);
  return *reinterpret_cast<double*>(&l);
  // return d;
}

std::optional<std::shared_ptr<std::vector<double>>> DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return std::nullopt;
    }
  }
  int length = buf->ReadInt();
  std::shared_ptr<std::vector<double>> data = std::make_shared<std::vector<double>>();
  data->reserve(length);
  for (int i = 0; i < length; i++) {
    data->emplace_back(InternalDecodeData(buf));
  }
  return data;
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>::SkipValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return;
    }
  }
  int length = buf->ReadInt();
  buf->Skip(length * 8);
}

}  // namespace dingodb
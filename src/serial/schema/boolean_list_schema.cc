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

#include "boolean_list_schema.h"

namespace dingodb {

int DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::GetDataLength() { return 1; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::GetWithNullTagLength() { return 2; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::InternalEncodeValue(Buf* buf, bool data) { buf->Write(data); }

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::InternalEncodeNull(Buf* buf) { buf->Write(0); }

BaseSchema::Type DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::GetType() { return kBoolList; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::SetIndex(int index) { this->index_ = index; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::GetIndex() { return this->index_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::SetIsKey(bool key) { this->key_ = key; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::IsKey() { return this->key_; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::SetAllowNull(bool allow_null) { this->allow_null_ = allow_null; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::AllowNull() { return this->allow_null_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::EncodeKey(Buf* buf, std::optional<std::shared_ptr<std::vector<bool>>> data) {
  throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::EncodeKeyPrefix(Buf* buf, std::optional<std::shared_ptr<std::vector<bool>>> data) { EncodeKey(buf, data); }

std::optional<std::shared_ptr<std::vector<bool>>> DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::DecodeKey(Buf* buf) {
  throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::SkipKey(Buf* buf) { throw std::runtime_error("Unsupported EncodeKey List Type");}

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::EncodeValue(Buf* buf, std::optional<std::shared_ptr<std::vector<bool>>> data) {
  if (this->allow_null_) {
    if (data.has_value()) {
      int data_size = data.value()->size();
      buf->EnsureRemainder(5 + data_size);
      buf->Write(k_not_null);
      buf->WriteInt(data_size);
      for (const bool &value : *data.value()) {
        InternalEncodeValue(buf, value);
      }
    } else {
      buf->EnsureRemainder(1);
      buf->Write(k_null);
    }
  } else {
    if (data.has_value()) {
      int data_size = data.value()->size();
      buf->EnsureRemainder(4 + data_size);
      buf->WriteInt(data_size);
      for (const bool &value : *data.value()) {
        InternalEncodeValue(buf, value);
      }
    } else {
      // WRONG EMPTY DATA
    }
  }
}

std::optional<std::shared_ptr<std::vector<bool>>> DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return std::nullopt;
    }
  }
  int length = buf->ReadInt();
  std::shared_ptr<std::vector<bool>> vector = std::make_shared<std::vector<bool>>(length);
  for (int i = 0; i < length; i++) {
    bool b = buf->Read();
    (*vector)[i] = b;
  }
   return vector;
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>::SkipValue(Buf* buf) { 
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return;
    }
  }
  int length = buf->ReadInt();
  buf->Skip(length); 
}

}  // namespace dingodb
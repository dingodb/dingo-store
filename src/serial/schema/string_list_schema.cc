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

#include "string_list_schema.h"

#include <memory>
#include <string>

namespace dingodb {

int DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::GetDataLength() { return 0; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::GetWithNullTagLength() { return 0; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::InternalEmlementEncodeValue(Buf* buf,
                                                                                  const std::string &data) {
  buf->EnsureRemainder(data.length() + 4);
  buf->WriteInt(data.length());
  buf->Write(data);
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::InternalEncodeValue(Buf* buf,
                                                                                   std::shared_ptr<std::vector<std::string>> data) {
    // vector size 
    buf->EnsureRemainder(4);
    buf->WriteInt(data->size());
    for (const std::string& str : *data) {
        InternalEmlementEncodeValue(buf, str);
    }
}

BaseSchema::Type DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::GetType() { return kStringList; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::SetIndex(int index) { this->index_ = index; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::GetIndex() { return this->index_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::SetIsKey(bool key) { this->key_ = key; }

bool DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::IsKey() { return this->key_; }

int DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::SetAllowNull(bool allow_null) {
  this->allow_null_ = allow_null;
}

bool DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::AllowNull() { return this->allow_null_; }

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::EncodeKey(
    Buf* buf, std::optional<std::shared_ptr<std::vector<std::string>>> data) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::EncodeKeyPrefix(
    Buf* buf, std::optional<std::shared_ptr<std::vector<std::string>>> data) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

std::optional<std::shared_ptr<std::vector<std::string>>> DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::DecodeKey(
    Buf* buf) {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::SkipKey(Buf* buf) const {
    throw std::runtime_error("Unsupported EncodeKey List Type");
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::EncodeValue(
    Buf* buf, std::optional<std::shared_ptr<std::vector<std::string>>> data) {
  if (this->allow_null_) {
    buf->EnsureRemainder(1);
    if (data.has_value()) {
      buf->Write(k_not_null);
      InternalEncodeValue(buf, data.value());
    } else {
      buf->Write(k_null);
    }
  } else {
    if (data.has_value()) {
      InternalEncodeValue(buf, data.value());
    } else {
      // WRONG EMPTY DATA
    }
  }
}

std::optional<std::shared_ptr<std::vector<std::string>>> DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::DecodeValue(
    Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return std::nullopt;
    }
  }
  int length = buf->ReadInt();
  std::shared_ptr<std::vector<std::string>> data = std::make_shared<std::vector<std::string>>();
  data->reserve(length);
  for (int i = 0; i < length; i++) {
    int str_len = buf->ReadInt();
    std::string str;
    for (int j = 0; j < str_len; j++) {
      str.push_back(buf->Read());
    }
    data->push_back(std::move(str));
  }

  return data;
}

void DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>::SkipValue(Buf* buf) const {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return;
    }
  }
  int length = buf->ReadInt();
  for (int i = 0; i < length; i++) {
    int str_len = buf->ReadInt();
    buf->Skip(str_len);
  }
}

}  // namespace dingodb
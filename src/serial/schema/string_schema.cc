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

#include "string_schema.h"

namespace dingodb {

int DingoSchema<std::optional<std::reference_wrapper<std::string>>>::GetDataLength() {
  return 0;
}
int DingoSchema<std::optional<std::reference_wrapper<std::string>>>::GetWithNullTagLength() {
  return 0;
}
int DingoSchema<std::optional<std::reference_wrapper<std::string>>>::InternalEncodeKey(Buf* buf, std::string &data) {
  int group_num = data.length() / 8;
  int size = (group_num + 1) * 9;
  int remainder_size = data.length() % 8;
  int remainder_zero;
  if (remainder_size == 0) {
    remainder_size = 8;
    remainder_zero = 8;
  } else {
    remainder_zero = 8 - remainder_size;
  }
  buf->EnsureRemainder(size + 4);
  int curr = 0;
  for (int i = 0; i < group_num; i++) {
    for (int j = 0; j < 8; j++) {
      buf->Write(data.at(curr++));
    }
    buf->Write((uint8_t)255);
  }
  if (remainder_size < 8) {
    for (int j = 0; j < remainder_size; j++) {
      buf->Write(data.at(curr++));
    }
  }
  for (int i = 0; i < remainder_zero; i++) {
    buf->Write((uint8_t)0);
  }
  buf->Write((uint8_t)(255 - remainder_zero));
  return size;
}
void DingoSchema<std::optional<std::reference_wrapper<std::string>>>::InternalEncodeValue(Buf* buf, std::string &data) {
  buf->EnsureRemainder(data.length() + 4);
  buf->WriteInt(data.length());
  for (char c : data) {
    buf->Write(c);
  }
}

BaseSchema::Type DingoSchema<std::optional<std::reference_wrapper<std::string>>>::GetType() {
  return kString;
}
void DingoSchema<std::optional<std::reference_wrapper<std::string>>>::SetIndex(int index) {
  this->index_ = index;
}
int DingoSchema<std::optional<std::reference_wrapper<std::string>>>::GetIndex() {
  return this->index_;
}
void DingoSchema<std::optional<std::reference_wrapper<std::string>>>::SetIsKey(bool key) {
  this->key_ = key;
}
bool DingoSchema<std::optional<std::reference_wrapper<std::string>>>::IsKey() {
  return this->key_;
}
int DingoSchema<std::optional<std::reference_wrapper<std::string>>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}
void DingoSchema<std::optional<std::reference_wrapper<std::string>>>::SetAllowNull(bool allow_null) {
  this->allow_null_ = allow_null;
}
bool DingoSchema<std::optional<std::reference_wrapper<std::string>>>::AllowNull() {
  return this->allow_null_;
}
void DingoSchema<std::optional<std::reference_wrapper<std::string>>>::EncodeKey(Buf* buf, std::optional<std::reference_wrapper<std::string>> data) {
  if (this->allow_null_) {
    if (data.has_value()) {
      buf->EnsureRemainder(1);
      buf->Write(k_not_null);
      buf->ReverseWriteInt(InternalEncodeKey(buf, data->get()));
    } else {;
      buf->EnsureRemainder(5);
      buf->Write(k_null);
      buf->ReverseWriteInt(0);
    }
  } else {
    if (data.has_value()) {
      buf->ReverseWriteInt(InternalEncodeKey(buf, data->get()));
    } else {
      // WRONG EMPTY DATA
    }
  }
}
void DingoSchema<std::optional<std::reference_wrapper<std::string>>>::EncodeKeyPrefix(Buf* buf, std::optional<std::reference_wrapper<std::string>> data) {
  if (this->allow_null_) {
    if (data.has_value()) {
      buf->EnsureRemainder(1);
      buf->Write(k_not_null);
      InternalEncodeKey(buf, data->get());
    } else {;
      buf->EnsureRemainder(5);
      buf->Write(k_null);
    }
  } else {
    if (data.has_value()) {
      InternalEncodeKey(buf, data->get());
    } else {
      // WRONG EMPTY DATA
    }
  }
}
std::optional<std::reference_wrapper<std::string>> DingoSchema<std::optional<std::reference_wrapper<std::string>>>::DecodeKey(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      buf->ReverseSkipInt();
      return std::nullopt;
    }
  }
  int length = buf->ReverseReadInt();
  int group_num = length / 9;
  buf->Skip(length - 1);
  int remainder_zero = 255 - buf->Read() & 0xFF;
  buf->Skip(0 - length);
  int ori_length = group_num * 8 - remainder_zero;
  char data[ori_length];
  if (ori_length != 0) {
    int curr = 0;
    group_num--;
    for (int i = 0; i < group_num; i++) {
      for (int j = 0; j < 8; j++) {
        data[curr++] = buf->Read();
      }
      buf->Skip(1);
    }
    if (remainder_zero != 8) {
      int non_zero_count = 8 - remainder_zero;
      for (int j = 0; j < non_zero_count; j++) {
        data[curr++] = buf->Read();
      }
    }
  }
  buf->Skip(remainder_zero + 1);
  std::string *s = new std::string(data, ori_length);
  return std::optional<std::reference_wrapper<std::string>>{*s};
}
void DingoSchema<std::optional<std::reference_wrapper<std::string>>>::SkipKey(Buf* buf) const {
  if (this->allow_null_) {
    buf->Skip(buf->ReverseReadInt() + 1);
  } else {
    buf->Skip(buf->ReverseReadInt());
  }
}
void DingoSchema<std::optional<std::reference_wrapper<std::string>>>::EncodeValue(Buf* buf, std::optional<std::reference_wrapper<std::string>> data) {
  if (this->allow_null_) {
    buf->EnsureRemainder(1);
    if (data.has_value()) {
      buf->Write(k_not_null);
      InternalEncodeValue(buf, data->get());
    } else {
      buf->Write(k_null);
    }
  } else {
    if (data.has_value()) {
      InternalEncodeValue(buf, data->get());
    } else {
      // WRONG EMPTY DATA
    }
  }
}
std::optional<std::reference_wrapper<std::string>> DingoSchema<std::optional<std::reference_wrapper<std::string>>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return std::nullopt;
    }
  }
  int length = buf->ReadInt();
  char su8[length];
  for (int i = 0; i < length; i++) {
    su8[i] = buf->Read();
  }
  std::string *s = new std::string(su8, length);
  return std::optional<std::reference_wrapper<std::string>>{*s};
}
void DingoSchema<std::optional<std::reference_wrapper<std::string>>>::SkipValue(Buf* buf) const {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return;
    }
  }
  buf->Skip(buf->ReadInt());
}

}  // namespace dingodb
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

#include "keyvalue.h"

namespace dingodb {

KeyValue::KeyValue(std::string* key, std::string* value) {
  this->key_ = key;
  this->value_ = value;
}
KeyValue::~KeyValue() {
  delete this->key_;
  delete this->value_;
}
void KeyValue::Set(std::string* key, std::string* value) {
  this->key_ = key;
  this->value_ = value;
}
void KeyValue::SetKey(std::string* key) {
  this->key_ = key;
}
void KeyValue::SetValue(std::string* value) {
  this->value_ = value;
}
std::string* KeyValue::GetKey() {
  return key_;
}
std::string* KeyValue::GetValue() {
  return value_;
}

}  // namespace dingodb
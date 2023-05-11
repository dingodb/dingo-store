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

#include <memory>
#include <string>

namespace dingodb {

KeyValue::KeyValue() : key_(std::make_shared<std::string>()), value_(std::make_shared<std::string>()) {}

KeyValue::KeyValue(std::shared_ptr<std::string> key, std::shared_ptr<std::string> value) : key_(key), value_(value) {}

void KeyValue::Set(std::shared_ptr<std::string> key, std::shared_ptr<std::string> value) {
  this->key_ = key;
  this->value_ = value;
}

void KeyValue::SetKey(std::shared_ptr<std::string> key) { this->key_ = key; }
void KeyValue::SetValue(std::shared_ptr<std::string> value) { this->value_ = value; }

std::shared_ptr<std::string> KeyValue::GetKey() const { return key_; }
std::shared_ptr<std::string> KeyValue::GetValue() const { return value_; }

}  // namespace dingodb
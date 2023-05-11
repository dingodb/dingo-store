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

#include "keyvalue_codec.h"

#include <algorithm>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "proto/common.pb.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/utils.h"

namespace dingodb {

KeyValueCodec::KeyValueCodec(std::shared_ptr<pb::meta::TableDefinition> td, uint64_t common_id)
    : td_(td),
      schemas_(TableDefinitionToDingoSchema(td)),
      re_(0, this->schemas_, common_id, IsLE()),
      rd_(0, this->schemas_, common_id, IsLE()) {}

KeyValueCodec::~KeyValueCodec() = default;

int KeyValueCodec::Decode(const std::string& key, const std::string& value, std::vector<std::any>& record) {
  std::vector<std::any> element_record;
  int ret = rd_.Decode(key, value, element_record);
  if (ret < 0) {
    return ret;
  }

  return ElementToSql(*td_, element_record, record);
}

int KeyValueCodec::Decode(const pb::common::KeyValue& key_value, std::vector<std::any>& record) {
  return Decode(key_value.key(), key_value.value(), record);
}

int KeyValueCodec::Decode(KeyValue& keyvalue, std::vector<std::any>& record) {
  return Decode(*keyvalue.GetKey(), *keyvalue.GetValue(), record);
}

int KeyValueCodec::Encode(const std::vector<std::any>& record, std::string& key, std::string& value) {
  std::vector<std::any> sql_record;
  int ret = SqlToElement(*td_, record, sql_record);
  if (ret < 0) {
    return ret;
  }
  return re_.Encode(sql_record, key, value);
}

int KeyValueCodec::Encode(const std::vector<std::any>& record, pb::common::KeyValue& key_value) {
  return Encode(record, *key_value.mutable_key(), *key_value.mutable_value());
}

int KeyValueCodec::Encode(const std::vector<std::any>& record, KeyValue& keyvalue) {
  return Encode(record, *keyvalue.GetKey(), *keyvalue.GetValue());
}

int KeyValueCodec::EncodeKey(const std::vector<std::any>& record, std::string& output) {
  std::vector<std::any> sql_record;
  int ret = SqlToElement(*td_, record, sql_record);
  if (ret < 0) {
    return ret;
  }
  return re_.EncodeKey(sql_record, output);
}

int KeyValueCodec::EncodeKeyPrefix(const std::vector<std::any>& record, int column_count, std::string& output) {
  std::vector<std::any> element_record;
  int ret = SqlToElement(*td_, record, element_record);
  if (ret < 0) {
    return ret;
  }
  return re_.EncodeKeyPrefix(element_record, column_count, output);
}

int KeyValueCodec::EncodeMaxKeyPrefix(std::string& output) { return re_.EncodeMinKeyPrefix(output); }

int KeyValueCodec::EncodeMinKeyPrefix(std::string& output) { return re_.EncodeMinKeyPrefix(output); }

}  // namespace dingodb
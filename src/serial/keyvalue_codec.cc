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

#include <new>
#include <string>
#include <vector>

#include "proto/common.pb.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/utils.h"

namespace dingodb {

KeyValueCodec::KeyValueCodec(pb::meta::TableDefinition* td, uint64_t common_id) {
  this->td_ = td;
  this->schemas_ = TableDefinitionToDingoSchema(td);
  bool le = IsLE();
  this->re_ = new RecordEncoder(0, this->schemas_, common_id, le);
  this->rd_ = new RecordDecoder(0, this->schemas_, common_id, le);
}

KeyValueCodec::~KeyValueCodec() {
  delete schemas_;
  delete re_;
  delete rd_;
}

int KeyValueCodec::Decode(const std::string& key, const std::string& value, std::vector<std::any>& record) {
  std::vector<std::any> element_record;
  int ret = rd_->Decode(key, value, element_record);
  if (ret < 0) {
    return ret;
  }

  return ElementToSql(*td_, element_record, record);
}

int KeyValueCodec::Decode(const pb::common::KeyValue& key_value, std::vector<std::any>& record) {
  return Decode(key_value.key(), key_value.value(), record);
}

// std::vector<std::any>* KeyValueCodec::Decode(KeyValue* keyvalue) { return ElementToSql(td_, rd_->Decode(keyvalue)); }
std::vector<std::any>* KeyValueCodec::Decode(KeyValue* keyvalue) {
  std::vector<std::any>* sql_record = new std::vector<std::any>();

  int ret = Decode(*keyvalue->GetKey(), *keyvalue->GetValue(), *sql_record);
  if (ret < 0) {
    delete sql_record;
    return nullptr;
  }
  return sql_record;
}

int KeyValueCodec::Encode(const std::vector<std::any>& record, std::string& key, std::string& value) {
  std::vector<std::any> sql_record;
  int ret = SqlToElement(*td_, record, sql_record);
  if (ret < 0) {
    return ret;
  }
  return re_->Encode(sql_record, key, value);
}

int KeyValueCodec::Encode(const std::vector<std::any>& record, pb::common::KeyValue& key_value) {
  return Encode(record, *key_value.mutable_key(), *key_value.mutable_value());
}

// KeyValue* KeyValueCodec::Encode(std::vector<std::any>* record) { return re_->Encode(SqlToElement(td_, record)); }
KeyValue* KeyValueCodec::Encode(std::vector<std::any>* record) {
  std::string* key = new std::string();
  std::string* value = new std::string();

  int ret = Encode(*record, *key, *value);
  if (ret < 0) {
    delete key;
    delete value;
    return nullptr;
  }

  return new KeyValue(key, value);
}

std::string* KeyValueCodec::EncodeKey(std::vector<std::any>* record) {
  return re_->EncodeKey(SqlToElement(td_, record));
}

int KeyValueCodec::EncodeKey(const std::vector<std::any>& record, std::string& output) {
  std::vector<std::any> sql_record;
  int ret = SqlToElement(*td_, record, sql_record);
  if (ret < 0) {
    return ret;
  }
  return re_->EncodeKey(sql_record, output);
}

std::string* KeyValueCodec::EncodeKeyPrefix(std::vector<std::any>* record, int column_count) {
  return re_->EncodeKeyPrefix(SqlToElement(td_, record), column_count);
}

int KeyValueCodec::EncodeKeyPrefix(const std::vector<std::any>& record, int column_count, std::string& output) {
  std::vector<std::any> element_record;
  int ret = SqlToElement(*td_, record, element_record);
  if (ret < 0) {
    return ret;
  }
  return re_->EncodeKeyPrefix(element_record, column_count, output);
}

std::string* KeyValueCodec::EncodeMaxKeyPrefix() { return re_->EncodeMinKeyPrefix(); }

int KeyValueCodec::EncodeMaxKeyPrefix(std::string& output) { return re_->EncodeMinKeyPrefix(output); }

std::string* KeyValueCodec::EncodeMinKeyPrefix() { return re_->EncodeMinKeyPrefix(); }

int KeyValueCodec::EncodeMinKeyPrefix(std::string& output) { return re_->EncodeMinKeyPrefix(output); }

}  // namespace dingodb
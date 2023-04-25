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
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/utils.h"

namespace dingodb {


  KeyValueCodec::KeyValueCodec(pb::meta::TableDefinition* td, uint64_t common_id) {
    this->td_ = td;
    this->schemas_ = TableDefinitionToDingoSchema(td);
    this->re_ = new RecordEncoder(0, this->schemas_, common_id);
    this->rd_ = new RecordDecoder(0, this->schemas_, common_id);
  }
  KeyValueCodec::~KeyValueCodec() {
    delete schemas_;
    delete re_;
    delete rd_;
  }
  std::vector<std::any>* KeyValueCodec::Decode(KeyValue* keyvalue) {
    return ElementToSql(td_, rd_->Decode(keyvalue));
  }
  KeyValue* KeyValueCodec::Encode(std::vector<std::any>* record) {
    return re_->Encode(SqlToElement(td_, record));
  }
  std::string* KeyValueCodec::EncodeKey(std::vector<std::any>* record) {
    return re_->EncodeKey(SqlToElement(td_, record));
  }
  std::string* KeyValueCodec::EncodeKeyPrefix(std::vector<std::any>* record, int column_count) {
    return re_->EncodeKeyPrefix(SqlToElement(td_, record), column_count);
  }
  std::string* KeyValueCodec::EncodeMaxKeyPrefix() {
    return re_->EncodeMinKeyPrefix();
  }
  std::string* KeyValueCodec::EncodeMinKeyPrefix() {
    return re_->EncodeMinKeyPrefix();
  }


}  // namespace dingodb
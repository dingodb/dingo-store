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

#include "record_encoder.h"
#include "serial/schema/base_schema.h"

namespace dingodb {

RecordEncoder::RecordEncoder(int schema_version, vector<BaseSchema*>*  schemas,
                             long common_id) {
  this->schema_version_ = schema_version;
  this->schemas_ = schemas;
  this->common_id_ = common_id;
  int32_t* size = GetApproPerRecordSize(schemas);
  this->key_buf_size_ = size[0];
  this->value_buf_size_ = size[1];
  delete[] size;
}
KeyValue* RecordEncoder::Encode(vector<any>*  record) {
  string* key = EncodeKey(record);
  string* value = EncodeValue(record);
  KeyValue* keyvalue = new KeyValue(key, value);
  return keyvalue;
}
string* RecordEncoder::EncodeKey(vector<any>*  record) {
  Buf* key_buf = new Buf(key_buf_size_);
  key_buf->EnsureRemainder(12);
  key_buf->WriteLong(common_id_);
  key_buf->ReverseWriteInt(codec_version_);
  for (BaseSchema *bs : *schemas_) {
     BaseSchema::Type type = bs->GetType();
    switch (type) {
      case BaseSchema::kBool: {
        DingoSchema<optional<bool>>* bos = static_cast<DingoSchema<optional<bool>>*>(bs);
        if (bos->IsKey()) {
          bos->EncodeKey(key_buf, any_cast<optional<bool>>(record->at(bos->GetIndex())));
        }
        break;
      }
      case BaseSchema::kInteger: {
        DingoSchema<optional<int32_t>>* is = static_cast<DingoSchema<optional<int32_t>>*>(bs);
        if (is->IsKey()) {
          is->EncodeKey(key_buf, any_cast<optional<int32_t>>(record->at(is->GetIndex())));
        }
        break;
      }
      case BaseSchema::kLong: {
        DingoSchema<optional<int64_t>>* ls = static_cast<DingoSchema<optional<int64_t>>*>(bs);
        if (ls->IsKey()) {
          ls->EncodeKey(key_buf, any_cast<optional<int64_t>>(record->at(ls->GetIndex())));
        }
        break;
      }
      case BaseSchema::kDouble: {
        DingoSchema<optional<double>>* ds = static_cast<DingoSchema<optional<double>>*>(bs);
        if (ds->IsKey()) {
          ds->EncodeKey(key_buf, any_cast<optional<double>>(record->at(ds->GetIndex())));
        }
        break;
      }
      case BaseSchema::kString: {
        DingoSchema<optional<reference_wrapper<string>>>* ss = static_cast<DingoSchema<optional<reference_wrapper<string>>>*>(bs);
        if (ss->IsKey()) {
          ss->EncodeKey(key_buf, any_cast<optional<reference_wrapper<string>>>(record->at(ss->GetIndex())));
        }
        break;
      }
      default: {
        break;
      }
    }
  }
  string* key = key_buf->GetBytes();
  delete key_buf;
  return key;
}
string* RecordEncoder::EncodeValue(vector<any>*  record) {
  Buf* value_buf = new Buf(value_buf_size_);
  value_buf->EnsureRemainder(4);
  value_buf->WriteInt(schema_version_);
  for (BaseSchema *bs : *schemas_) {
     BaseSchema::Type type = bs->GetType();
    switch (type) {
      case BaseSchema::kBool: {
        DingoSchema<optional<bool>>* bos = static_cast<DingoSchema<optional<bool>>*>(bs);
        if (!bos->IsKey()) {
          bos->EncodeValue(value_buf,
                           any_cast<optional<bool>>(record->at(bos->GetIndex())));
        }
        break;
      }
      case BaseSchema::kInteger: {
        DingoSchema<optional<int32_t>>* is = static_cast<DingoSchema<optional<int32_t>>*>(bs);
        if (!is->IsKey()) {
          is->EncodeValue(value_buf,
                          any_cast<optional<int32_t>>(record->at(is->GetIndex())));
        }
        break;
      }
      case BaseSchema::kLong: {
        DingoSchema<optional<int64_t>>* ls = static_cast<DingoSchema<optional<int64_t>>*>(bs);
        if (!ls->IsKey()) {
          ls->EncodeValue(value_buf,
                          any_cast<optional<int64_t>>(record->at(ls->GetIndex())));
        }
        break;
      }
      case BaseSchema::kDouble: {
        DingoSchema<optional<double>>* ds = static_cast<DingoSchema<optional<double>>*>(bs);
        if (!ds->IsKey()) {
          ds->EncodeValue(value_buf,
                          any_cast<optional<double>>(record->at(ds->GetIndex())));
        }
        break;
      }
      case BaseSchema::kString: {
        DingoSchema<optional<reference_wrapper<string>>>* ss = static_cast<DingoSchema<optional<reference_wrapper<string>>>*>(bs);
        if (!ss->IsKey()) {
          ss->EncodeValue(value_buf,
                          any_cast<optional<reference_wrapper<string>>>(record->at(ss->GetIndex())));
        }
        break;
      }
      default: {
        break;
      }
    }
  }
  string* value = value_buf->GetBytes();
  delete value_buf;
  return value;
}

}  // namespace dingodb
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

#include <string>

#include "proto/common.pb.h"
#include "serial/keyvalue.h"

namespace dingodb {

RecordEncoder::RecordEncoder(int schema_version, std::vector<BaseSchema*>* schemas, long common_id) {
  this->le_ = IsLE();
  Init(schema_version, schemas, common_id);
}
RecordEncoder::RecordEncoder(int schema_version, std::vector<BaseSchema*>* schemas, long common_id, bool le) {
  this->le_ = le;
  Init(schema_version, schemas, common_id);
}
void RecordEncoder::Init(int schema_version, std::vector<BaseSchema*>* schemas, long common_id) {
  this->schema_version_ = schema_version;
  FormatSchema(schemas, this->le_);
  this->schemas_ = schemas;
  this->common_id_ = common_id;
  int32_t* size = GetApproPerRecordSize(schemas);
  this->key_buf_size_ = size[0];
  this->value_buf_size_ = size[1];
  delete[] size;
}

int RecordEncoder::Encode(const std::vector<std::any>& record, std::string& key, std::string& value) {
  int ret = EncodeKey(record, key);
  if (ret < 0) {
    return ret;
  }
  ret = EncodeValue(record, value);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

int RecordEncoder::Encode(const std::vector<std::any>& record, pb::common::KeyValue& key_value) {
  int ret = EncodeKey(record, *key_value.mutable_key());
  if (ret < 0) {
    return ret;
  }
  ret = EncodeValue(record, *key_value.mutable_value());
  if (ret < 0) {
    return ret;
  }
  return 0;
}

int RecordEncoder::EncodeKey(const std::vector<std::any>& record, std::string& output) {
  Buf* key_buf = new Buf(key_buf_size_, this->le_);
  key_buf->EnsureRemainder(12);
  key_buf->WriteLong(common_id_);
  key_buf->ReverseWriteInt(codec_version_);
  for (BaseSchema* bs : *schemas_) {
    if (bs != nullptr) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kBool: {
          DingoSchema<std::optional<bool>>* bos = static_cast<DingoSchema<std::optional<bool>>*>(bs);
          if (bos->IsKey()) {
            bos->EncodeKey(key_buf, std::any_cast<std::optional<bool>>(record.at(bos->GetIndex())));
          }
          break;
        }
        case BaseSchema::kInteger: {
          DingoSchema<std::optional<int32_t>>* is = static_cast<DingoSchema<std::optional<int32_t>>*>(bs);
          if (is->IsKey()) {
            is->EncodeKey(key_buf, std::any_cast<std::optional<int32_t>>(record.at(is->GetIndex())));
          }
          break;
        }
        case BaseSchema::kLong: {
          DingoSchema<std::optional<int64_t>>* ls = static_cast<DingoSchema<std::optional<int64_t>>*>(bs);
          if (ls->IsKey()) {
            ls->EncodeKey(key_buf, std::any_cast<std::optional<int64_t>>(record.at(ls->GetIndex())));
          }
          break;
        }
        case BaseSchema::kDouble: {
          DingoSchema<std::optional<double>>* ds = static_cast<DingoSchema<std::optional<double>>*>(bs);
          if (ds->IsKey()) {
            ds->EncodeKey(key_buf, std::any_cast<std::optional<double>>(record.at(ds->GetIndex())));
          }
          break;
        }
        case BaseSchema::kString: {
          DingoSchema<std::optional<std::shared_ptr<std::string>>>* ss =
              static_cast<DingoSchema<std::optional<std::shared_ptr<std::string>>>*>(bs);
          if (ss->IsKey()) {
            ss->EncodeKey(key_buf,
                          std::any_cast<std::optional<std::shared_ptr<std::string>>>(record.at(ss->GetIndex())));
          }
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  key_buf->GetBytes(output);
  delete key_buf;
  return 0;
}

int RecordEncoder::EncodeValue(const std::vector<std::any>& record, std::string& output) {
  Buf* value_buf = new Buf(value_buf_size_, this->le_);
  value_buf->EnsureRemainder(4);
  value_buf->WriteInt(schema_version_);
  for (BaseSchema* bs : *schemas_) {
    if (bs != nullptr) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kBool: {
          DingoSchema<std::optional<bool>>* bos = static_cast<DingoSchema<std::optional<bool>>*>(bs);
          if (!bos->IsKey()) {
            bos->EncodeValue(value_buf, std::any_cast<std::optional<bool>>(record.at(bos->GetIndex())));
          }
          break;
        }
        case BaseSchema::kInteger: {
          DingoSchema<std::optional<int32_t>>* is = static_cast<DingoSchema<std::optional<int32_t>>*>(bs);
          if (!is->IsKey()) {
            is->EncodeValue(value_buf, std::any_cast<std::optional<int32_t>>(record.at(is->GetIndex())));
          }
          break;
        }
        case BaseSchema::kLong: {
          DingoSchema<std::optional<int64_t>>* ls = static_cast<DingoSchema<std::optional<int64_t>>*>(bs);
          if (!ls->IsKey()) {
            ls->EncodeValue(value_buf, std::any_cast<std::optional<int64_t>>(record.at(ls->GetIndex())));
          }
          break;
        }
        case BaseSchema::kDouble: {
          DingoSchema<std::optional<double>>* ds = static_cast<DingoSchema<std::optional<double>>*>(bs);
          if (!ds->IsKey()) {
            ds->EncodeValue(value_buf, std::any_cast<std::optional<double>>(record.at(ds->GetIndex())));
          }
          break;
        }
        case BaseSchema::kString: {
          DingoSchema<std::optional<std::shared_ptr<std::string>>>* ss =
              static_cast<DingoSchema<std::optional<std::shared_ptr<std::string>>>*>(bs);
          if (!ss->IsKey()) {
            ss->EncodeValue(value_buf,
                            std::any_cast<std::optional<std::shared_ptr<std::string>>>(record.at(ss->GetIndex())));
          }
          break;
        }
        default: {
          break;
        }
      }
    }
  }

  int ret = value_buf->GetBytes(output);
  delete value_buf;

  return ret;
}

int RecordEncoder::EncodeKeyPrefix(const std::vector<std::any>& record, int column_count, std::string& output) {
  Buf* key_prefix_buf = new Buf(key_buf_size_, this->le_);
  key_prefix_buf->EnsureRemainder(8);
  key_prefix_buf->WriteLong(common_id_);
  for (BaseSchema* bs : *schemas_) {
    if (bs != nullptr && bs->IsKey()) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kBool: {
          DingoSchema<std::optional<bool>>* bos = static_cast<DingoSchema<std::optional<bool>>*>(bs);
          bos->EncodeKeyPrefix(key_prefix_buf, std::any_cast<std::optional<bool>>(record.at(bos->GetIndex())));
          break;
        }
        case BaseSchema::kInteger: {
          DingoSchema<std::optional<int32_t>>* is = static_cast<DingoSchema<std::optional<int32_t>>*>(bs);
          is->EncodeKeyPrefix(key_prefix_buf, std::any_cast<std::optional<int32_t>>(record.at(is->GetIndex())));
          break;
        }
        case BaseSchema::kLong: {
          DingoSchema<std::optional<int64_t>>* ls = static_cast<DingoSchema<std::optional<int64_t>>*>(bs);
          ls->EncodeKeyPrefix(key_prefix_buf, std::any_cast<std::optional<int64_t>>(record.at(ls->GetIndex())));
          break;
        }
        case BaseSchema::kDouble: {
          DingoSchema<std::optional<double>>* ds = static_cast<DingoSchema<std::optional<double>>*>(bs);
          ds->EncodeKeyPrefix(key_prefix_buf, std::any_cast<std::optional<double>>(record.at(ds->GetIndex())));
          break;
        }
        case BaseSchema::kString: {
          DingoSchema<std::optional<std::shared_ptr<std::string>>>* ss =
              static_cast<DingoSchema<std::optional<std::shared_ptr<std::string>>>*>(bs);
          ss->EncodeKeyPrefix(key_prefix_buf, std::any_cast<std::optional<std::shared_ptr<std::string>>>(
                                                  record.at(ss->GetIndex())));
          break;
        }
        default: {
          break;
        }
      }
    }
    column_count--;
    if (column_count <= 0) {
      break;
    }
  }

  int ret = key_prefix_buf->GetBytes(output);
  delete key_prefix_buf;

  return ret;
}

int RecordEncoder::EncodeMaxKeyPrefix(std::string& output) const {
  if (common_id_ == UINT64_MAX) {
    // "CommonId reach max! Cannot generate Max Key Prefix"
    return -1;
  }

  Buf* max_key_prefix_buf = new Buf(key_buf_size_, this->le_);
  max_key_prefix_buf->EnsureRemainder(8);
  max_key_prefix_buf->WriteLong(common_id_ + 1);
  int ret = max_key_prefix_buf->GetBytes(output);
  delete max_key_prefix_buf;

  return ret;
}

int RecordEncoder::EncodeMinKeyPrefix(std::string& output) const {
  Buf* min_key_prefix_buf = new Buf(key_buf_size_, this->le_);
  min_key_prefix_buf->EnsureRemainder(8);
  min_key_prefix_buf->WriteLong(common_id_);
  int ret = min_key_prefix_buf->GetBytes(output);
  delete min_key_prefix_buf;

  return ret;
}

}  // namespace dingodb
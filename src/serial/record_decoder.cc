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

#include "record_decoder.h"

#include <memory>
#include <vector>

namespace dingodb {

RecordDecoder::RecordDecoder(int schema_version, std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
                             long common_id) {
  this->le_ = IsLE();
  Init(schema_version, schemas, common_id);
}

RecordDecoder::RecordDecoder(int schema_version, std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
                             long common_id, bool le) {
  this->le_ = le;
  Init(schema_version, schemas, common_id);
}

void RecordDecoder::Init(int schema_version, std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
                         long common_id) {
  this->schema_version_ = schema_version;
  FormatSchema(schemas, this->le_);
  this->schemas_ = schemas;
  this->common_id_ = common_id;
}

int RecordDecoder::Decode(const std::string& key, const std::string& value, std::vector<std::any>& record) {
  Buf* key_buf = new Buf(key, this->le_);
  Buf* value_buf = new Buf(value, this->le_);
  if (key_buf->ReadLong() != common_id_) {
    //"Wrong Common Id"
    delete key_buf;
    delete value_buf;
    return -1;
  }

  if (key_buf->ReverseReadInt() != codec_version_) {
    //"Wrong Codec Version"
    delete key_buf;
    delete value_buf;
    return -1;
  }

  if (value_buf->ReadInt() != schema_version_) {
    //"Wrong Schema Version"
    delete key_buf;
    delete value_buf;
    return -1;
  }

  record.resize(schemas_->size());
  for (const auto& bs : *schemas_) {
    if (bs) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kBool: {
          auto bos = std::dynamic_pointer_cast<DingoSchema<std::optional<bool>>>(bs);
          if (bos->IsKey()) {
            record.at(bos->GetIndex()) = bos->DecodeKey(key_buf);
          } else {
            record.at(bos->GetIndex()) = bos->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kInteger: {
          auto is = std::dynamic_pointer_cast<DingoSchema<std::optional<int32_t>>>(bs);
          if (is->IsKey()) {
            record.at(is->GetIndex()) = is->DecodeKey(key_buf);
          } else {
            record.at(is->GetIndex()) = is->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kFloat: {
          auto fs = std::dynamic_pointer_cast<DingoSchema<std::optional<float>>>(bs);
          if (fs->IsKey()) {
            record.at(fs->GetIndex()) = fs->DecodeKey(key_buf);
          } else {
            record.at(fs->GetIndex()) = fs->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kLong: {
          auto ls = std::dynamic_pointer_cast<DingoSchema<std::optional<int64_t>>>(bs);
          if (ls->IsKey()) {
            record.at(ls->GetIndex()) = ls->DecodeKey(key_buf);
          } else {
            record.at(ls->GetIndex()) = ls->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kDouble: {
          auto ds = std::dynamic_pointer_cast<DingoSchema<std::optional<double>>>(bs);
          if (ds->IsKey()) {
            record.at(ds->GetIndex()) = ds->DecodeKey(key_buf);
          } else {
            record.at(ds->GetIndex()) = ds->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kString: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::string>>>>(bs);
          if (ss->IsKey()) {
            record.at(ss->GetIndex()) = ss->DecodeKey(key_buf);
          } else {
            record.at(ss->GetIndex()) = ss->DecodeValue(value_buf);
          }
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  delete key_buf;
  delete value_buf;
  return 0;
}

int RecordDecoder::DecodeKey(const std::string& key, std::vector<std::any>& record /*output*/) {
  Buf* key_buf = new Buf(key, this->le_);

  if (key_buf->ReadLong() != common_id_) {
    //"Wrong Common Id"
    delete key_buf;
    return -1;
  }

  if (key_buf->ReverseReadInt() != codec_version_) {
    //"Wrong Codec Version"
    delete key_buf;
    return -1;
  }

  record.resize(schemas_->size());
  for (const auto& bs : *schemas_) {
    if (bs) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kBool: {
          auto bos = std::dynamic_pointer_cast<DingoSchema<std::optional<bool>>>(bs);
          if (bos->IsKey()) {
            record.at(bos->GetIndex()) = bos->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kInteger: {
          auto is = std::dynamic_pointer_cast<DingoSchema<std::optional<int32_t>>>(bs);
          if (is->IsKey()) {
            record.at(is->GetIndex()) = is->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kFloat: {
          auto fs = std::dynamic_pointer_cast<DingoSchema<std::optional<float>>>(bs);
          if (fs->IsKey()) {
            record.at(fs->GetIndex()) = fs->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kLong: {
          auto ls = std::dynamic_pointer_cast<DingoSchema<std::optional<int64_t>>>(bs);
          if (ls->IsKey()) {
            record.at(ls->GetIndex()) = ls->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kDouble: {
          auto ds = std::dynamic_pointer_cast<DingoSchema<std::optional<double>>>(bs);
          if (ds->IsKey()) {
            record.at(ds->GetIndex()) = ds->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kString: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::string>>>>(bs);
          if (ss->IsKey()) {
            record.at(ss->GetIndex()) = ss->DecodeKey(key_buf);
          }
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  delete key_buf;
  return 0;
}

int RecordDecoder::Decode(const KeyValue& key_value, std::vector<std::any>& record) {
  return Decode(*key_value.GetKey(), *key_value.GetValue(), record);
}

int RecordDecoder::Decode(const pb::common::KeyValue& key_value, std::vector<std::any>& record) {
  return Decode(key_value.key(), key_value.value(), record);
}

int RecordDecoder::Decode(const std::string& key, const std::string& value, const std::vector<int>& column_indexes,
                          std::vector<std::any>& record) {
  Buf* key_buf = new Buf(key, this->le_);
  Buf* value_buf = new Buf(value, this->le_);
  if (key_buf->ReadLong() != common_id_) {
    //"Wrong Common Id"
    delete key_buf;
    delete value_buf;
    return -1;
  }

  if (key_buf->ReverseReadInt() != codec_version_) {
    //"Wrong Codec Version"
    delete key_buf;
    delete value_buf;
    return -1;
  }

  if (value_buf->ReadInt() != schema_version_) {
    //"Wrong Schema Version"
    delete key_buf;
    delete value_buf;
    return -1;
  }

  record.resize(schemas_->size());
  for (const auto& bs : *schemas_) {
    if (bs) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kBool: {
          auto bos = std::dynamic_pointer_cast<DingoSchema<std::optional<bool>>>(bs);
          if (VectorFind(column_indexes, bos->GetIndex())) {
            if (bos->IsKey()) {
              record.at(bos->GetIndex()) = bos->DecodeKey(key_buf);
            } else {
              record.at(bos->GetIndex()) = bos->DecodeValue(value_buf);
            }
          } else {
            record.at(bos->GetIndex()) = (std::optional<bool>)std::nullopt;
            if (bos->IsKey()) {
              bos->SkipKey(key_buf);
            } else {
              bos->SkipValue(value_buf);
            }
          }
          break;
        }
        case BaseSchema::kInteger: {
          auto is = std::dynamic_pointer_cast<DingoSchema<std::optional<int32_t>>>(bs);
          if (VectorFind(column_indexes, is->GetIndex())) {
            if (is->IsKey()) {
              record.at(is->GetIndex()) = is->DecodeKey(key_buf);
            } else {
              record.at(is->GetIndex()) = is->DecodeValue(value_buf);
            }
          } else {
            record.at(is->GetIndex()) = (std::optional<int32_t>)std::nullopt;
            if (is->IsKey()) {
              is->SkipKey(key_buf);
            } else {
              is->SkipValue(value_buf);
            }
          }
          break;
        }
        case BaseSchema::kFloat: {
          auto fs = std::dynamic_pointer_cast<DingoSchema<std::optional<float>>>(bs);
          if (VectorFind(column_indexes, fs->GetIndex())) {
            if (fs->IsKey()) {
              record.at(fs->GetIndex()) = fs->DecodeKey(key_buf);
            } else {
              record.at(fs->GetIndex()) = fs->DecodeValue(value_buf);
            }
          } else {
            record.at(fs->GetIndex()) = (std::optional<float>)std::nullopt;
            if (fs->IsKey()) {
              fs->SkipKey(key_buf);
            } else {
              fs->SkipValue(value_buf);
            }
          }
          break;
        }
        case BaseSchema::kLong: {
          auto ls = std::dynamic_pointer_cast<DingoSchema<std::optional<int64_t>>>(bs);
          if (VectorFind(column_indexes, ls->GetIndex())) {
            if (ls->IsKey()) {
              record.at(ls->GetIndex()) = ls->DecodeKey(key_buf);
            } else {
              record.at(ls->GetIndex()) = ls->DecodeValue(value_buf);
            }
          } else {
            record.at(ls->GetIndex()) = (std::optional<int64_t>)std::nullopt;
            if (ls->IsKey()) {
              ls->SkipKey(key_buf);
            } else {
              ls->SkipValue(value_buf);
            }
          }
          break;
        }
        case BaseSchema::kDouble: {
          auto ds = std::dynamic_pointer_cast<DingoSchema<std::optional<double>>>(bs);
          if (VectorFind(column_indexes, ds->GetIndex())) {
            if (ds->IsKey()) {
              record.at(ds->GetIndex()) = ds->DecodeKey(key_buf);
            } else {
              record.at(ds->GetIndex()) = ds->DecodeValue(value_buf);
            }
          } else {
            record.at(ds->GetIndex()) = (std::optional<double>)std::nullopt;
            if (ds->IsKey()) {
              ds->SkipKey(key_buf);
            } else {
              ds->SkipValue(value_buf);
            }
          }
          break;
        }
        case BaseSchema::kString: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::string>>>>(bs);
          if (VectorFind(column_indexes, ss->GetIndex())) {
            if (ss->IsKey()) {
              record.at(ss->GetIndex()) = ss->DecodeKey(key_buf);
            } else {
              record.at(ss->GetIndex()) = ss->DecodeValue(value_buf);
            }
          } else {
            record.at(ss->GetIndex()) = (std::optional<std::shared_ptr<std::string>>)std::nullopt;
            if (ss->IsKey()) {
              ss->SkipKey(key_buf);
            } else {
              ss->SkipValue(value_buf);
            }
          }
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  delete key_buf;
  delete value_buf;
  return 0;
}

int RecordDecoder::Decode(const KeyValue& key_value, const std::vector<int>& column_indexes,
                          std::vector<std::any>& record) {
  return Decode(*key_value.GetKey(), *key_value.GetValue(), column_indexes, record);
}

int RecordDecoder::Decode(const pb::common::KeyValue& key_value, const std::vector<int>& column_indexes,
                          std::vector<std::any>& record) {
  return Decode(key_value.key(), key_value.value(), column_indexes, record);
}

}  // namespace dingodb
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

#include "common/logging.h"
#include "counter.h"

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
        case BaseSchema::kBoolList: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>>(bs);
          if (ss->IsKey()) {
            record.at(ss->GetIndex()) = ss->DecodeKey(key_buf);
          } else {
            record.at(ss->GetIndex()) = ss->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kStringList: {
          auto ss =
              std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>>(bs);
          if (ss->IsKey()) {
            record.at(ss->GetIndex()) = ss->DecodeKey(key_buf);
          } else {
            record.at(ss->GetIndex()) = ss->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kDoubleList: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>>(bs);
          if (ss->IsKey()) {
            record.at(ss->GetIndex()) = ss->DecodeKey(key_buf);
          } else {
            record.at(ss->GetIndex()) = ss->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kFloatList: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>>(bs);
          if (ss->IsKey()) {
            record.at(ss->GetIndex()) = ss->DecodeKey(key_buf);
          } else {
            record.at(ss->GetIndex()) = ss->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kIntegerList: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>>(bs);
          if (ss->IsKey()) {
            record.at(ss->GetIndex()) = ss->DecodeKey(key_buf);
          } else {
            record.at(ss->GetIndex()) = ss->DecodeValue(value_buf);
          }
          break;
        }
        case BaseSchema::kLongList: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>>(bs);
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
  int index = 0;
  for (const auto& bs : *schemas_) {
    if (bs) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kBool: {
          auto bos = std::dynamic_pointer_cast<DingoSchema<std::optional<bool>>>(bs);
          if (bos->IsKey()) {
            record.at(index) = bos->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kInteger: {
          auto is = std::dynamic_pointer_cast<DingoSchema<std::optional<int32_t>>>(bs);
          if (is->IsKey()) {
            record.at(index) = is->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kFloat: {
          auto fs = std::dynamic_pointer_cast<DingoSchema<std::optional<float>>>(bs);
          if (fs->IsKey()) {
            record.at(index) = fs->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kLong: {
          auto ls = std::dynamic_pointer_cast<DingoSchema<std::optional<int64_t>>>(bs);
          if (ls->IsKey()) {
            record.at(index) = ls->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kDouble: {
          auto ds = std::dynamic_pointer_cast<DingoSchema<std::optional<double>>>(bs);
          if (ds->IsKey()) {
            record.at(index) = ds->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kString: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::string>>>>(bs);
          if (ss->IsKey()) {
            record.at(index) = ss->DecodeKey(key_buf);
          }
          break;
        }
        case BaseSchema::kDoubleList: {
          auto ss = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<double>>>>(bs);
          if (ss->IsKey()) {
            record.at(index) = ss->DecodeKey(key_buf);
          }
          break;
        }
        default: {
          break;
        }
      }
    }
    index++;
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

template <typename T>
void DecodeOrSkip(const std::shared_ptr<DingoSchema<std::optional<T>>>& schema, Buf& key_buf, Buf& value_buf,
                  const std::vector<int>& column_indexes, std::vector<std::any>& record, int& n, int& m) {
  if (VectorFind(column_indexes, schema->GetIndex(), n)) {
    if (schema->IsKey()) {
      record.at(n) = schema->DecodeKey(&key_buf);
    } else {
      record.at(n) = schema->DecodeValue(&value_buf);
    }
    n++;
  } else {
    // record.at(schema->GetIndex()) = std::nullopt;
    if (schema->IsKey()) {
      schema->SkipKey(&key_buf);
    } else {
      schema->SkipValue(&value_buf);
    }
  }
  m++;
}

inline bool VectorPairFind(const std::vector<std::pair<int, int>>& indexed_mapping_index, int m, int n,
                           int& recordIndex) {
  int first = indexed_mapping_index[n].first;
  int second = indexed_mapping_index[n].second;
  DINGO_LOG(DEBUG) << "(" << first << ", " << second << ", " << m << "," << n << ") ";
  if (first == m) {
    recordIndex = second;
    return true;
  } else {
    return false;
  }
}

template <typename T>
void DecodeOrSkip1(const std::shared_ptr<DingoSchema<std::optional<T>>>& schema, Buf& key_buf, Buf& value_buf,
                   const std::vector<std::pair<int, int>>& indexed_mapping_index, std::vector<std::any>& record, int& n,
                   int& m) {
  int recordIndex = 0;
  if (VectorPairFind(indexed_mapping_index, m, n, recordIndex)) {
    DINGO_LOG(DEBUG) << "recordIndex" << recordIndex;
    if (schema->IsKey()) {
      record.at(recordIndex) = schema->DecodeKey(&key_buf);
    } else {
      record.at(recordIndex) = schema->DecodeValue(&value_buf);
    }
    n++;
  } else {
    // record.at(schema->GetIndex()) = std::nullopt;
    if (schema->IsKey()) {
      schema->SkipKey(&key_buf);
    } else {
      schema->SkipValue(&value_buf);
    }
  }
  m++;
}

int RecordDecoder::Decode(const std::string& key, const std::string& value, const std::vector<int>& column_indexes,
                          std::vector<std::any>& record) {
  Buf key_buf(key, this->le_);
  Buf value_buf(value, this->le_);
  if (key_buf.ReadLong() != common_id_ || key_buf.ReverseReadInt() != codec_version_ ||
      value_buf.ReadInt() != schema_version_) {
    return -1;
  }
  record.resize(column_indexes.size());
  int n = 0;
  int m = 0;
  // column_indexes [6,0,2,4]
  std::vector<int> mapping_index;
  for (int i : column_indexes) {
    mapping_index.push_back(i);
  }

  std::vector<std::pair<int, int>> indexed_mapping_index;

  // index
  for (int i = 0; i < mapping_index.size(); i++) {
    indexed_mapping_index.push_back(std::make_pair(mapping_index[i], i));
  }

  // sort indexed_mapping_index
  std::sort(indexed_mapping_index.begin(), indexed_mapping_index.end());

  // sort end indexed_mapping_index and mapping_index
  DINGO_LOG(DEBUG) << "indexed_mapping_index: ";
  for (auto p : indexed_mapping_index) {
    DINGO_LOG(DEBUG) << "(" << p.first << ", " << p.second << ") ";
  }

  for (auto iter = schemas_->begin(); iter != schemas_->end(); ++iter) {
    if (column_indexes.size() == n) {
      return 0;
    }
    const auto& bs = *iter;
    if (bs) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kBool: {
          DecodeOrSkip1(std::dynamic_pointer_cast<DingoSchema<std::optional<bool>>>(bs), key_buf, value_buf,
                        indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kInteger: {
          DecodeOrSkip1(std::dynamic_pointer_cast<DingoSchema<std::optional<int32_t>>>(bs), key_buf, value_buf,
                        indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kFloat: {
          DecodeOrSkip1(std::dynamic_pointer_cast<DingoSchema<std::optional<float>>>(bs), key_buf, value_buf,
                        indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kLong: {
          DecodeOrSkip1(std::dynamic_pointer_cast<DingoSchema<std::optional<int64_t>>>(bs), key_buf, value_buf,
                        indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kDouble: {
          DecodeOrSkip1(std::dynamic_pointer_cast<DingoSchema<std::optional<double>>>(bs), key_buf, value_buf,
                        indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kString: {
          DecodeOrSkip1(std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::string>>>>(bs),
                        key_buf, value_buf, indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kBoolList: {
          DecodeOrSkip1(std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>>(bs),
                        key_buf, value_buf, indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kStringList: {
          DecodeOrSkip1(
              std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>>>(bs),
              key_buf, value_buf, indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kDoubleList: {
          DecodeOrSkip1(std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>>(bs),
                        key_buf, value_buf, indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kFloatList: {
          DecodeOrSkip1(std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<float>>>>>(bs),
                        key_buf, value_buf, indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kIntegerList: {
          DecodeOrSkip1(
              std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>>(bs), key_buf,
              value_buf, indexed_mapping_index, record, n, m);
          break;
        }
        case BaseSchema::kLongList: {
          DecodeOrSkip1(
              std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>>(bs), key_buf,
              value_buf, indexed_mapping_index, record, n, m);
          break;
        }
        default: {
          break;
        }
      }
    }
  }
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
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

#include "utils.h"

namespace dingodb {

void SortSchema(std::vector<BaseSchema*>* schemas) {
  int flag = 1;
  for (int i = 0; i < schemas->size() - flag; i++) {
    BaseSchema* bs = schemas->at(i);
    if (bs != nullptr) {
      if ((!bs->IsKey()) && (bs->GetLength() == 0)) {
        int target = schemas->size() - flag++;
        BaseSchema* ts = schemas->at(target);
        while ((ts->GetLength() == 0) || ts->IsKey()) {
          target--;
          if (target == i) {
            return;
          }
          flag++;
        }
        schemas->at(i) = ts;
        schemas->at(target) = bs;
      }
    }
  }
}
void FormatSchema(std::vector<BaseSchema *> *schemas, bool le) {
  for (BaseSchema* bs : *schemas) {
    if (bs != nullptr) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kInteger: {
          DingoSchema<std::optional<int32_t>>* is = static_cast<DingoSchema<std::optional<int32_t>>*>(bs);
          is->SetIsLe(le);
          break;
        }
        case BaseSchema::kLong: {
          DingoSchema<std::optional<int64_t>>* ls = static_cast<DingoSchema<std::optional<int64_t>>*>(bs);
          ls->SetIsLe(le);
          break;
        }
        case BaseSchema::kDouble: {
          DingoSchema<std::optional<double>>* ds = static_cast<DingoSchema<std::optional<double>>*>(bs);
          ds->SetIsLe(le);
          break;
        }
        default: {
          break;
        }
      }
    }
  }
}
int32_t* GetApproPerRecordSize(std::vector<BaseSchema*>* schemas) {
  int32_t key_size = 8;
  int32_t value_size = 0;
  for (BaseSchema* bs : *schemas) {
    if (bs != nullptr) {
      if (bs->IsKey()) {
        key_size += (bs->GetLength() == 0 ? 100 : bs->GetLength());
      } else {
        value_size += (bs->GetLength() == 0 ? 100 : bs->GetLength());
      }
    }
  }
  int32_t* size = new int32_t[2]();
  size[0] = key_size;
  size[1] = value_size;
  return size;
}
bool VectorFindAndRemove(std::vector<int>* v, int t) {
  for (std::vector<int>::iterator it = v->begin(); it != v->end(); it++) {
    if (*it == t) {
      v->erase(it);
      return true;
    }
  }
  return false;
}
std::vector<BaseSchema*>* TableDefinitionToDingoSchema(pb::meta::TableDefinition* td) {
  std::vector<BaseSchema*> *schemas = new std::vector<BaseSchema*>(td->columns_size());
  int i = 0;
  for (const pb::meta::ColumnDefinition& cd : td->columns()) {
      pb::meta::ElementType type = cd.element_type();
      switch(type) {
        case pb::meta::ELEM_TYPE_DOUBLE: {
          DingoSchema<std::optional<double>> *double_schema = new DingoSchema<std::optional<double>>();
          double_schema->SetAllowNull(cd.nullable());
          double_schema->SetIndex(i);
          double_schema->SetIsKey(cd.indexofkey() >= 0);
          schemas->at(i++) = double_schema;
          break;
        }
        case pb::meta::ELEM_TYPE_FLOAT: {
          //TODO
          schemas->at(i++) = nullptr;
          break;
        }
        case pb::meta::ELEM_TYPE_INT32: {
          DingoSchema<std::optional<int32_t>> *integer_schema = new DingoSchema<std::optional<int32_t>>();
          integer_schema->SetAllowNull(cd.nullable());
          integer_schema->SetIndex(i);
          integer_schema->SetIsKey(cd.indexofkey() >= 0);
          schemas->at(i++) = integer_schema;
          break;
        }
        case pb::meta::ELEM_TYPE_INT64: {
          DingoSchema<std::optional<int64_t>> *long_schema = new DingoSchema<std::optional<int64_t>>();
          long_schema->SetAllowNull(cd.nullable());
          long_schema->SetIndex(i);
          long_schema->SetIsKey(cd.indexofkey() >= 0);
          schemas->at(i++) = long_schema;
          break;
        }
        case pb::meta::ELEM_TYPE_UINT32: {
          DingoSchema<std::optional<int32_t>> *integer_schema = new DingoSchema<std::optional<int32_t>>();
          integer_schema->SetAllowNull(cd.nullable());
          integer_schema->SetIndex(i);
          integer_schema->SetIsKey(cd.indexofkey() >= 0);
          schemas->at(i++) = integer_schema;
          break;
        }
        case pb::meta::ELEM_TYPE_UINT64: {
          DingoSchema<std::optional<int64_t>> *long_schema = new DingoSchema<std::optional<int64_t>>();
          long_schema->SetAllowNull(cd.nullable());
          long_schema->SetIndex(i);
          long_schema->SetIsKey(cd.indexofkey() >= 0);
          schemas->at(i++) = long_schema;
          break;
        }
        case pb::meta::ELEM_TYPE_BOOLEAN: {
          DingoSchema<std::optional<bool>> *bool_schema = new DingoSchema<std::optional<bool>>();
          bool_schema->SetAllowNull(cd.nullable());
          bool_schema->SetIndex(i);
          bool_schema->SetIsKey(cd.indexofkey() >= 0);
          schemas->at(i++) = bool_schema;
          break;
        }
        case pb::meta::ELEM_TYPE_STRING: {
          DingoSchema<std::optional<std::reference_wrapper<std::string>>> *string_schema = new DingoSchema<std::optional<std::reference_wrapper<std::string>>>();
          string_schema->SetAllowNull(cd.nullable());
          string_schema->SetIndex(i);
          string_schema->SetIsKey(cd.indexofkey() >= 0);
          schemas->at(i++) = string_schema;
          break;
        }
        case pb::meta::ELEM_TYPE_BYTES: {
          DingoSchema<std::optional<std::reference_wrapper<std::string>>> *string_schema = new DingoSchema<std::optional<std::reference_wrapper<std::string>>>();
          string_schema->SetAllowNull(cd.nullable());
          string_schema->SetIndex(i);
          string_schema->SetIsKey(cd.indexofkey() >= 0);
          schemas->at(i++) = string_schema;
          break;
        }
        case pb::meta::ELEM_TYPE_FIX32: {
          //TODO
          schemas->at(i++) = nullptr;
          break;
        }
        case pb::meta::ELEM_TYPE_FIX64: {
          //TODO
          schemas->at(i++) = nullptr;
          break;
        }
        case pb::meta::ELEM_TYPE_SFIX32: {
          //TODO
          schemas->at(i++) = nullptr;
          break;
        }
        case pb::meta::ELEM_TYPE_SFIX64: {
          //TODO
          schemas->at(i++) = nullptr;
          break;
        }
        case pb::meta::ElementType_INT_MIN_SENTINEL_DO_NOT_USE_: {
          //TODO
          schemas->at(i++) = nullptr;
          break;
        }
        case pb::meta::ElementType_INT_MAX_SENTINEL_DO_NOT_USE_: {
          //TODO
          schemas->at(i++) = nullptr;
          break;
        }
        default: {
          schemas->at(i++) = nullptr;
          break;
        }
      }
  }

  return schemas;
}
std::vector<std::any>* ElementToSql(pb::meta::TableDefinition* td, std::vector<std::any>* record) {
  for (int i = 0; i < td->columns_size(); i++) {
    pb::meta::ColumnDefinition cd = td->columns().at(i);
    switch (cd.sql_type()) {
      case pb::meta::SQL_TYPE_BOOLEAN: {
        break;
      }
      case pb::meta::SQL_TYPE_INTEGER: {
        break;
      }
      case pb::meta::SQL_TYPE_BIGINT: {
        break;
      }
      case pb::meta::SQL_TYPE_DOUBLE: {
        break;
      }
      case pb::meta::SQL_TYPE_FLOAT: {
        break;
      }
      case pb::meta::SQL_TYPE_DATE: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_TIME: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_TIMESTAMP: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_VARCHAR: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_ARRAY: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_MULTISET: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_BYTES: {
        break;
      }
      case pb::meta::SQL_TYPE_ANY: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SqlType_INT_MIN_SENTINEL_DO_NOT_USE_: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SqlType_INT_MAX_SENTINEL_DO_NOT_USE_: {
        record[i] = record[i]; //TODO
        break;
      }
      default: {
        break;
      }
    }
  }
  return record;
}
std::vector<std::any>* SqlToElement(pb::meta::TableDefinition* td, std::vector<std::any>* record) {
  for (int i = 0; i < td->columns_size(); i++) {
    pb::meta::ColumnDefinition cd = td->columns().at(i);
    switch (cd.sql_type()) {
      case pb::meta::SQL_TYPE_BOOLEAN: {
        break;
      }
      case pb::meta::SQL_TYPE_INTEGER: {
        break;
      }
      case pb::meta::SQL_TYPE_BIGINT: {
        break;
      }
      case pb::meta::SQL_TYPE_DOUBLE: {
        break;
      }
      case pb::meta::SQL_TYPE_FLOAT: {
        break;
      }
      case pb::meta::SQL_TYPE_DATE: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_TIME: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_TIMESTAMP: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_VARCHAR: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_ARRAY: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_MULTISET: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SQL_TYPE_BYTES: {
        break;
      }
      case pb::meta::SQL_TYPE_ANY: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SqlType_INT_MIN_SENTINEL_DO_NOT_USE_: {
        record[i] = record[i]; //TODO
        break;
      }
      case pb::meta::SqlType_INT_MAX_SENTINEL_DO_NOT_USE_: {
        record[i] = record[i]; //TODO
        break;
      }
      default: {
        break;
      }
    }
  }
  return record;
}
bool IsLE() {
  uint32_t i = 1;
  char* c = (char*) &i;
  return *c == 1;
}

}  // namespace dingodb
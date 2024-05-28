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

#include <cstdint>
#include <memory>
#include <vector>

#include "common/helper.h"
#include "glog/logging.h"
#include "serial/schema/base_schema.h"

namespace dingodb {

void SortSchema(std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas) {
  int flag = 1;
  for (int i = 0; i < schemas->size() - flag; i++) {
    auto bs = schemas->at(i);
    if (bs != nullptr) {
      if ((!bs->IsKey()) && (bs->GetLength() == 0)) {
        int target = schemas->size() - flag++;
        auto ts = schemas->at(target);
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

void FormatSchema(std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas, bool le) {
  for (auto& bs : *schemas) {
    if (bs != nullptr) {
      BaseSchema::Type type = bs->GetType();
      switch (type) {
        case BaseSchema::kInteger: {
          auto is = std::dynamic_pointer_cast<DingoSchema<std::optional<int32_t>>>(bs);
          is->SetIsLe(le);
          break;
        }
        case BaseSchema::kLong: {
          auto ls = std::dynamic_pointer_cast<DingoSchema<std::optional<int64_t>>>(bs);
          ls->SetIsLe(le);
          break;
        }
        case BaseSchema::kDouble: {
          auto ds = std::dynamic_pointer_cast<DingoSchema<std::optional<double>>>(bs);
          ds->SetIsLe(le);
          break;
        }
        case BaseSchema::kIntegerList: {
          auto ds = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<int32_t>>>>>(bs);
          ds->SetIsLe(le);
          break;
        }
        case BaseSchema::kLongList: {
          auto ds = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<int64_t>>>>>(bs);
          ds->SetIsLe(le);
          break;
        }
        case BaseSchema::kDoubleList: {
          auto ds = std::dynamic_pointer_cast<DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>>(bs);
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

int32_t* GetApproPerRecordSize(std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas) {
  int32_t key_size = 8;
  int32_t value_size = 0;

  for (const auto& bs : *schemas) {
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

// bool VectorFind(const std::vector<int>& v, int t) {
//   for (int it : v) {
//     if (it == t) {
//       return true;
//     }
//   }
//   return false;
// }

// bool VectorFind(const std::vector<int>& v, int t) {
//   return std::binary_search(v.begin(), v.end(), t);
// }

// bool VectorFind(const std::vector<int>& v, int t, int n) {
//   return v[n] == t;
// }

bool IsLE() {
  uint32_t i = 1;
  char* c = (char*)&i;
  return *c == 1;
}

std::string ConvertColumnValueToString(const pb::meta::ColumnDefinition& column_definition,
                                       const std::any& value) {  // NOLINT
  std::ostringstream ostr;

  if (value.type() == typeid(std::optional<std::string>)) {
    auto v = std::any_cast<std::optional<std::string>>(value);
    ostr << v.value_or("");
  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::string>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::string>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      if (column_definition.sql_type() == "BINARY" || column_definition.sql_type() == "ANY") {
        ostr << Helper::StringToHex(*ptr);
      } else {
        ostr << *ptr;
      }
    }
  } else if (value.type() == typeid(std::optional<int32_t>)) {
    auto v = std::any_cast<std::optional<int32_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<uint32_t>)) {
    auto v = std::any_cast<std::optional<uint32_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<int64_t>)) {
    auto v = std::any_cast<std::optional<int64_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<int64_t>)) {
    auto v = std::any_cast<std::optional<int64_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<double>)) {
    auto v = std::any_cast<std::optional<double>>(value);
    ostr << v.value_or(0.0);
  } else if (value.type() == typeid(std::optional<float>)) {
    auto v = std::any_cast<std::optional<float>>(value);
    ostr << v.value_or(0.0);
  } else if (value.type() == typeid(std::optional<bool>)) {
    auto v = std::any_cast<std::optional<bool>>(value);
    ostr << v.value_or(false);

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<bool>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<bool>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<std::string>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<std::string>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<double>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<double>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<float>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<float>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<int32_t>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<int32_t>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<int64_t>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<int64_t>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else {
    ostr << fmt::format("unknown type({})", value.type().name());
  }

  return ostr.str();
}

pb::common::Schema::Type TransformSchemaType(const std::string& name) {
  static std::map<std::string, dingodb::pb::common::Schema::Type> schema_type_map = {
      std::make_pair("CHAR", dingodb::pb::common::Schema::STRING),
      std::make_pair("VARCHAR", dingodb::pb::common::Schema::STRING),
      std::make_pair("ANY", dingodb::pb::common::Schema::STRING),
      std::make_pair("BINARY", dingodb::pb::common::Schema::STRING),
      std::make_pair("INT", dingodb::pb::common::Schema::INTEGER),
      std::make_pair("INTEGER", dingodb::pb::common::Schema::INTEGER),
      std::make_pair("BIGINT", dingodb::pb::common::Schema::LONG),
      std::make_pair("DATE", dingodb::pb::common::Schema::LONG),
      std::make_pair("TIME", dingodb::pb::common::Schema::LONG),
      std::make_pair("TIMESTAMP", dingodb::pb::common::Schema::LONG),
      std::make_pair("DOUBLE", dingodb::pb::common::Schema::DOUBLE),
      std::make_pair("BOOL", dingodb::pb::common::Schema::BOOL),
      std::make_pair("BOOLEAN", dingodb::pb::common::Schema::BOOL),
      std::make_pair("FLOAT", dingodb::pb::common::Schema::FLOAT),
      std::make_pair("LONG", dingodb::pb::common::Schema::LONG),

      std::make_pair("ARRAY_BOOL", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("ARRAY_BOOLEAN", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("ARRAY_INTEGER", dingodb::pb::common::Schema::INTEGERLIST),
      std::make_pair("ARRAY_FLOAT", dingodb::pb::common::Schema::FLOATLIST),
      std::make_pair("ARRAY_DOUBLE", dingodb::pb::common::Schema::DOUBLELIST),
      std::make_pair("ARRAY_LONG", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_BIGINT", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_DATE", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_TIME", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_TIMESTAMP", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_CHAR", dingodb::pb::common::Schema::STRINGLIST),
      std::make_pair("ARRAY_VARCHAR", dingodb::pb::common::Schema::STRINGLIST),

      std::make_pair("MULTISET_BOOL", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("MULTISET_BOOLEAN", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("MULTISET_INTEGER", dingodb::pb::common::Schema::INTEGERLIST),
      std::make_pair("MULTISET_FLOAT", dingodb::pb::common::Schema::FLOATLIST),
      std::make_pair("MULTISET_DOUBLE", dingodb::pb::common::Schema::DOUBLELIST),
      std::make_pair("MULTISET_LONG", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_BIGINT", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_DATE", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_TIME", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_TIMESTAMP", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_CHAR", dingodb::pb::common::Schema::STRINGLIST),
      std::make_pair("MULTISET_VARCHAR", dingodb::pb::common::Schema::STRINGLIST),
  };

  auto it = schema_type_map.find(Helper::ToUpper(name));
  CHECK(it != schema_type_map.end()) << "Not found schema type: " << Helper::ToUpper(name);

  return it->second;
}

}  // namespace dingodb

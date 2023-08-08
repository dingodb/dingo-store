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

}  // namespace dingodb

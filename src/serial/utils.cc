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
#include <iostream>

namespace dingodb {

void SortSchema(vector<BaseSchema*>* schemas) {
  int flag = 1;
  for (int i = 0; i < schemas->size() - flag; i++) {
    BaseSchema* bs = schemas->at(i);
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

int32_t* GetApproPerRecordSize(vector<BaseSchema*>* schemas) {
  int32_t key_size = 8;
  int32_t value_size = 0;
  for (auto *bs : *schemas) {
    if (bs->IsKey()) {
      if (bs->AllowNull()) {
        key_size += (bs->GetLength() == 0 ? 100 : (bs->GetLength() + 1));
      } else {
        key_size += (bs->GetLength() == 0 ? 100 : bs->GetLength());
      }
    } else {
      if (bs->AllowNull()) {
        value_size += (bs->GetLength() == 0 ? 100 : (bs->GetLength() + 1));
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

bool VectorFindAndRemove(vector<int>* v, int t) {
  for (vector<int>::iterator it = v->begin(); it != v->end(); it++) {
    if (*it == t) {
      v->erase(it);
      return true;
    }
  }
  return false;
}

}  // namespace dingodb
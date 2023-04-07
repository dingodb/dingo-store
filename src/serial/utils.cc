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

void sortSchema(vector<BaseSchema*> *schemas) {
    vector<BaseSchema*> nv;
    int flag = 1;
    for (int i = 0; i < schemas->size() - flag; i++) {
        BaseSchema* bs = schemas->at(i);
        if ((!bs->isKey()) && (bs->getLength() == 0)) {
            int target = schemas->size() - flag++;
            BaseSchema* ts = schemas->at(target);
            while ((ts->getLength() == 0) || ts->isKey()) {
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

int* getApproPerRecordSize(vector<BaseSchema*> *schemas) {
    int key_size = 8;
    int value_size = 0;
    for (vector<BaseSchema*>::iterator it = schemas->begin(); it != schemas->end(); it++) {
        BaseSchema *bs = *it;
        if (bs->isKey()) {
            if (bs->allowNull()) {
                key_size += (bs->getLength() == 0 ? 100 : (bs->getLength() + 1));
            } else {
                key_size += (bs->getLength() == 0 ? 100 : bs->getLength());
            }
        } else {
            if (bs->allowNull()) {
                value_size += (bs->getLength() == 0 ? 100 : (bs->getLength() + 1));
            } else {
                value_size += (bs->getLength() == 0 ? 100 : bs->getLength());
            }
        }
    }
    int size[2];
    size[0] = key_size;
    size[1] = value_size;
    int *size1 = &size[0];
    return size1;
}

bool vectorFindAndRemove(vector<int> *v, int t) {
    for (vector<int>::iterator it = v->begin(); it != v->end(); it++) {
        if (*it == t) {
            v->erase(it);
            return 1;
        }
    }
    return 0;
}

}
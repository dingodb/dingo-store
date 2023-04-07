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

namespace dingodb {

RecordDecoder::RecordDecoder(int schema_version, vector<BaseSchema*> *schemas, long common_id) {
    this->schema_version = schema_version;
    this->schemas = schemas;
    this->common_id = common_id;
    sortSchema(schemas);
}
vector<any>* RecordDecoder::decode(KeyValue *keyvalue) {
    Buf *key_buf = new Buf(keyvalue->getKey());
    Buf *value_buf = new Buf(keyvalue->getValue());
    if (key_buf->readLong() != common_id) {
        //"Wrong Common Id"
    }
    if (key_buf->reverseReadInt() != codec_version) {
        //"Wrong Codec Version"
    }
    if (value_buf->readInt() != schema_version) {
        //"Wrong Schema Version"
    }
    vector<any> record;
    for (vector<BaseSchema*>::iterator it = schemas->begin(); it != schemas->end(); it++) {
        BaseSchema *bs = *it;
        BaseSchema::type type = bs->getType();
        switch(type) {
            case BaseSchema::Bool:
            {
                DingoSchema<bool*> *bos = static_cast<DingoSchema<bool*> *>(bs);
                if (bos->isKey()) {
                    record.push_back(bos->decodeKey(key_buf));
                } else {
                    record.push_back(bos->decodeValue(value_buf));
                }
                break;
            }
            case BaseSchema::Integer:
            {
                DingoSchema<int*> *is = static_cast<DingoSchema<int*> *>(bs);
                if (is->isKey()) {
                    record.push_back(is->decodeKey(key_buf));
                } else {
                    record.push_back(is->decodeValue(value_buf));
                }
                break;
            }
            case BaseSchema::Long:
            {
                DingoSchema<long*> *ls = static_cast<DingoSchema<long*> *>(bs);
                if (ls->isKey()) {
                    record.push_back(ls->decodeKey(key_buf));
                } else {
                    record.push_back(ls->decodeValue(value_buf));
                }
                break;
            }
            case BaseSchema::Double:
            {
                DingoSchema<double*> *ds = static_cast<DingoSchema<double*> *>(bs);
                if (ds->isKey()) {
                    record.push_back(ds->decodeKey(key_buf));
                } else {
                    record.push_back(ds->decodeValue(value_buf));
                }
                break;
            }
            case BaseSchema::String:
            {
                DingoSchema<string*> *ss = static_cast<DingoSchema<string*> *>(bs);
                if (ss->isKey()) {
                    record.push_back(ss->decodeKey(key_buf));
                } else {
                    record.push_back(ss->decodeValue(value_buf));
                }
                break;
            }
            default:
            {
                break;
            }
        }
    }
    return &record;
}
vector<any>* RecordDecoder::decode(KeyValue *keyvalue, vector<int> *columnIndexes) {
    Buf *key_buf = new Buf(keyvalue->getKey());
    Buf *value_buf = new Buf(keyvalue->getValue());
    if (key_buf->readLong() != common_id) {
        //"Wrong Common Id"
    }
    if (key_buf->reverseReadInt() != codec_version) {
        //"Wrong Codec Version"
    }
    if (value_buf->readInt() != schema_version) {
        //"Wrong Schema Version"
    }
    vector<any> record;
    for (vector<BaseSchema*>::iterator it = schemas->begin(); it != schemas->end(); it++) {
        BaseSchema *bs = *it;
        BaseSchema::type type = bs->getType();
        switch(type) {
            case BaseSchema::Bool:
            {
                DingoSchema<bool*> *bos = static_cast<DingoSchema<bool*> *>(bs);
                if (vectorFindAndRemove(columnIndexes, bos->getIndex())) {
                    if (bos->isKey()) {
                        record.push_back(bos->decodeKey(key_buf));
                    } else {
                        record.push_back(bos->decodeValue(value_buf));
                    }
                } else {
                    bool *b = NULL;
                    record.push_back(*b);
                }
                break;
            }
            case BaseSchema::Integer:
            {
                DingoSchema<int*> *is = static_cast<DingoSchema<int*> *>(bs);
                if (vectorFindAndRemove(columnIndexes, is->getIndex())) {
                    if (is->isKey()) {
                        record.push_back(is->decodeKey(key_buf));
                    } else {
                        record.push_back(is->decodeValue(value_buf));
                    }
                } else {
                    int *b = NULL;
                    record.push_back(*b);
                }
                break;
            }
            case BaseSchema::Long:
            {
                DingoSchema<long*> *ls = static_cast<DingoSchema<long*> *>(bs);
                if (vectorFindAndRemove(columnIndexes, ls->getIndex())) {
                    if (ls->isKey()) {
                        record.push_back(ls->decodeKey(key_buf));
                    } else {
                        record.push_back(ls->decodeValue(value_buf));
                    }
                } else {
                    long *b = NULL;
                    record.push_back(*b);
                }
                break;
            }
            case BaseSchema::Double:
            {
                DingoSchema<double*> *ds = static_cast<DingoSchema<double*> *>(bs);
                if (vectorFindAndRemove(columnIndexes, ds->getIndex())) {
                    if (ds->isKey()) {
                        record.push_back(ds->decodeKey(key_buf));
                    } else {
                        record.push_back(ds->decodeValue(value_buf));
                    }
                } else {
                    double *b = NULL;
                    record.push_back(*b);
                }
                break;
            }
            case BaseSchema::String:
            {
                DingoSchema<string*> *ss = static_cast<DingoSchema<string*> *>(bs);
                if (vectorFindAndRemove(columnIndexes, ss->getIndex())) {
                    if (ss->isKey()) {
                        record.push_back(ss->decodeKey(key_buf));
                    } else {
                        record.push_back(ss->decodeValue(value_buf));
                    }
                } else {
                    string *b = NULL;
                    record.push_back(*b);
                }
                break;
            }
            default:
            {
                break;
            }
        }
    }
    return &record;
}

}
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

namespace dingodb {

RecordEncoder::RecordEncoder(int schema_version, vector<BaseSchema*> *schemas, long common_id) {
    this->schema_version = schema_version;
    this->schemas = schemas;
    this->common_id = common_id;
    sortSchema(schemas);
    int *size = getApproPerRecordSize(schemas);
    this->key_buf_size = *size;
    size++;
    this->value_buf_size = *size;
}
KeyValue* RecordEncoder::encode(vector<any> record) {
    KeyValue *keyvalue = new KeyValue();
    keyvalue->setKey(encodeKey(record));
    keyvalue->setValue(encodeValue(record));
    return keyvalue;
}
string RecordEncoder::encodeKey(vector<any> record) {
    Buf *keyBuf = new Buf(key_buf_size);
    keyBuf->writeLong(common_id);
    keyBuf->reverseWriteInt(codec_version);
    for (vector<BaseSchema*>::iterator it = schemas->begin(); it != schemas->end(); it++) {
        BaseSchema *bs = *it;
        BaseSchema::type type = bs->getType();
        switch(type) {
            case BaseSchema::Bool:
            {
                DingoSchema<bool*> *bos = static_cast<DingoSchema<bool*> *>(bs);
                if (bos->isKey()) {
                    bos->encodeKey(keyBuf, (bool*) &record.at(bos->getIndex()));
                }
                break;
            }
            case BaseSchema::Integer:
            {
                DingoSchema<int*> *is = static_cast<DingoSchema<int*> *>(bs);
                if (is->isKey()) {
                    is->encodeKey(keyBuf, (int*) &record.at(is->getIndex()));
                }
                break;
            }
            case BaseSchema::Long:
            {
                DingoSchema<long*> *ls = static_cast<DingoSchema<long*> *>(bs);
                if (ls->isKey()) {
                    ls->encodeKey(keyBuf, (long*) &record.at(ls->getIndex()));
                }
                break;
            }
            case BaseSchema::Double:
            {
                DingoSchema<double*> *ds = static_cast<DingoSchema<double*> *>(bs);
                if (ds->isKey()) {
                    ds->encodeKey(keyBuf, (double*) &record.at(ds->getIndex()));
                }
                break;
            }
            case BaseSchema::String:
            {
                DingoSchema<string*> *ss = static_cast<DingoSchema<string*> *>(bs);
                if (ss->isKey()) {
                    ss->encodeKey(keyBuf, (string*) &record.at(ss->getIndex()));
                }
                break;
            }
            default:
            {
                break;
            }
        }
    }
    return *keyBuf->getBytes();
}
string RecordEncoder::encodeValue(vector<any> record) {
    Buf *valueBuf = new Buf(value_buf_size);
    valueBuf->writeInt(schema_version);
    for (vector<BaseSchema*>::iterator it = schemas->begin(); it != schemas->end(); it++) {
        BaseSchema *bs = *it;
        BaseSchema::type type = bs->getType();
        switch(type) {
            case BaseSchema::Bool:
            {
                DingoSchema<bool*> *bos = static_cast<DingoSchema<bool*> *>(bs);
                if (!bos->isKey()) {
                    bos->encodeValue(valueBuf, (bool*) &record.at(bos->getIndex()));
                }
                break;
            }
            case BaseSchema::Integer:
            {
                DingoSchema<int*> *is = static_cast<DingoSchema<int*> *>(bs);
                if (!is->isKey()) {
                    is->encodeValue(valueBuf, (int*) &record.at(is->getIndex()));
                }
                break;
            }
            case BaseSchema::Long:
            {
                DingoSchema<long*> *ls = static_cast<DingoSchema<long*> *>(bs);
                if (!ls->isKey()) {
                    ls->encodeValue(valueBuf, (long*) &record.at(ls->getIndex()));
                }
                break;
            }
            case BaseSchema::Double:
            {
                DingoSchema<double*> *ds = static_cast<DingoSchema<double*> *>(bs);
                if (!ds->isKey()) {
                    ds->encodeValue(valueBuf, (double*) &record.at(ds->getIndex()));
                }
                break;
            }
            case BaseSchema::String:
            {
                DingoSchema<string*> *ss = static_cast<DingoSchema<string*> *>(bs);
                if (!ss->isKey()) {
                    ss->encodeValue(valueBuf, (string*) &record.at(ss->getIndex()));
                }
                break;
            }
            default:
            {
                break;
            }
        }
    }
    return *valueBuf->getBytes();
}

}
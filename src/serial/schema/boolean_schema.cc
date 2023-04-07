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

#include "boolean_schema.h"

namespace dingodb {

int DingoSchema<bool*>::getDataLength() {
    return 1;
}
int DingoSchema<bool*>::getWithNullTagLength() {
    return 2;
}
void DingoSchema<bool*>::internalEncodeValue(Buf *buf, bool *data) {
    buf->write(*data);

}
void DingoSchema<bool*>::internalEncodeNull(Buf *buf) {
    buf->write(0);
}

BaseSchema::type DingoSchema<bool*>::getType() {
    return Bool;
}
void DingoSchema<bool*>::setIndex(int index){ 
    this->index = index;
}
int DingoSchema<bool*>::getIndex(){
    return this->index;
}
void DingoSchema<bool*>::setIsKey(bool key) {
    this->key = key;
}
bool DingoSchema<bool*>::isKey() {
    return this->key;
}
int DingoSchema<bool*>::getLength() {
    if (allow_null) {
        return getWithNullTagLength();
    }
    return getDataLength();
}
void DingoSchema<bool*>::setAllowNull(bool allow_null) {
    this->allow_null = allow_null;
}
bool DingoSchema<bool*>::allowNull() {
    return this->allow_null;
}
void DingoSchema<bool*>::encodeKey(Buf *buf, bool *data) {
    if (allow_null) {
        buf->ensureRemainder(getWithNullTagLength());
        if (data == NULL) {
            buf->write(kNull);
        } else {
            buf->write(kNotNull);
            internalEncodeValue(buf, data);
        }
    } else {
        buf->ensureRemainder(getDataLength());
        internalEncodeValue(buf, data);
    }
}
bool* DingoSchema<bool*>::decodeKey(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            buf->skip(getDataLength());
            bool *null = NULL;
            return null;
        }
    }
    bool b = buf->read();
    bool *bb = &b;
    return bb;
}
void DingoSchema<bool*>::skipKey(Buf *buf) {
    buf->skip(getLength());
}
void DingoSchema<bool*>::encodeValue(Buf *buf, bool *data) {
    if (allow_null) {
        buf->ensureRemainder(getWithNullTagLength());
        if (NULL == data) {
            buf->write(kNull);
            internalEncodeNull(buf);
        } else {
            buf->write(kNotNull);
            internalEncodeValue(buf, data);
        }
    } else {
        buf->ensureRemainder(getDataLength());
        internalEncodeValue(buf, data);
    }
}
bool* DingoSchema<bool*>::decodeValue(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            buf->skip(getDataLength());
            bool *null = NULL;
            return null;
        }
    }
    bool b = buf->read();
    bool *bb = &b;
    return bb;
}
void DingoSchema<bool*>::skipValue(Buf *buf) {
    buf->skip(getLength());
}

}
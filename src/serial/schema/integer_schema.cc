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

#include "integer_schema.h"

namespace dingodb {

int DingoSchema<int*>::getDataLength() {
    return 4;
}
int DingoSchema<int*>::getWithNullTagLength() {
    return 5;
}
void DingoSchema<int*>::internalEncodeNull(Buf *buf) {
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
}
void DingoSchema<int*>::internalEncodeKey(Buf *buf, int *data) {
        uint32_t *i = (uint32_t*) data;
        buf->write(*i >> 24 ^ 0x80);
        buf->write(*i >> 16);
        buf->write(*i >> 8);
        buf->write(*i);
}
void DingoSchema<int*>::internalEncodeValue(Buf *buf, int *data) {
        uint32_t *i = (uint32_t*) data;
        buf->write(*i >> 24);
        buf->write(*i >> 16);
        buf->write(*i >> 8);
        buf->write(*i);
}

BaseSchema::type DingoSchema<int*>::getType() {
    return Integer;
}
void DingoSchema<int*>::setIndex(int index) { 
    this->index = index;
}
int DingoSchema<int*>::getIndex() {
    return this->index;
}
void DingoSchema<int*>::setIsKey(bool key) {
    this->key = key;
}
bool DingoSchema<int*>::isKey() {
    return this->key;
}
int DingoSchema<int*>::getLength() {
    if (allow_null) {
        return getWithNullTagLength();
    }
    return getDataLength();
}
void DingoSchema<int*>::setAllowNull(bool allow_null) {
    this->allow_null = allow_null;
}
bool DingoSchema<int*>::allowNull() {
    return this->allow_null;
}
void DingoSchema<int*>::encodeKey(Buf *buf, int *data) {
    if (allow_null) {
        buf->ensureRemainder(getWithNullTagLength());
        if (NULL == data) {
            buf->write(kNull);
            internalEncodeNull(buf);
        } else {
            buf->write(kNotNull);
            internalEncodeKey(buf, data);
        }
    } else {
        buf->ensureRemainder(getDataLength());
        internalEncodeKey(buf, data);
    }
}
int* DingoSchema<int*>::decodeKey(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            buf->skip(getDataLength());
            int *null = NULL;
            return null;
        }
    }
    unsigned int r = ((buf->read() & 0xFF ^ 0x80) << 24) | ((buf->read() & 0xFF) << 16) | ((buf->read() & 0xFF) << 8) | (buf->read() & 0xFF);
    int *rr = (int*) &r;
    return rr;
}
void DingoSchema<int*>::skipKey(Buf *buf) {
    buf->skip(getLength());
}
void DingoSchema<int*>::encodeValue(Buf *buf, int *data) {
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
int* DingoSchema<int*>::decodeValue(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            buf->skip(getDataLength());
            int *null = NULL;
            return null;
        }
    }
    unsigned int r = ((buf->read() & 0xFF) << 24) | ((buf->read() & 0xFF) << 16) | ((buf->read() & 0xFF) << 8) | (buf->read() & 0xFF);
    int *rr = (int*) &r;
    return rr;
}
void DingoSchema<int*>::skipValue(Buf *buf) {
    buf->skip(getLength());
}

}
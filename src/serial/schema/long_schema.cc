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

#include "long_schema.h"

namespace dingodb {

int DingoSchema<long*>::getDataLength() {
    return 8;
}
int DingoSchema<long*>::getWithNullTagLength() {
    return 9;
}
void DingoSchema<long*>::internalEncodeNull(Buf *buf) {
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
}
void DingoSchema<long*>::internalEncodeKey(Buf *buf, long *data) {
    uint64_t l = *data;
    buf->write(l >> 56 ^ 0x80);
    buf->write(l >> 48);
    buf->write(l >> 40);
    buf->write(l >> 32);
    buf->write(l >> 24);
    buf->write(l >> 16);
    buf->write(l >> 8);
    buf->write(l);
}
void DingoSchema<long*>::internalEncodeValue(Buf *buf, long *data) {
    uint64_t l = *data;
    buf->write(l >> 56);
    buf->write(l >> 48);
    buf->write(l >> 40);
    buf->write(l >> 32);
    buf->write(l >> 24);
    buf->write(l >> 16);
    buf->write(l >> 8);
    buf->write(l);
}

BaseSchema::type DingoSchema<long*>::getType() {
    return Long;
}
void DingoSchema<long*>::setIndex(int index) { 
    this->index = index;
}
int DingoSchema<long*>::getIndex() {
    return this->index;
}
void DingoSchema<long*>::setIsKey(bool key) {
    this->key = key;
}
bool DingoSchema<long*>::isKey() {
    return this->key;
}
int DingoSchema<long*>::getLength() {
    if (allow_null) {
        return getWithNullTagLength();
    }
    return getDataLength();
}
void DingoSchema<long*>::setAllowNull(bool allow_null) {
    this->allow_null = allow_null;
}
bool DingoSchema<long*>::allowNull() {
    return this->allow_null;
}
void DingoSchema<long*>::encodeKey(Buf *buf, long *data) {
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
long* DingoSchema<long*>::decodeKey(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            buf->skip(getDataLength());
            long *null = NULL;
            return null;
        }
    }
    uint64_t l = buf->read() & 0xFF ^ 0x80;
    for (int i = 0; i < 7; i++) {
        l <<= 8;
        l |= buf->read() & 0xFF;
    }
    long *ll = (long*) &l;
    return ll;
}
void DingoSchema<long*>::skipKey(Buf *buf) {
    buf->skip(getLength());
}
void DingoSchema<long*>::encodeValue(Buf *buf, long *data) {
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
long* DingoSchema<long*>::decodeValue(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            buf->skip(getDataLength());
            long *null = NULL;
            return null;
        }
    }
    uint64_t l = buf->read() & 0xFF;
    for (int i = 0; i < 7; i++) {
        l <<= 8;
        l |= buf->read() & 0xFF;
    }
    long *ll = (long*) &l;
    return ll;
}
void DingoSchema<long*>::skipValue(Buf *buf) {
    buf->skip(getLength());
}

}
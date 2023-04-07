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

#include "double_schema.h"

namespace dingodb {

int DingoSchema<double*>::getDataLength() {
    return 8;
}
int DingoSchema<double*>::getWithNullTagLength() {
    return 9;
}
void DingoSchema<double*>::internalEncodeNull(Buf *buf) {
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
    buf->write(0);
}
void DingoSchema<double*>::internalEncodeKey(Buf *buf, double *data) {
    uint64_t bits;
    memcpy(&bits, data, 8);
    if (*data >= 0) {
        buf->write(bits >> 56 ^ 0x80);
        buf->write(bits >> 48);
        buf->write(bits >> 40);
        buf->write(bits >> 32);
        buf->write(bits >> 24);
        buf->write(bits >> 16);
        buf->write(bits >> 8);
        buf->write(bits);
    } else {
        buf->write(~ bits >> 56);
        buf->write(~ bits >> 48);
        buf->write(~ bits >> 40);
        buf->write(~ bits >> 32);
        buf->write(~ bits >> 24);
        buf->write(~ bits >> 16);
        buf->write(~ bits >> 8);
        buf->write(~ bits);
    }
}
void DingoSchema<double*>::internalEncodeValue(Buf *buf, double *data) {
    uint64_t bits = *data;
    memcpy(&bits, data, 8);
    buf->write(bits >> 56);
    buf->write(bits >> 48);
    buf->write(bits >> 40);
    buf->write(bits >> 32);
    buf->write(bits >> 24);
    buf->write(bits >> 16);
    buf->write(bits >> 8);
    buf->write(bits);
}

BaseSchema::type DingoSchema<double*>::getType() {
    return Double;
}
void DingoSchema<double*>::setIndex(int index) { 
    this->index = index;
}
int DingoSchema<double*>::getIndex() {
    return this->index;
}
void DingoSchema<double*>::setIsKey(bool key) {
    this->key = key;
}
bool DingoSchema<double*>::isKey() {
    return this->key;
}
int DingoSchema<double*>::getLength() {
    if (allow_null) {
        return getWithNullTagLength();
    }
    return getDataLength();
}
void DingoSchema<double*>::setAllowNull(bool allow_null) {
    this->allow_null = allow_null;
}
bool DingoSchema<double*>::allowNull() {
    return allow_null;
}
void DingoSchema<double*>::encodeKey(Buf *buf, double *data) {
    if (allow_null) {
        buf->ensureRemainder(getWithNullTagLength());
        if (data == NULL) {
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
double* DingoSchema<double*>::decodeKey(Buf *buf) {
        if (allow_null) {
        if (buf->read() == this->kNull) {
            buf->skip(getDataLength());
            double *null = NULL;
            return null;
        }
    }
    uint64_t l = buf->read() & 0xFF;
    if (l >= 0x80) {
        l = l ^ 0x80;
        for (int i = 0; i < 7; i++) {
            l <<= 8;
            l |= buf->read() & 0xFF;
        }
    } else {
        l = ~l;
        for (int i = 0; i < 7; i++) {
            l <<= 8;
            l |= ~ buf->read() & 0xFF;
        }
    }
    double d;
    memcpy(&d, &l, 8);
    double *dd = &d;
    return dd;
}
void DingoSchema<double*>::skipKey(Buf *buf) {
    buf->skip(getLength());
}
void DingoSchema<double*>::encodeValue(Buf *buf, double *data) {
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
double* DingoSchema<double*>::decodeValue(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            buf->skip(getDataLength());
            double *null = NULL;
            return null;
        }
    }
    uint64_t l = buf->read() & 0xFF;
    for (int i = 0; i < 7; i++) {
        l <<= 8;
        l |= buf->read() & 0xFF;
    }
    double d;
    memcpy(&d, &l, 8);
    double *dd = &d;
    return dd;
}
void DingoSchema<double*>::skipValue(Buf *buf) {
    buf->skip(getLength());
}

}
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

#include "string_schema.h"

namespace dingodb {

int DingoSchema<string*>::getDataLength() {
    return 0;
}
int DingoSchema<string*>::getWithNullTagLength() {
    return 0;
}
void DingoSchema<string*>::internalEncodeKey(Buf *buf, string *data) {
    int group_num = data->length() / 8;
    int size = (group_num + 1) * 9;
    int remainder_size = data->length() % 8;
    int remaind_zero;
    if (remainder_size == 0) {
        remainder_size = 8;
        remaind_zero = 8;
    } else {
        remaind_zero = 8 - remainder_size;
    }
    buf->ensureRemainder(size+4);
    int curr = 0;
    for (int i = 0; i < group_num; i++) {
        for (int j = 0; j < 8; j++) {
            buf->write(data->at(curr++));
        }
        buf->write((uint8_t) 255);
    }
    if (remainder_size < 8) {
        for (int j = 0; j < remainder_size; j++) {
            buf->write(data->at(curr++));
        }
    }
    for (int i = 0; i < remaind_zero; i++) {
        buf->write((uint8_t) 0);
    }
    buf->write((uint8_t) (255 - remaind_zero));
    buf->reverseWriteInt(size);
}
void DingoSchema<string*>::internalEncodeValue(Buf *buf, string *data) {
    buf->writeInt(data->length());
    for (char c : *data) {
        buf->write(c);
    }
}

BaseSchema::type DingoSchema<string*>::getType() {
    return String;
}
void DingoSchema<string*>::setIndex(int index) { 
    this->index = index;
}
int DingoSchema<string*>::getIndex() {
    return this->index;
}
void DingoSchema<string*>::setIsKey(bool key) {
    this->key = key;
}
bool DingoSchema<string*>::isKey() {
    return this->key;
}
int DingoSchema<string*>::getLength() {
    if (allow_null) {
        return getWithNullTagLength();
    }
    return getDataLength();
}
void DingoSchema<string*>::setAllowNull(bool allow_null) {
    this->allow_null = allow_null;
}
bool DingoSchema<string*>::allowNull() {
    return this->allow_null;
}
void DingoSchema<string*>::encodeKey(Buf *buf, string *data) {
    if (allow_null) {
        if (data == NULL) {
            buf->ensureRemainder(5);
            buf->write(kNull);
            buf->reverseWriteInt(0);
        } else {
            buf->ensureRemainder(1);
            buf->write(kNotNull);
            internalEncodeKey(buf, data);
        }
    } else {
        internalEncodeKey(buf, data);
    }
}
string* DingoSchema<string*>::decodeKey(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            buf->reverseSkipInt();
            string *null = NULL;
            return null;
        }
    }
    int length = buf->reverseReadInt();
    int group_num = length / 9;
    buf->skip(length - 1);
    int reminder_zero = 255 - buf->read() & 0xFF;
    buf->skip(0 - length);
    int oriLength = group_num * 8 - reminder_zero;
    char data[oriLength];
    if (oriLength != 0) {
        int curr = 0;
        group_num --;
        for (int i = 0; i < group_num; i++) {
            for (int j = 0; j < 8; j++) {
                data[curr++] = buf->read();
            }
            buf->skip(1);
        }
        if (reminder_zero != 8) {
            for (int j = 0; j < 8; j++) {
                data[curr++] = buf->read();
            }
        }
    }
    buf->skip(reminder_zero + 1);
    string s(data, oriLength);
    string *ss = &s;
    return ss;
}
void DingoSchema<string*>::skipKey(Buf *buf) {
    if(allow_null) {
        buf->skip(buf->reverseReadInt() + 1);
    } else {
        buf->skip(buf->reverseReadInt());
    }
}
void DingoSchema<string*>::encodeValue(Buf *buf, string *data) {
    if (allow_null) {
        if (NULL == data) {
            buf->write(kNull);
        } else {
            buf->write(kNotNull);
            internalEncodeValue(buf, data);
        }
    } else {
        internalEncodeValue(buf, data);
    }

}
string* DingoSchema<string*>::decodeValue(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            string *null = NULL;
            return null;
        }
    }
    int length = buf->readInt();
    char su8[length];
    for (int i = 0; i < length; i++) {
        su8[i] = buf->read();
    }
    string s(su8, length);
    string *ss = &s;
    return ss;
}
void DingoSchema<string*>::skipValue(Buf *buf) {
    if (allow_null) {
        if (buf->read() == this->kNull) {
            return;
        }
    }
    buf->skip(buf->readInt());
}

}
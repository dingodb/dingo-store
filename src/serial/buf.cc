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

#include "buf.h"

namespace dingodb {

Buf::Buf(int size) {
    vector<uint8_t> buf_tmp(size);
    this->buf = buf_tmp;
    this->reverse_pos = size-1;
}
Buf::Buf(string buf) {
    vector<uint8_t> buf_tmp(buf.begin(), buf.end());
    this->buf = buf_tmp;
    this->reverse_pos = buf_tmp.size()-1;
}
void Buf::setForwardPos(int fp) {
    this->forward_pos = fp;
}
void Buf::setReversePos(int rp) {
    this->reverse_pos = rp;
}
vector<uint8_t>* Buf::getBuf() {
    return &buf;
}
void Buf::write(uint8_t b) {
    buf.at(forward_pos++) = b;
}
void Buf::writeInt(int i) {
    uint32_t *ii = (uint32_t*) &i;
    write(*ii>>24);
    write(*ii>>16);
    write(*ii>>8);
    write(*ii);
}
void Buf::writeLong(long l) {
    uint64_t *ll = (uint64_t*) &l;
    write(*ll>>56);
    write(*ll>>48);
    write(*ll>>40);
    write(*ll>>32);
    write(*ll>>24);
    write(*ll>>16);
    write(*ll>>8);
    write(*ll);
}
void Buf::reverseWrite(uint8_t b) {
    buf.at(reverse_pos--) = b;
}
void Buf::reverseWriteInt(int i) {
    uint32_t *ii = (uint32_t*) &i;
    reverseWrite(*ii>>24);
    reverseWrite(*ii>>16);
    reverseWrite(*ii>>8);
    reverseWrite(*ii);
}
uint8_t Buf::read() {
    return buf.at(forward_pos++);
}
int Buf::readInt() {
    return ((read() & 0xFF) << 24) | ((read() & 0xFF) << 16) | ((read() & 0xFF) << 8) | (read() & 0xFF);
}
long Buf::readLong() {
    uint64_t l = read() & 0xFF;
    for (int i = 0; i < 7; i++) {
        l <<= 8;
        l |= read() & 0xFF;
    }
    return l;
}
uint8_t Buf::reverseRead() {
    return buf.at(reverse_pos--);
}
int Buf::reverseReadInt() {
    return ((reverseRead() & 0xFF) << 24) | ((reverseRead() & 0xFF) << 16) | ((reverseRead() & 0xFF) << 8) | (reverseRead() & 0xFF);
}
void Buf::reverseSkipInt() {
    reverse_pos-=4;
}
void Buf::skip(int size) {
    forward_pos+=size;
}
void Buf::reverseSkip(int size) {
    reverse_pos-=size;
}
void Buf::ensureRemainder(int length) {
    if ((forward_pos + length - 1) > reverse_pos) {
        int new_size;
        if (length > 100) {
            new_size = buf.size() + length;
        } else {
            new_size = buf.size() + 100;
        }
        vector<uint8_t> new_buf(new_size);
        for (int i = 0; i < forward_pos; i++) {
            new_buf.at(i) = buf.at(i);
        }
        int reverseSize = buf.size() - reverse_pos - 1;
        int bufStart = reverse_pos + 1;
        int newBufStart = new_size - reverseSize;
        for (int i = 0; i < reverseSize; i++) {
            new_buf.at(newBufStart + i) = buf.at(bufStart + i);
        }
        reverse_pos = new_size - reverseSize - 1;
        buf = new_buf;
    }
}
string* Buf::getBytes() {
    int empty_size = reverse_pos - forward_pos + 1;
    if (empty_size == 0) {
        char u8[buf.size()];
        copy(buf.begin(), buf.end(), u8);
        string s(u8, buf.size());
        string *ss = &s;
        return ss;
    }
    if (empty_size > 0) {
        int finalSize = buf.size() - empty_size;
        char u8[finalSize];
        for (int i = 0; i < forward_pos; i++) {
            u8[i] = buf.at(i);
        }
        int curr = reverse_pos + 1;
        for (int i = forward_pos; i < finalSize; i++) {
            u8[i] = buf.at(curr++);
        }
        string s(u8, buf.size());
        string *ss = &s;
        return ss;
    }
    if (empty_size < 0) {
        //"Wrong Key Buf"
    }
    return NULL;
}

}
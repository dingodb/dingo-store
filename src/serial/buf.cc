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
#include <bitset>

namespace dingodb {

Buf::Buf(int size) {
  this->buf_.resize(size);
  this->reverse_pos_ = size - 1;
}
Buf::Buf(string* buf) {
  this->buf_.resize(buf->size());
  this->buf_.assign(buf->begin(), buf->end());
  this->reverse_pos_ = this->buf_.size() - 1;
}
Buf::~Buf() {
  this->buf_.clear();
  this->buf_.shrink_to_fit();
}
void Buf::SetForwardPos(int fp) {
  this->forward_pos_ = fp;
}
void Buf::SetReversePos(int rp) {
  this->reverse_pos_ = rp;
}
vector<uint8_t>* Buf::GetBuf() {
  return &buf_;
}
void Buf::Write(uint8_t b) {
  buf_.at(forward_pos_++) = b;
}
void Buf::WriteInt(int32_t i) {
  uint32_t* ii = (uint32_t*)&i;
  Write(*ii >> 24);
  Write(*ii >> 16);
  Write(*ii >> 8);
  Write(*ii);
}
void Buf::WriteLong(int64_t l) {
  uint64_t* ll = (uint64_t*)&l;
  Write(*ll >> 56);
  Write(*ll >> 48);
  Write(*ll >> 40);
  Write(*ll >> 32);
  Write(*ll >> 24);
  Write(*ll >> 16);
  Write(*ll >> 8);
  Write(*ll);
}
void Buf::ReverseWrite(uint8_t b) {
  buf_.at(reverse_pos_--) = b;
}
void Buf::ReverseWriteInt(int32_t i) {
  uint32_t* ii = (uint32_t*)&i;
  ReverseWrite(*ii >> 24);
  ReverseWrite(*ii >> 16);
  ReverseWrite(*ii >> 8);
  ReverseWrite(*ii);
}
uint8_t Buf::Read() {
  return buf_.at(forward_pos_++);
}
int32_t Buf::ReadInt() {
  return ((Read() & 0xFF) << 24) | ((Read() & 0xFF) << 16) |
         ((Read() & 0xFF) << 8) | (Read() & 0xFF);
}
int64_t Buf::ReadLong() {
  uint64_t l = Read() & 0xFF;
  for (int i = 0; i < 7; i++) {
    l <<= 8;
    l |= Read() & 0xFF;
  }
  return l;
}
uint8_t Buf::ReverseRead() {
  return buf_.at(reverse_pos_--);
}
int32_t Buf::ReverseReadInt() {
  return ((ReverseRead() & 0xFF) << 24) | ((ReverseRead() & 0xFF) << 16) |
         ((ReverseRead() & 0xFF) << 8) | (ReverseRead() & 0xFF);
}
void Buf::ReverseSkipInt() {
  reverse_pos_ -= 4;
}
void Buf::Skip(int size) {
  forward_pos_ += size;
}
void Buf::ReverseSkip(int size) {
  reverse_pos_ -= size;
}
void Buf::EnsureRemainder(int length) {
  if ((forward_pos_ + length - 1) > reverse_pos_) {
    int new_size;
    if (length > 100) {
      new_size = buf_.size() + length;
    } else {
      new_size = buf_.size() + 100;
    }
    vector<uint8_t> new_buf(new_size);
    for (int i = 0; i < forward_pos_; i++) {
      new_buf.at(i) = buf_.at(i);
    }
    int reverse_size = buf_.size() - reverse_pos_ - 1;
    int buf_start = reverse_pos_ + 1;
    int new_buf_start = new_size - reverse_size;
    for (int i = 0; i < reverse_size; i++) {
      new_buf.at(new_buf_start + i) = buf_.at(buf_start + i);
    }
    reverse_pos_ = new_size - reverse_size - 1;
    buf_ = new_buf;
  }
}
string* Buf::GetBytes() {
  int empty_size = reverse_pos_ - forward_pos_ + 1;
  if (empty_size == 0) {
    char u8[buf_.size()];
    copy(buf_.begin(), buf_.end(), u8);
    string* s = new string(u8, buf_.size());
    return s;
  }
  if (empty_size > 0) {
    int final_size = buf_.size() - empty_size;
    char u8[final_size];
    for (int i = 0; i < forward_pos_; i++) {
      u8[i] = buf_.at(i);
    }
    int curr = reverse_pos_ + 1;
    for (int i = forward_pos_; i < final_size; i++) {
      u8[i] = buf_.at(curr++);
    }
    string* s = new string(u8, final_size);
    return s;
  }
  if (empty_size < 0) {
    //"Wrong Key Buf"
  }
  return nullptr;
}

}  // namespace dingodb
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

#include "serial/utils.h"

namespace dingodb {

Buf::Buf(int size) {
  Init(size);
  this->le_ = IsLE();
}

Buf::Buf(int size, bool le) {
  Init(size);
  this->le_ = le;
}

Buf::Buf(std::string* buf) {
  Init(buf);
  this->le_ = IsLE();
}

Buf::Buf(std::string* buf, bool le) {
  Init(buf);
  this->le_ = le;
}

Buf::Buf(const std::string& buf) {
  Init(buf);
  this->le_ = IsLE();
}

Buf::Buf(const std::string& buf, bool le) {
  Init(buf);
  this->le_ = le;
}

Buf::~Buf() { this->buf_.clear(); }

void Buf::Init(int size) {
  this->buf_.resize(size);
  this->reverse_pos_ = size - 1;
}

void Buf::Init(std::string* buf) {
  this->buf_.resize(buf->size());
  this->buf_.assign(buf->begin(), buf->end());
  this->reverse_pos_ = this->buf_.size() - 1;
}

void Buf::Init(const std::string& buf) {
  this->buf_.resize(buf.size());
  this->buf_.assign(buf.begin(), buf.end());
  this->reverse_pos_ = this->buf_.size() - 1;
}

void Buf::SetForwardPos(int fp) { this->forward_pos_ = fp; }

void Buf::SetReversePos(int rp) { this->reverse_pos_ = rp; }

void Buf::Write(uint8_t b) { buf_.at(forward_pos_++) = b; }

void Buf::WriteWithNegation(uint8_t b) { buf_.at(forward_pos_++) = ~b; }

void Buf::Write(const std::string& data) {
  for (auto it : data) {
    buf_.at(forward_pos_++) = it;
  }
}

void Buf::WriteInt(int32_t i) {
  uint32_t* ii = (uint32_t*)&i;
  if (this->le_) {
    Write(*ii >> 24);
    Write(*ii >> 16);
    Write(*ii >> 8);
    Write(*ii);
  } else {
    Write(*ii);
    Write(*ii >> 8);
    Write(*ii >> 16);
    Write(*ii >> 24);
  }
}

void Buf::WriteLong(int64_t l) {
  uint64_t* ll = (uint64_t*)&l;
  if (this->le_) {
    Write(*ll >> 56);
    Write(*ll >> 48);
    Write(*ll >> 40);
    Write(*ll >> 32);
    Write(*ll >> 24);
    Write(*ll >> 16);
    Write(*ll >> 8);
    Write(*ll);
  } else {
    Write(*ll);
    Write(*ll >> 8);
    Write(*ll >> 16);
    Write(*ll >> 24);
    Write(*ll >> 32);
    Write(*ll >> 40);
    Write(*ll >> 48);
    Write(*ll >> 56);
  }
}

void Buf::WriteLongWithNegation(int64_t l) {
  uint64_t* ll = (uint64_t*)&l;
  if (this->le_) {
    WriteWithNegation(*ll >> 56);
    WriteWithNegation(*ll >> 48);
    WriteWithNegation(*ll >> 40);
    WriteWithNegation(*ll >> 32);
    WriteWithNegation(*ll >> 24);
    WriteWithNegation(*ll >> 16);
    WriteWithNegation(*ll >> 8);
    WriteWithNegation(*ll);
  } else {
    WriteWithNegation(*ll);
    WriteWithNegation(*ll >> 8);
    WriteWithNegation(*ll >> 16);
    WriteWithNegation(*ll >> 24);
    WriteWithNegation(*ll >> 32);
    WriteWithNegation(*ll >> 40);
    WriteWithNegation(*ll >> 48);
    WriteWithNegation(*ll >> 56);
  }
}

void Buf::ReverseWrite(uint8_t b) { buf_.at(reverse_pos_--) = b; }

void Buf::ReverseWriteInt(int32_t i) {
  uint32_t* ii = (uint32_t*)&i;
  if (this->le_) {
    ReverseWrite(*ii >> 24);
    ReverseWrite(*ii >> 16);
    ReverseWrite(*ii >> 8);
    ReverseWrite(*ii);
  } else {
    ReverseWrite(*ii);
    ReverseWrite(*ii >> 8);
    ReverseWrite(*ii >> 16);
    ReverseWrite(*ii >> 24);
  }
}

uint8_t Buf::Read() { return buf_.at(forward_pos_++); }

int32_t Buf::ReadInt() {
  if (this->le_) {
    return ((Read() & 0xFF) << 24) | ((Read() & 0xFF) << 16) | ((Read() & 0xFF) << 8) | (Read() & 0xFF);
  } else {
    return (Read() & 0xFF) | ((Read() & 0xFF) << 8) | ((Read() & 0xFF) << 16) | ((Read() & 0xFF) << 24);
  }
}

int64_t Buf::ReadLong() {
  uint64_t l = Read() & 0xFF;
  if (this->le_) {
    for (int i = 0; i < 7; i++) {
      l <<= 8;
      l |= Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; i++) {
      l |= (((uint64_t)Read() & 0xFF) << (8 * i));
    }
  }
  return l;
}

uint8_t Buf::ReverseRead() { return buf_.at(reverse_pos_--); }

int32_t Buf::ReverseReadInt() {
  if (this->le_) {
    return ((ReverseRead() & 0xFF) << 24) | ((ReverseRead() & 0xFF) << 16) | ((ReverseRead() & 0xFF) << 8) |
           (ReverseRead() & 0xFF);
  } else {
    return (ReverseRead() & 0xFF) | ((ReverseRead() & 0xFF) << 8) | ((ReverseRead() & 0xFF) << 16) |
           ((ReverseRead() & 0xFF) << 24);
  }
}

void Buf::ReverseSkipInt() { reverse_pos_ -= 4; }

void Buf::Skip(int size) { forward_pos_ += size; }

void Buf::ReverseSkip(int size) { reverse_pos_ -= size; }

void Buf::EnsureRemainder(int length) {
  if ((forward_pos_ + length - 1) > reverse_pos_) {
    int new_size;
    if (length > 100) {
      new_size = buf_.size() + length;
    } else {
      new_size = buf_.size() + 100;
    }
    std::string new_buf;
    new_buf.resize(new_size);
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

std::string* Buf::GetBytes() {
  std::string* s = new std::string();
  int ret = GetBytes(*s);
  if (ret < 0) {
    delete s;
    return nullptr;
  }
  return s;
}

int Buf::GetBytes(std::string& s) {
  int empty_size = reverse_pos_ - forward_pos_ + 1;
  if (empty_size == 0) {
    s.resize(buf_.size());
    copy(buf_.begin(), buf_.end(), s.begin());

    return buf_.size();
  }
  if (empty_size > 0) {
    int final_size = buf_.size() - empty_size;
    s.resize(final_size);
    for (int i = 0; i < forward_pos_; i++) {
      s[i] = buf_.at(i);
    }
    int curr = reverse_pos_ + 1;
    for (int i = forward_pos_; i < final_size; i++) {
      s[i] = buf_.at(curr++);
    }
    return final_size;
  }

  if (empty_size < 0) {
    //"Wrong Key Buf"
    return -1;
  }

  return 0;
}

std::string Buf::GetString() {
  std::string s;
  GetBytes(s);
  return s;
}

bool Buf::IsLe() const { return this->le_; }

}  // namespace dingodb
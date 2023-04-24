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

#ifndef DINGO_SERIAL_BUF_H_
#define DINGO_SERIAL_BUF_H_

#include <iostream>
#include <string>
#include <vector>

namespace dingodb {

class Buf {
 private:
  std::vector<uint8_t> buf_;
  int forward_pos_ = 0;
  int reverse_pos_ = 0;
  int count_ = 0;

 public:
  Buf(int size);
  Buf(std::string* buf);
  ~Buf();
  void SetForwardPos(int fp);
  void SetReversePos(int rp);
  std::vector<uint8_t>* GetBuf();
  void Write(uint8_t b);
  void WriteInt(int32_t i);
  void WriteLong(int64_t l);
  void ReverseWrite(uint8_t b);
  void ReverseWriteInt(int32_t i);
  uint8_t Read();
  int32_t ReadInt();
  int64_t ReadLong();
  uint8_t ReverseRead();
  int32_t ReverseReadInt();
  void ReverseSkipInt();
  void Skip(int size);
  void ReverseSkip(int size);
  void EnsureRemainder(int length);
  std::string* GetBytes();
};

}  // namespace dingodb

#endif
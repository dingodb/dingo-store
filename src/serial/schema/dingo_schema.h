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

#ifndef DINGO_SERIAL_DINGO_SCHEMA_H_
#define DINGO_SERIAL_DINGO_SCHEMA_H_

#include "base_schema.h"
#include "serial/buf.h"

namespace dingodb {

template <class T>
class DingoSchema : public BaseSchema {
 public:
  virtual void SetIndex(int index) = 0;
  virtual void SetIsKey(bool key) = 0;
  virtual void SetAllowNull(bool allow_null) = 0;
  virtual void EncodeKey(Buf* buf, T data) = 0;
  virtual void EncodeKeyPrefix(Buf* buf, T data) = 0;
  virtual T DecodeKey(Buf* buf) = 0;
  virtual void SkipKey(Buf* buf) = 0;
  virtual void EncodeValue(Buf* buf, T data) = 0;
  virtual T DecodeValue(Buf* buf) = 0;
  virtual void SkipValue(Buf* buf) = 0;
};

}  // namespace dingodb

#endif
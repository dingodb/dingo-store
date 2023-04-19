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

#ifndef DINGO_SERIAL_LONG_SCHEMA_H_
#define DINGO_SERIAL_LONG_SCHEMA_H_

#include <iostream>
#include <optional>
#include "dingo_schema.h"

using namespace std;

namespace dingodb {

template <>

class DingoSchema<optional<int64_t>> : public BaseSchema {
 private:
  int index_;
  bool key_, allow_null_;

  static int GetDataLength();
  static int GetWithNullTagLength();
  static void InternalEncodeNull(Buf* buf);
  static void InternalEncodeKey(Buf* buf, int64_t data);
  static void InternalEncodeValue(Buf* buf, int64_t data);

 public:
  Type GetType() override;
  bool AllowNull() override;
  int GetLength() override;
  bool IsKey() override;
  int GetIndex() override;
  void SetIndex(int index);
  void SetIsKey(bool key);
  void SetAllowNull(bool allow_null);
  void EncodeKey(Buf* buf, optional<int64_t> data);
  optional<int64_t> DecodeKey(Buf* buf);
  void SkipKey(Buf* buf);
  void EncodeValue(Buf* buf, optional<int64_t> data);
  optional<int64_t> DecodeValue(Buf* buf);
  void SkipValue(Buf* buf);
};

}  // namespace dingodb

#endif
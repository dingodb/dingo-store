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

#ifndef DINGO_SERIAL_DOUBLE_SCHEMA_H_
#define DINGO_SERIAL_DOUBLE_SCHEMA_H_

#include <cstring>
#include <iostream>
#include <optional>
#include "dingo_schema.h"

namespace dingodb {

template <>

class DingoSchema<std::optional<double>> : public BaseSchema {
 private:
  int index_;
  bool key_, allow_null_;
  bool le_ = true;

  static int GetDataLength();
  static int GetWithNullTagLength();
  static void InternalEncodeNull(Buf* buf);
  static void LeInternalEncodeKey(Buf* buf, double data);
  static void BeInternalEncodeKey(Buf* buf, double data);
  static void LeInternalEncodeValue(Buf* buf, double data);
  static void BeInternalEncodeValue(Buf* buf, double data);

 public:
  Type GetType() override;
  bool AllowNull() override;
  int GetLength() override;
  bool IsKey() override;
  int GetIndex() override;
  void SetIndex(int index);
  void SetIsKey(bool key);
  void SetAllowNull(bool allow_null);
  void SetIsLe(bool le);
  void EncodeKey(Buf* buf, std::optional<double> data);
  void EncodeKeyPrefix(Buf* buf, std::optional<double> data);
  std::optional<double> DecodeKey(Buf* buf);
  void SkipKey(Buf* buf);
  void EncodeValue(Buf* buf, std::optional<double> data);
  std::optional<double> DecodeValue(Buf* buf);
  void SkipValue(Buf* buf);
};

}  // namespace dingodb

#endif
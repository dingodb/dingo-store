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

#ifndef DINGO_SERIAL_STRING_LIST_SCHEMA_H_
#define DINGO_SERIAL_STRING_LIST_SCHEMA_H_

#include <functional>
#include <iostream>
#include <memory>
#include <optional>

#include "dingo_schema.h"

namespace dingodb {

template <>

class DingoSchema<std::optional<std::shared_ptr<std::vector<std::string>>>> : public BaseSchema {
 private:
  int index_;
  bool key_, allow_null_;

  static int GetDataLength();
  static int GetWithNullTagLength();
  static void InternalEncodeValue(Buf* buf, std::shared_ptr<std::vector<std::string>> data);
  static void InternalEmlementEncodeValue(Buf* buf, const std::string &data);

 public:
  Type GetType() override;
  bool AllowNull() override;
  int GetLength() override;
  bool IsKey() override;
  int GetIndex() override;
  void SetIndex(int index);
  void SetIsKey(bool key);
  void SetAllowNull(bool allow_null);

  void EncodeKey(Buf* buf, std::optional<std::shared_ptr<std::vector<std::string>>> data);
  void EncodeKeyPrefix(Buf* buf, std::optional<std::shared_ptr<std::vector<std::string>>> data);
  void EncodeValue(Buf* buf, std::optional<std::shared_ptr<std::vector<std::string>>> data);

  void SkipKey(Buf* buf) const;

  std::optional<std::shared_ptr<std::vector<std::string>>> DecodeKey(Buf* buf);
  std::optional<std::shared_ptr<std::vector<std::string>>> DecodeValue(Buf* buf);

  void SkipValue(Buf* buf) const;
};

}  // namespace dingodb

#endif
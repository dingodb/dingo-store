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

#ifndef DINGO_SERIAL_BASE_SCHEMA_H_
#define DINGO_SERIAL_BASE_SCHEMA_H_

#include <cstdint>
#include <string>

namespace dingodb {

class BaseSchema {
 protected:
  const uint8_t k_null = 0;
  const uint8_t k_not_null = 1;

 private:
  std::string name_;

 public:
  virtual ~BaseSchema() = default;
  enum Type {
    kBool,
    kInteger,
    kFloat,
    kLong,
    kDouble,
    kString,
    kBoolList,
    kIntegerList,
    kFloatList,
    kLongList,
    kDoubleList,
    kStringList
  };
  virtual Type GetType() = 0;
  virtual bool AllowNull() = 0;
  virtual int GetLength() = 0;
  virtual bool IsKey() = 0;
  virtual int GetIndex() = 0;
  void SetName(const std::string& name) { name_ = name; }
  const std::string& GetName() const { return name_; }
  static const char* GetTypeString(Type type) {
    switch (type) {
      case kBool:
        return "kBool";
      case kInteger:
        return "kInteger";
      case kFloat:
        return "kFloat";
      case kLong:
        return "kLong";
      case kDouble:
        return "kDouble";
      case kString:
        return "kString";
      case kBoolList:
        return "kBoolList";
      case kIntegerList:
        return "kIntegerList";
      case kFloatList:
        return "kFloatList";
      case kLongList:
        return "kLongList";
      case kDoubleList:
        return "kDoubleList";
      case kStringList:
        return "kStringList";
      default:
        return "unknown";
    }
  }
};

}  // namespace dingodb

#endif

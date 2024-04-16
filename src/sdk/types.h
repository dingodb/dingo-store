
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

#ifndef DINGODB_SDK_TYPES_H_
#define DINGODB_SDK_TYPES_H_

#include <cstdint>
#include <string>

namespace dingodb {
namespace sdk {

enum Type : uint8_t {
  kBOOL = 0,
  kINT64 = 1,
  kDOUBLE = 2,
  kSTRING = 3,
  // This must be the last line
  kTypeEnd
};

static const bool kTypeConversionMatrix[kTypeEnd][kTypeEnd] = {
    {true, false, false, false},  // kBOOL can be converted to kBOOL
    {false, true, true, false},   // kINT64 can be converted to kINT64, kDOUBLE
    {false, false, true, false},  // kDOUBLE can be converted to kDOUBLE
    {false, false, false, true}   // kSTRING can be converted to kSTRING
};

inline std::string TypeToString(Type type) {
  switch (type) {
    case kBOOL:
      return "Bool";
    case kINT64:
      return "Int64";
    case kDOUBLE:
      return "Double";
    case kSTRING:
      return "String";
    default:
      return "Unknown ValueType";
  }
}

template <Type>
struct TypeTraits;

template <>
struct TypeTraits<kBOOL> {
  using Type = bool;
};

template <>
struct TypeTraits<kINT64> {
  using Type = int64_t;
};

template <>
struct TypeTraits<kDOUBLE> {
  using Type = double;
};

template <>
struct TypeTraits<kSTRING> {
  using Type = std::string;
};

template <Type T>
using TypeOf = typename TypeTraits<T>::Type;

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TYPES_H_
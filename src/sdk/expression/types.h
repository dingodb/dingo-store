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

#ifndef DINGODB_SDK_EXPRESSION_TYPES_H_
#define DINGODB_SDK_EXPRESSION_TYPES_H_

#include <cstdint>
#include <string>

namespace dingodb {
namespace sdk {
namespace expression {

enum Type : uint8_t { STRING, DOUBLE, BOOL, INT64 };

std::string TypeToString(Type type);

template <Type>
struct TypeTraits;

template <>
struct TypeTraits<STRING> {
  using Type = std::string;
};

template <>
struct TypeTraits<DOUBLE> {
  using Type = double;
};

template <>
struct TypeTraits<BOOL> {
  using Type = bool;
};

template <>
struct TypeTraits<INT64> {
  using Type = int64_t;
};

template <Type T>
using TypeOf = typename TypeTraits<T>::Type;

}  // namespace expression
}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_EXPRESSION_TYPES_H_
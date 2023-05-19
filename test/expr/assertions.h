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

#ifndef DINGODB_EXPR_ASSERTIONS_H_
#define DINGODB_EXPR_ASSERTIONS_H_

#include <gtest/gtest.h>

#include "operand_stack.h"
#include "types.h"

namespace dingodb::expr {

template <int T>
testing::AssertionResult Equals(const Operand &actual, const Operand &expected) {
  auto a = std::any_cast<wrap<typename CxxTraits<T>::type>>(actual);
  auto e = std::any_cast<wrap<typename CxxTraits<T>::type>>(expected);
  if (a == e) {
    return testing::AssertionSuccess();
  }
  if (a.has_value()) {
    if (e.has_value()) {
      return testing::AssertionFailure() << *a << " != " << *e;
    }
    return testing::AssertionFailure() << *a << " != null";
  } else if (e.has_value()) {
    return testing::AssertionFailure() << "null != " << *e;
  }
  return testing::AssertionFailure() << "Not possible.";
}

testing::AssertionResult EqualsByType(int type, const Operand &actual, const Operand &expected) {
  switch (type) {
    case TYPE_INT32:
      return Equals<TYPE_INT32>(actual, expected);
      break;
    case TYPE_INT64:
      return Equals<TYPE_INT64>(actual, expected);
      break;
    case TYPE_BOOL:
      return Equals<TYPE_BOOL>(actual, expected);
      break;
    case TYPE_FLOAT:
      return Equals<TYPE_FLOAT>(actual, expected);
      break;
    case TYPE_DOUBLE:
      return Equals<TYPE_DOUBLE>(actual, expected);
      break;
    default:
      return testing::AssertionFailure() << "Unsupported type in assertion.";
      break;
  }
}

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_ASSERTIONS_H_

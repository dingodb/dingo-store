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

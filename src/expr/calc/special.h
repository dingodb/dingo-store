#ifndef DINGODB_EXPR_CALC_SPECIAL_H_
#define DINGODB_EXPR_CALC_SPECIAL_H_

#include "operand.h"

using namespace dingodb::expr;

template <typename T>
bool CalcIsNull(const wrap<T> &v) {
  return !v.has_value();
}

template <typename T>
bool CalcIsTrue(const wrap<T> &v) {
  return false;
}

template <>
bool CalcIsTrue(const wrap<bool> &v) {
  return v.has_value() && *v;
}

template <>
bool CalcIsTrue(const wrap<int32_t> &v) {
  return v.has_value() && *v != 0;
}

template <>
bool CalcIsTrue(const wrap<int64_t> &v) {
  return v.has_value() && *v != 0;
}

template <typename T>
bool CalcIsFalse(const wrap<T> &v) {
  return false;
}

template <>
bool CalcIsFalse(const wrap<bool> &v) {
  return v.has_value() && !*v;
}

template <>
bool CalcIsFalse(const wrap<int32_t> &v) {
  return v.has_value() && *v != 0;
}

template <>
bool CalcIsFalse(const wrap<int64_t> &v) {
  return v.has_value() && *v != 0;
}

#endif  // DINGODB_EXPR_CALC_SPECIAL_H_

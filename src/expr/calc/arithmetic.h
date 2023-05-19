#ifndef DINGODB_EXPR_OPERATORS_ARITHMETIC_H_
#define DINGODB_EXPR_OPERATORS_ARITHMETIC_H_

#include <string>

#include "defs.h"

namespace dingodb::expr {

template <typename T>
T CalcPos(T v) {
  return v;
}

INVALID_UNARY_OP(CalcPos, std::string)

template <typename T>
T CalcNeg(T v) {
  return -v;
}

INVALID_UNARY_OP(CalcNeg, std::string)

template <typename T>
T CalcAdd(T v0, T v1) {
  return v0 + v1;
}

INVALID_BINARY_OP(CalcAdd, std::string)

template <typename T>
T CalcSub(T v0, T v1) {
  return v0 - v1;
}

INVALID_BINARY_OP(CalcSub, std::string)

template <typename T>
T CalcMul(T v0, T v1) {
  return v0 * v1;
}

INVALID_BINARY_OP(CalcMul, std::string)

template <typename T>
T CalcDiv(T v0, T v1) {
  return v0 / v1;
}

INVALID_BINARY_OP(CalcDiv, std::string)

template <typename T>
T CalcMod(T v0, T v1) {
  return v0 % v1;
}

INVALID_BINARY_OP(CalcMod, float)
INVALID_BINARY_OP(CalcMod, double)
INVALID_BINARY_OP(CalcMod, std::string)

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_OPERATORS_ARITHMETIC_H_

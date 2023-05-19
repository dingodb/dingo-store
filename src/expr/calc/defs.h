#ifndef DINGODB_EXPR_CALC_DEFS_H_
#define DINGODB_EXPR_CALC_DEFS_H_

#include <stdexcept>

#define INVALID_UNARY_OP(OP, T)                                    \
  template <>                                                      \
  T OP(T v) {                                                      \
    throw std::runtime_error("Invalid operation " #OP "(" #T ")"); \
  }

#define INVALID_BINARY_OP(OP, T)                                   \
  template <>                                                      \
  T OP(T v0, T v1) {                                               \
    throw std::runtime_error("Invalid operation " #OP "(" #T ")"); \
  }

#endif  // DINGODB_EXPR_CALC_DEFS_H_

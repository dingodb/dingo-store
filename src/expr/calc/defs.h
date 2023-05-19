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

#ifndef DINGODB_EXPR_CALC_DEFS_H_
#define DINGODB_EXPR_CALC_DEFS_H_

#include <stdexcept>

#define DECLARE_INVALID_UNARY_OP(OP, T) \
  template <>                           \
  T OP(T v);

#define IMPLEMENT_INVALID_UNARY_OP(OP, T)                          \
  template <>                                                      \
  T OP(T v) {                                                      \
    throw std::runtime_error("Invalid operation " #OP "(" #T ")"); \
  }

#define DECLARE_INVALID_BINARY_OP(OP, T) \
  template <>                            \
  T OP(T v0, T v1);

#define IMPLEMENT_INVALID_BINARY_OP(OP, T)                         \
  template <>                                                      \
  T OP(T v0, T v1) {                                               \
    throw std::runtime_error("Invalid operation " #OP "(" #T ")"); \
  }

#endif  // DINGODB_EXPR_CALC_DEFS_H_

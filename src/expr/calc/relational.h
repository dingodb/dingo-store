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

#ifndef DINGODB_EXPR_CALC_CAMPARISON_H_
#define DINGODB_EXPR_CALC_CAMPARISON_H_

namespace dingodb::expr {

template <typename T>
bool CalcEq(T v0, T v1) {
  return v0 == v1;
}

template <typename T>
bool CalcGe(T v0, T v1) {
  return v0 >= v1;
}

template <typename T>
bool CalcGt(T v0, T v1) {
  return v0 > v1;
}

template <typename T>
bool CalcLe(T v0, T v1) {
  return v0 <= v1;
}

template <typename T>
bool CalcLt(T v0, T v1) {
  return v0 < v1;
}

template <typename T>
bool CalcNe(T v0, T v1) {
  return v0 != v1;
}

}  // namespace dingodb::expr

#endif  // DINGODB_EXPR_CALC_CAMPARISON_H_

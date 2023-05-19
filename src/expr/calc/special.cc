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

#include "special.h"

namespace dingodb::expr {

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

}  // namespace dingodb::expr

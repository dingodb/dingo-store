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

#ifndef DINGODB_EXPR_CALC_SPECIAL_H_
#define DINGODB_EXPR_CALC_SPECIAL_H_

#include <cstdint>

#include "operand.h"

namespace dingodb::expr
{

template <typename T> bool CalcIsNull(const wrap<T> &v)
{
    return !v.has_value();
}

template <typename T> bool CalcIsTrue(const wrap<T> &v)
{
    return false;
}

template <> bool CalcIsTrue(const wrap<bool> &v);

template <> bool CalcIsTrue(const wrap<int32_t> &v);

template <> bool CalcIsTrue(const wrap<int64_t> &v);

template <typename T> bool CalcIsFalse(const wrap<T> &v)
{
    return false;
}

template <> bool CalcIsFalse(const wrap<bool> &v);

template <> bool CalcIsFalse(const wrap<int32_t> &v);

template <> bool CalcIsFalse(const wrap<int64_t> &v);

} // namespace dingodb::expr

#endif // DINGODB_EXPR_CALC_SPECIAL_H_

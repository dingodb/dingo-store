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

#include "arithmetic.h"

namespace dingodb::expr
{

IMPLEMENT_INVALID_UNARY_OP(CalcPos, TYPE_DECIMAL)
IMPLEMENT_INVALID_UNARY_OP(CalcPos, TYPE_STRING)

IMPLEMENT_INVALID_UNARY_OP(CalcNeg, TYPE_DECIMAL)
IMPLEMENT_INVALID_UNARY_OP(CalcNeg, TYPE_STRING)

IMPLEMENT_INVALID_BINARY_OP(CalcAdd, TYPE_DECIMAL)
IMPLEMENT_INVALID_BINARY_OP(CalcAdd, TYPE_STRING)

IMPLEMENT_INVALID_BINARY_OP(CalcSub, TYPE_DECIMAL)
IMPLEMENT_INVALID_BINARY_OP(CalcSub, TYPE_STRING)

IMPLEMENT_INVALID_BINARY_OP(CalcMul, TYPE_DECIMAL)
IMPLEMENT_INVALID_BINARY_OP(CalcMul, TYPE_STRING)

IMPLEMENT_INVALID_BINARY_OP(CalcDiv, TYPE_DECIMAL)
IMPLEMENT_INVALID_BINARY_OP(CalcDiv, TYPE_STRING)

IMPLEMENT_INVALID_BINARY_OP(CalcMod, TYPE_FLOAT)
IMPLEMENT_INVALID_BINARY_OP(CalcMod, TYPE_DOUBLE)
IMPLEMENT_INVALID_BINARY_OP(CalcMod, TYPE_DECIMAL)
IMPLEMENT_INVALID_BINARY_OP(CalcMod, TYPE_STRING)

} // namespace dingodb::expr

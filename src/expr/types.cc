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

#include "types.h"

namespace dingodb::expr
{

const char *TypeName(byte type)
{
    switch (type) {
    case TYPE_INT32:
        return "INT32";
    case TYPE_INT64:
        return "INT64";
    case TYPE_BOOL:
        return "BOOL";
    case TYPE_FLOAT:
        return "FLOAT";
    case TYPE_DOUBLE:
        return "DOUBLE";
    case TYPE_DECIMAL:
        return "DECIMAL";
    case TYPE_STRING:
        return "STRING";
    default:
        return "UNKNOWN";
    }
}

} // namespace dingodb::expr

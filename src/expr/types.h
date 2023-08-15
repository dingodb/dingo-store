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

#ifndef DINGODB_EXPR_TYPES_H_
#define DINGODB_EXPR_TYPES_H_

#include <cstdint>
#include <memory>
#include <string>

#define TYPE_INT32   0x01
#define TYPE_INT64   0x02
#define TYPE_BOOL    0x03
#define TYPE_FLOAT   0x04
#define TYPE_DOUBLE  0x05
#define TYPE_DECIMAL 0x06
#define TYPE_STRING  0x07

namespace dingodb::expr
{

typedef unsigned char byte;

template <int T> class CxxTraits
{
};

template <> class CxxTraits<TYPE_INT32>
{
public:
    typedef int32_t type;
};

template <> class CxxTraits<TYPE_INT64>
{
public:
    typedef int64_t type;
};

template <> class CxxTraits<TYPE_BOOL>
{
public:
    typedef bool type;
};

template <> class CxxTraits<TYPE_FLOAT>
{
public:
    typedef float type;
};

template <> class CxxTraits<TYPE_DOUBLE>
{
public:
    typedef double type;
};

template <> class CxxTraits<TYPE_DECIMAL>
{
public:
    typedef std::string type; // TODO: what's the real type?
};

template <> class CxxTraits<TYPE_STRING>
{
public:
    typedef std::shared_ptr<std::string> type;
};

const char *TypeName(byte type);

} // namespace dingodb::expr

#endif // DINGODB_EXPR_TYPES_H_

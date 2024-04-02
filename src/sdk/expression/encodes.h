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

#ifndef DINGODB_SDK_EXPRESSION_ENCODES_H_
#define DINGODB_SDK_EXPRESSION_ENCODES_H_

namespace dingodb {
namespace sdk {
namespace expression {

using Byte = unsigned char;

static const Byte FILTER = 0x71;

// operator
static const Byte NOT = 0x51;
static const Byte AND = 0x52;
static const Byte OR = 0x53;

// comarator
static const Byte EQ = 0x91;
static const Byte GE = 0x92;
static const Byte GT = 0x93;
static const Byte LE = 0x94;
static const Byte LT = 0x95;
static const Byte NE = 0x96;

// var
static const Byte VAR = 0x30;

// const
static const Byte NULL_PREFIX = 0x00;
static const Byte CONST = 0x10;
static const Byte CONST_N = 0x20;

// type
const Byte TYPE_NULL = 0x00;
const Byte TYPE_INT32 = 0x01;
const Byte TYPE_INT64 = 0x02;
const Byte TYPE_BOOL = 0x03;
const Byte TYPE_FLOAT = 0x04;
const Byte TYPE_DOUBLE = 0x05;
const Byte TYPE_DECIMAL = 0x06;
const Byte TYPE_STRING = 0x07;

static const Byte EOE = 0x00;

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_EXPRESSION_ENCODES_H_
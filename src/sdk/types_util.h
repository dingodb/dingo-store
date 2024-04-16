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

#ifndef DINGODB_SDK_TYPES_UTIL_H_
#define DINGODB_SDK_TYPES_UTIL_H_

#include "glog/logging.h"
#include "proto/common.pb.h"
#include "sdk/types.h"

namespace dingodb {
namespace sdk {

static pb::common::Schema::Type Type2InternalSchemaTypePB(Type type) {
  switch (type) {
    case kBOOL:
      return pb::common::Schema::BOOL;
    case kINT64:
      return pb::common::Schema::LONG;
    case kDOUBLE:
      return pb::common::Schema::DOUBLE;
    case kSTRING:
      return pb::common::Schema::STRING;
    default:
      CHECK(false) << "Unimplement convert type: " << type;
  }
}

static pb::common::ScalarFieldType Type2InternalScalarFieldTypePB(Type type) {
  switch (type) {
    case kBOOL:
      return pb::common::ScalarFieldType::BOOL;
    case kINT64:
      return pb::common::ScalarFieldType::INT64;
    case kDOUBLE:
      return pb::common::ScalarFieldType::DOUBLE;
    case kSTRING:
      return pb::common::ScalarFieldType::STRING;
    default:
      CHECK(false) << "Unimplement convert type: " << type;
  }
}

static Type InternalSchemaTypePB2Type(pb::common::Schema::Type type) {
  switch (type) {
    case pb::common::Schema::BOOL:
      return kBOOL;
    case pb::common::Schema::LONG:
      return kINT64;
    case pb::common::Schema::DOUBLE:
      return kDOUBLE;
    case pb::common::Schema::STRING:
      return kSTRING;
    default:
      CHECK(false) << "unsupported schema type:" << pb::common::Schema::Type_Name(type);
  }
}

static Type InternalScalarFieldTypePB2Type(pb::common::ScalarFieldType type) {
  switch (type) {
    case pb::common::ScalarFieldType::BOOL:
      return kBOOL;
    case pb::common::ScalarFieldType::INT64:
      return kINT64;
    case pb::common::ScalarFieldType::DOUBLE:
      return kDOUBLE;
    case pb::common::ScalarFieldType::STRING:
      return kSTRING;
    default:
      CHECK(false) << "unsupported scalar field type:" << pb::common::ScalarFieldType_Name(type);
  }
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TYPES_UTIL_H_

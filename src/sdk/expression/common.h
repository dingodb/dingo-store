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

#ifndef DINGODB_SDK_EXPRESSION_COMMON_H_
#define DINGODB_SDK_EXPRESSION_COMMON_H_

#include "glog/logging.h"
#include "proto/common.pb.h"
#include "sdk/expression/types.h"

namespace dingodb {
namespace sdk {
namespace expression {

static pb::common::Schema::Type Type2InternalSchemaTypePB(Type type) {
  switch (type) {
    case STRING:
      return pb::common::Schema::STRING;
    case DOUBLE:
      return pb::common::Schema::DOUBLE;
    case BOOL:
      return pb::common::Schema::BOOL;
    case INT64:
      return pb::common::Schema::LONG;
    default:
      CHECK(false) << "Unimplement convert type: " << type;
  }
}

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_EXPRESSION_COMMON_H_

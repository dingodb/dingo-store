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

#ifndef DINGODB_SDK_VECTOR_COMMON_H_
#define DINGODB_SDK_VECTOR_COMMON_H_

#include "glog/logging.h"
#include "proto/common.pb.h"
#include "sdk/vector.h"

namespace dingodb {
namespace sdk {

static const char kVectorPrefix = 'r';

static pb::common::MetricType MetricType2InternalMetricTypePB(MetricType metric_type) {
  switch (metric_type) {
    case MetricType::kL2:
      return pb::common::MetricType::METRIC_TYPE_L2;
    case MetricType::kInnerProduct:
      return pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT;
    case MetricType::kCosine:
      return pb::common::MetricType::METRIC_TYPE_COSINE;
    default:
      CHECK(false) << "unsupported metric type:" << metric_type;
  }
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_COMMON_H_
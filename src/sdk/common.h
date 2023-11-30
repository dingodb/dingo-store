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

#ifndef DINGODB_SDK_COMMON_H_
#define DINGODB_SDK_COMMON_H_

#include "glog/logging.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "sdk/client.h"

static const int64_t kPhysicalShiftBits = 18;
static const int64_t kLogicalMask = (1 << kPhysicalShiftBits) - 1;

namespace dingodb {
namespace sdk {

static int64_t Tso2Timestamp(pb::meta::TsoTimestamp tso) { return (tso.physical() << kPhysicalShiftBits) + tso.logical(); }

// if a == b, return 0
// if a < b, return 1
// if a > b, return -1
static int EpochCompare(const pb::common::RegionEpoch& a, const pb::common::RegionEpoch& b) {
  if (b.version() > a.version()) {
    return 1;
  }

  if (b.version() < a.version()) {
    return -1;
  }

  // below version equal

  if (b.conf_version() > a.conf_version()) {
    return 1;
  }

  if (b.conf_version() < a.conf_version()) {
    return -1;
  }

  // version equal && conf_version equal
  return 0;
}

static void FillRpcContext(pb::store::Context& context, const int64_t region_id, const pb::common::RegionEpoch& epoch) {
  context.set_region_id(region_id);
  auto* to_fill = context.mutable_region_epoch();
  *to_fill = epoch;
}

static void FillRpcContext(pb::store::Context& context, const int64_t region_id, const pb::common::RegionEpoch& epoch,
                           const pb::store::IsolationLevel isolation) {
  context.set_region_id(region_id);

  auto* to_fill = context.mutable_region_epoch();
  *to_fill = epoch;

  context.set_isolation_level(isolation);
}

}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_COMMON_H_
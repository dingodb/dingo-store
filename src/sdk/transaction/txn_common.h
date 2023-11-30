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

#ifndef DINGODB_SDK_TRANSACTION_COMMON_H_
#define DINGODB_SDK_TRANSACTION_COMMON_H_

#include <cstdint>

#include "glog/logging.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "sdk/client.h"

namespace dingodb {
namespace sdk {

static pb::store::IsolationLevel TransactionIsolation2IsolationLevel(TransactionIsolation isolation) {
  switch (isolation) {
    case kSnapshotIsolation:
      return pb::store::IsolationLevel::SnapshotIsolation;
    case kReadCommitted:
      return pb::store::IsolationLevel::ReadCommitted;
    default:
      CHECK(false) << "unknow isolation:" << isolation;
  }
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TRANSACTION_COMMON_H_
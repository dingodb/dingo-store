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

#ifndef DINGODB_STORE_STORE_CONTROL_H_
#define DINGODB_STORE_STORE_CONTROL_H_

#include <atomic>
#include <cstdint>
#include <memory>

#include "butil/macros.h"
#include "common/context.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class StoreControl {
 public:
  StoreControl();
  ~StoreControl();

  StoreControl(const StoreControl &) = delete;
  const StoreControl &operator=(const StoreControl &) = delete;

  butil::Status AddRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region);
  butil::Status ChangeRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region);
  butil::Status DeleteRegion(std::shared_ptr<Context> ctx, uint64_t region_id);

  butil::Status Snapshot(std::shared_ptr<Context> ctx, uint64_t region_id);

  void SetHeartbeatIntervalMultiple(uint64_t interval_multiple);
  void TriggerHeartbeat();

  // return true if need to send heartbeat
  bool CheckNeedToHeartbeat();

  std::atomic_bool need_send_heartbeat_immediately;

 private:
  bthread_mutex_t control_mutex_;
  std::atomic_bool need_heartbeat_immediately;  // this value is true mean heartbeat is neeed immediately
  uint64_t heartbeat_interval_multiple;         // this value describe N times the heartbeat_interval to push_interval
  uint64_t heartbeat_count;                     // this value store heartbeat executed count
};

}  // namespace dingodb

#endif  // DINGODB_STORE_STORE_CONTROL_H_
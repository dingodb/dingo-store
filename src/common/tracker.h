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

#ifndef DINGODB_COMMON_TRACKER_H_
#define DINGODB_COMMON_TRACKER_H_

#include <cstdint>
#include <memory>

#include "proto/common.pb.h"

namespace dingodb {

class Tracker {
 public:
  Tracker(const pb::common::RequestInfo& request_info);
  ~Tracker() = default;

  static std::shared_ptr<Tracker> New(const pb::common::RequestInfo& request_info);

  struct Metrics {
    uint64_t total_rpc_time_ns{0};
    uint64_t service_queue_wait_time_ns{0};
    uint64_t prepair_commit_time_ns{0};
    uint64_t raft_commit_time_ns{0};
    uint64_t raft_queue_wait_time_ns{0};
    uint64_t raft_apply_time_ns{0};
  };

  void SetTotalRpcTime();
  uint64_t TotalRpcTime() const;

  void SetServiceQueueWaitTime();
  uint64_t ServiceQueueWaitTime() const;

  void SetPrepairCommitTime();
  uint64_t PrepairCommitTime() const;

  void SetRaftCommitTime();
  uint64_t RaftCommitTime() const;

  void SetRaftQueueWaitTime();
  uint64_t RaftQueueWaitTime() const;

  void SetRaftApplyTime();
  uint64_t RaftApplyTime() const;

 private:
  uint64_t start_time_;
  uint64_t last_time_;

  pb::common::RequestInfo request_info_;
  Metrics metrics_;
};
using TrackerPtr = std::shared_ptr<Tracker>;

}  // namespace dingodb

#endif  // DINGODB_COMMON_TRACKER_H_
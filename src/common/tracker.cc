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

#include "common/tracker.h"

#include <memory>

#include "common/helper.h"

namespace dingodb {

Tracker::Tracker(const pb::common::RequestInfo& request_info) : request_info_(request_info) {
  start_time_ = Helper::TimestampNs();
  last_time_ = start_time_;
}

std::shared_ptr<Tracker> Tracker::New(const pb::common::RequestInfo& request_info) {
  return std::make_shared<Tracker>(request_info);
}

void Tracker::SetTotalRpcTime() { metrics_.total_rpc_time_ns = Helper::TimestampNs() - start_time_; }
uint64_t Tracker::TotalRpcTime() const { return metrics_.total_rpc_time_ns; }

void Tracker::SetServiceQueueWaitTime() {
  uint64_t now_time = Helper::TimestampNs();
  metrics_.service_queue_wait_time_ns = now_time - last_time_;
  last_time_ = now_time;
}

uint64_t Tracker::ServiceQueueWaitTime() const { return metrics_.service_queue_wait_time_ns; }

void Tracker::SetPrepairCommitTime() {
  uint64_t now_time = Helper::TimestampNs();
  metrics_.prepair_commit_time_ns = now_time - last_time_;
  last_time_ = now_time;
}

uint64_t Tracker::PrepairCommitTime() const { return metrics_.prepair_commit_time_ns; }

void Tracker::SetRaftCommitTime() {
  uint64_t now_time = Helper::TimestampNs();
  metrics_.raft_commit_time_ns = now_time - last_time_;
  last_time_ = now_time;
}

uint64_t Tracker::RaftCommitTime() const { return metrics_.raft_commit_time_ns; }

void Tracker::SetRaftQueueWaitTime() {
  uint64_t now_time = Helper::TimestampNs();
  metrics_.raft_queue_wait_time_ns = now_time - last_time_;
  last_time_ = now_time;
}

uint64_t Tracker::RaftQueueWaitTime() const { return metrics_.raft_queue_wait_time_ns; }

void Tracker::SetRaftApplyTime() {
  uint64_t now_time = Helper::TimestampNs();
  metrics_.raft_apply_time_ns = now_time - last_time_;
  last_time_ = now_time;
}
uint64_t Tracker::RaftApplyTime() const { return metrics_.raft_apply_time_ns; }

}  // namespace dingodb
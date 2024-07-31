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

namespace dingodb {

bvar::LatencyRecorder Tracker::service_queue_latency("dingo_tracker_service_queue");
bvar::LatencyRecorder Tracker::prepair_commit_latency("dingo_tracker_prepair_commit");
bvar::LatencyRecorder Tracker::raft_commit_latency("dingo_tracker_raft_commit");
bvar::LatencyRecorder Tracker::raft_queue_wait_latency("dingo_tracker_raft_queue_wait");
bvar::LatencyRecorder Tracker::raft_apply_latency("dingo_tracker_raft_apply");
bvar::LatencyRecorder Tracker::read_store_latency("dingo_tracker_read_store");

}  // namespace dingodb
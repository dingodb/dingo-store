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

#include <sys/types.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "braft/closure_helper.h"
#include "braft/configuration.h"
#include "brpc/closure_guard.h"
#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "gflags/gflags.h"
#include "metrics/coordinator_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "proto/version.pb.h"
#include "serial/buf.h"

namespace dingodb {

butil::Status CoordinatorControl::OneTimeWatch(const std::string& watch_key, uint64_t start_revision, bool no_put_event,
                                               bool no_delete_event, bool need_prev_kv, bool wait_on_not_exist_key,
                                               google::protobuf::Closure* done, pb::version::WatchResponse* response,
                                               brpc::Controller* cntl) {
  brpc::ClosureGuard done_guard(done);

  // check if need to send back immediately
  std::vector<pb::version::Kv> kvs_temp;
  uint64_t total_count_in_range = 0;
  this->KvRange(watch_key, std::string(), 1, false, false, kvs_temp, total_count_in_range);

  if (kvs_temp.empty() && !wait_on_not_exist_key) {
    // not exist, no wait, send response
    auto* event = response->add_events();
    event->set_type(::dingodb::pb::version::Event_EventType::Event_EventType_NOT_EXISTS);
    return butil::Status::OK();
  }

  return butil::Status::OK();
}

}  // namespace dingodb

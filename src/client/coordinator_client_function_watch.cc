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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/time.h"
#include "client/coordinator_client_function.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/version.pb.h"

DECLARE_string(key);
DECLARE_uint64(revision);
DECLARE_bool(need_prev_kv);
DECLARE_bool(no_put);
DECLARE_bool(no_delete);
DECLARE_bool(wait_on_not_exist_key);
DECLARE_uint32(max_watch_count);

void SendOneTimeWatch(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::WatchRequest request;
  dingodb::pb::version::WatchResponse response;

  if (FLAGS_key.empty()) {
    DINGO_LOG(WARNING) << "key is empty, please input key";
    return;
  }

  if (FLAGS_revision == 0) {
    DINGO_LOG(WARNING) << "revision is 0, watch from now";
  }

  auto *one_time_watch_req = request.mutable_one_time_request();

  one_time_watch_req->set_key(FLAGS_key);
  one_time_watch_req->set_need_prev_kv(FLAGS_need_prev_kv);
  one_time_watch_req->set_wait_on_not_exist_key(FLAGS_wait_on_not_exist_key);
  one_time_watch_req->set_start_revision(FLAGS_revision);

  if (FLAGS_no_delete) {
    one_time_watch_req->add_filters(::dingodb::pb::version::EventFilterType::NODELETE);
  }

  if (FLAGS_no_put) {
    one_time_watch_req->add_filters(::dingodb::pb::version::EventFilterType::NOPUT);
  }

  DINGO_LOG(INFO) << "wait_on_not_exist_key=" << FLAGS_wait_on_not_exist_key << ", no_put=" << FLAGS_no_put
                  << ", no_delete=" << FLAGS_no_delete << ", need_prev_kv=" << FLAGS_need_prev_kv
                  << ", max_watch_count=" << FLAGS_max_watch_count << ", revision=" << FLAGS_revision
                  << ", key=" << FLAGS_key;

  for (uint32_t i = 0; i < FLAGS_max_watch_count; i++) {
    // wait 600s for event
    DINGO_LOG(INFO) << "SendRequest watch_count=" << i;
    auto status = coordinator_interaction->SendRequest("Watch", request, response, 600000);
    DINGO_LOG(INFO) << "SendRequest status=" << status << ", watch_count=" << i;
    DINGO_LOG_INFO << response.DebugString();
  }
}

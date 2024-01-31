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

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>

#include "butil/status.h"
#include "client/coordinator_client_function.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "gflags/gflags_declare.h"
#include "proto/meta.pb.h"

DECLARE_int64(watch_id);
DECLARE_int64(start_revision);

void SendListWatch(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::ListWatchRequest request;
  dingodb::pb::meta::ListWatchResponse response;

  if (FLAGS_watch_id == 0) {
    DINGO_LOG(WARNING) << "watch_id is 0, watch from now";
  }

  request.set_watch_id(FLAGS_watch_id);

  DINGO_LOG(INFO) << "SendRequest watch_id=" << FLAGS_watch_id;

  auto status = coordinator_interaction->SendRequest("ListWatch", request, response, 600000);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendCreateWatch(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::WatchRequest request;
  dingodb::pb::meta::WatchResponse response;

  if (FLAGS_watch_id == 0) {
    DINGO_LOG(WARNING) << "watch_id is 0, watch from now";
  }

  if (FLAGS_start_revision == 0) {
    DINGO_LOG(WARNING) << "start_revision is 0, watch from now";
  }

  auto* create_request = request.mutable_create_request();
  create_request->set_watch_id(FLAGS_watch_id);
  create_request->set_start_revision(FLAGS_start_revision);

  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_SCHEMA_CREATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_SCHEMA_UPDATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_SCHEMA_DELETE);

  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_CREATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_UPDATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_DELETE);

  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_INDEX_CREATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_INDEX_UPDATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_INDEX_DELETE);

  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_CREATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_UPDATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_DELETE);

  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_REGION_CREATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_REGION_UPDATE);
  create_request->add_event_types(dingodb::pb::meta::MetaEventType::META_EVENT_REGION_DELETE);

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = coordinator_interaction->SendRequest("Watch", request, response, 600000);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendCancelWatch(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::WatchRequest request;
  dingodb::pb::meta::WatchResponse response;

  if (FLAGS_watch_id == 0) {
    DINGO_LOG(WARNING) << "watch_id is 0, watch from now";
  }

  if (FLAGS_start_revision == 0) {
    DINGO_LOG(WARNING) << "start_revision is 0, watch from now";
  }

  auto* cancel_request = request.mutable_cancel_request();
  cancel_request->set_watch_id(FLAGS_watch_id);

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  auto status = coordinator_interaction->SendRequest("Watch", request, response, 600000);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendProgressWatch(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::WatchRequest request;
  dingodb::pb::meta::WatchResponse response;

  if (FLAGS_watch_id == 0) {
    DINGO_LOG(WARNING) << "watch_id is 0, watch from now";
  }

  auto* progress_request = request.mutable_progress_request();
  progress_request->set_watch_id(FLAGS_watch_id);

  DINGO_LOG(INFO) << "SendRequest: " << request.DebugString();

  for (uint64_t i = 0;; ++i) {
    auto status = coordinator_interaction->SendRequest("Watch", request, response, 600000);
    DINGO_LOG(INFO)
        << "SendRequest i: " << i << ", status=" << status
        << "========================================================================================================";
    DINGO_LOG(INFO) << "event_size: " << response.events_size();
    DINGO_LOG(INFO) << response.DebugString();

    if (response.error().errcode() != 0) {
      break;
    }
  }
}

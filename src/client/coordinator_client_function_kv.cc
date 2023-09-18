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
DECLARE_bool(key_is_hex);
DECLARE_uint64(revision);
DECLARE_uint64(sub_revision);
DECLARE_string(range_end);
DECLARE_uint64(limit);
DECLARE_string(value);
DECLARE_bool(count_only);
DECLARE_bool(keys_only);
DECLARE_bool(need_prev_kv);
DECLARE_bool(ignore_value);
DECLARE_bool(ignore_lease);
DECLARE_uint64(lease);

void SendGetRawKvIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::GetRawKvIndexRequest request;
  dingodb::pb::version::GetRawKvIndexResponse response;

  if (FLAGS_key.empty()) {
    DINGO_LOG(WARNING) << "key is empty, please input key";
    return;
  }

  request.set_key(FLAGS_key);

  auto status = coordinator_interaction->SendRequest("GetRawKvIndex", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendGetRawKvRev(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::GetRawKvRevRequest request;
  dingodb::pb::version::GetRawKvRevResponse response;

  if (FLAGS_revision == 0) {
    DINGO_LOG(WARNING) << "revision is empty, please input revision";
    return;
  }

  request.mutable_revision()->set_main(FLAGS_revision);
  request.mutable_revision()->set_sub(FLAGS_sub_revision);

  auto status = coordinator_interaction->SendRequest("GetRawKvRev", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendCoorKvRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::RangeRequest request;
  dingodb::pb::version::RangeResponse response;

  if (FLAGS_key.empty()) {
    DINGO_LOG(WARNING) << "key is empty, please input key";
    return;
  }

  request.set_key(FLAGS_key);
  request.set_range_end(FLAGS_range_end);
  request.set_limit(FLAGS_limit);
  request.set_keys_only(FLAGS_keys_only);
  request.set_count_only(FLAGS_count_only);

  auto status = coordinator_interaction->SendRequest("KvRange", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendCoorKvPut(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::PutRequest request;
  dingodb::pb::version::PutResponse response;

  if (FLAGS_key.empty()) {
    DINGO_LOG(WARNING) << "key is empty, please input key";
    return;
  }

  auto *key_value = request.mutable_key_value();
  key_value->set_key(FLAGS_key);
  key_value->set_value(FLAGS_value);

  request.set_lease(FLAGS_lease);
  request.set_ignore_lease(FLAGS_ignore_lease);
  request.set_ignore_value(FLAGS_ignore_value);
  request.set_need_prev_kv(FLAGS_need_prev_kv);

  auto status = coordinator_interaction->SendRequest("KvPut", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendCoorKvDeleteRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::DeleteRangeRequest request;
  dingodb::pb::version::DeleteRangeResponse response;

  if (FLAGS_key.empty()) {
    DINGO_LOG(WARNING) << "key is empty, please input key";
    return;
  }

  request.set_key(FLAGS_key);
  request.set_range_end(FLAGS_range_end);
  request.set_need_prev_kv(FLAGS_need_prev_kv);

  auto status = coordinator_interaction->SendRequest("KvDeleteRange", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

void SendCoorKvCompaction(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::CompactionRequest request;
  dingodb::pb::version::CompactionResponse response;

  if (FLAGS_key.empty()) {
    DINGO_LOG(WARNING) << "key is empty, please input key";
    return;
  }

  if (FLAGS_revision == 0) {
    DINGO_LOG(WARNING) << "revision is empty, please input revision";
    return;
  }

  request.set_key(FLAGS_key);
  request.set_range_end(FLAGS_range_end);
  request.set_compact_revision(FLAGS_revision);

  auto status = coordinator_interaction->SendRequest("KvCompaction", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG_INFO << response.DebugString();
}

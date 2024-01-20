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

#include <memory>
#include <string>

#include "client/coordinator_client_function.h"
#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "gflags/gflags_declare.h"
#include "proto/version.pb.h"

DECLARE_string(id);
DECLARE_int64(ttl);

void SendLeaseGrant(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::LeaseGrantRequest request;
  dingodb::pb::version::LeaseGrantResponse response;

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, use auto generate id";
  } else {
    request.set_id(std::stol(FLAGS_id));
  }

  if (FLAGS_ttl < 0) {
    DINGO_LOG(WARNING) << "ttl is negative, cannot grant lease";
    return;
  }
  request.set_ttl(FLAGS_ttl);

  auto status = coordinator_interaction->SendRequest("LeaseGrant", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendLeaseRevoke(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::LeaseRevokeRequest request;
  dingodb::pb::version::LeaseRevokeResponse response;

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, use auto generate id";
    return;
  }
  request.set_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("LeaseRevoke", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendLeaseRenew(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::LeaseRenewRequest request;
  dingodb::pb::version::LeaseRenewResponse response;

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, use auto generate id";
  }
  request.set_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("LeaseRenew", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendLeaseQuery(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::LeaseQueryRequest request;
  dingodb::pb::version::LeaseQueryResponse response;

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty, please set --id";
    return;
  }
  request.set_id(std::stol(FLAGS_id));
  request.set_keys(true);

  auto status = coordinator_interaction->SendRequest("LeaseQuery", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendListLeases(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::version::ListLeasesRequest request;
  dingodb::pb::version::ListLeasesResponse response;

  auto status = coordinator_interaction->SendRequest("ListLeases", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

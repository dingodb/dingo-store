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

#include "common/logging.h"
#include "coordinator/coordinator_interaction.h"
#include "coordinator_client_function.h"

DEFINE_int64(incr_start_id, 1, "Start id of auto_increment.");
DEFINE_bool(force, true, "Force set auto increment.");
DEFINE_int32(generate_count, 10000, "Generate auto increment id count.");
DEFINE_int32(auto_increment_increment, 1, "SQL var auto_increment_increment.");
DEFINE_int32(auto_increment_offset, 1, "SQL var auto_increment_offset.");

DECLARE_bool(log_each_request);
DECLARE_string(id);

// usage example:

// ./dingodb_client -method=GetCoordinatorMap
// ./dingodb_client -id=888 -method=CreateAutoIncrement
// ./dingodb_client -id=888 -method=GetAutoIncrement
// ./dingodb_client -id=888 -generate_count=10000 -method=GenerateAutoIncrement
// ./dingodb_client -id=888 -incr_start_id=110000 -method=UpdateAutoIncrement
// ./dingodb_client -id=888 -method=DeleteAutoIncrement

void SendGetAutoIncrements(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetAutoIncrementsRequest request;
  dingodb::pb::meta::GetAutoIncrementsResponse response;

  auto status = coordinator_interaction->SendRequest("GetAutoIncrements", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGetAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GetAutoIncrementRequest request;
  dingodb::pb::meta::GetAutoIncrementResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("GetAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendCreateAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::CreateAutoIncrementRequest request;
  dingodb::pb::meta::CreateAutoIncrementResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  request.set_start_id(FLAGS_incr_start_id);

  auto status = coordinator_interaction->SendRequest("CreateAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendUpdateAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::UpdateAutoIncrementRequest request;
  dingodb::pb::meta::UpdateAutoIncrementResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  request.set_start_id(FLAGS_incr_start_id);
  request.set_force(FLAGS_force);

  auto status = coordinator_interaction->SendRequest("UpdateAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendGenerateAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::GenerateAutoIncrementRequest request;
  dingodb::pb::meta::GenerateAutoIncrementResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  request.set_count(FLAGS_generate_count);
  request.set_auto_increment_increment(FLAGS_auto_increment_increment);
  request.set_auto_increment_offset(FLAGS_auto_increment_offset);

  auto status = coordinator_interaction->SendRequest("GenerateAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

void SendDeleteAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction) {
  dingodb::pb::meta::DeleteAutoIncrementRequest request;
  dingodb::pb::meta::DeleteAutoIncrementResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  auto status = coordinator_interaction->SendRequest("DeleteAutoIncrement", request, response);
  DINGO_LOG(INFO) << "SendRequest status=" << status;
  DINGO_LOG(INFO) << response.DebugString();
}

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

#include "common/logging.h"
#include "coordinator_client_function.h"
//#include "proto/common.pb.h"

DEFINE_uint64(start_id, 1, "Start id of auto_increment.");
DEFINE_bool(force, true, "Force set auto increment.");
DEFINE_uint32(generate_count, 10000, "Generate auto increment id count.");
DEFINE_uint32(auto_increment_increment, 1, "SQL var auto_increment_increment.");
DEFINE_uint32(auto_increment_offset, 1, "SQL var auto_increment_offset.");

DECLARE_bool(log_each_request);
DECLARE_string(id);

// use example:

// ./dingodb_client_coordinator -service_type=2 -coordinator_addr=172.20.3.96:22001 -id=888 -method=CreateAutoIncrement

// ./dingodb_client_coordinator -service_type=2 -coordinator_addr=172.20.3.96:22001 -id=888 -method=GetAutoIncrement

// ./dingodb_client_coordinator -service_type=2 -coordinator_addr=172.20.3.96:22001 -id=888 -generate_count=10000
// -method=GenerateAutoIncrement

// ./dingodb_client_coordinator -service_type=2 -coordinator_addr=172.20.3.96:22001 -id=888 -start_id=110000
// -method=UpdateAutoIncrement

// ./dingodb_client_coordinator -service_type=2 -coordinator_addr=172.20.3.96:22001 -id=888 -method=DeleteAutoIncrement

template <typename T>
static void ProcessResponse(brpc::Controller& cntl, T& response, std::string func_name) {
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << func_name << " Fail to send request to : " << cntl.ErrorText();
    return;
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << func_name << " Received response: " << response.Utf8DebugString();
  }
}

void SendGetAutoIncrement(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetAutoIncrementRequest request;
  dingodb::pb::meta::GetAutoIncrementResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  stub.GetAutoIncrement(&cntl, &request, &response, nullptr);
  ProcessResponse<dingodb::pb::meta::GetAutoIncrementResponse>(cntl, response, __FUNCTION__);
}

void SendCreateAutoIncrement(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::CreateAutoIncrementRequest request;
  dingodb::pb::meta::CreateAutoIncrementResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  request.set_start_id(FLAGS_start_id);

  stub.CreateAutoIncrement(&cntl, &request, &response, nullptr);
  ProcessResponse<dingodb::pb::meta::CreateAutoIncrementResponse>(cntl, response, __FUNCTION__);
}

void SendUpdateAutoIncrement(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::UpdateAutoIncrementRequest request;
  dingodb::pb::meta::UpdateAutoIncrementResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  request.set_start_id(FLAGS_start_id);
  request.set_force(FLAGS_force);

  stub.UpdateAutoIncrement(&cntl, &request, &response, nullptr);
  ProcessResponse<dingodb::pb::meta::UpdateAutoIncrementResponse>(cntl, response, __FUNCTION__);
}

void SendGenerateAutoIncrement(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
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

  stub.GenerateAutoIncrement(&cntl, &request, &response, nullptr);
  ProcessResponse<dingodb::pb::meta::GenerateAutoIncrementResponse>(cntl, response, __FUNCTION__);
}

void SendDeleteAutoIncrement(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::DeleteAutoIncrementRequest request;
  dingodb::pb::meta::DeleteAutoIncrementResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  stub.DeleteAutoIncrement(&cntl, &request, &response, nullptr);
  ProcessResponse<dingodb::pb::meta::DeleteAutoIncrementResponse>(cntl, response, __FUNCTION__);
}

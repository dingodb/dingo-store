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

#include "client/coordinator_client_function.h"

#include <string>

#include "common/logging.h"
#include "proto/common.pb.h"

DECLARE_bool(log_each_request);
DECLARE_int32(timeout_ms);
DECLARE_string(id);
DECLARE_string(keyring);
DECLARE_string(coordinator_addr);

std::string MessageToJsonString(const google::protobuf::Message& message) {
  std::string json_string;
  google::protobuf::util::JsonOptions options;
  options.always_print_primitive_fields = true;
  google::protobuf::util::Status status = google::protobuf::util::MessageToJsonString(message, &json_string, options);
  if (!status.ok()) {
    std::cerr << "Failed to convert message to JSON: [" << status.message() << "]" << std::endl;
  }
  return json_string;
}

std::string GetLeaderLocation() {
  braft::PeerId leader;
  if (!FLAGS_coordinator_addr.empty()) {
    if (leader.parse(FLAGS_coordinator_addr) != 0) {
      DINGO_LOG(ERROR) << "Fail to parse peer_id " << FLAGS_coordinator_addr;
      return std::string();
    }
  } else {
    DINGO_LOG(ERROR) << "Please set --coordinator_addr";
    return std::string();
  }

  // rpc
  brpc::Channel channel;
  if (channel.Init(leader.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << leader;
    bthread_usleep(FLAGS_timeout_ms * 1000L);
    return std::string();
  }
  dingodb::pb::coordinator::CoordinatorService_Stub stub(&channel);
  dingodb::pb::coordinator::GetCoordinatorMapRequest request;
  dingodb::pb::coordinator::GetCoordinatorMapResponse response;

  request.set_cluster_id(0);

  brpc::Controller cntl;
  stub.GetCoordinatorMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    return std::string();
  }

  auto leader_location = response.leader_location().host() + ":" + std::to_string(response.leader_location().port());
  DINGO_LOG(INFO) << "leader_location: " << leader_location;
  return leader_location;
}

void SendGetNodeInfo(brpc::Controller& cntl, dingodb::pb::node::NodeService_Stub& stub) {
  dingodb::pb::node::GetNodeInfoRequest request;
  dingodb::pb::node::GetNodeInfoResponse response;

  std::string key = "Hello";
  // const char* op = nullptr;
  request.set_cluster_id(0);
  stub.GetNodeInfo(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " cluster_id=" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendHello(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::HelloRequest request;
  dingodb::pb::coordinator::HelloResponse response;

  std::string key = "Hello";
  // const char* op = nullptr;
  request.set_hello(0);
  stub.Hello(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " hello=" << request.hello() << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetStoreMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetStoreMapRequest request;
  dingodb::pb::coordinator::GetStoreMapResponse response;

  request.set_epoch(1);

  stub.GetStoreMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " get_store_map=" << request.epoch()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetExecutorMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetExecutorMapRequest request;
  dingodb::pb::coordinator::GetExecutorMapResponse response;

  request.set_epoch(1);

  stub.GetExecutorMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " get_executor_map=" << request.epoch()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetCoordinatorMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetCoordinatorMapRequest request;
  dingodb::pb::coordinator::GetCoordinatorMapResponse response;

  request.set_cluster_id(0);

  stub.GetCoordinatorMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " get_coordinator_map=" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetRegionMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::GetRegionMapRequest request;
  dingodb::pb::coordinator::GetRegionMapResponse response;

  request.set_epoch(1);

  stub.GetRegionMap(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " get_store_map=" << request.epoch()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us()
                    << " response=" << MessageToJsonString(response);
    for (const auto& region : response.regionmap().regions()) {
      DINGO_LOG(INFO) << "Region id=" << region.id() << " name=" << region.name()
                      << " state=" << dingodb::pb::common::RegionState_Name(region.state())
                      << " leader_store_id=" << region.leader_store_id();
    }
    // DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendCreateStore(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::CreateStoreRequest request;
  dingodb::pb::coordinator::CreateStoreResponse response;

  request.set_cluster_id(1);
  stub.CreateStore(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " create store cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendDeleteStore(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::DeleteStoreRequest request;
  dingodb::pb::coordinator::DeleteStoreResponse response;

  request.set_cluster_id(1);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  request.set_store_id(std::stol(FLAGS_id));

  if (FLAGS_keyring.empty()) {
    DINGO_LOG(WARNING) << "keyring is empty";
    return;
  }
  auto* keyring = request.mutable_keyring();
  keyring->assign(FLAGS_keyring);

  stub.DeleteStore(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " create store cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendCreateExecutor(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::CreateExecutorRequest request;
  dingodb::pb::coordinator::CreateExecutorResponse response;

  request.set_cluster_id(1);
  stub.CreateExecutor(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " create executor cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendDeleteExecutor(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub) {
  dingodb::pb::coordinator::DeleteExecutorRequest request;
  dingodb::pb::coordinator::DeleteExecutorResponse response;

  request.set_cluster_id(1);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  request.set_executor_id(std::stol(FLAGS_id));

  if (FLAGS_keyring.empty()) {
    DINGO_LOG(WARNING) << "keyring is empty";
    return;
  }
  auto* keyring = request.mutable_keyring();
  keyring->assign(FLAGS_keyring);

  stub.DeleteExecutor(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorCode() << "[" << cntl.ErrorText() << "]";
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " create executor cluster_id =" << request.cluster_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendStoreHearbeat(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub,
                       uint64_t store_id) {
  dingodb::pb::coordinator::StoreHeartbeatRequest request;
  dingodb::pb::coordinator::StoreHeartbeatResponse response;

  request.set_self_storemap_epoch(1);
  request.set_self_regionmap_epoch(1);
  // mock store
  auto* store = request.mutable_store();
  store->set_id(store_id);
  store->set_state(::dingodb::pb::common::StoreState::STORE_NORMAL);
  auto* server_location = store->mutable_server_location();
  server_location->set_host("127.0.0.1");
  server_location->set_port(19191);
  auto* raft_location = store->mutable_raft_location();
  raft_location->set_host("127.0.0.1");
  raft_location->set_port(19192);
  store->set_resource_tag("DINGO_DEFAULT");

  // mock regions
  for (int i = 0; i < 3; i++) {
    auto* region = request.add_regions();
    region->set_id(store_id * 100 + i);
    region->set_epoch(1);
    std::string region_name("test_region_");
    region_name.append(std::to_string(i));
    region->set_name(region_name);
    region->set_state(::dingodb::pb::common::RegionState::REGION_NORMAL);
    region->set_leader_store_id(1);

    // mock peers
    for (int j = 0; j < 3; j++) {
      auto* peer = region->add_peers();
      peer->set_store_id(store_id);
      auto* server_location = peer->mutable_server_location();
      server_location->set_host("127.0.0.1");
      server_location->set_port(19191);
      auto* raft_location = peer->mutable_server_location();
      raft_location->set_host("127.0.0.1");
      raft_location->set_port(19192);
    }

    // mock range
    auto* range = region->mutable_range();
    const char start_key[] = {0, 0, 0, 0};
    const char end_key[] = {static_cast<char>(255), static_cast<char>(255), static_cast<char>(255),
                            static_cast<char>(255)};

    range->set_start_key(std::string(start_key));
    range->set_end_key(std::string(end_key));

    // mock meta
    region->set_schema_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
    region->set_table_id(2);

    // mock create ts
    region->set_create_timestamp(1677496540);
  }

  // DINGO_LOG(INFO) << request.DebugString();

  stub.StoreHeartbeat(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " store_heartbeat "
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendGetSchemas(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetSchemasRequest request;
  dingodb::pb::meta::GetSchemasResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  stub.GetSchemas(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " schema_id=" << request.schema_id().entity_id() << " schema_count=" << response.schemas_size()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();

    // for (int32_t i = 0; i < response.schemas_size(); i++) {
    //   DINGO_LOG(INFO) << "schema_id=[" << response.schemas(i).id().entity_id() << "]"
    //             << "child_schema_count=" << response.schemas(i).schema_ids_size()
    //             << "child_table_count=" << response.schemas(i).table_ids_size();
    //   for (int32_t j = 0; j < response.schemas(i).schema_ids_size(); j++) {
    //     DINGO_LOG(INFO) << "child schema_id=[" << response.schemas(i).schema_ids(j).entity_id() << "]";
    //   }
    //   for (int32_t j = 0; j < response.schemas(i).table_ids_size(); j++) {
    //     DINGO_LOG(INFO) << "child table_id=[" << response.schemas(i).table_ids(j).entity_id() << "]";
    //   }
    // }
  }
}

void SendGetTablesCount(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  stub.GetTables(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " table_count=" << response.table_definition_with_ids_size()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
  }
}

void SendGetTables(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetTablesRequest request;
  dingodb::pb::meta::GetTablesResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  stub.GetTables(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " table_count=" << response.table_definition_with_ids_size()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
    // for (int32_t i = 0; i < response.tables_size(); i++) {
    //   const auto& table = response.tables(i);
    //   DINGO_LOG(INFO) << "table_id=[" << table.id() << "]"
    //             << "partition_count=" << table.partitions_size();

    //   for (int32_t j = 0; j < table.partitions_size(); j++) {
    //     const auto& partition = table.partitions(j);
    //     DINGO_LOG(INFO) << "partition_id=[" << partition.id()
    //               << "] region_count = " << partition.regions_size()
    //               << " start_key " << partition.range().start_key()
    //               << " end_key " << partition.range().end_key();

    //     for (int32_t k = 0; k < partition.regions_size(); k++) {
    //       const auto& region  = partition.regions(k);
    //       DINGO_LOG(INFO) << "region_id = " << region.id() << " region_name = " <<
    //       region.name() ;
    //     }
    //   }
    // }
  }
}

void SendGetTable(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::GetTableRequest request;
  dingodb::pb::meta::GetTableResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);

  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  stub.GetTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " table_id=" << request.table_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendCreateTableId(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::CreateTableIdRequest request;
  dingodb::pb::meta::CreateTableIdResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  stub.CreateTableId(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << " request=" << request.DebugString();
    DINGO_LOG(INFO) << " response=" << response.DebugString();
  }
}

void SendCreateTable(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub, bool with_table_id) {
  dingodb::pb::meta::CreateTableRequest request;
  dingodb::pb::meta::CreateTableResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  if (with_table_id) {
    auto* table_id = request.mutable_table_id();
    table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
    table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
    if (FLAGS_id.empty()) {
      DINGO_LOG(WARNING) << "id is empty";
      return;
    }
    table_id->set_entity_id(std::stol(FLAGS_id));
  }

  // string name = 1;
  auto* table_definition = request.mutable_table_definition();
  table_definition->set_name("t_test1");
  // repeated ColumnDefinition columns = 2;
  for (int i = 0; i < 3; i++) {
    auto* column = table_definition->add_columns();
    std::string column_name("test_columen_");
    column_name.append(std::to_string(i));
    column->set_name(column_name);
    column->set_sql_type(::dingodb::pb::meta::SqlType::SQL_TYPE_INTEGER);
    column->set_element_type(::dingodb::pb::meta::ElementType::ELEM_TYPE_BYTES);
    column->set_precision(100);
    column->set_nullable(false);
    column->set_indexofkey(7);
    column->set_has_default_val(false);
    column->set_default_val("0");
  }
  // map<string, Index> indexes = 3;
  // uint32 version = 4;
  table_definition->set_version(1);
  // uint64 ttl = 5;
  table_definition->set_ttl(0);
  // PartitionRule table_partition = 6;
  // Engine engine = 7;
  table_definition->set_engine(::dingodb::pb::common::Engine::ENG_ROCKSDB);
  // map<string, string> properties = 8;
  auto* prop = table_definition->mutable_properties();
  (*prop)["test property"] = "test_property_value";

  // add partition_rule
  // repeated string columns = 1;
  // PartitionStrategy strategy = 2;
  // RangePartition range_partition = 3;
  // HashPartition hash_partition = 4;
  auto* partition_rule = table_definition->mutable_table_partition();
  auto* part_column = partition_rule->add_columns();
  part_column->assign("test_part_column");
  auto* range_partition = partition_rule->mutable_range_partition();

  for (int i = 0; i < 3; i++) {
    auto* part_range = range_partition->add_ranges();
    auto* part_range_start = part_range->mutable_start_key();
    part_range_start->assign(std::to_string(i * 100));
    auto* part_range_end = part_range->mutable_start_key();
    part_range_end->assign(std::to_string((i + 1) * 100));
  }

  stub.CreateTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << " request=" << request.DebugString();
    DINGO_LOG(INFO) << " response=" << response.DebugString();
  }
}

void SendDropTable(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::DropTableRequest request;
  dingodb::pb::meta::DropTableResponse response;

  auto* table_id = request.mutable_table_id();
  table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  table_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  table_id->set_entity_id(std::stol(FLAGS_id));

  stub.DropTable(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request schema_id=" << request.table_id().parent_entity_id()
                    << " request table_id=" << request.table_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendDropSchema(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::DropSchemaRequest request;
  dingodb::pb::meta::DropSchemaResponse response;

  auto* schema_id = request.mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  if (FLAGS_id.empty()) {
    DINGO_LOG(WARNING) << "id is empty";
    return;
  }
  schema_id->set_entity_id(std::stol(FLAGS_id));

  stub.DropSchema(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
    return;
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request parent_schema_id=" << request.schema_id().parent_entity_id()
                    << " request schema_id=" << request.schema_id().entity_id()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

void SendCreateSchema(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub) {
  dingodb::pb::meta::CreateSchemaRequest request;
  dingodb::pb::meta::CreateSchemaResponse response;

  auto* parent_schema_id = request.mutable_parent_schema_id();
  parent_schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  parent_schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  parent_schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  request.set_schema_name("test_create_schema");
  stub.CreateSchema(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    // bthread_usleep(FLAGS_timeout_ms * 1000L);
    return;
  }

  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << "Received response"
                    << " request parent_schema_id=" << request.parent_schema_id().entity_id()
                    << " request schema_name=" << request.schema_name()
                    << " request_attachment=" << cntl.request_attachment().size()
                    << " response_attachment=" << cntl.response_attachment().size() << " latency=" << cntl.latency_us();
    DINGO_LOG(INFO) << response.DebugString();
  }
}

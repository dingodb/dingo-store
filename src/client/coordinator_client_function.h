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

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

#include "braft/raft.h"
#include "braft/util.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "bthread/bthread.h"
#include "gflags/gflags.h"
#include "google/protobuf/util/json_util.h"
#include "proto/coordinator.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"

std::string MessageToJsonString(const google::protobuf::Message& message);

// common functions
std::string GetLeaderLocation();

// coordinator service functions
void SendGetNodeInfo(brpc::Controller& cntl, dingodb::pb::node::NodeService_Stub& stub);
void SendGetLogLevel(brpc::Controller& cntl, dingodb::pb::node::NodeService_Stub& stub);
void SendChangeLogLevel(brpc::Controller& cntl, dingodb::pb::node::NodeService_Stub& stub);
void SendHello(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);
void SendGetStoreMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);
void SendGetExecutorMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);
void SendGetCoordinatorMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);
void SendGetRegionMap(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);
void SendCreateStore(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);
void SendDeleteStore(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);
void SendCreateExecutor(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);
void SendDeleteExecutor(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);
void SendStoreHearbeat(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub,
                       uint64_t store_id);
void SendGetStoreMetrics(brpc::Controller& cntl, dingodb::pb::coordinator::CoordinatorService_Stub& stub);

// meta service functions
void SendGetSchemas(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);
void SendGetTablesCount(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);
void SendGetTables(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);
void SendGetTable(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);
void SendGetParts(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);
void SendCreateTableId(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);
void SendCreateTable(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub, bool with_table_id);
void SendDropTable(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);
void SendDropSchema(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);
void SendCreateSchema(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);
void SendGetTableMetrics(brpc::Controller& cntl, dingodb::pb::meta::MetaService_Stub& stub);

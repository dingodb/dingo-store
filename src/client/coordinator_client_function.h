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

#ifndef DINGODB_COORDINATOR_CLIENT_FUNCTION_H_
#define DINGODB_COORDINATOR_CLIENT_FUNCTION_H_

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "coordinator/coordinator_interaction.h"

// node service functions
void SendGetNodeInfo();
void SendGetLogLevel();
void SendChangeLogLevel();

// raft_control functions
void SendRaftAddPeer();
void SendRaftRemovePeer();
void SendRaftTransferLeader();
void SendRaftSnapshot();
void SendRaftResetPeer();

// coordinator service functions
void SendHello(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetStoreMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetExecutorMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetCoordinatorMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetDeletedRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendAddDeletedRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCleanDeletedRegionMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetRegionCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateStore(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteStore(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendUpdateStore(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendStoreHearbeat(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, uint64_t store_id);
void SendGetStoreMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteStoreMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateExecutor(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteExecutor(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateExecutorUser(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendUpdateExecutorUser(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteExecutorUser(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetExecutorUserMap(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendExecutorHeartbeat(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// region
void SendQueryRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateRegionForSplit(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDropRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDropRegionPermanently(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendSplitRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendMergeRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendAddPeerRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendRemovePeerRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetOrphanRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendTransferLeaderRegion(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// store operation
void SendGetStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCleanStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendAddStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendRemoveStoreOperation(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// task list
void SendGetTaskList(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCleanTaskList(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// meta service functions
void SendGetSchemas(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetSchemaByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTablesCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTables(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTableByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTableRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateTableId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, bool with_table_id,
                     bool with_increment);
void SendDropTable(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDropSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateSchema(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetTableMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

void SendGetIndexesCount(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndexes(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndexByName(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndexRange(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateIndexId(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, bool with_index_id);
void SendDropIndex(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetIndexMetrics(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// auto increment functions
void SendGetAutoIncrements(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGetAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendCreateAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendUpdateAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendGenerateAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);
void SendDeleteAutoIncrement(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction);

// debug
void SendDebug();
std::string EncodeUint64(uint64_t value);
uint64_t DecodeUint64(const std::string& str);
bool GetBrpcChannel(const std::string& location, brpc::Channel& channel);

#endif  // DINGODB_COORDINATOR_CLIENT_FUNCTION_H_

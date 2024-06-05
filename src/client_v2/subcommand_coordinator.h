
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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

#include "CLI/CLI.hpp"
#include "client_v2/client_helper.h"
#include "client_v2/client_interation.h"
#include "client_v2/store_client_function.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"

#ifndef DINGODB_SUBCOMMAND_COORDINATOR_H_
#define DINGODB_SUBCOMMAND_COORDINATOR_H_
namespace client_v2 {
void SetUpSubCommands(CLI::App &app);
// void SendDebug();
std::string EncodeUint64(int64_t value);
int64_t DecodeUint64(const std::string &str);
bool GetBrpcChannel(const std::string &location, brpc::Channel &channel);

static std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction;
static std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_meta;
static std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction_version;

static int SetUp(std::string url) {
  if (url.empty()) {
    // DINGO_LOG(ERROR) << "coordinator url is empty, try to use file://./coor_list";
    url = "file://./coor_list";
  }

  auto ctx = std::make_shared<Context>();
  if (!url.empty()) {
    std::string path = url;
    path = path.replace(path.find("file://"), 7, "");
    auto addrs = Helper::GetAddrsFromFile(path);
    if (addrs.empty()) {
      DINGO_LOG(ERROR) << "url not find addr, path=" << path;
      return -1;
    }

    auto coordinator_interaction = std::make_shared<ServerInteraction>();
    if (!coordinator_interaction->Init(addrs)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << url;
      return -1;
    }

    InteractionManager::GetInstance().SetCoorinatorInteraction(coordinator_interaction);
  }

  // this is for legacy coordinator_client use, will be removed in the future
  if (!url.empty()) {
    coordinator_interaction = std::make_shared<dingodb::CoordinatorInteraction>();
    if (!coordinator_interaction->InitByNameService(
            url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeCoordinator)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction, please check parameter --url=" << url;
      return -1;
    }

    coordinator_interaction_meta = std::make_shared<dingodb::CoordinatorInteraction>();
    if (!coordinator_interaction_meta->InitByNameService(
            url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeMeta)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction_meta, please check parameter --url=" << url;
      return -1;
    }

    coordinator_interaction_version = std::make_shared<dingodb::CoordinatorInteraction>();
    if (!coordinator_interaction_version->InitByNameService(
            url, dingodb::pb::common::CoordinatorServiceType::ServiceTypeVersion)) {
      DINGO_LOG(ERROR) << "Fail to init coordinator_interaction_version, please check parameter --url=" << url;
      return -1;
    }
  }
  return 0;
}

static bool SetUpStore(std::string url, std::vector<std::string> addrs, int64_t region_id, int64_t source_id) {
  if (SetUp(url) < 0) {
    DINGO_LOG(ERROR) << "Set Up failed coor_url=" << url;
    exit(-1);
  }
  if (!addrs.empty()) {
    return client_v2::InteractionManager::GetInstance().CreateStoreInteraction(addrs);
  } else {
    int64_t target_id = region_id != 0 ? region_id : source_id;
    // Get store addr from coordinator
    auto status = client_v2::InteractionManager::GetInstance().CreateStoreInteraction(region_id);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << "Create store interaction failed, error: " << status.error_cstr();
      std::cout << "Create store interaction failed, error: " << status.error_cstr();
      return false;
    }
  }
  return true;
}
struct RaftAddPeerCommandOptions {
  std::string coordinator_addr;
  std::string peer;
  int index;
};

void SetUpSubcommandRaftAddPeer(CLI::App &app);
void RunSubcommandRaftAddPeer(RaftAddPeerCommandOptions const &opt);

struct GetRegionMapCommandOptions {
  std::string coor_url;
};

void SetUpSubcommandGetRegionMap(CLI::App &app);
void RunSubcommandGetRegionMap(GetRegionMapCommandOptions const &opt);

struct GetLogLevelCommandOptions {
  std::string coordinator_addr;
  int64_t timeout_ms;
};

void SetUpSubcommandLogLevel(CLI::App &app);
void RunSubcommandLogLevel(GetLogLevelCommandOptions const &opt);

struct RaftRemovePeerOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpSubcommandRaftRemovePeer(CLI::App &app);
void RunSubcommandRaftRemovePeer(RaftRemovePeerOption const &opt);

// todo
struct RaftTansferLeaderOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpSubcommandRaftTansferLeader(CLI::App &app);
void RunSubcommandRaftTansferLeader(RaftTansferLeaderOption const &opt);

struct RaftSnapshotOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpSubcommandRaftSnapshot(CLI::App &app);
void RunSubcommandRaftSnapshot(RaftSnapshotOption const &opt);

struct RaftResetPeerOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpSubcommandRaftResetPeer(CLI::App &app);
void RunSubcommandRaftResetPeer(RaftResetPeerOption const &opt);

struct GetNodeInfoOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpSubcommandGetNodeInfo(CLI::App &app);
void RunSubcommandGetNodeInfo(GetNodeInfoOption const &opt);

// todo

struct GetChangeLogLevelOption {
  std::string coordinator_addr;
  std::string level;
};
void SetUpSubcommandChangeLogLevel(CLI::App &app);
void RunSubcommandChangeLogLevel(GetChangeLogLevelOption const &opt);

struct HelloOption {
  std::string coor_url;
};
void SetUpSubcommandHello(CLI::App &app);
void RunSubcommandHello(HelloOption const &opt);

struct StoreHeartbeatOption {
  std::string coor_url;
};
void SetUpSubcommandStoreHeartbeat(CLI::App &app);
void RunSubcommandStoreHeartbeat(StoreHeartbeatOption const &opt);
void SendStoreHearbeatV2(std::shared_ptr<dingodb::CoordinatorInteraction> coordinator_interaction, int64_t store_id);

struct CreateStoreOption {
  std::string coor_url;
};
void SetUpSubcommandCreateStore(CLI::App &app);
void RunSubcommandCreateStore(CreateStoreOption const &opt);

struct DeleteStoreOption {
  std::string coor_url;
  std::string keyring;
  std::string id;
};
void SetUpSubcommandDeleteStore(CLI::App &app);
void RunSubcommandDeleteStore(DeleteStoreOption const &opt);

struct UpdateStoreOption {
  std::string coor_url;
  std::string keyring;
  std::string id;
  std::string state;
};
void SetUpSubcommandUpdateStore(CLI::App &app);
void RunSubcommandUpdateStore(UpdateStoreOption const &opt);

struct CreateExecutorOption {
  std::string coor_url;
  std::string keyring;
  std::string host;
  int port;
  std::string user;
};
void SetUpSubcommandCreateExecutor(CLI::App &app);
void RunSubcommandCreateExecutor(CreateExecutorOption const &opt);

struct DeleteExecutorOption {
  std::string coor_url;
  std::string keyring;
  std::string id;
  std::string user;
};
void SetUpSubcommandDeleteExecutor(CLI::App &app);
void RunSubcommandDeleteExecutor(DeleteExecutorOption const &opt);

struct CreateExecutorUserOption {
  std::string coor_url;
  std::string keyring;
  std::string user;
};
void SetUpSubcommandCreateExecutorUser(CLI::App &app);
void RunSubcommandCreateExecutorUser(CreateExecutorUserOption const &opt);

struct UpdateExecutorUserOption {
  std::string coor_url;
  std::string keyring;
  std::string new_keyring;
  std::string user;
};
void SetUpSubcommandUpdateExecutorUser(CLI::App &app);
void RunSubcommandUpdateExecutorUser(UpdateExecutorUserOption const &opt);

struct DeleteExecutorUserOption {
  std::string coor_url;
  std::string keyring;
  std::string user;
};
void SetUpSubcommandDeleteExecutorUser(CLI::App &app);
void RunSubcommandDeleteExecutorUser(DeleteExecutorUserOption const &opt);

struct GetExecutorUserMapOption {
  std::string coor_url;
};
void SetUpSubcommandGetExecutorUserMap(CLI::App &app);
void RunSubcommandGetExecutorUserMap(GetExecutorUserMapOption const &opt);

struct ExecutorHeartbeatOption {
  std::string coor_url;
  std::string keyring;
  std::string host;
  int port;
  std::string user;
  std::string id;
};
void SetUpSubcommandExecutorHeartbeat(CLI::App &app);
void RunSubcommandExecutorHeartbeat(ExecutorHeartbeatOption const &opt);

struct GetStoreMapOption {
  std::string coor_url;
  int32_t filter_store_type;
  bool use_filter_store_type;
};
void SetUpSubcommandGetStoreMap(CLI::App &app);
void RunSubcommandGetStoreMap(GetStoreMapOption const &opt);

struct GetExecutorMapOption {
  std::string coor_url;
};
void SetUpSubcommandGetExecutorMap(CLI::App &app);
void RunSubcommandGetExecutorMap(GetExecutorMapOption const &opt);

struct GetDeleteRegionMapOption {
  std::string coor_url;
};
void SetUpSubcommandGetDeleteRegionMap(CLI::App &app);
void RunSubcommandGetDeleteRegionMap(GetDeleteRegionMapOption const &opt);

struct AddDeleteRegionMapOption {
  std::string coor_url;
  std::string id;
  bool is_force;
};
void SetUpSubcommandAddDeleteRegionMap(CLI::App &app);
void RunSubcommandAddDeleteRegionMap(AddDeleteRegionMapOption const &opt);

struct CleanDeleteRegionMapOption {
  std::string coor_url;
  std::string id;
};
void SetUpSubcommandCleanDeleteRegionMap(CLI::App &app);
void RunSubcommandCleanDeleteRegionMap(CleanDeleteRegionMapOption const &opt);

struct GetRegionCountOption {
  std::string coor_url;
};
void SetUpSubcommandGetRegionCount(CLI::App &app);
void RunSubcommandGetRegionCount(GetRegionCountOption const &opt);

struct GetCoordinatorMapOption {
  std::string coor_url;
  bool get_coordinator_map;
};
void SetUpSubcommandGetCoordinatorMap(CLI::App &app);
void RunSubcommandGetCoordinatorMap(GetCoordinatorMapOption const &opt);

struct CreateRegionIdOption {
  std::string coor_url;
  int count;
};
void SetUpSubcommandCreateRegionId(CLI::App &app);
void RunSubcommandCreateRegionId(CreateRegionIdOption const &opt);

struct QueryRegionOption {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandQueryRegion(CLI::App &app);
void RunSubcommandQueryRegion(QueryRegionOption const &opt);

struct CreateRegionOption {
  std::string coor_url;
  std::string name;
  int replica;
  std::string raw_engine;
  bool create_document_region;
  std::string region_prefix;
  int part_id;
  int start_id;
  int end_id;
  std::string start_key;
  std::string end_key;
  bool key_is_hex;

  bool use_json_parameter;
  std::string vector_index_type;
  int dimension;
  std::string metrics_type;
  int max_elements;
  int efconstruction;
  int nlinks;
  int ncentroids;
  int nbits_per_idx;
  int nsubvector;
};
void SetUpSubcommandCreateRegion(CLI::App &app);
void RunSubcommandCreateRegion(CreateRegionOption const &opt);

struct CreateRegionForSplitOption {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandCreateRegionForSplit(CLI::App &app);
void RunSubcommandCreateRegionForSplit(CreateRegionForSplitOption const &opt);

struct DropRegionOption {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandDropRegion(CLI::App &app);
void RunSubcommandDropRegion(DropRegionOption const &opt);

struct DropRegionPermanentlyOption {
  std::string coor_url;
  std::string id;
};
void SetUpSubcommandDropRegionPermanently(CLI::App &app);
void RunSubcommandDropRegionPermanently(DropRegionPermanentlyOption const &opt);

struct SplitRegionOption {
  std::string coor_url;
  int64_t split_to_id;
  int64_t split_from_id;
  std::string split_key;
  int64_t vector_id;
  int64_t document_id;
  bool store_create_region;
};
void SetUpSubcommandSplitRegion(CLI::App &app);
void RunSubcommandSplitRegion(SplitRegionOption const &opt);

struct MergeRegionOption {
  std::string coor_url;
  int64_t target_id;
  int64_t source_id;
};
void SetUpSubcommandMergeRegion(CLI::App &app);
void RunSubcommandMergeRegion(MergeRegionOption const &opt);

struct AddPeerRegionOption {
  std::string coor_url;
  int64_t store_id;
  int64_t region_id;
};
void SetUpSubcommandAddPeerRegion(CLI::App &app);
void RunSubcommandAddPeerRegion(AddPeerRegionOption const &opt);

struct RemovePeerRegionOption {
  std::string coor_url;
  int64_t store_id;
  int64_t region_id;
};
void SetUpSubcommandRemovePeerRegion(CLI::App &app);
void RunSubcommandRemovePeerRegion(RemovePeerRegionOption const &opt);

struct TransferLeaderRegionOption {
  std::string coor_url;
  int64_t store_id;
  int64_t region_id;
};
void SetUpSubcommandTransferLeaderRegion(CLI::App &app);
void RunSubcommandTransferLeaderRegion(TransferLeaderRegionOption const &opt);

struct GetOrphanRegionOption {
  std::string coor_url;
  int64_t store_id;
};
void SetUpSubcommandGetOrphanRegion(CLI::App &app);
void RunSubcommandGetOrphanRegion(GetOrphanRegionOption const &opt);

struct ScanRegionsOptions {
  std::string coor_url;
  std::string key;
  bool key_is_hex;
  std::string range_end;
  int64_t limit;
};
void SetUpSubcommandScanRegions(CLI::App &app);
void RunSubcommandScanRegions(ScanRegionsOptions const &opt);

struct GetRangeRegionMapOption {
  std::string coor_url;
};
void SetUpSubcommandGetRangeRegionMap(CLI::App &app);
void RunSubcommandGetRangeRegionMap(GetRangeRegionMapOption const &opt);

struct GetStoreOperationOption {
  std::string coor_url;
  int64_t store_id;
};
void SetUpSubcommandGetStoreOperation(CLI::App &app);
void RunSubcommandGetStoreOperation(GetStoreOperationOption const &opt);

struct GetTaskListOptions {
  std::string coor_url;
  int64_t id;
  bool include_archive;
  int64_t start_id;
  int64_t limit;
};
void SetUpSubcommandGetTaskList(CLI::App &app);
void RunSubcommandGetTaskList(GetTaskListOptions const &opt);

struct CleanTaskListOption {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandCleanTaskList(CLI::App &app);
void RunSubcommandCleanTaskList(CleanTaskListOption const &opt);

struct UpdateRegionCmdStatusOptions {
  std::string coor_url;
  int64_t task_list_id;
  int64_t region_cmd_id;
  int64_t status;
  int64_t errcode;
  std::string errmsg;
};
void SetUpSubcommandUpdateRegionCmdStatus(CLI::App &app);
void RunSubcommandUpdateRegionCmdStatus(UpdateRegionCmdStatusOptions const &opt);

struct CleanStoreOperationOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandCleanStoreOperation(CLI::App &app);
void RunSubcommandCleanStoreOperation(CleanStoreOperationOptions const &opt);

struct AddStoreOperationOptions {
  std::string coor_url;
  int64_t id;
  int64_t region_id;
};
void SetUpSubcommandAddStoreOperation(CLI::App &app);
void RunSubcommandAddStoreOperation(AddStoreOperationOptions const &opt);

struct RemoveStoreOperationOptions {
  std::string coor_url;
  int64_t id;
  int64_t region_cmd_id;
};
void SetUpSubcommandRemoveStoreOperation(CLI::App &app);
void RunSubcommandRemoveStoreOperation(RemoveStoreOperationOptions const &opt);

struct GetRegionCmdOptions {
  std::string coor_url;
  int64_t store_id;
  int64_t start_region_cmd_id;
  int64_t end_region_cmd_id;
};
void SetUpSubcommandGetRegionCmd(CLI::App &app);
void RunSubcommandGetRegionCmd(GetRegionCmdOptions const &opt);

struct GetStoreMetricsOptions {
  std::string coor_url;
  int64_t id;
  int64_t region_id;
};
void SetUpSubcommandGetStoreMetricsn(CLI::App &app);
void RunSubcommandGetStoreMetrics(GetStoreMetricsOptions const &opt);

struct DeleteStoreMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandDeleteStoreMetrics(CLI::App &app);
void RunSubcommandDeleteStoreMetrics(DeleteStoreMetricsOptions const &opt);

struct GetRegionMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetRegionMetrics(CLI::App &app);
void RunSubcommandGetRegionMetrics(GetRegionMetricsOptions const &opt);

struct DeleteRegionMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandDeleteRegionMetrics(CLI::App &app);
void RunSubcommandDeleteRegionMetrics(DeleteRegionMetricsOptions const &opt);

struct MetaHelloOptions {
  std::string coor_url;
};
void SetUpSubcommandMetaHello(CLI::App &app);
void RunSubcommandMetaHello(MetaHelloOptions const &opt);

struct GetSchemasOptions {
  std::string coor_url;
  int64_t tenant_id;
};
void SetUpSubcommandGetSchemas(CLI::App &app);
void RunSubcommandGetSchemas(GetSchemasOptions const &opt);

struct GetSchemaOptions {
  std::string coor_url;
  int64_t tenant_id;
  int64_t schema_id;
};
void SetUpSubcommandGetSchema(CLI::App &app);
void RunSubcommandGetSchema(GetSchemaOptions const &opt);

struct GetSchemaByNameOptions {
  std::string coor_url;
  int64_t tenant_id;
  std::string name;
};
void SetUpSubcommandGetSchemaByName(CLI::App &app);
void RunSubcommandGetSchemaByName(GetSchemaByNameOptions const &opt);

struct GetTablesBySchemaOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpSubcommandGetTablesBySchema(CLI::App &app);
void RunSubcommandGetTablesBySchema(GetTablesBySchemaOptions const &opt);

struct GetTablesCountOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpSubcommandGetTablesCount(CLI::App &app);
void RunSubcommandGetTablesCount(GetTablesCountOptions const &opt);

struct CreateTableOptions {
  std::string coor_url;
  std::string name;
  int64_t schema_id;
  int32_t part_count;
  bool enable_rocks_engine;
  std::string engine;
  int32_t replica;
  bool with_increment;
};
void SetUpSubcommandCreateTable(CLI::App &app);
void RunSubcommandCreateTable(CreateTableOptions const &opt);

struct CreateTableIdsOptions {
  std::string coor_url;
  int32_t part_count;
};
void SetUpSubcommandCreateTableIds(CLI::App &app);
void RunSubcommandCreateTableIds(CreateTableIdsOptions const &opt);

struct CreateTableIdOptions {
  std::string coor_url;
};
void SetUpSubcommandCreateTableId(CLI::App &app);
void RunSubcommandCreateTableId(CreateTableIdOptions const &opt);

struct DropTableOptions {
  std::string coor_url;
  int64_t id;
  int64_t schema_id;
};
void SetUpSubcommandDropTable(CLI::App &app);
void RunSubcommandDropTable(DropTableOptions const &opt);

struct CreateSchemaOptions {
  std::string coor_url;
  std::string name;
  int64_t tenant_id;
};
void SetUpSubcommandCreateSchema(CLI::App &app);
void RunSubcommandCreateSchema(CreateSchemaOptions const &opt);

struct DropSchemaOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpSubcommandDropSchema(CLI::App &app);
void RunSubcommandDropSchema(DropSchemaOptions const &opt);

struct GetTableOptions {
  std::string coor_url;
  bool is_index;
  int64_t id;
};
void SetUpSubcommandGetTable(CLI::App &app);
void RunSubcommandGetTable(GetTableOptions const &opt);

struct GetTableByNameOptions {
  std::string coor_url;
  std::string name;
  int64_t schema_id;
};
void SetUpSubcommandGetTableByName(CLI::App &app);
void RunSubcommandGetTableByName(GetTableByNameOptions const &opt);

struct GetTableRangeOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetTableRange(CLI::App &app);
void RunSubcommandGetTableRange(GetTableRangeOptions const &opt);

struct GetTableMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetTableMetrics(CLI::App &app);
void RunSubcommandGetTableMetrics(GetTableMetricsOptions const &opt);

struct SwitchAutoSplitOptions {
  std::string coor_url;
  int64_t id;
  int64_t schema_id;
  bool auto_split;
};
void SetUpSubcommandSwitchAutoSplit(CLI::App &app);
void RunSubcommandSwitchAutoSplit(SwitchAutoSplitOptions const &opt);

struct GetDeletedTableOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetDeletedTable(CLI::App &app);
void RunSubcommandGetDeletedTable(GetDeletedTableOptions const &opt);

struct GetDeletedIndexOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetDeletedIndex(CLI::App &app);
void RunSubcommandGetDeletedIndex(GetDeletedIndexOptions const &opt);

struct CleanDeletedTableOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandCleanDeletedTable(CLI::App &app);
void RunSubcommandCleanDeletedTable(CleanDeletedTableOptions const &opt);

struct CleanDeletedIndexOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandCleanDeletedIndex(CLI::App &app);
void RunSubcommandCleanDeletedIndex(CleanDeletedIndexOptions const &opt);

// // tenant
struct CreateTenantOptions {
  std::string coor_url;
  std::string name;
  std::string comment;
  int64_t tenant_id;
};
void SetUpSubcommandCreateTenant(CLI::App &app);
void RunSubcommandCreateTenant(CreateTenantOptions const &opt);

struct UpdateTenantOptions {
  std::string coor_url;
  std::string name;
  std::string comment;
  int64_t tenant_id;
};
void SetUpSubcommandUpdateTenant(CLI::App &app);
void RunSubcommandUpdateTenant(UpdateTenantOptions const &opt);

struct DropTenantOptions {
  std::string coor_url;
  int64_t tenant_id;
};
void SetUpSubcommandDropTenant(CLI::App &app);
void RunSubcommandDropTenant(DropTenantOptions const &opt);

struct GetTenantOptions {
  std::string coor_url;
  int64_t tenant_id;
};
void SetUpSubcommandGetTenant(CLI::App &app);
void RunSubcommandGetTenant(GetTenantOptions const &opt);

// indexs
struct GetIndexesOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpSubcommandGetIndexes(CLI::App &app);
void RunSubcommandGetIndexes(GetIndexesOptions const &opt);

struct GetIndexesCountOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpSubcommandGetIndexesCount(CLI::App &app);
void RunSubcommandGetIndexesCount(GetIndexesCountOptions const &opt);

struct CreateIndexOptions {
  std::string coor_url;
  std::string name;
  int64_t schema_id;
  int32_t part_count;
  int64_t replica;
  bool with_auto_increment;
  bool with_scalar_schema;
  std::string vector_index_type;
  int32_t dimension;
  std::string metrics_type;
  int32_t max_elements;
  int32_t efconstruction;
  int32_t nlinks;
  int ncentroids;
  int nbits_per_idx;
  int nsubvector;
};
void SetUpSubcommandCreateIndex(CLI::App &app);
void RunSubcommandCreateIndex(CreateIndexOptions const &opt);

struct CreateDocumentIndexOptions {
  std::string coor_url;
  std::string name;
  int64_t schema_id;
  int32_t part_count;
  int64_t replica;
  bool with_auto_increment;
  bool use_json_parameter;
};
void SetUpSubcommandCreateDocumentIndex(CLI::App &app);
void RunSubcommandCreateDocumentIndex(CreateDocumentIndexOptions const &opt);

struct CreateIndexIdOptions {
  std::string coor_url;
};
void SetUpSubcommandCreateIndexId(CLI::App &app);
void RunSubcommandCreateIndexId(CreateIndexIdOptions const &opt);

struct UpdateIndexOptions {
  std::string coor_url;
  int64_t id;
  int32_t max_elements;
};
void SetUpSubcommandUpdateIndex(CLI::App &app);
void RunSubcommandUpdateIndex(UpdateIndexOptions const &opt);

struct DropIndexOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandDropIndex(CLI::App &app);
void RunSubcommandDropIndex(DropIndexOptions const &opt);

struct GetIndexOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetIndex(CLI::App &app);
void RunSubcommandGetIndex(GetIndexOptions const &opt);

struct GetIndexByNameOptions {
  std::string coor_url;
  int64_t schema_id;
  std::string name;
};
void SetUpSubcommandGetIndexByName(CLI::App &app);
void RunSubcommandGetIndexByName(GetIndexByNameOptions const &opt);

struct GetIndexRangeOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetIndexRange(CLI::App &app);
void RunSubcommandGetIndexRange(GetIndexRangeOptions const &opt);

struct GetIndexMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetIndexMetrics(CLI::App &app);
void RunSubcommandGetIndexMetrics(GetIndexMetricsOptions const &opt);

struct GenerateTableIdsOptions {
  std::string coor_url;
  int64_t schema_id;
};
void SetUpSubcommandGenerateTableIds(CLI::App &app);
void RunSubcommandGenerateTableIds(GenerateTableIdsOptions const &opt);

struct CreateTablesOptions {
  std::string coor_url;
  std::string name;
  int64_t schema_id;
  int64_t part_count;
  int64_t replica;
  std::string engine;
};
void SetUpSubcommandCreateTables(CLI::App &app);
void RunSubcommandCreateTables(CreateTablesOptions const &opt);

struct UpdateTablesOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t def_version;
  int64_t part_count;
  int64_t replica;
  bool is_updating_index;
  std::string engine;
  std::string name;
};
void SetUpSubcommandUpdateTables(CLI::App &app);
void RunSubcommandUpdateTables(UpdateTablesOptions const &opt);

struct AddIndexOnTableOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t index_id;
  std::string name;
  int64_t def_version;
  int64_t part_count;
  int64_t replica;
  bool is_updating_index;
  std::string engine;
};
void SetUpSubcommandAddIndexOnTable(CLI::App &app);
void RunSubcommandAddIndexOnTable(AddIndexOnTableOptions const &opt);

struct DropIndexOnTableOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t index_id;
};
void SetUpSubcommandDropIndexOnTable(CLI::App &app);
void RunSubcommandDropIndexOnTable(DropIndexOnTableOptions const &opt);

struct GetTablesOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetTables(CLI::App &app);
void RunSubcommandGetTables(GetTablesOptions const &opt);

struct DropTablesOptions {
  std::string coor_url;
  int64_t id;
  int64_t schema_id;
};
void SetUpSubcommandDropTables(CLI::App &app);
void RunSubcommandDropTables(DropTablesOptions const &opt);

// autoincrement
struct GetAutoIncrementsOptions {
  std::string coor_url;
};
void SetUpSubcommandGetAutoIncrements(CLI::App &app);
void RunSubcommandGetAutoIncrements(GetAutoIncrementsOptions const &opt);

struct GetAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGetAutoIncrement(CLI::App &app);
void RunSubcommandGetAutoIncrement(GetAutoIncrementOptions const &opt);

struct CreateAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
  int64_t incr_start_id;
};
void SetUpSubcommandCreateAutoIncrement(CLI::App &app);
void RunSubcommandCreateAutoIncrement(CreateAutoIncrementOptions const &opt);

struct UpdateAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
  int64_t incr_start_id;
  bool force;
};
void SetUpSubcommandUpdateAutoIncrement(CLI::App &app);
void RunSubcommandUpdateAutoIncrement(UpdateAutoIncrementOptions const &opt);

struct GenerateAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
  int32_t generate_count;
  int32_t auto_increment_offset;
  int32_t auto_increment_increment;
};
void SetUpSubcommandGenerateAutoIncrement(CLI::App &app);
void RunSubcommandGenerateAutoIncrement(GenerateAutoIncrementOptions const &opt);

struct DeleteAutoIncrementOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandDeleteAutoIncrement(CLI::App &app);
void RunSubcommandDeleteAutoIncrement(DeleteAutoIncrementOptions const &opt);

struct LeaseGrantOptions {
  std::string coor_url;
  int64_t id;
  int64_t ttl;
};
void SetUpSubcommandLeaseGrant(CLI::App &app);
void RunSubcommandLeaseGrant(LeaseGrantOptions const &opt);

struct LeaseRevokeOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandLeaseRevoke(CLI::App &app);
void RunSubcommandLeaseRevoke(LeaseRevokeOptions const &opt);

struct LeaseRenewOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandLeaseRenew(CLI::App &app);
void RunSubcommandLeaseRenew(LeaseRenewOptions const &opt);

struct LeaseQueryOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandLeaseQuery(CLI::App &app);
void RunSubcommandLeaseQuery(LeaseQueryOptions const &opt);

struct ListLeasesOptions {
  std::string coor_url;
};
void SetUpSubcommandListLeases(CLI::App &app);
void RunSubcommandListLeases(ListLeasesOptions const &opt);

// coordinator kv
struct KvHelloOptions {
  std::string coor_url;
};
void SetUpSubcommandKvHello(CLI::App &app);
void RunSubcommandKvHello(KvHelloOptions const &opt);

struct GetRawKvIndexOptions {
  std::string coor_url;
  std::string key;
};
void SetUpSubcommandGetRawKvIndex(CLI::App &app);
void RunSubcommandGetRawKvIndex(GetRawKvIndexOptions const &opt);

struct GetRawKvRevOptions {
  std::string coor_url;
  int64_t revision;
  int64_t sub_revision;
};
void SetUpSubcommandGetRawKvRev(CLI::App &app);
void RunSubcommandGetRawKvRev(GetRawKvRevOptions const &opt);

struct CoorKvRangeOptions {
  std::string coor_url;
  std::string key;
  std::string range_end;
  int64_t limit;
  bool keys_only;
  bool count_only;
};
void SetUpSubcommandCoorKvRange(CLI::App &app);
void RunSubcommandCoorKvRange(CoorKvRangeOptions const &opt);

// coordinator kv
struct CoorKvPutOptions {
  std::string coor_url;
  std::string key;
  std::string value;
  int64_t lease;
  bool ignore_lease;
  bool ignore_value;
  bool need_prev_kv;
};
void SetUpSubcommandCoorKvPut(CLI::App &app);
void RunSubcommandCoorKvPut(CoorKvPutOptions const &opt);

struct CoorKvDeleteRangeOptions {
  std::string coor_url;
  std::string key;
  std::string range_end;
  bool need_prev_kv;
};
void SetUpSubcommandCoorKvDeleteRange(CLI::App &app);
void RunSubcommandCoorKvDeleteRange(CoorKvDeleteRangeOptions const &opt);

struct CoorKvCompactionOptions {
  std::string coor_url;
  std::string key;
  int64_t revision;
  std::string range_end;
};
void SetUpSubcommandCoorKvCompaction(CLI::App &app);
void RunSubcommandCoorKvCompaction(CoorKvCompactionOptions const &opt);

// coordinator watch
struct OneTimeWatchOptions {
  std::string coor_url;
  std::string key;
  int64_t revision;
  bool need_prev_kv;
  bool wait_on_not_exist_key;
  bool no_put;
  bool no_delete;
  int32_t max_watch_count;
};
void SetUpSubcommandOneTimeWatch(CLI::App &app);
void RunSubcommandOneTimeWatch(OneTimeWatchOptions const &opt);

struct LockOptions {
  std::string coor_url;
  std::string lock_name;
  std::string client_uuid;
};
void SetUpSubcommandLock(CLI::App &app);
void RunSubcommandLock(LockOptions const &opt);

// meta watch
struct ListWatchOptions {
  std::string coor_url;
  int64_t watch_id;
};
void SetUpSubcommandListWatch(CLI::App &app);
void RunSubcommandListWatch(ListWatchOptions const &opt);

struct CreateWatchOptions {
  std::string coor_url;
  int64_t watch_id;
  int64_t start_revision;
  std::string watch_type;
};
void SetUpSubcommandCreateWatch(CLI::App &app);
void RunSubcommandCreateWatch(CreateWatchOptions const &opt);

struct CancelWatchOptions {
  std::string coor_url;
  int64_t watch_id;
  int64_t start_revision;
};
void SetUpSubcommandCancelWatch(CLI::App &app);
void RunSubcommandCancelWatch(CancelWatchOptions const &opt);

struct ProgressWatchOptions {
  std::string coor_url;
  int64_t watch_id;
};
void SetUpSubcommandProgressWatch(CLI::App &app);
void RunSubcommandProgressWatch(ProgressWatchOptions const &opt);

// tso
struct GenTsoOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpSubcommandGenTso(CLI::App &app);
void RunSubcommandGenTso(GenTsoOptions const &opt);

struct ResetTsoOptions {
  std::string coor_url;
  int64_t tso_new_physical;
  int64_t tso_save_physical;
  int64_t tso_new_logical;
};
void SetUpSubcommandResetTso(CLI::App &app);
void RunSubcommandResetTso(ResetTsoOptions const &opt);

struct UpdateTsoOptions {
  std::string coor_url;
  int64_t tso_new_physical;
  int64_t tso_save_physical;
  int64_t tso_new_logical;
};
void SetUpSubcommandUpdateTso(CLI::App &app);
void RunSubcommandUpdateTso(UpdateTsoOptions const &opt);

// gc
struct UpdateGCSafePointOptions {
  std::string coor_url;
  int64_t safe_point;
  std::string gc_flag;
  int64_t tenant_id;
  int64_t safe_point2;
};
void SetUpSubcommandUpdateGCSafePoint(CLI::App &app);
void RunSubcommandUpdateGCSafePoint(UpdateGCSafePointOptions const &opt);

struct GetGCSafePointOptions {
  std::string coor_url;
  bool get_all_tenant;
  int64_t tenant_id;
};
void SetUpSubcommandGetGCSafePoint(CLI::App &app);
void RunSubcommandGetGCSafePoint(GetGCSafePointOptions const &opt);

// balance leader
struct BalanceLeaderOptions {
  std::string coor_url;
  bool dryrun;
  int32_t store_type;
};
void SetUpSubcommandBalanceLeader(CLI::App &app);
void RunSubcommandBalanceLeader(BalanceLeaderOptions const &opt);

// force_read_only
struct UpdateForceReadOnlyOptions {
  std::string coor_url;
  bool force_read_only;
  std::string force_read_only_reason;
};
void SetUpSubcommandUpdateForceReadOnly(CLI::App &app);
void RunSubcommandUpdateForceReadOnly(UpdateForceReadOnlyOptions const &opt);

// sumcomand tools
struct StringToHexOptions {
  std::string key;
};
void SetUpSubcommandStringToHex(CLI::App &app);
void RunSubcommandStringToHex(StringToHexOptions const &opt);

struct HexToStringOptions {
  std::string key;
};
void SetUpSubcommandHexToString(CLI::App &app);
void RunSubcommandHexToString(HexToStringOptions const &opt);

struct EncodeTablePrefixToHexOptions {
  std::string key;
  int64_t part_id;
  char region_prefix;
  bool key_is_hex;
};
void SetUpSubcommandEncodeTablePrefixToHex(CLI::App &app);
void RunSubcommandEncodeTablePrefixToHexr(EncodeTablePrefixToHexOptions const &opt);

struct EncodeVectorPrefixToHexOptions {
  std::string coor_url;
  int64_t vector_id;
  int64_t part_id;
  char region_prefix;
};
void SetUpSubcommandEncodeVectorPrefixToHex(CLI::App &app);
void RunSubcommandEncodeVectorPrefixToHex(EncodeVectorPrefixToHexOptions const &opt);

struct DecodeTablePrefixOptions {
  std::string key;
  bool key_is_hex;
  int64_t part_id;
};
void SetUpSubcommandDecodeTablePrefix(CLI::App &app);
void RunSubcommandDecodeTablePrefix(DecodeTablePrefixOptions const &opt);

struct DecodeVectorPrefixOptions {
  std::string coor_url;
  std::string key;
  bool key_is_hex;
};
void SetUpSubcommandDecodeVectorPrefix(CLI::App &app);
void RunSubcommandDecodeVectorPrefix(DecodeVectorPrefixOptions const &opt);

struct OctalToHexOptions {
  std::string coor_url;
  std::string key;
};
void SetUpSubcommandOctalToHex(CLI::App &app);
void RunSubcommandOctalToHex(OctalToHexOptions const &opt);

struct CoordinatorDebugOptions {
  std::string coor_url;
  std::string start_key;
  std::string end_key;
};
void SetUpSubcommandCoordinatorDebug(CLI::App &app);
void RunSubcommandCoordinatorDebug(CoordinatorDebugOptions const &opt);

// // store/index/document commands
struct AddRegionOptions {
  std::string coor_url;
  std::string raft_group;
  std::string raft_addrs;
  int64_t region_id;
};
void SetUpSubcommandAddRegion(CLI::App &app);
void RunSubcommandAddRegion(AddRegionOptions const &opt);

struct ChangeRegionOptions {
  std::string coor_url;
  int64_t region_id;
  std::string raft_group;
  std::string raft_addrs;
};
void SetUpSubcommandChangeRegion(CLI::App &app);
void RunSubcommandChangeRegion(ChangeRegionOptions const &opt);

struct MergeRegionAtStoreOptions {
  std::string coor_url;
  int64_t source_id;
  int64_t target_id;
};
void SetUpSubcommandMergeRegionAtStore(CLI::App &app);
void RunSubcommandMergeRegionAtStore(MergeRegionAtStoreOptions const &opt);

struct DestroyRegionOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSubcommandDestroyRegion(CLI::App &app);
void RunSubcommandDestroyRegion(DestroyRegionOptions const &opt);

struct SnapshotOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSubcommandSnapshot(CLI::App &app);
void RunSubcommandSnapshot(SnapshotOptions const &opt);

struct BatchAddRegionOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t region_count;
  int32_t thread_num;
  std::string raft_group;
  std::string raft_addrs;
};
void SetUpSubcommandBatchAddRegion(CLI::App &app);
void RunSubcommandBatchAddRegion(BatchAddRegionOptions const &opt);

struct SnapshotVectorIndexOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSubcommandSnapshotVectorIndex(CLI::App &app);
void RunSubcommandSnapshotVectorIndex(SnapshotVectorIndexOptions const &opt);

struct CompactOptions {
  std::string store_addrs;
};
void SetUpSubcommandCompact(CLI::App &app);
void RunSubcommandCompact(CompactOptions const &opt);

struct GetMemoryStatsOptions {
  std::string store_addrs;
};
void SetUpSubcommandGetMemoryStats(CLI::App &app);
void RunSubcommandGetMemoryStats(GetMemoryStatsOptions const &opt);

struct ReleaseFreeMemoryOptions {
  std::string store_addrs;
  double rate;
};
void SetUpSubcommandReleaseFreeMemory(CLI::App &app);
void RunSubcommandReleaseFreeMemory(ReleaseFreeMemoryOptions const &opt);

struct KvGetOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
};
void SetUpSubcommandKvGet(CLI::App &app);
void RunSubcommandKvGet(KvGetOptions const &opt);

struct KvBatchGetOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
  int32_t req_num;
};
void SetUpSubcommandKvBatchGet(CLI::App &app);
void RunSubcommandKvBatchGet(KvBatchGetOptions const &opt);

struct KvPutOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
  std::string value;
};
void SetUpSubcommandKvPut(CLI::App &app);
void RunSubcommandKvPut(KvPutOptions const &opt);

struct KvBatchPutOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
  int64_t count;
};
void SetUpSubcommandKvBatchPut(CLI::App &app);
void RunSubcommandKvBatchPut(KvBatchPutOptions const &opt);

struct KvPutIfAbsentOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
};
void SetUpSubcommandKvPutIfAbsent(CLI::App &app);
void RunSubcommandKvPutIfAbsent(KvPutIfAbsentOptions const &opt);

struct KvBatchPutIfAbsentOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
  int64_t count;
};
void SetUpSubcommandKvBatchPutIfAbsent(CLI::App &app);
void RunSubcommandKvBatchPutIfAbsent(KvBatchPutIfAbsentOptions const &opt);

struct KvBatchDeleteOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
};
void SetUpSubcommandKvBatchDelete(CLI::App &app);
void RunSubcommandKvBatchDelete(KvBatchDeleteOptions const &opt);

struct KvDeleteRangeOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
};
void SetUpSubcommandKvDeleteRange(CLI::App &app);
void RunSubcommandKvDeleteRange(KvDeleteRangeOptions const &opt);

struct KvScanOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
};
void SetUpSubcommandKvScan(CLI::App &app);
void RunSubcommandKvScan(KvScanOptions const &opt);

struct KvCompareAndSetOptions {
  std::string coor_url;
  int64_t region_id;
  std::string key;
};
void SetUpSubcommandKvCompareAndSet(CLI::App &app);
void RunSubcommandKvCompareAndSet(KvCompareAndSetOptions const &opt);

struct KvBatchCompareAndSetOptions {
  std::string coor_url;
  int64_t region_id;
  std::string prefix;
  int64_t count;
};
void SetUpSubcommandKvBatchCompareAndSet(CLI::App &app);
void RunSubcommandKvBatchCompareAndSet(KvBatchCompareAndSetOptions const &opt);

struct KvScanBeginV2Options {
  std::string coor_url;
  int64_t region_id;
  int64_t scan_id;
};
void SetUpSubcommandKvScanBeginV2(CLI::App &app);
void RunSubcommandKvScanBeginV2(KvScanBeginV2Options const &opt);

struct KvScanContinueV2Options {
  std::string coor_url;
  int64_t region_id;
  int64_t scan_id;
};
void SetUpSubcommandKvScanContinueV2(CLI::App &app);
void RunSubcommandKvScanContinueV2(KvScanContinueV2Options const &opt);

struct KvScanReleaseV2Options {
  std::string coor_url;
  int64_t region_id;
  int64_t scan_id;
};
void SetUpSubcommandKvScanReleaseV2(CLI::App &app);
void RunSubcommandKvScanReleaseV2(KvScanReleaseV2Options const &opt);

struct TxnGetOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string key;
  bool key_is_hex;
  int64_t start_ts;
  int64_t resolve_locks;
};
void SetUpSubcommandTxnGet(CLI::App &app);
void RunSubcommandTxnGet(TxnGetOptions const &opt);

struct TxnScanOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string start_key;
  std::string end_key;
  int64_t limit;
  int64_t start_ts;
  bool is_reverse;
  bool key_only;
  int64_t resolve_locks;
  bool key_is_hex;
  bool with_start;
  bool with_end;
};
void SetUpSubcommandTxnScan(CLI::App &app);
void RunSubcommandTxnScan(TxnScanOptions const &opt);

struct TxnPessimisticLockOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string primary_lock;
  bool key_is_hex;
  int64_t start_ts;
  int64_t lock_ttl;
  int64_t for_update_ts;
  std::string mutation_op;
  std::string key;
  std::string value;
  bool value_is_hex;
};
void SetUpSubcommandTxnPessimisticLock(CLI::App &app);
void RunSubcommandTxnPessimisticLock(TxnPessimisticLockOptions const &opt);

struct TxnPessimisticRollbackOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  int64_t start_ts;
  int64_t for_update_ts;
  std::string key;
  bool key_is_hex;
};
void SetUpSubcommandTxnPessimisticRollback(CLI::App &app);
void RunSubcommandTxnPessimisticRollback(TxnPessimisticRollbackOptions const &opt);

struct TxnPrewriteOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string primary_lock;
  bool key_is_hex;
  int64_t start_ts;
  int64_t lock_ttl;
  int64_t txn_size;
  bool try_one_pc;
  int64_t max_commit_ts;
  std::string mutation_op;
  std::string key;
  std::string key2;
  std::string value;
  std::string value2;
  bool value_is_hex;
  std::string extra_data;
  int64_t for_update_ts;

  int64_t vector_id;
  int64_t document_id;
  std::string document_text1;
  std::string document_text2;
};
void SetUpSubcommandTxnPrewrite(CLI::App &app);
void RunSubcommandTxnPrewrite(TxnPrewriteOptions const &opt);

struct TxnCommitOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  int64_t start_ts;
  int64_t commit_ts;
  std::string key;
  std::string key2;
  bool key_is_hex;
};
void SetUpSubcommandTxnCommit(CLI::App &app);
void RunSubcommandTxnCommit(TxnCommitOptions const &opt);

struct TxnCheckTxnStatusOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string primary_key;
  bool key_is_hex;
  int64_t lock_ts;
  int64_t caller_start_ts;
  int64_t current_ts;
};
void SetUpSubcommandTxnCheckTxnStatus(CLI::App &app);
void RunSubcommandTxnCheckTxnStatus(TxnCheckTxnStatusOptions const &opt);

struct TxnResolveLockOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  int64_t start_ts;
  int64_t commit_ts;
  std::string key;
  bool key_is_hex;
};
void SetUpSubcommandTxnResolveLock(CLI::App &app);
void RunSubcommandTxnResolveLock(TxnResolveLockOptions const &opt);

struct TxnBatchGetOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string key;
  std::string key2;
  bool key_is_hex;
  int64_t start_ts;
  int64_t resolve_locks;
};
void SetUpSubcommandTxnBatchGet(CLI::App &app);
void RunSubcommandTxnBatchGet(TxnBatchGetOptions const &opt);

struct TxnBatchRollbackOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string key;
  std::string key2;
  bool key_is_hex;
  int64_t start_ts;
};
void SetUpSubcommandTxnBatchRollback(CLI::App &app);
void RunSubcommandTxnBatchRollback(TxnBatchRollbackOptions const &opt);

struct TxnScanLockOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  int64_t max_ts;
  std::string start_key;
  std::string end_key;
  bool key_is_hex;
  int64_t limit;
};
void SetUpSubcommandTxnScanLock(CLI::App &app);
void RunSubcommandTxnScanLock(TxnScanLockOptions const &opt);

struct TxnHeartBeatOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string primary_lock;
  int64_t start_ts;
  int64_t advise_lock_ttl;
  bool key_is_hex;
};
void SetUpSubcommandTxnHeartBeat(CLI::App &app);
void RunSubcommandTxnHeartBeat(TxnHeartBeatOptions const &opt);

struct TxnGCOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  int64_t safe_point_ts;
};
void SetUpSubcommandTxnGC(CLI::App &app);
void RunSubcommandTxnGC(TxnGCOptions const &opt);

struct TxnDeleteRangeOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string start_key;
  std::string end_key;
  bool key_is_hex;
};
void SetUpSubcommandTxnDeleteRange(CLI::App &app);
void RunSubcommandTxnDeleteRange(TxnDeleteRangeOptions const &opt);

struct TxnDumpOptions {
  std::string coor_url;
  int64_t region_id;
  bool rc;
  std::string start_key;
  std::string end_key;
  bool key_is_hex;
  int64_t start_ts;
  int64_t end_ts;
};
void SetUpSubcommandTxnDump(CLI::App &app);
void RunSubcommandTxnDump(TxnDumpOptions const &opt);

// document operation
struct DocumentDeleteOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t count;
};
void SetUpSubcommandDocumentDelete(CLI::App &app);
void RunSubcommandDocumentDelete(DocumentDeleteOptions const &opt);

struct DocumentAddOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t document_id;
  std::string document_text1;
  std::string document_text2;
  bool is_update;
};
void SetUpSubcommandDocumentAdd(CLI::App &app);
void RunSubcommandDocumentAdd(DocumentAddOptions const &opt);

struct DocumentSearchOptions {
  std::string coor_url;
  int64_t region_id;
  std::string query_string;
  int32_t topn;
  bool without_scalar;
};
void SetUpSubcommandDocumentSearch(CLI::App &app);
void RunSubcommandDocumentSearch(DocumentSearchOptions const &opt);

struct DocumentBatchQueryOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t document_id;
  bool without_scalar;
  std::string key;
};
void SetUpSubcommandDocumentBatchQuery(CLI::App &app);
void RunSubcommandDocumentBatchQuery(DocumentBatchQueryOptions const &opt);

struct DocumentScanQueryOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
  int64_t limit;
  bool is_reverse;
  bool without_scalar;
  std::string key;
};
void SetUpSubcommandDocumentScanQuery(CLI::App &app);
void RunSubcommandDocumentScanQuery(DocumentScanQueryOptions const &opt);

struct DocumentGetMaxIdOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSubcommandDocumentGetMaxId(CLI::App &app);
void RunSubcommandDocumentGetMaxId(DocumentGetMaxIdOptions const &opt);

struct DocumentGetMinIdOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSubcommandDocumentGetMinId(CLI::App &app);
void RunSubcommandDocumentGetMinId(DocumentGetMinIdOptions const &opt);

struct DocumentCountOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
};
void SetUpSubcommandDocumentCount(CLI::App &app);
void RunSubcommandDocumentCount(DocumentCountOptions const &opt);

struct DocumentGetRegionMetricsOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSubcommandDocumentGetRegionMetrics(CLI::App &app);
void RunSubcommandDocumentGetRegionMetrics(DocumentGetRegionMetricsOptions const &opt);

// vector operation
struct VectorSearchOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int32_t topn;
  std::string vector_data;
  std::string key;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  bool with_scalar_pre_filter;
  bool with_table_pre_filter;
  std::string scalar_filter_key;
  std::string scalar_filter_value;
  std::string scalar_filter_key2;
  std::string scalar_filter_value2;
  bool with_scalar_post_filter;
  int64_t ef_search;
  bool bruteforce;
  bool print_vector_search_delay;
  std::string csv_output;
};
void SetUpSubcommandVectorSearch(CLI::App &app);
void RunSubcommandVectorSearch(VectorSearchOptions const &opt);

struct VectorSearchDebugOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int32_t topn;
  int64_t start_vector_id;
  int32_t batch_count;
  std::string key;
  std::string value;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  int32_t vector_ids_count;
  bool with_scalar_pre_filter;
  bool with_scalar_post_filter;
  bool print_vector_search_delay;
};
void SetUpSubcommandVectorSearchDebug(CLI::App &app);
void RunSubcommandVectorSearchDebug(VectorSearchDebugOptions const &opt);

struct VectorRangeSearchOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  double radius;
  std::string key;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  bool with_scalar_pre_filter;
  bool with_scalar_post_filter;
  bool print_vector_search_delay;
};
void SetUpSubcommandVectorRangeSearch(CLI::App &app);
void RunSubcommandVectorRangeSearch(VectorRangeSearchOptions const &opt);

struct VectorRangeSearchDebugOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  double radius;
  int64_t start_vector_id;
  int32_t batch_count;
  std::string key;
  std::string value;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  int32_t vector_ids_count;
  bool with_scalar_pre_filter;
  bool with_scalar_post_filter;
  bool print_vector_search_delay;
};
void SetUpSubcommandVectorRangeSearchDebug(CLI::App &app);
void RunSubcommandVectorRangeSearchDebug(VectorRangeSearchDebugOptions const &opt);

struct VectorBatchSearchOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int32_t topn;
  int32_t batch_count;
  std::string key;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool with_vector_ids;
  bool with_scalar_pre_filter;
  bool with_scalar_post_filter;
  bool print_vector_search_delay;
};
void SetUpSubcommandVectorBatchSearch(CLI::App &app);
void RunSubcommandVectorBatchSearch(VectorBatchSearchOptions const &opt);

struct VectorBatchQueryOptions {
  std::string coor_url;
  int64_t region_id;
  std::vector<int64_t> vector_ids;
  std::string key;
  bool without_vector;
  bool without_scalar;
  bool without_table;
};
void SetUpSubcommandVectorBatchQuery(CLI::App &app);
void RunSubcommandVectorBatchQuery(VectorBatchQueryOptions const &opt);

struct VectorScanQueryOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
  int64_t limit;
  bool without_vector;
  bool without_scalar;
  bool without_table;
  bool is_reverse;
  std::string key;
  std::string scalar_filter_key;
  std::string scalar_filter_value;
  std::string scalar_filter_key2;
  std::string scalar_filter_value2;
};
void SetUpSubcommandVectorScanQuery(CLI::App &app);
void RunSubcommandVectorScanQuery(VectorScanQueryOptions const &opt);

struct VectorScanDumpOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
  int64_t limit;
  bool is_reverse;
  std::string csv_output;
};
void SetUpSubcommandVectorScanDump(CLI::App &app);
void RunSubcommandVectorScanDump(VectorScanDumpOptions const &opt);

struct VectorGetRegionMetricsOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSubcommandVectorGetRegionMetrics(CLI::App &app);
void RunSubcommandVectorGetRegionMetricsd(VectorGetRegionMetricsOptions const &opt);

struct VectorAddOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t region_id;
  int32_t dimension;
  int64_t start_id;
  int32_t count;
  int32_t step_count;
  bool without_scalar;
  bool without_table;
  std::string csv_data;
  std::string json_data;

  std::string scalar_filter_key;
  std::string scalar_filter_value;
  std::string scalar_filter_key2;
  std::string scalar_filter_value2;
};
void SetUpSubcommandVectorAdd(CLI::App &app);
void RunSubcommandVectorAdd(VectorAddOptions const &opt);

struct VectorDeleteOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int32_t count;
};
void SetUpSubcommandVectorDelete(CLI::App &app);
void RunSubcommandVectorDelete(VectorDeleteOptions const &opt);

struct VectorGetMaxIdOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSubcommandVectorGetMaxId(CLI::App &app);
void RunSubcommandVectorGetMaxId(VectorGetMaxIdOptions const &opt);

struct VectorGetMinIdOptions {
  std::string coor_url;
  int64_t region_id;
};
void SetUpSubcommandVectorGetMinId(CLI::App &app);
void RunSubcommandVectorGetMinId(VectorGetMinIdOptions const &opt);

struct VectorAddBatchOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int64_t start_id;
  int32_t count;
  int32_t step_count;
  std::string vector_index_add_cost_file;
  bool without_scalar;
};
void SetUpSubcommandVectorAddBatch(CLI::App &app);
void RunSubcommandVectorAddBatch(VectorAddBatchOptions const &opt);

struct VectorAddBatchDebugOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  int64_t start_id;
  int32_t count;
  int32_t step_count;
  std::string vector_index_add_cost_file;
  bool without_scalar;
};
void SetUpSubcommandVectorAddBatchDebug(CLI::App &app);
void RunSubcommandVectorAddBatchDebug(VectorAddBatchDebugOptions const &opt);

struct VectorCalcDistanceOptions {
  std::string coor_url;
  int64_t region_id;
  int32_t dimension;
  std::string alg_type;
  std::string metric_type;
  int32_t left_vector_size;
  int32_t right_vector_size;
  bool is_return_normlize;
};
void SetUpSubcommandVectorCalcDistance(CLI::App &app);
void RunSubcommandVectorCalcDistance(VectorCalcDistanceOptions const &opt);

struct CalcDistanceOptions {
  std::string vector_data1;
  std::string vector_data2;
};
void SetUpSubcommandCalcDistance(CLI::App &app);
void RunSubcommandCalcDistance(CalcDistanceOptions const &opt);

struct VectorCountOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t start_id;
  int64_t end_id;
};
void SetUpSubcommandVectorCount(CLI::App &app);
void RunSubcommandVectorCount(VectorCountOptions const &opt);

struct CountVectorTableOptions {
  std::string coor_url;
  std::string store_addrs;
  int64_t table_id;
};
void SetUpSubcommandCountVectorTable(CLI::App &app);
void RunSubcommandCountVectorTable(CountVectorTableOptions const &opt);

// test operation
struct TestBatchPutOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t region_id;
  int32_t thread_num;
  int64_t req_num;
  std::string prefix;
};
void SetUpSubcommandTestBatchPut(CLI::App &app);
void RunSubcommandTestBatchPut(TestBatchPutOptions const &opt);

struct TestBatchPutGetOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t region_id;
  int32_t thread_num;
  int64_t req_num;
  std::string prefix;
};
void SetUpSubcommandTestBatchPutGet(CLI::App &app);
void RunSubcommandTestBatchPutGet(TestBatchPutGetOptions const &opt);

struct TestRegionLifecycleOptions {
  std::string coor_url;
  int64_t region_id;
  std::string raft_group;
  std::string raft_addrs;
  int64_t region_count;
  int32_t thread_num;
  int64_t req_num;
  std::string prefix;
};
void SetUpSubcommandTestRegionLifecycle(CLI::App &app);
void RunSubcommandTestRegionLifecycle(TestRegionLifecycleOptions const &opt);

struct TestDeleteRangeWhenTransferLeaderOptions {
  std::string coor_url;
  int64_t region_id;
  int64_t req_num;
  std::string prefix;
};
void SetUpSubcommandTestDeleteRangeWhenTransferLeader(CLI::App &app);
void RunSubcommandTestDeleteRangeWhenTransferLeader(TestDeleteRangeWhenTransferLeaderOptions const &opt);

struct AutoTestOptions {
  std::string coor_url;
  std::string store_addrs;
  std::string table_name;
  int64_t partition_num;
  int64_t req_num;
};
void SetUpSubcommandAutoTest(CLI::App &app);
void RunSubcommandAutoTest(AutoTestOptions const &opt);

struct AutoMergeRegionOptions {
  std::string coor_url;
  std::string store_addrs;
  int64_t table_id;
  int64_t index_id;
};
void SetUpSubcommandAutoMergeRegion(CLI::App &app);
void RunSubcommandAutoMergeRegion(AutoMergeRegionOptions const &opt);

// test operation
struct AutoDropTableOptions {
  std::string coor_url;
  int64_t req_num;
};
void SetUpSubcommandAutoDropTable(CLI::App &app);
void RunSubcommandAutoDropTable(AutoDropTableOptions const &opt);

struct CheckTableDistributionOptions {
  std::string coor_url;
  int64_t table_id;
  std::string key;
};
void SetUpSubcommandCheckTableDistribution(CLI::App &app);
void RunSubcommandCheckTableDistribution(CheckTableDistributionOptions const &opt);

struct CheckIndexDistributionOptions {
  std::string coor_url;
  int64_t table_id;
};
void SetUpSubcommandCheckIndexDistribution(CLI::App &app);
void RunSubcommandCheckIndexDistribution(CheckIndexDistributionOptions const &opt);

struct DumpDbOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t index_id;
  std::string db_path;
  int32_t offset;
  int64_t limit;
  bool show_vector;
  bool show_lock;
  bool show_write;
  bool show_last_data;
  bool show_all_data;
  bool show_pretty;
  int32_t print_column_width;
};
void SetUpSubcommandDumpDb(CLI::App &app);
void RunSubcommandDumpDb(DumpDbOptions const &opt);

struct WhichRegionOptions {
  std::string coor_url;
  int64_t table_id;
  int64_t index_id;
  std::string key;
};
void SetUpSubcommandWhichRegion(CLI::App &app);
void RunSubcommandWhichRegion(WhichRegionOptions const &opt);

struct RegionMetricsOptions {
  std::string coor_url;
  std::string store_addrs;
  std::vector<int64_t> region_ids;
  int type;
};
void SetUpSubcommandRegionMetrics(CLI::App &app);
void RunSubcommandRegionMetrics(RegionMetricsOptions const &opt);

}  // namespace client_v2
#endif  // DINGODB_SUBCOMMAND_COORDINATOR_H_
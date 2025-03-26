
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

#ifndef DINGODB_CLIENT_COORDINATOR_H_
#define DINGODB_CLIENT_COORDINATOR_H_

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

#include "CLI/CLI.hpp"
#include "client_v2/helper.h"
#include "client_v2/interation.h"
#include "client_v2/store.h"
#include "coordinator/coordinator_interaction.h"
#include "proto/coordinator.pb.h"

namespace client_v2 {

void SetUpCoordinatorSubCommands(CLI::App &app);

bool GetBrpcChannel(const std::string &location, brpc::Channel &channel);

dingodb::pb::common::Region SendQueryRegion(int64_t region_id);
void SendTransferLeaderByCoordinator(int64_t region_id, int64_t leader_store_id);
void SendTransferLeaderByCoordinator(int64_t region_id, int64_t leader_store_id);
void SendMergeRegionToCoor(int64_t source_id, int64_t target_id);
uint32_t SendGetJobList();
dingodb::pb::common::StoreMap SendGetStoreMap();
void SendChangePeer(const dingodb::pb::common::RegionDefinition &region_definition);
void SendSplitRegion(const dingodb::pb::common::RegionDefinition &region_definition);

struct RaftAddPeerCommandOptions {
  std::string coordinator_addr;
  std::string peer;
  int index;
};

void SetUpRaftAddPeer(CLI::App &app);
void RunRaftAddPeer(RaftAddPeerCommandOptions const &opt);

struct GetRegionMapCommandOptions {
  std::string coor_url;
  int64_t tenant_id;
};

void SetUpGetRegionMap(CLI::App &app);
void RunGetRegionMap(GetRegionMapCommandOptions const &opt);

struct GetLogLevelCommandOptions {
  std::string coordinator_addr;
  int64_t timeout_ms;
};

void SetUpLogLevel(CLI::App &app);
void RunLogLevel(GetLogLevelCommandOptions const &opt);

struct RaftRemovePeerOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpRaftRemovePeer(CLI::App &app);
void RunRaftRemovePeer(RaftRemovePeerOption const &opt);

struct RaftTransferLeaderOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpRaftTransferLeader(CLI::App &app);
void RunRaftTransferLeader(RaftTransferLeaderOption const &opt);

struct RaftSnapshotOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpRaftSnapshot(CLI::App &app);
void RunRaftSnapshot(RaftSnapshotOption const &opt);

struct RaftResetPeerOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpRaftResetPeer(CLI::App &app);
void RunRaftResetPeer(RaftResetPeerOption const &opt);

struct GetNodeInfoOption {
  std::string coordinator_addr;
  std::string peer;
  int index;
};
void SetUpGetNodeInfo(CLI::App &app);
void RunGetNodeInfo(GetNodeInfoOption const &opt);

// todo

struct GetChangeLogLevelOption {
  std::string coordinator_addr;
  std::string level;
};
void SetUpChangeLogLevel(CLI::App &app);
void RunChangeLogLevel(GetChangeLogLevelOption const &opt);

struct HelloOption {
  std::string coor_url;
};
void SetUpHello(CLI::App &app);
void RunHello(HelloOption const &opt);

struct StoreHeartbeatOption {
  std::string coor_url;
};
void SetUpStoreHeartbeat(CLI::App &app);
void RunStoreHeartbeat(StoreHeartbeatOption const &opt);
void SendStoreHearbeatV2(dingodb::CoordinatorInteractionPtr coordinator_interaction, int64_t store_id);

struct CreateStoreOption {
  std::string coor_url;
};
void SetUpCreateStore(CLI::App &app);
void RunCreateStore(CreateStoreOption const &opt);

struct DeleteStoreOption {
  std::string coor_url;
  std::string keyring;
  std::string id;
};
void SetUpDeleteStore(CLI::App &app);
void RunDeleteStore(DeleteStoreOption const &opt);

struct UpdateStoreOption {
  std::string coor_url;
  std::string keyring;
  std::string id;
  std::string state;
};
void SetUpUpdateStore(CLI::App &app);
void RunUpdateStore(UpdateStoreOption const &opt);

struct CreateExecutorOption {
  std::string coor_url;
  std::string keyring;
  std::string host;
  int port;
  std::string user;
};
void SetUpCreateExecutor(CLI::App &app);
void RunCreateExecutor(CreateExecutorOption const &opt);

struct DeleteExecutorOption {
  std::string coor_url;
  std::string keyring;
  std::string id;
  std::string user;
};
void SetUpDeleteExecutor(CLI::App &app);
void RunDeleteExecutor(DeleteExecutorOption const &opt);

struct CreateExecutorUserOption {
  std::string coor_url;
  std::string keyring;
  std::string user;
};
void SetUpCreateExecutorUser(CLI::App &app);
void RunCreateExecutorUser(CreateExecutorUserOption const &opt);

struct UpdateExecutorUserOption {
  std::string coor_url;
  std::string keyring;
  std::string new_keyring;
  std::string user;
};
void SetUpUpdateExecutorUser(CLI::App &app);
void RunUpdateExecutorUser(UpdateExecutorUserOption const &opt);

struct DeleteExecutorUserOption {
  std::string coor_url;
  std::string keyring;
  std::string user;
};
void SetUpDeleteExecutorUser(CLI::App &app);
void RunDeleteExecutorUser(DeleteExecutorUserOption const &opt);

struct GetExecutorUserMapOption {
  std::string coor_url;
};
void SetUpGetExecutorUserMap(CLI::App &app);
void RunGetExecutorUserMap(GetExecutorUserMapOption const &opt);

struct ExecutorHeartbeatOption {
  std::string coor_url;
  std::string keyring;
  std::string host;
  int port;
  std::string user;
  std::string id;
  std::string cluster_name;
};
void SetUpExecutorHeartbeat(CLI::App &app);
void RunExecutorHeartbeat(ExecutorHeartbeatOption const &opt);

struct GetStoreMapOption {
  std::string coor_url;
  int32_t filter_store_type;
  bool use_filter_store_type;
};
void SetUpGetStoreMap(CLI::App &app);
void RunGetStoreMap(GetStoreMapOption const &opt);

struct GetExecutorMapOption {
  std::string coor_url;
  std::string cluster_name;
};
void SetUpGetExecutorMap(CLI::App &app);
void RunGetExecutorMap(GetExecutorMapOption const &opt);

struct GetDeleteRegionMapOption {
  std::string coor_url;
};
void SetUpGetDeleteRegionMap(CLI::App &app);
void RunGetDeleteRegionMap(GetDeleteRegionMapOption const &opt);

struct AddDeleteRegionMapOption {
  std::string coor_url;
  std::string id;
  bool is_force;
};
void SetUpAddDeleteRegionMap(CLI::App &app);
void RunAddDeleteRegionMap(AddDeleteRegionMapOption const &opt);

struct CleanDeleteRegionMapOption {
  std::string coor_url;
  std::string id;
};
void SetUpCleanDeleteRegionMap(CLI::App &app);
void RunCleanDeleteRegionMap(CleanDeleteRegionMapOption const &opt);

struct GetRegionCountOption {
  std::string coor_url;
};
void SetUpGetRegionCount(CLI::App &app);
void RunGetRegionCount(GetRegionCountOption const &opt);

struct GetCoordinatorMapOption {
  std::string coor_url;
  bool get_coordinator_map;
};
void SetUpGetCoordinatorMap(CLI::App &app);
void RunGetCoordinatorMap(GetCoordinatorMapOption const &opt);

struct CreateRegionIdOption {
  std::string coor_url;
  int count;
};
void SetUpCreateRegionId(CLI::App &app);
void RunCreateRegionId(CreateRegionIdOption const &opt);

struct QueryRegionOption {
  std::string coor_url;
  int64_t id;
};
void SetUpQueryRegion(CLI::App &app);
void RunQueryRegion(QueryRegionOption const &opt);

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
void SetUpCreateRegion(CLI::App &app);
void RunCreateRegion(CreateRegionOption const &opt);

struct CreateRegionForSplitOption {
  std::string coor_url;
  int64_t id;
};
void SetUpCreateRegionForSplit(CLI::App &app);
void RunCreateRegionForSplit(CreateRegionForSplitOption const &opt);

struct DropRegionOption {
  std::string coor_url;
  int64_t id;
};
void SetUpDropRegion(CLI::App &app);
void RunDropRegion(DropRegionOption const &opt);

struct DropRegionPermanentlyOption {
  std::string coor_url;
  std::string id;
};
void SetUpDropRegionPermanently(CLI::App &app);
void RunDropRegionPermanently(DropRegionPermanentlyOption const &opt);

struct SplitRegionOption {
  std::string coor_url;
  int64_t split_to_id;
  int64_t split_from_id;
  std::string split_key;
  int64_t vector_id;
  int64_t document_id;
  bool store_create_region;
};
void SetUpSplitRegion(CLI::App &app);
void RunSplitRegion(SplitRegionOption const &opt);

struct MergeRegionOption {
  std::string coor_url;
  int64_t target_id;
  int64_t source_id;
};
void SetUpMergeRegion(CLI::App &app);
void RunMergeRegion(MergeRegionOption const &opt);

struct AddPeerRegionOption {
  std::string coor_url;
  int64_t store_id;
  int64_t region_id;
};
void SetUpAddPeerRegion(CLI::App &app);
void RunAddPeerRegion(AddPeerRegionOption const &opt);

struct RemovePeerRegionOption {
  std::string coor_url;
  int64_t store_id;
  int64_t region_id;
};
void SetUpRemovePeerRegion(CLI::App &app);
void RunRemovePeerRegion(RemovePeerRegionOption const &opt);

struct TransferLeaderRegionOption {
  std::string coor_url;
  int64_t store_id;
  int64_t region_id;
  bool is_force;
};
void SetUpTransferLeaderRegion(CLI::App &app);
void RunTransferLeaderRegion(TransferLeaderRegionOption const &opt);

struct GetOrphanRegionOption {
  std::string coor_url;
  int64_t store_id;
};
void SetUpGetOrphanRegion(CLI::App &app);
void RunGetOrphanRegion(GetOrphanRegionOption const &opt);

struct ScanRegionsOptions {
  std::string coor_url;
  std::string key;
  bool key_is_hex;
  std::string range_end;
  int64_t limit;
};
void SetUpScanRegions(CLI::App &app);
void RunScanRegions(ScanRegionsOptions const &opt);

struct GetRangeRegionMapOption {
  std::string coor_url;
};
void SetUpGetRangeRegionMap(CLI::App &app);
void RunGetRangeRegionMap(GetRangeRegionMapOption const &opt);

struct GetStoreOperationOption {
  std::string coor_url;
  int64_t store_id;
};
void SetUpGetStoreOperation(CLI::App &app);
void RunGetStoreOperation(GetStoreOperationOption const &opt);

struct GetJobListOptions {
  std::string coor_url;
  int64_t id;
  bool include_archive;
  int64_t start_id;
  int64_t limit;
  bool json_type;
};
void SetUpGetJobList(CLI::App &app);
void RunGetJobList(GetJobListOptions const &opt);

struct CleanJobListOption {
  std::string coor_url;
  int64_t id;
};
void SetUpCleanJobList(CLI::App &app);
void RunCleanJobList(CleanJobListOption const &opt);

struct UpdateRegionCmdStatusOptions {
  std::string coor_url;
  int64_t job_id;
  int64_t region_cmd_id;
  int64_t status;
  int64_t errcode;
  std::string errmsg;
};
void SetUpUpdateRegionCmdStatus(CLI::App &app);
void RunUpdateRegionCmdStatus(UpdateRegionCmdStatusOptions const &opt);

struct CleanStoreOperationOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpCleanStoreOperation(CLI::App &app);
void RunCleanStoreOperation(CleanStoreOperationOptions const &opt);

struct AddStoreOperationOptions {
  std::string coor_url;
  int64_t id;
  int64_t region_id;
};
void SetUpAddStoreOperation(CLI::App &app);
void RunAddStoreOperation(AddStoreOperationOptions const &opt);

struct RemoveStoreOperationOptions {
  std::string coor_url;
  int64_t id;
  int64_t region_cmd_id;
};
void SetUpRemoveStoreOperation(CLI::App &app);
void RunRemoveStoreOperation(RemoveStoreOperationOptions const &opt);

struct GetRegionCmdOptions {
  std::string coor_url;
  int64_t store_id;
  int64_t start_region_cmd_id;
  int64_t end_region_cmd_id;
};
void SetUpGetRegionCmd(CLI::App &app);
void RunGetRegionCmd(GetRegionCmdOptions const &opt);

struct GetStoreMetricsOptions {
  std::string coor_url;
  int64_t id;
  int64_t region_id;
};
void SetUpGetStoreMetricsn(CLI::App &app);
void RunGetStoreMetrics(GetStoreMetricsOptions const &opt);

struct DeleteStoreMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpDeleteStoreMetrics(CLI::App &app);
void RunDeleteStoreMetrics(DeleteStoreMetricsOptions const &opt);

struct GetRegionMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpGetRegionMetrics(CLI::App &app);
void RunGetRegionMetrics(GetRegionMetricsOptions const &opt);

struct DeleteRegionMetricsOptions {
  std::string coor_url;
  int64_t id;
};
void SetUpDeleteRegionMetrics(CLI::App &app);
void RunDeleteRegionMetrics(DeleteRegionMetricsOptions const &opt);

// gc
struct UpdateGCSafePointOptions {
  std::string coor_url;
  int64_t safe_point;
  std::string gc_flag;
  int64_t tenant_id;
  int64_t safe_point2;
};
void SetUpUpdateGCSafePoint(CLI::App &app);
void RunUpdateGCSafePoint(UpdateGCSafePointOptions const &opt);

struct UpdateTenantGCSafePointOptions {
  std::string coor_url;
  int64_t safe_point;
  int64_t tenant_id;
};
void SetUpUpdateTenantGCSafePoint(CLI::App &app);
void RunUpdateTenantGCSafePoint(UpdateTenantGCSafePointOptions const &opt);

struct UpdateGCFlagOptions {
  std::string coor_url;
  int64_t safe_point;
  std::string gc_flag;
};
void SetUpUpdateGCFlag(CLI::App &app);
void RunUpdateGCFlag(UpdateGCFlagOptions const &opt);

struct GetGCSafePointOptions {
  std::string coor_url;
  bool get_all_tenant;
  int64_t tenant_id;
};
void SetUpGetGCSafePoint(CLI::App &app);
void RunGetGCSafePoint(GetGCSafePointOptions const &opt);

// balance leader
struct BalanceLeaderOptions {
  std::string coor_url;
  bool dryrun;
  int32_t store_type;
};
void SetUpBalanceLeader(CLI::App &app);
void RunBalanceLeader(BalanceLeaderOptions const &opt);

struct BalanceRegionOptions {
  std::string coor_url;
  bool dryrun;
  int32_t store_type;
};
void SetUpBalanceRegion(CLI::App &app);
void RunBalanceRegion(BalanceRegionOptions const &opt);

// force_read_only
struct UpdateForceReadOnlyOptions {
  std::string coor_url;
  bool force_read_only;
  std::string force_read_only_reason;
};
void SetUpUpdateForceReadOnly(CLI::App &app);
void RunUpdateForceReadOnly(UpdateForceReadOnlyOptions const &opt);

struct GetBackUpStatusOptions {
  std::string coor_url;
};
void SetUpGetBackUpStatus(CLI::App &app);
void RunGetBackUpStatus(GetBackUpStatusOptions const &opt);

struct GetRestoreStatusOptions {
  std::string coor_url;
};
void SetUpGetRestoreStatus(CLI::App &app);
void RunGetRestoreStatus(GetRestoreStatusOptions const &opt);

struct EnableOrDisableBalanceOptions {
  std::string coor_url;
  std::string enable_balance_leader;
  std::string enable_balance_region;
};
void SetUpEnableOrDisableBalance(CLI::App &app);
void RunEnableOrDisableBalance(EnableOrDisableBalanceOptions const &opt);

}  // namespace client_v2

#endif  // DINGODB_CLIENT_COORDINATOR_H_
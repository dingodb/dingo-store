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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGODB_COORDINATOR_CONTROL_H_
#define DINGODB_COORDINATOR_CONTROL_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "butil/containers/flat_map.h"
#include "butil/status.h"
#include "common/meta_control.h"
#include "common/safe_map.h"
#include "coordinator/coordinator_meta_storage.h"
#include "engine/engine.h"
#include "engine/snapshot.h"
#include "google/protobuf/stubs/callback.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "metrics/coordinator_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "proto/version.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

struct WatchNode {
  WatchNode(google::protobuf::Closure *done, pb::version::WatchResponse *response)
      : done(done),
        response(response),
        start_revision(0),
        no_put_event(false),
        no_delete_event(false),
        need_prev_kv(false) {}

  WatchNode(google::protobuf::Closure *done, pb::version::WatchResponse *response, int64_t start_revision,
            bool no_put_event, bool no_delete_event, bool need_prev_kv)
      : done(done),
        response(response),
        start_revision(start_revision),
        no_put_event(no_put_event),
        no_delete_event(no_delete_event),
        need_prev_kv(need_prev_kv) {}

  google::protobuf::Closure *done;
  pb::version::WatchResponse *response;
  int64_t start_revision;
  bool no_put_event;
  bool no_delete_event;
  bool need_prev_kv;
};

class AtomicGuard {
 public:
  AtomicGuard(std::atomic<bool> &flag) : m_flag_(flag) { m_flag_.store(true); }
  ~AtomicGuard() {
    if (!released_) {
      m_flag_.store(false);
    }
  }

  void Release() { released_ = true; }

 private:
  bool released_ = false;
  std::atomic<bool> &m_flag_;
};

class MetaBvarCoordinator {
 public:
  MetaBvarCoordinator() {
    is_leader_.expose_as("dingo_metrics_coordinator", "is_leader");
    is_leader_.set_value(0);
  }
  ~MetaBvarCoordinator() = default;

  void SetValue(int64_t value) { is_leader_.set_value(value); }

 private:
  bvar::Status<int64_t> is_leader_;
};

class MetaBvarStore {
 public:
  MetaBvarStore(int64_t store_id) {
    total_capacity_.expose_as("dingo_metrics_store", "total_capacity_" + std::to_string(store_id));
    free_capacity_.expose_as("dingo_metrics_store", "free_capacity_" + std::to_string(store_id));
  }
  ~MetaBvarStore() = default;

  void SetTotalCapacity(int64_t value) { total_capacity_.set_value(value); }
  void SetFreeCapacity(int64_t value) { free_capacity_.set_value(value); }

 private:
  bvar::Status<int64_t> total_capacity_;
  bvar::Status<int64_t> free_capacity_;
};

class MetaBvarRegion {
 public:
  MetaBvarRegion(int64_t region_id) {
    row_count_.expose_as("dingo_metrics_region", "row_count_" + std::to_string(region_id));
    region_size_.expose_as("dingo_metrics_region", "region_size_" + std::to_string(region_id));
  }
  ~MetaBvarRegion() = default;

  void SetRowCount(int64_t value) { row_count_.set_value(value); }
  void SetRegionSize(int64_t value) { region_size_.set_value(value); }

 private:
  bvar::Status<int64_t> row_count_;
  bvar::Status<int64_t> region_size_;
};

class MetaBvarTable {
 public:
  MetaBvarTable(int64_t table_id) {
    row_count_.expose_as("dingo_metrics_table", "row_count_" + std::to_string(table_id));
    part_count_.expose_as("dingo_metrics_table", "part_count_" + std::to_string(table_id));
  }
  ~MetaBvarTable() = default;

  void SetRowCount(int64_t value) { row_count_.set_value(value); }
  void SetPartCount(int64_t value) { part_count_.set_value(value); }

 private:
  bvar::Status<int64_t> row_count_;
  bvar::Status<int64_t> part_count_;
};

struct LeaseWithKeys {
  pb::coordinator_internal::LeaseInternal lease;
  std::set<std::string> keys;
};

class CoordinatorControl : public MetaControl {
 public:
  CoordinatorControl(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer,
                     std::shared_ptr<RawEngine> raw_engine_of_meta);
  ~CoordinatorControl() override;

  template <typename T>
  void RedirectResponse(T response) {
    pb::common::Location leader_location;
    GetLeaderLocation(leader_location);

    auto *error_in_response = response->mutable_error();
    *(error_in_response->mutable_leader_location()) = leader_location;
    error_in_response->set_errcode(pb::error::Errno::ERAFT_NOTLEADER);
    error_in_response->set_errmsg("not leader, new leader location is " + leader_location.DebugString());
  }

  bool Recover();
  static void GenerateRootSchemas(pb::coordinator_internal::SchemaInternal &root_schema,
                                  pb::coordinator_internal::SchemaInternal &meta_schema,
                                  pb::coordinator_internal::SchemaInternal &dingo_schema,
                                  pb::coordinator_internal::SchemaInternal &mysql_schema,
                                  pb::coordinator_internal::SchemaInternal &information_schema);
  bool Init();
  void SetKvEngine(std::shared_ptr<Engine> engine) { engine_ = engine; };

  // SubmitMetaIncrement
  // in:  meta_increment
  // return: 0 or -1
  butil::Status SubmitMetaIncrementSync(pb::coordinator_internal::MetaIncrement &meta_increment);

  // GetMemoryInfo
  void GetMemoryInfo(pb::coordinator::CoordinatorMemoryInfo &memory_info);

  // Get raft leader's server location for sdk use
  void GetLeaderLocation(pb::common::Location &leader_server_location) override;

  // use raft_location to get server_location
  // in: raft_location
  // out: server_location
  void GetServerLocation(pb::common::Location &raft_location, pb::common::Location &server_location);
  void GetRaftLocation(pb::common::Location &server_location, pb::common::Location &raft_location);

  // create region ids
  butil::Status CreateRegionId(uint32_t count, std::vector<int64_t> &region_ids,
                               pb::coordinator_internal::MetaIncrement &meta_increment);

  // query region info
  butil::Status QueryRegion(int64_t region_id, pb::common::Region &region);

  // create region
  // in: resource_tag
  // out: new region id
  // return: errno
  butil::Status SelectStore(pb::common::StoreType store_type, int32_t replica_num, const std::string &resource_tag,
                            const pb::common::IndexParameter &index_parameter, std::vector<int64_t> &store_ids,
                            std::vector<pb::common::Store> &selected_stores_for_regions);

  butil::Status CheckRegionPrefix(const std::string &start_key, const std::string &end_key);

  butil::Status CreateShadowRegion(const std::string &region_name, pb::common::RegionType region_type,
                                   pb::common::RawEngine raw_engine, const std::string &resource_tag,
                                   int32_t replica_num, pb::common::Range region_range, int64_t schema_id,
                                   int64_t table_id, int64_t index_id, int64_t part_id,
                                   const pb::common::IndexParameter &index_parameter, std::vector<int64_t> &store_ids,
                                   int64_t split_from_region_id, int64_t &new_region_id,
                                   pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status CreateRegionFinal(const std::string &region_name, pb::common::RegionType region_type,
                                  pb::common::RawEngine raw_engine, const std::string &resource_tag,
                                  int32_t replica_num, pb::common::Range region_range, int64_t schema_id,
                                  int64_t table_id, int64_t index_id, int64_t part_id,
                                  const pb::common::IndexParameter &index_parameter, std::vector<int64_t> &store_ids,
                                  int64_t split_from_region_id, int64_t &new_region_id,
                                  std::vector<pb::coordinator::StoreOperation> &store_operations,
                                  pb::coordinator_internal::MetaIncrement &meta_increment);

  butil::Status CreateRegionAutoSelectStore(const std::string &region_name, pb::common::RegionType region_type,
                                            pb::common::RawEngine raw_engine, const std::string &resource_tag,
                                            int32_t replica_num, pb::common::Range region_range, int64_t schema_id,
                                            int64_t table_id, int64_t index_id, int64_t part_id,
                                            const pb::common::IndexParameter &index_parameter, int64_t &new_region_id,
                                            pb::coordinator_internal::MetaIncrement &meta_increment);

  butil::Status CreateRegionForSplit(const std::string &region_name, pb::common::RegionType region_type,
                                     const std::string &resource_tag, pb::common::Range region_range, int64_t schema_id,
                                     int64_t table_id, int64_t index_id, int64_t part_id,
                                     const pb::common::IndexParameter &index_parameter, int64_t split_from_region_id,
                                     int64_t &new_region_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  butil::Status CreateRegionForSplitInternal(int64_t split_from_region_id, int64_t &new_region_id,
                                             bool is_shadow_create,
                                             std::vector<pb::coordinator::StoreOperation> &store_operations,
                                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop region
  // in:  region_id
  // in:  need_update_table_range
  // return: errno
  butil::Status DropRegionFinal(int64_t region_id, std::vector<pb::coordinator::StoreOperation> &store_operations,
                                pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status DropRegion(int64_t region_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop region permanently
  // in:  region_id
  // return: errno
  butil::Status DropRegionPermanently(int64_t region_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // split region
  butil::Status SplitRegion(int64_t split_from_region_id, int64_t split_to_region_id, std::string split_watershed_key,
                            pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status SplitRegionWithTaskList(int64_t split_from_region_id, int64_t split_to_region_id,
                                        std::string split_watershed_key, bool store_create_region,
                                        pb::coordinator_internal::MetaIncrement &meta_increment);

  // merge region
  butil::Status MergeRegionWithTaskList(int64_t merge_from_region_id, int64_t merge_to_region_id,
                                        pb::coordinator_internal::MetaIncrement &meta_increment);

  // change peer region
  butil::Status ChangePeerRegionWithTaskList(int64_t region_id, std::vector<int64_t> &new_store_ids,
                                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // transfer leader region
  butil::Status TransferLeaderRegionWithTaskList(int64_t region_id, int64_t new_leader_store_id,
                                                 pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: parent_schema_id
  // in: schema_name
  // out: new schema_id
  // return: errno
  butil::Status CreateSchema(int64_t parent_schema_id, std::string schema_name, int64_t &new_schema_id,
                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop schema
  // in: parent_schema_id
  // in: schema_id
  // return: 0 or -1
  butil::Status DropSchema(int64_t parent_schema_id, int64_t schema_id,
                           pb::coordinator_internal::MetaIncrement &meta_increment);

  // delete table_name in safe_map in rollback
  butil::Status RollbackCreateTable(int64_t schema_id, const std::string &table_name);
  butil::Status RollbackCreateIndex(int64_t schema_id, const std::string &table_name);

  // create table_id
  // in: schema_id
  // out: new table_id
  // return: errno
  butil::Status CreateTableId(int64_t schema_id, int64_t &new_table_id,
                              pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status CreateTableIds(int64_t schema_id, int64_t count, std::vector<int64_t> &new_table_ids,
                               pb::coordinator_internal::MetaIncrement &meta_increment);

  // create table
  // in: schema_id
  // in: table_definition
  // out: new table_id
  // out: new region_ids
  // return: errno
  butil::Status CreateTable(int64_t schema_id, const pb::meta::TableDefinition &table_definition, int64_t &new_table_id,
                            std::vector<int64_t> &region_ids, pb::coordinator_internal::MetaIncrement &meta_increment);

  butil::Status UpdateTableDefinition(int64_t table_id, bool is_index,
                                      const pb::meta::TableDefinition &table_definition,
                                      pb::coordinator_internal::MetaIncrement &meta_increment);

  butil::Status AddIndexOnTable(int64_t table_id, int64_t index_id, const pb::meta::TableDefinition &table_definition,
                                pb::coordinator_internal::MetaIncrement &meta_increment);

  butil::Status DropIndexOnTable(int64_t table_id, int64_t index_id,
                                 pb::coordinator_internal::MetaIncrement &meta_increment);

  // create index_id
  // in: schema_id
  // out: new index_id
  // return: errno
  butil::Status CreateIndexId(int64_t schema_id, int64_t &new_index_id,
                              pb::coordinator_internal::MetaIncrement &meta_increment);

  // validate index definition
  // in: table_definition
  // return: errno
  static butil::Status ValidateIndexDefinition(const pb::meta::TableDefinition &table_definition);
  static butil::Status ValidateScalarIndexParameter(const pb::common::ScalarIndexParameter &scalar_index_parameter);

  butil::Status ValidateMaxTableCount();
  butil::Status ValidateMaxIndexCount();
  butil::Status ValidateMaxRegionCount();

  // create index
  // in: schema_id
  // in: table_definition
  // out: new index_id
  // out: new region_ids
  // return: errno
  butil::Status CreateIndex(int64_t schema_id, const pb::meta::TableDefinition &table_definition, int64_t table_id,
                            int64_t &new_index_id, std::vector<int64_t> &region_ids,
                            pb::coordinator_internal::MetaIncrement &meta_increment);

  // update index
  // in: schema_id
  // in: index_id
  // in: new_table_definition
  // return: errno
  butil::Status UpdateIndex(int64_t schema_id, int64_t index_id, const pb::meta::TableDefinition &new_table_definition,
                            pb::coordinator_internal::MetaIncrement &meta_increment);

  // generate table ids
  butil::Status GenerateTableIds(int64_t schema_id, const pb::meta::TableWithPartCount &count,
                                 pb::coordinator_internal::MetaIncrement &meta_increment,
                                 pb::meta::GenerateTableIdsResponse *response);

  // create table indexes map
  static void CreateTableIndexesMap(pb::coordinator_internal::TableIndexInternal &table_index_internal,
                                    pb::coordinator_internal::MetaIncrement &meta_increment);

  // get table indexes
  butil::Status GetTableIndexes(int64_t schema_id, int64_t table_id, pb::meta::GetTablesResponse *response);

  // drop table indexes
  butil::Status DropTableIndexes(int64_t schema_id, int64_t table_id,
                                 pb::coordinator_internal::MetaIncrement &meta_increment);

  // remove table index
  butil::Status RemoveTableIndex(int64_t table_id, int64_t index_id,
                                 pb::coordinator_internal::MetaIncrement &meta_increment);
  // create store
  // in: cluster_id
  // out: store_id, keyring
  // return: 0 or -1
  butil::Status CreateStore(int64_t cluster_id, int64_t &store_id, std::string &keyring,
                            pb::coordinator_internal::MetaIncrement &meta_increment);

  // delete store
  // in: cluster_id, store_id, keyring
  // return: errno
  butil::Status DeleteStore(int64_t cluster_id, int64_t store_id, std::string keyring,
                            pb::coordinator_internal::MetaIncrement &meta_increment);

  // update store
  // in: cluster_id, store_id, keyring
  // return: errno
  butil::Status UpdateStore(int64_t cluster_id, int64_t store_id, std::string keyring,
                            pb::common::StoreInState in_state, pb::coordinator_internal::MetaIncrement &meta_increment);

  // create executor
  // in: cluster_id
  // in: executor
  // out: executor
  // return: errno
  butil::Status CreateExecutor(int64_t cluster_id, pb::common::Executor &executor,
                               pb::coordinator_internal::MetaIncrement &meta_increment);

  // delete executor
  // in: cluster_id, executor
  // return: 0 or -1
  butil::Status DeleteExecutor(int64_t cluster_id, const pb::common::Executor &executor,
                               pb::coordinator_internal::MetaIncrement &meta_increment);

  // create executor_user
  // in: cluster_id
  // out: executor_user
  // return: errno
  butil::Status CreateExecutorUser(int64_t cluster_id, pb::common::ExecutorUser &executor_user,
                                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // update executor_user
  // in: cluster_id
  // out: executor_user
  // return: errno
  butil::Status UpdateExecutorUser(int64_t cluster_id, const pb::common::ExecutorUser &executor_user,
                                   const pb::common::ExecutorUser &executor_user_update,
                                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // delete executor_user
  // in: cluster_id
  // out: executor_user
  // return: errno
  butil::Status DeleteExecutorUser(int64_t cluster_id, pb::common::ExecutorUser &executor_user,
                                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // get executor_user_map
  // in: cluster_id
  // out: executor_user_map
  // return: errno
  butil::Status GetExecutorUserMap(int64_t cluster_id, pb::common::ExecutorUserMap &executor_user_map);

  // update executor map with new Executor info
  // return new epoch
  int64_t UpdateExecutorMap(const pb::common::Executor &executor,
                            pb::coordinator_internal::MetaIncrement &meta_increment);

  // try to set executor offline
  // return bool
  bool TrySetExecutorToOffline(std::string executor_id);

  // update store map with new Store info
  // return new epoch
  int64_t UpdateStoreMap(const pb::common::Store &store, pb::coordinator_internal::MetaIncrement &meta_increment);

  // try to set store offline
  // return bool
  bool TrySetStoreToOffline(int64_t store_id);

  // get storemap
  void GetStoreMap(pb::common::StoreMap &store_map);

  // get store metrics
  void GetStoreMetrics(int64_t store_id, std::vector<pb::common::StoreMetrics> &store_metrics);
  void GetStoreMetrics(int64_t store_id, int64_t region_id, std::vector<pb::common::StoreMetrics> &store_metrics);

  // delete store metrics
  void DeleteStoreMetrics(int64_t store_id);

  // region metrics
  void GetRegionMetrics(int64_t region_id, std::vector<pb::common::RegionMetrics> &region_metrics_array);
  void DeleteRegionMetrics(int64_t region_id);

  // get orphan region
  butil::Status GetOrphanRegion(int64_t store_id, std::map<int64_t, pb::common::RegionMetrics> &orphan_regions);

  // get store operation
  int GetStoreOperation(int64_t store_id, pb::coordinator::StoreOperation &store_operation);
  int GetStoreOperationForSend(int64_t store_id, pb::coordinator::StoreOperation &store_operation);

  // CleanStoreOperation
  // in:  store_id
  // return: 0 or -1
  butil::Status CleanStoreOperation(int64_t store_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  butil::Status AddStoreOperation(const pb::coordinator::StoreOperation &store_operation, bool check_conflict,
                                  pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status AddRegionCmd(int64_t store_id, const pb::coordinator::RegionCmd &region_cmd,
                             pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status UpdateRegionCmd(int64_t store_id, const pb::coordinator::RegionCmd &region_cmd,
                                const pb::error::Error &error, pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status RemoveRegionCmd(int64_t store_id, int64_t region_cmd_id,
                                pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status GetRegionCmd(int64_t store_id, int64_t start_region_cmd_id, int64_t end_region_cmd_id,
                             std::vector<pb::coordinator::RegionCmd> &region_cmds,
                             std::vector<pb::error::Error> &region_cmd_errors);
  butil::Status AddCoordinatorOperation(const pb::coordinator::CoordinatorOperation &coordinator_operation,
                                        pb::coordinator_internal::MetaIncrement &meta_increment);

  // UpdateRegionMapAndStoreOperation
  void UpdateRegionMapAndStoreOperation(const pb::common::StoreMetrics &store_metrics,
                                        pb::coordinator_internal::MetaIncrement &meta_increment);

  // get executormap
  void GetExecutorMap(pb::common::ExecutorMap &executor_map);

  // update region map with new Region info
  // return new epoch
  // int64_t UpdateRegionMap(std::vector<pb::common::Region> &regions,
  //                          pb::coordinator_internal::MetaIncrement &meta_increment);

  // try to set region to down
  // return bool
  bool TrySetRegionToDown(int64_t region_id);

  // try to set region to online
  // return bool
  bool TrySetRegionToOnline(int64_t region_id);

  // get regionmap
  void GenRegionFull(const pb::coordinator_internal::RegionInternal &region_internal, pb::common::Region &region);
  void GenRegionSlim(const pb::coordinator_internal::RegionInternal &region_internal, pb::common::Region &region);
  int64_t GetRegionLeaderId(int64_t region_id);
  pb::common::RegionStatus GetRegionStatus(int64_t region_id);
  void GetRegionLeaderAndStatus(int64_t region_id, pb::common::RegionStatus &region_status, int64_t &leader_store_id);
  static pb::common::RegionState GenRegionState(const pb::common::RegionMetrics &region_metrics,
                                                const pb::coordinator_internal::RegionInternal &region_internal);
  static pb::common::RegionStatus GenRegionStatus(const pb::common::RegionMetrics &region_metrics);

  void GetRegionMap(pb::common::RegionMap &region_map);
  void GetRegionMapFull(pb::common::RegionMap &region_map);
  void GetDeletedRegionMap(pb::common::RegionMap &region_map);
  butil::Status AddDeletedRegionMap(int64_t region_id, bool force);
  butil::Status CleanDeletedRegionMap(int64_t region_id);
  void GetRegionCount(int64_t &region_count);
  void GetRegionIdsInMap(std::vector<int64_t> &region_ids);
  void RecycleDeletedTableAndIndex();
  void RecycleOrphanRegionOnStore();
  void RecycleOrphanRegionOnCoordinator();
  void DeleteRegionBvar(int64_t region_id);

  void UpdateRegionState();
  void UpdateClusterReadOnly();

  // get schemas
  butil::Status GetSchemas(int64_t schema_id, std::vector<pb::meta::Schema> &schemas);

  // get schema
  butil::Status GetSchema(int64_t schema_id, pb::meta::Schema &schema);

  // get schema by name
  butil::Status GetSchemaByName(const std::string &schema_name, pb::meta::Schema &schema);

  // get tables
  butil::Status GetTables(int64_t schema_id, std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids);
  butil::Status GetTablesCount(int64_t schema_id, int64_t &tables_count);

  // get table
  // in: schema_id
  // in: table_id
  // out: TableDefinitionWithId
  butil::Status GetTable(int64_t schema_id, int64_t table_id, pb::meta::TableDefinitionWithId &table_definition);

  // get table by name
  // in: schema_id
  // in: table_name
  // out: TableDefinitionWithId
  butil::Status GetTableByName(int64_t schema_id, const std::string &table_name,
                               pb::meta::TableDefinitionWithId &table_definition);

  // get parts
  // in: schema_id
  // in: table_id
  // out: repeated parts
  butil::Status GetTableRange(int64_t schema_id, int64_t table_id, pb::meta::TableRange &table_range);

  // get table metrics
  // in: schema_id
  // in: table_id
  // out: TableMetricsWithId
  butil::Status GetTableMetrics(int64_t schema_id, int64_t table_id, pb::meta::TableMetricsWithId &table_metrics);

  // get indexes
  butil::Status GetIndexes(int64_t schema_id, std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids);
  butil::Status GetIndexesCount(int64_t schema_id, int64_t &indexes_count);

  // get index
  // in: schema_id
  // in: index_id
  // in: check_compatibility
  // out: TableDefinitionWithId
  butil::Status GetIndex(int64_t schema_id, int64_t index_id, bool check_compatibility,
                         pb::meta::TableDefinitionWithId &table_definition);

  // get index by name
  // in: schema_id
  // in: index_name
  // out: TableDefinitionWithId
  butil::Status GetIndexByName(int64_t schema_id, const std::string &index_name,
                               pb::meta::TableDefinitionWithId &table_definition);

  // get parts
  // in: schema_id
  // in: index_id
  // out: repeated parts
  butil::Status GetIndexRange(int64_t schema_id, int64_t index_id, pb::meta::IndexRange &index_range);

  // get index metrics
  // in: schema_id
  // in: index_id
  // out: IndexMetricsWithId
  butil::Status GetIndexMetrics(int64_t schema_id, int64_t index_id, pb::meta::IndexMetricsWithId &index_metrics);

  // update store metrics with new metrics
  // return new epoch
  int64_t UpdateStoreMetrics(const pb::common::StoreMetrics &store_metrics,
                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop table
  // in: schema_id
  // in: table_id
  // out: meta_increment
  // return: errno
  butil::Status DropTable(int64_t schema_id, int64_t table_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop index
  // in: schema_id
  // in: index_id
  // in: check_compatibility
  // out: meta_increment
  // return: errno
  butil::Status DropIndex(int64_t schema_id, int64_t index_id, bool check_compatibility,
                          pb::coordinator_internal::MetaIncrement &meta_increment);

  // SwitchAutoSplit
  // in: schema_id
  // in: table_id
  // in: auto_split
  // out: meta_increment
  butil::Status SwitchAutoSplit(int64_t schema_id, int64_t table_id, bool auto_split,
                                pb::coordinator_internal::MetaIncrement &meta_increment);

  // get coordinator_map
  void GetCoordinatorMap(int64_t cluster_id, int64_t &epoch, pb::common::Location &leader_location,
                         std::vector<pb::common::Location> &locations);

  // get next id/epoch
  int64_t GetNextId(const pb::coordinator_internal::IdEpochType &key,
                    pb::coordinator_internal::MetaIncrement &meta_increment);

  // get next ids/epochs
  std::vector<int64_t> GetNextIds(const pb::coordinator_internal::IdEpochType &key, int64_t count,
                                  pb::coordinator_internal::MetaIncrement &meta_increment);

  // get present id/epoch
  int64_t GetPresentId(const pb::coordinator_internal::IdEpochType &key);

  // update present id/epoch
  int64_t UpdatePresentId(const pb::coordinator_internal::IdEpochType &key, int64_t new_id,
                          pb::coordinator_internal::MetaIncrement &meta_increment);

  // init ids
  void InitIds();

  // validate schema if exists
  // in: schema_id
  // return: true/false
  bool ValidateSchema(int64_t schema_id);

  // validate store keyring
  // return: 0 or -1
  int ValidateStore(int64_t store_id, const std::string &keyring);

  // validate executor_user
  // return: bool
  bool ValidateExecutorUser(const pb::common::ExecutorUser &executor_user);

  // calculate table metrics
  void CalculateTableMetrics();

  // calculate single table metrics
  int64_t CalculateTableMetricsSingle(int64_t table_id, pb::meta::TableMetrics &table_metrics);

  // calculate index metrics
  void CalculateIndexMetrics();

  // calculate single index metrics
  int64_t CalculateIndexMetricsSingle(int64_t index_id, pb::meta::IndexMetrics &index_metrics);

  // functions below are for raft fsm
  bool IsLeader() override;                                            // for raft fsm
  void SetLeaderTerm(int64_t term) override;                           // for raft fsm
  void OnLeaderStart(int64_t term) override;                           // for raft fsm
  void OnLeaderStop() override;                                        // for raft fsm
  int GetAppliedTermAndIndex(int64_t &term, int64_t &index) override;  // for raft fsm

  void BuildTempMaps();

  // set raft_node to coordinator_control
  void SetRaftNode(std::shared_ptr<RaftNode> raft_node) override;  // for raft fsm
  std::shared_ptr<RaftNode> GetRaftNode() override;                // for raft fsm

  // on_apply callback
  void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool is_leader, int64_t term,
                          int64_t index, google::protobuf::Message *response) override;  // for raft fsm

  // prepare snapshot for raft snapshot
  // return: Snapshot
  std::shared_ptr<Snapshot> PrepareRaftSnapshot() override;  // for raft fsm

  // LoadMetaToSnapshotFile
  bool LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                              pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;  // for raft fsm

  // LoadMetaFromSnapshotFile
  bool LoadMetaFromSnapshotFile(
      pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;  // for raft fsm

  void GetTaskList(butil::FlatMap<int64_t, pb::coordinator::TaskList> &task_lists);

  pb::coordinator::TaskList *CreateTaskList(pb::coordinator_internal::MetaIncrement &meta_increment);
  static void AddCreateTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id,
                            const pb::common::RegionDefinition &region_definition);
  static void AddDeleteTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id);
  static void AddDeleteTaskWithCheck(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id,
                                     const ::google::protobuf::RepeatedPtrField<::dingodb::pb::common::Peer> &peers);
  // void AddPurgeTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id,
  //                   pb::coordinator_internal::MetaIncrement &meta_increment);
  static void AddChangePeerTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id,
                                const pb::common::RegionDefinition &region_definition);
  static void AddTransferLeaderTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id,
                                    const pb::common::Peer &new_leader_peer);
  static void AddMergeTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t merge_from_region_id,
                           int64_t merge_to_region_id);
  static void AddSplitTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id,
                           int64_t split_to_region_id, const std::string &water_shed_key, bool store_create_region);
  static void AddCheckSplitResultTask(pb::coordinator::TaskList *task_list, int64_t split_to_region_id);
  static void AddCheckVectorIndexTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id);
  static void AddLoadVectorIndexTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id);
  static void AddCheckStoreRegionTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id);
  static void AddCheckMergeResultTask(pb::coordinator::TaskList *task_list, int64_t merge_to_region_id,
                                      const pb::common::Range &range);
  static void AddCheckTombstoneRegionTask(pb::coordinator::TaskList *task_list, int64_t store_id, int64_t region_id);

  static void GenDeleteRegionStoreOperation(pb::coordinator::StoreOperation &store_operation, int64_t store_id,
                                            int64_t region_id);

  // check if task in task_lis can advance
  // if task advance, this function will contruct meta_increment and apply to state_machine
  butil::Status ProcessTaskList();

  // process single task
  butil::Status ProcessSingleTaskList(const pb::coordinator::TaskList &task_list,
                                      pb::coordinator_internal::MetaIncrement &meta_increment);
  void ReleaseProcessTaskListStatus(const butil::Status &);

  bool DoTaskPreCheck(const pb::coordinator::TaskPreCheck &task_pre_check);

  butil::Status CleanTaskList(int64_t task_list_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // deleted table and index
  butil::Status GetDeletedTable(int64_t deleted_table_id,
                                std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids);
  butil::Status GetDeletedIndex(int64_t deleted_index_id,
                                std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids);
  butil::Status CleanDeletedTable(int64_t table_id);
  butil::Status CleanDeletedIndex(int64_t index_id);

  // scan regions
  butil::Status ScanRegions(const std::string &start_key, const std::string &end_key, int64_t limit,
                            std::vector<pb::coordinator_internal::RegionInternal> &regions);
  butil::Status GetRangeRegionMap(std::vector<std::string> &start_keys,
                                  std::vector<pb::coordinator_internal::RegionInternal> &regions);
  static butil::Status CalcTableInternalRange(const pb::meta::PartitionRule &partition_rule,
                                              pb::common::Range &table_internal_range);

  // GC
  butil::Status UpdateGCSafePoint(int64_t safe_point, int64_t &new_safe_point,
                                  pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status GetGCSafePoint(int64_t &safe_point);

 private:
  butil::Status ValidateTaskListConflict(int64_t region_id, int64_t second_region_id);

  void GenerateTableIdAndPartIds(int64_t schema_id, int64_t part_count, pb::meta::EntityType entity_type,
                                 pb::coordinator_internal::MetaIncrement &meta_increment,
                                 pb::meta::TableIdWithPartIds *ids);

  // ids_epochs_temp (out of state machine, only for leader use)
  DingoSafeIdEpochMap id_epoch_map_safe_temp_;

  // 0.ids_epochs
  // TableInternal is combination of Table & TableDefinition
  DingoSafeIdEpochMap id_epoch_map_;
  MetaMemMapFlat<pb::coordinator_internal::IdEpochInternal> *id_epoch_meta_;

  // 1.coordinators
  DingoSafeMap<int64_t, pb::coordinator_internal::CoordinatorInternal> coordinator_map_;
  MetaMemMapFlat<pb::coordinator_internal::CoordinatorInternal> *coordinator_meta_;

  // 2.stores
  DingoSafeMap<int64_t, pb::common::Store> store_map_;
  MetaMemMapFlat<pb::common::Store> *store_meta_;  // need contruct

  // 3.executors
  DingoSafeStdMap<std::string, pb::common::Executor> executor_map_;
  MetaMemMapStd<pb::common::Executor> *executor_meta_;  // need construct

  // 4.schemas
  DingoSafeMap<int64_t, pb::coordinator_internal::SchemaInternal> schema_map_;
  MetaMemMapFlat<pb::coordinator_internal::SchemaInternal> *schema_meta_;

  // schema map temp, only for leader use, is out of state machine
  // schema_name -> schema-id
  DingoSafeMap<std::string, int64_t> schema_name_map_safe_temp_;

  // 5.regions
  DingoSafeMap<int64_t, pb::coordinator_internal::RegionInternal> region_map_;
  MetaMemMapFlat<pb::coordinator_internal::RegionInternal> *region_meta_;
  // 5.1 deleted_regions
  MetaDiskMap<pb::coordinator_internal::RegionInternal> *deleted_region_meta_;
  // 5.2 region_metrics, this map does not need to be persisted
  DingoSafeMap<int64_t, pb::common::RegionMetrics> region_metrics_map_;
  MetaMemMapFlat<pb::common::RegionMetrics> *region_metrics_meta_;
  // 5.3 range->region map
  DingoSafeStdMap<std::string, pb::coordinator_internal::RegionInternal> range_region_map_;

  // 6.tables
  // TableInternal is combination of Table & TableDefinition
  DingoSafeMap<int64_t, pb::coordinator_internal::TableInternal> table_map_;
  MetaMemMapFlat<pb::coordinator_internal::TableInternal> *table_meta_;
  MetaDiskMap<pb::coordinator_internal::TableInternal> *deleted_table_meta_;

  // table map temp, only for leader use, is out of state machine
  // table_name -> table-id
  DingoSafeMap<std::string, int64_t> table_name_map_safe_temp_;

  // 7.store_metrics
  butil::FlatMap<int64_t, pb::common::StoreMetrics> store_metrics_map_;
  bthread_mutex_t store_metrics_map_mutex_;

  // 8.table_metrics
  DingoSafeMap<int64_t, pb::coordinator_internal::TableMetricsInternal> table_metrics_map_;

  // 9.store_operation
  DingoSafeMap<int64_t, pb::coordinator_internal::StoreOperationInternal> store_operation_map_;
  MetaMemMapFlat<pb::coordinator_internal::StoreOperationInternal> *store_operation_meta_;
  DingoSafeMap<int64_t, pb::coordinator_internal::RegionCmdInternal> region_cmd_map_;
  MetaMemMapFlat<pb::coordinator_internal::RegionCmdInternal> *region_cmd_meta_;
  bthread_mutex_t store_operation_map_mutex_;  // may need a write lock

  // 10.executor_user
  DingoSafeStdMap<std::string, pb::coordinator_internal::ExecutorUserInternal>
      executor_user_map_;                                                              // executor_user -> keyring
  MetaMemMapStd<pb::coordinator_internal::ExecutorUserInternal> *executor_user_meta_;  // need construct

  // 11.task_list
  DingoSafeMap<int64_t, pb::coordinator::TaskList> task_list_map_;  // task_list_id -> task_list
  MetaMemMapFlat<pb::coordinator::TaskList> *task_list_meta_;       // need construct

  // 12.indexes
  DingoSafeMap<int64_t, pb::coordinator_internal::TableInternal> index_map_;
  MetaMemMapFlat<pb::coordinator_internal::TableInternal> *index_meta_;
  MetaDiskMap<pb::coordinator_internal::TableInternal> *deleted_index_meta_;

  // index map temp, only for leader use, is out of state machine
  // index_name -> index-id
  DingoSafeMap<std::string, int64_t> index_name_map_safe_temp_;

  // 13.index_metrics
  DingoSafeMap<int64_t, pb::coordinator_internal::IndexMetricsInternal> index_metrics_map_;

  // 50. table index
  DingoSafeMap<int64_t, pb::coordinator_internal::TableIndexInternal> table_index_map_;
  MetaMemMapFlat<pb::coordinator_internal::TableIndexInternal> *table_index_meta_;

  // root schema write to raft
  bool root_schema_writed_to_raft_;

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  // node is leader or not
  butil::atomic<int64_t> leader_term_;

  // raft node
  std::shared_ptr<RaftNode> raft_node_;

  // coordinator raft_location to server_location cache
  std::map<std::string, pb::common::Location> coordinator_location_cache_;

  // raw_engine for state_machine storage
  std::shared_ptr<RawEngine> raw_engine_of_meta_;

  // raft kv engine
  std::shared_ptr<Engine> engine_;
  butil::atomic<bool> is_processing_task_list_;

  // bvar
  MetaBvarCoordinator coordinator_bvar_;
  CoordinatorBvarMetricsStore coordinator_bvar_metrics_store_;
  CoordinatorBvarMetricsRegion coordinator_bvar_metrics_region_;
  CoordinatorBvarMetricsTable coordinator_bvar_metrics_table_;
  CoordinatorBvarMetricsIndex coordinator_bvar_metrics_index_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_CONTROL_H_

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
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "metrics/coordinator_bvar_metrics.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

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
  bvar::Status<uint64_t> is_leader_;
};

class MetaBvarStore {
 public:
  MetaBvarStore(uint64_t store_id) {
    total_capacity_.expose_as("dingo_metrics_store", "total_capacity_" + std::to_string(store_id));
    free_capacity_.expose_as("dingo_metrics_store", "free_capacity_" + std::to_string(store_id));
  }
  ~MetaBvarStore() = default;

  void SetTotalCapacity(int64_t value) { total_capacity_.set_value(value); }
  void SetFreeCapacity(int64_t value) { free_capacity_.set_value(value); }

 private:
  bvar::Status<uint64_t> total_capacity_;
  bvar::Status<uint64_t> free_capacity_;
};

class MetaBvarRegion {
 public:
  MetaBvarRegion(uint64_t region_id) {
    row_count_.expose_as("dingo_metrics_region", "row_count_" + std::to_string(region_id));
    region_size_.expose_as("dingo_metrics_region", "region_size_" + std::to_string(region_id));
  }
  ~MetaBvarRegion() = default;

  void SetRowCount(int64_t value) { row_count_.set_value(value); }
  void SetRegionSize(int64_t value) { region_size_.set_value(value); }

 private:
  bvar::Status<uint64_t> row_count_;
  bvar::Status<uint64_t> region_size_;
};

class MetaBvarTable {
 public:
  MetaBvarTable(uint64_t table_id) {
    row_count_.expose_as("dingo_metrics_table", "row_count_" + std::to_string(table_id));
    part_count_.expose_as("dingo_metrics_table", "part_count_" + std::to_string(table_id));
  }
  ~MetaBvarTable() = default;

  void SetRowCount(int64_t value) { row_count_.set_value(value); }
  void SetPartCount(int64_t value) { part_count_.set_value(value); }

 private:
  bvar::Status<uint64_t> row_count_;
  bvar::Status<uint64_t> part_count_;
};

class CoordinatorControl : public MetaControl {
 public:
  CoordinatorControl(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer,
                     std::shared_ptr<RawEngine> raw_engine_of_meta);
  ~CoordinatorControl() override;
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
  butil::Status SubmitMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status SubmitMetaIncrement(google::protobuf::Closure *done,
                                    pb::coordinator_internal::MetaIncrement &meta_increment);

  // GetMemoryInfo
  void GetMemoryInfo(pb::coordinator::CoordinatorMemoryInfo &memory_info);

  // Get raft leader's server location for sdk use
  void GetLeaderLocation(pb::common::Location &leader_server_location) override;

  // use raft_location to get server_location
  // in: raft_location
  // out: server_location
  void GetServerLocation(pb::common::Location &raft_location, pb::common::Location &server_location);
  void GetRaftLocation(pb::common::Location &server_location, pb::common::Location &raft_location);

  // query region info
  butil::Status QueryRegion(uint64_t region_id, pb::common::Region &region);

  // create region
  // in: resource_tag
  // out: new region id
  // return: errno
  butil::Status SelectStore(pb::common::StoreType store_type, int32_t replica_num, const std::string &resource_tag,
                            std::vector<uint64_t> &store_ids,
                            std::vector<pb::common::Store> &selected_stores_for_regions);
  butil::Status CreateRegion(const std::string &region_name, pb::common::RegionType region_type,
                             const std::string &resource_tag, int32_t replica_num, pb::common::Range region_range,
                             uint64_t schema_id, uint64_t table_id, uint64_t index_id,
                             const pb::common::IndexParameter &index_parameter, std::vector<uint64_t> &store_ids,
                             uint64_t split_from_region_id, uint64_t &new_region_id,
                             pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status CreateRegion(const std::string &region_name, pb::common::RegionType region_type,
                             const std::string &resource_tag, int32_t replica_num, pb::common::Range region_range,
                             uint64_t schema_id, uint64_t table_id, uint64_t index_id,
                             const pb::common::IndexParameter &index_parameter, uint64_t &new_region_id,
                             pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status CreateRegionForSplit(const std::string &region_name, pb::common::RegionType region_type,
                                     const std::string &resource_tag, pb::common::Range region_range,
                                     uint64_t schema_id, uint64_t table_id, uint64_t index_id,
                                     const pb::common::IndexParameter &index_parameter, uint64_t split_from_region_id,
                                     uint64_t &new_region_id, pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status CreateRegionForSplitInternal(uint64_t split_from_region_id, uint64_t &new_region_id,
                                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop region
  // in:  region_id
  // in:  need_update_table_range
  // return: errno
  butil::Status DropRegion(uint64_t region_id, pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status DropRegion(uint64_t region_id, bool need_update_table_range,
                           pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop region permanently
  // in:  region_id
  // return: errno
  butil::Status DropRegionPermanently(uint64_t region_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // split region
  butil::Status SplitRegion(uint64_t split_from_region_id, uint64_t split_to_region_id, std::string split_watershed_key,
                            pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status SplitRegionWithTaskList(uint64_t split_from_region_id, uint64_t split_to_region_id,
                                        std::string split_watershed_key,
                                        pb::coordinator_internal::MetaIncrement &meta_increment);

  // merge region
  butil::Status MergeRegionWithTaskList(uint64_t merge_from_region_id, uint64_t merge_to_region_id,
                                        pb::coordinator_internal::MetaIncrement &meta_increment);

  // change peer region
  butil::Status ChangePeerRegionWithTaskList(uint64_t region_id, std::vector<uint64_t> &new_store_ids,
                                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // transfer leader region
  butil::Status TransferLeaderRegionWithTaskList(uint64_t region_id, uint64_t new_leader_store_id,
                                                 pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: parent_schema_id
  // in: schema_name
  // out: new schema_id
  // return: errno
  butil::Status CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t &new_schema_id,
                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop schema
  // in: parent_schema_id
  // in: schema_id
  // return: 0 or -1
  butil::Status DropSchema(uint64_t parent_schema_id, uint64_t schema_id,
                           pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: schema_id
  // out: new table_id
  // return: errno
  butil::Status CreateTableId(uint64_t schema_id, uint64_t &new_table_id,
                              pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: schema_id
  // in: table_definition
  // out: new table_id
  // return: errno
  butil::Status CreateTable(uint64_t schema_id, const pb::meta::TableDefinition &table_definition,
                            uint64_t &new_table_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: schema_id
  // out: new index_id
  // return: errno
  butil::Status CreateIndexId(uint64_t schema_id, uint64_t &new_index_id,
                              pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: schema_id
  // in: index_definition
  // out: new index_id
  // return: errno
  butil::Status CreateIndex(uint64_t schema_id, const pb::meta::IndexDefinition &index_definition,
                            uint64_t &new_index_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // create store
  // in: cluster_id
  // out: store_id, keyring
  // return: 0 or -1
  butil::Status CreateStore(uint64_t cluster_id, uint64_t &store_id, std::string &keyring,
                            pb::coordinator_internal::MetaIncrement &meta_increment);

  // delete store
  // in: cluster_id, store_id, keyring
  // return: errno
  butil::Status DeleteStore(uint64_t cluster_id, uint64_t store_id, std::string keyring,
                            pb::coordinator_internal::MetaIncrement &meta_increment);

  // update store
  // in: cluster_id, store_id, keyring
  // return: errno
  butil::Status UpdateStore(uint64_t cluster_id, uint64_t store_id, std::string keyring,
                            pb::common::StoreInState in_state, pb::coordinator_internal::MetaIncrement &meta_increment);

  // create executor
  // in: cluster_id
  // in: executor
  // out: executor
  // return: errno
  butil::Status CreateExecutor(uint64_t cluster_id, pb::common::Executor &executor,
                               pb::coordinator_internal::MetaIncrement &meta_increment);

  // delete executor
  // in: cluster_id, executor
  // return: 0 or -1
  butil::Status DeleteExecutor(uint64_t cluster_id, const pb::common::Executor &executor,
                               pb::coordinator_internal::MetaIncrement &meta_increment);

  // create executor_user
  // in: cluster_id
  // out: executor_user
  // return: errno
  butil::Status CreateExecutorUser(uint64_t cluster_id, pb::common::ExecutorUser &executor_user,
                                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // update executor_user
  // in: cluster_id
  // out: executor_user
  // return: errno
  butil::Status UpdateExecutorUser(uint64_t cluster_id, const pb::common::ExecutorUser &executor_user,
                                   const pb::common::ExecutorUser &executor_user_update,
                                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // delete executor_user
  // in: cluster_id
  // out: executor_user
  // return: errno
  butil::Status DeleteExecutorUser(uint64_t cluster_id, pb::common::ExecutorUser &executor_user,
                                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // get executor_user_map
  // in: cluster_id
  // out: executor_user_map
  // return: errno
  butil::Status GetExecutorUserMap(uint64_t cluster_id, pb::common::ExecutorUserMap &executor_user_map);

  // update executor map with new Executor info
  // return new epoch
  uint64_t UpdateExecutorMap(const pb::common::Executor &executor,
                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // try to set executor offline
  // return bool
  bool TrySetExecutorToOffline(std::string executor_id);

  // update store map with new Store info
  // return new epoch
  uint64_t UpdateStoreMap(const pb::common::Store &store, pb::coordinator_internal::MetaIncrement &meta_increment);

  // try to set store offline
  // return bool
  bool TrySetStoreToOffline(uint64_t store_id);

  // get storemap
  void GetStoreMap(pb::common::StoreMap &store_map);

  // get store metrics
  void GetStoreMetrics(uint64_t store_id, std::vector<pb::common::StoreMetrics> &store_metrics);

  // delete store metrics
  void DeleteStoreMetrics(uint64_t store_id);

  // get orphan region
  butil::Status GetOrphanRegion(uint64_t store_id, std::map<uint64_t, pb::common::RegionMetrics> &orphan_regions);

  // get store operation
  int GetStoreOperation(uint64_t store_id, pb::coordinator::StoreOperation &store_operation);
  int GetStoreOperations(butil::FlatMap<uint64_t, pb::coordinator::StoreOperation> &store_operations);

  // CleanStoreOperation
  // in:  store_id
  // return: 0 or -1
  butil::Status CleanStoreOperation(uint64_t store_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  butil::Status AddStoreOperation(const pb::coordinator::StoreOperation &store_operation,
                                  pb::coordinator_internal::MetaIncrement &meta_increment);
  butil::Status RemoveStoreOperation(uint64_t store_id, uint64_t region_cmd_id,
                                     pb::coordinator_internal::MetaIncrement &meta_increment);

  // UpdateRegionMapAndStoreOperation
  void UpdateRegionMapAndStoreOperation(const pb::common::StoreMetrics &store_metrics,
                                        pb::coordinator_internal::MetaIncrement &meta_increment);

  // get executormap
  void GetExecutorMap(pb::common::ExecutorMap &executor_map);

  // get push storemap
  // this function will use std::swap to empty the class member store_need_push_
  void GetPushStoreMap(butil::FlatMap<uint64_t, pb::common::Store> &store_to_push);

  // get push executormap
  // this function will use std::swap to empty the class member executor_need_push_
  void GetPushExecutorMap(butil::FlatMap<std::string, pb::common::Executor> &executor_to_push);

  // update region map with new Region info
  // return new epoch
  uint64_t UpdateRegionMap(std::vector<pb::common::Region> &regions,
                           pb::coordinator_internal::MetaIncrement &meta_increment);

  // try to set region to down
  // return bool
  bool TrySetRegionToDown(uint64_t region_id);

  // try to set region to online
  // return bool
  bool TrySetRegionToOnline(uint64_t region_id);

  // get regionmap
  void GetRegionMap(pb::common::RegionMap &region_map);
  void GetRegionMapFull(pb::common::RegionMap &region_map);
  void GetDeletedRegionMap(pb::common::RegionMap &region_map);
  butil::Status AddDeletedRegionMap(uint64_t region_id, bool force);
  butil::Status CleanDeletedRegionMap(uint64_t region_id);
  void GetRegionCount(uint64_t &region_count);
  void GetRegionIdsInMap(std::vector<uint64_t> &region_ids);
  void RecycleOrphanRegionOnStore();
  void DeleteRegionBvar(uint64_t region_id);

  // get schemas
  butil::Status GetSchemas(uint64_t schema_id, std::vector<pb::meta::Schema> &schemas);

  // get schema
  butil::Status GetSchema(uint64_t schema_id, pb::meta::Schema &schema);

  // get schema by name
  butil::Status GetSchemaByName(const std::string &schema_name, pb::meta::Schema &schema);

  // get tables
  butil::Status GetTables(uint64_t schema_id, std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids);
  butil::Status GetTablesCount(uint64_t schema_id, uint64_t &tables_count);

  // get table
  // in: schema_id
  // in: table_id
  // out: TableDefinitionWithId
  butil::Status GetTable(uint64_t schema_id, uint64_t table_id, pb::meta::TableDefinitionWithId &table_definition);

  // get table by name
  // in: schema_id
  // in: table_name
  // out: TableDefinitionWithId
  butil::Status GetTableByName(uint64_t schema_id, const std::string &table_name,
                               pb::meta::TableDefinitionWithId &table_definition);

  // get parts
  // in: schema_id
  // in: table_id
  // out: repeated parts
  butil::Status GetTableRange(uint64_t schema_id, uint64_t table_id, pb::meta::TableRange &table_range);

  // get table metrics
  // in: schema_id
  // in: table_id
  // out: TableMetricsWithId
  butil::Status GetTableMetrics(uint64_t schema_id, uint64_t table_id, pb::meta::TableMetricsWithId &table_metrics);

  // get indexs
  butil::Status GetIndexs(uint64_t schema_id, std::vector<pb::meta::IndexDefinitionWithId> &index_definition_with_ids);
  butil::Status GetIndexsCount(uint64_t schema_id, uint64_t &indexs_count);

  // get index
  // in: schema_id
  // in: index_id
  // out: IndexDefinitionWithId
  butil::Status GetIndex(uint64_t schema_id, uint64_t index_id, pb::meta::IndexDefinitionWithId &index_definition);

  // get index by name
  // in: schema_id
  // in: index_name
  // out: IndexDefinitionWithId
  butil::Status GetIndexByName(uint64_t schema_id, const std::string &index_name,
                               pb::meta::IndexDefinitionWithId &index_definition);

  // get parts
  // in: schema_id
  // in: index_id
  // out: repeated parts
  butil::Status GetIndexRange(uint64_t schema_id, uint64_t index_id, pb::meta::IndexRange &index_range);

  // get index metrics
  // in: schema_id
  // in: index_id
  // out: IndexMetricsWithId
  butil::Status GetIndexMetrics(uint64_t schema_id, uint64_t index_id, pb::meta::IndexMetricsWithId &index_metrics);

  // update store metrics with new metrics
  // return new epoch
  uint64_t UpdateStoreMetrics(const pb::common::StoreMetrics &store_metrics,
                              pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop table
  // in: schema_id
  // in: table_id
  // out: meta_increment
  // return: errno
  butil::Status DropTable(uint64_t schema_id, uint64_t table_id,
                          pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop index
  // in: schema_id
  // in: index_id
  // out: meta_increment
  // return: errno
  butil::Status DropIndex(uint64_t schema_id, uint64_t index_id,
                          pb::coordinator_internal::MetaIncrement &meta_increment);

  // get coordinator_map
  void GetCoordinatorMap(uint64_t cluster_id, uint64_t &epoch, pb::common::Location &leader_location,
                         std::vector<pb::common::Location> &locations);

  // get next id/epoch
  uint64_t GetNextId(const pb::coordinator_internal::IdEpochType &key,
                     pb::coordinator_internal::MetaIncrement &meta_increment);

  // get present id/epoch
  uint64_t GetPresentId(const pb::coordinator_internal::IdEpochType &key);

  // init ids
  void InitIds();

  // validate schema if exists
  // in: schema_id
  // return: true/false
  bool ValidateSchema(uint64_t schema_id);

  // validate store keyring
  // return: 0 or -1
  int ValidateStore(uint64_t store_id, const std::string &keyring);

  // validate executor_user
  // return: bool
  bool ValidateExecutorUser(const pb::common::ExecutorUser &executor_user);

  // calculate table metrics
  void CalculateTableMetrics();

  // calculate single table metrics
  uint64_t CalculateTableMetricsSingle(uint64_t table_id, pb::meta::TableMetrics &table_metrics);

  // calculate index metrics
  void CalculateIndexMetrics();

  // calculate single index metrics
  uint64_t CalculateIndexMetricsSingle(uint64_t index_id, pb::meta::IndexMetrics &index_metrics);

  // functions below are for raft fsm
  bool IsLeader() override;                                              // for raft fsm
  void SetLeaderTerm(int64_t term) override;                             // for raft fsm
  void OnLeaderStart(int64_t term) override;                             // for raft fsm
  void OnLeaderStop() override;                                          // for raft fsm
  int GetAppliedTermAndIndex(uint64_t &term, uint64_t &index) override;  // for raft fsm

  // set raft_node to coordinator_control
  void SetRaftNode(std::shared_ptr<RaftNode> raft_node) override;  // for raft fsm
  std::shared_ptr<RaftNode> GetRaftNode() override;                // for raft fsm

  // on_apply callback
  void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool is_leader, uint64_t term,
                          uint64_t index, google::protobuf::Message *response) override;  // for raft fsm

  // prepare snapshot for raft snapshot
  // return: Snapshot
  std::shared_ptr<Snapshot> PrepareRaftSnapshot() override;  // for raft fsm

  // LoadMetaToSnapshotFile
  bool LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                              pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;  // for raft fsm

  // LoadMetaFromSnapshotFile
  bool LoadMetaFromSnapshotFile(
      pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;  // for raft fsm

  void GetTaskList(butil::FlatMap<uint64_t, pb::coordinator::TaskList> &task_lists);

  pb::coordinator::TaskList *CreateTaskList(pb::coordinator_internal::MetaIncrement &meta_increment);
  void AddCreateTask(pb::coordinator::TaskList *task_list, uint64_t store_id, uint64_t region_id,
                     const pb::common::RegionDefinition &region_definition,
                     pb::coordinator_internal::MetaIncrement &meta_increment);
  void AddDeleteTask(pb::coordinator::TaskList *task_list, uint64_t store_id, uint64_t region_id,
                     pb::coordinator_internal::MetaIncrement &meta_increment);
  void AddDeleteTaskWithCheck(pb::coordinator::TaskList *task_list, uint64_t store_id, uint64_t region_id,
                              const ::google::protobuf::RepeatedPtrField< ::dingodb::pb::common::Peer> &peers,
                              pb::coordinator_internal::MetaIncrement &meta_increment);
  // void AddPurgeTask(pb::coordinator::TaskList *task_list, uint64_t store_id, uint64_t region_id,
  //                   pb::coordinator_internal::MetaIncrement &meta_increment);
  void AddChangePeerTask(pb::coordinator::TaskList *task_list, uint64_t store_id, uint64_t region_id,
                         const pb::common::RegionDefinition &region_definition,
                         pb::coordinator_internal::MetaIncrement &meta_increment);
  void AddTransferLeaderTask(pb::coordinator::TaskList *task_list, uint64_t store_id, uint64_t region_id,
                             const pb::common::Peer &new_leader_peer,
                             pb::coordinator_internal::MetaIncrement &meta_increment);
  void AddMergeTask(pb::coordinator::TaskList *task_list, uint64_t store_id, uint64_t region_id,
                    uint64_t merge_to_region_id, pb::coordinator_internal::MetaIncrement &meta_increment);
  void AddSplitTask(pb::coordinator::TaskList *task_list, uint64_t store_id, uint64_t region_id,
                    uint64_t split_to_region_id, const std::string &water_shed_key,
                    pb::coordinator_internal::MetaIncrement &meta_increment);

  // check if task in task_lis can advance
  // if task advance, this function will contruct meta_increment and apply to state_machine
  butil::Status ProcessTaskList();

  // process single task
  butil::Status ProcessSingleTaskList(const pb::coordinator::TaskList &task_list,
                                      pb::coordinator_internal::MetaIncrement &meta_increment);
  void ReleaseProcessTaskListStatus(const butil::Status &);

  bool DoTaskPreCheck(const pb::coordinator::TaskPreCheck &task_pre_check);

  butil::Status CleanTaskList(uint64_t task_list_id, pb::coordinator_internal::MetaIncrement &meta_increment);

 private:
  butil::Status ValidateTaskListConflict(uint64_t region_id, uint64_t second_region_id);

  // ids_epochs_temp (out of state machine, only for leader use)
  DingoSafeIdEpochMap id_epoch_map_safe_temp_;

  // 0.ids_epochs
  // TableInternal is combination of Table & TableDefinition
  DingoSafeIdEpochMap id_epoch_map_;
  MetaSafeMapStorage<pb::coordinator_internal::IdEpochInternal> *id_epoch_meta_;

  // 1.coordinators
  DingoSafeMap<uint64_t, pb::coordinator_internal::CoordinatorInternal> coordinator_map_;
  MetaSafeMapStorage<pb::coordinator_internal::CoordinatorInternal> *coordinator_meta_;

  // 2.stores
  DingoSafeMap<uint64_t, pb::common::Store> store_map_;
  MetaSafeMapStorage<pb::common::Store> *store_meta_;            // need contruct
  butil::FlatMap<uint64_t, pb::common::Store> store_need_push_;  // will send push msg to these stores in crontab
  bthread_mutex_t store_need_push_mutex_;

  // 3.executors
  DingoSafeMap<std::string, pb::common::Executor> executor_map_;
  MetaSafeStringMapStorage<pb::common::Executor> *executor_meta_;  // need construct
  butil::FlatMap<std::string, pb::common::Executor>
      executor_need_push_;  // will send push msg to these executors in crontab
  bthread_mutex_t executor_need_push_mutex_;

  // 4.schemas
  DingoSafeMap<uint64_t, pb::coordinator_internal::SchemaInternal> schema_map_;
  MetaSafeMapStorage<pb::coordinator_internal::SchemaInternal> *schema_meta_;

  // schema map temp, only for leader use, is out of state machine
  // schema_name -> schema-id
  DingoSafeMap<std::string, uint64_t> schema_name_map_safe_temp_;

  // 5.regions
  DingoSafeMap<uint64_t, pb::common::Region> region_map_;
  MetaSafeMapStorage<pb::common::Region> *region_meta_;
  // 5.1 deleted_regions
  DingoSafeMap<uint64_t, pb::common::Region> deleted_region_map_;  // tombstone for deleted region
  MetaSafeMapStorage<pb::common::Region> *deleted_region_meta_;

  // 6.tables
  // TableInternal is combination of Table & TableDefinition
  DingoSafeMap<uint64_t, pb::coordinator_internal::TableInternal> table_map_;
  MetaSafeMapStorage<pb::coordinator_internal::TableInternal> *table_meta_;

  // table map temp, only for leader use, is out of state machine
  // table_name -> table-id
  DingoSafeMap<std::string, uint64_t> table_name_map_safe_temp_;

  // 7.store_metrics
  butil::FlatMap<uint64_t, pb::common::StoreMetrics> store_metrics_map_;
  MetaMapStorage<pb::common::StoreMetrics> *store_metrics_meta_;
  bthread_mutex_t store_metrics_map_mutex_;

  // 8.table_metrics
  DingoSafeMap<uint64_t, pb::coordinator_internal::TableMetricsInternal> table_metrics_map_;
  MetaSafeMapStorage<pb::coordinator_internal::TableMetricsInternal> *table_metrics_meta_;

  // 9.store_operation
  DingoSafeMap<uint64_t, pb::coordinator::StoreOperation> store_operation_map_;
  MetaSafeMapStorage<pb::coordinator::StoreOperation> *store_operation_meta_;
  bthread_mutex_t store_operation_map_mutex_;  // may need a write lock

  // 10.executor_user
  DingoSafeMap<std::string, pb::coordinator_internal::ExecutorUserInternal>
      executor_user_map_;  // executor_user -> keyring
  MetaSafeStringMapStorage<pb::coordinator_internal::ExecutorUserInternal> *executor_user_meta_;  // need construct

  // 11.task_list
  DingoSafeMap<uint64_t, pb::coordinator::TaskList> task_list_map_;  // task_list_id -> task_list
  MetaSafeMapStorage<pb::coordinator::TaskList> *task_list_meta_;    // need construct

  // 12.indexs
  DingoSafeMap<uint64_t, pb::coordinator_internal::IndexInternal> index_map_;
  MetaSafeMapStorage<pb::coordinator_internal::IndexInternal> *index_meta_;

  // index map temp, only for leader use, is out of state machine
  // index_name -> index-id
  DingoSafeMap<std::string, uint64_t> index_name_map_safe_temp_;

  // 13.index_metrics
  DingoSafeMap<uint64_t, pb::coordinator_internal::IndexMetricsInternal> index_metrics_map_;
  MetaSafeMapStorage<pb::coordinator_internal::IndexMetricsInternal> *index_metrics_meta_;

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

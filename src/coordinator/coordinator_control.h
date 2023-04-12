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

#include "brpc/controller.h"
#include "brpc/server.h"
#include "bthread/types.h"
#include "butil/containers/flat_map.h"
#include "butil/scoped_lock.h"
#include "butil/strings/stringprintf.h"
#include "common/logging.h"
#include "common/meta_control.h"
#include "common/safe_map.h"
#include "coordinator/coordinator_meta_storage.h"
#include "engine/engine.h"
#include "engine/snapshot.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

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
  int SubmitMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment);

  // GetMemoryInfo
  void GetMemoryInfo(pb::coordinator::CoordinatorMemoryInfo &memory_info);

  // Get raft leader's server location for sdk use
  void GetLeaderLocation(pb::common::Location &leader_server_location);

  // use raft_location to get server_location
  // in: raft_location
  // out: server_location
  void GetServerLocation(pb::common::Location &raft_location, pb::common::Location &server_location);

  // create region
  // in: resource_tag
  // out: new region id
  int CreateRegion(const std::string &region_name, const std::string &resource_tag, int32_t replica_num,
                   pb::common::Range region_range, uint64_t schema_id, uint64_t table_id, uint64_t &new_region_id,
                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop region
  // in:  region_id
  // return: 0 or -1
  int DropRegion(uint64_t region_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: parent_schema_id
  // in: schema_name
  // out: new schema_id
  // return: errno
  pb::error::Errno CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t &new_schema_id,
                                pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop schema
  // in: parent_schema_id
  // in: schema_id
  // return: 0 or -1
  pb::error::Errno DropSchema(uint64_t parent_schema_id, uint64_t schema_id,
                              pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: schema_id
  // out: new table_id
  // return: errno
  pb::error::Errno CreateTableId(uint64_t schema_id, uint64_t &new_table_id,
                                 pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: schema_id
  // in: table_definition
  // out: new table_id
  // return: errno
  pb::error::Errno CreateTable(uint64_t schema_id, const pb::meta::TableDefinition &table_definition,
                               uint64_t &new_table_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // create store
  // in: cluster_id
  // out: store_id, keyring
  // return: 0 or -1
  int CreateStore(uint64_t cluster_id, uint64_t &store_id, std::string &keyring,
                  pb::coordinator_internal::MetaIncrement &meta_increment);

  // delete store
  // in: cluster_id, store_id, keyring
  // return: 0 or -1
  int DeleteStore(uint64_t cluster_id, uint64_t store_id, std::string keyring,
                  pb::coordinator_internal::MetaIncrement &meta_increment);

  // create executor
  // in: cluster_id
  // out: executor_id, keyring
  // return: 0 or -1
  int CreateExecutor(uint64_t cluster_id, uint64_t &executor_id, std::string &keyring,
                     pb::coordinator_internal::MetaIncrement &meta_increment);

  // delete executor
  // in: cluster_id, executor_id, keyring
  // return: 0 or -1
  int DeleteExecutor(uint64_t cluster_id, uint64_t executor_id, std::string keyring,
                     pb::coordinator_internal::MetaIncrement &meta_increment);

  // update executor map with new Executor info
  // return new epoch
  uint64_t UpdateExecutorMap(const pb::common::Executor &executor,
                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // update store map with new Store info
  // return new epoch
  uint64_t UpdateStoreMap(const pb::common::Store &store, pb::coordinator_internal::MetaIncrement &meta_increment);

  // get storemap
  void GetStoreMap(pb::common::StoreMap &store_map);

  // get store metrics
  void GetStoreMetrics(std::vector<pb::common::StoreMetrics> &store_metrics);

  // get store operation
  void GetStoreOperation(uint64_t store_id, pb::coordinator::StoreOperation &store_operation);

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
  void GetPushExecutorMap(butil::FlatMap<uint64_t, pb::common::Executor> &executor_to_push);

  // update region map with new Region info
  // return new epoch
  uint64_t UpdateRegionMap(std::vector<pb::common::Region> &regions,
                           pb::coordinator_internal::MetaIncrement &meta_increment);

  // get regionmap
  void GetRegionMap(pb::common::RegionMap &region_map);

  // get schemas
  void GetSchemas(uint64_t schema_id, std::vector<pb::meta::Schema> &schemas);

  // get schema
  void GetSchema(uint64_t schema_id, pb::meta::Schema &schema);

  // get schema by name
  void GetSchemaByName(const std::string &schema_name, pb::meta::Schema &schema);

  // get tables
  void GetTables(uint64_t schema_id, std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids);

  // get table
  // in: schema_id
  // in: table_id
  // out: TableDefinitionWithId
  void GetTable(uint64_t schema_id, uint64_t table_id, pb::meta::TableDefinitionWithId &table_definition);

  // get table by name
  // in: schema_id
  // in: table_name
  // out: TableDefinitionWithId
  void GetTableByName(uint64_t schema_id, const std::string &table_name,
                      pb::meta::TableDefinitionWithId &table_definition);

  // get parts
  // in: schema_id
  // in: table_id
  // out: repeated parts
  void GetTableRange(uint64_t schema_id, uint64_t table_id, pb::meta::TableRange &table_range);

  // get table metrics
  // in: schema_id
  // in: table_id
  // out: TableMetricsWithId
  void GetTableMetrics(uint64_t schema_id, uint64_t table_id, pb::meta::TableMetricsWithId &table_metrics);

  // update store metrics with new metrics
  // return new epoch
  uint64_t UpdateStoreMetrics(const pb::common::StoreMetrics &store_metrics,
                              pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop table
  // in: schema_id
  // in: table_id
  // out: meta_increment
  // return: errno
  pb::error::Errno DropTable(uint64_t schema_id, uint64_t table_id,
                             pb::coordinator_internal::MetaIncrement &meta_increment);

  // get coordinator_map
  void GetCoordinatorMap(uint64_t cluster_id, uint64_t &epoch, pb::common::Location &leader_location,
                         std::vector<pb::common::Location> &locations);

  // get next id/epoch
  uint64_t GetNextId(const pb::coordinator_internal::IdEpochType &key,
                     pb::coordinator_internal::MetaIncrement &meta_increment);

  // get present id/epoch
  uint64_t GetPresentId(const pb::coordinator_internal::IdEpochType &key);

  // validate schema if exists
  // in: schema_id
  // return: true/false
  bool ValidateSchema(uint64_t schema_id);

  // validate store keyring
  // return: 0 or -1
  int ValidateStore(uint64_t store_id, const std::string &keyring);

  // validate executor keyring
  // return: 0 or -1
  int ValidateExecutor(uint64_t executor_id, const std::string &keyring);

  // calculate table metrics
  void CalculateTableMetrics();

  // calculate single table metrics
  uint64_t CalculateTableMetricsSingle(uint64_t table_id, pb::meta::TableMetrics &table_metrics);

  // functions below are for raft fsm
  bool IsLeader() override;                                              // for raft fsm
  void SetLeaderTerm(int64_t term) override;                             // for raft fsm
  void OnLeaderStart(int64_t term) override;                             // for raft fsm
  int GetAppliedTermAndIndex(uint64_t &term, uint64_t &index) override;  // for raft fsm

  // set raft_node to coordinator_control
  void SetRaftNode(std::shared_ptr<RaftNode> raft_node) override;  // for raft fsm

  // on_apply callback
  void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool id_leader, uint64_t term,
                          uint64_t index) override;  // for raft fsm

  // prepare snapshot for raft snapshot
  // return: Snapshot
  std::shared_ptr<Snapshot> PrepareRaftSnapshot() override;  // for raft fsm

  // LoadMetaToSnapshotFile
  bool LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                              pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;  // for raft fsm

  // LoadMetaFromSnapshotFile
  bool LoadMetaFromSnapshotFile(
      pb::coordinator_internal::MetaSnapshotFile &meta_snapshot_file) override;  // for raft fsm

 private:
  // ids_epochs_temp (out of state machine, only for leader use)
  DingoSafeIdEpochMap id_epoch_map_safe_temp_;

  // 0.ids_epochs
  // TableInternal is combination of Table & TableDefinition
  butil::FlatMap<uint64_t, pb::coordinator_internal::IdEpochInternal> id_epoch_map_;
  MetaMapStorage<pb::coordinator_internal::IdEpochInternal> *id_epoch_meta_;
  bthread_mutex_t id_epoch_map_mutex_;

  // 1.coordinators
  butil::FlatMap<uint64_t, pb::coordinator_internal::CoordinatorInternal> coordinator_map_;
  MetaMapStorage<pb::coordinator_internal::CoordinatorInternal> *coordinator_meta_;
  bthread_mutex_t coordinator_map_mutex_;

  // 2.stores
  butil::FlatMap<uint64_t, pb::common::Store> store_map_;
  MetaMapStorage<pb::common::Store> *store_meta_;                // need contruct
  butil::FlatMap<uint64_t, pb::common::Store> store_need_push_;  // will send push msg to these stores in crontab
  bthread_mutex_t store_map_mutex_;
  bthread_mutex_t store_need_push_mutex_;

  // 3.executors
  butil::FlatMap<uint64_t, pb::common::Executor> executor_map_;
  MetaMapStorage<pb::common::Executor> *executor_meta_;  // need construct
  butil::FlatMap<uint64_t, pb::common::Executor>
      executor_need_push_;  // will send push msg to these executors in crontab
  bthread_mutex_t executor_map_mutex_;
  bthread_mutex_t executor_need_push_mutex_;

  // 4.schemas
  butil::FlatMap<uint64_t, pb::coordinator_internal::SchemaInternal> schema_map_;
  MetaMapStorage<pb::coordinator_internal::SchemaInternal> *schema_meta_;
  bthread_mutex_t schema_map_mutex_;
  // schema map temp, only for leader use, is out of state machine
  // schema_name -> schema-id
  DingoSafeMap<std::string, uint64_t> schema_name_map_safe_temp_;

  // 5.regions
  butil::FlatMap<uint64_t, pb::common::Region> region_map_;
  MetaMapStorage<pb::common::Region> *region_meta_;
  bthread_mutex_t region_map_mutex_;

  // 6.tables
  // TableInternal is combination of Table & TableDefinition
  butil::FlatMap<uint64_t, pb::coordinator_internal::TableInternal> table_map_;
  MetaMapStorage<pb::coordinator_internal::TableInternal> *table_meta_;
  bthread_mutex_t table_map_mutex_;

  // table map temp, only for leader use, is out of state machine
  // table_name -> table-id
  DingoSafeMap<std::string, uint64_t> table_name_map_safe_temp_;

  // 7.store_metrics
  butil::FlatMap<uint64_t, pb::common::StoreMetrics> store_metrics_map_;
  MetaMapStorage<pb::common::StoreMetrics> *store_metrics_meta_;
  bthread_mutex_t store_metrics_map_mutex_;

  // 8.table_metrics
  butil::FlatMap<uint64_t, pb::coordinator_internal::TableMetricsInternal> table_metrics_map_;
  MetaMapStorage<pb::coordinator_internal::TableMetricsInternal> *table_metrics_meta_;
  bthread_mutex_t table_metrics_map_mutex_;

  // 9.store_operation
  DingoSafeMap<uint64_t, pb::coordinator::StoreOperation> store_operation_map_;
  MetaSafeMapStorage<pb::coordinator::StoreOperation> *store_operation_meta_;
  bthread_mutex_t store_operation_map_mutex_;  // may need a write lock

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
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_CONTROL_H_

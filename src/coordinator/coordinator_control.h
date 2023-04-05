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
#include "engine/snapshot.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

template <typename T>
class MetaMapStorage {
 public:
  const std::string internal_prefix;
  MetaMapStorage(butil::FlatMap<uint64_t, T> *elements_ptr)
      : internal_prefix(typeid(T).name()), elements_(elements_ptr){};
  MetaMapStorage(butil::FlatMap<uint64_t, T> *elements_ptr, const std::string &prefix)
      : internal_prefix(prefix), elements_(elements_ptr){};
  ~MetaMapStorage() = default;

  std::string Prefix() { return internal_prefix; }

  bool Init() {
    DINGO_LOG(INFO) << "coordinator server meta";
    return true;
  }

  bool Recover(const std::vector<pb::common::KeyValue> &kvs) {
    TransformFromKv(kvs);
    return true;
  }

  bool IsExist(uint64_t id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_->find(id);
    return static_cast<bool>(it != elements_->end());
  }

  uint64_t ParseId(const std::string &str) {
    if (str.size() <= internal_prefix.size()) {
      LOG(ERROR) << "Parse id failed, invalid str " << str;
      return 0;
    }

    std::string s(str.c_str() + internal_prefix.size() + 1);
    try {
      return std::stoull(s, nullptr, 10);
    } catch (std::invalid_argument &e) {
      LOG(ERROR) << "string to uint64_t failed: " << e.what();
    }

    return 0;
  }

  std::string GenKey(uint64_t id) { return butil::StringPrintf("%s_%lu", internal_prefix.c_str(), id); }

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_->find(id);
    if (it == elements_->end()) {
      return nullptr;
    }

    return TransformToKv(it->second);
  };

  std::shared_ptr<pb::common::KeyValue> TransformToKv(T element) {
    std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
    kv->set_key(GenKey(element.id()));
    kv->set_value(element.SerializeAsString());

    return kv;
  }

  pb::common::KeyValue TransformToKvValue(T element) {
    pb::common::KeyValue kv;
    kv.set_key(GenKey(element.id()));
    kv.set_value(element.SerializeAsString());

    return kv;
  }

  std::vector<pb::common::KeyValue> TransformToKvWithAll() {
    // std::shared_lock<std::shared_mutex> lock(mutex_);

    std::vector<pb::common::KeyValue> kvs;
    for (const auto &it : *elements_) {
      pb::common::KeyValue kv;
      kv.set_key(GenKey(it.first));
      kv.set_value(it.second.SerializeAsString());
      kvs.push_back(kv);
    }

    return kvs;
  }

  void TransformFromKv(const std::vector<pb::common::KeyValue> &kvs) {
    // std::unique_lock<std::shared_mutex> lock(mutex_);
    for (const auto &kv : kvs) {
      uint64_t id = ParseId(kv.key());
      T element;
      element.ParsePartialFromArray(kv.value().data(), kv.value().size());
      // elements_->insert_or_assign(id, element);
      elements_->insert(id, element);
    }
  };

  MetaMapStorage(const MetaMapStorage &) = delete;
  const MetaMapStorage &operator=(const MetaMapStorage &) = delete;

 private:
  // Coordinator all region meta data in this server.
  // std::map<uint64_t, std::shared_ptr<T>> *elements_;
  butil::FlatMap<uint64_t, T> *elements_;
};

#define COORDINATOR_ID_OF_MAP_MIN 1000

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
  // return: 0 or -1
  int CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t &new_schema_id,
                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop schema
  // in: parent_schema_id
  // in: schema_id
  // return: 0 or -1
  int DropSchema(uint64_t parent_schema_id, uint64_t schema_id,
                 pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: schema_id
  // out: new table_id
  // return: 0 or -1
  int CreateTableId(uint64_t schema_id, uint64_t &new_table_id,
                    pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: schema_id
  // in: table_definition
  // out: new table_id
  // return: 0 or -1
  int CreateTable(uint64_t schema_id, const pb::meta::TableDefinition &table_definition, uint64_t &new_table_id,
                  pb::coordinator_internal::MetaIncrement &meta_increment);

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

  // get tables
  void GetTables(uint64_t schema_id, std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids);

  // get table
  // in: schema_id
  // in: table_id
  // out: TableDefinitionWithId
  void GetTable(uint64_t schema_id, uint64_t table_id, pb::meta::TableDefinitionWithId &table_definition);

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
  // return: 0 or -1
  int DropTable(uint64_t schema_id, uint64_t table_id, pb::coordinator_internal::MetaIncrement &meta_increment);

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
  // TableInternal is combination of Table & TableDefinition
  butil::FlatMap<uint64_t, pb::coordinator_internal::IdEpochInternal> id_epoch_map_temp_;
  bthread_mutex_t id_epoch_map_temp_mutex_;

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

  // 5.regions
  butil::FlatMap<uint64_t, pb::common::Region> region_map_;
  MetaMapStorage<pb::common::Region> *region_meta_;
  bthread_mutex_t region_map_mutex_;

  // 6.tables
  // TableInternal is combination of Table & TableDefinition
  butil::FlatMap<uint64_t, pb::coordinator_internal::TableInternal> table_map_;
  MetaMapStorage<pb::coordinator_internal::TableInternal> *table_meta_;
  bthread_mutex_t table_map_mutex_;

  // 7.store_metrics
  butil::FlatMap<uint64_t, pb::common::StoreMetrics> store_metrics_map_;
  MetaMapStorage<pb::common::StoreMetrics> *store_metrics_meta_;
  bthread_mutex_t store_metrics_map_mutex_;

  // 8.table_metrics
  butil::FlatMap<uint64_t, pb::coordinator_internal::TableMetricsInternal> table_metrics_map_;
  MetaMapStorage<pb::coordinator_internal::TableMetricsInternal> *table_metrics_meta_;
  bthread_mutex_t table_metrics_map_mutex_;

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
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_CONTROL_H_

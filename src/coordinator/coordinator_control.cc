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

#include "coordinator/coordinator_control.h"

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "braft/configuration.h"
#include "bthread/mutex.h"
#include "butil/compiler_specific.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/safe_map.h"
#include "coordinator/coordinator_meta_storage.h"
#include "coordinator/coordinator_prefix.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "server/server.h"

namespace dingodb {

CoordinatorControl::CoordinatorControl(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer,
                                       std::shared_ptr<RawEngine> raw_engine_of_meta)
    : meta_reader_(meta_reader), meta_writer_(meta_writer), leader_term_(-1), raw_engine_of_meta_(raw_engine_of_meta) {
  // init bthread mutex
  bthread_mutex_init(&store_metrics_map_mutex_, nullptr);
  bthread_mutex_init(&store_region_metrics_map_mutex_, nullptr);
  bthread_mutex_init(&store_operation_map_mutex_, nullptr);
  // bthread_mutex_init(&lease_to_key_map_temp_mutex_, nullptr);
  // bthread_mutex_init(&one_time_watch_map_mutex_, nullptr);
  bthread_mutex_init(&meta_watch_bitmap_mutex_, nullptr);

  root_schema_writed_to_raft_ = false;
  is_processing_task_list_.store(false);
  leader_term_.store(-1, butil::memory_order_release);

  // the data structure below will write to raft
  coordinator_meta_ = new MetaMemMapFlat<pb::coordinator_internal::CoordinatorInternal>(
      &coordinator_map_, kPrefixCoordinator, raw_engine_of_meta);
  store_meta_ = new MetaMemMapFlat<pb::common::Store>(&store_map_, kPrefixStore, raw_engine_of_meta);
  schema_meta_ =
      new MetaMemMapFlat<pb::coordinator_internal::SchemaInternal>(&schema_map_, kPrefixSchema, raw_engine_of_meta);
  region_meta_ =
      new MetaMemMapFlat<pb::coordinator_internal::RegionInternal>(&region_map_, kPrefixRegion, raw_engine_of_meta);
  deleted_region_meta_ =
      new MetaDiskMap<pb::coordinator_internal::RegionInternal>(kPrefixDeletedRegion, raw_engine_of_meta);
  region_metrics_meta_ =
      new MetaMemMapFlat<pb::common::RegionMetrics>(&region_metrics_map_, kPrefixRegionMetrics, raw_engine_of_meta);
  table_meta_ =
      new MetaMemMapFlat<pb::coordinator_internal::TableInternal>(&table_map_, kPrefixTable, raw_engine_of_meta);
  deleted_table_meta_ =
      new MetaDiskMap<pb::coordinator_internal::TableInternal>(kPrefixDeletedTable, raw_engine_of_meta);
  id_epoch_meta_ =
      new MetaMemMapFlat<pb::coordinator_internal::IdEpochInternal>(&id_epoch_map_, kPrefixIdEpoch, raw_engine_of_meta);
  executor_meta_ = new MetaMemMapStd<pb::common::Executor>(&executor_map_, kPrefixExecutor, raw_engine_of_meta);
  store_operation_meta_ = new MetaMemMapFlat<pb::coordinator_internal::StoreOperationInternal>(
      &store_operation_map_, kPrefixStoreOperation, raw_engine_of_meta);
  region_cmd_meta_ = new MetaMemMapFlat<pb::coordinator_internal::RegionCmdInternal>(&region_cmd_map_, kPrefixRegionCmd,
                                                                                     raw_engine_of_meta);
  executor_user_meta_ = new MetaMemMapStd<pb::coordinator_internal::ExecutorUserInternal>(
      &executor_user_map_, kPrefixExecutorUser, raw_engine_of_meta);
  task_list_meta_ = new MetaMemMapFlat<pb::coordinator::TaskList>(&task_list_map_, kPrefixTaskList, raw_engine_of_meta);
  index_meta_ =
      new MetaMemMapFlat<pb::coordinator_internal::TableInternal>(&index_map_, kPrefixIndex, raw_engine_of_meta);
  deleted_index_meta_ =
      new MetaDiskMap<pb::coordinator_internal::TableInternal>(kPrefixDeletedIndex, raw_engine_of_meta);
  common_disk_meta_ = new MetaDiskMap<pb::coordinator_internal::CommonInternal>(kPrefixCommonDisk, raw_engine_of_meta);
  common_mem_meta_ = new MetaMemMapStd<pb::coordinator_internal::CommonInternal>(&common_mem_map_, kPrefixCommonMem,
                                                                                 raw_engine_of_meta);
  tenant_meta_ =
      new MetaMemMapFlat<pb::coordinator_internal::TenantInternal>(&tenant_map_, kPrefixTenant, raw_engine_of_meta);

  // table index
  table_index_meta_ = new MetaMemMapFlat<pb::coordinator_internal::TableIndexInternal>(
      &table_index_map_, kPrefixTableIndex, raw_engine_of_meta);

  // init SafeMap
  id_epoch_map_.Init(100);                // id_epoch_map_ is a small map
  id_epoch_map_safe_temp_.Init(100);      // id_epoch_map_temp_ is a small map
  schema_name_map_safe_temp_.Init(1000);  // schema_map_ is a big map
  table_name_map_safe_temp_.Init(10000);  // table_map_ is a big map
  store_operation_map_.Init(100);         // store_operation_map_ is a small map
  region_cmd_map_.Init(2000);             // region_cmd_map_ is a big map
  schema_map_.Init(10000);                // schema_map_ is a big map
  table_map_.Init(10000);                 // table_map_ is a big map
  region_map_.Init(30000);                // region_map_ is a big map
  region_metrics_map_.Init(30000);        // region_metrics_map_ is a big map
  coordinator_map_.Init(10);              // coordinator_map_ is a small map
  store_map_.Init(100);                   // store_map_ is a small map
  table_metrics_map_.Init(10000);         // table_metrics_map_ is a big map
  task_list_map_.Init(100);               // task_list_map_ is a small map
  index_name_map_safe_temp_.Init(10000);  // index_map_ is a big map
  index_map_.Init(10000);                 // index_map_ is a big map
  index_metrics_map_.Init(10000);         // index_metrics_map_ is a big map

  // table index
  table_index_map_.Init(10000);

  // tenant
  tenant_map_.Init(1000);  // tenant_map_ is a small map

  meta_watch_worker_set_ = WorkerSet::New("CoordinatorMetaWatchWorkerSet", 1, 0);
  if (!meta_watch_worker_set_->Init()) {
    DINGO_LOG(FATAL) << "Init CoordinatorMetaWatchWorkerSet failed!";
  }
}

CoordinatorControl::~CoordinatorControl() {
  delete coordinator_meta_;
  delete store_meta_;
  delete schema_meta_;
  delete region_meta_;
  delete deleted_region_meta_;
  delete region_metrics_meta_;
  delete table_meta_;
  delete id_epoch_meta_;
  delete executor_meta_;
  delete store_operation_meta_;
  delete executor_user_meta_;
  delete index_meta_;
  delete table_index_meta_;
  delete common_disk_meta_;
  delete common_mem_meta_;
  delete tenant_meta_;
  meta_watch_worker_set_->Destroy();
}

// InitIds
// Setup some initial ids for human readable
void CoordinatorControl::InitIds() {
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_COORINATOR) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_COORINATOR, 20000);
  }
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_STORE) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_STORE, 30000);
  }
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_EXECUTOR, 40000);
  }
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_SCHEMA) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_SCHEMA, 50000);
  }
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_TABLE, 60000);
  }
  // if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_INDEX) == 0) {
  //   id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_INDEX, 70000);
  // }
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION, 80000);
  }
  // if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_LEASE) == 0) {
  //   id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_LEASE, 90000);
  // }
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_REGION_CMD, 1000000);
  }
  if (id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_TENANT) == 0) {
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::ID_NEXT_TENANT, 1000);
  }
}

bool CoordinatorControl::Recover() {
  DINGO_LOG(INFO) << "Coordinator start to Recover";

  std::vector<pb::common::KeyValue> kvs;

  // 0.id_epoch map
  if (!meta_reader_->Scan(id_epoch_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);

    if (!id_epoch_meta_->Recover(kvs)) {
      return false;
    }

    // set id_epoch_map_ present id
    InitIds();

    DINGO_LOG(WARNING) << "id_epoch_map_ size=" << id_epoch_map_.Size();
    DINGO_LOG(WARNING) << "term=" << id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM);
    DINGO_LOG(WARNING) << "index="
                       << id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX);
  }
  DINGO_LOG(INFO) << "Recover id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 1.coordinator map
  if (!meta_reader_->Scan(coordinator_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    if (!coordinator_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover coordinator_meta, count=" << kvs.size();
  kvs.clear();

  // 2.store map
  if (!meta_reader_->Scan(store_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    if (!store_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover store_meta, count=" << kvs.size();
  kvs.clear();

  // 3.executor map
  if (!meta_reader_->Scan(executor_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    if (!executor_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover executor_meta, count=" << kvs.size();
  kvs.clear();

  // 4.schema map
  if (!meta_reader_->Scan(schema_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (!schema_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover schema_meta, count=" << kvs.size();
  kvs.clear();

  // 5.region map
  if (!meta_reader_->Scan(region_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    if (!region_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover region_meta, count=" << kvs.size();
  kvs.clear();

  // 5.1 deleted region map
  if (!meta_reader_->Scan(deleted_region_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    // if (!deleted_region_meta_->Recover(kvs)) {
    //   return false;
    // }
  }
  DINGO_LOG(INFO) << "Recover deleted_region_meta, count=" << kvs.size();
  kvs.clear();

  // 6.table map
  if (!meta_reader_->Scan(table_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (!table_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover table_meta, count=" << kvs.size();
  kvs.clear();

  // 6.1 deleted_table map
  if (!meta_reader_->Scan(deleted_table_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    // if (!deleted_table_meta_->Recover(kvs)) {
    //   return false;
    // }
  }
  DINGO_LOG(INFO) << "Recover deleted_table_meta, count=" << kvs.size();
  kvs.clear();

  // 8.table_metrics map

  // 9.store_operation map
  if (!meta_reader_->Scan(store_operation_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(store_operation_map_mutex_);
    if (!store_operation_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover store_operation_meta_, count=" << kvs.size();
  kvs.clear();

  // 9.1.region_cmd map
  if (!meta_reader_->Scan(region_cmd_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(region_cmd_map_mutex_);
    if (!region_cmd_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover region_cmd_meta_, count=" << kvs.size();
  kvs.clear();

  // 10.executor_user map
  if (!meta_reader_->Scan(executor_user_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(store_operation_map_mutex_);
    if (!executor_user_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover executor_user_meta_, count=" << kvs.size();
  kvs.clear();

  // 11.task_list map
  if (!meta_reader_->Scan(task_list_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(task_list_map_mutex_);
    if (!task_list_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover task_list_meta_, count=" << kvs.size();
  kvs.clear();

  // 12.index map
  if (!meta_reader_->Scan(index_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    if (!index_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover index_meta, count=" << kvs.size();
  kvs.clear();

  // 12.1 deleted_index map
  if (!meta_reader_->Scan(deleted_index_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    // if (!deleted_index_meta_->Recover(kvs)) {
    //   return false;
    // }
  }
  DINGO_LOG(INFO) << "Recover deleted_index_meta, count=" << kvs.size();
  kvs.clear();

  // 13.index_metrics map

  // 50.table_index map
  if (!meta_reader_->Scan(table_index_meta_->Prefix(), kvs)) {
    return false;
  }
  if (!table_index_meta_->Recover(kvs)) {
    return false;
  }

  DINGO_LOG(INFO) << "Recover table_index_meta, count=" << kvs.size();
  kvs.clear();

  // 51.1 common_disk_map
  if (!meta_reader_->Scan(common_disk_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    // if (!common_map_meta_->Recover(kvs)) {
    //   return false;
    // }
  }
  DINGO_LOG(INFO) << "Recover common_disk_map_meta, count=" << kvs.size();
  kvs.clear();

  // 51.2.common_mem_map
  if (!meta_reader_->Scan(common_mem_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    if (!common_mem_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover common_mem_meta, count=" << kvs.size();
  kvs.clear();

  // 52.tenant_mem_map
  if (!meta_reader_->Scan(tenant_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    if (!tenant_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover tenant_mem_meta, count=" << kvs.size();
  kvs.clear();

  // build id_epoch, schema_name, table_name, index_name maps
  BuildTempMaps();

  return true;
}

// Init
// init is called after recover
bool CoordinatorControl::Init() {
  // root=0 meta=1 dingo=2, other schema begins from 3
  // init schema_map_ at innocent cluster
  if (schema_map_.Size() == 0) {
    pb::coordinator_internal::SchemaInternal root_schema;
    pb::coordinator_internal::SchemaInternal meta_schema;
    pb::coordinator_internal::SchemaInternal dingo_schema;
    pb::coordinator_internal::SchemaInternal mysql_schema;
    pb::coordinator_internal::SchemaInternal information_schema;

    GenerateRootSchemas(root_schema, meta_schema, dingo_schema, mysql_schema, information_schema);

    // add the initial schemas to schema_map_
    schema_map_.Put(pb::meta::ReservedSchemaIds::ROOT_SCHEMA, root_schema);                // raft_kv_put
    schema_map_.Put(pb::meta::ReservedSchemaIds::META_SCHEMA, meta_schema);                // raft_kv_put
    schema_map_.Put(pb::meta::ReservedSchemaIds::DINGO_SCHEMA, dingo_schema);              // raft_kv_put
    schema_map_.Put(pb::meta::ReservedSchemaIds::MYSQL_SCHEMA, mysql_schema);              // raft_kv_put
    schema_map_.Put(pb::meta::ReservedSchemaIds::INFORMATION_SCHEMA, information_schema);  // raft_kv_put

    // write to rocksdb
    auto const schema_kvs = schema_meta_->TransformToKvWithAll();
    meta_writer_->Put(schema_kvs);

    DINGO_LOG(INFO) << "init schema_map_ finished";
  }

  return true;
}

void CoordinatorControl::GetServerLocation(pb::common::Location& raft_location, pb::common::Location& server_location) {
  // find in cache
  auto raft_location_string = raft_location.host() + ":" + std::to_string(raft_location.port());
  if (coordinator_location_cache_.find(raft_location_string) != coordinator_location_cache_.end()) {
    server_location = coordinator_location_cache_[raft_location_string];
    DINGO_LOG(DEBUG) << "GetServiceLocation Cache Hit raft_location=" << raft_location.host() << ":"
                     << raft_location.port();
    return;
  }

  Helper::GetServerLocation(raft_location, server_location);

  // transform ip to hostname
  Server::GetInstance().Ip2Hostname(*server_location.mutable_host());

  // add to cache if get server_location
  if (server_location.host().length() > 0 && server_location.port() > 0) {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, add new cache raft_location=" << raft_location.host() << ":"
                    << raft_location.port();
    coordinator_location_cache_[raft_location_string] = server_location;
  } else {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, can't get server_location, raft_location="
                    << raft_location.host() << ":" << raft_location.port();
  }
}

void CoordinatorControl::GetRaftLocation(pb::common::Location& server_location, pb::common::Location& raft_location) {
  // find in cache
  auto server_location_string = server_location.host() + ":" + std::to_string(server_location.port());
  if (coordinator_location_cache_.find(server_location_string) != coordinator_location_cache_.end()) {
    raft_location = coordinator_location_cache_[server_location_string];
    DINGO_LOG(INFO) << "GetServiceLocation Cache Hit server_location=" << server_location.host() << ":"
                    << server_location.port();
    return;
  }

  Helper::GetServerLocation(server_location, raft_location);

  // add to cache if get raft_location
  if (raft_location.host().length() > 0 && raft_location.port() > 0) {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, add new cache server_location=" << server_location.host() << ":"
                    << server_location.port();
    coordinator_location_cache_[server_location_string] = raft_location;
  } else {
    DINGO_LOG(INFO) << "GetServiceLocation Cache Miss, can't get raft_location, server_location="
                    << server_location.host() << ":" << server_location.port();
  }
}

void CoordinatorControl::GetLeaderLocation(pb::common::Location& leader_server_location) {
  if (raft_node_ == nullptr) {
    DINGO_LOG(ERROR) << "GetLeaderLocation raft_node_ is nullptr";
    return;
  }

  // parse leader raft location from string
  auto leader_string = raft_node_->GetLeaderId().to_string();

  pb::common::Location leader_raft_location;
  int ret = Helper::PeerIdToLocation(raft_node_->GetLeaderId(), leader_raft_location);
  if (ret < 0) {
    return;
  }

  // GetServerLocation
  this->GetServerLocation(leader_raft_location, leader_server_location);
}

// GetNextId only update id_epoch_map_temp_ in leader, the persistent id_epoch_map_ will be updated in on_apply
// When on_leader_start, the id_epoch_map_temp_ will init from id_epoch_map_
// only id_epoch_map_ is in state machine, and will persistent to raft and local rocksdb
int64_t CoordinatorControl::GetNextId(const pb::coordinator_internal::IdEpochType& key,
                                      pb::coordinator_internal::MetaIncrement& meta_increment) {
  // get next id from id_epoch_map_safe_temp_
  int64_t next_id = 0;
  id_epoch_map_safe_temp_.GetNextId(key, next_id);

  if (next_id == INT64_MAX || next_id < 0) {
    DINGO_LOG(FATAL) << "GetNextId next_id=" << next_id << " key=" << key << ", FATAL id is used up";
  }

  // generate meta_increment
  auto* idepoch = meta_increment.add_idepochs();
  idepoch->set_id(key);
  idepoch->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

  auto* idepoch_internal = idepoch->mutable_idepoch();
  idepoch_internal->set_id(key);
  idepoch_internal->set_value(next_id);

  return next_id;
}

std::vector<int64_t> CoordinatorControl::GetNextIds(const pb::coordinator_internal::IdEpochType& key, int64_t count,
                                                    pb::coordinator_internal::MetaIncrement& meta_increment) {
  std::vector<int64_t> next_ids_temp;

  if (count <= 0) {
    DINGO_LOG(ERROR) << "GetNextIds count=" << count << " key=" << key << ", ERROR count must > 0";
    return next_ids_temp;
  }

  // get next id from id_epoch_map_safe_temp_
  auto ret = id_epoch_map_safe_temp_.GetNextIds(key, count, next_ids_temp);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "GetNextIds count=" << count << " key=" << key << ", ERROR GetNextIds failed";
    return next_ids_temp;
  }

  if (next_ids_temp.empty()) {
    DINGO_LOG(FATAL) << "GetNextIds count=" << count << " key=" << key << ", ERROR next_ids_temp is empty";
  }

  int64_t max_next_id = next_ids_temp.at(next_ids_temp.size() - 1);
  for (const auto& id : next_ids_temp) {
    if (BAIDU_UNLIKELY(id > max_next_id)) {
      max_next_id = id;
    }
  }

  // generate meta_increment
  auto* idepoch = meta_increment.add_idepochs();
  idepoch->set_id(key);
  idepoch->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

  auto* idepoch_internal = idepoch->mutable_idepoch();
  idepoch_internal->set_id(key);
  idepoch_internal->set_value(max_next_id);

  return next_ids_temp;
}

int64_t CoordinatorControl::GetPresentId(const pb::coordinator_internal::IdEpochType& key) {
  int64_t value = 0;
  id_epoch_map_safe_temp_.GetPresentId(key, value);

  return value;
}

int64_t CoordinatorControl::UpdatePresentId(const pb::coordinator_internal::IdEpochType& key, int64_t new_id,
                                            pb::coordinator_internal::MetaIncrement& meta_increment) {
  // get next id from id_epoch_map_safe_temp_
  id_epoch_map_safe_temp_.UpdatePresentId(key, new_id);

  // generate meta_increment
  auto* idepoch = meta_increment.add_idepochs();
  idepoch->set_id(key);
  idepoch->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

  auto* idepoch_internal = idepoch->mutable_idepoch();
  idepoch_internal->set_id(key);
  idepoch_internal->set_value(new_id);

  return new_id;
}

}  // namespace dingodb

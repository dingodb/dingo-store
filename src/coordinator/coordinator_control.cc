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

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "braft/configuration.h"
#include "brpc/channel.h"
#include "butil/scoped_lock.h"
#include "butil/strings/string_split.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/snapshot.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"

namespace dingodb {

CoordinatorControl::CoordinatorControl(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer,
                                       std::shared_ptr<RawEngine> raw_engine_of_meta)
    : meta_reader_(meta_reader), meta_writer_(meta_writer), leader_term_(-1), raw_engine_of_meta_(raw_engine_of_meta) {
  // init bthread mutex
  bthread_mutex_init(&id_epoch_map_temp_mutex_, nullptr);
  bthread_mutex_init(&id_epoch_map_mutex_, nullptr);
  bthread_mutex_init(&coordinator_map_mutex_, nullptr);
  bthread_mutex_init(&store_map_mutex_, nullptr);
  bthread_mutex_init(&store_need_push_mutex_, nullptr);
  bthread_mutex_init(&executor_map_mutex_, nullptr);
  bthread_mutex_init(&executor_need_push_mutex_, nullptr);
  bthread_mutex_init(&schema_map_mutex_, nullptr);
  bthread_mutex_init(&region_map_mutex_, nullptr);
  bthread_mutex_init(&table_map_mutex_, nullptr);
  bthread_mutex_init(&store_metrics_map_mutex_, nullptr);
  bthread_mutex_init(&table_metrics_map_mutex_, nullptr);
  root_schema_writed_to_raft_ = false;

  // the data structure below will write to raft
  coordinator_meta_ = new MetaMapStorage<pb::coordinator_internal::CoordinatorInternal>(&coordinator_map_);
  store_meta_ = new MetaMapStorage<pb::common::Store>(&store_map_);
  schema_meta_ = new MetaMapStorage<pb::coordinator_internal::SchemaInternal>(&schema_map_);
  region_meta_ = new MetaMapStorage<pb::common::Region>(&region_map_);
  table_meta_ = new MetaMapStorage<pb::coordinator_internal::TableInternal>(&table_map_);
  id_epoch_meta_ = new MetaMapStorage<pb::coordinator_internal::IdEpochInternal>(&id_epoch_map_);
  executor_meta_ = new MetaMapStorage<pb::common::Executor>(&executor_map_);
  store_metrics_meta_ = new MetaMapStorage<pb::common::StoreMetrics>(&store_metrics_map_);
  table_metrics_meta_ = new MetaMapStorage<pb::coordinator_internal::TableMetricsInternal>(&table_metrics_map_);

  // init FlatMap
  id_epoch_map_temp_.init(1000, 80);
  id_epoch_map_.init(1000, 80);
  coordinator_map_.init(1000, 80);
  store_map_.init(1000, 80);
  store_need_push_.init(1000, 80);
  executor_map_.init(1000, 80);
  executor_need_push_.init(1000, 80);
  schema_map_.init(10000, 80);
  region_map_.init(300000, 80);
  table_map_.init(100000, 80);
  store_metrics_map_.init(1000, 80);
  table_metrics_map_.init(100000, 80);
}

CoordinatorControl::~CoordinatorControl() {
  delete coordinator_meta_;
  delete store_meta_;
  delete schema_meta_;
  delete region_meta_;
  delete table_meta_;
  delete id_epoch_meta_;
  delete executor_meta_;
}

bool CoordinatorControl::Recover() {
  DINGO_LOG(INFO) << "Coordinator start to Recover";

  std::vector<pb::common::KeyValue> kvs;

  // 0.id_epoch map
  if (!meta_reader_->Scan(id_epoch_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);

    if (!id_epoch_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 1.coordinator map
  if (!meta_reader_->Scan(coordinator_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
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
    BAIDU_SCOPED_LOCK(store_map_mutex_);
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
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
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
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
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
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    if (!region_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover region_meta, count=" << kvs.size();
  kvs.clear();

  // 6.table map
  if (!meta_reader_->Scan(table_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (!table_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover table_meta, count=" << kvs.size();
  kvs.clear();

  // 7.store_metrics map
  if (!meta_reader_->Scan(store_metrics_meta_->Prefix(), kvs)) {
    return false;
  }
  {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    if (!store_metrics_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "Recover store_metrics_meta, count=" << kvs.size();
  kvs.clear();

  // init id_epoch_map_temp_
  // copy id_epoch_map_ to id_epoch_map_temp_
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_temp_mutex_);
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    id_epoch_map_temp_ = id_epoch_map_;
  }
  DINGO_LOG(INFO) << "Recover id_epoch_map_temp, count=" << id_epoch_map_temp_.size();

  return true;
}

// Init
// init is called after recover
bool CoordinatorControl::Init() {
  // root=0 meta=1 dingo=2, other schema begins from 3
  // init schema_map_ at innocent cluster
  if (schema_map_.empty()) {
    pb::coordinator_internal::SchemaInternal root_schema;
    pb::coordinator_internal::SchemaInternal meta_schema;
    pb::coordinator_internal::SchemaInternal dingo_schema;
    pb::coordinator_internal::SchemaInternal mysql_schema;
    pb::coordinator_internal::SchemaInternal information_schema;

    GenerateRootSchemas(root_schema, meta_schema, dingo_schema, mysql_schema, information_schema);

    // add the initial schemas to schema_map_
    schema_map_.insert(std::make_pair(pb::meta::ReservedSchemaIds::ROOT_SCHEMA, root_schema));    // raft_kv_put
    schema_map_.insert(std::make_pair(pb::meta::ReservedSchemaIds::META_SCHEMA, meta_schema));    // raft_kv_put
    schema_map_.insert(std::make_pair(pb::meta::ReservedSchemaIds::DINGO_SCHEMA, dingo_schema));  // raft_kv_put
    schema_map_.insert(std::make_pair(pb::meta::ReservedSchemaIds::MYSQL_SCHEMA, mysql_schema));  // raft_kv_put
    schema_map_.insert(
        std::make_pair(pb::meta::ReservedSchemaIds::INFORMATION_SCHEMA, information_schema));  // raft_kv_put

    // write to rocksdb
    auto const schema_kvs = schema_meta_->TransformToKvWithAll();
    meta_writer_->Put(schema_kvs);

    DINGO_LOG(INFO) << "init schema_map_ finished";
  }

  return true;
}

uint64_t CoordinatorControl::GetPresentId(const pb::coordinator_internal::IdEpochType& key) {
  uint64_t value = 0;
  BAIDU_SCOPED_LOCK(id_epoch_map_temp_mutex_);

  auto* temp_id_epoch = id_epoch_map_temp_.seek(key);

  // if (id_epoch_map_temp_.find(key) == id_epoch_map_temp_.end()) {
  if (temp_id_epoch == nullptr) {
    value = COORDINATOR_ID_OF_MAP_MIN;
    DINGO_LOG(INFO) << "GetPresentId key=" << pb::coordinator_internal::IdEpochType_Name(key)
                    << " not found, generate new id=" << value;
  } else {
    value = id_epoch_map_temp_[key].value();
    DINGO_LOG(INFO) << "GetPresentId key=" << pb::coordinator_internal::IdEpochType_Name(key) << " value=" << value;
  }

  return value;
}

void CoordinatorControl::GetServerLocation(pb::common::Location& raft_location, pb::common::Location& server_location) {
  // find in cache
  auto raft_location_string = raft_location.host() + ":" + std::to_string(raft_location.port());
  if (coordinator_location_cache_.find(raft_location_string) != coordinator_location_cache_.end()) {
    server_location.CopyFrom(coordinator_location_cache_[raft_location_string]);
    DINGO_LOG(INFO) << "GetServiceLocation Cache Hit raft_location=" << raft_location.host() << ":"
                    << raft_location.port();
    return;
  }

  Helper::GetServerLocation(raft_location, server_location);

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
uint64_t CoordinatorControl::GetNextId(const pb::coordinator_internal::IdEpochType& key,
                                       pb::coordinator_internal::MetaIncrement& meta_increment) {
  uint64_t value = 0;
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_temp_mutex_);
    auto* temp_id_epoch = id_epoch_map_temp_.seek(key);

    // if (id_epoch_map_temp_.seek(key) == nullptr) {
    if (temp_id_epoch == nullptr) {
      value = COORDINATOR_ID_OF_MAP_MIN + 1;
      DINGO_LOG(INFO) << "GetNextId key=" << pb::coordinator_internal::IdEpochType_Name(key)
                      << " not found, generate new id=" << value;

      pb::coordinator_internal::IdEpochInternal id_epoch;
      id_epoch.set_id(key);
      id_epoch.set_value(value);

      // update id in memory
      id_epoch_map_temp_.insert(key, id_epoch);
    } else {
      value = temp_id_epoch->value() + 1;
      DINGO_LOG(INFO) << "GetNextId key=" << pb::coordinator_internal::IdEpochType_Name(key) << " value=" << value;
      // update id in memory
      temp_id_epoch->set_value(value);
    }
  }

  // generate meta_increment
  auto* idepoch = meta_increment.add_idepochs();
  idepoch->set_id(key);
  idepoch->set_op_type(::dingodb::pb::coordinator_internal::MetaIncrementOpType::UPDATE);

  auto* idepoch_internl = idepoch->mutable_idepoch();
  idepoch_internl->set_id(key);
  idepoch_internl->set_value(value);

  return value;
}

}  // namespace dingodb

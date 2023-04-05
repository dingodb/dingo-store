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

#include <sys/types.h>

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
#include "coordinator/coordinator_control.h"
#include "engine/snapshot.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"

namespace dingodb {

bool CoordinatorControl::IsLeader() { return leader_term_.load(butil::memory_order_acquire) > 0; }
void CoordinatorControl::SetLeaderTerm(int64_t term) { leader_term_.store(term, butil::memory_order_release); }
void CoordinatorControl::SetRaftNode(std::shared_ptr<RaftNode> raft_node) { raft_node_ = raft_node; }

int CoordinatorControl::GetAppliedTermAndIndex(uint64_t& term, uint64_t& index) {
  BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);

  int ret = 0;
  auto* temp_index = id_epoch_map_temp_.seek(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX);
  auto* temp_term = id_epoch_map_temp_.seek(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM);

  if (temp_index != nullptr) {
    index = temp_index->value();
  } else {
    DINGO_LOG(ERROR) << "GetAppliedTermAndIndex failed, id_epoch_map_ not contain RAFT_APPLY_INDEX";
    ret = -1;
  }

  if (temp_term != nullptr) {
    term = temp_term->value();
  } else {
    DINGO_LOG(ERROR) << "GetAppliedTermAndIndex failed, id_epoch_map_ not contain RAFT_APPLY_TERM";
    ret = -1;
  }

  DINGO_LOG(INFO) << "GetAppliedTermAndIndex, term=" << term << ", index=" << index;

  return ret;
}

// OnLeaderStart will init id_epoch_map_temp_ from id_epoch_map_ which is in state machine
void CoordinatorControl::OnLeaderStart(int64_t term) {
  BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
  id_epoch_map_temp_ = id_epoch_map_;
  DINGO_LOG(INFO) << "OnLeaderStart init id_epoch_map_temp_ finished, term=" << term;
}

std::shared_ptr<Snapshot> CoordinatorControl::PrepareRaftSnapshot() {
  DINGO_LOG(INFO) << "PrepareRaftSnapshot";
  return this->raw_engine_of_meta_->GetSnapshot();
}

bool CoordinatorControl::LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
                                                pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
  DINGO_LOG(INFO) << "Coordinator start to LoadMetaToSnapshotFile";

  std::vector<pb::common::KeyValue> kvs;

  // 0.id_epoch map
  if (!meta_reader_->Scan(snapshot, id_epoch_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_id_epoch_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }

  DINGO_LOG(INFO) << "Snapshot id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 1.coordinator map
  if (!meta_reader_->Scan(snapshot, coordinator_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_coordinator_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot coordinator_meta, count=" << kvs.size();
  kvs.clear();

  // 2.store map
  if (!meta_reader_->Scan(snapshot, store_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_store_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot store_meta, count=" << kvs.size();
  kvs.clear();

  // 3.executor map
  if (!meta_reader_->Scan(snapshot, executor_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_executor_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot executor_meta, count=" << kvs.size();
  kvs.clear();

  // 4.schema map
  if (!meta_reader_->Scan(snapshot, schema_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_schema_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot schema_meta, count=" << kvs.size();
  kvs.clear();

  // 5.region map
  if (!meta_reader_->Scan(snapshot, region_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_region_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot region_meta, count=" << kvs.size();
  kvs.clear();

  // 6.table map
  if (!meta_reader_->Scan(snapshot, table_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_table_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot table_meta, count=" << kvs.size();
  kvs.clear();

  // 7.store_metrics map
  if (!meta_reader_->Scan(snapshot, store_metrics_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_store_metrics_map_kvs();
    snapshot_file_kv->CopyFrom(kv);
  }
  DINGO_LOG(INFO) << "Snapshot store_metrics_meta, count=" << kvs.size();
  kvs.clear();

  return true;
}

bool CoordinatorControl::LoadMetaFromSnapshotFile(pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
  DINGO_LOG(INFO) << "Coordinator start to LoadMetaFromSnapshotFile";

  // clean all data of rocksdb
  if (!meta_writer_->DeleteRange(std::string(10, '\0'), std::string(10, '\xff'))) {
    DINGO_LOG(ERROR) << "Coordinator delete range failed in LoadMetaFromSnapshotFile";
    return false;
  }
  DINGO_LOG(INFO) << "Coordinator delete range success in LoadMetaFromSnapshotFile";

  std::vector<pb::common::KeyValue> kvs;

  // 0.id_epoch map
  kvs.reserve(meta_snapshot_file.id_epoch_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.id_epoch_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.id_epoch_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    if (!id_epoch_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 1.coordinator map
  kvs.reserve(meta_snapshot_file.coordinator_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.coordinator_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.coordinator_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    if (!coordinator_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot coordinator_meta, count=" << kvs.size();
  kvs.clear();

  // 2.store map
  kvs.reserve(meta_snapshot_file.store_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.store_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.store_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    if (!store_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot store_meta, count=" << kvs.size();
  kvs.clear();

  // 3.executor map
  kvs.reserve(meta_snapshot_file.executor_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.executor_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.executor_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    if (!executor_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot executor_meta, count=" << kvs.size();
  kvs.clear();

  // 4.schema map
  kvs.reserve(meta_snapshot_file.schema_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.schema_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.schema_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (!schema_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot schema_meta, count=" << kvs.size();
  kvs.clear();

  // 5.region map
  kvs.reserve(meta_snapshot_file.region_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.region_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.region_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    if (!region_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot region_meta, count=" << kvs.size();
  kvs.clear();

  // 6.table map
  kvs.reserve(meta_snapshot_file.table_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.table_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.table_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (!table_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot table_meta, count=" << kvs.size();
  kvs.clear();

  // 7.store_metrics map
  kvs.reserve(meta_snapshot_file.store_metrics_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.store_metrics_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.store_metrics_map_kvs(i));
  }
  {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    if (!store_metrics_meta_->Recover(kvs)) {
      return false;
    }
  }
  DINGO_LOG(INFO) << "LoadSnapshot store_metrics_meta, count=" << kvs.size();
  kvs.clear();

  // init id_epoch_map_temp_
  // copy id_epoch_map_ to id_epoch_map_temp_
  {
    BAIDU_SCOPED_LOCK(id_epoch_map_temp_mutex_);
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    id_epoch_map_temp_ = id_epoch_map_;
  }
  DINGO_LOG(INFO) << "LoadSnapshot id_epoch_map_temp, count=" << id_epoch_map_temp_.size();

  return true;
}

// ApplyMetaIncrement is on_apply callback
void CoordinatorControl::ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement& meta_increment,
                                            [[maybe_unused]] bool id_leader, uint64_t term, uint64_t index) {
  // prepare data to write to kv engine
  std::vector<pb::common::KeyValue> meta_write_to_kv;
  std::vector<pb::common::KeyValue> meta_delete_to_kv;

  {
    BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    // if index < local apply index, just return
    uint64_t applied_index = 0;
    uint64_t applied_term = 0;

    auto* temp_index = id_epoch_map_.seek(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX);
    auto* temp_term = id_epoch_map_.seek(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM);

    // if (id_epoch_map_.find(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX) != id_epoch_map_.end()) {
    if (temp_index != nullptr) {
      applied_index = temp_index->value();
    }

    // if (id_epoch_map_.find(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM) != id_epoch_map_.end()) {
    if (temp_term != nullptr) {
      applied_term = temp_term->value();
    }

    if (index <= applied_index && term <= applied_term) {
      DINGO_LOG(WARNING) << "ApplyMetaIncrement index <= applied_index && term <<= applied_term, just return, [index="
                         << index << "][applied_index=" << applied_index << "]"
                         << "[term=" << term << "][applied_term=" << applied_term;
      return;
    }

    // 0.id & epoch
    // raft_apply_term & raft_apply_index stores in id_epoch_map too
    pb::coordinator_internal::IdEpochInternal raft_apply_term;
    raft_apply_term.set_id(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM);
    raft_apply_term.set_value(term);

    pb::coordinator_internal::IdEpochInternal raft_apply_index;
    raft_apply_index.set_id(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX);
    raft_apply_index.set_value(index);

    // update applied term & index in fsm
    id_epoch_map_[pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM] = raft_apply_term;
    id_epoch_map_[pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX] = raft_apply_index;

    meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(raft_apply_term));
    meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(raft_apply_index));

    for (int i = 0; i < meta_increment.idepochs_size(); i++) {
      const auto& idepoch = meta_increment.idepochs(i);
      if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto& create_idepoch = id_epoch_map_[idepoch.id()];
        create_idepoch.CopyFrom(idepoch.idepoch());

        meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));

      } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_idepoch = id_epoch_map_[idepoch.id()];
        update_idepoch.CopyFrom(idepoch.idepoch());

        meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));

      } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        id_epoch_map_.erase(idepoch.id());

        meta_delete_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));
      }
    }
  }

  // 1.coordinator map
  {
    BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    for (int i = 0; i < meta_increment.coordinators_size(); i++) {
      const auto& coordinator = meta_increment.coordinators(i);
      if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        coordinator_map_[coordinator.id()] = coordinator.coordinator();

        // meta_write_kv
        meta_write_to_kv.push_back(coordinator_meta_->TransformToKvValue(coordinator.coordinator()));

      } else if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_coordinator = coordinator_map_[coordinator.id()];
        update_coordinator.CopyFrom(coordinator.coordinator());

        // meta_write_kv
        meta_write_to_kv.push_back(coordinator_meta_->TransformToKvValue(coordinator.coordinator()));

      } else if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        coordinator_map_.erase(coordinator.id());

        // meta_delete_kv
        meta_delete_to_kv.push_back(coordinator_meta_->TransformToKvValue(coordinator.coordinator()));
      }
    }
  }

  // 2.store map
  {
    BAIDU_SCOPED_LOCK(store_map_mutex_);
    for (int i = 0; i < meta_increment.stores_size(); i++) {
      const auto& store = meta_increment.stores(i);
      if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        store_map_[store.id()] = store.store();

        // meta_write_kv
        meta_write_to_kv.push_back(store_meta_->TransformToKvValue(store.store()));

      } else if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_store = store_map_[store.id()];
        update_store.CopyFrom(store.store());

        // meta_write_kv
        meta_write_to_kv.push_back(store_meta_->TransformToKvValue(store.store()));

      } else if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        store_map_.erase(store.id());

        // meta_delete_kv
        meta_delete_to_kv.push_back(store_meta_->TransformToKvValue(store.store()));
      }
    }
  }

  // 3.executor map
  {
    BAIDU_SCOPED_LOCK(executor_map_mutex_);
    for (int i = 0; i < meta_increment.executors_size(); i++) {
      const auto& executor = meta_increment.executors(i);
      if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        executor_map_[executor.id()] = executor.executor();

        // meta_write_kv
        meta_write_to_kv.push_back(executor_meta_->TransformToKvValue(executor.executor()));

      } else if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_executor = executor_map_[executor.id()];
        update_executor.CopyFrom(executor.executor());

        // meta_write_kv
        meta_write_to_kv.push_back(executor_meta_->TransformToKvValue(executor.executor()));

      } else if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        executor_map_.erase(executor.id());

        // meta_delete_kv
        meta_delete_to_kv.push_back(executor_meta_->TransformToKvValue(executor.executor()));
      }
    }
  }

  // 4.schema map
  {
    BAIDU_SCOPED_LOCK(schema_map_mutex_);
    for (int i = 0; i < meta_increment.schemas_size(); i++) {
      const auto& schema = meta_increment.schemas(i);
      if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // update parent schema for user schemas
        if (schema.id() > pb::meta::ReservedSchemaIds::MAX_INTERNAL_SCHEMA) {
          auto* parent_schema = schema_map_.seek(schema.schema_id());
          // if (schema_map_.find(schema.schema_id()) != schema_map_.end()) {
          if (parent_schema != nullptr) {
            // add new created schema's id to its parent schema's schema_ids
            parent_schema->add_schema_ids(schema.id());

            DINGO_LOG(INFO) << "3.schema map CREATE new_sub_schema id=" << schema.id()
                            << " parent_id=" << schema.schema_id();

            // meta_write_kv
            meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(*parent_schema));
          }
        }
        schema_map_[schema.id()] = schema.schema_internal();

        // meta_write_kv
        meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));

      } else if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_schema = schema_map_[schema.id()];
        update_schema.CopyFrom(schema.schema_internal());

        // meta_write_kv
        meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));

      } else if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        schema_map_.erase(schema.id());

        // delete from parent schema
        auto* parent_schema = schema_map_.seek(schema.schema_id());
        // if (schema_map_.find(schema.schema_id()) != schema_map_.end()) {
        if (parent_schema != nullptr) {
          // according to the protobuf document, we must use CopyFrom for protobuf message data structure here
          pb::coordinator_internal::SchemaInternal new_schema;
          new_schema.CopyFrom(*parent_schema);

          new_schema.clear_table_ids();

          // add left schema_id to new_schema
          for (auto x : parent_schema->table_ids()) {
            if (x != schema.id()) {
              new_schema.add_schema_ids(x);
            }
          }
          parent_schema->CopyFrom(new_schema);

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(*parent_schema));
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));
      }
    }
  }

  // 5.region map
  {
    BAIDU_SCOPED_LOCK(region_map_mutex_);
    for (int i = 0; i < meta_increment.regions_size(); i++) {
      const auto& region = meta_increment.regions(i);
      if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // add region to region_map
        region_map_[region.id()] = region.region();

        // add_store_for_push
        // only create region will push to store now
        {
          BAIDU_SCOPED_LOCK(store_need_push_mutex_);
          for (int j = 0; j < region.region().peers_size(); j++) {
            uint64_t store_id = region.region().peers(j).store_id();
            DINGO_LOG(INFO) << " add_store_for_push, peers_size=" << region.region().peers_size()
                            << " store_id =" << store_id;

            if (store_need_push_.seek(store_id) == nullptr) {
              auto* temp_store = store_map_.seek(store_id);
              if (temp_store != nullptr) {
                store_need_push_.insert(store_id, *temp_store);
                DINGO_LOG(INFO) << " add_store_for_push, store_id=" << store_id
                                << " in create region=" << region.region().id()
                                << " location=" << temp_store->server_location().host() << ":"
                                << temp_store->server_location().port();
              } else {
                DINGO_LOG(ERROR) << " add_store_for_push, illegal store_id=" << store_id
                                 << " in create region=" << region.region().id();
              }
            }
          }
        }

        // meta_write_kv
        meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update region to region_map
        auto& update_region = region_map_[region.id()];
        update_region.CopyFrom(region.region());

        // meta_write_kv
        meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // remove region from region_map
        region_map_.erase(region.id());

        // meta_delete_kv
        meta_delete_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));
      }
    }
  }

  // 6.table map
  {
    BAIDU_SCOPED_LOCK(table_map_mutex_);
    for (int i = 0; i < meta_increment.tables_size(); i++) {
      const auto& table = meta_increment.tables(i);
      if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // need to update schema, so acquire lock
        BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // add table to table_map
        table_map_[table.id()] = table.table();

        // meta_write_kv
        meta_write_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));

        // add table to parent schema
        auto* schema = schema_map_.seek(table.schema_id());
        // if (schema_map_.find(table.schema_id()) != schema_map_.end()) {
        if (schema != nullptr) {
          // add new created table's id to its parent schema's table_ids
          schema->add_table_ids(table.id());

          DINGO_LOG(INFO) << "5.table map CREATE new_sub_table id=" << table.id() << " parent_id=" << table.schema_id();

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(*schema));

        } else {
          DINGO_LOG(ERROR) << " CREATE TABLE apply illegal schema_id=" << table.schema_id()
                           << " table_id=" << table.id() << " table_name=" << table.table().definition().name();
        }

      } else if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update table to table_map
        auto& update_table = table_map_[table.id()];
        update_table.CopyFrom(table.table());

        // meta_write_kv
        meta_write_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));

      } else if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // need to update schema, so acquire lock
        BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // delete table from table_map
        table_map_.erase(table.id());

        // delete from parent schema
        auto* schema = schema_map_.seek(table.schema_id());
        // if (schema_map_.find(table.schema_id()) != schema_map_.end()) {
        if (schema != nullptr) {
          // according to the doc, we must use CopyFrom for protobuf message data structure here
          pb::coordinator_internal::SchemaInternal new_schema;
          new_schema.CopyFrom(*schema);

          new_schema.clear_table_ids();

          // add left table_id to new_schema
          for (auto x : schema->table_ids()) {
            if (x != table.id()) {
              new_schema.add_table_ids(x);
            }
          }
          schema->CopyFrom(new_schema);

          DINGO_LOG(INFO) << "5.table map DELETE new_sub_table id=" << table.id() << " parent_id=" << table.schema_id();

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(*schema));

        } else {
          DINGO_LOG(ERROR) << " DROP TABLE apply illegal schema_id=" << table.schema_id() << " table_id=" << table.id()
                           << " table_name=" << table.table().definition().name();
        }
        // meta_delete_kv
        meta_delete_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));
      }
    }
  }

  // 7.store_metrics map
  {
    BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
    for (int i = 0; i < meta_increment.store_metrics_size(); i++) {
      const auto& store_metrics = meta_increment.store_metrics(i);
      if (store_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        store_metrics_map_[store_metrics.id()] = store_metrics.store_metrics();

        // meta_write_kv
        meta_write_to_kv.push_back(store_metrics_meta_->TransformToKvValue(store_metrics.store_metrics()));

      } else if (store_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto& update_store = store_metrics_map_[store_metrics.id()];
        update_store.CopyFrom(store_metrics.store_metrics());

        // meta_write_kv
        meta_write_to_kv.push_back(store_metrics_meta_->TransformToKvValue(store_metrics.store_metrics()));

      } else if (store_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        store_metrics_map_.erase(store_metrics.id());

        // meta_delete_kv
        meta_delete_to_kv.push_back(store_metrics_meta_->TransformToKvValue(store_metrics.store_metrics()));
      }
    }
  }

  // write update to local engine, begin
  if ((!meta_write_to_kv.empty()) || (!meta_delete_to_kv.empty())) {
    if (!meta_writer_->PutAndDelete(meta_write_to_kv, meta_delete_to_kv)) {
      DINGO_LOG(ERROR) << "ApplyMetaIncrement PutAndDelete failed, exit program";
      exit(-1);
    }
  }
  // write update to local engine, end
}

}  // namespace dingodb
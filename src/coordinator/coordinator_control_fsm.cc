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

#include "bthread/bthread.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "coordinator/coordinator_control.h"
#include "engine/snapshot.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

bool CoordinatorControl::IsLeader() { return leader_term_.load(butil::memory_order_acquire) > 0; }

void CoordinatorControl::SetLeaderTerm(int64_t term) {
  DINGO_LOG(INFO) << "SetLeaderTerm, term=" << term;
  leader_term_.store(term, butil::memory_order_release);
}

void CoordinatorControl::SetRaftNode(std::shared_ptr<RaftNode> raft_node) { raft_node_ = raft_node; }
std::shared_ptr<RaftNode> CoordinatorControl::GetRaftNode() { return raft_node_; }

int CoordinatorControl::GetAppliedTermAndIndex(int64_t& term, int64_t& index) {
  id_epoch_map_safe_temp_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM, term);
  id_epoch_map_safe_temp_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX, index);

  DINGO_LOG(INFO) << "GetAppliedTermAndIndex, term=" << term << ", index=" << index;

  return 0;
}

void CoordinatorControl::BuildTempMaps() {
  // copy id_epoch_map_ to id_epoch_map_temp_
  {
    butil::FlatMap<int64_t, pb::coordinator_internal::IdEpochInternal> temp_copy;
    temp_copy.init(100);
    id_epoch_map_.GetRawMapCopy(temp_copy);
    id_epoch_map_safe_temp_.CopyFromRawMap(temp_copy);
  }
  DINGO_LOG(INFO) << "id_epoch_safe_map_temp, count=" << id_epoch_map_safe_temp_.Size();

  // copy schema_map_ to schema_name_map_safe_temp_
  {
    schema_name_map_safe_temp_.Clear();
    butil::FlatMap<int64_t, pb::coordinator_internal::SchemaInternal> schema_map_copy;
    schema_map_copy.init(10000);
    schema_map_.GetRawMapCopy(schema_map_copy);
    for (const auto& it : schema_map_copy) {
      schema_name_map_safe_temp_.Put(it.second.name(), it.first);
    }
  }
  DINGO_LOG(INFO) << "schema_name_map_safe_temp, count=" << schema_name_map_safe_temp_.Size();

  // copy table_map_ to table_name_map_safe_temp_
  {
    table_name_map_safe_temp_.Clear();
    butil::FlatMap<int64_t, pb::coordinator_internal::TableInternal> table_map_copy;
    table_map_copy.init(10000);
    table_map_.GetRawMapCopy(table_map_copy);
    for (const auto& it : table_map_copy) {
      // table_name_map_safe_temp_.Put(std::to_string(it.second.schema_id()) + it.second.definition().name(), it.first);
      table_name_map_safe_temp_.Put(Helper::GenNewTableCheckName(it.second.schema_id(), it.second.definition().name()),
                                    it.first);
    }
  }

  DINGO_LOG(INFO) << "table_name_map_safe_temp, count=" << table_name_map_safe_temp_.Size();

  // copy index_map_ to index_name_map_safe_temp_
  {
    index_name_map_safe_temp_.Clear();
    butil::FlatMap<int64_t, pb::coordinator_internal::TableInternal> index_map_copy;
    index_map_copy.init(10000);
    index_map_.GetRawMapCopy(index_map_copy);
    for (const auto& it : index_map_copy) {
      // index_name_map_safe_temp_.Put(std::to_string(it.second.schema_id()) + it.second.definition().name(), it.first);
      index_name_map_safe_temp_.Put(Helper::GenNewTableCheckName(it.second.schema_id(), it.second.definition().name()),
                                    it.first);
    }
  }
  DINGO_LOG(INFO) << "index_name_map_safe_temp_ finished, count=" << index_name_map_safe_temp_.Size();

  // copy region_map_ to range_region_map_
  {
    range_region_map_.Clear();
    butil::FlatMap<uint64_t, pb::coordinator_internal::RegionInternal> region_map_copy;
    region_map_copy.init(10000);
    region_map_.GetRawMapCopy(region_map_copy);
    for (const auto& it : region_map_copy) {
      range_region_map_.Put(it.second.definition().range().start_key(), it.second.id());
    }
  }
}

// OnLeaderStart will init id_epoch_map_temp_ from id_epoch_map_ which is in state machine
void CoordinatorControl::OnLeaderStart(int64_t term) {
  // build id_epoch, schema_name, table_name, index_name maps
  BuildTempMaps();

  // build lease_to_key_map_temp_
  BuildLeaseToKeyMap();

  // clear one time watch map
  {
    BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);
    one_time_watch_map_.clear();
  }

  DINGO_LOG(INFO) << "OnLeaderStart init lease_to_key_map_temp_ finished, term=" << term
                  << " count=" << lease_to_key_map_temp_.size();

  coordinator_bvar_.SetValue(1);
  DINGO_LOG(INFO) << "OnLeaderStart finished, term=" << term;
}

void CoordinatorControl::OnLeaderStop() {
  coordinator_bvar_.SetValue(0);
  coordinator_bvar_metrics_store_.Clear();
  coordinator_bvar_metrics_region_.Clear();
  coordinator_bvar_metrics_table_.Clear();
  coordinator_bvar_metrics_index_.Clear();

  // clear all table_metrics on follower
  table_metrics_map_.Clear();

  // clear all index_metrics on follower
  index_metrics_map_.Clear();

  // clear one time watch map
  {
    BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);
    for (auto& it : one_time_watch_map_) {
      auto& closure_to_reposne_map = it.second;
      for (auto& ctrm : closure_to_reposne_map) {
        auto* done = ctrm.first;
        if (done) {
          done->Run();
        }
      }
    }

    one_time_watch_map_.clear();
    one_time_watch_closure_map_.clear();
  }

  DINGO_LOG(INFO) << "OnLeaderStop finished";
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
    *snapshot_file_kv = kv;
  }

  DINGO_LOG(INFO) << "Snapshot id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 1.coordinator map
  if (!meta_reader_->Scan(snapshot, coordinator_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_coordinator_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot coordinator_meta, count=" << kvs.size();
  kvs.clear();

  // 2.store map
  if (!meta_reader_->Scan(snapshot, store_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_store_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot store_meta, count=" << kvs.size();
  kvs.clear();

  // 3.executor map
  if (!meta_reader_->Scan(snapshot, executor_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_executor_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot executor_meta, count=" << kvs.size();
  kvs.clear();

  // 4.schema map
  if (!meta_reader_->Scan(snapshot, schema_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_schema_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot schema_meta, count=" << kvs.size();
  kvs.clear();

  // 5.region map
  if (!meta_reader_->Scan(snapshot, region_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_region_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot region_meta, count=" << kvs.size();
  kvs.clear();

  // 5.1 deleted region map
  if (!meta_reader_->Scan(snapshot, deleted_region_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_deleted_region_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot deleted_region_meta, count=" << kvs.size();
  kvs.clear();

  // 6.table map
  if (!meta_reader_->Scan(snapshot, table_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_table_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot table_meta, count=" << kvs.size();
  kvs.clear();

  // 6.1 deleted table map
  if (!meta_reader_->Scan(snapshot, deleted_table_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_deleted_table_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot deleted_table_meta, count=" << kvs.size();
  kvs.clear();

  // 7.store_metrics map
  // if (!meta_reader_->Scan(snapshot, store_metrics_meta_->Prefix(), kvs)) {
  //   return false;
  // }

  // for (const auto& kv : kvs) {
  //   auto* snapshot_file_kv = meta_snapshot_file.add_store_metrics_map_kvs();
  //   *snapshot_file_kv = kv;
  // }
  // DINGO_LOG(INFO) << "Snapshot store_metrics_meta, count=" << kvs.size();
  // kvs.clear();

  // 8.table_metrics map
  if (!meta_reader_->Scan(snapshot, table_metrics_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_table_metrics_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot table_metrics_meta, count=" << kvs.size();
  kvs.clear();

  // 9.store_operation map
  if (!meta_reader_->Scan(snapshot, store_operation_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_store_operation_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot store_operation_meta_, count=" << kvs.size();
  kvs.clear();
  // 9.1 region_cmd_map_
  if (!meta_reader_->Scan(snapshot, region_cmd_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_region_cmd_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot region_cmd_meta_, count=" << kvs.size();
  kvs.clear();

  // 10.executor_user map
  if (!meta_reader_->Scan(snapshot, executor_user_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_executor_user_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot executor_user_meta_, count=" << kvs.size();
  kvs.clear();

  // 11.task_list map
  if (!meta_reader_->Scan(snapshot, task_list_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_task_list_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot task_list_meta_, count=" << kvs.size();
  kvs.clear();

  // 12.index map
  if (!meta_reader_->Scan(snapshot, index_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_index_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot index_meta, count=" << kvs.size();
  kvs.clear();

  // 12.1 deleted_index map
  if (!meta_reader_->Scan(snapshot, deleted_index_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_deleted_index_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot deleted_index_meta, count=" << kvs.size();
  kvs.clear();

  // 13.index_metrics map
  if (!meta_reader_->Scan(snapshot, index_metrics_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_index_metrics_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot index_metrics_meta, count=" << kvs.size();
  kvs.clear();

  // 14.lease_map_
  if (!meta_reader_->Scan(snapshot, lease_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_lease_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot lease_map_, count=" << kvs.size();
  kvs.clear();

  // 15.kv_index_map_
  if (!meta_reader_->Scan(snapshot, kv_index_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_kv_index_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot kv_index_map_, count=" << kvs.size();
  kvs.clear();

  // 16.version_kv_rev_map_
  if (!meta_reader_->Scan(snapshot, kv_rev_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_kv_rev_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot version_kv_rev_map_, count=" << kvs.size();
  kvs.clear();

  // 50.table_index map
  if (!meta_reader_->Scan(snapshot, table_index_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_table_index_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot table_index_meta, count=" << kvs.size();
  kvs.clear();

  return true;
}

bool CoordinatorControl::LoadMetaFromSnapshotFile(pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
  DINGO_LOG(INFO) << "Coordinator start to LoadMetaFromSnapshotFile";

  std::vector<pb::common::KeyValue> kvs;

  // 0.id_epoch map
  kvs.reserve(meta_snapshot_file.id_epoch_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.id_epoch_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.id_epoch_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    if (!id_epoch_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(id_epoch_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete id_epoch_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range id_epoch_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write id_epoch_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put id_epoch_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot id_epoch_meta, count=" << kvs.size();
  kvs.clear();

  // 1.coordinator map
  kvs.reserve(meta_snapshot_file.coordinator_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.coordinator_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.coordinator_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    if (!coordinator_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(coordinator_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete coordinator_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range coordinator_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write coordinator_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put coordinator_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot coordinator_meta, count=" << kvs.size();
  kvs.clear();

  // 2.store map
  kvs.reserve(meta_snapshot_file.store_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.store_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.store_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    if (!store_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(store_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete store_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range store_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write store_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put store_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot store_meta, count=" << kvs.size();
  kvs.clear();

  // 3.executor map
  kvs.reserve(meta_snapshot_file.executor_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.executor_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.executor_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    if (!executor_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(executor_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete executor_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range executor_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write executor_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put executor_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot executor_meta, count=" << kvs.size();
  kvs.clear();

  // 4.schema map
  kvs.reserve(meta_snapshot_file.schema_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.schema_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.schema_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    if (!schema_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(schema_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete schema_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range schema_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write schema_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put schema_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot schema_meta, count=" << kvs.size();
  kvs.clear();

  // 5.region map
  kvs.reserve(meta_snapshot_file.region_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.region_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.region_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    if (!region_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(region_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete region_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range region_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write region_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put region_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot region_meta, count=" << kvs.size();
  kvs.clear();

  // 5.1 deleted region map
  kvs.reserve(meta_snapshot_file.deleted_region_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.deleted_region_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.deleted_region_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    if (!deleted_region_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(deleted_region_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete deleted_region_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range deleted_region_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write deleted_region_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put deleted_region_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot deleted_region_meta, count=" << kvs.size();
  kvs.clear();

  // 6.table map
  kvs.reserve(meta_snapshot_file.table_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.table_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.table_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    if (!table_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(table_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete table_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range table_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write table_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put table_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot table_meta, count=" << kvs.size();
  kvs.clear();

  // 6.1 deleted table map
  kvs.reserve(meta_snapshot_file.deleted_table_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.deleted_table_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.deleted_table_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(deleted_table_map_mutex_);
    if (!deleted_table_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(deleted_table_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete deleted_table_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range deleted_table_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write deleted_table_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put deleted_table_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot deleted_table_meta, count=" << kvs.size();
  kvs.clear();

  // 7.store_metrics map
  // kvs.reserve(meta_snapshot_file.store_metrics_map_kvs_size());
  // for (int i = 0; i < meta_snapshot_file.store_metrics_map_kvs_size(); i++) {
  //   kvs.push_back(meta_snapshot_file.store_metrics_map_kvs(i));
  // }
  // {
  //   BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
  //   if (!store_metrics_meta_->Recover(kvs)) {
  //     return false;
  //   }
  // }
  // {  // remove data in rocksdb
  //   if (!meta_writer_->DeletePrefix(store_metrics_meta_->internal_prefix)) {
  //     DINGO_LOG(ERROR) << "Coordinator delete store_metrics_meta_ range failed in LoadMetaFromSnapshotFile";
  //     return false;
  //   }
  //   DINGO_LOG(INFO) << "Coordinator delete range store_metrics_meta_ success in LoadMetaFromSnapshotFile";

  //   // write data to rocksdb
  //   if (!meta_writer_->Put(kvs)) {
  //     DINGO_LOG(ERROR) << "Coordinator write store_metrics_meta_ failed in LoadMetaFromSnapshotFile";
  //     return false;
  //   }
  //   DINGO_LOG(INFO) << "Coordinator put store_metrics_meta_ success in LoadMetaFromSnapshotFile";
  // }
  // DINGO_LOG(INFO) << "LoadSnapshot store_metrics_meta, count=" << kvs.size();
  // kvs.clear();

  // 8.table_metrics map
  kvs.reserve(meta_snapshot_file.table_metrics_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.table_metrics_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.table_metrics_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(table_metrics_map_mutex_);
    if (!table_metrics_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(table_metrics_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete table_metrics_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range table_metrics_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write table_metrics_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put table_metrics_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot table_metrics_meta, count=" << kvs.size();
  kvs.clear();

  // 9.store_operation map
  kvs.reserve(meta_snapshot_file.store_operation_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.store_operation_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.store_operation_map_kvs(i));
  }
  {
    if (!store_operation_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(store_operation_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete store_operation_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range store_operation_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write store_operation_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put store_operation_meta_ success in LoadMetaFromSnapshotFile";
  }

  DINGO_LOG(INFO) << "LoadSnapshot store_operation_meta, count=" << kvs.size();
  kvs.clear();

  // 9.1.region_cmd map
  kvs.reserve(meta_snapshot_file.region_cmd_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.region_cmd_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.region_cmd_map_kvs(i));
  }
  {
    if (!region_cmd_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(region_cmd_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete region_cmd_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range region_cmd_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write region_cmd_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put region_cmd_meta_ success in LoadMetaFromSnapshotFile";
  }

  DINGO_LOG(INFO) << "LoadSnapshot region_cmd_meta, count=" << kvs.size();
  kvs.clear();

  // 10.executor_user map
  kvs.reserve(meta_snapshot_file.executor_user_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.executor_user_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.executor_user_map_kvs(i));
  }
  {
    if (!executor_user_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(executor_user_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete executor_user_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range executor_user_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write executor_user_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put executor_user_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot executor_user_meta, count=" << kvs.size();
  kvs.clear();

  // 11.task_list map
  kvs.reserve(meta_snapshot_file.task_list_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.task_list_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.task_list_map_kvs(i));
  }
  {
    if (!task_list_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(task_list_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete task_list_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range task_list_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write task_list_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put task_list_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot task_list_meta, count=" << kvs.size();
  kvs.clear();

  // 12.index map
  kvs.reserve(meta_snapshot_file.index_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.index_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.index_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    if (!index_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(index_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete index_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range index_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write index_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put index_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot index_meta, count=" << kvs.size();
  kvs.clear();

  // 12.1 deleted_index map
  kvs.reserve(meta_snapshot_file.deleted_index_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.deleted_index_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.deleted_index_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(deleted_index_map_mutex_);
    if (!deleted_index_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(deleted_index_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete deleted_index_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range deleted_index_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write deleted_index_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put deleted_index_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot deleted_index_meta, count=" << kvs.size();
  kvs.clear();

  // 13.index_metrics map
  kvs.reserve(meta_snapshot_file.index_metrics_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.index_metrics_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.index_metrics_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(index_metrics_map_mutex_);
    if (!index_metrics_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(index_metrics_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete index_metrics_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range index_metrics_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write index_metrics_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put index_metrics_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot index_metrics_meta, count=" << kvs.size();
  kvs.clear();

  // 14.lease_map_
  kvs.reserve(meta_snapshot_file.lease_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.lease_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.lease_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(lease_map_mutex_);
    if (!lease_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(lease_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete lease_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range lease_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write lease_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put lease_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot version_lease_meta, count=" << kvs.size();
  kvs.clear();

  // 15.kv_index_map_
  kvs.reserve(meta_snapshot_file.kv_index_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.kv_index_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.kv_index_map_kvs(i));
  }
  {
    if (!kv_index_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(kv_index_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete kv_index_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range kv_index_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write kv_index_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put kv_index_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot version_kv_meta, count=" << kvs.size();
  kvs.clear();

  // 16.kv_rev_map_
  kvs.reserve(meta_snapshot_file.kv_rev_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.kv_rev_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.kv_rev_map_kvs(i));
  }
  {
    if (!kv_rev_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(kv_rev_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete kv_rev_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range kv_rev_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write kv_rev_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put kv_rev_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot version_kv_rev_meta, count=" << kvs.size();
  kvs.clear();

  // 50.table_index map
  kvs.reserve(meta_snapshot_file.table_index_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.table_index_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.table_index_map_kvs(i));
  }
  if (!table_index_meta_->Recover(kvs)) {
    return false;
  }

  // remove data in rocksdb
  if (!meta_writer_->DeletePrefix(table_index_meta_->internal_prefix)) {
    DINGO_LOG(ERROR) << "Coordinator delete table_index_meta_ range failed in LoadMetaFromSnapshotFile";
    return false;
  }
  DINGO_LOG(INFO) << "Coordinator delete range table_index_meta_ success in LoadMetaFromSnapshotFile";

  // write data to rocksdb
  if (!meta_writer_->Put(kvs)) {
    DINGO_LOG(ERROR) << "Coordinator write table_index_meta_ failed in LoadMetaFromSnapshotFile";
    return false;
  }
  DINGO_LOG(INFO) << "Coordinator put table_index_meta_ success in LoadMetaFromSnapshotFile";
  DINGO_LOG(INFO) << "LoadSnapshot table_index_meta, count=" << kvs.size();
  kvs.clear();

  // build id_epoch, schema_name, table_name, index_name maps
  BuildTempMaps();

  // build lease_to_key_map_temp_
  BuildLeaseToKeyMap();

  DINGO_LOG(INFO) << "LoadSnapshot lease_to_key_map_temp, count=" << lease_to_key_map_temp_.size();

  return true;
}

void LogMetaIncrementSize(pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (meta_increment.ByteSizeLong() > 0) {
    DINGO_LOG(DEBUG) << "meta_increment byte_size=" << meta_increment.ByteSizeLong();
  } else {
    return;
  }
  if (meta_increment.idepochs_size() > 0) {
    DINGO_LOG(DEBUG) << "0.idepochs_size=" << meta_increment.idepochs_size();
  }
  if (meta_increment.coordinators_size() > 0) {
    DINGO_LOG(DEBUG) << "1.coordinators_size=" << meta_increment.coordinators_size();
  }
  if (meta_increment.stores_size() > 0) {
    DINGO_LOG(DEBUG) << "2.stores_size=" << meta_increment.stores_size();
  }
  if (meta_increment.executors_size() > 0) {
    DINGO_LOG(DEBUG) << "3.executors_size=" << meta_increment.executors_size();
  }
  if (meta_increment.schemas_size() > 0) {
    DINGO_LOG(DEBUG) << "4.schemas_size=" << meta_increment.schemas_size();
  }
  if (meta_increment.regions_size() > 0) {
    DINGO_LOG(DEBUG) << "5.regions_size=" << meta_increment.regions_size();
  }
  if (meta_increment.deleted_regions_size() > 0) {
    DINGO_LOG(DEBUG) << "5.1 deleted_regions_size=" << meta_increment.deleted_regions_size();
  }
  if (meta_increment.tables_size() > 0) {
    DINGO_LOG(DEBUG) << "6.tables_size=" << meta_increment.tables_size();
  }
  if (meta_increment.store_metrics_size() > 0) {
    DINGO_LOG(DEBUG) << "7.store_metrics_size=" << meta_increment.store_metrics_size();
  }
  if (meta_increment.table_metrics_size() > 0) {
    DINGO_LOG(DEBUG) << "8.table_metrics_size=" << meta_increment.table_metrics_size();
  }
  if (meta_increment.store_operations_size() > 0) {
    DINGO_LOG(DEBUG) << "9.store_operations_size=" << meta_increment.store_operations_size();
  }
  if (meta_increment.executor_users_size() > 0) {
    DINGO_LOG(DEBUG) << "10.executor_users_size=" << meta_increment.executor_users_size();
  }
  if (meta_increment.task_lists_size() > 0) {
    DINGO_LOG(DEBUG) << "11.task_lists_size=" << meta_increment.task_lists_size();
  }
  if (meta_increment.indexes_size() > 0) {
    DINGO_LOG(DEBUG) << "12.indexes_size=" << meta_increment.indexes_size();
  }
  if (meta_increment.index_metrics_size() > 0) {
    DINGO_LOG(DEBUG) << "13.index_metrics_size=" << meta_increment.index_metrics_size();
  }
  if (meta_increment.leases_size() > 0) {
    DINGO_LOG(DEBUG) << "14.leases_size=" << meta_increment.leases_size();
  }
  if (meta_increment.kv_indexes_size() > 0) {
    DINGO_LOG(DEBUG) << "15.kv_indexes_size=" << meta_increment.kv_indexes_size();
  }
  if (meta_increment.kv_revs_size() > 0) {
    DINGO_LOG(DEBUG) << "16.kv_revs_size=" << meta_increment.kv_revs_size();
  }

  DINGO_LOG(DEBUG) << meta_increment.ShortDebugString();
}

// ApplyMetaIncrement is on_apply callback
void CoordinatorControl::ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement& meta_increment, bool /*is_leader*/,
                                            int64_t term, int64_t index, google::protobuf::Message* /*response*/) {
  // prepare data to write to kv engine
  std::vector<pb::common::KeyValue> meta_write_to_kv;
  std::vector<pb::common::KeyValue> meta_delete_to_kv;

  {
    // BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    // if index < local apply index, just return
    int64_t applied_index = 0;
    int64_t applied_term = 0;

    id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM, applied_term);
    id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX, applied_index);

    if (index <= applied_index && term <= applied_term) {
      DINGO_LOG(WARNING)
          << "SKIP ApplyMetaIncrement index <= applied_index && term <<= applied_term, just return, [index=" << index
          << "][applied_index=" << applied_index << "]"
          << "[term=" << term << "][applied_term=" << applied_term;
      return;
    } else if (meta_increment.ByteSizeLong() > 0) {
      DINGO_LOG(INFO) << "NORMAL ApplyMetaIncrement index <= applied_index && term <<= applied_term [index=" << index
                      << "][applied_index=" << applied_index << "]"
                      << "[term=" << term << "][applied_term=" << applied_term;
      LogMetaIncrementSize(meta_increment);
    } else {
      DINGO_LOG(WARNING) << "meta_increment.ByteSizeLong() == 0, just return";
      return;
    }

    if (meta_increment.idepochs_size() > 0) {
      DINGO_LOG(INFO) << "0.idepochs_size=" << meta_increment.idepochs_size();
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
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM, term);
    id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX, index);

    meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(raft_apply_term));
    meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(raft_apply_index));

    for (int i = 0; i < meta_increment.idepochs_size(); i++) {
      const auto& idepoch = meta_increment.idepochs(i);
      if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        int ret = id_epoch_map_.UpdatePresentId(idepoch.id(), idepoch.idepoch().value());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement idepoch CREATE, success [id=" << idepoch.id() << "]"
                          << " value=" << idepoch.idepoch().value();
          meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement idepoch CREATE, but UpdatePresentId failed, [id=" << idepoch.id()
                             << "]"
                             << " value=" << idepoch.idepoch().value();
        }
      } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = id_epoch_map_.UpdatePresentId(idepoch.id(), idepoch.idepoch().value());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement idepoch UPDATE, success [id=" << idepoch.id() << "]"
                          << " value=" << idepoch.idepoch().value();
          meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement idepoch UPDATE, but UpdatePresentId failed, [id=" << idepoch.id()
                             << "]"
                             << " value=" << idepoch.idepoch().value();
        }
      } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = id_epoch_map_.Erase(idepoch.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement idepoch DELETE, success [id=" << idepoch.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement idepoch DELETE, but Erase failed, [id=" << idepoch.id() << "]";
        }
        meta_delete_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));
      }
    }
  }

  // 1.coordinator map
  {
    if (meta_increment.coordinators_size() > 0) {
      DINGO_LOG(INFO) << "1.coordinators_size=" << meta_increment.coordinators_size();
    }

    // BAIDU_SCOPED_LOCK(coordinator_map_mutex_);
    for (int i = 0; i < meta_increment.coordinators_size(); i++) {
      const auto& coordinator = meta_increment.coordinators(i);
      if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // coordinator_map_[coordinator.id()] = coordinator.coordinator();
        int ret = coordinator_map_.Put(coordinator.id(), coordinator.coordinator());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement coordinator CREATE, success [id=" << coordinator.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement coordinator CREATE, but Put failed, [id=" << coordinator.id()
                             << "]";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(coordinator_meta_->TransformToKvValue(coordinator.coordinator()));

      } else if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = coordinator_map_.Put(coordinator.id(), coordinator.coordinator());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement coordinator UPDATE, success [id=" << coordinator.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement coordinator UPDATE, but Put failed, [id=" << coordinator.id()
                             << "]";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(coordinator_meta_->TransformToKvValue(coordinator.coordinator()));

      } else if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = coordinator_map_.Erase(coordinator.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement coordinator DELETE, success [id=" << coordinator.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement coordinator DELETE, but Erase failed, [id=" << coordinator.id()
                             << "]";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(coordinator_meta_->TransformToKvValue(coordinator.coordinator()));
      }
    }
  }

  // 2.store map
  {
    if (meta_increment.stores_size() > 0) {
      DINGO_LOG(INFO) << "2.stores_size=" << meta_increment.stores_size();
    }

    // BAIDU_SCOPED_LOCK(store_map_mutex_);
    for (int i = 0; i < meta_increment.stores_size(); i++) {
      const auto& store = meta_increment.stores(i);
      if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // store_map_[store.id()] = store.store();
        int ret = store_map_.Put(store.id(), store.store());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement store CREATE, success [id=" << store.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement store CREATE, but Put failed, [id=" << store.id() << "]";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(store_meta_->TransformToKvValue(store.store()));

      } else if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = store_map_.Put(store.id(), store.store());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement store UPDATE, success [id=" << store.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement store UPDATE, but Put failed, [id=" << store.id() << "]";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(store_meta_->TransformToKvValue(store.store()));

      } else if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = store_map_.Erase(store.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement store DELETE, success [id=" << store.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement store DELETE, but Erase failed, [id=" << store.id() << "]";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(store_meta_->TransformToKvValue(store.store()));
      }
    }
  }

  // 3.executor map
  {
    if (meta_increment.executors_size() > 0) {
      DINGO_LOG(INFO) << "3.executors_size=" << meta_increment.executors_size();
    }

    // BAIDU_SCOPED_LOCK(executor_map_mutex_);
    for (int i = 0; i < meta_increment.executors_size(); i++) {
      const auto& executor = meta_increment.executors(i);
      if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // executor_map_[executor.id()] = executor.executor();
        int ret = executor_map_.Put(executor.id(), executor.executor());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor CREATE, success [id=" << executor.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement executor CREATE, but Put failed, [id=" << executor.id() << "]";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(executor_meta_->TransformToKvValue(executor.executor()));

      } else if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = executor_map_.Put(executor.id(), executor.executor());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor UPDATE, success [id=" << executor.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement executor UPDATE, but Put failed, [id=" << executor.id() << "]";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(executor_meta_->TransformToKvValue(executor.executor()));

      } else if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = executor_map_.Erase(executor.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor DELETE, success [id=" << executor.id() << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement executor DELETE, but Erase failed, [id=" << executor.id() << "]";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(executor_meta_->TransformToKvValue(executor.executor()));
      }
    }
  }

  // 4.schema map
  {
    if (meta_increment.schemas_size() > 0) {
      DINGO_LOG(INFO) << "4.schemas_size=" << meta_increment.schemas_size();
    }

    // BAIDU_SCOPED_LOCK(schema_map_mutex_);
    for (int i = 0; i < meta_increment.schemas_size(); i++) {
      const auto& schema = meta_increment.schemas(i);
      if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // schema_map_[schema.id()] = schema.schema_internal();
        int ret = schema_map_.PutIfAbsent(schema.id(), schema.schema_internal());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement schema CREATE, [id=" << schema.id() << "] success";

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement schema CREATE, [id=" << schema.id() << "] failed";
        }
      } else if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = schema_map_.Put(schema.id(), schema.schema_internal());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement schema UPDATE, [id=" << schema.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement schema UPDATE, [id=" << schema.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));

      } else if (schema.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = schema_map_.Erase(schema.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement schema DELETE, [id=" << schema.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement schema DELETE, [id=" << schema.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(schema_meta_->TransformToKvValue(schema.schema_internal()));
      }
    }
  }

  // 5.region map
  {
    if (meta_increment.regions_size() > 0) {
      DINGO_LOG(INFO) << "5.regions_size=" << meta_increment.regions_size();
    }

    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    // for region_map_ multiput
    std::vector<int64_t> region_id_to_write;
    std::vector<pb::coordinator_internal::RegionInternal> region_internal_to_write;
    // for range_region_map_ multiput
    std::vector<std::string> region_start_key_to_write;
    std::vector<int64_t> region_start_key_id_to_write;

    for (int i = 0; i < meta_increment.regions_size(); i++) {
      const auto& region = meta_increment.regions(i);
      if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // add region to region_map
        // region_map_[region.id()] = region.region();
        // int ret = region_map_.Put(region.id(), region.region());
        // if (ret > 0) {
        //   DINGO_LOG(INFO) << "ApplyMetaIncrement region CREATE, [id=" << region.id() << "] success";
        // } else {
        //   DINGO_LOG(WARNING) << "ApplyMetaIncrement region CREATE, [id=" << region.id() << "] failed";
        // }
        region_id_to_write.push_back(region.id());
        region_internal_to_write.push_back(region.region());

        // update range_region_map_
        // range_region_map_.Put(region.region().definition().range().start_key(), region.region().id());
        /* region_start_key_to_write.push_back(region.region().definition().range().start_key()); */
        /* region_start_key_id_to_write.push_back(region.region().id()); */

        // meta_write_kv
        meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));

        // add_store_for_push
        // only create region will push to store now
        {
          BAIDU_SCOPED_LOCK(store_need_push_mutex_);
          for (int j = 0; j < region.region().definition().peers_size(); j++) {
            int64_t store_id = region.region().definition().peers(j).store_id();
            DINGO_LOG(INFO) << " add_store_for_push, peers_size=" << region.region().definition().peers_size()
                            << " store_id =" << store_id;

            if (store_need_push_.seek(store_id) == nullptr) {
              pb::common::Store store_to_push;
              int ret = store_map_.Get(store_id, store_to_push);
              if (ret > 0) {
                store_need_push_.insert(store_id, store_to_push);
                DINGO_LOG(INFO) << " add_store_for_push, store_id=" << store_id
                                << " in create region=" << region.region().id()
                                << " location=" << store_to_push.server_location().host() << ":"
                                << store_to_push.server_location().port();
              } else {
                DINGO_LOG(ERROR) << " add_store_for_push, illegal store_id=" << store_id
                                 << " in create region=" << region.region().id();
              }
            }
          }
        }

        // meta_write_kv
        meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));

        // update range_region_map_
        const auto& new_region_range = region.region().definition().range();
        if (new_region_range.start_key() < new_region_range.end_key()) {
          /* range_region_map_.Put(new_region_range.start_key(), region.region().id()); */
          region_start_key_to_write.push_back(new_region_range.start_key());
          region_start_key_id_to_write.push_back(region.region().id());
          DINGO_LOG(INFO) << "add range_region_map_ success, region_id=[" << region.region().id() << "], start_key=["
                          << Helper::StringToHex(region.region().definition().range().start_key()) << "]";
        } else {
          DINGO_LOG(INFO) << "add range_region_map_ skipped of start_key >= end_key, region_id=["
                          << region.region().id() << "], start_key=["
                          << Helper::StringToHex(region.region().definition().range().start_key()) << "], end_key=["
                          << Helper::StringToHex(region.region().definition().range().end_key()) << "]";
        }

        // update table/index if this is a split region create
        const auto& new_region = region.region();
        if (new_region.region_type() == pb::common::RegionType::STORE_REGION) {
          pb::coordinator_internal::TableInternal table_internal;

          int64_t table_id = new_region.definition().table_id();

          auto ret = table_map_.Get(table_id, table_internal);
          if (ret < 0) {
            DINGO_LOG(INFO) << "process RegionCreate in fsm table_id not exists, id=" << table_id
                            << ", region_id=" << new_region.id() << ",region=" << new_region.ShortDebugString();
          } else {
            DINGO_LOG(INFO) << "process RegionCreate in fsm table_id exists, id=" << table_id
                            << ", region_id=" << new_region.id() << ",region=" << new_region.ShortDebugString();

            bool need_to_add_part = true;
            for (const auto& part : table_internal.partitions()) {
              if (part.region_id() == new_region.id()) {
                need_to_add_part = false;
                break;
              }
            }

            if (need_to_add_part) {
              auto* new_part = table_internal.add_partitions();
              new_part->set_region_id(new_region.id());
              new_part->set_part_id(new_region.definition().part_id());

              // update table to table_map
              ret = table_map_.Put(table_id, table_internal);
              if (ret > 0) {
                DINGO_LOG(INFO) << "ApplyMetaIncrement table UPDATE, [id=" << table_id << "] success";
              } else {
                DINGO_LOG(WARNING) << "ApplyMetaIncrement table UPDATE, [id=" << table_id << "] failed";
              }

              // meta_write_kv
              meta_write_to_kv.push_back(table_meta_->TransformToKvValue(table_internal));
            }
          }
        } else if (new_region.region_type() == pb::common::RegionType::INDEX_REGION) {
          pb::coordinator_internal::TableInternal index_internal;

          int64_t index_id = new_region.definition().index_id();

          auto ret = index_map_.Get(index_id, index_internal);
          if (ret < 0) {
            DINGO_LOG(INFO) << "process RegionCreate in fsm index_id not exists, id=" << index_id
                            << ", region_id=" << new_region.id() << ",region=" << new_region.ShortDebugString();
          } else {
            DINGO_LOG(INFO) << "process RegionCreate in fsm index_id exists, id=" << index_id
                            << ", region_id=" << new_region.id() << ",region=" << new_region.ShortDebugString();

            bool need_to_add_part = true;
            for (const auto& part : index_internal.partitions()) {
              if (part.region_id() == new_region.id()) {
                need_to_add_part = false;
                break;
              }
            }

            if (need_to_add_part) {
              auto* new_part = index_internal.add_partitions();
              new_part->set_region_id(new_region.id());
              new_part->set_part_id(new_region.definition().part_id());

              // update kv_index to kv_index_map
              auto ret = index_map_.Put(index_id, index_internal);
              if (ret > 0) {
                DINGO_LOG(INFO) << "ApplyMetaIncrement index UPDATE, [id=" << index_id << "] success";
              } else {
                DINGO_LOG(WARNING) << "ApplyMetaIncrement index UPDATE, [id=" << index_id << "] failed";
              }

              // meta_write_kv
              meta_write_to_kv.push_back(index_meta_->TransformToKvValue(index_internal));
            }
          }
        }

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // get old region
        pb::coordinator_internal::RegionInternal old_region;
        auto ret = region_map_.Get(region.id(), old_region);
        if (ret > 0) {
          range_region_map_.Erase(old_region.definition().range().start_key());
        }
        // update range_region_map_
        const auto& new_region_range = region.region().definition().range();
        if (new_region_range.start_key().compare(new_region_range.end_key()) < 0) {
          /* range_region_map_.Put(new_region_range.start_key(), region.region().id()); */
          region_start_key_to_write.push_back(new_region_range.start_key());
          region_start_key_id_to_write.push_back(region.region().id());
          DINGO_LOG(INFO) << "update range_region_map_ success, region_id=[" << region.region().id() << "], start_key=["
                          << Helper::StringToHex(new_region_range.start_key()) << "], old_start_key=["
                          << Helper::StringToHex(old_region.definition().range().start_key()) << "]";
        } else {
          DINGO_LOG(INFO) << "update range_region_map_ skipped of start_key >= end_key, region_id=["
                          << region.region().id() << "], start_key=["
                          << Helper::StringToHex(region.region().definition().range().start_key()) << "], end_key=["
                          << Helper::StringToHex(region.region().definition().range().end_key()) << "], old_start_key=["
                          << Helper::StringToHex(old_region.definition().range().start_key()) << "], old_end_key=["
                          << Helper::StringToHex(old_region.definition().range().end_key()) << "]";
        }

        // update region to region_map
        // int ret = region_map_.PutIfExists(region.id(), region.region());
        // if (ret > 0) {
        //   DINGO_LOG(INFO) << "ApplyMetaIncrement region UPDATE, [id=" << region.id() << "] success";
        //   // meta_write_kv
        //   meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));
        // } else {
        //   DINGO_LOG(WARNING) << "ApplyMetaIncrement region UPDATE, [id=" << region.id() << "] failed";
        // }

        region_id_to_write.push_back(region.id());
        region_internal_to_write.push_back(region.region());

        // meta_write_kv
        meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // remove region from region_map
        int ret = region_map_.Erase(region.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region DELETE, [id=" << region.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement region DELETE, [id=" << region.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(region_meta_->TransformToKvValue(region.region()));

        // remove region from region_metrics_map
        region_metrics_map_.Erase(region.id());

        // update range_region_map_
        range_region_map_.Erase(region.region().definition().range().start_key());
        DINGO_LOG(INFO) << "erase range_region_map_ success, region_id=[" << region.region().id() << "], start_key=["
                        << Helper::StringToHex(region.region().definition().range().start_key()) << "]";

        // update table/index if this is a merge region delete
        const auto& new_region = region.region();
        if (new_region.region_type() == pb::common::RegionType::STORE_REGION) {
          pb::coordinator_internal::TableInternal table_internal;

          int64_t table_id = new_region.definition().table_id();

          ret = table_map_.Get(table_id, table_internal);
          if (ret < 0) {
            DINGO_LOG(INFO) << "process RegionDrop in fsm table_id not exists, id=" << table_id
                            << ", region_id=" << new_region.id() << ",region=" << new_region.ShortDebugString();
          } else {
            DINGO_LOG(INFO) << "process RegionDrop in fsm table_id exists, id=" << table_id
                            << ", region_id=" << new_region.id() << ",region=" << new_region.ShortDebugString();

            pb::coordinator_internal::TableInternal new_table_internal = table_internal;
            new_table_internal.clear_partitions();

            for (const auto& part : table_internal.partitions()) {
              if (part.region_id() != new_region.id()) {
                auto* new_part = new_table_internal.add_partitions();
                new_part->set_region_id(part.region_id());
                new_part->set_part_id(part.part_id());
              }
            }

            if (new_table_internal.partitions_size() != table_internal.partitions_size()) {
              // update table to table_map
              ret = table_map_.Put(table_id, table_internal);
              if (ret > 0) {
                DINGO_LOG(INFO) << "ApplyMetaIncrement table UPDATE, [id=" << table_id << "] success";
              } else {
                DINGO_LOG(WARNING) << "ApplyMetaIncrement table UPDATE, [id=" << table_id << "] failed";
              }

              // meta_write_kv
              meta_write_to_kv.push_back(table_meta_->TransformToKvValue(new_table_internal));
            }
          }
        } else if (new_region.region_type() == pb::common::RegionType::INDEX_REGION) {
          pb::coordinator_internal::TableInternal index_internal;

          int64_t index_id = new_region.definition().index_id();

          ret = index_map_.Get(index_id, index_internal);
          if (ret < 0) {
            DINGO_LOG(INFO) << "process RegionDrop in fsm index_id not exists, id=" << index_id
                            << ", region_id=" << new_region.id() << ",region=" << new_region.ShortDebugString();
          } else {
            DINGO_LOG(INFO) << "process RegionDrop in fsm index_id exists, id=" << index_id
                            << ", region_id=" << new_region.id() << ",region=" << new_region.ShortDebugString();

            pb::coordinator_internal::TableInternal new_index_internal = index_internal;
            new_index_internal.clear_partitions();

            for (const auto& part : index_internal.partitions()) {
              if (part.region_id() != new_region.id()) {
                auto* new_part = new_index_internal.add_partitions();
                new_part->set_region_id(part.region_id());
                new_part->set_part_id(part.part_id());
              }
            }

            if (new_index_internal.partitions_size() != index_internal.partitions_size()) {
              // update table to table_map
              ret = table_map_.Put(index_id, index_internal);
              if (ret > 0) {
                DINGO_LOG(INFO) << "ApplyMetaIncrement table UPDATE, [id=" << index_id << "] success";
              } else {
                DINGO_LOG(WARNING) << "ApplyMetaIncrement table UPDATE, [id=" << index_id << "] failed";
              }

              // meta_write_kv
              meta_write_to_kv.push_back(table_meta_->TransformToKvValue(new_index_internal));
            }
          }
        }
      }

      if (!region_id_to_write.empty()) {
        if (region_id_to_write.size() != region_internal_to_write.size()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement region_id_to_write.size() != region_internal_to_write.size(), size="
                           << region_id_to_write.size() << " " << region_internal_to_write.size();
        }

        auto ret = region_map_.MultiPut(region_id_to_write, region_internal_to_write);
        if (ret < 0) {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement region UPDATE, count=[" << region_id_to_write.size() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region UPDATE, count=[" << region_id_to_write.size() << "] success";
        }
      }

      if (!region_start_key_to_write.empty()) {
        if (region_start_key_to_write.size() != region_start_key_id_to_write.size()) {
          DINGO_LOG(FATAL)
              << "ApplyMetaIncrement region_start_key_id_to_write.size() != region_start_key_to_write.size(), size="
              << region_start_key_id_to_write.size() << " " << region_start_key_to_write.size();
        }

        auto ret = range_region_map_.MultiPut(region_start_key_to_write, region_start_key_id_to_write);
        if (ret < 0) {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement range_region UPDATE, count=[" << region_start_key_id_to_write.size()
                             << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement range_region UPDATE, count=[" << region_start_key_id_to_write.size()
                          << "] success";
        }
      }
    }
  }

  // 5.1 deleted region map
  {
    if (meta_increment.deleted_regions_size() > 0) {
      DINGO_LOG(INFO) << "5.1 deleted_regions_size=" << meta_increment.deleted_regions_size();
    }

    // BAIDU_SCOPED_LOCK(region_map_mutex_);
    for (int i = 0; i < meta_increment.deleted_regions_size(); i++) {
      const auto& region = meta_increment.deleted_regions(i);
      if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // add deleted_region to deleted_region_map
        int ret = deleted_region_map_.Put(region.id(), region.region());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_region CREATE, [id=" << region.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement deleted_region CREATE, [id=" << region.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(deleted_region_meta_->TransformToKvValue(region.region()));

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update region to deleted_region_map
        int ret = deleted_region_map_.PutIfExists(region.id(), region.region());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_region UPDATE, [id=" << region.id() << "] success";
          // meta_write_kv
          meta_write_to_kv.push_back(deleted_region_meta_->TransformToKvValue(region.region()));
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement deleted_region UPDATE, [id=" << region.id() << "] failed";
        }

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // remove region from deleted_region_map
        int ret = deleted_region_map_.Erase(region.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_region DELETE, [id=" << region.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement deleted_region DELETE, [id=" << region.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(deleted_region_meta_->TransformToKvValue(region.region()));
      }
    }
  }

  // 6.table map
  {
    if (meta_increment.tables_size() > 0) {
      DINGO_LOG(INFO) << "6.tables_size=" << meta_increment.tables_size();
    }

    // BAIDU_SCOPED_LOCK(table_map_mutex_);
    for (int i = 0; i < meta_increment.tables_size(); i++) {
      const auto& table = meta_increment.tables(i);
      if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // need to update schema, so acquire lock
        // BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // add table to table_map
        // table_map_[table.id()] = table.table();
        int ret = table_map_.Put(table.id(), table.table());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table CREATE, [id=" << table.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement table CREATE, [id=" << table.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));

        // add table to parent schema
        pb::coordinator_internal::SchemaInternal schema_to_update;
        ret = schema_map_.Get(table.table().schema_id(), schema_to_update);
        // auto* schema = schema_map_.seek(table.schema_id());
        if (ret > 0) {
          // add new created table's id to its parent schema's table_ids
          schema_to_update.add_table_ids(table.id());
          schema_map_.Put(table.table().schema_id(), schema_to_update);

          DINGO_LOG(INFO) << "5.table map CREATE new_sub_table id=" << table.id()
                          << " parent_id=" << table.table().schema_id();

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema_to_update));
        } else {
          DINGO_LOG(ERROR) << " CREATE TABLE apply illegal schema_id=" << table.table().schema_id()
                           << " table_id=" << table.id() << " table_name=" << table.table().definition().name();
        }
      } else if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update table to table_map
        pb::coordinator_internal::TableInternal table_internal;
        int ret = table_map_.Get(table.id(), table_internal);
        if (ret > 0) {
          if (table.table().has_definition()) {
            *(table_internal.mutable_definition()) = table.table().definition();
          }
          if (table.table().partitions_size() > 0) {
            table_internal.clear_partitions();
            for (const auto& it : table.table().partitions()) {
              *(table_internal.add_partitions()) = it;
            }
          }
          ret = table_map_.Put(table.id(), table_internal);
          if (ret > 0) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement table UPDATE, [id=" << table.id() << "] success";
          } else {
            DINGO_LOG(WARNING) << "ApplyMetaIncrement table UPDATE, [id=" << table.id() << "] failed";
          }

          // meta_write_kv
          meta_write_to_kv.push_back(table_meta_->TransformToKvValue(table_internal));
        } else {
          DINGO_LOG(ERROR) << " UPDATE TABLE apply illegal table_id=" << table.id()
                           << " table_name=" << table.table().definition().name();
        }

      } else if (table.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // need to update schema, so acquire lock
        // BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // delete table from table_map
        int ret = table_map_.Erase(table.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table DELETE, [id=" << table.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement table DELETE, [id=" << table.id() << "] failed";
        }

        // delete table_metrics
        table_metrics_map_.Erase(table.id());

        // delete from parent schema
        pb::coordinator_internal::SchemaInternal schema_to_update;
        ret = schema_map_.Get(table.table().schema_id(), schema_to_update);

        if (ret > 0) {
          // according to the doc, we must use CopyFrom for protobuf message data structure here
          pb::coordinator_internal::SchemaInternal new_schema;
          new_schema = schema_to_update;

          new_schema.clear_table_ids();

          // add left table_id to new_schema
          for (auto x : schema_to_update.table_ids()) {
            if (x != table.id()) {
              new_schema.add_table_ids(x);
            }
          }
          schema_to_update = new_schema;
          schema_map_.Put(table.table().schema_id(), schema_to_update);

          DINGO_LOG(INFO) << "5.table map DELETE new_sub_table id=" << table.id()
                          << " parent_id=" << table.table().schema_id();

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema_to_update));

        } else {
          DINGO_LOG(ERROR) << " DROP TABLE apply illegal schema_id=" << table.table().schema_id()
                           << " table_id=" << table.id() << " table_name=" << table.table().definition().name();
        }
        // meta_delete_kv
        meta_delete_to_kv.push_back(table_meta_->TransformToKvValue(table.table()));
      }
    }
  }

  // 6.1 deleted table map
  {
    if (meta_increment.deleted_tables_size() > 0) {
      DINGO_LOG(INFO) << "6.deleted_tables_size=" << meta_increment.deleted_tables_size();
    }

    // BAIDU_SCOPED_LOCK(deleted_table_map_mutex_);
    for (int i = 0; i < meta_increment.deleted_tables_size(); i++) {
      const auto& deleted_table = meta_increment.deleted_tables(i);
      if (deleted_table.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // need to update schema, so acquire lock
        // BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // add deleted_table to deleted_table_map
        // deleted_table_map_[deleted_table.id()] = deleted_table.deleted_table();
        int ret = deleted_table_map_.Put(deleted_table.id(), deleted_table.table());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_table CREATE, [id=" << deleted_table.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement deleted_table CREATE, [id=" << deleted_table.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(deleted_table_meta_->TransformToKvValue(deleted_table.table()));

      } else if (deleted_table.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update deleted_table to deleted_table_map

        int ret = deleted_table_map_.Put(deleted_table.id(), deleted_table.table());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_table UPDATE, [id=" << deleted_table.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement deleted_table UPDATE, [id=" << deleted_table.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(deleted_table_meta_->TransformToKvValue(deleted_table.table()));

      } else if (deleted_table.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // need to update schema, so acquire lock
        // BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // delete deleted_table from deleted_table_map
        int ret = deleted_table_map_.Erase(deleted_table.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_table DELETE, [id=" << deleted_table.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement deleted_table DELETE, [id=" << deleted_table.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(deleted_table_meta_->TransformToKvValue(deleted_table.table()));
      }
    }
  }

  // 7.store_metrics map
  // {
  //   if (meta_increment.store_metrics_size() > 0) {
  //     DINGO_LOG(INFO) << "ApplyMetaIncrement store_metrics size=" << meta_increment.store_metrics_size();
  //   }

  //   BAIDU_SCOPED_LOCK(store_metrics_map_mutex_);
  //   for (int i = 0; i < meta_increment.store_metrics_size(); i++) {
  //     const auto& store_metrics = meta_increment.store_metrics(i);
  //     if (store_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
  //       store_metrics_map_[store_metrics.id()] = store_metrics.store_metrics();
  //       DINGO_LOG(INFO) << "ApplyMetaIncrement store_metrics CREATE, [id=" << store_metrics.id() << "] success";

  //       // meta_write_kv
  //       meta_write_to_kv.push_back(store_metrics_meta_->TransformToKvValue(store_metrics.store_metrics()));

  //     } else if (store_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
  //       auto& update_store = store_metrics_map_[store_metrics.id()];
  //       if (!store_metrics.is_partial_region_metrics()) {
  //         update_store = store_metrics.store_metrics();
  //       } else {
  //         if (store_metrics.store_metrics().region_metrics_map_size() > 0) {
  //           for (const auto& it : store_metrics.store_metrics().region_metrics_map()) {
  //             auto region_metrics_to_update = update_store.mutable_region_metrics_map()->find(it.first);
  //             if (region_metrics_to_update != update_store.mutable_region_metrics_map()->end()) {
  //               region_metrics_to_update->second = it.second;
  //             } else {
  //               update_store.mutable_region_metrics_map()->insert(it);
  //             }
  //           }
  //         }
  //       }
  //       DINGO_LOG(INFO) << "ApplyMetaIncrement store_metrics UPDATE, [id=" << store_metrics.id() << "] success, "
  //                       << "region_metrics_map_size=" << store_metrics.store_metrics().region_metrics_map_size()
  //                       << ", is_partial_region_metrics=" << store_metrics.is_partial_region_metrics()
  //                       << ", store_id=" << store_metrics.id();

  //       // meta_write_kv
  //       meta_write_to_kv.push_back(store_metrics_meta_->TransformToKvValue(update_store));

  //     } else if (store_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
  //       store_metrics_map_.erase(store_metrics.id());
  //       DINGO_LOG(INFO) << "ApplyMetaIncrement store_metrics DELETE, [id=" << store_metrics.id() << "] success";

  //       // meta_delete_kv
  //       meta_delete_to_kv.push_back(store_metrics_meta_->TransformToKvValue(store_metrics.store_metrics()));
  //     }
  //   }
  // }

  // 8.table_metrics map
  {
    if (meta_increment.table_metrics_size() > 0) {
      DINGO_LOG(INFO) << "ApplyMetaIncrement table_metrics size=" << meta_increment.table_metrics_size();
    }

    // BAIDU_SCOPED_LOCK(table_metrics_map_mutex_);
    for (int i = 0; i < meta_increment.table_metrics_size(); i++) {
      const auto& table_metrics = meta_increment.table_metrics(i);
      if (table_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // table_metrics_map_[table_metrics.id()] = table_metrics.table_metrics();
        int ret = table_metrics_map_.Put(table_metrics.id(), table_metrics.table_metrics());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table_metrics CREATE, [id=" << table_metrics.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement table_metrics CREATE, [id=" << table_metrics.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(table_metrics_meta_->TransformToKvValue(table_metrics.table_metrics()));

      } else if (table_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // auto& update_table = table_metrics_map_[table_metrics.id()];
        int ret = table_metrics_map_.Put(table_metrics.id(), table_metrics.table_metrics());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table_metrics UPDATE, [id=" << table_metrics.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement table_metrics UPDATE, [id=" << table_metrics.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(table_metrics_meta_->TransformToKvValue(table_metrics.table_metrics()));

      } else if (table_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = table_metrics_map_.Erase(table_metrics.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table_metrics DELETE, [id=" << table_metrics.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement table_metrics DELETE, [id=" << table_metrics.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(table_metrics_meta_->TransformToKvValue(table_metrics.table_metrics()));
      }
    }
  }

  // 9.store_operation map
  // only on_apply will really write store_operation_map_, so we don't need to lock it
  // store_operation only support CREATE and DELETE
  {
    if (meta_increment.store_operations_size() > 0) {
      DINGO_LOG(INFO) << "store_operation increment size=" << meta_increment.store_operations_size();
    }

    for (int i = 0; i < meta_increment.store_operations_size(); i++) {
      const auto& store_operation = meta_increment.store_operations(i);
      if (store_operation.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        pb::coordinator_internal::StoreOperationInternal store_operation_in_map;
        store_operation_in_map.set_id(store_operation.id());
        store_operation_map_.Get(store_operation_in_map.id(), store_operation_in_map);

        for (const auto& region_cmd_id : store_operation.store_operation().region_cmd_ids()) {
          store_operation_in_map.add_region_cmd_ids(region_cmd_id);
          DINGO_LOG(INFO) << "add a region_cmd to store_operation, store_id=" << store_operation.id()
                          << ", region_cmd_id=" << region_cmd_id;
        }
        int ret = store_operation_map_.Put(store_operation.id(), store_operation_in_map);
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement store_operation CREATE, [id=" << store_operation.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement store_operation CREATE, [id=" << store_operation.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(store_operation_meta_->TransformToKvValue(store_operation_in_map));

        DINGO_LOG(INFO) << "store_operation_map_ CREATE, store_operation=" << store_operation.ShortDebugString();

      } else if (store_operation.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // store_operation_map_.Put(store_operation.id(), store_operation.store_operation());

        // // meta_write_kv
        // meta_write_to_kv.push_back(store_operation_meta_->TransformToKvValue(store_operation.store_operation()));
        DINGO_LOG(ERROR) << "store_operation_map_ UPDATE not support, store_operation="
                         << store_operation.ShortDebugString();

      } else if (store_operation.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        pb::coordinator_internal::StoreOperationInternal store_operation_in_map;
        store_operation_in_map.set_id(store_operation.id());
        store_operation_map_.Get(store_operation_in_map.id(), store_operation_in_map);

        // delete region_cmd by id
        pb::coordinator_internal::StoreOperationInternal store_operation_residual;
        store_operation_residual.set_id(store_operation.id());

        for (int i = 0; i < store_operation_in_map.region_cmd_ids_size(); i++) {
          bool is_delete = false;
          for (const auto& region_cmd_id : store_operation.store_operation().region_cmd_ids()) {
            if (store_operation_in_map.region_cmd_ids(i) == region_cmd_id) {
              is_delete = true;

              DINGO_LOG(INFO) << "delete a region_cmd from store_operation, store_id=" << store_operation.id()
                              << ", region_cmd_id=" << region_cmd_id;
              break;
            }
          }
          if (!is_delete) {
            store_operation_residual.add_region_cmd_ids(store_operation_in_map.region_cmd_ids(i));
          }
        }

        int ret = store_operation_map_.Put(store_operation.id(), store_operation_residual);
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement store_operation DELETE, [id=" << store_operation.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement store_operation DELETE, [id=" << store_operation.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(store_operation_meta_->TransformToKvValue(store_operation_residual));

        DINGO_LOG(INFO) << "store_operation_map_.Put in DELETE, store_id=" << store_operation.id()
                        << " region_cmd count change [" << store_operation_in_map.region_cmd_ids_size() << ", "
                        << store_operation_residual.region_cmd_ids_size() << "]  orig_store_operation=["
                        << store_operation.ShortDebugString() << "] new_store_operation=["
                        << store_operation_residual.ShortDebugString() << "]";
      }
    }
  }

  // 9.1.region_cmd map
  {
    if (meta_increment.region_cmds_size() > 0) {
      DINGO_LOG(INFO) << "ApplyMetaIncrement region_cmd size=" << meta_increment.region_cmds_size();
    }

    // BAIDU_SCOPED_LOCK(region_cmd_map_mutex_);
    for (int i = 0; i < meta_increment.region_cmds_size(); i++) {
      const auto& region_cmd = meta_increment.region_cmds(i);
      if (region_cmd.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // region_cmd_map_[region_cmd.id()] = region_cmd.region_cmd();
        int ret = region_cmd_map_.Put(region_cmd.id(), region_cmd.region_cmd());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region_cmd CREATE, [id=" << region_cmd.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement region_cmd CREATE, [id=" << region_cmd.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(region_cmd_meta_->TransformToKvValue(region_cmd.region_cmd()));

      } else if (region_cmd.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // auto& update_table = region_cmd_map_[region_cmd.id()];
        int ret = region_cmd_map_.PutIfExists(region_cmd.id(), region_cmd.region_cmd());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region_cmd UPDATE, [id=" << region_cmd.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement region_cmd UPDATE, [id=" << region_cmd.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(region_cmd_meta_->TransformToKvValue(region_cmd.region_cmd()));

      } else if (region_cmd.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = region_cmd_map_.Erase(region_cmd.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region_cmd DELETE, [id=" << region_cmd.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement region_cmd DELETE, [id=" << region_cmd.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(region_cmd_meta_->TransformToKvValue(region_cmd.region_cmd()));
      }
    }
  }

  // 10.executor_user_map
  {
    if (meta_increment.executor_users_size() > 0) {
      DINGO_LOG(INFO) << "executor_user_map increment size=" << meta_increment.executor_users_size();
    }

    for (int i = 0; i < meta_increment.executor_users_size(); i++) {
      const auto& executor_user = meta_increment.executor_users(i);
      if (executor_user.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        int ret = executor_user_map_.Put(executor_user.id(), executor_user.executor_user());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor_user CREATE, [id=" << executor_user.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement executor_user CREATE, [id=" << executor_user.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(executor_user_meta_->TransformToKvValue(executor_user.executor_user()));

      } else if (executor_user.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = executor_user_map_.Put(executor_user.id(), executor_user.executor_user());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor_user UPDATE, [id=" << executor_user.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement executor_user UPDATE, [id=" << executor_user.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(executor_user_meta_->TransformToKvValue(executor_user.executor_user()));

      } else if (executor_user.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = executor_user_map_.Erase(executor_user.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor_user DELETE, [id=" << executor_user.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement executor_user DELETE, [id=" << executor_user.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(executor_user_meta_->TransformToKvValue(executor_user.executor_user()));
      }
    }
  }

  // 11.task_list_map
  {
    if (meta_increment.task_lists_size() > 0) {
      DINGO_LOG(INFO) << "task_list_map increment size=" << meta_increment.task_lists_size();
    }

    for (int i = 0; i < meta_increment.task_lists_size(); i++) {
      const auto& task_list = meta_increment.task_lists(i);
      if (task_list.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        int ret = task_list_map_.Put(task_list.id(), task_list.task_list());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement task_list CREATE, [id=" << task_list.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement task_list CREATE, [id=" << task_list.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(task_list_meta_->TransformToKvValue(task_list.task_list()));

      } else if (task_list.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = task_list_map_.Put(task_list.id(), task_list.task_list());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement task_list UPDATE, [id=" << task_list.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement task_list UPDATE, [id=" << task_list.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(task_list_meta_->TransformToKvValue(task_list.task_list()));

      } else if (task_list.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = task_list_map_.Erase(task_list.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement task_list DELETE, [id=" << task_list.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement task_list DELETE, [id=" << task_list.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(task_list_meta_->TransformToKvValue(task_list.task_list()));
      }
    }
  }

  // 12.index map
  {
    if (meta_increment.indexes_size() > 0) {
      DINGO_LOG(INFO) << "12.indexes_size=" << meta_increment.indexes_size();
    }

    // BAIDU_SCOPED_LOCK(index_map_mutex_);
    for (int i = 0; i < meta_increment.indexes_size(); i++) {
      const auto& index = meta_increment.indexes(i);
      if (index.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // need to update schema, so acquire lock
        // BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // add index to index_map
        // index_map_[index.id()] = index.index();
        int ret = index_map_.Put(index.id(), index.table());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement index CREATE, [id=" << index.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement index CREATE, [id=" << index.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(index_meta_->TransformToKvValue(index.table()));

        // add index to parent schema
        pb::coordinator_internal::SchemaInternal schema_to_update;
        ret = schema_map_.Get(index.table().schema_id(), schema_to_update);
        // auto* schema = schema_map_.seek(index.schema_id());
        if (ret > 0) {
          // add new created index's id to its parent schema's index_ids
          schema_to_update.add_index_ids(index.id());
          schema_map_.Put(index.table().schema_id(), schema_to_update);

          DINGO_LOG(INFO) << "5.index map CREATE new_sub_index id=" << index.id()
                          << " parent_id=" << index.table().schema_id();

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema_to_update));
        } else {
          DINGO_LOG(ERROR) << " CREATE INDEX apply illegal schema_id=" << index.table().schema_id()
                           << " index_id=" << index.id() << " index_name=" << index.table().definition().name();
        }
      } else if (index.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update index to index_map
        pb::coordinator_internal::TableInternal table_internal;
        int ret = index_map_.Get(index.id(), table_internal);
        if (ret > 0) {
          if (index.table().has_definition()) {
            *(table_internal.mutable_definition()) = index.table().definition();
          }
          if (index.table().partitions_size() > 0) {
            table_internal.clear_partitions();
            for (const auto& it : index.table().partitions()) {
              *(table_internal.add_partitions()) = it;
            }
          }
          ret = index_map_.Put(index.id(), table_internal);
          if (ret > 0) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement index UPDATE, [id=" << index.id() << "] success";
          } else {
            DINGO_LOG(WARNING) << "ApplyMetaIncrement index UPDATE, [id=" << index.id() << "] failed";
          }

          // meta_write_kv
          meta_write_to_kv.push_back(index_meta_->TransformToKvValue(table_internal));
        } else {
          DINGO_LOG(ERROR) << " UPDATE INDEX apply illegal index_id=" << index.id()
                           << " index_name=" << index.table().definition().name();
        }

      } else if (index.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // need to update schema, so acquire lock
        // BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // delete index from index_map
        int ret = index_map_.Erase(index.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement index DELETE, [id=" << index.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement index DELETE, [id=" << index.id() << "] failed";
        }

        // delete index_metrics
        index_metrics_map_.Erase(index.id());

        // delete from parent schema
        pb::coordinator_internal::SchemaInternal schema_to_update;
        ret = schema_map_.Get(index.table().schema_id(), schema_to_update);

        if (ret > 0) {
          // according to the doc, we must use CopyFrom for protobuf message data structure here
          pb::coordinator_internal::SchemaInternal new_schema;
          new_schema = schema_to_update;

          new_schema.clear_index_ids();

          // add left index_id to new_schema
          for (auto x : schema_to_update.index_ids()) {
            if (x != index.id()) {
              new_schema.add_index_ids(x);
            }
          }
          schema_to_update = new_schema;
          schema_map_.Put(index.table().schema_id(), schema_to_update);

          DINGO_LOG(INFO) << "5.index map DELETE new_sub_index id=" << index.id()
                          << " parent_id=" << index.table().schema_id();

          // meta_write_kv
          meta_write_to_kv.push_back(schema_meta_->TransformToKvValue(schema_to_update));

        } else {
          DINGO_LOG(ERROR) << " DROP INDEX apply illegal schema_id=" << index.table().schema_id()
                           << " index_id=" << index.id() << " index_name=" << index.table().definition().name();
        }
        // meta_delete_kv
        meta_delete_to_kv.push_back(index_meta_->TransformToKvValue(index.table()));
      }
    }
  }

  // 12.1 deleted index map
  {
    if (meta_increment.deleted_indexes_size() > 0) {
      DINGO_LOG(INFO) << "6.deleted_indexes_size=" << meta_increment.deleted_indexes_size();
    }

    // BAIDU_SCOPED_LOCK(deleted_index_map_mutex_);
    for (int i = 0; i < meta_increment.deleted_indexes_size(); i++) {
      const auto& deleted_index = meta_increment.deleted_indexes(i);
      if (deleted_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // need to update schema, so acquire lock
        // BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // add deleted_index to deleted_index_map
        int ret = deleted_index_map_.Put(deleted_index.id(), deleted_index.table());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_index CREATE, [id=" << deleted_index.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement deleted_index CREATE, [id=" << deleted_index.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(deleted_index_meta_->TransformToKvValue(deleted_index.table()));

      } else if (deleted_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update deleted_index to deleted_index_map

        int ret = deleted_index_map_.Put(deleted_index.id(), deleted_index.table());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_index UPDATE, [id=" << deleted_index.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement deleted_index UPDATE, [id=" << deleted_index.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(deleted_index_meta_->TransformToKvValue(deleted_index.table()));

      } else if (deleted_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // need to update schema, so acquire lock
        // BAIDU_SCOPED_LOCK(schema_map_mutex_);

        // delete deleted_index from deleted_index_map
        int ret = deleted_index_map_.Erase(deleted_index.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_index DELETE, [id=" << deleted_index.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement deleted_index DELETE, [id=" << deleted_index.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(deleted_index_meta_->TransformToKvValue(deleted_index.table()));
      }
    }
  }

  // 13.index_metrics map
  {
    if (meta_increment.index_metrics_size() > 0) {
      DINGO_LOG(INFO) << "ApplyMetaIncrement index_metrics size=" << meta_increment.index_metrics_size();
    }

    // BAIDU_SCOPED_LOCK(index_metrics_map_mutex_);
    for (int i = 0; i < meta_increment.index_metrics_size(); i++) {
      const auto& index_metrics = meta_increment.index_metrics(i);
      if (index_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // index_metrics_map_[index_metrics.id()] = index_metrics.index_metrics();
        int ret = index_metrics_map_.Put(index_metrics.id(), index_metrics.index_metrics());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement index_metrics CREATE, [id=" << index_metrics.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement index_metrics CREATE, [id=" << index_metrics.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(index_metrics_meta_->TransformToKvValue(index_metrics.index_metrics()));

      } else if (index_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = index_metrics_map_.Put(index_metrics.id(), index_metrics.index_metrics());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement index_metrics UPDATE, [id=" << index_metrics.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement index_metrics UPDATE, [id=" << index_metrics.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(index_metrics_meta_->TransformToKvValue(index_metrics.index_metrics()));

      } else if (index_metrics.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = index_metrics_map_.Erase(index_metrics.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement index_metrics DELETE, [id=" << index_metrics.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement index_metrics DELETE, [id=" << index_metrics.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(index_metrics_meta_->TransformToKvValue(index_metrics.index_metrics()));
      }
    }
  }

  // 14.lease_map_
  {
    if (meta_increment.leases_size() > 0) {
      DINGO_LOG(INFO) << "ApplyMetaIncrement version_leases size=" << meta_increment.leases_size();
    }

    // BAIDU_SCOPED_LOCK(lease_map_mutex_);
    for (int i = 0; i < meta_increment.leases_size(); i++) {
      const auto& version_lease = meta_increment.leases(i);
      if (version_lease.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // lease_map_[version_lease.id()] = version_lease.version_lease();
        int ret = lease_map_.Put(version_lease.id(), version_lease.lease());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement version_lease CREATE, [id=" << version_lease.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement version_lease CREATE, [id=" << version_lease.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(lease_meta_->TransformToKvValue(version_lease.lease()));

      } else if (version_lease.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = lease_map_.Put(version_lease.id(), version_lease.lease());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement version_lease UPDATE, [id=" << version_lease.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement version_lease UPDATE, [id=" << version_lease.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(lease_meta_->TransformToKvValue(version_lease.lease()));

      } else if (version_lease.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = lease_map_.Erase(version_lease.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement version_lease DELETE, [id=" << version_lease.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement version_lease DELETE, [id=" << version_lease.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(lease_meta_->TransformToKvValue(version_lease.lease()));
      }
    }
  }

  // 15.kv_index_map_
  {
    if (meta_increment.kv_indexes_size() > 0) {
      DINGO_LOG(INFO) << "ApplyMetaIncrement kv_indexes size=" << meta_increment.kv_indexes_size();
    }
    for (int i = 0; i < meta_increment.kv_indexes_size(); i++) {
      const auto& kv_index = meta_increment.kv_indexes(i);
      if (kv_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
      } else if (kv_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        if (kv_index.event_type() == pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_PUT) {
          // call KvPutApply
          auto ret = KvPutApply(kv_index.id(), kv_index.op_revision(), kv_index.ignore_lease(), kv_index.lease_id(),
                                kv_index.ignore_value(), kv_index.value());
          if (ret.ok()) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement kv_index UPDATE,PUT [id=" << kv_index.id() << "] success";
          } else {
            DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_index UPDATE,PUT [id=" << kv_index.id() << "] failed";
          }
        } else if (kv_index.event_type() == pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_DELETE) {
          // call KvDeleteApply
          auto ret = KvDeleteApply(kv_index.id(), kv_index.op_revision());
          if (ret.ok()) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement kv_index UPDATE,DELETE [id=" << kv_index.id() << "] success";
          } else {
            DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_index UPDATE,DELETE [id=" << kv_index.id() << "] failed";
          }
        } else if (kv_index.event_type() ==
                   pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_COMPACTION) {
          // call KvCompactApply
          auto ret = KvCompactApply(kv_index.id(), kv_index.op_revision());
          if (ret.ok()) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement kv_index UPDATE,COMPACT [id=" << kv_index.id() << "] success";
          } else {
            DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_index UPDATE,COMPACT [id=" << kv_index.id() << "] failed";
          }
        }
      } else if (kv_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = kv_index_map_.Erase(kv_index.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement kv_index DELETE, [id=" << kv_index.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_index DELETE, [id=" << kv_index.id() << "] failed";
        }

        // meta_delete_kv
        pb::coordinator_internal::KvIndexInternal kv_index_internal_to_delete;
        kv_index_internal_to_delete.set_id(kv_index.id());
        meta_delete_to_kv.push_back(kv_index_meta_->TransformToKvValue(kv_index_internal_to_delete));
      }
    }
  }

  // 16.kv_rev_map_
  {
    if (meta_increment.kv_revs_size() > 0) {
      DINGO_LOG(INFO) << "ApplyMetaIncrement kv_revs size=" << meta_increment.kv_revs_size();
    }

    // BAIDU_SCOPED_LOCK(kv_rev_map_mutex_);
    for (int i = 0; i < meta_increment.kv_revs_size(); i++) {
      const auto& kv_rev = meta_increment.kv_revs(i);
      if (kv_rev.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // kv_rev_map_[kv_rev.id()] = kv_rev.kv_rev();
        int ret = kv_rev_map_.PutIfAbsent(kv_rev.id(), kv_rev.kv_rev());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement kv_rev CREATE, [id=" << kv_rev.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_rev CREATE, [id=" << kv_rev.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(kv_rev_meta_->TransformToKvValue(kv_rev.kv_rev()));

      } else if (kv_rev.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = kv_rev_map_.PutIfExists(kv_rev.id(), kv_rev.kv_rev());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement kv_rev UPDATE, [id=" << kv_rev.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_rev UPDATE, [id=" << kv_rev.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(kv_rev_meta_->TransformToKvValue(kv_rev.kv_rev()));

      } else if (kv_rev.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = kv_rev_map_.Erase(kv_rev.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement kv_rev DELETE, [id=" << kv_rev.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_rev DELETE, [id=" << kv_rev.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(kv_rev_meta_->TransformToKvValue(kv_rev.kv_rev()));
      }
    }
  }

  // 50.table_index map
  {
    if (meta_increment.table_indexes_size() > 0) {
      DINGO_LOG(INFO) << "ApplyMetaIncrement table_indexes size=" << meta_increment.table_indexes_size();
    }

    for (int i = 0; i < meta_increment.table_indexes_size(); i++) {
      const auto& table_index = meta_increment.table_indexes(i);
      if (table_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        int ret = table_index_map_.Put(table_index.id(), table_index.table_indexes());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table_index CREATE, [id=" << table_index.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement table_index CREATE, [id=" << table_index.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(table_index_meta_->TransformToKvValue(table_index.table_indexes()));

      } else if (table_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = table_index_map_.Put(table_index.id(), table_index.table_indexes());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table_index UPDATE, [id=" << table_index.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement table_index UPDATE, [id=" << table_index.id() << "] failed";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(table_index_meta_->TransformToKvValue(table_index.table_indexes()));

      } else if (table_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = table_index_map_.Erase(table_index.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table_index DELETE, [id=" << table_index.id() << "] success";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement table_index DELETE, [id=" << table_index.id() << "] failed";
        }

        // meta_delete_kv
        meta_delete_to_kv.push_back(table_index_meta_->TransformToKvValue(table_index.table_indexes()));
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

// SubmitMetaIncrement
// commit meta increment to raft meta engine, with no closure
butil::Status CoordinatorControl::SubmitMetaIncrementAsync(google::protobuf::Closure* done,
                                                           pb::coordinator_internal::MetaIncrement& meta_increment) {
  LogMetaIncrementSize(meta_increment);

  std::shared_ptr<Context> const ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kCoordinatorRegionId);

  if (done != nullptr) {
    ctx->SetDone(done);
  }

  auto status = engine_->AsyncWrite(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "SubmitMetaIncrement failed, errno=" << status.error_code() << " errmsg=" << status.error_str();
    return status;
  }
  return butil::Status::OK();
}

static void SubmitMetaIncrementDone(BthreadCond* cond) { cond->DecreaseSignal(); }

butil::Status CoordinatorControl::SubmitMetaIncrementSync(pb::coordinator_internal::MetaIncrement& meta_increment) {
  BthreadCond cond(1);
  auto* closure = brpc::NewCallback(SubmitMetaIncrementDone, &cond);

  auto ret = SubmitMetaIncrementAsync(closure, meta_increment);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "SubmitMetaIncrementSync failed, errno=" << ret.error_code() << " errmsg=" << ret.error_str();
    return ret;
  }

  cond.Wait();

  return butil::Status::OK();
}

}  // namespace dingodb

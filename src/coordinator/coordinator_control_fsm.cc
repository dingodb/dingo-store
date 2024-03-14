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

#include <bitset>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coordinator/coordinator_control.h"
#include "engine/snapshot.h"
#include "gflags/gflags.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

DEFINE_int64(meta_revision_base, 0,
             "meta_revision base value, the real revision is meta_revision_base + applied_index");

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
    for (const auto& [schema_id, schema_internal] : schema_map_copy) {
      auto new_check_name = Helper::GenNewTenantCheckName(schema_internal.tenant_id(), schema_internal.name());
      schema_name_map_safe_temp_.Put(new_check_name, schema_id);
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
    butil::FlatMap<int64_t, pb::coordinator_internal::RegionInternal> region_map_copy;
    region_map_copy.init(10000);
    region_map_.GetRawMapCopy(region_map_copy);
    for (const auto& it : region_map_copy) {
      range_region_map_.Put(it.second.definition().range().start_key(), it.second);
    }
  }
}

// OnLeaderStart will init id_epoch_map_temp_ from id_epoch_map_ which is in state machine
void CoordinatorControl::OnLeaderStart(int64_t term) {
  // build id_epoch, schema_name, table_name, index_name maps
  BuildTempMaps();

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

  // clear all region_metrcs on follower
  region_metrics_map_.Clear();

  // clear all store_metrics on follower
  DeleteStoreRegionMetrics(0);

  // clean meta watchers
  auto meta_watch_clean_task = std::make_shared<MetaWatchCleanTask>(this, 0);
  auto ret3 = meta_watch_worker_set_->ExecuteRR(meta_watch_clean_task);
  if (!ret3) {
    DINGO_LOG(ERROR)
        << "OnLeaderStop will clean meta watchers. ApplyMetaIncrement meta_watch_clean_task ExecuteRR failed";
  } else {
    DINGO_LOG(INFO)
        << "OnLeaderStop will clean meta watchers. ApplyMetaIncrement meta_watch_clean_task ExecuteRR success";
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

  // 8.table_metrics map

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

  // 51.1 common_disk_map
  if (!meta_reader_->Scan(snapshot, common_disk_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_common_disk_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot common_disk_meta, count=" << kvs.size();
  kvs.clear();

  // 51.2 common_mem_map
  if (!meta_reader_->Scan(snapshot, common_mem_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_common_mem_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot common_mem_meta, count=" << kvs.size();
  kvs.clear();

  // 52 tenant_mem_map
  if (!meta_reader_->Scan(snapshot, tenant_meta_->Prefix(), kvs)) {
    return false;
  }

  for (const auto& kv : kvs) {
    auto* snapshot_file_kv = meta_snapshot_file.add_tenant_map_kvs();
    *snapshot_file_kv = kv;
  }
  DINGO_LOG(INFO) << "Snapshot tenant_mem_meta, count=" << kvs.size();
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
    // if (!deleted_region_meta_->Recover(kvs)) {
    //   return false;
    // }

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
    // if (!deleted_table_meta_->Recover(kvs)) {
    //   return false;
    // }

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

  // 8.table_metrics map

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
    // if (!deleted_index_meta_->Recover(kvs)) {
    //   return false;
    // }

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

  // 51.1 common_disk_map
  kvs.reserve(meta_snapshot_file.common_disk_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.common_disk_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.common_disk_map_kvs(i));
  }
  {
    // if (!common_disk_meta_->Recover(kvs)) {
    //   return false;
    // }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(common_disk_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete common_disk_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range common_disk_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write common_disk_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put common_disk_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot common_disk_meta, count=" << kvs.size();
  kvs.clear();

  // 51.2 common_mem_map
  kvs.reserve(meta_snapshot_file.common_mem_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.common_mem_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.common_mem_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(common_mem_map_mutex_);
    if (!common_mem_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(common_mem_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete common_mem_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range common_mem_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write common_mem_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put common_mem_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot common_mem_meta, count=" << kvs.size();
  kvs.clear();

  // 52 tenant_mem_map
  kvs.reserve(meta_snapshot_file.tenant_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.tenant_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.tenant_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(tenant_mem_map_mutex_);
    if (!tenant_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(tenant_meta_->internal_prefix)) {
      DINGO_LOG(ERROR) << "Coordinator delete tenant_mem_meta_ range failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator delete range tenant_mem_meta_ success in LoadMetaFromSnapshotFile";

    // write data to rocksdb
    if (!meta_writer_->Put(kvs)) {
      DINGO_LOG(ERROR) << "Coordinator write tenant_mem_meta_ failed in LoadMetaFromSnapshotFile";
      return false;
    }
    DINGO_LOG(INFO) << "Coordinator put tenant_mem_meta_ success in LoadMetaFromSnapshotFile";
  }
  DINGO_LOG(INFO) << "LoadSnapshot tenant_mem_meta, count=" << kvs.size();
  kvs.clear();

  // build id_epoch, schema_name, table_name, index_name maps
  BuildTempMaps();

  // build lease_to_key_map_temp_
  // BuildLeaseToKeyMap();

  // DINGO_LOG(INFO) << "LoadSnapshot lease_to_key_map_temp, count=" << lease_to_key_map_temp_.size();

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
  std::vector<std::string> meta_delete_to_kv;

  // if index < local apply index, just return
  int64_t applied_index = 0;
  int64_t applied_term = 0;

  id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM, applied_term);
  id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX, applied_index);

  if (index <= applied_index && term <= applied_term) {
    DINGO_LOG(WARNING)
        << "SKIP ApplyMetaIncrement index <= applied_index && term <<= applied_term, just return, [index=" << index
        << "][applied_index=" << applied_index << "]"
        << "[term=" << term << "][applied_term=" << applied_term << "]";
    return;
  } else if (meta_increment.ByteSizeLong() > 0) {
    DINGO_LOG(INFO) << "NORMAL ApplyMetaIncrement [index=" << index << "][applied_index=" << applied_index << "]"
                    << "[term=" << term << "][applied_term=" << applied_term << "]";
    LogMetaIncrementSize(meta_increment);
  } else {
    DINGO_LOG(WARNING) << "meta_increment.ByteSizeLong() == 0, just return";
    return;
  }

  if (meta_increment.idepochs_size() > 0) {
    DINGO_LOG(INFO) << "0.idepochs_size=" << meta_increment.idepochs_size();
  }

  // raft_apply_term & raft_apply_index stores in id_epoch_map too
  pb::coordinator_internal::IdEpochInternal raft_apply_term;
  raft_apply_term.set_id(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM);
  raft_apply_term.set_value(term);

  pb::coordinator_internal::IdEpochInternal raft_apply_index;
  raft_apply_index.set_id(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX);
  raft_apply_index.set_value(index);

  // 0.id & epoch
  {
    // BAIDU_SCOPED_LOCK(id_epoch_map_mutex_);
    for (int i = 0; i < meta_increment.idepochs_size(); i++) {
      const auto& idepoch = meta_increment.idepochs(i);
      if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        int ret = id_epoch_map_.UpdatePresentId(idepoch.id(), idepoch.idepoch().value());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement idepoch CREATE, success [id="
                          << pb::coordinator_internal::IdEpochType_Name(idepoch.id()) << "]"
                          << " value=" << idepoch.idepoch().value();
          meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement idepoch CREATE, but UpdatePresentId failed, [id="
                             << pb::coordinator_internal::IdEpochType_Name(idepoch.id()) << "]"
                             << " value=" << idepoch.idepoch().value();
        }
      } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        int ret = id_epoch_map_.UpdatePresentId(idepoch.id(), idepoch.idepoch().value());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement idepoch UPDATE, success [id="
                          << pb::coordinator_internal::IdEpochType_Name(idepoch.id()) << "]"
                          << " value=" << idepoch.idepoch().value();
          meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement idepoch UPDATE, but UpdatePresentId failed, [id="
                             << pb::coordinator_internal::IdEpochType_Name(idepoch.id()) << "]"
                             << " value=" << idepoch.idepoch().value();
        }
      } else if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        int ret = id_epoch_map_.Erase(idepoch.id());
        if (ret > 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement idepoch DELETE, success [id="
                          << pb::coordinator_internal::IdEpochType_Name(idepoch.id()) << "]";
        } else {
          DINGO_LOG(WARNING) << "ApplyMetaIncrement idepoch DELETE, but Erase failed, [id="
                             << pb::coordinator_internal::IdEpochType_Name(idepoch.id()) << "]";
        }
        meta_delete_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()).key());
      }
    }
  }

  // prepare meta_revision value and event_list
  int64_t meta_revision = FLAGS_meta_revision_base + index;
  auto event_list = std::make_shared<std::vector<pb::meta::MetaEvent>>();
  std::bitset<WATCH_BITSET_SIZE> watch_bitset;

  // 1.coordinator map
  {
    if (meta_increment.coordinators_size() > 0) {
      DINGO_LOG(INFO) << "1.coordinators_size=" << meta_increment.coordinators_size();
    }

    for (int i = 0; i < meta_increment.coordinators_size(); i++) {
      const auto& coordinator = meta_increment.coordinators(i);
      if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = coordinator_meta_->Put(coordinator.id(), coordinator.coordinator());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement coordinator CREATE, but Put failed, [id=" << coordinator.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement coordinator CREATE, success [id=" << coordinator.id() << "]";
        }

      } else if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = coordinator_meta_->Put(coordinator.id(), coordinator.coordinator());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement coordinator UPDATE, but Put failed, [id=" << coordinator.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement coordinator UPDATE, success [id=" << coordinator.id() << "]";
        }

      } else if (coordinator.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = coordinator_meta_->Erase(coordinator.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement coordinator DELETE, but Delete failed, [id=" << coordinator.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement coordinator DELETE, success [id=" << coordinator.id() << "]";
        }
      }
    }
  }

  // 2.store map
  {
    if (meta_increment.stores_size() > 0) {
      DINGO_LOG(DEBUG) << "2.stores_size=" << meta_increment.stores_size();
    }

    for (int i = 0; i < meta_increment.stores_size(); i++) {
      const auto& store = meta_increment.stores(i);
      if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = store_meta_->Put(store.id(), store.store());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement store CREATE, but Put failed, [id=" << store.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement store CREATE, success [id=" << store.id() << "]";
        }

      } else if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = store_meta_->Put(store.id(), store.store());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement store UPDATE, but Put failed, [id=" << store.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement store UPDATE, success [id=" << store.id() << "]";
        }

      } else if (store.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = store_meta_->Erase(store.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement store DELETE, but Delete failed, [id=" << store.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement store DELETE, success [id=" << store.id() << "]";
        }
      }
    }
  }

  // 3.executor map
  {
    if (meta_increment.executors_size() > 0) {
      DINGO_LOG(INFO) << "3.executors_size=" << meta_increment.executors_size();
    }

    for (int i = 0; i < meta_increment.executors_size(); i++) {
      const auto& executor = meta_increment.executors(i);
      if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = executor_meta_->Put(executor.id(), executor.executor());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement executor CREATE, but Put failed, [id=" << executor.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor CREATE, success [id=" << executor.id() << "]";
        }

      } else if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = executor_meta_->Put(executor.id(), executor.executor());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement executor UPDATE, but Put failed, [id=" << executor.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor UPDATE, success [id=" << executor.id() << "]";
        }

      } else if (executor.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = executor_meta_->Erase(executor.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement executor DELETE, but Delete failed, [id=" << executor.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor DELETE, success [id=" << executor.id() << "]";
        }
      }
    }
  }

  // 52 tenant_map
  {
    if (meta_increment.tenants_size() > 0) {
      DINGO_LOG(INFO) << "3.tenants_size=" << meta_increment.tenants_size();
    }

    for (int i = 0; i < meta_increment.tenants_size(); i++) {
      auto* tenant_increment = meta_increment.mutable_tenants(i);
      auto* tenant_internal = tenant_increment->mutable_tenant();
      tenant_internal->set_revision(meta_revision);

      if (tenant_increment->op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = tenant_meta_->Put(tenant_increment->id(), *tenant_internal);
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement tenant_mem CREATE, but Put failed, [id=" << tenant_increment->id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement tenant_mem CREATE, success [id=" << tenant_increment->id() << "]";
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_TENANT_CREATE);
        event.mutable_tenant()->set_id(tenant_increment->tenant().id());
        event.mutable_tenant()->set_name(tenant_increment->tenant().name());
        event.mutable_tenant()->set_comment(tenant_increment->tenant().comment());
        event.mutable_tenant()->set_revision(meta_revision);
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_TENANT_CREATE);

      } else if (tenant_increment->op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        pb::coordinator_internal::TenantInternal tenant_internal;
        auto ret1 = tenant_map_.Get(tenant_increment->id(), tenant_internal);
        if (ret1 < 0) {
          DINGO_LOG(ERROR) << "ERRROR: tenant_id not exist" << tenant_increment->id() << ", cannot do update, "
                           << tenant_increment->ShortDebugString();
          continue;
        }

        // if safe_point_ts > 0, update safe_point_ts only
        // else do not update safe_point_ts
        if (tenant_increment->tenant().safe_point_ts() > 0) {
          if (tenant_increment->tenant().safe_point_ts() <= tenant_internal.safe_point_ts()) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement tenant_mem UPDATE, but safe_point_ts <= old, [id="
                            << tenant_increment->id() << "], old_safe_point_ts=" << tenant_internal.safe_point_ts()
                            << ", new_safe_point_ts=" << tenant_increment->tenant().safe_point_ts();
            continue;
          }

          tenant_internal.set_safe_point_ts(tenant_increment->tenant().safe_point_ts());
          tenant_internal.set_update_timestamp(tenant_increment->tenant().update_timestamp());
        } else {
          tenant_internal.set_name(tenant_increment->tenant().name());
          tenant_internal.set_comment(tenant_increment->tenant().comment());
          tenant_internal.set_update_timestamp(tenant_increment->tenant().update_timestamp());
        }

        tenant_internal.set_revision(meta_revision);

        auto ret = tenant_meta_->Put(tenant_increment->id(), tenant_internal);
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement tenant_mem UPDATE, but Put failed, [id=" << tenant_increment->id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement tenant_mem UPDATE, success [id=" << tenant_increment->id() << "]";
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_TENANT_UPDATE);
        event.mutable_tenant()->set_id(tenant_increment->tenant().id());
        event.mutable_tenant()->set_name(tenant_increment->tenant().name());
        event.mutable_tenant()->set_comment(tenant_increment->tenant().comment());
        event.mutable_tenant()->set_revision(meta_revision);
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_TENANT_UPDATE);

      } else if (tenant_increment->op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = tenant_meta_->Erase(tenant_increment->id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement tenant_mem DELETE, but Delete failed, [id=" << tenant_increment->id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement tenant_mem DELETE, success [id=" << tenant_increment->id() << "]";
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_TENANT_DELETE);
        event.mutable_tenant()->set_id(tenant_increment->tenant().id());
        event.mutable_tenant()->set_name(tenant_increment->tenant().name());
        event.mutable_tenant()->set_comment(tenant_increment->tenant().comment());
        event.mutable_tenant()->set_revision(meta_revision);
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_TENANT_DELETE);
      }
    }
  }

  // 4.schema map
  {
    if (meta_increment.schemas_size() > 0) {
      DINGO_LOG(INFO) << "4.schemas_size=" << meta_increment.schemas_size();
    }

    for (int i = 0; i < meta_increment.schemas_size(); i++) {
      auto* schema = meta_increment.mutable_schemas(i);
      auto* schema_internal = schema->mutable_schema_internal();
      schema_internal->set_revision(meta_revision);

      if (schema->op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = schema_meta_->Put(schema->id(), *schema_internal);
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement schema CREATE, but Put failed, [id=" << schema->id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement schema CREATE, success [id=" << schema->id() << "]";
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_SCHEMA_CREATE);
        event.mutable_schema()->set_id(schema->id());
        event.mutable_schema()->set_name(schema_internal->name());
        event.mutable_schema()->set_revision(meta_revision);
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_SCHEMA_CREATE);

      } else if (schema->op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = schema_meta_->Put(schema->id(), *schema_internal);
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement schema UPDATE, but Put failed, [id=" << schema->id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement schema UPDATE, success [id=" << schema->id() << "]";
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_SCHEMA_UPDATE);
        event.mutable_schema()->set_id(schema->id());
        event.mutable_schema()->set_name(schema_internal->name());
        event.mutable_schema()->set_revision(meta_revision);
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_SCHEMA_UPDATE);

      } else if (schema->op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = schema_meta_->Erase(schema->id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement schema DELETE, but Delete failed, [id=" << schema->id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement schema DELETE, success [id=" << schema->id() << "]";
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_SCHEMA_DELETE);
        event.mutable_schema()->set_id(schema->id());
        event.mutable_schema()->set_name(schema_internal->name());
        event.mutable_schema()->set_revision(meta_revision);
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_SCHEMA_DELETE);
      }
    }
  }

  // 5.region map
  {
    if (meta_increment.regions_size() > 0) {
      DINGO_LOG(INFO) << "5.regions_size=" << meta_increment.regions_size();
    }

    // for region_map_ multiput
    std::vector<int64_t> region_id_to_write;
    std::vector<int64_t> region_id_to_delete;
    std::vector<pb::coordinator_internal::RegionInternal> region_internal_to_write;
    // for range_region_map_ multiput
    std::vector<std::string> region_start_key_to_write;
    std::vector<std::string> region_start_key_to_delete_for_update;
    std::vector<pb::coordinator_internal::RegionInternal> region_start_key_internal_to_write;
    std::vector<std::string> region_start_key_to_delete;

    for (int i = 0; i < meta_increment.regions_size(); i++) {
      auto* region = meta_increment.mutable_regions(i);
      region->mutable_region()->mutable_definition()->set_revision(meta_revision);

      if (region->op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        // add region to region_map
        region_id_to_write.push_back(region->id());
        region_internal_to_write.push_back(region->region());

        // update range_region_map_
        // range_region_map_.Put(region.region().definition().range().start_key(), region.region().id());
        const auto& new_region_range = region->region().definition().range();
        if (new_region_range.start_key() < new_region_range.end_key()) {
          /* range_region_map_.Put(new_region_range.start_key(), region.region().id()); */
          region_start_key_to_write.push_back(new_region_range.start_key());
          region_start_key_internal_to_write.push_back(region->region());
          DINGO_LOG(INFO) << "add range_region_map_ success, region_id=[" << region->region().id() << "], start_key=["
                          << Helper::StringToHex(region->region().definition().range().start_key()) << "]";
        } else {
          DINGO_LOG(INFO) << "add range_region_map_ skipped of start_key >= end_key, region_id=["
                          << region->region().id() << "], start_key=["
                          << Helper::StringToHex(region->region().definition().range().start_key()) << "], end_key=["
                          << Helper::StringToHex(region->region().definition().range().end_key()) << "]";
        }

        // meta_write_kv
        meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region->region()));

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_REGION_CREATE);
        event.mutable_region()->set_id(region->id());
        event.mutable_region()->set_region_type(region->region().region_type());
        *event.mutable_region()->mutable_definition() = region->region().definition();
        event.mutable_region()->set_state(region->region().state());
        event.mutable_region()->set_create_timestamp(region->region().create_timestamp());
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_REGION_CREATE);

      } else if (region->op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // get old region
        pb::coordinator_internal::RegionInternal old_region;
        auto ret = region_map_.Get(region->id(), old_region);
        if (ret < 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region UPDATE, [id=" << region->id() << "] failed, not found";
          continue;
        }

        if (region->region().definition().epoch().version() < old_region.definition().epoch().version()) {
          DINGO_LOG(ERROR) << "ApplyMetaIncrement region UPDATE, [id=" << region->id()
                           << "] failed, new epoch.version < old epoch.version";
          continue;
        }

        if (region->region().definition().range().start_key() != old_region.definition().range().start_key()) {
          if (region->region().definition().epoch().version() == old_region.definition().epoch().version()) {
            DINGO_LOG(ERROR) << "ApplyMetaIncrement region UPDATE, [id=" << region->id()
                             << "] failed, new start_key != old start_key, but epoch.version is same";
            continue;
          }

          // CAUTION: check the old_region's start_key is not in the range_region_map, if exists, and is not self
          // region, there must be some error, so we need to check and set is_legal_delete flag to control the deletiong
          // of RangeRegionMap.
          pb::coordinator_internal::RegionInternal region_for_old_start_key;
          auto ret1 = range_region_map_.Get(old_region.definition().range().start_key(), region_for_old_start_key);
          bool is_legal_delete = true;
          if (ret1 > 0 && region_for_old_start_key.id() > 0 && region_for_old_start_key.id() != region->id()) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement region UPDATE, [id=" << region->id()
                            << "] old_start_key is exists, and is not self region, old_start_key=["
                            << Helper::StringToHex(old_region.definition().range().start_key())
                            << "], region of old_start_key: [" << region_for_old_start_key.ShortDebugString()
                            << "], will update region: [" << region->ShortDebugString()
                            << "], old_region of will update region: [" << old_region.ShortDebugString()
                            << "], will check the region from RegionMap again";

            pb::coordinator_internal::RegionInternal temp_region;
            auto ret2 = region_map_.Get(region_for_old_start_key.id(), temp_region);
            if (ret > 0 && temp_region.id() > 0 && temp_region.id() == region_for_old_start_key.id() &&
                temp_region.definition().range().start_key() == old_region.definition().range().start_key()) {
              DINGO_LOG(ERROR)
                  << "ApplyMetaIncrement region UPDATE, [id=" << region->id()
                  << "] old_start_key is exists, and is not self region, old_start_key=["
                  << Helper::StringToHex(old_region.definition().range().start_key()) << "], region of old_start_key: ["
                  << region_for_old_start_key.ShortDebugString() << "], will update region: ["
                  << region->ShortDebugString() << "], old_region of will update region: ["
                  << old_region.ShortDebugString() << "], region of old_start_key in RegionMap: ["
                  << temp_region.ShortDebugString()
                  << "], the region from RegionMap is same as region from RangeRegionMap, so we cannot delete "
                     "the old_start_key from RangeRegionMap, it's not ok";
              is_legal_delete = false;
            } else {
              DINGO_LOG(INFO) << "ApplyMetaIncrement region UPDATE, [id=" << region->id()
                              << "] old_start_key is exists, and is not self region, old_start_key=["
                              << Helper::StringToHex(old_region.definition().range().start_key())
                              << "], region of old_start_key: [" << region_for_old_start_key.ShortDebugString()
                              << "], will update region: [" << region->ShortDebugString()
                              << "], old_region of will update region: [" << old_region.ShortDebugString()
                              << "], region of old_start_key in RegionMap: [" << temp_region.ShortDebugString()
                              << "], the region from RegionMap is not same as region from RangeRegionMap, so we "
                                 "can delete the old_start_key from RangeRegionMap, it's ok";
            }
          }

          // update range_region_map_
          // range_region_map_.Erase(old_region.definition().range().start_key());
          bool need_delete = true;
          for (const auto& start_key : region_start_key_to_write) {
            if (start_key == old_region.definition().range().start_key()) {
              need_delete = false;
              break;
            }
          }

          if (need_delete && is_legal_delete) {
            region_start_key_to_delete_for_update.push_back(old_region.definition().range().start_key());
            DINGO_LOG(INFO) << "erase range_region_map_ success, region_id=[" << region->region().id()
                            << "], start_key=[" << Helper::StringToHex(old_region.definition().range().start_key())
                            << "]";
          } else {
            DINGO_LOG(INFO) << "erase range_region_map_ skipped, region_id=[" << region->region().id()
                            << "], start_key=[" << Helper::StringToHex(old_region.definition().range().start_key())
                            << "], need_delete=[" << need_delete << "], is_legal_delete=[" << is_legal_delete << "]";
          }
        }

        // update range_region_map_
        const auto& new_region_range = region->region().definition().range();

        if (new_region_range.start_key() < new_region_range.end_key()) {
          region_start_key_to_write.push_back(new_region_range.start_key());
          region_start_key_internal_to_write.push_back(region->region());
          DINGO_LOG(INFO) << "update range_region_map_ success, region_id=[" << region->region().id()
                          << "], start_key=[" << Helper::StringToHex(new_region_range.start_key())
                          << "], old_start_key=[" << Helper::StringToHex(old_region.definition().range().start_key())
                          << "]";
        } else {
          DINGO_LOG(INFO) << "update range_region_map_ skipped of start_key >= end_key, region_id=["
                          << region->region().id() << "], start_key=["
                          << Helper::StringToHex(region->region().definition().range().start_key()) << "], end_key=["
                          << Helper::StringToHex(region->region().definition().range().end_key())
                          << "], old_start_key=[" << Helper::StringToHex(old_region.definition().range().start_key())
                          << "], old_end_key=[" << Helper::StringToHex(old_region.definition().range().end_key())
                          << "]";
        }

        region_id_to_write.push_back(region->id());
        region_internal_to_write.push_back(region->region());

        // meta_write_kv
        meta_write_to_kv.push_back(region_meta_->TransformToKvValue(region->region()));

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_REGION_UPDATE);
        event.mutable_region()->set_id(region->id());
        event.mutable_region()->set_region_type(region->region().region_type());
        *event.mutable_region()->mutable_definition() = region->region().definition();
        event.mutable_region()->set_state(region->region().state());
        event.mutable_region()->set_create_timestamp(region->region().create_timestamp());
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_REGION_UPDATE);

      } else if (region->op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        pb::coordinator_internal::RegionInternal old_region;
        int ret1 = region_map_.Get(region->id(), old_region);
        if (ret1 < 0) {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region DELETE, [id=" << region->id() << "] failed, not found";
          continue;
        }

        // remove region from region_map
        region_id_to_delete.push_back(region->id());
        DINGO_LOG(INFO) << "ApplyMetaIncrement region DELETE, [id=" << region->id() << "] will be deleted";

        // meta_delete_kv
        meta_delete_to_kv.push_back(region_meta_->TransformToKvValue(region->region()).key());

        // remove region from region_metrics_map
        region_metrics_map_.Erase(region->id());

        // update range_region_map_
        // range_region_map_.Erase(region.region().definition().range().start_key());
        region_start_key_to_delete.push_back(old_region.definition().range().start_key());
        DINGO_LOG(INFO) << "erase range_region_map_ success, region_id=[" << region->region().id() << "], start_key=["
                        << Helper::StringToHex(old_region.definition().range().start_key()) << "]";

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_REGION_DELETE);
        event.mutable_region()->set_id(region->id());
        event.mutable_region()->set_region_type(region->region().region_type());
        *event.mutable_region()->mutable_definition() = region->region().definition();
        event.mutable_region()->set_state(region->region().state());
        event.mutable_region()->set_create_timestamp(region->region().create_timestamp());
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_REGION_DELETE);
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

    if (!region_id_to_delete.empty()) {
      auto ret = region_map_.MultiErase(region_id_to_delete);
      if (ret < 0) {
        DINGO_LOG(WARNING) << "ApplyMetaIncrement region DELETE, count=[" << region_id_to_delete.size() << "] failed";
      } else {
        DINGO_LOG(INFO) << "ApplyMetaIncrement region DELETE, count=[" << region_id_to_delete.size() << "] success";
      }
    }

    if (!region_start_key_to_delete_for_update.empty() || !region_start_key_to_write.empty()) {
      auto ret = range_region_map_.MultiEraseThenPut(region_start_key_to_delete_for_update, region_start_key_to_write,
                                                     region_start_key_internal_to_write);
      if (ret < 0) {
        DINGO_LOG(WARNING) << "ApplyMetaIncrement range_region UPDATE, size=["
                           << region_start_key_to_delete_for_update.size() << "] put_size=["
                           << region_start_key_to_write.size() << "] failed";
      } else {
        DINGO_LOG(INFO) << "ApplyMetaIncrement range_region UPDATE, del_size=["
                        << region_start_key_to_delete_for_update.size() << "] put_size=["
                        << region_start_key_to_write.size() << "] success";
      }
    }

    if (!region_start_key_to_delete.empty()) {
      auto ret = range_region_map_.MultiErase(region_start_key_to_delete);
      if (ret < 0) {
        DINGO_LOG(WARNING) << "ApplyMetaIncrement range_region DELETE, size=[" << region_start_key_to_delete.size()
                           << "] failed";
      } else {
        DINGO_LOG(INFO) << "ApplyMetaIncrement range_region DELETE, size=[" << region_start_key_to_delete.size()
                        << "] success";
      }
    }
  }

  // 5.1 deleted region map
  {
    if (meta_increment.deleted_regions_size() > 0) {
      DINGO_LOG(INFO) << "5.1 deleted_regions_size=" << meta_increment.deleted_regions_size();
    }

    for (int i = 0; i < meta_increment.deleted_regions_size(); i++) {
      const auto& region = meta_increment.deleted_regions(i);
      if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = deleted_region_meta_->Put(region.id(), region.region());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement deleted_region CREATE, [id=" << region.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_region CREATE, [id=" << region.id() << "] success";
        }

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = deleted_region_meta_->Put(region.id(), region.region());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement deleted_region UPDATE, [id=" << region.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_region UPDATE, [id=" << region.id() << "] success";
        }

      } else if (region.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = deleted_region_meta_->Erase(region.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement deleted_region DELETE, [id=" << region.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_region DELETE, [id=" << region.id() << "] success";
        }
      }
    }
  }

  // 50.table_index map
  // table_index_map_ must updated before table_map_, schema_map_ and index_map_
  // GetTableIndexes/DropTableIndexes will first get from table_map_, and then get from table_index_map
  // and the schema_map_ is updated in the table_map_ update, so it's available after table_map_, and table_map_ is
  // available after table_index_map_
  {
    if (meta_increment.table_indexes_size() > 0) {
      DINGO_LOG(INFO) << "ApplyMetaIncrement table_indexes size=" << meta_increment.table_indexes_size();
    }

    for (int i = 0; i < meta_increment.table_indexes_size(); i++) {
      auto* table_index = meta_increment.mutable_table_indexes(i);
      table_index->mutable_table_indexes()->set_revision(meta_revision);

      if (table_index->op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = table_index_meta_->Put(table_index->id(), table_index->table_indexes());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement table_index CREATE, [id=" << table_index->id()
                           << "] failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table_index CREATE, [id=" << table_index->id() << "] success";
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_CREATE);
        event.mutable_table_index()->set_id(table_index->id());
        for (const auto& common_id : table_index->table_indexes().table_ids()) {
          *event.mutable_table_index()->add_table_ids() = common_id;
        }
        event.mutable_table_index()->set_revision(meta_revision);
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_CREATE);

      } else if (table_index->op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        pb::coordinator_internal::TableIndexInternal table_index_internal;
        int ret1 = table_index_map_.Get(table_index->id(), table_index_internal);
        if (ret1 < 0) {
          DINGO_LOG(ERROR) << "ApplyMetaIncrement table_index UPDATE, [id=" << table_index->id()
                           << "] failed, not found";
          continue;
        }

        if (table_index->table_ids_to_add_size() > 0 || table_index->table_ids_to_del_size() > 0) {
          std::map<int64_t, pb::meta::DingoCommonId> table_ids_old;
          for (const auto& table_id : table_index_internal.table_ids()) {
            table_ids_old.insert(std::make_pair(table_id.entity_id(), table_id));
          }

          if (table_index->table_ids_to_add_size() > 0) {
            for (const auto& table_id : table_index->table_ids_to_add()) {
              table_ids_old.insert(std::make_pair(table_id.entity_id(), table_id));
            }
            for (const auto& table_id : table_index->table_ids_to_del()) {
              table_ids_old.erase(table_id.entity_id());
            }
          }

          table_index_internal.clear_table_ids();
          for (const auto& [id, table_id] : table_ids_old) {
            *table_index_internal.add_table_ids() = table_id;
          }
        } else {
          table_index_internal = table_index->table_indexes();
        }

        auto ret = table_index_meta_->Put(table_index->id(), table_index_internal);
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement table_index UPDATE, [id=" << table_index->id()
                           << "] failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table_index UPDATE, [id=" << table_index->id() << "] success";
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_UPDATE);
        event.mutable_table_index()->set_id(table_index->id());
        for (const auto& common_id : table_index_internal.table_ids()) {
          *event.mutable_table_index()->add_table_ids() = common_id;
        }
        event.mutable_table_index()->set_revision(meta_revision);
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_UPDATE);

      } else if (table_index->op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = table_index_meta_->Erase(table_index->id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement table_index DELETE, [id=" << table_index->id()
                           << "] failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table_index DELETE, [id=" << table_index->id() << "] success";
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_DELETE);
        event.mutable_table_index()->set_id(table_index->id());
        for (const auto& common_id : table_index->table_indexes().table_ids()) {
          *event.mutable_table_index()->add_table_ids() = common_id;
        }
        event.mutable_table_index()->set_revision(meta_revision);
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_TABLE_INDEX_DELETE);
      }
    }
  }

  // 12.index map
  // CAUTION: for table with indexes, the read order is schema_map->table_map->table_index_map->index_map
  //          so we need to update index_map_ after table_index_map_ and before table_map_
  {
    if (meta_increment.indexes_size() > 0) {
      DINGO_LOG(INFO) << "12.indexes_size=" << meta_increment.indexes_size();
    }

    for (int i = 0; i < meta_increment.indexes_size(); i++) {
      auto* index = meta_increment.mutable_indexes(i);
      index->mutable_table()->mutable_definition()->set_revision(meta_revision);

      if (index->op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = index_meta_->Put(index->id(), index->table());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement index CREATE, [id=" << index->id()
                           << "] failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement index CREATE, [id=" << index->id() << "] success";
        }

        // add index to parent schema
        pb::coordinator_internal::SchemaInternal schema_to_update;
        auto ret1 = schema_map_.Get(index->table().schema_id(), schema_to_update);
        // auto* schema = schema_map_.seek(index.schema_id());
        if (ret1 > 0) {
          // add new created index's id to its parent schema's index_ids
          schema_to_update.add_index_ids(index->id());
          auto ret2 = schema_meta_->Put(index->table().schema_id(), schema_to_update);
          if (!ret2.ok()) {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement index CREATE, but schema_meta_->Put failed, [id=" << index->id()
                             << "], errcode: " << ret2.error_code() << ", errmsg: " << ret2.error_str()
                             << ", schema_id=" << index->table().schema_id();
          }

          DINGO_LOG(INFO) << "5.index map CREATE new_sub_index id=" << index->id()
                          << " parent_id=" << index->table().schema_id();

        } else {
          DINGO_LOG(FATAL) << " CREATE INDEX apply illegal schema_id=" << index->table().schema_id()
                           << " index_id=" << index->id() << " index_name=" << index->table().definition().name();
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_INDEX_CREATE);
        event.mutable_index()->set_id(index->id());
        *event.mutable_index()->mutable_definition() = index->table().definition();
        event.mutable_index()->set_schema_id(index->table().schema_id());
        event.mutable_index()->set_parent_table_id(index->table().table_id());
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_INDEX_CREATE);

      } else if (index->op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update index to index_map
        pb::coordinator_internal::TableInternal table_internal;
        int ret = index_map_.Get(index->id(), table_internal);
        if (ret > 0) {
          if (index->table().has_definition()) {
            *(table_internal.mutable_definition()) = index->table().definition();
          }
          auto ret1 = index_meta_->Put(index->id(), table_internal);
          if (!ret1.ok()) {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement index UPDATE, but Put failed, [id=" << index->id()
                             << "], errcode: " << ret1.error_code() << ", errmsg: " << ret1.error_str();
          } else {
            DINGO_LOG(INFO) << "ApplyMetaIncrement index UPDATE, [id=" << index->id() << "] success";
          }
        } else {
          DINGO_LOG(ERROR) << " UPDATE INDEX apply illegal index_id=" << index->id()
                           << " index_name=" << index->table().definition().name();
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_INDEX_UPDATE);
        event.mutable_index()->set_id(index->id());
        *event.mutable_index()->mutable_definition() = index->table().definition();
        event.mutable_index()->set_schema_id(index->table().schema_id());
        event.mutable_index()->set_parent_table_id(index->table().table_id());
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_INDEX_UPDATE);

      } else if (index->op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // delete from parent schema
        pb::coordinator_internal::SchemaInternal schema_to_update;
        auto ret1 = schema_map_.Get(index->table().schema_id(), schema_to_update);

        if (ret1 > 0) {
          pb::coordinator_internal::SchemaInternal new_schema;
          new_schema = schema_to_update;

          new_schema.clear_index_ids();

          // add left index_id to new_schema
          for (auto x : schema_to_update.index_ids()) {
            if (x != index->id()) {
              new_schema.add_index_ids(x);
            }
          }
          schema_to_update = new_schema;
          auto ret2 = schema_meta_->Put(index->table().schema_id(), schema_to_update);
          if (!ret2.ok()) {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement index DELETE, but Put failed, [id=" << index->id()
                             << "], errcode: " << ret2.error_code() << ", errmsg: " << ret2.error_str();
          }

          DINGO_LOG(INFO) << "5.index map DELETE new_sub_index id=" << index->id()
                          << " parent_id=" << index->table().schema_id();

        } else {
          DINGO_LOG(FATAL) << " DROP INDEX apply illegal schema_id=" << index->table().schema_id()
                           << " index_id=" << index->id() << " index_name=" << index->table().definition().name();
        }

        auto ret = index_meta_->Erase(index->id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement index DELETE, but Erase failed, [id=" << index->id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement index DELETE, success [id=" << index->id() << "]";
        }

        // delete index_metrics
        index_metrics_map_.Erase(index->id());

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_INDEX_DELETE);
        event.mutable_index()->set_id(index->id());
        *event.mutable_index()->mutable_definition() = index->table().definition();
        event.mutable_index()->set_schema_id(index->table().schema_id());
        event.mutable_index()->set_parent_table_id(index->table().table_id());
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_INDEX_DELETE);
      }
    }
  }

  // 6.table map
  {
    if (meta_increment.tables_size() > 0) {
      DINGO_LOG(INFO) << "6.tables_size=" << meta_increment.tables_size();
    }

    for (int i = 0; i < meta_increment.tables_size(); i++) {
      auto* table = meta_increment.mutable_tables(i);
      table->mutable_table()->mutable_definition()->set_revision(meta_revision);

      if (table->op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = table_meta_->Put(table->id(), table->table());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement table CREATE, but Put failed, [id=" << table->id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table CREATE, success [id=" << table->id() << "]";
        }

        // add table to parent schema
        pb::coordinator_internal::SchemaInternal schema_to_update;
        auto ret1 = schema_map_.Get(table->table().schema_id(), schema_to_update);
        if (ret1 > 0) {
          // add new created table's id to its parent schema's table_ids
          schema_to_update.add_table_ids(table->id());
          auto ret1 = schema_meta_->Put(table->table().schema_id(), schema_to_update);
          if (!ret1.ok()) {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement table CREATE, but schema_meta_->Put failed, [id=" << table->id()
                             << "], errcode: " << ret1.error_code() << ", errmsg: " << ret1.error_str()
                             << ", schema_id=" << table->table().schema_id();
          }

          DINGO_LOG(INFO) << "5.table map CREATE new_sub_table id=" << table->id()
                          << " parent_id=" << table->table().schema_id();

        } else {
          DINGO_LOG(FATAL) << " CREATE TABLE apply illegal schema_id=" << table->table().schema_id()
                           << " table_id=" << table->id() << " table_name=" << table->table().definition().name();
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_TABLE_CREATE);
        event.mutable_table()->set_id(table->id());
        *event.mutable_table()->mutable_definition() = table->table().definition();
        event.mutable_table()->set_schema_id(table->table().schema_id());
        event.mutable_table()->set_parent_table_id(table->table().table_id());
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_TABLE_CREATE);

      } else if (table->op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        // update table to table_map
        pb::coordinator_internal::TableInternal table_internal;
        int ret = table_map_.Get(table->id(), table_internal);
        if (ret > 0) {
          if (table->table().has_definition()) {
            *(table_internal.mutable_definition()) = table->table().definition();
          }
          auto ret1 = table_meta_->Put(table->id(), table_internal);
          if (!ret1.ok()) {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement table UPDATE, but Put failed, [id=" << table->id()
                             << "], errcode: " << ret1.error_code() << ", errmsg: " << ret1.error_str();
          } else {
            DINGO_LOG(INFO) << "ApplyMetaIncrement table UPDATE, success [id=" << table->id() << "]";
          }

        } else {
          DINGO_LOG(ERROR) << " UPDATE TABLE apply illegal table_id=" << table->id()
                           << " table_name=" << table->table().definition().name();
        }

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_TABLE_UPDATE);
        event.mutable_table()->set_id(table->id());
        *event.mutable_table()->mutable_definition() = table->table().definition();
        event.mutable_table()->set_schema_id(table->table().schema_id());
        event.mutable_table()->set_parent_table_id(table->table().table_id());
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_TABLE_UPDATE);

      } else if (table->op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        // delete from parent schema
        pb::coordinator_internal::SchemaInternal schema_to_update;
        auto ret1 = schema_map_.Get(table->table().schema_id(), schema_to_update);

        if (ret1 > 0) {
          pb::coordinator_internal::SchemaInternal new_schema;
          new_schema = schema_to_update;

          new_schema.clear_table_ids();

          // add left table_id to new_schema
          for (auto x : schema_to_update.table_ids()) {
            if (x != table->id()) {
              new_schema.add_table_ids(x);
            }
          }
          schema_to_update = new_schema;
          auto ret2 = schema_meta_->Put(table->table().schema_id(), schema_to_update);
          if (!ret2.ok()) {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement table DELETE, but Put failed, [id=" << table->id()
                             << "], errcode: " << ret2.error_code() << ", errmsg: " << ret2.error_str();
          } else {
            DINGO_LOG(INFO) << "ApplyMetaIncrement table DELETE, success [id=" << table->id() << "]";
          }

          DINGO_LOG(INFO) << "5.table map DELETE new_sub_table id=" << table->id()
                          << " parent_id=" << table->table().schema_id();

        } else {
          DINGO_LOG(FATAL) << " DROP TABLE apply illegal schema_id=" << table->table().schema_id()
                           << " table_id=" << table->id() << " table_name=" << table->table().definition().name();
        }

        // after schema update, delete table from table_map
        auto ret = table_meta_->Erase(table->id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement table DELETE, but Erase failed, [id=" << table->id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement table DELETE, success [id=" << table->id() << "]";
        }

        // delete table_metrics
        table_metrics_map_.Erase(table->id());

        // add event_list
        pb::meta::MetaEvent event;
        event.set_event_type(pb::meta::MetaEventType::META_EVENT_TABLE_DELETE);
        event.mutable_table()->set_id(table->id());
        *event.mutable_table()->mutable_definition() = table->table().definition();
        event.mutable_table()->set_schema_id(table->table().schema_id());
        event.mutable_table()->set_parent_table_id(table->table().table_id());
        event_list->push_back(event);
        watch_bitset.set(pb::meta::MetaEventType::META_EVENT_TABLE_DELETE);
      }
    }
  }

  // 6.1 deleted table map
  {
    if (meta_increment.deleted_tables_size() > 0) {
      DINGO_LOG(INFO) << "6.deleted_tables_size=" << meta_increment.deleted_tables_size();
    }

    for (int i = 0; i < meta_increment.deleted_tables_size(); i++) {
      const auto& deleted_table = meta_increment.deleted_tables(i);
      if (deleted_table.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = deleted_table_meta_->Put(deleted_table.id(), deleted_table.table());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement deleted_table CREATE, [id=" << deleted_table.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_table CREATE, [id=" << deleted_table.id() << "] success";
        }

      } else if (deleted_table.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = deleted_table_meta_->Put(deleted_table.id(), deleted_table.table());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement deleted_table UPDATE, [id=" << deleted_table.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_table UPDATE, [id=" << deleted_table.id() << "] success";
        }

      } else if (deleted_table.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = deleted_table_meta_->Erase(deleted_table.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement deleted_table DELETE, [id=" << deleted_table.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_table DELETE, [id=" << deleted_table.id() << "] success";
        }
      }
    }
  }

  // 8.table_metrics map
  // table_metrics_map is a temp map, only used in memory, so we don't need to update it in raft apply

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
                        << store_operation_residual.region_cmd_ids_size();

        DINGO_LOG(DEBUG) << "store_operation_map_.Put in DELETE, store_id=" << store_operation.id()
                         << " region_cmd count change [" << store_operation_in_map.region_cmd_ids_size() << ", "
                         << store_operation_residual.region_cmd_ids_size() << "]  orig_store_operation=["
                         << store_operation.ShortDebugString() << "] new_store_operation=["
                         << store_operation_residual.ShortDebugString() << "]";
      } else if (store_operation.op_type() == pb::coordinator_internal::MetaIncrementOpType::MODIFY) {
        if (store_operation.has_move_region_cmd()) {
          int64_t region_cmd_id = store_operation.move_region_cmd().region_cmd_id();
          int64_t old_store_id = store_operation.move_region_cmd().from_store_id();
          int64_t new_store_id = store_operation.move_region_cmd().to_store_id();

          DINGO_LOG(INFO) << "ApplyMetaIncrement store_operation MODIFY, region_cmd_id=" << region_cmd_id
                          << ", old_store_id=" << old_store_id << ", new_store_id=" << new_store_id;

          pb::coordinator_internal::StoreOperationInternal old_store_operation_in_map;
          pb::coordinator_internal::StoreOperationInternal new_store_operation_in_map;

          store_operation_map_.Get(old_store_id, old_store_operation_in_map);
          store_operation_map_.Get(new_store_id, new_store_operation_in_map);

          if (old_store_operation_in_map.id() != old_store_id || new_store_operation_in_map.id() != new_store_id) {
            DINGO_LOG(ERROR) << "ApplyMetaIncrement store_operation MODIFY, but not found, store_operation="
                             << store_operation.ShortDebugString() << ", old_store_id=" << old_store_id
                             << ", new_store_id=" << new_store_id
                             << ", old_store_operation_in_map=" << old_store_operation_in_map.ShortDebugString()
                             << ", new_store_operation_in_map=" << new_store_operation_in_map.ShortDebugString();
            continue;
          }

          pb::coordinator_internal::RegionCmdInternal region_cmd_internal;
          region_cmd_meta_->Get(region_cmd_id, region_cmd_internal);

          if (region_cmd_internal.id() != region_cmd_id) {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement store_operation MODIFY, but not found, store_operation="
                             << store_operation.ShortDebugString() << ", region_cmd_id=" << region_cmd_id
                             << ", region_cmd_internal=" << region_cmd_internal.ShortDebugString();
          }

          // delete region_cmd from old_store_operation
          pb::coordinator_internal::StoreOperationInternal old_store_operation_residual;
          old_store_operation_residual.set_id(old_store_id);

          for (int i = 0; i < old_store_operation_in_map.region_cmd_ids_size(); i++) {
            if (old_store_operation_in_map.region_cmd_ids(i) != region_cmd_id) {
              old_store_operation_residual.add_region_cmd_ids(old_store_operation_in_map.region_cmd_ids(i));
            }
          }

          int ret = store_operation_map_.Put(old_store_id, old_store_operation_residual);
          if (ret > 0) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement store_operation MODIFY, [old_store_id=" << old_store_id
                            << "] success";
          } else {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement store_operation MODIFY, [old_store_id=" << old_store_id
                             << "] failed, ret = " << ret
                             << ", old_store_operation_residual=" << old_store_operation_residual.ShortDebugString();
          }

          // add region_cmd to new_store_operation
          new_store_operation_in_map.add_region_cmd_ids(region_cmd_id);
          ret = store_operation_map_.Put(new_store_id, new_store_operation_in_map);
          if (ret > 0) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement store_operation MODIFY, [new_store_id=" << new_store_id
                            << "] success";
          } else {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement store_operation MODIFY, [new_store_id=" << new_store_id
                             << "] failed, ret = " << ret
                             << ", new_store_operation_in_map=" << new_store_operation_in_map.ShortDebugString();
          }

          // update region_cmd's store_id
          region_cmd_internal.mutable_region_cmd()->set_store_id(new_store_id);
          auto ret1 = region_cmd_meta_->Put(region_cmd_id, region_cmd_internal);
          if (ret1.ok()) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement store_operation MODIFY, [region_cmd_id=" << region_cmd_id
                            << "] success";
          } else {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement store_operation MODIFY, [region_cmd_id=" << region_cmd_id
                             << "] failed, ret1 = " << ret1.error_code() << ", errmsg: " << ret1.error_str()
                             << ", region_cmd_internal=" << region_cmd_internal.ShortDebugString();
          }

        } else {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement store_operation MODIFY, but not support, store_operation="
                           << store_operation.ShortDebugString();
        }
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
        auto ret = region_cmd_meta_->Put(region_cmd.id(), region_cmd.region_cmd());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement region_cmd CREATE, [id=" << region_cmd.id()
                           << "] failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region_cmd CREATE, [id=" << region_cmd.id() << "] success";
        }

      } else if (region_cmd.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = region_cmd_meta_->PutIfExists(region_cmd.id(), region_cmd.region_cmd());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement region_cmd UPDATE, [id=" << region_cmd.id()
                           << "] failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region_cmd UPDATE, [id=" << region_cmd.id() << "] success";
        }

      } else if (region_cmd.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = region_cmd_meta_->Erase(region_cmd.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement region_cmd DELETE, [id=" << region_cmd.id()
                           << "] failed, errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement region_cmd DELETE, [id=" << region_cmd.id() << "] success";
        }
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
        auto ret = executor_user_meta_->Put(executor_user.id(), executor_user.executor_user());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement executor_user CREATE, but Put failed, [id=" << executor_user.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor_user CREATE, success [id=" << executor_user.id() << "]";
        }

      } else if (executor_user.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = executor_user_meta_->Put(executor_user.id(), executor_user.executor_user());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement executor_user UPDATE, but Put failed, [id=" << executor_user.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor_user UPDATE, success [id=" << executor_user.id() << "]";
        }

      } else if (executor_user.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = executor_user_meta_->Erase(executor_user.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement executor_user DELETE, but Delete failed, [id=" << executor_user.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement executor_user DELETE, success [id=" << executor_user.id() << "]";
        }
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
        auto ret = task_list_meta_->Put(task_list.id(), task_list.task_list());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement task_list CREATE, but Put failed, [id=" << task_list.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement task_list CREATE, success [id=" << task_list.id() << "]";
        }

      } else if (task_list.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        if (!task_list.is_partial_update()) {
          auto ret = task_list_meta_->Put(task_list.id(), task_list.task_list());
          if (!ret.ok()) {
            DINGO_LOG(FATAL) << "ApplyMetaIncrement task_list UPDATE, but Put failed, [id=" << task_list.id()
                             << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
          } else {
            DINGO_LOG(INFO) << "ApplyMetaIncrement task_list UPDATE, success [id=" << task_list.id() << "]";
          }
        } else {
          // partial update
          bool is_updated = false;
          pb::coordinator::TaskList task_list_temp;
          int ret = task_list_map_.Get(task_list.id(), task_list_temp);
          if (ret > 0) {
            for (int i = 0; i < task_list_temp.tasks_size(); i++) {
              auto* task_ptr = task_list_temp.mutable_tasks(i);

              for (int j = 0; j < task_ptr->store_operations_size(); j++) {
                auto* store_operation_ptr = task_ptr->mutable_store_operations(j);

                for (int k = 0; k < store_operation_ptr->region_cmds_size(); k++) {
                  auto* region_cmd_ptr = store_operation_ptr->mutable_region_cmds(k);

                  for (const auto& region_cmd_status_update : task_list.region_cmds_status()) {
                    if (region_cmd_ptr->id() == region_cmd_status_update.region_cmd_id()) {
                      region_cmd_ptr->set_status(region_cmd_status_update.status());
                      *region_cmd_ptr->mutable_error() = region_cmd_status_update.error();
                      is_updated = true;
                      DINGO_LOG(INFO) << "ApplyMetaIncrement task_list UPDATE, partial update, [id=" << task_list.id()
                                      << "], region_cmd_id=" << region_cmd_ptr->id()
                                      << " status from: " << region_cmd_ptr->status()
                                      << ", to: " << region_cmd_status_update.status()
                                      << " error from: " << region_cmd_ptr->error().ShortDebugString()
                                      << ", to: " << region_cmd_status_update.error().ShortDebugString()
                                      << ", region_cmd: " << region_cmd_ptr->ShortDebugString();
                    }
                  }
                }
              }
            }

            if (is_updated) {
              auto ret1 = task_list_meta_->Put(task_list.id(), task_list_temp);
              if (!ret1.ok()) {
                DINGO_LOG(FATAL) << "ApplyMetaIncrement task_list UPDATE, but Put failed, [id=" << task_list.id()
                                 << "], errcode: " << ret1.error_code() << ", errmsg: " << ret1.error_str()
                                 << ", task_list_temp: " << task_list_temp.ShortDebugString();
              } else {
                DINGO_LOG(INFO) << "ApplyMetaIncrement task_list UPDATE, success [id=" << task_list.id() << "]";
              }
            }
          } else {
            DINGO_LOG(ERROR) << " UPDATE task_list apply illegal task_list_id=" << task_list.id()
                             << " task_list_id=" << task_list.id() << ", task_list: " << task_list.ShortDebugString();
          }
        }
      } else if (task_list.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = task_list_meta_->Erase(task_list.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement task_list DELETE, but Delete failed, [id=" << task_list.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement task_list DELETE, success [id=" << task_list.id() << "]";
        }
      }
    }
  }

  // 12.1 deleted index map
  {
    if (meta_increment.deleted_indexes_size() > 0) {
      DINGO_LOG(INFO) << "6.deleted_indexes_size=" << meta_increment.deleted_indexes_size();
    }

    for (int i = 0; i < meta_increment.deleted_indexes_size(); i++) {
      const auto& deleted_index = meta_increment.deleted_indexes(i);
      if (deleted_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = deleted_index_meta_->Put(deleted_index.id(), deleted_index.table());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement deleted_index CREATE, [id=" << deleted_index.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_index CREATE, [id=" << deleted_index.id() << "] success";
        }

      } else if (deleted_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = deleted_index_meta_->Put(deleted_index.id(), deleted_index.table());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement deleted_index UPDATE, [id=" << deleted_index.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_index UPDATE, [id=" << deleted_index.id() << "] success";
        }

      } else if (deleted_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = deleted_index_meta_->Erase(deleted_index.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement deleted_index DELETE, [id=" << deleted_index.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement deleted_index DELETE, [id=" << deleted_index.id() << "] success";
        }
      }
    }
  }

  // 13.index_metrics map
  // index_metrics_map is a temp map, only used in memory, so we don't need to update it in raft apply

  // 51.common_disk_map
  {
    if (meta_increment.common_disk_s_size() > 0) {
      DINGO_LOG(INFO) << "6.common_disk_s_size=" << meta_increment.common_disk_s_size();
    }

    for (int i = 0; i < meta_increment.common_disk_s_size(); i++) {
      const auto& common = meta_increment.common_disk_s(i);
      if (common.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = common_disk_meta_->Put(common.id(), common.common());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement common_disk CREATE, [id=" << common.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement common_disk CREATE, [id=" << common.id() << "] success";
        }

      } else if (common.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = common_disk_meta_->Put(common.id(), common.common());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement common_disk UPDATE, [id=" << common.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement common_disk UPDATE, [id=" << common.id() << "] success";
        }

      } else if (common.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = common_disk_meta_->Erase(common.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement common_disk DELETE, [id=" << common.id() << "] failed";
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement common_disk DELETE, [id=" << common.id() << "] success";
        }
      }
    }
  }

  // 51.2 common_mem_map
  {
    if (meta_increment.common_mem_s_size() > 0) {
      DINGO_LOG(INFO) << "3.common_mem_s_size=" << meta_increment.common_mem_s_size();
    }

    for (int i = 0; i < meta_increment.common_mem_s_size(); i++) {
      const auto& common_mem = meta_increment.common_mem_s(i);
      if (common_mem.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
        auto ret = common_mem_meta_->Put(common_mem.id(), common_mem.common());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement common_mem CREATE, but Put failed, [id=" << common_mem.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement common_mem CREATE, success [id=" << common_mem.id() << "]";
        }

      } else if (common_mem.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = common_mem_meta_->Put(common_mem.id(), common_mem.common());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement common_mem UPDATE, but Put failed, [id=" << common_mem.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement common_mem UPDATE, success [id=" << common_mem.id() << "]";
        }

      } else if (common_mem.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = common_mem_meta_->Erase(common_mem.id());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement common_mem DELETE, but Delete failed, [id=" << common_mem.id()
                           << "], errcode: " << ret.error_code() << ", errmsg: " << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement common_mem DELETE, success [id=" << common_mem.id() << "]";
        }
      }
    }
  }

  // update applied term & index after all fsm apply is fininshed
  id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM, term);
  id_epoch_map_.UpdatePresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX, index);

  meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(raft_apply_term));
  meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(raft_apply_index));

  // write update to local engine, begin
  if ((!meta_write_to_kv.empty()) || (!meta_delete_to_kv.empty())) {
    if (!meta_writer_->PutAndDelete(meta_write_to_kv, meta_delete_to_kv)) {
      DINGO_LOG(ERROR) << "ApplyMetaIncrement PutAndDelete failed, exit program";
      exit(-1);
    }
  }
  // write update to local engine, end

  // send event_list to all watchers
  auto meta_watch_send_task = std::make_shared<MetaWatchSendTask>(this, meta_revision, event_list, watch_bitset);
  auto ret3 = meta_watch_worker_set_->ExecuteRR(meta_watch_send_task);
  if (!ret3) {
    DINGO_LOG(ERROR) << "ApplyMetaIncrement meta_watch_send_task ExecuteRR failed";
  }
}

butil::Status CoordinatorControl::SubmitMetaIncrementSync(pb::coordinator_internal::MetaIncrement& meta_increment) {
  LogMetaIncrementSize(meta_increment);

  std::shared_ptr<Context> const ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kMetaRegionId);

  auto status = engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "SubmitMetaIncrement failed, errno=" << status.error_code() << " errmsg=" << status.error_str();
    return status;
  }
  return butil::Status::OK();
}

}  // namespace dingodb

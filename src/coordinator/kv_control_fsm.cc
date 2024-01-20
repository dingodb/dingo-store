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

#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "coordinator/kv_control.h"
#include "engine/snapshot.h"
#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "proto/node.pb.h"
#include "proto/version.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

bool KvControl::IsLeader() { return leader_term_.load(butil::memory_order_acquire) > 0; }

void KvControl::SetLeaderTerm(int64_t term) {
  DINGO_LOG(INFO) << "SetLeaderTerm, term=" << term;
  leader_term_.store(term, butil::memory_order_release);
}

void KvControl::SetRaftNode(std::shared_ptr<RaftNode> raft_node) { raft_node_ = raft_node; }
std::shared_ptr<RaftNode> KvControl::GetRaftNode() { return raft_node_; }

int KvControl::GetAppliedTermAndIndex(int64_t& term, int64_t& index) {
  id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_TERM, term);
  id_epoch_map_.GetPresentId(pb::coordinator_internal::IdEpochType::RAFT_APPLY_INDEX, index);

  DINGO_LOG(INFO) << "GetAppliedTermAndIndex, term=" << term << ", index=" << index;

  return 0;
}

void KvControl::BuildTempMaps() {
  // // copy id_epoch_map_ to id_epoch_map_temp_
  // {
  //   butil::FlatMap<int64_t, pb::coordinator_internal::IdEpochInternal> temp_copy;
  //   temp_copy.init(100);
  //   id_epoch_map_.GetRawMapCopy(temp_copy);
  //   id_epoch_map_safe_temp_.CopyFromRawMap(temp_copy);
  // }
  // DINGO_LOG(INFO) << "id_epoch_safe_map_temp, count=" << id_epoch_map_safe_temp_.Size();
}

// OnLeaderStart will init id_epoch_map_temp_ from id_epoch_map_ which is in state machine
void KvControl::OnLeaderStart(int64_t term) {
  DINGO_LOG(INFO) << "OnLeaderStart start, term=" << term;
  // build id_epoch, schema_name, table_name, index_name maps
  BuildTempMaps();

  // build lease_to_key_map_temp_
  BuildLeaseToKeyMap();

  // clear one time watch map
  one_time_watch_closure_status_map_.Clear();
  {
    BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);
    one_time_watch_map_.clear();
    one_time_watch_closure_map_.clear();
  }

  DINGO_LOG(INFO) << "OnLeaderStart init lease_to_key_map_temp_ finished, term=" << term
                  << " count=" << lease_to_key_map_temp_.size();

  DINGO_LOG(INFO) << "OnLeaderStart finished, term=" << term;
}

void KvControl::OnLeaderStop() {
  DINGO_LOG(INFO) << "OnLeaderStop start";
  // clear one time watch map
  one_time_watch_closure_status_map_.Clear();
  {
    BAIDU_SCOPED_LOCK(one_time_watch_map_mutex_);

    for (auto& it : one_time_watch_closure_map_) {
      it.second.Done();
    }

    one_time_watch_map_.clear();
    one_time_watch_closure_map_.clear();
  }

  DINGO_LOG(INFO) << "OnLeaderStop finished";
}

std::shared_ptr<Snapshot> KvControl::PrepareRaftSnapshot() {
  DINGO_LOG(INFO) << "PrepareRaftSnapshot";
  return raw_engine_of_meta_->GetSnapshot();
}

bool KvControl::LoadMetaToSnapshotFile(std::shared_ptr<Snapshot> snapshot,
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

  // 14.lease_map_
  if (!meta_reader_->Scan(snapshot, kv_lease_meta_->Prefix(), kvs)) {
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

  return true;
}

bool KvControl::LoadMetaFromSnapshotFile(pb::coordinator_internal::MetaSnapshotFile& meta_snapshot_file) {
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

  // 14.lease_map_
  kvs.reserve(meta_snapshot_file.lease_map_kvs_size());
  for (int i = 0; i < meta_snapshot_file.lease_map_kvs_size(); i++) {
    kvs.push_back(meta_snapshot_file.lease_map_kvs(i));
  }
  {
    // BAIDU_SCOPED_LOCK(lease_map_mutex_);
    if (!kv_lease_meta_->Recover(kvs)) {
      return false;
    }

    // remove data in rocksdb
    if (!meta_writer_->DeletePrefix(kv_lease_meta_->internal_prefix)) {
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
    // if (!kv_rev_meta_->Recover(kvs)) {
    //   return false;
    // }

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

  // build id_epoch, schema_name, table_name, index_name maps
  BuildTempMaps();

  // build lease_to_key_map_temp_
  BuildLeaseToKeyMap();

  DINGO_LOG(INFO) << "LoadSnapshot lease_to_key_map_temp, count=" << lease_to_key_map_temp_.size();

  return true;
}

void KvControl::KvLogMetaIncrementSize(pb::coordinator_internal::MetaIncrement& meta_increment) {
  if (meta_increment.ByteSizeLong() > 0) {
    DINGO_LOG(DEBUG) << "meta_increment byte_size=" << meta_increment.ByteSizeLong();
  } else {
    return;
  }
  if (meta_increment.idepochs_size() > 0) {
    DINGO_LOG(DEBUG) << "0.idepochs_size=" << meta_increment.idepochs_size();
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
void KvControl::ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement& meta_increment, bool /*is_leader*/,
                                   int64_t term, int64_t index, google::protobuf::Message* response) {
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
        << "[term=" << term << "][applied_term=" << applied_term;
    return;
  } else if (meta_increment.ByteSizeLong() > 0) {
    DINGO_LOG(INFO) << "NORMAL ApplyMetaIncrement [index=" << index << "][applied_index=" << applied_index << "]"
                    << "[term=" << term << "][applied_term=" << applied_term << "]";
    KvLogMetaIncrementSize(meta_increment);
  } else {
    DINGO_LOG(WARNING) << "meta_increment.ByteSizeLong() == 0, just return";
    return;
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
        auto ret = kv_lease_meta_->Put(version_lease.id(), version_lease.lease());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement version_lease CREATE, [id=" << version_lease.id()
                           << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement version_lease CREATE, [id=" << version_lease.id() << "] success";
        }

      } else if (version_lease.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        auto ret = kv_lease_meta_->Put(version_lease.id(), version_lease.lease());
        if (!ret.ok()) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement version_lease UPDATE, [id=" << version_lease.id()
                           << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement version_lease UPDATE, [id=" << version_lease.id() << "] success";
        }

      } else if (version_lease.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = kv_lease_meta_->Erase(version_lease.id());
        if (!ret.ok() && ret.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement version_lease DELETE, [id=" << version_lease.id()
                           << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement version_lease DELETE, [id=" << version_lease.id() << "] success";
        }
      }
    }
  }

  // we only update kv_rev_map_ in KvPutApply and KvDeleteApply, so we don't need to update kv_rev_map_ here, the code
  // below will be removed in future
  // 16.kv_rev_map_
  // {
  //   if (meta_increment.kv_revs_size() > 0) {
  //     DINGO_LOG(INFO) << "ApplyMetaIncrement kv_revs size=" << meta_increment.kv_revs_size();
  //   }

  //   for (int i = 0; i < meta_increment.kv_revs_size(); i++) {
  //     const auto& kv_rev = meta_increment.kv_revs(i);
  //     if (kv_rev.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
  //       auto ret = kv_rev_meta_->Put(kv_rev.id(), kv_rev.kv_rev());
  //       if (!ret.ok()) {
  //         DINGO_LOG(FATAL) << "ApplyMetaIncrement kv_rev CREATE, [id=" << kv_rev.id()
  //                          << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
  //       } else {
  //         DINGO_LOG(INFO) << "ApplyMetaIncrement kv_rev CREATE, [id=" << kv_rev.id() << "] success";
  //       }
  //     } else if (kv_rev.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
  //       auto ret = kv_rev_meta_->PutIfExists(kv_rev.id(), kv_rev.kv_rev());
  //       if (!ret.ok() && ret.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
  //         DINGO_LOG(FATAL) << "ApplyMetaIncrement kv_rev UPDATE, [id=" << kv_rev.id()
  //                          << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
  //       } else {
  //         DINGO_LOG(INFO) << "ApplyMetaIncrement kv_rev UPDATE, [id=" << kv_rev.id() << "] success";
  //       }

  //     } else if (kv_rev.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
  //       auto ret = kv_rev_meta_->Erase(kv_rev.id());
  //       if (!ret.ok() && ret.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
  //         DINGO_LOG(FATAL) << "ApplyMetaIncrement kv_rev DELETE, [id=" << kv_rev.id()
  //                          << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
  //       } else {
  //         DINGO_LOG(INFO) << "ApplyMetaIncrement kv_rev DELETE, [id=" << kv_rev.id() << "] success";
  //       }
  //     }
  //   }
  // }

  // 15.kv_index_map_
  {
    if (meta_increment.kv_indexes_size() > 0) {
      DINGO_LOG(INFO) << "ApplyMetaIncrement kv_indexes size=" << meta_increment.kv_indexes_size();
    }

    // prepare new_op_revision for KvPut and KvDelete
    pb::coordinator_internal::RevisionInternal new_op_revision;
    int64_t sub_revision = 1;
    bool new_revision_ok = false;

    for (int i = 0; i < meta_increment.kv_indexes_size(); i++) {
      const auto& kv_index = meta_increment.kv_indexes(i);
      if (kv_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
      } else if (kv_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::UPDATE) {
        if (kv_index.event_type() == pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_PUT) {
          // treat new_op_revision
          if (!new_revision_ok) {
            new_op_revision.set_main(
                GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION, meta_increment));
            new_op_revision.set_sub(sub_revision++);
            new_revision_ok = true;
          } else {
            new_op_revision.set_sub(sub_revision++);
          }

          // call KvPutApply
          auto ret = KvPutApply(kv_index.id(), new_op_revision, kv_index.ignore_lease(), kv_index.lease_id(),
                                kv_index.ignore_value(), kv_index.value());
          if (ret.ok()) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement kv_index UPDATE,PUT [id=" << kv_index.id() << "] success";

            // setup response for leader
            if (response != nullptr) {
              auto* put_response = dynamic_cast<pb::version::PutResponse*>(response);
              if (put_response != nullptr) {
                put_response->mutable_header()->set_revision(new_op_revision.main());
              }
            }
          } else {
            DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_index UPDATE,PUT [id=" << kv_index.id()
                               << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
            Helper::SetPbMessageError(ret, response);
          }
        } else if (kv_index.event_type() == pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_DELETE) {
          // treat new_op_revision
          if (!new_revision_ok) {
            new_op_revision.set_main(
                GetNextId(pb::coordinator_internal::IdEpochType::ID_NEXT_REVISION, meta_increment));
            new_op_revision.set_sub(sub_revision++);
            new_revision_ok = true;
          } else {
            new_op_revision.set_sub(sub_revision++);
          }

          // call KvDeleteApply
          auto ret = KvDeleteApply(kv_index.id(), new_op_revision);
          if (ret.ok()) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement kv_index UPDATE,DELETE [id=" << kv_index.id() << "] success";

            // setup response for leader
            if (response != nullptr) {
              auto* delete_range_response = dynamic_cast<pb::version::DeleteRangeResponse*>(response);
              if (delete_range_response != nullptr) {
                delete_range_response->mutable_header()->set_revision(new_op_revision.main());
              }
            }
          } else {
            DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_index UPDATE,DELETE [id=" << kv_index.id()
                               << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
            Helper::SetPbMessageError(ret, response);
          }
        } else if (kv_index.event_type() ==
                   pb::coordinator_internal::KvIndexEventType::KV_INDEX_EVENT_TYPE_COMPACTION) {
          // call KvCompactApply
          auto ret = KvCompactApply(kv_index.id(), kv_index.op_revision());
          if (ret.ok()) {
            DINGO_LOG(INFO) << "ApplyMetaIncrement kv_index UPDATE,COMPACT [id=" << kv_index.id() << "] success";
          } else {
            DINGO_LOG(WARNING) << "ApplyMetaIncrement kv_index UPDATE,COMPACT [id=" << kv_index.id()
                               << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
            Helper::SetPbMessageError(ret, response);
          }
        }
      } else if (kv_index.op_type() == pb::coordinator_internal::MetaIncrementOpType::DELETE) {
        auto ret = kv_index_meta_->Erase(kv_index.id());
        if (!ret.ok() && ret.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(FATAL) << "ApplyMetaIncrement kv_index DELETE, [id=" << kv_index.id()
                           << "] failed, set errcode=" << ret.error_code() << ", error_str=" << ret.error_str();
        } else {
          DINGO_LOG(INFO) << "ApplyMetaIncrement kv_index DELETE, [id=" << kv_index.id() << "] success";
        }
      }
    }
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

  for (int i = 0; i < meta_increment.idepochs_size(); i++) {
    const auto& idepoch = meta_increment.idepochs(i);
    if (idepoch.op_type() == pb::coordinator_internal::MetaIncrementOpType::CREATE) {
      int ret = id_epoch_map_.UpdatePresentId(idepoch.id(), idepoch.idepoch().value());
      if (ret >= 0) {
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
      if (ret >= 0) {
        DINGO_LOG(INFO) << "ApplyMetaIncrement idepoch UPDATE, success [id="
                        << pb::coordinator_internal::IdEpochType_Name(idepoch.id()) << "]"
                        << " value=" << idepoch.idepoch().value();
        meta_write_to_kv.push_back(id_epoch_meta_->TransformToKvValue(idepoch.idepoch()));
      } else {
        DINGO_LOG(INFO) << "ApplyMetaIncrement idepoch UPDATE, but UpdatePresentId skip, [id="
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
}

// SubmitMetaIncrement
// commit meta increment to raft meta engine, with no closure
butil::Status KvControl::SubmitMetaIncrementAsync(google::protobuf::Closure* done,
                                                  pb::coordinator_internal::MetaIncrement& meta_increment) {
  KvLogMetaIncrementSize(meta_increment);

  std::shared_ptr<Context> const ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kKvRegionId);

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

butil::Status KvControl::SubmitMetaIncrementSync(pb::coordinator_internal::MetaIncrement& meta_increment) {
  KvLogMetaIncrementSize(meta_increment);

  std::shared_ptr<Context> const ctx = std::make_shared<Context>();
  ctx->SetRegionId(Constant::kKvRegionId);

  auto status = engine_->Write(ctx, WriteDataBuilder::BuildWrite(ctx->CfName(), meta_increment));
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "SubmitMetaIncrement failed, errno=" << status.error_code() << " errmsg=" << status.error_str();
    return status;
  }
  return butil::Status::OK();
}

}  // namespace dingodb

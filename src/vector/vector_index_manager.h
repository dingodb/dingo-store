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

#ifndef DINGODB_VECTOR_INDEX_MANAGER_H_
#define DINGODB_VECTOR_INDEX_MANAGER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/safe_map.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "vector/vector_index.h"
#include "vector/vector_index_snapshot.h"

namespace dingodb {

class VectorIndexManager {
 public:
  VectorIndexManager(std::shared_ptr<RawEngine> raw_engine, std::shared_ptr<MetaReader> meta_reader,
                     std::shared_ptr<MetaWriter> meta_writer)
      : raw_engine_(raw_engine), meta_reader_(meta_reader), meta_writer_(meta_writer) {
    vector_index_snapshot_manager_ = std::make_shared<VectorIndexSnapshotManager>();
    vector_indexs_.Init(1000);
  }

  ~VectorIndexManager() = default;

  bool Init(std::vector<store::RegionPtr> regions);

  bool AddVectorIndex(uint64_t vector_index_id, const pb::common::IndexParameter& index_parameter);

  void DeleteVectorIndex(uint64_t vector_index_id);

  std::shared_ptr<VectorIndex> GetVectorIndex(uint64_t vector_index_id);
  std::shared_ptr<VectorIndex> GetVectorIndex(store::RegionPtr region);
  std::vector<std::shared_ptr<VectorIndex>> GetAllVectorIndex();

  // Load vector index for already exist vector index at bootstrap.
  // Priority load from snapshot, if snapshot not exist then load from rocksdb.
  butil::Status LoadOrBuildVectorIndex(uint64_t region_id);
  butil::Status LoadOrBuildVectorIndex(store::RegionPtr region);
  butil::Status ParallelLoadOrBuildVectorIndex(std::vector<store::RegionPtr> regions, int concurrency);

  // Save vector index snapshot.
  butil::Status SaveVectorIndex(std::shared_ptr<VectorIndex> vector_index);

  // Invoke when server runing.
  butil::Status RebuildVectorIndex(store::RegionPtr region, bool need_save = true);
  butil::Status AsyncRebuildVectorIndex(store::RegionPtr region, bool need_save = true);

  // Update vector index apply log index.
  void UpdateApplyLogId(std::shared_ptr<VectorIndex> vector_index, uint64_t log_index);
  void UpdateApplyLogId(uint64_t vector_index_id, uint64_t log_index);

  // Update vector index snapshot log index.
  void UpdateSnapshotLogId(std::shared_ptr<VectorIndex> vector_index, uint64_t log_index);
  void UpdateSnapshotLogId(uint64_t vector_index_id, uint64_t log_index);

  butil::Status ScrubVectorIndex();

  std::shared_ptr<VectorIndexSnapshotManager> GetVectorIndexSnapshotManager() { return vector_index_snapshot_manager_; }

 private:
  void SaveApplyLogId(uint64_t vector_index_id, uint64_t apply_log_id);
  butil::Status LoadApplyLogId(uint64_t vector_index_id, uint64_t& apply_log_id);
  void SaveSnapshotLogId(uint64_t vector_index_id, uint64_t snapshot_log_id);
  butil::Status LoadSnapshotLogId(uint64_t vector_index_id, uint64_t& snapshot_log_id);

  butil::Status GetVectorIndexLogIndex(uint64_t vector_index_id, uint64_t& snapshot_log_index,
                                       uint64_t& apply_log_index);

  bool AddVectorIndex(std::shared_ptr<VectorIndex> vector_index, bool force = true);

  // Build vector index with original all data(store rocksdb).
  // Invoke when server starting.
  std::shared_ptr<VectorIndex> BuildVectorIndex(store::RegionPtr region);

  // Replay log to vector index.
  static butil::Status ReplayWalToVectorIndex(std::shared_ptr<VectorIndex> vector_index, uint64_t start_log_id,
                                              uint64_t end_log_id);

  // Scrub vector index.
  butil::Status ScrubVectorIndex(store::RegionPtr region, bool need_rebuild, bool need_save);

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  std::shared_ptr<RawEngine> raw_engine_;
  // vector_index_id: vector_index
  DingoSafeMap<uint64_t, std::shared_ptr<VectorIndex>> vector_indexs_;

  // vector index snapshot manager
  std::shared_ptr<VectorIndexSnapshotManager> vector_index_snapshot_manager_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_MANAGER_H_

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

#ifndef DINGODB_VECTOR_INDEX_SNAPSHOT_H_
#define DINGODB_VECTOR_INDEX_SNAPSHOT_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "butil/endpoint.h"
#include "butil/status.h"
#include "common/context.h"
#include "engine/snapshot.h"
#include "meta/store_meta_manager.h"
#include "proto/node.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

namespace vector_index {

// Indicate a vector index snapshot
class SnapshotMeta {
 public:
  SnapshotMeta(uint64_t vector_index_id, const std::string& path);
  ~SnapshotMeta();

  static std::shared_ptr<SnapshotMeta> New(uint64_t vector_index_id, const std::string& path) {
    return std::make_shared<SnapshotMeta>(vector_index_id, path);
  }

  bool Init();
  void Destroy();
  bool IsDestroy();

  void IncUseRefferenceCount();
  void DescUseRefferenceCount();

  uint64_t VectorIndexId() const { return vector_index_id_; }
  uint64_t SnapshotLogId() const { return snapshot_log_id_; }
  std::string Path() const { return path_; }
  std::string MetaPath();
  std::string IndexDataPath();
  std::vector<std::string> ListFileNames();

 private:
  uint64_t vector_index_id_;
  uint64_t snapshot_log_id_;
  std::string path_;

  std::atomic<bool> is_destroy_;

  bthread_mutex_t mutex_;
  // When using_reference_count_>0, don't allow destroy.
  int using_reference_count_;
};

using SnapshotMetaPtr = std::shared_ptr<SnapshotMeta>;

}  // namespace vector_index

class VectorIndexSnapshotManager {
 public:
  VectorIndexSnapshotManager() { bthread_mutex_init(&mutex_, nullptr); }
  ~VectorIndexSnapshotManager() { bthread_mutex_destroy(&mutex_); }

  bool Init(std::vector<store::RegionPtr> regions);

  // Launch install snapshot at client.
  static butil::Status LaunchInstallSnapshot(const butil::EndPoint& endpoint, uint64_t vector_index_id);
  // Handle install snapshot at server.
  static butil::Status HandleInstallSnapshot(std::shared_ptr<Context> ctx, const std::string& uri,
                                             const pb::node::VectorIndexSnapshotMeta& meta);
  // Install snapshot to all followers.
  static butil::Status InstallSnapshotToFollowers(uint64_t region_id);

  // Launch pull snapshot at client.
  static butil::Status LaunchPullSnapshot(const butil::EndPoint& endpoint, uint64_t vector_index_id);
  // Handle install snapshot at server.
  static butil::Status HandlePullSnapshot(std::shared_ptr<Context> ctx, uint64_t vector_index_id);
  // Pull last snapshot from peers.
  static butil::Status PullLastSnapshotFromPeers(uint64_t region_id);

  bool IsExistVectorIndexSnapshot(uint64_t vector_index_id);

  // Save vecgor index snapshot.
  static butil::Status SaveVectorIndexSnapshot(std::shared_ptr<VectorIndex> vector_index, uint64_t& snapshot_log_index);

  // Load vector index from snapshot.
  static std::shared_ptr<VectorIndex> LoadVectorIndexSnapshot(store::RegionPtr region);

  bool AddSnapshot(vector_index::SnapshotMetaPtr snapshot);
  void DeleteSnapshot(vector_index::SnapshotMetaPtr snapshot);
  vector_index::SnapshotMetaPtr GetLastSnapshot(uint64_t vector_index_id);
  std::vector<vector_index::SnapshotMetaPtr> GetSnapshots(uint64_t vector_index_id);
  bool IsExistSnapshot(uint64_t vector_index_id, uint64_t snapshot_log_id);

 private:
  static std::string GetSnapshotParentPath(uint64_t vector_index_id);
  static std::string GetSnapshotTmpPath(uint64_t vector_index_id);
  static std::string GetSnapshotNewPath(uint64_t vector_index_id, uint64_t snapshot_log_id);
  static butil::Status DownloadSnapshotFile(const std::string& uri, const pb::node::VectorIndexSnapshotMeta& meta);

  bthread_mutex_t mutex_;
  std::map<uint64_t, std::map<uint64_t, vector_index::SnapshotMetaPtr>> snapshot_maps_;
};

}  // namespace dingodb

#endif
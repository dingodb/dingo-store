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

#ifndef DINGODB_VECTOR_INDEX_SNAPSHOT_MANAGER_H_
#define DINGODB_VECTOR_INDEX_SNAPSHOT_MANAGER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/endpoint.h"
#include "butil/status.h"
#include "proto/node.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

class VectorIndexSnapshotManager {
 public:
  VectorIndexSnapshotManager() = delete;
  ~VectorIndexSnapshotManager() = delete;

  // bool Init(std::vector<store::RegionPtr> regions);

  // Launch install snapshot at client.
  static butil::Status LaunchInstallSnapshot(const butil::EndPoint& endpoint, vector_index::SnapshotMetaPtr snapshot);
  // Handle install snapshot at server.
  static butil::Status HandleInstallSnapshot(const std::string& uri, const pb::node::VectorIndexSnapshotMeta& meta,
                                             vector_index::SnapshotMetaSetPtr snapshot_set);
  // Install snapshot to all followers.
  static butil::Status InstallSnapshotToFollowers(vector_index::SnapshotMetaPtr snapshot);

  // Launch pull snapshot at client.
  static butil::Status LaunchPullSnapshot(const butil::EndPoint& endpoint,
                                          vector_index::SnapshotMetaSetPtr snapshot_set);
  // Handle install snapshot at server.
  static butil::Status HandlePullSnapshot(vector_index::SnapshotMetaPtr snapshot,
                                          pb::node::GetVectorIndexSnapshotResponse* response);
  // Pull last snapshot from peers.
  static butil::Status PullLastSnapshotFromPeers(vector_index::SnapshotMetaSetPtr snapshot_set,
                                                 const pb::common::RegionEpoch& epoch);

  // Save vecgor index snapshot.
  static butil::Status SaveVectorIndexSnapshot(VectorIndexWrapperPtr vector_index, int64_t& snapshot_log_index);

  // Load vector index from snapshot.
  static std::shared_ptr<VectorIndex> LoadVectorIndexSnapshot(VectorIndexWrapperPtr vector_index_wrapper,
                                                              const pb::common::RegionEpoch& epoch);

  static std::string GetSnapshotParentPath(int64_t vector_index_id);

  static std::vector<std::string> GetSnapshotList(int64_t vector_index_id);

 private:
  static std::string GetSnapshotTmpPath(int64_t vector_index_id);
  static std::string GetSnapshotNewPath(int64_t vector_index_id, int64_t snapshot_log_id);
  static butil::Status DownloadSnapshotFile(const std::string& uri, const pb::node::VectorIndexSnapshotMeta& meta,
                                            vector_index::SnapshotMetaSetPtr snapshot_set);
};

}  // namespace dingodb

#endif
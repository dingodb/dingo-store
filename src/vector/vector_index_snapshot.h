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

#include <cstdint>

#include "butil/endpoint.h"
#include "butil/status.h"
#include "common/context.h"
#include "meta/store_meta_manager.h"
#include "proto/node.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

class VectorIndexSnapshot {
 public:
  // Launch install snapshot at client.
  static butil::Status LaunchInstallSnapshot(const butil::EndPoint& endpoint, uint64_t vector_index_id);
  // Handle install snapshot at server.
  static butil::Status HandleInstallSnapshot(std::shared_ptr<Context> ctx, const std::string& uri,
                                             const pb::node::VectorIndexSnapshotMeta& meta);

  // Launch pull snapshot at client.
  static butil::Status LaunchPullSnapshot(const butil::EndPoint& endpoint, uint64_t vector_index_id);
  // Handle install snapshot at server.
  static butil::Status HandlePullSnapshot(std::shared_ptr<Context> ctx, uint64_t vector_index_id);

  static bool IsExistVectorIndexSnapshot(uint64_t vector_index_id);
  static uint64_t GetLastVectorIndexSnapshotLogId(uint64_t vector_index_id);

  // Save vecgor index snapshot.
  static butil::Status SaveVectorIndexSnapshot(std::shared_ptr<VectorIndex> vector_index, uint64_t& snapshot_log_index);
  // Load vector index from snapshot.
  static std::shared_ptr<VectorIndex> LoadVectorIndexSnapshot(store::RegionPtr region);

 private:
  static butil::Status DownloadSnapshotFile(const std::string& uri, const pb::node::VectorIndexSnapshotMeta& meta);
};

}  // namespace dingodb

#endif
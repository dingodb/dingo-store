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

#ifndef DINGODB_DOCUMENT_INDEX_SNAPSHOT_H_
#define DINGODB_DOCUMENT_INDEX_SNAPSHOT_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "proto/common.pb.h"

namespace dingodb {

namespace document_index {

// Indicate a document index snapshot
class SnapshotMeta {
 public:
  SnapshotMeta(int64_t document_index_id, const std::string& path);
  ~SnapshotMeta() = default;

  static std::shared_ptr<SnapshotMeta> New(int64_t document_index_id, const std::string& path) {
    return std::make_shared<SnapshotMeta>(document_index_id, path);
  }

  bool Init();

  int64_t DocumentIndexId() const { return document_index_id_; }
  int64_t SnapshotLogId() const { return snapshot_log_id_; }
  std::string Path() const { return path_; }
  std::string MetaPath();
  std::string IndexDataPath();
  std::vector<std::string> ListFileNames();

  pb::common::RegionEpoch Epoch() const { return epoch_; }
  pb::common::Range Range() const { return range_; }

  void Destroy();

 private:
  int64_t document_index_id_;
  int64_t snapshot_log_id_;
  std::string path_;

  pb::common::RegionEpoch epoch_;
  pb::common::Range range_;

  std::atomic<bool> is_destroied_{false};
};

using SnapshotMetaPtr = std::shared_ptr<SnapshotMeta>;

class SnapshotMetaSet {
 public:
  SnapshotMetaSet(int64_t document_index_id, const std::string& path);
  ~SnapshotMetaSet();

  static std::shared_ptr<SnapshotMetaSet> New(int64_t document_index_id, const std::string& path);

  int64_t DocumentIndexId() const { return document_index_id_; }

  bool AddSnapshot(SnapshotMetaPtr snapshot);
  void ClearSnapshot();
  document_index::SnapshotMetaPtr GetLastSnapshot();
  std::vector<document_index::SnapshotMetaPtr> GetSnapshots();
  bool IsExistSnapshot(int64_t snapshot_log_id);
  bool IsExistLastSnapshot();

  void Destroy();

 private:
  void ClearSnapshotImpl();

  int64_t document_index_id_;
  std::string path_;

  bthread_mutex_t mutex_;
  // doucment index snapshots, key: log_id
  std::map<int64_t, SnapshotMetaPtr> snapshots_;
};

using SnapshotMetaSetPtr = std::shared_ptr<SnapshotMetaSet>;

}  // namespace document_index

}  // namespace dingodb

#endif
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

#ifndef DINGODB_DOCUMENT_INDEX_SNAPSHOT_MANAGER_H_
#define DINGODB_DOCUMENT_INDEX_SNAPSHOT_MANAGER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "document/document_index.h"
#include "proto/common.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

class DocumentIndexSnapshotManager {
 public:
  DocumentIndexSnapshotManager() = delete;
  ~DocumentIndexSnapshotManager() = delete;

  // bool Init(std::vector<store::RegionPtr> regions);

  // Save document index snapshot.
  static butil::Status SaveDocumentIndexSnapshot(DocumentIndexWrapperPtr document_index, int64_t& snapshot_log_index);

  // Load vector index from snapshot.
  static std::shared_ptr<DocumentIndex> LoadDocumentIndexSnapshot(DocumentIndexWrapperPtr document_index_wrapper,
                                                                  const pb::common::RegionEpoch& epoch);

  static std::string GetSnapshotParentPath(int64_t document_index_id);

  static std::string GetSnapshotPath(int64_t document_index_id, int64_t epoch_version);

  static std::vector<std::string> GetSnapshotList(int64_t document_index_id);

  static std::string GetSnapshotPath(int64_t document_index_id, const pb::common::RegionEpoch& epoch);

  //   static int64_t GetSnapshotApplyLogId(const std::string& document_index_path);

  static std::string GetLatestSnapshotPath(int64_t document_index_id);

  static butil::Status GetLatestSnapshotMeta(int64_t document_index_id,
                                             pb::store_internal::DocumentIndexSnapshotMeta& meta);

 private:
  static std::string GetSnapshotTmpPath(int64_t document_index_id);
  static std::string GetSnapshotNewPath(int64_t document_index_id, int64_t snapshot_log_id);
};

}  // namespace dingodb

#endif
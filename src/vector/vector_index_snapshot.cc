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

#include "vector/vector_index_snapshot.h"

#include <sys/wait.h>  // Add this include

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "braft/protobuf_file.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"

namespace dingodb {

namespace vector_index {

SnapshotMeta::SnapshotMeta(int64_t vector_index_id, const std::string& path)
    : vector_index_id_(vector_index_id), path_(path) {}

SnapshotMeta::~SnapshotMeta() {
  // Delete directory
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.snapshot][index_id({})] Delete snapshot, epoch: {} snapshot_index_id: {} path: {}.",
      vector_index_id_, Helper::RegionEpochToString(epoch_), snapshot_log_id_, path_);
  Helper::RemoveAllFileOrDirectory(path_);
}

bool SnapshotMeta::Init() {
  std::filesystem::path path(path_);
  path.filename();

  int64_t snapshot_index_id = 0;
  // int match = sscanf(path.filename().c_str(), "snapshot_%020" PRId64, &snapshot_index_id);  // NOLINT
  try {
    char* endptr;
    snapshot_index_id = strtoull(path.filename().c_str() + 9, &endptr, 10);
    if (*endptr != '\0') {
      DINGO_LOG(ERROR) << fmt::format(
          "[vector_index.snapshot][index_id({})] Parse snapshot index id failed from snapshot name, path: {}",
          vector_index_id_, path_);
      return false;
    }
  } catch (const std::exception& e) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.snapshot][index_id({})] Parse snapshot index id failed from snapshot name, path: {}",
        vector_index_id_, path_);
    return false;
  }

  snapshot_log_id_ = snapshot_index_id;

  pb::store_internal::VectorIndexSnapshotMeta meta;
  braft::ProtoBufFile pb_file_meta(MetaPath());
  if (pb_file_meta.load(&meta) != 0) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.snapshot][index_id({})] Parse vector index snapshot meta failed, path: {}", vector_index_id_,
        path_);
    return false;
  }

  epoch_ = meta.epoch();
  range_ = meta.range();

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.snapshot][index_id({})] Load snapshot meta, epoch: {} snapshot_index_id: {}, path: {}",
      vector_index_id_, Helper::RegionEpochToString(epoch_), snapshot_index_id, path_);

  return true;
}

std::string SnapshotMeta::MetaPath() { return fmt::format("{}/meta", path_); }

std::string SnapshotMeta::IndexDataPath() {
  return fmt::format("{}/index_{}_{}.idx", path_, vector_index_id_, snapshot_log_id_);
}

std::vector<std::string> SnapshotMeta::ListFileNames() { return Helper::TraverseDirectory(path_); }

SnapshotMetaSet::SnapshotMetaSet(int64_t vector_index_id) : vector_index_id_(vector_index_id) {
  bthread_mutex_init(&mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.SnapshotMetaSet][id({})]", vector_index_id_);
}

SnapshotMetaSet::~SnapshotMetaSet() {
  DINGO_LOG(DEBUG) << fmt::format("[delete.SnapshotMetaSet][id({})]", vector_index_id_);
  bthread_mutex_destroy(&mutex_);
}

std::shared_ptr<SnapshotMetaSet> SnapshotMetaSet::New(int64_t vector_index_id) {
  return std::make_shared<SnapshotMetaSet>(vector_index_id);
}

bool SnapshotMetaSet::AddSnapshot(SnapshotMetaPtr snapshot) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (snapshots_.find(snapshot->SnapshotLogId()) == snapshots_.end()) {
    // Delete stale snapshot
    snapshots_.clear();
    snapshots_[snapshot->SnapshotLogId()] = snapshot;
  } else {
    DINGO_LOG(WARNING) << fmt::format("Already exist vector index snapshot {} {}", snapshot->VectorIndexId(),
                                      snapshot->SnapshotLogId());
    return false;
  }

  return true;
}

void SnapshotMetaSet::ClearSnapshot() {
  BAIDU_SCOPED_LOCK(mutex_);

  snapshots_.clear();
}

SnapshotMetaPtr SnapshotMetaSet::GetLastSnapshot() {
  BAIDU_SCOPED_LOCK(mutex_);

  return !snapshots_.empty() ? snapshots_.rbegin()->second : nullptr;
}

std::vector<SnapshotMetaPtr> SnapshotMetaSet::GetSnapshots() {
  BAIDU_SCOPED_LOCK(mutex_);

  std::vector<SnapshotMetaPtr> result;
  result.reserve(snapshots_.size());
  for (auto& [_, snapshot] : snapshots_) {
    result.push_back(snapshot);
  }

  return result;
}

bool SnapshotMetaSet::IsExistSnapshot(int64_t snapshot_log_id) {
  auto snapshot = GetLastSnapshot();
  if (snapshot == nullptr) {
    return false;
  }

  return snapshot_log_id <= snapshot->SnapshotLogId();
}

bool SnapshotMetaSet::IsExistLastSnapshot() { return GetLastSnapshot() != nullptr; }

}  // namespace vector_index

}  // namespace dingodb

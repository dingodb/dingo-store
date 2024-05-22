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

#include "document/document_index_snapshot.h"

#include <sys/wait.h>  // Add this include

#include <atomic>
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

namespace document_index {

SnapshotMeta::SnapshotMeta(int64_t document_index_id, const std::string& path)
    : document_index_id_(document_index_id), path_(path) {}

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
          "[document_index.snapshot][index_id({})] Parse snapshot index id failed from snapshot name, path: {}",
          document_index_id_, path_);
      Destroy();
      return false;
    }
  } catch (const std::exception& e) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.snapshot][index_id({})] Parse snapshot index id failed from snapshot name, path: {}",
        document_index_id_, path_);
    Destroy();
    return false;
  }

  snapshot_log_id_ = snapshot_index_id;

  // pb::store_internal::DocumentIndexSnapshotMeta meta;
  // braft::ProtoBufFile pb_file_meta(MetaPath());
  // if (pb_file_meta.load(&meta) != 0) {
  //   DINGO_LOG(ERROR) << fmt::format(
  //       "[document_index.snapshot][index_id({})] Parse vector index snapshot meta failed, path: {}",
  //       document_index_id_, path_);
  //   Destroy();
  //   return false;
  // }

  // epoch_ = meta.epoch();
  // range_ = meta.range();

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.snapshot][index_id({})] Load snapshot meta, epoch: {} snapshot_index_id: {}, path: {}",
      document_index_id_, Helper::RegionEpochToString(epoch_), snapshot_index_id, path_);

  return true;
}

std::string SnapshotMeta::MetaPath() { return fmt::format("{}/meta", path_); }

std::string SnapshotMeta::IndexDataPath() {
  return fmt::format("{}/index_{}_{}.idx", path_, document_index_id_, snapshot_log_id_);
}

std::vector<std::string> SnapshotMeta::ListFileNames() { return Helper::TraverseDirectory(path_); }

void SnapshotMeta::Destroy() {
  bool is_destroied = false;
  if (!is_destroied_.compare_exchange_strong(is_destroied, true)) {
    return;
  }

  // Delete directory
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.snapshot][index_id({})] Delete snapshot, epoch: {} snapshot_index_id: {} path: {}.",
      document_index_id_, Helper::RegionEpochToString(epoch_), snapshot_log_id_, path_);
  Helper::RemoveAllFileOrDirectory(path_);
}

SnapshotMetaSet::SnapshotMetaSet(int64_t document_index_id, const std::string& path)
    : document_index_id_(document_index_id), path_(path) {
  bthread_mutex_init(&mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.SnapshotMetaSet][id({})]", document_index_id_);
}

SnapshotMetaSet::~SnapshotMetaSet() {
  DINGO_LOG(DEBUG) << fmt::format("[delete.SnapshotMetaSet][id({})]", document_index_id_);
  bthread_mutex_destroy(&mutex_);
}

std::shared_ptr<SnapshotMetaSet> SnapshotMetaSet::New(int64_t document_index_id, const std::string& path) {
  return std::make_shared<SnapshotMetaSet>(document_index_id, path);
}

bool SnapshotMetaSet::AddSnapshot(SnapshotMetaPtr snapshot) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (snapshots_.find(snapshot->SnapshotLogId()) == snapshots_.end()) {
    // Delete stale snapshot
    ClearSnapshotImpl();
    snapshots_[snapshot->SnapshotLogId()] = snapshot;
  } else {
    DINGO_LOG(WARNING) << fmt::format("Already exist document index snapshot {} {}", snapshot->DocumentIndexId(),
                                      snapshot->SnapshotLogId());
    return false;
  }

  return true;
}

void SnapshotMetaSet::ClearSnapshot() {
  BAIDU_SCOPED_LOCK(mutex_);

  ClearSnapshotImpl();
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

void SnapshotMetaSet::Destroy() {
  BAIDU_SCOPED_LOCK(mutex_);

  ClearSnapshotImpl();

  // Delete directory
  DINGO_LOG(INFO) << fmt::format("[document_index.snapshot][index_id({})] Delete snapshot set dir, path: {}.",
                                 document_index_id_, path_);
  Helper::RemoveAllFileOrDirectory(path_);
}

void SnapshotMetaSet::ClearSnapshotImpl() {
  for (auto& [_, snapshot] : snapshots_) {
    snapshot->Destroy();
  }

  snapshots_.clear();
}

}  // namespace document_index

}  // namespace dingodb

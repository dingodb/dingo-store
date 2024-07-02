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

#include "document/document_index_snapshot_manager.h"

#include <sys/wait.h>  // Add this include

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "braft/protobuf_file.h"
#include "butil/endpoint.h"
#include "butil/status.h"
#include "butil/strings/string_split.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/service_access.h"
#include "config/config_manager.h"
#include "document/document_index_factory.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "server/server.h"

namespace dingodb {

DEFINE_bool(document_index_snapshot_use_fork, true, "Use fork to save vector index snapshot.");

// Get all snapshot path, except tmp dir.
static std::vector<std::string> GetSnapshotPaths(std::string path) {
  auto filenames = Helper::TraverseDirectory(path);
  if (filenames.empty()) {
    return {};
  }

  std::sort(filenames.begin(), filenames.end(), std::greater<>());

  std::vector<std::string> result;
  for (const auto& filename : filenames) {
    if (filename.find("tmp") == std::string::npos) {
      result.push_back(fmt::format("{}/{}", path, filename));
    }
  }

  return result;
}

// Parse host
static butil::EndPoint ParseHost(const std::string& uri) {
  std::vector<std::string> strs;
  butil::SplitString(uri, '/', &strs);

  if (strs.size() < 4) {
    return {};
  }
  std::string host_and_port = strs[2];

  butil::EndPoint endpoint;
  butil::str2endpoint(host_and_port.c_str(), &endpoint);

  return endpoint;
}

// Parse reader id
static int64_t ParseReaderId(const std::string& uri) {
  std::vector<std::string> strs;
  butil::SplitString(uri, '/', &strs);

  if (strs.size() < 4) {
    return 0;
  }

  std::string& reader_id_str = strs[3];

  char* end = nullptr;
  int64_t result = std::strtoull(reader_id_str.c_str(), &end, 10);
  if ((end - reader_id_str.c_str()) + 1 <= reader_id_str.size()) {
    return 0;
  }

  return result;
}

static int64_t ParseMetaLogId(const std::string& path) {
  std::ifstream file;
  file.open(path, std::ifstream::in);

  std::string str;
  std::getline(file, str);

  try {
    return std::strtoull(str.c_str(), nullptr, 10);
  } catch (const std::invalid_argument& e) {
    DINGO_LOG(ERROR) << " path: " << path << ", Invalid argument: " << e.what();
  } catch (const std::out_of_range& e) {
    DINGO_LOG(ERROR) << " path: " << path << ", Out of range: " << e.what();
  } catch (...) {
    DINGO_LOG(ERROR) << " path: " << path << ", Unknown error";
  }

  return 0;
}

std::vector<std::string> DocumentIndexSnapshotManager::GetSnapshotList(int64_t document_index_id) {
  std::string snapshot_parent_path = GetSnapshotParentPath(document_index_id);

  auto dir_names = Helper::TraverseDirectory(snapshot_parent_path, std::string("epoch"), false, true);

  std::sort(dir_names.begin(), dir_names.end());

  std::vector<std::string> paths;
  paths.reserve(dir_names.size());
  for (const auto& dir_name : dir_names) {
    paths.push_back(fmt::format("{}/{}", snapshot_parent_path, dir_name));
    DINGO_LOG(INFO) << fmt::format("[document_index.snapshot] GetSnapshotList get snapshot path: {}", paths.back());
  }

  return paths;
}

// int64_t GetSnapshotApplyLogId(const std::string& document_index_path) {
//   pb::store_internal::ApplyLogId apply_log_id;

//   braft::ProtoBufFile pb_file_log_id(fmt::format("{}/apply_log_id", document_index_path));
//   if (pb_file_log_id.load(&apply_log_id) != 0) {
//     DINGO_LOG(ERROR) << fmt::format("[document_index.snapshot] Parse document index apply_log_id failed, path: {}",
//                                     document_index_path);
//     return -1;
//   }

//   return apply_log_id.apply_log_id();
// }

std::string DocumentIndexSnapshotManager::GetSnapshotPath(int64_t document_index_id,
                                                          const pb::common::RegionEpoch& epoch) {
  auto index_path_list = GetSnapshotList(document_index_id);

  if (index_path_list.empty()) {
    return std::string();
  }

  for (const auto& path : index_path_list) {
    pb::store_internal::DocumentIndexSnapshotMeta meta;
    braft::ProtoBufFile pb_file_meta(fmt::format("{}/meta", path));
    if (pb_file_meta.load(&meta) != 0) {
      DINGO_LOG(ERROR) << fmt::format(
          "[document_index.snapshot][index_id({})] Parse vector index snapshot meta failed, path: {}",
          document_index_id, path);
      continue;
    }

    if (meta.epoch().version() == epoch.version()) {
      return path;
    }
  }

  return std::string();
}

std::string DocumentIndexSnapshotManager::GetLatestSnapshotPath(int64_t document_index_id) {
  auto index_path_list = GetSnapshotList(document_index_id);

  if (index_path_list.empty()) {
    return std::string();
  }

  std::string latest_path{};
  int64_t latest_epoch = 0;

  for (const auto& path : index_path_list) {
    pb::store_internal::DocumentIndexSnapshotMeta meta;
    braft::ProtoBufFile pb_file_meta(fmt::format("{}/meta", path));
    if (pb_file_meta.load(&meta) != 0) {
      DINGO_LOG(ERROR) << fmt::format(
          "[document_index.snapshot][index_id({})] Parse document index snapshot meta failed, path: {}",
          document_index_id, path);
      continue;
    }

    if (meta.epoch().version() >= latest_epoch) {
      latest_path = path;
      latest_epoch = meta.epoch().version();
    }
  }

  return latest_path;
}

butil::Status DocumentIndexSnapshotManager::GetLatestSnapshotMeta(int64_t document_index_id,
                                                                  pb::store_internal::DocumentIndexSnapshotMeta& meta) {
  auto index_path_list = GetSnapshotList(document_index_id);

  if (index_path_list.empty()) {
    DINGO_LOG(INFO) << fmt::format("[document_index.snapshot][index_id({})] Not found snapshot", document_index_id);
    return butil::Status::OK();
  }

  std::string latest_path{};
  int64_t latest_epoch = 0;

  for (const auto& path : index_path_list) {
    pb::store_internal::DocumentIndexSnapshotMeta meta_on_disk;
    braft::ProtoBufFile pb_file_meta(fmt::format("{}/meta", path));
    if (pb_file_meta.load(&meta_on_disk) != 0) {
      DINGO_LOG(ERROR) << fmt::format(
          "[document_index.snapshot][index_id({})] Parse document index snapshot meta failed, path: {}",
          document_index_id, path);
      continue;
    }

    DINGO_LOG(INFO) << "meta_on_disk.epoch().version(): " << meta_on_disk.epoch().version()
                    << ", latest_epoch: " << latest_epoch << ", path: " << path;

    if (meta_on_disk.epoch().version() >= latest_epoch) {
      latest_path = path;
      latest_epoch = meta_on_disk.epoch().version();
      meta = meta_on_disk;

      DINGO_LOG(INFO) << fmt::format(
          "[document_index.snapshot][index_id({})] Get latest snapshot meta, epoch version: {}, path: {}",
          document_index_id, latest_epoch, latest_path);
    }
  }

  return butil::Status::OK();
}

std::string DocumentIndexSnapshotManager::GetSnapshotParentPath(int64_t document_index_id) {
  return fmt::format("{}/{}", Server::GetInstance().GetDocumentIndexPath(), document_index_id);
}

std::string DocumentIndexSnapshotManager::GetSnapshotPath(int64_t document_index_id, int64_t epoch_version) {
  return fmt::format("{}/{}/epoch_{}", Server::GetInstance().GetDocumentIndexPath(), document_index_id, epoch_version);
}

std::string DocumentIndexSnapshotManager::GetSnapshotTmpPath(int64_t document_index_id) {
  return fmt::format("{}/tmp_{}", GetSnapshotParentPath(document_index_id), Helper::TimestampNs());
}

std::string DocumentIndexSnapshotManager::GetSnapshotNewPath(int64_t document_index_id, int64_t snapshot_log_id) {
  return fmt::format("{}/snapshot_{:020}", GetSnapshotParentPath(document_index_id), snapshot_log_id);
}

// Save vector index snapshot, just one concurrence.
butil::Status DocumentIndexSnapshotManager::SaveDocumentIndexSnapshot(DocumentIndexWrapperPtr document_index_wrapper,
                                                                      int64_t& snapshot_log_index) {
  assert(document_index_wrapper != nullptr);

  auto start_time = Helper::TimestampMs();

  auto document_index = document_index_wrapper->GetOwnDocumentIndex();
  if (document_index == nullptr) {
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "Not found vector index.");
  }

  auto document_index_id = document_index->Id();

  document_index->LockWrite();

  snapshot_log_index = document_index_wrapper->ApplyLogId();
  document_index->SetApplyLogId(snapshot_log_index);

  // Write meta to meta_file
  pb::store_internal::DocumentIndexSnapshotMeta meta;
  meta.set_document_index_id(document_index_id);
  meta.set_snapshot_log_id(snapshot_log_index);
  *(meta.mutable_range()) = document_index->Range(false);
  *(meta.mutable_epoch()) = document_index->Epoch();

  std::string meta_filepath = fmt::format("{}/meta", document_index->IndexPath());

  braft::ProtoBufFile pb_file_meta(meta_filepath);
  if (pb_file_meta.save(&meta, true) != 0) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.child_save_snapshot][index_id({})] Save document index success, save meta to meta file failed",
        document_index_id);
  }

  document_index->UnlockWrite();

  // Set truncate wal log index.
  auto log_storage = Server::GetInstance().GetLogStorageManager()->GetLogStorage(document_index_id);
  if (log_storage != nullptr) {
    log_storage->TruncateVectorIndexPrefix(snapshot_log_index);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.save_snapshot][index_id({})] Save vector index snapshot snapshot_{:020} elapsed(2) time {}ms",
      document_index->Id(), snapshot_log_index, Helper::TimestampMs() - start_time);

  return butil::Status::OK();
}

// Load document index for already exist document index at bootstrap.
std::shared_ptr<DocumentIndex> DocumentIndexSnapshotManager::LoadDocumentIndexSnapshot(
    DocumentIndexWrapperPtr document_index_wrapper, const pb::common::RegionEpoch& epoch) {
  assert(document_index_wrapper != nullptr);

  int64_t start_time_ms = Helper::TimestampMs();

  int64_t document_index_id = document_index_wrapper->Id();

  auto document_index_path = GetSnapshotPath(document_index_id, epoch);

  if (document_index_path.empty()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[document_index.load_snapshot][index_id({})] load snapshot failed, not found snapshot.", document_index_id);
    return nullptr;
  }

  // auto apply_log_id = GetSnapshotApplyLogId(document_index_path);
  // if (apply_log_id < 0) {
  //   DINGO_LOG(WARNING) << fmt::format(
  //       "[document_index.load_snapshot][index_id({})] load snapshot failed, not found apply log id. index_path: {}",
  //       document_index_id, document_index_path);
  //   return nullptr;
  // }

  pb::store_internal::DocumentIndexSnapshotMeta meta;
  braft::ProtoBufFile pb_file_meta(fmt::format("{}/meta", document_index_path));
  if (pb_file_meta.load(&meta) != 0) {
    DINGO_LOG(WARNING) << fmt::format(
        "[document_index.load_snapshot][index_id({})] load snapshot failed, parse meta failed. index_path: {}",
        document_index_id, document_index_path);
    return nullptr;
  }

  if (meta.epoch().version() != epoch.version()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[document_index.load_snapshot][index_id({})] load snapshot failed, epoch version not match. index_path: {}",
        document_index_id, document_index_path);
    return nullptr;
  }

  // create a new document_index
  butil::Status status;
  auto document_index =
      DocumentIndexFactory::LoadIndex(document_index_id, document_index_path, document_index_wrapper->IndexParameter(),
                                      meta.epoch(), meta.range(), status);
  if (!document_index) {
    DINGO_LOG(WARNING) << fmt::format(
        "[document_index.load_snapshot][index_id({})] load snapshot failed, create document index failed. index_path: "
        "{}",
        document_index_id, document_index_path);
    return nullptr;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.load_snapshot][index_id({})] Load vector index snapshot snapshot_{:020} elapsed(2) time {}ms",
      document_index_id, meta.snapshot_log_id(), Helper::TimestampMs() - start_time_ms);

  return document_index;
}

}  // namespace dingodb

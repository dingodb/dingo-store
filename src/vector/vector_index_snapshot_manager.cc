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

#include "vector/vector_index_snapshot_manager.h"

#include <sys/wait.h>  // Add this include

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <istream>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "braft/file_system_adaptor.h"
#include "braft/protobuf_file.h"
#include "butil/endpoint.h"
#include "butil/iobuf.h"
#include "butil/status.h"
#include "common/failpoint.h"
#include "common/file_reader.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/service_access.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "proto/error.pb.h"
#include "proto/file_service.pb.h"
#include "proto/node.pb.h"
#include "proto/store_internal.pb.h"
#include "server/file_service.h"
#include "server/server.h"
#include "vector/vector_index_factory.h"

namespace dingodb {

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

butil::Status VectorIndexSnapshotManager::GetSnapshotList(int64_t vector_index_id, std::vector<std::string>& paths) {
  std::string snapshot_parent_path = GetSnapshotParentPath(vector_index_id);

  auto sub_paths = Helper::TraverseDirectory(snapshot_parent_path, "snapshot", false, true);

  std::sort(sub_paths.begin(), sub_paths.end());

  for (const auto& sub_path : sub_paths) {
    paths.push_back(fmt::format("{}/{}", snapshot_parent_path, sub_path));
  }

  return butil::Status();
}

std::string VectorIndexSnapshotManager::GetSnapshotParentPath(int64_t vector_index_id) {
  return fmt::format("{}/{}", Server::GetInstance()->GetIndexPath(), vector_index_id);
}

std::string VectorIndexSnapshotManager::GetSnapshotTmpPath(int64_t vector_index_id) {
  return fmt::format("{}/tmp_{}", GetSnapshotParentPath(vector_index_id), Helper::TimestampNs());
}

std::string VectorIndexSnapshotManager::GetSnapshotNewPath(int64_t vector_index_id, int64_t snapshot_log_id) {
  return fmt::format("{}/snapshot_{:020}", GetSnapshotParentPath(vector_index_id), snapshot_log_id);
}

butil::Status VectorIndexSnapshotManager::LaunchInstallSnapshot(const butil::EndPoint& endpoint,
                                                                vector_index::SnapshotMetaPtr snapshot) {
  assert(snapshot != nullptr);
  int64_t start_time = Helper::TimestampMs();

  DINGO_LOG(INFO) << fmt::format("[vector_index.snapshot][index({})] last vector index snapshot: {}",
                                 snapshot->VectorIndexId(), snapshot->Path());

  // Get uri
  auto reader = std::make_shared<FileReaderWrapper>(snapshot);
  int64_t reader_id = FileServiceReaderManager::GetInstance().AddReader(reader);
  auto config = Server::GetInstance()->GetConfig();
  auto host = config->GetString("server.host");
  int port = config->GetInt("server.port");
  if (host.empty() || port == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Parse server host or port error.");
  }
  std::string uri = fmt::format("remote://{}:{}/{}", host, port, reader_id);

  // Build request
  pb::node::InstallVectorIndexSnapshotRequest request;
  request.set_uri(uri);
  auto* meta = request.mutable_meta();
  meta->set_snapshot_log_index(snapshot->SnapshotLogId());
  for (const auto& filename : snapshot->ListFileNames()) {
    meta->add_filenames(filename);
  }
  meta->set_vector_index_id(snapshot->VectorIndexId());

  pb::node::InstallVectorIndexSnapshotResponse response;
  auto status = ServiceAccess::InstallVectorIndexSnapshot(request, endpoint, response);
  FileServiceReaderManager::GetInstance().DeleteReader(reader_id);

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.snapshot][index({})] install vector index snapshot {} to {} finish elapsed time {}ms request: {}",
      snapshot->VectorIndexId(), snapshot->SnapshotLogId(), Helper::EndPointToStr(endpoint),
      Helper::TimestampMs() - start_time, request.ShortDebugString());

  return status;
}

butil::Status VectorIndexSnapshotManager::HandleInstallSnapshot(std::shared_ptr<Context>, const std::string& uri,
                                                                const pb::node::VectorIndexSnapshotMeta& meta,
                                                                vector_index::SnapshotMetaSetPtr snapshot_set) {
  // auto vector_index = Server::GetInstance()->GetVectorIndexManager()->GetVectorIndex(meta.vector_index_id());
  // if (vector_index != nullptr) {
  //   return butil::Status(pb::error::EVECTOR_NOT_NEED_SNAPSHOT, "Not need snapshot, follower own vector index.");
  // }

  return DownloadSnapshotFile(uri, meta, snapshot_set);
}

butil::Status VectorIndexSnapshotManager::LaunchPullSnapshot(const butil::EndPoint& endpoint,
                                                             vector_index::SnapshotMetaSetPtr snapshot_set) {
  pb::node::GetVectorIndexSnapshotRequest request;
  request.set_vector_index_id(snapshot_set->VectorIndexId());

  pb::node::GetVectorIndexSnapshotResponse response;
  auto status = ServiceAccess::GetVectorIndexSnapshot(request, endpoint, response);
  if (!status.ok()) {
    return status;
  }

  status = DownloadSnapshotFile(response.uri(), response.meta(), snapshot_set);
  if (!status.ok()) {
    return status;
  }

  // Clean corresponding reader id.
  int64_t reader_id = ParseReaderId(response.uri());
  if (reader_id > 0) {
    pb::fileservice::CleanFileReaderRequest request;
    request.set_reader_id(reader_id);
    auto node_info = ServiceAccess::GetNodeInfo(endpoint);
    ServiceAccess::CleanFileReader(request, Helper::LocationToEndPoint(node_info.server_location()));
  }

  return butil::Status();
}

butil::Status VectorIndexSnapshotManager::InstallSnapshotToFollowers(vector_index::SnapshotMetaPtr snapshot) {
  assert(snapshot != nullptr);

  int64_t start_time = Helper::TimestampMs();
  auto raft_raft_engine = Server::GetInstance()->GetRaftStoreEngine();
  if (raft_raft_engine == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Not raft store engine.");
  }
  auto raft_node = raft_raft_engine->GetNode(snapshot->VectorIndexId());
  if (raft_node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node.");
  }

  auto self_peer = raft_node->GetPeerId();
  std::vector<braft::PeerId> peers;
  raft_node->ListPeers(&peers);
  for (const auto& peer : peers) {
    if (peer != self_peer) {
      auto status = LaunchInstallSnapshot(peer.addr, snapshot);
      if (!status.ok()) {
        if (status.error_code() == pb::error::EVECTOR_NOT_NEED_SNAPSHOT ||
            status.error_code() == pb::error::EVECTOR_SNAPSHOT_EXIST) {
          DINGO_LOG(INFO) << fmt::format("[vector_index.snapshot][index({})] vector index peer {} {}",
                                         snapshot->VectorIndexId(), Helper::EndPointToStr(peer.addr),
                                         status.error_str());
        } else {
          DINGO_LOG(ERROR) << fmt::format(
              "[vector_index.snapshot][index({})] install vector index snapshot {} to {} failed, error: {}",
              snapshot->VectorIndexId(), snapshot->SnapshotLogId(), Helper::EndPointToStr(peer.addr),
              status.error_str());
        }
      }
    }
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.snapshot][index({})] install vector index snapshot {} to all followers finish elapsed time {}ms",
      snapshot->VectorIndexId(), snapshot->SnapshotLogId(), Helper::TimestampMs() - start_time);

  return butil::Status();
}

butil::Status VectorIndexSnapshotManager::HandlePullSnapshot(std::shared_ptr<Context> ctx,
                                                             vector_index::SnapshotMetaPtr snapshot) {
  assert(snapshot != nullptr);

  DINGO_LOG(INFO) << fmt::format("[vector_index.snapshot][index({})] last vector index snapshot: {}",
                                 snapshot->VectorIndexId(), snapshot->Path());

  auto* response = dynamic_cast<pb::node::GetVectorIndexSnapshotResponse*>(ctx->Response());
  // Build response meta
  auto* meta = response->mutable_meta();
  meta->set_vector_index_id(snapshot->VectorIndexId());
  meta->set_snapshot_log_index(snapshot->SnapshotLogId());
  for (const auto& filename : snapshot->ListFileNames()) {
    meta->add_filenames(filename);
  }

  // Build response uri
  auto config = Server::GetInstance()->GetConfig();
  auto host = config->GetString("server.host");
  int port = config->GetInt("server.port");
  if (host.empty() || port == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Parse server host or port error.");
  }

  auto reader = std::make_shared<FileReaderWrapper>(snapshot);
  int64_t reader_id = FileServiceReaderManager::GetInstance().AddReader(reader);
  response->set_uri(fmt::format("remote://{}:{}/{}", host, port, reader_id));

  DINGO_LOG(INFO) << fmt::format("[vector_index.snapshot][index({})] response: {}", snapshot->VectorIndexId(),
                                 response->ShortDebugString());

  return butil::Status();
}

butil::Status VectorIndexSnapshotManager::PullLastSnapshotFromPeers(vector_index::SnapshotMetaSetPtr snapshot_set) {
  assert(snapshot_set != nullptr);

  int64_t start_time = Helper::TimestampMs();
  auto engine = Server::GetInstance()->GetEngine();
  if (engine->GetID() != pb::common::ENG_RAFT_STORE) {
    return butil::Status(pb::error::EINTERNAL, "Not raft store engine.");
  }

  int64_t vector_index_id = snapshot_set->VectorIndexId();
  auto raft_kv_engine = std::dynamic_pointer_cast<RaftStoreEngine>(engine);
  auto raft_node = raft_kv_engine->GetNode(vector_index_id);
  if (raft_node == nullptr) {
    return butil::Status(pb::error::ERAFT_NOT_FOUND, "Not found raft node.");
  }

  // Find max vector index snapshot peer.
  pb::node::GetVectorIndexSnapshotRequest request;
  request.set_vector_index_id(vector_index_id);

  int64_t max_snapshot_log_index = 0;
  butil::EndPoint endpoint;

  auto self_peer = raft_node->GetPeerId();
  std::vector<braft::PeerId> peers;
  raft_node->ListPeers(&peers);
  for (const auto& peer : peers) {
    if (peer == self_peer) {
      continue;
    }

    pb::node::GetVectorIndexSnapshotResponse response;
    auto status = ServiceAccess::GetVectorIndexSnapshot(request, peer.addr, response);
    if (!status.ok()) {
      continue;
    }

    if (max_snapshot_log_index < response.meta().snapshot_log_index()) {
      max_snapshot_log_index = response.meta().snapshot_log_index();
      endpoint = peer.addr;
    }
  }

  // Not found vector index snapshot, abandon.
  if (max_snapshot_log_index == 0) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.snapshot][index({})] other peers not exist vector index snapshot.",
                                   vector_index_id);
    return butil::Status();
  }

  auto last_snapshot = snapshot_set->GetLastSnapshot();
  if (last_snapshot != nullptr &&
      last_snapshot->SnapshotLogId() + Constant::kVectorIndexSnapshotCatchupMargin > max_snapshot_log_index) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.snapshot][index({})] catchup margin too small, use local vector index snapshot {}.",
        vector_index_id, last_snapshot->SnapshotLogId());
    return butil::Status();
  }

  // Has vector index snapshot, pull it.
  auto status = LaunchPullSnapshot(endpoint, snapshot_set);
  if (!status.ok()) {
    if (status.error_code() != pb::error::EVECTOR_SNAPSHOT_EXIST) {
      DINGO_LOG(ERROR) << fmt::format(
          "[vector_index.snapshot][index({})] pull vector index snapshot {} from {} failed, error: {}", vector_index_id,
          max_snapshot_log_index, Helper::EndPointToStr(endpoint), status.error_str());
    }
    return status;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.snapshot][index({})] pull vector index snapshot {} finish elapsed time {}ms", vector_index_id,
      max_snapshot_log_index, Helper::TimestampMs() - start_time);

  return butil::Status();
}

butil::Status VectorIndexSnapshotManager::DownloadSnapshotFile(const std::string& uri,
                                                               const pb::node::VectorIndexSnapshotMeta& meta,
                                                               vector_index::SnapshotMetaSetPtr snapshot_set) {
  // Parse reader_id and endpoint
  int64_t reader_id = ParseReaderId(uri);
  butil::EndPoint endpoint = ParseHost(uri);
  if (reader_id == 0 || endpoint.port == 0) {
    return butil::Status(pb::error::EINTERNAL, "Parse uri to reader_id and endpoint error");
  }

  if (snapshot_set->IsExistSnapshot(meta.snapshot_log_index())) {
    std::string msg =
        fmt::format("[vector_index.snapshot][index({})] already exist vector index snapshot snapshot_log_index {}",
                    meta.vector_index_id(), meta.snapshot_log_index());
    DINGO_LOG(INFO) << msg;
    return butil::Status(pb::error::EVECTOR_SNAPSHOT_EXIST, msg);
  }

  // temp snapshot path for save vector index.
  std::string tmp_snapshot_path = GetSnapshotTmpPath(meta.vector_index_id());
  if (std::filesystem::exists(tmp_snapshot_path)) {
    Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);
  } else {
    Helper::CreateDirectory(tmp_snapshot_path);
  }

  for (const auto& filename : meta.filenames()) {
    int64_t offset = 0;
    std::ofstream ofile;

    std::string filepath = fmt::format("{}/{}", tmp_snapshot_path, filename);
    ofile.open(filepath, std::ofstream::out | std::ofstream::binary);
    DINGO_LOG(INFO) << fmt::format("[vector_index.snapshot][index({})] get vector index snapshot file: {}",
                                   meta.vector_index_id(), filepath);

    for (;;) {
      pb::fileservice::GetFileRequest request;
      request.set_reader_id(reader_id);
      request.set_filename(filename);
      request.set_offset(offset);
      request.set_size(Constant::kFileTransportChunkSize);

      DINGO_LOG(DEBUG) << fmt::format("[vector_index.snapshot][index({})] GetFileRequest: {}", meta.vector_index_id(),
                                      request.ShortDebugString());

      butil::IOBuf buf;
      auto response = ServiceAccess::GetFile(request, endpoint, &buf);
      if (response == nullptr) {
        return butil::Status(pb::error::EINTERNAL, "Get file failed");
      }

      DINGO_LOG(DEBUG) << fmt::format("[vector_index.snapshot][index({})] GetFileResponse: {}", meta.vector_index_id(),
                                      response->ShortDebugString());

      // Write local file.
      ofile << buf;

      if (response->eof()) {
        break;
      }

      offset += response->read_size();
    }

    ofile.close();
  }

  if (snapshot_set->IsExistSnapshot(meta.snapshot_log_index())) {
    std::string msg =
        fmt::format("[vector_index.snapshot][index({})] already exist vector index snapshot snapshot_log_index {}",
                    meta.vector_index_id(), meta.snapshot_log_index());
    DINGO_LOG(INFO) << msg;
    return butil::Status(pb::error::EVECTOR_SNAPSHOT_EXIST, msg);
  }

  // Todo: lock rename
  // Rename
  std::string new_snapshot_path = GetSnapshotNewPath(meta.vector_index_id(), meta.snapshot_log_index());
  auto status = Helper::Rename(tmp_snapshot_path, new_snapshot_path);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.snapshot][index({})] rename vector index snapshot failed, {} -> {} error: {}",
        meta.vector_index_id(), tmp_snapshot_path, new_snapshot_path, status.error_str());
    return status;
  }

  auto new_snapshot = vector_index::SnapshotMeta::New(meta.vector_index_id(), new_snapshot_path);
  if (!new_snapshot->Init()) {
    return butil::Status(pb::error::EINTERNAL, "Init snapshot failed, path: %s", new_snapshot_path.c_str());
  }

  if (!snapshot_set->AddSnapshot(new_snapshot)) {
    return butil::Status(pb::error::EVECTOR_SNAPSHOT_EXIST, "Already exist vector index snapshot, path: %s",
                         new_snapshot_path.c_str());
  }

  return butil::Status();
}

// Save vector index snapshot, just one concurrence.
butil::Status VectorIndexSnapshotManager::SaveVectorIndexSnapshot(VectorIndexWrapperPtr vector_index_wrapper,
                                                                  int64_t& snapshot_log_index) {
  assert(vector_index_wrapper != nullptr);

  auto vector_index = vector_index_wrapper->GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "Not found vector index.");
  }

  // for index like FLAT does not implement save, just skip save to prevent directory creating
  if (!vector_index->SupportSave()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.save_snapshot][index_id({})] VectorIndex not support save, skip save",
                                   vector_index_wrapper->Id());
    return butil::Status::OK();
  }

  int64_t vector_index_id = vector_index_wrapper->Id();

  int64_t start_time = Helper::TimestampMs();

  // lock write for atomic ops
  // this lock will be unlocked after fork()
  vector_index->LockWrite();

  int64_t apply_log_index = vector_index_wrapper->ApplyLogId();
  auto snapshot_set = vector_index_wrapper->SnapshotSet();

  // If already exist snapshot then give up.
  if (snapshot_set->IsExistSnapshot(apply_log_index)) {
    snapshot_log_index = apply_log_index;
    // unlock write
    vector_index->UnlockWrite();

    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.save_snapshot][index_id({})] VectorIndex Snapshot already exist, cannot do save, log_id: {}",
        vector_index_id, apply_log_index);
    return butil::Status();
  }

  // Temp snapshot path for save vector index.
  std::string tmp_snapshot_path = GetSnapshotTmpPath(vector_index_id);
  if (std::filesystem::exists(tmp_snapshot_path)) {
    Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);
  }

  if (!Helper::CreateDirectory(tmp_snapshot_path)) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.save_snapshot][index_id({})] Create tmp snapshot path failed, path: {}", vector_index_id,
        tmp_snapshot_path);
    return butil::Status(pb::error::EINTERNAL, "Create tmp snapshot path failed");
  }

  // Get vector index file path
  std::string index_filepath = fmt::format("{}/index_{}_{}.idx", tmp_snapshot_path, vector_index_id, apply_log_index);
  std::string result_filepath =
      fmt::format("{}/index_{}_{}.result", tmp_snapshot_path, vector_index_id, apply_log_index);
  std::string log_filepath = fmt::format("{}/index_{}_{}.log", tmp_snapshot_path, vector_index_id, apply_log_index);
  std::string meta_filepath = fmt::format("{}/meta", tmp_snapshot_path);

  DINGO_LOG(INFO) << fmt::format("[vector_index.save_snapshot][index_id({})] Save vector index to file {}",
                                 vector_index_id, index_filepath);

  // Save vector index to tmp file
  // fork() a child process to save vector index to tmp file
  pid_t pid = fork();
  if (pid < 0) {
    // unlock write
    vector_index->UnlockWrite();

    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.save_snapshot][index_id({})] Save vector index snapshot failed, fork failed, error: {}",
        vector_index_id, strerror(errno));
    Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);
    return butil::Status(pb::error::Errno::EINTERNAL, "Save vector index failed, fork failed");
  } else if (pid == 0) {
    // Caution: child process can't do any DINGO_LOG, because DINGO_LOG will overwrite the whole log file
    //          but there is DINGO_LOG call in RemoveAllFileOrDirectory if error ocurred, careful to use it.
    std::ofstream log_file(log_filepath);
    if (!log_file.is_open()) {
      _exit(-1);
    }

    auto ret = vector_index->Save(index_filepath);
    if (ret.error_code() == pb::error::Errno::EVECTOR_NOT_SUPPORT) {
      log_file << fmt::format(
                      "[vector_index.child_save_snapshot][index_id({})] Vector index not support save, error: {}",
                      vector_index_id, ret.error_str())
               << '\n';

      // Write result to result_file
      pb::error::Error error;
      error.set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      error.set_errmsg(ret.error_str());

      braft::ProtoBufFile pb_file_result(result_filepath);
      if (pb_file_result.save(&error, true) != 0) {
        log_file << fmt::format(
                        "[vector_index.child_save_snapshot][index_id({})] Save vector index failed, save result to "
                        "result file "
                        "failed, "
                        "error: {}",
                        vector_index_id, error.ShortDebugString())
                 << '\n';
        log_file.close();
        _exit(-1);
      }

      log_file.close();
      _exit(-1);
    } else if (!ret.ok()) {
      log_file << fmt::format("[vector_index.child_save_snapshot][index_id({})] Save vector index failed, error: {}",
                              vector_index_id, ret.error_str())
               << '\n';
      // TODO: keep tmp dir for debug, may uncomment later
      // Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);

      // Write result to result_file
      pb::error::Error error;
      error.set_errcode(static_cast<pb::error::Errno>(ret.error_code()));
      error.set_errmsg(ret.error_str());

      braft::ProtoBufFile pb_file_result(result_filepath);
      if (pb_file_result.save(&error, true) != 0) {
        log_file << fmt::format(
                        "[vector_index.child_save_snapshot][index_id({})] Save vector index failed, save result to "
                        "result file "
                        "failed, "
                        "error: {}",
                        vector_index_id, error.ShortDebugString())
                 << '\n';
        log_file.close();
        _exit(-1);
      }

      log_file.close();
      _exit(-1);
    }

    // Write success result to result_file
    pb::error::Error error;
    error.set_errcode(pb::error::Errno::EVECTOR_INDEX_SAVE_SUCCESS);
    error.set_errmsg("EVECTOR_INDEX_SAVE_SUCCESS");

    braft::ProtoBufFile pb_file_result(result_filepath);
    if (pb_file_result.save(&error, true) != 0) {
      log_file << fmt::format(
                      "[vector_index.child_save_snapshot][index_id({})] Save vector index success, save result to "
                      "result file "
                      "failed, "
                      "error: {}",
                      vector_index_id, error.ShortDebugString())
               << '\n';
      log_file.close();
      _exit(-1);
    }

    // Write meta to meta_file
    pb::store_internal::VectorIndexSnapshotMeta meta;
    meta.set_vector_index_id(vector_index_id);
    meta.set_snapshot_log_id(apply_log_index);
    *(meta.mutable_range()) = vector_index->Range();

    braft::ProtoBufFile pb_file_meta(meta_filepath);
    if (pb_file_meta.save(&meta, true) != 0) {
      log_file << fmt::format(
                      "[vector_index.child_save_snapshot][index_id({})] Save vector index success, save meta to meta "
                      "file failed, "
                      "error: {}",
                      vector_index_id, meta.ShortDebugString())
               << '\n';
      log_file.close();
      _exit(-1);
    }

    log_file << fmt::format("[vector_index.child_save_snapshot][index_id({})] Save vector index success",
                            vector_index_id)
             << '\n';

    log_file.close();
    _exit(0);
  } else {
    // unlock write
    vector_index->UnlockWrite();

    // Wait for the child process to complete
    int status;

    waitpid(pid, &status, 0);

    // use stream id read all lines from log_filepath, use DINGO_LOG to print all lines in log file
    std::ifstream log_file(log_filepath);
    if (!log_file.is_open()) {
      DINGO_LOG(ERROR) << fmt::format(
          "[vector_index.save_snapshot][index_id({})] Save vector index snapshot failed, open log file failed",
          vector_index_id);
    } else {
      std::string line;
      while (std::getline(log_file, line)) {
        DINGO_LOG(INFO) << fmt::format(
            "[vector_index.save_snapshot][index_id({})] Save vector index snapshot log from child process, {}",
            vector_index_id, line);
      }
      log_file.close();
    }

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      DINGO_LOG(INFO) << fmt::format(
          "[vector_index.save_snapshot][index_id({})] Save vector index snapshot snapshot_{:020} elapsed(1) time {}ms",
          vector_index_id, apply_log_index, Helper::TimestampMs() - start_time);
    } else {
      DINGO_LOG(ERROR) << fmt::format(
          "[vector_index.save_snapshot][index_id({})] Save vector index snapshot failed, child process encountered an "
          "error",
          vector_index_id);
      // TODO: keep tmp dir for debug, may uncomment later
      // Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);
      return butil::Status(pb::error::Errno::EINTERNAL, "Save vector index failed, child process encountered an error");
    }
  }

  // read from result_filepath, and deserilize to pb::error::Error
  // check if result is SUCCESS
  pb::error::Error error;
  braft::ProtoBufFile pb_file_result(result_filepath);
  if (pb_file_result.load(&error) != 0) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.save_snapshot][index_id({})] Save vector index snapshot failed, load result file failed",
        vector_index_id);
    // Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);
    return butil::Status(pb::error::Errno::EINTERNAL, "Save vector index failed, load result file failed");
  }
  if (error.errcode() != pb::error::Errno::EVECTOR_INDEX_SAVE_SUCCESS) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.save_snapshot][index_id({})] Save vector index snapshot  failed, {}",
                                    vector_index_id, error.errmsg());
    // Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);
    return butil::Status(error.errcode(), error.errmsg());
  }

  // check if meta is legal
  pb::store_internal::VectorIndexSnapshotMeta meta;
  braft::ProtoBufFile pb_file_meta(meta_filepath);
  if (pb_file_meta.load(&meta) != 0) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.save_snapshot][index_id({})] Save vector index success, save meta to meta file failed, "
        "error: {}",
        vector_index_id, meta.ShortDebugString());
    return butil::Status(pb::error::Errno::EINTERNAL, "Save vector index failed, save meta to meta file failed");
  }
  if (meta.snapshot_log_id() <= 0 || meta.vector_index_id() <= 0) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.save_snapshot][index_id({})] Save vector index success, meta is illegal, "
        "error: {}",
        vector_index_id, meta.ShortDebugString());
    return butil::Status(pb::error::Errno::EINTERNAL, "Save vector index failed, meta is illegal");
  }

  // result is SUCCESS and meta is legal, the vector snapshot is succeed
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.save_snapshot][index_id({})] Save vector index snapshot child process success", vector_index_id);

  // If already exist snapshot then give up.
  if (snapshot_set->IsExistSnapshot(apply_log_index)) {
    snapshot_log_index = apply_log_index;
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.save_snapshot][index_id({})] VectorIndex Snapshot already exist, cannot do save, log_id: {}",
        vector_index_id, apply_log_index);
    return butil::Status();
  }

  // Rename
  std::string new_snapshot_path = GetSnapshotNewPath(vector_index_id, apply_log_index);
  auto status = Helper::Rename(tmp_snapshot_path, new_snapshot_path);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[vector_index.save_snapshot][index_id({})] Rename vector index snapshot failed, {} -> {} error: {}",
        vector_index_id, tmp_snapshot_path, new_snapshot_path, status.error_str());
    return status;
  }

  auto new_snapshot = vector_index::SnapshotMeta::New(vector_index_id, new_snapshot_path);
  if (!new_snapshot->Init()) {
    return butil::Status(pb::error::EINTERNAL, "Init snapshot failed, path: %s", new_snapshot_path.c_str());
  }

  if (!snapshot_set->AddSnapshot(new_snapshot)) {
    return butil::Status(pb::error::EVECTOR_SNAPSHOT_EXIST, "Already exist vector index snapshot, path: %s",
                         new_snapshot_path.c_str());
  }

  // Set truncate wal log index.
  auto log_storage = Server::GetInstance()->GetLogStorageManager()->GetLogStorage(vector_index_id);
  if (log_storage != nullptr) {
    log_storage->TruncateVectorIndexPrefix(apply_log_index);
  }

  snapshot_log_index = apply_log_index;

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.save_snapshot][index_id({})] Save vector index snapshot snapshot_{:020} elapsed(2) time {}ms",
      vector_index_id, apply_log_index, Helper::TimestampMs() - start_time);

  return butil::Status::OK();
}

// Load vector index for already exist vector index at bootstrap.
std::shared_ptr<VectorIndex> VectorIndexSnapshotManager::LoadVectorIndexSnapshot(
    VectorIndexWrapperPtr vector_index_wrapper) {
  assert(vector_index_wrapper != nullptr);

  int64_t vector_index_id = vector_index_wrapper->Id();
  auto snapshot_set = vector_index_wrapper->SnapshotSet();

  // Read vector index snapshot log id form snapshot meta file.
  auto last_snapshot = snapshot_set->GetLastSnapshot();
  if (last_snapshot == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.load_snapshot][index_id({})] not found vector index snapshot.",
                                      vector_index_id);
    return nullptr;
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.load_snapshot][index_id({})] snapshot log id is {}",
                                 last_snapshot->VectorIndexId(), last_snapshot->SnapshotLogId());

  // check whether index file exist.
  if (!Helper::IsExistPath(last_snapshot->IndexDataPath())) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.load_snapshot][index_id({})] index file {} not exist, can't load.",
                                    last_snapshot->VectorIndexId(), last_snapshot->IndexDataPath());
    return nullptr;
  }

  // check whether meta file exist.
  if (!Helper::IsExistPath(last_snapshot->MetaPath())) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.load_snapshot][index_id({})] meta file {} not exist, can't load.",
                                    last_snapshot->VectorIndexId(), last_snapshot->MetaPath());
    return nullptr;
  }

  pb::store_internal::VectorIndexSnapshotMeta meta;
  braft::ProtoBufFile pb_file_meta(last_snapshot->MetaPath());
  if (pb_file_meta.load(&meta) != 0) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.load_snapshot][index_id({})] load meta file failed.",
                                      vector_index_id);
    return nullptr;
  }

  // create a new vector_index
  auto vector_index = VectorIndexFactory::New(vector_index_id, vector_index_wrapper->IndexParameter(), meta.range());
  if (!vector_index) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.load_snapshot][index_id({})] New vector index failed.",
                                      vector_index_id);
    return nullptr;
  }

  // load index from file
  auto ret = vector_index->Load(last_snapshot->IndexDataPath());
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.load_snapshot][index_id({})] Load vector index failed.",
                                      vector_index_id);
    return nullptr;
  }

  // set vector_index apply log id
  vector_index->SetSnapshotLogId(last_snapshot->SnapshotLogId());
  vector_index->SetApplyLogId(last_snapshot->SnapshotLogId());

  return vector_index;
}

}  // namespace dingodb

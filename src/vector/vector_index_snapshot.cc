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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "braft/file_system_adaptor.h"
#include "butil/endpoint.h"
#include "butil/iobuf.h"
#include "butil/status.h"
#include "common/failpoint.h"
#include "common/file_reader.h"
#include "common/helper.h"
#include "common/service_access.h"
#include "fmt/core.h"
#include "proto/error.pb.h"
#include "proto/file_service.pb.h"
#include "proto/node.pb.h"
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
    if (filename != "tmp") {
      result.push_back(fmt::format("{}/{}", path, filename));
    }
  }

  return result;
}

// Get last snapshot path.
static std::string GetLastSnapshotPath(std::string path) {
  auto snapshot_paths = GetSnapshotPaths(path);

  return snapshot_paths.empty() ? "" : snapshot_paths[0];
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
static uint64_t ParseReaderId(const std::string& uri) {
  std::vector<std::string> strs;
  butil::SplitString(uri, '/', &strs);

  if (strs.size() < 4) {
    return 0;
  }

  std::string& reader_id_str = strs[3];

  char* end = nullptr;
  uint64_t result = std::strtoull(reader_id_str.c_str(), &end, 10);
  if ((end - reader_id_str.c_str()) + 1 <= reader_id_str.size()) {
    return 0;
  }

  return result;
}

static uint64_t ParseMetaLogId(const std::string& path) {
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

  return UINT64_MAX;
}

bool VectorIndexSnapshot::IsExistVectorIndexSnapshot(uint64_t vector_index_id) {
  std::string snapshot_parent_path = fmt::format("{}/{}", Server::GetInstance()->GetIndexPath(), vector_index_id);
  if (!std::filesystem::exists(snapshot_parent_path)) {
    return false;
  }

  std::string last_snapshot_path = GetLastSnapshotPath(snapshot_parent_path);
  return !last_snapshot_path.empty();
}

uint64_t VectorIndexSnapshot::GetLastVectorIndexSnapshotLogId(uint64_t vector_index_id) {
  std::string snapshot_parent_path = fmt::format("{}/{}", Server::GetInstance()->GetIndexPath(), vector_index_id);
  if (!std::filesystem::exists(snapshot_parent_path)) {
    return 0;
  }

  std::string last_snapshot_path = GetLastSnapshotPath(snapshot_parent_path);
  if (last_snapshot_path.empty()) {
    return 0;
  }

  std::string meta_path = fmt::format("{}/meta", last_snapshot_path);
  uint64_t log_id = ParseMetaLogId(meta_path);
  if (log_id == 0) {
    return 0;
  }

  return log_id;
}

butil::Status VectorIndexSnapshot::LaunchInstallSnapshot(const butil::EndPoint& endpoint, uint64_t vector_index_id) {
  std::string snapshot_parent_path = fmt::format("{}/{}", Server::GetInstance()->GetIndexPath(), vector_index_id);

  // Get last snapshot
  std::string last_snapshot_path = GetLastSnapshotPath(snapshot_parent_path);
  if (last_snapshot_path.empty()) {
    return butil::Status(pb::error::EVECTOR_SNAPSHOT_NOT_FOUND, "Not found vector index snapshot");
  }
  DINGO_LOG(INFO) << fmt::format("last vector index snapshot: {}", last_snapshot_path);

  // Get uri
  auto reader = std::make_shared<LocalDirReader>(new braft::PosixFileSystemAdaptor(), last_snapshot_path);
  uint64_t reader_id = FileServiceReaderManager::GetInstance().AddReader(reader);
  auto config = Server::GetInstance()->GetConfig();
  auto host = config->GetString("server.host");
  int port = config->GetInt("server.port");
  if (host.empty() || port == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Parse server host or port error.");
  }
  std::string uri = fmt::format("remote://{}:{}/{}", host, port, reader_id);

  // Get snapshot files
  auto filenames = Helper::TraverseDirectory(last_snapshot_path);

  // Get snapshot log id
  std::string meta_path = fmt::format("{}/meta", last_snapshot_path);
  uint64_t log_id = ParseMetaLogId(meta_path);
  if (log_id == 0) {
    return butil::Status(pb::error::EVECTOR_SNAPSHOT_INVALID, "Parse snapshot meta log id failed");
  }

  // Build request
  pb::node::InstallVectorIndexSnapshotRequest request;
  request.set_uri(uri);
  auto* meta = request.mutable_meta();
  meta->set_snapshot_log_index(log_id);
  for (const auto& filename : filenames) {
    meta->add_filenames(filename);
  }
  meta->set_vector_index_id(vector_index_id);

  DINGO_LOG(INFO) << "InstallVectorIndexSnapshotRequest: " << request.ShortDebugString();
  pb::node::InstallVectorIndexSnapshotResponse response;
  auto status = ServiceAccess::InstallVectorIndexSnapshot(request, endpoint, response);
  FileServiceReaderManager::GetInstance().DeleteReader(reader_id);

  return status;
}

butil::Status VectorIndexSnapshot::HandleInstallSnapshot(std::shared_ptr<Context>, const std::string& uri,
                                                         const pb::node::VectorIndexSnapshotMeta& meta) {
  return DownloadSnapshotFile(uri, meta);
}

butil::Status VectorIndexSnapshot::LaunchPullSnapshot(const butil::EndPoint& endpoint, uint64_t vector_index_id) {
  pb::node::GetVectorIndexSnapshotRequest request;
  request.set_vector_index_id(vector_index_id);

  pb::node::GetVectorIndexSnapshotResponse response;
  auto status = ServiceAccess::GetVectorIndexSnapshot(request, endpoint, response);
  if (!status.ok()) {
    return status;
  }

  status = DownloadSnapshotFile(response.uri(), response.meta());
  if (!status.ok()) {
    return status;
  }

  // Clean corresponding reader id.
  uint64_t reader_id = ParseReaderId(response.uri());
  if (reader_id > 0) {
    pb::fileservice::CleanFileReaderRequest request;
    request.set_reader_id(reader_id);
    ServiceAccess::CleanFileReader(request, endpoint);
  }

  return butil::Status();
}

butil::Status VectorIndexSnapshot::HandlePullSnapshot(std::shared_ptr<Context> ctx, uint64_t vector_index_id) {
  // Check last snapshot is exist.
  std::string snapshot_parent_path = fmt::format("{}/{}", Server::GetInstance()->GetIndexPath(), vector_index_id);
  std::string last_snapshot_path = GetLastSnapshotPath(snapshot_parent_path);
  if (last_snapshot_path.empty()) {
    return butil::Status(pb::error::EVECTOR_SNAPSHOT_NOT_FOUND, "Not found vector index snapshot");
  }
  DINGO_LOG(INFO) << fmt::format("====last_snapshot_path: {}", last_snapshot_path);

  auto* response = dynamic_cast<pb::node::GetVectorIndexSnapshotResponse*>(ctx->Response());
  // Build response meta
  auto* meta = response->mutable_meta();
  meta->set_vector_index_id(vector_index_id);

  std::string meta_path = fmt::format("{}/meta", last_snapshot_path);
  uint64_t log_id = ParseMetaLogId(meta_path);
  if (log_id == 0) {
    return butil::Status(pb::error::EVECTOR_SNAPSHOT_INVALID, "Parse snapshot meta log id failed");
  }
  meta->set_snapshot_log_index(log_id);

  auto snapshot_filenames = Helper::TraverseDirectory(last_snapshot_path);
  if (snapshot_filenames.empty()) {
    return butil::Status(pb::error::EVECTOR_SNAPSHOT_INVALID, "Not found snapshot file");
  }
  for (const auto& filename : snapshot_filenames) {
    meta->add_filenames(filename);
  }

  // Build response uri
  auto config = Server::GetInstance()->GetConfig();
  auto host = config->GetString("server.host");
  int port = config->GetInt("server.port");
  if (host.empty() || port == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Parse server host or port error.");
  }

  auto reader = std::make_shared<LocalDirReader>(new braft::PosixFileSystemAdaptor(), last_snapshot_path);
  uint64_t reader_id = FileServiceReaderManager::GetInstance().AddReader(reader);
  response->set_uri(fmt::format("remote://{}:{}/{}", host, port, reader_id));

  DINGO_LOG(INFO) << fmt::format("====response: {}", response->ShortDebugString());

  return butil::Status();
}

butil::Status VectorIndexSnapshot::DownloadSnapshotFile(const std::string& uri,
                                                        const pb::node::VectorIndexSnapshotMeta& meta) {
  // Parse reader_id and endpoint
  uint64_t reader_id = ParseReaderId(uri);
  butil::EndPoint endpoint = ParseHost(uri);
  if (reader_id == 0 || endpoint.port == 0) {
    return butil::Status(pb::error::EINTERNAL, "Parse uri to reader_id and endpoint error");
  }

  // The vector index dir.
  std::string snapshot_parent_path =
      fmt::format("{}/{}", Server::GetInstance()->GetIndexPath(), meta.vector_index_id());
  if (!std::filesystem::exists(snapshot_parent_path)) {
    std::filesystem::create_directories(snapshot_parent_path);
  }

  // Get all exist snapshot path
  auto snapshot_paths = GetSnapshotPaths(snapshot_parent_path);

  // temp snapshot path for save vector index.
  std::string tmp_snapshot_path = fmt::format("{}/tmp", snapshot_parent_path);
  if (std::filesystem::exists(tmp_snapshot_path)) {
    Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);
  } else {
    std::filesystem::create_directories(tmp_snapshot_path);
  }

  for (const auto& filename : meta.filenames()) {
    uint64_t offset = 0;
    std::ofstream ofile;

    std::string filepath = fmt::format("{}/{}", tmp_snapshot_path, filename);
    ofile.open(filepath, std::ofstream::out | std::ofstream::binary);
    DINGO_LOG(INFO) << "Get vector index snapshot file: " << filepath;

    for (;;) {
      pb::fileservice::GetFileRequest request;
      request.set_reader_id(reader_id);
      request.set_filename(filename);
      request.set_offset(offset);
      request.set_size(Constant::kFileTransportChunkSize);

      DINGO_LOG(DEBUG) << "GetFileRequest: " << request.ShortDebugString();

      butil::IOBuf buf;
      auto response = ServiceAccess::GetFile(request, endpoint, &buf);
      if (response == nullptr) {
        return butil::Status(pb::error::EINTERNAL, "Get file failed");
      }

      DINGO_LOG(DEBUG) << "GetFileResponse: " << response->ShortDebugString();

      // Write local file.
      ofile << buf;

      if (response->eof()) {
        break;
      }

      offset += response->read_size();
    }

    ofile.close();
  }

  // Rename
  std::string new_snapshot_path = fmt::format("{}/{}/snapshot_{:020}", Server::GetInstance()->GetIndexPath(),
                                              meta.vector_index_id(), meta.snapshot_log_index());
  auto ret = Helper::Rename(new_snapshot_path, snapshot_parent_path);
  if (!ret.ok()) {
    DINGO_LOG(ERROR) << "Rename vector index snapshot failed, src: " << tmp_snapshot_path
                     << ", dst: " << new_snapshot_path << ", error: " << ret.error_str();
    return ret;
  }

  // Remove old snapshot
  for (auto& snapshot_path : snapshot_paths) {
    DINGO_LOG(INFO) << "delete vector index snapshot: " << snapshot_path;
    Helper::RemoveAllFileOrDirectory(snapshot_path);
  }

  return butil::Status();
}

butil::Status VectorIndexSnapshot::SaveVectorIndexSnapshot(std::shared_ptr<VectorIndex> vector_index,
                                                           uint64_t& snapshot_log_index) {
  // Check if vector_index is null
  if (!vector_index) {
    DINGO_LOG(WARNING) << fmt::format("Save vector index failed, vector_index is null");
    return butil::Status(pb::error::Errno::EINTERNAL, "Save vector index failed, vector_index is null");
  }

  uint64_t apply_log_index = vector_index->ApplyLogIndex();

  std::string snapshot_parent_path = fmt::format("{}/{}", Server::GetInstance()->GetIndexPath(), vector_index->Id());
  if (!std::filesystem::exists(snapshot_parent_path)) {
    std::filesystem::create_directories(snapshot_parent_path);
  }

  // New snapshot path, if already exist then give up.
  std::string new_snapshot_path = fmt::format("{}/snapshot_{:020}", snapshot_parent_path, apply_log_index);
  if (std::filesystem::exists(new_snapshot_path)) {
    return butil::Status(pb::error::Errno::EVECTOR_SNAPSHOT_EXIST,
                         "Snapshot already exist, vector index: %lu log_id: %lu", vector_index->Id(), apply_log_index);
  }

  // Last snapshot path
  auto snapshot_paths = GetSnapshotPaths(snapshot_parent_path);

  // Temp snapshot path for save vector index.
  std::string tmp_snapshot_path = fmt::format("{}/tmp", snapshot_parent_path);
  if (std::filesystem::exists(tmp_snapshot_path)) {
    Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);
  } else {
    std::filesystem::create_directory(tmp_snapshot_path);
  }

  // Get vector index file path
  std::string index_filepath = fmt::format("{}/index_{}.idx", tmp_snapshot_path, apply_log_index);

  DINGO_LOG(INFO) << fmt::format("Save vector index {} to file {}", vector_index->Id(), index_filepath);

  // Save vector index to tmp file
  auto ret = vector_index->Save(index_filepath);
  if (!ret.ok()) {
    if (ret.error_code() == pb::error::Errno::EVECTOR_NOT_SUPPORT) {
      DINGO_LOG(WARNING) << fmt::format("Save vector index {} to tmp file not supported for this index, {}",
                                        vector_index->Id(), ret.error_str());
      return butil::Status::OK();
    }

    DINGO_LOG(ERROR) << fmt::format("Save vector index {} to tmp file failed, {}", vector_index->Id(), ret.error_str());
    Helper::RemoveAllFileOrDirectory(tmp_snapshot_path);
    return ret;
  }

  // Write vector index meta
  std::string meta_filepath = fmt::format("{}/meta", tmp_snapshot_path, vector_index->Id());
  std::ofstream meta_file(meta_filepath);
  if (!meta_file.is_open()) {
    DINGO_LOG(ERROR) << fmt::format("Open vector index file log_id file {} failed", meta_filepath);
    return butil::Status(pb::error::Errno::EINTERNAL, "Open vector index file log_id file failed");
  }

  meta_file << apply_log_index;
  meta_file.close();

  // Rename
  std::filesystem::rename(tmp_snapshot_path, new_snapshot_path);

  // Remove old snapshot
  for (auto& snapshot_path : snapshot_paths) {
    DINGO_LOG(INFO) << "delete vector index snapshot: " << snapshot_path;
    Helper::RemoveAllFileOrDirectory(snapshot_path);
  }

  // Set truncate wal log index.
  auto log_storage = Server::GetInstance()->GetLogStorageManager()->GetLogStorage(vector_index->Id());
  if (log_storage != nullptr) {
    log_storage->SetVectorIndexTruncateLogIndex(apply_log_index);
  }

  snapshot_log_index = apply_log_index;

  return butil::Status::OK();
}

// Load vector index for already exist vector index at bootstrap.
std::shared_ptr<VectorIndex> VectorIndexSnapshot::LoadVectorIndexSnapshot(store::RegionPtr region) {
  assert(region != nullptr);

  // open vector_index_file_log_id_file_path and read its content to a std::string
  uint64_t vector_index_file_log_id = VectorIndexSnapshot::GetLastVectorIndexSnapshotLogId(region->Id());
  if (vector_index_file_log_id == 0) {
    DINGO_LOG(ERROR) << fmt::format("GetLastVectorIndexSnapshotLogId failed, id {}", region->Id());
    return nullptr;
  }

  DINGO_LOG(INFO) << fmt::format("Vector index {} log id is {}", region->Id(), vector_index_file_log_id);

  // check if can load from file
  std::string vector_index_file_path =
      fmt::format("{}/{}_{}.idx", Server::GetInstance()->GetIndexPath(), region->Id(), vector_index_file_log_id);

  // check if file vector_index_file_path exists
  if (!std::filesystem::exists(vector_index_file_path)) {
    DINGO_LOG(ERROR) << fmt::format("Vector index {} file {} not exist, can't load, need to build vector_index",
                                    region->Id(), vector_index_file_path);
    return nullptr;
  }

  // create a new vector_index
  auto vector_index = VectorIndexFactory::New(region->Id(), region->InnerRegion().definition().index_parameter());
  if (!vector_index) {
    DINGO_LOG(WARNING) << fmt::format("New vector index failed, id {}", region->Id());
    return nullptr;
  }

  // load index from file
  auto ret = vector_index->Load(vector_index_file_path);
  if (!ret.ok()) {
    DINGO_LOG(WARNING) << fmt::format("Load vector index failed, id {}", region->Id());
    return nullptr;
  }

  // set vector_index apply log id
  vector_index->SetApplyLogIndex(vector_index_file_log_id);

  return vector_index;
}

}  // namespace dingodb

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

#include "diskann/diskann_utils.h"

#include <omp.h>
#include <xmmintrin.h>

#include <cerrno>
#include <filesystem>
#include <string>

#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"

namespace dingodb {
butil::Status DiskANNUtils::FileExistsAndRegular(const std::string& file_path) {
  std::filesystem::path file_path_check(file_path);
  if (std::filesystem::exists(file_path_check)) {
    std::error_code ec;
    if (std::filesystem::is_symlink(file_path)) {
      file_path_check = std::filesystem::read_symlink(file_path);
    }
    if (!std::filesystem::is_regular_file(file_path_check, ec)) {
      std::string s = fmt::format("file_path : {} is not regular file : {} {}", file_path, ec.value(), ec.message());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EFILE_NOT_REGULAR, s);
    }

    auto perms = std::filesystem::status(file_path_check, ec).permissions();
    if (ec) {
      std::string s = fmt::format("file_path : {} does not have owner read permission", file_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // Check if the file has owner read permission
    bool has_owner_read = (perms & std::filesystem::perms::owner_read) != std::filesystem::perms::none;
    if (!has_owner_read) {
      std::string s = fmt::format("file_path : {} does not have owner read permission", file_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // TODO : Check if the file has owner write permission
  } else {  // data_path not exist
    std::string s = fmt::format("file_path : {} not exist", file_path);
    // DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EFILE_NOT_EXIST, s);
  }
  return butil::Status::OK();
}

butil::Status DiskANNUtils::DirExists(const std::string& dir_path) {
  std::filesystem::path dir_path_check(dir_path);
  if (std::filesystem::exists(dir_path_check)) {
    std::error_code ec;
    if (std::filesystem::is_symlink(dir_path)) {
      dir_path_check = std::filesystem::read_symlink(dir_path);
    }

    if (!std::filesystem::is_directory(dir_path_check, ec)) {
      std::string s = fmt::format("dir_path : {} is not directory, {}, {}", dir_path, ec.value(), ec.message());
      // DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EFILE_NOT_DIRECTORY, s);
    }
    auto perms = std::filesystem::status(dir_path_check, ec).permissions();
    if (ec) {
      std::string s = fmt::format("dir_path : {} does not have owner read permission", dir_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // Check if the file has owner read permission
    bool has_owner_read = (perms & std::filesystem::perms::owner_read) != std::filesystem::perms::none;
    if (!has_owner_read) {
      std::string s = fmt::format("dir_path : {} does not have owner read permission", dir_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // TODO : Check if the file has owner write permission
  } else {  // index_path_prefix not exist
    std::string s = fmt::format("dir_path : {} not exist", dir_path);
    // DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EFILE_NOT_EXIST, s);
  }
  return butil::Status::OK();
}

butil::Status DiskANNUtils::ClearDir(const std::string& dir_path) {
  butil::Status status = DirExists(dir_path);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  return RemoveAllDir(dir_path, false);
}

butil::Status DiskANNUtils::DiskANNIndexPathPrefixExists(const std::string& dir_path, bool& build_with_mem_index,
                                                         pb::common::MetricType metric_type) {
  build_with_mem_index = false;

  butil::Status status = DirExists(dir_path);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  std::string dir_path2 = dir_path;
  if (!dir_path2.empty() && dir_path2.back() != '/') {
    dir_path2 += "/";
  }

  std::vector<std::string> common_files;
  common_files.push_back(dir_path2 + "_disk.index");
  common_files.push_back(dir_path2 + "_pq_compressed.bin");
  common_files.push_back(dir_path2 + "_pq_pivots.bin");
  common_files.push_back(dir_path2 + "_sample_data.bin");
  common_files.push_back(dir_path2 + "_sample_ids.bin");
  if (pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT == metric_type) {
    common_files.push_back(dir_path2 + "_disk.index_max_base_norm.bin");
  }

  std::vector<std::string> mem_files;
  mem_files.push_back(dir_path2 + "_mem.index.data");

  std::vector<std::string> disk_files;
  disk_files.push_back(dir_path2 + "_disk.index_centroids.bin");
  disk_files.push_back(dir_path2 + "_disk.index_medoids.bin");

  for (const auto& file : common_files) {
    status = DiskANNUtils::FileExistsAndRegular(file);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  }

  for (const auto& file : mem_files) {
    status = DiskANNUtils::FileExistsAndRegular(file);
    if (status.ok()) {
      build_with_mem_index = true;
    }
  }

  if (!build_with_mem_index) {
    for (const auto& file : disk_files) {
      status = DiskANNUtils::FileExistsAndRegular(file);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
    }
  } else {  // already build with mem index. disk must not exist
    for (const auto& file : disk_files) {
      status = DiskANNUtils::FileExistsAndRegular(file);
      if (status.ok()) {
        std::string s =
            fmt::format("dir_path : {} already build with mem index, but disk index exist : {}", dir_path, file);
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
    }
  }

  return butil::Status::OK();
}

butil::Status DiskANNUtils::RemoveFile(const std::string& file_path) {
  butil::Status status = FileExistsAndRegular(file_path);
  if (!status.ok()) {
    // DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  std::error_code ec;
  std::filesystem::remove(file_path, ec);
  if (0 != ec.value()) {
    std::string s = fmt::format("remove file : {} failed, {} {}", file_path, ec.value(), ec.message());
    DINGO_LOG(ERROR) << s;
    if (ec.value() == EACCES) {
      return butil::Status(pb::error::Errno::EFILE_PERMISSION_DENIED, s);
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }
  return butil::Status::OK();
}

butil::Status DiskANNUtils::RemoveDir(const std::string& dir_path) {
  butil::Status status = DirExists(dir_path);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  return RemoveAllDir(dir_path, true);
}

butil::Status DiskANNUtils::Rename(const std::string& old_path, const std::string& new_path) {
  std::error_code ec;

  butil::Status status = DirExists(old_path);
  if (status.ok()) {
    return RenameDir(old_path, new_path);
  } else if (pb::error::Errno::EFILE_NOT_DIRECTORY == status.error_code()) {
    status = FileExistsAndRegular(old_path);
    if (status.ok()) {
      return RenameFile(old_path, new_path);
    }
  }

  DINGO_LOG(ERROR) << status.error_cstr();
  return status;
}

std::string DiskANNUtils::DiskANNCoreStateToString(DiskANNCoreState state) {
  std::string str = "Unknown";
  switch (state) {
    case DiskANNCoreState::kUnknown: {
      str = "DiskANNCoreState::kUnknown";
      break;
    }
    case DiskANNCoreState::kImporting: {
      str = "DiskANNCoreState::kImporting";
      break;
    }
    case DiskANNCoreState::kImported: {
      str = "DiskANNCoreState::kImported";
      break;
    }
    case DiskANNCoreState::kUninitialized: {
      str = "DiskANNCoreState::kUninitialized";
      break;
    }
    case DiskANNCoreState::kInitialized: {
      str = "DiskANNCoreState::kInitialized";
      break;
    }
    case DiskANNCoreState::kBuilding: {
      str = "DiskANNCoreState::kBuilding";
      break;
    }
    case DiskANNCoreState::kBuilded: {
      str = "DiskANNCoreState::kBuilded";
      break;
    }
    case DiskANNCoreState::kUpdatingPath: {
      str = "DiskANNCoreState::kUpdatingPath";
      break;
    }
    case DiskANNCoreState::kUpdatedPath: {
      str = "DiskANNCoreState::kUpdatedPath";
      break;
    }
    case DiskANNCoreState::kLoading: {
      str = "DiskANNCoreState::kLoading";
      break;
    }
    case DiskANNCoreState::kLoaded: {
      str = "DiskANNCoreState::kLoaded";
      break;
    }
    case DiskANNCoreState::kSearing: {
      str = "DiskANNCoreState::kSearing";
      break;
    }
    case DiskANNCoreState::kSearched: {
      str = "DiskANNCoreState::kSearched";
      break;
    }
    case DiskANNCoreState::kReseting: {
      str = "DiskANNCoreState::kReseting";
      break;
    }
    case DiskANNCoreState::kReset: {
      str = "DiskANNCoreState::kReset";
      break;
    }
    case DiskANNCoreState::kDestroying: {
      str = "DiskANNCoreState::kDestroying";
      break;
    }
    case DiskANNCoreState::kDestroyed: {
      str = "DiskANNCoreState::kDestroyed";
      break;
    }
    case DiskANNCoreState::kIdle: {
      str = "DiskANNCoreState::kIdle";
      break;
    }
    case DiskANNCoreState::kFailed: {
      str = "DiskANNCoreState::kFailed";
      break;
    }
    default:
      break;
  }

  return str;
}

std::string DiskANNUtils::DiskANNCoreStateToString(const std::atomic<DiskANNCoreState>& state) {
  DiskANNCoreState state_val = state.load(std::memory_order_relaxed);
  return DiskANNUtils::DiskANNCoreStateToString(state_val);
}

butil::Status DiskANNUtils::RemoveAllDir(const std::string& dir_path, bool remove_self) {
  std::error_code ec;

  try {
    std::filesystem::path dir_path_check(dir_path);
    std::filesystem::path dir_path_link_check(dir_path);

    if (std::filesystem::is_symlink(dir_path)) {
      dir_path_check = std::filesystem::read_symlink(dir_path);
    }

    if (!remove_self) {
      for (const auto& entry : std::filesystem::directory_iterator(dir_path_check)) {
        std::filesystem::remove_all(entry, ec);
        if (0 != ec.value()) {
          std::string s =
              fmt::format("remove_all dir_path : {} failed, {} {}", entry.path().string(), ec.value(), ec.message());
          DINGO_LOG(ERROR) << s;
          if (ec.value() == EACCES) {
            return butil::Status(pb::error::Errno::EFILE_PERMISSION_DENIED, s);
          } else {
            return butil::Status(pb::error::Errno::EINTERNAL, s);
          }
        }
      }
    } else {
      std::filesystem::remove_all(dir_path_check, ec);
      if (0 != ec.value()) {
        std::string s =
            fmt::format("remove_all dir_path : {} failed, {} {}", dir_path_check.string(), ec.value(), ec.message());
        DINGO_LOG(ERROR) << s;
        if (ec.value() == EACCES) {
          return butil::Status(pb::error::Errno::EFILE_PERMISSION_DENIED, s);
        } else {
          return butil::Status(pb::error::Errno::EINTERNAL, s);
        }
      }
    }

    if (remove_self) {
      if (std::filesystem::is_symlink(dir_path)) {
        std::filesystem::remove(dir_path_link_check, ec);
        if (0 != ec.value()) {
          std::string s = fmt::format("remove_all dir_path : {} failed, {} {}", dir_path_link_check.string(),
                                      ec.value(), ec.message());
          DINGO_LOG(ERROR) << s;
          if (ec.value() == EACCES) {
            return butil::Status(pb::error::Errno::EFILE_PERMISSION_DENIED, s);
          } else {
            return butil::Status(pb::error::Errno::EINTERNAL, s);
          }
        }
      }
    }
  } catch (const std::filesystem::filesystem_error& e) {
    std::string s = fmt::format("clear index_path_prefix : {} failed, {}", dir_path, e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }
  return butil::Status::OK();
}

butil::Status DiskANNUtils::CreateDir(const std::string& dir_path) {
  butil::Status status = DirExists(dir_path);
  if (status.ok()) {
    return butil::Status::OK();
  } else {
    std::error_code ec;
    std::filesystem::create_directory(dir_path, ec);
    if (0 != ec.value()) {
      std::string s = fmt::format("remove_all dir_path : {} failed, {} {}", dir_path, ec.value(), ec.message());
      DINGO_LOG(ERROR) << s;
      if (ec.value() == EACCES) {
        return butil::Status(pb::error::Errno::EFILE_PERMISSION_DENIED, s);
      } else {
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
    }
  }

  return butil::Status::OK();
}

pb::common::DiskANNCoreState DiskANNUtils::DiskANNCoreStateToPb(DiskANNCoreState state) {
  pb::common::DiskANNCoreState pb_state = pb::common::DiskANNCoreState::UNKNOWN;

  switch (state) {
    case DiskANNCoreState::kUnknown: {
      pb_state = pb::common::DiskANNCoreState::UNKNOWN;
      break;
    }
    case DiskANNCoreState::kImporting: {
      pb_state = pb::common::DiskANNCoreState::IMPORTING;
      break;
    }
    case DiskANNCoreState::kImported: {
      pb_state = pb::common::DiskANNCoreState::IMPORTED;
      break;
    }
    case DiskANNCoreState::kUninitialized: {
      pb_state = pb::common::DiskANNCoreState::UNINITIALIZED;
      break;
    }
    case DiskANNCoreState::kInitialized: {
      pb_state = pb::common::DiskANNCoreState::INITIALIZED;
      break;
    }
    case DiskANNCoreState::kBuilding: {
      pb_state = pb::common::DiskANNCoreState::BUILDING;
      break;
    }
    case DiskANNCoreState::kBuilded: {
      pb_state = pb::common::DiskANNCoreState::BUILDED;
      break;
    }
    case DiskANNCoreState::kUpdatingPath: {
      pb_state = pb::common::DiskANNCoreState::UPDATINGPATH;
      break;
    }
    case DiskANNCoreState::kUpdatedPath: {
      pb_state = pb::common::DiskANNCoreState::UPDATEDPATH;
      break;
    }
    case DiskANNCoreState::kLoading: {
      pb_state = pb::common::DiskANNCoreState::LOADING;
      break;
    }
    case DiskANNCoreState::kLoaded: {
      pb_state = pb::common::DiskANNCoreState::LOADED;
      break;
    }
    case DiskANNCoreState::kSearing: {
      pb_state = pb::common::DiskANNCoreState::SEARING;
      break;
    }
    case DiskANNCoreState::kSearched: {
      pb_state = pb::common::DiskANNCoreState::SEARCHED;
      break;
    }

    case DiskANNCoreState::kReseting: {
      pb_state = pb::common::DiskANNCoreState::RESETING;
      break;
    }
    case DiskANNCoreState::kReset: {
      pb_state = pb::common::DiskANNCoreState::RESET;
      break;
    }
    case DiskANNCoreState::kDestroying: {
      pb_state = pb::common::DiskANNCoreState::DESTROYING;
      break;
    }
    case DiskANNCoreState::kDestroyed: {
      pb_state = pb::common::DiskANNCoreState::DESTROYED;
      break;
    }
    case DiskANNCoreState::kIdle: {
      pb_state = pb::common::DiskANNCoreState::IDLE;
      break;
    }
    case DiskANNCoreState::kFailed: {
      pb_state = pb::common::DiskANNCoreState::FAILED;
      break;
    }
    default:
      break;
  }

  return pb_state;
}
DiskANNCoreState DiskANNUtils::DiskANNCoreStateFromPb(pb::common::DiskANNCoreState state) {
  DiskANNCoreState core_state = DiskANNCoreState::kUnknown;

  switch (state) {
    case pb::common::DiskANNCoreState::UNKNOWN: {
      core_state = DiskANNCoreState::kUnknown;
      break;
    }
    case pb::common::DiskANNCoreState::IMPORTING: {
      core_state = DiskANNCoreState::kImporting;
      break;
    }
    case pb::common::DiskANNCoreState::IMPORTED: {
      core_state = DiskANNCoreState::kImported;
      break;
    }
    case pb::common::DiskANNCoreState::UNINITIALIZED: {
      core_state = DiskANNCoreState::kUninitialized;
      break;
    }
    case pb::common::DiskANNCoreState::INITIALIZED: {
      core_state = DiskANNCoreState::kInitialized;
      break;
    }
    case pb::common::DiskANNCoreState::BUILDING: {
      core_state = DiskANNCoreState::kBuilding;
      break;
    }
    case pb::common::DiskANNCoreState::BUILDED: {
      core_state = DiskANNCoreState::kBuilded;
      break;
    }
    case pb::common::DiskANNCoreState::UPDATINGPATH: {
      core_state = DiskANNCoreState::kUpdatingPath;
      break;
    }
    case pb::common::DiskANNCoreState::UPDATEDPATH: {
      core_state = DiskANNCoreState::kUpdatedPath;
      break;
    }
    case pb::common::DiskANNCoreState::LOADING: {
      core_state = DiskANNCoreState::kLoading;
      break;
    }
    case pb::common::DiskANNCoreState::LOADED: {
      core_state = DiskANNCoreState::kLoaded;
      break;
    }
    case pb::common::DiskANNCoreState::SEARING: {
      core_state = DiskANNCoreState::kSearing;
      break;
    }
    case pb::common::DiskANNCoreState::SEARCHED: {
      core_state = DiskANNCoreState::kSearched;
      break;
    }
    case pb::common::DiskANNCoreState::RESETING: {
      core_state = DiskANNCoreState::kReseting;
      break;
    }
    case pb::common::DiskANNCoreState::RESET: {
      core_state = DiskANNCoreState::kReset;
      break;
    }
    case pb::common::DiskANNCoreState::DESTROYING: {
      core_state = DiskANNCoreState::kDestroying;
      break;
    }
    case pb::common::DiskANNCoreState::DESTROYED: {
      core_state = DiskANNCoreState::kDestroyed;
      break;
    }
    case pb::common::DiskANNCoreState::IDLE: {
      core_state = DiskANNCoreState::kIdle;
      break;
    }
    case pb::common::DiskANNCoreState::FAILED: {
      core_state = DiskANNCoreState::kFailed;
      break;
    }
    default:
      break;
  }

  return core_state;
}

butil::Status DiskANNUtils::RenameDir(const std::string& old_dir_path, const std::string& new_dir_path) {
  std::error_code ec;

  butil::Status status = DirExists(new_dir_path);
  if (pb::error::Errno::OK == status.error_code()) {
    status = RemoveDir(new_dir_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  } else if (pb::error::Errno::EFILE_NOT_EXIST == status.error_code()) {
    // do nothing
  } else {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  std::filesystem::rename(old_dir_path, new_dir_path, ec);
  if (0 != ec.value()) {
    std::string s =
        fmt::format("rename dir : {} to {} failed, {} {}", old_dir_path, new_dir_path, ec.value(), ec.message());
    DINGO_LOG(ERROR) << s;
    if (ec.value() == EXDEV) {
      return butil::Status(pb::error::Errno::EFILE_CROSS_DEVICE, s);
    } else if (ec.value() == EACCES) {
      return butil::Status(pb::error::Errno::EFILE_PERMISSION_DENIED, s);
    } else if (ec.value() == ENOENT) {
      return butil::Status(pb::error::Errno::EFILE_NOT_EXIST, s);
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  return butil::Status::OK();
}
butil::Status DiskANNUtils::RenameFile(const std::string& old_file_path, const std::string& new_file_path) {
  std::error_code ec;

  butil::Status status = FileExistsAndRegular(new_file_path);
  if (pb::error::Errno::OK == status.error_code()) {
    status = RemoveFile(new_file_path);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
  } else if (pb::error::Errno::EFILE_NOT_EXIST == status.error_code()) {
    // do nothing
  } else {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  std::filesystem::rename(old_file_path, new_file_path, ec);
  if (0 != ec.value()) {
    std::string s =
        fmt::format("rename dir : {} to {} failed, {} {}", old_file_path, new_file_path, ec.value(), ec.message());
    DINGO_LOG(ERROR) << s;

    if (ec.value() == EXDEV) {
      return butil::Status(pb::error::Errno::EFILE_CROSS_DEVICE, s);
    } else if (ec.value() == EACCES) {
      return butil::Status(pb::error::Errno::EFILE_PERMISSION_DENIED, s);
    } else if (ec.value() == ENOENT) {
      return butil::Status(pb::error::Errno::EFILE_NOT_EXIST, s);
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb

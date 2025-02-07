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

#include "br/utils.h"

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <string>

#include "common/logging.h"
#include "coordinator/tso_control.h"
#include "fmt/core.h"
#include "proto/error.pb.h"

namespace br {

butil::Status Utils::FileExistsAndRegular(const std::string& file_path) {
  std::filesystem::path file_path_check(file_path);
  if (std::filesystem::exists(file_path_check)) {
    std::error_code ec;
    if (std::filesystem::is_symlink(file_path)) {
      file_path_check = std::filesystem::read_symlink(file_path);
    }
    if (!std::filesystem::is_regular_file(file_path_check, ec)) {
      std::string s = fmt::format("file_path : {} is not regular file : {} {}", file_path, ec.value(), ec.message());
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_REGULAR, s);
    }

    auto perms = std::filesystem::status(file_path_check, ec).permissions();
    if (ec) {
      std::string s = fmt::format("file_path : {} does not have owner read permission", file_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // Check if the file has owner read permission
    bool has_owner_read = (perms & std::filesystem::perms::owner_read) != std::filesystem::perms::none;
    if (!has_owner_read) {
      std::string s = fmt::format("file_path : {} does not have owner read permission", file_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // TODO : Check if the file has owner write permission
  } else {  // data_path not exist
    std::string s = fmt::format("file_path : {} not exist", file_path);
    // DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_EXIST, s);
  }
  return butil::Status::OK();
}

butil::Status Utils::DirExists(const std::string& dir_path) {
  std::filesystem::path dir_path_check(dir_path);
  if (std::filesystem::exists(dir_path_check)) {
    std::error_code ec;
    if (std::filesystem::is_symlink(dir_path)) {
      dir_path_check = std::filesystem::read_symlink(dir_path);
    }

    if (!std::filesystem::is_directory(dir_path_check, ec)) {
      std::string s = fmt::format("dir_path : {} is not directory, {}, {}", dir_path, ec.value(), ec.message());
      // DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_DIRECTORY, s);
    }
    auto perms = std::filesystem::status(dir_path_check, ec).permissions();
    if (ec) {
      std::string s = fmt::format("dir_path : {} does not have owner read permission", dir_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // Check if the file has owner read permission
    bool has_owner_read = (perms & std::filesystem::perms::owner_read) != std::filesystem::perms::none;
    if (!has_owner_read) {
      std::string s = fmt::format("dir_path : {} does not have owner read permission", dir_path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_OWNER_READABLE, s);
    }
    // TODO : Check if the file has owner write permission
  } else {  // index_path_prefix not exist
    std::string s = fmt::format("dir_path : {} not exist", dir_path);
    // DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EFILE_NOT_EXIST, s);
  }
  return butil::Status::OK();
}

butil::Status Utils::ClearDir(const std::string& dir_path) {
  butil::Status status = DirExists(dir_path);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }
  return RemoveAllDir(dir_path, false);
}

butil::Status Utils::CreateDir(const std::string& dir_path) {
  butil::Status status = DirExists(dir_path);
  if (status.ok()) {
    return butil::Status::OK();
  } else {
    std::error_code ec;
    std::filesystem::create_directory(dir_path, ec);
    if (0 != ec.value()) {
      std::string s = fmt::format("create_directory dir_path : {} failed, {} {}", dir_path, ec.value(), ec.message());
      DINGO_LOG(ERROR) << s;
      if (ec.value() == EACCES) {
        return butil::Status(dingodb::pb::error::Errno::EFILE_PERMISSION_DENIED, s);
      } else {
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
    }
  }

  return butil::Status::OK();
}

butil::Status Utils::CreateDirRecursion(const std::string& dir_path) {
  butil::Status status = DirExists(dir_path);
  if (status.ok()) {
    return butil::Status::OK();
  } else {
    std::error_code ec;
    std::filesystem::create_directories(dir_path, ec);
    if (0 != ec.value()) {
      std::string s = fmt::format("create_directories dir_path : {} failed, {} {}", dir_path, ec.value(), ec.message());
      DINGO_LOG(ERROR) << s;
      if (ec.value() == EACCES) {
        return butil::Status(dingodb::pb::error::Errno::EFILE_PERMISSION_DENIED, s);
      } else {
        return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
      }
    }
  }

  return butil::Status::OK();
}

butil::Status Utils::CreateFile(std::ofstream& writer, const std::string& filename) {
  writer.exceptions(std::ofstream::failbit | std::ofstream::badbit);
  writer.open(filename, std::ios::binary | std::ios::out);

  if (writer.fail()) {
    char buff[1024];
    auto ret = std::string(strerror_r(errno, buff, 1024));
    std::string s = std::string("Failed to open file") + filename + " for write because " + buff + ", ret=" + ret;
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  return butil::Status::OK();
}

butil::Status Utils::RemoveAllDir(const std::string& dir_path, bool remove_self) {
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
            return butil::Status(dingodb::pb::error::Errno::EFILE_PERMISSION_DENIED, s);
          } else {
            return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
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
          return butil::Status(dingodb::pb::error::Errno::EFILE_PERMISSION_DENIED, s);
        } else {
          return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
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
            return butil::Status(dingodb::pb::error::Errno::EFILE_PERMISSION_DENIED, s);
          } else {
            return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
          }
        }
      }
    }
  } catch (const std::filesystem::filesystem_error& e) {
    std::string s = fmt::format("clear index_path_prefix : {} failed, {}", dir_path, e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }
  return butil::Status::OK();
}

int64_t Utils::ConvertToMilliseconds(const std::string& datetime) {
  std::istringstream ss(datetime);

  std::tm tm = {};
  ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

  if (ss.fail()) {
    throw std::runtime_error("Failed to parse date-time");
  }

  std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));

  auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();

  return milliseconds;
}

// std::string backup_ts = "2022-09-08 13:30:00 +08:00";
// TODO : Ignore the time zone for now and use Boost.DateTime or date.h provided by Howard Hinnant later.
butil::Status Utils::ConvertBackupTsToTso(const std::string& backup_ts, int64_t& tso) {
  int64_t milliseconds = 0;
  try {
    milliseconds = ConvertToMilliseconds(backup_ts);

  } catch (const std::exception& e) {
    std::string s = fmt::format("Failed to parse backup_ts, {} backup_ts : {}", e.what(), backup_ts);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::EINTERNAL, s);
  }

  int64_t internal_milliseconds = milliseconds - dingodb::kBaseTimestampMs;
  tso = 0;
  tso = internal_milliseconds << dingodb::kLogicalBits;

  return butil::Status();
}

std::string Utils::ConvertTsoToDateTime(int64_t tso) {
  int64_t milliseconds = tso >> dingodb::kLogicalBits;
  milliseconds += dingodb::kBaseTimestampMs;

  std::chrono::milliseconds ms(milliseconds);
  auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>(ms);

  std::time_t time = std::chrono::system_clock::to_time_t(tp);
  std::tm tm = *std::localtime(&time);

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");

  return oss.str();
}

butil::Status Utils::ReadFile(std::ifstream& reader, const std::string& filename) {
  reader.exceptions(std::ifstream::failbit | std::ifstream::badbit);
  reader.open(filename, std::ios::binary | std::ios::in);

  if (reader.fail()) {
    char buff[1024];
    auto ret = std::string(strerror_r(errno, buff, 1024));
    std::string s = std::string("Failed to open file") + filename + " for read because " + buff + ", ret=" + ret;
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  return butil::Status::OK();
}

butil::Status Utils::CheckBackupMeta(std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta,
                                     const std::string& storage_internal, const std::string& file_name,
                                     const std::string& dir_name, const std::string& exec_node) {
  butil::Status status;

  const std::string& internal_file_name = backup_meta->file_name();
  if (internal_file_name != file_name) {
    std::string s = fmt::format("file_name: {} is invalid. must be equal to : {} ", internal_file_name, file_name);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_FIELD_NOT_MATCH, s);
  }

  const std::string& internal_dir_name = backup_meta->dir_name();
  if (internal_dir_name != dir_name) {
    std::string s = fmt::format("dir_name: {} is invalid. must be equal to : {}", internal_dir_name, dir_name);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_FIELD_NOT_MATCH, s);
  }

  const std::string& internal_exec_node = backup_meta->exec_node();
  if (internal_exec_node != exec_node) {
    std::string s = fmt::format("exec_node: {} is invalid. must be equal to : {}", internal_exec_node, exec_node);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_FIELD_NOT_MATCH, s);
  }

  uint64_t file_size = backup_meta->file_size();
  uint64_t calc_file_size = 0;

  std::string file_path = storage_internal + "/" + file_name;
  calc_file_size = static_cast<uint64_t>(dingodb::Helper::GetFileSize(file_path));
  if (file_size != calc_file_size) {
    std::string s = fmt::format("file_size: {} is invalid. must be equal to : {}", file_size, calc_file_size);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_FIELD_NOT_MATCH, s);
  }

  const std::string encryption = backup_meta->encryption();
  std::string calc_encryption;
  status = dingodb::Helper::CalSha1CodeWithFileEx(file_path, calc_encryption);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (encryption != calc_encryption) {
    std::string s = fmt::format("encryption: {} is invalid. must be equal to : {}", encryption, calc_encryption);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::ERESTORE_FILE_CHECKSUM_NOT_MATCH, s);
  }

  return butil::Status::OK();
}

}  // namespace br
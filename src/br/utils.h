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

#ifndef DINGODB_BR_UTILS_H_
#define DINGODB_BR_UTILS_H_

#include <fstream>
#include <memory>
#include <string>

#include "butil/status.h"
#include "proto/common.pb.h"

namespace br {

class Utils {
 public:
  static butil::Status FileExistsAndRegular(const std::string& file_path);
  static butil::Status DirExists(const std::string& dir_path);
  static butil::Status ClearDir(const std::string& dir_path);
  static butil::Status CreateDir(const std::string& dir_path);
  static butil::Status CreateDirRecursion(const std::string& dir_path);
  static butil::Status CreateFile(std::ofstream& writer, const std::string& filename);
  static butil::Status RemoveAllDir(const std::string& dir_path, bool remove_self);

  static int64_t ConvertToMilliseconds(const std::string& datetime);
  static butil::Status ConvertBackupTsToTso(const std::string& backup_ts, int64_t& tso);
  static std::string ConvertTsoToDateTime(int64_t tso);

  static butil::Status ReadFile(std::ifstream& reader, const std::string& filename);

  static butil::Status CheckBackupMeta(std::shared_ptr<dingodb::pb::common::BackupMeta> backup_meta,
                                       const std::string& storage_internal, const std::string& file_name,
                                       const std::string& dir_name, const std::string& exec_node);

 private:
  Utils() = default;
  ~Utils() = default;
};
}  // namespace br

#endif  // DINGODB_BR_UTILS_H_
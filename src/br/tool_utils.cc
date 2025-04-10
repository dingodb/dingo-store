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

#include "br/tool_utils.h"

#include <filesystem>
#include <iostream>

#include "br/utils.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "proto/error.pb.h"

namespace br {

void ToolUtils::PrintDumpHead() { ToolUtils::PrintDumpHeadOrTail(); }

void ToolUtils::PrintDumpItem(const std::string& item_name, const std::string& item) {
  std::cerr << item_name << " : " << item << std::endl;
  DINGO_LOG(INFO) << item_name << " : " << item;
}

void ToolUtils::PrintDumpTail() { ToolUtils::PrintDumpHeadOrTail(); }

void ToolUtils::PrintDumpHeadOrTail() {
  std::cerr << "========================================" << std::endl;
  DINGO_LOG(INFO) << "========================================";
}

butil::Status ToolUtils::CheckParameterAndHandle(const std::string& br_diff_file, const std::string& br_diff_file_name,
                                                 std::string& path_internal, std::string& file_name) {
  butil::Status status;

  std::string path = br_diff_file;

  if (path.find("local://") == 0) {
    path = path.replace(path.find("local://"), 8, "");
    if (!path.empty()) {
      if (path.back() == '/') {
        path.pop_back();
      }
    }

    if (path.empty()) {
      std::string s = fmt::format("path is empty, please check parameter --{}={}", br_diff_file_name, br_diff_file);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    std::filesystem::path temp_path = path;
    if (temp_path.is_relative()) {
      std::string s = fmt::format("file not support relative path. use absolute path. {}", br_diff_file);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    status = Utils::FileExistsAndRegular(temp_path);
    if (!status.ok()) {
      std::string s = fmt::format("file not exist or not regular. {}", br_diff_file);
      DINGO_LOG(ERROR) << s;
      return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
    }

    path_internal = path;
  } else {
    std::string s = fmt::format("file not support, please check parameter --{}={}", br_diff_file_name, br_diff_file);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  // split file name
  size_t pos = path_internal.find_last_of('/');
  if (pos == std::string::npos) {
    file_name = path_internal;
  } else {
    file_name = path_internal.substr(pos + 1);
  }

  if (file_name.empty()) {
    std::string s = fmt::format("file name is empty, please check parameter --{}={}", br_diff_file_name, br_diff_file);
    DINGO_LOG(ERROR) << s;
    return butil::Status(dingodb::pb::error::Errno::EINTERNAL, s);
  }

  return butil::Status::OK();
}

std::vector<std::string_view> ToolUtils::Split(std::string_view str, char delimiter) {
  std::vector<std::string_view> result;
  size_t start = 0;
  size_t end = str.find(delimiter);

  while (end != std::string_view::npos) {
    result.emplace_back(str.substr(start, end - start));
    start = end + 1;
    end = str.find(delimiter, start);
  }
  result.emplace_back(str.substr(start));

  return result;
}

}  // namespace br
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

#ifndef DINGODB_BR_TOOL_UTILS_H_
#define DINGODB_BR_TOOL_UTILS_H_

#include <string>
#include <vector>

#include "butil/status.h"

namespace br {

class ToolUtils {
 public:
  static void PrintDumpHead();
  static void PrintDumpItem(const std::string& item_name, const std::string& item);
  static void PrintDumpTail();
  static butil::Status CheckParameterAndHandle(const std::string& br_diff_file, const std::string& br_diff_file_name,
                                               std::string& path_internal, std::string& file_name);
  static std::vector<std::string_view> Split(std::string_view str, char delimiter);

 private:
  static void PrintDumpHeadOrTail();
  ToolUtils() = default;
  ~ToolUtils() = default;
};
}  // namespace br

#endif  // DINGODB_BR_TOOL_UTILS_H_
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

#ifndef DINGODB_BR_HELPER_H_
#define DINGODB_BR_HELPER_H_

#include <ctime>
#include <string>
#include <vector>

#include "butil/endpoint.h"
#include "fmt/core.h"

namespace br {

class Helper {
 public:
  static std::string Ltrim(const std::string& s, const std::string& delete_str);

  static std::string Rtrim(const std::string& s, const std::string& delete_str);

  static std::string Trim(const std::string& s, const std::string& delete_str);

  static int GetRandInt();

  static std::vector<butil::EndPoint> StringToEndpoints(const std::string& str);

  static std::vector<butil::EndPoint> VectorToEndpoints(std::vector<std::string> addrs);

  static std::vector<std::string> GetAddrsFromFile(const std::string& path);
};

}  // namespace br

#endif  // DINGODB_BR_HELPER_H_
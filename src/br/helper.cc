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

#include "br/helper.h"

#include <fstream>
#include <random>

#include "butil/strings/string_split.h"

namespace br {

std::string Helper::Ltrim(const std::string& s, const std::string& delete_str) {
  size_t start = s.find_first_not_of(delete_str);
  return (start == std::string::npos) ? "" : s.substr(start);
}

std::string Helper::Rtrim(const std::string& s, const std::string& delete_str) {
  size_t end = s.find_last_not_of(delete_str);
  return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

std::string Helper::Trim(const std::string& s, const std::string& delete_str) {
  return Rtrim(Ltrim(s, delete_str), delete_str);
}

int Helper::GetRandInt() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<std::mt19937::result_type> distrib(1, 1000000000);
  return distrib(gen);
}

std::vector<butil::EndPoint> Helper::StringToEndpoints(const std::string& str) {
  std::vector<std::string> addrs;
  butil::SplitString(str, ',', &addrs);

  std::vector<butil::EndPoint> endpoints;
  for (const auto& addr : addrs) {
    butil::EndPoint endpoint;
    if (butil::hostname2endpoint(addr.c_str(), &endpoint) != 0 && str2endpoint(addr.c_str(), &endpoint) != 0) {
      continue;
    }

    endpoints.push_back(endpoint);
  }

  return endpoints;
}

std::vector<butil::EndPoint> Helper::VectorToEndpoints(std::vector<std::string> addrs) {
  std::vector<butil::EndPoint> endpoints;
  for (const auto& addr : addrs) {
    butil::EndPoint endpoint;
    if (butil::hostname2endpoint(addr.c_str(), &endpoint) != 0 && str2endpoint(addr.c_str(), &endpoint) != 0) {
      continue;
    }

    endpoints.push_back(endpoint);
  }

  return endpoints;
}

std::vector<std::string> Helper::GetAddrsFromFile(const std::string& path) {
  std::vector<std::string> addrs;

  std::ifstream input(path);
  for (std::string line; getline(input, line);) {
    if (line.find('#') != std::string::npos) {
      continue;
    }

    addrs.push_back(Trim(line, " "));
  }

  return addrs;
}

}  // namespace br

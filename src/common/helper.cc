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

#include "common/helper.h"

#include <regex>

#include "butil/strings/string_split.h"

namespace dingodb {

bool Helper::IsIp(const std::string& s) {
  std::regex reg(
      "(?=(\\b|\\D))(((\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))\\.){3}(("
      "\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))(?=(\\b|\\D))");
  return std::regex_match(s, reg);
}

std::vector<pb::common::Location> Helper::ExtractLocations(
    const google::protobuf::RepeatedPtrField<pb::common::Store>& stores) {
  std::vector<pb::common::Location> locations;
  for (auto store : stores) {
    locations.push_back(store.raft_location());
  }
  return locations;
}

// format: 127.0.0.1:8201:0
std::string Helper::LocationToString(const pb::common::Location& location) {
  return butil::StringPrintf("%s:%d:0", location.host().c_str(),
                             location.port());
}

// format: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
std::string Helper::FormatPeers(
    const std::vector<pb::common::Location>& locations) {
  std::string s;
  for (int i = 0; i < locations.size(); ++i) {
    s += LocationToString(locations[i]);
    if (i + 1 < locations.size()) {
      s += ",";
    }
  }
  return s;
}

std::vector<butil::EndPoint> Helper::StrToEndpoint(const std::string& str) {
  std::vector<std::string> addrs;
  butil::SplitString(str, ',', &addrs);

  std::vector<butil::EndPoint> endpoints;
  for (auto addr : addrs) {
    butil::EndPoint endpoint;
    if (butil::hostname2endpoint(addr.c_str(), &endpoint) != 0 &&
        str2endpoint(addr.c_str(), &endpoint) != 0) {
      continue;
    }

    endpoints.push_back(endpoint);
  }

  return endpoints;
}

}  // namespace dingodb
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

#ifndef DINGODB_COMMON_HELPER_H_
#define DINGODB_COMMON_HELPER_H_

#include <string>
#include <vector>

#include "butil/strings/stringprintf.h"
#include "proto/common.pb.h"

namespace dingodb {

class Helper {
 public:
  static bool IsIp(const std::string& s);

  static std::vector<pb::common::Location> ExtractLocations(
      const google::protobuf::RepeatedPtrField<pb::common::Store>& stores) {
    std::vector<pb::common::Location> locations;
    for (auto store : stores) {
      locations.push_back(store.raft_location());
    }
    return locations;
  }

  // format: 127.0.0.1:8201:0
  static std::string LocationToString(const pb::common::Location& location) {
    return butil::StringPrintf("%s:%d:0", location.host().c_str(),
                               location.port());
  }

  // format: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
  static std::string FormatPeers(
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
};

}  // namespace dingodb

#endif

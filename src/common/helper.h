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

#include "butil/endpoint.h"
#include "butil/strings/stringprintf.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class Helper {
 public:
  static bool IsIp(const std::string& s);

  static butil::EndPoint GetEndPoint(const std::string& host, int port);

  static bool IsDifferenceLocation(const pb::common::Location& location,
                                   const pb::common::Location& other_location);
  static bool IsDifferencePeers(
      const std::vector<pb::common::Peer>& peers,
      const std::vector<pb::common::Peer>& other_peers);

  static void SortPeers(std::vector<pb::common::Peer>& peers);
  static std::vector<pb::common::Location> ExtractLocations(
      const google::protobuf::RepeatedPtrField<pb::common::Peer>& peers);

  // format: 127.0.0.1:8201:0
  static std::string LocationToString(const pb::common::Location& location);

  static butil::EndPoint LocationToEndPoint(
      const pb::common::Location& location);

  // format: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
  static std::string FormatPeers(
      const std::vector<pb::common::Location>& locations);

  // 127.0.0.1:8201,127.0.0.1:8202,127.0.0.1:8203 to EndPoint
  static std::vector<butil::EndPoint> StrToEndpoint(const std::string& str);

  static std::shared_ptr<pb::error::Error> Error(pb::error::Errno errcode,
                                                 const std::string& errmsg);
  static bool Error(pb::error::Errno errcode, const std::string& errmsg,
                    pb::error::Error& err);
  static bool Error(pb::error::Errno errcode, const std::string& errmsg,
                    std::shared_ptr<pb::error::Error> err);

  static bool IsEqualIgnoreCase(const std::string& str1,
                                const std::string& str2);

  template <typename T>
  static std::vector<T> PbRepeatedToVector(
      const google::protobuf::RepeatedPtrField<T>& data) {
    std::vector<T> vec;
    for (auto& item : data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec,
                                 google::protobuf::RepeatedPtrField<T>* out) {
    for (auto& item : vec) {
      *(out->Add()) = item;
    }
  }
};

}  // namespace dingodb

#endif

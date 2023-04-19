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

#include <sstream>
#include <string>
#include <vector>

#include "braft/configuration.h"
#include "butil/endpoint.h"
#include "butil/status.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class Helper {
  using Errno = pb::error::Errno;
  using PbError = pb::error::Error;

 public:
  static bool IsIp(const std::string& s);

  static butil::EndPoint GetEndPoint(const std::string& host, int port);

  static bool IsDifferenceLocation(const pb::common::Location& location, const pb::common::Location& other_location);
  static bool IsDifferencePeers(const std::vector<pb::common::Peer>& peers,
                                const std::vector<pb::common::Peer>& other_peers);

  static void SortPeers(std::vector<pb::common::Peer>& peers);
  static std::vector<pb::common::Location> ExtractLocations(
      const google::protobuf::RepeatedPtrField<pb::common::Peer>& peers);

  // format: 127.0.0.1:8201:0
  static std::string LocationToString(const pb::common::Location& location);

  // transform braft::PeerId to Location
  // return 0 or -1
  static int PeerIdToLocation(braft::PeerId peer_id, pb::common::Location& location);

  static butil::EndPoint LocationToEndPoint(const pb::common::Location& location);
  static pb::common::Location EndPointToLocation(const butil::EndPoint& endpoint);

  // format: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
  static std::string FormatPeers(const std::vector<pb::common::Location>& locations);

  // 127.0.0.1:8201,127.0.0.1:8202,127.0.0.1:8203 to EndPoint
  static butil::EndPoint StrToEndPoint(std::string str);
  static std::vector<butil::EndPoint> StrToEndpoints(const std::string& str);

  static std::shared_ptr<PbError> Error(Errno errcode, const std::string& errmsg);
  static bool Error(Errno errcode, const std::string& errmsg, PbError& err);
  static bool Error(Errno errcode, const std::string& errmsg, std::shared_ptr<PbError> err);

  static bool IsEqualIgnoreCase(const std::string& str1, const std::string& str2);

  template <typename T>
  static std::vector<T> PbRepeatedToVector(const google::protobuf::RepeatedPtrField<T>& data) {
    std::vector<T> vec;
    for (auto& item : data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(google::protobuf::RepeatedPtrField<T>* data) {
    std::vector<T> vec;
    for (auto& item : *data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec, google::protobuf::RepeatedPtrField<T>* out) {
    for (auto& item : vec) {
      *(out->Add()) = item;
    }
  }

  static std::string Increment(const std::string& input);

  static std::string StringToHex(const std::string& str);

  static void SetPbMessageError(butil::Status status, google::protobuf::Message* message);

  template <typename T>
  static void SetPbMessageErrorLeader(butil::EndPoint endpoint, T* message) {
    auto leader_location = message->mutable_error()->mutable_leader_location();
    leader_location->set_host(std::string(butil::ip2str(endpoint.ip).c_str()));
    leader_location->set_port(endpoint.port);
  }

  static std::string MessageToJsonString(const google::protobuf::Message& message);

  // Use raft endpoint query server endpoint
  static butil::EndPoint QueryServerEndpointByRaftEndpoint(
      std::map<uint64_t, std::shared_ptr<pb::common::Store>> stores, butil::EndPoint endpoint);

  // use raft_location to get server_location
  // in: raft_location
  // out: server_location
  static void GetServerLocation(const pb::common::Location& raft_location, pb::common::Location& server_location);
  static void GetRaftLocation(const pb::common::Location& server_location, pb::common::Location& raft_location);

  // generate random string for keyring
  static std::string GenerateRandomString(int length);
  static uint64_t GenId();

  // Create hard link
  static bool Link(const std::string& old_path, const std::string& new_path);

  static uint64_t Timestamp();

  // end key of all table
  // We are based on this assumption. In general, it is rare to see all 0xFF
  static bool KeyIsEndOfAllTable(const std::string& key);

  static bool GetDiskCapacity(const std::string& path, std::map<std::string, uint64_t>& output);
};

}  // namespace dingodb

#endif

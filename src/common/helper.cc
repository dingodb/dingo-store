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

#include <sys/statvfs.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <regex>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "butil/strings/string_split.h"
#include "butil/strings/stringprintf.h"
#include "common/logging.h"
#include "google/protobuf/util/json_util.h"
#include "proto/node.pb.h"

namespace dingodb {

using Errno = pb::error::Errno;
using PbError = pb::error::Error;

bool Helper::IsIp(const std::string& s) {
  std::regex const reg(
      "(?=(\\b|\\D))(((\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))\\.){3}(("
      "\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))(?=(\\b|\\D))");
  return std::regex_match(s, reg);
}

int Helper::PeerIdToLocation(braft::PeerId peer_id, pb::common::Location& location) {
  // parse leader raft location from string
  auto peer_id_string = peer_id.to_string();

  std::vector<std::string> addrs;
  butil::SplitString(peer_id_string, ':', &addrs);

  if (addrs.size() != 3) {
    DINGO_LOG(ERROR) << "GetLeaderLocation peerid to string error " << peer_id_string;
    return -1;
  }

  pb::common::Location temp_location;
  temp_location.set_host(addrs[0]);

  int32_t value;
  try {
    value = std::stoi(addrs[1]);
    temp_location.set_port(value);
  } catch (const std::invalid_argument& ia) {
    DINGO_LOG(ERROR) << "PeerIdToLocation parse port error Irnvalid argument: " << ia.what() << std::endl;
    return -1;
  } catch (const std::out_of_range& oor) {
    DINGO_LOG(ERROR) << "PeerIdToLocation parse port error Out of Range error: " << oor.what() << std::endl;
    return -1;
  }

  location.CopyFrom(temp_location);
  return 0;
}

butil::EndPoint Helper::GetEndPoint(const std::string& host, int port) {
  butil::ip_t ip;
  if (host.empty()) {
    ip = butil::IP_ANY;
  } else {
    if (Helper::IsIp(host)) {
      butil::str2ip(host.c_str(), &ip);
    } else {
      butil::hostname2ip(host.c_str(), &ip);
    }
  }
  return butil::EndPoint(ip, port);
}

bool Helper::IsDifferenceLocation(const pb::common::Location& location, const pb::common::Location& other_location) {
  return location.host() != other_location.host() || location.port() != other_location.port();
}

void Helper::SortPeers(std::vector<pb::common::Peer>& peers) {
  auto compare_func = [](pb::common::Peer& a, pb::common::Peer& b) -> bool { return a.store_id() < b.store_id(); };
  std::sort(peers.begin(), peers.end(), compare_func);
}

bool Helper::IsDifferencePeers(const std::vector<pb::common::Peer>& peers,
                               const std::vector<pb::common::Peer>& other_peers) {
  if (peers.size() != other_peers.size()) {
    return true;
  }

  for (int i = 0; i < peers.size(); ++i) {
    if (Helper::IsDifferenceLocation(peers[i].raft_location(), other_peers[i].raft_location())) {
      return true;
    }
  }

  return false;
}

std::vector<pb::common::Location> Helper::ExtractLocations(
    const google::protobuf::RepeatedPtrField<pb::common::Peer>& peers) {
  std::vector<pb::common::Location> locations;
  locations.reserve(peers.size());
  for (const auto& peer : peers) {
    locations.push_back(peer.raft_location());
  }
  return locations;
}

std::vector<pb::common::Location> Helper::ExtractLocations(const std::vector<pb::common::Peer>& peers) {
  std::vector<pb::common::Location> locations;
  locations.reserve(peers.size());
  for (const auto& peer : peers) {
    locations.push_back(peer.raft_location());
  }
  return locations;
}

// format: 127.0.0.1:8201:0
std::string Helper::LocationToString(const pb::common::Location& location) {
  return butil::StringPrintf("%s:%d:%ld", location.host().c_str(), location.port(), location.index());
}

butil::EndPoint Helper::LocationToEndPoint(const pb::common::Location& location) {
  butil::EndPoint endpoint;
  if (butil::hostname2endpoint(location.host().c_str(), location.port(), &endpoint) != 0 &&
      str2endpoint(location.host().c_str(), location.port(), &endpoint) != 0) {
  }

  return endpoint;
}

pb::common::Location Helper::EndPointToLocation(const butil::EndPoint& endpoint) {
  pb::common::Location location;
  location.set_host(butil::ip2str(endpoint.ip).c_str());
  location.set_port(endpoint.port);

  return location;
}

// format: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
std::string Helper::FormatPeers(const std::vector<pb::common::Location>& locations) {
  std::string s;
  for (int i = 0; i < locations.size(); ++i) {
    s += LocationToString(locations[i]);
    if (i + 1 < locations.size()) {
      s += ",";
    }
  }
  return s;
}

// 127.0.0.1:8201:0 to endpoint
butil::EndPoint Helper::StrToEndPoint(const std::string str) {
  std::vector<std::string> strs;
  butil::SplitString(str, ':', &strs);

  butil::EndPoint endpoint;
  if (strs.size() >= 2) {
    butil::str2endpoint(strs[0].c_str(), std::stoi(strs[1]), &endpoint);
  }
  return endpoint;
}

std::vector<butil::EndPoint> Helper::StrToEndpoints(const std::string& str) {
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

std::shared_ptr<PbError> Helper::Error(Errno errcode, const std::string& errmsg) {
  std::shared_ptr<PbError> err = std::make_shared<PbError>();
  err->set_errcode(errcode);
  err->set_errmsg(errmsg);
  return err;
}

bool Helper::Error(Errno errcode, const std::string& errmsg, PbError& err) {
  err.set_errcode(errcode);
  err.set_errmsg(errmsg);
  return false;
}

bool Helper::Error(Errno errcode, const std::string& errmsg, std::shared_ptr<PbError> err) {
  err->set_errcode(errcode);
  err->set_errmsg(errmsg);
  return false;
}

bool Helper::IsEqualIgnoreCase(const std::string& str1, const std::string& str2) {
  if (str1.size() != str2.size()) {
    return false;
  }
  return std::equal(str1.begin(), str1.end(), str2.begin(),
                    [](const char c1, const char c2) { return std::tolower(c1) == std::tolower(c2); });
}

std::string Helper::Increment(const std::string& input) {
  std::string ret(input.size(), 0);
  int carry = 1;
  for (int i = input.size() - 1; i >= 0; --i) {
    if (static_cast<int32_t>(input[i]) == 0xFF && carry == 1) {
      ret[i] = 0;
    } else {
      ret[i] = (input[i] + carry);
      carry = 0;
    }
  }

  return (carry == 0) ? ret : input;
}

std::string Helper::StringToHex(const std::string& str) {
  std::string result = "0x";
  std::string tmp;
  std::stringstream ss;
  for (char const i : str) {
    ss << std::hex << int(i) << std::endl;
    ss >> tmp;
    result += tmp;
  }
  return result;
}

void Helper::SetPbMessageError(butil::Status status, google::protobuf::Message* message) {
  const google::protobuf::Reflection* reflection = message->GetReflection();
  const google::protobuf::Descriptor* desc = message->GetDescriptor();

  const google::protobuf::FieldDescriptor* error_field = desc->FindFieldByName("error");
  google::protobuf::Message* error = reflection->MutableMessage(message, error_field);
  const google::protobuf::Reflection* error_ref = error->GetReflection();
  const google::protobuf::Descriptor* error_desc = error->GetDescriptor();
  const google::protobuf::FieldDescriptor* errcode_field = error_desc->FindFieldByName("errcode");
  error_ref->SetEnumValue(error, errcode_field, status.error_code());
  const google::protobuf::FieldDescriptor* errmsg_field = error_desc->FindFieldByName("errmsg");
  error_ref->SetString(error, errmsg_field, status.error_str());
}

std::string Helper::MessageToJsonString(const google::protobuf::Message& message) {
  std::string json_string;
  google::protobuf::util::JsonOptions options;
  options.always_print_primitive_fields = true;
  google::protobuf::util::Status status = google::protobuf::util::MessageToJsonString(message, &json_string, options);
  if (!status.ok()) {
    std::cerr << "Failed to convert message to JSON: [" << status.message() << "]" << std::endl;
  }
  return json_string;
}

butil::EndPoint Helper::QueryServerEndpointByRaftEndpoint(std::map<uint64_t, std::shared_ptr<pb::common::Store>> stores,
                                                          butil::EndPoint endpoint) {
  butil::EndPoint result;
  std::string host(butil::ip2str(endpoint.ip).c_str());
  for (const auto& it : stores) {
    if (it.second->raft_location().host() == host && it.second->raft_location().port() == endpoint.port) {
      str2endpoint(it.second->server_location().host().c_str(), it.second->server_location().port(), &result);
    }
  }

  return result;
}

void Helper::GetServerLocation(const pb::common::Location& raft_location, pb::common::Location& server_location) {
  // validate raft_location
  // TODO: how to support ipv6
  if (raft_location.host().length() <= 0 || raft_location.port() <= 0) {
    DINGO_LOG(ERROR) << "GetServiceLocation illegal raft_location=" << raft_location.host() << ":"
                     << raft_location.port();
    return;
  }

  // find in cache
  auto raft_location_string = raft_location.host() + ":" + std::to_string(raft_location.port());

  // cache miss, use rpc to get remote_node server location, and store in cache
  braft::PeerId remote_node(raft_location_string);

  // rpc
  brpc::Channel channel;
  if (channel.Init(remote_node.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << remote_node;
    return;
  }

  pb::node::NodeService_Stub stub(&channel);

  brpc::Controller cntl;
  cntl.set_timeout_ms(1000L);

  pb::node::GetNodeInfoRequest request;
  pb::node::GetNodeInfoResponse response;

  std::string key = "Hello";
  request.set_cluster_id(0);
  stub.GetNodeInfo(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    return;
  }

  DINGO_LOG(INFO) << "Received response"
                  << " cluster_id=" << request.cluster_id() << " latency=" << cntl.latency_us()
                  << " server_location=" << response.node_info().server_location().host() << ":"
                  << response.node_info().server_location().port();

  server_location.CopyFrom(response.node_info().server_location());
}

void Helper::GetRaftLocation(const pb::common::Location& server_location, pb::common::Location& raft_location) {
  // validate server_location
  // TODO: how to support ipv6
  if (server_location.host().length() <= 0 || server_location.port() <= 0) {
    DINGO_LOG(ERROR) << "GetServiceLocation illegal server_location=" << server_location.host() << ":"
                     << server_location.port();
    return;
  }

  // find in cache
  auto server_location_string = server_location.host() + ":" + std::to_string(server_location.port());

  // cache miss, use rpc to get remote_node server location, and store in cache
  braft::PeerId remote_node(server_location_string);

  // rpc
  brpc::Channel channel;
  if (channel.Init(remote_node.addr, nullptr) != 0) {
    DINGO_LOG(ERROR) << "Fail to init channel to " << remote_node;
    return;
  }

  pb::node::NodeService_Stub stub(&channel);

  brpc::Controller cntl;
  cntl.set_timeout_ms(1000L);

  pb::node::GetNodeInfoRequest request;
  pb::node::GetNodeInfoResponse response;

  std::string key = "Hello";
  request.set_cluster_id(0);
  stub.GetNodeInfo(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    DINGO_LOG(WARNING) << "Fail to send request to : " << cntl.ErrorText();
    return;
  }

  DINGO_LOG(INFO) << "Received response"
                  << " cluster_id=" << request.cluster_id() << " latency=" << cntl.latency_us()
                  << " raft_location=" << response.node_info().raft_location().host() << ":"
                  << response.node_info().raft_location().port();

  raft_location.CopyFrom(response.node_info().raft_location());
}

std::string Helper::GenerateRandomString(int length) {
  std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::string rand_string;

  unsigned int seed = time(nullptr);  // Get seed value for rand_r()

  for (int i = 0; i < length; i++) {
    int rand_index = rand_r(&seed) % chars.size();
    rand_string += chars[rand_index];
  }

  return rand_string;
}

uint64_t Helper::GenId() {
  static uint64_t id = 0;
  return ++id;
}

bool Helper::Link(const std::string& old_path, const std::string& new_path) {
  return ::link(old_path.c_str(), new_path.c_str()) == 0;
}

uint64_t Helper::TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

uint64_t Helper::Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string Helper::FormatMsTime(uint64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str()) << "." << timestamp % 1000;
  return ss.str();
}

std::string Helper::FormatTime(uint64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp((std::chrono::seconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str());
  return ss.str();
}

std::string Helper::GetNowFormatMsTime() {
  uint64_t timestamp = TimestampMs();
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%dT%H:%M:%S.000Z");
  return ss.str();
}

// end key of all table
bool Helper::KeyIsEndOfAllTable(const std::string& key) {
  for (const auto& elem : key) {
    if (static_cast<char>(0xFF) != elem) {
      return false;
    }
  }
  return true;
}

bool Helper::GetDiskCapacity(const std::string& path, std::map<std::string, uint64_t>& output) {
  struct statvfs stat;
  if (statvfs(path.c_str(), &stat) != 0) {
    std::cerr << "Failed to get file system statistics\n";
    return false;
  }

  uint64_t total_space = stat.f_frsize * stat.f_blocks;
  uint64_t free_space = stat.f_frsize * stat.f_bfree;

  output["TotalCapacity"] = total_space;
  output["FreeCcapacity"] = free_space;
  return true;
}

}  // namespace dingodb

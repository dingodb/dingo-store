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

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <ratio>
#include <regex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "butil/endpoint.h"
#include "butil/status.h"
#include "butil/strings/string_split.h"
#include "common/logging.h"
#include "common/service_access.h"
#include "fmt/core.h"
#include "google/protobuf/util/json_util.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "serial/buf.h"

namespace dingodb {

using Errno = pb::error::Errno;
using PbError = pb::error::Error;

int Helper::GetCoreNum() { return sysconf(_SC_NPROCESSORS_ONLN); }

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
    DINGO_LOG(ERROR) << "PeerIdToLocation parse port error Irnvalid argument: " << ia.what() << '\n';
    return -1;
  } catch (const std::out_of_range& oor) {
    DINGO_LOG(ERROR) << "PeerIdToLocation parse port error Out of Range error: " << oor.what() << '\n';
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

butil::EndPoint Helper::GetEndPoint(const std::string& addr) {
  butil::EndPoint endpoint;
  str2endpoint(addr.c_str(), &endpoint);
  return endpoint;
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
  return fmt::format("{}:{}:{}", location.host(), location.port(), location.index());
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

braft::PeerId Helper::LocationToPeer(const pb::common::Location& location) {
  return braft::PeerId(LocationToEndPoint(location), location.index());
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

// format: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
std::string Helper::FormatPeers(const braft::Configuration& conf) {
  std::vector<braft::PeerId> peers;
  conf.list_peers(&peers);

  std::string s;
  for (int i = 0; i < peers.size(); ++i) {
    s += peers[i].to_string();
    if (i + 1 < peers.size()) {
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

// Next prefix
std::string Helper::PrefixNext(const std::string& input) {
  std::string ret(input.size(), 0);
  int carry = 1;
  for (int i = input.size() - 1; i >= 0; --i) {
    if (static_cast<uint8_t>(input[i]) == (uint8_t)0xFF && carry == 1) {
      ret[i] = 0;
    } else {
      ret[i] = (input[i] + carry);
      carry = 0;
    }
  }

  return (carry == 0) ? ret : input;
}

std::string Helper::PrefixNext(const std::string_view& input) {
  std::string ret(input.size(), 0);
  int carry = 1;
  for (int i = input.size() - 1; i >= 0; --i) {
    if (static_cast<uint8_t>(input[i]) == (uint8_t)0xFF && carry == 1) {
      ret[i] = 0;
    } else {
      ret[i] = (input[i] + carry);
      carry = 0;
    }
  }

  return (carry == 0) ? ret : std::string(input.data(), input.size());
}

// Transform RangeWithOptions to Range for scan/deleteRange
pb::common::Range Helper::TransformRangeWithOptions(const pb::common::RangeWithOptions& range_with_options) {
  pb::common::Range range;

  range.set_start_key(range_with_options.with_start() ? range_with_options.range().start_key()
                                                      : PrefixNext(range_with_options.range().start_key()));

  range.set_end_key(range_with_options.with_end() ? PrefixNext(range_with_options.range().end_key())
                                                  : range_with_options.range().end_key());

  return range;
}

// Take range intersection
pb::common::Range Helper::IntersectRange(const pb::common::Range& range1, const pb::common::Range& range2) {
  pb::common::Range range;

  range.set_start_key((range1.start_key() <= range2.start_key()) ? range2.start_key() : range1.start_key());
  range.set_end_key((range1.end_key() <= range2.end_key()) ? range1.end_key() : range2.end_key());

  return range;
}

std::string Helper::StringToHex(const std::string& str) {
  std::stringstream ss;
  for (const auto& ch : str) {
    ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(static_cast<unsigned char>(ch));
  }
  return ss.str();
}

std::string Helper::StringToHex(const std::string_view& str) {
  std::stringstream ss;
  for (const auto& ch : str) {
    ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(static_cast<unsigned char>(ch));
  }
  return ss.str();
}

std::string Helper::HexToString(const std::string& hex_str) {
  std::string result;
  // The hex_string must be of even length
  for (size_t i = 0; i < hex_str.length(); i += 2) {
    std::string hex_byte = hex_str.substr(i, 2);
    // Convert the hex byte to an integer
    int byte_value = std::stoi(hex_byte, nullptr, 16);
    // Cast the integer to a char and append it to the result string
    result += static_cast<char>(byte_value);
  }
  return result;
}

void Helper::AlignByteArrays(std::string& a, std::string& b) {
  if (a.size() < b.size()) {
    a.resize(b.size(), 0);
  } else if (a.size() > b.size()) {
    b.resize(a.size(), 0);
  }
}

void Helper::RightAlignByteArrays(std::string& a, std::string& b) {
  if (a.size() < b.size()) {
    std::string tmp(b.size(), 0);

    for (int i = 0; i < a.size(); ++i) {
      tmp[i + b.size() - a.size()] = a[i];
    }

    std::swap(a, tmp);

  } else if (a.size() > b.size()) {
    std::string tmp(a.size(), 0);

    for (int i = 0; i < b.size(); ++i) {
      tmp[i + a.size() - b.size()] = b[i];
    }

    std::swap(b, tmp);
  }
}

// Notice: String will add one element as a prefix of the result, this element is for the carry
// if you want the equal length of your input, you need to do substr by yourself
std::string Helper::StringAdd(const std::string& input_a, const std::string& input_b) {
  std::string a = input_a;
  std::string b = input_b;
  AlignByteArrays(a, b);

  size_t max_length = std::max(a.size(), b.size());
  std::string result(max_length + 1, 0);

  for (size_t i = 0; i < max_length; ++i) {
    uint8_t a_value = (i < a.size()) ? a[a.size() - 1 - i] : 0;
    uint8_t b_value = (i < b.size()) ? b[b.size() - 1 - i] : 0;
    uint16_t sum = static_cast<uint16_t>(a_value) + static_cast<uint16_t>(b_value) + result[max_length - i];
    result[max_length - i] = sum & 0xFF;
    result[max_length - i - 1] = (sum >> 8) & 0xFF;
  }

  return result;
}

// Notice: String will add one element as a prefix of the result, this element is for the carry
// if you want the equal length of your input, you need to do substr by yourself
std::string Helper::StringAddRightAlign(const std::string& input_a, const std::string& input_b) {
  std::string a = input_a;
  std::string b = input_b;
  RightAlignByteArrays(a, b);

  return StringAdd(a, b);
}

std::string Helper::StringSubtract(const std::string& input_a, const std::string& input_b) {
  std::string a = input_a;
  std::string b = input_b;
  AlignByteArrays(a, b);

  size_t max_length = std::max(a.size(), b.size());

  std::string result(max_length, 0);

  int8_t borrow = 0;
  for (size_t i = 0; i < max_length; ++i) {
    uint8_t a_value = (i < a.size()) ? a[a.size() - 1 - i] : 0;
    uint8_t b_value = (i < b.size()) ? b[b.size() - 1 - i] : 0;
    int16_t diff = static_cast<int16_t>(b_value) - static_cast<int16_t>(a_value) - borrow;
    if (diff < 0) {
      diff += 256;
      borrow = 1;
    } else {
      borrow = 0;
    }
    result[max_length - 1 - i] = diff;
  }

  return result;
}

std::string Helper::StringSubtractRightAlign(const std::string& input_a, const std::string& input_b) {
  std::string a = input_a;
  std::string b = input_b;
  RightAlignByteArrays(a, b);

  return StringSubtract(a, b);
}

std::string Helper::StringDivideByTwo(const std::string& array) {
  if (array.at(array.size() - 1) % 2 == 0) {
    return StringDivideByTwoRightAlign(array);
  } else {
    std::string new_array = array;
    new_array.push_back(0);
    return StringDivideByTwoRightAlign(new_array);
  }
}

std::string Helper::StringDivideByTwoRightAlign(const std::string& array) {
  std::string result(array.size(), 0);
  uint16_t carry = 0;

  for (size_t i = 0; i < array.size(); ++i) {
    uint16_t value = (carry << 8) | static_cast<uint8_t>(array[i]);
    result[i] = value / 2;
    carry = value % 2;
  }

  return result;
}

std::string Helper::CalculateMiddleKey(const std::string& start_key, const std::string& end_key) {
  DINGO_LOG(INFO) << " start_key = " << dingodb::Helper::StringToHex(start_key);
  DINGO_LOG(INFO) << " end_key   = " << dingodb::Helper::StringToHex(end_key);

  auto diff = dingodb::Helper::StringSubtract(start_key, end_key);
  auto half_diff = dingodb::Helper::StringDivideByTwo(diff);
  auto mid = dingodb::Helper::StringAdd(start_key, half_diff);

  DINGO_LOG(INFO) << " diff      = " << dingodb::Helper::StringToHex(diff);
  DINGO_LOG(INFO) << " half_diff = " << dingodb::Helper::StringToHex(half_diff);
  DINGO_LOG(INFO) << " mid       = " << dingodb::Helper::StringToHex(mid);

  auto real_mid = mid.substr(1, mid.size() - 1);
  DINGO_LOG(INFO) << " mid real  = " << dingodb::Helper::StringToHex(real_mid);

  return real_mid;
}

std::vector<uint8_t> Helper::SubtractByteArrays(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) {
  size_t max_length = std::max(a.size(), b.size());

  std::vector<uint8_t> result(max_length, 0);

  int8_t borrow = 0;
  for (size_t i = 0; i < max_length; ++i) {
    uint8_t a_value = (i < a.size()) ? a[a.size() - 1 - i] : 0;
    uint8_t b_value = (i < b.size()) ? b[b.size() - 1 - i] : 0;
    int16_t diff = static_cast<int16_t>(b_value) - static_cast<int16_t>(a_value) - borrow;
    if (diff < 0) {
      diff += 256;
      borrow = 1;
    } else {
      borrow = 0;
    }
    result[max_length - 1 - i] = diff;
  }

  return result;
}

std::vector<uint8_t> Helper::DivideByteArrayByTwo(const std::vector<uint8_t>& array) {
  std::vector<uint8_t> result(array.size(), 0);
  uint16_t carry = 0;

  for (size_t i = 0; i < array.size(); ++i) {
    uint16_t value = (carry << 8) | array[i];
    result[i] = value / 2;
    carry = value % 2;
  }

  return result;
}

// Notice: AddByteArrays will add one element as a prefix of the result, this element is for the carry
// if you want the equal length of your input, you need to do substr by yourself
std::vector<uint8_t> Helper::AddByteArrays(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) {
  size_t max_length = std::max(a.size(), b.size());
  std::vector<uint8_t> result(max_length + 1, 0);

  for (size_t i = 0; i < max_length; ++i) {
    uint8_t a_value = (i < a.size()) ? a[a.size() - 1 - i] : 0;
    uint8_t b_value = (i < b.size()) ? b[b.size() - 1 - i] : 0;
    uint16_t sum = static_cast<uint16_t>(a_value) + static_cast<uint16_t>(b_value) + result[max_length - i];
    result[max_length - i] = sum & 0xFF;
    result[max_length - i - 1] = (sum >> 8) & 0xFF;
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
    std::cerr << "Failed to convert message to JSON: [" << status.message() << "]" << '\n';
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

  auto node_info = ServiceAccess::GetNodeInfo(raft_location.host(), raft_location.port());

  server_location.CopyFrom(node_info.server_location());
}

void Helper::GetRaftLocation(const pb::common::Location& server_location, pb::common::Location& raft_location) {
  // validate server_location
  // TODO: how to support ipv6
  if (server_location.host().length() <= 0 || server_location.port() <= 0) {
    DINGO_LOG(ERROR) << "GetServiceLocation illegal server_location=" << server_location.host() << ":"
                     << server_location.port();
    return;
  }

  auto node_info = ServiceAccess::GetNodeInfo(server_location.host(), server_location.port());

  raft_location.CopyFrom(node_info.raft_location());
}

pb::common::Peer Helper::GetPeerInfo(const butil::EndPoint& endpoint) {
  pb::common::Peer peer;

  auto node_info = ServiceAccess::GetNodeInfo(endpoint);
  peer.set_store_id(node_info.id());
  peer.set_role(pb::common::PeerRole::VOTER);
  peer.mutable_raft_location()->Swap(node_info.mutable_raft_location());
  peer.mutable_server_location()->Swap(node_info.mutable_server_location());

  return peer;
}

uint64_t Helper::GenerateRandomInteger(uint64_t min_value, uint64_t max_value) {
  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(min_value, max_value);

  return distrib(rng);
}

float Helper::GenerateRandomFloat(float min_value, float max_value) {
  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(min_value, max_value);

  return distrib(rng);
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

uint64_t Helper::TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
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

// Not recursion
std::vector<std::string> Helper::TraverseDirectory(const std::string& path) {
  std::vector<std::string> filenames;
  for (const auto& fe : std::filesystem::directory_iterator(path)) {
    filenames.push_back(fe.path().filename().string());
  }

  return filenames;
}

std::string Helper::FindFileInDirectory(const std::string& dirpath, const std::string& prefix) {
  for (const auto& fe : std::filesystem::directory_iterator(dirpath)) {
    auto filename = fe.path().filename().string();
    if (filename.find(prefix) != std::string::npos) {
      return filename;
    }
  }

  return "";
}

bool Helper::RemoveFileOrDirectory(const std::string& path) {
  std::error_code ec;
  if (!std::filesystem::remove(path, ec)) {
    DINGO_LOG(ERROR) << fmt::format("remove directory failed, error: {} {}", ec.value(), ec.message());
    return false;
  }

  return true;
}

bool Helper::RemoveAllFileOrDirectory(const std::string& path) {
  std::error_code ec;
  auto num = std::filesystem::remove_all(path, ec);
  if (num == static_cast<std::uintmax_t>(-1)) {
    DINGO_LOG(ERROR) << fmt::format("remove all directory failed, error: {} {}", ec.value(), ec.message());
    return false;
  }

  return true;
}

butil::Status Helper::Rename(const std::string& src_path, const std::string& dst_path, bool is_force) {
  std::filesystem::path source_path = src_path;
  std::filesystem::path destination_path = dst_path;

  // Check if the destination path already exists
  if (std::filesystem::exists(destination_path)) {
    if (!is_force) {
      // If is_force is false, return error
      DINGO_LOG(ERROR) << fmt::format("Destination {} already exists, is_force = false, so cannot rename from {}",
                                      dst_path, src_path);
      return butil::Status(pb::error::Errno::EINTERNAL, "Destination already exists");
    }

    // Remove the existing destination
    RemoveAllFileOrDirectory(dst_path);

    // Check if the removal was successful
    if (std::filesystem::exists(destination_path)) {
      DINGO_LOG(ERROR) << fmt::format("Failed to remove the existing destination {} ", dst_path);
      return butil::Status(pb::error::Errno::EINTERNAL, "Failed to remove the existing destination");
    }
  }

  // Perform the rename operation
  try {
    std::filesystem::rename(source_path, destination_path);
    DINGO_LOG(DEBUG) << fmt::format("Rename {} to {}", src_path, dst_path);
  } catch (const std::exception& ex) {
    DINGO_LOG(ERROR) << fmt::format("Rename operation failed, src_path: {}, dst_path: {}, error: {}", src_path,
                                    dst_path, ex.what());
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("Rename operation failed, error: {}", ex.what()));
  }

  return butil::Status::OK();
}

bool Helper::IsEqualVectorScalarValue(const pb::common::ScalarValue& value1, const pb::common::ScalarValue& value2) {
  if (value1.field_type() != value2.field_type()) {
    return false;
  }

#define CASE(scalar_field_type, type_data)                                      \
  case scalar_field_type: {                                                     \
    if (value1.fields_size() == value2.fields_size()) {                         \
      for (int i = 0; i < value1.fields_size(); i++) {                          \
        if (value1.fields()[i].type_data() != value2.fields()[i].type_data()) { \
          return false;                                                         \
        }                                                                       \
      }                                                                         \
    } else {                                                                    \
      return false;                                                             \
    }                                                                           \
    return true;                                                                \
  }

  switch (value1.field_type()) {
    CASE(pb::common::ScalarFieldType::BOOL, bool_data)
    CASE(pb::common::ScalarFieldType::INT8, int_data)
    CASE(pb::common::ScalarFieldType::INT16, int_data)
    CASE(pb::common::ScalarFieldType::INT32, int_data)
    CASE(pb::common::ScalarFieldType::INT64, long_data)
    CASE(pb::common::ScalarFieldType::FLOAT32, float_data)
    CASE(pb::common::ScalarFieldType::DOUBLE, double_data)
    CASE(pb::common::ScalarFieldType::STRING, string_data)
    CASE(pb::common::ScalarFieldType::BYTES, bytes_data)

    default: {
      DINGO_LOG(WARNING) << fmt::format("Invalid scalar field type: {}.", static_cast<int>(value1.field_type()));
      break;
    }
  }

#undef CASE

  return false;
}

std::string Helper::EncodeIndexRegionHeader(uint64_t region_id) {
  Buf buf(17);
  buf.WriteLong(region_id);

  return buf.GetString();
}

std::string Helper::ToUpper(const std::string& str) {
  std::string result;
  result.resize(str.size());
  std::transform(str.begin(), str.end(), result.begin(), ::toupper);
  return result;
}

std::string Helper::ToLower(const std::string& str) {
  std::string result;
  result.resize(str.size());
  std::transform(str.begin(), str.end(), result.begin(), ::tolower);
  return result;
}

}  // namespace dingodb

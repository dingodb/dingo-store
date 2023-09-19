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

#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <ratio>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "butil/endpoint.h"
#include "butil/status.h"
#include "butil/strings/string_split.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/service_access.h"
#include "fmt/core.h"
#include "google/protobuf/util/json_util.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "serial/buf.h"

namespace dingodb {

const std::map<std::string, uint32_t> kTxnCf2Id = {{Constant::kTxnDataCF, Constant::kTxnDataCfId},
                                                   {Constant::kTxnLockCF, Constant::kTxnLockCfId},
                                                   {Constant::kTxnWriteCF, Constant::kTxnWriteCfId}};

using Errno = pb::error::Errno;
using PbError = pb::error::Error;

int Helper::GetCoreNum() { return sysconf(_SC_NPROCESSORS_ONLN); }

bool Helper::IsIp(const std::string& s) {
  std::regex const reg(
      "(?=(\\b|\\D))(((\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))\\.){3}(("
      "\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))(?=(\\b|\\D))");
  return std::regex_match(s, reg);
}

std::string Helper::Ip2HostName(const std::string& ip) {
  std::string hostname;
  butil::ip_t ip_t;
  int ret = butil::str2ip(ip.c_str(), &ip_t);
  if (ret != 0) {
    ret = butil::hostname2ip(ip.c_str(), &ip_t);
  }
  if (ret != 0) {
    DINGO_LOG(ERROR) << "Ip2HostName failed, ip=" << ip;
    return hostname;
  }

  butil::ip2hostname(ip_t, &hostname);
  return hostname;
}

int Helper::PeerIdToLocation(braft::PeerId peer_id, pb::common::Location& location) {
  // parse leader raft location from string
  auto peer_id_string = peer_id.to_string();

  std::vector<std::string> addrs;
  butil::SplitString(peer_id_string, ':', &addrs);

  if (addrs.size() < 3) {
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

  location = temp_location;
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

std::string Helper::PeersToString(const std::vector<pb::common::Peer>& peers) {
  std::string result;

  for (int i = 0; i < peers.size(); ++i) {
    result += LocationToString(peers[i].raft_location());
    if (i + 1 < peers.size()) {
      result += ",";
    }
  }

  return result;
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

std::string Helper::EndPointToStr(const butil::EndPoint& end_point) {
  return std::string(butil::endpoint2str(end_point).c_str());
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
    result += static_cast<unsigned char>(byte_value);
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

void Helper::GetNodeInfoByRaftLocation(const pb::common::Location& raft_location, pb::node::NodeInfo& node_info) {
  // validate raft_location
  // TODO: how to support ipv6
  if (raft_location.host().length() <= 0 || raft_location.port() <= 0) {
    DINGO_LOG(ERROR) << "GetNodeInfoByRaftLocation illegal raft_location=" << raft_location.host() << ":"
                     << raft_location.port();
    return;
  }

  node_info = ServiceAccess::GetNodeInfo(raft_location.host(), raft_location.port());
}

void Helper::GetNodeInfoByServerLocation(const pb::common::Location& server_location, pb::node::NodeInfo& node_info) {
  // validate server_location
  // TODO: how to support ipv6
  if (server_location.host().length() <= 0 || server_location.port() <= 0) {
    DINGO_LOG(ERROR) << "GetNodeInfoByServerLocation illegal server_location=" << server_location.host() << ":"
                     << server_location.port();
    return;
  }

  node_info = ServiceAccess::GetNodeInfo(server_location.host(), server_location.port());
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

  server_location = node_info.server_location();
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

  raft_location = node_info.raft_location();
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

int64_t Helper::GenerateRealRandomInteger(int64_t min_value, int64_t max_value) {
  // Create a random number generator engine
  std::random_device rd;      // Obtain a random seed from the hardware
  std::mt19937_64 gen(rd());  // Standard 64-bit mersenne_twister_engine seeded with rd()

  // Create a distribution for the desired range
  std::uniform_int_distribution<int64_t> dis(min_value, max_value);

  // Generate and print a random int64 number
  int64_t random_number = dis(gen);

  return random_number;
}

int64_t Helper::GenerateRandomInteger(int64_t min_value, int64_t max_value) {
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

int64_t Helper::GenId() {
  static int64_t id = 0;
  return ++id;
}

std::string Helper::GenNewTableCheckName(int64_t schema_id, const std::string& table_name) {
  Buf buf(8);
  buf.WriteLong(schema_id);
  return buf.GetString() + table_name;
}

bool Helper::Link(const std::string& old_path, const std::string& new_path) {
  int ret = ::link(old_path.c_str(), new_path.c_str());
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("Create hard link failed, old_path: {} new_path: {} errno: {}", old_path, new_path,
                                    errno);
  }

  return ret == 0;
}

std::vector<std::string> Helper::GenMvccCfVector() {
  std::vector<std::string> cfs;

  // the order of the cf is important, and can not be changed
  cfs.push_back(Constant::kTxnDataCF);
  cfs.push_back(Constant::kTxnLockCF);
  cfs.push_back(Constant::kTxnWriteCF);

  return cfs;
}

int64_t Helper::TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string Helper::NowTime() { return FormatMsTime(TimestampMs(), "%Y-%m-%d %H:%M:%S"); }

std::string Helper::FormatMsTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str()) << "." << timestamp % 1000;
  return ss.str();
}

std::string Helper::FormatTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp((std::chrono::seconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str());
  return ss.str();
}

std::string Helper::GetNowFormatMsTime() {
  int64_t timestamp = TimestampMs();
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

bool Helper::GetSystemDiskCapacity(const std::string& path, std::map<std::string, int64_t>& output) {
  // system capacity
  struct statvfs stat;
  if (statvfs(path.c_str(), &stat) != 0) {
    std::cerr << "Failed to get file system statistics\n";
    return false;
  }

  int64_t total_space = stat.f_frsize * stat.f_blocks;
  int64_t free_space = stat.f_frsize * stat.f_bfree;

  output["system_total_capacity"] = total_space;
  output["system_free_capacity"] = free_space;

  DINGO_LOG(INFO) << fmt::format("Disk total space: {} bytes, free space: {} bytes", total_space, free_space);

  return true;
}

bool Helper::GetSystemMemoryInfo(std::map<std::string, int64_t>& output) {
  // system memory info
  struct sysinfo mem_info;
  if (sysinfo(&mem_info) != -1) {
    DINGO_LOG(INFO) << "Total RAM: " << mem_info.totalram * mem_info.mem_unit << " bytes, "
                    << "Available RAM: " << mem_info.freeram * mem_info.mem_unit << " bytes, "
                    << "Total Swap: " << mem_info.totalswap * mem_info.mem_unit << " bytes, "
                    << "Available Swap: " << mem_info.freeswap * mem_info.mem_unit << " bytes";

    output["system_total_memory"] = mem_info.totalram * mem_info.mem_unit;
    output["system_free_memory"] = mem_info.freeram * mem_info.mem_unit;
    output["system_shared_memory"] = mem_info.sharedram * mem_info.mem_unit;
    output["system_buffer_memory"] = mem_info.bufferram * mem_info.mem_unit;
    output["system_total_swap"] = mem_info.totalswap * mem_info.mem_unit;
    output["system_free_swap"] = mem_info.freeswap * mem_info.mem_unit;
  } else {
    DINGO_LOG(ERROR) << "Failed to retrieve memory information using sysinfo.";
    return false;
  }

  // get cache/available memory
  try {
    output["system_available_memory"] = INT64_MAX;
    output["system_cached_memory"] = INT64_MAX;
    std::ifstream file("/proc/meminfo");
    if (!file.is_open()) {
      DINGO_LOG(ERROR) << "Open file /proc/meminfo failed.";
      return false;
    }

    auto parse_value = [](std::string& str) -> int64_t {
      std::vector<std::string> strs;
      butil::SplitString(str, ' ', &strs);
      for (auto s : strs) {
        if (!s.empty() && std::isdigit(s[0])) {
          return std::strtol(s.c_str(), nullptr, 10);
        }
      }
      return 0;
    };

    int get_count = 2;
    while (!file.eof()) {
      std::string line;
      std::getline(file, line);

      if (line.find("MemAvailable") != line.npos) {
        --get_count;
        output["system_available_memory"] = parse_value(line) * 1024;
      } else if (line.find("Cached") != line.npos) {
        --get_count;
        output["system_cached_memory"] = parse_value(line) * 1024;
      }
      if (get_count <= 0) {
        break;
      }
    }
    file.close();
    if (get_count > 0) {
      return false;
    }
  } catch (const std::exception& e) {
    std::string s = fmt::format("Read /proc/meminfo failed, error: {}", e.what());
    std::cerr << s << '\n';
    DINGO_LOG(ERROR) << s;
    return false;
  }

  DINGO_LOG(INFO) << "Available RAM (proc/meminfo): " << output["system_available_memory"] << " bytes, "
                  << "Cached RAM (proc/meminfo): " << output["system_cached_memory"] << " bytes";

  return true;
}

bool Helper::GetProcessMemoryInfo(std::map<std::string, int64_t>& output) {
  struct rusage usage;
  if (getrusage(RUSAGE_SELF, &usage) != -1) {
    output["process_used_memory"] = usage.ru_maxrss;
    DINGO_LOG(INFO) << "Process Memory usage: " << usage.ru_maxrss << " kilobytes, "
                    << "Shared memory size: " << usage.ru_ixrss << " kilobytes";
    return true;
  } else {
    DINGO_LOG(INFO) << "Failed to retrieve memory usage using getrusage.";
    return false;
  }
}

bool Helper::GetSystemCpuUsage(std::map<std::string, int64_t>& output) {
  try {
    std::ifstream file("/proc/stat");
    if (!file.is_open()) {
      DINGO_LOG(WARNING) << "Failed to open /proc/stat";
      return false;
    }

    std::string line;
    std::getline(file, line);
    file.close();

    std::istringstream iss(line);
    std::vector<std::string> tokens(std::istream_iterator<std::string>{iss}, std::istream_iterator<std::string>());

    // Calculate total CPU time
    unsigned long long total_cpu_time = 0;
    for (size_t i = 1; i < tokens.size(); i++) {
      total_cpu_time += std::stoll(tokens[i]);
    }

    // Calculate idle CPU time
    unsigned long long idle_cpu_time = std::stoll(tokens[4]);

    // Calculate CPU usage percentage
    double cpu_usage = 100.0 * (total_cpu_time - idle_cpu_time) / total_cpu_time;

    DINGO_LOG(INFO) << fmt::format("CPU usage: {}%", cpu_usage);

    output["system_cpu_usage"] = static_cast<int64_t>(cpu_usage * 100);

    return true;
  } catch (const std::exception& e) {
    std::cerr << "Failed to get system cpu usage: " << e.what() << '\n';
    DINGO_LOG(INFO) << "Failed to get system cpu usage: " << e.what();
    return false;
  }
}

struct DiskStats {
  unsigned long long int major_num;
  unsigned long long int minor_num;
  std::string device;
  unsigned long long int reads_completed;
  unsigned long long int reads_merged;
  unsigned long long int sectors_read;
  unsigned long long int read_time;
  unsigned long long int writes_completed;
  unsigned long long int writes_merged;
  unsigned long long int sectors_written;
  unsigned long long int write_time;
  unsigned long long int io_in_progress;
  unsigned long long int io_time;
  unsigned long long int weighted_io_time;
  unsigned long long int discards_completed;
  unsigned long long int discards_merged;
  unsigned long long int sectors_discarded;
  unsigned long long int discard_time;
  unsigned long long int flush_completed;
  unsigned long long int flush_time;
  // unsigned long long int io_time_currently_being_weighted;
};

std::vector<DiskStats> GetDiskStats() {
  std::ifstream file("/proc/diskstats");
  std::vector<DiskStats> disk_stats;
  if (file) {
    std::string line;
    while (std::getline(file, line)) {
      std::istringstream iss(line);
      DiskStats stats;
      iss >> stats.major_num >> stats.minor_num >> stats.device >> stats.reads_completed >> stats.reads_merged >>
          stats.sectors_read >> stats.read_time >> stats.writes_completed >> stats.writes_merged >>
          stats.sectors_written >> stats.write_time >> stats.io_in_progress >> stats.io_time >>
          stats.weighted_io_time >> stats.discards_completed >> stats.discards_merged >> stats.sectors_discarded >>
          stats.discard_time >> stats.flush_completed >> stats.flush_time;
      disk_stats.push_back(stats);

      DINGO_LOG(INFO) << "Disk stats.device: " << stats.device << " stats.read_completed: " << stats.reads_completed
                      << " stats.reads_merged: " << stats.reads_merged << " stats.sectors_read: " << stats.sectors_read
                      << " stats.read_time: " << stats.read_time
                      << " stats.writes_completed: " << stats.writes_completed
                      << " stats.writes_merged: " << stats.writes_merged
                      << " stats.sectors_written: " << stats.sectors_written
                      << " stats.write_time: " << stats.write_time << " stats.io_in_process: " << stats.io_in_progress
                      << " stats.io_time: " << stats.io_time << " stats.weighted_io_time: " << stats.weighted_io_time
                      << " stats.discards_completed: " << stats.discards_completed
                      << " stats.discards_merged: " << stats.discards_merged
                      << " stats.sectors_discarded: " << stats.sectors_discarded
                      << " stats.discard_time: " << stats.discard_time
                      << " stats.flush_completed: " << stats.flush_completed
                      << " stats.flush_time: " << stats.flush_time;
    }
    file.close();
  } else {
    std::cerr << "Failed to open /proc/diskstats." << '\n';
  }
  return disk_stats;
}

double GetSystemIoUtilization(std::string device) {
  std::vector<DiskStats> prev_stats = GetDiskStats();
  usleep(1000000);  // Sleep for 1 second
  std::vector<DiskStats> cur_stats = GetDiskStats();

  unsigned long long int prev_io_time = 0;
  unsigned long long int cur_io_time = 0;
  unsigned long long int prev_weighted_io_time = 0;
  unsigned long long int cur_weighted_io_time = 0;

  for (const DiskStats& stats : prev_stats) {
    if (stats.device != device) {
      continue;
    }
    prev_io_time += stats.io_time;
    prev_weighted_io_time += stats.weighted_io_time;
  }

  for (const DiskStats& stats : cur_stats) {
    if (stats.device != device) {
      continue;
    }
    cur_io_time += stats.io_time;
    cur_weighted_io_time += stats.weighted_io_time;
  }

  DINGO_LOG(INFO) << "cur_io_time: " << cur_io_time << " prev_io_time: " << prev_io_time
                  << " cur_weighted_io_time: " << cur_weighted_io_time
                  << " prev_weighted_io_time: " << prev_weighted_io_time;

  unsigned long long int io_time_diff = cur_io_time - prev_io_time;
  unsigned long long int weighted_io_time_diff = cur_weighted_io_time - prev_weighted_io_time;

  DINGO_LOG(INFO) << "io_time_diff: " << io_time_diff << " weighted_io_time_diff: " << weighted_io_time_diff;

  double io_utilization = 0.0;
  if (io_time_diff == 0 || weighted_io_time_diff == 0 || io_time_diff == weighted_io_time_diff) {
    return io_utilization;
  }
  io_utilization = 100.0 * io_time_diff / weighted_io_time_diff;

  return io_utilization;
}

bool Helper::GetSystemDiskIoUtil(const std::string& device_name, std::map<std::string, int64_t>& output) {
  try {
    double io_utilization = GetSystemIoUtilization(device_name);
    output["system_total_capacity"] = static_cast<int64_t>(io_utilization);
    DINGO_LOG(INFO) << fmt::format("System disk io utilization: {}", io_utilization);

    return true;
  } catch (std::exception& e) {
    DINGO_LOG(ERROR) << fmt::format("GetDiskIoUtil failed, error: {}", e.what());
    return false;
  }
}

std::vector<std::string> Helper::TraverseDirectory(const std::string& path, bool ignore_dir, bool ignore_file) {
  return TraverseDirectory(path, "", ignore_dir, ignore_file);
}

std::vector<std::string> Helper::TraverseDirectory(const std::string& path, const std::string& prefix, bool ignore_dir,
                                                   bool ignore_file) {
  std::vector<std::string> filenames;
  try {
    if (std::filesystem::exists(path)) {
      for (const auto& fe : std::filesystem::directory_iterator(path)) {
        if (ignore_dir && fe.is_directory()) {
          continue;
        }

        if (ignore_file && fe.is_regular_file()) {
          continue;
        }

        if (prefix.empty()) {
          filenames.push_back(fe.path().filename().string());
        } else {
          // check if the filename start with prefix
          auto filename = fe.path().filename().string();
          if (filename.find(prefix) == 0L) {
            filenames.push_back(filename);
          }
        }
      }
    }
  } catch (std::filesystem::filesystem_error const& ex) {
    DINGO_LOG(ERROR) << fmt::format("directory_iterator failed, path: {} error: {}", path, ex.what());
  }

  return filenames;
}

std::string Helper::FindFileInDirectory(const std::string& dirpath, const std::string& prefix) {
  try {
    if (std::filesystem::exists(dirpath)) {
      for (const auto& fe : std::filesystem::directory_iterator(dirpath)) {
        auto filename = fe.path().filename().string();
        if (filename.find(prefix) != std::string::npos) {
          return filename;
        }
      }
    }
  } catch (std::filesystem::filesystem_error const& ex) {
    DINGO_LOG(ERROR) << fmt::format("directory_iterator failed, path: {} prefix: {} error: {}", dirpath, prefix,
                                    ex.what());
  }

  return "";
}

bool Helper::CreateDirectory(const std::string& path) {
  std::error_code ec;
  if (!std::filesystem::create_directories(path, ec)) {
    DINGO_LOG(ERROR) << fmt::format("Create directory failed, error: {} {}", ec.value(), ec.message());
    return false;
  }

  return true;
}

butil::Status Helper::CreateDirectories(const std::string& path) {
  std::error_code ec;
  if (std::filesystem::exists(path)) {
    DINGO_LOG(INFO) << fmt::format("Directory already exists, path: {}", path);
    return butil::Status::OK();
  }

  if (!std::filesystem::create_directories(path, ec)) {
    DINGO_LOG(ERROR) << fmt::format("Create directory {} failed, error: {} {}", path, ec.value(), ec.message());
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("Create directory failed, error: {}", ec.message()));
  }

  DINGO_LOG(INFO) << fmt::format("Create directory success, path: {}", path);
  return butil::Status::OK();
}

bool Helper::RemoveFileOrDirectory(const std::string& path) {
  std::error_code ec;
  DINGO_LOG(INFO) << fmt::format("Remove file or directory, path: {}", path);
  if (!std::filesystem::remove(path, ec)) {
    DINGO_LOG(ERROR) << fmt::format("Remove directory failed, path: {} error: {} {}", path, ec.value(), ec.message());
    return false;
  }

  return true;
}

bool Helper::RemoveAllFileOrDirectory(const std::string& path) {
  std::error_code ec;
  DINGO_LOG(INFO) << fmt::format("Remove all file or directory, path: {}", path);
  auto num = std::filesystem::remove_all(path, ec);
  if (num == static_cast<std::uintmax_t>(-1)) {
    DINGO_LOG(ERROR) << fmt::format("Remove all directory failed, path: {} error: {} {}", path, ec.value(),
                                    ec.message());
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

bool Helper::IsExistPath(const std::string& path) { return std::filesystem::exists(path); }

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

// std::string Helper::EncodeIndexRegionHeader(int64_t partition_id, int64_t vector_id) {
//   Buf buf(16);
//   buf.WriteLong(partition_id);
//   buf.WriteLong(vector_id);

//   return buf.GetString();
// }

uint64_t Helper::DecodeVectorId(const std::string& value) {
  dingodb::Buf buf(value);
  if (value.size() == 17) {
    buf.Skip(9);
  } else if (value.size() == 16) {
    buf.Skip(8);
  } else if (value.size() == 9 || value.size() == 8) {
    return 0;
  } else {
    DINGO_LOG(ERROR) << "Decode vector id failed, value size is not 16 or 17, value:["
                     << dingodb::Helper::StringToHex(value) << "]"
                     << " size: " << value.size();
    return 0;
  }

  return buf.ReadLong();
}

// for txn, encode start_ts/commit_ts to std::string
std::string Helper::EncodeTso(uint64_t ts) {
  Buf buf(8);
  buf.WriteLongWithNegation(ts);

  return buf.GetString();
}

std::string Helper::PaddingUserKey(const std::string& key) {
  uint32_t new_size = ((8 + key.length()) / 8) * 9;
  char buf[new_size];
  const auto* data = key.data();

  int i = 0;
  int j = 0;
  for (i = 0; i < key.length(); i++) {
    if ((i + 1) % 8 != 0) {
      buf[j++] = data[i];
    } else {
      buf[j++] = data[i];
      buf[j++] = '\xff';
    }
  }

  int padding_num = 8 - (i % 8);

  for (int k = 0; k < padding_num; k++) {
    buf[j++] = '\x00';
  }
  buf[j] = '\xff' - padding_num;

  return std::string(buf, new_size);
}

std::string Helper::UnpaddingUserKey(const std::string& padding_key) {
  if (padding_key.length() % 9 != 0) {
    return std::string();
  }

  uint32_t new_size = ((padding_key.length() / 9) * 8);
  uint32_t padding_num = '\xff' - padding_key.back();

  if (padding_num == 0) {
    return std::string();
  }

  char buf[new_size];
  const auto* data = padding_key.data();

  int i = 0;
  int j = 0;
  for (i = 0; i < padding_key.length(); i++) {
    if ((i + 1) % 9 != 0) {
      buf[j++] = data[i];
    }
  }

  return std::string(buf, new_size - padding_num);
}

// for txn, encode data/write key
std::string Helper::EncodeTxnKey(const std::string& key, uint64_t ts) {
  std::string padding_key = Helper::PaddingUserKey(key);
  Buf buf(padding_key.length() + 8);
  buf.Write(padding_key);
  buf.WriteLongWithNegation(ts);

  return buf.GetString();
}

std::string Helper::EncodeTxnKey(const std::string_view& key, uint64_t ts) {
  std::string padding_key = Helper::PaddingUserKey(std::string(key));
  Buf buf(padding_key.length() + 8);
  buf.Write(padding_key);
  buf.WriteLongWithNegation(ts);

  return buf.GetString();
}

// for txn, encode data/write key
butil::Status Helper::DecodeTxnKey(const std::string& txn_key, std::string& key, uint64_t& ts) {
  if (txn_key.length() <= 8) {
    return butil::Status(pb::error::EINTERNAL, "DecodeTxnKey failed, txn_key length <= 8");
  }

  auto padding_key = txn_key.substr(0, txn_key.length() - 8);
  key = Helper::UnpaddingUserKey(padding_key);
  if (key.empty()) {
    return butil::Status(pb::error::EINTERNAL, "DecodeTxnKey failed, padding_key is empty");
  }

  auto ts_str = txn_key.substr(txn_key.length() - 8);
  Buf buf(ts_str);
  ts = ~buf.ReadLong();

  return butil::Status::OK();
}

// for txn, encode data/write key
butil::Status Helper::DecodeTxnKey(const std::string_view& txn_key, std::string& key, uint64_t& ts) {
  if (txn_key.length() <= 8) {
    return butil::Status(pb::error::EINTERNAL, "DecodeTxnKey failed, txn_key length <= 8");
  }

  auto padding_key = txn_key.substr(0, txn_key.length() - 8);
  key = Helper::UnpaddingUserKey(std::string(padding_key));
  if (key.empty()) {
    return butil::Status(pb::error::EINTERNAL, "DecodeTxnKey failed, padding_key is empty");
  }

  std::string ts_str;
  ts_str = txn_key.substr(txn_key.length() - 8);
  Buf buf(ts_str);
  ts = ~buf.ReadLong();

  return butil::Status::OK();
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

std::string Helper::CleanFirstSlash(const std::string& str) { return (str.front() == '/') ? str.substr(1) : str; }

bool Helper::ParallelRunTask(TaskFunctor task, void* arg, int concurrency) {
  int64_t start_time = Helper::TimestampMs();

  bool all_success = true;
  std::vector<bthread_t> tids;
  tids.resize(concurrency);
  for (int i = 0; i < concurrency; ++i) {
    int ret = bthread_start_background(&tids[i], nullptr, task, arg);
    if (ret != 0) {
      DINGO_LOG(ERROR) << "Create bthread failed, ret: " << ret;
      all_success = false;
      tids[i] = 0;
      break;
    }
  }

  if (!all_success) {
    for (int i = 0; i < concurrency; ++i) {
      if (tids[i] != 0) {
        bthread_stop(tids[i]);
      }
    }
    return false;
  }

  for (int i = 0; i < concurrency; ++i) {
    bthread_join(tids[i], nullptr);
  }

  DINGO_LOG(INFO) << fmt::format("parallel run task elapsed time {}ms", Helper::TimestampMs() - start_time);

  return true;
}

butil::Status Helper::ValidateRaftStatusForSplit(std::shared_ptr<pb::common::BRaftStatus> raft_status) {  // NOLINT
  if (raft_status == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Get raft status failed.");
  }

  if (!raft_status->unstable_followers().empty()) {
    return butil::Status(pb::error::EINTERNAL, "Has unstable followers.");
  }

  for (const auto& [peer, follower] : raft_status->stable_followers()) {
    if (follower.consecutive_error_times() > 0) {
      return butil::Status(pb::error::EINTERNAL, "follower %s abnormal.", peer.c_str());
    }

    if (follower.next_index() + Constant::kRaftLogFallBehindThreshold < raft_status->last_index()) {
      return butil::Status(pb::error::EINTERNAL, "Follower %s log fall behind exceed %d.", peer.c_str(),
                           Constant::kRaftLogFallBehindThreshold);
    }
  }

  return butil::Status();
}

// calc hnsw count from memory
uint32_t Helper::CalcHnswCountFromMemory(int64_t memory_size_limit, int64_t dimension, int64_t nlinks) {
  // size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
  int64_t size_links_level0 = nlinks * 2 + sizeof(int64_t) + sizeof(int64_t);

  // int64_t size_data_per_element_ = size_links_level0_ + data_size_ + sizeof(labeltype);
  int64_t size_data_per_element = size_links_level0 + sizeof(float_t) * dimension + sizeof(int64_t);

  // int64_t size_link_list_per_element =  sizeof(void*);
  int64_t size_link_list_per_element = sizeof(int64_t);

  int64_t count = memory_size_limit / (size_data_per_element + size_link_list_per_element);

  if (count > UINT32_MAX) {
    count = UINT32_MAX;
  }

  return static_cast<uint32_t>(count);
}

std::string Helper::GenMaxStartKey() {
  Buf buf(8);
  buf.WriteLong(INT64_MAX);
  return buf.GetString();
}

std::string Helper::GenMinStartKey() {
  Buf buf(8);
  buf.WriteLong(0);
  return buf.GetString();
}

// Parse region meta
butil::Status Helper::ParseRaftSnapshotRegionMeta(const std::string& snapshot_path,
                                                  pb::store_internal::RaftSnapshotRegionMeta& meta) {
  std::string filepath = snapshot_path + "/" + Constant::kRaftSnapshotRegionMetaFileName;
  if (!Helper::IsExistPath(filepath)) {
    return butil::Status(pb::error::EINTERNAL, "region meta file not exist, filepath: %s", filepath.c_str());
  }
  std::ifstream file(filepath);
  if (!file.is_open()) {
    return butil::Status(pb::error::EINTERNAL, "open file failed, filepath: %s", filepath.c_str());
  }
  if (!meta.ParseFromIstream(&file)) {
    return butil::Status(pb::error::EINTERNAL, "parse region meta file failed, filepath: %s", filepath.c_str());
  }

  return butil::Status();
}

}  // namespace dingodb

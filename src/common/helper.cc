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
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <random>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "braft/configuration.h"
#include "brpc/builtin/common.h"
#include "bthread/bthread.h"
#include "butil/compiler_specific.h"
#include "butil/endpoint.h"
#include "butil/status.h"
#include "butil/strings/string_split.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/role.h"
#include "common/service_access.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "google/protobuf/util/json_util.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "serial/buf.h"
#include "serial/schema/long_schema.h"

namespace dingodb {

using Errno = pb::error::Errno;
using PbError = pb::error::Error;

int Helper::GetCoreNum() { return sysconf(_SC_NPROCESSORS_ONLN); }

bool Helper::IsIp(const std::string& s) {
  std::regex const reg(
      R"((25[0-5]|2[0-4]\d|[0-1]\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|[0-1]\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|[0-1]\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|[0-1]\d{2}|[1-9]?\d))");
  return std::regex_match(s, reg);
}

// ip:port or hostname:port
bool Helper::IsIpInAddr(const std::string& addr) {
  std::vector<std::string> parts;
  SplitString(addr, ':', parts);
  if (parts.empty()) {
    return false;
  }

  return IsIp(parts[0]);
}

bool Helper::IsExecutorRaw(const std::string& key) { return key.empty() ? false : key[0] == Constant::kExecutorRaw; }

bool Helper::IsExecutorTxn(const std::string& key) { return key.empty() ? false : key[0] == Constant::kExecutorTxn; }

bool Helper::IsClientRaw(const std::string& key) { return key.empty() ? false : key[0] == Constant::kClientRaw; }

bool Helper::IsClientTxn(const std::string& key) { return key.empty() ? false : key[0] == Constant::kClientTxn; }

char Helper::GetKeyPrefix(const std::string& key) {
  CHECK(!key.empty()) << "key is empty";
  return key[0];
}

char Helper::GetKeyPrefix(const pb::common::Range& range) {
  CHECK(!range.start_key().empty()) << "key is empty";
  return range.start_key()[0];
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

// =======================================

// addr format: 127.0.0.1:8201:0
pb::common::Location Helper::StringToLocation(const std::string& addr) {
  pb::common::Location location;

  std::vector<std::string> parts;
  SplitString(addr, ':', parts);
  if (!parts.empty()) {
    location.set_host(parts[0]);
  }
  if (parts.size() >= 2) {
    location.set_port(std::stoi(parts[1]));
  }
  if (parts.size() >= 3) {
    location.set_index(std::stoi(parts[2]));
  }

  return location;
}

pb::common::Location Helper::StringToLocation(const std::string& ip_or_host, int port, int index) {
  pb::common::Location location;
  location.set_host(ip_or_host);
  location.set_port(port);
  location.set_index(index);

  return location;
}

// format: 127.0.0.1:8201:0
std::string Helper::LocationToString(const pb::common::Location& location) {
  return fmt::format("{}:{}:{}", location.host(), location.port(), location.index());
}

// format: 127.0.0.1:8201:0,127.0.0.1:8202:0,127.0.0.1:8203:0
std::string Helper::LocationsToString(const std::vector<pb::common::Location>& locations) {
  std::string addrs;
  for (int i = 0; i < locations.size(); ++i) {
    addrs += LocationToString(locations[i]);
    if (i + 1 < locations.size()) {
      addrs += ",";
    }
  }

  return addrs;
}

// ip or ip:port or ip:port:xxx
butil::EndPoint Helper::StringToEndPoint(const std::string& addr) {
  butil::EndPoint endpoint;

  std::vector<std::string> parts;
  SplitString(addr, ':', parts);

  std::string fix_addr;
  if (addr.empty()) {
    return endpoint;
  } else if (parts.size() == 1) {
    fix_addr = fmt::format("{}:0", parts[0]);
  } else if (parts.size() == 2) {
    fix_addr = addr;
  } else {
    fix_addr = fmt::format("{}:{}", parts[0], parts[1]);
  }

  if (IsIpInAddr(fix_addr)) {
    int ret = butil::str2endpoint(fix_addr.c_str(), &endpoint);
    DINGO_LOG_IF(ERROR, ret != 0) << fmt::format("str2endpoint failed, {}", addr);
  } else {
    int ret = butil::hostname2endpoint(fix_addr.c_str(), &endpoint);
    DINGO_LOG_IF(ERROR, ret != 0) << fmt::format("hostname2endpoint failed, {}", addr);
  }

  return endpoint;
}

butil::EndPoint Helper::StringToEndPoint(const std::string& ip_or_host, int port) {
  butil::EndPoint endpoint;

  if (IsIp(ip_or_host)) {
    int ret = butil::str2endpoint(ip_or_host.c_str(), port, &endpoint);
    DINGO_LOG_IF(ERROR, ret != 0) << fmt::format("str2endpoint failed, {}:{}", ip_or_host, port);
  } else {
    int ret = butil::hostname2endpoint(ip_or_host.c_str(), port, &endpoint);
    DINGO_LOG_IF(ERROR, ret != 0) << fmt::format("hostname2endpoint failed, {}:{}", ip_or_host, port);
  }

  return endpoint;
}

// addrs: 127.0.0.1:8201,127.0.0.1:8202,127.0.0.1:8203
std::vector<butil::EndPoint> Helper::StringToEndpoints(const std::string& addrs) {
  std::vector<std::string> parts;
  SplitString(addrs, ',', parts);

  std::vector<butil::EndPoint> endpoints;
  endpoints.reserve(parts.size());
  for (auto& addr : parts) {
    endpoints.push_back(StringToEndPoint(addr));
  }

  return endpoints;
}

std::string Helper::EndPointToString(const butil::EndPoint& endpoint) {
  return std::string(butil::endpoint2str(endpoint).c_str());
}

braft::PeerId Helper::StringToPeerId(const std::string& addr) {
  braft::PeerId peer_id;

  std::vector<std::string> parts;
  SplitString(addr, ':', parts);
  if (parts.empty()) {
  } else if (parts.size() == 1) {
    peer_id.parse(fmt::format("{}:0", addr));
  } else {
    peer_id.parse(addr);
  }

  return peer_id;
}

braft::PeerId Helper::StringToPeerId(const std::string& ip_or_host, int port) {
  return braft::PeerId(StringToEndPoint(ip_or_host, port));
}

pb::common::Location Helper::PeerIdToLocation(const braft::PeerId& peer_id) {
  pb::common::Location location;
  location.set_port(peer_id.addr.port);
  location.set_host(butil::ip2str(peer_id.addr.ip).c_str());

  return location;
}

// 127.0.0.1:8201:0:0,127.0.0.1:8202:0:0,127.0.0.1:8203:0:0
std::string Helper::PeerIdsToString(const std::vector<braft::PeerId>& peer_ids) {
  std::string addrs;
  for (int i = 0; i < peer_ids.size(); ++i) {
    addrs += peer_ids[i].to_string();
    if (i + 1 < peer_ids.size()) {
      addrs += ",";
    }
  }

  return addrs;
}

butil::EndPoint Helper::LocationToEndPoint(const pb::common::Location& location) {
  CHECK(!location.host().empty());

  butil::EndPoint endpoint;
  if (IsIp(location.host())) {
    int ret = butil::str2endpoint(location.host().c_str(), location.port(), &endpoint);
    DINGO_LOG_IF(ERROR, ret != 0) << fmt::format("str2endpoint failed, {}:{}", location.host(), location.port());
  } else {
    int ret = butil::hostname2endpoint(location.host().c_str(), location.port(), &endpoint);
    DINGO_LOG_IF(ERROR, ret != 0) << fmt::format("hostname2endpoint failed, {}:{}", location.host(), location.port());
  }

  return endpoint;
}

pb::common::Location Helper::EndPointToLocation(const butil::EndPoint& endpoint) {
  pb::common::Location location;
  location.set_host(butil::ip2str(endpoint.ip).c_str());
  location.set_port(endpoint.port);

  return location;
}

braft::PeerId Helper::LocationToPeerId(const pb::common::Location& location) {
  return braft::PeerId(LocationToEndPoint(location), location.index());
}

// =======================================

bool Helper::IsDifferenceEndPoint(const butil::EndPoint& location, const butil::EndPoint& other_location) {
  return 0 != strcmp(butil::ip2str(location.ip).c_str(), butil::ip2str(other_location.ip).c_str()) ||
         location.port != other_location.port;
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

bool Helper::IsDifferencePeers(const pb::common::RegionDefinition& src_definition,
                               const pb::common::RegionDefinition& dst_definition) {
  if (src_definition.peers_size() != dst_definition.peers_size()) {
    return true;
  }

  for (const auto& src_peer : src_definition.peers()) {
    bool is_exist = false;
    for (const auto& dst_peer : dst_definition.peers()) {
      if (src_peer.store_id() == dst_peer.store_id()) {
        is_exist = true;
        break;
      }
    }
    if (!is_exist) {
      return true;
    }
  }

  return false;
}

std::vector<pb::common::Location> Helper::ExtractRaftLocations(
    const google::protobuf::RepeatedPtrField<pb::common::Peer>& peers) {
  std::vector<pb::common::Location> locations;
  locations.reserve(peers.size());
  for (const auto& peer : peers) {
    locations.push_back(peer.raft_location());
  }
  return locations;
}

std::vector<pb::common::Location> Helper::ExtractRaftLocations(const std::vector<pb::common::Peer>& peers) {
  std::vector<pb::common::Location> locations;
  locations.reserve(peers.size());
  for (const auto& peer : peers) {
    locations.push_back(peer.raft_location());
  }
  return locations;
}

std::vector<braft::PeerId> Helper::ExtractPeerIds(const braft::Configuration& conf) {
  std::vector<braft::PeerId> peers;
  conf.list_peers(&peers);

  return peers;
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

std::string Helper::RangeToString(const pb::common::Range& range) {
  return fmt::format("[{}-{})", Helper::StringToHex(range.start_key()), Helper::StringToHex(range.end_key()));
}

bool Helper::IsContainRange(const pb::common::Range& range1, const pb::common::Range& range2) {
  return range2.start_key() >= range1.start_key() && range2.end_key() <= range1.end_key();
}

bool Helper::IsConflictRange(const pb::common::Range& range1, const pb::common::Range& range2) {
  // check if range1 and range2 is intersect
  return !(range1.start_key() >= range2.end_key() || range1.end_key() <= range2.start_key());
}

bool Helper::InvalidRange(const pb::common::Range& range) { return range.start_key() >= range.end_key(); }

butil::Status Helper::CheckRange(const pb::common::Range& range) {
  if (range.start_key().empty() || range.end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
  }
  if (range.start_key() >= range.end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
  }

  return butil::Status();
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

  try {
    // The hex_string must be of even length
    for (size_t i = 0; i < hex_str.length(); i += 2) {
      std::string hex_byte = hex_str.substr(i, 2);
      // Convert the hex byte to an integer
      int byte_value = std::stoi(hex_byte, nullptr, 16);
      // Cast the integer to a char and append it to the result string
      result += static_cast<unsigned char>(byte_value);
    }
  } catch (const std::invalid_argument& ia) {
    DINGO_LOG(ERROR) << "HexToString error Irnvalid argument: " << ia.what() << '\n';
    return "";
  } catch (const std::out_of_range& oor) {
    DINGO_LOG(ERROR) << "HexToString error Out of Range error: " << oor.what() << '\n';
    return "";
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
  if (BAIDU_UNLIKELY(message == nullptr)) {
    return;
  }
  const google::protobuf::Reflection* reflection = message->GetReflection();
  const google::protobuf::Descriptor* desc = message->GetDescriptor();

  const google::protobuf::FieldDescriptor* error_field = desc->FindFieldByName("error");
  if (BAIDU_UNLIKELY(error_field == nullptr)) {
    DINGO_LOG(ERROR) << "SetPbMessageError error_field is nullptr";
    return;
  }
  if (BAIDU_UNLIKELY(error_field->message_type()->full_name() != "dingodb.pb.error.Error")) {
    DINGO_LOG(ERROR) << "SetPbMessageError error_field->message_type()->full_name() is not pb::error::Errno, its_type="
                     << error_field->message_type()->full_name();
    return;
  }
  pb::error::Error* error = dynamic_cast<pb::error::Error*>(reflection->MutableMessage(message, error_field));
  error->set_errcode(static_cast<pb::error::Errno>(status.error_code()));
  error->set_errmsg(status.error_str());
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
    DINGO_LOG(WARNING) << "GetServiceLocation illegal raft_location=" << raft_location.host() << ":"
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
  std::random_device rd;  // Obtain a random seed from the hardware
  std::mt19937 rng(rd());
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

std::vector<float> Helper::GenerateFloatVector(int dimension) {
  // Create a random number generator engine
  std::random_device rd;      // Obtain a random seed from the hardware
  std::mt19937_64 gen(rd());  // Standard 64-bit mersenne_twister_engine seeded with rd()

  // Create a distribution for the desired range
  std::uniform_real_distribution<> dis(0, 1);

  std::vector<float> vec;
  vec.reserve(dimension);
  for (int i = 0; i < dimension; ++i) {
    vec.push_back(dis(gen));
  }
  return vec;
}

std::vector<uint8_t> Helper::GenerateInt8Vector(int dimension) {
  // Create a random number generator engine
  std::random_device rd;      // Obtain a random seed from the hardware
  std::mt19937_64 gen(rd());  // Standard 64-bit mersenne_twister_engine seeded with rd()

  // Create a distribution for the desired range
  std::uniform_int_distribution<uint8_t> dis(0, 255);

  std::vector<uint8_t> vec;
  vec.reserve(dimension);
  for (int i = 0; i < dimension; ++i) {
    vec.push_back(dis(gen));
  }
  return vec;
}

std::string Helper::GenNewTableCheckName(int64_t schema_id, const std::string& table_name) {
  Buf buf(8);
  buf.WriteLong(schema_id);
  return buf.GetString() + table_name;
}

std::string Helper::GenNewTenantCheckName(int64_t tenant_id, const std::string& name) {
  Buf buf(8);
  buf.WriteLong(tenant_id);
  return buf.GetString() + name;
}

bool Helper::Link(const std::string& old_path, const std::string& new_path) {
  int ret = ::link(old_path.c_str(), new_path.c_str());
  if (ret != 0) {
    DINGO_LOG(ERROR) << fmt::format("Create hard link failed, old_path: {} new_path: {} errno: {}", old_path, new_path,
                                    errno);
  }

  return ret == 0;
}

std::vector<std::string> Helper::GetColumnFamilyNamesByRole() {
  if (GetRole() == pb::common::ClusterRole::COORDINATOR) {
    return {Constant::kStoreDataCF, Constant::kStoreMetaCF};
  } else if (GetRole() == pb::common::ClusterRole::STORE || GetRole() == pb::common::ClusterRole::DOCUMENT) {
    return {Constant::kStoreDataCF, Constant::kStoreMetaCF, Constant::kTxnDataCF, Constant::kTxnLockCF,
            Constant::kTxnWriteCF};
  } else if (GetRole() == pb::common::ClusterRole::INDEX) {
    return {Constant::kStoreDataCF,
            Constant::kStoreMetaCF,
            Constant::kTxnDataCF,
            Constant::kTxnLockCF,
            Constant::kTxnWriteCF,
            Constant::kVectorScalarCF,
            Constant::kVectorScalarKeySpeedUpCF,
            Constant::kVectorTableCF};
  }

  return {};
}

std::vector<std::string> Helper::GetColumnFamilyNamesExecptMetaByRole() {
  if (GetRole() == pb::common::ClusterRole::COORDINATOR) {
    return {Constant::kStoreDataCF};
  } else if (GetRole() == pb::common::ClusterRole::STORE || GetRole() == pb::common::ClusterRole::DOCUMENT) {
    return {Constant::kStoreDataCF, Constant::kTxnDataCF, Constant::kTxnLockCF, Constant::kTxnWriteCF};
  } else if (GetRole() == pb::common::ClusterRole::INDEX) {
    return {Constant::kStoreDataCF,  Constant::kTxnDataCF,      Constant::kTxnLockCF,
            Constant::kTxnWriteCF,   Constant::kVectorScalarCF, Constant::kVectorScalarKeySpeedUpCF,
            Constant::kVectorTableCF};
  }

  return {};
}

std::vector<std::string> Helper::GetColumnFamilyNames(const std::string& key) {
  if (GetRole() == pb::common::ClusterRole::COORDINATOR) {
    return {Constant::kStoreDataCF};
  } else if (GetRole() == pb::common::ClusterRole::STORE) {
    if (IsExecutorTxn(key) || IsClientTxn(key)) {
      return {Constant::kTxnDataCF, Constant::kTxnLockCF, Constant::kTxnWriteCF};
    }
    return {Constant::kStoreDataCF};
  } else if (GetRole() == pb::common::ClusterRole::INDEX) {
    if (IsExecutorTxn(key) || IsClientTxn(key)) {
      return {Constant::kTxnDataCF,    Constant::kTxnLockCF,      Constant::kTxnWriteCF,
              Constant::kVectorDataCF, Constant::kVectorScalarCF, Constant::kVectorScalarKeySpeedUpCF,
              Constant::kVectorTableCF};
    }
    return {Constant::kVectorDataCF, Constant::kVectorScalarCF, Constant::kVectorScalarKeySpeedUpCF,
            Constant::kVectorTableCF};
  } else if (GetRole() == pb::common::ClusterRole::DOCUMENT) {
    if (IsExecutorTxn(key) || IsClientTxn(key)) {
      return {Constant::kTxnDataCF, Constant::kTxnLockCF, Constant::kTxnWriteCF, Constant::kStoreDataCF};
    }
    return {Constant::kStoreDataCF};
  }

  return {};
}

void Helper::GetColumnFamilyNames(const std::string& key, std::vector<std::string>& raw_cf_names,
                                  std::vector<std::string>& txn_cf_names) {
  if (GetRole() == pb::common::ClusterRole::COORDINATOR) {
    raw_cf_names.push_back(Constant::kStoreDataCF);
    return;
  } else if (GetRole() == pb::common::ClusterRole::STORE || GetRole() == pb::common::ClusterRole::DOCUMENT) {
    if (IsExecutorTxn(key) || IsClientTxn(key)) {
      txn_cf_names.push_back(Constant::kTxnDataCF);
      txn_cf_names.push_back(Constant::kTxnLockCF);
      txn_cf_names.push_back(Constant::kTxnWriteCF);
      return;
    } else {
      raw_cf_names.push_back(Constant::kStoreDataCF);
      return;
    }
  } else if (GetRole() == pb::common::ClusterRole::INDEX) {
    if (IsExecutorTxn(key) || IsClientTxn(key)) {
      txn_cf_names.push_back(Constant::kTxnDataCF);
      txn_cf_names.push_back(Constant::kTxnLockCF);
      txn_cf_names.push_back(Constant::kTxnWriteCF);

      raw_cf_names.push_back(Constant::kVectorDataCF);
      raw_cf_names.push_back(Constant::kVectorScalarCF);
      raw_cf_names.push_back(Constant::kVectorScalarKeySpeedUpCF);
      raw_cf_names.push_back(Constant::kVectorTableCF);

      return;
    } else {
      raw_cf_names.push_back(Constant::kVectorDataCF);
      raw_cf_names.push_back(Constant::kVectorScalarCF);
      raw_cf_names.push_back(Constant::kVectorScalarKeySpeedUpCF);
      raw_cf_names.push_back(Constant::kVectorTableCF);
    }
  }
}

bool Helper::IsTxnColumnFamilyName(const std::string& cf_name) {
  return cf_name == Constant::kTxnDataCF || cf_name == Constant::kTxnLockCF || cf_name == Constant::kTxnWriteCF;
}

int64_t Helper::TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::TimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
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

int Helper::NowHour() {
  std::time_t tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::tm* ptm = std::localtime(&tt);
  return ptm->tm_hour;
}

std::string Helper::PastDate(int64_t day) { return FormatTime(Timestamp() - day * 86400, "%Y-%m-%d"); }

std::string Helper::FormatMsTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str()) << "." << timestamp % 1000;
  return ss.str();
}

std::string Helper::FormatMsTime(int64_t timestamp) { return FormatMsTime(timestamp, "%Y-%m-%d %H:%M:%S"); }

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

std::string Helper::ConcatPath(const std::string& path1, const std::string& path2) {
  std::filesystem::path path_a(path1);
  std::filesystem::path path_b(path2);
  return (path_a / path_b).string();
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

int64_t Helper::GetFileSize(const std::string& path) {
  try {
    std::uintmax_t size = std::filesystem::file_size(path);
    DINGO_LOG(INFO) << fmt::format("File size: {} bytes", size);
    return size;
  } catch (const std::filesystem::filesystem_error& ex) {
    DINGO_LOG(ERROR) << fmt::format("Get file size failed, path: {}, error: {}", path, ex.what());
    return -1;
  }
}

butil::Status Helper::SavePBFile(const std::string& path, const ::google::protobuf::Message* message) {
  std::string tmp_path(path);
  tmp_path.append(".tmp");

  std::ofstream file(tmp_path);
  if (!file.is_open()) {
    return butil::Status(1, "open file fail");
  }

  file << message->SerializeAsString();

  file.close();

  return Rename(tmp_path, path);
}

butil::Status Helper::LoadPBFile(const std::string& path, google::protobuf::Message* message) {
  std::ifstream file(path);
  if (!file.is_open()) {
    return butil::Status(1, "open file fail");
  }

  std::stringstream buffer;
  buffer << file.rdbuf();

  file.close();

  if (!message->ParseFromString(buffer.str())) {
    return butil::Status(1, "parse protobuf fail");
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

bool Helper::StringToBool(const std::string& str) { return !(str == "0" || str == "false"); }
int32_t Helper::StringToInt32(const std::string& str) { return std::strtol(str.c_str(), nullptr, 10); }
int64_t Helper::StringToInt64(const std::string& str) { return std::strtoll(str.c_str(), nullptr, 10); }
float Helper::StringToFloat(const std::string& str) { return std::strtof(str.c_str(), nullptr); }
double Helper::StringToDouble(const std::string& str) { return std::strtod(str.c_str(), nullptr); }

std::vector<float> Helper::StringToVector(const std::string& str) {
  std::vector<float> vec;
  std::stringstream ss(str);
  std::string token;

  while (std::getline(ss, token, ',')) {
    vec.push_back(std::stof(token));
  }

  return vec;
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

butil::Status Helper::ValidateRaftStatusForSplitMerge(std::shared_ptr<pb::common::BRaftStatus> raft_status) {  // NOLINT
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

std::string Helper::GenMaxStartKey() { return std::string(9, '\xff'); }

std::string Helper::GenMinStartKey() { return std::string(1, '\x00'); }

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

int Helper::CompareRegionEpoch(const pb::common::RegionEpoch& src_epoch, const pb::common::RegionEpoch& dst_epoch) {
  if (src_epoch.conf_version() == dst_epoch.conf_version() && src_epoch.version() == dst_epoch.version()) {
    return 0;
  }

  if (src_epoch.conf_version() < dst_epoch.conf_version() || src_epoch.version() < dst_epoch.version()) {
    return -1;
  }

  return 1;
}

bool Helper::IsEqualRegionEpoch(const pb::common::RegionEpoch& src_epoch, const pb::common::RegionEpoch& dst_epoch) {
  return src_epoch.conf_version() == dst_epoch.conf_version() && src_epoch.version() == dst_epoch.version();
}

std::string Helper::RegionEpochToString(const pb::common::RegionEpoch& epoch) {
  return fmt::format("{}-{}", epoch.conf_version(), epoch.version());
}

std::string Helper::PrintStatus(const butil::Status& status) {
  return fmt::format("{} {}", pb::error::Errno_Name(status.error_code()), status.error_str());
}

bool Helper::IsContinuous(const std::set<int64_t>& numbers) {
  if (numbers.empty()) {
    return true;
  }

  auto it = numbers.begin();
  int previous = *it;
  ++it;

  for (; it != numbers.end(); ++it) {
    if (*it != previous + 1) {
      return false;
    }
    previous = *it;
  }

  return true;
}

void Helper::SplitString(const std::string& str, char c, std::vector<std::string>& vec) {
  butil::SplitString(str, c, &vec);
}

void Helper::SplitString(const std::string& str, char c, std::vector<int64_t>& vec) {
  std::vector<std::string> strs;
  SplitString(str, c, strs);
  for (auto& s : strs) {
    try {
      vec.push_back(std::stoll(s));
    } catch (const std::exception& e) {
      DINGO_LOG(ERROR) << "stoll exception: " << e.what();
    }
  }
}

#if 1  // NOLINT
#define DINGO_PRAGMA_IMPRECISE_FUNCTION_BEGIN _Pragma("float_control(precise, off, push)")
#define DINGO_PRAGMA_IMPRECISE_FUNCTION_END _Pragma("float_control(pop)")
#define DINGO_PRAGMA_IMPRECISE_LOOP _Pragma("clang loop vectorize(enable) interleave(enable)")
#else
#define DINGO_PRAGMA_IMPRECISE_FUNCTION_BEGIN
#define DINGO_PRAGMA_IMPRECISE_FUNCTION_END
#define DINGO_PRAGMA_IMPRECISE_LOOP
#endif

DINGO_PRAGMA_IMPRECISE_FUNCTION_BEGIN
float Helper::DingoFaissInnerProduct(const float* x, const float* y, size_t d) {
  float res = 0.F;
  DINGO_PRAGMA_IMPRECISE_LOOP
  for (size_t i = 0; i != d; ++i) {
    res += x[i] * y[i];
  }
  return res;
}
DINGO_PRAGMA_IMPRECISE_FUNCTION_END

DINGO_PRAGMA_IMPRECISE_FUNCTION_BEGIN
float Helper::DingoFaissL2sqr(const float* x, const float* y, size_t d) {
  size_t i;
  float res = 0;
  DINGO_PRAGMA_IMPRECISE_LOOP
  for (i = 0; i < d; i++) {
    const float tmp = x[i] - y[i];
    res += tmp * tmp;
  }
  return res;
}
DINGO_PRAGMA_IMPRECISE_FUNCTION_END

float Helper::DingoHnswInnerProduct(const float* p_vect1, const float* p_vect2, size_t d) {
  float res = 0;
  for (unsigned i = 0; i < d; i++) {
    res += ((float*)p_vect1)[i] * ((float*)p_vect2)[i];
  }
  return res;
}

float Helper::DingoHnswInnerProductDistance(const float* p_vect1, const float* p_vect2, size_t d) {
  return 1.0f - DingoHnswInnerProduct(p_vect1, p_vect2, d);
}

float Helper::DingoHnswL2Sqr(const float* p_vect1v, const float* p_vect2v, size_t d) {
  float* p_vect1 = (float*)p_vect1v;
  float* p_vect2 = (float*)p_vect2v;

  float res = 0;
  for (size_t i = 0; i < d; i++) {
    float t = *p_vect1 - *p_vect2;
    p_vect1++;
    p_vect2++;
    res += t * t;
  }
  return (res);
}

bool Helper::SaveFile(const std::string& filepath, const std::string& data) {
  std::ofstream file(filepath);
  if (!file.is_open()) {
    return false;
  }

  file << data;
  file.close();

  return true;
}

// Print a table in HTML or plain text format.
// @param os Output stream
// @param use_html Whether to use HTML format
// @param table_header Header of the table
// @param min_widths Minimum width of each column
// @param table_contents Contents of the table
// @param table_urls Urls of the table
void Helper::PrintHtmlTable(std::ostream& os, bool use_html, const std::vector<std::string>& table_header,
                            const std::vector<int32_t>& min_widths,
                            const std::vector<std::vector<std::string>>& table_contents,
                            const std::vector<std::vector<std::string>>& table_urls) {
  if (BAIDU_UNLIKELY(table_header.size() != min_widths.size())) {
    os << "! table_header.size(" << table_header.size() << ") == min_widths.size(" << min_widths.size() << ")";
    return;
  }
  if (BAIDU_UNLIKELY(!table_contents.empty() && table_header.size() != table_contents[0].size())) {
    os << "! table_header.size(" << table_header.size() << ") == table_contents[0].size(" << table_contents[0].size()
       << ")";
    return;
  }
  if (BAIDU_UNLIKELY(!table_urls.empty() && table_header.size() != table_urls[0].size())) {
    os << "! table_header.size(" << table_header.size() << ") == table_urls[0].size(" << table_urls[0].size() << ")";
    return;
  }

  if (BAIDU_LIKELY(use_html)) {
    // os << "<table class=\"gridtable sortable\" border=\"1\"><tr>";
    os << R"(<table class="gridtable sortable" border="1"><tr>)";
    for (const auto& header : table_header) {
      os << "<th>" << header << "</th>";
    }
    os << "</tr>\n";
  } else {
    for (const auto& header : table_header) {
      os << header << "|";
    }

    os << '\n';
  }

  for (int i = 0; i < table_contents.size(); i++) {
    const auto& line = table_contents[i];
    const std::vector<std::string>& url_line = table_urls.empty() ? std::vector<std::string>() : table_urls[i];

    if (use_html) {
      os << "<tr>";
    }
    for (size_t i = 0; i < line.size(); ++i) {
      if (use_html) {
        os << "<td>";
      }

      if (use_html) {
        if (!url_line.empty() && !url_line[i].empty()) {
          if (url_line[i].substr(0, 4) == "http") {
            os << "<a href=\"" << url_line[i] << "\">" << line[i] << "</a>";
          } else {
            os << "<a href=\"" << url_line[i] << line[i] << "\">" << line[i] << "</a>";
          }
        } else {
          if (line[i].size() <= 64) {
            os << brpc::min_width(line[i], min_widths[i]);
          } else {
            os << "<div class=\"part\">" << line[i].substr(0, 64) << "..."
               << "<span class=\"full\">" << line[i] << "</span></div>";
          }
        }
      } else {
        os << brpc::min_width(line[i], min_widths[i]) << '\n';
      }
      if (use_html) {
        os << "</td>";
      } else {
        os << "|";
      }
    }

    if (use_html) {
      os << "</tr>";
    }

    os << '\n';
  }

  if (use_html) {
    os << "</table>\n";
  }
}

// Print a multi-lines in HTML or plain text format.
// @param os Output stream
// @param use_html Whether to use HTML format
// @param table_header Header of the table
// @param min_widths Minimum width of each column
// @param table_contents Contents of the table
// @param table_urls Urls of the table
void Helper::PrintHtmlLines(std::ostream& os, bool use_html, const std::vector<std::string>& table_header,
                            const std::vector<int32_t>& min_widths,
                            const std::vector<std::vector<std::string>>& table_contents,
                            const std::vector<std::vector<std::string>>& table_urls) {
  if (BAIDU_UNLIKELY(table_header.size() != min_widths.size())) {
    os << "! table_header.size(" << table_header.size() << ") == min_widths.size(" << min_widths.size() << ")";
    return;
  }
  if (BAIDU_UNLIKELY(!table_contents.empty() && table_header.size() != table_contents[0].size())) {
    os << "! table_header.size(" << table_header.size() << ") == table_contents[0].size(" << table_contents[0].size()
       << ")";
    return;
  }
  if (BAIDU_UNLIKELY(!table_urls.empty() && table_header.size() != table_urls[0].size())) {
    os << "! table_header.size(" << table_header.size() << ") == table_urls[0].size(" << table_urls[0].size() << ")";
    return;
  }

  if (BAIDU_LIKELY(use_html)) {
    os << "<p>";
    for (const auto& header : table_header) {
      os << header << "|";
    }
    os << "</p>";
  } else {
    for (const auto& header : table_header) {
      os << header << "|";
    }
    os << '\n';
  }

  for (int i = 0; i < table_contents.size(); i++) {
    const auto& line = table_contents[i];
    const std::vector<std::string>& url_line = table_urls.empty() ? std::vector<std::string>() : table_urls[i];

    if (use_html) {
      os << "<p>";
    }

    for (size_t i = 0; i < line.size(); ++i) {
      if (use_html) {
        if (!url_line.empty() && !url_line[i].empty()) {
          if (url_line[i].substr(0, 4) == "http") {
            os << "<a href=\"" << url_line[i] << "\">" << line[i] << "|</a>";
          } else {
            os << "<a href=\"" << url_line[i] << line[i] << "\">" << line[i] << "|</a>";
          }
        } else {
          os << brpc::min_width(line[i], min_widths[i]) << "|";
        }
      } else {
        os << brpc::min_width(line[i], min_widths[i]) << "|";
      }
    }

    if (use_html) {
      os << "</p>";
    } else {
      os << '\n';
    }
  }
}

int32_t Helper::GetCores() {
  int32_t cores = sysconf(_SC_NPROCESSORS_ONLN);
  CHECK(cores > 0) << "System not support cpu core count.";

  return cores;
}

int64_t Helper::GetPid() {
  pid_t pid = getpid();

  return static_cast<int64_t>(pid);
}

std::vector<int64_t> Helper::GetThreadIds(int64_t pid) {
  std::vector<int64_t> thread_ids;

  const std::string path = fmt::format("/proc/{}/task", pid);
  try {
    if (std::filesystem::exists(path)) {
      for (const auto& fe : std::filesystem::directory_iterator(path)) {
        if (!fe.is_directory()) {
          continue;
        }

        thread_ids.push_back(std::stoll(fe.path().filename().string()));
      }
    }
  } catch (std::filesystem::filesystem_error const& ex) {
    DINGO_LOG(ERROR) << fmt::format("get thread id failed, error: {}", ex.what());
  }

  return thread_ids;
}

std::vector<std::string> Helper::GetThreadNames(int64_t pid) {
  auto thread_ids = GetThreadIds(pid);

  std::vector<std::string> thread_names;
  for (auto thread_id : thread_ids) {
    const std::string comm_path = fmt::format("/proc/{}/task/{}/comm", pid, thread_id);
    std::ifstream comm_file(comm_path);
    if (comm_file.is_open()) {
      std::string thread_name;
      std::getline(comm_file, thread_name);
      thread_names.push_back(thread_name);
      comm_file.close();
    }
  }

  return thread_names;
}

std::vector<std::string> Helper::GetThreadNames(int64_t pid, const std::string& filter_name) {
  std::vector<std::string> thread_names = GetThreadNames(pid);

  std::vector<std::string> filter_thread_names;
  for (const auto& thread_name : thread_names) {
    if (thread_name.find(filter_name) != std::string::npos) {
      filter_thread_names.push_back(thread_name);
    }
  }

  return filter_thread_names;
}

LogLevel Helper::LogLevelPB2LogLevel(const pb::node::LogLevel& level) {
  switch (level) {
    case pb::node::LogLevel::DEBUG:
      return LogLevel::kDEBUG;
    case pb::node::LogLevel::INFO:
      return LogLevel::kINFO;
    case pb::node::LogLevel::WARNING:
      return LogLevel::kWARNING;
    case pb::node::LogLevel::ERROR:
      return LogLevel::kERROR;
    case pb::node::LogLevel::FATAL:
      return LogLevel::kFATAL;
    default:
      CHECK(false) << "Invalid log level: " << level;
  }
}

std::string Helper::ConvertColumnValueToString(const pb::meta::ColumnDefinition& column_definition,
                                               const std::any& value) {  // NOLINT
  std::ostringstream ostr;

  if (value.type() == typeid(std::optional<std::string>)) {
    auto v = std::any_cast<std::optional<std::string>>(value);
    ostr << v.value_or("");
  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::string>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::string>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      if (column_definition.sql_type() == "BINARY" || column_definition.sql_type() == "ANY") {
        ostr << Helper::StringToHex(*ptr);
      } else {
        ostr << *ptr;
      }
    }
  } else if (value.type() == typeid(std::optional<int32_t>)) {
    auto v = std::any_cast<std::optional<int32_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<uint32_t>)) {
    auto v = std::any_cast<std::optional<uint32_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<int64_t>)) {
    auto v = std::any_cast<std::optional<int64_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<int64_t>)) {
    auto v = std::any_cast<std::optional<int64_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<double>)) {
    auto v = std::any_cast<std::optional<double>>(value);
    ostr << v.value_or(0.0);
  } else if (value.type() == typeid(std::optional<float>)) {
    auto v = std::any_cast<std::optional<float>>(value);
    ostr << v.value_or(0.0);
  } else if (value.type() == typeid(std::optional<bool>)) {
    auto v = std::any_cast<std::optional<bool>>(value);
    ostr << v.value_or(false);

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<bool>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<bool>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<std::string>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<std::string>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<double>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<double>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<float>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<float>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<int32_t>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<int32_t>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<int64_t>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<int64_t>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << Helper::VectorToString(*ptr);
    }

  } else {
    ostr << fmt::format("unknown type({})", value.type().name());
  }

  return ostr.str();
}

pb::common::Schema::Type Helper::TransformSchemaType(const std::string& name) {
  static std::map<std::string, dingodb::pb::common::Schema::Type> schema_type_map = {
      std::make_pair("CHAR", dingodb::pb::common::Schema::STRING),
      std::make_pair("VARCHAR", dingodb::pb::common::Schema::STRING),
      std::make_pair("ANY", dingodb::pb::common::Schema::STRING),
      std::make_pair("BINARY", dingodb::pb::common::Schema::STRING),
      std::make_pair("VARBINARY", dingodb::pb::common::Schema::STRING),
      std::make_pair("INT", dingodb::pb::common::Schema::INTEGER),
      std::make_pair("INTEGER", dingodb::pb::common::Schema::INTEGER),
      std::make_pair("BIGINT", dingodb::pb::common::Schema::LONG),
      std::make_pair("DATE", dingodb::pb::common::Schema::LONG),
      std::make_pair("TIME", dingodb::pb::common::Schema::LONG),
      std::make_pair("TIMESTAMP", dingodb::pb::common::Schema::LONG),
      std::make_pair("DOUBLE", dingodb::pb::common::Schema::DOUBLE),
      std::make_pair("BOOL", dingodb::pb::common::Schema::BOOL),
      std::make_pair("BOOLEAN", dingodb::pb::common::Schema::BOOL),
      std::make_pair("FLOAT", dingodb::pb::common::Schema::FLOAT),
      std::make_pair("LONG", dingodb::pb::common::Schema::LONG),

      std::make_pair("ARRAY_BOOL", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("ARRAY_BOOLEAN", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("ARRAY_INTEGER", dingodb::pb::common::Schema::INTEGERLIST),
      std::make_pair("ARRAY_FLOAT", dingodb::pb::common::Schema::FLOATLIST),
      std::make_pair("ARRAY_DOUBLE", dingodb::pb::common::Schema::DOUBLELIST),
      std::make_pair("ARRAY_LONG", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_BIGINT", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_DATE", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_TIME", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_TIMESTAMP", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_CHAR", dingodb::pb::common::Schema::STRINGLIST),
      std::make_pair("ARRAY_VARCHAR", dingodb::pb::common::Schema::STRINGLIST),

      std::make_pair("MULTISET_BOOL", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("MULTISET_BOOLEAN", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("MULTISET_INTEGER", dingodb::pb::common::Schema::INTEGERLIST),
      std::make_pair("MULTISET_FLOAT", dingodb::pb::common::Schema::FLOATLIST),
      std::make_pair("MULTISET_DOUBLE", dingodb::pb::common::Schema::DOUBLELIST),
      std::make_pair("MULTISET_LONG", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_BIGINT", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_DATE", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_TIME", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_TIMESTAMP", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_CHAR", dingodb::pb::common::Schema::STRINGLIST),
      std::make_pair("MULTISET_VARCHAR", dingodb::pb::common::Schema::STRINGLIST),
  };

  auto it = schema_type_map.find(Helper::ToUpper(name));
  CHECK(it != schema_type_map.end()) << "Not found schema type: " << Helper::ToUpper(name);

  return it->second;
}

bool Helper::IsSupportSplitAndMerge(const pb::common::RegionDefinition& definition) {
  const auto& index_parameter = definition.index_parameter();
  return index_parameter.index_type() != pb::common::IndexType::INDEX_TYPE_VECTOR ||
         index_parameter.vector_index_parameter().vector_index_type() !=
             pb::common::VectorIndexType::VECTOR_INDEX_TYPE_DISKANN;
}

}  // namespace dingodb

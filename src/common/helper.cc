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

#include "butil/endpoint.h"
#include "butil/strings/string_split.h"
#include "butil/strings/stringprintf.h"
#include "google/protobuf/util/json_util.h"

namespace dingodb {

bool Helper::IsIp(const std::string& s) {
  std::regex reg(
      "(?=(\\b|\\D))(((\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))\\.){3}(("
      "\\d{1,2})|(1\\d{1,2})|(2[0-4]\\d)|(25[0-5]))(?=(\\b|\\D))");
  return std::regex_match(s, reg);
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
  for (const auto& peer : peers) {
    locations.push_back(peer.raft_location());
  }
  return locations;
}

// format: 127.0.0.1:8201:0
std::string Helper::LocationToString(const pb::common::Location& location) {
  return butil::StringPrintf("%s:%d:0", location.host().c_str(), location.port());
}

butil::EndPoint Helper::LocationToEndPoint(const pb::common::Location& location) {
  butil::EndPoint endpoint;
  if (butil::hostname2endpoint(location.host().c_str(), location.port(), &endpoint) != 0 &&
      str2endpoint(location.host().c_str(), location.port(), &endpoint) != 0) {
  }

  return endpoint;
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

std::shared_ptr<pb::error::Error> Helper::Error(pb::error::Errno errcode, const std::string& errmsg) {
  std::shared_ptr<pb::error::Error> err = std::make_shared<pb::error::Error>();
  err->set_errcode(errcode);
  err->set_errmsg(errmsg);
  return err;
}

bool Helper::Error(pb::error::Errno errcode, const std::string& errmsg, pb::error::Error& err) {
  err.set_errcode(errcode);
  err.set_errmsg(errmsg);
  return false;
}

bool Helper::Error(pb::error::Errno errcode, const std::string& errmsg, std::shared_ptr<pb::error::Error> err) {
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
    if (input[i] == 0xFF && carry == 1) {
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
  for (auto it : stores) {
    if (it.second->raft_location().host() == host && it.second->raft_location().port() == endpoint.port) {
      str2endpoint(it.second->server_location().host().c_str(), it.second->server_location().port(), &result);
    }
  }

  return result;
}

}  // namespace dingodb

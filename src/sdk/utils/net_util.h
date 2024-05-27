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

#ifndef DINGODB_SDK_UTILS_NET_UTIL_H
#define DINGODB_SDK_UTILS_NET_UTIL_H

#include <cstdint>
#include <string>
namespace dingodb {
namespace sdk {

class EndPoint {
 public:
  EndPoint() = default;
  ~EndPoint() = default;

  EndPoint(const std::string& host, uint16_t port) : host_(host), port_(port) {}

  std::string Host() const { return host_; }
  void SetHost(const std::string& host) { host_ = host; }

  uint16_t Port() const { return port_; }
  void SetPort(uint16_t port) { port_ = port; }

  std::string ToString() const { return host_ + ":" + std::to_string(port_); }

  std::string StringAddr() const { return host_ + ":" + std::to_string(port_); }

  void ReSet() {
    host_.clear();
    port_ = 0;
  }

  bool IsValid() const { return !host_.empty() && port_ > 0; }

  bool operator==(const EndPoint& other) const { return host_ == other.host_ && port_ == other.port_; }

  bool operator!=(const EndPoint& other) const { return !(*this == other); }

  bool operator<(const EndPoint& other) const {
    if (host_ < other.host_) {
      return true;
    }

    if (host_ == other.host_ && port_ < other.port_) {
      return true;
    }

    return false;
  }

 private:
  std::string host_;
  uint16_t port_;
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_UTILS_NET_UTIL_H
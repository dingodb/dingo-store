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

#ifndef DINGODB_SDK_META_MEMBER_INFO_H_
#define DINGODB_SDK_META_MEMBER_INFO_H_

#include <atomic>
#include <cstdint>
#include <shared_mutex>
#include <sstream>
#include <vector>

#include "sdk/utils/net_util.h"
namespace dingodb {
namespace sdk {

class MetaMemberInfo {
 public:
  MetaMemberInfo() = default;

  MetaMemberInfo(std::vector<EndPoint> members) : members_(std::move(members)) {}

  ~MetaMemberInfo() = default;

  EndPoint PickNextLeader();

  void MarkLeader(const EndPoint &end_point);

  void MarkFollower(const EndPoint &end_point);

  std::vector<EndPoint> GetMembers() const;

  void SetMembers(std::vector<EndPoint> members);

  std::string ToString() const {
    std::shared_lock lock(rw_lock_);

    std::stringstream ss;
    ss << "leader: " << leader_.ToString() << ", members: [";
    for (const auto &member : members_) {
      ss << member.ToString() << ", ";
    }
    ss << "]";

    return ss.str();
  }

 private:
  mutable std::shared_mutex rw_lock_;
  EndPoint leader_;
  std::vector<EndPoint> members_;

  std::atomic<int64_t> next_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_META_MEMBER_INFO_H_

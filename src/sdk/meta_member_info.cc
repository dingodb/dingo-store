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

#include "sdk/meta_member_info.h"

#include <algorithm>
#include <mutex>

#include "glog/logging.h"
#include "sdk/utils/net_util.h"

namespace dingodb {
namespace sdk {

EndPoint MetaMemberInfo::PickNextLeader() {
  EndPoint leader;

  {
    std::shared_lock lock(rw_lock_);

    if (leader_.IsValid()) {
      leader = leader_;
    } else {
      leader = members_[next_ % members_.size()];
      next_++;
    }
  }

  return leader;
}

void MetaMemberInfo::MarkLeader(const EndPoint &end_point) {
  CHECK(end_point.IsValid()) << "end_point is invalid: " << end_point.ToString();
  std::unique_lock lock(rw_lock_);
  leader_ = end_point;

  auto it = std::find(members_.begin(), members_.end(), end_point);
  if (it == members_.end()) {
    members_.push_back(end_point);
  }
}

void MetaMemberInfo::MarkFollower(const EndPoint &end_point) {
  CHECK(end_point.IsValid()) << "end_point is invalid: " << end_point.ToString();
  std::unique_lock lock(rw_lock_);
  if (leader_ == end_point) {
    leader_.ReSet();
  }

  auto it = std::find(members_.begin(), members_.end(), end_point);
  if (it == members_.end()) {
    members_.push_back(end_point);
  }
}

void MetaMemberInfo::SetMembers(std::vector<EndPoint> members) {
  std::unique_lock lock(rw_lock_);
  members_ = std::move(members);
}

std::vector<EndPoint> MetaMemberInfo::GetMembers() const {
  std::shared_lock lock(rw_lock_);
  return members_;
}

}  // namespace sdk

}  // namespace dingodb

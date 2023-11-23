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

#include "sdk/region.h"

#include "common/logging.h"

namespace dingodb {
namespace sdk {

Region::Region(int64_t id, pb::common::Range range, pb::common::RegionEpoch epoch, pb::common::RegionType type,
               std::vector<Replica> replicas)
    : region_id_(id),
      range_(std::move(range)),
      epoch_(std::move(epoch)),
      region_type_(type),
      replicas_(std::move(replicas)),
      stale_(true) {
  for (auto& r : replicas_) {
    if (r.role == kLeader) {
      leader_addr_ = r.end_point;
      break;
    }
  }
}

std::vector<Replica> Region::Replicas() {
  std::shared_lock<std::shared_mutex> r(rw_lock_);
  return replicas_;
}

std::vector<butil::EndPoint> Region::ReplicaEndPoint() {
  std::shared_lock<std::shared_mutex> r(rw_lock_);

  std::vector<butil::EndPoint> end_points;
  end_points.reserve(replicas_.size());
  for (const auto& r : replicas_) {
    end_points.push_back(r.end_point);
  }

  return end_points;
}

void Region::MarkLeader(const butil::EndPoint& end_point) {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  for (auto& r : replicas_) {
    if (r.end_point == end_point) {
      r.role = kLeader;
    } else {
      r.role = kFollower;
    }
  }

  leader_addr_ = end_point;

  DINGO_LOG(INFO) << "region:" << region_id_ << " replicas:" << ReplicasAsStringUnlocked();
}

void Region::MarkFollower(const butil::EndPoint& end_point) {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  for (auto& r : replicas_) {
    if (r.end_point == end_point) {
      r.role = kFollower;
    }
  }

  if (leader_addr_ == end_point) {
    leader_addr_.reset();
  }

  DINGO_LOG(INFO) << "region:" << region_id_ << " mark replica:" << butil::endpoint2str(end_point).c_str()
                  << " follower, current replicas:" << ReplicasAsStringUnlocked();
}

Status Region::GetLeader(butil::EndPoint& leader) {
  std::shared_lock<std::shared_mutex> r(rw_lock_);
  if (leader_addr_.ip != butil::IP_ANY && leader_addr_.port != 0) {
    leader = leader_addr_;
    return Status::OK();
  }

  std::string msg = fmt::format("region:{} not found leader", region_id_);
  DINGO_LOG(WARNING) << msg << " replicas:" << ReplicasAsStringUnlocked();
  return Status::NotFound(msg);
}

std::string Region::ReplicasAsString() const {
  std::shared_lock<std::shared_mutex> r(rw_lock_);
  return ReplicasAsStringUnlocked();
}

std::string Region::ReplicasAsStringUnlocked() const {
  std::string replicas_str;
  for (const auto& r : replicas_) {
    if (!replicas_str.empty()) {
      replicas_str.append(", ");
    }

    std::string msg = fmt::format("({},{})", butil::endpoint2str(r.end_point).c_str(), RaftRoleName(r.role));
    replicas_str.append(msg);
  }
  return replicas_str;
}

}  // namespace sdk
}  // namespace dingodb
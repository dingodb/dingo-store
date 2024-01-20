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

#ifndef DINGODB_SDK_REGION_H_
#define DINGODB_SDK_REGION_H_

#include <cstdint>
#include <shared_mutex>

#include "butil/endpoint.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

class MetaCache;

enum RaftRole : uint8_t { kLeader, kFollower };

struct Replica {
  butil::EndPoint end_point;
  RaftRole role;
};

class Region {
 public:
  Region(const Region&) = delete;
  const Region& operator=(const Region&) = delete;

  explicit Region(int64_t id, pb::common::Range range, pb::common::RegionEpoch epoch, pb::common::RegionType type,
                  std::vector<Replica> replicas);

  ~Region() = default;

  int64_t RegionId() const { return region_id_; }

  const pb::common::Range& Range() const { return range_; }

  const pb::common::RegionEpoch& Epoch() const { return epoch_; }

  pb::common::RegionType RegionType() const { return region_type_; }

  std::vector<Replica> Replicas();

  std::vector<butil::EndPoint> ReplicaEndPoint();

  void MarkLeader(const butil::EndPoint& end_point);

  void MarkFollower(const butil::EndPoint& end_point);

  Status GetLeader(butil::EndPoint& leader);

  bool IsStale() { return stale_.load(std::memory_order_relaxed); }

  std::string ReplicasAsString() const;

  std::string ToString() const {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    // region_id, start_key-end_key, version, config_version, type, replicas
    return fmt::format("({}, [{}-{}], [{},{}], {}, {})", region_id_, range_.start_key(), range_.end_key(),
                       epoch_.version(), epoch_.conf_version(), RegionType_Name(region_type_),
                       ReplicasAsStringUnlocked());
  }

  void TEST_MarkStale() {  // NOLINT
    MarkStale();
  }

  void TEST_UnMarkStale() {  // NOLINT
    UnMarkStale();
  }

 private:
  friend class MetaCache;

  void MarkStale() { stale_.store(true, std::memory_order_relaxed); }

  void UnMarkStale() { stale_.store(false, std::memory_order_relaxed); }

  std::string ReplicasAsStringUnlocked() const;

  const int64_t region_id_;
  const pb::common::Range range_;
  const pb::common::RegionEpoch epoch_;
  const pb::common::RegionType region_type_;

  mutable std::shared_mutex rw_lock_;
  butil::EndPoint leader_addr_;
  std::vector<Replica> replicas_;

  std::atomic<bool> stale_;
};

inline std::ostream& operator<<(std::ostream& os, const Region& region) { return os << region.ToString(); }

static std::string RaftRoleName(const RaftRole& role) {
  switch (role) {
    case kLeader:
      return "Leader";
    case kFollower:
      return "Follower";
    default:
      CHECK(false) << "role is illeagal";
  }
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_REGION_H_
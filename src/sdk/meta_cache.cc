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

#include "sdk/meta_cache.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "butil/endpoint.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

using pb::coordinator::ScanRegionInfo;

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

  DINGO_LOG_DEBUG << "region:" << region_id_ << " replicas:" << ReplicasAsStringUnlocked();
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

  DINGO_LOG_DEBUG << "region:" << region_id_ << " replicas:" << ReplicasAsStringUnlocked();
}

Status Region::GetLeader(butil::EndPoint& leader) {
  std::shared_lock<std::shared_mutex> r(rw_lock_);
  if (leader_addr_.ip != butil::IP_ANY && leader_addr_.port != 0) {
    leader = leader_addr_;
    return Status::OK();
  }

  std::string msg = fmt::format("region:{} not found leader", region_id_);
  DINGO_LOG_INFO << msg << " replicas:" << ReplicasAsStringUnlocked();
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

MetaCache::MetaCache(std::shared_ptr<CoordinatorInteraction> coordinator_interaction)
    : coordinator_interaction_(std::move(coordinator_interaction)) {}

MetaCache::~MetaCache() = default;

Status MetaCache::LookupRegionByKey(const std::string& key, std::shared_ptr<Region>& region) {
  Status s;
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    s = FastLookUpRegionByKeyUnlocked(key, region);
    if (s.ok()) {
      return s;
    }
  }

  s = SlowLookUpRegionByKey(key, region);
  return s;
}

void MetaCache::ClearRange(const std::shared_ptr<Region>& region) {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  auto iter = region_by_id_.find(region->RegionId());
  if (region->IsStale()) {
    CHECK(iter == region_by_id_.end());
    return;
  } else {
    CHECK(iter != region_by_id_.end());
    RemoveRegionUnlocked(region->RegionId());
  }
}

void MetaCache::MaybeAddRegion(const std::shared_ptr<Region>& new_region) {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  MaybeAddRegionUnlocked(new_region);
}

void MetaCache::MaybeAddRegionUnlocked(const std::shared_ptr<Region>& new_region) {
  CHECK(new_region.get() != nullptr);
  auto region_id = new_region->RegionId();
  auto iter = region_by_id_.find(region_id);
  if (iter != region_by_id_.end()) {
    // old region has same region_id
    if (NeedUpdateRegion(iter->second, new_region)) {
      // old region is stale
      RemoveRegionUnlocked(region_id);
    } else {
      // old region same epoch or newer
      return;
    }
  }

  AddRangeToCacheUnlocked(new_region);
}

Status MetaCache::FastLookUpRegionByKeyUnlocked(const std::string& key, std::shared_ptr<Region>& region) {
  auto iter = region_by_key_.upper_bound(key);
  if (iter == region_by_key_.begin()) {
    return Status::NotFound(fmt::format("not found region for key:{}", key));
  }

  iter--;
  auto found_region = iter->second;
  CHECK(!found_region->IsStale());

  auto range = found_region->Range();
  CHECK(key >= range.start_key());

  if (key >= range.end_key()) {
    std::string msg =
        fmt::format("not found range, key:{} is out of bounds range:({}-{})", key, range.start_key(), range.end_key());

    DINGO_LOG_DEBUG << msg;
    return Status::NotFound(msg);
  } else {
    // lucky we found it
    region = found_region;
    return Status::OK();
  }
}

Status MetaCache::SlowLookUpRegionByKey(const std::string& key, std::shared_ptr<Region>& region) {
  pb::coordinator::ScanRegionsRequest request;
  pb::coordinator::ScanRegionsResponse response;
  request.set_key(key);
  Status send = SendScanRegionsRequest(request, response);
  if (!send.ok()) {
    return send;
  }

  return ProcessScanRangeByKeyResponse(response, region);
}

Status MetaCache::ProcessScanRangeByKeyResponse(const pb::coordinator::ScanRegionsResponse& response,
                                                std::shared_ptr<Region>& region) {
  if (response.regions_size() > 0) {
    CHECK(response.regions_size() == 1) << "expect ScanRegionsResponse  has one region";

    const auto& scan_region_info = response.regions(0);
    std::shared_ptr<Region> new_region;
    ProcessScanRegionInfo(scan_region_info, new_region);
    {
      std::unique_lock<std::shared_mutex> w(rw_lock_);
      MaybeAddRegionUnlocked(new_region);
      auto iter = region_by_id_.find(scan_region_info.region_id());
      CHECK(iter != region_by_id_.end());
      CHECK(iter->second.get() != nullptr);
      region = iter->second;
    }
    return Status::OK();
  } else {
    DINGO_LOG_INFO << "response:" << response.DebugString();
    return Status::NotFound("region not found");
  }
}

void MetaCache::ProcessScanRegionInfo(const ScanRegionInfo& scan_region_info, std::shared_ptr<Region>& region) {
  int64_t region_id = scan_region_info.region_id();
  CHECK(scan_region_info.has_range());
  CHECK(scan_region_info.has_region_epoch());

  std::vector<Replica> replicas;
  if (scan_region_info.has_leader()) {
    const auto& leader = scan_region_info.leader();
    replicas.push_back({Helper::LocationToEndPoint(leader), kLeader});
  }

  for (const auto& voter : scan_region_info.voters()) {
    replicas.push_back({Helper::LocationToEndPoint(voter), kFollower});
  }

  // TODO: support learner
  for (const auto& leaner : scan_region_info.learners()) {
    replicas.push_back({Helper::LocationToEndPoint(leaner), kFollower});
  }

  region = std::make_shared<Region>(region_id, scan_region_info.range(), scan_region_info.region_epoch(),
                                    scan_region_info.status().region_type(), replicas);
}

bool MetaCache::NeedUpdateRegion(const std::shared_ptr<Region>& old_region, const std::shared_ptr<Region>& new_region) {
  return EpochCompare(old_region->Epoch(), new_region->Epoch()) > 0;
}

void MetaCache::RemoveRegionIfPresentUnlocked(int64_t region_id) {
  if (region_by_id_.find(region_id) != region_by_id_.end()) {
    RemoveRegionUnlocked(region_id);
  }
}

void MetaCache::RemoveRegionUnlocked(int64_t region_id) {
  auto iter = region_by_id_.find(region_id);
  CHECK(iter != region_by_id_.end());

  auto region = iter->second;
  region->MarkStale();
  region_by_id_.erase(iter);

  CHECK(region_by_key_.erase(region->Range().start_key()) == 1);

  DINGO_LOG_INFO << "remove region and mark stale, region_id:" << region_id << ", region: " << region->ToString();
}

void MetaCache::AddRangeToCacheUnlocked(const std::shared_ptr<Region>& region) {
  auto region_start_key = region->Range().start_key();

  std::vector<std::shared_ptr<Region>> to_removes;
  auto key_iter = region_by_key_.lower_bound(region_start_key);

  // remove before range when end_key > region_start_key
  if (key_iter != region_by_key_.begin()) {
    key_iter--;
    auto to_remove_start_key = key_iter->second->Range().start_key();
    CHECK(to_remove_start_key < region_start_key)
        << "to_remove_start_key:" << to_remove_start_key << " expect le:" << region_start_key;

    if (key_iter->second->Range().end_key() > region_start_key) {
      to_removes.emplace_back(key_iter->second);
    }
    key_iter++;
  }

  auto region_end_key = region->Range().end_key();
  // remove ranges which  region_start_key <= start_key < region_end_key
  while (key_iter != region_by_key_.end() && key_iter->second->Range().start_key() < region_end_key) {
    to_removes.emplace_back(key_iter->second);
    key_iter++;
  }

  for (const auto& remove : to_removes) {
    RemoveRegionUnlocked(remove->RegionId());
  }

  // add region to cache
  CHECK(region_by_id_.insert(std::make_pair(region->RegionId(), region)).second);
  CHECK(region_by_key_.insert(std::make_pair(region->Range().start_key(), region)).second);

  region->UnMarkStale();

  DINGO_LOG_DEBUG << "add region success, region:" << region->ToString();
}

void MetaCache::Dump() {
  std::shared_lock<std::shared_mutex> r(rw_lock_);
  for (const auto& r : region_by_id_) {
    std::string dump = fmt::format("region_id:{}, region:{}", r.first, r.second->ToString());
    DINGO_LOG_INFO << dump;
  }

  for (const auto& r : region_by_key_) {
    std::string dump = fmt::format("start_key:{}, region:{}", r.first, r.second->ToString());
    DINGO_LOG_INFO << dump;
  }
}

}  // namespace sdk
}  // namespace dingodb
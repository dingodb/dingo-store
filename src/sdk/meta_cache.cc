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

#include <string_view>

#include "common/logging.h"
#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

using pb::coordinator::ScanRegionInfo;

MetaCache::MetaCache(std::shared_ptr<CoordinatorProxy> coordinator_proxy)
    : coordinator_proxy_(std::move(coordinator_proxy)) {}

MetaCache::~MetaCache() = default;

Status MetaCache::LookupRegionByKey(std::string_view key, std::shared_ptr<Region>& region) {
  CHECK(!key.empty()) << "key should not empty";
  Status s;
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    s = FastLookUpRegionByKeyUnlocked(key, region);
    if (s.IsOK()) {
      return s;
    }
  }

  s = SlowLookUpRegionByKey(key, region);
  return s;
}

Status MetaCache::LookupRegionBetweenRange(std::string_view start_key, std::string_view end_key,
                                           std::shared_ptr<Region>& region) {
  CHECK(!start_key.empty()) << "start_key should not empty";
  CHECK(!end_key.empty()) << "end_key should not empty";
  Status s;
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    s = FastLookUpRegionByKeyUnlocked(start_key, region);
    if (s.IsOK()) {
      return s;
    }
  }

  std::vector<std::shared_ptr<Region>> regions;
  s = ScanRegionsBetweenRange(start_key, end_key, kPrefetchRegionCount, regions);
  if (s.IsOK() && !regions.empty()) {
    region = std::move(regions.front());
  }

  return s;
}

Status MetaCache::LookupRegionBetweenRangeNoPrefetch(std::string_view start_key, std::string_view end_key,
                                                     std::shared_ptr<Region>& region) {
  CHECK(!start_key.empty()) << "start_key should not empty";
  CHECK(!end_key.empty()) << "end_key should not empty";
  Status s;
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    s = FastLookUpRegionByKeyUnlocked(start_key, region);
    if (s.IsOK()) {
      return s;
    }
  }

  std::vector<std::shared_ptr<Region>> regions;
  s = ScanRegionsBetweenRange(start_key, end_key, 1, regions);
  if (s.IsOK() && !regions.empty()) {
    region = std::move(regions.front());
  }

  return s;
}

Status MetaCache::ScanRegionsBetweenRange(std::string_view start_key, std::string_view end_key, int64_t limit,
                                          std::vector<std::shared_ptr<Region>>& regions) {
  CHECK(!start_key.empty()) << "start_key should not empty";
  CHECK(!end_key.empty()) << "end_key should not empty";
  CHECK_GE(limit, 0) << "limit should greater or equal 0";

  pb::coordinator::ScanRegionsRequest request;
  pb::coordinator::ScanRegionsResponse response;
  request.set_key(std::string(start_key));
  request.set_range_end(std::string(end_key));
  request.set_limit(limit);

  Status send = SendScanRegionsRequest(request, response);
  if (!send.IsOK()) {
    return send;
  }

  return ProcessScanRegionsBetweenRangeResponse(response, regions);
}

Status MetaCache::ScanRegionsBetweenContinuousRange(std::string_view start_key, std::string_view end_key,
                                                    std::vector<std::shared_ptr<Region>>& regions) {
  std::vector<std::shared_ptr<Region>> to_return;
  {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    // find region start_key >= start_key
    auto start_iter = region_by_key_.lower_bound(start_key);
    if (start_iter != region_by_key_.end() && start_iter->first == start_key) {
      // find region start_key >= end_key
      auto end_iter = region_by_key_.lower_bound(end_key);
      if (end_iter != region_by_key_.begin()) {
        end_iter--;
        if (end_iter->second->Range().end_key() == end_key) {
          auto iter = start_iter;
          while (iter != end_iter) {
            to_return.push_back(iter->second);
            iter++;
          }
          to_return.push_back(end_iter->second);
        }
      }
    }
  }

  if (!to_return.empty()) {
    if (to_return.size() == 1) {
      auto& find = to_return[0];
      CHECK_EQ(find->Range().start_key(), start_key);
      CHECK_EQ(find->Range().end_key(), end_key);
      regions.swap(to_return);
      return Status::OK();
    } else {
      auto cur = to_return.begin();
      auto next = cur;
      next++;
      CHECK(next != to_return.end());

      bool continues = true;
      while (next != to_return.end()) {
        if ((*cur)->Range().end_key() != (*next)->Range().start_key()) {
          continues = false;
          break;
        }
        ++cur;
        ++next;
      }

      if (continues) {
        CHECK(!to_return.empty());
        regions.swap(to_return);
        return Status::OK();
      }
    }
  }

  pb::coordinator::ScanRegionsRequest request;
  pb::coordinator::ScanRegionsResponse response;
  request.set_key(std::string(start_key));
  request.set_range_end(std::string(end_key));
  request.set_limit(0);

  Status send = SendScanRegionsRequest(request, response);
  if (!send.IsOK()) {
    return send;
  }

  return ProcessScanRegionsBetweenRangeResponse(response, regions);
}

void MetaCache::ClearRange(const std::shared_ptr<Region>& region) {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  auto iter = region_by_id_.find(region->RegionId());
  if (region->IsStale()) {
    DINGO_LOG(DEBUG) << "region is stale, no need clear, region:" << region->ToString();
    return;
  } else {
    CHECK(iter != region_by_id_.end());
    RemoveRegionUnlocked(region->RegionId());
  }
}

void MetaCache::RemoveRegion(int64_t region_id) {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  RemoveRegionIfPresentUnlocked(region_id);
}

void MetaCache::RemoveRegionIfPresentUnlocked(int64_t region_id) {
  if (region_by_id_.find(region_id) != region_by_id_.end()) {
    RemoveRegionUnlocked(region_id);
  }
}

void MetaCache::ClearCache() {
  std::unique_lock<std::shared_mutex> w(rw_lock_);
  for (const auto& [region_id, region] : region_by_id_) {
    region->MarkStale();
  }
  region_by_key_.clear();
  region_by_id_.clear();
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

Status MetaCache::FastLookUpRegionByKeyUnlocked(std::string_view key, std::shared_ptr<Region>& region) {
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
        fmt::format("not found region for key:{} in cache, key is out of bounds, nearest found_region:{} range:({}-{})",
                    key, found_region->RegionId(), range.start_key(), range.end_key());

    DINGO_LOG(DEBUG) << msg;
    return Status::NotFound(msg);
  } else {
    // lucky we found it
    region = found_region;
    return Status::OK();
  }
}

Status MetaCache::SlowLookUpRegionByKey(std::string_view key, std::shared_ptr<Region>& region) {
  pb::coordinator::ScanRegionsRequest request;
  pb::coordinator::ScanRegionsResponse response;
  request.set_key(std::string(key));
  Status send = SendScanRegionsRequest(request, response);
  if (!send.IsOK()) {
    return send;
  }

  return ProcessScanRegionsByKeyResponse(response, region);
}

Status MetaCache::SendScanRegionsRequest(const pb::coordinator::ScanRegionsRequest& request,
                                         pb::coordinator::ScanRegionsResponse& response) {
  return coordinator_proxy_->ScanRegions(request, response);
}

Status MetaCache::ProcessScanRegionsByKeyResponse(const pb::coordinator::ScanRegionsResponse& response,
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
    DINGO_LOG(WARNING) << "response:" << response.DebugString();
    return Status::NotFound("region not found");
  }
}

Status MetaCache::ProcessScanRegionsBetweenRangeResponse(const pb::coordinator::ScanRegionsResponse& response,
                                                         std::vector<std::shared_ptr<Region>>& regions) {
  if (response.regions_size() > 0) {
    std::vector<std::shared_ptr<Region>> tmp_regions;

    for (const auto& scan_region_info : response.regions()) {
      std::shared_ptr<Region> new_region;
      ProcessScanRegionInfo(scan_region_info, new_region);
      {
        std::unique_lock<std::shared_mutex> w(rw_lock_);
        MaybeAddRegionUnlocked(new_region);
        auto iter = region_by_id_.find(scan_region_info.region_id());
        CHECK(iter != region_by_id_.end());
        CHECK(iter->second.get() != nullptr);
        tmp_regions.push_back(iter->second);
      }
    }

    CHECK(!tmp_regions.empty());
    regions = std::move(tmp_regions);

    return Status::OK();
  } else {
    DINGO_LOG(DEBUG) << "no scan_region_info in ScanRegionsResponse, response:" << response.DebugString();
    return Status::NotFound("regions not found");
  }
}

// TODO: check region state
void MetaCache::ProcessScanRegionInfo(const ScanRegionInfo& scan_region_info, std::shared_ptr<Region>& region) {
  int64_t region_id = scan_region_info.region_id();
  CHECK(scan_region_info.has_range());
  CHECK(scan_region_info.has_region_epoch());

  std::vector<Replica> replicas;
  if (scan_region_info.has_leader()) {
    const auto& leader = scan_region_info.leader();
    auto endpoint = Helper::LocationToEndPoint(leader);
    if (endpoint.ip == butil::IP_ANY || endpoint.port == 0) {
      DINGO_LOG(WARNING) << "receive leader is invalid:" << butil::endpoint2str(endpoint);
    } else {
      replicas.push_back({endpoint, kLeader});
    }
  }

  for (const auto& voter : scan_region_info.voters()) {
    auto endpoint = Helper::LocationToEndPoint(voter);
    if (endpoint.ip == butil::IP_ANY || endpoint.port == 0) {
      DINGO_LOG(WARNING) << "receive voter is invalid:" << butil::endpoint2str(endpoint);
    } else {
      replicas.push_back({endpoint, kFollower});
    }
  }

  // TODO: support learner
  for (const auto& leaner : scan_region_info.learners()) {
    auto endpoint = Helper::LocationToEndPoint(leaner);
    if (endpoint.ip == butil::IP_ANY || endpoint.port == 0) {
      DINGO_LOG(WARNING) << "receive leaner invalid:" << butil::endpoint2str(endpoint);
    } else {
      replicas.push_back({endpoint, kFollower});
    }
  }

  region = std::make_shared<Region>(region_id, scan_region_info.range(), scan_region_info.region_epoch(),
                                    scan_region_info.status().region_type(), replicas);
}

bool MetaCache::NeedUpdateRegion(const std::shared_ptr<Region>& old_region, const std::shared_ptr<Region>& new_region) {
  return EpochCompare(old_region->Epoch(), new_region->Epoch()) > 0;
}

void MetaCache::RemoveRegionUnlocked(int64_t region_id) {
  auto iter = region_by_id_.find(region_id);
  CHECK(iter != region_by_id_.end());

  auto region = iter->second;
  region->MarkStale();
  region_by_id_.erase(iter);

  CHECK(region_by_key_.erase(region->Range().start_key()) == 1);

  DINGO_LOG(DEBUG) << "remove region and mark stale, region_id:" << region_id << ", region: " << region->ToString();
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

  DINGO_LOG(DEBUG) << "add region success, region:" << region->ToString();
}

void MetaCache::Dump() {
  std::shared_lock<std::shared_mutex> r(rw_lock_);
  DumpUnlocked();
}

void MetaCache::DumpUnlocked() {
  for (const auto& r : region_by_id_) {
    std::string dump = fmt::format("region_id:{}, region:{}", r.first, r.second->ToString());
    DINGO_LOG(INFO) << dump;
  }

  for (const auto& r : region_by_key_) {
    std::string dump = fmt::format("start_key:{}, region:{}", r.first, r.second->ToString());
    DINGO_LOG(INFO) << dump;
  }
}
}  // namespace sdk
}  // namespace dingodb
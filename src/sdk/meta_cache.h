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

#ifndef DINGODB_SDK_META_CACHE_H_
#define DINGODB_SDK_META_CACHE_H_

#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "proto/coordinator.pb.h"
#include "sdk/coordinator_proxy.h"
#include "sdk/region.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {

class MetaCache {
 public:
  MetaCache(const MetaCache&) = delete;
  const MetaCache& operator=(const MetaCache&) = delete;

  explicit MetaCache(std::shared_ptr<CoordinatorProxy> coordinator_proxy);

  ~MetaCache();

  Status LookupRegionByKey(std::string_view key, std::shared_ptr<Region>& region);

  // return first region between [start_key, end_key), this will prefetch regions and put into cache
  Status LookupRegionBetweenRange(std::string_view start_key, std::string_view end_key,
                                  std::shared_ptr<Region>& region);

  // return first region between [start_key, end_key), no prefetch regions
  Status LookupRegionBetweenRangeNoPrefetch(std::string_view start_key, std::string_view end_key,
                                            std::shared_ptr<Region>& region);

  // NOTE: this will not lookup cache and will send rpc request directly to coordinator
  // limit: 0 means no limit and will return all regions between [start_key, end_key)
  Status ScanRegionsBetweenRange(std::string_view start_key, std::string_view end_key, int64_t limit,
                                 std::vector<std::shared_ptr<Region>>& regions);

  //  return all regions between [start_key, end_key), used for get partion regions
  Status ScanRegionsBetweenContinuousRange(std::string_view start_key, std::string_view end_key, std::vector<std::shared_ptr<Region>>& regions);

  void ClearRange(const std::shared_ptr<Region>& region);

  void RemoveRegion(int64_t region_id);

  void ClearCache();

  // be sure new_region will not destroy when call this func
  void MaybeAddRegion(const std::shared_ptr<Region>& new_region);

  Status TEST_FastLookUpRegionByKey(std::string_view key, std::shared_ptr<Region>& region) {  // NOLINT
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    return FastLookUpRegionByKeyUnlocked(key, region);
  }

  void Dump();

 private:
  // TODO: backoff when region not ready
  Status SlowLookUpRegionByKey(std::string_view key, std::shared_ptr<Region>& region);

  Status FastLookUpRegionByKeyUnlocked(std::string_view key, std::shared_ptr<Region>& region);

  Status SendScanRegionsRequest(const pb::coordinator::ScanRegionsRequest& request,
                                pb::coordinator::ScanRegionsResponse& response);

  Status ProcessScanRegionsByKeyResponse(const pb::coordinator::ScanRegionsResponse& response,
                                         std::shared_ptr<Region>& region);

  Status ProcessScanRegionsBetweenRangeResponse(const pb::coordinator::ScanRegionsResponse& response,
                                                std::vector<std::shared_ptr<Region>>& regions);

  static void ProcessScanRegionInfo(const pb::coordinator::ScanRegionInfo& scan_region_info,
                                    std::shared_ptr<Region>& new_region);

  void MaybeAddRegionUnlocked(const std::shared_ptr<Region>& new_region);

  void RemoveRegionIfPresentUnlocked(int64_t region_id);

  // NOTE: be sure region is exist
  void RemoveRegionUnlocked(int64_t region_id);

  void AddRangeToCacheUnlocked(const std::shared_ptr<Region>& region);

  void DumpUnlocked();

  static bool NeedUpdateRegion(const std::shared_ptr<Region>& old_region, const std::shared_ptr<Region>& new_region);

  std::shared_ptr<CoordinatorProxy> coordinator_proxy_;

  mutable std::shared_mutex rw_lock_;
  std::unordered_map<int64_t, std::shared_ptr<Region>> region_by_id_;
  // start-key -> region
  std::map<std::string, std::shared_ptr<Region>, std::less<void>> region_by_key_;
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_META_CACHE_H_
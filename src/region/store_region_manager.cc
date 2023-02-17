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

#include "region/store_region_manager.h"

#include "glog/logging.h"
#include "butil/memory/singleton.h"
#include "butil/strings/stringprintf.h"

namespace dingodb {


StoreRegionManager::StoreRegionManager() {}
StoreRegionManager::~StoreRegionManager() {}

StoreRegionManager* StoreRegionManager::GetInstance() {
  return Singleton<StoreRegionManager>::get();
}

bool StoreRegionManager::IsExist(uint64_t region_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return regions_.find(region_id) != regions_.end();
}

void StoreRegionManager::AddRegion(uint64_t region_id, const dingodb::pb::common::RegionInfo& region) {
  if (IsExist(region_id)) {
    LOG(WARNING) << butil::StringPrintf("region %lu already exist!", region_id);
    return;
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);
  regions_.insert(std::make_pair(region_id, std::make_shared<dingodb::pb::common::RegionInfo>(region)));
}

std::shared_ptr<dingodb::pb::common::RegionInfo> StoreRegionManager::GetRegion(uint64_t region_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    LOG(WARNING) << butil::StringPrintf("region %lu not exist!", region_id);
    return nullptr;
  }
  
  return it->second;
}

std::vector<std::shared_ptr<dingodb::pb::common::RegionInfo> > StoreRegionManager::GetAllRegion() {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::vector<std::shared_ptr<dingodb::pb::common::RegionInfo> > result;
  for (auto it = regions_.begin(); it != regions_.end(); ++it) {
    result.push_back(it->second);
  }

  return result;
}


} // namespace dingodb
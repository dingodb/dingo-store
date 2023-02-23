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


#ifndef DINGODB_REGION_STORE_REGION_MANAGER_H_
#define DINGODB_REGION_STORE_REGION_MANAGER_H_

#include <memory>
#include <vector>
#include <shared_mutex>

#include "butil/macros.h"
#include "proto/common.pb.h"

template <typename T> struct DefaultSingletonTraits;

namespace dingodb {

class StoreRegionManager {
 public:
 static StoreRegionManager* GetInstance();

  bool IsExist(uint64_t region_id);
  void AddRegion(uint64_t region_id, const dingodb::pb::common::Region& region);
  std::shared_ptr<dingodb::pb::common::Region> GetRegion(uint64_t region_id);
  std::vector<std::shared_ptr<dingodb::pb::common::Region> > GetAllRegion();

 private:
  StoreRegionManager();
  ~StoreRegionManager();
  friend struct DefaultSingletonTraits<StoreRegionManager>;
  DISALLOW_COPY_AND_ASSIGN(StoreRegionManager);

  std::shared_mutex mutex_;
  std::map<uint64_t, std::shared_ptr<dingodb::pb::common::Region> > regions_;
};

} // namespace dingodb

#endif
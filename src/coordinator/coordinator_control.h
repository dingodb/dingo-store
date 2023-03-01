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

#ifndef DINGODB_COORDINATOR_COMMON_H_
#define DINGODB_COORDINATOR_COMMON_H_

#include <cstdint>
#include <vector>

#include "brpc/controller.h"
#include "brpc/server.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

class CoordinatorControl {
 public:
  CoordinatorControl() = default;
  void Init();
  uint64_t CreateCoordinatorId();
  uint64_t CreateStoreId();
  uint64_t CreateRegionId();

  uint64_t CreateSchemaId();
  uint64_t CreateTableId();
  uint64_t CreatePartitionId();

  // create region
  uint64_t CreateRegion();

  // update store map with new Store info
  // return new epoch
  uint64_t UpdateStoreMap(const pb::common::Store &store);

  // get storemap
  const pb::common::StoreMap &GetStoreMap();

  // update region map with new Region info
  // return new epoch
  bool UpdateOneRegionMap(const pb::common::Region &region);
  uint64_t UpdateRegionMap(const pb::common::Region &region);
  uint64_t UpdateRegionMapMulti(std::vector<pb::common::Region> regions);

  // get regionmap
  const pb::common::RegionMap &GetRegionMap();

 private:
  // global ids
  uint64_t next_coordinator_id_;
  uint64_t next_store_id_;
  uint64_t next_region_id_;

  uint64_t next_schema_id_;
  uint64_t next_table_id_;
  uint64_t next_partition_id_;

  // global maps
  uint64_t region_map_epoch_;
  pb::common::RegionMap region_map_;
  uint64_t store_map_epoch_;
  pb::common::StoreMap store_map_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_COMMON_H_
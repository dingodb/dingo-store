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
#include <map>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "brpc/server.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
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

  // create schema
  // in: parent_schema_id
  // in: schema_name
  // out: new schema_id
  int CreateSchema(uint64_t parent_schema_id, std::string schema_name,
                   uint64_t &new_schema_id);

  // create store
  // in: cluster_id
  // out: store_id, password
  int CreateStore(uint64_t cluster_id, uint64_t &store_id,
                  std::string &password);

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

  // get schemas
  void GetSchemas(uint64_t schema_id, std::vector<pb::common::Schema> &schemas);

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

  // schemas
  std::map<uint64_t, pb::common::Schema> schema_map_;

  // tables
  std::map<uint64_t, pb::common::Table> table_map_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_COMMON_H_
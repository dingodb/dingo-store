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
#include "bthread/types.h"
#include "butil/scoped_lock.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"

namespace dingodb {

class CoordinatorControl {
 public:
  CoordinatorControl();
  static void GenerateRootSchemas(pb::meta::Schema &root_schema, pb::meta::Schema &meta_schema,
                                  pb::meta::Schema &dingo_schema);
  static void GenerateRootSchemasMetaIncrement(pb::meta::Schema &root_schema, pb::meta::Schema &meta_schema,
                                               pb::meta::Schema &dingo_schema,
                                               pb::coordinator_internal::MetaIncrement &meta_increment);
  void Init();
  uint64_t CreateCoordinatorId(pb::coordinator_internal::MetaIncrement &meta_increment);
  uint64_t CreateStoreId(pb::coordinator_internal::MetaIncrement &meta_increment);
  uint64_t CreateRegionId(pb::coordinator_internal::MetaIncrement &meta_increment);
  uint64_t CreateSchemaId(pb::coordinator_internal::MetaIncrement &meta_increment);
  uint64_t CreateTableId(pb::coordinator_internal::MetaIncrement &meta_increment);

  // create region
  // in: resource_tag
  // out: new region id
  int CreateRegion(const std::string &region_name, const std::string &resource_tag, int32_t replica_num,
                   pb::common::Range region_range, uint64_t schema_id, uint64_t table_id, uint64_t &new_region_id,
                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // drop region
  // in:  region_id
  // return: 0 or -1
  int DropRegion(uint64_t region_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: parent_schema_id
  // in: schema_name
  // out: new schema_id
  // return: 0 or -1
  int CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t &new_schema_id,
                   pb::coordinator_internal::MetaIncrement &meta_increment);

  // create schema
  // in: schema_id
  // in: table_definition
  // out: new table_id
  // return: 0 or -1
  int CreateTable(uint64_t schema_id, const pb::meta::TableDefinition &table_definition, uint64_t &new_table_id,
                  pb::coordinator_internal::MetaIncrement &meta_increment);

  // create store
  // in: cluster_id
  // out: store_id, password
  // return: 0 or -1
  int CreateStore(uint64_t cluster_id, uint64_t &store_id, std::string &password,
                  pb::coordinator_internal::MetaIncrement &meta_increment);

  // update store map with new Store info
  // return new epoch
  uint64_t UpdateStoreMap(const pb::common::Store &store, pb::coordinator_internal::MetaIncrement &meta_increment);

  // get storemap
  void GetStoreMap(pb::common::StoreMap &store_map);

  // update region map with new Region info
  // return new epoch
  uint64_t UpdateRegionMap(std::vector<pb::common::Region> &regions,
                           pb::coordinator_internal::MetaIncrement &meta_increment);

  // get regionmap
  void GetRegionMap(pb::common::RegionMap &region_map);

  // get schemas
  void GetSchemas(uint64_t schema_id, std::vector<pb::meta::Schema> &schemas);

  // get tables
  void GetTables(uint64_t schema_id, std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids);

  // get table
  void GetTable(uint64_t schema_id, uint64_t table_id, pb::meta::Table &table);

  // get coordinator_map
  void GetCoordinatorMap(uint64_t cluster_id, uint64_t &epoch, pb::common::Location &leader_location,
                         std::vector<pb::common::Location> &locations) const;

  // on_apply callback
  // leader do need update next_xx_id, so leader call this function with update_ids=false
  void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool update_ids);

 private:
  // mutex
  bthread_mutex_t control_mutex_;

  // global ids
  uint64_t next_coordinator_id_;
  uint64_t next_store_id_;
  uint64_t next_region_id_;

  uint64_t next_schema_id_;
  uint64_t next_table_id_;
  uint64_t next_partition_id_;

  // regions
  uint64_t region_map_epoch_;
  std::map<uint64_t, pb::common::Region> region_map_;

  // stores
  uint64_t store_map_epoch_;
  std::map<uint64_t, pb::common::Store> store_map_;

  // schemas
  uint64_t schema_map_epoch_;
  std::map<uint64_t, pb::meta::Schema> schema_map_;

  // tables
  // TableInternal is combination of Table & TableDefinition
  uint64_t table_map_epoch_;
  std::map<uint64_t, pb::coordinator_internal::TableInternal> table_map_;

  // coordinators
  uint64_t coordinator_map_epoch_;
  std::map<uint64_t, pb::coordinator_internal::CoordinatorInternal> coordinator_map_;

  // root schema write to raft
  bool root_schema_writed_to_raft_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_COMMON_H_
